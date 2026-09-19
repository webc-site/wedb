//! 段物理生命周期：尺寸查询、显式删段、有界容量逐出与批量截断
//!
//! 对位 C# 设备基类的容量逐出与本地设备的段删除面：`truncate_until_segment` 为
//! 常规回收路径（推进 `start_segment` + 驱逐句柄 + 物理删段），`remove_segment`
//! 为显式单段删除；Windows 下读者持句柄导致的删除失败走延迟队列重试。

use std::{fs::metadata, io::ErrorKind, sync::atomic::Ordering};

use compio::fs::remove_file;

#[cfg(debug_assertions)]
use super::sync::SYNC_GUARD_SEGMENTS;
use super::{SegmentedDevice, handle::LOCAL_FILES};
use crate::{
  chunk::segment_shift,
  device::Device,
  error::{Error, Result},
};

impl SegmentedDevice {
  /// 获取指定段的文件大小（若段文件不存在或已被截断则返回 0）
  ///
  /// 与 C# 的刻意差异：C# `LocalStorageDevice.GetFileSize` 优先返回配置段尺寸
  /// （segmentSize > 0 时不查磁盘）；Rust 一律返回实际磁盘占用，信息更真实
  /// （对标 C# 测试 `Native_GetFileSize_ReflectsWrites` 的"反映实际写入"语义）。
  pub fn get_file_size(&self, segment_id: u32) -> Result<u64> {
    if segment_id < self.start_segment.load(Ordering::SeqCst)
      || (self.segment_size.is_none() && segment_id > 0)
    {
      return Ok(0);
    }
    let path = self.segment_path(segment_id);
    match metadata(&path) {
      Ok(meta) => Ok(meta.len()),
      Err(e) if e.kind() == ErrorKind::NotFound => Ok(0),
      Err(e) => Err(Error::Io(e)),
    }
  }

  /// 删除单个段文件并从缓存中关闭移除（对标 libs/storage/Tsavorite/cs/src/core/Device/NativeStorageDevice.cs:RemoveSegment）
  ///
  /// TLS 架构差异注记：C# 从进程级共享句柄表移除（全线程即时生效）；Rust 仅驱逐
  /// 调用线程的本地句柄——他线程 TLS 中的陈旧句柄仍指向已解除链接的 inode，后续
  /// 经其写入的数据会静默失效，故多线程场景下对同一被删段的访问须由上层串行化
  ///（C# 亦注明本方法不应常规调用，常规回收走 [`Device::truncate_until_segment`]）
  pub async fn remove_segment(&self, segment_id: u32) -> Result<()> {
    if self.segment_size.is_none() && segment_id > 0 {
      return Ok(());
    }
    LOCAL_FILES.with(|m| {
      m.borrow_mut().remove(&(self.device_id, segment_id));
    });
    #[cfg(debug_assertions)]
    self.debug_clear_segment(segment_id);
    let path = self.segment_path(segment_id);
    match remove_file(&path).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
      Err(e) => Err(Error::Io(e)),
    }
  }

  /// 有界容量设备的段逐出（对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:HandleCapacity）：
  /// 写入新段时单调推进 end_segment，若容量有限则截断至
  /// `end_segment - capacity/segment_size` 之前以腾出空间
  pub(super) async fn handle_capacity(&self, segment: u32) -> Result<()> {
    // Windows：先重试此前延迟的段删除（读者句柄释放后通常即可成功）
    #[cfg(windows)]
    self.retry_pending_removes().await;
    // 单调推进 end_segment：按 IDevice.EndSegment 接口契约（"最后已写段号"）始终跟踪；
    // C# 实现仅在设置 Capacity 时更新，属实现怪癖，此处依接口文档语义修正
    let seg = i32::try_from(segment).unwrap_or(i32::MAX);
    if self.end_segment.fetch_max(seg, Ordering::SeqCst) >= seg {
      return Ok(());
    }
    let (Some(cap), Some(seg_size)) = (self.capacity, self.segment_size) else {
      return Ok(());
    };
    // 全程 u64 饱和运算，杜绝 C# unchecked 截断在巨容量下的回绕（结果不超 segment，as 转换安全）
    let new_start = (segment as u64).saturating_sub(cap >> segment_shift(seg_size));
    if new_start > 0 {
      self.truncate_until_segment(new_start as u32).await?;
    }
    Ok(())
  }

  /// 重试延迟删除队列（Windows 专属）：删除成功或文件已消失则移出队列
  ///
  /// Windows 语义与 C# 的差异：C# LocalStorageDevice 删除失败即抛异常上抛调用方；
  /// 本实现 Windows 下读者持句柄的删除失败（sharing violation）不阻塞容量逐出，
  /// 记入 [`SegmentedDevice::pending_removes`] 延迟重试。队列跨进程重启丢失属
  /// 可接受边界：重启后 `recover` 的段号空隙扫描重建 start_segment，残留段文件
  /// 不参与有效日志语义（上层恢复以记录链 CRC 自定位）
  #[cfg(windows)]
  async fn retry_pending_removes(&self) {
    let ids: Vec<u32> = {
      let pin = self.pending_removes.pin();
      if pin.is_empty() {
        return;
      }
      pin.iter().copied().collect()
    };
    let mut done = Vec::with_capacity(ids.len());
    for id in ids {
      match remove_file(self.segment_path(id)).await {
        Ok(()) => done.push(id),
        Err(e) if e.kind() == ErrorKind::NotFound => done.push(id),
        // 仍被读者句柄占用，留在队列下次重试
        Err(_) => {}
      }
    }
    if !done.is_empty() {
      let pin = self.pending_removes.pin();
      for id in done {
        pin.remove(&id);
      }
    }
  }

  /// 截断清理内核：单调推进 `start_segment`、驱逐本地句柄并物理删除其前段文件
  ///
  /// 由 `Device::truncate_until_segment` 门面转发，无第二实现点。
  pub(super) async fn truncate_until_segment_impl(&self, segment_id: u32) -> Result<()> {
    if self.segment_size.is_none() {
      return Ok(());
    }

    // Windows：先重试此前延迟的段删除，避免队列滞留
    #[cfg(windows)]
    self.retry_pending_removes().await;

    // 0. 单调更新起始段编号（对齐 libs/client/Utility.cs:MonotonicUpdate）：
    //    未推进则视为无操作快速返回，跳过句柄清理与目录扫描
    let old_start = self.start_segment.fetch_max(segment_id, Ordering::SeqCst);
    if old_start >= segment_id {
      return Ok(());
    }

    // 0.5 守护位图同步免责（仅 debug）：被截断区间 [old_start, segment_id) 的在册
    //     脏位即时清除，维护"脏位仅存在于 start_segment 之后"的守护不变量——
    //     被截断段已物理删除无须 fsync 背书，后续 sync 不再为其消耗免责逻辑
    #[cfg(debug_assertions)]
    {
      let clear_end = segment_id.min(SYNC_GUARD_SEGMENTS as u32);
      for seg in old_start..clear_end {
        self.debug_clear_segment(seg);
      }
    }

    // 1. 从当前 CPU 核心本地缓存中移除并关闭已打开的文件句柄
    LOCAL_FILES.with(|m| {
      m.borrow_mut()
        .retain(|&(dev_id, sid), _| dev_id != self.device_id || sid >= segment_id);
    });

    // 2. 从磁盘物理删除小于 segment_id 的段文件
    if let Some(entries) = self.segment_entries()? {
      for item in entries {
        let (id, entry) = item?;
        if id >= segment_id {
          continue;
        }
        match remove_file(entry.path()).await {
          Ok(()) => {}
          Err(e) if e.kind() == ErrorKind::NotFound => {}
          // Windows：读者持句柄导致删除失败（sharing violation 等占用类错误），
          // 记入延迟队列下次 handle_capacity / truncate 重试，不阻塞逐出路径——
          // 段删除失败若直接上抛，write_aligned 会连带报错，容量逐出反而失效
          #[cfg(windows)]
          Err(e) => {
            self.pending_removes.pin().insert(id);
            log::warn!("段 {id} 删除失败（{e}），已记入延迟删除队列");
          }
          // Unix：删除失败为真实 I/O 故障，快速失败上抛（对齐 C# RemoveSegment）
          #[cfg(not(windows))]
          Err(e) => return Err(Error::Io(e)),
        }
      }
    }

    Ok(())
  }
}
