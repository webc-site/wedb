//! 段物理生命周期：尺寸查询、显式删段、有界容量逐出与批量截断
//!
//! 对位 C# 设备基类的容量逐出与本地设备的段删除面：`truncate_until_segment` 为
//! 常规回收路径（推进 `start_segment` + 广播句柄失效 + 物理删段），`remove_segment`
//! 为显式单段删除；Windows 下读者持句柄导致的删除失败走延迟队列重试。
//!
//! 句柄失效的跨线程口径见 [`handle`] 模块注记：本域的截断/删段即 C# 进程级共享表
//! `TryRemove` + `Dispose` 的对位广播点。

use std::{fs::metadata, io::ErrorKind, sync::atomic::Ordering};

use compio::fs::{OpenOptions, remove_file};

use super::SegmentedDevice;
#[cfg(debug_assertions)]
use super::sync::SYNC_GUARD_SEGMENTS;
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
    if segment_id < self.start_segment.load(Ordering::SeqCst) {
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
  /// 失效口径对位 C# `LocalStorageDevice.cs:354-358`（进程级共享表 `TryRemove` 后
  /// 立即 `Dispose`，全线程即时生效）：本方法先广播整表失效世代再就地对账，
  /// 故**全部线程**的本设备陈旧句柄均被弃用（他线程在下次句柄访问时关闭 fd）。
  /// 与 C# 的残余差异仅在于广播的生效时点是"下次访问"而非"当场"——`compio::fs::File`
  /// 实测 `!Send`，他线程无法代为释放其独占句柄；已在途 I/O 由 compio `SharedFd`
  /// 计数延命，不会读到已关闭的 fd。本方法仍不应常规调用，常规回收走
  /// [`Device::truncate_until_segment`]
  pub async fn remove_segment(&self, segment_id: u32) -> Result<()> {
    // 先广播后 unlink：使"unlink 成功而句柄仍在册"的窗口不因本线程重开而复活
    self.broadcast_invalid();
    self.reconcile(self.stamp());
    #[cfg(debug_assertions)]
    self.debug_clear_segment(segment_id);
    let path = self.segment_path(segment_id);
    match remove_file(&path).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
      Err(e) => Err(Error::Io(e)),
    }
  }

  /// 提交上界之后的物理擦尾（对标 C# AllocatorBase.cs 的 RecoveryReset 位点收敛至
  /// 提交点后 `ClearPage(tailPage, offsetInPage)` 清未提交尾的**盘面对位**）：
  /// `from_address` 所在段文件截断收缩至段内该偏移，其后孤儿段整段物理删除并把
  /// `end_segment` 收敛回提交段——撕裂坏写与未提交孤儿记录从盘面上彻底消失，
  /// 二次恢复扫描不再重复观测到同一残尾（恢复幂等）。
  ///
  /// 恢复静默期专用（唯一调用方 `waof::WalLog::recover` 持提交锁且契约要求无
  /// 并发写入），不做在途 I/O 防护；经独立 fd `set_len` 收缩与同 inode 在册
  /// 句柄共读共写不冲突，无须句柄失效广播（区别于 unlink 路径的
  /// [`Self::remove_segment`]）
  pub async fn erase_tail_after(&self, from_address: u64) -> Result<()> {
    let (seg_id, off_in_seg) = self.get_segment_and_offset(from_address)?;
    // 1. 提交段内残尾收缩：仅当段文件物理超出提交偏移（存在未提交孤儿或撕裂
    //    坏写）才截断；等齐 EOF 为常态 no-op
    let file_size = self.get_file_size(seg_id)?;
    if off_in_seg < file_size {
      let path = self.segment_path(seg_id);
      match OpenOptions::new().write(true).open(&path).await {
        Ok(file) => {
          file.set_len(off_in_seg).await.map_err(Error::Io)?;
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(Error::Io(e)),
      }
    }
    // 2. 提交段之后的孤儿段物理删除：恢复扫描按记录链覆盖全部可读段，落在
    //    提交段之后的段必在末 commit 帧之后——按定义不可提交，整段回收。
    //    由高到低降序删除，消除中间崩溃产生中部空洞的可能
    if let Some(end) = self.end_segment() {
      for seg in ((seg_id + 1)..=end).rev() {
        self.remove_segment(seg).await?;
      }
    }
    // 3. end_segment 收敛回提交段（fetch_min 不回退更低的既有水位；后续写入
    //    新段经 handle_capacity 再单调推高）
    self.end_segment.fetch_min(seg_id as i32, Ordering::SeqCst);
    Ok(())
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
    let Some(cap) = self.capacity else {
      return Ok(());
    };
    let seg_size = self.segment_size;
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

  /// 截断清理内核：单调推进 `start_segment`、广播句柄失效并物理删除其前段文件
  ///
  /// 由 `Device::truncate_until_segment` 门面转发，无第二实现点。
  ///
  /// 逻辑栅栏与物理水位解耦（C# 各本地设备 `RemoveSegment` 为尽力删除、天然不
  /// 上抛；Rust 依工业契约失败即透明上抛，故幂等重试必须可补完）：入口仍前置
  /// 推进 `start_segment` 即刻拦截逻辑读写并驱逐陈旧句柄，但快速短路判定改由
  /// `purged_segment` 承接——仅在删段遍历全量成功后推进，中途 I/O 故障时停留
  /// 旧值，相同段号的重试不会被短路、可再次进入扫描清除残留段。
  ///
  /// 失效广播对位 C# `LocalStorageDevice` 的进程级共享表移除（全线程即时生效）：
  /// `start_segment` 即天然代际，推进后**全部线程**的本设备句柄表在下次访问时
  /// 由 [`SegmentedDevice::reconcile`] 精确驱逐 `sid < segment_id` 的陈旧项——
  /// `Rc<File>` 归零当场关闭 fd，被删段的磁盘空间随之释放，不再依赖该线程自身
  /// 再跑一次截断；存活段句柄零误伤、零重开。
  pub(super) async fn truncate_until_segment_impl(&self, segment_id: u32) -> Result<()> {
    // Windows：先重试此前延迟的段删除，避免队列滞留
    #[cfg(windows)]
    self.retry_pending_removes().await;

    // 0. 物理清理水位快速短路：purged_segment 之上无残留段待删才跳过扫描；
    //    此前截断若中途失败，水位未推进，重试即使 start_segment 已到位也必须
    //    继续向下补删，杜绝孤儿段文件永久泄漏
    if self.purged_segment.load(Ordering::SeqCst) >= segment_id {
      return Ok(());
    }

    // 0.5 单调更新起始段编号（对齐 libs/client/Utility.cs:MonotonicUpdate）：
    //     未推进（失败重试或回退截断补扫）时也须继续执行下方扫描与删除
    let old_start = self.start_segment.fetch_max(segment_id, Ordering::SeqCst);
    // old_start 仅 debug 守护位图分支消费，release 显式弃置消未用告警
    let _ = old_start;

    // 0.8 守护位图同步免责（仅 debug）：被截断区间 [old_start, segment_id) 的在册
    //     脏位即时清除，维护"脏位仅存在于 start_segment 之后"的守护不变量——
    //     被截断段已物理删除无须 fsync 背书，后续 sync 不再为其消耗免责逻辑
    #[cfg(debug_assertions)]
    {
      let clear_end = segment_id.min(SYNC_GUARD_SEGMENTS as u32);
      for seg in old_start..clear_end {
        self.debug_clear_segment(seg);
      }
    }

    // 1. 本线程句柄表按新失效戳对账，关闭并遗忘已截断段的句柄；他线程的表由各自
    //    下次访问对账（`start_segment` 已推进，广播无须额外原子写）
    self.reconcile(self.stamp());

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

    // 3. 扫描与删除全量无错完成（Windows 下全部成功入延迟队列亦视为完成，
    //    由 retry_pending_removes 承接补删）后才推进物理清理水位
    self.purged_segment.fetch_max(segment_id, Ordering::SeqCst);

    Ok(())
  }
}
