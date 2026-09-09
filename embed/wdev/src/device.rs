use std::{future::Future, sync::Arc};

/// 块存储设备 trait 别名（对标 C# IDevice / StorageDeviceBase）
pub use Device as StorageDevice;
use wram::{AlignedBuf, BufferPool, SectorRange};

use crate::{
  chunk::segment_shift,
  error::{Error, Result},
};

/// 底层块存储设备抽象
///
/// `'static` 超界：设备实例一律以具体类型经 `Arc` 跨任务共享（后台 GC 循环等
/// compio `spawn` 要求 future `'static`），仓库内全部实现均为具名具体类型，
/// 恒满足该约束，不构成实际侵入。
pub trait Device: Send + Sync + 'static {
  /// 扇区物理大小（字节，默认 4096，至少 512，须为 2 的幂）
  fn sector_size(&self) -> usize;

  /// 段大小（字节，None 表示单一无界文件）
  fn segment_size(&self) -> Option<u64>;

  /// 是否启用 Direct I/O（对齐 C# 设备族默认策略：
  /// Linux 原生设备 O_DIRECT，其余平台 Managed 设备缓冲 I/O）
  #[inline]
  fn direct_io(&self) -> bool {
    cfg!(target_os = "linux")
  }

  /// 起始有效段编号（小于此编号的段已被截断，对应 C# IDevice.StartSegment）
  #[inline]
  fn start_segment(&self) -> u32 {
    0
  }

  /// 设备容量上限（字节；None 对应 C# Devices.CAPACITY_UNSPECIFIED）
  #[inline]
  fn capacity(&self) -> Option<u64> {
    None
  }

  /// 已写入的最高段编号（None 表示尚未写入任何段；对应 C# IDevice.EndSegment，初始 -1）
  #[inline]
  fn end_segment(&self) -> Option<u32> {
    None
  }

  /// 关联的扇区对齐缓冲池 (对应 C# IDevice/RandomAccessLocalStorageDevice.pool)
  fn pool(&self) -> &Arc<BufferPool>;

  /// 写入扇区对齐的内存块，要求 offset 是 sector_size 的整数倍
  fn write_aligned(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)>;

  /// 读取扇区对齐的内存块，要求 offset 是 sector_size 的整数倍
  fn read_aligned(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)>;

  /// 读取任意逻辑范围的内存块（缓冲 I/O 模式专用，无对齐约束）
  ///
  /// 仅当 `direct_io()` 为 false 时被 `read_range` 路由调用；
  /// 缓冲 I/O 无对齐要求，按逻辑范围精确直读，规避对齐圆整的读放大与子视图拷贝。
  fn read_raw(
    &self,
    offset: u64,
    buf: AlignedBuf,
  ) -> impl Future<Output = (Result<usize>, AlignedBuf)>;

  /// 便捷读取任意逻辑范围：
  /// 默认基于设备缓冲池 (`self.pool()`) 获取缓冲区并在 drop 时自动回池复用，
  /// 免除热路径每次 I/O 的物理分配与整块清零开销 (对标 C# `clearOnReturn: false` 读目的地优化)。
  fn read_range(&self, offset: u64, len: usize) -> impl Future<Output = Result<AlignedBuf>> {
    self.read_range_pooled(offset, len, self.pool())
  }

  /// 池化便捷读取：缓冲区自共享 `pool` 获取并在 drop 时归还复用，
  /// 免除热路径每次 I/O 的分配与整块清零 (读目的地整体覆写，对应 C#
  /// `clearOnReturn:false`；class 过量分配由 `required_len` 精确读取规避读放大)。
  ///
  /// 对齐命中 (`internal_offset == 0` 且请求长度恰为扇区整数倍) 时零拷贝返回；
  /// 否则才在池内做一次子视图拷贝。
  fn read_range_pooled(
    &self,
    offset: u64,
    len: usize,
    pool: &Arc<BufferPool>,
  ) -> impl Future<Output = Result<AlignedBuf>> {
    async move {
      if len == 0 {
        return AlignedBuf::new(0, self.sector_size()).map_err(Error::from);
      }
      if !self.direct_io() {
        // 缓冲 I/O 模式：免对齐读放大与子视图拷贝，按逻辑范围精确直读
        let buf = pool.get_with_policy(len, false)?;
        let (res, mut buf) = self.read_raw(offset, buf).await;
        let bytes_read = res?;
        if bytes_read < len {
          return Err(Error::UnexpectedEof {
            expected: len,
            actual: bytes_read,
          });
        }
        buf.set_len(len)?;
        return Ok(buf);
      }
      let sector_size = self.sector_size();
      let range = SectorRange::calculate(offset, len, sector_size)?;
      let buf = pool.get_with_policy(range.aligned_len, false)?;
      let (res, mut buf) = self.read_aligned(range.aligned_offset, buf).await;
      let bytes_read = res?;
      let sub = range.sub_range(len);
      if bytes_read < sub.end {
        // 统一为逻辑口径：expected 为请求长度，actual 为对齐读取后可得的逻辑字节数
        return Err(Error::UnexpectedEof {
          expected: len,
          actual: bytes_read.saturating_sub(range.internal_offset),
        });
      }
      if range.internal_offset == 0 && range.aligned_len == len {
        buf.set_len(len)?;
        return Ok(buf);
      }
      // 非对齐子视图：从切片获取池化缓冲区
      pool.get_from_slice(&buf[sub]).map_err(Error::from)
    }
  }

  /// 全局刷盘同步：遍历设备上全部在表段句柄（按段号去重）逐 inode fsync/fdatasync
  ///
  /// 语义对齐 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:LocalStorageDevice（句柄表进程级共享、任意线程可 sync）：任意线程
  /// 调用即覆盖所有线程在 sync 发起前完成的写入，release 构建无"他线程写入漏刷"的
  /// 静默丢失窗口。跨线程 fsync 可行性：fd 属进程级 files_struct，fsync 按 inode
  /// 全量生效、无线程亲和（`compio-driver/sync` feature 保证句柄本身可跨线程共享）。
  /// 排序契约与 POSIX fsync 同口径：仅覆盖 sync 发起前已完成的写入。
  ///
  /// 并发边界：`reset` 清空句柄表但不删文件，与其并发时本方法收集不到句柄、
  /// 返回空成功——reset 与写入/sync 的交错须由上层协议串行化（对齐 C# `IDevice.Reset`
  /// 关闭遗忘句柄的同源语义）。平台注记：macOS poll 后端走 `fsync(2)` 而非
  /// `F_FULLFSYNC`，断电持久性强于页缓存可见性弱于 Linux io_uring/O_DIRECT 路径；
  /// 生产目标以 Linux 为准。
  fn sync(&self) -> impl Future<Output = Result<()>>;

  /// 全局刷盘仅同步文件数据（fdatasync），尽量避免同步 inode 元数据（时间戳等），
  /// 降低磁盘元数据写入开销（对标 C# 日志/WAL 快速刷盘）；覆盖范围与 [`Device::sync`] 相同
  fn sync_data(&self) -> impl Future<Output = Result<()>> {
    self.sync()
  }

  /// 获取指定段的文件大小（字节，若段文件不存在或已截断返回 0，对标 libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs:GetFileSize）
  #[inline]
  fn get_file_size(&self, _segment_id: u32) -> Result<u64> {
    Ok(0)
  }

  /// 物理删除单个段文件并从句柄缓存中移除（对标 libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs:RemoveSegment）
  fn remove_segment(&self, _segment_id: u32) -> impl Future<Output = Result<()>> {
    async move { Ok(()) }
  }

  /// 重置设备句柄缓存（关闭并遗忘所有当前打开的文件句柄，对标 libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs:Reset）
  #[inline]
  fn reset(&self) {}

  /// 截断清理指定段编号之前的段（例如删除编号小于 `segment_id` 的所有段文件）
  fn truncate_until_segment(&self, segment_id: u32) -> impl Future<Output = Result<()>>;

  /// 根据逻辑地址截断历史段文件（截断至该地址所在的段边界之前，删除该段之前的所有段文件）
  /// 对照 libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs:TruncateUntilAddress 语义
  fn truncate_until_address(&self, to_address: u64) -> impl Future<Output = Result<()>> {
    async move {
      if let Some(seg_size) = self.segment_size() {
        let to_seg_u64 = if seg_size.is_power_of_two() {
          to_address >> segment_shift(seg_size)
        } else {
          to_address / seg_size
        };
        let to_seg = u32::try_from(to_seg_u64).unwrap_or(u32::MAX);
        self.truncate_until_segment(to_seg).await
      } else {
        Ok(())
      }
    }
  }

  /// 恢复设备元数据（如段文件扫描、start_segment / end_segment 重建；单文件模式默认无操作）
  #[inline]
  fn recover(&self) -> Result<()> {
    Ok(())
  }
}
