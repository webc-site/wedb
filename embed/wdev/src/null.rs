//! 空设备实现（对标 C# `Tsavorite/core/Device/NullDevice.cs`）
//!
//! 全部 I/O 即时假成功、零物理 I/O 与持久化，用于免落盘场景
//! （对标 Garnet `UseAofNullDevice` 与 `SubscribeBroker` 的内存日志 `new NullDevice()`）。

use std::sync::{
  Arc,
  atomic::{AtomicU32, Ordering::SeqCst},
};

use wram::{AlignedBuf, BufferPool, DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, is_valid_sector_size};

use crate::{
  device::Device,
  error::{Error, Result},
};

/// 空设备：写入即时确认、读取返回零填充，无任何物理 I/O
///
/// 与 C# 的刻意差异：C# 读取假成功时原样返回缓冲区脏数据；此处把请求范围清零后返回，
/// 避免池化缓冲残留脏数据被调用方当作"成功读取"的有效数据。
pub struct NullDevice {
  sector_size: usize,
  /// 起始有效段编号（截断单调推进，对标 C# StorageDeviceBase.startSegment）
  start_segment: AtomicU32,
  pool: Arc<BufferPool>,
}

impl NullDevice {
  /// 创建默认 4096 扇区大小的空设备
  pub fn new() -> Result<Self> {
    Self::with_sector_size(DEFAULT_SECTOR_SIZE)
  }

  /// 以指定扇区大小创建空设备（须为 2 的幂且不小于 512）
  pub fn with_sector_size(sector_size: usize) -> Result<Self> {
    if !is_valid_sector_size(sector_size) {
      return Err(Error::InvalidSectorSize {
        size: sector_size,
        min: MIN_SECTOR_SIZE,
      });
    }
    Ok(Self {
      sector_size,
      start_segment: AtomicU32::new(0),
      pool: BufferPool::new(sector_size)?,
    })
  }
}

impl Device for NullDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.sector_size
  }

  #[inline]
  fn segment_size(&self) -> Option<u64> {
    None
  }

  /// 无物理 I/O，Direct 与否不影响语义，恒 false 使便捷读取走零拷贝精确直读路由
  #[inline]
  fn direct_io(&self) -> bool {
    false
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    &self.pool
  }

  async fn write_aligned(&self, _offset: u64, buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    // 即时假成功确认全部逻辑长度（对标 C# callback(0, numBytesToWrite)）
    (Ok(buf.len()), buf)
  }

  async fn read_aligned(&self, _offset: u64, mut buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    // 读取请求口径与 `SegmentedDevice::read_impl` 一致：按有效需求长度封顶容量
    // （池化缓冲初始 len 为 0，请求长度由 required_len 携带）
    let n = buf.required_len().min(buf.capacity());
    // 与 C# 的刻意差异：C# 假成功时原样返回缓冲区脏数据；此处把请求范围清零，
    // 避免池化缓冲残留脏数据被调用方当作"成功读取"的有效数据
    buf.as_allocated_slice_mut()[..n].fill(0);
    // SAFETY: n <= buf.capacity() 恒成立
    unsafe { buf.set_len_unchecked(n) };
    (Ok(n), buf)
  }

  #[inline]
  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    self.read_aligned(offset, buf).await
  }

  async fn sync(&self) -> Result<()> {
    Ok(())
  }

  /// 单一无界假段（对标 C# 基类 segmentSize 无效时返回 long.MaxValue）
  #[inline]
  fn get_file_size(&self, _segment_id: u32) -> Result<u64> {
    Ok(u64::MAX)
  }

  /// 截断为单调推进起始段编号（对标 C# Utility.MonotonicUpdate，物理删除无操作）
  async fn truncate_until_segment(&self, segment_id: u32) -> Result<()> {
    self.start_segment.fetch_max(segment_id, SeqCst);
    Ok(())
  }

  #[inline]
  fn start_segment(&self) -> u32 {
    self.start_segment.load(SeqCst)
  }
}
