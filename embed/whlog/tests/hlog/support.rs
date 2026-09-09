use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering},
  },
};

use wdev::{BufferPool, Device, Result as DeviceResult, SegmentedDevice};
use wutil::AlignedBuf;

/// 统计底层读 I/O 次数与读请求字节数的委托设备包装（仅测试用：以现有公开 Device trait 断言缓存命中）
///
/// `reads` 计设备读调用次数；`read_bytes` 累加每次读请求的缓冲容量（扇区圆整后的
/// 请求长度），用于区分 4KB 级 probe 小读与整页（page_size）大读。
pub(crate) struct CountingDevice {
  inner: SegmentedDevice,
  reads: AtomicUsize,
  read_bytes: AtomicU64,
}

impl CountingDevice {
  /// 以底层设备构造，计数清零
  #[inline]
  pub(crate) fn new(inner: SegmentedDevice) -> Self {
    Self {
      inner,
      reads: AtomicUsize::new(0),
      read_bytes: AtomicU64::new(0),
    }
  }

  /// 已发生的设备读调用次数
  #[inline]
  pub(crate) fn reads(&self) -> usize {
    self.reads.load(Ordering::Relaxed)
  }

  /// 累计读请求缓冲容量（扇区圆整后字节数）
  #[inline]
  pub(crate) fn read_bytes(&self) -> u64 {
    self.read_bytes.load(Ordering::Relaxed)
  }
}

impl Device for CountingDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> Option<u64> {
    self.inner.segment_size()
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.reads.fetch_add(1, Ordering::Relaxed);
    self
      .read_bytes
      .fetch_add(buf.capacity() as u64, Ordering::Relaxed);
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.reads.fetch_add(1, Ordering::Relaxed);
    self
      .read_bytes
      .fetch_add(buf.capacity() as u64, Ordering::Relaxed);
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> DeviceResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DeviceResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 故障注入设备包装（仅测试用）：按注入模式劫持 write_aligned
///
/// - `MODE_NORMAL`: 正常透传底层设备；
/// - `MODE_FAIL`: 返回 I/O 错误（模拟写入失败）；
/// - `MODE_SHORT`: 返回「短写成功」但实际不落盘（模拟设备跨段写入慢路径的部分成功）。
pub(crate) struct FaultDevice {
  inner: SegmentedDevice,
  mode: AtomicU8,
}

pub(crate) const MODE_NORMAL: u8 = 0;
pub(crate) const MODE_FAIL: u8 = 1;
pub(crate) const MODE_SHORT: u8 = 2;

impl FaultDevice {
  /// 以底层设备构造，初始为正常转发模式
  #[inline]
  pub(crate) fn new(inner: SegmentedDevice) -> Self {
    Self {
      inner,
      mode: AtomicU8::new(MODE_NORMAL),
    }
  }

  /// 切换故障注入模式（后续 write_aligned 生效）
  #[inline]
  pub(crate) fn set_mode(&self, mode: u8) {
    self.mode.store(mode, Ordering::Relaxed);
  }
}

impl Device for FaultDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> Option<u64> {
    self.inner.segment_size()
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    match self.mode.load(Ordering::Relaxed) {
      MODE_FAIL => (
        Err(wdev::Error::ReadOnly {
          offset,
          len: buf.len(),
        }),
        buf,
      ),
      MODE_SHORT => (Ok(buf.len() / 2), buf),
      _ => self.inner.write_aligned(offset, buf).await,
    }
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> DeviceResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DeviceResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 同步故障注入设备包装（仅测试用）：读在"设备下发前"即失败
///
/// 对标 libs/storage/Tsavorite/cs/test/SimulatedFlakyDevice.cs 的
/// SyncThrowOnReadDevice（上游 d20d63993 新增测试支撑）：C# 在 ReadAsync 内同步抛
/// IOException 且不投递完成回调，模拟请求在设备下发前即失败；Rust 错误即返回值，
/// 等价实现为 read_aligned / read_raw 直接返回 Err。C# 同文件的
/// SyncThrowOnWriteDevice 服务于 ShardedStorageDevice 扇出清理的测试，Rust 无容量
/// 分片设备对应物（SegmentedDevice 逐段顺序下发、错误即返回），不移植写失败臂
/// （写故障注入已有 [`FaultDevice`] 覆盖刷盘路径）。
///
/// - `arm_read_failure`：置位后所有读失败（对标 SyncThrowOnReadDevice.ArmReadFailure）；
/// - `throw_on_read_ordinal`：仅第 N 次（0 基）读失败、其余正常下发，用于命中
///   特定页读（对标 ThrowOnReadOrdinal，负数禁用）；
/// - `read_failure_injected`：序数命中后置位，供测试断言注入确实生效
///   （对标 ReadFailureInjected）。
pub(crate) struct SyncThrowDevice {
  inner: SegmentedDevice,
  arm_read_failure: AtomicBool,
  throw_on_read_ordinal: AtomicI64,
  read_ordinal: AtomicI64,
  read_failure_injected: AtomicBool,
}

impl SyncThrowDevice {
  /// 以底层设备构造，所有注入初始禁用
  #[inline]
  pub(crate) fn new(inner: SegmentedDevice) -> Self {
    Self {
      inner,
      arm_read_failure: AtomicBool::new(false),
      throw_on_read_ordinal: AtomicI64::new(-1),
      read_ordinal: AtomicI64::new(0),
      read_failure_injected: AtomicBool::new(false),
    }
  }

  /// 切换全量读失败注入（对标 ArmReadFailure）
  #[inline]
  pub(crate) fn set_arm_read_failure(&self, armed: bool) {
    self.arm_read_failure.store(armed, Ordering::Relaxed);
  }

  /// 指定第 N 次（0 基）读失败，负数禁用（对标 ThrowOnReadOrdinal）
  #[inline]
  pub(crate) fn set_throw_on_read_ordinal(&self, ordinal: i64) {
    self.throw_on_read_ordinal.store(ordinal, Ordering::Relaxed);
  }

  /// 序数注入是否已命中（对标 ReadFailureInjected）
  #[inline]
  pub(crate) fn read_failure_injected(&self) -> bool {
    self.read_failure_injected.load(Ordering::Relaxed)
  }

  /// 读失败闸门：命中注入时返回模拟的同步读失败错误
  fn read_gate(&self) -> Option<wdev::Error> {
    if self.arm_read_failure.load(Ordering::Relaxed) {
      return Some(wdev::Error::Io(io::Error::other(
        "Simulated synchronous device read failure",
      )));
    }
    let ordinal = self.throw_on_read_ordinal.load(Ordering::Relaxed);
    if ordinal >= 0 {
      let n = self.read_ordinal.fetch_add(1, Ordering::Relaxed);
      if n == ordinal {
        self.read_failure_injected.store(true, Ordering::Relaxed);
        return Some(wdev::Error::Io(io::Error::other(format!(
          "Simulated synchronous device read failure on read ordinal {ordinal}"
        ))));
      }
    }
    None
  }
}

impl Device for SyncThrowDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> Option<u64> {
    self.inner.segment_size()
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    match self.read_gate() {
      Some(e) => (Err(e), buf),
      None => self.inner.read_aligned(offset, buf).await,
    }
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    match self.read_gate() {
      Some(e) => (Err(e), buf),
      None => self.inner.read_raw(offset, buf).await,
    }
  }

  async fn sync(&self) -> DeviceResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DeviceResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}
