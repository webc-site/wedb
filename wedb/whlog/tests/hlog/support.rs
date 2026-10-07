use std::{
  future::Future,
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
  },
};

use event_listener::Event;
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Error, Result as DeviceResult, SegmentedDevice};

/// 故障注入设备包装（仅测试用）：按注入模式劫持 write_aligned
///
/// - `MODE_NORMAL`: 正常透传底层设备；
/// - `MODE_FAIL`: 返回 I/O 错误（模拟写入失败）；
/// - `MODE_SHORT`: 返回「短写成功」但实际不落盘（模拟设备跨段写入慢路径的部分成功）；
/// - 另有独立于模式的一次性写前挂起握手（`arm_stall` / `is_stalled` /
///   `release_stall`）：布防后的首个 write_aligned 在内层写之前 await 放行事件，
///   把「拷贝已成形、设备写在途」的窗口钉死为确定性交错（复用 event_listener，
///   与错误模式正交可叠加）。
pub(crate) struct FaultDevice {
  inner: SegmentedDevice,
  mode: AtomicU8,
  /// 一次性挂起布防（写序闸测试握手）
  stall_armed: AtomicBool,
  /// 已扣停在写前窗口的标志（测试据此确认挂起窗口就位）
  stall_parked: AtomicBool,
  /// 放行通道（listener 先于 parked 置位注册，notify 必达）
  release: event_listener::Event,
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
      stall_armed: AtomicBool::new(false),
      stall_parked: AtomicBool::new(false),
      release: Event::new(),
    }
  }

  /// 切换故障注入模式（后续 write_aligned 生效）
  #[inline]
  pub(crate) fn set_mode(&self, mode: u8) {
    self.mode.store(mode, Ordering::Relaxed);
  }

  /// 布防一次性写前挂起：下一次 write_aligned 在内层写之前扣停至放行
  #[inline]
  pub(crate) fn arm_stall(&self) {
    self.stall_armed.store(true, Ordering::Release);
  }

  /// 是否已有写扣停在写前窗口（挂起窗口就位）
  #[inline]
  pub(crate) fn is_stalled(&self) -> bool {
    self.stall_parked.load(Ordering::Acquire)
  }

  /// 放行扣停中的写（无扣停者时无害空操作）
  #[inline]
  pub(crate) fn release_stall(&self) {
    self.release.notify(1);
  }
}

impl Device for FaultDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> u64 {
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
    // 一次性写前挂起（握手语义见结构体文档）：listener 先注册再宣告扣停，
    // 测试侧 is_stalled() 观察到 true 时 notify 必达，无丢失窗口
    if self.stall_armed.swap(false, Ordering::AcqRel) {
      let listener = self.release.listen();
      self.stall_parked.store(true, Ordering::Release);
      listener.await;
      self.stall_parked.store(false, Ordering::Release);
    }
    match self.mode.load(Ordering::Relaxed) {
      MODE_FAIL => (
        Err(Error::ReadOnly {
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

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> DeviceResult<u64> {
    self.inner.get_file_size(segment_id)
  }

  fn remove_segment(&self, segment_id: u32) -> impl Future<Output = DeviceResult<()>> {
    self.inner.remove_segment(segment_id)
  }

  fn erase_tail_after(&self, from_address: u64) -> impl Future<Output = DeviceResult<()>> {
    self.inner.erase_tail_after(from_address)
  }

  #[inline]
  fn recover(&self) -> DeviceResult<()> {
    self.inner.recover()
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
      return Some(Error::Io(io::Error::other(
        "Simulated synchronous device read failure",
      )));
    }
    let ordinal = self.throw_on_read_ordinal.load(Ordering::Relaxed);
    if ordinal >= 0 {
      let n = self.read_ordinal.fetch_add(1, Ordering::Relaxed);
      if n == ordinal {
        self.read_failure_injected.store(true, Ordering::Relaxed);
        return Some(Error::Io(io::Error::other(format!(
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
  fn segment_size(&self) -> u64 {
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

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> DeviceResult<u64> {
    self.inner.get_file_size(segment_id)
  }

  fn remove_segment(&self, segment_id: u32) -> impl Future<Output = DeviceResult<()>> {
    self.inner.remove_segment(segment_id)
  }

  fn erase_tail_after(&self, from_address: u64) -> impl Future<Output = DeviceResult<()>> {
    self.inner.erase_tail_after(from_address)
  }

  #[inline]
  fn recover(&self) -> DeviceResult<()> {
    self.inner.recover()
  }
}

/// 扇区尺寸可切换设备包装（仅测试用）：运行期改报扇区几何
///
/// 模拟日志文件被搬到不同扇区尺寸设备上再恢复的场景（对标
/// libs/storage/Tsavorite/cs/test/test.recovery/RecoveryTests.cs::RecoveryTestFailOnSectorSize 的 smallSector
/// 设备几何切换）：数据面全部透传，仅 `sector_size()` 按注入值回报
pub(crate) struct SectorShiftDevice {
  inner: SegmentedDevice,
  sector_size: AtomicUsize,
}

impl SectorShiftDevice {
  /// 以底层设备构造，初始回报底层扇区尺寸
  #[inline]
  pub(crate) fn new(inner: SegmentedDevice) -> Self {
    Self {
      sector_size: AtomicUsize::new(inner.sector_size()),
      inner,
    }
  }

  /// 切换回报的扇区尺寸（须为 2 的幂）
  #[inline]
  pub(crate) fn set_sector_size(&self, sector_size: usize) {
    self.sector_size.store(sector_size, Ordering::Relaxed);
  }
}

impl Device for SectorShiftDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.sector_size.load(Ordering::Relaxed)
  }

  #[inline]
  fn segment_size(&self) -> u64 {
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

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> DeviceResult<u64> {
    self.inner.get_file_size(segment_id)
  }

  fn remove_segment(&self, segment_id: u32) -> impl Future<Output = DeviceResult<()>> {
    self.inner.remove_segment(segment_id)
  }

  fn erase_tail_after(&self, from_address: u64) -> impl Future<Output = DeviceResult<()>> {
    self.inner.erase_tail_after(from_address)
  }

  #[inline]
  fn recover(&self) -> DeviceResult<()> {
    self.inner.recover()
  }
}
