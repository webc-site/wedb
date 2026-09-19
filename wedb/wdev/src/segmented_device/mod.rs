//! 段文件设备（Direct I/O + Thread-Per-Core 句柄托管）
//!
//! 本件只持设备门面：[`SegmentedDevice`] 结构与字段不变量、构造参数族
//! [`DeviceParams`]（旋钮唯一注入口）、`Device` trait 转发面与 `Drop`；
//! 实现按职责分域于子模块——[`handle`] 路径与线程本地句柄表、
//! [`io`] 跨段寻址与对齐读写主体、[`sync`] 全局刷盘与 sync 契约守护、
//! [`truncate`] 物理截断删除与容量逐出、[`recover`] 目录恢复扫描与段名编解码。

mod handle;
mod io;
mod recover;
mod sync;
mod truncate;

use std::{
  fs::{create_dir_all, remove_file as sync_remove_file},
  path::PathBuf,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering},
  },
};

#[cfg(windows)]
use wbase::map::{ConcurrentSet, new_concurrent_set};
use wbase::{
  align::{DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, is_valid_sector_size},
  pool::{AlignedBuf, BufferPool},
};

use crate::{
  device::Device,
  error::{Error, Result},
  sys::MAX_SEGMENT_SIZE,
};

static NEXT_DEVICE_ID: AtomicU64 = AtomicU64::new(1);

/// 基于段文件管理与 Direct I/O 的异步块存储设备
///
/// 字段全部私有：原子字段（start/end segment、direct_io、守护位图）的
/// 不变量由设备自身维护，外部经 getter 与 `Device` trait 观测。
/// 设备本身仅包含元数据（Send + Sync），文件句柄由各 CPU 核心本地通过 Thread-Local 托管。
pub struct SegmentedDevice {
  /// 设备全局唯一实例编号（用于区分多设备在 Thread-Local 句柄表中的缓存槽）
  device_id: u64,
  /// 基础路径（单文件模式下的文件路径，或分段模式下的路径前缀）
  base_path: PathBuf,
  /// 单段文件大小（None 表示单文件无界模式）
  segment_size: Option<u64>,
  /// 物理扇区大小（字节数，至少 512，须为 2 的幂）
  sector_size: usize,
  /// 只读模式（对标 C# ManagedLocalStorageDevice 构造形参 readOnly；与 C# 同为
  /// private readonly 语义，仅经 [`DeviceParams`] 构造注入，运行期不可变）
  read_only: bool,
  /// 首次创建段文件时是否预分配段尺寸物理空间（对标 C# 构造形参 preallocateFile）
  preallocate: bool,
  /// 析构关闭时是否自动物理删除段文件（对标 C# 构造形参 deleteOnClose）
  delete_on_close: bool,
  /// 起始有效段编号（小于此编号的段已被截断，禁止访问；对齐 Garnet begin_segment_）
  start_segment: AtomicU32,
  /// 已写入的最高段编号（-1 表示尚未写入；对齐 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:endSegment）
  end_segment: AtomicI32,
  /// 是否启用 Direct I/O（对齐 C# 设备族默认策略：Linux 原生设备 O_DIRECT，
  /// 其余平台 Managed 设备缓冲 I/O）
  ///
  /// 探测定型协议（Linux）：首个段文件打开时以 O_DIRECT 真实探测一次文件系统/内核
  /// 支持性，不支持类错误一次性定型为 false 并驱逐既有 O_DIRECT 句柄，此后写入路径
  /// 不存在运行中翻转——定型后 Direct 打开失败直接上抛（对齐 C# NativeDevice：
  /// 打开失败即异常，无静默回退）
  direct_io: AtomicBool,
  /// O_DIRECT 支持性是否已探测定型（Linux 专属协议位；false 时下次 open_file 探测）
  #[cfg(target_os = "linux")]
  direct_io_probed: AtomicBool,
  /// Windows 容量逐出延迟删除队列（仅 Windows 参与编译）：Windows 下读者持句柄
  /// 会导致段文件删除失败（sharing violation），记入队列延迟重试而非让
  /// write_aligned 报错；Unix 语义为失败即上抛，不涉及本队列
  #[cfg(windows)]
  pending_removes: ConcurrentSet<u32>,
  /// 设备容量上限（字节；None 对应 C# Devices.CAPACITY_UNSPECIFIED，
  /// 仅经 [`DeviceParams`] 构造注入，对标 C# 构造形参 capacity）
  capacity: Option<u64>,
  /// 关联的扇区对齐缓冲池 (对应 C# RandomAccessLocalStorageDevice.pool)
  pool: Arc<BufferPool>,
  /// sync 持久化契约守护（仅 debug 构建参与编译，release 零成本）：
  /// 前 128 段的"已写入待 sync"全局位图。write 成功后置位、全局 sync 覆盖后清位，
  /// `sync_internal` 末尾校验全部在册写入均被本次 fsync 覆盖（段被截断推进
  /// `start_segment` 背书或 `remove_segment` 显式清除者免责），捕捉"写入段句柄被
  /// 异常路径移除导致脏页无人 fsync"的契约破坏
  #[cfg(debug_assertions)]
  dirty_segs: [AtomicU64; 2],
}

/// 设备构造期参数族（对标 C# 设备构造的可选形参，字段一经确定运行期不可变）
///
/// C# 侧该四参数的唯一形态即构造形参（`Devices.CreateLogDevice` 形参
/// preallocateFile/deleteOnClose/capacity/readOnly 分派入各设备构造函数，
/// 落地为 private readonly 字段），全仓无运行期 mutator；生产链路一律取缺省
/// （Garnet 侧 PreallocateLog 恒 false、设备容量恒 CAPACITY_UNSPECIFIED），
/// 非缺省值仅测试与基准显式传入。rust 同口径：本结构是唯一注入口。
#[derive(Clone, Copy, Debug, Default)]
pub struct DeviceParams {
  /// 首次新建段文件时是否把物理空间预分配到整段尺寸（C# preallocateFile）
  pub preallocate: bool,
  /// 设备析构时是否物理删除段文件（C# deleteOnClose）
  pub delete_on_close: bool,
  /// 只读模式：打开文件不带写权限，写入直接拒绝（C# readOnly）
  pub read_only: bool,
  /// 容量上限字节数，None 表示不设限（C# capacity = CAPACITY_UNSPECIFIED）
  pub capacity: Option<u64>,
}

impl SegmentedDevice {
  /// 创建新的块存储设备（缺省参数 + 自动初始化所属扇区的缓冲池）
  pub fn new(
    base_path: impl Into<PathBuf>,
    segment_size: Option<u64>,
    sector_size: usize,
  ) -> Result<Self> {
    Self::with_params(
      base_path,
      segment_size,
      sector_size,
      DeviceParams::default(),
    )
  }

  /// 以显式参数族创建块存储设备（自动初始化所属扇区的缓冲池）
  pub fn with_params(
    base_path: impl Into<PathBuf>,
    segment_size: Option<u64>,
    sector_size: usize,
    params: DeviceParams,
  ) -> Result<Self> {
    if !is_valid_sector_size(sector_size) {
      return Err(Error::InvalidSectorSize {
        size: sector_size,
        min: MIN_SECTOR_SIZE,
      });
    }
    let pool = BufferPool::new(sector_size)?;
    Self::with_pool(base_path, segment_size, sector_size, pool, params)
  }

  /// 以指定共享缓冲池与参数族创建块存储设备 (对应 C# 注入外部 bufferPool 语义)
  ///
  /// 全部设备旋钮（容量/预分配/只读/关闭即删）只经此口注入，无运行期 mutator。
  ///
  /// Rust 补齐防御：注入池的扇区必须与设备扇区一致——错配时池化缓冲区的
  /// 地址/长度对齐口径与设备 Direct I/O 要求错位，运行期才以 EINVAL 暴露；
  /// 构造期直接拒绝（C# RandomAccessLocalStorageDevice 无此校验，属其隐患）
  pub fn with_pool(
    base_path: impl Into<PathBuf>,
    segment_size: Option<u64>,
    sector_size: usize,
    pool: Arc<BufferPool>,
    params: DeviceParams,
  ) -> Result<Self> {
    if !is_valid_sector_size(sector_size) {
      return Err(Error::InvalidSectorSize {
        size: sector_size,
        min: MIN_SECTOR_SIZE,
      });
    }
    if pool.sector_size() != sector_size {
      return Err(Error::PoolSectorMismatch {
        pool: pool.sector_size(),
        device: sector_size,
      });
    }

    if let Some(seg_size) = segment_size
      && (seg_size == 0
        || !seg_size.is_power_of_two()
        || seg_size < sector_size as u64
        || seg_size > MAX_SEGMENT_SIZE)
    {
      return Err(Error::InvalidSegmentSize(seg_size));
    }

    // 容量口径校验（对标 C# 设备基准的 "Capacity must be a multiple of segment
    // size"：分段模式下非整段容量会让末段逐出算术错位，构造期即拒）
    if let (Some(cap), Some(seg_size)) = (params.capacity, segment_size)
      && (cap == 0 || cap % seg_size != 0)
    {
      return Err(Error::InvalidCapacity { capacity: cap });
    }

    let base_path = base_path.into();
    if let Some(parent) = base_path.parent()
      && !parent.as_os_str().is_empty()
    {
      create_dir_all(parent)?;
    }

    Ok(Self {
      base_path,
      segment_size,
      sector_size,
      read_only: params.read_only,
      preallocate: params.preallocate,
      delete_on_close: params.delete_on_close,
      device_id: NEXT_DEVICE_ID.fetch_add(1, Ordering::Relaxed),
      start_segment: AtomicU32::new(0),
      end_segment: AtomicI32::new(-1),
      direct_io: AtomicBool::new(cfg!(target_os = "linux")),
      #[cfg(target_os = "linux")]
      direct_io_probed: AtomicBool::new(false),
      #[cfg(windows)]
      pending_removes: new_concurrent_set(),
      #[cfg(debug_assertions)]
      dirty_segs: [const { AtomicU64::new(0) }; 2],
      capacity: params.capacity,
      pool,
    })
  }

  /// 物理扇区大小（对标 C# IDevice.SectorSize 属性）
  #[inline]
  pub fn sector_size(&self) -> usize {
    self.sector_size
  }

  /// 单段文件大小，None 表示单文件无界模式（对标 C# IDevice.SegmentSize 属性）
  #[inline]
  pub fn segment_size(&self) -> Option<u64> {
    self.segment_size
  }

  /// 是否启用 Direct I/O（对标 C# disableFileBuffering 取反语义）
  #[inline]
  pub fn direct_io(&self) -> bool {
    self.direct_io.load(Ordering::Relaxed)
  }

  /// 创建单文件无界存储设备（默认 4096 扇区大小；分段形态经 [`SegmentedDevice::new`]
  /// 传 `Some(段尺寸)` 构造，不再有第二套别名入口）
  #[inline]
  pub fn single_file(base_path: impl Into<PathBuf>) -> Result<Self> {
    Self::new(base_path, None, DEFAULT_SECTOR_SIZE)
  }

  /// 获取起始有效段编号（对应 C# IDevice.StartSegment）
  #[inline]
  pub fn start_segment(&self) -> u32 {
    self.start_segment.load(Ordering::SeqCst)
  }

  /// 已写入的最高段编号（None 表示尚未写入任何段；对应 C# IDevice.EndSegment，初始 -1）
  #[inline]
  pub fn end_segment(&self) -> Option<u32> {
    let v = self.end_segment.load(Ordering::SeqCst);
    (v >= 0).then_some(v as u32)
  }

  /// 获取设备容量上限（None 对应 C# Devices.CAPACITY_UNSPECIFIED）
  #[inline]
  pub fn capacity(&self) -> Option<u64> {
    self.capacity
  }
}

// Thread-Per-Core 无锁化架构论证：
// 1. `SegmentedDevice` 本身仅包含元数据与原子计数器，全字段天然满足 Send + Sync + 'static；
// 2. 段文件句柄通过 Thread-Local (`LOCAL_FILES`) 由各 CPU 核心本地隔离托管，完全解耦跨线程同步；
// 3. 句柄使用纯 `Rc<File>`，彻底消除原子引用计数与全局并发 Map 争用，compio 依赖恢复为纯 unsync 零成本；
// 4. 全局 sync 优先刷写本地在表句柄，并按需补齐全局在册写入段，各核心在各自 compio 驱动下独立提交，无跨核句柄竞争。

impl Device for SegmentedDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    SegmentedDevice::sector_size(self)
  }

  #[inline]
  fn segment_size(&self) -> Option<u64> {
    SegmentedDevice::segment_size(self)
  }

  #[inline]
  fn direct_io(&self) -> bool {
    SegmentedDevice::direct_io(self)
  }

  #[inline]
  fn recover(&self) -> Result<()> {
    SegmentedDevice::recover(self)
  }

  #[inline]
  fn start_segment(&self) -> u32 {
    self.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.end_segment()
  }

  #[inline]
  fn capacity(&self) -> Option<u64> {
    self.capacity
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    &self.pool
  }

  #[inline]
  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    self.write_impl(offset, buf).await
  }

  #[inline]
  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    self.read_impl(offset, buf, true).await
  }

  #[inline]
  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    self.read_impl(offset, buf, false).await
  }

  #[inline]
  fn sync(&self) -> impl Future<Output = Result<()>> {
    SegmentedDevice::sync(self)
  }

  #[inline]
  fn sync_data(&self) -> impl Future<Output = Result<()>> {
    SegmentedDevice::sync_data(self)
  }

  /// libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:TruncateUntilSegmentAsync
  #[inline]
  async fn truncate_until_segment(&self, segment_id: u32) -> Result<()> {
    self.truncate_until_segment_impl(segment_id).await
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> Result<u64> {
    SegmentedDevice::get_file_size(self, segment_id)
  }

  #[inline]
  fn remove_segment(&self, segment_id: u32) -> impl Future<Output = Result<()>> {
    SegmentedDevice::remove_segment(self, segment_id)
  }

  #[inline]
  fn reset(&self) {
    SegmentedDevice::reset(self);
  }
}

impl Drop for SegmentedDevice {
  fn drop(&mut self) {
    if !self.delete_on_close {
      return;
    }
    // 单文件模式直接删除主文件；分段模式按命名约定清理全部段文件
    if self.segment_size.is_none() {
      let _ = sync_remove_file(&self.base_path);
      return;
    }
    if let Ok(Some(entries)) = self.segment_entries() {
      for (_, entry) in entries.flatten() {
        let _ = sync_remove_file(entry.path());
      }
    }
  }
}
