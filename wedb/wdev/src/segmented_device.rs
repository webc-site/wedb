use std::{
  cell::RefCell,
  collections::hash_map::Entry,
  ffi::OsString,
  fs::{DirEntry, ReadDir, create_dir_all, metadata, read_dir, remove_file as sync_remove_file},
  io::{Error as IoError, ErrorKind, Result as IoResult},
  path::{Path, PathBuf},
  rc::Rc,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering},
  },
};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  fs::{File, OpenOptions, remove_file},
  io::{AsyncReadAt, AsyncWriteAt},
};
use futures_util::future::join_all;
use gxhash::HashMap;
#[cfg(windows)]
use wbase::map::{ConcurrentMap, new_concurrent_map};
use wbase::{
  AlignedBuf, BufferPool, DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE,
  base32::{BASE32_LEN_U64, decode_u64, encode_u64},
  is_valid_sector_size,
};

use crate::{
  chunk::{SegmentChunks, segment_mask, segment_shift, validate_aligned_io},
  device::Device,
  error::{Error, Result},
  sys::MAX_SEGMENT_SIZE,
};

static NEXT_DEVICE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
  /// Thread-Per-Core 本地段文件句柄缓存：
  /// 按 `(device_id, segment_id)` 隔离，单线程独占拥有 `Rc<File>`，
  /// 彻底杜绝全局并发 Map、跨核 cache-line 争用与原子引用计数开销
  static LOCAL_FILES: RefCell<HashMap<(u64, u32), Rc<File>>> =
    RefCell::new(HashMap::default());
}

/// 构造 papaya + gxhash 无锁并发字典（Windows 延迟删除队列专用；
/// 单一来源 `wbase::map::new_concurrent_map`，转写规范统一出口）
#[cfg(windows)]
#[inline]
fn concurrent_map<K, V>() -> ConcurrentMap<K, V>
where
  K: Send + Sync + 'static,
  V: Send + Sync + 'static,
{
  new_concurrent_map()
}

/// sync 持久化契约守护跟踪的段数上限（仅 debug 构建参与编译，release 零成本）
///
/// 守护目标为测试与 WAL 场景的契约回归，128 段（2 个 AtomicU64）已充分覆盖；
/// 超出窗口的写入段不跟踪、不校验
#[cfg(debug_assertions)]
const SYNC_GUARD_SEGMENTS: usize = 128;

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
  /// 是否处于只读模式（对标 C# StorageDeviceBase / ManagedLocalStorageDevice readOnly 参数）
  read_only: bool,
  /// 首次创建段文件时是否预分配段尺寸物理空间（对标 C# preallocateFile 参数）
  preallocate: bool,
  /// 析构关闭时是否自动物理删除段文件（对标 C# deleteOnClose 参数）
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
  pending_removes: ConcurrentMap<u32, ()>,
  /// 设备容量上限（字节；None 对应 C# Devices.CAPACITY_UNSPECIFIED）
  capacity: Option<u64>,
  /// 关联的扇区对齐缓冲池 (对应 C# RandomAccessLocalStorageDevice.pool)
  pool: Arc<BufferPool>,
  /// 新建段文件后父目录 fsync 的执行次数（可观测 hook：持久化目录项，保证
  /// 断电崩溃后新建段文件仍可见，详见 `sync_dir`）
  dir_syncs: AtomicU64,
  /// sync 持久化契约守护（仅 debug 构建参与编译，release 零成本）：
  /// 前 128 段的"已写入待 sync"全局位图。write 成功后置位、全局 sync 覆盖后清位，
  /// `sync_internal` 末尾校验全部在册写入均被本次 fsync 覆盖（段被截断推进
  /// `start_segment` 背书或 `remove_segment` 显式清除者免责），捕捉"写入段句柄被
  /// 异常路径移除导致脏页无人 fsync"的契约破坏
  #[cfg(debug_assertions)]
  dirty_segs: [AtomicU64; 2],
}

/// fsync 父目录持久化新建文件的目录项（返回是否实际执行了目录 fsync 系统调用）
///
/// 委托公共原语 [`crate::sync_dir`]。POSIX 语义下新建段文件后须 fsync 父目录，
/// 失败降级为 warn 日志不阻断写入；非 Unix 平台恒返回 false。
fn sync_dir(parent: &Path) -> bool {
  #[cfg(unix)]
  match crate::sync_dir(parent) {
    Ok(()) => true,
    Err(e) => {
      log::warn!(
        "新建段文件后 fsync 父目录 {} 失败: {e}，崩溃后新段可能不可见",
        parent.display()
      );
      false
    }
  }
  #[cfg(not(unix))]
  {
    let _ = crate::sync_dir(parent);
    false
  }
}

/// 解析段文件名后缀（`<base>.` 之后的 13 字符定长小写 Base32）为段号
///
/// 转写规范偏离（SKILL「文件名用 base32 编码」优先于 C# 十进制 1:1）：定长编码使
/// 文件名字典序与段号数值序严格一致。长度不符、非法字符、非规范小写（大写变体）、
/// 超 u32 段号域或非 UTF-8 的后缀一律判为非段文件（跨平台一致，枚举路径跳过）
#[inline]
fn parse_segment_suffix(rest: &[u8]) -> Option<u32> {
  let s = from_utf8(rest).ok()?;
  let val = decode_u64(s)?;
  // 规范性回验：仅认本设备写出的定长小写编码，杜绝大写变体杂散文件被误认段号
  if encode_u64(val).as_str() != s {
    return None;
  }
  u32::try_from(val).ok()
}

/// 目录中段文件迭代器（零多余内存分配，流式产出段号与目录项）
struct SegmentEntries<'a> {
  prefix: &'a [u8],
  read_dir: ReadDir,
}

impl Iterator for SegmentEntries<'_> {
  type Item = IoResult<(u32, DirEntry)>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let entry = match self.read_dir.next()? {
        Ok(e) => e,
        Err(e) => return Some(Err(e)),
      };
      let name = entry.file_name();
      let name_bytes = name.as_encoded_bytes();
      let Some(rest) = name_bytes.strip_prefix(self.prefix) else {
        continue;
      };
      let Some(rest) = rest.strip_prefix(b".") else {
        continue;
      };
      let Some(id) = parse_segment_suffix(rest) else {
        continue;
      };
      return Some(Ok((id, entry)));
    }
  }
}

impl SegmentedDevice {
  /// 创建新的块存储设备（自动初始化所属扇区的缓冲池）
  pub fn new(
    base_path: impl Into<PathBuf>,
    segment_size: Option<u64>,
    sector_size: usize,
  ) -> Result<Self> {
    if !is_valid_sector_size(sector_size) {
      return Err(Error::InvalidSectorSize {
        size: sector_size,
        min: MIN_SECTOR_SIZE,
      });
    }
    let pool = BufferPool::new(sector_size)?;
    Self::with_pool(base_path, segment_size, sector_size, pool)
  }

  /// 以指定共享缓冲池创建块存储设备 (对应 C# 注入外部 bufferPool 语义)
  ///
  /// Rust 补齐防御：注入池的扇区必须与设备扇区一致——错配时池化缓冲区的
  /// 地址/长度对齐口径与设备 Direct I/O 要求错位，运行期才以 EINVAL 暴露；
  /// 构造期直接拒绝（C# RandomAccessLocalStorageDevice 无此校验，属其隐患）
  pub fn with_pool(
    base_path: impl Into<PathBuf>,
    segment_size: Option<u64>,
    sector_size: usize,
    pool: Arc<BufferPool>,
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
      read_only: false,
      preallocate: false,
      delete_on_close: false,
      device_id: NEXT_DEVICE_ID.fetch_add(1, Ordering::Relaxed),
      start_segment: AtomicU32::new(0),
      end_segment: AtomicI32::new(-1),
      direct_io: AtomicBool::new(cfg!(target_os = "linux")),
      #[cfg(target_os = "linux")]
      direct_io_probed: AtomicBool::new(false),
      #[cfg(windows)]
      pending_removes: concurrent_map(),
      #[cfg(debug_assertions)]
      dirty_segs: [const { AtomicU64::new(0) }; 2],
      capacity: None,
      pool,
      dir_syncs: AtomicU64::new(0),
    })
  }

  /// 设置设备容量上限（须在首次 I/O 前调用；对标 C# Initialize 的容量校验：
  /// "capacity must be a multiple of segment sizes"）
  pub fn set_capacity(&mut self, capacity: Option<u64>) -> Result<()> {
    if let (Some(cap), Some(seg_size)) = (capacity, self.segment_size)
      && (cap == 0 || cap % seg_size != 0)
    {
      return Err(Error::InvalidCapacity { capacity: cap });
    }
    self.capacity = capacity;
    Ok(())
  }

  /// 设置是否处于只读保护模式（对标 C# readOnly 参数）
  #[inline]
  pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
    self.read_only = read_only;
    self
  }

  /// 设置首次打开段文件时是否预分配物理空间（对标 C# preallocateFile 参数）
  #[inline]
  pub fn set_preallocate(&mut self, preallocate: bool) -> &mut Self {
    self.preallocate = preallocate;
    self
  }

  /// 设置析构关闭时是否自动物理删除段文件（对标 C# deleteOnClose 参数）
  #[inline]
  pub fn set_delete_on_close(&mut self, delete_on_close: bool) -> &mut Self {
    self.delete_on_close = delete_on_close;
    self
  }

  /// 是否处于只读模式
  #[inline]
  pub fn is_read_only(&self) -> bool {
    self.read_only
  }

  /// 是否启用了段预分配
  #[inline]
  pub fn is_preallocate(&self) -> bool {
    self.preallocate
  }

  /// 是否在析构时删除段文件
  #[inline]
  pub fn is_delete_on_close(&self) -> bool {
    self.delete_on_close
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

  /// 新建段文件后父目录 fsync 的累计执行次数（目录项持久化可观测 hook）
  #[inline]
  pub fn dir_sync_count(&self) -> u64 {
    self.dir_syncs.load(Ordering::Relaxed)
  }

  /// 创建单文件无界存储设备（默认 4096 扇区大小）
  #[inline]
  pub fn single_file(base_path: impl Into<PathBuf>) -> Result<Self> {
    Self::new(base_path, None, DEFAULT_SECTOR_SIZE)
  }

  /// 创建分段存储设备（默认 4096 扇区大小）
  #[inline]
  pub fn segmented(base_path: impl Into<PathBuf>, segment_size: u64) -> Result<Self> {
    Self::new(base_path, Some(segment_size), DEFAULT_SECTOR_SIZE)
  }

  /// 获取父目录路径（base_path 无父目录分量时以当前工作目录 "." 兜底）
  #[inline]
  fn parent_dir(&self) -> &Path {
    match self.base_path.parent() {
      Some(p) if !p.as_os_str().is_empty() => p,
      _ => Path::new("."),
    }
  }

  /// 获取指定段编号对应的实际文件路径
  pub fn segment_path(&self, segment_id: u32) -> PathBuf {
    match self.segment_size {
      // OsString 拼接保证非 UTF-8 路径的字节精确性（对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:GetSegmentFilename）；
      // 段号编码为 13 字符定长小写 Base32（转写规范偏离，优先于 C# 十进制 1:1）：
      // 定长保证文件名字典序与段号数值序严格一致，杜绝十进制 ".10" < ".2" 的字典序倒挂
      Some(_) => {
        let seg_str = encode_u64(u64::from(segment_id));
        let base = self.base_path.as_os_str();
        let mut path = OsString::with_capacity(base.len() + 1 + BASE32_LEN_U64);
        path.push(base);
        path.push(".");
        path.push(seg_str);
        PathBuf::from(path)
      }
      None => self.base_path.clone(),
    }
  }

  /// 扫描目录中全部 `<base_name>.<段号>` 命名的段文件条目（流式迭代器，零多余内存分配）
  fn segment_entries(&self) -> IoResult<Option<SegmentEntries<'_>>> {
    let Some(file_name) = self.base_path.file_name() else {
      return Ok(None);
    };
    let read_dir = read_dir(self.parent_dir())?;
    Ok(Some(SegmentEntries {
      prefix: file_name.as_encoded_bytes(),
      read_dir,
    }))
  }

  /// 根据逻辑 offset 计算段编号及段内偏移
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:WriteAsync /
  /// StorageDeviceBase.cs:ReadAsync 的 `address >> segmentSizeBits` / `& segmentSizeMask`
  /// 单点换段算术（C# 内联于 IO 入口，Rust 抽为独立查询方法）
  #[inline]
  pub fn get_segment_and_offset(&self, offset: u64) -> Result<(u32, u64)> {
    match self.segment_size {
      Some(seg_size) => {
        let seg_id_u64 = offset >> segment_shift(seg_size);
        let seg_id = u32::try_from(seg_id_u64).map_err(|_| Error::SegmentExceeded(seg_id_u64))?;
        Ok((seg_id, offset & segment_mask(seg_size)))
      }
      None => Ok((0, offset)),
    }
  }

  /// 构造文件打开选项
  ///
  /// `create` 决定可写打开是否携带 O_CREAT：读写路径 true（对标 C# `OpenOrCreate`），
  /// sync 补开路径 false（仅打开已存在段——元数据预检后、open 前段文件被并发删除
  /// （`remove_segment`/外部删除）时以 NotFound 失败而非竞态窗口重建幽灵段）
  #[inline]
  fn open_options(read_only: bool, create: bool) -> OpenOptions {
    let mut opts = OpenOptions::new();
    opts.read(true);
    if read_only {
      opts.write(false).create(false);
    } else {
      opts.write(true).create(create);
    }
    opts
  }

  async fn try_preallocate(file: &File, path: &Path, preallocate: Option<u64>) {
    if let Some(sz) = preallocate
      && let Err(e) = file.set_len(sz).await
    {
      // 预分配仅为性能提示（写路径按需扩展文件），失败降级不致命，但须可观测
      log::warn!("段文件 {} 预分配至 {sz} 字节失败: {e}", path.display());
    }
  }

  /// 异步打开文件句柄（Linux O_DIRECT、只读保护与预分配）
  ///
  /// Direct I/O 启用策略对齐 C# 设备族：Linux 原生设备（libaio/io_uring）默认 O_DIRECT，
  /// 其余平台对齐 Managed 设备采用缓冲 I/O（享受 OS 页缓存与预读）；
  /// 只读打开同样适用 Direct（对标 C# disableFileBuffering 与 readOnly 可自由组合，
  /// 读路径绕过页缓存污染）。
  ///
  /// 探测定型（与 C# 的差异论证）：C# NativeDevice 以 O_DIRECT 打开失败即异常上抛，
  /// 无回退；早期 Rust 实现的"任意时刻失败即全局翻转"存在竞争隐患（并发写入途中
  /// 翻转标志并驱逐全部线程句柄）。本实现收窄为首次真实打开时探测一次支持性：
  /// 不支持类错误一次性定型为缓冲 I/O（此后 write 路径无运行中翻转），定型后
  /// Direct 打开失败直接上抛，语义对齐 C# 的快速失败。
  async fn open_file(
    &self,
    path: &Path,
    read_only: bool,
    preallocate: Option<u64>,
    create: bool,
  ) -> Result<File> {
    if !read_only
      && let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
    {
      let _ = create_dir_all(parent);
    }

    #[cfg(target_os = "linux")]
    if self.direct_io.load(Ordering::Relaxed) {
      let mut opts = Self::open_options(read_only, create);
      opts.custom_flags(libc::O_DIRECT);
      match opts.open(path).await {
        Ok(file) => {
          self.direct_io_probed.store(true, Ordering::Relaxed);
          log::debug!("成功以 Direct I/O (O_DIRECT) 打开文件: {}", path.display());
          if !read_only {
            Self::try_preallocate(&file, path, preallocate).await;
          }
          return Ok(file);
        }
        Err(e) if matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::Unsupported) => {
          // 未定型时才允许降级：仅对文件系统/内核不支持类错误定型，其余错误原样
          // 上抛避免掩盖真实故障；定型时驱逐全部已打开的 O_DIRECT 句柄，防止陈旧
          // 句柄收到未对齐 I/O (EINVAL)。此后 direct_io 恒为 false，写入路径不再回退
          if !self.direct_io_probed.swap(true, Ordering::Relaxed) {
            self.direct_io.store(false, Ordering::Relaxed);
            LOCAL_FILES.with(|m| {
              m.borrow_mut()
                .retain(|&(dev_id, _), _| dev_id != self.device_id);
            });
            log::error!(
              "Direct I/O 探测失败（{}: {e}），设备一次性定型为常规缓存 I/O，此后 Direct 打开失败将直接上抛",
              path.display()
            );
          } else if !self.direct_io.load(Ordering::Acquire) {
            // 并发首探竞态败方：胜方已将设备定型为常规缓存 I/O，落入下方
            // 缓冲打开重试，避免吃到本应一次性定型即可避免的假错误
            log::debug!(
              "并发 Direct I/O 探测竞态败方（{e}），按定型后的常规缓存 I/O 打开: {}",
              path.display()
            );
          } else {
            // Direct 形态（探测成功定型）下的打开失败：设计路径直接上抛，不回退。
            // 与胜方 swap→store 两指令窗口并发的败方可能在此误上抛一次伪失败
            //（无句柄副作用，下次重试即恢复；消除需合并双原子为状态字，收益不抵复杂度）
            return Err(Error::from(e));
          }
        }
        Err(e) => return Err(Error::from(e)),
      }
    }

    let file = Self::open_options(read_only, create).open(path).await?;
    log::debug!("成功打开文件: {}", path.display());

    if !read_only {
      Self::try_preallocate(&file, path, preallocate).await;
    }

    Ok(file)
  }

  /// 获取或异步打开指定段的句柄（当前 CPU 核心本地缓存优先，0 跨核争用）
  ///
  /// `create` 为 true 时段文件缺失即物理新建（对标 C# `GetOrAddHandle` 的
  /// OpenOrCreate 语义，读写路径专用）；为 false 时仅打开磁盘上已存在的段文件，
  /// 缺失段返回 [`Error::SegmentNotFound`]（sync 补开路径专用，杜绝从未写入的
  /// 空洞段因 sync 被幽灵创建，对齐 `recover` 的幽灵段防御）。
  async fn get_or_open_file(&self, segment_id: u32, create: bool) -> Result<Rc<File>> {
    // 0. 防御校验：已被截断的段严禁访问（与 Garnet begin_segment_ 语义一致）
    if segment_id < self.start_segment.load(Ordering::SeqCst) {
      return Err(Error::SegmentNotFound(segment_id));
    }

    let key = (self.device_id, segment_id);

    // 1. 快速路径：Thread-local 极速命中（L1 CPU 本地缓存，0 原子、0 锁、0 争用）
    if let Some(file) = LOCAL_FILES.with(|m| m.borrow().get(&key).cloned()) {
      return Ok(file);
    }

    // 2. 缓存未命中：当前线程驱动下异步打开文件
    let path = self.segment_path(segment_id);
    // 写模式下先探测段文件是否缺失：open(create) 将物理新建段文件，成功后须
    // fsync 父目录持久化目录项（探测与打开之间被并发抢先建文件仅多刷一次目录，无害）
    let is_new_segment = if create {
      !self.read_only && metadata(&path).is_err_and(|e| e.kind() == ErrorKind::NotFound)
    } else {
      // 仅打开已存在的段：他线程写入过的段必然已创建文件；缺失段无须为其刷盘
      match metadata(&path) {
        Ok(_) => false,
        Err(e) if e.kind() == ErrorKind::NotFound => {
          return Err(Error::SegmentNotFound(segment_id));
        }
        Err(e) => return Err(e.into()),
      }
    };
    let prealloc = if self.preallocate && !self.read_only {
      self.segment_size
    } else {
      None
    };
    let file = match self
      .open_file(&path, self.read_only, prealloc, create)
      .await
    {
      Ok(f) => f,
      // 补开路径（create=false）的删除竞态兜底：元数据预检后、open 前段文件被并发
      // 删除，不带 O_CREAT 的 open 以 NotFound 失败，映射为 SegmentNotFound——
      // 与预检同口径，杜绝竞态窗口物理重建幽灵段文件
      Err(Error::Io(e)) if !create && e.kind() == ErrorKind::NotFound => {
        return Err(Error::SegmentNotFound(segment_id));
      }
      Err(e) => return Err(e),
    };
    if is_new_segment && sync_dir(self.parent_dir()) {
      self.dir_syncs.fetch_add(1, Ordering::Relaxed);
    }

    // 3. 复核打开期间是否被并发截断，若是则清理并拒绝
    let truncated = segment_id < self.start_segment.load(Ordering::SeqCst);
    if truncated {
      if !self.read_only {
        let _ = remove_file(&path).await;
      }
      return Err(Error::SegmentNotFound(segment_id));
    }

    let rc = Rc::new(file);
    LOCAL_FILES.with(|m| {
      m.borrow_mut().insert(key, Rc::clone(&rc));
    });

    Ok(rc)
  }

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

  /// 重置设备句柄缓存（关闭并遗忘当前线程打开的文件句柄，与 C# IDevice.Reset 语义一致）
  pub fn reset(&self) {
    LOCAL_FILES.with(|m| {
      m.borrow_mut()
        .retain(|&(dev_id, _), _| dev_id != self.device_id);
    });
    #[cfg(debug_assertions)]
    {
      // 遗忘句柄后 fsync 职责移交给后续任意线程的重新打开（fsync 按 inode 全量生效，
      // 关闭 fd 不丢内核脏页），在册写入位图随之失效，全部清除避免守护误报
      for word in &self.dirty_segs {
        word.store(0, Ordering::Relaxed);
      }
    }
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

  /// 判断 [offset, offset+len) 是否完全落在单个段内（单段零切片快速路径判定）
  #[inline]
  fn within_single_segment(&self, offset: u64, len: usize) -> bool {
    match self.segment_size {
      None => true,
      Some(seg_size) => {
        let off_in_seg = offset & segment_mask(seg_size);
        off_in_seg
          .checked_add(len as u64)
          .is_some_and(|end| end <= seg_size)
      }
    }
  }

  /// 从磁盘恢复设备元数据（须在首次 I/O 前调用）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:RecoverFiles + Native ValidateRecoveredSegments：
  /// 1. 扫描 `<base_path>.<id>` 段文件并解析段号；
  /// 2. 校验已存在段文件大小不超过配置段大小（超过返回 SegmentSizeMismatch）；
  /// 3. 段号出现空隙处即恢复后的 start_segment（空隙前的段视为已被截断删除，
  ///    防止重启后对已删段的访问幽灵重建段文件）；
  /// 4. end_segment 恢复为最大连续段号。单文件模式为无操作。
  pub fn recover(&self) -> Result<()> {
    let Some(seg_size) = self.segment_size else {
      return Ok(());
    };

    let mut segids: Vec<u32> = Vec::new();
    if let Some(entries) = self.segment_entries()? {
      for item in entries {
        let (id, entry) = item?;
        // 校验已存在段文件大小（对标 Native ValidateRecoveredSegments）
        match entry.metadata() {
          Ok(m) => {
            let file_size = m.len();
            if file_size > seg_size {
              return Err(Error::SegmentSizeMismatch {
                segment: id,
                file_size,
                segment_size: seg_size,
              });
            }
            segids.push(id);
          }
          // 扫描间隙被外部删除的文件不计数，避免恢复出幽灵段
          Err(e) if e.kind() == ErrorKind::NotFound => {}
          Err(e) => return Err(Error::Io(e)),
        }
      }
    }
    segids.sort_unstable();

    // 对齐 C# RecoverFiles 状态机：prev 初始 -1，出现空隙处更新 start_segment，
    // 连续处更新 end_segment
    let mut prev: i64 = -1;
    let mut recovered_start = 0u32;
    for id in segids {
      if i64::from(id) != prev + 1 {
        recovered_start = id;
      } else {
        let seg = i32::try_from(id).unwrap_or(i32::MAX);
        self.end_segment.fetch_max(seg, Ordering::SeqCst);
      }
      prev = i64::from(id);
    }
    self
      .start_segment
      .fetch_max(recovered_start, Ordering::SeqCst);
    Ok(())
  }

  /// 有界容量设备的段逐出（对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:HandleCapacity）：
  /// 写入新段时单调推进 end_segment，若容量有限则截断至
  /// `end_segment - capacity/segment_size` 之前以腾出空间
  async fn handle_capacity(&self, segment: u32) -> Result<()> {
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
    let ids: Vec<u32> = self
      .pending_removes
      .pin()
      .iter()
      .map(|(&id, _)| id)
      .collect();
    let mut done = Vec::new();
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

  /// 刷盘同步设备上全部在表段文件句柄（全量落盘，包含数据与元数据 fsync）
  ///
  /// 语义对齐 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:LocalStorageDevice：句柄表进程级共享，任意线程的 sync 覆盖设备上
  /// 全部线程已打开的句柄（遍历全集并按段号去重——fsync 按 inode 全量生效，同段多个
  /// fd 仅需一次 fsync，调用线程不再重复刷其他线程已刷过的同一 inode）。sync 返回
  /// 即保证：调用发起前已在任意线程完成的全部写入持久化，release 构建不存在
  /// "线程 A 写入、线程 B sync 漏刷他线程句柄"的静默丢失窗口。
  ///
  /// # 跨线程可行性（Thread-Per-Core 无句柄迁移）
  ///
  /// - compio `File` 未启用 `sync` feature 时内含 `Rc`（!Send），句柄无法跨线程共享；
  ///   本实现据此设计为"句柄永不过线程"：sync 只收集调用线程 TLS 内的本地句柄，
  ///   他线程写入过的段由调用线程按文件存在性就地补开（打开动作发生且仅发生在
  ///   调用线程自身的驱动上），随后对本线程句柄提交 fsync/fdatasync；
  /// - fd 属进程级 files_struct，fsync 按 inode 全量生效、无线程亲和——调用线程对
  ///   同一 inode 补开刷新，与持有原始写入句柄的他线程自行刷新落盘等价；
  ///   O_DIRECT fd 的 fsync/fdatasync 同样无线程亲和要求。
  ///
  /// # 排序契约（与 POSIX fsync 同口径）
  ///
  /// 仅覆盖"sync 开始收集句柄前已在任意线程完成"的写入；收集后完成的并发写入由
  /// 下一次 sync 背书。调用方（whlog flush → wkv flush_all / evict_pages_for）均为
  /// 同线程"写后即 sync"，天然满足。
  ///
  /// # 持久化承诺（含目录项）
  ///
  /// 段文件由设备层创建时已同步 fsync 父目录，因此本方法返回后"新段写入 + sync
  /// 即持久"的承诺同时覆盖段文件数据与新建段的目录项，断电崩溃后新段保证可见；
  /// WAL/HLog 等调用方无需自行刷盘目录。
  pub async fn sync(&self) -> Result<()> {
    self.sync_internal(false).await
  }

  /// 异步刷盘仅同步文件数据（fdatasync），尽量避免同步 inode 元数据（时间戳等）
  ///
  /// 语义与 [`SegmentedDevice::sync`] 相同（全局覆盖、按段去重），仅系统调用降级为
  /// fdatasync；新建段的目录项持久化口径亦同：段创建时设备层已一次性 fsync 父目录，
  /// 本方法的持久化承诺同样覆盖新段可见性。
  pub async fn sync_data(&self) -> Result<()> {
    self.sync_internal(true).await
  }

  /// 全局 sync 统一实现：收集当前 CPU 核心本地在表句柄并按需补齐在册脏段后逐个 fsync
  async fn sync_internal(&self, datasync: bool) -> Result<()> {
    let min_seg = self.start_segment.load(Ordering::Relaxed);
    let max_seg = self.end_segment.load(Ordering::Relaxed);
    // 守护基线：先取"发起前在册"的脏段快照（这些写入必须被本次 sync 覆盖），
    // 快照后并发完成的新写入由下一次 sync 背书（排序契约同 POSIX fsync）
    #[cfg(debug_assertions)]
    let pending = self.debug_dirty_segments();

    // 1. 获取当前 CPU 核心本地已打开的段句柄并主动释放已被截断的陈旧句柄
    let mut files: HashMap<u32, Rc<File>> = LOCAL_FILES.with(|map| {
      let mut map = map.borrow_mut();
      map.retain(|&(dev_id, sid), _| dev_id != self.device_id || sid >= min_seg);
      map
        .iter()
        .filter(|&(&(dev_id, sid), _)| dev_id == self.device_id && sid >= min_seg)
        .map(|(&(_, sid), f)| (sid, Rc::clone(f)))
        .collect()
    });

    // 2. 若存在本核心未打开但全局在册已写入的段（如他核心写入后跨线程 sync），按需补开。
    //    仅打开磁盘上已存在的段文件（create=false）：从未写入的空洞段无脏页无须刷盘，
    //    绝不因 sync 被幽灵创建（对齐 recover 的幽灵段防御）；在册写入段打开失败属
    //    真实 I/O 故障，静默跳过会造成"返回 Ok 却漏刷"的假持久化，必须上抛
    if max_seg >= min_seg as i32 {
      let max_u32 = max_seg as u32;
      for sid in min_seg..=max_u32 {
        if let Entry::Vacant(e) = files.entry(sid) {
          match self.get_or_open_file(sid, false).await {
            Ok(file) => {
              e.insert(file);
            }
            // 空洞段（从未写入）或已并发截断的段（截断推进 start_segment 背书免责）
            Err(Error::SegmentNotFound(_)) => {}
            Err(e) => return Err(e),
          }
        }
      }
    }

    // 快速路径 1：无段需刷盘
    if files.is_empty() {
      #[cfg(debug_assertions)]
      self.debug_verify_synced(&pending, &files);
      return Ok(());
    }

    // 快速路径 2：单段高频场景（WAL 顺序追加写尾段），免除 join 堆内存分配
    if files.len() == 1
      && let Some(file) = files.values().next()
    {
      let res = if datasync {
        file.sync_data().await
      } else {
        file.sync_all().await
      };
      res.map_err(Error::from)?;
      #[cfg(debug_assertions)]
      self.debug_verify_synced(&pending, &files);
      return Ok(());
    }

    // 多段并发场景：在 compio 驱动下并发下发所有段的 sync，由操作系统内核与 NVMe 硬件并发刷新脏段
    // 采用 join_all 确保所有段均尽最大努力完成下发与落盘，避免短路取消导致后续段脏页滞留
    let results = join_all(files.values().map(|file| {
      let file = Rc::clone(file);
      async move {
        if datasync {
          file.sync_data().await
        } else {
          file.sync_all().await
        }
      }
    }))
    .await;

    let mut first_err = None;
    for res in results {
      if let Err(e) = res
        && first_err.is_none()
      {
        first_err = Some(Error::from(e));
      }
    }

    if let Some(err) = first_err {
      return Err(err);
    }

    // 仅在全部段成功刷盘后，才清位并断言契约（失败时保留在册脏段状态）
    #[cfg(debug_assertions)]
    self.debug_verify_synced(&pending, &files);

    Ok(())
  }

  /// 置位全局写入段位图（sync 持久化契约守护，仅 debug 构建参与编译）
  ///
  /// 超出守护窗口（前 128 段）的段不跟踪：守护目标为测试与 WAL 场景的契约回归，
  /// 位图窗口取 2 个 AtomicU64 编译期定长，避免动态扩容开销
  #[cfg(debug_assertions)]
  fn debug_mark_dirty(&self, segment_id: u32) {
    let seg = segment_id as usize;
    if seg < SYNC_GUARD_SEGMENTS {
      self.dirty_segs[seg / 64].fetch_or(1 << (seg % 64), Ordering::Relaxed);
    }
  }

  /// 校验全局 sync 覆盖发起前在册的全部写入段并清位（sync 持久化契约守护，仅 debug 构建）
  ///
  /// `pending` 为 sync 发起时的在册脏段快照（位图置位发生在写入成功之后，故快照段
  /// 的段文件必已存在于磁盘，sync 补开循环必然能按文件存在性打开覆盖）。
  /// 违约判定：快照段既不在本次全局 fsync 覆盖集合（在表句柄按段
  /// 去重全集）中，也未获截断背书（段号 < `start_segment` 为已删除段）——脏页将
  /// 无人 fsync，断言失败暴露"句柄被异常路径移除"的契约破坏。快照后的并发新写入
  /// 不在本快照内，由下一次 sync 背书；免责段一并清位防陈旧位滞留
  #[cfg(debug_assertions)]
  fn debug_verify_synced(&self, pending: &[u32], synced: &HashMap<u32, Rc<File>>) {
    let start_seg = u64::from(self.start_segment.load(Ordering::Relaxed));
    for &seg in pending {
      let bit = !(1 << (seg as usize % 64));
      let covered = synced.contains_key(&seg) || u64::from(seg) < start_seg;
      assert!(
        covered,
        "sync 契约违约：段 {seg} 在册有写入，但本次全局 sync 未覆盖其句柄且段未被截断背书，脏页无人 fsync"
      );
      self.dirty_segs[seg as usize / 64].fetch_and(bit, Ordering::Relaxed);
    }
  }

  /// 清除全局位图中指定段的在册位（sync 持久化契约守护，仅 debug 构建参与编译）
  ///
  /// 供 `remove_segment` 调用：段被显式删除后其数据语义上已废弃，无须 fsync 背书
  #[cfg(debug_assertions)]
  fn debug_clear_segment(&self, segment_id: u32) {
    let seg = segment_id as usize;
    if seg < SYNC_GUARD_SEGMENTS {
      self.dirty_segs[seg / 64].fetch_and(!(1 << (seg % 64)), Ordering::Relaxed);
    }
  }

  /// 读取契约守护仍在册的待 sync 段列表（仅 debug 构建编译；供测试与调用方观测
  /// "写入后、sync 前"的未覆盖窗口，sync 覆盖、显式删段或截断后即清空）
  #[cfg(debug_assertions)]
  pub fn debug_dirty_segments(&self) -> Vec<u32> {
    (0..SYNC_GUARD_SEGMENTS)
      .filter(|&seg| self.dirty_segs[seg / 64].load(Ordering::Relaxed) & (1 << (seg % 64)) != 0)
      .map(|seg| seg as u32)
      .collect()
  }

  /// 读取统一内核：`aligned` 为 true 时执行扇区对齐校验（Direct I/O 专用），
  /// 为 false 时按任意逻辑范围直读（缓冲 I/O 专用）；单段/跨段切片与收割逻辑共享
  async fn read_impl(
    &self,
    offset: u64,
    mut buf: AlignedBuf,
    aligned: bool,
  ) -> (Result<usize>, AlignedBuf) {
    let sector_size = self.sector_size;
    // 读长度按租借时的请求需求封顶（非池化缓冲区即容量），杜绝 class 圆整导致的读放大
    let target_len = buf.required_len().min(buf.capacity());
    if aligned {
      if let Err(e) = validate_aligned_io(offset, target_len, &buf, sector_size) {
        return (Err(e), buf);
      }
    } else if offset.checked_add(target_len as u64).is_none() {
      return (
        Err(Error::OutOfBounds {
          offset,
          len: target_len,
        }),
        buf,
      );
    }
    if target_len == 0 {
      return (Ok(0), buf);
    }

    // 单段快速路径：整块直接异步读取，避免逐段 slice 开销；
    // 按 required_len 精确封顶，杜绝池 class 圆整导致的读放大与末段幽灵段创建
    if self.within_single_segment(offset, target_len) {
      let (seg_id, start_off) = match self.get_segment_and_offset(offset) {
        Ok(v) => v,
        Err(e) => return (Err(e), buf),
      };
      let file = match self.get_or_open_file(seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };
      let slice = buf.slice(0..target_len);
      let BufResult(res, slice) = file.read_at(slice, start_off).await;
      buf = slice.into_inner();
      let bytes_read = match res {
        Ok(n) => n,
        Err(e) => {
          unsafe { buf.set_len_unchecked(0) };
          return (Err(Error::from(e)), buf);
        }
      };
      // 成功路径 compio 已借 SetLen 回写长度，此处显式收口保证口径一致
      unsafe { buf.set_len_unchecked(bytes_read) };
      return (Ok(bytes_read), buf);
    }

    // 跨段读取慢路径：一次性暴露请求长度以支持逐段 slice，收尾时统一收缩到实际读到的长度
    unsafe { buf.set_len_unchecked(target_len) };

    let mut total_read = 0;
    let mut first_err = None;
    for chunk in SegmentChunks::new(offset, target_len, self.segment_size) {
      let chunk = match chunk {
        Ok(c) => c,
        Err(e) => {
          first_err = Some(e);
          break;
        }
      };

      let file = match self.get_or_open_file(chunk.seg_id, true).await {
        Ok(f) => f,
        Err(e) => {
          first_err = Some(e);
          break;
        }
      };

      // slice/into_inner 不改变父缓冲区长度，循环内无需重复 set_len
      let slice = buf.slice(chunk.buf_pos..chunk.buf_pos + chunk.len);
      let BufResult(res, slice) = file.read_at(slice, chunk.off_in_seg).await;
      buf = slice.into_inner();

      match res {
        Ok(n) => {
          total_read += n;
          if n < chunk.len {
            // 已读至文件末尾 (EOF)
            break;
          }
        }
        Err(e) => {
          first_err = Some(Error::Io(e));
          break;
        }
      }
    }

    unsafe { buf.set_len_unchecked(total_read) };
    match first_err {
      Some(e) => (Err(e), buf),
      None => (Ok(total_read), buf),
    }
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

  async fn write_aligned(&self, offset: u64, mut buf: AlignedBuf) -> (Result<usize>, AlignedBuf) {
    let sector_size = self.sector_size;
    let total_len = buf.len();
    if self.read_only {
      return (
        Err(Error::ReadOnly {
          offset,
          len: total_len,
        }),
        buf,
      );
    }
    // 单文件模式容量为硬上界：offset + len 越界即拒绝。与 C# 的差异论证：C#
    // StorageDeviceBase.HandleCapacity 在 segmentSize = -1（单文件）时
    // `Capacity >> segmentSizeBits`（bits=64）位移按 C# 语义归约为 >> 0，
    // newStartSegment 为巨负数使单调推进失效，容量形同虚设、写入无限增长；
    // Rust 补齐该语义空洞，以显式越界拒绝背书"容量即物理上限"承诺。
    // 分段模式容量不走此分支：经 handle_capacity 逐出最老段腾挪（对标 HandleCapacity）
    if let Some(cap) = self.capacity
      && self.segment_size.is_none()
      && offset.saturating_add(total_len as u64) > cap
    {
      return (
        Err(Error::OutOfBounds {
          offset,
          len: total_len,
        }),
        buf,
      );
    }
    if let Err(e) = validate_aligned_io(offset, total_len, &buf, sector_size) {
      return (Err(e), buf);
    }
    if total_len == 0 {
      return (Ok(0), buf);
    }

    // 单段快速路径：整块直接异步写入，避免 slice 开销
    if self.within_single_segment(offset, total_len) {
      let (seg_id, start_off) = match self.get_segment_and_offset(offset) {
        Ok(v) => v,
        Err(e) => return (Err(e), buf),
      };
      if let Err(e) = self.handle_capacity(seg_id).await {
        return (Err(e), buf);
      }
      let file = match self.get_or_open_file(seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };
      let mut file_ref = &*file;
      let BufResult(res, mut cur_buf) = file_ref.write_at(buf, start_off).await;
      match res {
        Ok(n) if n == total_len => {
          #[cfg(debug_assertions)]
          self.debug_mark_dirty(seg_id);
          return (Ok(n), cur_buf);
        }
        Ok(0) => {
          return (
            Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
            cur_buf,
          );
        }
        Ok(mut written) => {
          // 短写补写循环：切片递进直至写满或报错
          while written < total_len {
            let slice = cur_buf.slice(written..total_len);
            let mut file_ref = &*file;
            let BufResult(res, slice) = file_ref.write_at(slice, start_off + written as u64).await;
            cur_buf = slice.into_inner();
            match res {
              Ok(0) => {
                return (
                  Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
                  cur_buf,
                );
              }
              Ok(n) => written += n,
              Err(e) => return (Err(Error::Io(e)), cur_buf),
            }
          }
          #[cfg(debug_assertions)]
          self.debug_mark_dirty(seg_id);
          return (Ok(written), cur_buf);
        }
        Err(e) => return (Err(Error::from(e)), cur_buf),
      }
    }

    // 跨段写入慢路径：基于 SegmentChunks 进行流式分片写入
    let mut total_written = 0;
    for chunk in SegmentChunks::new(offset, total_len, self.segment_size) {
      let chunk = match chunk {
        Ok(c) => c,
        Err(e) => return (Err(e), buf),
      };
      if let Err(e) = self.handle_capacity(chunk.seg_id).await {
        return (Err(e), buf);
      }
      let file = match self.get_or_open_file(chunk.seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };

      let mut chunk_written = 0;
      while chunk_written < chunk.len {
        let slice = buf.slice(chunk.buf_pos + chunk_written..chunk.buf_pos + chunk.len);
        let mut file_ref = &*file;
        let BufResult(res, slice) = file_ref
          .write_at(slice, chunk.off_in_seg + chunk_written as u64)
          .await;
        buf = slice.into_inner();

        match res {
          Ok(0) => {
            return (
              Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
              buf,
            );
          }
          Ok(n) => {
            chunk_written += n;
            total_written += n;
          }
          Err(e) => return (Err(Error::Io(e)), buf),
        }
      }
      #[cfg(debug_assertions)]
      self.debug_mark_dirty(chunk.seg_id);
    }

    (Ok(total_written), buf)
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
  async fn truncate_until_segment(&self, segment_id: u32) -> Result<()> {
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
            self.pending_removes.pin().insert(id, ());
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

#[cfg(test)]
mod tests {
  use wbase::base32::encode_u64;

  use super::parse_segment_suffix;

  /// 定长 Base32 段名的字典序与段号数值序严格一致（目录按名排序即按段号排序）
  #[test]
  fn test_segment_name_order_preserving() {
    let samples = [
      0u32,
      1,
      2,
      9,
      10,
      11,
      99,
      100,
      999,
      1000,
      u32::MAX - 1,
      u32::MAX,
    ];
    for pair in samples.windows(2) {
      let prev = encode_u64(u64::from(pair[0]));
      let next = encode_u64(u64::from(pair[1]));
      assert!(
        prev.as_str() < next.as_str(),
        "段 {} 与 {} 的文件名字典序应随段号单调递增（{} < {}）",
        pair[0],
        pair[1],
        prev.as_str(),
        next.as_str()
      );
    }
  }

  #[test]
  fn test_parse_segment_suffix() {
    // 编解码回环：边界段号全通过
    for seg in [0u32, 1, 2, 9, 10, 12, 1000, u32::MAX] {
      assert_eq!(
        parse_segment_suffix(encode_u64(u64::from(seg)).as_bytes()),
        Some(seg)
      );
    }

    // 长度不符（历史十进制短名与杂散后缀均非段文件）
    assert_eq!(parse_segment_suffix(b""), None);
    assert_eq!(parse_segment_suffix(b"0"), None);
    assert_eq!(parse_segment_suffix(b"12"), None);
    assert_eq!(parse_segment_suffix(b"txt"), None);
    assert_eq!(parse_segment_suffix(b"000000000000"), None);
    assert_eq!(parse_segment_suffix(b"00000000000000"), None);

    // 高位字符表边界：'u'=30、'v'=31 为合法最大数字位（11 个前导 '0' 凑足定长 13）
    assert_eq!(parse_segment_suffix(b"00000000000uv"), Some(30 * 32 + 31));

    // 非法字符与符号前缀
    assert_eq!(parse_segment_suffix(b"00000000000+"), None);
    assert_eq!(parse_segment_suffix(b"00000000000.0"), None);
    assert_eq!(parse_segment_suffix(b"00000000000w"), None);

    // 大写变体：可解码但非本设备规范小写命名，判非段文件
    assert_eq!(parse_segment_suffix(b"000000000000A"), None);

    // 超 u32 段号域（首字符载荷非 0，本设备永不生成）
    assert_eq!(parse_segment_suffix(encode_u64(1 << 40).as_bytes()), None);

    // 非 UTF-8 字节序列
    assert_eq!(parse_segment_suffix(b"0000000000\xff00"), None);
  }
}
