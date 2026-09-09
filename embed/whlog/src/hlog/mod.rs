use std::{
  io::{Error as IoError, ErrorKind},
  ops::Deref,
  slice::from_raw_parts_mut,
  sync::{Arc, atomic::AtomicU64},
};

use log::{debug, info};
use parking_lot::{Mutex, RwLockReadGuard};
use wdev::Device;
use wepoch::LightEpoch;
use wrecord::{
  ADDRESS_MASK, HEADER_SIZE, RecordHeader, RecordRef, checked_record_size, encode_to_slice,
};
use wutil::AlignedBuf;

use crate::{
  address::{AddressManager, AddressSnapshot},
  buffer::CircularPageBuffer,
  config::HybridLogConfig,
  error::{Error, Result},
  flush::PendingFlushList,
};

/// 追加与换页（对标 C# Garnet TryAllocate / HandlePageOverflow）
mod append;
/// 原位更新 / 墓碑标记 / RMW / 复活（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:InternalRMW / InternalDelete / InternalUpsert / BlockAllocate）
mod inplace;
/// 读路径与批量刷盘 I/O（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead / AsyncGetFromDisk / AsyncFlushPages）
mod io;
/// 地址滑动与截断（对标 C# ShiftReadOnlyAddress / ShiftHeadAddress / ShiftBeginAddress）
mod shift;

/// 换页填充标记中的特殊魔数（key_len 为 u32::MAX 表示 Pad 填充，统一由 wrecord 提供）
pub use wrecord::PAD_KEY_LEN;

/// 点读冷路径直接映射磁盘页缓存槽位数（`page_id % SLOTS` 寻址，
/// 对标 [crate::scan::ScanIterator] 的单页磁盘预取语义）
const DISK_READ_CACHE_SLOTS: usize = 2;
const DISK_READ_CACHE_MASK: usize = DISK_READ_CACHE_SLOTS - 1;

/// 磁盘冷读首段 I/O 探测长度。C# Garnet DefaultInitialIORecordSize = 128（IStreamBuffer.cs），
/// 此处按扇区对齐设备模型取 4096，为刻意差异
const DISK_READ_PROBE_LEN: usize = 4096;

/// 磁盘页缓存单槽状态：`(逻辑页号, 整页缓冲)`，None 表示槽位未装载
type DiskPageSlot = Option<(u64, AlignedBuf)>;

/// Microsoft Garnet Tsavorite 架构风格的 HybridLog 混合日志分配器
///
/// 具备 64 位连续逻辑地址空间、环形页缓冲池与
/// Mutable (可变) / ReadOnly (只读) / OnDisk (磁盘) 三区动态滑动状态机。
///
/// 与 C# AllocatorBase.cs 的刻意差异（compio 线程每核模型）：
/// - 页级刷盘重设计为调用方驱动的单线程批量刷盘（[Self::flush_pages_range]），
///   硬件持久化屏障由调用方 [Self::sync] 承担，崩溃一致性边界为
///   `flushed_until`（连续已落盘前缀）+ sync；
/// - 崩溃残留的撕裂记录在 [Self::recover] 中按 `flushed_until` 前缀规则强制清零，
///   恢复可见状态严格限定为已持久化前缀。
pub struct HybridLog<D: Device> {
  /// 日志配置
  pub config: HybridLogConfig,
  /// 块存储设备
  pub device: Arc<D>,
  /// 纪元并发保护管理器
  pub epoch: Arc<LightEpoch>,
  /// 三区逻辑地址状态机
  pub addresses: Arc<AddressManager>,
  /// 环形页缓冲池
  pub buffer: CircularPageBuffer,
  /// 换页互斥锁（仅在页内空间不足触发换页时获取，页内追加完全无锁并发，对标 Garnet HandlePageOverflow）
  page_turn_lock: Mutex<()>,
  /// 刷盘 staging 缓冲复用（消除每次刷盘的池借还与归还清零，None 表示需新分配）
  flush_staging: Mutex<Option<AlignedBuf>>,
  /// 点读冷路径直接映射磁盘页缓存（[DISK_READ_CACHE_SLOTS] 槽，`page_id % SLOTS` 寻址）
  ///
  /// 每槽 `Option<(逻辑页号, 整页 AlignedBuf)>`，槽缓冲惰性分配、拷贝式换装。
  /// 仅装载整页均已刷盘且冻结只读（`页起点 + page_size <= min(flushed_until, read_only)`）
  /// 的页面——边界单调 ⇒ 页设备字节此后不可变，缓存与设备恒一致；同进程截断仅整段删除
  /// 历史段文件，begin 以下地址再也无法通过读守卫，陈旧槽位永不命中，无需失效回调。
  /// 完整安全性论证见 [HybridLog::read_disk_record]。
  disk_read_cache: Mutex<Box<[DiskPageSlot]>>,
  /// 连续性装载门槛的启发式状态：上一次未命中 probe 的逻辑页号（0 表示无记录）
  ///
  /// read_disk_record 未命中时 swap 入当前页号，仅当上一次未命中同为该页（连续性启发：
  /// 顺序读 / 同页热点负载）且满足缓存装载门槛时才整页装载磁盘读缓存；均匀随机负载
  /// 页号几乎不重复，恒走 [DISK_READ_PROBE_LEN] 级小读，避免零命中率场景整页读的
  /// 16 倍单次字节放大。64 位逻辑页号单调不复用，纯启发式状态无需失效清空；
  /// 设计动机与行为矩阵见 [HybridLog::read_disk_record]。
  ///
  /// 边界说明：初值 0 与页 0 页号重合，仅可能令进程内首个页 0 冷读未命中提前触发
  /// 一次装载——装载门槛仍强制校验，无正确性影响，仅一次性的无害启发误差。
  last_probe_page: AtomicU64,
  /// 待刷盘区间贪心合并队列（对标 Garnet PendingFlushList.cs）
  pub pending_flush: PendingFlushList,
}

/// 扫描/探测共用的页数据零拷贝借用形态（三区统一）
pub(crate) enum PageBytes<'a> {
  /// 磁盘冷读整页（独占拥有，扫描消费后回填单页预取缓存）
  Disk(AlignedBuf),
  /// epoch 保护区裸指针直读切片（只读区 + 可变区统一无锁直读，
  /// 可变区撕裂安全论证见 [HybridLog::probe_resident]）
  Raw(&'a [u8]),
  /// 页读锁守卫（无锁直读未命中的瞬态窗口兜底）
  Locked(RwLockReadGuard<'a, AlignedBuf>),
}

impl Deref for PageBytes<'_> {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &[u8] {
    match self {
      Self::Disk(buf) => buf.as_slice(),
      Self::Raw(slice) => slice,
      Self::Locked(guard) => guard.as_slice(),
    }
  }
}

/// Pad 标记或全零空洞记录统一拦截（内存与磁盘读路径共用，对标 C# 零头守卫）
#[inline(always)]
pub(crate) const fn reject_pad(header: RecordHeader, addr: u64) -> Result<()> {
  if header.is_pad() || header.is_null() {
    Err(Error::PadRecord(addr))
  } else {
    Ok(())
  }
}

/// 解析内存或磁盘切片中的记录引用（单次零拷贝直接构建，杜绝二次头解码与冗余边界检查）
#[inline]
pub(crate) fn parse_record_from_slice(
  page_slice: &[u8],
  offset: usize,
  addr: u64,
  page_size: usize,
) -> Result<RecordRef<'_>> {
  if offset + HEADER_SIZE > page_slice.len() || offset + HEADER_SIZE > page_size {
    return Err(Error::PadRecord(addr));
  }

  let header = RecordHeader::from_slice(&page_slice[offset..offset + HEADER_SIZE])?;
  // 对齐 C# Tsavorite：Pad 标记或全零空洞记录均作为换页填充处理
  reject_pad(header, addr)?;

  let logical_size = header.record_size();
  let physical_size = header.physical_size();
  let end = match offset.checked_add(logical_size) {
    Some(e)
      if offset.saturating_add(physical_size) <= page_slice.len()
        && offset.saturating_add(physical_size) <= page_size =>
    {
      e
    }
    _ => {
      return Err(Error::RecordCorrupted {
        addr,
        detail: "记录完整内容超出页面容量边界".into(),
      });
    }
  };

  let key_start = offset + HEADER_SIZE;
  let key_end = key_start + header.key_len as usize;
  // SAFETY: end <= page_slice.len() 且 key_end <= end 已在上方 checked_add / saturating_add 分支完全校验
  let (key, value) = unsafe {
    (
      page_slice.get_unchecked(key_start..key_end),
      page_slice.get_unchecked(key_end..end),
    )
  };

  Ok(RecordRef { header, key, value })
}

/// 待写入记录的键值参数包（消除多参透传）
pub(crate) struct RecParams<'a> {
  /// 48 位前驱版本逻辑地址
  pub prev_addr: u64,
  pub key: &'a [u8],
  pub val: &'a [u8],
  pub is_tombstone: bool,
}

impl<D: Device> HybridLog<D> {
  /// 组装实例公共字段（[Self::new] 与 [Self::recover] 共用，收敛重复构造）
  fn assemble(
    config: HybridLogConfig,
    device: Arc<D>,
    epoch: Arc<LightEpoch>,
    addresses: Arc<AddressManager>,
    buffer: CircularPageBuffer,
  ) -> Self {
    Self {
      config,
      device,
      epoch,
      addresses,
      buffer,
      page_turn_lock: Mutex::new(()),
      flush_staging: Mutex::new(None),
      disk_read_cache: Mutex::new(vec![None; DISK_READ_CACHE_SLOTS].into_boxed_slice()),
      last_probe_page: AtomicU64::new(0),
      pending_flush: PendingFlushList::new(),
    }
  }

  /// 创建新的 HybridLog 实例

  /// 校验日志块分配器的页面配置与底层设备扇区大小的兼容性
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:VerifyCompatibleSectorSize
  #[inline]
  pub(crate) fn verify_compatible_sector_size(config: &HybridLogConfig, device: &D) -> Result<()> {
    let sector_size = device.sector_size();
    if !config.page_size.is_multiple_of(sector_size) {
      return Err(Error::Io(IoError::new(
        ErrorKind::InvalidInput,
        format!(
          "Allocator with page size {} cannot flush to device with sector size {}",
          config.page_size, sector_size
        ),
      )));
    }
    Ok(())
  }

  /// 校验日志块分配器的页面配置与底层设备扇区大小的兼容性
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:VerifyCompatibleSectorSize
  #[inline]

  pub fn new(config: HybridLogConfig, device: Arc<D>, epoch: Arc<LightEpoch>) -> Result<Self> {
    Self::verify_compatible_sector_size(&config, &*device)?;
    let initial_address = config.initial_address;
    let addresses = Arc::new(AddressManager::new(initial_address));
    let buffer = CircularPageBuffer::new(&config)?;

    // 初始化并清零初始页槽位
    let initial_page = config.page_id(initial_address);
    buffer.clear_page(initial_page);

    info!(
      "初始化 HybridLog: page_size={}, num_pages={}, mutable_fraction={}, initial_address={initial_address:#x}",
      config.page_size, config.num_pages, config.mutable_fraction
    );

    Ok(Self::assemble(config, device, epoch, addresses, buffer))
  }

  /// 基于持久化快照恢复已存在的 HybridLog 状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:AsyncReadPagesForRecovery）
  ///
  /// # 崩溃一致性契约（恢复可见前缀）
  /// - 调用方必须在构造 `snapshot` 前完成 `flush_all` + `sync`，保证 `flushed_until`
  ///   及以下的数据已持久化；
  /// - 恢复时 `[flushed_until, tail)` 视为未获持久化承诺的未定义区域（可能为崩溃残留的
  ///   撕裂字节或陈旧数据），一律强制清零：扫描遇零头按 Pad 跳过、读取返回
  ///   [Error::PadRecord]，新追加从 `tail` 起点无缝续写覆盖该区域。
  ///
  /// # SEALED 位约定（对照 wrecord SEALED_BIT / wedb_reviv FreeRecord）
  /// SEALED_BIT 是纯易失标记（复活池槽位锁定），任何持久化路径都不会置位；
  /// 恢复端无需显式 `set_sealed(false)`——槽位复用唯一入口
  /// [Self::revivify_record_at] 整头覆写天然解除密封，读取路径亦不依赖该位。

  /// 获取底层设备的物理扇区大小
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetDeviceSectorSize
  #[inline]

  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetMainLogSegmentSize

  pub fn get_main_log_segment_size(&self) -> u64 {
    self.device.segment_size().unwrap_or(u64::MAX)
  }

  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetObjectLogSegmentSize
  #[inline]
  pub fn get_object_log_segment_size(&self) -> u64 {
    // wedb does not have separate object log segment size, defaults to main log segment size
    self.get_main_log_segment_size()
  }

  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetOffsetOnSegment
  #[inline]
  pub fn get_offset_on_segment(&self, address: u64) -> u64 {
    if let Some(segment_size) = self.device.segment_size() {
      address % segment_size
    } else {
      address
    }
  }

  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetStartLogicalAddressOfSegment
  #[inline]
  pub fn get_start_logical_address_of_segment(&self, address: u64) -> u64 {
    if let Some(segment_size) = self.device.segment_size() {
      address - (address % segment_size)
    } else {
      0
    }
  }

  pub fn get_device_sector_size(&self) -> usize {
    self.device.sector_size()
  }

  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:IsAllocated
  #[inline]
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:IsAllocated
  pub fn is_allocated(&self, _page_id: u64) -> bool {
    // Rust HybridLog relies on buffer capacity
    true
  }

  pub async fn recover(
    config: HybridLogConfig,
    device: Arc<D>,
    epoch: Arc<LightEpoch>,
    snapshot: AddressSnapshot,
  ) -> Result<Self> {
    if !snapshot.validate() {
      let mut ibuf = itoa::Buffer::new();
      let mut msg = String::from("恢复快照违反单调不变式: ");
      for (name, v) in [
        ("begin", snapshot.begin),
        ("safe_head", snapshot.safe_head),
        ("head", snapshot.head),
        ("safe_ro", snapshot.safe_read_only),
        ("ro", snapshot.read_only),
        ("tail", snapshot.tail),
        ("flushed_until", snapshot.flushed_until),
      ] {
        msg.push_str(name);
        msg.push('=');
        msg.push_str(ibuf.format(v));
        msg.push_str(", ");
      }
      msg.pop();
      msg.pop();
      return Err(Error::InvalidState(msg));
    }

    let page_size = config.page_size;
    let tail_page = config.page_id(snapshot.tail);

    // 环形缓冲区容量守卫：head 与 tail 的页间隔不得超过环形页数
    //（Tsavorite 窗口不变式：追加在进入页 N 前必已驱逐页 N-num_pages）
    if snapshot.tail > snapshot.head
      && tail_page >= config.page_id(snapshot.head) + config.num_pages as u64
    {
      let mut ibuf = itoa::Buffer::new();
      let mut msg = String::from("恢复快照驻留窗口超过环形页数: head_page=");
      msg.push_str(ibuf.format(config.page_id(snapshot.head)));
      msg.push_str(", tail_page=");
      msg.push_str(ibuf.format(tail_page));
      msg.push_str(", num_pages=");
      msg.push_str(ibuf.format(config.num_pages));
      return Err(Error::InvalidState(msg));
    }

    let buffer = CircularPageBuffer::new(&config)?;
    let addresses = Arc::new(AddressManager::with_snapshot(snapshot));

    if snapshot.tail > snapshot.head {
      let head_page = config.page_id(snapshot.head);
      let first_start = config.page_start_address(head_page);
      let span = (config.page_start_address(tail_page.saturating_add(1)) - first_start) as usize;

      // 段级批量预热：单次 I/O 读取 [head_page 起点, tail_page 末尾) 整段连续区间，
      // 消除逐页串行 I/O 往返（对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadPagesForRecovery 的连续页读合并）
      let span_buf = match device.read_range(first_start, span).await {
        Ok(buf) => Some(buf),
        Err(e) => {
          debug!("恢复阶段整段批量预热失败，退回逐页加载: {e}");
          None
        }
      };

      for p in head_page..=tail_page {
        let page_start = config.page_start_address(p);
        // 持久化承诺之外的页不预热：整页清零，恢复视图严格限定为已落盘前缀
        if page_start >= snapshot.flushed_until {
          buffer.clear_page(p);
          continue;
        }
        match span_buf.as_ref() {
          Some(all) => {
            let base = (page_start - first_start) as usize;
            let end = (base + page_size).min(all.len());
            if base < all.len() {
              buffer.load_page(p, &all[base..end]);
            } else {
              buffer.clear_page(p);
            }
          }
          None => match device.read_range(page_start, page_size).await {
            Ok(buf) => buffer.load_page(p, &buf),
            Err(e) => {
              debug!("恢复阶段预热加载逻辑页 {p} 失败（空设备或短文件，按空页处理）: {e}");
              buffer.clear_page(p);
              continue;
            }
          },
        }
        // 崩溃一致性前缀清洗：该页内 flushed_until 之后的未定义区域强制清零
        //（flushed_until <= tail，故对尾页而言 tail_offset 之后的区域亦被一并覆盖）
        let scrub = snapshot
          .flushed_until
          .saturating_sub(page_start)
          .min(page_size as u64) as usize;
        if scrub < page_size {
          buffer.clear_page_from_offset(p, scrub);
        }
      }
    } else {
      // head == tail：无驻留数据，仅需初始化尾页槽位供追加起步
      buffer.clear_page(tail_page);
    }

    info!(
      "从快照恢复 HybridLog: page_size={}, tail={:#x}, head={:#x}, flushed_until={:#x}",
      config.page_size, snapshot.tail, snapshot.head, snapshot.flushed_until
    );

    Ok(Self::assemble(config, device, epoch, addresses, buffer))
  }

  /// 三区内存驻留探测内核：解析给定地址的记录零拷贝借用形态
  ///
  /// - LightEpoch 纪元保护下，可变区与只读区统一优先走 `try_read_page_unlocked`
  ///   纯裸指针无锁直读，彻底消除页级 RwLock 原子计数器开销（C# 在 LightEpoch
  ///   保护下对可变区同样是裸读）；
  /// - 撕裂安全论证（与 C# Tsavorite 语义严格等价）：
  ///   1. 页生命周期：调用方必须处于 `LightEpoch` 保护内（已审计全部调用点：store session 的
  ///      `try_upsert_raw_sync`/`try_delete_raw_sync`/`try_read_*` 家族与 redis 扫描均持
  ///      `participant.enter()` 守卫，批处理走 `enter_batch` 持有守卫）。页槽位回收的前置条件是
  ///      `safe_head` 越过旧页，而 `safe_head` 仅能经 epoch drain 动作推进——本线程持守卫期间
  ///      drain 无法完成，故读取期间页内存绝不会被清空复用，裸指针无悬垂风险。
  ///   2. 撕裂语义：可变区写入方仅做同物理槽位内的原位覆写（`try_update_in_place` 家族：先写
  ///      value 字节、后单次刷回 16 字节记录头，`val_len + filler_bytes` 恒定，记录结构不变），
  ///      并发读者至多观察到更新前或更新后的值，解析边界恒在页内；该风险与 C# 一致。
  /// - 无锁直读未命中（槽位标定双重校验失败的瞬态窗口）：回退页读锁保护，
  ///   防止与原位更新并发撕裂（保守兜底路径，热路径不触发）；
  /// - 返回 `Ok(None)` 表示未驻留内存（磁盘区、未加载页或已滑出内存窗口）。
  ///
  /// # Safety（调用方契约）
  /// Raw 形态要求调用线程处于 `LightEpoch` 保护下（如 `EpochGuard` /
  /// `Participant`）；探测与使用期间 `head` 不得越过 `addr`，否则页可能被
  /// 并发驱逐清空（页槽位 `page_ids` 双重校验已消除绝大多数窗口）。
  pub(crate) fn probe_resident(&self, addr: u64) -> Result<Option<PageBytes<'_>>> {
    let head = self.addresses.head();
    if addr < head || addr >= self.addresses.tail() {
      return Ok(None);
    }

    let page_id = self.config.page_id(addr);
    // 纪元保护下统一无锁直读（可变区 + 只读区，撕裂安全论证见方法文档）
    if let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && addr >= self.addresses.head()
    {
      return Ok(Some(PageBytes::Raw(page_slice)));
    }

    // 无锁直读未命中：先取页读锁，再在锁内校验页号与 head——「先校验后加锁」存在
    // TOCTOU：校验通过到加锁之间页可被换页驱逐复用（page_ids 翻转为新页号），
    // 锁内读到的将是新页数据被按旧偏移误解析。页清空/标定均持写锁，读锁持有期间
    // 页号与数据稳定，锁内双重校验通过即保证整段读取期间槽位承载的恒为目标页
    let guard = self.buffer.read_page(page_id);
    if addr >= self.addresses.head() && self.buffer.is_page_loaded(page_id) {
      return Ok(Some(PageBytes::Locked(guard)));
    }
    Ok(None)
  }

  /// 在 Epoch 保护下解析指定逻辑地址的记录（供裸指针物理寻址，严格对标 C# GetPhysicalAddress）
  ///
  /// # Safety
  /// 调用方必须确保处于 `LightEpoch` 保护下且该逻辑地址驻留在内存中（`addr >= head && addr < tail`）。
  #[inline(always)]
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetPage
  pub unsafe fn get_physical_address(&self, addr: u64) -> *const u8 {
    unsafe { self.buffer.get_physical_address(addr) }
  }

  /// 校验追加记录尺寸与 48 位前驱地址有效性（append 与复活写入共用）
  #[inline]
  pub(crate) fn validate_append_args(&self, p: &RecParams<'_>) -> Result<usize> {
    if p.prev_addr & !ADDRESS_MASK != 0 {
      return Err(Error::InvalidAddress(p.prev_addr));
    }
    let rec_size = match checked_record_size(p.key.len(), p.val.len()) {
      Some(s) => s,
      None => {
        return Err(Error::RecordTooLarge {
          size: usize::MAX,
          page_size: self.config.page_size,
        });
      }
    };
    if rec_size > self.config.page_size {
      return Err(Error::RecordTooLarge {
        size: rec_size,
        page_size: self.config.page_size,
      });
    }
    Ok(rec_size)
  }

  /// 将已编码记录写入环形页槽位的独占物理切片
  ///
  /// # Safety
  /// 调用方必须已通过 CAS 或换页锁独占 `[offset, offset + rec_size)` 物理空间，
  /// 且该页槽位已完成置零初始化；`offset + rec_size <= page_size`。
  #[inline]
  pub(crate) unsafe fn encode_at(
    &self,
    page_id: u64,
    offset: usize,
    rec_size: usize,
    p: &RecParams<'_>,
  ) -> Result<()> {
    // SAFETY: 前置契约由调用方保证（CAS/换页锁独占切片 + 槽位已初始化）
    unsafe {
      let slot = self.buffer.page_idx(page_id);
      let page_ptr = self.buffer.raw_page_ptr_mut(slot);
      let dest = from_raw_parts_mut(page_ptr.add(offset), rec_size);
      encode_to_slice(dest, p.prev_addr, p.key, p.val, p.is_tombstone)?;
    }
    Ok(())
  }

  /// 写入换页 Pad 填充标记（对标 Tsavorite 页尾 Invalid 记录：保证并发读者绝不观察半截残页）
  ///
  /// - 剩余空间 >= 记录头：写入 PAD_KEY_LEN 标记头，逻辑尺寸恰好覆盖至页尾；
  /// - 剩余空间不足一个头：填充 0xFF 残片（扫描器按子头残片跳页处理）。
  ///
  /// # Safety
  /// 调用方必须持有 `page_turn_lock`（或等效互斥），独占 `[offset, page_size)` 区间。
  pub(crate) unsafe fn write_pad_tail(&self, page_id: u64, offset: usize, remaining: usize) {
    // SAFETY: 前置契约由调用方保证（page_turn_lock 独占页尾区间）
    unsafe {
      let slot = self.buffer.page_idx(page_id);
      let page_ptr = self.buffer.raw_page_ptr_mut(slot);
      let dest = from_raw_parts_mut(page_ptr.add(offset), remaining);
      if remaining >= HEADER_SIZE {
        let pad_header = RecordHeader::pad(remaining);
        dest[..HEADER_SIZE].copy_from_slice(&pad_header.to_bytes());
      } else {
        dest.fill(0xFF);
      }
    }
  }
}
