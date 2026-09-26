#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;
use std::{
  io::{Error as IoError, ErrorKind},
  ops::Deref,
  slice::from_raw_parts_mut,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use event_listener::Event;
use log::{debug, info};
#[cfg(debug_assertions)]
use parking_lot::Condvar;
use parking_lot::{Mutex, RwLockReadGuard};
use wbase::{addr::ADDRESS_MASK, pool::AlignedBuf};
use wdev::{Device, Error as WdevError};
use wepoch::LightEpoch;
use wrecord::{
  HEADER_SIZE, RecordHeader, RecordRef, ValSrc, encode_to_slice, publish_extent_header, record_size,
};

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

/// 测试注入挂起门内核（仅测试用，一处定义）：布防 → 单次挂起 → 放行三态
/// 生命周期与睡眠让核协议的公共实现，经 [EncodeStall] / [VersionReadStall]
/// 两个类型别名门面导出，挂点语义见各别名文档（同 wbftree `SCAN_FAIL_INJECT` 纪律）。
///
/// 生产路径开销：挂入点一条 `Relaxed` 载入恒假，无跨核争用、无锁、无分配；
/// 门随实例存活、逐实例隔离（并发测试互不劫持），布防仅由集成测试触发。
#[cfg(debug_assertions)]
pub struct StallGate {
  /// 布防标记（生产者挂起前消费即清，一次性）
  armed: AtomicBool,
  /// 已挂起标记（测试据此确认挂起窗口就位）
  parked: AtomicBool,
  /// 放行通道（挂起线程睡眠让核，绝不自旋占核）
  resume: (Mutex<bool>, Condvar),
}

#[cfg(debug_assertions)]
impl StallGate {
  /// 新建关闭态挂起门
  pub const fn new() -> Self {
    Self {
      armed: AtomicBool::new(false),
      parked: AtomicBool::new(false),
      resume: (Mutex::new(false), Condvar::new()),
    }
  }

  /// 布防：下一次挂入点挂起（幂等）
  pub fn arm(&self) {
    self.armed.store(true, Ordering::Relaxed);
  }

  /// 是否有生产者已挂起
  pub fn is_parked(&self) -> bool {
    self.parked.load(Ordering::Acquire)
  }

  /// 放行挂起中的生产者并复位门（无挂起者时仅复位）
  pub fn release(&self) {
    *self.resume.0.lock() = true;
    self.resume.1.notify_all();
  }

  /// 挂入点：未布防直返，布防则睡眠至放行
  pub(crate) fn park_if_armed(&self) {
    if !self.armed.swap(false, Ordering::AcqRel) {
      return;
    }
    self.parked.store(true, Ordering::Release);
    let (lock, cv) = &self.resume;
    let mut guard = lock.lock();
    while !*guard {
      cv.wait(&mut guard);
    }
    *guard = false;
    self.parked.store(false, Ordering::Relaxed);
  }
}

#[cfg(debug_assertions)]
impl Default for StallGate {
  fn default() -> Self {
    Self::new()
  }
}

/// 在途编码窗口挂起门（仅测试用注入面，语义即 [`StallGate`] 内核）
///
/// 布防后，本实例下一次 `encode_at` 在**在途 extent 头发布之后、键值落笔
/// 之前**挂起，直至测试 [`StallGate::release`] 放行——确定性复现「生产者已 CAS
/// 独占槽位、整段编码期被长时间抢占」的在途窗口。该窗口在真实并发下窄于
/// 扫描器自旋预算（`ZERO_HEADER_SPIN_BUDGET`），任何睡眠/忙等猜测都无法
/// 稳定命中，故须显式挂起点。挂点在 [`HybridLog::encode_at`] 首拍之后。
#[cfg(debug_assertions)]
pub type EncodeStall = StallGate;

/// 版本合字读点挂起门（仅测试用注入面，语义即 [`StallGate`] 内核）
///
/// 布防后，本实例下一次 `append` 在 **tail CAS 胜点之后、version_shift_word
/// 读点之前**挂起，直至测试 [`StallGate::release`] 放行——确定性复现「CAS 与相邻
/// 读点之间窗关」的纳秒域交错（x86 LOCK CMPXCHG 至后续 MOV 序）：挂起期间
/// 由测试线程执行 end_version_shift 关窗，放行后读点即取关闭态，产出
/// 「无位 + 版本域等于关窗保留值」的等值戳记录（恢复期重插臂与 AOF 等值
/// 重放两侧皆承接的双承形态）。[`EncodeStall`] 挂点在 encode_at 之后不达本
/// 交错，故另立此门面。挂入点在 [`HybridLog::append`] 两处 CAS 胜点之后
/// （version_shift_word 读点之前）。
#[cfg(debug_assertions)]
pub type VersionReadStall = StallGate;

/// 磁盘冷读首段 I/O 探测长度。C# Garnet DefaultInitialIORecordSize = 128（IStreamBuffer.cs），
/// 此处按扇区对齐设备模型取 4096，为刻意差异
const DISK_READ_PROBE_LEN: usize = 4096;

/// Microsoft Garnet Tsavorite 架构风格的 HybridLog 混合日志分配器
///
/// 具备 64 位连续逻辑地址空间、环形页缓冲池与
/// Mutable (可变) / ReadOnly (只读) / OnDisk (磁盘) 三区动态滑动状态机。
///
/// 与 C# AllocatorBase.cs 的刻意差异（compio 线程每核模型）：
/// - 页级刷盘重设计为调用方驱动的单线程批量刷盘（[Self::flush_pages_range]），
///   其读侧上界由该内核自保（写入前先封印 SafeReadOnlyAddress 并等纪元排空，
///   对标 C# 由 `OnPagesMarkedReadOnly` 纪元动作发起 `AsyncFlushPagesForReadOnly`），
///   硬件持久化屏障由调用方 [Self::sync] 承担，崩溃一致性边界为
///   `flushed_until`（连续已落盘前缀）+ sync；
/// - 崩溃残留的撕裂记录在 [Self::recover] 中按 `flushed_until` 前缀规则强制清零，
///   恢复可见状态严格限定为已持久化前缀；
/// - 无跨页 oversized 记录路径：单页容量即单记录硬上限，本版本 C# 同样没有
///   OversizedAllocation 分支，两侧语义一致（校验点见 `validate_append_args`）。
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
  /// 待刷盘区间贪心合并队列（对标 Garnet PendingFlushList.cs）
  pub pending_flush: PendingFlushList,
  /// 页面刷盘完成事件（支持多协程等待落盘被动唤醒，对标 Garnet flushEvent）
  pub flush_event: event_listener::Event,
  /// 版本推进窗口与存储版本的合字（单原子字，对标 C# StateTransitions.cs
  /// SystemState 的 version+phase 单 8 字节原子 Word——Phase 占高位、Version
  /// 占低位，版本切换经 _systemState 单原子推进）：
  /// 位 63 = [`VERSION_SHIFT_OPEN_BIT`] 窗口开哨兵；低 63 位 = 当前存储版本
  /// （[`VERSION_MASK`] 掩取）。窗口开启即版本推进（[`HybridLog::begin_version_shift`]
  /// 单次 store 发布），随窗口落笔的记录携带 IN_NEW_VERSION_BIT，恢复内核据此
  /// 在模糊区回滚（undoNextVersion），原位更新面据此冻结旧纪元记录（对标 C#
  /// Helpers.cs:IsFrozen）。
  /// 单原子化的一致性契约：追加者「tail CAS → 读本字取（窗口位, 版本）」与
  /// 开窗者「load tail → 单次 store 本字」之间，写者对窗口位与版本的观察恒
  /// 同源同点——读到开启态的记录必携新版本戳（AOF 条目与记录头位同取自该次
  /// 读，经 hlog 分配成功点单读向上传导），恢复期「回滚剔除 + AOF 重放」
  /// 恰一次；读到关闭态的记录必落快照收录/重插面，AOF 按旧版本跳过，两态
  /// 各自闭环，结构上不存在「带位旧版本戳」或「无位新版本戳」的撕裂记录。
  ///
  /// Arc 形态供宿主装配期 clone（[`Self::version_shift_atomic`]）：版本号、
  /// AOF 版本戳、复制面版本号与恢复基线共享同一原子本体，绝无第二套版本源。
  version_shift: Arc<AtomicU64>,
  /// 模糊区版本推进窗口下界地址（对标 C# _hybridLogCheckpoint.info.startLogicalAddress）
  shift_floor: AtomicU64,
  /// 在途编码窗口挂起门（仅测试注入面，生产恒关闭，见 [EncodeStall]）
  #[cfg(debug_assertions)]
  pub encode_stall: EncodeStall,
  /// 版本合字读点挂起门（仅测试注入面，生产恒关闭，见 [VersionReadStall]）
  #[cfg(debug_assertions)]
  pub version_read_stall: VersionReadStall,
}

/// 版本推进窗口开启哨兵位（合字位 63，48 位逻辑地址域之外，天然不与任何
/// 版本值冲突）：置位即窗口开启，同字低 63 位承载当前存储版本
pub const VERSION_SHIFT_OPEN_BIT: u64 = 1 << 63;

/// 合字版本域掩码（低 63 位；[`HybridLog::version_shift`] 的版本取值入口）
pub const VERSION_MASK: u64 = !VERSION_SHIFT_OPEN_BIT;

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
///
/// 头部两字经 [RecordHeader::from_ptr_atomic] 以 8 字节对齐 Acquire 原子载入（对标 C#
/// RecordDataHeader 单原子字读协议）：可变区原位松弛更新以单次原子 store 发布 RDH 字
/// （filler + key_len + val_len 同字），RecordInfo 字标志位（墓碑/密封）以原子 RMW 维护，
/// 无锁直读者只会观察到前态或后态，绝无半字混合态；磁盘/页锁保护路径的字节已定稿，
/// 原子载入与普通载入等价（对齐 Acquire 载入在主流架构为零开销指令）。
#[inline]
pub(crate) fn parse_record_from_slice(
  page_slice: &[u8],
  offset: usize,
  addr: u64,
  page_size: usize,
) -> Result<RecordRef<'_>> {
  let max_len = page_slice.len().min(page_size);
  if offset
    .checked_add(HEADER_SIZE)
    .is_none_or(|end| end > max_len)
  {
    return Err(Error::PadRecord(addr));
  }

  // SAFETY: 记录 8 字节对齐不变式（RECORD_ALIGNMENT）保证页内记录头恒对齐，
  // offset + HEADER_SIZE <= max_len <= page_slice.len() 已校验
  let header = unsafe { RecordHeader::from_ptr_atomic(page_slice.as_ptr().add(offset)) };
  // 对齐 C# Tsavorite：Pad 标记或全零空洞记录均作为换页填充处理
  reject_pad(header, addr)?;

  let physical_size = header.physical_size();
  let phys_end = offset.saturating_add(physical_size);
  if phys_end > max_len {
    return Err(Error::RecordCorrupted {
      addr,
      detail: "记录完整内容超出页面容量边界".into(),
    });
  }

  // 值数据精确结束于未对齐 KV 区段（隐式对齐填充与显式松弛填充不外露）
  let kv_size = header.kv_size();
  let end = offset + kv_size;
  let key_start = offset + HEADER_SIZE;
  let key_end = key_start + header.key_len() as usize;
  // SAFETY: end <= phys_end <= max_len <= page_slice.len() 且 key_end <= end 完全满足切片边界
  let (key, value) = unsafe {
    (
      page_slice.get_unchecked(key_start..key_end),
      page_slice.get_unchecked(key_end..end),
    )
  };

  Ok(RecordRef { header, key, value })
}

/// 复活槽位覆写参数包（消除多参透传，避免参数传错）
///
/// 值源泛型 [`ValSrc`]：连续切片（默认 `[u8]`）或分段直写源（对象信封单次
/// 成形，消除中间整值缓冲——对标 C# 序列化器直写记录 value span）
///
/// 字段全为共享引用与标量，恒可 Copy（Clone/Copy 手动 impl，免除 derive 给
/// 泛型 `V` 强加的 bound——分段值源无 Copy/Clone 语义）
#[derive(Debug)]
pub struct RevivifyArgs<'a, V: ValSrc + ?Sized = [u8]> {
  pub addr: u64,
  pub slot_size: usize,
  pub key: &'a [u8],
  pub val: &'a V,
  pub prev_addr: u64,
  pub is_tombstone: bool,
  pub in_new_version: bool,
}

impl<V: ValSrc + ?Sized> Clone for RevivifyArgs<'_, V> {
  #[inline(always)]
  fn clone(&self) -> Self {
    *self
  }
}

impl<V: ValSrc + ?Sized> Copy for RevivifyArgs<'_, V> {}

/// 待写入记录的键值参数包（消除多参透传；值源泛型 [`ValSrc`]，默认连续切片）
pub(crate) struct RecParams<'a, V: ValSrc + ?Sized = [u8]> {
  /// 48 位前驱版本逻辑地址
  pub prev_addr: u64,
  pub key: &'a [u8],
  pub val: &'a V,
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
      pending_flush: PendingFlushList::new(),
      flush_event: Event::new(),
      version_shift: Arc::new(AtomicU64::new(0)),
      shift_floor: AtomicU64::new(0),
      #[cfg(debug_assertions)]
      encode_stall: EncodeStall::new(),
      #[cfg(debug_assertions)]
      version_read_stall: VersionReadStall::new(),
    }
  }

  /// 开启版本推进窗口并推进存储版本（单原子发布，返回模糊区地板 = 窗口开启
  /// 瞬间的 tail 逻辑地址）
  ///
  /// 对标 C# 检查点状态机进入版本推进相位的两件事合一：`phase < Phase.REST` 起
  /// 前台新记录携带 InNewVersion（ExecutionContext.cs:162），且版本推进经
  /// _systemState 单原子字发布（StateTransitions.cs 的 Version 占低位）。
  /// 本口一次 store 同时完成「开窗 + 版本推进」，结构性消除两独立原子间的
  /// 撕裂窗口（版本号与窗口位对写者恒同源同点，见
  /// [`HybridLog::version_shift`] 字段注释的一致性契约）。
  /// 宿主时序契约：本口返回值即本轮检查点的 `index_start_logical_address`
  /// （版本推进瞬间的 tail），恢复内核 undoNextVersion 回滚与模糊区重插的
  /// 同一下界；AOF 盖版本戳由写入口同字读出（[`Self::version_shift_word`]），
  /// 与本口的全序由原子字修改序承接。
  ///
  /// 重复开窗为整值覆盖（前一轮检查点失败未 [`Self::end_version_shift`] 收口的
  /// 兜底重开）：陈旧窗口的遗留标记位无消费方（回滚判定要求记录携带本轮位），
  /// 覆盖即收敛，无需第二套清位路径。
  pub fn begin_version_shift(&self, new_version: u64) -> u64 {
    let floor = self.addresses.tail_address.load(Ordering::SeqCst);
    self.shift_floor.store(floor, Ordering::SeqCst);
    self.version_shift.store(
      VERSION_SHIFT_OPEN_BIT | (new_version & VERSION_MASK),
      Ordering::SeqCst,
    );
    floor
  }

  /// 关闭版本推进窗口（检查点快照段返回后无条件收口；单 RMW 清哨兵位，
  /// 版本域原样保留——窗口关闭后存储版本仍为推进后的新值）
  ///
  /// 对标 C# 状态机回到 REST 相位后 InNewVersion 恒假；窗口关闭后新追加不再
  /// 携带纪元位，原位更新冻结同步解除。
  pub fn end_version_shift(&self) {
    self.shift_floor.store(0, Ordering::SeqCst);
    self
      .version_shift
      .fetch_and(!VERSION_SHIFT_OPEN_BIT, Ordering::SeqCst);
  }

  /// 合字快照（窗口位 + 版本域单次原子读）：一次读同取「本写是否携带纪元位」
  /// 与「AOF 条目版本戳」，杜绝两次独立读跨开窗点撕裂。读点单（读点下移后）：
  /// 追加域在本口之上的分配成功点（[`Self::append`] 两处 CAS 胜点、池取复活口），
  /// 原位生效臂与事件分发在生效点/分发点（对齐 wkv `emit_event` 口径）
  #[inline(always)]
  pub fn version_shift_word(&self) -> u64 {
    self.version_shift.load(Ordering::SeqCst)
  }

  /// 合字原子的共享引用（Arc 无环捕获口）：宿主装配期 clone 持有，使
  /// 「存储版本」与 hlog 窗口字本体同源（AOF 版本戳/复制面版本号/恢复基线
  /// 全部读写同一原子，绝无第二套版本源；对标 C# StoreWrapper.store
  /// .CurrentVersion 的单一真源形态）
  #[inline(always)]
  pub fn version_shift_atomic(&self) -> &Arc<AtomicU64> {
    &self.version_shift
  }

  /// 版本推进窗口是否开启（供宿主写侧各单点作窗口期保守裁剪谓词）
  ///
  /// 对标 C# 检查点版本推进相位对复活/脱钩面的抑制（RevivificationManager 各
  /// CanElide/CanRevivify 判据复合 `!Ctx.IsInNewVersion` 侧条件）：窗口开启期间
  /// 脱钩（seal+归池）老版本记录或原位复活快照已收录的槽位，会让恢复内核的
  /// undoNextVersion 回滚失去可回退的老版本锚点（前驱被密封成复活池废料），
  /// 故两臂在窗口期一律禁用，落回「追加携带纪元位 + 链位接管」形态。
  #[inline(always)]
  pub fn is_version_shift_open(&self) -> bool {
    self.version_shift.load(Ordering::SeqCst) & VERSION_SHIFT_OPEN_BIT != 0
  }

  /// 当前模糊区地板读数（窗口关闭期为 0；开窗瞬间起即为本轮检查点的
  /// `index_start_logical_address`，与 [`Self::begin_version_shift`] 返回值同源）
  ///
  /// 消费方仅限取槽下界抬升一类「读数参与下界计算」的宿主单点；判据类消费
  /// 一律走 [`Self::is_frozen_by_version_shift`] / [`Self::is_version_shift_open`]
  /// 复合谓词，不得各自拼装两点读（见 [`Self::version_shift`] 一致性契约）。
  #[inline(always)]
  pub fn version_shift_floor(&self) -> u64 {
    self.shift_floor.load(Ordering::SeqCst)
  }

  /// 版本推进窗口判定：冻结可变区原位修改（1:1 对标 C# Helpers.cs:IsFrozen）
  ///
  /// `Ctx.IsInV1 && (logicalAddress <= startLogicalAddress || !srcRecordInfo.IsInNewVersion)`
  /// 的 rust 形态：窗口开启期间：
  /// - 若记录在快照范围（`addr <= startLogicalAddress`），禁止原位修改（必须冻结降级为追加）；
  /// - 若记录在模糊区且为旧版本（`!record_in_new_version`），禁止原位修改；
  ///
  /// 仅当记录处于本轮新纪元（`addr > startLogicalAddress && record_in_new_version`）时才允许原位修改。
  #[inline(always)]
  pub(crate) fn is_frozen_by_version_shift(&self, addr: u64, record_in_new_version: bool) -> bool {
    if !self.is_version_shift_open() {
      return false;
    }
    let floor = self.shift_floor.load(Ordering::SeqCst);
    addr <= floor || !record_in_new_version
  }

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

  /// 创建新的 HybridLog 实例
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:Initialize
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
  /// # 故障快停契约（对标 C#
  /// test.hlog/LogCommitFailureTests.cs:FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog
  /// Phase 2a，`TolerateDeviceFailure = false` 默认臂）
  /// - 恢复期设备页读故障（非文件末端短读）必须以 [Error::Device] 快停，绝不
  ///   以「整页清零的空日志」静默伪装恢复成功（毒化日志）；
  /// - 文件末端短读（[wdev::Error::UnexpectedEof]）为合法未写区域，按空页处理；
  /// - 扇区尺寸不兼容的设备在构造期显式报错（对标
  ///   test.recovery/RecoveryTests.cs::RecoveryTestFailOnSectorSize 语义）。
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
  pub async fn recover(
    config: HybridLogConfig,
    device: Arc<D>,
    epoch: Arc<LightEpoch>,
    snapshot: AddressSnapshot,
  ) -> Result<Self> {
    // 扇区兼容性校验（与 [Self::new] 同源，对标
    // libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:VerifyCompatibleSectorSize）：
    // 换扇区尺寸设备上的恢复必须在构造期显式报错，绝不以错误几何静默继续
    Self::verify_compatible_sector_size(&config, &*device)?;
    if !snapshot.validate() {
      return Err(Error::InvalidState(format!(
        "恢复快照违反单调不变式: begin={}, safe_head={}, head={}, safe_ro={}, ro={}, tail={}, flushed_until={}",
        snapshot.begin,
        snapshot.safe_head,
        snapshot.head,
        snapshot.safe_read_only,
        snapshot.read_only,
        snapshot.tail,
        snapshot.flushed_until
      )));
    }

    let page_size = config.page_size;
    let tail_page = config.page_id(snapshot.tail);

    // 环形缓冲区容量守卫：head 与 tail 的页间隔不得超过环形页数
    //（Tsavorite 窗口不变式：追加在进入页 N 前必已驱逐页 N-num_pages）
    if snapshot.tail > snapshot.head
      && tail_page >= config.page_id(snapshot.head) + config.num_pages as u64
    {
      return Err(Error::InvalidState(format!(
        "恢复快照驻留窗口超过环形页数: head_page={}, tail_page={}, num_pages={}",
        config.page_id(snapshot.head),
        tail_page,
        config.num_pages
      )));
    }

    let buffer = CircularPageBuffer::new(&config)?;
    let addresses = Arc::new(AddressManager::with_snapshot(snapshot));

    if snapshot.tail > snapshot.head {
      let head_page = config.page_id(snapshot.head);
      let first_start = config.page_start_address(head_page);

      // 段级批量预热：单次 I/O 读取 [head_page 起点, flushed_end_page 末尾) 已落盘连续区间，
      // 消除逐页串行 I/O 往返（对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadPagesForRecovery 的连续页读合并）
      // 严禁请求超出 flushed_until 的未写页面，避免读取文件末端越界触发短读或 I/O 错误退化为逐页加载
      let span_buf = if snapshot.flushed_until > first_start {
        let flushed_end_page = config
          .page_id(snapshot.flushed_until.saturating_sub(1))
          .min(tail_page);
        let read_end = config.page_start_address(flushed_end_page.saturating_add(1));
        let span = (read_end - first_start) as usize;
        match device.read_range(first_start, span).await {
          Ok(buf) => Some(buf),
          Err(e) => {
            debug!("恢复阶段整段批量预热失败，退回逐页加载: {e}");
            None
          }
        }
      } else {
        None
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
            Err(WdevError::UnexpectedEof { .. }) => {
              // 空设备或短文件（文件末端未写区域）：合法空页，按空页处理
              debug!("恢复阶段逻辑页 {p} 越出文件末端，按空页处理");
              buffer.clear_page(p);
              continue;
            }
            Err(e) => {
              // 设备读故障必须快停（对标 C#
              // LogCommitFailureTests:FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog
              // Phase 2a：恢复期设备故障下构造必须抛错，绝不静默交出
              // 被清零毒化的空日志伪装恢复成功）
              return Err(Error::Device(e));
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
  /// - 整个内存驻留区（`addr < read_only`，含不可变区与模糊区）在 LightEpoch 纪元
  ///   保护下走 `try_read_page_unlocked` 纯裸指针无锁直读，消除页级 RwLock 原子计数器
  ///   开销（对标 C# InternalRead.cs:114-124 可变区甚至模糊区 >= SafeReadOnlyAddress 的
  ///   无锁 CreateLogRecord 直读）；仅真可变区 `[read_only, tail)` 走页读锁与原位更新
  ///   写锁互斥；
  /// - 撕裂安全论证（对标 C# RDH 单原子字发布协议）：
  ///   1. 页生命周期：调用方必须处于 `LightEpoch` 保护内（已审计全部调用点：store session 的
  ///      `try_upsert_raw_sync`/`try_delete_raw_sync`/`try_read_*` 家族与 redis 扫描均持
  ///      `participant.enter()` 守卫，批处理走 `enter_batch` 持有守卫）。页槽位回收的前置条件是
  ///      `safe_head` 越过旧页，而 `safe_head` 仅能经 epoch drain 动作推进——本线程持守卫期间
  ///      drain 无法完成，故读取期间页内存绝不会被清空复用，裸指针无悬垂风险。
  ///   2. 布局撕裂安全（对标 C# RecordDataHeader 单 8 字节原子字发布协议）：原位松弛更新
  ///      （`try_update_in_place` 家族）同步改写 `val_len` 与 `filler`——本实现 wrecord 头
  ///      将 key_len + val_len + filler 收进 RDH 单个 8 字节原子字（记录 8 字节对齐不变式
  ///      保证其对齐），单次对齐原子写发布完整新布局（"the derived recordLength is preserved
  ///      for any concurrent scanner"）；无锁读者的头解析走 `RecordHeader::from_ptr_atomic`
  ///      双字 Acquire 载入，与发布 store 配对，只会观察到前态或后态，推出的物理尺寸恒一致。
  ///   3. 无锁直读门槛为 read_only（覆盖模糊区 [safe_read_only, read_only)，对照 C#
  ///      InternalRead.cs:114-124 显式把模糊区并入无锁直读范围）：原位更新者仅在页写锁内
  ///      复验「可变区」（`with_mutable_record` / `revivify_record_at` 的锁内 double-check）
  ///      后落笔，read_only 推进（换页 `maybe_advance_read_only` / `shift_head_address`
  ///      连带强推 / checkpoint 的 `shift_read_only_to_tail` / 刷盘内核的
  ///      `seal_read_only_and_drain`）不取页锁——复验通过到 RDH
  ///      单字刷回之间记录可被并发封入模糊区，但 RDH 单原子字发布使布局撕裂不可能发生；
  ///      RecordInfo 字标志位（墓碑/密封）以原子 RMW 维护，同样撕裂安全。
  ///   4. 值字节可见性窗口：模糊区内仅存在「复验于封区之前」的在途原位写的值字节覆写
  ///      （布局已原子安全），无锁读者可能观察到值字节的中间态——与 C# Tsavorite RMW
  ///      原位更新的可见性语义一致（值内容原子性由上层应用语义承担）；真可变区读者
  ///      持页读锁与原位更新写锁互斥，绝无值字节竞态。
  /// - 无锁直读未命中（槽位标定双重校验失败的瞬态窗口）：回退页读锁保护；
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
    // 只读区（含模糊区）无锁直读：门槛 read_only 而非 safe_read_only——对标 C#
    // InternalRead.cs:114-124 可变区甚至模糊区的无锁 CreateLogRecord 直读。撕裂安全
    // 由 wrecord 头双原子字协议保证：RDH 单 8 字节原子字单次发布 key_len + val_len +
    // filler 完整布局（C# RecordDataHeader 同款豁免），RecordInfo 字标志位原子 RMW，
    // 无锁读者经 from_ptr_atomic Acquire 载入绝不观察到半字混合态（论证见方法文档第
    // 2/3 条）；LightEpoch 保证页内存不释放（延迟退役），页清空复用以 safe_head
    // 纪元排空为门槛
    if addr < self.addresses.read_only()
      && let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && addr >= self.addresses.head()
    {
      return Ok(Some(PageBytes::Raw(page_slice)));
    }

    // 无锁直读未命中（真可变区 / 槽位标定双重校验失败的瞬态窗口）：
    // 先取页读锁，再在锁内校验页号与 head——「先校验后加锁」存在 TOCTOU：
    // 校验通过到加锁之间页可被换页驱逐复用（page_ids 翻转为新页号），锁内读到的
    // 将是新页数据被按旧偏移误解析。页清空/标定均持写锁，读锁持有期间页号与
    // 数据稳定，锁内双重校验通过即保证整段读取期间槽位承载的恒为目标页
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
  ///
  /// 单记录硬上限边界：记录不可跨页，对齐尺寸（含头与填充）超过页容量即在
  /// 写侧拒为 [`Error::RecordTooLarge`]，页容量因此就是单条记录（含对象信封
  /// 整包内联，见 wkv KeyTag::ObjectEnvelope 与 config.rs 页钳制 [64KB,16MB]）
  /// 的容量上限；本版本 C# 无跨页 oversized 路径，对位
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:TryAllocate
  /// 的 "Entry does not fit on page" 硬抛，两侧行为一致。
  #[inline]
  pub(crate) fn validate_append_args<V: ValSrc + ?Sized>(
    &self,
    p: &RecParams<'_, V>,
  ) -> Result<usize> {
    if p.prev_addr & !ADDRESS_MASK != 0 {
      return Err(Error::InvalidAddress(p.prev_addr));
    }
    // 记录对齐逻辑尺寸（含隐式对齐填充，保证页内记录头 8 字节对齐）
    let rec_size = record_size(p.key.len(), p.val.val_len());
    if rec_size > self.config.page_size {
      return Err(Error::RecordTooLarge {
        size: rec_size,
        page_size: self.config.page_size,
      });
    }
    Ok(rec_size)
  }

  /// 探测记录（键 + 值）能否落入当前页容量（与 [`Self::validate_append_args`]
  /// 共用 record_size 单一公式，只读无副作用）
  ///
  /// 集合信封升阶容量门消费：页容量随内存预算收缩（wkv
  /// `from_memory_budget_with_keys` 按 budget/64 钳制 [64KB,16MB]），低于升阶
  /// 内存阈（wcol::TIERED_PROMOTE_BYTES = 4MB）的配置下，未达升阶阈的中间信封
  /// 会先撞页上限（写侧 [`Error::RecordTooLarge`]），调用方据此改走既有升阶臂
  ///（wbftree 树按成员逐条入表，无单记录上限）
  #[inline]
  pub fn record_fits(&self, key_len: usize, val_len: usize) -> bool {
    record_size(key_len, val_len) <= self.config.page_size
  }

  /// 将记录写入环形页槽位的独占物理切片（在途两阶段头发布协议）
  ///
  /// 落笔次序：先以 [wrecord::publish_extent_header] 单条对齐 Release store 发布只含槽位尺寸的
  /// Pad 形态最小头，再编码键值与完整头（RecordInfo 字原子 store → RDH 字原子 Release store）。
  /// 「已 CAS 预占、尚未编码」的在途窗口对扫描器恒为可按 `physical_size` 精确步度
  /// 的 Pad，游标原地越过本槽而非整页跳过，同页后续记录不再漏扫——对标 C# 新记录先写
  /// 扫描可见的关闭态头（RecordInfo.WriteInfo「Otherwise, Scan could return partial
  /// records」）+ 扫描器 SkipOnScan「跳记录不跳页」（SpanByteScanIterator.GetNext）
  /// 的同形语义；两 C# 符号均经 ignore 登记为单写发布整体取代，无 rust 对位体，
  /// 故本叙述不持符号锚，协议第一拍的单点实现见 [`wrecord::publish_extent_header`]。
  /// 残留全零窗口压缩至 tail CAS 胜出到本方法首条 store 之间，由扫描器
  /// `ZERO_HEADER_SPIN_BUDGET` 有界自旋兜底；extent 头本身无需额外屏障——槽位就绪与
  /// 置零已由 tail CAS 的 Release 发布，扫描器以 `tail_address` 的 Acquire 载入承接。
  ///
  /// `word` 为分配成功点（tail CAS / 池取 take 之后、本方法之前）单读的
  /// [`Self::version_shift_word`] 值：纪元位由本方法自 word 掩取（位 63），
  /// 与 AOF 版本戳（调用方自同一 word 掩取版本域）严格同源同点——杜绝位判定
  /// 与版本戳采样两点读跨开窗点撕裂（一致性契约见
  /// [`HybridLog::version_shift`]，读点下移见 [`HybridLog::append`] 方法文档）。
  ///
  /// # Safety
  /// 调用方必须已通过 CAS 或换页锁独占 `[offset, offset + rec_size)` 物理空间，
  /// 且该页槽位已完成置零初始化；`offset + rec_size <= page_size` 且 `offset` 8 字节对齐。
  #[inline]
  pub(crate) unsafe fn encode_at<V: ValSrc + ?Sized>(
    &self,
    page_id: u64,
    offset: usize,
    rec_size: usize,
    p: &RecParams<'_, V>,
    word: u64,
  ) -> Result<()> {
    // SAFETY: 前置契约由调用方保证（CAS/换页锁独占切片 + 槽位已初始化）
    unsafe {
      let slot = self.buffer.page_idx(page_id);
      let page_ptr = self.buffer.raw_page_ptr_mut(slot);
      // 第一拍：在途 extent 头单字发布（RecordInfo 字保持置零槽位的零值）
      publish_extent_header(page_ptr.add(offset), rec_size);
      // 测试注入面：此处在途窗口对扫描器恒为尺寸可解的 Pad（见 [EncodeStall]）
      #[cfg(debug_assertions)]
      self.encode_stall.park_if_armed();
      // 版本推进窗口纪元位（对标 C# WriteNewRecordInfo(sessionFunctions
      // .Ctx.InNewVersion)）：取自分配成功后单读、反映落笔时刻窗口态（见方法文档）
      let dest = from_raw_parts_mut(page_ptr.add(offset), rec_size);
      encode_to_slice(
        dest,
        p.prev_addr,
        p.key,
        p.val,
        p.is_tombstone,
        word & VERSION_SHIFT_OPEN_BIT != 0,
      )?;
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
