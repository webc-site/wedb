//! 物理键单点读取路径（对标 C# Garnet ClientSession 的 Read 快慢路径）

use std::mem::take;

use wbase::{addr::is_read_cache, backoff::Backoff, simd::fast_key_eq, time::now_ticks};
use wdev::Device;
use wepoch::EpochSuspendGuard;
use windex::{CandidateAddresses, Error as WindexError, HashBucketEntry};
use wrecord::record_size;
use wval::KeyTag;

use super::{MemDrive, ReadProbeResult};
use crate::{
  error::{Error, Result},
  read_cache::RcVisit,
  session::{INNER_LATCH_RETRY_BUDGET, StoreSession},
  ttl::TtlGate,
};

/// 内存直读内部结果（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead 单遍分类，附加磁盘候选透传）
///
/// 仅内核自用：`RETRY_LATER` 的刷新重试由 [`StoreSession::drive_mem_read`] 驱动环
/// 内部闭环，环外只见 [`MemDrive`] 终态
enum MemRead<R> {
  /// 内存阶段已闭环：`Some` 为命中值，`None` 为确认不存在（含墓碑，对应 `NOTFOUND`）
  Done(Option<R>),
  /// 记录位于磁盘区：携带按新版本优先降序排列的磁盘候选地址（对应 `RECORD_ON_DISK`）
  OnDisk(CandidateAddresses),
  /// 命中密封在途记录（对应 `RETRY_LATER`）：驱动环刷新纪元后整链重试
  Retry,
}

/// ReadCache 整链走查结果（[`StoreSession::find_in_read_cache`] 出口）
enum RcWalk<R> {
  /// 键匹配的未作废 RC 记录命中，已消费读闭包
  Found(R),
  /// 链走尽至首个主日志地址（或链尽 0），`curr` 已就位
  ChainEnd,
  /// 驱逐窗口/滑窗竞态不可判读：刷新纪元后回链头重探（对应 `RETRY_LATER`）
  Retry,
}

/// [`RcVisit`] → [`RcWalk`] 判据映射单点（raw 读侧唯一换算口）：C# 读缓存探测
/// 无第二套枚举——FindInReadCache 只回 bool，命中/续链/回链头重探直接汇入
/// InternalRead 的 OperationStatus 同源分类；rust 侧 [`RcVisit`]（RC 记录访问
/// 三态，read_cache 层产出）与 [`RcWalk`]（整链走查出口）域不同须分立，换算
/// 仅此一处，并与 [`ReadProbeResult`]（主日志记录探针四态，InternalRead.cs:105-131
/// 单遍分类）按判据对位：
/// - `Found` ↔ `ReadProbeResult::Found` ↔ SUCCESS（命中）
/// - `Next(prev)` ↔ `ReadProbeResult::Miss(prev)` ↔ 沿 PreviousAddress 续链
/// - `Gone` ↔ `ReadProbeResult::Retry` ↔ RETRY_LATER（回链头重探，绝不降级 NOTFOUND）
///
/// 返回 `Some` 为走查终态（调用方直接上抛）；`None` 为续链，前驱地址已写入 `curr`
#[inline]
fn map_rc_visit<R>(visit: RcVisit<R>, curr: &mut u64) -> Option<RcWalk<R>> {
  match visit {
    RcVisit::Found(val) => Some(RcWalk::Found(val)),
    RcVisit::Next(prev) => {
      *curr = prev;
      None
    }
    RcVisit::Gone => Some(RcWalk::Retry),
  }
}

/// [`RcWalk`] → [`MemRead`] 终态换算单点：读主路径步 1 与多候选扫描共用同一判据
#[inline]
fn rc_walk_term<R>(walk: RcWalk<R>) -> Option<MemRead<R>> {
  match walk {
    RcWalk::Found(val) => Some(MemRead::Done(Some(val))),
    RcWalk::Retry => Some(MemRead::Retry),
    RcWalk::ChainEnd => None,
  }
}

/// [`MemBack`] → [`MemRead`] 终态换算单点：回溯两调用点共用同一判据
#[inline]
fn mem_back_term<R>(back: MemBack<R>) -> Option<MemRead<R>> {
  match back {
    MemBack::Found(val) => Some(MemRead::Done(Some(val))),
    MemBack::Tombstone => Some(MemRead::Done(None)),
    MemBack::Retry => Some(MemRead::Retry),
    MemBack::Stopped => None,
  }
}

/// 内存反向回溯结果（[`StoreSession::trace_back_for_key_match`] 出口）
enum MemBack<R> {
  /// 键命中：safe_ro 快照下的读后晋升判定已内含
  Found(R),
  /// 最新匹配记录为墓碑（NOTFOUND）
  Tombstone,
  /// 命中密封在途记录（RETRY_LATER）
  Retry,
  /// 链尽（`curr` 置 0）/ 降至 head 之下的主日志地址 / 页换出竞态（`curr` 保留原址）
  Stopped,
}

/// 磁盘冷读复检内存并发写入状态（[`StoreSession::recheck_memory_concurrent_write`] 出口）
enum MemRecheck<R> {
  /// 内存中已闭环（命中最新值，或确认墓碑/不存在）
  Done(Option<R>),
  /// 发现更高版本的磁盘候选（探针链头严格越过已见最高链头上界、且指向不在原候选
  /// 列表中的新地址），须重走候选扫描；附携带本轮触发重投的新链头地址（升位为
  /// 下轮让位界，对标 C# ContinuePending.cs:83-84 谓词的收敛驱动项）
  Retry(CandidateAddresses, u64),
  /// 内存无更新，继续沿当前磁盘候选处理（含链头未越过下界的稳态——上轮复检已
  /// 自同等或更新链头走完全链，重探零增量信息，按真实走尽口径收口）
  None,
}

/// 同步内存直读权威状态枚举（严格对标 C# OperationStatus：
/// garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/OperationStatus.cs）
///
/// 三态单点定义，替代历史 `Result<Option<Option<R>>>` 嵌套表达：
/// - [`StoreResult::Success`]：内存精准命中，携带闭包消费结果（SUCCESS）；
/// - [`StoreResult::NotFound`]：内存中明确不存在（无候选 Tag / 最新记录为墓碑 /
///   TTL 已到期，NOTFOUND）；
/// - [`StoreResult::RecordOnDisk`]：存在磁盘候选地址，调用方须转入异步磁盘 I/O
///   （RECORD_ON_DISK，降级 `read_with().await` 系）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreResult<T> {
  /// 内存命中（OperationStatus.SUCCESS）
  Success(T),
  /// 内存中明确不存在（OperationStatus.NOTFOUND）
  NotFound,
  /// 冷数据落盘，须降级异步读（OperationStatus.RECORD_ON_DISK）
  RecordOnDisk,
}

impl<T> StoreResult<T> {
  /// 命中值视图（Success(v) → Some(v)，其余 None）
  #[inline]
  pub fn value(self) -> Option<T> {
    match self {
      Self::Success(v) => Some(v),
      Self::NotFound | Self::RecordOnDisk => None,
    }
  }
}

/// 记录读消费者抽象：命中时向闭包披露值切片与记录物理分配尺寸
///
/// 对标 C# MEMORY USAGE 在同一 Read 操作内带出 `srcLogRecord.AllocatedSize`
/// 的通道复用精神（libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleMemoryUsage）：
/// 单一读内核同时服务普通读（单参闭包，忽略尺寸）与物理占用统计
///（双参闭包经 [`with_record_size`] 注入），杜绝为统计另设一条平行读链。
///
/// 公开读入口一律保持 `FnOnce(&[u8]) -> R` 签名（闭包字面量参数类型无法从
/// 自定义 trait bound 推断，`RecordRead` 只出现在 [`Self::try_read_mem`]/
/// [`Self::try_read_mem_fallback`]/[`Self::read_from_disk`] 内核与
/// `*_reader`/`*_with_size` 中转层）。
pub trait RecordRead<R> {
  /// 记录命中回调：`value` 为值切片；`physical_size` 为整条记录物理占用
  ///（记录头 + 键 + 值 + 对齐填充/显式松弛，对标 C# `AllocatedSize`）
  fn read_record(self, value: &[u8], physical_size: usize) -> R;
}

impl<R, F: FnOnce(&[u8]) -> R> RecordRead<R> for F {
  #[inline(always)]
  fn read_record(self, value: &[u8], _physical_size: usize) -> R {
    self(value)
  }
}

/// 单条记录探针分类单点（严格对照 InternalRead.cs:118 IsClosedOrTombstoned：
/// closed 优先于 tombstone 判定，closed → RETRY_LATER、tombstone → NOTFOUND、
/// 键匹配 → 消费读闭包、Tag 碰撞 → 携带 prev_address 供回溯/磁盘候选收集）
///
/// 主链回溯（try_read_mem）与多候选扫描（try_read_mem_fallback）的
/// immutable/memory 双分区四调用点共用，杜绝同构闭包体四处复制；
/// 无错误路径，由调用方闭包以 `Ok(..)` 适配 whlog 访问 API
#[inline]
fn probe_hlog_record<R, F: RecordRead<R>>(
  rec: wrecord::RecordRef<'_>,
  key: &[u8],
  f: &mut Option<F>,
) -> ReadProbeResult<R> {
  if rec.matches_key(key) {
    if rec.is_closed() {
      ReadProbeResult::Retry
    } else if rec.is_tombstone() {
      ReadProbeResult::Tombstone
    } else {
      // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
      let func = unsafe { f.take().unwrap_unchecked() };
      ReadProbeResult::Found(func.read_record(rec.value(), rec.physical_size()))
    }
  } else {
    // 发生 15 位 Tag 碰撞，沿反向链表回溯前驱版本（prev_address）
    ReadProbeResult::Miss(rec.prev_address())
  }
}

/// 双参闭包适配器：`(value, physical_size) -> R`
pub struct WithRecordSize<F>(F);

impl<R, F: FnOnce(&[u8], usize) -> R> RecordRead<R> for WithRecordSize<F> {
  #[inline(always)]
  fn read_record(self, value: &[u8], physical_size: usize) -> R {
    (self.0)(value, physical_size)
  }
}

/// 包装双参闭包为 [`RecordRead`]（MEMORY USAGE 等记录物理占用统计入口用）
#[inline]
pub fn with_record_size<F>(f: F) -> WithRecordSize<F> {
  WithRecordSize(f)
}

/// 内存读边界与回退地址快照（收敛 try_read_mem_fallback 入参，对标 C# Read 慢路径）
#[derive(Clone, Copy)]
struct ReadMemBounds {
  begin_addr: u64,
  head_addr: u64,
  safe_ro_addr: u64,
  chain_disk_addr: u64,
}

impl<D: Device> StoreSession<D> {
  /// 基于预先算定的哈希与首地址探针执行底层物理同步内存直读快路径（Raw，严格对照 Tsavorite InternalRead 与 FindTag 探针）
  ///
  /// # 注意
  /// 调用方须确保当前线程处于 LightEpoch 纪元保护下。
  ///
  /// - `hash` 为调用方对 `key` 单次算定的键哈希，首地址探针 `first_addr` 必须由同一
  ///   `hash` 经 `find_tag_by_hash` 产出，本调用链内绝不二次哈希（严格对标 C#
  ///   `InternalRead(TKey key, long keyHash)`：keyHash 一次算定经
  ///   `OperationStackContext(keyHash)` 装载，`hei.hash` 全程复用，
  ///   SplitBuckets 与 FindTagAndTryEphemeralSLock 均不再重算）；
  /// - **首项快速探针（FindTag）**：绝大多数情况下（99.9%）哈希索引首个槽位即命中，
  ///   直接进行单次内存记录解析并零拷贝执行闭包 `f` 返回，
  ///   彻底消除全桶 7 槽位原子加载（load(Acquire)）与候选数组构建开销。
  /// - **反向链表回溯（TraceBackForKeyMatch）**：遇到 15 位 Tag 碰撞时，沿着记录的
  ///   `prev_address` 反向链表回溯检查前驱版本；若未命中则回退到完整候选扫描路径保证 100% 正确性。
  /// - 返回值语义（三态，对齐 Garnet OperationStatus，见 [`StoreResult`]）：
  ///   - `Ok(StoreResult::Success(val))`：在内存页中精准命中并零拷贝执行闭包 `f` 返回（对应 Garnet `OperationStatus.SUCCESS`）。
  ///   - `Ok(StoreResult::NotFound)`：在内存中明确确认不存在（无候选 Tag，或最新记录为墓碑 Tombstone，对应 Garnet `OperationStatus.NOTFOUND`）。
  ///   - `Ok(StoreResult::RecordOnDisk)`：内存中未找到匹配记录，但存在磁盘候选地址（`addr < head_address && addr >= begin_address`），需要调用方转入异步磁盘 I/O（对应 Garnet `OperationStatus.RECORD_ON_DISK`）。
  #[inline]
  pub fn try_read_raw_in_memory_with_addr<R>(
    &self,
    key: &[u8],
    hash: u64,
    first_addr: Option<u64>,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.with_addr_reader(key, hash, first_addr, f)
  }

  /// 纪元保护已就绪下的地址探针读中转层（[`RecordRead`] 面，供尺寸统计入口注入）
  fn with_addr_reader<R>(
    &self,
    key: &[u8],
    hash: u64,
    first_addr: Option<u64>,
    reader: impl RecordRead<R>,
  ) -> Result<StoreResult<R>> {
    let mut f = Some(reader);
    Ok(match self.drive_mem_read(key, hash, first_addr, &mut f)? {
      MemDrive::Done(res) => res.map_or(StoreResult::NotFound, StoreResult::Success),
      MemDrive::OnDisk(_) => StoreResult::RecordOnDisk,
    })
  }

  /// 内存读驱动环单点：`RETRY_LATER` 的刷新重试在环内闭环，绝不外漏（对标 C#
  /// 会话层 HandleOperationStatus.HandleOperationStatus 的「Refresh the epoch and
  /// retry」单点）；哈希恒为调用方单源算定值，重试仅重探首地址（严格对标 C#
  /// `InternalRead(TKey key, long keyHash)` 一经 `OperationStackContext(keyHash)`
  /// 算定全程经 `hei.hash` 复用，本环绝不重算）。三个消费面共用本环：同步三态
  /// 入口 [`Self::with_addr_reader`]、异步读 [`Self::read_raw_with_reader`]、
  /// 批量读内层（batch.rs），杜绝平行重试环
  ///
  /// 重试环有界（[`INNER_LATCH_RETRY_BUDGET`]）：reviv 开启下外层 user_key 桶闩
  /// 窗口（ttl.rs 的 expire_at/persist/check_expired KeyLatch）与 TTL 记录键桶闩
  /// 偶合同桶时，S-Latch 取闩恒失败回 Retry、而持有者正是本调用自身未返回的
  /// 外层窗口——预算耗尽即回 [`Error::Index`]([`WindexError::LockTimeout`]) 上抛
  /// 退窗（KeyLatch 随栈 Drop 放闩），由调用方按既有错误通道应答可重试错误而非
  /// 挂死；每轮 Retry 经 Backoff 退避（spin → yield，预算内不触微睡档），
  /// 消除同核互阻面（原环纯忙等不让出 reactor，同核持闩者的完成事件永不被 poll）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:HandleOperationStatus
  /// （rust 无按 op 状态回传的中间态：RETRY_LATER 的「刷纪元重试」在环内消化，
  /// 其余终态直接返回，HandleOperationStatus 的状态分派面随之折叠进各操作臂）
  #[inline]
  pub(super) fn drive_mem_read<R>(
    &self,
    key: &[u8],
    hash: u64,
    mut first_addr: Option<u64>,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemDrive<R>> {
    let mut backoff = Backoff::new();
    loop {
      match self.try_read_mem(key, hash, first_addr, f)? {
        MemRead::Done(res) => return Ok(MemDrive::Done(res)),
        MemRead::OnDisk(cands) => return Ok(MemDrive::OnDisk(cands)),
        MemRead::Retry => {
          // C# RETRY_LATER（InternalRead.cs:105-106）：刷新纪元（ProtectAndDrain 语义，
          // 推进密封在途记录的写者完成 CAS 解封）后整链重试；预算门先行判定
          // （step_count 比较零成本，预算语义见 INNER_LATCH_RETRY_BUDGET 文档）
          if backoff.step_count() >= INNER_LATCH_RETRY_BUDGET {
            return Err(Error::Index(WindexError::LockTimeout));
          }
          self.participant.refresh();
          first_addr = self.reprobe_first_addr(hash);
          backoff.snooze();
        }
      }
    }
  }

  /// 读入口探测两连单点：分裂协同 + 单次哈希 + 首地址探针（严格对标 C#
  /// InternalRead.cs:70-73 入口铁律：`phase == IN_PROGRESS_GROW →
  /// SplitBuckets(hei.hash)` 先于 FindTagAndTryEphemeralSLock 的装载探针）——
  /// 探针只允许落在已完成迁移（SPLIT_COMPLETED）的桶上，消除「探针在 growing
  /// 期采得未迁移新桶 None、随后扩容收尾进入 Rest 致 try_read_mem 不再重探而
  /// 采纳陈旧 None」的跨阶段 TOCTOU 采样空洞（幽灵读未命中）；直传首地址的
  /// 外部调用面由 [`Self::try_read_mem`] 入内兜底协同承接
  #[inline]
  fn read_probe(&self, key: &[u8]) -> Result<(u64, Option<u64>)> {
    let hash = whasher::fast_hash(key);
    self.ensure_split_by_hash(hash)?;
    let first_addr = self.reprobe_first_addr(hash);
    // 定向测试留钩（生产恒 None，仅一次原子相位读门控后的无争锁取用）：
    // growing 期仍采得缺席即探针-内核同相位不变式被破坏的注入点，回调在
    // 采样与进入读内核的间隙内确定性完成迁移并翻回 Rest，复现跨阶段 TOCTOU
    if first_addr.is_none()
      && self.store.is_growing()
      && let Some(hook) = self.store.test_read_gap_hook.lock().take()
    {
      hook();
    }
    Ok((hash, first_addr))
  }

  /// 哈希定地址的首地址重探针（驱动环整链重试与扩容 split_buckets 后重读共用）
  #[inline]
  fn reprobe_first_addr(&self, hash: u64) -> Option<u64> {
    self.store.index.load().find_tag_by_hash(hash)
  }

  /// 底层物理同步内存直读快路径（Raw）
  #[inline]
  pub fn try_read_raw_in_memory<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    let _guard = self.enter_gated();
    let (hash, first_addr) = self.read_probe(key)?;
    self.try_read_raw_in_memory_with_addr(key, hash, first_addr, f)
  }

  /// 在已有纪元保护下执行当前会话指定标签物理键同步内存直读快路径（完全绕过 enter() 原子开销）
  ///
  /// 带标签读内核（一处定义）：对象信封与普通物理键共用本实现
  #[inline]
  pub fn try_read_tag_in_memory_unprotected<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    let prefix = self.session_prefix();
    self.try_read_tag_in_memory_unprotected_with_prefix(prefix.as_slice(), user_key, tag, f)
  }

  /// 显式前缀带标签内存直读内核（循环前缀外提对位，语义与
  /// [`Self::try_read_tag_in_memory_unprotected`] 完全一致；rust 工程优化无 c# 对应）
  #[inline]
  pub fn try_read_tag_in_memory_unprotected_with_prefix<R>(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    let (hash, first_addr) = self.read_probe(&rec_k)?;
    self.try_read_raw_in_memory_with_addr(&rec_k, hash, first_addr, f)
  }

  /// 在已有纪元保护下执行指定标签物理键同步内存直读并披露记录物理尺寸
  ///（MEMORY USAGE 统计内核，[`Self::try_read_tag_in_memory_unprotected`] 的带尺寸对位）
  #[inline]
  pub fn try_read_tag_in_memory_with_size<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<StoreResult<R>> {
    let rec_k = self.session_tag_key(tag, user_key);
    let (hash, first_addr) = self.read_probe(&rec_k)?;
    self.with_addr_reader(&rec_k, hash, first_addr, with_record_size(f))
  }

  /// 在已有纪元保护下同步读取当前会话指定标签物理键（TTL 同栈门裁决 + 内存直读，彻底绕过 enter() 原子开销）
  ///
  /// TTL 门控按用户键（KeyTag::Ttl 旁路记录）同栈裁决（[`crate::ttl::StoreSession::ttl_gate_mem_at`]，
  /// 对标 C# libs/server/Storage/Functions/LogRecordUtils.cs:CheckExpiry 在 ReadMethods.cs
  /// 的 Reader 内的内联判定）：
  /// 无 TTL / 未到期 → 内存直读零拷贝放行；已到期 → 快路径直接 NOTFOUND（对应
  /// OperationStatus.NOTFOUND，物理清理留写路径惰性清退与后台 GC）；TTL 记录有
  /// 磁盘候选 → 降级异步读裁决。与数据记录标签无关
  #[inline]
  pub fn try_read_tag_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    let prefix = self.session_prefix();
    self.try_read_tag_sync_unprotected_with_prefix(prefix.as_slice(), user_key, tag, f)
  }

  /// 显式前缀带标签同步读内核（循环前缀外提对位，语义与
  /// [`Self::try_read_tag_sync_unprotected`] 完全一致；rust 工程优化无 c# 对应）：
  /// TTL 门裁决与数据记录读取复用同一外提前缀
  #[inline]
  pub fn try_read_tag_sync_unprotected_with_prefix<R>(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    match self.ttl_gate_mem_at_with_prefix(prefix, user_key, now_ticks())? {
      TtlGate::Pass => {
        self.try_read_tag_in_memory_unprotected_with_prefix(prefix, user_key, tag, f)
      }
      // 已过期：快路径 NOTFOUND 语义，不做物理删除
      TtlGate::Due => Ok(StoreResult::NotFound),
      TtlGate::Degrade => Ok(StoreResult::RecordOnDisk),
    }
  }

  /// 同步读指定标签物理键快路径并披露记录物理尺寸（TTL 同栈门裁决 + 内存直读）
  ///
  /// MEMORY USAGE 统计内核：[`Self::try_read_tag_sync_unprotected`] 的带尺寸对位，
  /// 三态语义一致
  #[inline]
  pub fn try_read_tag_sync_with_size<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<StoreResult<R>> {
    match self.ttl_gate_mem_at(user_key, now_ticks())? {
      TtlGate::Pass => self.try_read_tag_in_memory_with_size(user_key, tag, f),
      TtlGate::Due => Ok(StoreResult::NotFound),
      TtlGate::Degrade => Ok(StoreResult::RecordOnDisk),
    }
  }

  /// 在已有纪元保护下同步读取当前会话普通字符串键（TTL 快门控 + 内存直读，彻底绕过 enter() 原子开销）
  ///
  /// 返回三态（严格对标 Garnet 同步读状态，见 [`StoreResult`]）：
  /// - `Ok(StoreResult::Success(r))`：内存命中，闭包零拷贝消费（对应 OperationStatus.SUCCESS）；
  /// - `Ok(StoreResult::NotFound)`：内存中明确不存在（无候选 Tag、最新记录为墓碑，或 TTL 已到期），对应 OperationStatus.NOTFOUND；
  /// - `Ok(StoreResult::RecordOnDisk)`：须降级全异步 `read_with().await`（数据或 TTL 记录存在磁盘候选，对应 OperationStatus.RECORD_ON_DISK）。
  #[inline]
  pub fn try_read_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.try_read_tag_sync_unprotected(user_key, KeyTag::String, f)
  }

  /// 同步读取当前会话普通字符串键（TTL 快门控 + 内存直读，严格对标 Garnet 同步读取路径）
  #[inline]
  pub fn try_read_sync<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    let _guard = self.enter_gated();
    self.try_read_sync_unprotected(user_key, f)
  }

  /// ReadCache 整链走查单点（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache
  /// 「非 Invalid 才比对键、无条件沿 PreviousAddress 继续」；读侧 `alwaysFindLatestLA:false`
  /// 命中即返，即读语义）
  ///
  /// 驱逐等待协议（对标 ReadCacheNeedToWaitForEviction）：地址滑出 RC 环形窗口
  /// （abs < head）时短自旋等待驱逐方 cleanse 完成并发布 ClosedUntilAddress，再按
  /// UpdateRecordSourceToCurrentHashEntry 语义回链头重探，返回 [`RcWalk::Retry`]——
  /// 否则本次读会被误判为 NOTFOUND（瞬态一致性缺口）；closed 作废记录携 prev 跳过
  /// 续链（并发写 CAS 脱钩与本走查交错时，读者沿链取到脱钩前旧值——读线性化在写
  /// CAS 之前，合法）；滑窗竞态不可判读回链头重探，绝不允许折叠成链终止产出假
  /// NOTFOUND。退出时 `curr` 指向首个主日志地址（可能为链尽 0）。
  #[inline]
  fn find_in_read_cache<R>(
    &self,
    key: &[u8],
    curr: &mut u64,
    f: &mut Option<impl RecordRead<R>>,
  ) -> RcWalk<R> {
    while is_read_cache(*curr) {
      if self
        .store
        .read_cache
        .need_to_wait_for_eviction(*curr, || self.participant.refresh())
      {
        return RcWalk::Retry;
      }
      let visit = self
        .store
        .read_cache
        .with_record(*curr, |rec_key, rec_val| {
          if fast_key_eq(rec_key, key) {
            // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
            let func = unsafe { f.take().unwrap_unchecked() };
            // ReadCache 记录由 append 以 record_size 对齐编码且无松弛填充，
            // 物理占用即 record_size（与主日志 [RecordHeader::physical_size] 同口径）
            Some(func.read_record(rec_val, record_size(rec_key.len(), rec_val.len())))
          } else {
            None
          }
        });
      // RcVisit → RcWalk 判据换算只走 map_rc_visit 单点：终态上抛，续链则前驱已就位
      if let Some(walk) = map_rc_visit(visit, curr) {
        return walk;
      }
      if *curr == 0 {
        break;
      }
    }
    RcWalk::ChainEnd
  }

  /// 内存反向链表回溯单点（严格对照
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TraceBackForKeyMatch
  /// 与 InternalRead.CopyFromImmutable）：每跳按 IsValidTracebackRecord 口径消费
  /// is_valid/is_closed——密封在途记录参与键比对，命中后按 IsClosedOrTombstoned
  /// （InternalRead.cs:118-131）降级 [`MemBack::Retry`]；read_only 之前的记录走纯
  /// 指针直读（CreateLogRecord + GetPhysicalAddress 口径），真可变区保留页读锁探针。
  ///
  /// Found 内含 safe_ro 快照提升判定（对标 CopyFromImmutable 仅作用于不可变区
  /// [HeadAddress, SafeReadOnlyAddress)，模糊区瞬态窗口内不提升，保守方向无损
  /// 正确性），promote 调用点由 2 收敛为本单点。Miss(0) 链尽统一置 `curr` 为 0
  /// （[`MemBack::Stopped`]），杜绝末条内存记录地址被当作伪磁盘候选透传（链尽即
  /// C# NOTFOUND 口径）。退出 `curr`：链尽 0 / 降至 head 之下的主日志地址（磁盘
  /// 区候选）/ 页换出竞态保留原址。
  #[inline]
  fn trace_back_for_key_match<R>(
    &self,
    key: &[u8],
    curr: &mut u64,
    head_addr: u64,
    ro_addr: u64,
    safe_ro_addr: u64,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemBack<R>> {
    while *curr >= head_addr {
      // 探针以单点函数直接内联传入两分支（不用 &mut 提取：间接层会阻断
      // with_*_record 与闭包的一体化内联，热点工况实测退化 ~12%）
      let probed = if *curr < ro_addr {
        // SAFETY: 调用方纪元保护 + *curr ∈ [head_addr, ro_addr) 均为进入前快照，
        // 双门槛契约见 with_immutable_record 文档；该分区驻留由快照门槛保证，
        // 直接产出探针结果（包装 Some 与页锁分支的 Option 口径对齐）
        Some(unsafe {
          self
            .store
            .hlog
            .with_immutable_record(*curr, |rec| Ok(probe_hlog_record(rec, key, f)))?
        })
      } else {
        self
          .store
          .hlog
          .with_memory_record(*curr, |rec| Ok(probe_hlog_record(rec, key, f)))?
      };
      match probed {
        Some(ReadProbeResult::Found(val)) => {
          if *curr < safe_ro_addr {
            self.promote_immutable_read_hit(*curr, key);
          }
          return Ok(MemBack::Found(val));
        }
        Some(ReadProbeResult::Tombstone) => return Ok(MemBack::Tombstone),
        Some(ReadProbeResult::Retry) => return Ok(MemBack::Retry),
        Some(ReadProbeResult::Miss(next)) => {
          *curr = next;
          if next == 0 {
            return Ok(MemBack::Stopped);
          }
        }
        // 页换出竞态（head 推进瞬态）：curr 保留原址，交调用方候选扫描/磁盘口径处理
        None => return Ok(MemBack::Stopped),
      }
    }
    Ok(MemBack::Stopped)
  }

  /// 内存直读核心路径（调用方须处于纪元保护下，严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead 单遍分类）
  ///
  /// - **首项快速探针（FindTag）**：绝大多数情况下（99.9%）哈希索引首个槽位即命中，
  ///   直接进行单次内存记录解析并零拷贝执行闭包 `f` 返回。
  /// - **ReadCache 整链走查 / 反向链表回溯**：分别经 [`Self::find_in_read_cache`]
  ///   （对标 FindInReadCache）与 [`Self::trace_back_for_key_match`]（对标
  ///   TraceBackForKeyMatch）两单点执行，与多候选扫描路径共用，杜绝逐字复制；
  ///   若未命中则回退到完整候选扫描路径保证 100% 正确性。
  /// - `RETRY_LATER` 以 [`MemRead::Retry`] 上抛，由 [`Self::drive_mem_read`] 驱动环
  ///   刷新纪元后整链重试（对标 C# HandleOperationStatus 单点），本内核不自旋。
  /// - 磁盘候选地址单遍收集后经 `MemRead::OnDisk` 原样透传，冷读回退路径零重复索引遍历。
  ///
  /// `hash` 为调用方对 `key` 单次算定的键哈希（对标 C# `InternalRead(TKey key, long keyHash)`
  /// 的 keyHash：一经 `OperationStackContext(keyHash)` 算定，SplitBuckets 与
  /// FindTagAndTryEphemeralSLock 全程经 `hei.hash` 复用，本内核不再重算）。
  #[inline]
  fn try_read_mem<R>(
    &self,
    key: &[u8],
    hash: u64,
    first_addr: Option<u64>,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemRead<R>> {
    let mut curr_addr = first_addr;
    if self.store.is_growing() {
      // 兜底协同（read_probe 入口已先行协同，此处承接直传 first_addr 的外部调用
      // 与驱动环重试臂）：扩容期对未迁移新桶采得的 None 一律重探，杜绝跨阶段
      // TOCTOU 假未命中
      self.ensure_split_by_hash(hash)?;
      curr_addr = self.reprobe_first_addr(hash);
    }
    let Some(mut curr_addr) = curr_addr else {
      // 哈希表中连对应 Tag 都完全不存在，100% 确认无此键，极速返回
      return Ok(MemRead::Done(None));
    };

    let index = self.store.index.load();
    // 优化5（严格对标 Helpers.FindTagAndTryEphemeralSLock）：复活相关配置开启时，
    // traceback 前以共享 latch 锁住链首桶，防止复活/脱钩在回溯期间把记录从链上抽走
    // （"Ephemeral must lock the bucket before traceback"）；锁成功后重读当前链首槽位
    // （对标 HashEntryInfo.SetToCurrent），锁失败按 C# RETRY_LATER 协议交由调用方
    // 刷新纪元后整链重试。复活池关闭时脱钩虽恒常态发生，但脱钩槽位即经
    // try_seal_record 密封且不被原地复用，无锁回溯至多读到已被取代的旧链
    // （epoch 保护与 CAS 定序承接安全面，判据详见
    // `StoreSession::ephemeral_lock_enabled`），维持现状免锁
    let _s_latch = if self.ephemeral_lock_enabled() {
      let bucket = index.bucket(index.bucket_index_for_hash(hash));
      let Some(latch) = bucket.lock_shared_guard() else {
        return Ok(MemRead::Retry);
      };
      // SetToCurrent：定位 Tag 与加锁之间槽位可能已被并发 CAS/脱钩改写，
      // 链首槽位不再持有原地址时按同一 hash 重读当前条目（C# hei.hash 单源口径）
      let tag = HashBucketEntry::tag_from_hash(hash);
      if bucket.find_entry_by_address(tag, curr_addr).is_none()
        && let Some(hei) = index.find_tag_entry_by_hash_with_min_addr(hash, 0)
      {
        curr_addr = hei.address();
      }
      Some(latch)
    } else {
      None
    };
    let head_addr = self.store.head_address();
    let begin_addr = self.store.begin_address();
    // 免锁直读门槛快照：对标 InternalRead.cs:114-124 可变区甚至模糊区（>= SafeReadOnlyAddress）
    // 的无锁 CreateLogRecord 直读——门槛 read_only 覆盖模糊区 [safe_read_only, read_only)，
    // 撕裂安全由 wrecord 头 RDH 单 8 字节原子字发布协议保证（whlog::probe_resident 文档
    // 第 2/3 条）；真可变区 [read_only, tail) 走保守页读锁与原位更新互斥。
    // 偏旧快照只会把可变区记录误判入页锁路径（方向安全）；偏新不可能（Acquire 载入单调原子）。
    let ro_addr = self.store.hlog.read_only_address();
    // 不可变区命中提升门槛快照：严格对标 C# CopyFromImmutable 仅作用于不可变区
    // [HeadAddress, SafeReadOnlyAddress)（InternalRead.cs:126-139 immutable 分支），
    // 模糊区瞬态窗口内不提升（保守方向正确性无损）
    let safe_ro_addr = self.store.safe_read_only_address();

    // 1. ReadCache 内存直读快路径：整链走查单点（对标 FindInReadCache，
    //    与多候选扫描共用同一内核，终态换算走 rc_walk_term）
    if let Some(term) = rc_walk_term(self.find_in_read_cache(key, &mut curr_addr, f)) {
      return Ok(term);
    }

    // 2. 内存常态快路径（99%+ 场景）：处于 HLog 内存驻留区，反向链表回溯单点
    //    （对标 TraceBackForKeyMatch + CopyFromImmutable，与多候选扫描共用）；
    //    Stopped 时 curr 已按链尽 0 / 磁盘区地址 / 竞态原址三口径就位，续步 3/4 处理
    if !is_read_cache(curr_addr) && curr_addr >= head_addr {
      let back =
        self.trace_back_for_key_match(key, &mut curr_addr, head_addr, ro_addr, safe_ro_addr, f)?;
      if let Some(term) = mem_back_term(back) {
        return Ok(term);
      }
    }

    // 3. 链条已在内存区终结（curr_addr == 0 或已低于 begin_addr 截断边界）：
    //    严格对标 Tsavorite InternalRead.cs:142-157：
    //    若链条未伸入有效磁盘区（curr_addr < begin_addr），确认该键在整个存储中不存在，
    //    直接返回 MemRead::Done(None)，彻底消除无效的多候选扫描与二次哈希遍历！
    if curr_addr == 0 || (!is_read_cache(curr_addr) && curr_addr < begin_addr) {
      return Ok(MemRead::Done(None));
    }

    // 4. 首项未命中、处于磁盘区或发生跨槽位 Tag 碰撞，回退到多候选扫描与落盘判定路径；
    //    curr_addr 为内存链回溯穷尽后的下一地址——链已伸入磁盘区时作为附加磁盘候选
    //    传递（严格对照 C# pending read 沿 prev 链从内存伸入磁盘的读取语义）
    self.try_read_mem_fallback(
      key,
      hash,
      f,
      ReadMemBounds {
        begin_addr,
        head_addr,
        safe_ro_addr,
        chain_disk_addr: curr_addr,
      },
    )
  }

  /// 多候选扫描与冷数据落盘判定路径（标记为 cold 避免内联膨胀热点指令缓存）
  ///
  /// 单次 `lookup_candidates_by_hash` 遍历同时闭环内存 Tag 碰撞消解与磁盘候选收集，
  /// 磁盘候选按新版本优先降序经 `MemRead::OnDisk` 透传，供冷读回退路径直接消费。
  /// `chain_disk_addr` 为内存链回溯穷尽后的下一地址（< head 且 >= begin 时有效）。
  #[cold]
  fn try_read_mem_fallback<R>(
    &self,
    key: &[u8],
    hash: u64,
    f: &mut Option<impl RecordRead<R>>,
    bounds: ReadMemBounds,
  ) -> Result<MemRead<R>> {
    let mut addrs = self.store.index.load().lookup_candidates_by_hash(hash);
    if addrs.is_empty() {
      // 无槽位候选（并发清退等瞬态）且内存链已伸入磁盘区：直读链出地址兜底；
      // 链仍在内存区或已穷尽为 0 则确认不存在
      if bounds.chain_disk_addr != 0 && bounds.chain_disk_addr >= bounds.begin_addr {
        let mut disk = CandidateAddresses::new();
        disk.push(bounds.chain_disk_addr);
        return Ok(MemRead::OnDisk(disk));
      }
      return Ok(MemRead::Done(None));
    }
    addrs.sort_descending();

    // 免锁直读门槛快照：与主路径 trace_back 单点口径一致（read_only 覆盖模糊区）
    let ro_addr = self.store.hlog.read_only_address();
    let mut disk_cands = CandidateAddresses::new();

    for &addr in addrs.iter() {
      let mut cur_addr = addr;
      // RC 段：与主路径同一整链走查单点（对标 FindInReadCache）——沿 PreviousAddress
      // 逐条判读直至首个主日志地址，杜绝旧版「只访首条 RC 即 skip 剥链」对同 Tag
      // 深链记录的漏配与 skip 竞态 Retry 分支
      if let Some(term) = rc_walk_term(self.find_in_read_cache(key, &mut cur_addr, f)) {
        return Ok(term);
      }
      if cur_addr == 0 || cur_addr < bounds.begin_addr {
        continue;
      }

      // 主日志段：与主路径同一反向回溯单点（对标 TraceBackForKeyMatch +
      // CopyFromImmutable），Found 提升判定/safe_ro 快照口径两处合一
      let back = self.trace_back_for_key_match(
        key,
        &mut cur_addr,
        bounds.head_addr,
        ro_addr,
        bounds.safe_ro_addr,
        f,
      )?;
      if let Some(term) = mem_back_term(back) {
        return Ok(term);
      }

      if cur_addr != 0 && cur_addr >= bounds.begin_addr {
        disk_cands.push(cur_addr);
      }
    }

    if disk_cands.is_empty() {
      Ok(MemRead::Done(None))
    } else {
      Ok(MemRead::OnDisk(disk_cands))
    }
  }

  /// 不可变区命中的读后晋升（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:CopyFromImmutable
  /// 的单目的地分派，两臂绝不并行动作）
  ///
  /// - ReadCache 启用 → 挂入非脏 DRAM 环形缓存：append 内部完成索引 CAS 挂载
  ///   （对标 TryCopyToReadCache 的 hei.TryCAS），失败静默降级、环形覆盖自然回收；
  /// - ReadCache 关而 `copy_reads_to_tail` 开 → 同步最佳努力尾部晋升（对标
  ///   ConditionalCopyToTail(wantIO:false)——该臂只是「省一次未来 I/O」的优化，
  ///   追加需发 I/O（环形缓冲翻页待驱逐）即放弃，留给下一次读）；挂载与败帧
  ///   回收走 [`Self::cas_mount_copied_frame`] 单点，与磁盘回填臂同一内核；
  /// - 两位皆关 → 零动作（对标 C# `CopyTo == None`）。
  ///
  /// 目的地次序与磁盘回填臂严格一致（read_cache 优先于 tail）：C# `--copy-reads-to-tail`
  /// 把 `ReadCopyOptions.CopyTo` 定为 MainLog（GarnetServerOptions.cs:899-900），
  /// rust 以两个独立布尔承载同一语义，次序差异只在两者同开的非 C# 形态下可见。
  ///
  /// 仅不可变区命中时调用：可变区记录本就是热数据，避免无谓的重复缓存占用；
  /// append 失败（未启用/单记录超页容量/CAS 冲突）一律静默降级，不影响正确性。
  #[cold]
  fn promote_immutable_read_hit(&self, addr: u64, key: &[u8]) {
    let to_read_cache = self.store.read_cache.is_enabled;
    if !to_read_cache && !self.copy_reads_to_tail() {
      return;
    }
    // 独立第二次单遍记录访问：不可变区在纪元保护下无锁纯指针直读，无页锁嵌套；
    // 若期间已被驱逐出内存则返回 None，静默放弃本次提升
    let mut tail_val: Option<Vec<u8>> = None;
    // 索引句柄先出 Arc：借用闭包内 append 只完成回绕换页的纪元关闭屏障武装，
    // 注册动作必须在裸借用结束后的安全点执行（wepoch 注册即刷新本线程公布纪元，
    // 借期内注册会提前解除本线程借用页保护，见 read_cache/append.rs:pump_close_barrier）
    let index = self.store.index.load_full();
    let _ = unsafe {
      self.store.hlog.with_immutable_record(addr, |rec| {
        if !rec.is_tombstone() && rec.matches_key(key) {
          if to_read_cache {
            self
              .store
              .read_cache
              .append(key, rec.value(), &index, self.store.begin_address());
          } else {
            // 尾部追加须取尾页写锁，绝不可嵌套在本记录的页读锁内：值出锁后落笔
            tail_val = Some(rec.value().to_vec());
          }
        }
        Ok(())
      })
    };
    // 出借期安全点：被驱逐旧页的关闭序列在此挂入纪元延迟排空队列（对标
    // AllocatorBase.cs:ShiftHeadAddress 的 BumpCurrentEpoch(→OnPagesClosed)）；
    // 未武装时零开销直返。grow 迁移窗内并取迁移源旧表快照随闭包捕获，
    // 双表并洗恢复未迁分块槽位（见 read_cache/append.rs:pump_close_barrier）
    self
      .store
      .read_cache
      .pump_close_barrier(&index, self.store.resize.old_index.load_full().as_ref());
    let Some(val) = tail_val else {
      return;
    };
    // wantIO:false 内核（对标 C# ConditionalCopyToTail(wantIO:false)——该臂只是
    // 「省一次未来 I/O」的优化，追加需发 I/O 即放弃，留给下一次读；C# 侧同样以
    // AllocateOptions{recycle=true} 经 TryAllocateRecord 池取）：池取收编与追加臂
    // 同一单点，PageNotReady（需驱逐刷盘）即放弃本次晋升，不触发驱逐；旁路写
    // 监听——帧内容为已存在的旧值，属物理布局优化而非用户写效果，镜像入 AOF 会在
    // 并发写下造成恢复回退（与磁盘回填臂同口径，见其注释）
    let min_eligible = self.reviv_chain_floor(addr);
    if let Ok(Ok((new_addr, frame, _ver))) =
      self.try_allocate_or_append_record_sync(key, val.as_slice(), addr, false, min_eligible)
    {
      self.cas_mount_copied_frame(key, addr, new_addr, frame);
    }
  }

  /// 磁盘冷读复检内存并发写入单点（严格对标 C# ContinuePendingRead.cs：
  /// During the pending operation, a record for the key may have been added to the log or readcache.
  /// 检查在等待磁盘 I/O 期间是否有并发事务写入新记录、追加墓碑或前推链头）：
  /// 磁盘命中（rec 匹配键）与磁盘走尽（未找到匹配记录）共用本复检，杜绝逻辑两处复制；
  /// RETRY_LATER 的刷新重试内聚于 drive_mem_read 驱动环。磁盘回退路径口径见
  /// [`Self::read_from_disk`]
  ///
  /// 重投收窄谓词（对标 C# ContinuePending.cs:83-84 谓词
  /// `LogicalAddress > initialLatestLogicalAddress && LogicalAddress >= minAddress`
  /// 的链头单调收窄同形）：`min_address` 为已见最高链头下界（0 = 首轮无界），仅当
  /// 探针链头严格越过该下界且不在已检候选集内才产出 [`MemRecheck::Retry`]——链头
  /// 未越过上界即上轮已自同等或更新地址走完全链、零增量信息，按
  /// [`MemRecheck::None`] 真实走尽口径收口（修复前无此界，稳态内存驻留链头每轮都
  /// 重触 Retry，重投窗永不收窄，复检预算必然推满上抛——VADD 冷读臂间歇 -ERR 主根因）
  #[inline]
  fn recheck_memory_concurrent_write<R>(
    &self,
    key: &[u8],
    addrs: &CandidateAddresses,
    min_address: u64,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemRecheck<R>> {
    let _guard = self.enter_gated();
    let (hash, cur_first_addr) = self.read_probe(key)?;
    match self.drive_mem_read(key, hash, cur_first_addr, f)? {
      MemDrive::Done(res) => Ok(MemRecheck::Done(res)),
      MemDrive::OnDisk(new_cands) => {
        if let Some(first) = cur_first_addr
          && first > min_address
          && !addrs.contains(first)
        {
          Ok(MemRecheck::Retry(new_cands, first))
        } else {
          Ok(MemRecheck::None)
        }
      }
    }
  }

  /// 异步磁盘回退路径（仅当内存阶段确认存在磁盘候选地址时调用）
  ///
  /// - 候选列表由 `try_read_mem_fallback` 单遍索引扫描收集并降序透传，此处零重复索引遍历；
  /// - 磁盘 I/O 期间不持有纪元守卫（对标 C# IO 期间 `UnsafeSuspendThread`），
  ///   彻底消除单次冷读阻塞纪元推进与页回收的尾延迟隐患；挂起经
  ///   `EpochSuspendGuard` 按当前重入深度生效，外层批守卫（enter_batch）在场时
  ///   同样解除自钉，本宣称对批处理会话形态一并成立；
  /// - 读取走 `read_disk_record` 纯设备路径（不触碰内存页缓冲），无守卫读取安全性成立；
  /// - 磁盘链回溯：Tag 碰撞键不匹配时沿记录 `prev_address` 前驱链继续冷读
  ///   （严格对照 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncGetFromDiskCallback "skips colliding keys by
  ///   following the .PreviousAddress chain"；链地址在磁盘区内严格单调下降，
  ///   直至链尽 0 或低于截断线 begin_address）。
  /// - 复检重投收窄（对标 C# ContinuePending.cs:ContinuePendingRead :76-123 重发
  ///   收敛——本函数即其 rust 内聚承接位）：每轮 [`MemRecheck::Retry`] 触发链头
  ///   须严格越过已见最高链头上界（C# `LogicalAddress > initialLatestLogicalAddress`
  ///   谓词同形的让位序），界随轮单调升；重投窗取复检重探的磁盘再入点，prev 链
  ///   自再入点下扫覆盖全部旧窗深度，稳态链头即断供重投，数轮内落真实走尽/命中
  ///   出口（走尽臂见循环尾注；按地址阈滤旧窗的更窄形态经 collision_chain 四测
  ///   实测证伪——再入点之下被碰撞链掩埋的活记录会被误杀，不收）。
  ///
  /// 冷读命中即 OnDiskRead 记录触发器落点（rust 侧收敛为读晋升/统计联动，无
  /// 宿主回调面；触发器契约见
  /// libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:OnDiskRead）
  #[cold]
  pub(super) async fn read_from_disk<R>(
    &self,
    key: &[u8],
    mut addrs: CandidateAddresses,
    f: impl RecordRead<R>,
  ) -> Result<Option<R>> {
    const MAX_DISK_RECHECKS: usize = 16;
    let mut f = Some(f);
    let mut rechecks = 0;
    // 已见最高链头单调让位界（0 = 首轮无界；每轮 Retry 触发链头须严格越过该界，
    // 界随轮升位——C# ContinuePendingRead 重发以新链头收窄搜索域的同源让位序）
    let mut min_address = 0u64;

    'outer: loop {
      let cur_addrs = take(&mut addrs);
      for addr in cur_addrs.iter().copied() {
        let mut cur = addr;
        while cur >= self.store.begin_address() {
          // 冷读磁盘 I/O 窗口零纪元占用：批会话形态下外层批守卫（enter_batch）
          // 在场，挂起守卫按当前重入深度逐层解除自钉（重入臂不刷新公布纪元，
          // 不挂起即把会话槽位公布纪元钉死入场值，safe_head/closed_until 排空
          // 屏障在整段冷读期间停摆）；无外层守卫时重入深度为 0，净零开销。
          // 严格对标 C# MainStoreOps.cs:ReadWithUnsafeContext 的
          // EndUnsafe → CompletePendingForSession（I/O）→ BeginUnsafe 协议与
          // CompletePendingAsync.cs:43-56 的 UnsafeSuspendThread 包裹口径。
          // 重入后快照复核即重探由本循环既有协议承接：`while cur >=
          // begin_address` 每轮现读截断线（safe_ro/head 前移即链终止）+
          // recheck_memory_concurrent_write 复检并发写，即 C# epochChanged
          // 整体重来的等价收敛，杜绝悬挂借用
          let record = {
            let _suspend = EpochSuspendGuard::new(&self.participant);
            match self.store.hlog.read_disk_record(cur).await {
              Ok(r) => r,
              Err(e) => {
                if cur < self.store.begin_address() {
                  break;
                }
                return Err(e.into());
              }
            }
          };

          // 单次解析零拷贝记录视图，键比对 / 墓碑判定 / 值提取 / 物理尺寸共用
          //（消除逐字段重复头解析；RecordOutput 缓冲即完整记录映像）
          let Ok(rec) = record.as_record_ref() else {
            // 头解析失败（截断/损坏）：按链终止口径处理
            break;
          };
          if !fast_key_eq(rec.key(), key) {
            // Tag 碰撞：提取前驱版本地址，磁盘链回溯
            cur = rec.prev_address();
            continue;
          }

          // 对标 C# ContinuePendingRead.cs：
          // During the pending operation, a record for the key may have been added to the log or readcache.
          // 检查在等待磁盘 I/O 期间是否有并发事务写入新记录或追加墓碑。
          // 命中臂预算尽不动（勿改上抛）：本臂此刻已持有该键磁盘记录的实读
          // 证据（rec 键匹配已核对），回落继续返回该磁盘值是合法线性化——
          // C# 侧同形：diskLogRecord 已到手即以其裁决（ContinuePending.cs
          // :131-142 墓碑与 :205-206 读值出口），与走尽臂「零证据断言缺席」
          // 有本质区别
          match self.recheck_memory_concurrent_write(key, &cur_addrs, min_address, &mut f)? {
            MemRecheck::Done(res) => return Ok(res),
            MemRecheck::Retry(new_cands, head) => {
              rechecks += 1;
              if rechecks < MAX_DISK_RECHECKS {
                // 收窄让位：下界升位至本轮触发链头（谓词已保证严格越过旧界，界随
                // 轮单调升）；候选窗取复检重探产出 new_cands 不另滤旧——new_cands
                // 锚定桶链当前磁盘再入点，prev 链自该点整体下扫即覆盖旧窗全部深度
                // （按地址阈滤旧会误杀再入点之下被碰撞链掩埋的活记录，破坏命中臂
                // 证据链，collision_chain 四测实测证伪后收口为本形态）
                min_address = head;
                addrs = new_cands;
                continue 'outer;
              }
            }
            MemRecheck::None => {}
          }

          if rec.is_tombstone() {
            return Ok(None);
          }
          let val_slice = rec.value();
          // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
          let func = unsafe { f.take().unwrap_unchecked() };
          let result = func.read_record(val_slice, rec.physical_size());

          // 回填阶段重新进入纪元保护（ReadCache 挂链与索引地址更新均为共享内存结构变更）；
          // 严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache 与 TryCopyToTail：
          // 1. 若启用了 ReadCache，优先将冷数据挂入纯 DRAM 只读非脏页内存日志（零持久化开销、零写放大）；
          // 2. 否则若开启 copy_reads_to_tail，则回退到追加 Tail 内存活跃区晋升。
          // （目的地二者择一，与内存不可变区命中臂 promote_immutable_read_hit 同一分派口径）
          // 晋升帧旁路写监听：帧内容为已存在的
          // 旧值，属物理布局优化而非用户写效果，镜像入 AOF 会在并发写下造成恢复回退
          // （旧值帧晚于并发新值帧入队，重放序错乱，见 allocate_record 注释）
          if self.store.read_cache.is_enabled {
            let index = self.store.index.load_full();
            {
              let _guard = self.enter_gated();
              // append 内部以链首插入协议完成索引 CAS 挂载（对标 TryCopyToReadCache 的
              // hei.TryCAS：prev 取槽位链头地址，碰撞链深层键同样可挂载），挂载失败
              // （链头被并发抢占）静默放弃，环形覆盖自然回收
              self
                .store
                .read_cache
                .append(key, val_slice, &index, self.store.begin_address());
            }
            // 出借期安全点：被驱逐旧页的关闭序列挂入纪元延迟排空队列，发布 ClosedUntilAddress；
            // grow 迁移窗内并取迁移源旧表快照随闭包捕获，双表并洗恢复未迁分块槽位
            self
              .store
              .read_cache
              .pump_close_barrier(&index, self.store.resize.old_index.load_full().as_ref());
          } else if self.copy_reads_to_tail() {
            // 池取收编（对标 C# ContinuePendingConditionalCopyToTail 磁盘回填臂统一
            // 经 TryCopyToTail 的 AllocateOptions{recycle=true}）：下界按源链首抬升；
            // PageNotReady 驱逐旧页重试与紧缩搬迁同款；失败静默放弃挂载，读结果
            // 不受影响；旁路写监听与晋升帧口径见 promote_immutable_read_hit
            let min_eligible = self.reviv_chain_floor(cur);
            if let Ok((new_addr, frame, _ver)) = self
              .allocate_record(key, val_slice, cur, false, min_eligible)
              .await
            {
              self.cas_mount_copied_frame(key, cur, new_addr, frame);
            }
          }

          return Ok(Some(result));
        }
      }

      // 磁盘上未找到匹配记录：复检等待期间是否有并发写入
      match self.recheck_memory_concurrent_write(key, &cur_addrs, min_address, &mut f)? {
        MemRecheck::Done(res) => return Ok(res),
        MemRecheck::Retry(new_cands, head) => {
          rechecks += 1;
          if rechecks < MAX_DISK_RECHECKS {
            // 收窄让位重投（C# ContinuePendingRead 同形收敛，让位序在触发界）：
            // 下界升位至本轮触发链头，new_cands 即复检重探的磁盘再入点窗，prev
            // 链自再入点整体下扫覆盖全部旧窗深度（不另按地址阈滤旧——阈滤会误杀
            // 再入点之下被碰撞链掩埋的活记录，collision_chain 四测实测证伪）；
            // 触发界随轮严格升高，并发写一旦停增即落 MemRecheck::None 真实走尽
            // 出口，预算内可达收敛（修复前无触发界，稳态链头每轮重触 Retry 必推
            // 满预算上抛）
            min_address = head;
            addrs = new_cands;
            continue 'outer;
          }
          // 复检预算耗尽且链头仍逐轮严格越过上界：手中 new_cands 即未读的活候选，
          // 此刻零缺席证据，不得断言不存在——上抛可重试 LockTimeout 交调用方重投
          // （收窄机制已内聚承接 C# ContinuePending.cs:83-123 重发收敛，预算降为
          // 对抗持续快写的有界上抛兜底，预算尽上抛形态与零缺席证据纪律不变）；
          // 同文件 drive_mem_read 的 INNER_LATCH_RETRY_BUDGET 尽先例（:299-301）
          // 同形
          return Err(Error::Index(WindexError::LockTimeout));
        }
        // MemRecheck::None：复检确认链头未滑入新磁盘地址或未越过已见上界（无新增
        // 信息，链真实走尽），落回 NotFound 出口——对标 ContinuePending.cs:37/:41
        // 真实走尽出口
        MemRecheck::None => {}
      }

      return Ok(None);
    }
  }

  /// 读中转层（[`RecordRead`] 面）：内存直读驱动环 + 磁盘冷读回退
  ///
  /// 公开入口与尺寸统计入口（`with_record_size` 注入）共用本中转，杜绝平行读链；
  /// `RETRY_LATER` 刷新重试收敛于 [`Self::drive_mem_read`] 单点
  async fn read_raw_with_reader<R>(
    &self,
    key: &[u8],
    reader: impl RecordRead<R>,
  ) -> Result<Option<R>> {
    let mut f = Some(reader);
    let cands = {
      let _guard = self.enter_gated();
      let (hash, first_addr) = self.read_probe(key)?;
      match self.drive_mem_read(key, hash, first_addr, &mut f)? {
        MemDrive::Done(res) => return Ok(res),
        MemDrive::OnDisk(cands) => cands,
      }
    };
    // SAFETY: drive_mem_read 返回 OnDisk 时闭包 f 未被消费，必为 Some
    let func = unsafe { f.take().unwrap_unchecked() };
    self.read_from_disk(key, cands, func).await
  }

  /// 底层无包装零拷贝读取物理键的值（Read Raw With Closure）
  ///
  /// 无 TTL 守卫的裸读内核：load_meta / ttl_of / contains_key_ignore_ttl 等已在上层
  /// 完成 TTL 裁决的调用链统一走此处，绝不嵌套二次裁决（读路径 TTL 探测收敛不变式：
  /// 同一同步调用链内同一用户键的 TTL 裁决只在唯一入口做一次）。
  ///
  /// - **同步内存直读快路径**：首先尝试纯同步内存零拷贝直读（`try_read_raw_in_memory`），
  ///   若在内存中精准命中或确认不存在（墓碑/无候选），纳秒级同步返回，彻底规避异步 Future 状态机开销。
  /// - **异步磁盘扫描回退**：仅当数据位于磁盘区时，才进入 `read_from_disk` 异步 I/O 等待。
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalReadAtAddress
  #[inline]
  pub async fn read_raw_with<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self.read_raw_with_reader(key, f).await
  }

  /// 底层无包装零拷贝读取物理键并披露记录物理尺寸（MEMORY USAGE 统计内核）
  #[inline]
  pub async fn read_raw_with_size<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<Option<R>> {
    self.read_raw_with_reader(key, with_record_size(f)).await
  }

  /// 带标签异步读中转单点（TTL 快门 + 标签键拼装，`read_tag_with` 两入口共用）
  async fn read_tag_with_reader<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    reader: impl RecordRead<R>,
  ) -> Result<Option<R>> {
    if !self.probe_alive(user_key).await? {
      return Ok(None);
    }
    let rec_k = self.session_tag_key(tag, user_key);
    self.read_raw_with_reader(&rec_k, reader).await
  }

  /// 零拷贝读取当前会话指定标签物理键的值（Read With Closure，带标签读内核）
  ///
  /// TTL 门控按用户键裁决（与数据记录标签无关），语义同 [`Self::read_with`]
  #[inline]
  pub async fn read_tag_with<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self.read_tag_with_reader(user_key, tag, f).await
  }

  /// 零拷贝读取指定标签物理键并披露记录物理尺寸（MEMORY USAGE 统计内核）
  ///
  /// TTL 门控与 [`Self::read_tag_with`] 一致；读全路径（内存直读 + 磁盘冷读）
  /// 均向闭包披露记录物理分配尺寸
  #[inline]
  pub async fn read_tag_with_size<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<Option<R>> {
    let reader = with_record_size(f);
    self.read_tag_with_reader(user_key, tag, reader).await
  }

  /// 零拷贝读取当前会话普通字符串键的值（Read With Closure）
  ///
  /// 命中后惰性探测 key 级 TTL 记录；已过期则经统一 DEL 路径物理清除并视同不存在
  /// （check_expired 内部仅走 raw 路径，无递归）。read/read_string/type_of/load_object
  /// 等入口均经此处获得惰性过期语义。
  /// f 可能带副作用（如直写响应缓冲），TTL 裁决必须前移到闭包执行前，
  /// 杜绝过期键"先执行读闭包后回 None"的双写；has_ttl_tag 为单次哈希探针，
  /// 无 TTL 记录时保持快路径零额外 I/O。本守卫是本键在整条同步调用链内的
  /// 唯一 TTL 裁决点，内部裸读（read_raw_with）不再重复探测
  #[inline]
  pub async fn read_with<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self.read_tag_with(user_key, KeyTag::String, f).await
  }

  /// 底层无包装物理读取（原始 Key-Value 读取）
  ///
  /// C# 上下文层 Read 入口族在本 rust 单点的折叠映射（多态上下文已被统一会话
  /// 消除，一臂承接全部变体；TTL 惰性裁决属宿主层，见 [`Self::read`]）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:Read
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:Read
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:Read
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalConsistentReadContext.cs:Read
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:Read
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:Read
  #[inline]
  pub async fn read_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_raw_with(key, |v| v.to_vec()).await
  }

  /// 读取当前会话普通字符串键的值（Read）
  ///
  /// - 优先内存驻留零拷贝探针：直接在页上比对键，命中时直接提取值切片，彻底消除整记录拷贝
  /// - 命中后经 read_with 惰性探测 TTL 记录，已过期物理清除并视同不存在
  #[inline]
  pub async fn read(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_with(user_key, |v| v.to_vec()).await
  }
}
