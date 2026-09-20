//! 物理键单点读取路径（对标 C# Garnet ClientSession 的 Read 快慢路径）

use std::ops::ControlFlow;

use wbase::{addr::is_read_cache, simd::fast_key_eq, time::now_ticks};
use wdev::Device;
use windex::{CandidateAddresses, HashBucketEntry};
use wrecord::record_size;
use wval::KeyTag;

use super::MemDrive;
use crate::{error::Result, read_cache::RcVisit, session::StoreSession, ttl::TtlGate};

/// 内存扫描中间动作（统一替代原有的 RcWalk/MemBack/ReadProbeResult/MemRead）
///
/// 精简状态机：合并了所有内存段（ReadCache、Immutable、Mutable）内探针与回溯的
/// 动作语义，消除过多胶水转换代码。
enum MemAction<R> {
  /// 匹配成功并提取值（对应 SUCCESS / Found）
  Done(Option<R>),
  /// 需要刷新纪元并重试（对应 RETRY_LATER）
  Retry,
  /// 续链下一地址，或 0 表示终止（对应 Miss(prev) / Stopped）
  Next(u64),
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
) -> MemAction<R> {
  if rec.matches_key(key) {
    if rec.is_closed() {
      MemAction::Retry
    } else if rec.is_tombstone() {
      MemAction::Done(None)
    } else {
      // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
      let func = unsafe { f.take().unwrap_unchecked() };
      MemAction::Done(Some(func.read_record(rec.value(), rec.physical_size())))
    }
  } else {
    // 发生 15 位 Tag 碰撞，沿反向链表回溯前驱版本（prev_address）
    MemAction::Next(rec.prev_address())
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
  #[inline]
  pub(super) fn drive_mem_read<R>(
    &self,
    key: &[u8],
    hash: u64,
    mut first_addr: Option<u64>,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemDrive<R>> {
    loop {
      if let ControlFlow::Break(res) = self.try_read_mem(key, hash, first_addr, f)? {
        return Ok(res);
      }
      // C# RETRY_LATER（InternalRead.cs:105-106）：刷新纪元（ProtectAndDrain 语义，
      // 推进密封在途记录的写者完成 CAS 解封）后整链重试
      self.participant.refresh();
      first_addr = self.reprobe_first_addr(hash);
    }
  }

  /// 读入口探测两连单点：单次哈希 + 首地址探针（对标 C# InternalRead 入口
  /// `OperationStackContext(keyHash)` 一次算定后 FindTag 装载）
  #[inline]
  fn read_probe(&self, key: &[u8]) -> (u64, Option<u64>) {
    let hash = whasher::fast_hash(key);
    (hash, self.reprobe_first_addr(hash))
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
    let (hash, first_addr) = self.read_probe(key);
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
    let (hash, first_addr) = self.read_probe(&rec_k);
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
    let (hash, first_addr) = self.read_probe(&rec_k);
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
  ) -> MemAction<R> {
    while is_read_cache(*curr) {
      if self
        .store
        .read_cache
        .need_to_wait_for_eviction(*curr, || self.participant.refresh())
      {
        return MemAction::Retry;
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
      match visit {
        RcVisit::Found(val) => return MemAction::Done(Some(val)),
        RcVisit::Next(prev) => *curr = prev,
        RcVisit::Gone => return MemAction::Retry,
      }
      if *curr == 0 {
        break;
      }
    }
    MemAction::Next(*curr)
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
  ) -> Result<MemAction<R>> {
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
        Some(MemAction::Done(Some(val))) => {
          if *curr < safe_ro_addr {
            self.promote_immutable_read_hit(*curr, key);
          }
          return Ok(MemAction::Done(Some(val)));
        }
        Some(MemAction::Done(None)) => return Ok(MemAction::Done(None)),
        Some(MemAction::Retry) => return Ok(MemAction::Retry),
        Some(MemAction::Next(next)) => {
          *curr = next;
          if next == 0 {
            return Ok(MemAction::Next(0));
          }
        }
        // 页换出竞态（head 推进瞬态）：curr 保留原址，交调用方候选扫描/磁盘口径处理
        None => return Ok(MemAction::Next(*curr)),
      }
    }
    Ok(MemAction::Next(*curr))
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
  ) -> Result<ControlFlow<MemDrive<R>, ()>> {
    let mut curr_addr = first_addr;
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
      curr_addr = self.reprobe_first_addr(hash);
    }
    let Some(mut curr_addr) = curr_addr else {
      // 哈希表中连对应 Tag 都完全不存在，100% 确认无此键，极速返回
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
    };

    let index = self.store.index.load();
    // 优化5（严格对标 Helpers.FindTagAndTryEphemeralSLock）：复活相关配置开启时，
    // traceback 前以共享 latch 锁住链首桶，防止复活/脱钩在回溯期间把记录从链上抽走
    // （"Ephemeral must lock the bucket before traceback"）；锁成功后重读当前链首槽位
    // （对标 HashEntryInfo.SetToCurrent），锁失败按 C# RETRY_LATER 协议交由调用方
    // 刷新纪元后整链重试。复活功能全关时无抽链风险，维持现状免锁（语义等价裁剪）
    let _s_latch = if self.ephemeral_lock_enabled() {
      let bucket = index.bucket(index.bucket_index_for_hash(hash));
      let Some(latch) = bucket.lock_shared_guard() else {
        return Ok(ControlFlow::Continue(()));
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
    //    与多候选扫描共用同一内核）
    match self.find_in_read_cache(key, &mut curr_addr, f) {
      MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
      MemAction::Retry => return Ok(ControlFlow::Continue(())),
      MemAction::Next(_) => {}
    }

    // 2. 内存常态快路径（99%+ 场景）：处于 HLog 内存驻留区，反向链表回溯单点
    //    （对标 TraceBackForKeyMatch + CopyFromImmutable，与多候选扫描共用）；
    //    Stopped 时 curr 已按链尽 0 / 磁盘区地址 / 竞态原址三口径就位，续步 3/4 处理
    if !is_read_cache(curr_addr) && curr_addr >= head_addr {
      match self.trace_back_for_key_match(
        key,
        &mut curr_addr,
        head_addr,
        ro_addr,
        safe_ro_addr,
        f,
      )? {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }
    }

    // 3. 链条已在内存区终结（curr_addr == 0 或已低于 begin_addr 截断边界）：
    //    严格对标 Tsavorite InternalRead.cs:142-157：
    //    若链条未伸入有效磁盘区（curr_addr < begin_addr），确认该键在整个存储中不存在，
    //    直接返回 MemDrive::Done(None)，彻底消除无效的多候选扫描与二次哈希遍历！
    if curr_addr == 0 || (!is_read_cache(curr_addr) && curr_addr < begin_addr) {
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
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
  ) -> Result<ControlFlow<MemDrive<R>, ()>> {
    let mut addrs = self.store.index.load().lookup_candidates_by_hash(hash);
    if addrs.is_empty() {
      // 无槽位候选（并发清退等瞬态）且内存链已伸入磁盘区：直读链出地址兜底；
      // 链仍在内存区或已穷尽为 0 则确认不存在
      if bounds.chain_disk_addr != 0 && bounds.chain_disk_addr >= bounds.begin_addr {
        let mut disk = CandidateAddresses::new();
        disk.push(bounds.chain_disk_addr);
        return Ok(ControlFlow::Break(MemDrive::OnDisk(disk)));
      }
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
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
      match self.find_in_read_cache(key, &mut cur_addr, f) {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }
      if cur_addr == 0 || cur_addr < bounds.begin_addr {
        continue;
      }

      // 主日志段：与主路径同一反向回溯单点（对标 TraceBackForKeyMatch +
      // CopyFromImmutable），Found 提升判定/safe_ro 快照口径两处合一
      match self.trace_back_for_key_match(
        key,
        &mut cur_addr,
        bounds.head_addr,
        ro_addr,
        bounds.safe_ro_addr,
        f,
      )? {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }

      if cur_addr != 0 && cur_addr >= bounds.begin_addr {
        disk_cands.push(cur_addr);
      }
    }

    if disk_cands.is_empty() {
      Ok(ControlFlow::Break(MemDrive::Done(None)))
    } else {
      Ok(ControlFlow::Break(MemDrive::OnDisk(disk_cands)))
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
    let _ = unsafe {
      self.store.hlog.with_immutable_record(addr, |rec| {
        if !rec.is_tombstone() && rec.matches_key(key) {
          if to_read_cache {
            self
              .store
              .read_cache
              .append(key, rec.value(), addr, &self.store.index.load());
          } else {
            // 尾部追加须取尾页写锁，绝不可嵌套在本记录的页读锁内：值出锁后落笔
            tail_val = Some(rec.value().to_vec());
          }
        }
        Ok(())
      })
    };
    let Some(val) = tail_val else {
      return;
    };
    // wantIO:false 内核：直连 hlog.append，PageNotReady（需驱逐刷盘）即放弃本次
    // 晋升；旁路写监听——帧内容为已存在的旧值，属物理布局优化而非用户写效果，
    // 镜像入 AOF 会在并发写下造成恢复回退（与磁盘回填臂 append_record_compacted
    // 同口径，见其注释）
    if let Ok(new_addr) = self.store.hlog.append(key, &val, addr, false) {
      self.cas_mount_copied_frame(
        key,
        addr,
        new_addr,
        record_size(key.len(), val.len()) as u32,
      );
    }
  }

  /// 异步磁盘回退路径（仅当内存阶段确认存在磁盘候选地址时调用）
  ///
  /// - 候选列表由 `try_read_mem_fallback` 单遍索引扫描收集并降序透传，此处零重复索引遍历；
  /// - 磁盘 I/O 期间不持有纪元守卫（对标 C# IO 期间 `UnsafeSuspendThread`），
  ///   彻底消除单次冷读阻塞纪元推进与页回收的尾延迟隐患；
  /// - 读取走 `read_disk_record` 纯设备路径（不触碰内存页缓冲），无守卫读取安全性成立；
  /// - 磁盘链回溯：Tag 碰撞键不匹配时沿记录 `prev_address` 前驱链继续冷读
  ///   （严格对照 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncGetFromDiskCallback "skips colliding keys by
  ///   following the .PreviousAddress chain"；链地址在磁盘区内严格单调下降，
  ///   直至链尽 0 或低于截断线 begin_address）。
  #[cold]
  pub(super) async fn read_from_disk<R>(
    &self,
    key: &[u8],
    addrs: CandidateAddresses,
    f: impl RecordRead<R>,
  ) -> Result<Option<R>> {
    let mut f = Some(f);
    for &addr in addrs.iter() {
      let mut cur = addr;
      while cur >= self.store.begin_address() {
        let record = match self.store.hlog.read_disk_record(cur).await {
          Ok(r) => r,
          Err(e) => {
            if cur < self.store.begin_address() {
              break;
            }
            return Err(e.into());
          }
        };

        // 单次解析零拷贝记录视图，键比对 / 墓碑判定 / 值提取 / 物理尺寸共用
        //（消除逐字段重复头解析；RecordOutput 缓冲即完整记录映像）
        let Ok(rec) = record.as_record_ref() else {
          // 头解析失败（截断/损坏）：按链终止口径处理（对齐原 from_slice 失败回 0）
          cur = 0;
          continue;
        };
        if !fast_key_eq(rec.key(), key) {
          // Tag 碰撞：提取前驱版本地址，磁盘链回溯
          cur = rec.prev_address();
          continue;
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
        // 晋升帧走紧缩搬迁同款旁路写监听（append_record_compacted）：帧内容为已存在的
        // 旧值，属物理布局优化而非用户写效果，镜像入 AOF 会在并发写下造成恢复回退
        // （旧值帧晚于并发新值帧入队，重放序错乱，见 append_record_compacted 注释）
        if self.store.read_cache.is_enabled {
          let _guard = self.enter_gated();
          // append 内部完成索引 CAS 挂载（对标 TryCopyToReadCache 的 hei.TryCAS），
          // 挂载失败（并发写已 detach）静默放弃，环形覆盖自然回收
          self
            .store
            .read_cache
            .append(key, val_slice, cur, &self.store.index.load());
        } else if self.copy_reads_to_tail()
          && let Ok(new_addr) = self
            .append_record_compacted(key, val_slice, cur, false)
            .await
        {
          self.cas_mount_copied_frame(
            key,
            cur,
            new_addr,
            record_size(key.len(), val_slice.len()) as u32,
          );
        }

        return Ok(Some(result));
      }
    }

    Ok(None)
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
      let (hash, first_addr) = self.read_probe(key);
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
    if !self.probe_alive(user_key).await? {
      return Ok(None);
    }
    let rec_k = self.session_tag_key(tag, user_key);
    self.read_raw_with(&rec_k, f).await
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
    if !self.probe_alive(user_key).await? {
      return Ok(None);
    }
    let rec_k = self.session_tag_key(tag, user_key);
    self.read_raw_with_size(&rec_k, f).await
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
