//! 混合日志在线紧缩器（对标 C# Tsavorite Compaction: Lookup 与 Scan 策略）
//!
//! 与 C# Garnet / Tsavorite 的对应关系：
//! - [`CompactionType`] ← `Tsavorite.core.CompactionType` (Lookup, Scan)
//! - [`LogCompactor::compact`] ← `TsavoriteKV.Compact`（默认非删除过滤，对标 `DefaultCompactionFunctions`）
//! - [`LogCompactor::compact_with_filter`] ← `TsavoriteKV.Compact` 带 `ICompactionFunctions`
//! - `compact_lookup` ← `TsavoriteKV.CompactLookup`（`iter1.GetNext()` → cursor）
//! - `compact_scan` ← `TsavoriteKV.CompactScan`
//! - `conditional_copy_to_tail` ← `TsavoriteKV.CompactionConditionalCopyToTail`
//!
//! 与 C# 的刻意差异：
//! - 扫描脚手架（页尾残片、PadRecord 换页、磁盘单页预取、可复用拉取缓冲）全部委托
//!   whlog [`ScanIterator`]，本 crate 只消费拉取结果，不重复实现遍历逻辑；
//! - Scan 模式以单趟候选哈希表替代 C# 的临时 TsavoriteKV 实例：空间 O(唯一键) 元数据、
//!   值体仅在确认存活时按需回读一次，省去临时日志的全部分配与 I/O；
//! - C# 靠 `minAddress` 边界搜索 + 临时 KV 删除判定存活，此处直接校验哈希索引现值并以
//!   CAS 原子替换收尾，并发保护等价且免版本链遍历；
//! - C# `ConditionalCopyToTail` 采用 `while (true)` 无界重试，此处为有界重试 + 记录级
//!   保守保留：竞争重试耗尽且记录仍为最新存活版本时（`CopyOutcome::Retain`），仅该
//!   记录的截断点下界回退至其起始边界，扫描照常推进，最终截断点取全部保留记录的最小
//!   起始边界——有界工作量、绝不误删存活数据，且单点竞争不阻断区间推进；
//! - C# Scan 的 `ScanImmutableTailToRemoveFromTempKv(ref untilAddress, ...)` 仅把改写后的
//!   `untilAddress` 用作阶段 3 探测下界（minAddress），最终 `ShiftBeginAddress` 仍取阶段 1
//!   的记录边界 `originalUntilAddress`；此处与之一致，截断点同样止步于紧缩区间记录边界，
//!   并额外受保守保留回退（`CopyOutcome::Retain`）收紧（宁可多保留，绝不误删）；
//! - 紧缩上界以 `safe_read_only_address`（SafeReadOnlyAddress）为界，属内核自保
//!   （TsavoriteCompaction.cs:35,72 同款入口硬校验）：模糊区 [safe_ro, read_only) 按
//!   可变区处理（InternalRead.cs "Mutable region (even fuzzy region is included
//!   here)"），在途原位写仅持页锁复验 read_only 后落笔，紧缩绝不触达该区间，
//!   亦不依赖调用方做任何排空/封印配合。

mod probe;
mod run;

use std::sync::Arc;

use log::info;
use parking_lot::Mutex;

use crate::{
  error::{Error, Result},
  host::{CompactStore, CompactionFunctions, DefaultCompactionFunctions},
};

/// CAS 迁移竞争重试上限（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ConditionalCopyToTail.cs:ConditionalCopyToTail 的 `while (true)` 重试环；
/// 良性竞争需在纳秒级 CAS 窗口内连续命中 8 次，概率趋近零。与 C# 的刻意差异：
/// 耗尽时不无界重试也不弃迁存活数据，而是以 [`CopyOutcome::Retain`] 做记录级保守
/// 保留——截断点下界回退至该记录起始边界，紧缩扫描继续推进——有界工作量与
/// 绝不误删并存）
const CAS_COPY_RETRIES: usize = 8;

/// 条件迁移结果（细分 C# `ConditionalCopyToTail` 的 bool 返回形态）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyOutcome {
  /// 迁移成功：尾部副本已生效，索引已原子替换至新地址
  Copied,
  /// 真并发覆盖/删除已生效，记录被新版本取代：旧版本安全放弃迁移
  Superseded,
  /// CAS 竞争重试耗尽且记录仍为最新存活版本：绝不随截断丢弃，
  /// 调用方必须将截断点回退至该记录起始边界（含），剩余区间留给下一轮紧缩
  Retain,
}

/// 混合日志紧缩策略模式（对标 C# `Tsavorite.core.CompactionType`）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionType {
  /// 索引探针模式（对标 Tsavorite `CompactionType.Lookup`）
  ///
  /// 顺序扫描紧缩范围内的记录，逐条通过哈希索引探查最新记录地址以判定是否存活。
  Lookup,

  /// 扫描去重模式（对标 Tsavorite `CompactionType.Scan`）
  ///
  /// 扫描紧缩范围建立候选集合，通过只读区扫描批量消除过期版本，降低哈希索引探针开销。
  Scan,
}

/// 日志紧缩统计报告
///
/// 对照差异：C# Tsavorite Compaction 无统计返回面（`Compact` 仅返回 untilAddress），
/// 本结构为 wedb 扩展。字段口径满足精确守恒：
/// `scanned_records == live_copied + superseded + dead_dropped + retained`
/// （Scan 模式经阶段 1 区间内替换计数与阶段 2 候选剔除计数保证；截断点被
/// 保守保留回退收紧时，floor 之上的记录照常计入已处理的各自桶中）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactionStats {
  /// 扫描到的总记录数（不含换页 Pad 标记；Scan 模式仅计阶段 1 区间）
  pub scanned_records: usize,
  /// 判定为存活并成功复制追加到尾部的记录数
  pub live_copied: usize,
  /// 因并发更新/删除已产生更新版本而弃迁的记录数（含区间内被更新版本替换的
  /// 历史版本、阶段 2 剔除与 CAS 复核确认被覆盖三类；索引指向新版本，旧版本随截断退役）
  pub superseded: usize,
  /// 判死丢弃数（墓碑、用户过滤谓词、TTL 过期/孤儿三通道），
  /// 索引陈旧引用已同步清理
  pub dead_dropped: usize,
  /// CAS 竞争预算耗尽且复核仍为最新存活版本的保守保留数
  /// （截断点回退至其起始边界，区间留给下一轮紧缩）
  pub retained: usize,
  /// 推进释放的日志字节数
  pub bytes_freed: u64,
  /// 紧缩完成后新的有效起始逻辑地址
  pub new_begin_address: u64,
}

impl CompactionStats {
  /// 是否未紧缩任何记录
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.scanned_records == 0
  }
}

/// 单次紧缩执行内部结果
struct CompactRunResult {
  scanned_records: usize,
  live_copied: usize,
  superseded: usize,
  dead_dropped: usize,
  retained: usize,
  actual_until: u64,
}

/// 混合日志物理紧缩器
pub struct LogCompactor<S: CompactStore> {
  store: Arc<S>,
  /// CAS 迁移竞争重试上限（生产默认 [`CAS_COPY_RETRIES`]）
  cas_retries: usize,
  /// 探针确定性扩容窗注入钩（wkv 读面 `test_read_gap_hook` 同族测试留钩，
  /// 逐紧缩器实例挂载杜绝跨测试串扰，生产恒 None）：grow 迁移窗内探针协同门
  /// 仍采得空候选即同相位不变式被破坏的注入点，钩在采样与判活分派的间隙内
  /// 确定性完成全量迁移并翻回 Rest，复现「陈旧空候选被直判 superseded 弃迁」
  /// 的跨阶段 TOCTOU；协同门在位时钩永不触发
  probe_gap_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl<S: CompactStore> Clone for LogCompactor<S> {
  fn clone(&self) -> Self {
    Self {
      store: Arc::clone(&self.store),
      cas_retries: self.cas_retries,
      probe_gap_hook: Mutex::new(None),
    }
  }
}

impl<S: CompactStore> LogCompactor<S> {
  /// 创建混合日志紧缩器
  pub fn new(store: Arc<S>) -> Self {
    Self {
      store,
      cas_retries: CAS_COPY_RETRIES,
      probe_gap_hook: Mutex::new(None),
    }
  }

  /// 自定义 CAS 竞争重试预算（0 = 不迁移即保守保留，确定性触发截断点回退路径）
  #[inline]
  pub fn with_cas_retries(store: Arc<S>, cas_retries: usize) -> Self {
    Self {
      store,
      cas_retries,
      probe_gap_hook: Mutex::new(None),
    }
  }

  /// 挂载探针扩容窗注入钩（仅集成测试基础设施，生产宿主零使用）
  #[doc(hidden)]
  pub fn arm_probe_gap_hook<F: FnOnce() + Send + 'static>(&self, hook: F) {
    *self.probe_gap_hook.lock() = Some(Box::new(hook));
  }

  /// 执行混合日志在线紧缩（业务过滤经泛型注入，对标 Tsavorite `TsavoriteKV.Compact<TCompactionFunctions>`
  /// 与 `ICompactionFunctions`；libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:Compact）
  ///
  /// 流程：
  /// 1. 校验 `until_address <= store.hlog().safe_read_only_address()`（内核自保硬拒，
  ///    对标 TsavoriteCompaction.cs:35,72 "Can compact only until Log.SafeReadOnlyAddress"，
  ///    模糊区 [safe_ro, read_only) 内的在途原位写因此绝不被截断/架空）；
  /// 2. 顺序扫描从 `store.hlog().begin_address()` 到 `until_address` 的全部日志记录；
  /// 3. 对每条记录：
  ///    - 若为墓碑（tombstone）或被 `cf.is_deleted` 业务谓词判定为失效，直接丢弃，并清理索引中的陈旧引用；
  ///    - 若非墓碑，在 store 中探查当前 key 对应的最新记录地址。如果最新记录地址恰好等于
  ///      该记录地址（说明该版本仍存活未被覆盖），则通过 `conditional_copy_to_tail` 原子 CAS 迁移写入 Tail，
  ///      并发写入若先发生则放弃旧记录迁移，绝不错误覆盖并发新版本数据；
  /// 4. 紧缩完成后，更新 `store.hlog().shift_begin_address(actual_until)`（推进 begin_address），
  ///    由底层自动物理回收旧段文件，并显式调度复活池清理失效槽位。
  pub async fn compact_with_filter<C>(
    &self,
    until_address: u64,
    comp_type: CompactionType,
    cf: &C,
  ) -> Result<CompactionStats>
  where
    C: CompactionFunctions<S>,
  {
    // 内核自保硬校验（对标 TsavoriteCompaction.cs:35-36/:72-73：`if (untilAddress >
    // hlogBase.SafeReadOnlyAddress) throw`）：模糊区 [safe_ro, read_only) 内存在复验于
    // 封区之前的在途原位写（whlog inplace 以 read_only 为准入边界、ro 推进不取页锁），
    // 越过 safe_ro 的紧缩会把旧值副本 CAS 顶掉索引并随 begin 推进物理截断在途新值
    // （丢更新/已删键复活），故宁可硬拒不静默钳制，令调用方以 safe_ro 同源计算上界
    let safe_ro_addr = self.store.safe_read_only_address();
    if until_address > safe_ro_addr {
      return Err(Error::UntilAddressOutOfRange {
        until_address,
        safe_read_only_address: safe_ro_addr,
      });
    }

    let old_begin = self.store.begin_address();
    if until_address <= old_begin {
      return Ok(CompactionStats {
        new_begin_address: old_begin,
        ..CompactionStats::default()
      });
    }

    let session = self.store.new_session()?;

    let run_res = match comp_type {
      CompactionType::Lookup => {
        self
          .compact_lookup(&session, old_begin, until_address, safe_ro_addr, cf)
          .await?
      }
      CompactionType::Scan => {
        self
          .compact_scan(&session, old_begin, until_address, safe_ro_addr, cf)
          .await?
      }
    };

    // 推进 begin_address（底层 hlog 截断设备旧段文件；whlog 契约：async，
    // 自动补刷 [flushed, new_begin) 且无法落盘时返回 InvalidState——
    // 调用侧若先推进只读边界并排空纪元，可免除补刷开销）。
    // 复用 store 封装：截断成功后随按需门控调度复活池 purge_below，
    // 清理低于新起始地址的失效槽位，防止冷槽位长期滞留分桶
    self.store.shift_begin_address(run_res.actual_until).await?;

    let new_begin_address = self.store.begin_address();
    let bytes_freed = run_res.actual_until.saturating_sub(old_begin);

    info!(
      "紧缩完成: 类型={comp_type:?}, 扫描={}, 迁移={}, 弃迁={}, 判死={}, 保留={}, 释放={bytes_freed}B, 新起始地址={new_begin_address:#x}",
      run_res.scanned_records,
      run_res.live_copied,
      run_res.superseded,
      run_res.dead_dropped,
      run_res.retained
    );

    Ok(CompactionStats {
      scanned_records: run_res.scanned_records,
      live_copied: run_res.live_copied,
      superseded: run_res.superseded,
      dead_dropped: run_res.dead_dropped,
      retained: run_res.retained,
      bytes_freed,
      new_begin_address,
    })
  }

  /// 周期惰性紧缩便捷入口（wedb 自有入口，非 Garnet 对标）
  ///
  /// Garnet 的周期紧缩（`DatabaseManagerBase.DoCompactionAsync`）为触发式回退语义：
  /// `read_only - begin > seg×max` 时 `until = read_only − seg×(max−n)` 自只读端回退，
  /// 该语义由 wkv gc.rs 实现。本入口是与之互补的 wedb 特有便捷路径：以 `max_seek_bytes`
  /// 约束单轮自 begin 前推的扫描预算，适配渐进式后台消化。
  ///
  /// 行为约定：
  /// - 紧缩区间为 `[begin_address, min(safe_read_only_address, begin_address + max_seek_bytes))`
  ///   （上界与内核校验同源取 safe_ro：safe_ro 单调不回退，恒过 [`Self::compact_with_filter`]
  ///   前置硬校验；模糊区在途原位写不被触达）；`until` 落在记录中部时由
  ///   [`Self::compact_with_filter`] 自动对齐到记录边界；
  /// - `begin_address >= safe_read_only_address`（定稿区无待紧缩区间）或 `max_seek_bytes == 0`
  ///   （单轮推进上限为 0，等价紧缩关闭）时直接返回空统计，零 I/O 零分配开销；
  /// - 固定采用 [`CompactionType::Lookup`]：逐条哈希索引探查判定存活（Garnet 生产环境
  ///   推荐策略），无 Scan 模式的候选表内存开销，适合无人值守的周期后台执行。
  pub async fn compact_lazy(&self, max_seek_bytes: u64) -> Result<CompactionStats> {
    let begin_addr = self.store.begin_address();
    let safe_ro_addr = self.store.safe_read_only_address();
    if begin_addr >= safe_ro_addr || max_seek_bytes == 0 {
      // 无可紧缩区间或单轮上限为 0：空统计直返（回报当前起始地址，零 I/O）
      return Ok(CompactionStats {
        new_begin_address: begin_addr,
        ..CompactionStats::default()
      });
    }
    // 饱和加法防溢出；区间末端对齐记录边界由 compact_with_filter 内部处理
    let until_address = safe_ro_addr.min(begin_addr.saturating_add(max_seek_bytes));
    self.compact(until_address, CompactionType::Lookup).await
  }

  /// 执行混合日志在线紧缩（默认无业务过滤，对标 Tsavorite `DefaultCompactionFunctions` 恒 false；
  /// 生产入口应经宿主封装注入业务谓词，对标 DatabaseManagerBase.cs:449 注入 GarnetRecordTriggers）
  #[inline]
  pub async fn compact(
    &self,
    until_address: u64,
    comp_type: CompactionType,
  ) -> Result<CompactionStats> {
    self
      .compact_with_filter(until_address, comp_type, &DefaultCompactionFunctions)
      .await
  }
}
