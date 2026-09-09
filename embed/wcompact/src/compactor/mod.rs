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
//! - 紧缩边界以 `read_only_address`（ReadOnlyAddress）为界，未用 C# 的 SafeReadOnlyAddress
//!   （TsavoriteCompaction.cs:35,72,108）：依赖调用方保证紧缩窗口内无与封印/驱逐并发的
//!   在途写入（whlog 的 safe 边界滞后一个纪元排空，窗口内 `[begin, until)` 可含半截记录）；
//! - 附加 wedb 方案 A 语义：紧缩途中单调回放集合元数据水位（`MetaDeathScope`），
//!   并按集合版本淘汰已删集合或过期版本的历史子键（`is_stale_subkey`，对标 Fast Drop）；
//!   收尾时对死亡记录已完整落入紧缩区间的集合回收 key_id_versions 死条目，防止高删除
//!   负载下内存无限增长。

mod copy;
mod cursor;
mod judge;
mod probe;
mod run;
mod scope;

use std::sync::Arc;

use log::info;

use crate::{
  error::{Error, Result},
  host::CompactStore,
};

/// CAS 迁移竞争重试上限（对标 C# `ConditionalCopyToTail` 的 `while (true)` 重试环；
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
  /// 判死丢弃数（墓碑、用户过滤谓词、集合历史子键淘汰、TTL 过期/孤儿四通道），
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
  /// 扫描作用域内的集合元数据归属与 key_id 死亡登记（收尾时据此回收死条目）
  scope: scope::MetaDeathScope,
}

/// 混合日志物理紧缩器
pub struct LogCompactor<S: CompactStore> {
  store: Arc<S>,
  /// CAS 迁移竞争重试上限（生产默认 [`CAS_COPY_RETRIES`]）
  cas_retries: usize,
}

impl<S: CompactStore> Clone for LogCompactor<S> {
  fn clone(&self) -> Self {
    Self {
      store: Arc::clone(&self.store),
      cas_retries: self.cas_retries,
    }
  }
}

impl<S: CompactStore> LogCompactor<S> {
  /// 创建混合日志紧缩器
  pub fn new(store: Arc<S>) -> Self {
    Self {
      store,
      cas_retries: CAS_COPY_RETRIES,
    }
  }

  /// 自定义 CAS 竞争重试预算（0 = 不迁移即保守保留，确定性触发截断点回退路径）
  #[inline]
  pub fn with_cas_retries(store: Arc<S>, cas_retries: usize) -> Self {
    Self { store, cas_retries }
  }

  /// 执行混合日志在线紧缩（支持自定义过滤判定，对标 Tsavorite `ICompactionFunctions`）
  ///
  /// 流程：
  /// 1. 校验 `until_address <= store.hlog().read_only_address()`；
  /// 2. 顺序扫描从 `store.hlog().begin_address()` 到 `until_address` 的全部日志记录；
  /// 3. 对每条记录：
  ///    - 若为墓碑（tombstone）或被 `is_deleted` 自定义函数判定为失效，直接丢弃，并清理索引中的陈旧引用；
  ///    - 若非墓碑，在 store 中探查当前 key 对应的最新记录地址。如果最新记录地址恰好等于
  ///      该记录地址（说明该版本仍存活未被覆盖），则通过 `conditional_copy_to_tail` 原子 CAS 迁移写入 Tail，
  ///      并发写入若先发生则放弃旧记录迁移，绝不错误覆盖并发新版本数据；
  /// 4. 紧缩完成后，更新 `store.hlog().shift_begin_address(actual_until)`（推进 begin_address），
  ///    由底层自动物理回收旧段文件，并显式调度复活池清理失效槽位。
  pub async fn compact_with_filter<F>(
    &self,
    until_address: u64,
    comp_type: CompactionType,
    is_deleted: F,
  ) -> Result<CompactionStats>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let read_only_addr = self.store.read_only_address();
    if until_address > read_only_addr {
      return Err(Error::UntilAddressOutOfRange {
        until_address,
        read_only_address: read_only_addr,
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
          .compact_lookup(
            &session,
            old_begin,
            until_address,
            read_only_addr,
            is_deleted,
          )
          .await?
      }
      CompactionType::Scan => {
        self
          .compact_scan(
            &session,
            old_begin,
            until_address,
            read_only_addr,
            is_deleted,
          )
          .await?
      }
    };

    // 推进 begin_address（底层 hlog 截断设备旧段文件；whlog 契约：async，
    // 自动补刷 [flushed, new_begin) 且无法落盘时返回 InvalidState——
    // 调用侧若先推进只读边界并排空纪元，可免除补刷开销）。
    // 复用 store 封装：截断成功后随按需门控调度复活池 purge_below，
    // 清理低于新起始地址的失效槽位，防止冷槽位长期滞留分桶
    self.store.shift_begin_address(run_res.actual_until).await?;

    // 紧缩收尾：安全回收本次扫描区间内已完整死亡的 key_id_versions 元数据条目，
    // 防止高删除负载下内存无限增长（健全性论证见 MetaDeathScope 文档）
    run_res
      .scope
      .collect_dead(&*self.store, run_res.actual_until);

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
  /// - 紧缩区间为 `[begin_address, min(read_only_address, begin_address + max_seek_bytes))`；
  ///   `until` 落在记录中部时由 [`Self::compact_with_filter`] 自动对齐到记录边界；
  /// - `begin_address >= read_only_address`（只读区无待紧缩区间）或 `max_seek_bytes == 0`
  ///   （单轮推进上限为 0，等价紧缩关闭）时直接返回空统计，零 I/O 零分配开销；
  /// - 固定采用 [`CompactionType::Lookup`]：逐条哈希索引探查判定存活（Garnet 生产环境
  ///   推荐策略），无 Scan 模式的候选表内存开销，适合无人值守的周期后台执行。
  pub async fn compact_lazy(&self, max_seek_bytes: u64) -> Result<CompactionStats> {
    let begin_addr = self.store.begin_address();
    let read_only_addr = self.store.read_only_address();
    if begin_addr >= read_only_addr || max_seek_bytes == 0 {
      // 无可紧缩区间或单轮上限为 0：空统计直返（回报当前起始地址，零 I/O）
      return Ok(CompactionStats {
        new_begin_address: begin_addr,
        ..CompactionStats::default()
      });
    }
    // 饱和加法防溢出；区间末端对齐记录边界由 compact_with_filter 内部处理
    let until_address = read_only_addr.min(begin_addr.saturating_add(max_seek_bytes));
    self.compact(until_address, CompactionType::Lookup).await
  }

  /// 执行混合日志在线紧缩（对标 Tsavorite `DefaultCompactionFunctions`）
  #[inline]
  pub async fn compact(
    &self,
    until_address: u64,
    comp_type: CompactionType,
  ) -> Result<CompactionStats> {
    self
      .compact_with_filter(until_address, comp_type, |_, _| false)
      .await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn compaction_stats_default_and_is_empty() {
    let stats = CompactionStats::default();
    assert!(stats.is_empty());
    assert_eq!(stats.scanned_records, 0);
    assert_eq!(stats.live_copied, 0);
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(stats.bytes_freed, 0);
  }
}
