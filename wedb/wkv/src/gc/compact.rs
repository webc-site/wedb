//! 日志紧缩域：死亡虚拟 ID 队列高低水位迟滞熔断判定
//! （[`GcManager::refresh_compact_boost`]）与阈值紧缩推进
//! （[`GcManager::try_compact`]，经 [`WedbStore::compact`] 注入活性校验档）。
//! 由换号物理回收内核 [`GcManager::reclaim_physical`] 驱动，总述见门面件。

use std::sync::{Arc, atomic::Ordering::Relaxed};

use log::info;
use wbase::cfg::LogCompactionType;
use wcompact::CompactionType;
use wdev::Device;

use super::GcManager;
use crate::{config::GcConfig, error::Result, store::WedbStore};

impl<D: Device> GcManager<D> {
  /// 死亡虚拟 ID 队列积压水位判定（迟滞双水位状态机，落地 doc/zh/db.md
  /// 「高低水位熔断」承诺）。
  ///
  /// 积压口径为 `vdb.gc_dead` 长度：每个死亡条目对应一次换号（FLUSHDB/FLUSHNS）
  /// 遗留的整库废弃记录，须待紧缩推进 begin 越过其 `tail_address` 方可摘除，
  /// 是「旧垃圾积压」的直接度量。
  ///
  /// 状态转移：
  /// - 积压 > 高水位 → 置位（进入熔断加速）；
  /// - 积压 <= 低水位 → 清位（退出加速）；
  /// - 迟滞死区内维持原态，防临界振荡；低水位配置倒挂时按高水位钳制。
  ///
  /// 高水位为 0 视作熔断关闭（恒清位，行为同无水位判定）。返回是否处于加速态
  fn refresh_compact_boost(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> bool {
    let hi = cfg.gc_dead_high_watermark;
    if hi == 0 {
      self.compact_boost.store(false, Relaxed);
      return false;
    }
    let lo = cfg.gc_dead_low_watermark.min(hi);
    let backlog = store.vdb.gc_dead.len();
    let boosting = if backlog > hi {
      true
    } else if backlog <= lo {
      false
    } else {
      // 迟滞死区：维持原态
      self.compact_boost.load(Relaxed)
    };
    self.compact_boost.store(boosting, Relaxed);
    boosting
  }

  /// 日志紧缩调度（对标 CompactionTask / DatabaseManagerBase.cs:425 DoCompactionAsync）
  ///
  /// libs/server/StoreWrapper.cs:CompactionTaskAsync
  /// libs/server/Databases/IDatabaseManager.cs:DoCompactionAsync
  /// libs/server/Databases/DatabaseManagerBase.cs:DoCompactionAsync
  /// libs/server/Databases/SingleDatabaseManager.cs:DoCompactionAsync
  ///（C# 周期任务循环 + 管理器抽象/实现三层入口：rust 无独立 CompactionTask
  /// 线程，紧缩调度随内置 GC 物理回收轮次推进——doc/zh/db.md「紧缩回收
  /// (Compaction-Driven GC)」；周期性由 GcManager 循环承担，本调度即每轮的
  /// DoCompactionAsync 等价体）
  ///
  /// None 档短路（对标 DatabaseManagerBase.cs:427 `if compactionType == None return`）；
  /// 触发条件 `safe_ro - begin > max_segments × segment_size`；回退量
  /// `until = safe_ro - segment_size × (max - n)`（n 为回退段数：常规档恒取 C#
  /// 调用点同款字面量 1（DatabaseManagerBase.cs:379 段数不可配，仅 maxSegments 与
  /// compactionType 有配置源），熔断档取 max），保证 `until <= safe_ro` 满足
  /// 紧缩器前置硬校验。
  ///
  /// 上界口径（阈值与回退同源单选 safe_read_only_address，不混用双界）：
  /// C# 驱动以 ReadOnlyAddress 度量与回退、内核以 SafeReadOnlyAddress 硬拒
  /// （DatabaseManagerBase.cs:444/:450 与 TsavoriteCompaction.cs:35-36/:72-73），
  /// 两界间的模糊区在途原位写若被紧缩触达即丢更新/已删键复活；此处单源取
  /// safe_ro——语义为「定稿区积压」（唯一可紧缩域），且 safe_ro 单调不回退保证
  /// 计算出的 until 恒过内核校验，杜绝 C# 两界口径分叉下内核抛异常的窗口。
  ///
  /// 档位分派（对标 DatabaseManagerBase.cs:438 switch）：
  /// - Shift：[`WedbStore::shift_begin_address`] 不搬记录直接推进 begin（数据丢弃档，
  ///   对标 C# `ShiftBeginAddress(untilAddress, true, …)`）；回退段数钳制 max-1，
  ///   until 恒低于安全只读线至少一段——C# 回退段数字面量 1 的同款保守，
  ///   杜绝把全部活记录移位丢弃；
  /// - Lookup / Scan：[`WedbStore::compact`] 活性校验紧缩。
  ///
  /// 高低水位熔断：死亡虚拟 ID 队列积压超高水位时旁路 None 短路（以 Lookup 活性校验
  /// 档执行，Shift 不判活会误杀），且回退段数提升至 max（until 推进到 safe_ro，
  /// 单轮全速紧缩全部积压段），加速 begin 越过死亡条目 `tail_address`，防止换号旧
  /// 垃圾积压膨胀磁盘；回落低水位以下恢复常规单步回退。换号物理回收是
  /// doc/zh/db.md「偏序 GC 屏障 + 高低水位熔断」承诺的安全机制，不受
  /// compaction_type 旋钮关闭。
  ///
  /// 判定节奏无第二层节流（C# `DoCompactionAsync` 内亦无）：本方法随物理回收内核
  /// [`Self::reclaim_physical`] 由常驻 [`spawn_bftree_reclaimer`] 兜底驱动，
  /// `gc.enabled` 关闭态亦每 [`RELEASE_POLL_MS`] 评估一次水位，不再随扫描开关整体停摆
  pub(super) async fn try_compact(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<()> {
    // 水位判定先行：熔断态须旁路下方 None 短路，故不得延后
    let boosting = self.refresh_compact_boost(store, cfg);
    self.stats.compact_boosting.store(boosting, Relaxed);

    // None 档短路：常规阈值紧缩关闭（熔断态旁路继续往下走旁路紧缩）
    if !boosting && cfg.compaction_type == LogCompactionType::None {
      return Ok(());
    }

    let begin = store.begin_address();
    // 上界单源 safe_ro（阈值度量与推进上界同一口径，论证见方法文档）；
    // 紧缩链上不再以 Unsafe read_only 作任何上界判据
    let safe_ro = store.safe_read_only_address();
    // segment_size：取设备段大小（物理回收单元）
    let seg = store.device.segment_size();
    let max = cfg.compaction_max_segments as u64;
    // 未超阈值（或阈值/段长为 0 视作紧缩关闭）：不动日志（熔断亦不空转——
    // 积压段未超限时紧缩本无可推进空间，死亡条目等 begin 自然推进即可摘除）
    if seg == 0 || max == 0 || safe_ro.saturating_sub(begin) <= max.saturating_mul(seg) {
      return Ok(());
    }
    // 回退段数：熔断加速取 max（until = safe_ro，全速紧缩）；常规档恒 1——
    // C# DatabaseManagerBase.cs:379 调用点字面量同款，段数在 C# 不设配置项
    let n = if boosting { max } else { 1 };
    let until = safe_ro
      .saturating_sub(seg.saturating_mul(max - n))
      .max(begin);

    // Shift 档：回退段数钳制 max-1，until 恒低于安全只读线至少一段（不搬记录，
    // 移位越过的活记录将丢失，绝不外推到安全只读线——更不触达其上的模糊区）
    if cfg.compaction_type == LogCompactionType::Shift {
      let shift_n = n.min(max - 1);
      let shift_until = safe_ro
        .saturating_sub(seg.saturating_mul(max - shift_n))
        .max(begin);
      store.shift_begin_address(shift_until).await?;
      self.stats.compactions.fetch_add(1, Relaxed);
      self.stats.last_compact_dropped.store(0, Relaxed);
      info!(
        "内置 GC 移位推进: until={shift_until:#x}, 新起始地址={:#x}, 熔断加速={boosting}（Shift 档不搬记录，设备历史段物理回收）",
        store.begin_address()
      );
      return Ok(());
    }
    // None + 熔断：旁路档归一为 Lookup（活性校验，见方法文档）；Lookup/Scan 为显式档
    let tier = match cfg.compaction_type {
      LogCompactionType::Scan => CompactionType::Scan,
      _ => CompactionType::Lookup,
    };
    let outcome = store.compact(until, tier).await?;
    self.stats.compactions.fetch_add(1, Relaxed);
    self
      .stats
      .last_compact_dropped
      .store(outcome.dead_dropped as u64, Relaxed);
    info!(
      "内置 GC 紧缩完成: until={until:#x}, 档位={}, 丢弃={}, 释放={}B, 新起始地址={:#x}, 熔断加速={boosting}（推进 begin 后由设备 truncate_until_address 物理回收已回收段）",
      cfg.compaction_type.as_name(),
      outcome.dead_dropped,
      outcome.bytes_freed,
      outcome.new_begin_address
    );
    Ok(())
  }
}
