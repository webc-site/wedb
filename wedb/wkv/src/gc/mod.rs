//! 内置 GC：主动过期清理与日志紧缩的后台引擎（对标 Garnet ExpiredKeyDeletionTask + CompactionTask）。
//!
//! 扫描分两段，主路径对标 Garnet `[ReadOnlyAddress, TailAddress]` 滑动窗口语义：
//! - 热区 `[read_only, tail)`：每轮无游标全量窗口扫描。纯内存指针行走、零磁盘 I/O
//!   （窗口由 hlog 内存缓冲区天然约束，与 Garnet 每 tick 全窗口同一取舍），刚写入
//!   即过期的短 TTL 键至多一个扫描间隔即被物理清除；
//! - 冷区 `[cold_cursor, read_only)`：游标增量推进，单轮受 `max_scan_records` 有界
//!   （磁盘区 I/O 有界，wedb 增强）。长 TTL 键在落盘冷区过期后不留死角，游标最终覆盖。
//!
//! 候选经最新 TTL 双检（内存探针初筛 + 删除阶段 `check_expired` 终审）后，走与用户
//! DEL 完全一致的统一删除路径；删除完成后才提交游标，取消或失败下一轮重试（幂等）。
//! 紧缩按阈值判定经 `wcompact::LogCompactor` 执行；其单条记录死亡判定对标 C# 两通道
//! 短路（墓碑 → 业务谓词，`WedbStore::compact` 注入 `WedbCompactionFunctions`），
//! 业务谓词以**最新 TTL 态**同口径判定数据记录与 TTL 记录两者——过期时一并判死，
//! 绝不出现「丢弃 TTL 而迁移数据」的永不过期键；
//! 显式启用态（CONFIG SET expired-key-deletion-scan-freq > 0）下本模块的后台扫描
//! 是主动过期清理通道，默认禁用态（对标 C# ExpiredKeyDeletionScanFrequencySecs
//! = -1）由读路径惰性清除承担；紧缩期 TTL 判死只是同一最新态判定的幂等复用。
//!
//! 配置为运行态共享句柄（`WedbStore::gc_cfg`），驱动循环每轮重读（对标 Garnet 每
//! 轮读取 RuntimeServerConfig），`WedbStore::update_gc_config` 热更新下一轮生效。
//! 紧缩判定不设独立周期旋钮：节奏单点为下「换号物理回收四面」段的驱动轮次，
//! 阈值与档位由 `GcConfig` 承载（口径详见 [`GcManager::try_compact`] 文档）。
//! 紧缩档位 `GcConfig::compaction_type`（None/Shift/Lookup/Scan）经调停消息自
//! CONFIG SET compaction-type 投影，默认 None 关闭常规阈值紧缩（对标 C#
//! DoCompactionAsync 首行判 None 短路；C# 侧 CompactionTask 默认也不注册）。
//! C# `--compaction-frequency-secs`（Options.cs:265 注册，defaults.conf:194
//! 默认 0，注册门 StoreWrapper.cs:967-969）本仓缺席的登记见
//! doc/zh/deviations.md §158 b，严禁按 C# 形态回改。
//! 紧缩另设高低水位熔断（doc/zh/db.md 承诺，Garnet 无对应物）：死亡虚拟 ID 队列
//! 积压超高水位时旁路 None 短路并全速回退紧缩（以 Lookup 活性校验档执行），
//! 回落低水位退出，防换号旧垃圾膨胀磁盘。
//! 扫描会话懒建复用（对标 Garnet StoreExpiredKeyDeletionDbStorageSession）。
//! 扫描观测对标 Garnet `ExpiredKeyDeletionScan` 的 `(numExpiredKeysFound,
//! totalRecordsScanned)` 双口径（见 [`GcStatsSnapshot`]）。
//! 候选收集内核 [`collect_expired`] 与 EXPDELSCAN 命令共享（对标 C#
//! StoreExpiredKeyDeletionScan 单内核双入口：判定与收集一处定义，调度各自入口）。
//!
//! 字段级过期收集（C# ObjectCollectTaskAsync 周期驱动 HashCollect）调度不在本引擎：
//! 信封域 rust 字段 TTL 以 wcol 信封对象内联过期堆承载，读路径惰性淘汰 +
//! HCOLLECT/ZCOLLECT 命令闭环清理；分层态树记录带 member_ttl 过期刻度，
//! 收集宿主在 wnode（object_collect_all 扫 Meta 元记录候选 → 分层收集执行体
//! 树内物理出账），本引擎只承担键级 TTL 与紧缩。
//!
//! 驱动分三层：[`GcManager::run_once`]（单轮纯逻辑）、[`GcManager::drive`]（强引用
//! 循环，供上层调度器接管）、[`GcManager::spawn`]（内置弱引用循环，引擎 Drop 自动
//! 退出）。三层与外部拉起入口 [`WedbStore::reconcile_gc_scan`] 的「禁用即停」判定
//! 一律转调 [`enabled_by_config`] 单点（配置快照入参、不二次取锁）。
//!
//! 换号物理回收四面（`pop_reclaimable` 墓碑注销 / 退役路由释放 / 空闲路由析构 /
//! 高低水位紧缩）随内核 [`GcManager::reclaim_physical`] 与过期键 SCAN 解耦：扫描
//! 循环在跑时由 [`GcManager::tick`] 顺带推进，扫描禁用（`gc.enabled = false`，默认）
//! 循环缺席时由常驻 [`spawn_bftree_reclaimer`] 经 [`GcManager::reclaim_when_scan_idle`]
//! 兜底推进。故 `gc.enabled` 只门控过期键 SCAN 删除，物理回收恒被某一驱动推进。
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/ExpiredKeyDeletionTests.cs（后台过期扫描）

use std::{
  sync::{
    Arc, Weak,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{Acquire, Relaxed, Release},
    },
  },
  time::Duration,
};

use compio::{
  runtime::{JoinHandle, spawn},
  time::sleep,
};
use log::warn;
use parking_lot::{Mutex, RwLock};
use wbase::supervise::supervise_task;
use wdev::Device;

use crate::{config::GcConfig, error::Result, session::SessionSlot, store::WedbStore};

/// 冷树缓存回收域（页驱逐联动释放冷树常驻页环——C# OnEvict 对位的轮询补位形态）
mod cold_tree;
/// 日志紧缩域（双水位迟滞熔断判定与紧缩推进）
mod compact;
/// 换号物理回收域（回收内核双面入口与 bftree 待释放排空常驻驱动）
mod reclaim;
/// 扫描任务生命周期编排（reconcile/start/stop 与统计观测入口）
mod task;
/// TTL 过期键扫描域（两段式扫描调度与候选收集共享内核）
mod ttl_sweep;
/// VDB 换号清扫域（死亡账本墓碑注销与空闲租户路由释放）
mod vdb;

pub(crate) use cold_tree::ColdBftreeObserved;
pub use reclaim::spawn_bftree_reclaimer;
pub(crate) use ttl_sweep::{ExpiredKeySet, ScanBudget, collect_expired};

/// 后台循环扫描间隔下限毫秒（防 scan_interval_ms=0 退化为忙轮询）
const MIN_SCAN_INTERVAL_MS: u64 = 10;

/// 监督快照里的过期扫描循环任务名（[`GcManager::spawn`] 内置循环的监督归组名，
/// 随 INFO bg_task_health 出；对标 C# StoreWrapper.cs:ExpiredKeyDeletionScanTaskAsync
/// catch LogCritical 的死亡留痕契约）
const GC_SCAN_TASK: &str = "gc_scan";

/// 内置 GC「禁用即停」判定单点：`enabled` 为真且 `scan_interval_ms > 0` 才运行
/// 后台定时循环，两类禁用态（开关关闭 / 间隔清零）恒判为不运行。
///
/// 入参为调用方自持的 [`GcConfig`] 快照，本函数不取锁——三处入口
/// （[`GcManager::drive`] 强引用循环、[`GcManager::spawn`] 内置弱引用循环、
/// [`WedbStore::reconcile_gc_scan`] 外部拉起）全部转调本函数，热更新后
/// 「循环内判定」与「外部拉起判定」不可能漂移。
///
/// 对标 C# 侧的同款单点收敛：后台任务开关只在
/// `libs/server/StoreWrapper.cs:TryStartExpiredKeyDeletionTask` 读一次
/// `frequencySecs > 0`，循环体自身不再判开关（`TaskManager.cs:CancelAsync`
/// 经 CancellationToken 停任务、`ReconcilePrimaryTask` 按新配置重拉），
/// 故 rust 侧也不得在循环与拉起两侧各写一份谓词。
///
/// [`WedbStore::reconcile_gc_scan`]: crate::store::WedbStore::reconcile_gc_scan
pub(crate) fn enabled_by_config(cfg: &GcConfig) -> bool {
  cfg.enabled && cfg.scan_interval_ms > 0
}

/// 快照对应的后台循环睡眠间隔毫秒（同 [`enabled_by_config`]：纯函数取快照、
/// 不取锁，`spawn` 与 `drive` 两条循环共用，杜绝间隔口径各自漂移；
/// 下限钳制防 0 值忙轮询）
fn scan_interval_ms(cfg: &GcConfig) -> u64 {
  cfg.scan_interval_ms.max(MIN_SCAN_INTERVAL_MS)
}

/// 内置 GC 原子计数器（Relaxed 语义：仅观测，无同步依赖）
#[derive(Default)]
struct GcStats {
  /// 累计物理删除的过期键数
  expired_deleted: AtomicU64,
  /// 累计紧缩执行次数
  compactions: AtomicU64,
  /// 最近一轮过期扫描物理删除数
  last_scan_deleted: AtomicU64,
  /// 最近一轮过期扫描记录数（两段扫描求和，对标 Garnet totalRecordsScanned 口径）
  last_scan_scanned: AtomicU64,
  /// 累计过期扫描记录数（各轮 last_scan_scanned 求和）
  total_scanned: AtomicU64,
  /// 最近一轮紧缩丢弃记录数
  last_compact_dropped: AtomicU64,
  /// 最近一轮紧缩判定是否处于熔断加速态（死亡虚拟 ID 队列超高水位）
  compact_boosting: AtomicBool,
}

/// 内置 GC 统计快照（轻量 Copy 结构）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcStatsSnapshot {
  /// 累计物理删除的过期键数
  pub expired_deleted: u64,
  /// 累计紧缩执行次数
  pub compactions: u64,
  /// 最近一轮过期扫描物理删除数
  pub last_scan_deleted: u64,
  /// 最近一轮过期扫描记录数（热区 + 冷区两段求和，对标 Garnet
  /// `ExpiredKeyDeletionScan` 返回的 `totalRecordsScanned`）
  pub last_scan_scanned: u64,
  /// 累计过期扫描记录数（各轮求和；观测扫描吞吐与游标推进进度）
  pub total_scanned: u64,
  /// 最近一轮紧缩丢弃记录数
  pub last_compact_dropped: u64,
  /// 最近一轮紧缩判定是否处于熔断加速态（死亡虚拟 ID 队列积压超高水位；
  /// 置位期间旁路 None 短路并全速回退，回落低水位以下自动退出）
  pub compact_boosting: bool,
}

/// 内置 GC 管理器共享核（后台循环与 [`GcHandle`] 共享）
pub struct GcManager<D: Device> {
  /// 引擎弱引用：不与 WedbStore 构成强引用环，引擎 Drop 后循环自行退出
  store: Weak<WedbStore<D>>,
  /// 运行态 GC 配置共享句柄（与 `WedbStore::gc_cfg` 同源；每轮重读支持热更新）
  cfg: Arc<RwLock<GcConfig>>,
  /// 协作取消标志（compio JoinHandle 无 abort，循环每轮轮询）
  cancel: AtomicBool,
  /// 单轮执行闸：后台循环与手动 run_once 并发时跳过后到者
  pub(crate) inflight: AtomicBool,
  /// 冷区欠账游标（已扫描到的日志地址；0 = 自 begin 起）。恒钳制在只读线以下，
  /// 热区由每轮窗口扫描覆盖；紧缩推进 begin 后经 max(begin) 自动适配
  cold_cursor: AtomicU64,
  /// 紧缩熔断加速态（迟滞双水位状态机，见 [`Self::refresh_compact_boost`]）：
  /// 死亡虚拟 ID 队列积压超高水位置位，回落低水位以下清位，死区维持原态防振荡
  compact_boost: AtomicBool,
  /// 复用的扫描会话槽位（懒建，对标 Garnet 专用扫描 StorageSession）
  sweep_session: SessionSlot<D>,
  stats: GcStats,
}

impl<D: Device> GcManager<D> {
  /// 创建 GC 管理器（共享 `store.gc_cfg` 运行态配置）
  pub fn new(store: &Arc<WedbStore<D>>) -> Self {
    Self {
      store: Arc::downgrade(store),
      cfg: Arc::clone(&store.gc_cfg),
      cancel: AtomicBool::new(false),
      inflight: AtomicBool::new(false),
      cold_cursor: AtomicU64::new(0),
      compact_boost: AtomicBool::new(false),
      sweep_session: SessionSlot::new(),
      stats: GcStats::default(),
    }
  }

  /// 派生内置 GC 后台循环（须在 compio 运行时内调用）
  ///
  /// 弱引用驱动：每间隔升级一次弱引用读配置并执行单轮，间隔内不持强引用——
  /// 引擎或句柄全部释放后循环在一个间隔内自行退出。`D: 'static` 为 compio
  /// `spawn` 的 'static 任务硬性要求（未来体捕获引擎弱引用），仅约束本方法。
  ///
  /// 禁用语义（对标 C# TaskManager.CancelAsync(ExpiredKeyDeletionTask)）：
  /// [`enabled_by_config`] 判否（`enabled = false` 或 `scan_interval_ms = 0`）时
  /// 循环退出，由 `WedbStore::start_gc` 按新配置重新拉起——等价于 C#
  /// ReconcilePrimaryTask 停任务 → 按新间隔重启的外部可观测行为。
  pub fn spawn(store: Arc<WedbStore<D>>) -> GcHandle<D>
  where
    D: Device + 'static,
  {
    let mgr = Arc::new(Self::new(&store));
    let weak = Arc::downgrade(&mgr);
    // 任务体经 wbase [`supervise_task`] 单点顶层监督（沿同族 reclaim.rs /
    // primary_tasks.rs 现成形态）：panic 落 log::error（带任务名）并计监督快照
    // panics，终局后外层 spawn 未来体随 Err 结束 → JoinHandle finished →
    // [`GcHandle::is_active`] 变假，由既有 [`WedbStore::reconcile_gc_scan`] 的
    // is_active 判定经 CONFIG SET 调停 / 角色切换重拉——不加自动重拉（防毒丸
    // 风暴，与 reclaimer REMOUNT_LIMIT 有界重挂裁量一致），零新增复位逻辑
    let join = spawn(async move {
      let _ = supervise_task(GC_SCAN_TASK, gc_scan_loop(weak)).await;
    });
    GcHandle {
      inner: mgr,
      join: Mutex::new(Some(join)),
    }
  }

  /// 强引用驱动循环：每间隔执行一次 [`Self::run_once`]，取消标志置位、配置
  /// 禁用或引擎释放时退出。供上层调度器以自有生命周期接管（调用方持强引用即
  /// 有意常驻，与 [`Self::spawn`] 的弱引用自退出语义互补）
  pub async fn drive(&self) {
    loop {
      // 每轮一次取锁同时得出启停判定与睡眠间隔（与 spawn 同型）
      let interval_ms = {
        let cfg = self.cfg.read();
        if self.cancel.load(Relaxed) || !enabled_by_config(&cfg) {
          return;
        }
        scan_interval_ms(&cfg)
      };
      if self.store.upgrade().is_none() {
        return;
      }
      sleep(Duration::from_millis(interval_ms)).await;
      if let Err(e) = self.run_once().await {
        warn!("内置 GC 轮次失败，留待下轮: err={e}");
      }
    }
  }

  /// 读取统计快照
  pub fn stats(&self) -> GcStatsSnapshot {
    GcStatsSnapshot {
      expired_deleted: self.stats.expired_deleted.load(Relaxed),
      compactions: self.stats.compactions.load(Relaxed),
      last_scan_deleted: self.stats.last_scan_deleted.load(Relaxed),
      last_scan_scanned: self.stats.last_scan_scanned.load(Relaxed),
      total_scanned: self.stats.total_scanned.load(Relaxed),
      last_compact_dropped: self.stats.last_compact_dropped.load(Relaxed),
      compact_boosting: self.stats.compact_boosting.load(Relaxed),
    }
  }

  /// 单轮 GC：先过期扫描，后按阈值判定紧缩（供后台循环复用，亦可手动驱动）
  pub async fn run_once(&self) -> Result<()> {
    // 单轮闸：上一轮未结束（含后台与手动并发）则跳过本轮
    if self.inflight.swap(true, Acquire) {
      return Ok(());
    }
    let _guard = RunGuard(&self.inflight);
    self.tick().await
  }

  /// 单轮内部实现（调用方须持 inflight 闸）
  async fn tick(&self) -> Result<()> {
    // 引擎已释放（弱引用悬空）：静默退出，无任何可回收对象
    let Some(store) = self.store.upgrade() else {
      return Ok(());
    };
    // 每轮重读一次运行态配置快照（GcConfig 为纯标量结构，clone 即快照）
    let cfg = self.cfg.read().clone();
    let (deleted, scanned) = self.sweep_expired(&store, &cfg).await?;
    self.stats.expired_deleted.fetch_add(deleted, Relaxed);
    self.stats.last_scan_deleted.store(deleted, Relaxed);
    self.stats.last_scan_scanned.store(scanned, Relaxed);
    self.stats.total_scanned.fetch_add(scanned, Relaxed);
    self.reclaim_physical(&store, &cfg).await
  }
}

/// [`GcManager::spawn`] 内置弱引用循环体（[`GC_SCAN_TASK`] 的监督对象）：
/// 每间隔升格一次弱引用读配置并执行单轮，间隔内不持强引用——引擎或句柄全部
/// 释放后循环在一个间隔内自行退出；睡前读间隔使 update_gc_config 热更新下一轮
/// 生效（对标 Garnet 每轮重读 RuntimeServerConfig），禁用即退出（热更新可经
/// start_gc 重拉）；读后立即释放强引用保证释放语义不被任务自持阻断
async fn gc_scan_loop<D: Device>(weak: Weak<GcManager<D>>) {
  loop {
    let interval_ms = match weak.upgrade() {
      None => return,
      Some(m) => {
        if m.cancel.load(Relaxed) {
          return;
        }
        let cfg = m.cfg.read();
        if !enabled_by_config(&cfg) {
          return;
        }
        let ms = scan_interval_ms(&cfg);
        drop(cfg);
        drop(m);
        ms
      }
    };
    sleep(Duration::from_millis(interval_ms)).await;
    let Some(m) = weak.upgrade() else {
      return;
    };
    // 单轮失败仅留痕不中断循环：后台 GC 的韧性优先于快速失败
    if let Err(e) = m.run_once().await {
      warn!("内置 GC 轮次失败，留待下轮: err={e}");
    }
  }
}

/// 单轮 GC 执行闸守卫（GC 循环内部专用，Drop 时自动释放执行闸，确保任务取消时不残留死锁）
pub(super) struct RunGuard<'a>(pub(super) &'a AtomicBool);
impl Drop for RunGuard<'_> {
  fn drop(&mut self) {
    self.0.store(false, Release);
  }
}

/// 内置 GC 后台循环句柄：`stop()`/Drop 取消，`stats()` 只读观测
pub struct GcHandle<D: Device> {
  inner: Arc<GcManager<D>>,
  /// 后台循环任务句柄；Drop 时内建 task cancel 兜底（协作标志之外的最后一道闸）
  join: Mutex<Option<JoinHandle<()>>>,
}

impl<D: Device> GcHandle<D> {
  /// 请求后台循环退出（协作式：至多再运行一个扫描间隔；可重复调用）
  pub fn stop(&self) {
    self.inner.cancel.store(true, Relaxed);
  }

  /// 读取统计快照
  pub fn stats(&self) -> GcStatsSnapshot {
    self.inner.stats()
  }

  /// 后台循环是否已退出（stop 后至多一个扫描间隔内变真）
  pub fn is_finished(&self) -> bool {
    self
      .join
      .lock()
      .as_ref()
      .is_none_or(JoinHandle::is_finished)
  }

  /// 后台循环是否仍在运行（未请求退出且任务未收敛；
  /// `WedbStore::start_gc` 以此判定复用或重拉）
  pub fn is_active(&self) -> bool {
    !self.inner.cancel.load(Relaxed) && !self.is_finished()
  }

  /// 手动驱动一轮 GC（与后台循环共享单轮闸，并发调用安全；供测试）
  #[cfg(test)]
  pub async fn run_once(&self) -> Result<()> {
    self.inner.run_once().await
  }
}

impl<D: Device> Drop for GcHandle<D> {
  fn drop(&mut self) {
    self.inner.cancel.store(true, Relaxed);
    // 兜底取消：JoinHandle Drop 内建 task cancel；此刻任务至多处于 sleep 或单轮
    // 中间态，未来体在 await 点安全丢弃（与任意前台任务的取消语义一致）
    self.join.lock().take();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 「禁用即停」判定真值表：仅开关开启且间隔 > 0 判「运行」，开关关闭与间隔清零
  /// 两类禁用态同判「不运行」（默认配置对标 Garnet `ExpiredKeyDeletionScanFrequencySecs
  /// = -1` 亦不运行）；判定与间隔口径同源，判「运行」的快照间隔恒不低于忙轮询下限
  #[test]
  fn gc_scan_predicate_disabled_states() {
    let zero_interval = GcConfig {
      enabled: true,
      scan_interval_ms: 0,
      ..GcConfig::default()
    };
    let switch_off = GcConfig {
      enabled: false,
      scan_interval_ms: 1_000,
      ..GcConfig::default()
    };
    let effective = GcConfig {
      enabled: true,
      scan_interval_ms: 1_000,
      ..GcConfig::default()
    };
    assert!(!enabled_by_config(&zero_interval), "间隔清零须判为不运行");
    assert!(!enabled_by_config(&switch_off), "开关关闭须判为不运行");
    assert!(
      !enabled_by_config(&GcConfig::default()),
      "默认配置须判为不运行（惰性过期兜底）"
    );
    assert!(enabled_by_config(&effective), "开关开启且间隔 > 0 才运行");
    assert!(
      scan_interval_ms(&effective) >= MIN_SCAN_INTERVAL_MS,
      "判「运行」的快照间隔不得低于忙轮询下限"
    );
  }
}
