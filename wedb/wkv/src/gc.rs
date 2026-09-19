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

use std::{
  ops::Range,
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
use gxhash::{GxBuildHasher, HashSet};
use log::{info, warn};
use parking_lot::{Mutex, RwLock};
use wbase::time::{now_ms, now_ticks};
use wcompact::CompactionType;
use wconf::LogCompactionType;
use wdev::Device;

use crate::{
  config::GcConfig,
  error::Result,
  session::{SessionSlot, StoreSession},
  store::WedbStore,
  ttl::{TtlGate, is_expired},
  vdb::DbMetaRecord,
};

/// 后台循环扫描间隔下限毫秒（防 scan_interval_ms=0 退化为忙轮询）
const MIN_SCAN_INTERVAL_MS: u64 = 10;

/// 换号物理回收常驻轮询间隔毫秒：物理回收是正确性面（杜绝死域树文件滞留磁盘、
/// 换号墓碑与旧域日志垃圾永驻、退役/空闲路由表永不析构，doc/zh/db.md 主从异步屏障
/// 与高低水位熔断承诺），与 expired-key-deletion 扫描开关无关，恒常驻。本节拍驱动
/// [`spawn_bftree_reclaimer`]：每轮排空 bftree 待释放队列，并在后台扫描循环缺席
/// （[`enabled_by_config`] 判否）时兜底推进 vdb 墓碑注销 / 退役与空闲路由释放 /
/// 水位紧缩，使 `gc.enabled` 只门控过期键 SCAN 删除，不再顺带关停物理回收
const RELEASE_POLL_MS: u64 = 200;

/// 单轮待物理释放批消费上限（[`WedbStore::drain_bftree_release`] 消费条数）：
/// 换号风暴下释放有界推进，单次唤醒不长期霸占后台任务
const RELEASE_BATCH: usize = 256;

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

/// 拉起换号物理回收后台任务（[`WedbStore::open_shared`] 在 compio 运行时内
/// 调用；须在运行时上下文中执行）
///
/// 本任务是与过期键 SCAN 解耦的换号物理回收唯一常驻驱动，`gc.enabled` 门禁绝不
/// 关停它（doc/zh/db.md 主从异步屏障与高低水位熔断承诺），每 [`RELEASE_POLL_MS`]
/// 轮询执行两类回收：
/// - 从库本地 bftree 待释放队列的唯一常驻消费者：
///   [`WedbStore::reclaim_bftree_keys`] 投递的待释放树批次经
///   [`WedbStore::drain_bftree_release`] 消费，纪元注册与就绪收割（含数据文件删除）
///   都发生在本任务线程——长读事务的排空等待被隔离于此，绝不回到复制流水线；
/// - 扫描循环缺席时的物理回收四面兜底：经 [`GcManager::reclaim_when_scan_idle`]
///   推进死亡账本墓碑注销 / 退役与空闲路由释放 / 水位紧缩。后台扫描循环在跑
///   （[`enabled_by_config`] 为真）时其 [`GcManager::tick`] 已含同一回收内核，本
///   调用自动让位；判否时由本任务无条件推进，故 `gc.enabled` 只门控过期键 SCAN 删除。
///
/// 生命周期闭环：任务只持引擎弱引用，句柄显式 `detach` 交由执行器持有——
/// compio `JoinHandle` 直抛是 fire-and-cancel（见 `open_shared` 历史教训注释），
/// 必须经 `detach` 才是后台运行。引擎 Arc 归零后升级失败自行退出，残留批次由
/// `WedbStore::Drop` 末轮收割兜底；无运行时上下文构造的引擎（纯同步测试形态）
/// 不经本任务，手动驱动 [`WedbStore::drain_bftree_release`] 与
/// [`GcManager::reclaim_when_scan_idle`] 承接。
///
/// [`WedbStore::drain_bftree_release`]: crate::store::WedbStore::drain_bftree_release
/// [`WedbStore::reclaim_bftree_keys`]: crate::store::WedbStore::reclaim_bftree_keys
pub fn spawn_bftree_reclaimer<D: Device + 'static>(store: &Arc<WedbStore<D>>) {
  let mgr = Arc::new(GcManager::new(store));
  let weak = Arc::downgrade(store);
  spawn(async move {
    loop {
      sleep(Duration::from_millis(RELEASE_POLL_MS)).await;
      let Some(store) = weak.upgrade() else {
        return;
      };
      store.drain_bftree_release(RELEASE_BATCH);
      if let Err(e) = mgr.reclaim_when_scan_idle().await {
        warn!("内置 GC 常驻物理回收轮次失败，留待下轮: err={e}");
      }
    }
  })
  .detach();
}

/// 快照对应的后台循环睡眠间隔毫秒（同 [`enabled_by_config`]：纯函数取快照、
/// 不取锁，`spawn` 与 `drive` 两条循环共用，杜绝间隔口径各自漂移；
/// 下限钳制防 0 值忙轮询）
fn scan_interval_ms(cfg: &GcConfig) -> u64 {
  cfg.scan_interval_ms.max(MIN_SCAN_INTERVAL_MS)
}

/// 过期候选集：(ns, db, 用户键)。收集与删除两阶段共用（类型别名收敛复合泛型实例化）
pub(crate) type ExpiredKeySet = HashSet<(u64, u64, Box<[u8]>)>;

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
    let join = spawn(async move {
      loop {
        // 睡前读间隔：update_gc_config 热更新下一轮生效（对标 Garnet 每轮重读
        // RuntimeServerConfig）；禁用即退出（热更新可经 start_gc 重拉）；
        // 读后立即释放强引用保证释放语义不被任务自持阻断
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

  /// 换号物理回收内核（四正确性面）：死亡账本墓碑注销 / 退役租户路由释放 /
  /// 空闲路由析构（[`Self::sweep_vdb`]）+ 高低水位熔断判定与推进 begin 的日志紧缩
  /// （[`Self::try_compact`]）。`pop_reclaimable` 须待 begin 越过死亡条目 tail_address
  /// 方可摘账本，故紧缩推进与账本注销同属一个回收内核，须一并驱动。
  ///
  /// 两处共用，构成「物理回收恒被推进、不随 `gc.enabled` 关停」的单机制：
  /// - [`Self::tick`]：完整 GC 轮次（后台扫描循环在跑、及手动 [`Self::run_once`]）；
  /// - [`Self::reclaim_when_scan_idle`]：扫描循环缺席时 [`spawn_bftree_reclaimer`] 兜底。
  async fn reclaim_physical(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<()> {
    self.sweep_vdb(store).await?;
    self.try_compact(store, cfg).await
  }

  /// 常驻回收循环单轮：仅当后台扫描循环未在运行（[`enabled_by_config`] 判否）时执行
  /// 物理回收内核 [`Self::reclaim_physical`]。扫描循环在跑时其 [`Self::tick`] 已含同一
  /// 内核，本入口自动让位，杜绝双重紧缩。使换号物理回收与 expired-key-deletion 扫描
  /// 开关彻底解耦：无论扫描开否，四面回收恒被某一驱动推进
  pub async fn reclaim_when_scan_idle(&self) -> Result<()> {
    let cfg = self.cfg.read().clone();
    if enabled_by_config(&cfg) {
      return Ok(());
    }
    let Some(store) = self.store.upgrade() else {
      return Ok(());
    };
    self.reclaim_physical(&store, &cfg).await
  }

  async fn sweep_vdb(&self, store: &Arc<WedbStore<D>>) -> Result<()> {
    let now_ticks = now_ticks();
    let begin_addr = store.begin_address();
    // 死亡账本按到期时间小根堆只弹「已到期且日志截断线已越界」的前缀，
    // 扫描成本与到期前缀成正比，与账本总量（历史租户数）无关
    let to_remove = store.vdb.gc_dead.pop_reclaimable(now_ticks, begin_addr);
    if !to_remove.is_empty()
      && let Ok(session) = store.new_session()
    {
      session.set_context(0, 0);
      let pin_routing = store.vdb.db_routing.pin();
      for (vid, item) in to_remove {
        // 墓碑键载荷与值经 DbMetaRecord 单点编解码（退役角色由 vns 有无甄别：
        // Some = 库级 GcDeadDb，None = 命名空间级 GcDeadNs），杜绝双分支共享
        // 裸缓冲的错位隐患
        let rec = match item.vns {
          Some(vns) => DbMetaRecord::GcDeadDb {
            expired_at: item.expired_at,
            vns,
            old_vdb: vid,
            tail_address: item.tail_address,
          },
          None => {
            pin_routing.remove(&vid); // 彻底释放废弃租户路由快照表，内存归零
            DbMetaRecord::GcDeadNs {
              expired_at: item.expired_at,
              old_vns: vid,
              tail_address: item.tail_address,
            }
          }
        };
        // 墓碑注销落盘失败 warn 留痕：账本条目已离册，盘上墓碑未注销则重启
        // 重建后下轮重扫重投（pop_reclaimable 双检幂等），绝不静默吞硬错
        if let Err(err) = session.delete_dbmeta(rec.key().as_slice()).await {
          warn!("内置 GC 退役墓碑注销失败: vid={vid} err={err}");
        }
      }
    }
    // 空闲析构：引用归零且空闲期限已满的租户路由快照摘除释放（连接断开 +
    // 长期空闲双触发；映射权威在磁盘 DbMeta，析构后访问点查装载回建）
    let evicted = store
      .vdb
      .pop_idle_candidates(now_ms())
      .into_iter()
      .filter(|&vns| store.vdb.evict_idle_route(vns))
      .count();
    if evicted > 0 {
      info!("内置 GC 空闲析构租户路由快照: 数量={evicted}（内存归零，磁盘权威可重载）");
    }
    Ok(())
  }

  /// 两段式过期扫描：热区窗口优先（对标 Garnet 滑动窗口），冷区欠账用剩余删除预算。
  /// 返回 (物理删除键数, 扫描记录数)，对标 Garnet
  /// `ExpiredKeyDeletionScan` 的 `(numExpiredKeysFound, totalRecordsScanned)` 双口径；
  /// 扫描记录数为热区与冷区两段 `collect_expired` 的 scanned 求和。
  /// 候选收集走与 EXPDELSCAN 共享的 [`collect_expired`] 内核（C# 侧同为单内核：
  /// StoreExpiredKeyDeletionScan 双入口共用 ExpiredKeysBase），本入口独有热/冷两段
  /// 调度与容错删除语义（单键失败 warn 留痕，下轮双检幂等重试）。
  async fn sweep_expired(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<(u64, u64)> {
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let cap = cfg.max_batch_deletes.max(1);
    let cold_cap = cfg.max_scan_records.max(1);
    let session = self.sweep_session.take(store)?;
    let mut picked = ExpiredKeySet::with_hasher(GxBuildHasher::default());
    let mut scanned = 0u64;
    let all_dbs = |_: u64, _: u64| true;

    // 段 1：热区窗口 [read_only, tail) —— 每轮无游标全量扫描（纯内存零 I/O），
    // 记录数不限（与 Garnet 全窗口扫描同一取舍，窗口由内存缓冲区约束），
    // 刚写入即过期的短 TTL 键至多一个扫描间隔即被物理清除
    let read_only = store.read_only_address();
    let tail = store.tail_address();
    if read_only < tail {
      let (n, ..) = collect_expired(
        &session,
        store,
        read_only..tail,
        now,
        ScanBudget {
          max_records: u64::MAX,
          max_picks: usize::MAX,
        },
        &mut picked,
        &all_dbs,
      )
      .await?;
      scanned += n;
    }

    // 段 2：冷区欠账 [cold_cursor, read_only) —— 游标增量推进，单轮记录预算有界
    // （磁盘 I/O 有界）；删除预算已被键级候选占满时本轮跳过（游标不动，下轮重扫，幂等）
    let mut cold_commit = None;
    if picked.len() < cap {
      let cold_from = self.cold_cursor.load(Relaxed).max(store.begin_address());
      if cold_from < read_only {
        let (n, next_addr, exhausted) = collect_expired(
          &session,
          store,
          cold_from..read_only,
          now,
          ScanBudget {
            max_records: cold_cap as u64,
            max_picks: cap.saturating_sub(picked.len()),
          },
          &mut picked,
          &all_dbs,
        )
        .await?;
        scanned += n;
        // 游标恒钳制在只读线以下（热区由段 1 覆盖，不得重复计入冷区欠账）
        cold_commit = Some(if exhausted || next_addr >= read_only {
          read_only
        } else {
          next_addr
        });
      }
    }

    // 收集完成后统一物理删除：逐键双检，走与用户 DEL 完全一致的路径
    // （check_expired 内部经 purge_expired = 删 TTL 记录 + 删数据，索引/墓碑/WAL 一致）
    let mut deleted = 0u64;
    for (ns, db, key) in &picked {
      session.set_virtual_context(*ns, *db);
      match session.check_expired(key).await {
        Ok(true) => deleted += 1,
        // 双检未过期：扫描与删除间隙内被用户续期/删除/紧缩，安全放行
        Ok(false) => {}
        Err(e) => warn!("内置 GC 过期删除失败，留待下一扫描周期重试: err={e}"),
      }
    }

    if deleted > 0 {
      info!(
        "内置 GC 过期扫描完成: 键候选={}, 物理删除键={deleted}",
        picked.len()
      );
    }
    // 删除完成才提交冷区游标；任务取消时重扫当前批，最新 TTL 双检保证幂等
    if let Some(addr) = cold_commit {
      self.cold_cursor.store(addr, Relaxed);
    }
    self.sweep_session.restore(session);
    Ok((deleted, scanned))
  }

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
  async fn try_compact(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<()> {
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
    // segment_size：分段设备取设备段大小（物理回收单元），单文件设备回退 hlog 页大小
    let seg = store
      .device
      .segment_size()
      .unwrap_or(store.hlog.config.page_size as u64);
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

/// 单段扫描预算（收集内核 [`collect_expired`] 参数收敛：记录数上限与候选数上限；
/// EXPDELSCAN 全量预算双 MAX，GC 热区全量记录、冷区 `max_scan_records` 记录预算）
pub(crate) struct ScanBudget {
  /// 单段记录扫描上限（u64::MAX = 全量）
  pub max_records: u64,
  /// 候选收集上限（usize::MAX = 全量）
  pub max_picks: usize,
}

/// 单段过期候选收集共享内核（EXPDELSCAN 与内置 GC 双入口共用，对标 C# 同构单内核：
/// ArrayKeyIterationFunctions.cs:ExpiredKeysBase —— 后台 ExpiredKeyDeletionScanTaskAsync
/// 与命令 ExpiredKeyDeletionScan(dbId) 均经 DatabaseManagerBase.StoreExpiredKeyDeletionScan
/// 落到同一 Reader；调度差异只在入口，判定与收集内核一处定义）
///
/// 扫描 `[range]` 中至多 `budget.max_records` 条记录，将已过期 TTL 键（经 `db_match`
/// 逻辑库过滤）加入 `picked`（至多 `budget.max_picks` 个）。
/// 返回 (扫描数, 游标位置, 是否扫到线头)。
///
/// 过滤链四级：墓碑位单次读取 → TTL 物理键变长前缀反解（零分配，非 TTL 记录跳过）→
/// 值定长校验（非法长度按无 TTL 容错）+ 到期比较（严格小于读路径口径）+ 最新态
/// 内存探针双检（陈旧日志版本已续期/已删时放行，防其反复占据批预算饿死存活过期键）。
/// `now` 为 .NET Ticks 过期判定基准，与 TTL 记录值同域
pub(crate) async fn collect_expired<D: Device>(
  session: &StoreSession<D>,
  store: &Arc<WedbStore<D>>,
  range: Range<u64>,
  now: i64,
  budget: ScanBudget,
  picked: &mut ExpiredKeySet,
  db_match: &impl Fn(u64, u64) -> bool,
) -> Result<(u64, u64, bool)> {
  let mut scanned = 0u64;
  let mut exhausted = false;
  let mut scan = store.hlog.scan_iter(range.start, range.end);
  loop {
    if picked.len() >= budget.max_picks || scanned >= budget.max_records {
      break;
    }
    let next = scan
      .next_ref(|item| {
        let rec = item.rec;
        // 快路径 1：墓碑位单次读取
        if rec.is_tombstone() {
          return Ok(true);
        }
        // 快路径 2：TTL 物理键反解（变长前缀反解 + 标签比对，零分配），非 TTL 记录跳过
        let Some((ns, db, user_key)) = StoreSession::<D>::user_key_from_ttl_key(rec.key) else {
          return Ok(true);
        };
        // 统一过期判定：未到期或非法长度（按无 TTL 容错）放行跳过（严格小于读路径口径）
        if !is_expired(rec.value, now) || !db_match(ns, db) {
          return Ok(true);
        }
        // 陈旧版本双检：该键最新 TTL 态已不过期（续期）或已删除（墓碑）时放行
        session.set_virtual_context(ns, db);
        if matches!(session.probe_ttl(user_key, now), TtlGate::Pass) {
          return Ok(true);
        }
        picked.insert((ns, db, Box::from(user_key)));
        Ok(true)
      })
      .await?;
    match next {
      None => {
        exhausted = true;
        break;
      }
      Some(_) => scanned += 1,
    }
  }
  Ok((scanned, scan.current_address(), exhausted))
}

/// 单轮 GC 执行闸守卫（GC 循环内部专用，Drop 时自动释放执行闸，确保任务取消时不残留死锁）
struct RunGuard<'a>(&'a AtomicBool);

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
