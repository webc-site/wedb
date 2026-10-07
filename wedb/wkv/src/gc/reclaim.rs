//! 换号物理回收域：回收内核 [`GcManager::reclaim_physical`]（VDB 清扫 +
//! 水位紧缩）双面入口——[`GcManager::tick`] 完整轮次与
//! [`GcManager::reclaim_when_scan_idle`] 扫描缺席兜底，后者由常驻
//! [`spawn_bftree_reclaimer`] 驱动（bftree 待释放队列唯一常驻消费者）。
//!
//! 自研依据: 批预算截断与游标推进（C# 对应过期扫描预算面）

use std::{
  sync::{Arc, Weak, atomic::Ordering::Relaxed},
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use log::warn;
use wbase::supervise::supervise_task;
use wdev::Device;

use super::{GcManager, enabled_by_config};
use crate::{config::GcConfig, error::Result, range_index::range_index_blocking, store::WedbStore};

/// 换号物理回收常驻轮询间隔毫秒：物理回收是正确性面（杜绝死域树文件滞留磁盘、
/// 换号墓碑与旧域日志垃圾永驻、退役/空闲路由表永不析构，doc/zh/db.md 主从异步屏障
/// 与高低水位熔断承诺），与 expired-key-deletion 扫描开关无关，恒常驻。本节拍驱动
/// [`spawn_bftree_reclaimer`]：每轮排空 bftree 待释放队列（只消费已过安全纪元
/// 期限的条目，期限内留队——树文件删除侧与日志紧缩同受 expired_at 门控），并在
/// 后台扫描循环缺席（[`enabled_by_config`] 判否）时兜底推进 vdb 墓碑注销 /
/// 退役与空闲路由释放 / 水位紧缩，使 `gc.enabled` 只门控过期键 SCAN 删除，
/// 不再顺带关停物理回收
const RELEASE_POLL_MS: u64 = 200;

/// 单轮待物理释放批消费上限（[`WedbStore::drain_bftree_release`] 消费条数）：
/// 换号风暴下释放有界推进，单次唤醒不长期霸占后台任务
pub(super) const RELEASE_BATCH: usize = 256;

/// 监督快照里的任务名（[`spawn_bftree_reclaimer`] 的监督归组名）
const RECLAIMER_TASK: &str = "bftree_reclaimer";

/// panic 臂重挂退避：以 [`RELEASE_POLL_MS`] 为单位乘数的等待轮数
const REMOUNT_BACKOFF_POLLS: u64 = 5;

/// panic 臂重挂上限（有界）：连败后留监督快照的 panic 计数与复位态挂载位，
/// 交后续挂载点（恢复臂 / 副本换入收口）重新认领，绝不禁回「位永真」形态
const REMOUNT_LIMIT: u32 = 3;

/// 拉起换号物理回收后台任务（实例产生点必带：[`WedbStore::open_shared`] 冷启动、
/// 数据库管理面恢复臂与副本换入收口在 compio 运行时内调用；须在运行时上下文中
/// 执行）
///
/// 幂等：同实例重复调用（挂载点叠加 / 未来调用方误双挂）经挂载位
/// `compare_exchange` 只认首次，二次起直接返回——双常驻循环会双倍轮询排空
/// 同一待释放队列。引擎 Arc 归零后任务自行退出，挂载位随实例生命周期终结，
/// 不构成跨实例泄漏。
///
/// panic 监督（wbase [`supervise_task`] 单点）：任务体经 wbase::supervise 的
/// [`supervise_task`] 顶层包装，panic 臂落 log::error + 监督快照 panic 计数
/// 后复位挂载位并退避重挂（[`REMOUNT_LIMIT`] 有界，重挂 CAS 失败即让位——
/// 退避窗口内别处已挂），重挂期间挂载位由重挂环 CAS 认领，绝不禁回「位永真
/// 堵死一切重挂」的死亡形态（物理回收是正确性面，见 [`RELEASE_POLL_MS`] 注）。
///
/// 本任务是与过期键 SCAN 解耦的换号物理回收唯一常驻驱动，`gc.enabled` 门禁绝不
/// 关停它（doc/zh/db.md 主从异步屏障与高低水位熔断承诺），每 [`RELEASE_POLL_MS`]
/// 轮询执行三类回收：
/// - 从库本地 bftree 待释放队列的唯一常驻消费者：
///   [`WedbStore::reclaim_bftree_keys`] 投递的待释放树批次经
///   [`WedbStore::drain_bftree_release`] 消费（安全纪元期限内只暂存），纪元注册
///   与就绪收割（含数据文件删除）都发生在本任务线程——长读事务的排空等待被隔离
///   于此，绝不回到复制流水线；
/// - 扫描循环缺席时的物理回收四面兜底：经 [`GcManager::reclaim_when_scan_idle`]
///   推进死亡账本墓碑注销 / 退役与空闲路由释放 / 水位紧缩。后台扫描循环在跑
///   （[`enabled_by_config`] 为真）时其 [`GcManager::tick`] 已含同一回收内核，本
///   调用自动让位；判否时由本任务无条件推进，故 `gc.enabled` 只门控过期键 SCAN 删除。
/// - 冷树缓存回收（C# GarnetRecordTriggers.cs:OnEvict → DisposeTreeUnderLock
///   (deleteFiles:false) 的轮询对位）：[`WedbStore::recycle_cold_bftrees`] 摘除
///   Meta 存根页已越出内存窗且冷满迟滞窗的树常驻页环并归还预算，冷键再读经
///   get_or_open_tree 懒重开——预算先到先得永占、耗尽后新升阶与 RI.CREATE
///   永久拒绝的缺口由此自愈。
///
/// 生命周期闭环：任务只持引擎弱引用，句柄显式 `detach` 交由执行器持有——
/// compio `JoinHandle` 直抛是 fire-and-cancel（见 `open_shared` 历史教训注释），
/// 必须经 `detach` 才是后台运行。引擎 Arc 归零后升级失败自行退出，残留批次由
/// `WedbStore::Drop` 末轮收割兜底；无运行时上下文构造的引擎（纯同步测试形态）
/// 不经本任务，手动驱动 [`WedbStore::drain_bftree_release`] 与
/// [`GcManager::reclaim_when_scan_idle`] 承接。
///
/// [`supervise_task`]: wbase::supervise::supervise_task
/// [`WedbStore::drain_bftree_release`]: crate::store::WedbStore::drain_bftree_release
/// [`WedbStore::reclaim_bftree_keys`]: crate::store::WedbStore::reclaim_bftree_keys
pub fn spawn_bftree_reclaimer<D: Device + 'static>(store: &Arc<WedbStore<D>>) {
  // 防重复挂载：首次置位认领，同实例二次调用直接返回（Relaxed 足够——位本身
  // 只做去重裁决，任务经弱引用观察的引擎可见性由 Arc 构造序保证）
  if store
    .reclaimer_mounted
    .compare_exchange(false, true, Relaxed, Relaxed)
    .is_err()
  {
    return;
  }
  let weak = Arc::downgrade(store);
  spawn(async move {
    if supervise_task(RECLAIMER_TASK, reclaimer_loop(weak.clone()))
      .await
      .is_err()
    {
      remount_reclaimer(weak, REMOUNT_LIMIT).await;
    }
  })
  .detach();
}

/// 常驻回收循环体（[`spawn_bftree_reclaimer`] 的监督对象）：引擎弱引用每轮
/// 升格，归零即正常退出
async fn reclaimer_loop<D: Device>(weak: Weak<WedbStore<D>>) {
  let Some(store) = weak.upgrade() else {
    return;
  };
  let mgr = Arc::new(GcManager::new(&store));
  drop(store);
  loop {
    sleep(Duration::from_millis(RELEASE_POLL_MS)).await;
    let Some(store) = weak.upgrade() else {
      return;
    };
    store.drain_bftree_release(RELEASE_BATCH);
    // 冷树缓存回收臂（C# GarnetRecordTriggers.cs:OnEvict → DisposeTreeUnderLock
    // (deleteFiles:false) 的轮询对位，判据与限批纪律见 [`crate::gc::cold_tree`]）：
    // 摘除冷满迟滞窗的树常驻页环并归还预算，冷键再读经 get_or_open_tree 懒重开；
    // 与本轮待释放排空同宿主同轮，释放内核的纪元收割正好复用 drain 臂。
    // 摘除内核 dispose_tree_under_lock 内 detach_tree 直取条带无界停车档，整段
    // 卸载阻塞线程离核——本任务内直调与同核持条带写锁跨 await 的分层写臂互候
    // 永挂（票 wkv-flush-replay-detach-tree-async-stripe-park-core-deadlock，
    // 与 ops/promote 预算自愈臂同为 [`range_index_blocking`] 宿主，彼处已在
    // spawn_blocking 线程内）
    let recycler = Arc::clone(&store);
    if let Err(e) = range_index_blocking(move || recycler.recycle_cold_bftrees()).await {
      warn!("冷树回收卸载任务异常退出，摘除留待下轮: err={e:?}");
    }
    if let Err(e) = mgr.reclaim_when_scan_idle().await {
      warn!("内置 GC 常驻物理回收轮次失败，留待下轮: err={e}");
    }
  }
}

/// panic 臂重挂环：复位挂载位 → 退避 → CAS 重新认领 → 再监督一轮（有界）。
/// CAS 失败 = 退避窗口内其他挂载点已认领，本环立即让位；连败至上限则留复位态
/// 挂载位与监督 panic 计数退出，由后续挂载点重新认领
async fn remount_reclaimer<D: Device>(weak: Weak<WedbStore<D>>, mut retries: u32) {
  loop {
    let Some(store) = weak.upgrade() else {
      return;
    };
    store.reclaimer_mounted.store(false, Relaxed);
    drop(store);
    sleep(Duration::from_millis(
      RELEASE_POLL_MS * REMOUNT_BACKOFF_POLLS,
    ))
    .await;
    let Some(store) = weak.upgrade() else {
      return;
    };
    if store
      .reclaimer_mounted
      .compare_exchange(false, true, Relaxed, Relaxed)
      .is_err()
    {
      return;
    }
    if supervise_task(RECLAIMER_TASK, reclaimer_loop(weak.clone()))
      .await
      .is_ok()
    {
      return;
    }
    retries -= 1;
    if retries == 0 {
      return;
    }
  }
}

impl<D: Device> GcManager<D> {
  /// 换号物理回收内核（四正确性面）：死亡账本墓碑注销 / 退役租户路由释放 /
  /// 空闲路由析构（[`Self::sweep_vdb`]）+ 高低水位熔断判定与推进 begin 的日志紧缩
  /// （[`Self::try_compact`]）。`pop_reclaimable` 须待 begin 越过死亡条目 tail_address
  /// 方可摘账本，故紧缩推进与账本注销同属一个回收内核，须一并驱动。
  ///
  /// 两处共用，构成「物理回收恒被推进、不随 `gc.enabled` 关停」的单机制：
  /// - [`Self::tick`]：完整 GC 轮次（后台扫描循环在跑、及手动 [`Self::run_once`]）；
  /// - [`Self::reclaim_when_scan_idle`]：扫描循环缺席时 [`spawn_bftree_reclaimer`] 兜底。
  /// [`doc(hidden)`] 测试专用隐藏面：物理回收内核本体除 `tick`/
  /// `reclaim_when_scan_idle` 两生产入口外，仅 wkv/tests/gc_reclaimer_mount.rs
  /// 集成测试直调（在途互斥与 RunGuard 复位臂），非公共 API 契约
  #[doc(hidden)]
  pub async fn reclaim_physical(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<()> {
    use std::sync::atomic::Ordering::Acquire;

    use super::RunGuard;
    // 物理回收防重入单飞闸（Store 级，跨 GcManager 实例互斥）
    if store.reclaim_inflight.swap(true, Acquire) {
      return Ok(());
    }
    let _guard = RunGuard(&store.reclaim_inflight);

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
}
