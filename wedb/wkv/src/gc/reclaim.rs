//! 换号物理回收域：回收内核 [`GcManager::reclaim_physical`]（VDB 清扫 +
//! 水位紧缩）双面入口——[`GcManager::tick`] 完整轮次与
//! [`GcManager::reclaim_when_scan_idle`] 扫描缺席兜底，后者由常驻
//! [`spawn_bftree_reclaimer`] 驱动（bftree 待释放队列唯一常驻消费者）。

use std::{sync::Arc, time::Duration};

use compio::{runtime::spawn, time::sleep};
use log::warn;
use wdev::Device;

use super::{GcManager, enabled_by_config};
use crate::{config::GcConfig, error::Result, store::WedbStore};

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

impl<D: Device> GcManager<D> {
  /// 换号物理回收内核（四正确性面）：死亡账本墓碑注销 / 退役租户路由释放 /
  /// 空闲路由析构（[`Self::sweep_vdb`]）+ 高低水位熔断判定与推进 begin 的日志紧缩
  /// （[`Self::try_compact`]）。`pop_reclaimable` 须待 begin 越过死亡条目 tail_address
  /// 方可摘账本，故紧缩推进与账本注销同属一个回收内核，须一并驱动。
  ///
  /// 两处共用，构成「物理回收恒被推进、不随 `gc.enabled` 关停」的单机制：
  /// - [`Self::tick`]：完整 GC 轮次（后台扫描循环在跑、及手动 [`Self::run_once`]）；
  /// - [`Self::reclaim_when_scan_idle`]：扫描循环缺席时 [`spawn_bftree_reclaimer`] 兜底。
  pub(super) async fn reclaim_physical(
    &self,
    store: &Arc<WedbStore<D>>,
    cfg: &GcConfig,
  ) -> Result<()> {
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
