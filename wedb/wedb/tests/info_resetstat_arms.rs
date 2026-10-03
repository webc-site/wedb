//! INFO RESETSTAT 的 gossip 复位臂端到端测试
//!
//! 覆盖链：`INFO STATS` 段取数面（`wnode::resp::info_provider::SessionInfoSource::gossip_stats`
//! → 本文件的 `wnode::ClusterProvider::get_gossip_stats`）与 RESETSTAT 消费面
//! （`wmetric::GarnetServerMonitor::cleanup_global_stats` 的 STATS 分支 →
//! `ConsumerRegistry::monitor_iteration_inputs` 注入的两臂回调 →
//! `wedb::server::cluster_provider::ClusterProvider::reset_gossip_stats`）。
//!
//! C# 对位:libs/server/Metrics/GarnetServerMonitor.cs:CleanupGlobalStats 内
//! `storeWrapper.clusterProvider?.ResetGossipStats()` 一条（终点
//! libs/cluster/Server/ClusterProvider.cs:ResetGossipStats →
//! gossipStats.Reset，计数源
//! libs/cluster/Server/Gossip/GossipStats.cs:Reset）。
//!
//! 复活化臂的真实终点（`WedbStore::reviv_pool` 四计数）在存储可达面
//! wnode/tests/database_manager.rs 的 `reset_revivification_stats_zeroes_pool_counters`
//! 断言；本文件以计数占位同轮观测两臂的触达时机。

use std::{
  future::ready,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
};

use aok::Void;
use compio::runtime::Runtime;
use wedb::server::cluster_provider::ClusterProvider;
use wmetric::GarnetServerMonitor;
use wnode::{ClusterProvider as _, servers::consumer_registry::ConsumerRegistry};
use wresp::metrics::InfoMetricsType;

/// 取 INFO STATS 段 gossip 段中指定字段值（与 INFO 命令同一取数面）
fn gossip_field(provider: &ClusterProvider, name: &str) -> Option<String> {
  provider
    .get_gossip_stats(false)
    .into_iter()
    .find(|item| item.name == name)
    .map(|item| item.value)
}

/// 按宿主 `start_server_monitor` 同构驱动 N 轮采样：两臂闭包每轮重建、
/// 各持一份句柄克隆（gossip 臂直连真集群提供者，reviv 臂以计数占位）
async fn run_monitor_rounds(
  monitor: &GarnetServerMonitor,
  registry: &Arc<ConsumerRegistry>,
  cluster: &Arc<ClusterProvider>,
  reviv_resets: &Arc<AtomicU32>,
  rounds: u32,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        let gossip_handle = Arc::clone(cluster);
        let reviv = Arc::clone(reviv_resets);
        registry.monitor_iteration_inputs(
          move || gossip_handle.reset_gossip_stats(),
          move || {
            reviv.fetch_add(1, Ordering::Relaxed);
          },
        )
      },
    )
    .await;
}

/// RESETSTAT 后 INFO 的 gossip 计数回落 0；未 RESETSTAT 的采样轮不影响计数
#[test]
fn test_resetstat_arms_zero_gossip_stats() -> Void {
  Runtime::new().unwrap().block_on(async {
    let cp = ClusterProvider::new();
    let gm = cp.gossip_manager().expect("集群提供者应装配 gossip 管理器");
    gm.stats.update_meet_requests_recv();
    gm.stats.update_gossip_bytes_send(128);
    assert_eq!(
      gossip_field(&cp, "meet_requests_recv").as_deref(),
      Some("1")
    );
    assert_eq!(
      gossip_field(&cp, "gossip_bytes_send").as_deref(),
      Some("128")
    );

    let monitor = GarnetServerMonitor::new(1, true, false, false);
    let registry = Arc::new(ConsumerRegistry::new());
    let reviv_resets = Arc::new(AtomicU32::new(0));

    // 1. 未置复位标志的常规采样轮：gossip 计数与 INFO 口径不受影响
    run_monitor_rounds(&monitor, &registry, &cp, &reviv_resets, 2).await;
    assert_eq!(
      gossip_field(&cp, "meet_requests_recv").as_deref(),
      Some("1"),
      "无 RESETSTAT 的两轮采样不得清零 gossip 计数"
    );
    assert_eq!(
      (
        gm.stats.meet_requests_recv.load(Ordering::Relaxed),
        reviv_resets.load(Ordering::Relaxed)
      ),
      (1, 0),
      "两臂只随 STATS 标志触达"
    );

    // 2. INFO RESETSTAT（置 STATS 标志）的下一轮：两臂各下达一次，
    //    gossip 计数经集群层真实现回落 0
    monitor.set_info_reset_flag(InfoMetricsType::Stats);
    run_monitor_rounds(&monitor, &registry, &cp, &reviv_resets, 1).await;
    assert_eq!(
      (
        gossip_field(&cp, "meet_requests_recv").as_deref(),
        gossip_field(&cp, "gossip_bytes_send").as_deref()
      ),
      (Some("0"), Some("0")),
      "RESETSTAT 后 INFO 的 gossip 计数应回落 0"
    );
    assert_eq!(
      reviv_resets.load(Ordering::Relaxed),
      1,
      "同一 STATS 分支应同轮下达复活化臂"
    );

    // 3. 标志一次性消费：清位后新流量正常累计，不再被复位吞掉
    gm.stats.update_gossip_bytes_send(7);
    run_monitor_rounds(&monitor, &registry, &cp, &reviv_resets, 2).await;
    assert_eq!(
      gossip_field(&cp, "gossip_bytes_send").as_deref(),
      Some("7"),
      "复位标志清位后的新一轮流量应保留计数"
    );
    assert_eq!(
      reviv_resets.load(Ordering::Relaxed),
      1,
      "标志清位后两臂不应再被触达"
    );
    aok::OK
  })
}
