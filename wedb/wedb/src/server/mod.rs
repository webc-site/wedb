pub mod boot;
pub mod cluster;
pub mod cluster_config;
pub mod cluster_manager;
pub mod cluster_manager_slot_gate;
pub mod cluster_manager_slot_state;
pub mod cluster_manager_worker_state;
pub mod cluster_provider;
pub mod cluster_session;
pub mod connection_info;
pub mod failover;
pub mod gossip;
pub mod hash_slot;
pub mod migration;
pub mod replication;
pub mod slot_verify;
pub mod sync_transport;
pub mod worker;

use std::{future::Future, time::Duration};

use compio::time::timeout;

/// compio 定时器的无限形态：compio 的 `timeout(d, f)` 内部
/// `Instant::now() + d`，d 取 `Duration::MAX` 即溢出 panic，故无限时
/// （None = 对标 C# `Timeout.InfiniteTimeSpan`，RuntimeServerConfig.cs:320
/// 非正值语义）不挂计时器直接 await。超时到点返 None
pub(crate) async fn wait_async<T, F: Future<Output = T>>(
  limit: Option<Duration>,
  fut: F,
) -> Option<T> {
  match limit {
    Some(d) => timeout(d, fut).await.ok(),
    None => Some(fut.await),
  }
}
