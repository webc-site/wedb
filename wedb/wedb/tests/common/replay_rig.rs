//! 副本回放装配束单源（存储 + 回放资产 + 恢复门控 + 接收会话 + 宿主服务器）
//!
//! 收口 diskless 系六册逐字同形的副本整套装配（原 script_txn_replica_replay
//! 的 replica_rig 单源升格）：open_node → 角色 provider（攒批窗关窗）→
//! single_log_aof 覆盖同一 wal 挂回放资产 → 恢复门控就位（ReadRole）→
//! 接收会话挂接（见 replica_attach 单源）→ 宿主服务器（见 replica_host
//! 单源）。差异面（节点身份、端口、槽位图形态、worker 线程数）以参数暴露。
//! 宿主册直挂：
//!
//! ```text
//! #[path = "common/replay_rig.rs"]
//! mod replay_rig;
//! ```
//!
//! 宿主服务器装配尾随原 common/replica_host.rs 一并收口至
//! `wedb_test::replica_host`；存储底座与角色 provider 单源已收口至
//! `wedb_test::{node_storage, diskless_provider, replica_attach}`。

use std::{num::NonZeroUsize, sync::Arc};

use wconf::RuntimeServerOptions;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    recovery_status::RecoveryStatus, replica_replay_task::ReplayAssets,
    replication_manager::ReplicationManager,
  },
  worker::NodeRole,
};
use wedb_test::{
  diskless_provider::diskless_provider,
  node_storage::{self as common, NodeStorage},
  replica_attach::attach_replica_session,
  replica_host::{ReplicaSessionProvider, replica_host},
};
use wnode::{GarnetServer, aof::waof_sublog::single_log_aof};

/// 副本整套装配束（副本存储节点 / 角色 provider / 复制管理器 /
/// 宿主服务器 / 监听地址串）；服务器须由调用方持有至用例末并 `dispose`。
pub struct ReplayRig {
  /// 副本存储节点（wal + store）
  pub node: NodeStorage,
  /// 角色 provider
  pub provider: Arc<ClusterProvider>,
  /// 复制管理器
  pub rm: Arc<ReplicationManager>,
  /// 宿主服务器
  pub server: GarnetServer<ReplicaSessionProvider>,
  /// 监听地址串
  pub addr: String,
}

/// 副本整套装配；各册按需解构，未消费位以 `_` 前缀承接。
pub fn replay_rig(
  tag: &str,
  replica_id: u128,
  primary_id: u128,
  port: i32,
  stable_slots: bool,
  worker_threads: Option<NonZeroUsize>,
) -> ReplayRig {
  // ===== 副本：真实回放装配（ReplayAssets + single_log_aof 覆盖同一 wal）
  let node = common::open_node(tag);
  let provider = diskless_provider(
    &node,
    replica_id,
    port,
    NodeRole::Replica,
    primary_id,
    stable_slots,
  );
  let rm = provider.replication_manager().unwrap();
  let aof_options = RuntimeServerOptions::default();
  let replica_aof =
    single_log_aof(Arc::clone(&node.wal), &aof_options).expect("装配副本 single_log_aof");
  rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    replica_aof,
    Arc::clone(&node.store),
    None,
    None,
  ))));
  assert!(
    rm.begin_recovery(RecoveryStatus::ReadRole, false),
    "副本恢复门控就位"
  );
  attach_replica_session(&provider, &node.wal);
  let (server, addr) = replica_host(&provider, worker_threads);
  ReplayRig {
    node,
    provider,
    rm,
    server,
    addr,
  }
}
