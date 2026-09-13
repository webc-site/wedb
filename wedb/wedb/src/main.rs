//! WeDB 分布式集群服务端主程序入口
//!
//! 集群与单机共用同一套 wnode::RespServerSession 会话体系、存储执行域与
//! StorageSessionProvider 装配基座（功能一致：存储 + 经纪 + 向量三件套同源）：
//! 集群差异仅为 decorate 钩子构造 with_cluster_session 会话消费者（挂
//! ClusterSession 切面，maxDatabases = 2），槽位验证、MOVED/ASK 重定向、
//! CLUSTER 命令族、ROLE/HELLO 集群分支均由会话主循环经切面驱动。

use std::sync::Arc;

use clap::Parser;
use wedb::{
  ClusterArgs,
  server::{cluster::IClusterProvider, cluster_provider::ClusterProvider},
};
use wnode::{
  RespSessionConsumer, ServerArgs, ServerBootstrap,
  resp::resp_server_session::RespServerSessionOptions, service::StorageSessionProvider,
};

/// 集群数据文件名（node dir 内）
const DATA_FILE: &str = "wedb-cluster.db";

/// C# 构造：EnableCluster 时 maxDatabases = 2
const CLUSTER_MAX_DATABASES: i32 = 2;

/// 单物理日志域的子日志下标（AOF 直推架构唯一子日志）
const PHYSICAL_SUBLOG_IDX: usize = 0;

/// 副本重连轮询频率秒数（C# ClusterConfig.ReplicationPollFrequencySeconds
/// 默认 1；0 = 禁用自动重连）
const REPLICATION_REESTABLISHMENT_TIMEOUT_SECS: i32 = 1;

/// 副本重连发起（C# ReplicationManager.RecoverReplication 的 AOF 直推形态：
/// 重置重放驱动仓库 + 重注册子日志驱动，随后的 INITIATE_REPLICA_SYNC 握手
/// 由主端经 ensure_replication 的会话健康检查驱动补齐数据面）
///
/// 最简有效体对标 wedb/tests/cluster_replication.rs 的重连钩子样板
fn recover_replication(provider: &Arc<ClusterProvider>, primary: &str) {
  let Some(rm) = provider.replication_manager() else {
    return;
  };
  log::info!("Recovering replication stream to primary {primary}");
  rm.reset_replica_replay_driver_store();
  rm.initialize_replica_replay_driver(PHYSICAL_SUBLOG_IDX);
}

fn main() -> wnode::Result<()> {
  let args = ClusterArgs::parse();
  ServerBootstrap::new(args)
    .with_cluster_provider(ClusterProvider::new())
    .banner("WeDB 分布式集群节点")
    .run(|args, cluster| {
      let provider = StorageSessionProvider::open(args.node_args().dir.join(DATA_FILE), {
        let cluster = Arc::clone(cluster);
        move |network_sender_id, api| {
          let options = RespServerSessionOptions {
            max_databases: CLUSTER_MAX_DATABASES,
            ..RespServerSessionOptions::default()
          };
          Some(RespSessionConsumer::with_cluster_session(
            network_sender_id,
            options,
            Arc::new(cluster.create_cluster_session()),
            api,
          ))
        }
      })?;
      // 存储注入集群提供者（对标 C# clusterProvider.storeWrapper 装配期建立；
      // CLUSTER RESET 的 HasKeysInSlots 扫描与 HARD 清库经此下达）
      cluster.set_store(Arc::clone(&provider.store));
      // 向量集合管理器注入（CLUSTER RESERVE 迁移预保留面，对标 C# 会话侧
      // vectorManager 可达面）
      cluster.set_vector_manager(Arc::clone(&provider.vector_manager));
      // AOF 门控（C# StoreWrapper.EnableAOF）：NodeArgs 无 AOF 开关选项，
      // 装配默认关闭（行为与历史逐字节一致）；点亮路径经
      // StorageSessionProvider::open_with_aof 构造后在此注入 set_aof /
      // set_replica_replication_session / set_primary_replication
      if let Some(aof) = provider.aof() {
        cluster.set_aof(Some(Arc::clone(aof)));
      }
      // 副本重连发起钩子 + 轮询频率（REPLICAOF 数据面点亮：断链后按轮询
      // 节奏自动 recover；0 = 禁用则钩子永不触发）
      cluster.set_recover_replication_hook(Some(recover_replication));
      cluster.set_replication_reestablishment_timeout(REPLICATION_REESTABLISHMENT_TIMEOUT_SECS);
      Ok(Arc::new(provider))
    })
}
