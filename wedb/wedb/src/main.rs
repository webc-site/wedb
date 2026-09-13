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
      Ok(Arc::new(provider))
    })
}
