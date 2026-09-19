//! 集群形态节点测试装配：集群装饰钩子工厂 + 生产宿主形态起服
//!
//! 集群集成测试共用：[`cluster_decorate`] 构造挂 ClusterSession 切面的
//! 装饰钩子，[`start_node`] 起随机端口生产宿主形态节点（AOF 门控点亮）。

use std::sync::Arc;

use aok::Result;
use tempfile::{TempDir, tempdir};
use waof::WalLog;
use wdev::SegmentedDevice;
use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wnode::{
  ClusterProviderHandle, GarnetServer, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

/// 节点装配产物（临时目录 / 服务器 / 集群提供者 / AOF 日志 / 监听端口）
pub type NodeAssembly<D> = (
  TempDir,
  GarnetServer<StorageSessionProvider<D>>,
  Arc<ClusterProvider>,
  Arc<WalLog<SegmentedDevice>>,
  u16,
);

/// 集群装饰钩子工厂：挂 ClusterSession 切面（会话基线取默认，全模式自由切库）
///
/// 闭包捕获集群提供者 Arc；`impl Fn` 返回使闭包类型可进入
/// `StorageSessionProvider<D>` / `GarnetServer<StorageSessionProvider<D>>` 签名
pub fn cluster_decorate(
  cluster: Arc<ClusterProvider>,
) -> impl Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> {
  move |network_sender_id, api| {
    let provider_handle: ClusterProviderHandle = cluster.clone();
    Some(RespSessionConsumer::with_cluster(
      network_sender_id,
      RespServerSessionOptions::default(),
      cluster.create_cluster_session(),
      provider_handle,
      Arc::new(api),
    ))
  }
}

/// 起一个生产宿主形态节点（随机端口，AOF 门控点亮）
pub fn start_node<D>(provider: Arc<ClusterProvider>, decorate: D) -> Result<NodeAssembly<D>>
where
  D:
    Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync + 'static,
{
  let dir = tempdir()?;
  let session_provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("node.db"),
    None,
    None,
    decorate,
  )?;
  let wal = session_provider
    .wal()
    .cloned()
    .expect("AOF 门控点亮后 wal 必在场");
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(session_provider),
  )?;
  server.start(None)?;
  let port = server.local_addr()?.port();
  Ok((dir, server, provider, wal, port))
}
