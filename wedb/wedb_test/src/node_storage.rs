//! 独立存储节点底座单源（db + wal 双件，无盘全量同步系）
//!
//! 收口各 diskless/快照/过期回放系测试逐字同形的装配段：独立存储节点、
//! 带角色 provider。消费面经 `wedb_test::node_storage` 引用（原
//! common/mod.rs 聚合根已收口进本 crate）；宿主服务器装配尾见
//! `wedb_test::replica_host`（同系 crate 化单源）；测试专属差异面
//! （节点身份、槽位图形态）以参数暴露，禁全局状态。

use std::sync::Arc;

use waof::{WalConfig, WalLog};
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wtest_base::test_store_config;

/// 独立存储节点（db 文件 + wal）
pub struct NodeStorage {
  /// 临时目录守卫（Drop 即清理落盘文件）
  pub _dir: tempfile::TempDir,
  pub store: Arc<WedbStore<SegmentedDevice>>,
  pub wal: Arc<WalLog<SegmentedDevice>>,
}

/// 开一套临时目录内的独立存储节点（db + wal 各一文件，测试配置）
pub fn open_node(tag: &str) -> NodeStorage {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  NodeStorage {
    _dir: dir,
    store,
    wal,
  }
}

/// 带角色 provider（对齐宿主装配：rm/store/wal 全接线，集群配置自持；
/// leader 攒批窗是否关窗由 `diskless_sync_delay` 参数位决定）
///
/// - `primary_id`：副本角色固定挂靠的主端 ID（APPENDLOG init 的主 ID 校验
///   依赖该字段；主端角色不取用）
/// - `stable_slots`：true = 槽位图全量 Stable 本地（真 RESP 写命令门评走
///   Stable 本地臂；false 沿用 `ClusterConfig::new()` 默认槽位图）
/// - `diskless_sync_delay`：Some(n) = 攒批窗延迟设为 n 秒（各 diskless 系
///   测试传 Some(0) 关窗）；None = 不调该 setter，保留 provider 默认窗
///   （检查点导入系 provider 原生不经攒批窗 setter，形态由本参数保持）
pub fn provider_with_role(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
  primary_id: u128,
  stable_slots: bool,
  diskless_sync_delay: Option<i32>,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  if let Some(secs) = diskless_sync_delay {
    provider.set_replica_diskless_sync_delay(secs);
  }
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: (role == NodeRole::Replica).then_some(primary_id),
    hostname: None,
  });
  if stable_slots {
    for s in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[s] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_wal(Arc::clone(&node.wal));
  provider
}
