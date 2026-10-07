#![recursion_limit = "256"]
//! 副本检查点接收臂错误分流集成测试（票 zcode-r135c 案一）
//!
//! 旧形态：`checkpoint_import_ctx` 把三态（目录未接线 / 引擎未接线 /
//! `create_dir_all` 磁盘失败）全塞进同一 String，唯一消费点
//! `execute_checkpoint_recv` 的 `let Ok else` 兜底臂一律回 CLUSTERNOTINIT，
//! 盘硬错与配置态不可辨、IO 因由整体丢弃。
//! 修复后：签名改 typed `Error`（复用既有 `Io(transparent)` /
//! `ClusterNotInitialized` 变体零新增），接收臂按两态分流应答帧。
//! 全部走真实错误路径：只读父目录注入 `create_dir_all` 失败（EPERM/EACCES），
//! 无 mock。

use std::{fs, path::PathBuf, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wedb::{
  Error,
  server::{
    cluster_config::ClusterConfig,
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    worker::{LocalWorkerSpec, NodeRole},
  },
};
use wedb_test::{
  cluster_consumer::cluster_consumer,
  store_node::{StoreNode, open_store},
};
use wkv::WedbStore;
use wnode::{MessageConsumerFace, RespSessionConsumer};
use wtest_base::resp_frame;

/// 副本角色 provider（rm/store 全接线；checkpoint_dir 由调用方按用例注入）
fn replica_provider(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0002,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(0x0DE1_0000_0000_0000_0000_0000_0000_0001),
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(store));
  provider
}

/// 帧直投收割应答（本文件用例全走同步段错误臂，不触发慢路径）
fn feed(c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame_bytes);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  assert!(c.take_slow_wait().is_none(), "错误臂不应挂起慢路径");
  out
}

/// SNAPSHOT_DATA 接收帧（token + filetype 4 + 段地址 0 + 非空载荷）
fn snapshot_data_frame() -> Vec<u8> {
  let token = 0xABCD_EF01_2345_6789_ABCD_EF01_2345_6789u128;
  resp_frame(&[
    b"CLUSTER",
    b"SNAPSHOT_DATA",
    &token.to_le_bytes(),
    b"4",
    b"0",
    b"payload",
  ])
}

/// 磁盘 IO 失败注入：把 checkpoint_dir 置于普通文件路径下，令 create_dir_all
/// 必定触发 IO 错误（跨平台：Unix 报 ENOTDIR，Windows 报 ERROR_ALREADY_EXISTS/ERROR_DIRECTORY，
/// 且即使在 root 环境下也 100% 拒绝，无需依赖目录只读权限）
fn uncreatable_child_dir(tag: &str, dir: &tempfile::TempDir) -> PathBuf {
  let blocking_file = dir.path().join(format!("{tag}_file"));
  fs::write(&blocking_file, b"collision_marker").unwrap();
  blocking_file.join("checkpoints")
}

/// 类型面：目录未接线 → ClusterNotInitialized（配置态，拓扑收敛后可重试）
#[test]
fn checkpoint_import_ctx_unwired_dir_is_cluster_not_initialized() {
  let StoreNode { _dir, store } = open_store("unwired.db");
  let provider = replica_provider(&store);
  let err = provider
    .checkpoint_import_ctx()
    .err()
    .expect("目录未接线必须报错");
  assert!(
    matches!(err, Error::ClusterNotInitialized),
    "未接线臂必须落 ClusterNotInitialized 变体: {err:?}"
  );
}

/// 类型面：磁盘 IO 失败 → Error::Io 透明转发（含 path/errno 因由链）
#[test]
fn checkpoint_import_ctx_disk_io_error_is_typed_io() {
  let StoreNode { _dir: dir, store } = open_store("ro_type.db");
  let cp_dir = uncreatable_child_dir("ro_type", &dir);
  let provider = replica_provider(&store);
  provider.set_checkpoint_dir(cp_dir.clone());
  let err = provider
    .checkpoint_import_ctx()
    .err()
    .expect("文件节点下建目录必须令 create_dir_all 失败");
  let Error::Io(io) = &err else {
    panic!("磁盘失败必须落 Io(transparent) 变体: {err:?}");
  };
  let text = io.to_string();
  assert!(
    text.contains(&cp_dir.display().to_string())
      || text.contains("Not a directory")
      || text.contains("already exists")
      || text.contains("directory name is invalid")
      || text.contains("Permission denied"),
    "Io 透明链必须保全 path/errno 因由: {text}"
  );
}

/// 帧面零漂移：目录未接线仍逐字节回 CLUSTERNOTINIT 原帧
#[test]
fn snapshot_data_recv_arm_keeps_notinit_frame_when_unwired() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let StoreNode { _dir, store } = open_store("notinit_frame.db");
    let provider = replica_provider(&store);
    let mut consumer = cluster_consumer(&provider);
    let resp = feed(&mut consumer, &snapshot_data_frame());
    assert_eq!(
      resp, b"-ERR Cluster not initialized\r\n",
      "配置态臂应答帧必须逐字节零漂移"
    );
  });
}

/// 帧面分流：磁盘注入 create_dir_all 失败，接收臂应答帧非
/// CLUSTERNOTINIT 且含 IOERR 前缀与 errno 因由（主端可辨「盘错」）
#[test]
fn snapshot_data_recv_arm_reports_ioerr_not_notinit_on_disk_error() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let StoreNode { _dir: dir, store } = open_store("ioerr_frame.db");
    let cp_dir = uncreatable_child_dir("ioerr_frame", &dir);
    let provider = replica_provider(&store);
    provider.set_checkpoint_dir(cp_dir.clone());
    let mut consumer = cluster_consumer(&provider);
    let resp = feed(&mut consumer, &snapshot_data_frame());
    let text = String::from_utf8_lossy(&resp).into_owned();
    assert!(
      text.starts_with("-IOERR create checkpoint dir:"),
      "盘硬错臂必须回 IOERR 前缀帧: {text:?}"
    );
    assert!(
      !text.contains("Cluster not initialized"),
      "盘硬错不得再坍缩为 CLUSTERNOTINIT: {text:?}"
    );
  });
}
