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

#[path = "common/cluster_consumer.rs"]
mod cluster_cc;

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};

use cluster_cc::cluster_consumer;
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
use wkv::WedbStore;
use wnode::{MessageConsumerFace, RespSessionConsumer};
use wtest_base::{resp_frame, test_store_config};

/// 独立测试库（db 文件 + wal 设备）
fn open_store(tag: &str) -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&device)).unwrap());
  (dir, store)
}

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

/// 只读父目录注入：把 checkpoint_dir 置于 0o500 父目录下，返回不可创建的
/// 子目录路径（root 环境无法注入即显式失败，绝不静默放行假通过）
fn read_only_child_dir(tag: &str, dir: &tempfile::TempDir) -> PathBuf {
  let parent = dir.path().join(format!("{tag}_ro"));
  fs::create_dir_all(&parent).unwrap();
  fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
  let probe = fs::create_dir(parent.join("__inject_probe__"));
  if probe.is_ok() {
    panic!("测试环境必须以非 root 运行以注入只读目录 IO 失败（create_dir 未被 EACCES 拒绝）");
  }
  parent.join("checkpoints")
}

/// 类型面：目录未接线 → ClusterNotInitialized（配置态，拓扑收敛后可重试）
#[test]
fn checkpoint_import_ctx_unwired_dir_is_cluster_not_initialized() {
  let (_dir, store) = open_store("unwired");
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

/// 类型面：只读父目录 → Error::Io 透明转发（含 path/errno 因由链）
#[test]
fn checkpoint_import_ctx_read_only_parent_is_typed_io() {
  let (dir, store) = open_store("ro_type");
  let cp_dir = read_only_child_dir("ro_type", &dir);
  let provider = replica_provider(&store);
  provider.set_checkpoint_dir(cp_dir.clone());
  let err = provider
    .checkpoint_import_ctx()
    .err()
    .expect("只读父目录必须令 create_dir_all 失败");
  let Error::Io(io) = &err else {
    panic!("磁盘失败必须落 Io(transparent) 变体: {err:?}");
  };
  let text = io.to_string();
  assert!(
    text.contains(&cp_dir.display().to_string()) || text.contains("Permission denied"),
    "Io 透明链必须保全 path/errno 因由: {text}"
  );
  fs::set_permissions(cp_dir.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
}

/// 帧面零漂移：目录未接线仍逐字节回 CLUSTERNOTINIT 原帧
#[test]
fn snapshot_data_recv_arm_keeps_notinit_frame_when_unwired() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = open_store("notinit_frame");
    let provider = replica_provider(&store);
    let mut consumer = cluster_consumer(&provider);
    let resp = feed(&mut consumer, &snapshot_data_frame());
    assert_eq!(
      resp, b"-ERR Cluster not initialized\r\n",
      "配置态臂应答帧必须逐字节零漂移"
    );
  });
}

/// 帧面分流：只读父目录注入 create_dir_all 失败，接收臂应答帧非
/// CLUSTERNOTINIT 且含 IOERR 前缀与 errno 因由（主端可辨「盘错」）
#[test]
fn snapshot_data_recv_arm_reports_ioerr_not_notinit_on_disk_error() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (dir, store) = open_store("ioerr_frame");
    let cp_dir = read_only_child_dir("ioerr_frame", &dir);
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
    fs::set_permissions(cp_dir.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
  });
}
