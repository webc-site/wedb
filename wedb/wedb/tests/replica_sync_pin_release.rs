//! 主端全量同步后续建连失败退钉回归（对标 C# ReplicaSyncSession.cs
//! SendCheckpointAsync catch 块 `if (aofSyncDriver != null)
//! TryRemove(aofSyncDriver)` 的异常清理契约）：
//! FullResync 钉线驱动在册后 TCP 建连失败，驱动必须出册——否则幽灵驱动
//! 零推流进度、previous_address 恒钉预锁位点，safe_truncate_aof 与背压
//! 闸门被永久钳制（AOF 删段失效 + 写背压闭锁）。

use std::sync::Arc;

use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump, aof_sync_driver::AofSyncDriver,
    replica_sync_session::ReplicaSyncSession, sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

const PRIMARY_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_0002;

/// 副本协商元数据：current_aof_begin_address 高于主端 AOF 起点（副本自身
/// 日志已物理截断过高，协商断层）→ FullResync；本地检查点缺席走 skip 直推
///（send_checkpoint_and_recover 返回 Ok(None)），钉线驱动保持在册直达建连段
fn full_resync_meta() -> SyncMetadata {
  SyncMetadata {
    full_sync: true,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: String::new(),
    current_store_version: -1,
    current_aof_begin_address: AofAddress::create(1, 100),
    current_aof_tail_address: AofAddress::create(1, 0),
    current_replication_offset: AofAddress::create(1, 0),
    checkpoint_entry: None,
  }
}

/// FullResync 预锁钉线在册 + 建连失败：驱动出册、截断线解除钳制
#[compio::test]
async fn connect_failure_after_pin_releases_driver_and_unclamps_truncation() {
  let dir = tempfile::tempdir().unwrap();
  let wal_device = Arc::new(SegmentedDevice::single_file(dir.path().join("primary.wal")).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());

  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  // 允许丢数据形态（C# 派生式 FastAofTruncate && !OnDemandCheckpoint）：
  // 截断线前移的按需重拍判定命中时直接落回 skip 直推，不阻塞到建连段
  provider.set_fast_aof_truncate(true);
  provider.set_on_demand_checkpoint(false);
  let rm = provider.replication_manager().unwrap();
  let assets = PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  };

  // 预锁钉线（send_checkpoint_and_recover 预锁成功的在册形态：pin_start=40
  // 的真实驱动入真实 store）
  let pin_driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 40),
    None,
  ));
  assert!(
    rm.aof_sync_driver_store
      .try_add_replication_driver(pin_driver, false)
  );
  assert_eq!(rm.aof_sync_driver_store.count(), 1, "预锁钉线在册");

  // 泄漏危害对照：钉线在册时截断推进被钳制在 pin 位点
  let clamped = rm
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(1, 100))
    .await;
  assert_eq!(clamped.get(0), Some(40), "钉线在册应钳制截断位点");

  // 对不可达端点发起全量同步：快照段 skip 直推，第 3 步 TCP 建连失败
  let res = assets
    .sync_session
    .initiate_replica_sync(
      &provider,
      &assets,
      PRIMARY_ID,
      "127.0.0.1:1",
      &full_resync_meta(),
    )
    .await;
  assert!(
    res
      .as_ref()
      .is_err_and(|e| e.contains("Failed connecting to replica for aofSync")),
    "建连失败应上抛 aofSync 建连错误: {res:?}"
  );

  // 退钉断言：幽灵驱动必须彻底出册（对标 C# catch 块 TryRemove）
  assert_eq!(
    rm.aof_sync_driver_store.count(),
    0,
    "建连失败后钉线驱动必须出册"
  );
  assert!(
    rm.aof_sync_driver_store
      .drivers()
      .iter()
      .all(|d| d.remote_node_id() != REPLICA_ID),
  );

  // 截断线解除钳制：推进位点与目标一致，不再被历史 pin_start 阻滞
  let unclamped = rm
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(1, 100))
    .await;
  assert_eq!(unclamped.get(0), Some(100), "退钉后截断应推进到目标位点");
}
