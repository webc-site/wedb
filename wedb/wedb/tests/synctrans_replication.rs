//! zcode-r25-synctrans 专扫修复端到端与协议契约对标测试：
//! - 发现一：尾段驱动登记带损放行逃生门（allow_data_loss 透传与放行）
//! - 发现二：DataLossCheck 以当下活体 Log.BeginAddress 为基线（非快照覆盖起点）
//! - 发现三：驱动置换同锁原地原子覆盖（无先摘后挂空档、旧驱动 dispose）

use std::sync::Arc;

use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    aof_sync_driver::AofSyncDriver,
    replica_sync_session::ReplicaSyncSession,
    replica_wire::{AofSyncWire, test_wire::CallbackWire},
  },
};

const PRIMARY_ID: u128 = 0x10CA_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x10CA_0000_0000_0000_0000_0000_0000_0002;

fn test_wire() -> AofSyncWire {
  AofSyncWire::Callback(Arc::new(CallbackWire::new(Arc::new(
    Mutex::new(Vec::new()),
  ))))
}

fn open_test_wal(dir: &tempfile::TempDir) -> Arc<WalLog<SegmentedDevice>> {
  let dev = Arc::new(
    SegmentedDevice::single_file(dir.path().join("primary.wal")).expect("create wal device"),
  );
  Arc::new(WalLog::new(dev, WalConfig::default()).expect("create wal"))
}

/// 发现一测试：attach_replica_wire 尾段驱动登记透传 allow_data_loss，恢复带损放行逃生门
#[test]
fn test_finding1_attach_replica_wire_allow_data_loss_escape_hatch() {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();
  let session = ReplicaSyncSession::new(Arc::clone(&rm));

  // 主端截断线推进至 500
  rm.aof_sync_driver_store
    .update_truncated_until(&AofAddress::create(1, 500));

  let start_addr = AofAddress::create(1, 300);

  // 1. 不带损形态：尾段登记确定性拒绝
  assert!(
    !session.attach_replica_wire(
      PRIMARY_ID,
      REPLICA_ID,
      test_wire(),
      &start_addr,
      None,
      false,
    ),
    "不允许丢数据时，低于截断线的 attach_replica_wire 应被拒绝"
  );
  assert_eq!(rm.aof_sync_driver_store.count(), 0);

  // 2. 带损形态：恢复 C# 带损放行语义，收敛成功
  assert!(
    session.attach_replica_wire(PRIMARY_ID, REPLICA_ID, test_wire(), &start_addr, None, true,),
    "允许丢数据时，低于截断线的 attach_replica_wire 应带损放行"
  );
  assert_eq!(rm.aof_sync_driver_store.count(), 1);
  assert_eq!(
    rm.aof_sync_driver_store.drivers()[0].remote_node_id(),
    REPLICA_ID
  );
}

/// 发现二测试：DataLossCheck 比对基准为活体 Log.BeginAddress（非快照覆盖起点）
/// 对标 C# DataLossCheck(possibleAofDataLoss, syncFromAofAddress)
#[test]
fn test_finding2_data_loss_check_uses_live_wal_begin_address() {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();

  let dir = tempfile::tempdir().unwrap();
  let wal = open_test_wal(&dir);

  // 主端物理日志起点在 200（快照期间发生物理截断删段）
  let begin_addr = AofAddress::create(rm.sublog_count() as i32, 200);

  // 副本恢复位点 100（虽与历史快照覆盖起点 100 相同，但已被主端活体日志截断）
  let truncated_req = AofAddress::create(rm.sublog_count() as i32, 100);

  // 不带损：抛出 C# 同款错误文案
  let err = rm
    .data_loss_check(false, &truncated_req, &begin_addr)
    .unwrap_err();
  assert!(
    err.contains("Failed syncing because replica requested truncated AOF address"),
    "错误信息必须对标 C# DataLossCheck: {err}"
  );

  // 带损：降级为告警放行
  assert!(
    rm.data_loss_check(true, &truncated_req, &begin_addr)
      .is_ok(),
    "带损形态下低于 Log.BeginAddress 的位点应告警放行"
  );

  // 正常路径：位点高于日志起点，任何形态均放行
  let normal_req = AofAddress::create(rm.sublog_count() as i32, 250);
  assert!(rm.data_loss_check(false, &normal_req, &begin_addr).is_ok());
  assert!(rm.data_loss_check(true, &normal_req, &begin_addr).is_ok());

  let _ = wal;
}

/// 发现三测试：attach_replica_wire 原地原子置换预锁驱动，消除先摘后挂空档并 dispose 旧实例
#[test]
fn test_finding3_attach_replica_wire_in_place_atomic_replacement() {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();
  let session = ReplicaSyncSession::new(Arc::clone(&rm));

  // 预锁阶段钉线驱动（位点 100）
  let pin_driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 100),
    None,
  ));
  assert!(
    rm.aof_sync_driver_store
      .try_add_replication_driver(Arc::clone(&pin_driver), false)
  );
  assert_eq!(rm.aof_sync_driver_store.count(), 1);
  assert!(pin_driver.is_connected());

  // 尾段 attach_replica_wire 置换为新驱动（位点 200）
  assert!(session.attach_replica_wire(
    PRIMARY_ID,
    REPLICA_ID,
    test_wire(),
    &AofAddress::create(1, 200),
    None,
    false,
  ));

  // 断言总数恒为 1（原地置换，无中间 0 驱动状态）
  assert_eq!(rm.aof_sync_driver_store.count(), 1);

  // 旧驱动已被原地 dispose
  assert!(
    !pin_driver.is_connected(),
    "被置换的预锁驱动必须被自动 dispose"
  );

  // 新驱动正常在册且位点推进
  let drivers = rm.aof_sync_driver_store.drivers();
  assert_eq!(drivers.len(), 1);
  assert_eq!(drivers[0].remote_node_id(), REPLICA_ID);
  assert!(drivers[0].is_connected());
  assert_eq!(
    rm.aof_sync_driver_store
      .min_aof_address_from_active_sync_tasks()
      .get(0),
    Some(200)
  );
}
