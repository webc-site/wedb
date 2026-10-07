#![recursion_limit = "256"]
//! 主端全量同步后续建连失败退钉回归（对标 C# ReplicaSyncSession.cs
//! SendCheckpointAsync catch 块 `if (aofSyncDriver != null)
//! TryRemove(aofSyncDriver)` 的异常清理契约）：
//! FullResync 钉线驱动在册后 TCP 建连失败，驱动必须出册——否则幽灵驱动
//! 零推流进度、previous_address 恒钉预锁位点，safe_truncate_aof 与背压
//! 闸门被永久钳制（AOF 删段失效 + 写背压闭锁）。

use std::{fs::create_dir_all, sync::Arc};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wcpr::{CheckpointType, create_checkpoint};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump,
    aof_sync_driver::AofSyncDriver,
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    error::{ConnectStage, ReplicationError},
    replica_sync_session::{ReplicaSyncSession, SendReaderGuard},
    sync_metadata::SyncMetadata,
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
    res.as_ref().is_err_and(|e| {
      matches!(
        e,
        ReplicationError::Connect {
          stage: ConnectStage::AofSync,
          ..
        }
      )
    }),
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

#[test]
fn send_reader_guard_pins_and_unpins_live_delete_floor() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (store_dir, store) = wtest_base::open_test_store("guard_pin")?;
    let cp_dir = store_dir.path().join("checkpoints");
    create_dir_all(&cp_dir)?;
    let meta = create_checkpoint(
      store.as_ref(),
      &store.ckpt_gate,
      &cp_dir,
      CheckpointType::FoldOver,
    )
    .await?;

    let provider = ClusterProvider::new();
    provider.set_store(Arc::clone(&store));
    provider.set_checkpoint_dir(cp_dir);
    let rm = provider.replication_manager().unwrap();

    let sector = store.device.sector_size() as u64;
    let aligned = meta.hlog_meta.begin_address / sector * sector;

    let entry = || {
      let mut m = CheckpointMetadata::new(1);
      m.store_version = 1;
      m.store_hlog_token = meta.token;
      m.store_index_token = meta.token;
      CheckpointEntry::new(m)
    };

    let mut guard_a = SendReaderGuard::new(&provider, &rm);
    guard_a.replace(Some(Arc::new(entry()))).await;
    assert_eq!(rm.snapshot_reader_count(), 1, "取条目即登记");
    assert_eq!(
      store.hlog().reader_pin(),
      aligned,
      "水位 = 条目扇区对齐 begin"
    );
    assert_eq!(entry().reader_count(), 0, "未入册条目不计读者");

    // 钉在册期地板抬升被钉位点钳制（发布侧 raise 内部取 min，fetch_max
    // 单调只升不降）：抬升后地板须停在 max(既有值, 钉位点)——检查点发布
    // 链已按未对齐 begin 抬过地板，既有值可高于钉位点；撤钳实现即越钉
    // 直达抬升目标，本断言必红
    let floor_before = store.hlog().delete_floor();
    store.hlog().raise_delete_floor(aligned + 0x10_0000);
    assert_eq!(
      store.hlog().delete_floor(),
      floor_before.max(aligned),
      "地板抬升须被在传读者钉钳制"
    );

    // 同 token 第二守卫：共享计数，单守卫退场不抬他会话的钉
    let mut guard_b = SendReaderGuard::new(&provider, &rm);
    guard_b.replace(Some(Arc::new(entry()))).await;
    assert_eq!(rm.snapshot_reader_count(), 1, "同 token 共享在册条目");
    guard_a.release();
    assert_eq!(rm.snapshot_reader_count(), 1);
    assert_eq!(store.hlog().reader_pin(), aligned, "单守卫退场不回抬水位");

    // 全部注销：回无读者哨兵，地板抬升放行
    guard_b.release();
    assert_eq!(rm.snapshot_reader_count(), 0);
    assert_eq!(store.hlog().reader_pin(), u64::MAX);
    store.hlog().raise_delete_floor(aligned + 0x10_0000);
    assert_eq!(
      store.hlog().delete_floor(),
      aligned + 0x10_0000,
      "全部注销后地板抬升放行"
    );
    Ok(())
  })
}
