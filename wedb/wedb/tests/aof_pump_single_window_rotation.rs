//! AOF 推流泵单趟窗口 + 轮转公平集成测试（自
//! src/server/replication/aof_replication_pump.rs 内联测迁入，断言与覆盖
//! 原样保留；暴露面经 [`doc(hidden)`] 测试专用口 `pump_backlog` /
//! `AofReplicationPump::throttle_tx`，见定义处注）

use std::sync::Arc;

use compio::runtime::Runtime;
use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::replication::{
  aof_replication_pump::{AofReplicationPump, pump_backlog},
  aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
};
use wedb_test::replica_wire_test_wire::{FrameSink, callback_wire};

/// 单趟窗口受限 + 轮转公平（对标 C# BulkConsumeAllAsync maxChunkSize 分块
/// 与每副本独立任务的并发推进）：积压超过单趟窗口时一轮 pump_backlog 只
/// 推进各副本一个窗口即换驱动——首驱动不再追尾扫至日志尾无限占泵，
/// 快照序靠后的副本同轮被触达
#[test]
fn pump_backlog_single_window_rotates_across_drivers() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(
      SegmentedDevice::single_file(dir.path().join("primary.wal")).expect("create wal device"),
    );
    let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"));

    wal.enqueue(&[0u8; 56]).unwrap();
    // 积压 2048 × 1024B ≈ 2MB > 单趟窗口 REPLAY_CHUNK_BYTES (1MB)
    for _ in 0..2048u32 {
      wal.enqueue(&[0u8; 1024]).unwrap();
    }
    wal.commit().await.unwrap();
    let tail = wal.tail_address() as i64;

    let store = Arc::new(AofSyncDriverStore::new(1));
    let driver_a = Arc::new(AofSyncDriver::new(
      1,
      0xA,
      1,
      &AofAddress::create(1, 64),
      None,
    ));
    let driver_b = Arc::new(AofSyncDriver::new(
      1,
      0xB,
      1,
      &AofAddress::create(1, 64),
      None,
    ));
    driver_a.attach_wire(callback_wire(FrameSink::Buffer(Arc::new(Mutex::new(
      Vec::new(),
    )))));
    driver_b.attach_wire(callback_wire(FrameSink::Buffer(Arc::new(Mutex::new(
      Vec::new(),
    )))));
    assert!(store.try_add_replication_driver(driver_a.clone(), false));
    assert!(store.try_add_replication_driver(driver_b.clone(), false));

    let pump = AofReplicationPump::new(Arc::clone(&store));
    let (forwarded, skipped) = pump_backlog(&store, &pump.throttle_tx, &wal).await;
    assert_eq!(skipped, 0);
    assert!(forwarded > 0);

    // 副本 A 未追平：单趟窗口封顶，不再追尾扫至日志尾
    let pos_a = driver_a.get_previous_address(0);
    assert!(
      pos_a < tail,
      "单趟窗口受限，首驱动不得在一轮内追平全量积压（{pos_a} vs {tail}）"
    );
    // 副本 B 同轮被触达：轮转后推进越过起点（追尾 loop 不设界时 B 在
    // 持续写入下永不被触达）
    let pos_b = driver_b.get_previous_address(0);
    assert!(pos_b > 64, "轮转游标应使后续副本同轮被触达（{pos_b}）");
  });
}
