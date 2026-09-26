//! AOF 复制推流泵断连可观测性集成测试：断言错误面日志包含 remote_node_id、错误原文与 accepted 位点

use std::sync::Arc;

use compio::runtime::Runtime;
use log::{Level, LevelFilter, Log, Metadata, Record};
use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wbase::hex::hex_str_u128;
use wdev::SegmentedDevice;
use wedb::server::replication::{
  aof_replication_pump::AofReplicationPump,
  aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_DEAD: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000D;
const HEADER_PAD_PAYLOAD_LEN: usize = 56;

static LOGS: Mutex<Vec<(Level, String)>> = Mutex::new(Vec::new());

struct TestLogger;

impl Log for TestLogger {
  fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
    true
  }

  fn log(&self, record: &Record<'_>) {
    LOGS
      .lock()
      .push((record.level(), record.args().to_string()));
  }

  fn flush(&self) {}
}

#[test]
fn disconnected_replica_logs_warn_with_node_id_and_error() {
  let _ = log::set_boxed_logger(Box::new(TestLogger));
  log::set_max_level(LevelFilter::Warn);

  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(
      SegmentedDevice::single_file(dir.path().join("primary.wal")).expect("create wal device"),
    );
    let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"));

    wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
    for i in 0..3u8 {
      wal.enqueue(format!("record-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();

    let store = Arc::new(AofSyncDriverStore::new(1));
    let driver = Arc::new(AofSyncDriver::new(
      PRIMARY_ID,
      REPLICA_DEAD,
      1,
      &AofAddress::create(1, 64),
      None,
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));
    driver.task_ref(0).unwrap().set_connected(false);

    let pump = AofReplicationPump::new(Arc::clone(&store));
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
    assert_eq!((forwarded, skipped), (0, 1), "断连副本应终止补扫");

    let expected_hex = hex_str_u128(REPLICA_DEAD);
    let captured = LOGS.lock().clone();
    let matched = captured.iter().find(|(lvl, msg)| {
      *lvl == Level::Warn
        && msg.contains(&expected_hex)
        && msg.contains("AOF stream client disconnected!")
        && msg.contains("accepted:")
    });

    assert!(
      matched.is_some(),
      "应记录包含远端节点 ID hex 与错误原文的 warn 日志，实际捕获：{captured:?}"
    );
  });
}
