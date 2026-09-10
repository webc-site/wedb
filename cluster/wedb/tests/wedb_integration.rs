//! wedb 集群数据面集成测试：选主角色门控、AOF 同步与树文件迁移流重组
use std::{future::Future, io, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wedb::{
  AofSyncDriver, AofTransport, FailoverManager, NoopConsensus, Role, TreeChunkFrame, TreeFileSink,
};
use wedb_standalone::{WalConfig, WalLog};

#[derive(Default)]
struct MemoryTransport(Mutex<Vec<Vec<u8>>>);

impl AofTransport for MemoryTransport {
  fn send_frame(&self, frame: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
    self.0.lock().push(frame.to_vec());
    async { Ok(()) }
  }
}

#[test]
fn test_failover_and_migration_integration() -> Void {
  // 1. Failover gating
  let leader_engine = NoopConsensus::default();
  let failover = FailoverManager::new(leader_engine);
  assert_eq!(failover.role(), Role::Leader);
  assert!(failover.can_ship_aof());
  assert!(failover.can_accept_writes());

  // 2. Migration streaming & reassembly
  let original_payload = b"test_btree_index_stream_data_chunked_1234567890";
  let key = b"range_idx_key_1";
  let chunk_size = 10;
  let mut sink = TreeFileSink::default();

  let chunks: Vec<&[u8]> = original_payload.chunks(chunk_size).collect();
  let total_chunks = chunks.len();

  for (idx, chunk) in chunks.iter().enumerate() {
    let last = idx == total_chunks - 1;
    let frame = TreeChunkFrame {
      key,
      seq: idx as u32,
      last,
      chunk,
    };
    let wire = frame.encode();

    let decoded = TreeChunkFrame::decode(&wire).expect("valid frame decode");
    assert_eq!(decoded.seq, idx as u32);
    assert_eq!(decoded.last, last);
    sink.push(decoded).expect("sink push success");
  }

  assert!(sink.is_finished());
  assert_eq!(sink.into_data(), original_payload);

  OK
}

#[test]
fn test_aof_sync_driver_progress() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("integration_sync.wal"),
    )?);
    let wal = Arc::new(WalLog::new(device, WalConfig::default())?);

    let a = wedb_standalone::encode_entry(wedb_standalone::AofOp::RiCreate, 1, b"k1", b"blob-a");
    let addr_a = wal.enqueue(&a)?;
    wal.commit().await?;

    let transport = Arc::new(MemoryTransport::default());
    let driver = AofSyncDriver::new(Arc::clone(&wal), Arc::clone(&transport));

    let next = driver.ship_since(addr_a).await?;
    let sent = transport.0.lock();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0], a);
    assert_eq!(next, wal.committed_until_address());

    OK
  })
}
