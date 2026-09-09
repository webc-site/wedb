//! NodeService 编排集成测试：apply → log 顺序、提交后回放与条目分发

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StorageBackend, StoreConfig, WedbStore};
use wnode::{aof::AofOp, service::NodeService};

/// 与 wkv/tests/range_index_scan.rs TUNE 对齐的合法调优参数
const TUNE: wkv::TreeTuning = wkv::TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

type TestEnv = (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

/// 在临时目录装配 存储引擎 + 预写日志 双设备
fn open_node(name: &str) -> aok::Result<TestEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  Ok((dir, store, wal))
}

/// 回放收集器：记录操作类型与键，断言用
#[derive(Default)]
struct CollectingReplay {
  seen: Vec<(AofOp, Vec<u8>)>,
}

impl wnode::Replay for CollectingReplay {
  fn on_entry(&mut self, entry: wnode::AofEntryRef<'_>) -> wnode::AofResult<()> {
    self.seen.push((entry.op, entry.key.to_vec()));
    Ok(())
  }
}

#[test]
fn ri_ops_apply_then_log_and_replay() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_replay")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackend::Memory, TUNE)
      .await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;
    service.ri_set(b"idx", b"field-2", b"value-0002").await?;
    service.ri_del(b"idx", b"field-2").await?;
    wal.commit().await?;

    let mut replay = CollectingReplay::default();
    let count = service.replay(&mut replay).await?;
    assert_eq!(count, 4);
    assert_eq!(
      replay.seen,
      vec![
        (AofOp::RiCreate, b"idx".to_vec()),
        (AofOp::RiSet, b"idx".to_vec()),
        (AofOp::RiSet, b"idx".to_vec()),
        (AofOp::RiDel, b"idx".to_vec()),
      ]
    );
    OK
  })
}

#[test]
fn ri_del_missing_field_still_logs() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_skip")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackend::Memory, TUNE)
      .await?;
    let deleted = service.ri_del(b"idx", b"absent").await?;
    assert!(deleted);
    wal.commit().await?;

    let mut replay = CollectingReplay::default();
    assert_eq!(service.replay(&mut replay).await?, 2);
    assert_eq!(replay.seen[0].0, AofOp::RiCreate);
    assert_eq!(replay.seen[1].0, AofOp::RiDel);
    OK
  })
}

#[test]
fn ri_set_reaches_bftree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_data")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service.ri_create(b"idx", StorageBackend::Std, TUNE).await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;

    let got = service
      .session()
      .range_index_get(b"idx", b"field-1")
      .await?;
    assert_eq!(got.as_deref(), Some(&b"value-0001"[..]));
    OK
  })
}
