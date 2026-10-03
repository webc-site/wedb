//! INFO 存储域快照与诊断转储集成测试
//!
//! 在 garnet 中的相对路径:
//! - libs/server/StoreWrapper.cs:GetDatabasesSnapshot
//! - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:DumpDistributionInternal

use std::{
  fs::create_dir_all,
  sync::Arc,
};

use aok::{OK, Void};
use tempfile::tempdir;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

#[compio::test]
async fn store_snapshot_aggregates_real_watermarks() -> Void {
  let dir = tempdir()?;
  create_dir_all(dir.path())?;
  let db_path = dir.path().join("stats_test.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  let before = store.store_snapshot();
  // 写入确定量键值推进日志水位与索引条目
  for i in 0..50u32 {
    let key = format!("stats:key:{i}");
    let val = format!("stats:value:{i}");
    session.upsert(key.as_bytes(), val.as_bytes()).await?;
  }
  let after = store.store_snapshot();

  // 桶数 / 桶大小 / 页数为静态配置直读
  assert_eq!(after.index_bucket_count, 64);
  assert_eq!(after.index_bucket_size_bytes, 64);
  assert_eq!(after.log_num_pages, 16);
  assert_eq!(
    after.log_page_size_bytes,
    DEFAULT_SECTOR_SIZE as u64,
    "页大小取构造配置"
  );
  // 日志地址随写入单调推进
  assert!(after.log_tail_address > before.log_tail_address);
  assert!(after.log_flushed_until_address >= before.log_flushed_until_address);
  // 溢出计数守恒：分配数 ≥ 在用数（free_count 是空闲栈残留子集）
  assert!(after.index_overflow_bucket_count >= after.index_overflow_free_bucket_count);
  // 读缓存未启用为 None
  assert!(after.read_cache.is_none());
  Ok(())
}

#[compio::test]
async fn hash_distribution_dump_reflects_real_structure() -> Void {
  let dir = tempdir()?;
  create_dir_all(dir.path())?;
  let db_path = dir.path().join("hash_dump_test.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  for i in 0..200u32 {
    let key = format!("hash:key:{i}");
    let val = format!("hash:value:{i}");
    session.upsert(key.as_bytes(), val.as_bytes()).await?;
  }

  let dump = store.hash_distribution_dump();
  // 输出骨架对齐 C# DumpDistributionInternal 的汇总行
  assert!(dump.contains("Number of hash buckets: 64"));
  assert!(dump.contains("Size of each bucket: 64 bytes"));
  assert!(dump.contains("Hash-table size: 4096 bytes"));
  assert!(dump.contains("Histogram of #entries per bucket:"));
  assert!(dump.contains("Histogram of #unused slots per bucket in main hash index:"));
  // 200 个不同键全部在索引内：总条目 = 200
  assert!(dump.contains("Total distinct hash-table entry count: 200"));
  assert!(dump.contains("Total zeroed out slots:"));
  Ok(())
}

#[compio::test]
async fn hash_distribution_dump_zeroed_out_slots_parity() -> Void {
  let dir = tempdir()?;
  create_dir_all(dir.path())?;
  let db_path = dir.path().join("zeroed_slots_parity.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);

  // 1. 空表手算对拍：64 个主桶，0 个溢出桶，每个主桶 7 个数据槽，零槽总数 = 64 * 7 = 448
  let dump_empty = store.hash_distribution_dump();
  assert!(dump_empty.contains("Total zeroed out slots: 448"));

  // 2. 写入 10 个键，手算对拍零槽总数 = (主桶数 64 + 溢出桶数) * 7 - 10
  let session = store.new_session()?;
  for i in 0..10u32 {
    let key = format!("parity:key:{i}");
    let val = format!("parity:value:{i}");
    session.upsert(key.as_bytes(), val.as_bytes()).await?;
  }
  let dump_written = store.hash_distribution_dump();
  let ofb_count = store.store_snapshot().index_overflow_bucket_count;
  let expected_zeroed = (64 + ofb_count) * 7 - 10;
  assert!(dump_written.contains(&format!("Total zeroed out slots: {expected_zeroed}")));
  Ok(())
}

#[compio::test]
async fn revivification_dump_reports_four_counters() -> Void {
  let dir = tempdir()?;
  create_dir_all(dir.path())?;
  let db_path = dir.path().join("reviv_dump_test.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);

  // 未启用复活的裸 store：四计数全 0 仍是真实读数（非占位）
  let dump = store.revivification_dump();
  assert!(dump.contains("Puts: 0"));
  assert!(dump.contains("Takes: 0"));
  assert!(dump.contains("Take hits: 0"));
  assert!(dump.contains("Dropped or invalidated: 0"));
  Ok(())
}
