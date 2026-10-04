use std::sync::Arc;

use aok::{Result, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use whlog::HybridLogConfig;
use wkv::{StoreConfig, WedbStore};

#[test]
fn hlog_config_num_pages_validation() -> Void {
  // num_pages = 1 should fail
  let res = HybridLogConfig::new(65536, 1, 0.5);
  assert!(res.is_err());
  let err_str = res.unwrap_err().to_string();
  assert!(err_str.contains("num_pages 最小为 2"));

  // num_pages = 1 should fail in StoreConfig
  let res = StoreConfig::new(1024, 65536, 1, 0.5);
  assert!(res.is_err());
  let err_str = res.unwrap_err().to_string();
  assert!(err_str.contains("num_pages 最小为 2"));

  Ok(())
}

#[compio::test]
async fn writing_regression_numpages_2() -> Result<()> {
  // num_pages = 2 should work
  let config = StoreConfig::new(1024, 65536, 2, 0.5)?;
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("db"))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  // writing enough data to cross a page
  let key = b"key";
  let val = vec![0u8; 40000]; // 40KB
  session.upsert(key, &val).await?;

  // write another to cross the 64KB page boundary
  let key2 = b"key2";
  let val2 = vec![0u8; 40000]; // 40KB
  session.upsert(key2, &val2).await?;

  // 读回断言：跨 64KB 页界写入后两值完整性（num_pages=2 翻页不丢不改）
  assert_eq!(session.read(key).await?, Some(val));
  assert_eq!(session.read(key2).await?, Some(val2));

  Ok(())
}
