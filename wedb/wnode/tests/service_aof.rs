use std::sync::Arc;

use tempfile::tempdir;
use wnode::{resp::resp_session_consumer::RespSessionConsumer, service::StorageSessionProvider};
use wtest_base::test_store_config;

#[test]
fn open_with_config_and_aof_custom_wal_dir() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  let custom_wal = dir.path().join("custom_wal_dir");
  // 小预算测试配置注入（生产缺省走 StoreConfig::auto，大机上规划出 GB 级索引）
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    Some(&custom_wal),
    None,
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap();

  assert!(provider.aof().is_some());
  let wal_log_path = custom_wal.join("wal.log");
  assert!(
    wal_log_path.exists(),
    "自定义 wal_dir 下必须生成 wal.log 文件"
  );
}

#[test]
fn open_with_config_and_aof_default_wal_dir() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  // 小预算测试配置注入（生产缺省走 StoreConfig::auto，大机上规划出 GB 级索引）
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    None,
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap();

  assert!(provider.aof().is_some());
  let default_wal_log_path = dir.path().join("data").join("wal").join("wal.log");
  assert!(
    default_wal_log_path.exists(),
    "缺省情况下应在 <data>/wal/ 下生成 wal.log 文件"
  );
}
