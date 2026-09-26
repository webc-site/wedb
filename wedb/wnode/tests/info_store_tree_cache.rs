//! INFO STORE 总闸披露口径锁（task/ing/zcode-r49-memlimit.md）
//!
//! 树页缓存定额经 wconf `tree-cache-budget` 旋钮注入后，INFO STORE 段须披露
//! TreeCache.ReservedBytes / TreeCache.BudgetBytes 两行：reserved 与在线活跃树
//! 环容量和一致（wbftree cache_reserved 验收口径）、budget 为装配定额。
//! 观测面对标 C# CacheSizeTracker 的 TargetSize 高水位语义（C# RangeIndexManager
//! 无预算字段，本闸为 rust 自研分层架构组件）。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::test_store_config;

/// 测试树调参（页环 64KiB，预算 128KiB = 恰容两棵）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 64 * 1024,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者（自定义树缓存定额）
fn consumer_with_budget(budget: usize) -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("treecache.db")).unwrap());
  let config = test_store_config().with_tree_cache_budget(budget);
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  // 预算内登记一棵在线树（页环 64KiB 即进入记账），登记需进执行域驱动
  let session_store = Arc::clone(&store);
  rt.block_on(async {
    let session = session_store.new_session().unwrap();
    session
      .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
      .await
      .unwrap();
  });
  let session = store.new_session().unwrap();
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
    0,
    Arc::clone(&store),
    Arc::clone(&device),
    dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir, db));
  let api = StoreGarnetApi::new(session).with_database_manager(mgr);
  (
    rt,
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api)),
  )
}

fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = consumer.try_consume_messages_into(&mut resp);
  resp
}

/// INFO STORE 披露锁：定额披露 == 装配注入值，reserved == 在线树环容量和
#[test]
fn info_store_discloses_tree_cache_lines() {
  let (_rt, mut c) = consumer_with_budget(128 * 1024);
  let info = pump(&mut c, b"*2\r\n$4\r\nINFO\r\n$5\r\nstore\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(
    text.contains("# Store_DB_0\r\n"),
    "INFO STORE 段头必须在场: {text}"
  );
  assert!(
    text.contains("TreeCache.BudgetBytes:131072\r\n"),
    "总闸定额行必须披露装配值: {text}"
  );
  assert!(
    text.contains("TreeCache.ReservedBytes:65536\r\n"),
    "总闸水位行必须与在线树环容量和一致: {text}"
  );
}
