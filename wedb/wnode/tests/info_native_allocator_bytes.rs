//! INFO memory 段 native_allocator_bytes 嵌入链验收（工单
//! wmetric-native-allocator-bytes-unwired 嵌入链臂）
//!
//! 真实起库建主索引：`WedbStore::open` → `HashIndex::new` 桶数组分配经
//! `DirectVirtualMemory::allocate` 全程喂账 `windex::ram::NativeMemoryTracker`，
//! 会话 INFO memory 段经 `SessionInfoSource` 单点透传披露非零记账。下界断言
//! ＞0 即可——tracker 系进程级全局总账且记页对齐 reserve 后跨度，恒 ≥ 主索引
//! 桶数组净字节，禁与 store_index_size 等值断言。夹具形态对标本仓
//! resp_info_per_db.rs。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::test_store_config;

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者
fn consumer() -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("allocbytes.db")).unwrap());
  let config = test_store_config();
  // 建库即建主索引（wkv store open → HashIndex::new），桶数组分配全程喂账
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
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

/// 同步单命令往返（INFO memory 为同步段，不走慢路径）
fn sync_roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0));
  out
}

/// 起库建主索引后 INFO memory 段 native_allocator_bytes 严格大于 0
///（下界断言；禁与 store_index_size 等值——恢复期换表瞬态另有新旧交叠）
#[test]
fn info_memory_discloses_nonzero_native_allocator_after_store_open() {
  let (_rt, mut c) = consumer();
  let info = sync_roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$6\r\nmemory\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("# Memory\r\n"), "应有 Memory 段头: {text}");

  let native: i64 = text
    .split("\r\n")
    .find_map(|l| l.strip_prefix("native_allocator_bytes:"))
    .unwrap_or_else(|| panic!("缺 native_allocator_bytes 行: {text}"))
    .parse()
    .unwrap_or_else(|_| panic!("native_allocator_bytes 非整数: {text}"));
  assert!(native > 0, "建库建主索引后原生记账必须在场: {text}");
}
