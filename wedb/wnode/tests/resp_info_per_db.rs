//! 端到端集成测试：SELECT 切库后 INFO 按库段非空（工单
//! wmetric-info-per-db-sections-nonzero-active-db-empty-body）
//!
//! 对标 C# GarnetInfoMetrics.GetRespInfo 的 `storeInfo[dbId]` 取行链路：
//! wedb 单物理存储多库前缀隔离（`SingleDatabaseManager::get_databases_snapshot`
//! 恒返回唯一 db 0 快照），会话 `SELECT n` 后 `INFO` 的 Store / Persistence /
//! HlogScan / StoreHashtable / StoreReviv 段回落唯一物理首行、出完整字段集，
//! 段头按活跃库号呈现。旧实现在活跃库 ≥ 1 时 `get(n)` 越界回 None，段体恒空。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::test_store_config;

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者
fn consumer() -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("perdb.db")).unwrap());
  let config = test_store_config();
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

fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// 同步单命令往返（SELECT / 无参 INFO）
fn sync_roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0));
  out
}

/// SELECT 1 后普通 INFO（同步默认段）：Store_DB_1 段落出完整存储字段集，
/// 不再空体（对标 C# Store_DB_n 全字段口径）。
#[test]
fn select_db1_info_store_section_populated() {
  let (_rt, mut c) = consumer();
  // 写入若干键，令物理存储统计非默认值
  for k in ["k1", "k2", "k3"] {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    let (consumed, out) = pump(&mut c, frame.as_bytes());
    assert_eq!(consumed, Some(0));
    assert!(out.starts_with(b"+OK"), "{}", from_utf8(&out).unwrap());
  }

  let sel = sync_roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");
  assert!(sel.starts_with(b"+OK"), "SELECT 1 应答: {sel:?}");

  let info = sync_roundtrip(&mut c, b"*1\r\n$4\r\nINFO\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("# Store_DB_1\r\n"), "{text}");
  // 段体含 CurrentVersion / Log.* 等完整字段行（旧实现此处段体恒空即红）
  let store_body = text
    .split("\r\n# ")
    .find(|blk| blk.starts_with("Store_DB_1\r\n"))
    .expect("Store_DB_1 段");
  assert!(
    store_body.contains("CurrentVersion:"),
    "Store_DB_1 段应含 CurrentVersion: {store_body}"
  );
  assert!(
    store_body.contains("Log.TailAddress:"),
    "Store_DB_1 段应含 Log.TailAddress: {store_body}"
  );
}

/// SELECT 1 后显式慢段 STOREHASHTABLE / STOREREVIV / HLOGSCAN：段头按 DB_1
/// 呈现且转储/分布条目非空（旧实现越界回 None、段体空即红）。
#[test]
fn select_db1_info_slow_scan_sections_populated() {
  let (rt, mut c) = consumer();
  for k in ["k1", "k2", "k3"] {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    let (consumed, out) = pump(&mut c, frame.as_bytes());
    assert_eq!(consumed, Some(0));
    assert!(out.starts_with(b"+OK"));
  }
  // k1 覆盖 + 删除，制造被取代/墓碑记录，令 hlog 分布非 Empty
  let (consumed, out) = pump(&mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$4\r\nwwww\r\n");
  assert_eq!(consumed, Some(0));
  assert!(out.starts_with(b"+OK"));
  let (consumed, out) = pump(&mut c, b"*2\r\n$3\r\nDEL\r\n$2\r\nk3\r\n");
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b":1\r\n");

  let sel = sync_roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");
  assert!(sel.starts_with(b"+OK"));

  let hash = slow_roundtrip(
    &rt,
    &mut c,
    b"*2\r\n$4\r\nINFO\r\n$14\r\nstorehashtable\r\n",
  );
  let hash = from_utf8(&hash).unwrap();
  assert!(
    hash.contains("# StoreHashTableDistribution_DB_1\r\n"),
    "{hash}"
  );
  assert!(
    hash.contains("Number of hash buckets:"),
    "STOREHASHTABLE 段体应非空: {hash}"
  );

  let reviv = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$10\r\nstorereviv\r\n");
  let reviv = from_utf8(&reviv).unwrap();
  assert!(
    reviv.contains("# StoreDeletedRecordRevivification_DB_1\r\n"),
    "{reviv}"
  );
  assert!(reviv.contains("Puts"), "STOREREVIV 段体应非空: {reviv}");

  let hlog = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nhlogscan\r\n");
  let hlog = from_utf8(&hlog).unwrap();
  assert!(hlog.contains("# MainStoreHLogScan_DB_1\r\n"), "{hlog}");
  assert!(
    hlog.contains("MainStore_HLog_0:") && !hlog.contains("MainStore_HLog_0:Empty"),
    "HLOGSCAN 段体应为真实分布: {hlog}"
  );
}
