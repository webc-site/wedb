//! 端到端集成测试：对象族命令经分派表真执行——命令过完整会话主循环
//! （解析 → 门控 → GarnetApi 分派 → 存储落盘 → 应答回写）。
//!
//! 逐命令语义细节由直调套件权威覆盖（resp_hash.rs / resp_set.rs /
//! resp_list.rs / resp_sorted_set.rs / garnet_bitmap.rs / hyperloglog.rs /
//! geo_hash_tests.rs），本文件只为每族抽样 1-2 条保住「命令名 → 分派路由 →
//! 真执行」维度，并保留 SELECT / SWAPDB 分派语义。
use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::{resp_frame, test_store_config};

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer_with(options: RespServerSessionOptions) -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("obj.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  RespSessionConsumer::new(1, options, Arc::new(StoreGarnetApi::new(session)))
}

/// 默认单库形态消费者
fn consumer() -> RespSessionConsumer {
  consumer_with(RespServerSessionOptions::default())
}

/// 单命令往返
fn rt(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

#[test]
fn hash_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"HSET", b"h", b"f1", b"v1"])),
    b":1\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"HGET", b"h", b"f1"])),
    b"$2\r\nv1\r\n"
  );
}

/// Set 族分派冒烟（RespSetTests.cs:CanAddListItems 等；细节由 resp_set.rs 承接）
#[test]
fn set_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SADD", b"s", b"a", b"b"])),
    b":2\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SISMEMBER", b"s", b"a"])),
    b":1\r\n"
  );
}

/// ZSet 族分派冒烟（RespSortedSetTests.cs:CanAddSortedSet 等；细节由 resp_sorted_set.rs 承接）
#[test]
fn zset_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"ZADD", b"z", b"1", b"a"])),
    b":1\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"ZSCORE", b"z", b"a"])),
    b"$1\r\n1\r\n"
  );
}

/// List 族分派冒烟（RespListTests.cs:CanAddListItems 等；细节由 resp_list.rs 承接）
#[test]
fn list_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"RPUSH", b"l", b"a", b"b"])),
    b":2\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"LRANGE", b"l", b"0", b"-1"])),
    b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
  );
}

/// Bitmap 族分派冒烟（RespBitmapTests.cs:CanSetGetBit 等；细节由 garnet_bitmap.rs 承接）
#[test]
fn bitmap_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SETBIT", b"bk", b"7", b"1"])),
    b":0\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"GETBIT", b"bk", b"7"])),
    b":1\r\n"
  );
}

/// HyperLogLog 族分派抽样（RespHyperLogLogTests.cs）
#[test]
fn hll_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"PFADD", b"p", b"a", b"b"])),
    b":1\r\n"
  );
  assert_eq!(rt(&mut c, &resp_frame(&[b"PFCOUNT", b"p"])), b":2\r\n");
}

/// Geo 族分派冒烟（RespSortedSetGeoTests.cs:CanUseGeoAdd / CanUseGeoPos；细节由 resp_sorted_set.rs 承接）
#[test]
fn geo_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(
      &mut c,
      &resp_frame(&[b"GEOADD", b"g", b"13.361389", b"38.115556", b"Palermo"])
    ),
    b":1\r\n"
  );
  let out = rt(&mut c, &resp_frame(&[b"GEOPOS", b"g", b"Palermo"]));
  let payload = String::from_utf8_lossy(&out);
  assert!(payload.contains("13.36138"), "{payload}");
}

/// 对象扫描族分派抽样（HSCAN，RespHashTests.cs:CanScanHashItems）
#[test]
fn object_scan_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"HSET", b"h", b"f1", b"v1"])),
    b":1\r\n"
  );
  // HSCAN h 0 → [cursor, [f1, v1]]
  let out = rt(&mut c, &resp_frame(&[b"HSCAN", b"h", b"0"]));
  assert!(
    out.starts_with(b"*2\r\n$1\r\n0\r\n*2\r\n"),
    "HSCAN: {out:?}"
  );
  // 非法光标 → 分派层参数校验错误
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"HSCAN", b"h", b"-1"])),
    b"-ERR invalid cursor\r\n"
  );
}

/// SELECT 真切库（MultiDatabaseTests.cs:CanSelectDb 等）——库间键空间隔离
#[test]
fn select_switches_active_database() {
  let mut c = consumer();
  // db0 写入 k
  assert_eq!(rt(&mut c, &resp_frame(&[b"SET", b"k", b"v0"])), b"+OK\r\n");
  // SELECT 1 → +OK；GET k → nil（库隔离）
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"GET", b"k"])), b"$-1\r\n");
  // db1 写入 k；回 db0 验证互不可见
  assert_eq!(rt(&mut c, &resp_frame(&[b"SET", b"k", b"v1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"0"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"GET", b"k"])), b"$2\r\nv0\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"GET", b"k"])), b"$2\r\nv1\r\n");
  // SELECT 1（当前库重复选择）→ +OK
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  // 越界库号
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"16"])),
    b"-ERR DB index is out of range.\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"xx"])),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}

/// max_databases=1 时 SELECT 0 成功，SELECT 1 越界被拒
#[test]
fn select_with_max_databases_1() {
  let options = RespServerSessionOptions {
    max_databases: 1,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"0"])), b"+OK\r\n");
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"1"])),
    b"-ERR DB index is out of range.\r\n"
  );
}

/// SWAPDB：同库短路 +OK；异库同步域降级（不误答成功）
#[test]
fn swapdb_same_db_ok_cross_db_degrades() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SWAPDB", b"0", b"0"])),
    b"+OK\r\n"
  );
  // 跨库交换挂起慢路径（同步段不残留输出），网络泵 await 后产出兜底
  // 应答（SWAPDB 慢表接入前的 ASYNC_REQUIRED）
  let (consumed, out) = pump(&mut c, &resp_frame(&[b"SWAPDB", b"0", b"1"]));
  assert!(consumed.is_some());
  assert!(out.is_empty(), "同步段仅校验，不残留输出");
  let slow = c.take_slow_wait().expect("跨库 SWAPDB 应挂起慢路径");
  let out = Runtime::new().unwrap().block_on(slow.resolve());
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SWAPDB", b"a", b"1"])),
    b"-ERR invalid first DB index.\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SWAPDB", b"1", b"99"])),
    b"-ERR DB index is out of range.\r\n"
  );
}

/// 校验 max_databases 边界
#[test]
fn select_checks_max_databases_limit() {
  let options = RespServerSessionOptions {
    max_databases: 2,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"2"])),
    b"-ERR DB index is out of range.\r\n"
  );
}

/// 大库号切库不膨胀（doc/zh/db.md §1.3：会话库 ID 为 u64 标量，无 database_sessions
/// 数组）；线面字面量域按 C# TryGetInt 收口为 int32 档，故取 i32::MAX 为上界锚，
/// 超档字面量回「不是整数」而非切库成功
#[test]
fn select_supports_large_db_id() {
  let options = RespServerSessionOptions {
    max_databases: u64::MAX,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"2147483647"])),
    b"+OK\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SET", b"k", b"val_u64"])),
    b"+OK\r\n"
  );
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"GET", b"k"])),
    b"$7\r\nval_u64\r\n"
  );
  // 切换回 db 0
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"0"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &resp_frame(&[b"GET", b"k"])), b"$-1\r\n");
  // 超 int32 值域：C# 侧属「不是整数」档（ArrayCommands.cs:NetworkSELECT）
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"18446744073709551614"])),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}

/// INFO STORE 段头跟随当前库号且无 16 截断（max_databases 64 → SELECT 40 →
/// INFO STORE 出 Store_DB_40 段；存储域快照通道未接入时空行表只出段头）
#[test]
fn info_store_section_for_database_above_16() {
  let options = RespServerSessionOptions {
    max_databases: 64,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"40"])), b"+OK\r\n");
  let out = rt(&mut c, &resp_frame(&[b"INFO", b"STORE"]));
  let payload = String::from_utf8_lossy(&out);
  assert!(payload.contains("# Store_DB_40"), "{payload}");
  assert!(!payload.contains("Store_DB_0"), "{payload}");
}
