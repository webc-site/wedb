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
use wedb_test::{resp_frame, test_store_config};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer_with(options: RespServerSessionOptions) -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("obj.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
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

/// Hash 族分派冒烟（RespHashTests.cs:CanAddAndListHashItems 等；细节由 resp_hash.rs 承接）
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
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
  let options = RespServerSessionOptions {
    allow_multi_db: true,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
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
  // SELECT 0（当前库重复选择）→ +OK
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

/// 单库形态 SELECT 非 0 库被拒（C# allowMultiDb=false 语义）
#[test]
fn select_rejects_nonzero_in_single_db_mode() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"1"])),
    b"-ERR unable to select database.\r\n"
  );
  // SELECT 0（当前库）恒成功
  assert_eq!(rt(&mut c, &resp_frame(&[b"SELECT", b"0"])), b"+OK\r\n");
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

/// 集群形态 SELECT 非 0 库被拒（C# EnableCluster 门槛）
#[test]
fn select_rejects_nonzero_in_cluster_mode() {
  // 集群切面挂接形态由 wedb crate 集成测试覆盖；此处校验单机会话下
  // allow_multi_db=true 但 max_databases=2（集群装配形态）时 SELECT 2 越界
  let options = RespServerSessionOptions {
    allow_multi_db: true,
    max_databases: 2,
    ..RespServerSessionOptions::default()
  };
  let mut c = consumer_with(options);
  assert_eq!(
    rt(&mut c, &resp_frame(&[b"SELECT", b"2"])),
    b"-ERR DB index is out of range.\r\n"
  );
}
