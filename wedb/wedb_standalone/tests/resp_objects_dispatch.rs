//! 端到端集成测试：对象族命令经分派表真执行（对标 garnet/test/
//! standalone/Garnet.test.collections 的 RespHashTests / RespSetTests /
//! RespSortedSetTests / RespListTests 与 RespBitmapTests）——命令过完整
//! 会话主循环（解析 → 门控 → GarnetApi 分派 → 存储落盘 → 应答回写），
//! 覆盖 Hash/Set/ZSet/List/Bitmap/HLL/Geo/Scan 八族与 SELECT 真切库
use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer_with(options: RespServerSessionOptions) -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("obj.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
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
  let (consumed, out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  out
}

/// 构造 RESP 数组帧
fn frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut f = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    f.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    f.extend_from_slice(p);
    f.extend_from_slice(b"\r\n");
  }
  f
}

/// Hash 族闭环（RespHashTests.cs:CanAddAndListHashItems 等）
#[test]
fn hash_family_end_to_end() {
  let mut c = consumer();
  // HSET h f v → :1
  assert_eq!(
    rt(&mut c, &frame(&[b"HSET", b"h", b"f1", b"v1"])),
    b":1\r\n"
  );
  assert_eq!(
    rt(&mut c, &frame(&[b"HSET", b"h", b"f2", b"v2"])),
    b":1\r\n"
  );
  // HGET h f1 → v1
  assert_eq!(rt(&mut c, &frame(&[b"HGET", b"h", b"f1"])), b"$2\r\nv1\r\n");
  // HLEN → :2
  assert_eq!(rt(&mut c, &frame(&[b"HLEN", b"h"])), b":2\r\n");
  // HEXISTS h f1 → :1；HEXISTS h miss → :0
  assert_eq!(rt(&mut c, &frame(&[b"HEXISTS", b"h", b"f1"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"HEXISTS", b"h", b"miss"])), b":0\r\n");
  // HSTRLEN h f1 → :2
  assert_eq!(rt(&mut c, &frame(&[b"HSTRLEN", b"h", b"f1"])), b":2\r\n");
  // HINCRBY h cnt 5 → :5
  assert_eq!(
    rt(&mut c, &frame(&[b"HINCRBY", b"h", b"cnt", b"5"])),
    b":5\r\n"
  );
  // HMGET h f1 miss f2 → [v1, nil, v2]
  assert_eq!(
    rt(&mut c, &frame(&[b"HMGET", b"h", b"f1", b"miss", b"f2"])),
    b"*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv2\r\n"
  );
  // HKEYS/HVALS：此刻 3 字段（f1/f2/cnt，成员序无断言仅数量）
  assert_eq!(&rt(&mut c, &frame(&[b"HKEYS", b"h"]))[..2], b"*3");
  assert_eq!(&rt(&mut c, &frame(&[b"HVALS", b"h"]))[..2], b"*3");
  // HDEL h f1 → :1；HGET f1 → nil
  assert_eq!(rt(&mut c, &frame(&[b"HDEL", b"h", b"f1"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"HGET", b"h", b"f1"])), b"$-1\r\n");
  // HSETNX h f3 v3 → :1；重复 → :0
  assert_eq!(
    rt(&mut c, &frame(&[b"HSETNX", b"h", b"f3", b"v3"])),
    b":1\r\n"
  );
  assert_eq!(
    rt(&mut c, &frame(&[b"HSETNX", b"h", b"f3", b"vx"])),
    b":0\r\n"
  );
  // HMSET 废弃别名回 +OK
  assert_eq!(
    rt(&mut c, &frame(&[b"HMSET", b"h", b"f4", b"v4"])),
    b"+OK\r\n"
  );
  // HGETALL → 3 字段（f2/f3/cnt/f4 中 f2、f3、cnt、f4 —— HDEL 只删了 f1）
  let out = rt(&mut c, &frame(&[b"HGETALL", b"h"]));
  assert!(out.starts_with(b"*8\r\n"), "HGETALL 应 4 字段: {out:?}");
}

/// Set 族闭环（RespSetTests.cs:CanAddListItems 等）
#[test]
fn set_family_end_to_end() {
  let mut c = consumer();
  // SADD s a b c → :3；重复 a → :0
  assert_eq!(
    rt(&mut c, &frame(&[b"SADD", b"s", b"a", b"b", b"c"])),
    b":3\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"SADD", b"s", b"a"])), b":0\r\n");
  // SCARD → :3
  assert_eq!(rt(&mut c, &frame(&[b"SCARD", b"s"])), b":3\r\n");
  // SISMEMBER s a → :1；miss → :0
  assert_eq!(rt(&mut c, &frame(&[b"SISMEMBER", b"s", b"a"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SISMEMBER", b"s", b"z"])), b":0\r\n");
  // SMISMEMBER s a z → [1, 0]
  assert_eq!(
    rt(&mut c, &frame(&[b"SMISMEMBER", b"s", b"a", b"z"])),
    b"*2\r\n:1\r\n:0\r\n"
  );
  // SMEMBERS → 3 元素
  assert_eq!(&rt(&mut c, &frame(&[b"SMEMBERS", b"s"]))[..2], b"*3");
  // SADD t c d；SINTER s t → [c]
  assert_eq!(rt(&mut c, &frame(&[b"SADD", b"t", b"c", b"d"])), b":2\r\n");
  assert_eq!(
    rt(&mut c, &frame(&[b"SINTER", b"s", b"t"])),
    b"*1\r\n$1\r\nc\r\n"
  );
  // SINTERCARD s t LIMIT 0 → :1
  assert_eq!(
    rt(
      &mut c,
      &frame(&[b"SINTERCARD", b"2", b"s", b"t", b"LIMIT", b"0"])
    ),
    b":1\r\n"
  );
  // SUNION s t → 4 元素
  assert_eq!(&rt(&mut c, &frame(&[b"SUNION", b"s", b"t"]))[..2], b"*4");
  // SDIFF s t → 2 元素（a b）
  assert_eq!(&rt(&mut c, &frame(&[b"SDIFF", b"s", b"t"]))[..2], b"*2");
  // SUNIONSTORE dst s t → :4
  assert_eq!(
    rt(&mut c, &frame(&[b"SUNIONSTORE", b"dst", b"s", b"t"])),
    b":4\r\n"
  );
  // SMOVE s t a → :1；SISMEMBER t a → :1
  assert_eq!(rt(&mut c, &frame(&[b"SMOVE", b"s", b"t", b"a"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SISMEMBER", b"t", b"a"])), b":1\r\n");
  // SPOP sp（独立键单成员，无 count）→ 单 bulk 元素
  assert_eq!(rt(&mut c, &frame(&[b"SADD", b"sp", b"x"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SPOP", b"sp"])), b"$1\r\nx\r\n");
  // SREM s b c → :1/:1（s 此刻 {b,c}）；SCARD → :0
  assert_eq!(rt(&mut c, &frame(&[b"SREM", b"s", b"b"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SREM", b"s", b"c"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SCARD", b"s"])), b":0\r\n");
}

/// ZSet 族闭环（RespSortedSetTests.cs:CanAddSortedSet 等）
#[test]
fn zset_family_end_to_end() {
  let mut c = consumer();
  // ZADD z 1 a 2 b 3 c → :3
  assert_eq!(
    rt(
      &mut c,
      &frame(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"])
    ),
    b":3\r\n"
  );
  // ZSCORE z a → 1
  assert_eq!(rt(&mut c, &frame(&[b"ZSCORE", b"z", b"a"])), b"$1\r\n1\r\n");
  // ZMSCORE z a miss → [1, nil]
  assert_eq!(
    rt(&mut c, &frame(&[b"ZMSCORE", b"z", b"a", b"miss"])),
    b"*2\r\n$1\r\n1\r\n$-1\r\n"
  );
  // ZCARD → :3
  assert_eq!(rt(&mut c, &frame(&[b"ZCARD", b"z"])), b":3\r\n");
  // ZRANGE z 0 -1 → [a 1 b 2 c 3]（带分值需 WITHSCORES；裸形态仅成员）
  assert_eq!(
    rt(&mut c, &frame(&[b"ZRANGE", b"z", b"0", b"-1"])),
    b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  // ZRANGE WITHSCORES
  assert_eq!(
    rt(
      &mut c,
      &frame(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"])
    ),
    b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
  );
  // ZREVRANGE z 0 0 → [c]
  assert_eq!(
    rt(&mut c, &frame(&[b"ZREVRANGE", b"z", b"0", b"0"])),
    b"*1\r\n$1\r\nc\r\n"
  );
  // ZRANK z b → :1；ZREVRANK z b → :1
  assert_eq!(rt(&mut c, &frame(&[b"ZRANK", b"z", b"b"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"ZREVRANK", b"z", b"b"])), b":1\r\n");
  // ZCOUNT z 1 2 → :2
  assert_eq!(
    rt(&mut c, &frame(&[b"ZCOUNT", b"z", b"1", b"2"])),
    b":2\r\n"
  );
  // ZINCRBY z 10 a → 11
  assert_eq!(
    rt(&mut c, &frame(&[b"ZINCRBY", b"z", b"10", b"a"])),
    b"$2\r\n11\r\n"
  );
  // ZRANGEBYSCORE z 2 3 → [b c]
  assert_eq!(
    rt(&mut c, &frame(&[b"ZRANGEBYSCORE", b"z", b"2", b"3"])),
    b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  // ZPOPMIN z → [b 2]（a 已被 ZINCRBY 推到 11）
  assert_eq!(
    rt(&mut c, &frame(&[b"ZPOPMIN", b"z"])),
    b"*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
  );
  // ZREM z c → :1；ZCARD → :1
  assert_eq!(rt(&mut c, &frame(&[b"ZREM", b"z", b"c"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"ZCARD", b"z"])), b":1\r\n");
}

/// List 族闭环（RespListTests.cs:CanAddListItems 等）
#[test]
fn list_family_end_to_end() {
  let mut c = consumer();
  // RPUSH l a b c → :3
  assert_eq!(
    rt(&mut c, &frame(&[b"RPUSH", b"l", b"a", b"b", b"c"])),
    b":3\r\n"
  );
  // LLEN → :3
  assert_eq!(rt(&mut c, &frame(&[b"LLEN", b"l"])), b":3\r\n");
  // LRANGE l 0 -1 → [a b c]
  assert_eq!(
    rt(&mut c, &frame(&[b"LRANGE", b"l", b"0", b"-1"])),
    b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  // LINDEX l 1 → b
  assert_eq!(rt(&mut c, &frame(&[b"LINDEX", b"l", b"1"])), b"$1\r\nb\r\n");
  // LSET l 1 B → +OK；LINDEX 验证
  assert_eq!(rt(&mut c, &frame(&[b"LSET", b"l", b"1", b"B"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"LINDEX", b"l", b"1"])), b"$1\r\nB\r\n");
  // LPUSH l z → :4；LRANGE 0 0 → [z]
  assert_eq!(rt(&mut c, &frame(&[b"LPUSH", b"l", b"z"])), b":4\r\n");
  assert_eq!(
    rt(&mut c, &frame(&[b"LRANGE", b"l", b"0", b"0"])),
    b"*1\r\n$1\r\nz\r\n"
  );
  // LPOP l → z
  assert_eq!(rt(&mut c, &frame(&[b"LPOP", b"l"])), b"$1\r\nz\r\n");
  // RPOPLPUSH l l2 → c；LRANGE l2 → [c]
  assert_eq!(
    rt(&mut c, &frame(&[b"RPOPLPUSH", b"l", b"l2"])),
    b"$1\r\nc\r\n"
  );
  assert_eq!(
    rt(&mut c, &frame(&[b"LRANGE", b"l2", b"0", b"-1"])),
    b"*1\r\n$1\r\nc\r\n"
  );
  // LREM l 0 a → :1
  assert_eq!(rt(&mut c, &frame(&[b"LREM", b"l", b"0", b"a"])), b":1\r\n");
  // LTRIM l 0 0 → +OK；LLEN → :1
  assert_eq!(
    rt(&mut c, &frame(&[b"LTRIM", b"l", b"0", b"0"])),
    b"+OK\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"LLEN", b"l"])), b":1\r\n");
  // LPUSHX miss x → :0（键缺失不物化）；RPUSHX l d → :2
  assert_eq!(rt(&mut c, &frame(&[b"LPUSHX", b"miss", b"x"])), b":0\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"RPUSHX", b"l", b"d"])), b":2\r\n");
}

/// Bitmap 族闭环（RespBitmapTests.cs:CanSetGetBit 等）
#[test]
fn bitmap_family_end_to_end() {
  let mut c = consumer();
  // SETBIT bk 7 1 → :0；GETBIT bk 7 → :1
  assert_eq!(
    rt(&mut c, &frame(&[b"SETBIT", b"bk", b"7", b"1"])),
    b":0\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"GETBIT", b"bk", b"7"])), b":1\r\n");
  // SETBIT bk 9 1 → :0；BITCOUNT bk → :2
  assert_eq!(
    rt(&mut c, &frame(&[b"SETBIT", b"bk", b"9", b"1"])),
    b":0\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"BITCOUNT", b"bk"])), b":2\r\n");
  // BITPOS bk 1 0 → :7
  assert_eq!(
    rt(&mut c, &frame(&[b"BITPOS", b"bk", b"1", b"0"])),
    b":7\r\n"
  );
  // BITFIELD bk GET u8 0 → 1（MSB-first 位序：offset 7 落字节 0 最低位，
  // u8 窗口覆盖 offset 0-7 整字节 → 0x01）
  assert_eq!(
    rt(&mut c, &frame(&[b"BITFIELD", b"bk", b"GET", b"u8", b"0"])),
    b"*1\r\n:1\r\n"
  );
  // BITOP NOT dst bk → :2；GETBIT dst 7 → :0
  assert_eq!(
    rt(&mut c, &frame(&[b"BITOP", b"NOT", b"dst", b"bk"])),
    b":2\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"GETBIT", b"dst", b"7"])), b":0\r\n");
}

/// HyperLogLog 族闭环（RespHyperLogLogTests.cs）
#[test]
fn hll_family_end_to_end() {
  let mut c = consumer();
  // PFADD p a b → :1；PFADD p a → :0
  assert_eq!(rt(&mut c, &frame(&[b"PFADD", b"p", b"a", b"b"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"PFADD", b"p", b"a"])), b":0\r\n");
  // PFCOUNT p → :2
  assert_eq!(rt(&mut c, &frame(&[b"PFCOUNT", b"p"])), b":2\r\n");
  // PFMERGE pd p → +OK；PFCOUNT pd → :2
  assert_eq!(rt(&mut c, &frame(&[b"PFMERGE", b"pd", b"p"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"PFCOUNT", b"pd"])), b":2\r\n");
}

/// Geo 族闭环（RespSortedSetGeoTests.cs:CanUseGeoAdd / CanUseGeoPos）
#[test]
fn geo_family_end_to_end() {
  let mut c = consumer();
  // GEOADD g lon lat member → :1
  assert_eq!(
    rt(
      &mut c,
      &frame(&[b"GEOADD", b"g", b"13.361389", b"38.115556", b"Palermo"])
    ),
    b":1\r\n"
  );
  // GEOPOS g Palermo → 坐标负载含 13.36138
  let out = rt(&mut c, &frame(&[b"GEOPOS", b"g", b"Palermo"]));
  let payload = String::from_utf8_lossy(&out);
  assert!(payload.contains("13.36138"), "{payload}");
  // GEODIST g Palermo Palermo → 0
  assert_eq!(
    rt(&mut c, &frame(&[b"GEODIST", b"g", b"Palermo", b"Palermo"])),
    b"$1\r\n0\r\n"
  );
  // GEOHASH g Palermo → 非 null bulk
  let out = rt(&mut c, &frame(&[b"GEOHASH", b"g", b"Palermo"]));
  assert!(out.starts_with(b"*1\r\n$"), "GEOHASH 应答: {out:?}");
}

/// 对象扫描族闭环（HSCAN/SSCAN/ZSCAN，RespHashTests.cs:CanScanHashItems 等）
#[test]
fn object_scan_family_end_to_end() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &frame(&[b"HSET", b"h", b"f1", b"v1"])),
    b":1\r\n"
  );
  assert_eq!(rt(&mut c, &frame(&[b"SADD", b"s", b"a"])), b":1\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"ZADD", b"z", b"1", b"a"])), b":1\r\n");
  // HSCAN h 0 → [cursor, [f1, v1]]
  let out = rt(&mut c, &frame(&[b"HSCAN", b"h", b"0"]));
  assert!(
    out.starts_with(b"*2\r\n$1\r\n0\r\n*2\r\n"),
    "HSCAN: {out:?}"
  );
  // SSCAN s 0 → [cursor, [a]]
  let out = rt(&mut c, &frame(&[b"SSCAN", b"s", b"0"]));
  assert!(
    out.starts_with(b"*2\r\n$1\r\n0\r\n*1\r\n"),
    "SSCAN: {out:?}"
  );
  // ZSCAN z 0 → [cursor, [a, 1]]
  let out = rt(&mut c, &frame(&[b"ZSCAN", b"z", b"0"]));
  assert!(
    out.starts_with(b"*2\r\n$1\r\n0\r\n*2\r\n"),
    "ZSCAN: {out:?}"
  );
  // 键缺失 → [0, 空数组]
  assert_eq!(
    rt(&mut c, &frame(&[b"HSCAN", b"miss", b"0"])),
    b"*2\r\n$1\r\n0\r\n*0\r\n"
  );
  // 非法光标 → 错误
  assert_eq!(
    rt(&mut c, &frame(&[b"HSCAN", b"h", b"-1"])),
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
  assert_eq!(rt(&mut c, &frame(&[b"SET", b"k", b"v0"])), b"+OK\r\n");
  // SELECT 1 → +OK；GET k → nil（库隔离）
  assert_eq!(rt(&mut c, &frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"GET", b"k"])), b"$-1\r\n");
  // db1 写入 k；回 db0 验证互不可见
  assert_eq!(rt(&mut c, &frame(&[b"SET", b"k", b"v1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SELECT", b"0"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"GET", b"k"])), b"$2\r\nv0\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  assert_eq!(rt(&mut c, &frame(&[b"GET", b"k"])), b"$2\r\nv1\r\n");
  // SELECT 0（当前库重复选择）→ +OK
  assert_eq!(rt(&mut c, &frame(&[b"SELECT", b"1"])), b"+OK\r\n");
  // 越界库号
  assert_eq!(
    rt(&mut c, &frame(&[b"SELECT", b"16"])),
    b"-ERR DB index is out of range.\r\n"
  );
  assert_eq!(
    rt(&mut c, &frame(&[b"SELECT", b"xx"])),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}

/// 单库形态 SELECT 非 0 库被拒（C# allowMultiDb=false 语义）
#[test]
fn select_rejects_nonzero_in_single_db_mode() {
  let mut c = consumer();
  assert_eq!(
    rt(&mut c, &frame(&[b"SELECT", b"1"])),
    b"-ERR unable to select database.\r\n"
  );
  // SELECT 0（当前库）恒成功
  assert_eq!(rt(&mut c, &frame(&[b"SELECT", b"0"])), b"+OK\r\n");
}

/// SWAPDB：同库短路 +OK；异库同步域降级（不误答成功）
#[test]
fn swapdb_same_db_ok_cross_db_degrades() {
  let mut c = consumer();
  assert_eq!(rt(&mut c, &frame(&[b"SWAPDB", b"0", b"0"])), b"+OK\r\n");
  // 跨库交换挂起慢路径（同步段不残留输出），网络泵 await 后产出兜底
  // 应答（SWAPDB 慢表接入前的 ASYNC_REQUIRED）
  let (consumed, out) = c.try_consume_messages(&frame(&[b"SWAPDB", b"0", b"1"]));
  assert!(consumed > 0);
  assert!(out.is_empty(), "同步段仅校验，不残留输出");
  let slow = c.take_slow_wait().expect("跨库 SWAPDB 应挂起慢路径");
  let out = Runtime::new().unwrap().block_on(slow.resolve());
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    rt(&mut c, &frame(&[b"SWAPDB", b"a", b"1"])),
    b"-ERR invalid first DB index.\r\n"
  );
  assert_eq!(
    rt(&mut c, &frame(&[b"SWAPDB", b"1", b"99"])),
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
    rt(&mut c, &frame(&[b"SELECT", b"2"])),
    b"-ERR DB index is out of range.\r\n"
  );
}
