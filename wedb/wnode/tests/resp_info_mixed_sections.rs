//! 端到端集成测试：INFO 混合段（含扫描族段）整请求降级与组合数据源
//!（工单 zcode-r126c-infosec1 案一 P2）
//!
//! 修复前分派门为 all 语义：`INFO server keyspace` 一类混合段名不降级，
//! 走同步渲染面，而同步侧 `SessionInfoSource::keyspace_stats` 为 (0,0)
//! 假桩 → "# Keyspace" 段头出、零 dbN 行 = 静默虚报零键。修复后 any 语义：
//! 凡解析段集含扫描族段整请求降级慢路径，扫描行（异步存储域）与非扫描
//! 面（调度点 InfoSurface 快照）合成组合源单次 GetRespInfo 全段集渲染，
//! 对标 C# InfoCommand.NetworkINFO → GarnetInfoMetrics.GetRespInfo 逐段
//! 实填（libs/server/Metrics/Info/GarnetInfoMetrics.cs:388-406）。
//!
//! 逐字节帧锁：RESP2 bulk string 帧头 `$len` + 段体字节与纯扫描段请求、
//! 纯同步段请求、DBSIZE 三方对拍一致；跳过段（EnableAOF=false 的
//! PERSISTENCE）段间分隔符字节形不变；虚库行最小事实不外溢 STORE 段。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::test_store_config;

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者（同 resp_info_per_db
/// 骨架：wkv 真存储，严禁假 mock）
fn consumer() -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("mixed.db")).unwrap());
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

/// 构造 `INFO <段名..>` inline 阵列帧
fn info_frame(sections: &[&str]) -> Vec<u8> {
  let mut frame = format!("*{}\r\n$4\r\nINFO\r\n", sections.len() + 1).into_bytes();
  for s in sections {
    frame.extend_from_slice(format!("${}\r\n{s}\r\n", s.len()).as_bytes());
  }
  frame
}

/// 任意单命令往返：同步闭环直返；降级命令 resolve 挂起的慢路径应答
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame);
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let consumed = c.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { resp.extend_from_slice(&slow.resolve().await) });
  }
  resp
}

/// 断言并剥出 RESP2 bulk string 载荷（`$<len>\r\n<payload>\r\n`），
/// 长度字节与载荷实长逐字节核验
fn bulk(reply: &[u8]) -> &[u8] {
  let text = from_utf8(reply).unwrap();
  let head = text
    .strip_prefix('$')
    .unwrap_or_else(|| panic!("非 RESP2 bulk 帧: {reply:?}"));
  let (len_s, rest) = head.split_once("\r\n").expect("bulk 帧头");
  let body = rest.strip_suffix("\r\n").expect("bulk 帧尾");
  let len: usize = len_s.parse().unwrap();
  assert_eq!(len, body.len(), "bulk 长度字节与载荷实长不符");
  body.as_bytes()
}

/// 逐字节帧级载荷断言
fn assert_payload(reply: &[u8], want: &[u8]) {
  assert_eq!(bulk(reply), want, "载荷字节不符");
}

#[test]
fn mixed_sections_keyspace_frames_locked() {
  let (rt, mut c) = consumer();
  // db 0：k1（带 TTL）+ k2；切库 db 1：k3 → 两库皆有键
  for k in ["k1", "k2"] {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    assert_eq!(&roundtrip(&rt, &mut c, frame.as_bytes()), b"+OK\r\n");
  }
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$6\r\nEXPIRE\r\n$2\r\nk1\r\n$3\r\n100\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    &roundtrip(&rt, &mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$1\r\nv\r\n"),
    b"+OK\r\n"
  );

  // 纯扫描段请求基准帧（整段载荷逐字节锁）
  const KEYS: &[u8] =
    b"# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0\r\ndb1:keys=1,expires=0,avg_ttl=0\r\n";
  assert_payload(&roundtrip(&rt, &mut c, &info_frame(&["keyspace"])), KEYS);

  // 案一修复主锁：混合段 `INFO server keyspace` 的 KEYSPACE 段与纯
  // `INFO keyspace` 逐字节一致（修复前此处段头出而零 dbN 行 = 假零键）
  let mixed_reply = roundtrip(&rt, &mut c, &info_frame(&["server", "keyspace"]));
  let mixed = from_utf8(bulk(&mixed_reply)).unwrap();
  assert!(mixed.starts_with("# Server\r\n"), "段序按请求序: {mixed}");
  let sep = "\r\n\r\n# Keyspace\r\n";
  let at = mixed.find(sep).expect("server 段后分隔符 + Keyspace 段头");
  let tail = &mixed[at + 2..]; // 段体尾 \r\n 之后即段间分隔符 + Keyspace 段
  let tail = tail.strip_prefix("\r\n").expect("段间分隔符");
  assert_eq!(
    tail.as_bytes(),
    KEYS,
    "混合段 Keyspace 须与纯 keyspace 逐字节一致（P2 假零键回归锁）"
  );
  // 非扫描面真值渲染：SERVER 段完整字段集在场（非最小 facts 外溢形态）
  // SERVER 段体含自身尾 \r\n（at 处首个 \r\n 属段体尾，at+2 起为段间分隔符）
  let server = &mixed[..at + 2];
  assert!(server.ends_with("\r\n"));
  assert!(
    server.contains("run_id:"),
    "SERVER 段应含 run_id 行: {server}"
  );
  assert!(
    server.contains("garnet_version:")
      && server.contains("redis_version:")
      && server.contains("uptime_in_seconds:"),
    "SERVER 段应含完整字段集: {server}"
  );

  // 段序反向锁：`INFO keyspace server` Keyspace 在前、段间分隔符不变
  let rev_reply = roundtrip(&rt, &mut c, &info_frame(&["keyspace", "server"]));
  let rev = from_utf8(bulk(&rev_reply)).unwrap();
  assert!(
    rev.starts_with("# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0\r\ndb1:keys=1,expires=0,avg_ttl=0\r\n\r\n# Server\r\n"),
    "段序与分隔符字节形不符: {rev}"
  );

  // DBSIZE 三方对拍：会话停 db 1 = 1 键；回 db 0 = 2 键（与 dbN 行同源）
  assert_eq!(
    &roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    &roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":2\r\n"
  );

  // 跳过段分隔符不变锁：EnableAOF=false 下 PERSISTENCE 段不出字节，
  // `INFO persistence keyspace` 帧载荷 = 段间分隔符 + Keyspace 段
  assert_payload(
    &roundtrip(&rt, &mut c, &info_frame(&["persistence", "keyspace"])),
    b"\r\n# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0\r\ndb1:keys=1,expires=0,avg_ttl=0\r\n",
  );
}

#[test]
fn mixed_all_and_stats_match_pure_sections() {
  let (rt, mut c) = consumer();
  for k in ["a", "b"] {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    assert_eq!(&roundtrip(&rt, &mut c, frame.as_bytes()), b"+OK\r\n");
  }
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$6\r\nEXPIRE\r\n$1\r\na\r\n$3\r\n100\r\n"
    ),
    b":1\r\n"
  );

  // `INFO all keyspace`：ALL 关键字展开段集 + KEYSPACE 追加，末段 Keyspace
  // 与纯 `INFO keyspace` 逐字节一致；STORE 段出物理事实（虚库行不外溢）
  let pure = bulk(&roundtrip(&rt, &mut c, &info_frame(&["keyspace"]))).to_vec();
  let all = bulk(&roundtrip(&rt, &mut c, &info_frame(&["all", "keyspace"]))).to_vec();
  let all = from_utf8(&all).unwrap();
  assert!(
    all.ends_with(from_utf8(&pure).unwrap()),
    "`all keyspace` 末段锁: {all}"
  );
  assert!(
    all.contains("# Server\r\n") && all.contains("# Memory\r\n"),
    "{all}"
  );

  // `INFO keyspace stats`：STATS 段与纯同步 `INFO stats` 逐字节一致
  let stats_pure = bulk(&roundtrip(&rt, &mut c, &info_frame(&["stats"]))).to_vec();
  let stats_pure = from_utf8(&stats_pure).unwrap().to_string();
  let mixed = bulk(&roundtrip(&rt, &mut c, &info_frame(&["keyspace", "stats"]))).to_vec();
  let mixed = from_utf8(&mixed).unwrap();
  assert!(
    mixed.ends_with(&format!("\r\n{stats_pure}")[..]),
    "混合段 STATS 须与纯 stats 逐字节一致: {mixed} != {stats_pure}"
  );
}

#[test]
fn mixed_virtual_store_row_falls_back_to_physical() {
  let (rt, mut c) = consumer();
  // db 0 两键、db 1 一键（db 1 为 wkv 虚库号，物理事实唯一归 db 0）
  for k in ["x", "y"] {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    assert_eq!(&roundtrip(&rt, &mut c, frame.as_bytes()), b"+OK\r\n");
  }
  assert_eq!(
    roundtrip(&rt, &mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    &roundtrip(&rt, &mut c, b"*3\r\n$3\r\nSET\r\n$1\r\nz\r\n$1\r\nv\r\n"),
    b"+OK\r\n"
  );

  // 纯同步 `INFO store`（活跃库 db 1 → store_row 回落物理首行）基准段
  let store_pure = bulk(&roundtrip(&rt, &mut c, &info_frame(&["store"]))).to_vec();
  let store_pure = from_utf8(&store_pure).unwrap().to_string();

  // 混合 `INFO store keyspace`：STORE 段字节与纯 store 段一致（扫描侧
  // 虚库行零值事实严禁外溢），Keyspace 段两库行齐
  let mixed = bulk(&roundtrip(&rt, &mut c, &info_frame(&["store", "keyspace"]))).to_vec();
  let mixed = from_utf8(&mixed).unwrap();
  assert!(
    mixed.starts_with(&format!("{store_pure}\r\n# Keyspace\r\n")),
    "混合段 STORE 段体须与纯同步逐字节一致且段间分隔符形不变（虚库零值外溢回归锁）: \
     期望前缀 {store_pure:?}.. 实得 {mixed:?}"
  );
  assert!(
    store_pure.contains("CurrentVersion:") && store_pure.contains("IndexBucketCount:"),
    "STORE 段应含完整物理字段: {store_pure}"
  );
  assert!(
    mixed.contains("db0:keys=2,expires=0,avg_ttl=0")
      && mixed.contains("db1:keys=1,expires=0,avg_ttl=0"),
    "Keyspace 两库行齐: {mixed}"
  );
}

#[test]
fn mixed_precedence_arms_stay_sync() {
  let (rt, mut c) = consumer();
  // HELP / RESET / 非法段在 C# 先于任何段填充短路且从不触达扫描数据：
  // 一律留在同步面，即便段名混有 keyspace
  let help = roundtrip(&rt, &mut c, &info_frame(&["keyspace", "help"]));
  assert!(help.starts_with(b"*"), "HELP 应出帮助数组帧: {help:?}");
  let bogus = roundtrip(&rt, &mut c, &info_frame(&["bogus", "keyspace"]));
  assert!(
    bogus.starts_with(b"-ERR Invalid section bogus"),
    "非法段应同步回错: {bogus:?}"
  );
  let reset = roundtrip(&rt, &mut c, &info_frame(&["keyspace", "reset"]));
  assert_eq!(&reset, b"+OK\r\n", "RESET 应同步回 OK: {reset:?}");
  // 三条均不得挂起慢路径（take_slow_wait 已由 roundtrip 消费；此处再证
  // 纯非法段名不降级）
  assert_eq!(
    &roundtrip(&rt, &mut c, &info_frame(&["keyspace", "bogus2"])),
    b"-ERR Invalid section bogus2. Try INFO HELP\r\n"
  );
}
