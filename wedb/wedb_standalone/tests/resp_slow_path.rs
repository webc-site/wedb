//! 端到端集成测试：慢路径执行器（SCAN / KEYS / DBSIZE）
//!
//! 会话同步段仅校验参数，实际扫描经 [`wnode::resp::slow_path::SlowWait`]
//! 挂起、网络泵侧 await 驱动闭环（此处以 compio Runtime 的 block_on 承担
//! 网络泵角色），对标 garnet/test/standalone/Garnet.test 的 SCAN/KEYS/
//! DBSIZE 用例族（RespTests / RespTestsBytes）
use std::{str::from_utf8, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("slow.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session)),
  )
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返：同步段消费（挂起不产输出）→ 网络泵 await 慢路径 →
/// 应答按流水线顺序写回（此处 block_on 承担网络泵角色）
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

/// SET 若干键
fn set_keys(c: &mut RespSessionConsumer, keys: &[&str]) {
  for k in keys {
    assert_eq!(
      roundtrip(
        c,
        format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len()).as_bytes()
      ),
      b"+OK\r\n"
    );
  }
}

/// DBSIZE / KEYS 慢路径闭环
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
fn dbsize_and_keys_via_slow_path() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["user:1", "user:2", "order:1"]);

  // DBSIZE → :3（全库计数走慢路径）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":3\r\n"
  );

  // KEYS user:* → 2 键（glob 匹配走慢路径；快照按键序）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nKEYS\r\n$6\r\nuser:*\r\n"),
    b"*2\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n"
  );

  // KEYS * → 全量 3 键；空匹配 → *0
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n"),
    b"*3\r\n$7\r\norder:1\r\n$6\r\nuser:1\r\n$6\r\nuser:2\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nKEYS\r\n$4\r\nmiss\r\n"),
    b"*0\r\n"
  );
}

/// SCAN 游标分页闭环：COUNT 截断 → 非零游标续扫 → 收尽归零
#[test]
fn scan_cursor_pagination() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["k1", "k2", "k3", "k4"]);

  // SCAN 0 COUNT 2 → 首页 2 键 + 非零游标
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n",
  );
  let text = String::from_utf8_lossy(&out).to_string();
  let (cursor, keys) = parse_scan_reply(&text);
  assert_eq!(keys.len(), 2, "首页应收 2 键: {text}");
  assert!(cursor > 0, "COUNT 截断应报告非零游标: {text}");

  // SCAN <cursor> → 续扫收尽 → 游标归 0，全轮恰好覆盖 4 键各一次
  let cursor_str = cursor.to_string();
  let frame = format!(
    "*2\r\n$4\r\nSCAN\r\n${}\r\n{cursor_str}\r\n",
    cursor_str.len()
  );
  let out = slow_roundtrip(&rt, &mut c, frame.as_bytes());
  let text = String::from_utf8_lossy(&out).to_string();
  let (cursor2, keys2) = parse_scan_reply(&text);
  assert_eq!(cursor2, 0, "收尽后游标归零: {text}");
  assert_eq!(keys2.len(), 2, "余量 2 键: {text}");

  let mut all = keys;
  all.extend(keys2);
  all.sort();
  assert_eq!(all, vec!["k1", "k2", "k3", "k4"], "全轮覆盖全部键各一次");

  // 终态再扫：SCAN 0 → 空键 + 游标 0
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n",
  );
  let text = String::from_utf8_lossy(&out).to_string();
  let (cursor3, keys3) = parse_scan_reply(&text);
  assert_eq!((cursor3, keys3.len()), (0, 4), "全量单页收尽: {text}");
}

/// SCAN MATCH / TYPE 过滤与参数校验
#[test]
fn scan_match_type_and_validation() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["str:1", "str:2"]);
  // HASH 键经 HSET 建立（物理形态为 Meta 记录）
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$3\r\nh:a\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );

  // MATCH 过滤
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$5\r\nstr:*\r\n",
  );
  let text = String::from_utf8_lossy(&out).to_string();
  let (_, keys) = parse_scan_reply(&text);
  assert_eq!(keys, vec!["str:1", "str:2"], "MATCH 过滤: {text}");

  // TYPE string：仅字符串键（meta 键排除）
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$4\r\nTYPE\r\n$6\r\nstring\r\n",
  );
  let text = String::from_utf8_lossy(&out).to_string();
  let (_, keys) = parse_scan_reply(&text);
  assert_eq!(keys, vec!["str:1", "str:2"], "TYPE string: {text}");

  // TYPE hash：仅 meta 键
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$4\r\nTYPE\r\n$4\r\nhash\r\n",
  );
  let text = String::from_utf8_lossy(&out).to_string();
  let (_, keys) = parse_scan_reply(&text);
  assert_eq!(keys, vec!["h:a"], "TYPE hash: {text}");

  // 参数校验（同步段闭环）：非法游标 / 语法错误
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nSCAN\r\n$2\r\nxy\r\n"),
    b"-ERR invalid cursor\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n"),
    b"-ERR syntax error\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\nx\r\n"
    ),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}

/// 慢路径挂起期间的流水线保序：前置快命令应答先写、慢命令应答后接
#[test]
fn slow_path_preserves_pipeline_order() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["a", "b"]);

  // 同批两帧：GET a（快）+ DBSIZE（慢）——单次消费调用内快应答即时写出，
  // 慢应答由网络泵补齐
  let (consumed, mut out) = pump(
    &mut c,
    b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$6\r\nDBSIZE\r\n",
  );
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"$1\r\nv\r\n", "快命令应答即时写出");
  let slow = c.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"$1\r\nv\r\n:2\r\n", "慢应答按流水线顺序后接");
}

/// 解析 SCAN 应答 `*2\r\n$<n>\r\n<cursor>\r\n*<m>\r\n...`
fn parse_scan_reply(text: &str) -> (u64, Vec<String>) {
  let mut lines = text.split("\r\n");
  assert_eq!(lines.next(), Some("*2"), "SCAN 应答为二元数组: {text}");
  let cursor_len = lines.next().unwrap().strip_prefix('$').unwrap();
  let cursor: u64 = lines.next().unwrap().parse().unwrap();
  let _ = cursor_len;
  let arr = lines.next().unwrap().strip_prefix('*').unwrap();
  let n: usize = arr.parse().unwrap();
  let mut keys = Vec::with_capacity(n);
  for _ in 0..n {
    let len: usize = lines
      .next()
      .unwrap()
      .strip_prefix('$')
      .unwrap()
      .parse()
      .unwrap();
    let key = lines.next().unwrap();
    assert_eq!(key.len(), len, "bulk 长度一致: {text}");
    keys.push(key.to_string());
  }
  keys.sort();
  (cursor, keys)
}

/// MEMORY USAGE 慢路径闭环：键 TTL 已过期时快路径 ttl_gate 降级（Ok(false)），
/// 慢路径经 read_tag_with_size 的 TTL 门控物理清除后回 nil（对齐 C# 存储层
/// 原子过期判定；带 TTL 的活键尺寸统计走快路径整数应答）
#[test]
fn memory_usage_expired_key_via_slow_path() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  // SET ek v + PEXPIRE ek 1（毫秒级 TTL，随即过期）
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nek\r\n$1\r\nv\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$7\r\nPEXPIRE\r\n$2\r\nek\r\n$1\r\n1\r\n"),
    b":1\r\n"
  );
  sleep(Duration::from_millis(20));

  // 过期键：快路径降级慢路径 → check_expired 物理清除 → 缺键 nil
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$2\r\nek\r\n"
    ),
    b"$-1\r\n"
  );
}

/// MEMORY USAGE 活键：带 TTL 的活键走快路径整数应答（不过期不降级）
#[test]
fn memory_usage_live_key_fast_path() {
  let mut c = consumer();
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nlk\r\n$4\r\nvalu\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*3\r\n$7\r\nPEXPIRE\r\n$2\r\nlk\r\n$5\r\n10000\r\n"
    ),
    b":1\r\n"
  );

  // 未过期：同步 ttl_gate 通过，整数应答（>0），无慢路径挂起
  let (consumed, out) = pump(&mut c, b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$2\r\nlk\r\n");
  assert_eq!(consumed, Some(0));
  assert_eq!(out[0], b':', "活键应直接整数应答: {out:?}");
  assert!(c.take_slow_wait().is_none(), "活键不应挂起慢路径");
}

/// INFO KEYSPACE 慢路径闭环（对标 C# PopulateKeyspaceInfo →
/// GetKeyspaceStats：活键数与带 TTL 键数；DEFAULT/ALL 段集合不含
/// KEYSPACE，仅显式请求触发全库扫描）
#[test]
fn info_keyspace_via_slow_path() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  // 空库：仅段头，无条目（C# 仅列出至少持有一个键的库）
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n");
  let text = from_utf8(&out).unwrap();
  assert!(text.contains("# Keyspace\r\n"), "{text}");
  assert!(!text.contains("db0"), "空库不得出条目: {text}");

  set_keys(&mut c, &["k1", "k2"]);
  // k1 加 TTL → expires=1
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nEXPIRE\r\n$2\r\nk1\r\n$3\r\n100\r\n"),
    b":1\r\n"
  );
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n");
  let text = from_utf8(&out).unwrap();
  assert!(
    text.contains("db0:keys=2,expires=1,avg_ttl=0"),
    "键数与 TTL 计数应同时上报: {text}"
  );

  // DEL 带 TTL 的键 → 键数与 expires 同步回落
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nDEL\r\n$2\r\nk1\r\n"),
    b":1\r\n"
  );
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n");
  let text = from_utf8(&out).unwrap();
  assert!(text.contains("db0:keys=1,expires=0,avg_ttl=0"), "{text}");
}
