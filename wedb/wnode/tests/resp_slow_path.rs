//! 端到端集成测试：慢路径执行器（SCAN / KEYS / DBSIZE）
//!
//! 会话同步段仅校验参数，实际扫描经 [`wnode::resp::slow_path::SlowWait`]
//! 挂起、网络泵侧 await 驱动闭环（此处以 compio Runtime 的 block_on 承担
//! 网络泵角色），对标 garnet/test/standalone/Garnet.test 的 SCAN/KEYS/
//! DBSIZE 用例族（RespTests / RespTestsBytes）
use std::{
  str::from_utf8,
  sync::{Arc, atomic::Ordering},
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
};
use wnode_test::err_frame;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, command::RespCommand};
use wtest_base::test_store_config;

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  consumer_with_api().0
}

/// [`consumer`] 的双句柄形态：同时保留慢路径分派句柄（直答 exec_slow，
/// 与降级快照投递面同径；事务 / AOF 的非命令入口同此抵达）
fn consumer_with_api() -> (RespSessionConsumer, GarnetApi) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("slow.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  (
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone()),
    api,
  )
}

/// 组 RESP 请求数组帧（cmd + args）
fn frame_of(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
  let mut frame = format!("*{}\r\n", args.len() + 1).into_bytes();
  for token in std::iter::once(cmd).chain(args.iter().copied()) {
    frame.extend_from_slice(format!("${}\r\n", token.len()).as_bytes());
    frame.extend_from_slice(token);
    frame.extend_from_slice(b"\r\n");
  }
  frame
}

/// 慢路径直答（不经会话快路径，resp_version 显式指定）
fn slow_reply(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resp_version: u8,
) -> Vec<u8> {
  rt.block_on(async {
    SlowWait::for_command(
      api,
      cmd,
      args.iter().map(|a| a.to_vec()).collect(),
      resp_version,
    )
    .resolve()
    .await
  })
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
    err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
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

/// INFO KEYSPACE 逐库归属与只读副作用为零（回归旧慢路径断链：逐库
/// `set_active_db` 丢弃切库结果 → 遇冷库仍按上一库前缀扫描并把库号错贴到
/// 结果上，且统计完不回上下文（本用例以「INFO 后写入仍落 db 1」证伪）；
/// 权威路由租户对 0..max_databases 逐号盲分配虚库并逐个落 KeyTag::DbMeta
/// 真实写盘 → 以 tail_address 与 next_virtual_id 双不动证伪）。现由引擎单内核
/// 只读遍历在册库、一趟分桶扫描，连接会话上下文全程零改动
#[test]
fn info_keyspace_multi_db_read_only() {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("keyspace.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let session = store.new_session().unwrap();
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session)),
  );

  // db 0：2 键（a 带 TTL）；切到 db 1 后：1 键
  set_keys(&mut c, &["a", "b"]);
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nEXPIRE\r\n$1\r\na\r\n$3\r\n100\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["c"]);

  let tail = store.tail_address();
  let allocated = store.vdb.next_virtual_id.load(Ordering::Relaxed);
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n");
  let text = from_utf8(&out).unwrap();
  // 各库计数各归本库：会话停在 db 1，db 0 行仍如实报出 2 键 1 过期
  assert!(
    text.contains("db0:keys=2,expires=1,avg_ttl=0")
      && text.contains("db1:keys=1,expires=0,avg_ttl=0"),
    "逐库归属须各自正确: {text}"
  );
  assert_eq!(
    store.tail_address(),
    tail,
    "只读统计不得追加日志记录（无 DbMeta 写盘）"
  );
  // INFO 不新分配虚库：旧形态对 0..max_databases 逐号 set_active_db →
  // get_or_create_db 单调抬升分配水位并 CoW 全表克隆，一条 INFO 即把从未用过的
  // 虚库号永久吃掉
  assert_eq!(
    store.vdb.next_virtual_id.load(Ordering::Relaxed),
    allocated,
    "INFO 绝不消耗虚拟 ID"
  );
  // 连接会话上下文零改动：统计后再发一条写命令，仍落会话所在 db 1（旧形态逐库
  // 切库后上下文停在末库号，后续写入静默进错库）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":1\r\n",
    "INFO 后会话仍停在 db 1"
  );
  set_keys(&mut c, &["after_info"]);
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":2\r\n",
    "INFO 后的写入须落在原会话库 db 1"
  );

  // 在册库集合不因统计膨胀：只有被 SELECT 过的 0/1 两库出行
  let rows = rt
    .block_on(async { store.keyspace_stats(0).await })
    .unwrap();
  assert_eq!(rows, [(0, 2, 1), (1, 2, 0)], "虚库不得被盲分配");
}

/// 对象族慢分派参数校验帧与快路径逐字节一致（参数推导单源内核；修复点：
/// 慢侧不再把解析失败折叠为 ASYNC_REQUIRED 哨兵、SINTERCARD 负 LIMIT 不再
/// 放行、HRANDFIELD 第三词元恢复 WITHVALUES 大小写门）
///
/// 全部用例为解析失败面（不触达存储），快侧经会话直答、慢侧经 exec_slow
/// 直答（降级快照 / 事务 / AOF 非命令入口同径）
#[test]
fn object_slow_parse_frames_match_fast() {
  let rt = Runtime::new().unwrap();
  let (mut c, api) = consumer_with_api();

  let cases: &[(RespCommand, &[u8], &[&[u8]])] = &[
    (
      RespCommand::Sintercard,
      b"SINTERCARD",
      &[b"-3000000000", b"k1"],
    ),
    (
      RespCommand::Sintercard,
      b"SINTERCARD",
      &[b"2", b"k1", b"k2", b"LIMIT", b"-3000000000"],
    ),
    (
      RespCommand::Sintercard,
      b"SINTERCARD",
      &[b"2", b"k1", b"k2", b"LIMIT", b"-1"],
    ),
    (RespCommand::Sintercard, b"SINTERCARD", &[b"0", b"k1"]),
    (RespCommand::Zintercard, b"ZINTERCARD", &[b"0", b"k1"]),
    (
      RespCommand::Zintercard,
      b"ZINTERCARD",
      &[b"2", b"k1", b"k2", b"LIMIT", b"-1"],
    ),
    (
      RespCommand::Hexpire,
      b"HEXPIRE",
      &[b"hk", b"-1", b"FIELDS", b"1", b"f"],
    ),
    (
      RespCommand::Hexpire,
      b"HEXPIRE",
      &[b"hk", b"abc", b"FIELDS", b"1", b"f"],
    ),
    (
      RespCommand::Zexpire,
      b"ZEXPIRE",
      &[b"zk", b"abc", b"MEMBERS", b"1", b"m"],
    ),
    (RespCommand::Httl, b"HTTL", &[b"hk", b"XFIELDS", b"1", b"f"]),
    (
      RespCommand::Zrandmember,
      b"ZRANDMEMBER",
      &[b"zk", b"1", b"BAD"],
    ),
    (
      RespCommand::Hrandfield,
      b"HRANDFIELD",
      &[b"hk", b"1", b"BAD"],
    ),
    (RespCommand::Zrank, b"ZRANK", &[b"zk", b"m", b"BAD"]),
    (RespCommand::Lmpop, b"LMPOP", &[b"1", b"lk", b"UP"]),
    (
      RespCommand::Lmpop,
      b"LMPOP",
      &[b"1", b"lk", b"LEFT", b"COUNT", b"0"],
    ),
    (
      RespCommand::Zmpop,
      b"ZMPOP",
      &[b"1", b"zk", b"MIN", b"COUNT", b"0"],
    ),
    (RespCommand::Ltrim, b"LTRIM", &[b"lk", b"a", b"1"]),
    (RespCommand::Lrange, b"LRANGE", &[b"lk", b"0", b"b"]),
    (RespCommand::Spop, b"SPOP", &[b"sk", b"-1"]),
  ];

  for (cmd, name, args) in cases {
    let fast = roundtrip(&mut c, &frame_of(name, args));
    let slow = slow_reply(&rt, &api, *cmd, args, wconf::DEFAULT_RESP_VERSION);
    assert_eq!(
      slow, fast,
      "{name:?} 慢侧校验帧与快侧不一致（参数推导应单源）"
    );
  }

  // 门禁判据：SINTERCARD 慢侧负溢出应答与快侧逐字节一致，
  // 不再出现 "-ERR command requires asynchronous completion"
  let not_integer: &[u8] = b"-ERR value is not an integer or out of range.\r\n";
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Sintercard,
      &[b"-3000000000", b"k1"],
      2
    ),
    not_integer
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Sintercard,
      &[b"2", b"k1", b"k2", b"LIMIT", b"-3000000000"],
      2
    ),
    not_integer
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Sintercard,
      &[b"2", b"k1", b"k2", b"LIMIT", b"-1"],
      2
    ),
    b"-ERR LIMIT can't be negative\r\n"
  );
}

/// 同输入快慢同字节（HEXPIRE/ZEXPIRE/ZRANDMEMBER/LMPOP/HRANDFIELD 热键
/// 直答对慢路径直答；nil 帧走 write_resp_null_ver 单点，RESP2/RESP3 双版本）
#[test]
fn object_slow_happy_path_matches_fast() {
  let rt = Runtime::new().unwrap();
  let (mut c, api) = consumer_with_api();
  let ver = wconf::DEFAULT_RESP_VERSION;

  // 热键置数（快路径写）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"HSET", &[b"hk", b"f1", b"v1"])),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"HSET", &[b"hk2", b"f1", b"v1"])),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZADD", &[b"zk", b"1.5", b"m1"])),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZADD", &[b"zk2", b"1", b"m1"])),
    b":1\r\n"
  );

  // HEXPIRE 热键直答 vs 慢答（同字段各执一键，应答 *1 + :1）
  assert_eq!(
    roundtrip(
      &mut c,
      &frame_of(b"HEXPIRE", &[b"hk", b"100", b"FIELDS", b"1", b"f1"])
    ),
    b"*1\r\n:1\r\n"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Hexpire,
      &[b"hk2", b"100", b"FIELDS", b"1", b"f1"],
      ver
    ),
    b"*1\r\n:1\r\n"
  );

  // ZEXPIRE 热键直答 vs 慢答
  assert_eq!(
    roundtrip(
      &mut c,
      &frame_of(b"ZEXPIRE", &[b"zk", b"100", b"MEMBERS", b"1", b"m1"])
    ),
    b"*1\r\n:1\r\n"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Zexpire,
      &[b"zk2", b"100", b"MEMBERS", b"1", b"m1"],
      ver
    ),
    b"*1\r\n:1\r\n"
  );

  // ZRANDMEMBER 单成员集合（随机种子无歧义）：无 count / 带 count 双形态
  let member_frame: &[u8] = b"$2\r\nm1\r\n";
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZRANDMEMBER", &[b"zk"])),
    member_frame
  );
  // 慢侧再答需要独立键（快侧已答不改状态，只读可同键复答）
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Zrandmember, &[b"zk"], ver),
    member_frame
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZRANDMEMBER", &[b"zk", b"1"])),
    b"*1\r\n$2\r\nm1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Zrandmember, &[b"zk", b"1"], ver),
    b"*1\r\n$2\r\nm1\r\n"
  );

  // LMPOP 弹出直答 vs 慢答（同键重灌同元素，应答逐字节一致）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"RPUSH", &[b"lk", b"e1"])),
    b":1\r\n"
  );
  let lmpop_frame: &[u8] = b"*2\r\n$2\r\nlk\r\n*1\r\n$2\r\ne1\r\n";
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"LMPOP", &[b"1", b"lk", b"LEFT"])),
    lmpop_frame
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"RPUSH", &[b"lk", b"e1"])),
    b":1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Lmpop, &[b"1", b"lk", b"LEFT"], ver),
    lmpop_frame
  );

  // 缺失态应答双版本：nil 帧走 write_resp_null_ver 单点（RESP2 `$-1` /
  // RESP3 `_`），null 数组 RESP2 `*-1` / RESP3 `_`
  // ZRANDMEMBER 缺键：无 count → nil；带 count → *0（两版本同形）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZRANDMEMBER", &[b"nokey-z"])),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Zrandmember, &[b"nokey-z"], 2),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Zrandmember, &[b"nokey-z"], 3),
    b"_\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"ZRANDMEMBER", &[b"nokey-z", b"1"])),
    b"*0\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Zrandmember, &[b"nokey-z", b"1"], 3),
    b"*0\r\n"
  );

  // HRANDFIELD 缺键：无 count → nil；带 count → *0
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"HRANDFIELD", &[b"nokey-h"])),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Hrandfield, &[b"nokey-h"], 2),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Hrandfield, &[b"nokey-h"], 3),
    b"_\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Hrandfield, &[b"nokey-h", b"1"], 3),
    b"*0\r\n"
  );

  // LMPOP 缺键：null 数组（RESP2 `*-1` / RESP3 `_`）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"LMPOP", &[b"1", b"nokey-l", b"LEFT"])),
    b"*-1\r\n"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Lmpop,
      &[b"1", b"nokey-l", b"LEFT"],
      2
    ),
    b"*-1\r\n"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Lmpop,
      &[b"1", b"nokey-l", b"LEFT"],
      3
    ),
    b"_\r\n"
  );

  // HEXPIRE 缺键：逐字段 -2 数组（两版本同形）
  let notfound: &[u8] = b"*1\r\n:-2\r\n";
  assert_eq!(
    roundtrip(
      &mut c,
      &frame_of(b"HEXPIRE", &[b"nokey-hx", b"100", b"FIELDS", b"1", b"f"])
    ),
    notfound
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Hexpire,
      &[b"nokey-hx", b"100", b"FIELDS", b"1", b"f"],
      3
    ),
    notfound
  );
}
