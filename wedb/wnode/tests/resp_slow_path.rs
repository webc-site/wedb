//! 端到端集成测试：慢路径执行器（SCAN / KEYS / DBSIZE）
//!
//! 会话同步段仅校验参数，实际扫描经 [`wnode::resp::slow_path::SlowWait`]
//! 挂起、网络泵侧 await 驱动闭环（此处以 compio Runtime 的 block_on 承担
//! 网络泵角色），对标 garnet/test/standalone/Garnet.test 的 SCAN/KEYS/
//! DBSIZE 用例族（RespTests / RespTestsBytes）
use std::{
  iter::once,
  str::from_utf8,
  sync::{Arc, atomic::Ordering},
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::{err_frame, pump};
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, command::RespCommand};
use wtest_base::test_store_config;

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  consumer_with_handles().0
}

/// [`consumer`] 的双句柄形态：同时保留慢路径分派句柄（直答 exec_slow，
/// 与降级快照投递面同径；事务 / AOF 的非命令入口同此抵达）
fn consumer_with_api() -> (RespSessionConsumer, GarnetApi) {
  let (c, api, _) = consumer_with_handles();
  (c, api)
}

/// 三句柄形态：额外保留存储句柄（flush_and_evict_all 冷化构造磁盘候选
/// 降级场景）
fn consumer_with_handles() -> (
  RespSessionConsumer,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
) {
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
    store,
  )
}

/// 组 RESP 请求数组帧（cmd + args）
fn frame_of(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
  let mut frame = format!("*{}\r\n", args.len() + 1).into_bytes();
  for token in once(cmd).chain(args.iter().copied()) {
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

/// MEMORY USAGE 信封域磁盘候选回归：冷存落盘的信封集合（Hash），快路径
/// RecordOnDisk 必须降级慢路径（Ok(false)），经 exec_slow MemoryUsage 异步
/// 回读反序列化回正整数占用。修复前信封域把 RecordOnDisk 并入 NotFound
/// 空操作块，击穿至 Meta 域误答 nil，旁路 slow.rs C::MemoryUsage 臂——
/// 键完全存活（HGET 可读）却报告不存在，破坏 RESP 契约与可观测性
///（对标 C# NetworkMemoryUsage 经 CompletePendingForUnifiedStoreSession
/// 异步回读冷记录回 AllocatedSize，仅 status != OK 才 WriteNull）
#[test]
fn memory_usage_envelope_on_disk_via_slow_path() {
  let rt = Runtime::new().unwrap();
  let (mut c, _api, store) = consumer_with_handles();

  // 信封集合（Hash）建键 → 冷化落盘（数据与索引均出内存）
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nhk\r\n$1\r\nf\r\n$3\r\nval\r\n"
    ),
    b":1\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 快路径信封域遇磁盘候选：不得同步应答（修复前击穿 Meta 域误答 nil）
  let (consumed, out) = pump(&mut c, b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$2\r\nhk\r\n");
  assert_eq!(consumed, Some(0));
  assert!(
    out.is_empty(),
    "磁盘候选信封不得同步应答（修复前击穿误答 nil）: {out:?}"
  );
  let slow = c.take_slow_wait().expect("信封磁盘候选应挂起慢路径");
  let out = rt.block_on(async { slow.resolve().await });
  assert_eq!(out[0], b':', "冷存信封应答整数: {out:?}");
  let total: i64 = from_utf8(&out[1..out.len() - 2]).unwrap().parse().unwrap();
  assert!(total > 0, "冷存信封占用应为正整数: {total}");

  // 键仍存活可读（防「答了整数键却没了」的假阳性）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$4\r\nHGET\r\n$2\r\nhk\r\n$1\r\nf\r\n"),
    b"$3\r\nval\r\n"
  );
}

/// EXPIRE 族慢路径 -2 判死口径（wkv expire_at 判定表的 RESP 应答镜像，
/// 快路径 expire_apply_sync 同态）：键缺失与「键存活但 TTL 已过期」两种
/// -2 形态慢路径一律 :0，与快路径同输入逐字节一致。修复前 `applied != 0`
/// 判据把 -2 误答 :1——应答 :1 而键已被惰性清除的用户可见发散（与臂内
/// 自注、快路径 Some(0) 臂、C# status != OK 回 :0 三方矛盾）
#[test]
fn expire_slow_path_minus_two_answers_zero() {
  let rt = Runtime::new().unwrap();
  let (mut c, api, store) = consumer_with_handles();

  // 形态一：键完全缺失。快路径 :0（C# status != OK 同口径）；慢路径直答
  // wkv expire_at 回 -2 → :0，同输入快慢逐字节一致
  let expire_miss = b"*3\r\n$6\r\nEXPIRE\r\n$4\r\nmiss\r\n$3\r\n100\r\n";
  assert_eq!(roundtrip(&mut c, expire_miss), b":0\r\n");
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Expire,
      &[b"miss", b"100"],
      wconf::DEFAULT_RESP_VERSION
    ),
    b":0\r\n",
    "慢路径对缺失键 EXPIRE 应与快路径同答 :0（修复前 -2 误落 :1）"
  );

  // 形态二：TTL 已过期 + 记录磁盘候选。SET + PEXPIRE 后直写过期 TTL
  //（RESP 面无法自然构造「过期未清」态，put_ttl_sync 裸写内核原位覆写），
  // 冷化落盘使快路径 probe_alive 遇磁盘候选整体降级 → 慢路径 wkv
  // expire_at 惰性 purge 后回 -2 → :0，且键已物理消失
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nek\r\n$1\r\nv\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$7\r\nPEXPIRE\r\n$2\r\nek\r\n$3\r\n100\r\n"),
    b":1\r\n"
  );
  {
    let probe = store.new_session().unwrap();
    let batch = probe.enter_batch();
    put_ttl_sync(&batch, b"ek", now_ticks() - TICKS_PER_SECOND).unwrap();
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 快路径降级（数据与 TTL 记录均磁盘候选）→ 慢路径闭环 :0（修复前 :1）
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*3\r\n$6\r\nEXPIRE\r\n$2\r\nek\r\n$3\r\n100\r\n"
    ),
    b":0\r\n",
    "过期键 + 磁盘候选降级慢路径应答 :0（修复前 -2 误落 :1）"
  );
  // purge 副作用可见：键已物理消失（GET nil；再 EXPIRE :0 不再降级）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nek\r\n"),
    b"$-1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nEXPIRE\r\n$2\r\nek\r\n$3\r\n100\r\n"),
    b":0\r\n"
  );
}

/// GETRANGE/SUBSTR 慢路径存储回读分支反转区间回归（对标 C#
/// MainStore PrivateMethods.CopyRespTo 41 行 `(start < end) ? .. : 0` 半边防御，
/// 该方法已整条登记忽略，不作实现映射锚点声明）：
/// normalize_range 可产出 start > end 的反转区间——len=5 时 GETRANGE k 5 -2
/// 归一化 (5, 4)（正起点分支）、GETRANGE k -6 -3 归一化 (4, 3)（负起点分支）。
/// 修复前慢路径臂只判 start == end，val[5..4] 安全索引直接 panic，破坏
/// 生产路径零未受控 panic 红线；修复后回空串且与快路径逐字节一致
#[test]
fn getrange_substr_slow_path_inverted_range() {
  let rt = Runtime::new().unwrap();
  let (mut c, api) = consumer_with_api();
  let ver = wconf::DEFAULT_RESP_VERSION;

  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"k", b"abcde"])),
    b"+OK\r\n"
  );

  // 票面两反转样例 + 相等/正常对照锚点：慢直答与快路径同输入应答全等
  let cases: &[(&[&[u8]], &[u8])] = &[
    (&[b"k", b"5", b"-2"], b"$0\r\n\r\n"),  // 正起点分支反转 (5, 4)
    (&[b"k", b"-6", b"-3"], b"$0\r\n\r\n"), // 负起点分支反转 (4, 3)
    (&[b"k", b"5", b"-1"], b"$0\r\n\r\n"),  // 归一化相等 (5, 5)
    (&[b"k", b"0", b"4"], b"$5\r\nabcde\r\n"), // 正常非空区间
  ];
  for (cmd, cmd_name) in [
    (RespCommand::Getrange, &b"GETRANGE"[..]),
    (RespCommand::Substr, &b"SUBSTR"[..]),
  ] {
    for &(args, want) in cases {
      assert_eq!(
        slow_reply(&rt, &api, cmd, args, ver),
        want,
        "慢路径 {cmd_name:?} {args:?} 回读分支应答"
      );
      assert_eq!(
        roundtrip(&mut c, &frame_of(cmd_name, args)),
        want,
        "快路径 {cmd_name:?} {args:?} 应与慢路径逐字节一致"
      );
    }
  }
}

/// INFO KEYSPACE 对空 RangeIndex 的活键计数（元记录存活 MetaValue::is_live
/// 单点收敛：RangeIndex 恒活——修复前统计面残留 `read_size > 0` 旧判据，
/// 空索引（RI.CREATE 后未写入，size == 0）被排除在活键数与带 TTL 列之外，
/// 与 EXISTS :1 / TTL : -1 同刻三方应答不自洽）
#[test]
fn info_keyspace_counts_empty_range_index() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  // 空索引：Meta 元记录在册而 size == 0
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      &frame_of(b"RI.CREATE", &[b"rix", b"MEMORY", b"CACHESIZE", b"65536"])
    ),
    b"+OK\r\n"
  );

  // 同刻三方自洽：EXISTS :1（is_live 口径）、TTL : -1（存活无过期）、
  // INFO keyspace 活键数 1（修复前计 0）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"EXISTS", &[b"rix"])),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&mut c, &frame_of(b"TTL", &[b"rix"])), b":-1\r\n");
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$4\r\nINFO\r\n$8\r\nkeyspace\r\n");
  let text = from_utf8(&out).unwrap();
  assert!(
    text.contains("db0:keys=1,expires=0,avg_ttl=0"),
    "空 RangeIndex 应计入活键数（修复前 size>0 旧口径漏计）: {text}"
  );
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
type SlowParseCase<'a> = (RespCommand, &'a [u8], &'a [&'a [u8]]);

#[test]
fn object_slow_parse_frames_match_fast() {
  let rt = Runtime::new().unwrap();
  let (mut c, api) = consumer_with_api();

  let cases: &[SlowParseCase<'_>] = &[
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

/// SPOP / SRANDMEMBER 缺失键应答四象限（D15）：无 count → RESP null，
/// 显式 count 才允许空集合/空数组；快路径与慢路径同输入逐字节一致。
/// 对标 C# SetCommands.cs SetPop / SetRandomMember 的 NOTFOUND 分支
/// （WriteNull vs WriteEmptySet / TryWriteEmptyArray）
#[test]
fn spop_srandmember_missing_key_null_vs_empty() {
  let rt = Runtime::new().unwrap();
  let (mut c, api) = consumer_with_api();

  // SRANDMEMBER 缺键：无 count → nil（RESP2 `$-1` / RESP3 `_`）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SRANDMEMBER", &[b"nokey-s"])),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Srandmember, &[b"nokey-s"], 2),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Srandmember, &[b"nokey-s"], 3),
    b"_\r\n"
  );
  // SRANDMEMBER 缺键带 count → 空数组（RESP_EMPTYLIST，两版本同形）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SRANDMEMBER", &[b"nokey-s", b"1"])),
    b"*0\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Srandmember, &[b"nokey-s", b"1"], 3),
    b"*0\r\n"
  );

  // SPOP 缺键：无 count → nil；带 count → 空集合（版本分派 RESP2 `*0` / RESP3 `~0`）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SPOP", &[b"nokey-p"])),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Spop, &[b"nokey-p"], 2),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Spop, &[b"nokey-p"], 3),
    b"_\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SPOP", &[b"nokey-p", b"1"])),
    b"*0\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Spop, &[b"nokey-p", b"1"], 2),
    b"*0\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Spop, &[b"nokey-p", b"1"], 3),
    b"~0\r\n"
  );

  // SPOP 弹空自愈后再答：键已物理回收 → 同缺失口径（nil）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SADD", &[b"drain-s", b"a"])),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SPOP", &[b"drain-s"])),
    b"$1\r\na\r\n"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Spop, &[b"drain-s"], 2),
    b"$-1\r\n"
  );
}

/// MGET 慢路径批量读回归：普通字符串键命中、已过期键答 nil、缺失键答 nil、
/// 对象键答 nil；RESP2 / RESP3 双协议版本逐字节对齐（RESP2 `$-1` / RESP3 `_`）。
#[test]
fn mget_slow_path_expired_missing_and_object_keys() {
  let rt = Runtime::new().unwrap();
  let (mut c, api, store) = consumer_with_handles();

  // 1. 写入普通字符串键 str1 = "hello"
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"str1", b"hello"])),
    b"+OK\r\n"
  );

  // 2. 写入已过期键 exp1 = "expired_val"（Ticks = 1）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"exp1", b"expired_val"])),
    b"+OK\r\n"
  );
  rt.block_on(async {
    let session = store.new_session().unwrap();
    session.put_ttl(b"exp1", 1).await.unwrap();
  });

  // 3. 写入集合对象键 obj1
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SADD", &[b"obj1", b"member1"])),
    b":1\r\n"
  );

  // 4. 调用 MGET 慢路径（通过 slow_reply）：str1, exp1, missing1, obj1
  // RESP2 应答：*4\r\n$5\r\nhello\r\n$-1\r\n$-1\r\n$-1\r\n
  let resp2 = slow_reply(
    &rt,
    &api,
    RespCommand::Mget,
    &[b"str1", b"exp1", b"missing1", b"obj1"],
    2,
  );
  assert_eq!(resp2, b"*4\r\n$5\r\nhello\r\n$-1\r\n$-1\r\n$-1\r\n");

  // RESP3 应答：*4\r\n$5\r\nhello\r\n_\r\n_\r\n_\r\n
  let resp3 = slow_reply(
    &rt,
    &api,
    RespCommand::Mget,
    &[b"str1", b"exp1", b"missing1", b"obj1"],
    3,
  );
  assert_eq!(resp3, b"*4\r\n$5\r\nhello\r\n_\r\n_\r\n_\r\n");
}
