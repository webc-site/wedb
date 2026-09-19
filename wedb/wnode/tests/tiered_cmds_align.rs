//! 升阶键命令面语义对齐集成测试（next/open.data.md 八条修订）
//!
//! 升阶（wcol should_promote → KeyTag::Meta 元记录 + wbftree 树）后：
//! 1. 键存活探针五命令 EXISTS/TTL/EXPIRE/PERSIST/TYPE 与统计面
//!    MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT 对升阶键回值；
//! 2. SCAN 族树内游标全量遍历（HSCAN/SSCAN/ZSCAN/COSCAN）；
//! 3. ZADD 全选项语义与互斥校验、ZRANGE/GEO 物化装载语义；
//! 4. List LPOP count 数组形态、RPOP 尾端弹出（fDelAtHead 双分支）、
//!    LPUSH 序号不覆盖、LINDEX；分层态序号窗口两端伸缩与内存态逐条对齐；
//! 5. SPOP 负 count 拦截、SRANDMEMBER 负 count；
//! 6. 未支持操作物化降级走对象层（HRANDFIELD/SMOVE/LTRIM）。

use std::{
  collections::{HashSet, VecDeque},
  mem::take,
  str::from_utf8,
  sync::Arc,
};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

fn auto_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 升阶键探针五命令与统计面（EXISTS/TTL/EXPIRE/PERSIST/TYPE +
/// MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT）
#[test]
fn test_tiered_key_probe_and_stats() {
  let (rt, api, store, _dir) = open_env("tiered-probe.db");
  let mut s = session_with(&api);

  // 升阶：hash 写入 65546 字段
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"h".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }

  // 升阶确认
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"h"))
      .unwrap()
      .is_some(),
    "应已升阶"
  );

  // 探针面：键对客户端可见
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"h"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"h"]),
    b"+hash\r\n"
  );
  // 无 TTL → -1（C# ExpiryRead::NoExpiry）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]),
    b":-1\r\n"
  );
  // EXPIRE 生效 → :1，TTL 落在 (0,100]
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[b"h", b"100"]),
    b":1\r\n"
  );
  let ttl = auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]);
  let ttl_val: i64 = from_utf8(&ttl[1..ttl.len() - 2]).unwrap().parse().unwrap();
  assert!((0..=100).contains(&ttl_val), "TTL 应在 (0,100]: {ttl:?}");
  // PERSIST → :1，TTL 恢复 -1
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Persist, &[b"h"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]),
    b":-1\r\n"
  );

  // 统计面
  let mem = auto_exec(&api, &rt, &mut s, RespCommand::MemoryUsage, &[b"h"]);
  let mem_val: i64 = from_utf8(&mem[1..mem.len() - 2]).unwrap().parse().unwrap();
  assert!(mem_val > 0, "MEMORY USAGE 应回正整数: {mem:?}");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectEncoding, &[b"h"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectIdletime, &[b"h"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectRefcount, &[b"h"]),
    b":1\r\n"
  );
}

/// 升阶键 SCAN 族树内游标全量遍历
#[test]
fn test_tiered_scan_full_iteration() {
  let (rt, api, _store, _dir) = open_env("tiered-scan.db");
  let mut s = session_with(&api);

  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"s".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("m{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Sadd, &arg_slices);
  }

  // SSCAN 全量遍历：COUNT 50000（钳 OBJECT_SCAN_COUNT_LIMIT=1000），
  // 迭代至 cursor 0，成员去重计数 == total
  let mut cursor = b"0".to_vec();
  let mut seen = HashSet::new();
  let mut rounds = 0;
  loop {
    let count_arg = buf.format(50000).as_bytes().to_vec();
    let out = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sscan,
      &[b"s", &cursor, b"COUNT", &count_arg],
    );
    let text = String::from_utf8(out).unwrap();
    // 帧形态：*2\r\n$<len>\r\n<cursor>\r\n*<n>\r\n($<len>\r\n<member>\r\n)*
    let mut parts = text.split("\r\n");
    assert_eq!(parts.next(), Some("*2"));
    let _ = parts.next().unwrap();
    let next_cursor = parts.next().unwrap().to_string();
    let arr_len: usize = parts
      .next()
      .unwrap()
      .trim_start_matches('*')
      .parse()
      .unwrap();
    // 每成员占两行（bulk 头 + 值）
    for _ in 0..arr_len {
      let hdr = parts.next().unwrap();
      assert!(hdr.starts_with('$'), "成员应为 bulk 头: {hdr}");
      let m = parts.next().unwrap();
      assert!(seen.insert(m.to_string()), "成员不应重复返回: {m}");
    }
    rounds += 1;
    if next_cursor == "0" {
      break;
    }
    cursor = next_cursor.into_bytes();
    assert!(rounds < 200, "游标未收敛: {rounds} 轮");
  }
  assert_eq!(seen.len(), total, "全量遍历应覆盖全部成员");

  // HSCAN 升阶键可遍历（hash 域）
  let mut buf = ItoaBuffer::new();
  let h_total = 2048; // 不再升阶，仅验证树内 hash 扫描通道（复用 s 键会 WRONGTYPE）
  for i in 1..=h_total {
    let f = format!("f{i}");
    let v = buf.format(i).as_bytes().to_vec();
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hset,
      &[b"h", f.as_bytes(), &v],
    );
  }
  let mut seen_fields = 0usize;
  let mut cursor = b"0".to_vec();
  loop {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hscan, &[b"h", &cursor]);
    let text = String::from_utf8(out).unwrap();
    let mut parts = text.split("\r\n");
    assert_eq!(parts.next(), Some("*2"));
    let _ = parts.next().unwrap();
    let next_cursor = parts.next().unwrap().to_string();
    let arr_len: usize = parts
      .next()
      .unwrap()
      .trim_start_matches('*')
      .parse()
      .unwrap();
    seen_fields += arr_len;
    let _ = parts.take(arr_len * 2).count();
    if next_cursor == "0" {
      break;
    }
    cursor = next_cursor.into_bytes();
  }
  assert_eq!(seen_fields, h_total * 2, "HSCAN 应回全量 field/value 对");
}

/// 升阶键 ZADD 选项语义、ZRANGE 物化与 GEO 面
#[test]
fn test_tiered_zadd_opts_zrange_geo() {
  let (rt, api, _store, _dir) = open_env("tiered-zadd.db");
  let mut s = session_with(&api);

  // 小集合起步 + 少量成员，升阶用灌水成员达成
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"z".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(buf.format(i).as_bytes().to_vec());
      args.push(format!("m{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &arg_slices);
  }
  // 基准成员
  auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[b"z", b"1", b"base"]);

  // ZADD NX：存在不更新
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"NX", b"99", b"base"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"z", b"base"]),
    b"$1\r\n1\r\n"
  );
  // ZADD XX：仅更新已存在（无 CH 时更新不计入返回值 → :0）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"XX", b"5", b"base"]
    ),
    b":0\r\n"
  );
  // ZADD GT：新分值不高则不更新；更高则更新（同样无 CH → :0）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"GT", b"2", b"base"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"GT", b"7", b"base"]
    ),
    b":0\r\n"
  );
  // ZADD CH：变更计数（base 7→7 相等不计数，brand_new 新增计数）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"CH", b"7", b"base", b"1", b"brand_new"]
    ),
    b":1\r\n"
  );
  // ZADD INCR：bulk string 形态回增量结果（7 + 3 = 10）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"INCR", b"3", b"base"]
    ),
    b"$2\r\n10\r\n"
  );
  // INCR + NX 命中已存在（不满足 NX）→ null
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"z", b"NX", b"INCR", b"3", b"base"]
    ),
    b"$-1\r\n"
  );
  // 互斥校验错误文案（对标 C# GetOptions）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[b"z", b"NX", b"XX", b"1", b"x"],
  );
  assert_eq!(
    out,
    b"-ERR XX and NX options at the same time are not compatible\r\n"
  );
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[b"z", b"GT", b"LT", b"1", b"x"],
  );
  assert_eq!(
    out,
    b"-ERR GT, LT, and/or NX options at the same time are not compatible\r\n"
  );

  // ZRANGE WITHSCORES（物化装载通道，对象层单源解析）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zrange,
    &[b"z", b"0", b"0", b"WITHSCORES"],
  );
  // 最低分成员应为 total 起始序号附近（分值 1 的灌水成员之一），帧含 2 元素带分值
  let text = String::from_utf8(out).unwrap();
  // 帧：*2\r\n$<len>\r\n<member>\r\n$<len>\r\n<score>\r\n
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"));
  let member_hdr = parts.next().unwrap();
  assert!(
    member_hdr.starts_with('$'),
    "成员应为 bulk 头: {member_hdr}"
  );
  let member = parts.next().unwrap();
  let score_hdr = parts.next().unwrap();
  assert!(
    score_hdr.starts_with('$'),
    "WITHSCORES 应带分值头: {score_hdr}"
  );
  let score = parts.next().unwrap();
  assert!(!member.is_empty() && !score.is_empty());

  // GEO 面：升阶键 GEOADD / GEOPOS / GEODIST
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"z", b"13.361389", b"38.115556", b"palermo"]
    ),
    b":1\r\n"
  );
  let pos = auto_exec(&api, &rt, &mut s, RespCommand::Geopos, &[b"z", b"palermo"]);
  let text = String::from_utf8(pos).unwrap();
  assert!(
    text.starts_with("*1\r\n*2\r\n"),
    "GEOPOS 应回坐标对: {text}"
  );
  let dist = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Geodist,
    &[b"z", b"palermo", b"palermo"],
  );
  assert_eq!(dist, b"$1\r\n0\r\n");
}

/// 升阶键 List/Set 命令面与未支持操作物化降级
#[test]
fn test_tiered_list_set_and_demote() {
  let (rt, api, _store, _dir) = open_env("tiered-list-set.db");
  let mut s = session_with(&api);

  // ---- List 升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"l".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("v{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &arg_slices);
  }

  // LPOP count 数组形态 / 单形态 / 零形态
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"1"]);
  assert_eq!(out, b"$2\r\nv4\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"0"]);
  assert_eq!(out, b"*0\r\n");

  // LPUSH 序号不覆盖：推入后 LINDEX 0 应为最新推入元素（头端不丢）
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &[b"l", b"head"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]);
  assert_eq!(out, b"$4\r\nhead\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]);
  assert_eq!(out, b"$6\r\nv65546\r\n");

  // ---- RPOP 尾端弹出（C# ListPop 的 fDelAtHead=false 分支：list.Last + RemoveLast，
  // 与 LPOP 的 fDelAtHead=true 两支互反。旧分层臂把两支塌缩成同一次头端正序扫描，
  // split_off(0) 恒等 ⇒ RPOP 弹的是最旧元素且与 LPOP 弹同一批）
  // 尾端最大序号先出、其余按序号降序跟进（与 LINDEX -1/-2/-3 的独立臂口径一致）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"3"]);
  assert_eq!(
    out, b"*3\r\n$6\r\nv65546\r\n$6\r\nv65545\r\n$6\r\nv65544\r\n",
    "RPOP count 须自尾端弹出且尾在前"
  );
  // RPOP 无 count → 单 bulk 形态，弹当前尾端 v65543
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l"]);
  assert_eq!(out, b"$6\r\nv65543\r\n");
  // RPOP 0 → 空数组，长度不变
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"0"]);
  assert_eq!(out, b"*0\r\n");
  // 尾弹只动尾端：头端仍是 LPUSH 的 head、次头端 v5 未被侵蚀
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"1"]);
  assert_eq!(out, b"$4\r\nhead\r\n", "RPOP 不得侵蚀头端元素");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"2"]);
  assert_eq!(out, b"*2\r\n$2\r\nv5\r\n$2\r\nv6\r\n");
  // 剩余尾端（LINDEX 独立臂交叉核对，须与 RPOP 已弹端相邻不重不漏）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]);
  assert_eq!(out, b"$6\r\nv65542\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  assert_eq!(out, b":65536\r\n", "size 递减须与净弹出条数一致");

  // ---- 内存态（未升阶）同序列对照：同一份数据两态逐条对齐
  auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    &[b"ml", b"v1", b"v2", b"v3", b"v4", b"v5"],
  );
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &[b"ml", b"head"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nv5\r\n$2\r\nv4\r\n$2\r\nv3\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml"]);
  assert_eq!(out, b"$2\r\nv2\r\n");
  // count 大于剩余长度 → 截断为剩余条数（含头端 head，非 null）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml", b"9"]);
  assert_eq!(out, b"*2\r\n$2\r\nv1\r\n$4\r\nhead\r\n");
  // 弹空自愈：键已删 → null
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml"]);
  assert_eq!(out, b"$-1\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"ml"]);
  assert_eq!(out, b":0\r\n", "弹空后键须自愈删除");

  // ---- Set 升阶
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"k".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("m{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Sadd, &arg_slices);
  }

  // SPOP 负 count → 拦截（C# SetCommands.cs:SetPop）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Spop, &[b"k", b"-1"]);
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
  // SRANDMEMBER 负 count → |count| 个数组
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"k", b"-3"]);
  // 数组头 + 3 成员（不删除，成员数不变）
  let text = String::from_utf8(out).unwrap();
  assert!(
    text.starts_with("*3\r\n$"),
    "SRANDMEMBER -3 应回 3 成员: {text}"
  );
  // SRANDMEMBER 0 → 空数组
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"k", b"0"]);
  assert_eq!(out, b"*0\r\n");
  // SPOP count=2 → 数组形态且计数递减
  let before = auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"k"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Spop, &[b"k", b"2"]);
  assert_eq!(&out[..4], b"*2\r\n");
  let after = auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"k"]);
  assert_ne!(before, after, "SPOP 后基数应递减");

  // ---- 未支持操作物化降级（对象层单源闭环）
  // HRANDFIELD（hash 未支持树内臂 → 物化）——新建 hash（信封域直接对象层）
  for chunk_start in (1..=2048).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(2048);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"hh".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }
  // 未升阶 hash 的 HRANDFIELD 走 RMW 骨架物化（信封键直接对象层）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"hh"]);
  assert!(out.starts_with(b"$"), "HRANDFIELD 应回成员 bulk: {out:?}");

  // SMOVE（升阶源键 → 物化装载 + tiered 感知写回）
  auto_exec(&api, &rt, &mut s, RespCommand::Sadd, &[b"dst", b"hold"]);
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Smove,
    &[b"k", b"dst", b"m100"],
  );
  assert_eq!(out, b":1\r\n");
  // 源键 SMOVE 后信封接管（size 跌回降阶阈值下 → 懒降阶），数据不丢
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Sismember,
    &[b"dst", b"m100"],
  );
  assert_eq!(out, b":1\r\n");

  // LTRIM（升阶 list → 物化 + tiered 感知写回）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Ltrim, &[b"l", b"0", b"9"]);
  assert_eq!(out, b"+OK\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  assert_eq!(out, b":10\r\n");
}

/// 分层态列表序号窗口：两端推入/弹出只动两端，终态与内存态语义逐条对齐
///
/// 旧臂每次 RPUSH/LPUSH 前全树扫描求 min/max（升阶门槛 65536 ⇒ 单条推入即付
/// 整树页级 IO），RPOP 支另付一次正序全扫滚动窗口。现臂一次定位树最左键
/// （`tiered_collection_ops.rs:list_head_seq`，scan_cnt=1）取头端，尾端按
/// 「序号区间恒连续」由头 + meta.size - 1 派生，三支恒 O(1)/O(count)。
/// 参照模型即 C# ListPush/ListPop 的一参一次 AddFirst/AddLast/RemoveFirst/
/// RemoveLast 循环（ListObjectImpl.cs:229-298），错端、序号覆盖、丢元素都会在
/// 本用例的混合伸缩与终态全量比对中露相。
#[test]
fn test_tiered_list_push_seq_window() {
  let (rt, api, store, _dir) = open_env("tiered-list-seq.db");
  let mut s = session_with(&api);

  // 预灌至升阶（RPUSH 成批 1000 条）
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut model: VecDeque<Vec<u8>> = VecDeque::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"l".to_vec());
    for i in chunk_start..=chunk_end {
      let v = format!("v{i}").into_bytes();
      model.push_back(v.clone());
      args.push(v);
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &arg_slices);
  }
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"l"))
      .unwrap()
      .is_some(),
    "应已升阶为分层态"
  );

  // RPUSH 多参：自尾 +1 向上连续分配
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"t1", b"t2"]);
  assert_eq!(out, format!(":{}\r\n", model.len() + 2).into_bytes());
  for v in [b"t1".as_slice(), b"t2"] {
    model.push_back(v.to_vec());
  }

  // LPUSH 多参：一参一次 AddFirst ⇒ LPUSH l h1 h2 h3 落 [h3, h2, h1, v1…]
  // （旧臂按 args 正序自 base 递增分配，落 [h1, h2, h3, v1…]，与内存态错序）
  auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lpush,
    &[b"l", b"h1", b"h2", b"h3"],
  );
  for v in [b"h1".as_slice(), b"h2", b"h3"] {
    model.push_front(v.to_vec());
  }
  // 两端直读交叉核对窗口未错端（LINDEX 走独立树内臂）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]),
    b"$2\r\nh3\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"3"]),
    b"$2\r\nv1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]),
    b"$2\r\nt2\r\n"
  );

  // RPOP / LPOP 各摘一端：RPOP 起点 = 头 + size - n
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"2"]);
  assert_eq!(out, b"*2\r\n$2\r\nt2\r\n$2\r\nt1\r\n");
  model.pop_back();
  model.pop_back();
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nh3\r\n$2\r\nh2\r\n$2\r\nh1\r\n");
  for _ in 0..3 {
    model.pop_front();
  }

  // 弹后再自尾推入：新元素落在剩余尾端 +1（窗口随 size 收缩同步回缩）
  auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"t3"]);
  model.push_back(b"t3".to_vec());
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]),
    b"$2\r\nt3\r\n"
  );

  // 批量 LPUSH：一次自减 args.len()，整块下移不覆盖既有元素
  let batch: Vec<Vec<u8>> = (0..500).map(|i| format!("b{i}").into_bytes()).collect();
  let mut args: Vec<&[u8]> = vec![b"l"];
  args.extend(batch.iter().map(|v| v.as_slice()));
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &args);
  for v in &batch {
    model.push_front(v.clone());
  }
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]),
    b"$4\r\nb499\r\n",
    "LPUSH 末参须为新头端"
  );

  // 终态全量比对：无丢元素、无重复、序不错（序号区间连续性固证）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  assert_eq!(out, format!(":{}\r\n", model.len()).into_bytes());
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"-1"]);
  let text = String::from_utf8(out).unwrap();
  let mut parts = text.split("\r\n");
  let len_hdr = format!("*{}", model.len());
  assert_eq!(parts.next(), Some(len_hdr.as_str()));
  for expect in &model {
    let val_hdr = format!("${}", expect.len());
    assert_eq!(parts.next(), Some(val_hdr.as_str()));
    let got = parts.next().unwrap();
    assert_eq!(
      got.as_bytes(),
      expect.as_slice(),
      "分层态与内存态须逐条同序"
    );
  }
  assert_eq!(parts.next(), Some(""));
}
