//! 升阶键命令面语义对齐集成测试（对标 C# Garnet.test.collections 命令语义）
//!
//! 升阶（wcol should_promote → KeyTag::Meta 元记录 + wbftree 树）后：
//! 1. 键存活探针五命令 EXISTS/TTL/EXPIRE/PERSIST/TYPE 与统计面
//!    MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT 对升阶键回值；
//! 2. SCAN 族树内游标全量遍历（HSCAN/SSCAN/ZSCAN；COSCAN 域收口仅服务
//!    自定义对象，升阶键不再在其扫描域）；
//! 3. ZADD 全选项语义与互斥校验（含同分值分支 ±0.0 等值面位模式保真：双态
//!    对照 + 成员级 TTL 清除 + 升阶→同分值 ZADD→降阶往返全等）、ZRANGE 族树内
//!    流式臂与对象层逐字节对齐、GEO 物化装载语义；
//! 4. List LPOP count 数组形态、RPOP 尾端弹出（fDelAtHead 双分支）、
//!    LPUSH 序号不覆盖、LINDEX；分层态序号窗口两端伸缩与内存态逐条对齐；
//! 5. SPOP 负 count 拦截、SRANDMEMBER 负 count；HRANDFIELD 树内只读抽样
//!    臂双态矩阵（§8.5 SRANDMEMBER 对偶，票 zcode-r151c-smembers）；
//! 6. 未支持操作物化降级走对象层（SMOVE/LTRIM）。

use std::{
  collections::VecDeque,
  str::from_utf8,
  sync::Arc,
  thread::sleep,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wbase::{
  convert::TICKS_PER_SECOND,
  map::{HashSet, HashSetExt},
  time::now_ticks,
};
use wcol::types::{
  garnet_object::LIST_SEQ_BASE,
  member_ttl::{decode_member, encode_member},
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::{auto_exec, pump, roundtrip};
use wresp::command::RespCommand;
use wtest_base::resp_frame;
use wval::GarnetObjectType;

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

/// 序号文本（纯数字成员/分值用）
fn num(buf: &mut ItoaBuffer, i: usize) -> Vec<u8> {
  buf.format(i).as_bytes().to_vec()
}

/// 前缀字节 + 序号文本（成员命名 f1/v1/m1 同型）
fn prefixed(prefix: u8, buf: &mut ItoaBuffer, i: usize) -> Vec<u8> {
  let s = buf.format(i).as_bytes();
  let mut v = Vec::with_capacity(s.len() + 1);
  v.push(prefix);
  v.extend_from_slice(s);
  v
}

/// 升阶灌水批量写入：按 16384 一片组装 args（首元素为键）驱动 auto_exec。
/// `elem` 产出第 i 个成员的追加字节段——单段（list/set 成员）或双段
/// （hash field+value、zset score+member，段序与 RESP 参数序一致）
fn bulk_fill(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  key: &[u8],
  total: usize,
  mut elem: impl FnMut(usize, &mut ItoaBuffer) -> Vec<Vec<u8>>,
) {
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(16384) {
    let chunk_end = (chunk_start + 16383).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(key.to_vec());
    for i in chunk_start..=chunk_end {
      args.extend(elem(i, &mut buf));
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(api, rt, s, cmd, &arg_slices);
  }
}

/// 升阶键探针五命令与统计面（EXISTS/TTL/EXPIRE/PERSIST/TYPE +
/// MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT）
#[test]
fn test_tiered_key_probe_and_stats() {
  let (rt, api, store, _dir) = open_env("tiered-probe.db");
  let mut s = session_with(&api);

  // 升阶：hash 写入 65546 字段
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    b"h",
    total,
    |i, buf| vec![prefixed(b'f', buf, i), num(buf, i)],
  );

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
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Sadd,
    b"s",
    total,
    |i, buf| vec![prefixed(b'm', buf, i)],
  );

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
  let mut h_args: Vec<Vec<u8>> = Vec::with_capacity(h_total * 2 + 1);
  h_args.push(b"h".to_vec());
  for i in 1..=h_total {
    let s = buf.format(i).as_bytes();
    let mut f = Vec::with_capacity(s.len() + 1);
    f.push(b'f');
    f.extend_from_slice(s);
    h_args.push(f);
    h_args.push(s.to_vec());
  }
  let h_slices: Vec<&[u8]> = h_args.iter().map(|v| v.as_slice()).collect();
  auto_exec(&api, &rt, &mut s, RespCommand::Hset, &h_slices);
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
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    b"z",
    total,
    |i, buf| vec![num(buf, i), prefixed(b'm', buf, i)],
  );
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

  // ZRANGE WITHSCORES（分层树内流式读臂，wcol 参数解析与结果负载单源；
  // 与小集合对象层的逐字节对照见 test_tiered_zset_range_rank_parity）
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

/// 分层态 ZADD 同分值分支必须保留树内存储分值位模式（±0.0 等值面）
///
/// 对位 C# SortedSetObjectImpl.cs:SortedSetAdd（libs/server/Objects/SortedSet/
/// SortedSetObjectImpl.cs:163）`if (score == scoreStored) { _ = TryRemoveExpiration
/// (member); continue; }`：IEEE 754 下 `-0.0 == +0.0` 判真，同分值分支只清成员级
/// TTL、绝不回写本次输入分值的位模式；内存态 sorted_set_object_impl.rs:
/// sorted_set_add 同分支仅 remove_expiration、sorted_set_object.rs:add 的
/// `*old_score != score` 对 ±0.0 恒假不更新。复现链 `ZADD k -0 m` → 升阶 →
/// `ZADD k 0 m` → `ZSCORE k m`：树写若回写输入位模式则 -0.0 被固化成 +0.0，
/// 且降阶物化（f64::from_be_bytes 位级还原）把错误位带进信封，与纯内存路径
/// 逐字节分叉——本用例双态对照 + 独立硬编码锚 + 升阶→同分值 ZADD→降阶往返三面全等
#[test]
fn test_tiered_zadd_signed_zero_parity() {
  let (rt, api, store, _dir) = open_env("tiered-zadd-signed-zero.db");
  let mut s = session_with(&api);
  const KS: &[u8] = b"zs0"; // 内存态对照键（不过升阶门槛）
  const KT: &[u8] = b"zt0"; // 分层态键（bulk 升阶）
  // 独立硬编码锚（免自证）：终态三成员 (±0.0 判序相等 → 成员字典序 m < mt < zp)
  const FINAL_FRAME: &[u8] =
    b"*6\r\n$1\r\nm\r\n$2\r\n-0\r\n$2\r\nmt\r\n$2\r\n-0\r\n$2\r\nzp\r\n$1\r\n0\r\n";
  const NEG0: &[u8] = b"$2\r\n-0\r\n";
  const POS0: &[u8] = b"$1\r\n0\r\n";
  const TTL_CLEARED: &[u8] = b"*1\r\n:-1\r\n";

  // ZTTL 帧唯一整数项（*1\r\n:n\r\n）
  let ttl_int = |frame: &[u8]| -> i64 {
    let text = from_utf8(frame).expect("ZTTL 帧应为 UTF-8");
    text
      .split("\r\n")
      .find(|l| l.starts_with(':'))
      .unwrap_or_else(|| panic!("ZTTL 帧无整数项: {text:?}"))[1..]
      .parse()
      .expect("ZTTL 整数")
  };

  // ---- 内存态基线：正向 -0→0、反向 0→-0 同分值覆盖均不得动存储位模式
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"-0", b"m"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"0", b"m"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"m"]),
    NEG0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"0", b"zp"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"-0", b"zp"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"zp"]),
    POS0
  );
  // 成员级 TTL 面：同分值 ZADD 清 TTL、分值位不变（C# TryRemoveExpiration）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"-0", b"mt"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zexpire,
      &[KS, b"600", b"MEMBERS", b"1", b"mt"]
    ),
    b"*1\r\n:1\r\n"
  );
  assert!(
    ttl_int(&auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KS, b"MEMBERS", b"1", b"mt"]
    )) > 0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"0", b"mt"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"mt"]),
    NEG0
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KS, b"MEMBERS", b"1", b"mt"]
    ),
    TTL_CLEARED
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangebyscore,
      &[KS, b"-inf", b"1000", b"WITHSCORES"]
    ),
    FINAL_FRAME
  );

  // ---- 分层态键：灌水成员分值 1001..（降阶臂按分值窗剔除）+ m(-0.0)、
  // zp(+0.0)、mt(-0.0 挂远未来 TTL，预烘纯 base 页，同 range-parity 用例灌树契约)
  let fill = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let future = now_ticks() + 600 * TICKS_PER_SECOND;
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(fill + 3);
  let mut buf = ItoaBuffer::new();
  for i in 1..=fill {
    let s = buf.format(i).as_bytes();
    let mut m = Vec::with_capacity(s.len() + 1);
    m.push(b'z');
    m.extend_from_slice(s);
    ents.push((m, encode_member(&((1000 + i) as f64).to_be_bytes(), None)));
  }
  ents.push((
    b"m".to_vec(),
    encode_member(&(-0.0_f64).to_be_bytes(), None),
  ));
  ents.push((
    b"zp".to_vec(),
    encode_member(&(0.0_f64).to_be_bytes(), None),
  ));
  ents.push((
    b"mt".to_vec(),
    encode_member(&(-0.0_f64).to_be_bytes(), Some(future)),
  ));
  let next_expiry = ents
    .iter()
    .filter_map(|(_, record)| decode_member(record).0)
    .min()
    .unwrap_or(i64::MAX);
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    KT,
    GarnetObjectType::SortedSet,
    ents,
    next_expiry,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_some(),
    "zt0 应已升阶为分层态"
  );

  // 基线：树内读臂 ±0 位模式各自保真
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"m"]),
    NEG0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"zp"]),
    POS0
  );
  assert!(
    ttl_int(&auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KT, b"MEMBERS", b"1", b"mt"]
    )) > 0
  );

  // 同分值覆盖臂：与内存态逐字节同形（缺陷态分层回 "0"/"-0" 翻转即红）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KT, b"0", b"m"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"m"]),
    NEG0,
    "分层态 ZADD 同分值分支覆写了树内 -0.0 位模式（与内存态分叉）"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KT, b"-0", b"zp"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"zp"]),
    POS0,
    "分层态 ZADD 同分值分支覆写了树内 +0.0 位模式（反向分叉）"
  );
  // 成员挂 TTL：同分值 ZADD 清 TTL 且分值位不变
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KT, b"0", b"mt"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"mt"]),
    NEG0
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KT, b"MEMBERS", b"1", b"mt"]
    ),
    TTL_CLEARED
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangebyscore,
      &[KT, b"-inf", b"1000", b"WITHSCORES"]
    ),
    FINAL_FRAME,
    "分层树内流式臂 ±0 位模式窗口须与内存态全等"
  );

  // 分层源 ZRANGESTORE：物化装载面（export→对象层→信封）分值位同须保真
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangestore,
      &[b"dts", KT, b"-inf", b"1000", b"BYSCORE"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[b"dts", b"0", b"-1", b"WITHSCORES"]
    ),
    FINAL_FRAME
  );

  // ---- 降阶往返：按分值窗删尽灌水成员 → size 跌回降阶阈值下懒降阶，
  // 信封承接的位模式必须与纯内存路径全等（覆盖会把错误位固化进信封即红）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zremrangebyscore,
      &[KT, b"1000", b"+inf"]
    ),
    format!(":{fill}\r\n").into_bytes()
  );
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_none(),
    "删空灌水段后 zt0 应懒降阶回信封态"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"m"]),
    NEG0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"zp"]),
    POS0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"mt"]),
    NEG0
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[KT, b"0", b"-1", b"WITHSCORES"]
    ),
    FINAL_FRAME
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KT, b"MEMBERS", b"1", b"mt"]
    ),
    TTL_CLEARED
  );
}

/// deviations §151 数据段奇数尾巴分层树态锁：首 token 即分值形「ZADD k 1 m 5」
/// 升阶树内臂防御截断——应答 :1、树内基数恰 +1（垃圾成员零落）、会话存活、
/// 错误帧零输出（C# 此形对象层主循环 GetArgSliceByRef 越界读 UB：debug 断言
/// 掐连 / release 垃圾成员写入；真 Redis 该形报 syntax error，rust 截断系防御
/// 容忍非 Redis 对齐形，严禁按 C# 形回改为越界读或 panic）；合法单对 :1 对照，
/// 与内存信封锁 zadd_odd_tail_token_truncated_defensive 双态同形。
///
/// 全拍经真 RESP 线协议往返驱动（与信封锁 roundtrip 同一套机制，禁第二套
/// 探针）：截断拍入参帧由解析器整帧消费，截断臂只裁参数窗、不触会话输入
/// 游标；PING 系 @fast 会话侧命令、不入存储 exec 分派表，存活拍必须走线
/// 协议（经 auto_exec 漏斗恒落 unknown command 兜底臂，非分层臂缺陷）
#[test]
fn test_tiered_zadd_odd_tail_token_truncated() {
  let (rt, api, store, _dir) = open_env("tiered-zadd-odd-tail.db");
  const KT: &[u8] = b"ztodd";

  // 小集合预烘升阶（三成员形，§138 同制），隔离基数判垃圾成员零落
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    KT,
    GarnetObjectType::SortedSet,
    vec![
      (b"alpha".to_vec(), encode_member(&0f64.to_be_bytes(), None)),
      (b"beta".to_vec(), encode_member(&1f64.to_be_bytes(), None)),
      (b"gamma".to_vec(), encode_member(&2f64.to_be_bytes(), None)),
    ],
    i64::MAX,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_some(),
    "ztodd 应已升阶为分层态"
  );
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);

  // 奇数尾巴：解析扫描尾轮 curr==args.len() 截断 + 主循环 args.get(curr) else
  // 截断——应答恰 :1 一帧（错误帧零输出），尾巴分值 5 丢弃
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", KT, b"1", b"m", b"5"]),
    b":1\r\n"
  );

  // 树内基数恰 3→4（垃圾成员零落）＋新成员分值原样 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZCARD", KT]),
    b":4\r\n",
    "截断后首拍 ZCARD 错位即残字节滞留"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZSCORE", KT, b"m"]),
    b"$1\r\n1\r\n"
  );

  // 对照：合法单对完整 :1、基数 4→5
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", KT, b"2", b"n"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", KT]), b":5\r\n");

  // 残字节探针：PING+ZCARD 流水线一投（截断拍后连续两拍常规命令），应答恰
  // 两帧、输入缓冲零残余——残 token 滞留必致本拍起帧粘连错位
  let mut frame = resp_frame(&[b"PING"]);
  frame.extend_from_slice(&resp_frame(&[b"ZCARD", KT]));
  let (remaining, mut out) = pump(&mut c, &frame);
  if let Some(slow) = c.take_slow_wait() {
    out.extend(rt.block_on(slow.resolve()));
  }
  assert_eq!(out, b"+PONG\r\n:5\r\n");
  assert_eq!(remaining, Some(0), "流水线双拍后输入缓冲应零残余");

  // 截断后会话存活（与信封锁末拍 Ping 同断言）
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}

/// 分层态 ZINCRBY 缺席/到期新增臂必须直存增量原值（±0.0 词形面）
///
/// 契约对标 C# SortedSetObjectImpl.cs:SortedSetIncrement 缺席臂
/// `sortedSetDict.Add(member, incrValue)` 直存原值（不做任何基底加法）、信封
/// sorted_set_object_impl.rs:sorted_set_increment None 臂同形直插 incr_value 与
/// 本册 ZADD ±0 锁（test_tiered_zadd_signed_zero_parity）的 ZADD 新增直存正解形。
/// 分层 Zincrby 臂此前不分臂恒以 0.0 基底折叠增量——IEEE 754 就近舍入
/// 0.0 + (-0.0) = +0.0 抹洗符号，`ZINCRBY k -0 新成员` 内存态回
/// "$2\r\n-0\r\n" 而分层态回 "$1\r\n0\r\n"，双态应答帧与存储位模式逐字节分叉，
/// 并随降阶物化扩散进信封。四面锁：3a 真缺席 -0 建员双态硬编码锚全等；
/// 3b 到期在树形（信封侧 ZPEXPIRE 恰到期先摘后新增、分层侧预烘到期记录物理
/// 覆盖零计数，夹具沿 tiered_field_ttl.rs 到期在树形制）双态 "-0" 且 ZCARD
/// 不虚增、重插成员 ZTTL 归 -1 不携旧刻度；3c 反向防回摆——存活成员 -0.0 加
/// 0 双态折叠回 "0"（-0.0 + 0.0 = +0.0 为 IEEE 双侧一致面，禁误改成直存）；
/// 3d 降阶往返位模式逐字节存续（三面全等锁形照抄 ZADD ±0 模板）
#[test]
fn test_tiered_zincrby_signed_zero_absent_dualstate_parity() {
  let (rt, api, store, _dir) = open_env("tiered-zincrby-signed-zero.db");
  let mut s = session_with(&api);
  const KS: &[u8] = b"zi0"; // 内存态对照键（不过升阶门槛）
  const KT: &[u8] = b"zi0t"; // 分层态键（预烘纯 base 页升阶）
  // 独立硬编码锚（免自证）：RESP2 write_double_numeric 经 format_double 对
  // -0.0 敏感产 "-0"（zmij 无 ".0" 可剥），双态必须逐字节全等
  const NEG0: &[u8] = b"$2\r\n-0\r\n";
  const POS0: &[u8] = b"$1\r\n0\r\n";
  const TTL_CLEARED: &[u8] = b"*1\r\n:-1\r\n";

  // ---- 3a 内存态基线：真缺席 -0 建员直存增量原值（信封缺席臂锚）
  let out_mem_new = auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KS, b"-0", b"bn"]);
  assert_eq!(
    out_mem_new, NEG0,
    "内存态 ZINCRBY -0 建员应答须逐字节为 -0 原值"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"bn"]),
    NEG0,
    "内存态建员后 ZSCORE 读回 \"-0\""
  );

  // ---- 3b 内存态：§66 到期守卫恰到期先摘后落新增臂，仍直存原值
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"5", b"em"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zpexpire,
      &[KS, b"50", b"MEMBERS", b"1", b"em"]
    ),
    b"*1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(80));
  let out_mem_exp = auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KS, b"-0", b"em"]);
  assert_eq!(
    out_mem_exp, NEG0,
    "内存态恰到期成员 ZINCRBY -0 应先摘后直存原值"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"em"]),
    NEG0
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KS, b"MEMBERS", b"1", b"em"]
    ),
    TTL_CLEARED,
    "到期重插成员不得携带旧 TTL"
  );

  // ---- 3c 内存态：存活臂折叠保留（防回摆——禁把存活分支也改成直存增量）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &[KS, b"-0", b"am"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KS, b"0", b"am"]),
    POS0,
    "存活成员 -0.0 上加 0 应折叠为 +0.0（IEEE 双侧一致面）"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"am"]),
    POS0
  );
  // 计数基线：bn/em/am 三成员，先摘后加净零不虚增
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]),
    b":3\r\n",
    "内存态缺席/到期两径 ZINCRBY 后成员数恒 3"
  );

  // ---- 分层态键：灌水成员分值 1001..（降阶臂按分值窗剔除）+ em 到期在树
  //（5.0 挂已过期 TTL，tiered_field_ttl.rs 到期在树预烘形制）+ am(-0.0 存活)
  let fill = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let stale = now_ticks() - TICKS_PER_SECOND;
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(fill + 2);
  let mut buf = ItoaBuffer::new();
  for i in 1..=fill {
    let m = prefixed(b'z', &mut buf, i);
    ents.push((m, encode_member(&((1000 + i) as f64).to_be_bytes(), None)));
  }
  ents.push((
    b"em".to_vec(),
    encode_member(&5.0_f64.to_be_bytes(), Some(stale)),
  ));
  ents.push((
    b"am".to_vec(),
    encode_member(&(-0.0_f64).to_be_bytes(), None),
  ));
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    KT,
    GarnetObjectType::SortedSet,
    ents,
    stale,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_some(),
    "zi0t 应已升阶为分层态"
  );

  // 3a 分层态：真缺席 -0 建员（修复态直存回 NEG0；缺陷态 0.0 基底折叠回
  // POS0 即红，与内存态分叉同红）
  let out_tier_new = auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KT, b"-0", b"bn"]);
  assert_eq!(
    out_tier_new, out_mem_new,
    "分层/内存态缺席 -0 建员应答逐字节分叉"
  );
  assert_eq!(
    out_tier_new, NEG0,
    "分层态 ZINCRBY -0 建员被 0.0 基底折叠抹洗符号"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"bn"]),
    NEG0
  );

  // 3b 分层态：到期在树命中视同缺席直存原值、物理覆盖零计数
  let out_tier_exp = auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KT, b"-0", b"em"]);
  assert_eq!(
    out_tier_exp, out_mem_exp,
    "分层/内存态到期在树 -0 应答逐字节分叉"
  );
  assert_eq!(out_tier_exp, NEG0, "分层态到期命中被 0.0 基底折叠抹洗符号");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"em"]),
    NEG0
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zttl,
      &[KT, b"MEMBERS", b"1", b"em"]
    ),
    TTL_CLEARED,
    "分层态到期重插成员不得携带旧 TTL"
  );
  // 绝对计数（沿 tiered_field_ttl 形制：覆盖承接零计数，虚增即 fill+4 红）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]),
    format!(":{}\r\n", fill + 3).into_bytes(),
    "分层态到期命中零计数 + 真缺席 +1，ZCARD 恒 fill+3 不得虚增"
  );

  // 3c 分层态：存活臂折叠与内存态同形
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zincrby, &[KT, b"0", b"am"]),
    POS0,
    "分层态存活成员 -0.0 上加 0 须与内存态折叠同回 \"0\""
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"am"]),
    POS0
  );

  // ---- 3d 降阶往返：删尽灌水段 size 跌回降阶阈值下懒降阶，信封承接的位
  // 模式必须与纯内存路径全等（树 payload 折叠成 +0.0 即此面红）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zremrangebyscore,
      &[KT, b"1000", b"+inf"]
    ),
    format!(":{fill}\r\n").into_bytes()
  );
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_none(),
    "删空灌水段后 zi0t 应懒降阶回信封态"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"bn"]),
    NEG0,
    "降阶往返后 -0.0 位模式必须自树内逐字节存续"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"em"]),
    NEG0
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"am"]),
    POS0
  );
}

/// 分层态与内存态 ZINCRBY / ZADD INCR 产生 NaN 拒绝门对齐（±inf 相消对拍）
///
/// 契约对标 C# SortedSetObjectImpl.cs:SortedSetIncrement / SortedSetAdd INCR：
/// IEEE 754 下 +inf + (-inf) 以及 -inf + (+inf) 产生 NaN，内存态与分层态必须
/// 均拦截并返回逐字节一致的 -ERR resulting score is not a number (NaN)，
/// 且树内分值不变、ZCARD 不变，不得持久化入树或推进元记录与镜像。
#[test]
fn test_tiered_zincrby_nan_gate_dualstate_parity() {
  let (rt, api, store, _dir) = open_env("tiered-zincrby-nan.db");
  let mut s = session_with(&api);
  const KS: &[u8] = b"zs_nan";
  const KT: &[u8] = b"zt_nan";
  const ERR_NAN: &[u8] = b"-ERR resulting score is not a number (NaN)\r\n";
  const POS_INF: &[u8] = b"$3\r\ninf\r\n";
  const NEG_INF: &[u8] = b"$4\r\n-inf\r\n";

  // ---- 分层态键升阶灌水（纯 base 页）
  let fill = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(fill + 2);
  let mut buf = ItoaBuffer::new();
  for i in 1..=fill {
    let s = buf.format(i).as_bytes();
    let mut m = Vec::with_capacity(s.len() + 1);
    m.push(b'z');
    m.extend_from_slice(s);
    ents.push((m, encode_member(&((1000 + i) as f64).to_be_bytes(), None)));
  }
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    KT,
    GarnetObjectType::SortedSet,
    ents,
    i64::MAX,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_some(),
    "zt_nan 应已升阶为分层态"
  );

  // ---- 1. +inf 成员 × -inf 增量：ZADD +inf 成员
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[KS, b"+inf", b"pinf"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[KT, b"+inf", b"pinf"]
    ),
    b":1\r\n"
  );
  let card_ks_pos = auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]);
  let card_kt_pos = auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]);
  assert_eq!(card_ks_pos, b":1\r\n");
  assert_eq!(card_kt_pos, format!(":{}\r\n", fill + 1).into_bytes());
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"pinf"]),
    POS_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"pinf"]),
    POS_INF
  );

  // ZINCRBY -inf pinf：双态对拍
  let out_ks = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zincrby,
    &[KS, b"-inf", b"pinf"],
  );
  let out_kt = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zincrby,
    &[KT, b"-inf", b"pinf"],
  );
  assert_eq!(out_ks, ERR_NAN);
  assert_eq!(out_kt, ERR_NAN);
  assert_eq!(out_ks, out_kt);

  // 断言分值不变、ZCARD 不变
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"pinf"]),
    POS_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"pinf"]),
    POS_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]),
    card_ks_pos
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]),
    card_kt_pos
  );

  // 补 ZADD INCR 形态同锚防臂序回摆
  let out_ks = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[KS, b"INCR", b"-inf", b"pinf"],
  );
  let out_kt = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[KT, b"INCR", b"-inf", b"pinf"],
  );
  assert_eq!(out_ks, ERR_NAN);
  assert_eq!(out_kt, ERR_NAN);
  assert_eq!(out_ks, out_kt);

  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"pinf"]),
    POS_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"pinf"]),
    POS_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]),
    card_ks_pos
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]),
    card_kt_pos
  );

  // ---- 2. -inf 成员 × +inf 增量对照组：ZADD -inf 成员
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[KS, b"-inf", b"ninf"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[KT, b"-inf", b"ninf"]
    ),
    b":1\r\n"
  );
  let card_ks_neg = auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]);
  let card_kt_neg = auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]);
  assert_eq!(card_ks_neg, b":2\r\n");
  assert_eq!(card_kt_neg, format!(":{}\r\n", fill + 2).into_bytes());
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"ninf"]),
    NEG_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"ninf"]),
    NEG_INF
  );

  // ZINCRBY +inf ninf：双态对拍
  let out_ks = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zincrby,
    &[KS, b"+inf", b"ninf"],
  );
  let out_kt = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zincrby,
    &[KT, b"+inf", b"ninf"],
  );
  assert_eq!(out_ks, ERR_NAN);
  assert_eq!(out_kt, ERR_NAN);
  assert_eq!(out_ks, out_kt);

  // 断言分值不变、ZCARD 不变
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"ninf"]),
    NEG_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"ninf"]),
    NEG_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]),
    card_ks_neg
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]),
    card_kt_neg
  );

  // 补 ZADD INCR 形态对照组
  let out_ks = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[KS, b"INCR", b"+inf", b"ninf"],
  );
  let out_kt = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zadd,
    &[KT, b"INCR", b"+inf", b"ninf"],
  );
  assert_eq!(out_ks, ERR_NAN);
  assert_eq!(out_kt, ERR_NAN);
  assert_eq!(out_ks, out_kt);

  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KS, b"ninf"]),
    NEG_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[KT, b"ninf"]),
    NEG_INF
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KS]),
    card_ks_neg
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[KT]),
    card_kt_neg
  );
}

/// 分层 zset 范围/排名族树内流式读臂与内存态对象层逐字节对齐
///
/// 本票（task/done/my-tiered-zset-range-materialize.md）验收面：ZRANGE 六形
/// （byIndex / byScore / byLex × REV / WITHSCORES / LIMIT）、ZCOUNT、ZLEXCOUNT、
/// ZRANK / ZREVRANK 由「穿透全量物化」改为树内流式求值（`zset_scan_select`），
/// 内存只随窗口增长。树内无分值序索引，故「序」在扫描侧由 wcol
/// `SortedSetComparer` 单源重建——本用例把同一成员集灌进内存态键 `zs`（15 成员，
/// 走对象层）与分层态键 `zt`（同 15 成员 + 65546 灌水成员，走树内臂），逐条断言
/// 两态应答字节全等，另附若干硬编码期望（免自证）。
///
/// 灌水成员 `z{i}` 分值 `1000+i`：在「(分值,成员)」序与成员字典序上均排在
/// 全部兴趣成员之后，故小集合的 byScore/byLex/名次窗口在大集合上同参可平移对照，
/// byIndex 按 `大 = 小 + 灌水数` 正向平移、REV 按 `大 = 灌水数 + 小末位 .. 灌水数
/// + 小首位` 平移。
#[test]
fn test_tiered_zset_range_rank_parity() {
  // 兴趣成员（已按 SortedSetComparer 升序列出，索引 0..14）
  const MEM: &[(&str, f64)] = &[
    ("l", -100.0),
    ("f", -1.0),
    ("e", 0.0),
    ("g", 0.5),
    ("a", 1.0),
    ("b", 1.0),
    ("c", 2.0),
    ("d", 2.0),
    ("o", 2.0),
    ("j", 2.5),
    ("h", 3.0),
    ("i", 3.0),
    ("m", 7.0),
    ("n", 7.0),
    ("k", 100.0),
  ];
  // 字典断点两侧取样（ZREVRANK 平移不变量用）
  const SPOT: &[&[u8]] = &[b"l", b"e", b"c", b"k"];
  const KS: &[u8] = b"zs";
  const KT: &[u8] = b"zt";
  // 硬编码锚点：ZRANGEBYLEX [a [m 的成员列（断点 (2,"o") 前 8 条）
  const LEX_ANCHOR: &[u8] =
    b"*8\r\n$1\r\nl\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\ng\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";

  let (rt, api, store, _dir) = open_env("tiered-zrange-parity.db");
  let mut s = session_with(&api);
  // 双键同参/平移参对照（小集合 = 内存态对象层基准）
  macro_rules! ck {
    ($cmd:expr, $small:expr, $big:expr, $label:literal) => {
      check_range_parity(&api, &rt, &mut s, $cmd, $small, $big, $label)
    };
  }

  // ---- 分层态键 zt：一次性 bulk 升阶灌入 fill + 兴趣成员（纯 base 页，
  // 不经 chunked ZADD 的门槛穿越，避免升阶后追加成员遗留 mini-page 增量）
  let fill = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let encode = |member: &[u8], score: f64, expired: bool| {
    let expiry = if expired { Some(0_i64) } else { None };
    (member.to_vec(), encode_member(&score.to_be_bytes(), expiry))
  };
  // 兴趣成员分值表 → 入树条目（`expired_member` 命中的成员预烘即时到期刻度）
  let build = |expired_member: Option<&[u8]>| {
    let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(fill + MEM.len());
    let mut buf = ItoaBuffer::new();
    for i in 1..=fill {
      let s = buf.format(i).as_bytes();
      let mut m = Vec::with_capacity(s.len() + 1);
      m.push(b'z');
      m.extend_from_slice(s);
      ents.push(encode(&m, (1000 + i) as f64, false));
    }
    for (member, score) in MEM {
      let expired = expired_member == Some(member.as_bytes());
      ents.push(encode(member.as_bytes(), *score, expired));
    }
    ents
  };
  let promote = |key: &[u8], ents: Vec<(Vec<u8>, Vec<u8>)>| {
    // 水位与灌入批同源（对位生产侧 earliest_expiry 单点：无挂 TTL 成员回 MAX，
    // 挂到期成员的批必须把水位带进元记录，否则计数快路径被假水位骗过）
    let next_expiry = ents
      .iter()
      .filter_map(|(_, record)| decode_member(record).0)
      .min()
      .unwrap_or(i64::MAX);
    let sess = store.new_session().unwrap();
    rt.block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::SortedSet,
      ents,
      next_expiry,
      // 一次性首升阶（键尚无旧树）：replace=false 保留 IndexExists 去重门
      false,
    ))
    .unwrap();
  };
  promote(KT, build(None));
  // ---- 兴趣成员灌入内存态键 zs（不过升阶门槛 → 对象层单源，对照基准）
  let mut pairs: Vec<Vec<u8>> = Vec::with_capacity(MEM.len() * 2);
  for (member, score) in MEM {
    pairs.push(format!("{score}").into_bytes());
    pairs.push(member.as_bytes().to_vec());
  }
  {
    let mut args: Vec<&[u8]> = vec![KS];
    args.extend(pairs.iter().map(|v| v.as_slice()));
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &args);
  }

  // ---- 两态定位：zt 已升阶、zs 仍在内存态；元记录计数 = 全量成员
  let size_of = |key: &[u8]| -> u64 {
    let sess = store.new_session().unwrap();
    let (meta, _) = rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .unwrap_or_else(|| panic!("{} 无元记录存根", from_utf8(key).unwrap()));
    meta.size
  };
  assert_eq!(size_of(KT), (fill + MEM.len()) as u64, "zt 应已升阶");
  {
    let sess = store.new_session().unwrap();
    assert!(
      rt.block_on(sess.load_collection_stub(KS))
        .unwrap()
        .is_none(),
      "zs 应保持内存态（对照基准）"
    );
  }

  // ============================ 硬编码锚点（RESP2） ============================
  // 字典序断点（本票最易错面）：C# `GetElementsInRangeByLex` 是 take_while 真
  // break，越过上界的序最小条目 (2,"o") 即断点 → h,i,j,k,m,n 虽落在 [a,[m 的
  // 成员窗内亦不得出现（朴素成员过滤会错列 12 条）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[m"]
    ),
    LEX_ANCHOR,
    "内存态 ZRANGEBYLEX [a [m 基准"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangebylex,
      &[KT, b"[a", b"[m"]
    ),
    LEX_ANCHOR,
    "分层态 ZRANGEBYLEX [a [m 断点裁剪"
  );
  // 名次/计数面
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zrank, &[KT, b"k"]),
    b":14\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcount, &[KT, b"2", b"2"]),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zlexcount,
      &[KT, b"[a", b"[z"]
    ),
    b":15\r\n"
  );
  // 全量 byIndex：首条兴趣成员、末条灌水成员（应答规模 = 键规模，形态自证）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Zrange, &[KT, b"0", b"-1"]);
  assert!(
    out.starts_with(format!("*{}\r\n$1\r\nl\r\n", fill + MEM.len()).as_bytes()),
    "全量窗口首条应为序最小成员: {:?}",
    &out[..32.min(out.len())]
  );
  assert!(
    out.ends_with(b"$6\r\nz65546\r\n"),
    "全量窗口末条应为最大灌水成员: {:?}",
    &out[out.len().saturating_sub(32)..]
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zrange, &[KS, b"0", b"-1"]),
    flat_frame(MEM.iter().map(|(m, _)| *m)),
    "小集合全量帧（独立构造，非自证）"
  );
  // 空窗：min 越过末位（小集合 15 越界 ↔ 大集合 65561 越界）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zrange, &[KS, b"15", b"20"]),
    b"*0\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[KT, b"65561", b"65566"]
    ),
    b"*0\r\n"
  );

  // ======================= 双协议全对照电池（RESP2 / RESP3） =======================
  for ver in [2u8, 3u8] {
    s.resp_protocol_version = ver;

    // ---- byIndex（同参对照：兴趣成员恒排在灌水成员之前）
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"14"],
      &[KT, b"0", b"14"],
      "ZRANGE 0 14"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"2", b"6", b"WITHSCORES"],
      &[KT, b"2", b"6", b"WITHSCORES"],
      "ZRANGE 2 6 WITHSCORES"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"5", b"5"],
      &[KT, b"5", b"5"],
      "ZRANGE 5 5"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"3", b"1"],
      &[KT, b"3", b"1"],
      "ZRANGE 3 1 (min>max)"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"15", b"20"],
      &[KT, b"65561", b"65566"],
      "ZRANGE min 越过末位"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"-15", b"-1"],
      &[KT, b"-65561", b"-65547"],
      "ZRANGE 负索引归一（全量）"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"-5", b"-1", b"WITHSCORES"],
      &[KT, b"-65551", b"-65547", b"WITHSCORES"],
      "ZRANGE 负索引归一（尾段）"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"-20", b"-16"],
      &[KT, b"-65566", b"-65562"],
      "ZRANGE 归一后两端仍负 → 空"
    );
    ck!(
      RespCommand::Zrevrange,
      &[KS, b"0", b"14"],
      &[KT, b"65546", b"65560"],
      "ZREVRANGE 0 14"
    );
    ck!(
      RespCommand::Zrevrange,
      &[KS, b"0", b"4", b"WITHSCORES"],
      &[KT, b"65546", b"65550", b"WITHSCORES"],
      "ZREVRANGE 0 4 WITHSCORES"
    );
    ck!(
      RespCommand::Zrevrange,
      &[KS, b"6", b"2"],
      &[KT, b"6", b"2"],
      "ZREVRANGE min>max"
    );
    // byIndex + LIMIT → 对象层同款拒否（分层臂须同错误行，非静默忽略）
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"1", b"LIMIT", b"0", b"5"],
      &[KT, b"0", b"1", b"LIMIT", b"0", b"5"],
      "ZRANGE BYINDEX+LIMIT → not supported"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"1", b"LIMIT", b"1"],
      &[KT, b"0", b"1", b"LIMIT", b"1"],
      "ZRANGE LIMIT 缺 count"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"1", b"LIMIT", b"abc", b"1"],
      &[KT, b"0", b"1", b"LIMIT", b"abc", b"1"],
      "ZRANGE LIMIT 非整数"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"1", b"BYSCORE", b"WITHSCORES"],
      &[KT, b"0", b"1", b"BYSCORE", b"WITHSCORES"],
      "ZRANGE 0 1 BYSCORE WITHSCORES"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"abc", b"1"],
      &[KT, b"abc", b"1"],
      "ZRANGE 非浮点界"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"1", b"BYLEX"],
      &[KT, b"0", b"1", b"BYLEX"],
      "ZRANGE 区间块 1 产出后块 2 解析失败 → 复位改写"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"(", b"1"],
      &[KT, b"(", b"1"],
      "ZRANGE 裸独占前缀"
    );

    // ---- byScore
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"1", b"3"],
      &[KT, b"1", b"3"],
      "ZRANGEBYSCORE 1 3"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"(1", b"(3"],
      &[KT, b"(1", b"(3"],
      "ZRANGEBYSCORE (1 (3"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"-inf", b"100", b"WITHSCORES"],
      &[KT, b"-inf", b"100", b"WITHSCORES"],
      "ZRANGEBYSCORE -inf 100 WITHSCORES"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"-inf", b"100", b"LIMIT", b"2", b"3"],
      &[KT, b"-inf", b"100", b"LIMIT", b"2", b"3"],
      "ZRANGEBYSCORE LIMIT 2 3"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"-inf", b"100", b"LIMIT", b"0", b"-1"],
      &[KT, b"-inf", b"100", b"LIMIT", b"0", b"-1"],
      "ZRANGEBYSCORE LIMIT 0 -1（取到末尾）"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"-inf", b"100", b"LIMIT", b"-1", b"2"],
      &[KT, b"-inf", b"100", b"LIMIT", b"-1", b"2"],
      "ZRANGEBYSCORE LIMIT 负 offset → 空"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"WITHSCORES", b"100", b"100"],
      &[KT, b"WITHSCORES", b"100", b"100"],
      "ZRANGEBYSCORE 界位被词元占位"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"5", b"6"],
      &[KT, b"5", b"6"],
      "ZRANGEBYSCORE 空带"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"100", b"1000"],
      &[KT, b"100", b"1000"],
      "ZRANGEBYSCORE 100 1000（灌水带外）"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"nan", b"100"],
      &[KT, b"nan", b"100"],
      "ZRANGEBYSCORE NaN 界"
    );
    ck!(
      RespCommand::Zrevrangebyscore,
      &[KS, b"100", b"-inf"],
      &[KT, b"100", b"-inf"],
      "ZREVRANGEBYSCORE 100 -inf"
    );
    ck!(
      RespCommand::Zrevrangebyscore,
      &[KS, b"3", b"1", b"WITHSCORES"],
      &[KT, b"3", b"1", b"WITHSCORES"],
      "ZREVRANGEBYSCORE 3 1 WITHSCORES"
    );
    ck!(
      RespCommand::Zrevrangebyscore,
      &[KS, b"100", b"-inf", b"LIMIT", b"1", b"3"],
      &[KT, b"100", b"-inf", b"LIMIT", b"1", b"3"],
      "ZREVRANGEBYSCORE REV LIMIT 1 3"
    );
    // LIMIT 折位早退（案一）：rev×count 0 契约恒空，对位 C# ByScore :1095-1099
    // 早退臂——分层树内臂不得跑空扫（损坏载荷面锁测见
    // test_tiered_zset_limit_early_exit_with_corrupt_payload）
    ck!(
      RespCommand::Zrevrangebyscore,
      &[KS, b"100", b"-inf", b"LIMIT", b"0", b"0"],
      &[KT, b"100", b"-inf", b"LIMIT", b"0", b"0"],
      "ZREVRANGEBYSCORE 100 -inf LIMIT 0 0（折位恒空早退）"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"3", b"1", b"BYSCORE", b"REV", b"LIMIT", b"1", b"2"],
      &[KT, b"3", b"1", b"BYSCORE", b"REV", b"LIMIT", b"1", b"2"],
      "ZRANGE 3 1 BYSCORE REV LIMIT 1 2"
    );
    // BYSCORE + BYLEX 并置：C# 写两份回复（块 1 分值窗、块 2 字典窗）
    ck!(
      RespCommand::Zrange,
      &[KS, b"(1", b"(3", b"BYSCORE", b"BYLEX"],
      &[KT, b"(1", b"(3", b"BYSCORE", b"BYLEX"],
      "ZRANGE BYSCORE+BYLEX 双回复"
    );

    // ---- byLex
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[m"],
      &[KT, b"[a", b"[m"],
      "ZRANGEBYLEX [a [m"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[m", b"WITHSCORES"],
      &[KT, b"[a", b"[m", b"WITHSCORES"],
      "ZRANGEBYLEX [a [m WITHSCORES"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[b", b"[n"],
      &[KT, b"[b", b"[n"],
      "ZRANGEBYLEX [b [n"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"-", b"[n"],
      &[KT, b"-", b"[n"],
      "ZRANGEBYLEX - [n"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[z", b"LIMIT", b"3", b"4"],
      &[KT, b"[a", b"[z", b"LIMIT", b"3", b"4"],
      "ZRANGEBYLEX [a [z LIMIT 3 4"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[z", b"LIMIT", b"0", b"0"],
      &[KT, b"[a", b"[z", b"LIMIT", b"0", b"0"],
      "ZRANGEBYLEX LIMIT count 0"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[z", b"[a"],
      &[KT, b"[z", b"[a"],
      "ZRANGEBYLEX 逆窗口"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"+", b"-"],
      &[KT, b"+", b"-"],
      "ZRANGEBYLEX + - 恒空"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"abc", b"[m"],
      &[KT, b"abc", b"[m"],
      "ZRANGEBYLEX 非法字典界"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[m", b"[a"],
      &[KT, b"[m", b"[a"],
      "ZREVRANGEBYLEX [m [a"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[z", b"[a", b"LIMIT", b"2", b"3"],
      &[KT, b"[z", b"[a", b"LIMIT", b"2", b"3"],
      "ZREVRANGEBYLEX [z [a LIMIT 2 3"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[a", b"[z"],
      &[KT, b"[a", b"[z"],
      "ZREVRANGEBYLEX [a [z（逆序窗口空）"
    );
    // ---- rev 镜像语义锁（案二）：换序即翻答案——恒空镜像序锁、
    // exclusive×无穷忽略锁（单字符界形在解析期折 InfiniteMin 后跨三换，
    // 无穷臂忽略 exclusive 位）。正向 "+ -" 恒空已锁（ZRANGEBYLEX + -），
    // 此处补反向 "- +"（交换后仍恒空）与 "+ -"（交换后为全量倒序非空）
    // 镜像两形，「先翻转后裁界」序一旦被改回「先判后换」即时以空/非空翻转暴露
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"-", b"+"],
      &[KT, b"-", b"+"],
      "ZREVRANGEBYLEX - +（镜像恒空）"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"+", b"-"],
      &[KT, b"[y", b"-"],
      "ZREVRANGEBYLEX + -（全量倒序非空；分层态以 [y 裁去灌水 z 尾段对照）"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"(-", b"(+"],
      &[KT, b"(-", b"(+"],
      "ZREVRANGEBYLEX (- (+（exclusive 跨换、无穷臂忽略独占位）"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[m", b"(+"],
      &[KT, b"[m", b"(+"],
      "ZREVRANGEBYLEX [m (+（max 折 InfiniteMin → 恒空）"
    );
    // ---- byIndex 反向三窄形锁（案二）：负索引归一、双越界早退空、
    // `0 -1` 快速路径与归一路径逐字节同构
    ck!(
      RespCommand::Zrevrange,
      &[KS, b"-3", b"-1", b"WITHSCORES"],
      &[KT, b"65558", b"65560", b"WITHSCORES"],
      "ZREVRANGE -3 -1 WITHSCORES（负索引归一）"
    );
    ck!(
      RespCommand::Zrevrange,
      &[KS, b"100", b"200"],
      &[KT, b"65661", b"65761"],
      "ZREVRANGE 双越界早退空"
    );
    ck!(
      RespCommand::Zrevrange,
      &[KT, b"0", b"-1", b"WITHSCORES"],
      &[KT, b"0", b"65560", b"WITHSCORES"],
      "ZREVRANGE 0 -1 快速路径与归一路径同构"
    );
    // ---- LIMIT 折位早退（案一）：rev 形负 offset 契约恒空，对位 C#
    // GetElementsInRangeByLex :1004-1010 与内存信封臂 :1294-1299；恒空面
    // 另见既有「ZRANGEBYLEX LIMIT count 0」正形锁与下方损坏载荷锁测
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[z", b"[a", b"LIMIT", b"-1", b"2"],
      &[KT, b"[z", b"[a", b"LIMIT", b"-1", b"2"],
      "ZREVRANGEBYLEX [z [a LIMIT -1 2（折位恒空早退）"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[a", b"[z", b"LIMIT", b"0", b"0"],
      &[KT, b"[a", b"[z", b"LIMIT", b"0", b"0"],
      "ZREVRANGEBYLEX [a [z LIMIT 0 0（rev 向折位恒空早退，正形已有既锁）"
    );
    ck!(
      RespCommand::Zrange,
      &[KS, b"[a", b"[m", b"BYLEX", b"REV"],
      &[KT, b"[a", b"[m", b"BYLEX", b"REV"],
      "ZRANGE [a [m BYLEX REV"
    );
    ck!(
      RespCommand::Zlexcount,
      &[KS, b"[a", b"[m"],
      &[KT, b"[a", b"[m"],
      "ZLEXCOUNT [a [m"
    );
    ck!(
      RespCommand::Zlexcount,
      &[KS, b"[a", b"[z"],
      &[KT, b"[a", b"[z"],
      "ZLEXCOUNT [a [z"
    );
    ck!(
      RespCommand::Zlexcount,
      &[KS, b"[n", b"[b"],
      &[KT, b"[n", b"[b"],
      "ZLEXCOUNT 逆窗口"
    );
    ck!(
      RespCommand::Zlexcount,
      &[KS, b"abc", b"[m"],
      &[KT, b"abc", b"[m"],
      "ZLEXCOUNT 非法字典界"
    );

    // ---- 计数与名次
    ck!(
      RespCommand::Zcount,
      &[KS, b"1", b"3"],
      &[KT, b"1", b"3"],
      "ZCOUNT 1 3"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"(1", b"(3"],
      &[KT, b"(1", b"(3"],
      "ZCOUNT (1 (3"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"-inf", b"100"],
      &[KT, b"-inf", b"100"],
      "ZCOUNT -inf 100"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"2", b"2"],
      &[KT, b"2", b"2"],
      "ZCOUNT 2 2"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"0", b"0"],
      &[KT, b"0", b"0"],
      "ZCOUNT 0 0"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"1000", b"1000"],
      &[KT, b"1000", b"1000"],
      "ZCOUNT 1000 1000（灌水带隙）"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"100", b"-inf"],
      &[KT, b"100", b"-inf"],
      "ZCOUNT min>max"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"nan", b"100"],
      &[KT, b"nan", b"100"],
      "ZCOUNT NaN 界（C# 外层守卫面）"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"abc", b"1"],
      &[KT, b"abc", b"1"],
      "ZCOUNT 非浮点界"
    );
    for (member, _) in MEM {
      let m = member.as_bytes();
      ck!(RespCommand::Zrank, &[KS, m], &[KT, m], "ZRANK 逐成员");
    }
    ck!(
      RespCommand::Zrank,
      &[KS, b"k", b"WITHSCORE"],
      &[KT, b"k", b"WITHSCORE"],
      "ZRANK k WITHSCORE"
    );
    ck!(
      RespCommand::Zrank,
      &[KS, b"g", b"WITHSCORE"],
      &[KT, b"g", b"WITHSCORE"],
      "ZRANK g WITHSCORE"
    );
    ck!(
      RespCommand::Zrank,
      &[KS, b"nope"],
      &[KT, b"nope"],
      "ZRANK 缺成员"
    );
    ck!(
      RespCommand::Zrank,
      &[KS, b"nope", b"WITHSCORE"],
      &[KT, b"nope", b"WITHSCORE"],
      "ZRANK 缺成员 WITHSCORE"
    );
    ck!(
      RespCommand::Zrevrank,
      &[KS, b"nope"],
      &[KT, b"nope"],
      "ZREVRANK 缺成员"
    );
    // ZREVRANK 基准 = 存活总数 → 大集合恰多出灌水成员段（偏移 = 灌水数）
    for &member in SPOT {
      let small = auto_exec(&api, &rt, &mut s, RespCommand::Zrevrank, &[KS, member]);
      let big = auto_exec(&api, &rt, &mut s, RespCommand::Zrevrank, &[KT, member]);
      assert_eq!(
        rank_of(&big),
        rank_of(&small) + fill as i64,
        "ZREVRANK 平移不变量: {} ver={}",
        from_utf8(member).unwrap(),
        ver
      );
    }
  }

  // ---- 读族不得改动作答无关的元记录计数，也不得触发降阶
  assert_eq!(
    size_of(KT),
    (fill + MEM.len()) as u64,
    "读族不得改动 meta.size"
  );
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(KT))
      .unwrap()
      .is_some(),
    "读族后 zt 仍应为分层态"
  );

  // ============================ 到期成员内联过滤 ============================
  // 成员 d（分值 2，序 7）即时到期：两态一律「过滤不出产」，应答须继续全等。
  // 分层侧不再对 zt 下 ZEXPIRE（那会向树写 mini-page 增量），改另建 zte：与 zt
  // 同规模 bulk 升阶、d 预烘到期刻度（纯 base 页）；内存侧 zs 用 ZEXPIRE 到期 d。
  auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zexpire,
    &[KS, b"0", b"MEMBERS", b"1", b"d"],
  );
  const ZTE: &[u8] = b"zte";
  promote(ZTE, build(Some(b"d")));
  for ver in [2u8, 3u8] {
    s.resp_protocol_version = ver;
    ck!(
      RespCommand::Zrange,
      &[KS, b"0", b"13"],
      &[ZTE, b"0", b"13"],
      "到期后 ZRANGE 0 13"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"1", b"3", b"WITHSCORES"],
      &[ZTE, b"1", b"3", b"WITHSCORES"],
      "到期后 ZRANGEBYSCORE 1 3"
    );
    ck!(
      RespCommand::Zrangebyscore,
      &[KS, b"2", b"2", b"LIMIT", b"1", b"2"],
      &[ZTE, b"2", b"2", b"LIMIT", b"1", b"2"],
      "到期后 ZRANGEBYSCORE 2 2 LIMIT"
    );
    ck!(
      RespCommand::Zrangebylex,
      &[KS, b"[a", b"[m", b"WITHSCORES"],
      &[ZTE, b"[a", b"[m", b"WITHSCORES"],
      "到期后 ZRANGEBYLEX [a [m"
    );
    ck!(
      RespCommand::Zrevrangebylex,
      &[KS, b"[z", b"[a"],
      &[ZTE, b"[z", b"[a"],
      "到期后 ZREVRANGEBYLEX"
    );
    ck!(
      RespCommand::Zcount,
      &[KS, b"2", b"2"],
      &[ZTE, b"2", b"2"],
      "到期后 ZCOUNT 2 2"
    );
    ck!(
      RespCommand::Zlexcount,
      &[KS, b"[a", b"[m"],
      &[ZTE, b"[a", b"[m"],
      "到期后 ZLEXCOUNT [a [m"
    );
    ck!(
      RespCommand::Zrank,
      &[KS, b"k"],
      &[ZTE, b"k"],
      "到期后 ZRANK k"
    );
    ck!(
      RespCommand::Zrank,
      &[KS, b"d"],
      &[ZTE, b"d"],
      "到期后 ZRANK 到期成员"
    );
    for &member in SPOT {
      let small = auto_exec(&api, &rt, &mut s, RespCommand::Zrevrank, &[KS, member]);
      let big = auto_exec(&api, &rt, &mut s, RespCommand::Zrevrank, &[ZTE, member]);
      assert_eq!(
        rank_of(&big),
        rank_of(&small) + fill as i64,
        "到期后 ZREVRANK 平移不变量: {} ver={}",
        from_utf8(member).unwrap(),
        ver
      );
    }
    ck!(
      RespCommand::Zrange,
      &[KS, b"9", b"13", b"WITHSCORES"],
      &[ZTE, b"9", b"13", b"WITHSCORES"],
      "到期后 ZRANGE 9 13（兴趣成员恒居序首）"
    );
  }
  // 到期成员在两态一律从全量窗口中剔除（内存态 zs：14 兴趣成员，无灌水）
  s.resp_protocol_version = 2;
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zrange, &[KS, b"0", b"-1"]),
    flat_frame(MEM.iter().filter(|(m, _)| *m != "d").map(|(m, _)| *m)),
    "到期成员不参与全量窗口"
  );
  // 分层态 zte 全量窗口末段仍为最大灌水成员、首段兴趣成员剔除 d（规模自证）
  let big_full = auto_exec(&api, &rt, &mut s, RespCommand::Zrange, &[ZTE, b"0", b"-1"]);
  assert!(
    big_full.starts_with(format!("*{}\r\n$1\r\nl\r\n", fill + MEM.len() - 1).as_bytes()),
    "到期后分层全量窗口应剔除 d 且规模 = 存活总数: {:?}",
    &big_full[..32.min(big_full.len())]
  );
  assert_eq!(size_of(ZTE), (fill + MEM.len()) as u64);
}

/// LIMIT 折位早退损坏载荷锁（案一）：带损坏分值载荷成员的分层键上，
/// `LIMIT 0 0` / `LIMIT -1 n` 各形必须按契约恒空回 `*0`——对位 C#
/// GetElementsInRangeByLex :1004-1010 与 GetElementsInRangeByScore :1095-1099
/// 早退不触集合内容。未接早退时树内臂跑扫描撞上 corrupt fail-fast
/// （zset.rs 内核头注成文纪律）：lex 臂折 SLOW_PATH_STORAGE 错误帧、
/// score 臂上抛穿透存储错误，恒空应答双态分叉。对照组锁：同键无 LIMIT
/// 全窗仍须回错误帧——早退不得吞掉损坏判定
#[test]
fn test_tiered_zset_limit_early_exit_with_corrupt_payload() {
  let (rt, api, store, _dir) = open_env("tiered-zlimit-corrupt.db");
  let mut s = session_with(&api);
  let key = b"zc";
  // 正常成员 a/b/c（分值 1/2/3）+ 损坏成员 z9：分值载荷 16B 超界（非 8B
  // f64），扫描至该项即置 corrupt fail-fast
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = [(b"a", 1.0f64), (b"b", 2.0), (b"c", 3.0)]
    .into_iter()
    .map(|(m, score)| (m.to_vec(), encode_member(&score.to_be_bytes(), None)))
    .collect();
  ents.push((b"z9".to_vec(), encode_member(&[7u8; 16], None)));
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::SortedSet,
    ents,
    i64::MAX,
    false,
  ))
  .unwrap();

  for ver in [2u8, 3u8] {
    s.resp_protocol_version = ver;
    // ---- 早退生效：三形契约恒空，不触损坏内容（逐字节 *0，与内存信封
    // 恒空形同帧）
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zrangebylex,
        &[key, b"[a", b"[z", b"LIMIT", b"0", b"0"]
      ),
      b"*0\r\n",
      "ZRANGEBYLEX [a [z LIMIT 0 0 恒空早退 (resp={ver})"
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zrevrangebylex,
        &[key, b"[z", b"[a", b"LIMIT", b"-1", b"2"]
      ),
      b"*0\r\n",
      "ZREVRANGEBYLEX [z [a LIMIT -1 2 恒空早退 (resp={ver})"
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zrevrangebyscore,
        &[key, b"100", b"-inf", b"LIMIT", b"0", b"0"]
      ),
      b"*0\r\n",
      "ZREVRANGEBYSCORE 100 -inf LIMIT 0 0 恒空早退 (resp={ver})"
    );
    // ---- 对照组：同键无折位的窗口形照常撞损坏 → fail-fast 不被早退吞掉
    // lex 臂折帧逐字节 SLOW_PATH_STORAGE
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Zrangebylex,
        &[key, b"[a", b"[z"]
      ),
      b"-ERR slow path storage error\r\n",
      "无 LIMIT 全窗损坏载荷仍回 SLOW_PATH_STORAGE (resp={ver})"
    );
    // score 臂 Err 上抛穿透（非折帧），形态与 tiered_scan_err_propagate
    // zrange 先例同口径断 -ERR 行首
    let out = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrangebyscore,
      &[key, b"-inf", b"+inf"],
    );
    assert!(
      out.starts_with(b"-ERR "),
      "score 臂损坏载荷仍上抛存储错误帧，实际: {:?}",
      String::from_utf8_lossy(&out)
    );
  }
}

/// 双键范围/排名应答逐字节对照：`small` = 内存态对象层基准，`big` = 分层树内臂
fn check_range_parity(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  small: &[&[u8]],
  big: &[&[u8]],
  label: &str,
) {
  let base = auto_exec(api, rt, s, cmd, small);
  let got = auto_exec(api, rt, s, cmd, big);
  assert_eq!(
    got,
    base,
    "[{label}] {cmd} 分层态与内存态应答不一致 (resp={})\n  内存态: {:?}\n  分层态: {:?}",
    s.resp_protocol_version,
    String::from_utf8_lossy(&base[..base.len().min(160)]),
    String::from_utf8_lossy(&got[..got.len().min(160)])
  );
}

/// 名次帧首整数（`:n` 与 `*2\r\n:n\r\n$…` 两形态）
fn rank_of(frame: &[u8]) -> i64 {
  let text = from_utf8(frame).expect("名次帧应为 UTF-8");
  let line = text
    .split("\r\n")
    .find(|l| l.starts_with(':'))
    .unwrap_or_else(|| panic!("名次帧无整数项: {text:?}"));
  line[1..].parse().expect("名次整数")
}

/// RESP2 无分值成员数组帧独立构造（测试侧实现，避免与被测写器同源自证）
fn flat_frame<'a>(members: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
  let members: Vec<&str> = members.into_iter().collect();
  let mut out = format!("*{}\r\n", members.len()).into_bytes();
  for m in members {
    out.extend_from_slice(format!("${}\r\n{m}\r\n", m.len()).as_bytes());
  }
  out
}

/// 升阶键 List/Set 命令面与未支持操作物化降级
#[test]
fn test_tiered_list_set_and_demote() {
  let (rt, api, _store, _dir) = open_env("tiered-list-set.db");
  let mut s = session_with(&api);

  // ---- List 升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l",
    total,
    |i, buf| vec![prefixed(b'v', buf, i)],
  );

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
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Sadd,
    b"k",
    total,
    |i, buf| vec![prefixed(b'm', buf, i)],
  );

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
  // HRANDFIELD 信封键兜底臂（分层树态已另立树内只读抽样臂矩阵用例
  // test_tiered_hash_random_field_tree_arm_matrix）——新建 hash（信封域直接对象层）
  for chunk_start in (1..=2048).step_by(2048) {
    let chunk_end = (chunk_start + 2047).min(2048);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"hh".to_vec());
    for i in chunk_start..=chunk_end {
      let s = buf.format(i).as_bytes();
      let mut f = Vec::with_capacity(s.len() + 1);
      f.push(b'f');
      f.extend_from_slice(s);
      args.push(f);
      args.push(s.to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }
  // 未升阶 hash 的 HRANDFIELD 走 slow 段信封兜底臂（对象层单源）
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
/// （`tiered_collection_ops/list.rs:list_head_seq`，scan_cnt=1）取头端，尾端按
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
  let mut buf = ItoaBuffer::new();
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l",
    total,
    |i, buf| {
      let v = prefixed(b'v', buf, i);
      model.push_back(v.clone());
      vec![v]
    },
  );
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
  let mut batch: Vec<Vec<u8>> = Vec::with_capacity(500);
  for i in 0..500_usize {
    let s = buf.format(i).as_bytes();
    let mut b = Vec::with_capacity(s.len() + 1);
    b.push(b'b');
    b.extend_from_slice(s);
    batch.push(b);
  }
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
  let mut exp_len = Vec::with_capacity(16);
  exp_len.push(b':');
  exp_len.extend_from_slice(buf.format(model.len()).as_bytes());
  exp_len.extend_from_slice(b"\r\n");
  assert_eq!(out, exp_len);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"-1"]);
  let text = String::from_utf8(out).unwrap();
  let mut parts = text.split("\r\n");
  let len_str = buf.format(model.len()).to_string();
  let len_hdr = format!("*{len_str}");
  assert_eq!(parts.next(), Some(len_hdr.as_str()));
  for expect in &model {
    let hdr = parts.next().unwrap();
    assert!(hdr.starts_with('$'));
    assert_eq!(&hdr[1..], buf.format(expect.len()));
    let got = parts.next().unwrap();
    assert_eq!(
      got.as_bytes(),
      expect.as_slice(),
      "分层态与内存态须逐条同序"
    );
  }
  assert_eq!(parts.next(), Some(""));
}

/// 空分层集合键 SRANDMEMBER 应答语义（D15）：手工升阶空条目（size=0 元
/// 记录）被 MetaValue::is_live 门（size > 0）判死，分层臂结构性不可达，
/// 一律按缺失口径应答——无 count / 负 count → RESP null，count>0 → 空集
/// 头，count==0 → 键态无关空数组。对标 C# SetObjectImpl.cs SetRandomMember
/// 空集三分支（WriteSetLength(0) / WriteNull / WriteNull）与 SetCommands.cs
/// SetRandomMember 的 count==0 拦截；SPOP 为分层穿透臂（物化降级），
/// 无 SPOP 分层位点
#[test]
fn test_tiered_empty_set_srandmember_null_semantics() {
  let (rt, api, store, _dir) = open_env("tiered-empty-set.db");
  let mut s = session_with(&api);

  // 手工升阶空集（count=0 元记录 + 空树）：元记录在册但 is_live 判死
  rt.block_on(async {
    store
      .new_session()
      .unwrap()
      .promote_collection_to_bftree(b"es", GarnetObjectType::Set, vec![], i64::MAX, false)
      .await
      .unwrap();
  });
  assert!(
    !rt.block_on(async {
      store
        .new_session()
        .unwrap()
        .load_collection_stub(b"es")
        .await
        .unwrap()
        .is_some()
    }),
    "size=0 集合元记录应被 is_live 门判死（空分层键不得以活键形态泄出）"
  );

  // 无 count → RESP null
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es"]),
    b"$-1\r\n"
  );
  // 带 count（含负）→ NOTFOUND 口径空数组（C# SetCommands.cs SetRandomMember
  // 的 NOTFOUND 分支只区分有无 count，不分公司负；对象层空集负 count 的
  // WriteNull 是另一层口径，与本缺失形态无关）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"3"]),
    b"*0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"0"]),
    b"*0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"-2"]),
    b"*0\r\n"
  );
}

/// LPOS 独立参照模型（不经被测码，对位 C# ListObjectImpl.cs:ListPosition :383-444
/// 的扫描算法：正/负 rank 窗口折 maxlen、rank 跳过、noOfFoundItem==count break、
/// 命中序正/降序直出），输出 RESP 帧字节。tiered_cmds_align LRANGE 窗口用例
/// 同款「测试侧独立复写参照」口径
fn lpos_reference(
  list: &[Vec<u8>],
  element: &[u8],
  rank: i64,
  count: Option<i64>,
  maxlen: Option<i64>,
  resp3: bool,
) -> Vec<u8> {
  let len = list.len() as i64;
  let is_default = count.is_none();
  let parsed = count.unwrap_or(1);
  let cap = if parsed == 0 { len } else { parsed };
  let ml = maxlen.unwrap_or(0);
  let mut hits: Vec<i64> = Vec::new();
  if rank > 0 {
    let bound = if ml == 0 { len } else { len.min(ml) };
    let mut r = rank;
    for (i, item) in list.iter().enumerate().take(bound.max(0) as usize) {
      if item.as_slice() == element {
        if r == 1 {
          hits.push(i as i64);
          if is_default || hits.len() as i64 == cap {
            break;
          }
        } else {
          r -= 1;
        }
      }
    }
  } else {
    let mut r = rank.unsigned_abs() as i64;
    let low = if ml == 0 { 0 } else { (len - ml).max(0) };
    let mut i = len - 1;
    while i >= low && i >= 0 {
      if list[i as usize].as_slice() == element {
        if r == 1 {
          hits.push(i);
          if is_default || hits.len() as i64 == cap {
            break;
          }
        } else {
          r -= 1;
        }
      }
      i -= 1;
    }
  }
  if is_default {
    match hits.first() {
      Some(idx) => format!(":{idx}\r\n").into_bytes(),
      None => {
        if resp3 {
          b"_\r\n".to_vec()
        } else {
          b"$-1\r\n".to_vec()
        }
      }
    }
  } else if hits.is_empty() {
    b"*0\r\n".to_vec()
  } else {
    let mut out = format!("*{}\r\n", hits.len()).into_bytes();
    for idx in &hits {
      out.extend_from_slice(format!(":{idx}\r\n").as_bytes());
    }
    out
  }
}

/// LPOS 单命令装配往返（RANK 恒显式给出；COUNT/MAXLEN 依缺省形省略）
fn lpos_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  elem: &[u8],
  (rank, count, maxlen): (i64, Option<i64>, Option<i64>),
) -> Vec<u8> {
  let mut owned: Vec<Vec<u8>> = vec![
    key.to_vec(),
    elem.to_vec(),
    b"RANK".to_vec(),
    rank.to_string().into_bytes(),
  ];
  if let Some(c) = count {
    owned.push(b"COUNT".to_vec());
    owned.push(c.to_string().into_bytes());
  }
  if let Some(m) = maxlen {
    owned.push(b"MAXLEN".to_vec());
    owned.push(m.to_string().into_bytes());
  }
  let arg_slices: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
  auto_exec(api, rt, s, RespCommand::Lpos, &arg_slices)
}

/// 分层/信封双态 LPOS 树内臂逐字节全等矩阵（票 zcode-r149c-lposrank 案二）：
/// RANK{1,2,3,5,6,-1,-2,-5,-6} × COUNT{缺省,1,0,2} × MAXLEN{缺省,0,阈中,=len,>len}
/// × 命中{头,尾,多,零} × RESP{2,3}，全组合三方交叉——独立参照模型、手工升阶
/// 分层键（seed_list 同款原语）、RPUSH 信封键（对象层单源）逐字节全等；
/// 三门拒绝/语法错误帧双态同帧（wcol read_list_position_params 单源验证）
#[test]
fn test_tiered_list_lpos_dualstate_parity_matrix() {
  let (rt, api, store, _dir) = open_env("tiered-list-lpos-matrix.db");
  let mut s = session_with(&api);
  // 16 元素：c 出现于 {0,2,5,10,14}（多重命中、头命中、尾侧命中），
  // z15 唯一尾命中、m3 唯一中段命中、nope 零命中
  let content: Vec<Vec<u8>> = (0..16usize)
    .map(|i| match i {
      0 | 2 | 5 | 10 | 14 => b"c".to_vec(),
      15 => b"z15".to_vec(),
      3 => b"m3".to_vec(),
      _ => format!("e{i}").into_bytes(),
    })
    .collect();
  {
    let sess = store.new_session().unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = content
      .iter()
      .enumerate()
      .map(|(i, e)| {
        (
          (LIST_SEQ_BASE + i as u128).to_be_bytes().to_vec(),
          encode_member(e, None),
        )
      })
      .collect();
    rt.block_on(sess.promote_collection_to_bftree(
      b"tl",
      GarnetObjectType::List,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();
    assert!(
      rt.block_on(sess.load_collection_stub(b"tl"))
        .unwrap()
        .is_some(),
      "前置判据：tl 须处于分层树态"
    );
  }
  // 信封对照键 tm：同一份数据一次 RPUSH（16 元素远低于升阶门限）
  let mut args: Vec<&[u8]> = vec![b"tm"];
  args.extend(content.iter().map(|v| v.as_slice()));
  auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &args);

  for version in [2u8, 3] {
    s.resp_protocol_version = version;
    let resp3 = version == 3;
    for elem in [b"c".as_slice(), b"z15", b"m3", b"nope"] {
      for rank in [1i64, 2, 3, 5, 6, -1, -2, -5, -6] {
        for count in [None, Some(1i64), Some(0), Some(2)] {
          for maxlen in [None, Some(0i64), Some(3), Some(16), Some(20)] {
            let expect = lpos_reference(&content, elem, rank, count, maxlen, resp3);
            let got_tiered = lpos_exec(&api, &rt, &mut s, b"tl", elem, (rank, count, maxlen));
            assert_eq!(
              got_tiered,
              expect,
              "分层态 v{version} elem={:?} rank={rank} count={count:?} maxlen={maxlen:?}",
              String::from_utf8_lossy(elem)
            );
            let got_envelope = lpos_exec(&api, &rt, &mut s, b"tm", elem, (rank, count, maxlen));
            assert_eq!(
              got_envelope,
              expect,
              "信封态 v{version} elem={:?} rank={rank} count={count:?} maxlen={maxlen:?}",
              String::from_utf8_lossy(elem)
            );
          }
        }
      }
    }
  }

  // 三门拒绝/语法错误帧双态同帧（词元与三门与信封态同一份码）
  s.resp_protocol_version = 2;
  for bad in [
    vec![b"c".to_vec(), b"RANK".to_vec(), b"0".to_vec()],
    vec![b"c".to_vec(), b"COUNT".to_vec(), b"-1".to_vec()],
    vec![b"c".to_vec(), b"MAXLEN".to_vec(), b"-5".to_vec()],
    vec![b"c".to_vec(), b"RANK".to_vec(), b"abc".to_vec()],
    vec![b"c".to_vec(), b"FOO".to_vec(), b"1".to_vec()],
    vec![b"c".to_vec(), b"COUNT".to_vec()],
  ] {
    let mut t_args: Vec<&[u8]> = vec![b"tl"];
    t_args.extend(bad.iter().map(|v| v.as_slice()));
    let mut m_args: Vec<&[u8]> = vec![b"tm"];
    m_args.extend(bad.iter().map(|v| v.as_slice()));
    let t = auto_exec(&api, &rt, &mut s, RespCommand::Lpos, &t_args);
    let m = auto_exec(&api, &rt, &mut s, RespCommand::Lpos, &m_args);
    assert_eq!(t, m, "错误帧双态同帧: bad={bad:?}");
  }
  // 字面量抽查：三门 not-an-integer 帧（既有 Spop 界测同款文本）
  let t = lpos_exec(&api, &rt, &mut s, b"tl", b"c", (0, None, None));
  assert_eq!(t, b"-ERR value is not an integer or out of range.\r\n");
}

/// 缺省形首命中早停（票 zcode-r149c-lposrank 案二测试点）：2×阈 键 canary 居
/// 头，LPOS 缺省形命中即 return false 截断（O(1) 定位 + 一条记录读），与全树
/// 131072 条逐条解码比较的未命中形计时对照——旧形物化通道两形皆付全树三拷贝，
/// 无早停面。量级差判据（8 倍裕度），先各跑一轮预热使冷页 IO 不参与对照
#[test]
fn test_tiered_list_lpos_default_first_hit_early_stop() {
  let (rt, api, _store, _dir) = open_env("tiered-list-lpos-stop.db");
  let mut s = session_with(&api);
  let total = 2 * wcol::TIERED_PROMOTE_THRESHOLD;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l2",
    total,
    |i, buf| {
      if i == 1 {
        vec![b"head_canary".to_vec()]
      } else {
        vec![prefixed(b'v', buf, i)]
      }
    },
  );

  // 预热两形（页缓存收敛后对比）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"head_canary"]
    ),
    b":0\r\n",
    "缺省形头命中位次 0"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"absent_elem"]
    ),
    b"$-1\r\n",
    "未命中形回 null（严禁折存储错误帧）"
  );

  let mark = Instant::now();
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"head_canary"]
    ),
    b":0\r\n"
  );
  let d_head = mark.elapsed();
  let mark = Instant::now();
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"absent_elem"]
    ),
    b"$-1\r\n"
  );
  let d_full = mark.elapsed();
  assert!(
    d_head * 8 < d_full,
    "缺省形首命中须早停截断（头命中 O(1) vs 全树 {total} 条扫）: head={d_head:?} full={d_full:?}"
  );
}

/// 分层树 HRANDFIELD 只读抽样臂矩阵（票 zcode-r151c-smembers 案一，
/// collection.md §8.5 SRANDMEMBER 树内臂对偶）：真正 bftree 大 hash
/// （2×阈值灌水自动升阶）× 无 count/正 count/负 count/WITHVALUES ×
/// RESP2/RESP3 与信封态小 hash 应答集合等价，确定性帧（count 0、错误帧、
/// 缺键 null、互异全量域）逐字节锁头。两态随机源独立（§12 不承诺同 seed
/// 同序），本用例锁帧形/条数/值域/配对契约面，非具体抽样位点。
///
/// 成员级 TTL 段沿用 zte 预烘到期形（水位随灌入批落真实最早值）：抽样域
/// 收敛至存活集、声明头恒等实发，剔除不固化——出账仍归计数校正臂（HLEN）。
///
/// 测试侧独立递归帧解析器（与被测写侧代码异源），解析后另验全帧消费：
/// 声明头 > 实发即解析越界 panic，声明头 < 实发即残留 assert 红。
#[test]
fn test_tiered_hash_random_field_tree_arm_matrix() {
  #[derive(Debug)]
  enum Rd {
    Bulk(Vec<u8>),
    Null,
    Arr(Vec<Rd>),
  }
  fn px(b: &[u8], i: &mut usize) -> Rd {
    assert!(*i < b.len(), "帧在偏移 {i} 提前耗尽: {b:?}");
    let t = b[*i];
    *i += 1;
    let nl = b[*i..]
      .iter()
      .position(|&c| c == b'\n')
      .unwrap_or_else(|| panic!("缺行终止: {b:?}"))
      + *i;
    let line = &b[*i..nl - 1];
    *i = nl + 1;
    match t {
      b'$' => {
        let len: i64 = from_utf8(line).unwrap().parse().unwrap();
        if len < 0 {
          Rd::Null
        } else {
          let l = len as usize;
          assert!(
            *i + l + 2 <= b.len() && &b[*i + l..*i + l + 2] == b"\r\n",
            "bulk 体越界: {b:?}"
          );
          let v = b[*i..*i + l].to_vec();
          *i += l + 2;
          Rd::Bulk(v)
        }
      }
      b'*' => {
        let n: i64 = from_utf8(line).unwrap().parse().unwrap();
        if n < 0 {
          Rd::Null
        } else {
          let mut v = Vec::with_capacity(n as usize);
          for _ in 0..n {
            v.push(px(b, i));
          }
          Rd::Arr(v)
        }
      }
      b'_' => Rd::Null,
      b'+' | b'-' | b':' => Rd::Bulk(line.to_vec()),
      other => panic!("未知帧首 {} @ {i}", other as char),
    }
  }
  fn parse1(out: &[u8], ctxmsg: &str) -> Rd {
    let mut pos = 0usize;
    let f = px(out, &mut pos);
    assert_eq!(
      pos,
      out.len(),
      "{ctxmsg}: 声明头与实发不符（越界/残留）raw={out:?}"
    );
    f
  }
  fn as_arr<'a>(f: &'a Rd, ctxmsg: &str) -> &'a [Rd] {
    match f {
      Rd::Arr(v) => &v[..],
      other => panic!("{ctxmsg} 非数组帧: {other:?}"),
    }
  }
  fn as_bulk<'a>(f: &'a Rd, ctxmsg: &str) -> &'a [u8] {
    match f {
      Rd::Bulk(v) => &v[..],
      other => panic!("{ctxmsg} 非 bulk 帧: {other:?}"),
    }
  }

  let (rt, api, store, _dir) = open_env("hrandfield-tree-arm.db");
  let mut s = session_with(&api);
  let mut buf = ItoaBuffer::new();

  // ---- 分层大 hash：2×阈值 灌水自动升阶（§8.3 信封上界）
  let total = 2 * wcol::TIERED_PROMOTE_THRESHOLD;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    b"th",
    total,
    |i, buf| vec![prefixed(b'f', buf, i), prefixed(b'v', buf, i)],
  );
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"th"))
    .unwrap()
    .expect("th 须经灌水自动升阶为分层态");
  assert_eq!(meta.size, total as u64, "升阶后 meta.size 须为灌入总数");

  // ---- 信封对照 hash：32 字段（阈下永不升阶）
  let mh_total = 32usize;
  let mut args: Vec<Vec<u8>> = vec![b"mh".to_vec()];
  for i in 1..=mh_total {
    args.push(prefixed(b'f', &mut buf, i));
    args.push(prefixed(b'v', &mut buf, i));
  }
  let argv: Vec<&[u8]> = args.iter().map(|v| &v[..]).collect();
  auto_exec(&api, &rt, &mut s, RespCommand::Hset, &argv);
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(b"mh"))
      .unwrap()
      .is_none(),
    "mh 须保持信封态"
  );

  // f<i>→v<i> 确定性配对（WITHVALUES 保真判据）
  let expect_val = |f: &[u8]| -> Vec<u8> {
    assert_eq!(f[0], b'f', "抽样字段须落在 f<i>/v<i> 值域: {f:?}");
    let mut v = Vec::with_capacity(f.len());
    v.push(b'v');
    v.extend_from_slice(&f[1..]);
    v
  };
  // 存活探针：HGET 命中 bulk ⟺ 字段域内且未到期（两态 Hget 臂同做成员级
  // 剔除，异臂交叉验证，不依赖被测抽样臂自身）
  let assert_alive =
    |api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], f: &[u8]| {
      let o = auto_exec(api, rt, s, RespCommand::Hget, &[key, f]);
      assert!(
        o.starts_with(b"$") && !o.starts_with(b"$-1"),
        "抽样字段 {f:?} ∈ {key:?} 须可 HGET 命中存活: {o:?}",
      );
    };

  let mut space: HashSet<Vec<u8>> = HashSet::new();
  for i in 1..=total {
    let mut b2 = ItoaBuffer::new();
    space.insert(prefixed(b'f', &mut b2, i));
  }
  let mut space32: HashSet<Vec<u8>> = HashSet::new();
  for i in 1..=mh_total {
    let mut b2 = ItoaBuffer::new();
    space32.insert(prefixed(b'f', &mut b2, i));
  }

  for ver in [2u8, 3u8] {
    s.resp_protocol_version = ver;
    for key in [b"th".as_slice(), b"mh".as_slice()] {
      let tag = if key == b"th".as_slice() {
        "分层"
      } else {
        "信封"
      };

      // ---- 无 count 单成员形：bulk 且域内存活（位点各自随机，§12 独立源）
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key]);
      assert!(out.starts_with(b"$"), "{tag} 无 count 形须 bulk: {out:?}");
      let frame = parse1(&out, &format!("{tag} 无count ver={ver}"));
      let f = as_bulk(&frame, "无count 字段").to_vec();
      assert_alive(&api, &rt, &mut s, key, &f);

      // ---- 正 count 3：帧头 *3 逐字节锁，互异不重样、域内存活
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"3"]);
      assert_eq!(
        &out[..4],
        b"*3\r\n",
        "{tag} 正 count 帧头逐字节锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} count3 ver={ver}"));
      let items = as_arr(&frame, "count3");
      assert_eq!(items.len(), 3);
      let mut uniq: HashSet<Vec<u8>> = HashSet::new();
      for it in items {
        let f = as_bulk(it, "字段项").to_vec();
        assert!(uniq.insert(f.clone()), "互异 count 形不得重样: {f:?}");
        assert_alive(&api, &rt, &mut s, key, &f);
      }

      // ---- WITHVALUES：RESP2 平铺 2n 头、RESP3 每项 *2 对帧，配对保真
      let out = auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[key, b"3", b"WITHVALUES"],
      );
      let frame = parse1(&out, &format!("{tag} HWV ver={ver}"));
      if ver == 2 {
        assert_eq!(&out[..4], b"*6\r\n", "{tag} RESP2 WITHVALUES 平铺 2n 头锁");
        let items = as_arr(&frame, "RESP2 HWV");
        assert_eq!(items.len(), 6);
        for j in 0..3 {
          let f = as_bulk(&items[j * 2], "HWV 字段项").to_vec();
          let v = as_bulk(&items[j * 2 + 1], "HWV 值项");
          assert_eq!(v, expect_val(&f), "WITHVALUES 须 f<i>→v<i> 配对: {f:?}");
          assert_alive(&api, &rt, &mut s, key, &f);
        }
      } else {
        assert_eq!(&out[..4], b"*3\r\n", "{tag} RESP3 WITHVALUES 外层头锁");
        let items = as_arr(&frame, "RESP3 HWV");
        assert_eq!(items.len(), 3);
        for it in items {
          let pair = as_arr(it, "RESP3 每项对帧");
          assert_eq!(pair.len(), 2, "RESP3 WITHVALUES 每项 *2 对帧");
          let f = as_bulk(&pair[0], "HWV 字段项").to_vec();
          let v = as_bulk(&pair[1], "HWV 值项");
          assert_eq!(v, expect_val(&f), "WITHVALUES 须 f<i>→v<i> 配对: {f:?}");
          assert_alive(&api, &rt, &mut s, key, &f);
        }
      }

      // ---- 负 count -4：可重复形帧头恒 *4（RESP2 HWV *8 / RESP3 *4 对帧）
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"-4"]);
      assert_eq!(
        &out[..4],
        b"*4\r\n",
        "{tag} 负 count 帧头逐字节锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} neg ver={ver}"));
      for it in as_arr(&frame, "neg") {
        let f = as_bulk(it, "neg 字段项").to_vec();
        assert!(
          space.contains(&f) || space32.contains(&f),
          "负 count 字段越域: {f:?}"
        );
        assert_alive(&api, &rt, &mut s, key, &f);
      }
      let out = auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[key, b"-4", b"WITHVALUES"],
      );
      let head = if ver == 2 { "*8\r\n" } else { "*4\r\n" };
      assert_eq!(
        &out[..head.len()],
        head.as_bytes(),
        "{tag} 负 count HWV 头锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} negHWV ver={ver}"));
      let items = as_arr(&frame, "negHWV");
      if ver == 2 {
        assert_eq!(items.len(), 8);
        for j in 0..4 {
          let f = as_bulk(&items[j * 2], "negHWV 字段项").to_vec();
          assert_eq!(as_bulk(&items[j * 2 + 1], "negHWV 值项"), expect_val(&f));
        }
      } else {
        assert_eq!(items.len(), 4);
        for it in items {
          let pair = as_arr(it, "negHWV 对帧");
          assert_eq!(pair.len(), 2);
          let f = as_bulk(&pair[0], "negHWV 字段项").to_vec();
          assert_eq!(as_bulk(&pair[1], "negHWV 值项"), expect_val(&f));
        }
      }

      // ---- count 0：不触后端，双态双版本 *0 逐字节锁（含 WITHVALUES 尾缀）
      for extra in [None, Some(&b"WITHVALUES"[..])] {
        let mut a: Vec<&[u8]> = vec![key, b"0"];
        if let Some(e) = extra {
          a.push(e);
        }
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &a);
        assert_eq!(out, b"*0\r\n", "{tag} count 0 短路帧锁 args={a:?}");
      }

      // ---- 互异钳制域：count ≥ size → 恰全集，帧头字节锁 + 集合等价
      if key == b"th".as_slice() {
        let narg = buf.format(total).as_bytes().to_vec();
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, &narg]);
        let hdr = format!("*{total}\r\n");
        assert_eq!(
          &out[..hdr.len()],
          hdr.as_bytes(),
          "分层全量域钳制帧头逐字节锁"
        );
        let frame = parse1(&out, "th clamp");
        let items = as_arr(&frame, "th clamp");
        assert_eq!(items.len(), total);
        let mut got: HashSet<Vec<u8>> = HashSet::new();
        for it in items {
          let f = as_bulk(it, "clamp 字段项").to_vec();
          assert!(got.insert(f.clone()), "互异全量域不得重样: {f:?}");
          assert!(space.contains(&f), "clamp 字段越域: {f:?}");
        }
        assert_eq!(got, space, "HRANDFIELD 全量域须与灌入字段域集合等价");
      } else {
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"40"]);
        assert_eq!(
          &out[..5],
          b"*32\r\n",
          "信封态 count 越界须钳至 size（帧头锁）"
        );
        let frame = parse1(&out, "mh clamp");
        let items = as_arr(&frame, "mh clamp");
        let mut got: HashSet<Vec<u8>> = HashSet::new();
        for it in items {
          got.insert(as_bulk(it, "clamp32 字段项").to_vec());
        }
        assert_eq!(got, space32, "信封态全量域集合等价");
      }
    }

    // ---- 错误帧双态逐字节全等（单源上游解析器，同源即同帧）
    let err_int = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"th", b"xx"]);
    assert_eq!(
      err_int,
      b"-ERR value is not an integer or out of range.\r\n"
    );
    assert_eq!(
      err_int,
      auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"mh", b"xx"]),
      "非整数错误帧分层/信封须同字节"
    );
    assert_eq!(
      err_int,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"th", b"99999999999999"]
      ),
      "int32 越界同 VALUE_IS_NOT_INTEGER 帧（C# TryGetInt 口径）"
    );
    let err_syn = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hrandfield,
      &[b"th", b"3", b"WITHSCORES"],
    );
    assert_eq!(err_syn, b"-ERR syntax error\r\n");
    assert_eq!(
      err_syn,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"mh", b"3", b"WITHSCORES"]
      ),
      "第三词元语法门错误帧双态同字节"
    );
    let err_ar = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hrandfield,
      &[b"th", b"1", b"2", b"3"],
    );
    assert!(
      err_ar.starts_with(b"-ERR "),
      "arity 越界须错误帧: {err_ar:?}"
    );
    assert_eq!(
      err_ar,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"mh", b"1", b"2", b"3"]
      ),
      "arity 错误帧双态同字节"
    );

    // ---- 缺键：版本感知 null 与 *0 逐字节锁（双态共用短路出口）
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"nope"]);
    assert_eq!(
      &out[..],
      if ver == 2 {
        &b"$-1\r\n"[..]
      } else {
        &b"_\r\n"[..]
      },
      "缺键无 count 形版本感知 null 锁"
    );
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"nope", b"5"]);
    assert_eq!(out, b"*0\r\n", "缺键带 count 形空数组锁");
  }

  // ---- 抽样电池为纯读：不触元记录、不触发降阶（§8.5 读侧口径）
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"th"))
    .unwrap()
    .expect("抽样电池后 th 仍分层态");
  assert_eq!(
    meta.size, total as u64,
    "HRANDFIELD 树内臂不得改动 meta.size（不置脏）"
  );

  // ---- 随机源独立面（§12）：131072 字段域连抽 32 次无 count 形，
  // 全同概率 ≈ 131072^-31 ≈ 0，须见 ≥2 相异（锁随机起始键定位，
  // 非退化为固定树头命中）
  s.resp_protocol_version = 2;
  let mut seen: HashSet<Vec<u8>> = HashSet::new();
  for _ in 0..32 {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"th"]);
    let frame = parse1(&out, "随机性抽样");
    seen.insert(as_bulk(&frame, "随机性字段").to_vec());
  }
  assert!(
    seen.len() >= 2,
    "同一树连抽 32 次位点须呈随机散布（随机起始键）: {seen:?}"
  );

  // ---- 成员级 TTL：抽样域收敛至存活集，剔除不固化（§53 读侧口径）。
  // fixture 沿用 zte 预烘形：1..=64 中 4 的倍数共 16 条即时到期，水位随
  // 灌入批落真实最早值（到期出账归后续 HLEN 计数校正臂）
  let past = now_ticks() - 60 * TICKS_PER_SECOND;
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(64);
  for i in 1..=64usize {
    let f = prefixed(b'f', &mut buf, i);
    let v = prefixed(b'v', &mut buf, i);
    let expiry = if i % 4 == 0 { Some(past) } else { None };
    ents.push((f, encode_member(&v, expiry)));
  }
  let next_expiry = ents
    .iter()
    .filter_map(|(_, record)| decode_member(record).0)
    .min()
    .unwrap_or(i64::MAX);
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"thx",
    GarnetObjectType::Hash,
    ents,
    next_expiry,
    false,
  ))
  .unwrap();
  // 正 count 超存活域：诚实短供，声明头恒等实发（TTL 二次遍历去重形）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx", b"64"]);
  assert_eq!(&out[..5], b"*48\r\n", "TTL 短供：互异形帧头收敛至存活数");
  let frame = parse1(&out, "thx 64");
  let items = as_arr(&frame, "thx 64");
  assert_eq!(items.len(), 48);
  let mut uniq: HashSet<Vec<u8>> = HashSet::new();
  for it in items {
    let f = as_bulk(it, "存活字段").to_vec();
    assert!(
      uniq.insert(f.clone()),
      "TTL 短供回绕亦不得重样（分段互斥）: {f:?}"
    );
    assert_alive(&api, &rt, &mut s, b"thx", &f);
  }
  // 越 meta.size 钳制后同域收敛（min(200,64) 再 TTL 滤）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thx", b"200"],
  );
  assert_eq!(&out[..5], b"*48\r\n", "越 meta.size 钳制后 TTL 收敛帧头锁");
  let frame = parse1(&out, "thx 200");
  assert_eq!(as_arr(&frame, "thx 200").len(), 48);
  // RESP2 WITHVALUES：平铺头 *96 且配对全存活（到期成员不入帧）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thx", b"64", b"WITHVALUES"],
  );
  assert_eq!(&out[..5], b"*96\r\n", "TTL 短供 RESP2 WITHVALUES 2n 头锁");
  let frame = parse1(&out, "thx HWV 短供");
  let items = as_arr(&frame, "thx HWV");
  assert_eq!(items.len(), 96);
  for j in 0..48 {
    let f = as_bulk(&items[j * 2], "HWV 存活字段").to_vec();
    assert_eq!(as_bulk(&items[j * 2 + 1], "HWV 存活值项"), expect_val(&f));
    assert_alive(&api, &rt, &mut s, b"thx", &f);
  }
  // 负 count（可重复）亦只在存活域收敛
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx", b"-5"]);
  assert_eq!(&out[..4], b"*5\r\n", "TTL 负 count 帧头锁");
  let frame = parse1(&out, "thx -5");
  for it in as_arr(&frame, "thx -5") {
    assert_alive(&api, &rt, &mut s, b"thx", as_bulk(it, "neg 存活字段"));
  }
  // 无 count 形 16 连抽全存活（HGET 异臂探针）
  for _ in 0..16 {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx"]);
    assert!(
      out.starts_with(b"$"),
      "TTL 树无 count 形须命中存活 bulk: {out:?}"
    );
    let frame = parse1(&out, "thx 单抽");
    assert_alive(&api, &rt, &mut s, b"thx", as_bulk(&frame, "单抽字段"));
  }
  // 剔除不固化：抽样电池后元记录 size 仍为灌入物理值（本臂零置脏零出账）
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"thx"))
    .unwrap()
    .expect("thx 抽样电池后仍分层态");
  assert_eq!(
    meta.size, 64,
    "HRANDFIELD 剔除不固化：出账归计数臂，本臂零置脏"
  );
  // 对照口径：水位越过的出账校正仍由 HLEN 计数臂完成（与本臂分工相承）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"thx"]),
    b":48\r\n",
    "计数校正臂照常出账（HRANDFIELD 未替其固化）"
  );

  // ---- 全到期树：count 形诚实短帧 *0、无 count 形版本感知 null
  //（对标对象层 purge 归零形）
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(16);
  let all_past = past - 10 * TICKS_PER_SECOND;
  for i in 1..=16usize {
    let f = prefixed(b'f', &mut buf, i);
    let v = prefixed(b'v', &mut buf, i);
    ents.push((f, encode_member(&v, Some(all_past))));
  }
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"thz",
    GarnetObjectType::Hash,
    ents,
    all_past,
    false,
  ))
  .unwrap();
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz", b"3"]);
  assert_eq!(out, b"*0\r\n", "全到期树 count 形诚实空数组帧");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz"]);
  assert_eq!(out, b"$-1\r\n", "全到期树无 count 形 RESP2 null 锁");
  s.resp_protocol_version = 3;
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz"]);
  assert_eq!(out, b"_\r\n", "全到期树无 count 形 RESP3 null 锁");
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thz", b"3", b"WITHVALUES"],
  );
  assert_eq!(out, b"*0\r\n", "全到期树 WITHVALUES 形诚实空数组帧");
}
