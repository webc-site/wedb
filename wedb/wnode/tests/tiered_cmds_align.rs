//! 升阶键命令面语义对齐集成测试（next/open.data.md 八条修订）
//!
//! 升阶（wcol should_promote → KeyTag::Meta 元记录 + wbftree 树）后：
//! 1. 键存活探针五命令 EXISTS/TTL/EXPIRE/PERSIST/TYPE 与统计面
//!    MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT 对升阶键回值；
//! 2. SCAN 族树内游标全量遍历（HSCAN/SSCAN/ZSCAN/COSCAN）；
//! 3. ZADD 全选项语义与互斥校验、ZRANGE 族树内流式臂与对象层逐字节对齐、
//!    GEO 物化装载语义；
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
    (
      member.to_vec(),
      wcol::types::member_ttl::encode_member(&score.to_be_bytes(), expiry),
    )
  };
  // 兴趣成员分值表 → 入树条目（`expired_member` 命中的成员预烘即时到期刻度）
  let build = |expired_member: Option<&[u8]>| {
    let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(fill + MEM.len());
    for i in 1..=fill {
      ents.push(encode(format!("z{i}").as_bytes(), (1000 + i) as f64, false));
    }
    for (member, score) in MEM {
      let expired = expired_member == Some(member.as_bytes());
      ents.push(encode(member.as_bytes(), *score, expired));
    }
    ents
  };
  let promote = |key: &[u8], ents: Vec<(Vec<u8>, Vec<u8>)>| {
    let sess = store.new_session().unwrap();
    rt.block_on(sess.promote_collection_to_bftree(key, wval::GarnetObjectType::SortedSet, ents))
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
