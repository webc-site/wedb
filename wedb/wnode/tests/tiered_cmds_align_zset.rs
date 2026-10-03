//! 升阶 zset 命令面语义对齐集成测试（自 tiered_cmds_align.rs 按主题拆分，
//! 对标 C# Garnet.test.collections 命令语义）
//!
//! 覆盖：
//! 1. ZADD 全选项语义与互斥校验（含同分值分支 ±0.0 等值面位模式保真：双态
//!    对照 + 成员级 TTL 清除 + 升阶→同分值 ZADD→降阶往返全等）、GEO 物化
//!    装载语义；
//! 2. ZINCRBY 缺席/到期新增臂 ±0.0 词形直存、NaN 拒绝门双态对齐；
//! 3. ZRANGE 族（byIndex/byScore/byLex × REV/WITHSCORES/LIMIT）、ZCOUNT/
//!    ZLEXCOUNT/ZRANK/ZREVRANK 树内流式臂与内存态对象层逐字节全等矩阵
//!    （含到期成员内联过滤与 LIMIT 折位早退损坏载荷锁）。

use std::{str::from_utf8, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::types::member_ttl::{decode_member, encode_member};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::GarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::{auto_exec, open_env, promote_at, pump, roundtrip, session_on as session_with};
use wresp::command::RespCommand;
use wtest_base::resp_frame;
use wval::GarnetObjectType;

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
/// 对位 C# SortedSetObjectImpl 的 SortedSetAdd 臂
/// (:163) `if (score == scoreStored) { _ = TryRemoveExpiration
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
/// 契约对标 C# SortedSetObjectImpl 的 SortedSetIncrement 缺席臂
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
/// 契约对标 C# SortedSetObjectImpl 的 SortedSetIncrement / SortedSetAdd INCR：
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
  promote_at(
    &rt,
    &store,
    key,
    GarnetObjectType::SortedSet,
    ents,
    i64::MAX,
  );

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
