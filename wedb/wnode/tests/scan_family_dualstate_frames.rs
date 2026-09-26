//! SCAN 族（HSCAN/SSCAN/ZSCAN）双态应答帧逐字节回归（task/ing/
//! wnode-scanfam-materialize-dualstate.md）
//!
//! 直写改造（帧头预留-回填 + emit 回调借切片直写）后，应答帧必须与旧
//! owned Vec 二次写出的帧逐字节全等。信封态迭代序源自 gxhash 随机种子
//! （进程级随机），故信封态逐字节断言只取「序无关形」：至多发出 1 条目
//! （MATCH 恰命中单成员 / 空匹配）或空产出（`*0\r\n` + 收敛游标）；分层态
//! 树内字典序恒定，多成员整帧逐字节可断。双态等价（同数据集升阶前后）
//! 同样取序无关形逐字节比对。
//!
//! 覆盖面（对齐票面测试验证点）：MATCH 过滤、NOVALUES、COUNT 截断（含
//! count=0 首个未命中即停的上游怪癖与负 COUNT 全量）、游标收敛含到期垫数
//! 归零、空集 `*0\r\n`、ZSCAN inf 文本分值项（format_double "inf"/"-inf"）、CONFIG SET
//! object-scan-count-limit 钳制边界、起始游标越过总量、缺键 `[0, []]`。
//!
//! 自研依据: 双态应答帧逐字节全等回归（C# 服务端面对标 test/standalone/
//! Garnet.test/RespScanCommandsTests.cs 游标面）

use std::{iter::once, thread::sleep, time::Duration};

use wbase::time::now_ticks;
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::member_ttl::{decode_member, encode_member},
};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, MetaValue};

/// 扫描双态用例组：（命令，信封键，升阶键，完整参数尾段含游标）
type ScanCase = (
  RespCommand,
  &'static [u8],
  &'static [u8],
  &'static [&'static [u8]],
);

/// 到期垫数刻度跨度（直挂过去刻度，与真实 now 保持足够间隔免慢机竞态）
const EXPIRED_SPAN: i64 = 1_000_000;

/// 慢路径命令同步求值并回帧字节（与 tiered_field_ttl 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构的编码形态）
fn promote(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, next_expiry, false))
    .unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

fn bulk(v: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", v.len()).into_bytes();
  out.extend_from_slice(v);
  out.extend_from_slice(b"\r\n");
  out
}

/// SCAN 族应答帧组装：`*2\r\n` + 游标 bulk + 条目数组头 + 逐条目帧
fn scan_frame(cursor: i64, items: &[Vec<u8>]) -> Vec<u8> {
  let mut out = b"*2\r\n".to_vec();
  out.extend_from_slice(&bulk(cursor.to_string().as_bytes()));
  out.extend_from_slice(&format!("*{}\r\n", items.len()).into_bytes());
  for item in items {
    out.extend_from_slice(item);
  }
  out
}

/// 解析 SCAN 族应答帧 → (游标, 条目字节列表；null 项以空 Vec 占位)
fn parse_scan(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "外层应为 *2: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "条目数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    if hdr == "$-1" {
      items.push(Vec::new());
    } else {
      let len: usize = hdr[1..].parse().unwrap();
      let val = parts.next().unwrap().as_bytes().to_vec();
      assert_eq!(val.len(), len);
      items.push(val);
    }
  }
  (cursor, items)
}

/// 信封三键固定小数据集：h3={f1..f3}、s3={m1..m3}、z3={a:1.5, b:2, i:+inf}
fn envelope_fixtures(env: &TestEnv, s: &mut RespServerSession) {
  assert_eq!(
    auto_exec(
      env,
      s,
      RespCommand::Hset,
      &[b"h3", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(env, s, RespCommand::Sadd, &[b"s3", b"m1", b"m2", b"m3"]),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(
      env,
      s,
      RespCommand::Zadd,
      &[b"z3", b"1.5", b"a", b"2", b"b"]
    ),
    b":2\r\n"
  );
  // ZINCRBY +inf 造 inf 分值成员（±inf 词形放行，tiered_field_ttl 同法）
  assert_eq!(
    auto_exec(env, s, RespCommand::Zincrby, &[b"z3", b"+inf", b"i"]),
    bulk(b"inf")
  );
}

/// 信封态逐字节回归（序无关形：单命中 / 空匹配 / 怪癖截断 / 越界游标 / 缺键）
#[test]
fn envelope_scan_frames_byte_exact() {
  let env = tiered_env("scan-frame-env.db");
  let mut s = session_with(&env);
  envelope_fixtures(&env, &mut s);

  // 缺键：C# NOTFOUND → [0, []]
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"missing", b"0"]),
    scan_frame(0, &[])
  );

  // 空匹配全量遍历：游标归零 + 空数组（*0\r\n）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sscan,
      &[b"s3", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"z3", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );

  // MATCH 恰命中单成员（全量遍历，单条目发出，序无关）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"MATCH", b"f2"]
    ),
    scan_frame(0, &[bulk(b"f2"), bulk(b"v2")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"MATCH", b"f2", b"NOVALUES"]
    ),
    scan_frame(0, &[bulk(b"f2")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sscan,
      &[b"s3", b"0", b"MATCH", b"m2"]
    ),
    scan_frame(0, &[bulk(b"m2")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"z3", b"0", b"MATCH", b"a"]
    ),
    scan_frame(0, &[bulk(b"a"), bulk(b"1.5")])
  );

  // ZSCAN inf 分值：统一走 format_double 文本化（"inf" 文本 bulk 项）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"z3", b"0", b"MATCH", b"i"]
    ),
    scan_frame(0, &[bulk(b"i"), bulk(b"inf")])
  );

  // count=0 首个未命中即停（上游怪癖）：首条目即不匹配 → 空页 + 游标 1
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"COUNT", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(1, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sscan,
      &[b"s3", b"0", b"COUNT", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(1, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"z3", b"0", b"COUNT", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(1, &[])
  );

  // 负 COUNT 恒不命中截断 → 全量遍历（空匹配 / 单命中形逐字节可断）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"COUNT", b"-5", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"z3", b"0", b"COUNT", b"-5", b"MATCH", b"i"]
    ),
    scan_frame(0, &[bulk(b"i"), bulk(b"inf")])
  );

  // 起始游标越过总量：空页 + 游标归零
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"h3", b"99"]),
    scan_frame(0, &[])
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Sscan, &[b"s3", b"99"]),
    scan_frame(0, &[])
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"z3", b"99"]),
    scan_frame(0, &[])
  );

  // 非法游标 / COUNT 非整数：解析臂错误帧
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"h3", b"-1"]),
    b"-ERR invalid cursor\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h3", b"0", b"COUNT", b"xyz"]
    ),
    b"-ERR value is not an integer or out of range.\r\n"
  );

  // COUNT 截断中间页（内容随信封迭代序进程随机，取结构断言）：
  // NOVALUES COUNT 1 → 单条目 + 续页游标 1
  let out = auto_exec(
    &env,
    &mut s,
    RespCommand::Hscan,
    &[b"h3", b"0", b"COUNT", b"1", b"NOVALUES"],
  );
  let (cursor, items) = parse_scan(&out);
  assert_eq!(items.len(), 1, "COUNT 1 应截断到单字段");
  assert_eq!(cursor, 1, "截断中间页须保留续页游标");

  // 游标帧位宽不匹配搬移：10 字段集合按 digits(10) 预留 2 位游标位（7B），
  // 空匹配收敛游标 0 仅 1 位（6B）→ 回填左移收窄一档，帧不得受损
  let mut args10: Vec<Vec<u8>> = vec![b"h10".to_vec()];
  for i in 0..10 {
    args10.push(format!("g{i}").into_bytes());
    args10.push(b"v".to_vec());
  }
  let slices10: Vec<&[u8]> = args10.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &slices10),
    b":10\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"h10", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );
}

/// 信封态到期垫数窗口（存活数 L < start <= 含到期总数 N）：到期字段滞留
/// 字典垫高计数（HPEXPIRE 短 TTL + 等待，只读扫描不清到期），HSCAN 起始
/// 游标落窗口内须归零收敛，不得回原游标死锁
#[test]
fn envelope_scan_expired_padding_converges() {
  let env = tiered_env("scan-frame-pad.db");
  let mut s = session_with(&env);
  let mut args: Vec<Vec<u8>> = vec![b"hpad".to_vec()];
  for i in 1..=6 {
    args.push(format!("k{i}").into_bytes());
    args.push(b"v".to_vec());
  }
  let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &slices),
    b":6\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hpexpire,
      &[b"hpad", b"300", b"FIELDS", b"2", b"k1", b"k2"]
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(600));

  // L=4 存活、N=6 含垫数，start=5 ∈ (L, N]：空页 + 游标归零（死锁死角修复面）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"hpad", b"5"]),
    scan_frame(0, &[])
  );
  // 全量页：4 存活字段成对（内容序进程随机，取结构断言）
  let out = auto_exec(&env, &mut s, RespCommand::Hscan, &[b"hpad", b"0"]);
  let (cursor, items) = parse_scan(&out);
  assert_eq!(cursor, 0);
  assert_eq!(items.len(), 8, "4 存活字段应成对发出，到期字段不可见");
  let mut fields: Vec<Vec<u8>> = items.chunks(2).map(|c| c[0].clone()).collect();
  fields.sort();
  assert_eq!(
    fields,
    vec![
      b"k3".to_vec(),
      b"k4".to_vec(),
      b"k5".to_vec(),
      b"k6".to_vec()
    ],
    "到期字段 k1/k2 不得出现在应答"
  );
}

/// 分层态逐字节回归（树内字典序恒定，多成员整帧可断）：
/// 全量 / NOVALUES / COUNT 分页续页 / count=0 / 负 COUNT / inf 与坏载荷
/// null 项 / 到期垫数 / CONFIG SET 钳制
#[test]
fn tiered_scan_frames_byte_exact() {
  let env = tiered_env("scan-frame-tiered.db");
  let mut s = session_with(&env);

  promote(
    &env,
    b"th",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"ts",
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"tz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
      (
        b"i".to_vec(),
        encode_member(&f64::INFINITY.to_be_bytes(), None),
      ),
      (
        b"x".to_vec(),
        encode_member(&(-f64::INFINITY).to_be_bytes(), None),
      ),
    ],
    i64::MAX,
  );

  // 全量：字典序整帧
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"th", b"0"]),
    scan_frame(
      0,
      &[
        bulk(b"f1"),
        bulk(b"v1"),
        bulk(b"f2"),
        bulk(b"v2"),
        bulk(b"f3"),
        bulk(b"v3")
      ]
    )
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"th", b"0", b"NOVALUES"]
    ),
    scan_frame(0, &[bulk(b"f1"), bulk(b"f2"), bulk(b"f3")])
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Sscan, &[b"ts", b"0"]),
    scan_frame(0, &[bulk(b"m1"), bulk(b"m2"), bulk(b"m3")])
  );
  // ZSCAN：±inf 分值统一走 format_double 输出 "inf"/"-inf" 文本 bulk 项
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"tz", b"0"]),
    scan_frame(
      0,
      &[
        bulk(b"a"),
        bulk(b"1.5"),
        bulk(b"b"),
        bulk(b"2"),
        bulk(b"i"),
        bulk(b"inf"),
        bulk(b"x"),
        bulk(b"-inf")
      ]
    )
  );

  // COUNT 分页：截断页保留续页游标，续页至收敛
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"th", b"0", b"COUNT", b"2"]
    ),
    scan_frame(2, &[bulk(b"f1"), bulk(b"v1"), bulk(b"f2"), bulk(b"v2")])
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"th", b"2"]),
    scan_frame(0, &[bulk(b"f3"), bulk(b"v3")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sscan,
      &[b"ts", b"0", b"COUNT", b"1"]
    ),
    scan_frame(1, &[bulk(b"m1")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"tz", b"0", b"COUNT", b"2"]
    ),
    scan_frame(2, &[bulk(b"a"), bulk(b"1.5"), bulk(b"b"), bulk(b"2")])
  );

  // count=0 首个未命中即停 + 负 COUNT 全量（与信封态同字节）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"th", b"0", b"COUNT", b"0", b"MATCH", b"zz*"]
    ),
    scan_frame(1, &[])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"tz", b"0", b"COUNT", b"-5", b"MATCH", b"zz*"]
    ),
    scan_frame(0, &[])
  );

  // 起始游标越过总量：空页 + 游标归零
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"tz", b"99"]),
    scan_frame(0, &[])
  );

  // CONFIG SET object-scan-count-limit 钳制边界：热更上限 2 单轮至多 2 条
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::ConfigSet,
      &[b"object-scan-count-limit", b"2"]
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"th", b"0", b"NOVALUES", b"COUNT", b"100"]
    ),
    scan_frame(2, &[bulk(b"f1"), bulk(b"f2")])
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::ConfigSet,
      &[b"object-scan-count-limit", b"1000"]
    ),
    b"+OK\r\n"
  );

  // 到期垫数：6 字段含 2 条过去刻度记录，L=4、N=6，start=5 归零收敛
  let past = now_ticks() - EXPIRED_SPAN;
  let mut padded: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  for i in 1..=6 {
    let expiry = if i <= 2 { Some(past) } else { None };
    padded.push((format!("k{i}").into_bytes(), encode_member(b"v", expiry)));
  }
  promote(&env, b"thpad", GarnetObjectType::Hash, padded, past);
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"thpad", b"5"]),
    scan_frame(0, &[])
  );
  // 全量页：到期字段不可见，4 存活对 + 游标归零（4+2>=6 收敛）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"thpad", b"0"]),
    scan_frame(
      0,
      &[
        bulk(b"k3"),
        bulk(b"v"),
        bulk(b"k4"),
        bulk(b"v"),
        bulk(b"k5"),
        bulk(b"v"),
        bulk(b"k6"),
        bulk(b"v")
      ]
    )
  );

  // 帧位宽不匹配搬移（分层 12 字段）：游标位按 digits(12) 预留 2 位、全量
  // （COUNT 100 显式越过默认 COUNT 10 截断）收敛游标 0 仅 1 位（左移收窄）；
  // NOVALUES MATCH 过滤后条目头 12 位预留收窄到实际命中数——两档搬移后
  // 整帧仍逐字节定型
  let mut big: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  for i in 1..=12 {
    big.push((format!("f{i:02}").into_bytes(), encode_member(b"v", None)));
  }
  promote(&env, b"tbig", GarnetObjectType::Hash, big, i64::MAX);
  let want_all: Vec<Vec<u8>> = (1..=12)
    .flat_map(|i| vec![bulk(format!("f{i:02}").as_bytes()), bulk(b"v")])
    .collect();
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"tbig", b"0", b"COUNT", b"100"]
    ),
    scan_frame(0, &want_all)
  );
  let want_nov: Vec<Vec<u8>> = (1..=9)
    .map(|i| bulk(format!("f{i:02}").as_bytes()))
    .collect();
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"tbig", b"0", b"NOVALUES", b"MATCH", b"f0?"]
    ),
    scan_frame(0, &want_nov)
  );
}

/// 双态逐字节全等（同数据集、序无关形）：信封态与升阶后分层态应答帧
/// 逐字节相同——MATCH 单命中 / 空匹配 / NOVALUES / count=0 / 负 COUNT /
/// inf null 项 / 越界游标 / 到期垫数窗口
#[test]
fn dual_state_scan_frames_byte_identical() {
  let env = tiered_env("scan-frame-dual.db");
  let mut s = session_with(&env);
  envelope_fixtures(&env, &mut s);

  // 同数据集升阶键（手工 promote 与信封数据一致）
  promote(
    &env,
    b"th",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"ts",
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"tz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
      (
        b"i".to_vec(),
        encode_member(&f64::INFINITY.to_be_bytes(), None),
      ),
    ],
    i64::MAX,
  );

  // (命令, 信封键, 升阶键, 完整参数尾段含游标) 逐组比对
  let cases: &[ScanCase] = &[
    // 空匹配全量 + 游标归零
    (RespCommand::Hscan, b"h3", b"th", &[b"0", b"MATCH", b"zz*"]),
    (RespCommand::Sscan, b"s3", b"ts", &[b"0", b"MATCH", b"zz*"]),
    (RespCommand::Zscan, b"z3", b"tz", &[b"0", b"MATCH", b"zz*"]),
    // MATCH 单命中（含 NOVALUES 与 inf 分值项）
    (RespCommand::Hscan, b"h3", b"th", &[b"0", b"MATCH", b"f2"]),
    (
      RespCommand::Hscan,
      b"h3",
      b"th",
      &[b"0", b"MATCH", b"f2", b"NOVALUES"],
    ),
    (RespCommand::Sscan, b"s3", b"ts", &[b"0", b"MATCH", b"m2"]),
    (RespCommand::Zscan, b"z3", b"tz", &[b"0", b"MATCH", b"a"]),
    (RespCommand::Zscan, b"z3", b"tz", &[b"0", b"MATCH", b"i"]),
    // count=0 首个未命中即停（游标 1 + 空页）
    (
      RespCommand::Hscan,
      b"h3",
      b"th",
      &[b"0", b"COUNT", b"0", b"MATCH", b"zz*"],
    ),
    (
      RespCommand::Sscan,
      b"s3",
      b"ts",
      &[b"0", b"COUNT", b"0", b"MATCH", b"zz*"],
    ),
    (
      RespCommand::Zscan,
      b"z3",
      b"tz",
      &[b"0", b"COUNT", b"0", b"MATCH", b"zz*"],
    ),
    // 负 COUNT 全量（空匹配形）
    (
      RespCommand::Hscan,
      b"h3",
      b"th",
      &[b"0", b"COUNT", b"-5", b"MATCH", b"zz*"],
    ),
    // 起始游标越过总量
    (RespCommand::Hscan, b"h3", b"th", &[b"99"]),
    (RespCommand::Sscan, b"s3", b"ts", &[b"99"]),
    (RespCommand::Zscan, b"z3", b"tz", &[b"99"]),
  ];
  for (cmd, env_key, tiered_key, tail) in cases {
    let mut env_args: Vec<&[u8]> = vec![env_key];
    env_args.extend_from_slice(tail);
    let mut tiered_args: Vec<&[u8]> = vec![tiered_key];
    tiered_args.extend_from_slice(tail);
    assert_eq!(
      auto_exec(&env, &mut s, *cmd, &env_args),
      auto_exec(&env, &mut s, *cmd, &tiered_args),
      "{cmd} {tiered_key:?} {tail:?} 双态应答帧应逐字节全等"
    );
  }

  // 到期垫数窗口双态：信封 6 字段（2 到期滞留）与分层同构，start=5 双态 [0, []]
  let mut args: Vec<Vec<u8>> = vec![b"hpad".to_vec()];
  for i in 1..=6 {
    args.push(format!("k{i}").into_bytes());
    args.push(b"v".to_vec());
  }
  let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &slices),
    b":6\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hpexpire,
      &[b"hpad", b"300", b"FIELDS", b"2", b"k1", b"k2"]
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(600));

  let past = now_ticks() - EXPIRED_SPAN;
  let mut padded: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  for i in 1..=6 {
    let expiry = if i <= 2 { Some(past) } else { None };
    padded.push((format!("k{i}").into_bytes(), encode_member(b"v", expiry)));
  }
  promote(&env, b"thpad", GarnetObjectType::Hash, padded, past);

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"hpad", b"5"]),
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"thpad", b"5"]),
    "到期垫数窗口双态应答帧应逐字节全等"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"hpad", b"5"]),
    scan_frame(0, &[]),
    "垫数窗口 start ∈ (L, N] 双态均须归零收敛"
  );
}

/// ZADD ±inf 后，ZSCAN 与 ZRANGE WITHSCORES 应答项逐字节一致回归：
/// 1. ZADD k +inf m_pos、ZADD k -inf m_neg；
/// 2. RESP2/RESP3 双协议断言：分值项为 "inf"/"-inf" 文本形态（RESP2 为 bulk string，
///    ZSCAN 与 ZRANGE WITHSCORES 逐分值项字节全等；RESP3 ZSCAN 维持 bulk string、
///    ZRANGE 走 double numeric，两端均含 "inf"/"-inf" 文本且绝非 null）；
/// 3. 分层升阶后同组用例复跑。
#[test]
fn test_zscan_non_finite_score_format_and_zrange_parity() {
  let env = tiered_env("scan-inf-zrange-parity.db");
  let mut s = session_with(&env);

  let key = b"zset_inf";
  // 1. ZADD k +inf m_pos 与 ZADD k -inf m_neg 写入信封态
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zadd, &[key, b"+inf", b"m_pos"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zadd, &[key, b"-inf", b"m_neg"]),
    b":1\r\n"
  );

  // 验证闭包：对当前状态（信封态 / 升阶分层态）执行 RESP2/RESP3 断言
  let run_assertions = |s: &mut RespServerSession, is_tiered: bool| {
    // ---- RESP2 协议断言 ----
    s.resp_protocol_version = 2;

    // ZRANGE key 0 -1 WITHSCORES：按分值全序升序排（-inf < +inf），输出 m_neg(-inf) 再 m_pos(+inf)
    let zrange_resp2 = auto_exec(
      &env,
      s,
      RespCommand::Zrange,
      &[key, b"0", b"-1", b"WITHSCORES"],
    );
    assert_eq!(
      zrange_resp2, b"*4\r\n$5\r\nm_neg\r\n$4\r\n-inf\r\n$5\r\nm_pos\r\n$3\r\ninf\r\n",
      "RESP2 ZRANGE WITHSCORES 应答项应为 -inf 与 inf 文本 bulk string"
    );

    // ZSCAN key 0：单条目 MATCH 隔离成员序，逐项验证分值与 ZRANGE 单项应答逐字节一致
    let zscan_pos_resp2 = auto_exec(
      &env,
      s,
      RespCommand::Zscan,
      &[key, b"0", b"MATCH", b"m_pos"],
    );
    assert_eq!(
      zscan_pos_resp2,
      scan_frame(0, &[bulk(b"m_pos"), bulk(b"inf")]),
      "RESP2 ZSCAN +inf 项应为 \"inf\" 文本 bulk string"
    );
    let zrange_pos_resp2 = auto_exec(
      &env,
      s,
      RespCommand::Zrange,
      &[key, b"-1", b"-1", b"WITHSCORES"],
    );
    // ZSCAN MATCH m_pos 的条目部分与单元素 ZRANGE WITHSCORES 逐字节全等
    let (_, pos_items) = parse_scan(&zscan_pos_resp2);
    assert_eq!(pos_items, vec![b"m_pos".to_vec(), b"inf".to_vec()]);
    assert_eq!(
      zrange_pos_resp2, b"*2\r\n$5\r\nm_pos\r\n$3\r\ninf\r\n",
      "ZRANGE 单项分值应与 ZSCAN 分值项逐字节一致（$3\\r\\ninf\\r\\n）"
    );

    let zscan_neg_resp2 = auto_exec(
      &env,
      s,
      RespCommand::Zscan,
      &[key, b"0", b"MATCH", b"m_neg"],
    );
    assert_eq!(
      zscan_neg_resp2,
      scan_frame(0, &[bulk(b"m_neg"), bulk(b"-inf")]),
      "RESP2 ZSCAN -inf 项应为 \"-inf\" 文本 bulk string"
    );
    let zrange_neg_resp2 = auto_exec(
      &env,
      s,
      RespCommand::Zrange,
      &[key, b"0", b"0", b"WITHSCORES"],
    );
    let (_, neg_items) = parse_scan(&zscan_neg_resp2);
    assert_eq!(neg_items, vec![b"m_neg".to_vec(), b"-inf".to_vec()]);
    assert_eq!(
      zrange_neg_resp2, b"*2\r\n$5\r\nm_neg\r\n$4\r\n-inf\r\n",
      "ZRANGE 单项分值应与 ZSCAN 分值项逐字节一致（$4\\r\\n-inf\\r\\n）"
    );

    // 分层态下成员按树内字典序恒定（m_neg < m_pos），ZSCAN 全量项数组与 ZRANGE 全量整帧逐字节全等
    if is_tiered {
      let zscan_full = auto_exec(&env, s, RespCommand::Zscan, &[key, b"0"]);
      let expected_scan_full = scan_frame(
        0,
        &[bulk(b"m_neg"), bulk(b"-inf"), bulk(b"m_pos"), bulk(b"inf")],
      );
      assert_eq!(zscan_full, expected_scan_full);
      // ZSCAN 内部条目数组段与 ZRANGE 结果逐字节全等
      let prefix_len = b"*2\r\n$1\r\n0\r\n".len();
      assert_eq!(&zscan_full[prefix_len..], &zrange_resp2[..]);
    }

    // ---- RESP3 协议断言 ----
    s.resp_protocol_version = 3;

    let zscan_pos_resp3 = auto_exec(
      &env,
      s,
      RespCommand::Zscan,
      &[key, b"0", b"MATCH", b"m_pos"],
    );
    assert_eq!(
      zscan_pos_resp3,
      scan_frame(0, &[bulk(b"m_pos"), bulk(b"inf")]),
      "RESP3 ZSCAN +inf 项应维持 bulk string \"inf\"，绝非 RESP3 null (_)"
    );
    let zscan_neg_resp3 = auto_exec(
      &env,
      s,
      RespCommand::Zscan,
      &[key, b"0", b"MATCH", b"m_neg"],
    );
    assert_eq!(
      zscan_neg_resp3,
      scan_frame(0, &[bulk(b"m_neg"), bulk(b"-inf")]),
      "RESP3 ZSCAN -inf 项应维持 bulk string \"-inf\"，绝非 RESP3 null (_)"
    );

    let zrange_resp3 = auto_exec(
      &env,
      s,
      RespCommand::Zrange,
      &[key, b"0", b"-1", b"WITHSCORES"],
    );
    assert_eq!(
      zrange_resp3, b"*2\r\n*2\r\n$5\r\nm_neg\r\n,-inf\r\n*2\r\n$5\r\nm_pos\r\n,inf\r\n",
      "RESP3 ZRANGE WITHSCORES 应答项应为 ,-inf 与 ,inf 文本数值形态"
    );
  };

  // 阶段 1：内存信封态断言
  run_assertions(&mut s, false);

  // 阶段 2：升阶为分层 bftree 态，同组用例复跑
  promote(
    &env,
    key,
    GarnetObjectType::SortedSet,
    vec![
      (
        b"m_neg".to_vec(),
        encode_member(&(-f64::INFINITY).to_be_bytes(), None),
      ),
      (
        b"m_pos".to_vec(),
        encode_member(&f64::INFINITY.to_be_bytes(), None),
      ),
    ],
    i64::MAX,
  );

  run_assertions(&mut s, true);
}

/// 分层 SCAN 族迁移 claim 窗回退快照逐字节回归 + claim-fallback 臂
/// items_upper 越缝帧头加宽缝（票 zcode-r127c-setscan1 执行注记落点）：
/// `exec_tiered_scan` 锁窗内刷新单点落地后，claim 在册三态与
/// `tiered_guard` 读臂逐臂同形——路由装载门 MigrationBusy 经读臂装载单点
/// [`load_collection_stub_for_read`](wnode::resp::tiered_collection_ops)
/// 回退未门禁装载快照，锁内刷新同判回退照常扫（读面不扩大忙拒面）。
/// 修复前三态不可达：装载门 MigrationBusy 直接折 `-ERR` 存储错误帧（红形）。
/// 越缝形：迁移窗内装载快照 size 陈旧于现行树（窗前已增长、claim 后元记录
/// 未及回写），帧头上界按陈旧 size 预留、实扫条目越界——wresp 预留-回填
/// 帧头加宽（resize + copy_within 搬移）兜底，整帧仍逐字节定型
#[test]
fn tiered_scan_claim_window_snapshot_fallback_frames() {
  let env = tiered_env("scan-frame-claim-window.db");
  let mut s = session_with(&env);

  // 基线数据（与 tiered_scan_frames_byte_exact 同谱系，字典序恒定可逐字节断）
  promote(
    &env,
    b"cs",
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"ch",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"cz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
    i64::MAX,
  );
  // 越缝夹具：12 字段分层哈希（全量出帧 24 项）
  let mut big: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  for i in 1..=12 {
    big.push((format!("f{i:02}").into_bytes(), encode_member(b"v", None)));
  }
  promote(&env, b"cb12", GarnetObjectType::Hash, big, i64::MAX);

  let want_s = scan_frame(0, &[bulk(b"m1"), bulk(b"m2"), bulk(b"m3")]);
  let want_h = scan_frame(
    0,
    &[
      bulk(b"f1"),
      bulk(b"v1"),
      bulk(b"f2"),
      bulk(b"v2"),
      bulk(b"f3"),
      bulk(b"v3"),
    ],
  );
  let want_z = scan_frame(0, &[bulk(b"a"), bulk(b"1.5"), bulk(b"b"), bulk(b"2")]);
  let want_big: Vec<Vec<u8>> = (1..=12)
    .flat_map(|i| vec![bulk(format!("f{i:02}").as_bytes()), bulk(b"v")])
    .collect();
  let want_big_full = scan_frame(0, &want_big);
  let want_big_mid = scan_frame(2, &[bulk(b"f01"), bulk(b"v"), bulk(b"f02"), bulk(b"v")]);

  // 基线采集（未开窗，锁窗内刷新现势值）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Sscan, &[b"cs", b"0"]),
    want_s
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"ch", b"0"]),
    want_h
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"cz", b"0"]),
    want_z
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"cb12", b"0", b"COUNT", b"100"]
    ),
    want_big_full
  );

  // 手工登记迁移 claim（确定性开窗，wbftree 公开判据原语，非 mock）
  let mgr = env.store.range_index();
  let meta_keys: Vec<Vec<u8>> = {
    let sess = env.store.new_session().unwrap();
    [b"cs".as_slice(), b"ch", b"cz", b"cb12"]
      .into_iter()
      .map(|k| sess.session_meta_key(k).to_vec())
      .collect()
  };
  for mk in &meta_keys {
    assert!(mgr.try_claim_migration(mk), "claim 应登记成功");
  }

  // claim 窗内：三域 SCAN 一律回退装载快照照常执行，应答与基线逐字节全等
  //（修复前红形：装载门 MigrationBusy 直折 -ERR 存储错误帧）
  let cases: &[(RespCommand, &[u8], Vec<u8>)] = &[
    (RespCommand::Sscan, b"cs", want_s.clone()),
    (RespCommand::Hscan, b"ch", want_h.clone()),
    (RespCommand::Zscan, b"cz", want_z.clone()),
  ];
  for &(cmd, key, ref want) in cases {
    let out = auto_exec(&env, &mut s, cmd, &[key, b"0"]);
    assert_eq!(out, *want, "claim 窗 {cmd} 应回退快照照常出基线帧");
  }
  // COUNT 截断续页窗内同形
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"cb12", b"0", b"COUNT", b"2"]
    ),
    want_big_mid,
    "claim 窗 COUNT 截断页应回退快照照常出帧"
  );

  // items_upper 越缝：窗内把元记录 size 覆旧为 3（模拟「装载快照陈旧于现行
  // 12 字段树」的窗前增长-窗内读态；upsert_raw 真实原语改树侧不动的元域），
  // 帧头上界按陈旧 size 预留（Hash ×2 = 6），实扫 24 项越缝 → 预留-回填
  // 加宽搬移，整帧仍与全量基线逐字节全等、游标经 total=3 归零收敛
  let cb12_meta = meta_keys.last().unwrap();
  let orig_meta_record = env
    .rt
    .block_on(env.store.new_session().unwrap().read_raw(cb12_meta))
    .unwrap()
    .expect("分层元记录应在册");
  let mut stale = orig_meta_record.clone();
  let mut meta = MetaValue::from_slice(&stale[..wval::META_VALUE_SIZE]).unwrap();
  assert_eq!(meta.size, 12);
  meta.size = 3;
  stale[..wval::META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
  env
    .rt
    .block_on(
      env
        .store
        .new_session()
        .unwrap()
        .upsert_raw(cb12_meta, &stale),
    )
    .unwrap();
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"cb12", b"0", b"COUNT", b"100"]
    ),
    want_big_full,
    "claim 窗陈旧上界越缝出帧应经预留-回填加宽仍逐字节定型"
  );
  // 复原元记录（真实原语回写），出窗后一切原态
  env
    .rt
    .block_on(
      env
        .store
        .new_session()
        .unwrap()
        .upsert_raw(cb12_meta, &orig_meta_record),
    )
    .unwrap();
  for mk in &meta_keys {
    mgr.release_migration_claim(mk);
  }
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"cb12", b"0", b"COUNT", b"100"]
    ),
    want_big_full,
    "出窗后全量帧应复原基线"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Sscan, &[b"cs", b"0"]),
    want_s
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"ch", b"0"]),
    want_h
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"cz", b"0"]),
    want_z
  );
}

/// 案一：COUNT i32 下溢极值翻倍回绕不复刻双态锁（票 zcode-r147c-hscanmt，
/// 登记见 doc/zh/deviations.md 第 20 条 d)）
///
/// C# int32 unchecked 侧 `count * 2` 在 COUNT=-2147483648 回绕恰 0，退化为
/// 「首条未命中即停」空页爬行形态；rust i64 加宽永不回绕，负 COUNT 六档
/// （-2147483648 / -2147483647 / -1073741825 / -1073741824 / -1，含 C# 回绕
/// 为正大数的 -2147483647 与 -1073741825 两支——C# 该二档在模式全命中形下
/// 分别截断于 2 项与 2147483646 项）与默认 10 档一律单页全量 + 游标 bulk
/// "0"，SSCAN 判净臂（直比无翻倍）同形入锁。COUNT=0 档锁上游怪癖形
/// （MATCH 不命中→首条即停 `[1, []]`；MATCH 全命中→ emitted 恒不等于 0
/// 归全量），且显式锁 MIN 档应答 ≠ COUNT=0 档应答——该二象正是 rust 不回绕
/// 与 C# 回绕的分叉点，严禁按 C# 回改。信封态与分层态双态全等门：空产出
/// 形逐字节全等，多产出形游标同值 + 条目集全等（信封迭代序进程随机）。
#[test]
fn count_i32_min_no_wraparound_lock() {
  let env = tiered_env("scan-frame-countwrap.db");
  let mut s = session_with(&env);
  envelope_fixtures(&env, &mut s);
  promote(
    &env,
    b"th",
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"ts",
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );
  promote(
    &env,
    b"tz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
      (
        b"i".to_vec(),
        encode_member(&f64::INFINITY.to_be_bytes(), None),
      ),
    ],
    i64::MAX,
  );

  type ScanCase = (
    RespCommand,
    &'static [u8],
    &'static [u8],
    &'static [u8],
    &'static [u8],
    Vec<u8>,
  );
  let cases: &[ScanCase] = &[
    (
      RespCommand::Hscan,
      b"h3",
      b"th",
      b"f?",
      b"zz*",
      scan_frame(
        0,
        &[
          bulk(b"f1"),
          bulk(b"v1"),
          bulk(b"f2"),
          bulk(b"v2"),
          bulk(b"f3"),
          bulk(b"v3"),
        ],
      ),
    ),
    (
      RespCommand::Sscan,
      b"s3",
      b"ts",
      b"m?",
      b"zz*",
      scan_frame(0, &[bulk(b"m1"), bulk(b"m2"), bulk(b"m3")]),
    ),
    (
      RespCommand::Zscan,
      b"z3",
      b"tz",
      b"?",
      b"zz*",
      scan_frame(
        0,
        &[
          bulk(b"a"),
          bulk(b"1.5"),
          bulk(b"b"),
          bulk(b"2"),
          bulk(b"i"),
          bulk(b"inf"),
        ],
      ),
    ),
  ];

  // 负值五档（含 C# 回绕为正大数的 -2147483647 / -1073741825 两支）
  const NEG_COUNTS: &[&[u8]] = &[
    b"-2147483648",
    b"-2147483647",
    b"-1073741825",
    b"-1073741824",
    b"-1",
  ];

  for (cmd, ekey, tkey, p_all, p_none, want_full) in cases {
    let (cmd, ekey, tkey, p_all, p_none) = (*cmd, *ekey, *tkey, *p_all, *p_none);
    // 七档 = 负五档 + 默认（无 COUNT，恒 10）× 双 pattern 形
    let stages: Vec<Option<&[u8]>> = NEG_COUNTS
      .iter()
      .copied()
      .map(Some)
      .chain(once(None))
      .collect();
    for &count in &stages {
      for pattern in [p_all, p_none] {
        let mut targs: Vec<&[u8]> = vec![tkey, b"0"];
        let mut eargs: Vec<&[u8]> = vec![ekey, b"0"];
        if let Some(c) = count {
          targs.extend_from_slice(&[b"COUNT", c]);
          eargs.extend_from_slice(&[b"COUNT", c]);
        }
        targs.extend_from_slice(&[b"MATCH", pattern]);
        eargs.extend_from_slice(&[b"MATCH", pattern]);
        let tout = auto_exec(&env, &mut s, cmd, &targs);
        let eout = auto_exec(&env, &mut s, cmd, &eargs);

        // 分层态字典序恒定：负档与默认档一律逐字节锁单页遍历到底 + 游标
        // bulk "0"；全命中形锁全量帧，全不命中形锁空产出帧
        let want = if pattern == p_all {
          want_full.clone()
        } else {
          scan_frame(0, &[])
        };
        assert_eq!(
          &tout, &want,
          "{cmd} COUNT {count:?} MATCH {pattern:?} 分层态应恒单页遍历到底，\
           绝不得出现 C# int32 回绕的空页爬行形"
        );
        // 双态全等门：游标同为 0；空产出形逐字节全等，多产出形条目集全等
        let (ecursor, mut eitems) = parse_scan(&eout);
        let (tcursor, mut titems) = parse_scan(&tout);
        assert_eq!(ecursor, 0, "{cmd} {ekey:?} 信封态负档游标须归零");
        assert_eq!(tcursor, 0);
        if eitems.is_empty() {
          assert_eq!(eout, tout, "空产出形双态应逐字节全等");
        }
        eitems.sort();
        titems.sort();
        assert_eq!(eitems, titems, "{cmd} 双态条目集应全等");
      }
    }

    // COUNT=0 档怪癖双象：全不命中 → 首条即停 [1, []]；全命中 → emitted 恒
    // 不等于 0 归全量。MIN 档应答须与 COUNT=0 不命中形分叉（rust 不回绕
    // 即不归 0），该比较为 C# 回绕形态的显式反锁
    let mut min_args: Vec<&[u8]> = vec![tkey, b"0", b"COUNT", b"-2147483648", b"MATCH"];
    min_args.push(p_none);
    let min_out = auto_exec(&env, &mut s, cmd, &min_args);
    let mut zero_args: Vec<&[u8]> = vec![tkey, b"0", b"COUNT", b"0", b"MATCH"];
    zero_args.push(p_none);
    let zero_out = auto_exec(&env, &mut s, cmd, &zero_args);
    assert_eq!(
      zero_out,
      scan_frame(1, &[]),
      "{cmd} COUNT=0 全不命中应维持首条即停怪癖形（双侧同形）"
    );
    assert_ne!(
      min_out, zero_out,
      "{cmd} MIN 档 rust 按 i64 加宽恒全量，绝不得与 COUNT=0 爬行形同帧（回改即破）"
    );
    let mut zero_all: Vec<&[u8]> = vec![tkey, b"0", b"COUNT", b"0", b"MATCH"];
    zero_all.push(p_all);
    assert_eq!(
      auto_exec(&env, &mut s, cmd, &zero_all),
      *want_full,
      "{cmd} COUNT=0 全命中形 emitted 恒不等于 0，归全量"
    );
  }
}

/// i64::MAX 越界游标档锁测（票 task/ing/wnode-tiered-scan-start-cursor-overflow.md）：
/// C# 对象层 Scan 三臂入遍历前一律 `Count < start` 早退（HashObject.cs:373 /
/// SetObject.cs:195 / SortedSetObject.cs:471），rust 内存态三臂守卫在位；分层态
/// exec_tiered_scan 补同型守卫前，start = i64::MAX 且树内驻留已到期未收集成员
/// （expired >= 1）时尾段 scan_converge_cursor 判定 `cursor + expired >= total`
/// 裸 i64 加法溢出——debug 构建 overflow-checks panic、release 回绕负值恒不命中
/// 回显 i64::MAX 垃圾游标 + 空条目死循环（每轮 O(N) 全树空走）。守卫落地后
/// 双态 Hash/ZSet（含到期垫数键）与 Set 一律逐字节归零收敛出 [0, 空] 且双态
/// 全等；游标 9223372036854775808（i64::MAX + 1）经 read_scan_input strict_i64
/// 门双态同出 invalid cursor 错误帧（解析协议面零改动锁测）。既有 99 越界档、
/// claim 窗回退族与双态全等族由本文件其余用例兜底零回归
#[test]
fn tiered_scan_start_cursor_overflow_i64_max_dualstate() {
  let env = tiered_env("scan-frame-cursor-overflow.db");
  let mut s = session_with(&env);

  // 信封态到期垫数键：Hash/ZSet 各 6 成员挂 2 条短 TTL，等待到期滞留
  //（纯读扫描不出账，与分层态 encode_member 过去刻度夹具同构）
  let mut hargs: Vec<Vec<u8>> = vec![b"hpad".to_vec()];
  let mut zargs: Vec<Vec<u8>> = vec![b"zpad".to_vec()];
  for i in 1..=6 {
    hargs.push(format!("k{i}").into_bytes());
    hargs.push(b"v".to_vec());
    zargs.push(b"2".to_vec());
    zargs.push(format!("k{i}").into_bytes());
  }
  let hslices: Vec<&[u8]> = hargs.iter().map(|v| v.as_slice()).collect();
  let zslices: Vec<&[u8]> = zargs.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &hslices),
    b":6\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zadd, &zslices),
    b":6\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hpexpire,
      &[b"hpad", b"300", b"FIELDS", b"2", b"k1", b"k2"]
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zexpire,
      &[b"zpad", b"300", b"MEMBERS", b"2", b"k1", b"k2"]
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(600));

  // 分层态到期垫数键：Hash/ZSet 各 6 成员含 2 条过去刻度（expired >= 1 驻留
  // 树内，修复前 i64::MAX 档在此触发溢出加法）
  let past = now_ticks() - EXPIRED_SPAN;
  let mut padded: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  for i in 1..=6 {
    let expiry = if i <= 2 { Some(past) } else { None };
    padded.push((format!("k{i}").into_bytes(), encode_member(b"v", expiry)));
  }
  promote(&env, b"thpad", GarnetObjectType::Hash, padded.clone(), past);
  promote(
    &env,
    b"tzpad",
    GarnetObjectType::SortedSet,
    padded
      .into_iter()
      .map(|(f, v)| (f, encode_member(&2.0f64.to_be_bytes(), decode_member(&v).0)))
      .collect(),
    past,
  );
  // Set 臂：成员值为裸哨兵恒零到期（天然豁免臂），越界守卫仍须与内存态同形早退
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sadd,
      &[b"s3", b"m1", b"m2", b"m3"]
    ),
    b":3\r\n"
  );
  promote(
    &env,
    b"ts3",
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );

  // i64::MAX 游标档：守卫先于加法拦截，全部 [0, 空] 且双态逐字节全等
  //（修复前红形：分层 Hash/ZSet debug panic / release 回
  // [9223372036854775807, 空] 垃圾游标死循环）
  let cursor_max = b"9223372036854775807";
  let empty = scan_frame(0, &[]);
  for (cmd, ekey, tkey) in [
    (RespCommand::Hscan, b"hpad".as_slice(), b"thpad".as_slice()),
    (RespCommand::Zscan, b"zpad".as_slice(), b"tzpad".as_slice()),
    (RespCommand::Sscan, b"s3".as_slice(), b"ts3".as_slice()),
  ] {
    let eout = auto_exec(&env, &mut s, cmd, &[ekey, cursor_max]);
    let tout = auto_exec(&env, &mut s, cmd, &[tkey, cursor_max]);
    assert_eq!(
      tout, empty,
      "{cmd} 分层 {tkey:?} i64::MAX 越界游标 + 到期驻留应守卫早退出 [0, 空]，\
       绝不得溢出/回绕出垃圾游标"
    );
    assert_eq!(
      eout, tout,
      "{cmd} i64::MAX 越界档双态应答帧应逐字节全等（内存态守卫 ⇔ 分层守卫）"
    );
  }

  // start == total 边界（守卫不触发，恒走遍历臂）：L=4 存活 + 2 到期垫数、
  // total=6，start=6 空走 skipped 臂经尾段判定归零收敛，双态 [0, 空] 同帧
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"hpad", b"6"]),
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"thpad", b"6"]),
    "Hash start==total 边界双态应逐字节全等"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hscan, &[b"thpad", b"6"]),
    empty,
    "Hash start==total 边界应经尾段判定归零收敛出 [0, 空]"
  );

  // 游标 9223372036854775808（i64::MAX + 1）：read_scan_input strict_i64 门
  // 双态同出 invalid cursor 错误帧
  let err = b"-ERR invalid cursor\r\n";
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"hpad", b"9223372036854775808"]
    ),
    err,
    "信封 Hash 超 i64 游标应 invalid cursor"
  );
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Hscan,
      &[b"thpad", b"9223372036854775808"]
    ),
    err,
    "分层 Hash 超 i64 游标应 invalid cursor，与信封态全等"
  );
}
