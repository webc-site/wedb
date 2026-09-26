//! 分层态 SRANDMEMBER 负 count 长度契约回归（票 zcode-r143c-spopcnt 案二）
//!
//! 缺陷形：分层臂首轮自随机起点只拿到树尾后缀 a 个键，不足 |count| 时仅回绕
//! 自树头补扫一轮（单轮上限 a+d 且 a ≤ d），|count| > a+d 短供、> 2d 必短，
//! 头以实扫数诚实落帧（无流错位）但数组长度小于承诺的 |count|；同一数据集经
//! 内存态 set_object_impl 放回臂恒产恰 |count|——双态长度分叉违 doc/zh/
//! collection.md §5/§8 与 review.md §5.1（负 count 臂长度是唯一硬契约，成员集
//! 无序可重复但 |count| 为承诺值）。
//!
//! 修法：回绕补扫改为收敛循环（未达 n 即自重入树头续扫，每轮以本轮新增数为
//! 进度判据、零新增即诚实落帧早退），负 count 可重复语义容许同键多轮重复入列，
//! 不加去重、不改诚实头形；正 count 臂 n ≤ 基数，首两扫已闭包不感变更。
//!
//! 对标 C# SetObjectImpl.cs:216-236 负 count 臂 `new int[|count|]` 放回抽样恒满
//! 长度（C# 无分层态对照，分层契约自源于本仓双态同承诺文档）。

use std::str;

use wcol::SET_MEMBER_DUMMY_VALUE;
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 命令同步求值并回帧字节（快路径直答，慢路径 block_on 承担网络泵角色）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶分层 set（成员 dummy 值裸编码，与 export_entries 同构）
fn promote_set(env: &TestEnv, key: &[u8], members: &[&[u8]]) {
  let sess = env.store.new_session().unwrap();
  let entries = members
    .iter()
    .map(|m| (m.to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()))
    .collect();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::Set,
      entries,
      i64::MAX,
      false,
    ))
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

/// 解析 RESP2 bulk 数组应答为成员序列（帧须整帧消费，尾随残余即串位）
fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let head_end = frame.iter().position(|&b| b == b'\n').expect("缺数组头");
  let declared: usize = str::from_utf8(&frame[1..head_end - 1])
    .expect("数组头非 ASCII")
    .parse()
    .expect("数组头非整数");
  let mut rest = &frame[head_end + 1..];
  let mut items = Vec::with_capacity(declared);
  for _ in 0..declared {
    let len_end = rest.iter().position(|&b| b == b'\n').expect("缺 bulk 头");
    assert_eq!(rest.first(), Some(&b'$'), "成员须为 bulk 串");
    let len: usize = str::from_utf8(&rest[1..len_end - 1])
      .expect("bulk 头非 ASCII")
      .parse()
      .expect("bulk 头非整数");
    let body = &rest[len_end + 1..len_end + 1 + len];
    items.push(body.to_vec());
    rest = &rest[len_end + 1 + len + 2..];
  }
  assert!(rest.is_empty(), "帧界串位：尾随 {} 字节未消费", rest.len());
  items
}

const MEMBERS: &[&[u8]] = &[b"m1", b"m2", b"m3"];

/// 案一：分层态 |count| 超基数两倍仍恒满，成员均属集内
#[test]
fn tiered_srandmember_negative_count_length_always_full() {
  let env = tiered_env("tiered-srand-neg.db");
  let mut s = session_with(&env);
  promote_set(&env, b"ts", MEMBERS);

  // d=3，|count|=2d+1=7：单轮回绕上限 a+d ≤ 6 必短供，收敛循环补足至恰 7
  for count in [4usize, 7, 8, 37] {
    let arg = format!("-{count}");
    let out = auto_exec(
      &env,
      &mut s,
      RespCommand::Srandmember,
      &[b"ts", arg.as_bytes()],
    );
    let items = parse_bulk_array(&out);
    assert_eq!(items.len(), count, "|count|={count} 长度恒满");
    assert!(
      items.iter().all(|m| MEMBERS.contains(&m.as_slice())),
      "|count|={count} 成员须均属集内"
    );
  }
}

/// 案二：同数据集内存态与分层态负 count 应答长度同值（双态长度同构锁）
#[test]
fn memory_and_tiered_negative_count_length_parity() {
  let env = tiered_env("tiered-srand-parity.db");
  let mut s = session_with(&env);
  promote_set(&env, b"ts", MEMBERS);
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Scard, &[b"ts"]),
    b":3\r\n",
    "分层态基数应为 3"
  );

  // 内存态同数据集（未升阶键走对象臂放回抽样）
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Sadd,
      &[b"ms", MEMBERS[0], MEMBERS[1], MEMBERS[2]]
    ),
    b":3\r\n"
  );

  for count in [1usize, 3, 7, 20] {
    let arg = format!("-{count}");
    let tiered = auto_exec(
      &env,
      &mut s,
      RespCommand::Srandmember,
      &[b"ts", arg.as_bytes()],
    );
    let tiered = parse_bulk_array(&tiered);
    let memory = auto_exec(
      &env,
      &mut s,
      RespCommand::Srandmember,
      &[b"ms", arg.as_bytes()],
    );
    let memory = parse_bulk_array(&memory);
    assert_eq!(tiered.len(), count, "分层态 |count|={count} 恒满");
    assert_eq!(memory.len(), count, "内存态 |count|={count} 恒满");
    assert_eq!(
      tiered.len(),
      memory.len(),
      "双态负 count 应答长度须同值（|count|={count}）"
    );
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Scard, &[b"ts"]),
      b":3\r\n",
      "SRANDMEMBER 纯读不得侵蚀基数"
    );
  }
}

/// 案三：正 count 臂不感变更（≤ 基数互异、> 基数钳制），无 count 形单 bulk
#[test]
fn tiered_positive_and_no_count_shape_unchanged() {
  let env = tiered_env("tiered-srand-pos.db");
  let mut s = session_with(&env);
  promote_set(&env, b"ts", MEMBERS);

  // 正 count=2 → 2 个互异成员（首两扫闭包，循环不参与）
  let items = parse_bulk_array(&auto_exec(
    &env,
    &mut s,
    RespCommand::Srandmember,
    &[b"ts", b"2"],
  ));
  assert_eq!(items.len(), 2);
  let mut sorted = items.clone();
  sorted.sort();
  sorted.dedup();
  assert_eq!(sorted.len(), 2, "正 count 成员互异");
  assert!(sorted.iter().all(|m| MEMBERS.contains(&m.as_slice())));

  // 正 count 超基数 → 钳至基数（3 互异）
  let items = parse_bulk_array(&auto_exec(
    &env,
    &mut s,
    RespCommand::Srandmember,
    &[b"ts", b"9"],
  ));
  assert_eq!(items.len(), 3, "正 count 超基数钳制为基数");
  let mut sorted = items;
  sorted.sort();
  sorted.dedup();
  assert_eq!(sorted.len(), 3, "钳制后成员互异");

  // 无 count → 单 bulk（非数组帧）
  let out = auto_exec(&env, &mut s, RespCommand::Srandmember, &[b"ts"]);
  assert!(out.starts_with(b"$2\r\n"), "无 count 应回单 bulk: {out:?}");
  assert_eq!(
    out.len(),
    8,
    "无 count 单 bulk 帧全长锁（$2 头 + 2 字节成员 + CRLF）"
  );
  assert_eq!(&out[6..], b"\r\n", "单 bulk 帧尾 CRLF 锁");

  // 极值 |count| 亦须终止且满长（收敛循环防无界回归锁）
  let out = auto_exec(&env, &mut s, RespCommand::Srandmember, &[b"ts", b"-1000"]);
  assert_eq!(parse_bulk_array(&out).len(), 1_000);
}
