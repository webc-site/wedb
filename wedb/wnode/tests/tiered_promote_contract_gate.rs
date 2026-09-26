//! 升阶建树口 bftree 记录长度契约闸回归测试
//!
//! 缺陷：升阶建树口（promote_collection_to_bftree）按引擎页容量受理（不折
//! key.len()）、稳态写臂按 `validate_bftree_record`（折 key 后的存根契约）受理，
//! 两上限不同源致「能升阶、不能续写」——1008B 元素的 List 升阶成功，随后稳态
//! RPUSH 明确回 InvalidKV（命令面承诺反转）。同形残留于空成员边界：原单点只有
//! 三条长度界、无空键/空记录门，空成员过闸后才在装载内核被引擎拒，错归
//! KeyTooLong 落 error 级「装载被拒」日志——含空成员集合每次触阈写都付全套
//! O(N) 建树尝试（export + scratch 实例化 + 失败回收）且永不升阶。
//!
//! 修复语义：建树装载前逐条过与稳态写臂同一的 validate_bftree_record 单点
//!（含空键 `key.is_empty()` / 空记录 `record_len == 0` 两门，即引擎受理面
//! bf-tree insert 拒空键、write_batch 前置拒空值的精确镜像），任一越限**整批拒**
//! ——闸在建树之前，scratch 工作文件与引擎实例零创建，信封态分毫未动，半树换入
//! 结构性不可达；RMW 升阶臂 Failed 与 BudgetExhausted 同入信封回落臂，命令照常
//! ACK、键存活续写。
//!
//! 断言口径：
//! - List（16B 序号键 + 1008B 载荷 = 1025B > 1024B 记录上限）与 Set（200B 成员
//!   键 > 128B max_key_len）升阶整批拒，错误文案与稳态写臂同源逐字节一致
//!   （反向注入：撤掉建树侧契约闸后本断言转红——升阶将被引擎受理而成功）；
//! - Set/ZSet 空成员（0B 树键）升阶整批拒——错误是契约闸的 InvalidKV 存根契约
//!   文案而非装载被拒的 KeyTooLong 失真归类，即证「未进入建树段」；空成员与
//!   超长成员同待遇，拒升阶后信封态保全、空成员仍可读；
//! - 拒升阶后集合保持信封态（load_collection_stub / load_meta 双空，树态水位
//!   零伪造），后续 RPUSH/SADD 应答与从未升阶的对照键逐字节一致；
//! - 越阈写（65537 条单命令 RPUSH/SADD 触 should_promote）升阶被拒后回落信封
//!   写回，ACK 与升阶成功的对照键逐字节一致，越限成员可读、信封态保持。

use std::sync::Arc;

use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, TestStore, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 慢路径命令同步求值并回帧字节（与 tiered_watch_fence 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（与生产 export_entries 同形 entries），返回升阶结果供断言
async fn try_promote(
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), wkv::Error> {
  store
    .new_session()
    .unwrap()
    .promote_collection_to_bftree(key, obj_type, entries, i64::MAX, false)
    .await
}

/// List：16B 序号键 + 1008B 载荷 = 1025B 越记录上限 → 升阶整批拒、信封态保持、
/// 后续 RPUSH 与从未升阶的对照键逐字节一致
#[test]
fn list_promote_rejected_on_record_max_and_envelope_keeps_writing() {
  let env = tiered_env("promote_gate_list");
  let big = vec![b'x'; 1008];
  let (l, ctrl) = (b"l", b"l_ctrl");

  // 被测键与对照键同序灌入（小元素 + 1008B 越限元素）
  let w = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, w, RespCommand::Rpush, &[l, b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&env, w, RespCommand::Rpush, &[l, big.as_slice()]),
    b":2\r\n"
  );
  let c = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, c, RespCommand::Rpush, &[ctrl, b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&env, c, RespCommand::Rpush, &[ctrl, big.as_slice()]),
    b":2\r\n"
  );

  // 升阶整批拒：entries 镜像 ListObject::export_entries（16B 大端序号键 +
  // member_ttl 裸载荷编码），16 + (1008+1) = 1025 > 1024。反向注入（撤闸）
  // 后本 unwrap_err 转红——升阶将被引擎页容量受理而成功
  let entries = vec![
    (
      LIST_SEQ_BASE.to_be_bytes().to_vec(),
      encode_member(b"a", None),
    ),
    (
      (LIST_SEQ_BASE + 1).to_be_bytes().to_vec(),
      encode_member(&big, None),
    ),
  ];
  let err = env
    .rt
    .block_on(try_promote(&env.store, l, GarnetObjectType::List, entries))
    .unwrap_err();
  assert_eq!(
    err.to_string(),
    "ERR key+value size must be between 2 and 1024 bytes (got 1025), max key length 128 (got 16)",
    "拒升阶错误文案应与稳态写臂同源"
  );

  // 信封态保持：分层存根与树态元记录（水位载体）双空——升阶被拒不伪造 meta
  let sess = env.store.new_session().unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(l))
      .unwrap()
      .is_none(),
    "拒升阶后键应保持信封态"
  );
  assert!(
    env.rt.block_on(sess.load_meta(l)).unwrap().is_none(),
    "拒升阶后不得伪造树态元记录与水位"
  );

  // 拒升阶后 RPUSH 照常续写，应答与对照键（从未升阶）逐字节一致
  let out = auto_exec(&env, w, RespCommand::Rpush, &[l, b"c"]);
  let ctrl_out = auto_exec(&env, c, RespCommand::Rpush, &[ctrl, b"c"]);
  assert_eq!(out, b":3\r\n");
  assert_eq!(out, ctrl_out, "拒升阶前后 RPUSH 应答应逐字节一致");
}

/// Set：200B 成员键 > 128B max_key_len → 升阶整批拒、信封态保持、后续 SADD
/// 与从未升阶的对照键逐字节一致
#[test]
fn set_promote_rejected_on_max_key_len_and_envelope_keeps_writing() {
  let env = tiered_env("promote_gate_set");
  let big_member = vec![b'm'; 200];
  let (s, ctrl) = (b"s", b"s_ctrl");

  let w = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, w, RespCommand::Sadd, &[s, big_member.as_slice()]),
    b":1\r\n"
  );
  let c = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, c, RespCommand::Sadd, &[ctrl, big_member.as_slice()]),
    b":1\r\n"
  );

  // 升阶整批拒：entries 镜像 SetObject::export_entries（成员即树键，记录为
  // 统一哑值 b"1111"），200 + 4 = 204 在记录上下限内、但 200 > 128 键上限。
  // 反向注入（撤闸）后本 unwrap_err 转红
  let entries = vec![(big_member.clone(), SET_MEMBER_DUMMY_VALUE.to_vec())];
  let err = env
    .rt
    .block_on(try_promote(&env.store, s, GarnetObjectType::Set, entries))
    .unwrap_err();
  assert_eq!(
    err.to_string(),
    "ERR key+value size must be between 2 and 1024 bytes (got 204), max key length 128 (got 200)",
    "拒升阶错误文案应与稳态写臂同源"
  );

  let sess = env.store.new_session().unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(s))
      .unwrap()
      .is_none(),
    "拒升阶后键应保持信封态"
  );
  assert!(
    env.rt.block_on(sess.load_meta(s)).unwrap().is_none(),
    "拒升阶后不得伪造树态元记录与水位"
  );

  let out = auto_exec(&env, w, RespCommand::Sadd, &[s, b"small"]);
  let ctrl_out = auto_exec(&env, c, RespCommand::Sadd, &[ctrl, b"small"]);
  // SADD 回「新增成员数」而非基数（C# SetObjectImpl.cs:18-35 `output.result1 =
  // added`）：本例两次各新增 1，故回 :1；基数保全另由 SCARD 断言承接
  assert_eq!(out, b":1\r\n");
  assert_eq!(out, ctrl_out, "拒升阶前后 SADD 应答应逐字节一致");
  assert_eq!(
    auto_exec(&env, w, RespCommand::Scard, &[s]),
    b":2\r\n",
    "拒升阶须保信封已有成员"
  );
  assert_eq!(
    auto_exec(&env, c, RespCommand::Scard, &[ctrl]),
    b":2\r\n",
    "对照键基数应与被测键一致"
  );
}

/// 越阈写回落：单命令 65537 条 RPUSH 触 should_promote → 升阶被契约闸整批拒 →
/// RMW 臂信封回落写回（Failed 与 BudgetExhausted 同臂），ACK 与升阶成功的对照
/// 键逐字节一致，越限成员可读、键保持信封态
#[test]
fn rpush_crossing_promote_threshold_falls_back_to_envelope_and_acks_identically() {
  let env = tiered_env("promote_gate_fallback");
  let big = vec![b'x'; 1008];
  let (l, ctrl) = (b"fl", b"fl_ctrl");

  // 被测键：65536 条合规 + 1 条 1008B 越限（先推），单命令越 65536 条目阈
  let mut args: Vec<Vec<u8>> = Vec::with_capacity(65538);
  args.push(l.to_vec());
  args.push(big.clone());
  args.extend((0..65536u32).map(|i| i.to_string().into_bytes()));
  let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  let out = auto_exec(&env, &mut session_with(&env), RespCommand::Rpush, &refs);

  // 对照键：同规模（首元素换为合规载荷，总条数同为 65537）且全部合规 →
  // 升阶成功换入分层态
  let mut cargs: Vec<Vec<u8>> = Vec::with_capacity(65538);
  cargs.push(ctrl.to_vec());
  cargs.push(b"head".to_vec());
  cargs.extend((0..65536u32).map(|i| i.to_string().into_bytes()));
  let crefs: Vec<&[u8]> = cargs.iter().map(|v| v.as_slice()).collect();
  let ctrl_out = auto_exec(&env, &mut session_with(&env), RespCommand::Rpush, &crefs);

  // 命令面承诺：越阈写无论升阶成败 ACK 逐字节一致（回落信封照常受理）
  assert_eq!(out, b":65537\r\n");
  assert_eq!(out, ctrl_out, "越阈写应答不应因契约闸拒绝而变形");

  let sess = env.store.new_session().unwrap();
  // 物理面分叉：被测键回落信封、对照键已分层
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(l))
      .unwrap()
      .is_none(),
    "越阈被拒后键应回落信封态"
  );
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(ctrl))
      .unwrap()
      .is_some(),
    "全合规对照键应升阶换入分层态"
  );
  // 回落写回零丢员：首元素（1008B 越限成员）可读且逐字节还原
  let head = auto_exec(
    &env,
    &mut session_with(&env),
    RespCommand::Lrange,
    &[l, b"0", b"0"],
  );
  let mut expect = Vec::with_capacity(1008 + 16);
  expect.extend_from_slice(b"*1\r\n$1008\r\n");
  expect.extend_from_slice(&big);
  expect.extend_from_slice(b"\r\n");
  assert_eq!(head, expect, "越限成员应随信封回落完整可读");
}

/// Set：空成员（0B 树键低于引擎「Key too small」受理下限，4B dummy 记录在
/// 记录界内）→ 升阶整批拒、信封态保持、空成员可读、后续 SADD 与从未升阶的
/// 对照键逐字节一致
#[test]
fn set_promote_rejected_on_empty_member_and_envelope_keeps_writing() {
  let env = tiered_env("promote_gate_set_empty");
  let (s, ctrl) = (b"se", b"se_ctrl");

  // 内存态受理面锚定（C# SetObjectImpl.SetAdd 无条件 setLookup.Add）：
  // 空成员成功 :1——分层态拒空成员与内存态受理的双态分叉沿超长成员契约先例
  let w = &mut session_with(&env);
  assert_eq!(auto_exec(&env, w, RespCommand::Sadd, &[s, b""]), b":1\r\n");
  let c = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, c, RespCommand::Sadd, &[ctrl, b"m"]),
    b":1\r\n"
  );

  // 升阶整批拒：entries 镜像 SetObject::export_entries（成员即树键，记录为统一
  // 哑值 b"1111"），0 + 4 = 4 在记录上下限内、但空键低于引擎受理下限。错误是
  // 契约闸的 InvalidKV 存根契约文案（key_len=0）而非装载被拒的 KeyTooLong
  // 失真归类——即证闸在建树之前拦截，零引擎实例化零 scratch 创建（反向注入：
  // 撤两门后本 unwrap_err 转红，错误来自引擎装载被拒且归类失真）
  let entries = vec![(Vec::new(), SET_MEMBER_DUMMY_VALUE.to_vec())];
  let err = env
    .rt
    .block_on(try_promote(&env.store, s, GarnetObjectType::Set, entries))
    .unwrap_err();
  assert_eq!(
    err.to_string(),
    "ERR key+value size must be between 2 and 1024 bytes (got 4), max key length 128 (got 0)",
    "空成员拒升阶错误文案应与稳态写臂同源（契约闸前置，未进入建树段）"
  );

  let sess = env.store.new_session().unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(s))
      .unwrap()
      .is_none(),
    "拒升阶后键应保持信封态"
  );
  assert!(
    env.rt.block_on(sess.load_meta(s)).unwrap().is_none(),
    "拒升阶后不得伪造树态元记录与水位"
  );

  // 拒升阶后 SADD 照常续写，应答与对照键（从未升阶）逐字节一致；空成员随信封保全
  let out = auto_exec(&env, w, RespCommand::Sadd, &[s, b"small"]);
  let ctrl_out = auto_exec(&env, c, RespCommand::Sadd, &[ctrl, b"small"]);
  assert_eq!(out, b":1\r\n");
  assert_eq!(out, ctrl_out, "拒升阶前后 SADD 应答应逐字节一致");
  assert_eq!(
    auto_exec(&env, w, RespCommand::Sismember, &[s, b""]),
    b":1\r\n",
    "空成员应随信封回落完整保全"
  );
  assert_eq!(
    auto_exec(&env, w, RespCommand::Scard, &[s]),
    b":2\r\n",
    "拒升阶须保信封已有成员"
  );
}

/// ZSet：空成员（0B 树键 + 9B 分值记录在记录界内）→ 升阶整批拒、信封态保持、
/// 空成员可读、后续 ZADD 与从未升阶的对照键逐字节一致
#[test]
fn zset_promote_rejected_on_empty_member_and_envelope_keeps_writing() {
  let env = tiered_env("promote_gate_zset_empty");
  let (z, ctrl) = (b"ze", b"ze_ctrl");

  // 内存态受理面锚定（C# SortedSetAdd 字典直插，空成员合法输入）
  let w = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, w, RespCommand::Zadd, &[z, b"1", b""]),
    b":1\r\n"
  );
  let c = &mut session_with(&env);
  assert_eq!(
    auto_exec(&env, c, RespCommand::Zadd, &[ctrl, b"1", b"m"]),
    b":1\r\n"
  );

  // 升阶整批拒：entries 镜像 SortedSetObject::export_entries（成员即树键，记录
  // 为 member_ttl 裸载荷编码的 8B 大端分值），0 + 9 = 9 在记录上下限内、但空键
  // 低于引擎受理下限——契约闸 InvalidKV 文案前置拦截，未进入建树段
  let entries = vec![(Vec::new(), encode_member(&1f64.to_be_bytes(), None))];
  let err = env
    .rt
    .block_on(try_promote(
      &env.store,
      z,
      GarnetObjectType::SortedSet,
      entries,
    ))
    .unwrap_err();
  assert_eq!(
    err.to_string(),
    "ERR key+value size must be between 2 and 1024 bytes (got 9), max key length 128 (got 0)",
    "空成员拒升阶错误文案应与稳态写臂同源（契约闸前置，未进入建树段）"
  );

  let sess = env.store.new_session().unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(z))
      .unwrap()
      .is_none(),
    "拒升阶后键应保持信封态"
  );
  assert!(
    env.rt.block_on(sess.load_meta(z)).unwrap().is_none(),
    "拒升阶后不得伪造树态元记录与水位"
  );

  // 拒升阶后 ZADD 照常续写，应答与对照键（从未升阶）逐字节一致；空成员随信封保全
  let out = auto_exec(&env, w, RespCommand::Zadd, &[z, b"2", b"m2"]);
  let ctrl_out = auto_exec(&env, c, RespCommand::Zadd, &[ctrl, b"2", b"m2"]);
  assert_eq!(out, b":1\r\n");
  assert_eq!(out, ctrl_out, "拒升阶前后 ZADD 应答应逐字节一致");
  assert_eq!(
    auto_exec(&env, w, RespCommand::Zscore, &[z, b""]),
    b"$1\r\n1\r\n",
    "空成员应随信封回落完整保全"
  );
  assert_eq!(
    auto_exec(&env, w, RespCommand::Zcard, &[z]),
    b":2\r\n",
    "拒升阶须保信封已有成员"
  );
}

/// 越阈写回落（空成员形态）：单命令 65537 条 SADD（含 1 空成员）触
/// should_promote → 契约闸在建树前整批拒（零引擎实例化零 scratch 创建）→
/// RMW 臂信封回落写回，ACK 与升阶成功的对照键逐字节一致，空成员可读、键
/// 保持信封态——修复前该形态每次触阈写都付 O(N) 建树尝试且永不升阶
#[test]
fn sadd_crossing_promote_threshold_with_empty_member_falls_back_to_envelope() {
  let env = tiered_env("promote_gate_empty_fallback");
  let (s, ctrl) = (b"fe", b"fe_ctrl");

  // 被测键：65536 条合规 + 1 条空成员（先入），单命令越 65536 条目阈
  let mut args: Vec<Vec<u8>> = Vec::with_capacity(65538);
  args.push(s.to_vec());
  args.push(Vec::new());
  args.extend((0..65536u32).map(|i| i.to_string().into_bytes()));
  let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  let out = auto_exec(&env, &mut session_with(&env), RespCommand::Sadd, &refs);

  // 对照键：同规模（首成员换为合规成员，总条数同为 65537）且全部合规 →
  // 升阶成功换入分层态
  let mut cargs: Vec<Vec<u8>> = Vec::with_capacity(65538);
  cargs.push(ctrl.to_vec());
  cargs.push(b"head".to_vec());
  cargs.extend((0..65536u32).map(|i| i.to_string().into_bytes()));
  let crefs: Vec<&[u8]> = cargs.iter().map(|v| v.as_slice()).collect();
  let ctrl_out = auto_exec(&env, &mut session_with(&env), RespCommand::Sadd, &crefs);

  // 命令面承诺：越阈写无论升阶成败 ACK 逐字节一致（回落信封照常受理）
  assert_eq!(out, b":65537\r\n");
  assert_eq!(out, ctrl_out, "越阈写应答不应因契约闸拒绝而变形");

  let sess = env.store.new_session().unwrap();
  // 物理面分叉：被测键回落信封、对照键已分层
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(s))
      .unwrap()
      .is_none(),
    "越阈被拒后键应回落信封态"
  );
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(ctrl))
      .unwrap()
      .is_some(),
    "全合规对照键应升阶换入分层态"
  );
  // 回落写回零丢员：空成员随信封完整可读
  assert_eq!(
    auto_exec(
      &env,
      &mut session_with(&env),
      RespCommand::Sismember,
      &[s, b""]
    ),
    b":1\r\n",
    "空成员应随信封回落完整保全"
  );
}
