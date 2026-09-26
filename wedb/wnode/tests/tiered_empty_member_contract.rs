//! 分层态空成员契约门回归测试（validate_bftree_record 空键/空记录两门）
//!
//! 缺陷：契约单点 validate_bftree_record 原仅三条长度界、无空键/空记录门，
//! 分层稳态写对空成员放行到树内核才被拒（bf-tree insert 拒空键「Key too
//! small」）——ZADD 逐成员循环中途失败，其前成员已落树并计数入账，收尾回
//! tree_put_rejected「树配置与存根长度契约偏离」失真文案：同命令部分提交 +
//! 错误应答，违背全有全无原子性；SADD/HSET 走批量内核侥幸整批原子，但同样
//! 回失真文案（正常配置即触达，向客户端误报内部偏离态）。
//!
//! 修复语义：空键 `key.is_empty()` 与空记录 `record_len == 0` 两门是引擎受理
//! 面（bf-tree insert 拒空键、wbftree write_batch 前置拒空值）的精确镜像，
//! 预校验段整体拦截——空成员自此与超长成员同待遇：回存根契约 InvalidKV 明确
//! 文案、批次内其余成员零提交（零树内副作用）。
//!
//! 双态契约锚定（沿 tiered_promote_contract_gate「两上限不同源」同类目先例，
//! 引擎硬限下结构性无他解）：
//! - 空成员（树键侧：set/zset 成员、hash 字段）：分层态回 InvalidKV 契约文案
//!   且批次零提交，内存态（C# 对象层无条件受理，SetObjectImpl.SetAdd /
//!   HashObjectImpl.HashSet / SortedSetObjectImpl.SortedSetAdd 同形）成功回 :N；
//! - 空载荷（值侧：list 元素、hash 值）：member_ttl 编码后记录恒 ≥ 1B（旗标
//!   字节），不触两门也不触引擎空值门，双态一致受理成功 :N——分叉边界线即
//!   引擎受理面本身，键侧载荷侧不混同。

use std::sync::Arc;

use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, TestStore, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 慢路径命令同步求值并回帧字节（与 tiered_promote_contract_gate 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（与生产 export_entries 同形 entries），unwrap 断言升阶成功
async fn promote(
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  store
    .new_session()
    .unwrap()
    .promote_collection_to_bftree(key, obj_type, entries, i64::MAX, false)
    .await
    .unwrap();
}

/// 分层态含空成员批次：预校验段整体拦截回存根契约 InvalidKV 文案、批次内其余
/// 成员零提交（ZADD 双成员断言树内无残留），拒绝后合规写照常受理
#[test]
fn tiered_empty_member_batch_rejected_with_zero_side_effect() {
  let env = tiered_env("tiered_empty_member");
  let s = &mut session_with(&env);

  // 四族各升阶 8 成员为分层态
  let hash_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| (format!("f{i}").into_bytes(), encode_member(b"v", None)))
    .collect();
  let set_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| {
      (
        format!("m{i}").into_bytes(),
        SET_MEMBER_DUMMY_VALUE.to_vec(),
      )
    })
    .collect();
  let zset_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| {
      (
        format!("m{i}").into_bytes(),
        encode_member(&(i as f64).to_be_bytes(), None),
      )
    })
    .collect();
  let list_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| {
      (
        (LIST_SEQ_BASE + i).to_be_bytes().to_vec(),
        encode_member(b"e", None),
      )
    })
    .collect();
  env.rt.block_on(promote(
    &env.store,
    b"th",
    GarnetObjectType::Hash,
    hash_entries,
  ));
  env.rt.block_on(promote(
    &env.store,
    b"ts",
    GarnetObjectType::Set,
    set_entries,
  ));
  env.rt.block_on(promote(
    &env.store,
    b"tz",
    GarnetObjectType::SortedSet,
    zset_entries,
  ));
  env.rt.block_on(promote(
    &env.store,
    b"tl",
    GarnetObjectType::List,
    list_entries,
  ));

  // ZADD 双成员（m9 合规 + 空成员）：整体失败，m9 零落树零计数——修复前 m9
  // 已部分提交后回 tree_put_rejected 失真文案，树内残留
  assert_eq!(
    auto_exec(&env, s, RespCommand::Zadd, &[b"tz", b"1", b"m9", b"2", b""]),
    b"-ERR key+value size must be between 2 and 1024 bytes (got 9), max key length 128 (got 0)\r\n",
    "空成员应回存根契约 InvalidKV 文案（0B 树键 + 9B 分值记录）"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Zscore, &[b"tz", b"m9"]),
    b"$-1\r\n",
    "同批合规成员不得残留树内"
  );
  assert_eq!(auto_exec(&env, s, RespCommand::Zcard, &[b"tz"]), b":8\r\n");

  // SADD 含空成员批次：整体失败，m9 零提交（0B 树键 + 5B dummy 编码记录）
  assert_eq!(
    auto_exec(&env, s, RespCommand::Sadd, &[b"ts", b"m9", b""]),
    b"-ERR key+value size must be between 2 and 1024 bytes (got 5), max key length 128 (got 0)\r\n",
    "空成员应回存根契约 InvalidKV 文案（0B 树键 + 5B dummy 编码记录）"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Sismember, &[b"ts", b"m9"]),
    b":0\r\n",
    "同批合规成员不得残留树内"
  );
  assert_eq!(auto_exec(&env, s, RespCommand::Scard, &[b"ts"]), b":8\r\n");

  // HSET 含空字段批次：整体失败，f9 零提交（0B 树键 + 3B 值编码记录）
  assert_eq!(
    auto_exec(
      &env,
      s,
      RespCommand::Hset,
      &[b"th", b"f9", b"v9", b"", b"v2"]
    ),
    b"-ERR key+value size must be between 2 and 1024 bytes (got 3), max key length 128 (got 0)\r\n",
    "空字段应回存根契约 InvalidKV 文案（0B 树键 + 3B 值编码记录）"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Hget, &[b"th", b"f9"]),
    b"$-1\r\n",
    "同批合规字段不得残留树内"
  );
  assert_eq!(auto_exec(&env, s, RespCommand::Hlen, &[b"th"]), b":8\r\n");

  // 拒绝后合规写照常受理（预校验只拒空边界，不冻结键）
  assert_eq!(
    auto_exec(&env, s, RespCommand::Zadd, &[b"tz", b"3", b"m9"]),
    b":1\r\n"
  );
  assert_eq!(auto_exec(&env, s, RespCommand::Zcard, &[b"tz"]), b":9\r\n");

  // 内存态同输入成功回 :N（C# 对象层无条件受理，双态各按既定契约锚定）
  assert_eq!(
    auto_exec(&env, s, RespCommand::Zadd, &[b"mz", b"1", b"m9", b"2", b""]),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Sadd, &[b"ms", b"m9", b""]),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &env,
      s,
      RespCommand::Hset,
      &[b"mh", b"f9", b"v9", b"", b"v2"]
    ),
    b":2\r\n"
  );
}

/// 空载荷（list 元素 / hash 值）双态一致受理：member_ttl 编码后记录恒 ≥ 1B，
/// 不触空记录门也不触引擎空值门——分叉边界线即引擎受理面（键侧拒、载荷侧收）
#[test]
fn empty_payload_accepted_identically_in_both_states() {
  let env = tiered_env("tiered_empty_payload");
  let s = &mut session_with(&env);

  // 分层态锚定：list/hash 各升阶 8 条
  let list_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| {
      (
        (LIST_SEQ_BASE + i).to_be_bytes().to_vec(),
        encode_member(b"e", None),
      )
    })
    .collect();
  let hash_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
    .map(|i| (format!("f{i}").into_bytes(), encode_member(b"v", None)))
    .collect();
  env.rt.block_on(promote(
    &env.store,
    b"tl",
    GarnetObjectType::List,
    list_entries,
  ));
  env.rt.block_on(promote(
    &env.store,
    b"th",
    GarnetObjectType::Hash,
    hash_entries,
  ));

  // 分层态 RPUSH 含空元素：受理成功，ACK 与内存态同形，元素可空读还原
  assert_eq!(
    auto_exec(&env, s, RespCommand::Rpush, &[b"tl", b"a", b""]),
    b":10\r\n",
    "空元素编码后 1B 旗标记录，不触契约门，分层态应受理"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Lindex, &[b"tl", b"9"]),
    b"$0\r\n\r\n",
    "空元素应完整空读还原"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Rpush, &[b"ml", b"a", b""]),
    b":2\r\n",
    "内存态同输入受理（C# ListPush 一参一次 AddLast）"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Lindex, &[b"ml", b"1"]),
    b"$0\r\n\r\n"
  );

  // 分层态 HSET 空值字段：受理成功，与内存态同 ACK 同空读
  assert_eq!(
    auto_exec(&env, s, RespCommand::Hset, &[b"th", b"fe", b""]),
    b":1\r\n",
    "空值编码后 1B 旗标记录，不触契约门，分层态应受理"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Hget, &[b"th", b"fe"]),
    b"$0\r\n\r\n",
    "空值应完整空读还原"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Hset, &[b"mh", b"fe", b""]),
    b":1\r\n",
    "内存态同输入受理（C# HashSet 无空域判定）"
  );
  assert_eq!(
    auto_exec(&env, s, RespCommand::Hget, &[b"mh", b"fe"]),
    b"$0\r\n\r\n"
  );
}
