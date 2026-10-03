//! 分层列表最左键解码失败不回落回归测试
//!
//! 当分层列表底层树的最左键损坏或为非 16 字节外族记录时，`list_head_seq`
//! 必须返回 `Err(())` 上抛给上层闭环为错误帧，严禁静默回落 `LIST_SEQ_BASE`，
//! 杜绝覆写既有数据与虚增计数的危害。

use wbftree::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult};
use wcol::types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member};
use wnode_test::{TestEnv, auto_exec, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 构造分层列表（3 元素：e1, e2, e3，序号自 LIST_SEQ_BASE 连续排布）
fn promote_list3(env: &TestEnv, key: &[u8]) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::List,
      vec![
        (
          LIST_SEQ_BASE.to_be_bytes().to_vec(),
          encode_member(b"e1", None),
        ),
        (
          (LIST_SEQ_BASE + 1).to_be_bytes().to_vec(),
          encode_member(b"e2", None),
        ),
        (
          (LIST_SEQ_BASE + 2).to_be_bytes().to_vec(),
          encode_member(b"e3", None),
        ),
      ],
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
    "目标键应处于分层态"
  );
}

/// 验证树中既有 3 个元素未被篡改或覆写
fn assert_existing_data_intact(env: &TestEnv, key: &[u8]) {
  let sess = env.store.new_session().unwrap();
  let (_meta, mut stub) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  let guard = env
    .rt
    .block_on(sess.acquire_tree_read(key, &mut stub, None))
    .unwrap();
  let mut buf = vec![0u8; 128];

  let expected = [
    (LIST_SEQ_BASE, b"e1".as_slice()),
    (LIST_SEQ_BASE + 1, b"e2".as_slice()),
    (LIST_SEQ_BASE + 2, b"e3".as_slice()),
  ];

  for (seq, val) in expected {
    let (res, len) = guard.tree().read_into(&seq.to_be_bytes(), &mut buf);
    assert_eq!(res, BfTreeReadResult::Found, "既有记录 seq={seq} 必须存在");
    assert_eq!(
      &buf[..len],
      &encode_member(val, None),
      "既有记录 seq={seq} 载荷不得被覆写"
    );
  }
}

/// 最左键非 16 字节时，RPUSH / LPUSH / RPUSHX / LPUSHX 四臂必须失败上抛，
/// 不得覆写已有数据，且不得虚增元记录计数
#[test]
fn test_corrupt_leftmost_key_push_arms_fail() {
  let env = tiered_env("corrupt-seq-arms.db");
  let mut s = session_with(&env);
  let key = b"tiered_list_corrupt";

  promote_list3(&env, key);

  // 在树中注入排在最左侧的 1 字节非法键（0x00 比 [0,0,0,0,0,0,0,1,...] 更小）
  let sess = env.store.new_session().unwrap();
  let (_meta, mut stub) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  let guard = env
    .rt
    .block_on(sess.acquire_tree_write(key, &mut stub, None))
    .unwrap();
  let res = guard
    .tree()
    .insert(&[0u8], &encode_member(b"corrupt_node", None));
  assert_eq!(res, BfTreeInsertResult::Success, "注入损坏记录成功");
  drop(guard);

  // 四个 push 写臂逐个测试，均应失败并返回错误帧
  let commands = [
    (RespCommand::Rpush, b"val_rpush".as_slice(), "RPUSH"),
    (RespCommand::Lpush, b"val_lpush".as_slice(), "LPUSH"),
    (RespCommand::Rpushx, b"val_rpushx".as_slice(), "RPUSHX"),
    (RespCommand::Lpushx, b"val_lpushx".as_slice(), "LPUSHX"),
  ];

  for (cmd, val, name) in commands {
    let out = auto_exec(&env.api, &env.rt, &mut s, cmd, &[key, val]);
    let text = String::from_utf8_lossy(&out);
    assert!(
      out.starts_with(b"-ERR "),
      "{name} 遇到最左键损坏时必须返回错误帧，实际输出: {text}"
    );
  }

  // 既有数据完整未被覆写
  assert_existing_data_intact(&env, key);

  // 元记录计数未虚增
  let (meta_after, _) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  assert_eq!(meta_after.size, 3, "元记录 size 不得被虚增");

  // LLEN 读面应答未虚增
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Llen, &[key]);
  assert_eq!(out, b":3\r\n", "LLEN 计数必须与元记录一致且未虚增");
}

/// 注入 8 字节最左键（旧版位次导出形态残留场景），同样必须拒绝并保持数据完整
#[test]
fn test_corrupt_8byte_leftmost_key_fails() {
  let env = tiered_env("corrupt-seq-8b.db");
  let mut s = session_with(&env);
  let key = b"tiered_list_8b";

  promote_list3(&env, key);

  // 注入 8 字节键（[0u8; 8] 字典序排在 LIST_SEQ_BASE 前面）
  let sess = env.store.new_session().unwrap();
  let (_meta, mut stub) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  let guard = env
    .rt
    .block_on(sess.acquire_tree_write(key, &mut stub, None))
    .unwrap();
  let res = guard
    .tree()
    .insert(&[0u8; 8], &encode_member(b"old_8b_node", None));
  assert_eq!(res, BfTreeInsertResult::Success);
  drop(guard);

  // RPUSH 必须失败
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Rpush, &[key, b"v1"]);
  assert!(
    out.starts_with(b"-ERR "),
    "RPUSH 遇到 8 字节最左键必须报错，实际: {}",
    String::from_utf8_lossy(&out)
  );

  // LPUSH 必须失败
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Lpush, &[key, b"v2"]);
  assert!(
    out.starts_with(b"-ERR "),
    "LPUSH 遇到 8 字节最左键必须报错，实际: {}",
    String::from_utf8_lossy(&out)
  );

  assert_existing_data_intact(&env, key);

  let (meta_after, _) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  assert_eq!(meta_after.size, 3, "元记录 size 不得被虚增");
}

/// 正常 16 字节树的 push 操作回归：成功推进且有序，两端伸缩与计数均正常
#[test]
fn test_normal_tiered_list_push_regression() {
  let env = tiered_env("normal-seq-push.db");
  let mut s = session_with(&env);
  let key = b"tiered_list_normal";

  promote_list3(&env, key);

  // 正常 RPUSH
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Rpush,
    &[key, b"tail1"],
  );
  assert_eq!(out, b":4\r\n");

  // 正常 LPUSH
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Lpush,
    &[key, b"head1"],
  );
  assert_eq!(out, b":5\r\n");

  // 正常 RPUSHX
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Rpushx,
    &[key, b"tail2"],
  );
  assert_eq!(out, b":6\r\n");

  // 正常 LPUSHX
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Lpushx,
    &[key, b"head2"],
  );
  assert_eq!(out, b":7\r\n");

  // 检验 LRANGE 全集顺序：[head2, head1, e1, e2, e3, tail1, tail2]
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Lrange,
    &[key, b"0", b"-1"],
  );
  let expected = b"*7\r\n$5\r\nhead2\r\n$5\r\nhead1\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n$5\r\ntail1\r\n$5\r\ntail2\r\n";
  assert_eq!(out, expected, "LRANGE 结果顺序不符合预期");
}

/// 空树场景回归：树内无记录时回落 LIST_SEQ_BASE，首推成功
#[test]
fn test_empty_tree_fallback_regression() {
  let env = tiered_env("empty-tree-fallback.db");
  let mut s = session_with(&env);
  let key = b"tiered_list_empty_tree";

  // 先升阶 1 条记录
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::List,
      vec![(
        LIST_SEQ_BASE.to_be_bytes().to_vec(),
        encode_member(b"temp", None),
      )],
      i64::MAX,
      false,
    ))
    .unwrap();

  // 从底层树中直接删除该记录，使树变为空树（模拟扫描返回 0 条记录）
  let (_meta, mut stub) = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();
  let guard = env
    .rt
    .block_on(sess.acquire_tree_write(key, &mut stub, None))
    .unwrap();
  let del_res = guard.tree().delete(&LIST_SEQ_BASE.to_be_bytes());
  assert_eq!(del_res, BfTreeDeleteResult::Success);
  drop(guard);

  // 此时树中已无条目，RPUSH 应能够成功回落 LIST_SEQ_BASE 并正常写入
  let out = auto_exec(
    &env.api,
    &env.rt,
    &mut s,
    RespCommand::Rpush,
    &[key, b"new_head"],
  );
  // 原 meta.size = 1，push 1 个后 size 变为 2
  assert_eq!(out, b":2\r\n", "空树首推应成功");
}
