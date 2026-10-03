//! 分层死键读臂早退守卫回归测试（票 wnode-tiered-dead-key-read-arms-touch-tree-stale-emit）
//!
//! 验证点：
//! 1. 当 `meta.size == 0`（键已被并发排空/消亡，但底层残树未及注销）时，
//!    `Smembers`、`Srandmember`（单成员形）、`Lpos` 三臂均在触树前短路：
//!    - `Smembers`：直接出合法空集帧（RESP2 `*0\r\n` / RESP3 `~0\r\n`）
//!    - `Srandmember` 单成员形：直接出空帧（RESP2 `$-1\r\n` / RESP3 `_\r\n`）
//!    - `Lpos` 缺省形：直接出空帧（RESP2 `$-1\r\n` / RESP3 `_\r\n`）
//!    - `Lpos` COUNT 形：直接出空数组帧（`*0\r\n`）
//!    - 残树内即使仍有历史数据，三臂也绝不触碰残树、绝不回放陈旧成员
//! 2. 词元语法错误优先：`Lpos` 遇非法参数时错误帧优先于 `size == 0` 判定
//! 3. 正常存活键（`meta.size > 0`）三命令行为保持一致不回摆

use wbftree::ScanReturnField;
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  list::list_object::ListOperation,
  set::set_object::SetOperation,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wnode::resp::objects::tiered_collection_ops::{
  TieredCollectionArgs, TieredCtx, exec_tiered_list, exec_tiered_set,
};
use wnode_test::{TestEnv, tiered_env};
use wval::{GarnetObjectType, KeyTag};

/// 分层集合 N 成员手工升阶
fn seed_set(env: &TestEnv, key: &[u8], n: u64) {
  let sess = env.store.new_session().unwrap();
  let entries = (0..n)
    .map(|i| {
      (
        format!("s{i}").into_bytes(),
        SET_MEMBER_DUMMY_VALUE.to_vec(),
      )
    })
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
}

/// 分层列表 N 元素手工升阶
fn seed_list(env: &TestEnv, key: &[u8], n: u64) {
  let sess = env.store.new_session().unwrap();
  let entries = (0..n)
    .map(|i| {
      (
        (LIST_SEQ_BASE + i as u128).to_be_bytes().to_vec(),
        encode_member(format!("e{i}").as_bytes(), None),
      )
    })
    .collect();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::List,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();
}

/// 场景一：死键（meta.size == 0）读臂零触树并出合法空应答
#[test]
fn dead_key_zero_size_arms_do_not_touch_tree() {
  let env = tiered_env("tiered-dead-key-guards.db");
  let rt = &env.rt;

  let set_key = b"dead_set";
  let list_key = b"dead_list";

  seed_set(&env, set_key, 5);
  seed_list(&env, list_key, 5);

  let sess = env.store.new_session().unwrap();

  // 装载存根以获得树句柄，并断言升阶后树内确实有 5 条记录
  let (mut set_meta, mut set_stub) = rt
    .block_on(sess.load_collection_stub(set_key))
    .unwrap()
    .expect("set 存根必须存在");
  assert_eq!(set_meta.size, 5);

  let (mut list_meta, mut list_stub) = rt
    .block_on(sess.load_collection_stub(list_key))
    .unwrap()
    .expect("list 存根必须存在");
  assert_eq!(list_meta.size, 5);

  // 模拟并发排空交错窗：元记录被删除（墓碑），使 refresh_tiered_meta 返回 Ok(false) 判死，
  // 锁内 ctx.meta.size 置 0；但底层树尚未注销，树内仍存 5 条残余记录
  let set_meta_k = sess.session_tag_key(KeyTag::Meta, set_key);
  let list_meta_k = sess.session_tag_key(KeyTag::Meta, list_key);
  rt.block_on(sess.delete_raw(&set_meta_k)).unwrap();
  rt.block_on(sess.delete_raw(&list_meta_k)).unwrap();

  let batch_sess = sess.enter_batch();

  // ---- 1. Smembers 臂：RESP2 与 RESP3 下直接出空集帧，绝不触树回放 s0..s4 ----
  {
    let mut out_resp2 = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_resp2 = TieredCollectionArgs::new(SetOperation::Smembers, (0, 0), &[], 2);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_resp2,
        &mut out_resp2,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_resp2, b"*0\r\n", "Smembers RESP2 必须直接出空集帧");

    let mut out_resp3 = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_resp3 = TieredCollectionArgs::new(SetOperation::Smembers, (0, 0), &[], 3);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_resp3,
        &mut out_resp3,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_resp3, b"~0\r\n", "Smembers RESP3 必须直接出空集帧");
  }

  // ---- 2. Srandmember 单成员形：RESP2 与 RESP3 下直接出 null 帧，绝不触树抽取残余成员 ----
  {
    let mut out_resp2 = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_resp2 = TieredCollectionArgs::new(SetOperation::Srandmember, (0, 0), &[], 2);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_resp2,
        &mut out_resp2,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(
      out_resp2, b"$-1\r\n",
      "Srandmember 单成员形 RESP2 必须出 null"
    );

    let mut out_resp3 = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_resp3 = TieredCollectionArgs::new(SetOperation::Srandmember, (0, 0), &[], 3);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_resp3,
        &mut out_resp3,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(
      out_resp3, b"_\r\n",
      "Srandmember 单成员形 RESP3 必须出 null"
    );

    // 对照组：既有 count 形门回归（正 count 出空集帧、负 count 出空列表）
    let mut out_count = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_count = TieredCollectionArgs::new(SetOperation::Srandmember, (0, 0), &[b"2"], 2);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_count,
        &mut out_count,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_count, b"*0\r\n", "Srandmember 正 count 形必须出空集帧");

    let mut out_neg = Vec::new();
    let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
    let call_neg = TieredCollectionArgs::new(SetOperation::Srandmember, (0, 0), &[b"-2"], 2);
    let res = rt
      .block_on(exec_tiered_set(
        &batch_sess,
        set_key,
        &mut ctx,
        call_neg,
        &mut out_neg,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_neg, b"*0\r\n", "Srandmember 负 count 形必须出空列表帧");
  }

  // ---- 3. Lpos 臂：缺省形出 null、COUNT 形出空数组，且语法错误帧优先 ----
  {
    // 缺省形：查找残树中客观存在的元素 e0，必须直接出 null，绝不触树
    let mut out_resp2 = Vec::new();
    let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
    let call_resp2 = TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0"], 2);
    let res = rt
      .block_on(exec_tiered_list(
        &batch_sess,
        list_key,
        &mut ctx,
        call_resp2,
        &mut out_resp2,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_resp2, b"$-1\r\n", "Lpos 缺省形 RESP2 必须出 null");

    let mut out_resp3 = Vec::new();
    let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
    let call_resp3 = TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0"], 3);
    let res = rt
      .block_on(exec_tiered_list(
        &batch_sess,
        list_key,
        &mut ctx,
        call_resp3,
        &mut out_resp3,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_resp3, b"_\r\n", "Lpos 缺省形 RESP3 必须出 null");

    // COUNT 形：必须出空数组帧
    let mut out_count = Vec::new();
    let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
    let call_count =
      TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0", b"COUNT", b"2"], 2);
    let res = rt
      .block_on(exec_tiered_list(
        &batch_sess,
        list_key,
        &mut ctx,
        call_count,
        &mut out_count,
      ))
      .unwrap();
    assert!(res);
    assert_eq!(out_count, b"*0\r\n", "Lpos COUNT 形必须出空数组帧");

    // 语法错误优先保证：rank 0 为非法参数，即使 size == 0 也必须先出错误帧
    let mut out_err = Vec::new();
    let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
    let call_err =
      TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0", b"RANK", b"0"], 2);
    let res = rt
      .block_on(exec_tiered_list(
        &batch_sess,
        list_key,
        &mut ctx,
        call_err,
        &mut out_err,
      ))
      .unwrap();
    assert!(res);
    assert!(
      out_err.starts_with(b"-ERR "),
      "Lpos 语法错误必须优先于 size == 0 早退: {}",
      String::from_utf8_lossy(&out_err)
    );
  }

  // ---- 4. 树内残存数据亲验：确认底层残树确实仍有 5 条记录（未触树断言非真空） ----
  {
    let tree_guard = rt
      .block_on(sess.acquire_tree_read(set_key, &mut set_stub, None))
      .unwrap();
    let mut seen = 0usize;
    tree_guard
      .tree()
      .scan_with_count_callback(&[0], usize::MAX, ScanReturnField::Key, |_, _| {
        seen += 1;
        true
      })
      .unwrap();
    assert_eq!(
      seen, 5,
      "底层残树内实存 5 条记录，证明上述早退确系未触树拦截"
    );
  }
}

/// 场景二：存活键（meta.size > 0）正常读全量回归不回摆
#[test]
fn live_key_arms_behave_normally() {
  let env = tiered_env("tiered-live-key-guards.db");
  let rt = &env.rt;

  let set_key = b"live_set";
  let list_key = b"live_list";

  seed_set(&env, set_key, 3);
  seed_list(&env, list_key, 3);

  let sess = env.store.new_session().unwrap();
  let (mut set_meta, mut set_stub) = rt
    .block_on(sess.load_collection_stub(set_key))
    .unwrap()
    .expect("live set 存根");
  let (mut list_meta, mut list_stub) = rt
    .block_on(sess.load_collection_stub(list_key))
    .unwrap()
    .expect("live list 存根");

  let batch_sess = sess.enter_batch();

  // Smembers 正常全量输出 3 个成员
  let mut out = Vec::new();
  let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
  let call = TieredCollectionArgs::new(SetOperation::Smembers, (0, 0), &[], 2);
  let res = rt
    .block_on(exec_tiered_set(
      &batch_sess,
      set_key,
      &mut ctx,
      call,
      &mut out,
    ))
    .unwrap();
  assert!(res);
  assert!(
    out.starts_with(b"*3\r\n"),
    "存活集合 Smembers 必须出 3 成员帧"
  );

  // Srandmember 单成员形正常抽中非空元素
  let mut out = Vec::new();
  let mut ctx = TieredCtx::new(&mut set_meta, &mut set_stub);
  let call = TieredCollectionArgs::new(SetOperation::Srandmember, (0, 0), &[], 2);
  let res = rt
    .block_on(exec_tiered_set(
      &batch_sess,
      set_key,
      &mut ctx,
      call,
      &mut out,
    ))
    .unwrap();
  assert!(res);
  assert!(
    out.starts_with(b"$2\r\ns"),
    "存活集合 Srandmember 单成员形必须返回成员 bulk 帧"
  );

  // Lpos 缺省形正常返回命中位次 :0\r\n
  let mut out = Vec::new();
  let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
  let call = TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0"], 2);
  let res = rt
    .block_on(exec_tiered_list(
      &batch_sess,
      list_key,
      &mut ctx,
      call,
      &mut out,
    ))
    .unwrap();
  assert!(res);
  assert_eq!(out, b":0\r\n", "存活列表 Lpos e0 必须返回索引 0");

  // Lpos COUNT 2 正常返回 *1\r\n:0\r\n
  let mut out = Vec::new();
  let mut ctx = TieredCtx::new(&mut list_meta, &mut list_stub);
  let call = TieredCollectionArgs::new(ListOperation::Lpos, (0, 0), &[b"e0", b"COUNT", b"2"], 2);
  let res = rt
    .block_on(exec_tiered_list(
      &batch_sess,
      list_key,
      &mut ctx,
      call,
      &mut out,
    ))
    .unwrap();
  assert!(res);
  assert_eq!(
    out, b"*1\r\n:0\r\n",
    "存活列表 Lpos COUNT 形必须返回单命中数组"
  );
}
