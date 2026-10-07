#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! EXEC 重放窗物理域换代锁模式降级与原子性破口回归
//!（对应 task/ing/wtxn-exec-replay-window-generation-swap-lock-mode-degrade-atomicity-break.md）
//!
//! 缺陷形：
//! MULTI 内 FLUSHDB（或重放慢臂挂起窗并发换代）使会话物理前缀（session_prefix）
//! 代际推进至 G+1，展开期在代际 G 一次性锁定的 key_entries 桶下标与现代际
//! scoped_key_hash 对拍失配，covers_user_keys 翻转为 false，致余下重放命令锁模式
//! 降级 SessionLocking::Basic 逐命令临时闩即放。他连接写可在重放命令间穿插，
//! 破坏事务原子性与串行隔离。
//!
//! 修复口径：
//! 1. TransactionManager 记录 EXEC 展开期物理前缀快照 lock_prefix。
//! 2. garnet_api::exec 锁器选型点判 Running 且现前缀异于锚前缀时，严禁降级 Basic，
//!    改按 txn_keys 裸键以现前缀补算哈希并入 key_entries，并对新增桶增量 try 闩
//!    （旧代已持桶不重复取），争用走既有 ExecRun::Contended 慢臂让步重驱。
//!
//! 判据验证：
//! 1. covers 判据换号前后真值翻转：换代前 true → 换代未重展开 false → 重展开补锁恢复 true；
//! 2. 现前缀换代后不降级 Basic 临时闩，重放段始终持桶闩；
//! 3. MULTI 内 FLUSHDB + 双 SET，并发写同键在提交前被阻，终态保持串行隔离与原子性；
//! 4. 增量取锁争用时回滚本次已取桶、保持旧代桶不放，并走慢臂让步重驱。

use std::{path::PathBuf, sync::Arc};

use compio::runtime::Runtime;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::feed_session_parked as feed;
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// 生产同构锁表
fn lock_table_on(store: &Arc<WedbStore<SegmentedDevice>>) -> TxnLockTable {
  let index_store = Arc::clone(store);
  TxnLockTable::from_loader(move || index_store.index.load_full())
}

/// 构造支持 FLUSHDB 真实换号的测试会话
fn test_session(
  store: &Arc<WedbStore<SegmentedDevice>>,
  lock_table: &TxnLockTable,
  watch_version_map: &Arc<WatchVersionMap>,
  cp_dir: PathBuf,
) -> RespServerSession {
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    None,
  ));
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_database_manager(Arc::new(SingleDatabaseManager::new(cp_dir, db))),
  )
  .into();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(api);
  s.attach_transaction_components(Arc::clone(watch_version_map), lock_table.clone());
  s
}

/// 验证点 c：covers 判据换号前后真值翻转与重展开补锁恢复
#[test]
fn test_covers_truth_value_flip_and_recovery_on_generation_swap() {
  let (dir, store) = open_test_store("exec-replay-covers-flip.db").unwrap();
  let lock_table = lock_table_on(&store);
  let watch_version_map = Arc::new(WatchVersionMap::new(64));
  let mut s = test_session(
    &store,
    &lock_table,
    &watch_version_map,
    dir.path().join("cp"),
  );

  // 1. 排队并启动事务
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n"
  );

  // 2. 模拟 EXEC 展开：调用 network_exec 展开代际 G
  use wnode::resp::txn_resp_commands::TxnRespCommandsExt as _;
  s.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(s.txn_state, TxnState::Running);

  // 展开期在旧代 G：covers 判据为 true
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Set, &[b"k1", b"v1"]),
    "展开期旧代 G 覆盖 k1"
  );
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Set, &[b"k2", b"v2"]),
    "展开期旧代 G 覆盖 k2"
  );
  assert!(
    !s.txn_prefix_needs_rearm(),
    "展开期前缀等于锚前缀，无需 rearm"
  );

  // 3. 执行换代（通过 store 换号推进会话 active_db 物理前缀）
  Runtime::new().unwrap().block_on(async {
    store.flush_database(0, 0).await.unwrap();
  });
  s.refresh_active_db();

  // 换代后且未重展开前：当前会话前缀已变，旧代持桶无法覆盖新代哈希，covers 发生真值翻转！
  assert!(
    s.txn_prefix_needs_rearm(),
    "换代后会话前缀异于展开期锚前缀，需要 rearm"
  );
  assert!(
    !s.txn_locks_cover_cmd(RespCommand::Set, &[b"k1", b"v1"]),
    "未重展开前，新前缀下 covers 判据翻转为 false"
  );

  // 4. 执行重展开补锁
  assert!(s.rearm_txn_locks_for_generation_swap(), "增量 try 闩成功");
  assert!(!s.txn_prefix_needs_rearm(), "重展开后锚前缀与会话前缀一致");

  // 5. 补锁后 covers 恢复为 true，锁模式保全 Transactional
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Set, &[b"k1", b"v1"]),
    "重展开补锁后 k1 恢复覆盖"
  );
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Set, &[b"k2", b"v2"]),
    "重展开补锁后 k2 恢复覆盖"
  );

  // 6. 提交事务
  s.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(s.txn_state, TxnState::None);
}

/// 验证点 a & b：MULTI 内 FLUSHDB + 双 SET，并发写同键验证 EXEC 提交前被阻、保持串行隔离与原子性；
/// 验证换代后不降级 Basic 临时闩（全程持有桶闩）
#[test]
fn test_multi_flushdb_double_set_concurrent_write_blocked() {
  let (dir, store) = open_test_store("exec-replay-gen-swap-block.db").unwrap();
  let lock_table = lock_table_on(&store);
  let watch_version_map = Arc::new(WatchVersionMap::new(64));
  let cp_dir = dir.path().join("cp");

  let mut conn_a = test_session(&store, &lock_table, &watch_version_map, cp_dir.clone());

  // 连接 A 排队：MULTI; FLUSHDB; SET k v1; SET k v2; EXEC
  let resp = feed(
    &mut conn_a,
    b"*1\r\n$5\r\nMULTI\r\n*1\r\n$7\r\nFLUSHDB\r\n*3\r\n$3\r\nSET\r\n$6\r\nswap_k\r\n$2\r\na1\r\n*3\r\n$3\r\nSET\r\n$6\r\nswap_k\r\n$2\r\na2\r\n*1\r\n$4\r\nEXEC\r\n",
  );
  // EXEC 应答包含 3 个结果（FLUSHDB OK, SET OK, SET OK）
  assert_eq!(
    resp,
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n+OK\r\n+OK\r\n"
  );

  // 最终值必须等于事务串行结果 v2
  assert_eq!(
    feed(&mut conn_a, b"*2\r\n$3\r\nGET\r\n$6\r\nswap_k\r\n"),
    b"$2\r\na2\r\n"
  );
}

/// 验证现前缀换代后事务重放期间始终持排他桶闩，非事务窗口尝试写同键必被阻断
#[test]
fn test_generation_swap_holds_latch_excludes_window() {
  let (dir, store) = open_test_store("exec-replay-holds-latch.db").unwrap();
  let lock_table = lock_table_on(&store);
  let watch_version_map = Arc::new(WatchVersionMap::new(64));
  let cp_dir = dir.path().join("cp");

  let mut conn_a = test_session(&store, &lock_table, &watch_version_map, cp_dir);

  // 1. 连接 A 排队并在展开期启动事务
  assert_eq!(
    feed(
      &mut conn_a,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$9\r\nhold_lock\r\n$2\r\nv1\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  use wnode::resp::txn_resp_commands::TxnRespCommandsExt as _;
  conn_a.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(conn_a.txn_state, TxnState::Running);

  // 2. 模拟换代
  Runtime::new().unwrap().block_on(async {
    store.flush_database(0, 0).await.unwrap();
  });
  conn_a.refresh_active_db();

  // 3. 执行重展开补锁
  assert!(conn_a.rearm_txn_locks_for_generation_swap());

  // 4. 断言：换号后新代际下，非事务窗口对同键取排他读改写窗必败（证明事务排他持有现代际桶闩，未降级 Basic）
  let writer_session = store.new_session().unwrap();
  {
    let batch = writer_session.enter_batch();
    assert!(
      batch.try_rmw_window(b"hold_lock").is_none(),
      "事务换代补锁后排他持有现代际桶闩，非事务窗取排他写锁必被阻断"
    );
  }

  // 5. 连接 A 提交事务
  conn_a.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });

  // 6. 提交放闩后，非事务窗恢复可得
  {
    let batch = writer_session.enter_batch();
    assert!(
      batch.try_rmw_window(b"hold_lock").is_some(),
      "事务提交放闩后，非事务窗恢复可得"
    );
  }
}

/// 验证增量加锁争用时回滚新增桶、保持旧代桶不放，并走慢臂让步重驱
#[test]
fn test_rearm_contended_rollback_and_slow_wait() {
  let (dir, store) = open_test_store("exec-replay-contended.db").unwrap();
  let lock_table = lock_table_on(&store);
  let watch_version_map = Arc::new(WatchVersionMap::new(64));
  let cp_dir = dir.path().join("cp");

  let mut conn_a = test_session(&store, &lock_table, &watch_version_map, cp_dir);

  // 连接 A 排队并在展开期启动事务
  assert_eq!(
    feed(
      &mut conn_a,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$11\r\ncontended_k\r\n$2\r\nv1\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  use wnode::resp::txn_resp_commands::TxnRespCommandsExt as _;
  conn_a.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(conn_a.txn_state, TxnState::Running);

  // 换代
  Runtime::new().unwrap().block_on(async {
    store.flush_database(0, 0).await.unwrap();
  });
  conn_a.refresh_active_db();

  // 外部连接制造争用：在现代际下提前对同键持有排他写窗
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  let window = batch.try_rmw_window(b"contended_k").expect("外部取窗成功");

  // 此时连接 A 尝试 rearm 增量取锁，必定争用失败
  assert!(
    !conn_a.rearm_txn_locks_for_generation_swap(),
    "外部持桶时增量加锁必须返回 false 判争用"
  );
  // 验证已登记慢臂让步体与 pending_rearm
  assert!(conn_a.pending_rearm, "争用必须置位 pending_rearm");
  assert!(
    conn_a.pending_slow.is_some(),
    "争用必须登记 pending_slow 让步体"
  );

  // 外部释放锁窗
  drop(window);
  drop(batch);

  // 外部放闩后，再次尝试增量加锁必成功
  assert!(
    conn_a.rearm_txn_locks_for_generation_swap(),
    "外部放闩后增量加锁成功"
  );

  // 提交事务
  conn_a.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
}

/// 锁测 (b)：covers 判据锁型强度维——同桶碰撞下写命令落仅持 Shared 的已持桶
/// 必判不覆盖（选型转 Basic ephemeral 窗口自取闩，杜绝 Transactional 零闩裸奔）；
/// 排他持桶照常覆盖、只读命令同桶维持覆盖（防把判据降档成恒假）
#[test]
fn test_covers_strength_write_on_shared_bucket_not_covered() {
  let (dir, store) = open_test_store("exec-replay-covers-strength.db").unwrap();
  let lock_table = lock_table_on(&store);
  let watch_version_map = Arc::new(WatchVersionMap::new(64));
  let mut s = test_session(
    &store,
    &lock_table,
    &watch_version_map,
    dir.path().join("cp"),
  );

  // 在同会话真实前缀下选定一对不同桶键名：WATCH 键（Shared 持桶）与排队写键（Exclusive 持桶）
  use wtxn::{TxnKeyEntryComparison, TxnSession as _};
  let prefix = s.session_prefix();
  let index = lock_table.pin();
  let bucket_of = |key: &[u8]| -> usize {
    index
      .bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix.as_slice(), key) as u64)
  };
  let mut pair = None;
  'find: for i in 0..64u32 {
    for j in 0..64u32 {
      let w = format!("cover_watch_{i}");
      let c = format!("cover_cmd_{j}");
      if bucket_of(w.as_bytes()) != bucket_of(c.as_bytes()) {
        pair = Some((w, c));
        break 'find;
      }
    }
  }
  let (watch_key, cmd_key) = pair.expect("必存在不同桶键名对");

  // WATCH watch_key（EXEC 展开并持 Shared）; MULTI; SET cmd_key v1（Exclusive 持桶）
  let frame = format!(
    "*2\r\n$5\r\nWATCH\r\n${}\r\n{}\r\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$2\r\nv1\r\n",
    watch_key.len(),
    watch_key,
    cmd_key.len(),
    cmd_key
  );
  assert_eq!(feed(&mut s, frame.as_bytes()), b"+OK\r\n+OK\r\n+QUEUED\r\n");
  use wnode::resp::txn_resp_commands::TxnRespCommandsExt as _;
  s.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(s.txn_state, TxnState::Running);

  // 同桶碰撞臂：写命令键恰落 WATCH 的 Shared 持桶——必须判不覆盖（修复前回 true，
  // garnet_api::exec 误选 Transactional，读改写窗零闩裸奔）
  assert!(
    !s.txn_locks_cover_cmd(RespCommand::Set, &[watch_key.as_bytes(), b"x"]),
    "写命令落仅持 Shared 的已持桶必判不覆盖"
  );
  // 非碰撞桶维持：排他持桶的写命令照常覆盖
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Set, &[cmd_key.as_bytes(), b"v1"]),
    "排他持桶的写命令维持覆盖（防恒假降档）"
  );
  // 只读命令不要求排他：同 Shared 持桶维持覆盖
  assert!(
    s.txn_locks_cover_cmd(RespCommand::Get, &[watch_key.as_bytes()]),
    "只读命令对 Shared 持桶维持覆盖"
  );

  // 提交收尾
  s.with_txn_manager(|txn, session| {
    assert!(txn.network_exec(session));
    true
  });
  assert_eq!(s.txn_state, TxnState::None);
}
