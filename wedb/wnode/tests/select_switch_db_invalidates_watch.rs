//! WATCH 后跨库切库位点作废（票 task/ing/zcode-r133c-selectdb.md 案二，
//! 方向裁决 a：对齐 C# 逐库事务管理器换任清 watch）
//!
//! C# 每库独持 TransactionManager（garnet/libs/server/Resp/
//! GarnetDatabaseSession.cs:46/63），`SwitchActiveDatabaseSession`（
//! garnet/libs/server/Resp/RespServerSession.cs:1714-1724）切库成功提交点
//! `this.txnManager = dbSession.TransactionManager` 整体换任，watch 容器随
//! 管理器归库、位点随旧库作废。修复前 rust 切库臂只翻标量不触容器，且
//! add_watch 登记期烘逻辑前缀 hash、validate_watch_version 跨库恒校：
//! WATCH k(db0) → 他会话改 k → SELECT 1 → MULTI/GET/EXEC 被 db0 侧推进误杀
//! 回 nil（而事务全部命令跑在 db1），与 C# 正常执行相反；附带旧库 watch
//! 派生 hash 并入新库事务锁集的跨库带外锁条目。
//!
//! 裁决收口（方向注记 a）：rust 单实例容器采「切库成功提交点主动清」，
//! SELECT 回原库位点不复活——与 C# 缓存容器回库复活非全等，本票裁一次性
//! 作废，严禁补逐库容器（过度设计）。
//!
//! 真协议帧 + 真共享存储（引擎级 watch 钩子推进版本表）+ 真事务状态机
//! 闭环，无假 mock。

use std::sync::Arc;

use compio::runtime::Runtime;
use wnode::{
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wnode_test::drain_output;
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// 喂一整批帧并冲出应答（消费循环 + 停车臂同步闭环的泵替身；冷库挂起面
/// SELECT 亦经本泵物化收口，切库成功提交点两臂均落 watch 作废单点）
fn feed(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

/// 双态镜像读数（会话镜像 / 事务管理器真值源）
fn txn_state(s: &RespServerSession) -> (TxnState, TxnState) {
  (
    s.txn_state,
    s.txn_manager.as_ref().expect("事务组件已挂载").state,
  )
}

/// 共享存储会话对：WATCH 方（挂事务组件，与会话共用同一版本表）与
/// 写入方（异 RESP 会话经引擎级 watch 钩子真实推进版本槽）
fn session_pair() -> (RespServerSession, RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("wnode-select-switch-watch.db").unwrap();
  let map = Arc::new(WatchVersionMap::new(64));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));

  let mut watcher = RespServerSession::new(1, RespServerSessionOptions::default());
  watcher.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  watcher.attach_transaction_components(map, TxnLockTable::new());

  let mut writer = RespServerSession::new(2, RespServerSessionOptions::default());
  writer.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  (watcher, writer, dir)
}

/// 案二主用例（裁决档 a）：WATCH k(db0) → 他会话 SET k → SELECT 1 →
/// MULTI/GET/EXEC 必须正常执行回结果数组（切库成功提交点清容器，db0 侧
/// 推进不再参与校验，对齐 C# 换任作废形）；再 SELECT 回 db0，位点不复活
/// ——回库后事务照常成功（本票裁一次性作废，非 C# 缓存容器复活形）
#[test]
fn switch_db_invalidates_watch_and_no_revival_on_return() {
  let (mut s, mut w, _dir) = session_pair();

  assert_eq!(
    feed(&mut s, b"*2\r\n$5\r\nWATCH\r\n$2\r\nwk\r\n"),
    b"+OK\r\n",
    "WATCH db0 键位点登记"
  );
  assert_eq!(
    feed(&mut w, b"*3\r\n$3\r\nSET\r\n$2\r\nwk\r\n$2\r\nv1\r\n"),
    b"+OK\r\n",
    "他会话真实推进 db0 版本槽"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n",
    "切库成功提交"
  );

  // 异库事务：db1 无 wk，GET 回 nil 但事务必须成功提交（修复前被 db0
  // 旧位点误杀回 *-1）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$2\r\nwk\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    b"*1\r\n$-1\r\n",
    "切库后旧库位点作废：EXEC 回结果数组而非 nil（对齐 C# 换任形）"
  );
  assert_eq!(txn_state(&s), (TxnState::None, TxnState::None));

  // 回原库不复活（裁决收口断言）：wk 在回库前确已被他会话改写（v1 在场），
  // 若位点随 C# 缓存容器复活则本事务必误回 *-1
  assert_eq!(
    feed(&mut s, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$2\r\nwk\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n*1\r\n$2\r\nv1\r\n",
    "回原库位点不复活（一次性作废裁决），事务照常执行并读到 db0 现值"
  );
}

/// 对照 1（不回退）：不切库维持现中止形——WATCH k → 他会话改 k →
/// MULTI/GET/EXEC 必回 *-1（乐观锁隔离域在本库内照常生效）
#[test]
fn no_switch_watch_abort_shape_persists() {
  let (mut s, mut w, _dir) = session_pair();

  assert_eq!(
    feed(&mut s, b"*2\r\n$5\r\nWATCH\r\n$2\r\nwk\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut w, b"*3\r\n$3\r\nSET\r\n$2\r\nwk\r\n$2\r\nv1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$2\r\nwk\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n*-1\r\n",
    "同库冲突写入必须照常中止"
  );
}

/// 对照 2（no-op 护栏）：同库 SELECT 不触作废——C# NetworkSELECT 以
/// `index == activeDbId` 短路不入 Switch（ArrayCommands.cs:143），位点必须
/// 保持，异会话改写后 EXEC 仍中止；同时护住 EXEC 重放遍同库 SELECT 臂的
/// 在途位点不被误清
#[test]
fn same_db_select_keeps_watch() {
  let (mut s, mut w, _dir) = session_pair();

  assert_eq!(
    feed(&mut s, b"*2\r\n$5\r\nWATCH\r\n$2\r\nwk\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut w, b"*3\r\n$3\r\nSET\r\n$2\r\nwk\r\n$2\r\nv1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$2\r\nwk\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n*-1\r\n",
    "同库 SELECT 严禁误清位点"
  );
}
