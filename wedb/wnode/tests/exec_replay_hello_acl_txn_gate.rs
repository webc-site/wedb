//! EXEC 重放窗一态收口三锁（波次票 wnode-exec-replay-hello-acl-txn-gate-overflow，
//! 登记见 doc/zh/deviations.md §58d）
//!
//! C# 锚：NetworkSKIP 无 HELLO/ACL 拒臂（garnet/libs/server/Transaction/
//! TxnRespCommands.cs:105-204）、HELLO 与 ACL 族 Flags 无 NoMulti
//! （libs/resources/RespCommandsInfo.json HELLO :2104-2108、ACL|LIST :41-45）、
//! 重放窗 Running 直通后 NetworkHELLO（RespServerSession.cs:1090）与 ACL 族
//! （AdminCommands.cs:65-74）无事务门正常执行、应答写入 EXEC 数组。
//! rust 修复前形态：分派漏斗（core.rs dispatch_via_garnet_api）预筛扩大化，
//! 无 AUTH 合法形 HELLO 与 ACL 族十子命令在重放窗一律回错误元素且 ACL 共回
//! HELLO 错位文案——本锁族钉死收口形态：
//! 1. MULTI + HELLO 3（无 AUTH）+ EXEC：元素为正常 HELLO 应答 map（逐字节与
//!    独立同形 HELLO 等形），协议版本真实落位；
//! 2. MULTI + ACL LIST + EXEC：元素为 ACL 专属拒绝帧，不含 "ERR HELLO" 字样，
//!    对照事务外 ACL LIST 正常数组闭环；
//! 3. 同库 SELECT 冷库窄窗（装配期卸载映射后重放撞事务窗围栏）：回
//!    SELECT_IN_TXN 族专属帧（修复前共回 HELLO 文案），且零停泊（严禁登记
//!    SlowWait）、会话标量零撕裂。

use std::{future::Future, sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

const HELLO_TXN_ERR: &[u8] = b"-ERR HELLO is currently unsupported inside a transaction.\r\n";
const ACL_TXN_ERR: &[u8] = b"-ERR ACL is currently unsupported inside a transaction.\r\n";
const SELECT_TXN_ERR: &[u8] = b"-ERR SELECT is currently unsupported inside a transaction.\r\n";

/// 生产装配单机会话骨架（对标 transaction_tests 既有 §58 锁族：ACL 认证器 +
/// 事务组件；存储执行域按各用例形态经 set_garnet_api 挂入）
fn txn_session() -> RespServerSession {
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::new("").unwrap(),
  )))));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  s
}

/// 产线形态驱动单点（transaction_tests::feed_txn_session 同款：入帧 → 消费 →
/// 停车臂闭环 → 线面应答字节）
fn feed(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf);
  block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  wnode_test::drain_output(s)
}

/// 同步测试壳内闭环 async 存储/泵链（transaction_tests::block_on 同款先例）
fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

/// 锁 1：MULTI + HELLO 3（无 AUTH）+ EXEC——重放窗同步快臂直出正常 HELLO map
#[test]
fn exec_replay_hello_without_auth_writes_normal_map() {
  let (_dir, store) = wtest_base::open_test_store("exec-replay-hello-map.db").unwrap();
  let mut s = txn_session();
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  assert_eq!(feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n"), b"+OK\r\n");
  assert_eq!(
    feed(&mut s, &wtest_base::resp_frame(&[b"HELLO", b"3"])),
    b"+QUEUED\r\n",
    "无 AUTH HELLO 排队面与 C# 同构（§58a 中止面恰等于携 AUTH 组）"
  );
  let exec = feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n");
  // 重放窗执行真落位：协议版本 3、事务收口复位
  assert_eq!(s.txn_state, TxnState::None);
  assert_eq!(s.resp_protocol_version, 3, "重放窗 HELLO 协议升级真实落位");
  // 元素与独立同形 HELLO 应答逐字节等形（对位 C# 重放窗 NetworkHELLO 同形）
  let ref_map = feed(&mut s, &wtest_base::resp_frame(&[b"HELLO", b"3"]));
  assert!(ref_map.starts_with(b"%8\r\n"), "参照形须为 RESP3 map");
  assert_eq!(
    exec,
    [b"*1\r\n".as_slice(), ref_map.as_slice()].concat(),
    "EXEC 元素须为正常 HELLO map 而非围栏错误帧"
  );
  assert!(
    !exec
      .windows(HELLO_TXN_ERR.len())
      .any(|w| w == HELLO_TXN_ERR),
    "收口后 EXEC 应答不得出现围栏错误帧"
  );
}

/// 锁 2：MULTI + ACL LIST + EXEC——元素为 ACL 专属拒绝帧（非 HELLO 错位文案），
/// 对照事务外 ACL LIST 正常数组闭环
#[test]
fn exec_replay_acl_list_gets_acl_specific_frame() {
  let (_dir, store) = wtest_base::open_test_store("exec-replay-acl-frame.db").unwrap();
  let mut s = txn_session();
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  assert_eq!(feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n"), b"+OK\r\n");
  assert_eq!(
    feed(&mut s, &wtest_base::resp_frame(&[b"ACL", b"LIST"])),
    b"+QUEUED\r\n",
    "ACL 族无 NoMulti（C# Flags 亲验），排队面与 C# 同构"
  );
  let exec = feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n");
  assert_eq!(exec, [b"*1\r\n".as_slice(), ACL_TXN_ERR].concat());
  assert!(
    !exec.windows(b"ERR HELLO".len()).any(|w| w == b"ERR HELLO"),
    "ACL 命令不得共回 HELLO 错位文案（修复前此断言失败）"
  );
  assert_eq!(s.txn_state, TxnState::None, "事务闭环复位");
  // 对照真形态：普通命令窗口 ACL LIST 经停车臂正常闭环为规则数组（非错误帧）
  let list = feed(&mut s, &wtest_base::resp_frame(&[b"ACL", b"LIST"]));
  assert!(
    list.starts_with(b"*") && !list.starts_with(b"-"),
    "事务外 ACL LIST 须正常应答，拒绝面仅事务窗: {list:?}"
  );
}

/// 冷态可达库配置（hello_cold_park_pipeline_frames 同款：路由空闲析构期限 0
/// 秒即期，GC 手动驱动）
fn cold_config() -> StoreConfig {
  let mut cfg = wtest_base::test_store_config();
  cfg.gc = GcConfig {
    route_idle_evict_secs: 0,
    ..GcConfig::default()
  };
  cfg
}

/// 锁 3：同库 SELECT 冷库窄窗——排队准入后重放撞事务窗围栏，回 SELECT 专属帧
///
/// 装配期卸载映射（flush_and_evict_all + GC 后租户 5 db3 冷，
/// hello_cold_park_pipeline_frames::cold_env 同款物理介质形态）；会话标量按
/// 装配态落 ns5/db3（逻辑视图已在目标库、物理映射未装载），MULTI + SELECT 3
///（同库不中止）+ EXEC 重放撞 park_cold_context_load 围栏
#[test]
fn exec_replay_same_db_select_cold_window_gets_select_frame() {
  let dir = tempdir().unwrap();
  let store = Arc::new(
    WedbStore::open(
      cold_config(),
      Arc::new(SegmentedDevice::single_file(dir.path().join("exectxn.db")).unwrap()),
    )
    .unwrap(),
  );
  // 阶段 A：非严格会话建 db3 映射与数据后全量析构（磁盘为映射权威）
  {
    let session = store.new_session().unwrap();
    session.set_context(5, 3);
    let batch = session.enter_batch();
    batch.try_upsert_sync(b"k3", b"v3").unwrap().unwrap();
  }
  block_on(async {
    store.flush_and_evict_all().await.unwrap();
    sleep(Duration::from_millis(5)).await;
    GcManager::new(&store).run_once().await.unwrap();
  });
  assert!(
    store.vdb.is_cold_db(5, 3),
    "空闲析构后租户 5 db3 须冷，否则本用例不触达围栏"
  );

  let session = store.new_session().unwrap();
  session.set_strict_context(true);
  let mut s = txn_session();
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  s.max_databases = 16;
  // 装配期标量：逻辑视图已在 db3（同库 SELECT 排队准入放行）、物理映射未装载
  s.namespace = 5;
  s.active_db_id = 3;

  assert_eq!(feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n"), b"+OK\r\n");
  assert_eq!(
    feed(&mut s, &wtest_base::resp_frame(&[b"SELECT", b"3"])),
    b"+QUEUED\r\n",
    "同库 SELECT 与 C# NetworkSKIP 同构放行（异库中止臂不触达）"
  );
  let exec = feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n");
  eprintln!("EXEC={:?}", String::from_utf8_lossy(&exec));
  eprintln!(
    "cold={:?} slow={} txn={:?}",
    s.cold_pending_ctx(),
    s.take_slow_wait().is_some(),
    s.txn_state
  );
  assert_eq!(exec, [b"*1\r\n".as_slice(), SELECT_TXN_ERR].concat());
  assert!(s.take_slow_wait().is_none(), "事务窗严禁登记停泊挂起");
  assert!(s.cold_pending_ctx().is_none(), "暂存挂起面必已弃置");
  assert_eq!(s.active_db_id, 3, "装载未确认前标量零撕裂");
  assert_eq!(s.namespace, 5);
  assert_eq!(s.txn_state, TxnState::None, "事务闭环复位");
}
