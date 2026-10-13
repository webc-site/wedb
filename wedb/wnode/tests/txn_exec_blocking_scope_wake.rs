#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 阻塞族 EXEC 内嵌形状 + 经纪 zset 臂 scoped 同桶互斥回归
//!（票 wtxn-wkv-keybucket-hash-scope-desync 补强注记）
//!
//! C# 对位 CollectionItemBroker.cs:585-598：`txnManager.state==Running` 时经纪
//! TryGetResult 并入外层事务直取臂（外层已持本键排他闩，恒不遇闩）。rust 经纪
//! 为独立会话不可并入他事务，拆两臂承接：外域瞬时持闩 = 争用重投 wake 臂
//!（[`wcol`] `TryGetOutcome::is_contended` → 主循环让核重投 CollectionUpdated，
//! 重投闭环机制由 wcol 经纪单测锁定）；本事务自持闩（EXEC 重放段）= 会话
//! 命令臂让闩直取臂（持闩窗 = 阻塞等待窗，重投永不收敛，见判据二）。本件锁
//! 三判据：
//! 1. 口径统一后经纪 zset 臂 `try_rmw_window` 与外层事务在同一 scoped 主桶上
//!    互斥——持外层事务同款 scoped 桶闩时 `try_get_result` 报争用位（修复前
//!    裸桶域异桶直取 = 本票已裁 bypass 危害的消费方实例，异桶永不争用）；
//! 2. MULTI; BZPOPMIN k 0; EXEC 自给键场景：重放臂让闩直取即出即应答，
//!    不挂经纪不 park——修复前照常 park 后经纪对本事务自持闩恒报争用位，
//!    主循环让核重投永不收敛，timeout=0 永悬即回归炸出；
//! 3. 出件物理落库：弹出后键空。
//!
//! List 面扩展（票 wnode-list-blocking-exec-txn-latch-self-deadlock，护门判据
//! 与 zset 先例同源）：BLPOP/BLMOVE 非空键 MULTI+EXEC 重放直取即出即应答
//! 不 park；空键直取未果照先例回挂臂登记经纪等待面、超时收口不挂死；
//! 非事务会话照常 park（护门 `!txn_direct` 短路保持原挂起形状）。

use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wcol::itembroker::{
  collection_item_broker::{CollectionItemBroker, CollectionItemStore},
  item_broker_face::SharedItemBroker,
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use windex::HashIndex;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource,
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
};
use wnode_test::AssertBytes;
use wresp::command::RespCommand;
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 事务锁面钉定 store 当前索引（生产 `build_txn_lock_table` 的测试等价形态：
/// 事务闩与 wkv 窗/经纪臂同一份 HashIndex 锁内存、同一 scoped 寻桶口径）
fn lock_table_on(store: &Arc<WedbStore<SegmentedDevice>>) -> TxnLockTable {
  let index_store = Arc::clone(store);
  TxnLockTable::from_loader(move || index_store.index.load_full())
}

#[test]
fn exec_embedded_bzpopmin_self_provided_key_no_hang() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("exec-block.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let runtime_config = RuntimeServerConfig::shared_default();
  let lock_table = lock_table_on(&store);

  let make_client = |id: u64| -> RespSessionConsumer {
    let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
    let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
    consumer.set_item_broker(Arc::clone(&broker));
    consumer.set_runtime_config(Arc::clone(&runtime_config));
    consumer
      .attach_transaction_components(Arc::new(WatchVersionMap::new(1024)), lock_table.clone());
    consumer
  };

  Runtime::new().unwrap().block_on(async {
    let mut a = make_client(1);

    // 自给键：EXEC 前 ZADD 预置非空集合（取件对象由本事务之外的前置写入提供）
    {
      let mut scratch = a.take_recv_scratch();
      scratch.extend_from_slice(&resp_frame_str(&["ZADD", "k", "1", "m1"]));
      a.return_recv_scratch(scratch);
      let mut out = Vec::new();
      assert!(a.try_consume_messages_into(&mut out).is_some());
      assert_eq!(out, b":1\r\n");
    }

    // 判据一：持本键 scoped 桶排他闩（EXEC 重放期外层事务的持闩形状）时，
    // 经纪 zset 臂 try_get_result 报争用位而非异桶直取；放闩即直取得件
    let index = store.active_index();
    let scoped_bucket =
      index.bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), b"k"));
    assert_ne!(
      scoped_bucket,
      index.bucket_index_for_hash(HashIndex::hash_key(b"k")),
      "夹具前提：本键 scoped 桶与裸哈希桶互异（修复前异桶直取 bypass 形态）"
    );
    {
      assert!(
        index.get_bucket(scoped_bucket).try_lock_exclusive(),
        "夹具钉闩前提：目标桶须空闲"
      );
      let source = CollectionItemSource::new(store.new_session().unwrap());
      let outcome = source.try_get_result(0, 0, b"k", RespCommand::Bzpopmin, &[], false);
      assert!(
        outcome.is_contended && outcome.result.is_none(),
        "外层持闩期经纪 zset 臂必须报争用位（scoped 同桶互斥闭环）"
      );
      index.get_bucket(scoped_bucket).unlock_exclusive();
      let outcome = source.try_get_result(0, 0, b"k", RespCommand::Bzpopmin, &[], false);
      assert!(outcome.result.is_some(), "放闩后自给键直取得件");
    }

    // 判据二：MULTI; BZPOPMIN k 0; EXEC —— 事务重放段外层直取臂（C#
    // CollectionItemBroker.cs:585-598 txnManagerLock 对位）：外层已持本键
    // scoped 桶排他闩，重放臂让闩直取即出即应答，不挂经纪不 park——修复前
    // 照常 park 后经纪对本事务自持闩恒报争用位、主循环让核重投永不收敛
    //（持闩窗 = 阻塞等待窗：放闩待尾帧 EXEC 提交，提交待阻塞出件闭环），
    // timeout=0 永悬即本回归炸出。自给键重播：判据一探针已把 m1 弹空，
    // 预置前置写入复原非空前提
    {
      let mut scratch = a.take_recv_scratch();
      scratch.extend_from_slice(&resp_frame_str(&["ZADD", "k", "1", "m1"]));
      a.return_recv_scratch(scratch);
      let mut out = Vec::new();
      assert!(a.try_consume_messages_into(&mut out).is_some());
      assert_eq!(out, b":1\r\n", "自给键重播复原非空前提");
    }
    {
      let mut scratch = a.take_recv_scratch();
      for frame in [
        resp_frame_str(&["MULTI"]),
        resp_frame_str(&["BZPOPMIN", "k", "0"]),
        resp_frame_str(&["EXEC"]),
      ] {
        scratch.extend_from_slice(&frame);
      }
      a.return_recv_scratch(scratch);
      let mut out = Vec::new();
      assert!(a.try_consume_messages_into(&mut out).is_some());
      assert_eq!(
        out, b"+OK\r\n+QUEUED\r\n*1\r\n*3\r\n$1\r\nk\r\n$2\r\nm1\r\n$1\r\n1\r\n",
        "MULTI/QUEUED 应答与 EXEC 数组头先写，重放臂让闩直取即时补元素行闭环"
      );
      assert!(
        a.take_blocked_wait().is_none(),
        "事务直取臂不得挂起阻塞等待（park 即自持闩争用永悬回归）"
      );
    }

    // 判据三：出件物理落库，键弹空
    {
      let mut scratch = a.take_recv_scratch();
      scratch.extend_from_slice(&resp_frame_str(&["ZCARD", "k"]));
      a.return_recv_scratch(scratch);
      let mut out = Vec::new();
      assert!(a.try_consume_messages_into(&mut out).is_some());
      assert_eq!(out, b":0\r\n");
    }
  });
}

/// List 面回归装配（判据与 zset 装配同源：store + 共享经纪 + 带事务组件客户端）
struct ListFaceHarness {
  _dir: tempfile::TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  lock_table: TxnLockTable,
}

impl ListFaceHarness {
  fn new(name: &str) -> Self {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name)).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
      CollectionItemSource::new(store.new_session().unwrap()),
    ))));
    Self {
      _dir: dir,
      lock_table: lock_table_on(&store),
      store,
      broker,
    }
  }

  fn client(&self, id: u64) -> RespSessionConsumer {
    let api = Arc::new(StoreGarnetApi::new(self.store.new_session().unwrap()));
    let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
    consumer.set_item_broker(Arc::clone(&self.broker));
    consumer.set_runtime_config(RuntimeServerConfig::shared_default());
    consumer.attach_transaction_components(
      Arc::new(WatchVersionMap::new(1024)),
      self.lock_table.clone(),
    );
    consumer
  }
}

/// 发一帧并同步取答应（不含阻塞命令的延迟应答，泵直填序）
fn feed(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  resp
}

/// 发多帧一批并同步取答应
fn feed_batch(consumer: &mut RespSessionConsumer, frames: &[&[u8]]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  for frame in frames {
    scratch.extend_from_slice(frame);
  }
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  resp
}

/// 判据（List 面单弹臂）：MULTI; BLPOP k 0; EXEC 非空自给键——重放臂让闩直取
/// 即出即应答不挂经纪不 park；修复前照常 park 后经纪对本事务自持闩恒报争用
/// 位，主循环让核重投永不收敛，timeout=0 永悬即回归炸出
#[test]
fn exec_embedded_blpop_self_provided_key_no_hang() {
  let h = ListFaceHarness::new("blpop-direct.db");

  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);

    // 自给键预置：EXEC 前 RPUSH 提供非空前提（事务外前置写入）
    feed(&mut a, resp_frame_str(&["RPUSH", "k", "v1"]).as_slice()).assert_eq_bytes(b":1\r\n");

    let out = feed_batch(
      &mut a,
      &[
        &resp_frame_str(&["MULTI"]),
        &resp_frame_str(&["BLPOP", "k", "0"]),
        &resp_frame_str(&["EXEC"]),
      ],
    );
    out.assert_eq_bytes(b"+OK\r\n+QUEUED\r\n*1\r\n*2\r\n$1\r\nk\r\n$2\r\nv1\r\n");
    assert!(
      a.take_blocked_wait().is_none(),
      "事务直取臂不得挂起阻塞等待（park 即自持闩争用永悬回归，timeout=0 必炸）"
    );

    // 出件物理落库：键弹空
    feed(&mut a, resp_frame_str(&["LLEN", "k"]).as_slice()).assert_eq_bytes(b":0\r\n");
  });
}

/// 判据（List 面搬移臂）：MULTI; BLMOVE src dst RIGHT LEFT 0; EXEC 非空自给
/// 键——重放臂让闩直取复用 list_move_core 即出即应答，元素落目标键、源键弹空
#[test]
fn exec_embedded_blmove_self_provided_key_no_hang() {
  let h = ListFaceHarness::new("blmove-direct.db");

  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);

    feed(&mut a, resp_frame_str(&["RPUSH", "src", "v1"]).as_slice()).assert_eq_bytes(b":1\r\n");

    let out = feed_batch(
      &mut a,
      &[
        &resp_frame_str(&["MULTI"]),
        &resp_frame_str(&["BLMOVE", "src", "dst", "RIGHT", "LEFT", "0"]),
        &resp_frame_str(&["EXEC"]),
      ],
    );
    out.assert_eq_bytes(b"+OK\r\n+QUEUED\r\n*1\r\n$2\r\nv1\r\n");
    assert!(
      a.take_blocked_wait().is_none(),
      "事务直取臂不得挂起阻塞等待（park 即自持闩争用永悬回归，timeout=0 必炸）"
    );

    // 搬移物理落库：目标键持值、源键弹空回收
    feed(
      &mut a,
      resp_frame_str(&["LRANGE", "dst", "0", "-1"]).as_slice(),
    )
    .assert_eq_bytes(b"*1\r\n$2\r\nv1\r\n");
    feed(&mut a, resp_frame_str(&["LLEN", "src"]).as_slice()).assert_eq_bytes(b":0\r\n");
  });
}

/// 判据（List 面空键回挂臂）：MULTI; BLPOP ek 0.2; EXEC 空键直取未果——照
/// zset 先例回挂臂登记经纪等待面（应答延后写），超时收口空数组不挂死；尾帧
/// EXEC 经泵续驱提交，事务落幕后会话恢复正常消费
#[test]
fn exec_embedded_blpop_empty_key_backhang_no_hang() {
  let h = ListFaceHarness::new("blpop-backhang.db");

  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);

    let out = feed_batch(
      &mut a,
      &[
        &resp_frame_str(&["MULTI"]),
        &resp_frame_str(&["BLPOP", "ek", "0.2"]),
        &resp_frame_str(&["EXEC"]),
      ],
    );
    out.assert_eq_bytes(b"+OK\r\n+QUEUED\r\n*1\r\n");

    // 回挂臂：登记经纪等待面（挂起体在，超时可收口，非同步挂死）
    let mut blocked = a
      .take_blocked_wait()
      .expect("空键直取未果须照先例回挂经纪等待面");
    let start = Instant::now();
    let (cmd, result) = blocked.resolve().await;
    assert!(
      start.elapsed() >= Duration::from_millis(190),
      "应真实等待至超时，实际 {:?}",
      start.elapsed()
    );
    let mut reply = Vec::new();
    a.resolve_blocked_wait_into(cmd, result, &mut reply);
    reply.assert_eq_bytes(b"*-1\r\n");

    // 尾帧 EXEC 续驱提交（网络泵 await 后继续消费的泵序等价），事务落幕后会话可用
    let mut out = Vec::new();
    assert!(a.try_consume_messages_into(&mut out).is_some());
    out.assert_eq_bytes(b"");
    assert!(a.take_blocked_wait().is_none());

    let pong = feed(&mut a, resp_frame_str(&["PING"]).as_slice());
    pong.assert_eq_bytes(b"+PONG\r\n");
  });
}

/// 判据（非事务回归）：非事务会话 BLPOP 照常挂经纪（护门 `!txn_direct` 短路
/// 保持原挂起形状），超时空回不破
#[test]
fn non_txn_blpop_still_parks_and_times_out() {
  let h = ListFaceHarness::new("blpop-nontxn.db");

  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);

    let out = feed(
      &mut a,
      resp_frame_str(&["BLPOP", "absent", "0.2"]).as_slice(),
    );
    out.assert_eq_bytes(b"");

    let mut blocked = a.take_blocked_wait().expect("非事务会话须照常挂经纪");
    let (cmd, result) = blocked.resolve().await;
    let mut reply = Vec::new();
    a.resolve_blocked_wait_into(cmd, result, &mut reply);
    reply.assert_eq_bytes(b"*-1\r\n");
  });
}
