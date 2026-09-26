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

use std::sync::Arc;

use compio::runtime::Runtime;
use wcol::itembroker::{
  collection_item_broker::{CollectionItemBroker, CollectionItemStore},
  item_broker_face::SharedItemBroker,
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource,
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
};
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
      index.bucket_index_for_key(b"k"),
      "夹具前提：本键 scoped 桶与裸哈希桶互异（修复前异桶直取 bypass 形态）"
    );
    {
      assert!(
        index.bucket(scoped_bucket).try_lock_exclusive(),
        "夹具钉闩前提：目标桶须空闲"
      );
      let source = CollectionItemSource::new(store.new_session().unwrap());
      let outcome = source.try_get_result(0, 0, b"k", RespCommand::Bzpopmin, &[], false);
      assert!(
        outcome.is_contended && outcome.result.is_none(),
        "外层持闩期经纪 zset 臂必须报争用位（scoped 同桶互斥闭环）"
      );
      index.bucket(scoped_bucket).unlock_exclusive();
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
