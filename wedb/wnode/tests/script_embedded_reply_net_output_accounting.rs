#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 脚本内嵌 reply 净出计量回归锁（R4-4 实证探针，灭失判定复核的实证锁）
//!
//! 工单 task/ing/wnode-script-embedded-reply-double-count-net-output-metrics：
//! C# 契约（RespServerSession.cs:1451-1463 Send 唯一出向记账点、
//! SessionScriptCache.cs:24-26/:60-64 内嵌 processor 应答只进
//! ScratchBufferNetworkSender 与私有计数句柄）——外层会话活跃面
//! total_net_output_bytes 只应含最终 EVAL 应答一次。
//!
//! 统一不变式：每条命令泵一轮后，会话活跃面指标增量 == 本轮真实出网字节数
//! （内嵌 redis.call 应答随脚本转换值消亡、不入出网流亦不入账；脚本内挂起
//! 体应答同形，最终 EVAL 应答经出网通道入账一次）。
//! R4-4 在基线（take_output_into 无条件入账 + resume 余量绕账）实测转红：
//! 纯内嵌案活跃面 29 ≠ 出网 8（内嵌 SET/GET 应答入活跃面）；
//! 修复（account 判别分流，C# SessionScriptCache.cs:64 对位）后恒等。
//! 本席补水位让渡臂案（dispatch_resp 两臂同口径：只修收尾臂则水位臂大应答
//! 仍虚增活跃面）与反向漏计收口（resume 余量最终应答入账一次）。

use std::sync::Arc;

use compio::runtime::Runtime;
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wmetric::SessionMetricsHandle;
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  objects::collection_item_source::CollectionItemSource,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::{open_test_store, resp_frame as resp};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 会话活跃面出向指标读数（形态对位 resp_server_session_tests.rs 直读）
fn net_output_bytes(s: &RespServerSession) -> u64 {
  s.session_metrics
    .as_ref()
    .unwrap()
    .snapshot()
    .total_net_output_bytes
}

/// EVAL script numkeys keys... argv...
fn eval_frame(script: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<u8> {
  let mut parts: Vec<Vec<u8>> = vec![
    b"EVAL".to_vec(),
    script.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  parts.extend(keys.iter().map(|k| k.to_vec()));
  parts.extend(argv.iter().map(|a| a.to_vec()));
  let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
  resp(&refs)
}

/// 泵一轮（测试侧无网络泵对位）：消费 + 挂起让渡驱动，返回本轮全部出网
/// 字节。drive_pending_parks 承接挂起臂（应答直写 wire），drain_output 冲
/// 会话累积输出（唯一冲出口）；wire 即外层连接真实出向量，与指标增量对照
async fn pump(s: &mut RespServerSession, bytes: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages();
  let mut wire = Vec::new();
  wnode_test::drive_pending_parks(s, &mut wire, true).await;
  wire.extend_from_slice(&drain_output(s));
  wire
}

/// 同步壳泵一轮（无挂起案专用，形态对位 lua_script_tests.rs 的 block_on 壳）
fn pump_sync(s: &mut RespServerSession, bytes: &[u8]) -> Vec<u8> {
  Runtime::new().unwrap().block_on(pump(s, bytes))
}

/// enable_lua 会话 + 采样句柄装配（生产 service.rs 采样门控创建后同口注入
/// 的测试对位，形态镜像 lua_script_tests.rs:lua_session）
fn metrics_lua_session(store: &TestStore) -> RespServerSession {
  let session = store.new_session().unwrap();
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  s.attach_session_metrics(Some(Arc::new(SessionMetricsHandle::default())));
  s
}

/// 非脚本外层命令记账回归（不变式基线案）：SET 应答入账一次
#[test]
fn outer_command_accounts_wire_bytes_once() {
  let (_dir, store) = open_test_store("embedded-net-outer.db").expect("open test store");
  let mut s = metrics_lua_session(&store);
  let wire = pump_sync(&mut s, &resp(&[&b"SET"[..], b"k", b"v"]));
  assert_eq!(wire, b"+OK\r\n");
  assert_eq!(
    net_output_bytes(&s),
    wire.len() as u64,
    "非脚本命令活跃面入账须等于出网字节（回归不回退）"
  );
}

/// 纯内嵌案：EVAL 内多条 redis.call，内嵌应答只喂脚本不出网，
/// 活跃面只应见最终应答一次（基线下 delta = 内嵌和 + 最终 = 29 ≠ 8，转红）
#[test]
fn script_embedded_reply_not_in_active_net_output() {
  let (_dir, store) = open_test_store("embedded-net-pure.db").expect("open test store");
  let mut s = metrics_lua_session(&store);
  assert_eq!(net_output_bytes(&s), 0, "采样句柄装配后基线读数应为零");
  let wire = pump_sync(
    &mut s,
    &eval_frame(
      "redis.call('SET', KEYS[1], ARGV[1]); redis.call('GET', KEYS[1]); return redis.call('GET', KEYS[1])",
      &[b"ek"],
      &[b"vv"],
    ),
  );
  // 最终应答 = GET 的 bulk 串；内嵌 SET/GET 应答随转换值消亡不出网
  assert_eq!(wire, b"$2\r\nvv\r\n");
  assert_eq!(
    net_output_bytes(&s),
    wire.len() as u64,
    "内嵌 redis.call 应答字节不得入外层活跃面（C# 内嵌 processor 私有计数对位），\
     活跃面只计最终 EVAL 应答一次"
  );
}

type SharedBroker = Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>;

/// 脚本内阻塞挂起案：挂起体应答喂脚本（内嵌形态）不入活跃面，
/// 最终 EVAL 应答经 resume 余量直写通道入账一次
/// （基线下 delta = 挂起体 *2 帧 20 字节且最终应答漏计，转红）
#[compio::test]
async fn script_suspended_reply_not_in_active_net_output() {
  let (_dir, store) = open_test_store("embedded-net-blpop.db").expect("open test store");
  let broker: SharedBroker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let notify_broker = Arc::clone(&broker);
  let wait_broker = Arc::clone(&broker);
  let api = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
        notify_broker.handle_collection_update(domain, key)
      })))
      .with_item_broker_wait(Some(wait_broker)),
  );
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_garnet_api(api);
  s.set_item_broker(Arc::clone(&broker));
  s.attach_session_metrics(Some(Arc::new(SessionMetricsHandle::default())));

  // 预置队列元素（外层 RPUSH 入账回归：+4 字节一次）
  let wire = pump(&mut s, &resp(&[&b"RPUSH"[..], b"bq", b"v1"])).await;
  assert_eq!(wire, b":1\r\n");
  assert_eq!(net_output_bytes(&s), 4, "外层 RPUSH 应答入账一次");

  let wire = pump(
    &mut s,
    &eval_frame(
      "local r = redis.call('BLPOP', KEYS[1], 10) return r[1] .. ':' .. r[2]",
      &[b"bq"],
      &[],
    ),
  )
  .await;
  // 最终 EVAL 应答 = 拼接串 bulk；挂起体 *2 帧（20 字节）只喂脚本不出网
  assert_eq!(wire, b"$5\r\nbq:v1\r\n");
  assert_eq!(
    net_output_bytes(&s) - 4,
    wire.len() as u64,
    "脚本内挂起体应答不入活跃面；最终 EVAL 应答（resume 余量直写段）须入账一次"
  );
}

/// 水位让渡臂锁测（dispatch_resp 两臂同口径判据：只修收尾臂漏改水位让渡臂
/// 即本案转红）——脚本窗内单条 redis.call 应答越 OUTPUT_WATERMARK_BYTES，
/// 走水位让渡独立冲出；基线（水位臂无条件入账）下活跃面虚增该内嵌大应答
/// 字节量，delta ≫ 最终应答 9 字节，恒等断言红
#[test]
fn script_watermark_embedded_reply_not_in_active_net_output() {
  let (_dir, store) = open_test_store("embedded-net-wm.db").expect("open test store");
  let mut s = metrics_lua_session(&store);

  // 135168 字节值经外层 SET 写入（应答仅 +OK，不触水位）
  let big = vec![b'v'; (1 << 17) + 4096];
  let wire = pump_sync(&mut s, &resp(&[&b"SET"[..], b"wk", &big[..]]));
  assert_eq!(wire, b"+OK\r\n");
  assert_eq!(net_output_bytes(&s), 5, "外层大值 SET 只计 +OK 应答");

  let wire = pump_sync(
    &mut s,
    &eval_frame(
      "local v = redis.call('GET', KEYS[1]) return #v",
      &[b"wk"],
      &[],
    ),
  );
  assert_eq!(wire, b":135168\r\n");
  assert_eq!(
    net_output_bytes(&s) - 5,
    wire.len() as u64,
    "水位让渡冲出的内嵌大应答字节不得入外层活跃面（收尾臂与水位臂同收不入账）"
  );
}
