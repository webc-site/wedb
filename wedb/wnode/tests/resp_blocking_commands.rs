//! 阻塞命令端到端集成测试（会话 + 存储执行域 + CollectionItemBroker 装配）
//!
//! 对标 garnet/test/standalone/Garnet.test.collections/RespBlockingCollectionTests.cs：
//! 立即可取、真阻塞唤醒（对端推入）、超时空回、FIFO 键序、BLMOVE/BRPOPLPUSH
//! 搬移落库、BLMPOP COUNT 批量、BZPOPMIN、WRONGTYPE、CLIENT UNBLOCK。

use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{runtime::spawn, time::sleep};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker,
  item_broker_face::{ItemBrokerFinisher, SharedItemBroker},
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource,
    resp_server_session::RespServerSessionOptions,
  },
};
use wnode_test::err_frame;
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::{resp_frame_str, test_store_config};

/// 双客户端测试装配：共享存储 + 共享经纪 + 共享运行时配置
struct Harness {
  store: Arc<WedbStore<SegmentedDevice>>,
  broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  runtime_config: Arc<RuntimeServerConfig>,
  _dir: tempfile::TempDir,
}

impl Harness {
  fn new() -> Self {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("blocking.db")).unwrap());
    // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
    let config = test_store_config();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
      CollectionItemSource::new(store.new_session().unwrap()),
    ))));
    Self {
      store,
      broker,
      runtime_config: RuntimeServerConfig::shared_default(),
      _dir: dir,
    }
  }

  /// 新建客户端（独立存储会话 + 经纪/配置/慢路径写回唤醒注入，同单机装配
  /// 形态 service.rs：慢路径升阶收尾 notify 须经 collection_notify 回调抵达
  /// 经纪，缺面则 park 后升阶场景挂起观察者无人唤醒）
  fn client(&self, id: u64) -> Client {
    let notify_broker = Arc::clone(&self.broker);
    let wait_broker = Arc::clone(&self.broker) as Arc<dyn ItemBrokerFinisher>;
    let api = Arc::new(
      StoreGarnetApi::new(self.store.new_session().unwrap())
        .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
          notify_broker.handle_collection_update(domain, key)
        })))
        .with_item_broker_wait(Some(wait_broker)),
    );
    let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
    consumer.set_item_broker(self.broker.clone());
    consumer.set_runtime_config(self.runtime_config.clone());
    Client { consumer }
  }
}

/// 单客户端驱动面：同步消费 + 阻塞挂起续驱
struct Client {
  consumer: RespSessionConsumer,
}

impl Client {
  /// 发一帧并同步取答应（不含阻塞命令的延迟应答）
  fn feed(&mut self, frame: &[u8]) -> Vec<u8> {
    // 泵等价序：直填会话接收缓冲 → 唯一入口消费
    let mut scratch = self.consumer.take_recv_scratch();
    scratch.extend_from_slice(frame);
    self.consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    let remaining = self.consumer.try_consume_messages_into(&mut resp);
    assert!(remaining.is_some(), "命令帧应被完整消费");
    resp
  }

  /// 发一帧并驱动到全部完成（含慢路径挂起与阻塞命令挂起的 await 续驱、
  /// 应答写出）
  async fn roundtrip(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut resp = self.feed(frame);
    // AUTH/HELLO/ACL 族与挂载刷新停车臂闭环（HELLO 协议升级经此面）
    wnode_test::drive_pending_parks_consumer(&mut self.consumer, &mut resp).await;
    if let Some(slow) = self.consumer.take_slow_wait() {
      resp.extend_from_slice(&slow.resolve().await);
    }
    if let Some(mut blocked) = self.consumer.take_blocked_wait() {
      let (cmd, result) = blocked.resolve().await;
      // 应答字节按序进泵写缓冲（drive.rs 阻塞续驱同形，会话侧无 Vec 形态出口）
      self
        .consumer
        .resolve_blocked_wait_into(cmd, result, &mut resp);
    }
    resp
  }

  /// 已 feed 挂起的阻塞命令驱动到完成（网络泵 await 语义的测试等价物）
  async fn resolve_blocked(&mut self) -> Vec<u8> {
    let mut blocked = self
      .consumer
      .take_blocked_wait()
      .expect("应存在挂起的阻塞等待");
    let (cmd, result) = blocked.resolve().await;
    let mut reply = Vec::new();
    self
      .consumer
      .resolve_blocked_wait_into(cmd, result, &mut reply);
    reply
  }
}

/// 应答字节断言辅助
trait AssertBytes {
  fn assert_eq_bytes(&self, expected: &[u8]);
}

impl AssertBytes for Vec<u8> {
  fn assert_eq_bytes(&self, expected: &[u8]) {
    assert_eq!(
      String::from_utf8_lossy(self),
      String::from_utf8_lossy(expected),
      "应答不匹配"
    );
  }
}

#[compio::test]
async fn blpop_immediate_via_broker() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["RPUSH", "k", "v"]))
    .assert_eq_bytes(b":1\r\n");

  // 预置数据：BLPOP 挂起后由经纪 InitializeObserver 立即试取指派
  let resp = a.roundtrip(&resp_frame_str(&["BLPOP", "k", "10"])).await;
  resp.assert_eq_bytes(b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");
}

#[compio::test]
async fn blpop_blocks_until_push() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  // A 挂起（真等待：resolve 无人推入则不返回）；50ms 后 B 推入唤醒
  a.feed(&resp_frame_str(&["BLPOP", "k2", "30"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    let resp = b.feed(&resp_frame_str(&["LPUSH", "k2", "v2"]));
    resp.assert_eq_bytes(b":1\r\n");
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"*2\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
}

#[compio::test]
async fn blpop_timeout_returns_null_array() {
  let h = Harness::new();
  let mut a = h.client(1);

  let start = Instant::now();
  let resp = a
    .roundtrip(&resp_frame_str(&["BLPOP", "absent", "0.2"]))
    .await;
  resp.assert_eq_bytes(b"*-1\r\n");
  assert!(
    start.elapsed() >= Duration::from_millis(190),
    "应真实等待至超时，实际 {:?}",
    start.elapsed()
  );
}

/// RespBlockingCollectionTests.cs:ListBlockingPopOrderTest
#[compio::test]
async fn blpop_multi_key_fifo_order() {
  let h = Harness::new();
  let mut a = h.client(1);

  for i in 1..=5 {
    a.feed(&resp_frame_str(&[
      "RPUSH",
      &format!("key{i}"),
      &format!("value{i}"),
    ]))
    .assert_eq_bytes(b":1\r\n");
  }
  for i in 1..=5 {
    let resp = a
      .roundtrip(&resp_frame_str(&[
        "BLPOP", "key1", "key2", "key3", "key4", "key5", "10",
      ]))
      .await;
    resp.assert_eq_bytes(format!("*2\r\n$4\r\nkey{i}\r\n$6\r\nvalue{i}\r\n").as_bytes());
  }
}

/// RespBlockingCollectionTests.cs:BasicBlockingListMoveTest（立即段）
#[compio::test]
async fn blmove_immediate_moves_to_dst() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["LPUSH", "src", "v"]))
    .assert_eq_bytes(b":1\r\n");
  let resp = a
    .roundtrip(&resp_frame_str(&[
      "BLMOVE", "src", "dst", "RIGHT", "LEFT", "10",
    ]))
    .await;
  resp.assert_eq_bytes(b"$1\r\nv\r\n");

  a.roundtrip(&resp_frame_str(&["LRANGE", "src", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*0\r\n");
  a.roundtrip(&resp_frame_str(&["LRANGE", "dst", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*1\r\n$1\r\nv\r\n");
}

/// RespBlockingCollectionTests.cs:BasicBlockingListPopPushTest（阻塞段）
#[compio::test]
async fn brpoplpush_blocks_until_push() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  a.feed(&resp_frame_str(&["BRPOPLPUSH", "src2", "dst2", "30"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    b.feed(&resp_frame_str(&["LPUSH", "src2", "v2"]));
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"$2\r\nv2\r\n");

  // 源被弹空、目标持值
  a.roundtrip(&resp_frame_str(&["LRANGE", "src2", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*0\r\n");
  a.roundtrip(&resp_frame_str(&["LRANGE", "dst2", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*1\r\n$2\r\nv2\r\n");
}

/// RespBlockingCollectionTests.cs:BlmpopBlockingWithCountTest
#[compio::test]
async fn blmpop_blocking_with_count() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  a.feed(&resp_frame_str(&[
    "BLMPOP", "30", "1", "countkey", "LEFT", "COUNT", "3",
  ]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    b.feed(&resp_frame_str(&[
      "RPUSH", "countkey", "value1", "value2", "value3", "value4",
    ]));
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(
    b"*2\r\n$8\r\ncountkey\r\n*3\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n",
  );

  // 剩余 1 项再弹后键回收
  let resp = a
    .roundtrip(&resp_frame_str(&[
      "BLMPOP", "30", "1", "countkey", "LEFT", "COUNT", "1",
    ]))
    .await;
  resp.assert_eq_bytes(b"*2\r\n$8\r\ncountkey\r\n*1\r\n$6\r\nvalue4\r\n");
  a.roundtrip(&resp_frame_str(&["EXISTS", "countkey"]))
    .await
    .assert_eq_bytes(b":0\r\n");
}

/// RespBlockingCollectionTests.cs:BlockingSortedSetPopWrongTypeTests 的
/// String 反例 + 立即可取正例
#[compio::test]
async fn bzpopmin_immediate_and_wrongtype() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["ZADD", "z", "1.5", "m"]))
    .assert_eq_bytes(b":1\r\n");
  let resp = a.roundtrip(&resp_frame_str(&["BZPOPMIN", "z", "10"])).await;
  resp.assert_eq_bytes(b"*3\r\n$1\r\nz\r\n$1\r\nm\r\n$3\r\n1.5\r\n");

  // WRONGTYPE：键持字符串
  a.feed(&resp_frame_str(&["SET", "str", "x"]))
    .assert_eq_bytes(b"+OK\r\n");
  let resp = a.roundtrip(&resp_frame_str(&["BLPOP", "str", "10"])).await;
  resp.assert_eq_bytes(&err_frame(RESP_ERR_WRONG_TYPE));
}

#[compio::test]
async fn bzpopmin_blocks_until_zadd() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  a.feed(&resp_frame_str(&["BZPOPMIN", "zb", "30"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    b.feed(&resp_frame_str(&["ZADD", "zb", "2.5", "zm"]));
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"*3\r\n$2\r\nzb\r\n$2\r\nzm\r\n$3\r\n2.5\r\n");
}

/// CLIENT UNBLOCK ERROR：被阻塞客户端收到 UNBLOCKED 错误应答
#[compio::test]
async fn client_unblock_error_wakes_blocked() {
  let h = Harness::new();
  let mut a = h.client(11);
  let mut b = h.client(22);

  // timeout=0 → 无限等待；期间 B 以 ERROR 形态解除 A
  a.feed(&resp_frame_str(&["BLPOP", "ub", "0"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    let resp = b.feed(&resp_frame_str(&["CLIENT", "UNBLOCK", "11", "ERROR"]));
    resp.assert_eq_bytes(b":1\r\n");
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
}

/// 同键 BLMOVE 同端捷径：列表不变、元素原样返回（C# 同键 no-pop 捷径）
#[compio::test]
async fn blmove_same_key_same_end_noop() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["RPUSH", "rot", "a", "b"]))
    .assert_eq_bytes(b":2\r\n");
  // 同键 RIGHT RIGHT：恒等搬移，不弹出
  let resp = a
    .roundtrip(&resp_frame_str(&[
      "BLMOVE", "rot", "rot", "RIGHT", "RIGHT", "10",
    ]))
    .await;
  resp.assert_eq_bytes(b"$1\r\nb\r\n");
  a.roundtrip(&resp_frame_str(&["LLEN", "rot"]))
    .await
    .assert_eq_bytes(b":2\r\n");
}

/// 同键 BLMOVE 多元素异端旋转经经纪出件：元素完整保留、端点流转正确、长度不减
///（对标 C# RespListTests.cs:ListMoveSameKeyRotationOnImmutableRecord 的旋转契约
/// 在阻塞经纪出件面的同款形态，出件路径 RespBlockingCollectionTests.cs:
/// BasicBlockingListMoveTest；回归票 zcode-r165c-listmove 案一——旧形态同键
/// 双写回覆盖致元素蒸发、长度递减）
#[compio::test]
async fn blmove_same_key_rotation_via_broker_preserves_elements() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["RPUSH", "rotr", "a", "b", "c", "d"]))
    .assert_eq_bytes(b":4\r\n");
  // LEFT → RIGHT：队首 a 流转到队尾，列表 [b c d a]，长度不变
  a.roundtrip(&resp_frame_str(&[
    "BLMOVE", "rotr", "rotr", "LEFT", "RIGHT", "10",
  ]))
  .await
  .assert_eq_bytes(b"$1\r\na\r\n");
  a.roundtrip(&resp_frame_str(&["LRANGE", "rotr", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*4\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\na\r\n");
  // RIGHT → LEFT：队尾 a 流转回队首，列表还原 [a b c d]，长度不变
  a.roundtrip(&resp_frame_str(&[
    "BLMOVE", "rotr", "rotr", "RIGHT", "LEFT", "10",
  ]))
  .await
  .assert_eq_bytes(b"$1\r\na\r\n");
  a.roundtrip(&resp_frame_str(&["LRANGE", "rotr", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");
  a.roundtrip(&resp_frame_str(&["LLEN", "rotr"]))
    .await
    .assert_eq_bytes(b":4\r\n");
}

/// 同键 BRPOPLPUSH 多元素旋转经经纪出件：队尾弹出推入队首，元素完整、长度不减
///（C# ListBlockingPopPush = ListBlockingMove(Right, Left) 定式；回归票
/// zcode-r165c-listmove 案一同键臂）
#[compio::test]
async fn brpoplpush_same_key_rotation_via_broker_preserves_elements() {
  let h = Harness::new();
  let mut a = h.client(1);

  a.feed(&resp_frame_str(&["RPUSH", "rplp", "a", "b", "c"]))
    .assert_eq_bytes(b":3\r\n");
  a.roundtrip(&resp_frame_str(&["BRPOPLPUSH", "rplp", "rplp", "10"]))
    .await
    .assert_eq_bytes(b"$1\r\nc\r\n");
  a.roundtrip(&resp_frame_str(&["LRANGE", "rplp", "0", "-1"]))
    .await
    .assert_eq_bytes(b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n");
  a.roundtrip(&resp_frame_str(&["LLEN", "rplp"]))
    .await
    .assert_eq_bytes(b":3\r\n");
}

/// 活跃分层键上 BLPOP 立即出件（对标 C# ListCommands.cs 阻塞取件）：经纪取件源
/// 对分层键恒判不可取，命令层 park 前预探后整体路由慢路径异步臂出件，
/// 不再挂起到超时回空
#[compio::test]
async fn blpop_on_tiered_key_pops_immediately() {
  let h = Harness::new();
  let mut a = h.client(1);

  // 升阶：单条 RPUSH 灌 65540 元素跨过条目数阈值（65536）自动分层。
  // 同步段对超阈写回一律 Degrade 转异步臂（杜绝 4MB 级信封整值写），
  // 应答由慢路径补写，故用 roundtrip 驱动到完成
  let mut args: Vec<String> = vec!["RPUSH".into(), "big".into()];
  for i in 0..65_540 {
    args.push(format!("v{i}"));
  }
  let refs: Vec<&str> = args.iter().map(String::as_str).collect();
  a.roundtrip(&resp_frame_str(&refs))
    .await
    .assert_eq_bytes(b":65540\r\n");

  // BLPOP 队首直取（v0 为 RPUSH 首位元素）；未修复前挂经纪恒不可取至超时
  let resp = a.roundtrip(&resp_frame_str(&["BLPOP", "big", "5"])).await;
  resp.assert_eq_bytes(b"*2\r\n$3\r\nbig\r\n$2\r\nv0\r\n");
}

/// RESP3 会话下阻塞族与 LMPOP 空回按版本分派为 `_\r\n`（对标 C# WriteNull/WriteNullArray 版本感知单点）
#[compio::test]
async fn blocking_empty_reply_resp3_null() {
  let h = Harness::new();
  let mut a = h.client(1);

  // HELLO 3 协议升级经停车臂 async 闭环（应答不参与本用例断言）
  let _ = a.roundtrip(&resp_frame_str(&["HELLO", "3"])).await;

  // 经纪挂起至超时：BLPOP 空回 WriteNullArray → RESP3 `_\r\n`
  let resp = a
    .roundtrip(&resp_frame_str(&["BLPOP", "absent", "0.2"]))
    .await;
  resp.assert_eq_bytes(b"_\r\n");

  // 立即段 LMPOP NOTFOUND → WriteNullArray → RESP3 `_\r\n`
  let resp = a
    .roundtrip(&resp_frame_str(&["LMPOP", "1", "absent", "LEFT"]))
    .await;
  resp.assert_eq_bytes(b"_\r\n");
}

/// RESP3 会话下 BZPOPMIN 出件（唤醒收口 write_collection_item_result）分值
/// 按版本分派为 `,1.5` double（对标 C# SortedSetCommands.cs:1619
/// WriteDoubleNumeric；修复前恒 `$3\r\n1.5` bulk，与同族四臂异形）
#[compio::test]
async fn bzpopmin_wake_reply_resp3_double() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  let _ = a.roundtrip(&resp_frame_str(&["HELLO", "3"])).await;

  a.feed(&resp_frame_str(&["BZPOPMIN", "zc", "30"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    b.feed(&resp_frame_str(&["ZADD", "zc", "1.5", "zn"]));
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"*3\r\n$2\r\nzc\r\n$2\r\nzn\r\n,1.5\r\n");
}

/// RESP3 会话下 BZMPOP 出件逐对分值同款 `,1.5` double（对标 C#
/// SortedSetCommands.cs:1734 WriteDoubleNumeric）
#[compio::test]
async fn bzmpop_wake_reply_resp3_double() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  let _ = a.roundtrip(&resp_frame_str(&["HELLO", "3"])).await;

  a.feed(&resp_frame_str(&["BZMPOP", "30", "1", "zd", "MIN"]));

  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    b.feed(&resp_frame_str(&["ZADD", "zd", "1.5", "ze"]));
  })
  .detach();

  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"*2\r\n$2\r\nzd\r\n*1\r\n*2\r\n$2\r\nze\r\n,1.5\r\n");
}

/// 票 zcode-r23-broker 发现一（端到端）：BLPOP k 0 挂起后，单命令 LPUSH
/// 7 万元素令 k 升阶分层——经纪取件源对分层键恒报同步不可出件，挂起观察
/// 者被送空应答（非永久挂死）；空回复重试经 park 前预探整体路由慢路径，
/// 从分层键弹出元素闭环。升阶前形态下此序列为队首 FIFO break 永久饥饿
#[compio::test]
async fn blpop_promoted_key_unblocks_with_empty_reply() {
  let h = Harness::new();
  let mut a = h.client(1);
  let mut b = h.client(2);

  a.feed(&resp_frame_str(&["BLPOP", "k", "0"]));

  // 单命令 7 万元素推入：快臂 should_promote 命中整体转慢路径，升阶落
  // 库后收尾 notify 经纪
  let items: Vec<String> = (0..70_000).map(|i| format!("x{i:05}")).collect();
  let mut refs: Vec<&str> = Vec::with_capacity(items.len() + 2);
  refs.push("LPUSH");
  refs.push("k");
  refs.extend(items.iter().map(String::as_str));
  b.roundtrip(&resp_frame_str(&refs))
    .await
    .assert_eq_bytes(b":70000\r\n");

  // 挂起观察者被送空应答（RESP2 空数组），元素留集
  let reply = a.resolve_blocked().await;
  reply.assert_eq_bytes(b"*-1\r\n");

  // 空回复重试：预探命中分层键，整体路由慢路径并从分层键弹出（LPUSH
  // 头插，队首为 x69999）
  a.feed(&resp_frame_str(&["BLPOP", "k", "0"]));
  let slow = a.consumer.take_slow_wait().expect("重试须路由慢路径");
  let reply = slow.resolve().await;
  assert!(
    String::from_utf8_lossy(&reply).starts_with("*2\r\n$1\r\nk\r\n$6\r\nx69999"),
    "重试应经慢路径弹出分层键元素，实际 {reply:?}"
  );
}

// 票 zcode-r23-broker 发现一的 zset 同形（BZPOPMIN 挂起 + ZUNIONSTORE 大
// 结果落 k 升阶）在集成层不可达闭环，甄别记录：zset 慢路径 STORE 冷臂按
// 整 blob 一次性写目标键——16MB 测试基线推导 256KB 页，7 万成员目标 blob
// 约 1.3MB 超页被拒（-ERR slow path storage error，写入根本未及升阶）；
// 大预算大页下 blob 直写信封同步可装载，不触发分层、经纪照常出件。即
// zset 侧「挂起后升阶」窗被存储写面形态挡死，升阶饥饿修复面由 list 集成
// 断言（blpop_promoted_key_unblocks_with_empty_reply，同一 TryGetOutcome
// 送客机制）与 wcol 层 MemStore degrade 断言（命令族无关）共同覆盖；
// zset 非升阶写入唤醒回归由既有 bzpopmin_blocks_until_zadd 覆盖。
