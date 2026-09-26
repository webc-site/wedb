//! 阻塞族慢路径阻塞语义回归测试（zcode-r16-list 发现一）
//!
//! 修复前：BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP 经冷键/分层键降级慢路径
//! 后，装载未取到即立即写空值应答——timeout 计时与 0 无限等待契约失效，
//! 同一命令对同一键因冷热给出「挂起后 null」与「瞬时 null」两种时序。
//! 修复后：慢路径装载未取到经经纪等待面闭环（slow.rs BlockWaitFace，
//! C# ListBlockingPop :284 / ListBlockingMove :373-376 /
//! ListBlockingPopMultiple :913 无条件 BlockingWait 的键态解耦语义），
//! 出件或超时后经 write_collection_item_result 与快路径同一应答单源出帧。
//!
//! 对标 garnet/test/standalone/Garnet.test.collections/
//! RespBlockingCollectionTests.cs 的阻塞唤醒/超时形态 + 冷态（磁盘降级）
//! 构造（list_pop_cold_wrongtype.rs 的 flush_and_evict_all 冷化同款）。
//! 全部用例单 Runtime 驱动（经纪主循环随首次 start_wait 驻留该 runtime，
//! 唤醒方任务与等待体必须同域调度）。

use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker,
  item_broker_face::{ItemBrokerFinisher, SharedItemBroker},
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource},
};
use wtest_base::{resp_frame_str, test_store_config};

/// 双客户端装配：共享存储 + 共享经纪（慢路径等待面与快路径 park 同源）
struct Harness {
  store: Arc<WedbStore<SegmentedDevice>>,
  broker: Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>,
  _dir: tempfile::TempDir,
}

impl Harness {
  fn new(tag: &str) -> Self {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
      CollectionItemSource::new(store.new_session().unwrap()),
    ))));
    Self {
      store,
      broker,
      _dir: dir,
    }
  }

  /// 新建客户端（快路径 park 经纪 + 慢路径等待面 + 慢路径写回唤醒三面
  /// 注入，同单机装配形态 service.rs）
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
    let mut consumer = RespSessionConsumer::new(id, Default::default(), api);
    consumer.set_item_broker(Arc::clone(&self.broker));
    Client {
      consumer,
      store: Arc::clone(&self.store),
    }
  }

  /// 新建无经纪客户端（独立会话域：慢路径无等待面，维持立即可取语义）
  fn client_no_broker(&self, id: u64) -> Client {
    let api = Arc::new(StoreGarnetApi::new(self.store.new_session().unwrap()));
    let consumer = RespSessionConsumer::new(id, Default::default(), api);
    Client {
      consumer,
      store: Arc::clone(&self.store),
    }
  }
}

/// 单客户端驱动面：同步消费 + 挂起体续驱（全部 await 于用例外层 runtime）
struct Client {
  consumer: RespSessionConsumer,
  store: Arc<WedbStore<SegmentedDevice>>,
}

impl Client {
  /// 发一帧并同步取答应（不含阻塞/慢路径延迟应答）
  fn feed(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut scratch = self.consumer.take_recv_scratch();
    scratch.extend_from_slice(frame);
    self.consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    let remaining = self.consumer.try_consume_messages_into(&mut resp);
    assert!(remaining.is_some(), "命令帧应被完整消费");
    resp
  }

  /// 慢路径写回冷化的全量刷盘驱逐（两键均仅驻留磁盘）
  async fn evict(&self) {
    self.store.flush_and_evict_all().await.unwrap();
  }

  /// 发一帧并驱动到全部完成：慢路径挂起体 resolve（阻塞等待闭环在其内
  /// 联 await）→ 阻塞挂起体 resolve（快路径 park 形态），两形态应答均按
  /// 泵序并入
  async fn roundtrip(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut resp = self.feed(frame);
    if let Some(slow) = self.consumer.take_slow_wait() {
      resp.extend_from_slice(&slow.resolve().await);
    }
    if let Some(mut blocked) = self.consumer.take_blocked_wait() {
      let (cmd, result) = blocked.resolve().await;
      self
        .consumer
        .resolve_blocked_wait_into(cmd, result, &mut resp);
    }
    resp
  }
}

/// 冷墓碑定式：写键 → 冷化 → DEL（墓碑热）→ 再冷化（墓碑冷）。
/// BLPOP 族 String 域探针遇冷墓碑 Deferred 整体路由慢路径，异步装载裁决
/// Missing——修复前此处立即回空值，修复后转经纪等待闭环
async fn cold_tombstone(c: &mut Client, key: &str, val: &str) {
  assert!(
    !c.roundtrip(&resp_frame_str(&["RPUSH", key, val]))
      .await
      .is_empty()
  );
  c.evict().await;
  assert!(!c.roundtrip(&resp_frame_str(&["DEL", key])).await.is_empty());
  c.evict().await;
}

/// 延迟推入任务（等待期间另一会话写键唤醒经纪观察者；spawn 于用例
/// runtime，与经纪主循环同域调度）
fn delayed_push(h: &Harness, delay: u64, key: &'static str, val: &'static str) {
  let mut c = h.client(99);
  spawn(async move {
    sleep(Duration::from_millis(delay)).await;
    assert!(
      !c.roundtrip(&resp_frame_str(&["LPUSH", key, val]))
        .await
        .is_empty()
    );
  })
  .detach();
}

#[test]
fn blpop_cold_tombstone_waits_then_wakes() {
  let h = Harness::new("blocking-cold-wake.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    cold_tombstone(&mut a, "k", "v").await;
    // 修复前：瞬时回 *-1（阻塞语义丢失）；修复后：挂起等待 200ms 后唤醒出件
    let resp = b.feed(&resp_frame_str(&["BLPOP", "k", "5"]));
    assert!(resp.is_empty(), "冷墓碑 BLPOP 应转慢路径挂起，不立即应答");
    let slow = b.consumer.take_slow_wait().expect("应存在慢路径挂起体");
    delayed_push(&h, 200, "k", "w");
    let resp = slow.resolve().await;
    assert_eq!(resp, b"*2\r\n$1\r\nk\r\n$1\r\nw\r\n");
    // 唤醒出件即弹走元素，键被删空自愈
    assert_eq!(
      a.roundtrip(&resp_frame_str(&["EXISTS", "k"])).await,
      b":0\r\n"
    );
  });
}

#[test]
fn blpop_cold_tombstone_timeout_not_instant() {
  let h = Harness::new("blocking-cold-timeout.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    cold_tombstone(&mut a, "k", "v").await;
    // timeout=1：修复前瞬时回 *-1；修复后等待满 1 秒超时（RESP2 空数组）
    let resp = b.feed(&resp_frame_str(&["BLPOP", "k", "1"]));
    assert!(resp.is_empty());
    let slow = b.consumer.take_slow_wait().expect("应存在慢路径挂起体");
    let started = Instant::now();
    let resp = slow.resolve().await;
    assert_eq!(resp, b"*-1\r\n");
    assert!(
      started.elapsed() >= Duration::from_millis(900),
      "超时应付满 timeout，不应瞬时回空"
    );
  });
}

#[test]
fn blmove_cold_missing_src_waits_then_wakes() {
  let h = Harness::new("blocking-cold-blmove.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    cold_tombstone(&mut a, "src", "sv").await;
    // src 冷墓碑装载 Missing → 等待闭环；唤醒后经纪 BLMOVE 臂完整执行
    // 弹+推（C# ListBlockingMove :373-376 BlockingWait 同位）
    let resp = b.feed(&resp_frame_str(&[
      "BLMOVE", "src", "dst", "RIGHT", "LEFT", "5",
    ]));
    assert!(resp.is_empty());
    let slow = b.consumer.take_slow_wait().expect("应存在慢路径挂起体");
    delayed_push(&h, 200, "src", "mv");
    let resp = slow.resolve().await;
    // 应答恰为一个 bulk 帧（弹出元素），流水线不错位
    assert_eq!(resp, b"$2\r\nmv\r\n");
    // dst 落库断言（经纪出件臂完整执行搬移）
    assert_eq!(
      a.roundtrip(&resp_frame_str(&["LRANGE", "dst", "0", "-1"]))
        .await,
      b"*1\r\n$2\r\nmv\r\n"
    );
    assert_eq!(
      a.roundtrip(&resp_frame_str(&["EXISTS", "src"])).await,
      b":0\r\n"
    );
  });
}

#[test]
fn blmpop_cold_tombstone_timeout_single_null_frame() {
  let h = Harness::new("blocking-cold-blmpop.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    cold_tombstone(&mut a, "k", "v").await;
    let resp = b.feed(&resp_frame_str(&["BLMPOP", "1", "1", "k", "LEFT"]));
    assert!(resp.is_empty());
    let slow = b.consumer.take_slow_wait().expect("应存在慢路径挂起体");
    let started = Instant::now();
    let resp = slow.resolve().await;
    // BLMPOP 未取到 → 单个 null 帧（RESP2 $-1），超时竞速付满
    assert_eq!(resp, b"$-1\r\n");
    assert!(started.elapsed() >= Duration::from_millis(900));
  });
}

#[test]
fn blpop_cold_present_pops_without_wait() {
  let h = Harness::new("blocking-cold-present.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    // 冷键但装载可出件：直接弹出，零等待（C# 首轮试取即完成同位）
    assert!(
      !a.roundtrip(&resp_frame_str(&["RPUSH", "k", "x", "y"]))
        .await
        .is_empty()
    );
    a.evict().await;
    let resp = b.roundtrip(&resp_frame_str(&["BLPOP", "k", "5"])).await;
    assert_eq!(resp, b"*2\r\n$1\r\nk\r\n$1\r\nx\r\n");
  });
}

#[test]
fn blpop_hot_and_cold_wake_bytes_identical() {
  let h = Harness::new("blocking-cold-align.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut hot = h.client(2);
    let mut cold = h.client(3);

    // 热路径：键在内存信封，快路径 park_broker_wait 挂起，经纪首轮试取出件
    assert!(
      !a.roundtrip(&resp_frame_str(&["RPUSH", "hot", "v"]))
        .await
        .is_empty()
    );
    let hot_resp = hot.roundtrip(&resp_frame_str(&["BLPOP", "hot", "5"])).await;
    // 冷路径：冷墓碑装载 Missing，慢路径等待闭环唤醒出件
    cold_tombstone(&mut a, "cold", "v").await;
    let resp = cold.feed(&resp_frame_str(&["BLPOP", "cold", "5"]));
    assert!(resp.is_empty());
    let cold_slow = cold.consumer.take_slow_wait().expect("应存在慢路径挂起体");
    delayed_push(&h, 200, "cold", "v");
    let cold_resp = cold_slow.resolve().await;
    // 帧型同构：*2 + bulk 键 + bulk 元素，元素编码逐字节全等
    assert_eq!(hot_resp, b"*2\r\n$3\r\nhot\r\n$1\r\nv\r\n");
    assert_eq!(cold_resp, b"*2\r\n$4\r\ncold\r\n$1\r\nv\r\n");
  });
}

#[test]
fn blpop_broker_absent_stays_immediate() {
  // 经纪未注入域（独立会话域）：冷墓碑经慢路径装载 Missing 后无等待面，
  // 维持立即可取语义立即回空值，不挂起不等待
  let h = Harness::new("blocking-no-broker.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client_no_broker(1);

    cold_tombstone(&mut a, "k", "v").await;
    let started = Instant::now();
    let resp = a.roundtrip(&resp_frame_str(&["BLPOP", "k", "5"])).await;
    assert_eq!(resp, b"*-1\r\n");
    assert!(
      started.elapsed() < Duration::from_millis(500),
      "无经纪域应立即回空，不得挂起等待"
    );
  });
}

#[test]
fn blpop_cold_tombstone_client_unblock_negative_id_noop_and_timeout_kept() {
  // 慢路径观察者 ID 取自 usize::MAX 递减专用域：
  // 客户端发送 CLIENT UNBLOCK -1 等负数 ID 时，被 network_clientunblock
  // 单点门禁拦截并回 :0，不得通过 as usize 别名命中解脱慢路径等待体；
  // 慢路径等待体不受影响，超时 1 秒后正常付满并回 *-1
  let h = Harness::new("blocking-cold-unblock-neg.db");
  Runtime::new().unwrap().block_on(async {
    let mut a = h.client(1);
    let mut b = h.client(2);

    cold_tombstone(&mut a, "k", "v").await;
    // client b 发起 BLPOP k 1 进入慢路径挂起
    let resp = b.feed(&resp_frame_str(&["BLPOP", "k", "1"]));
    assert!(resp.is_empty(), "冷墓碑 BLPOP 应转慢路径挂起");
    let slow = b.consumer.take_slow_wait().expect("应存在慢路径挂起体");

    // client a 发起 CLIENT UNBLOCK -1、CLIENT UNBLOCK -1 TIMEOUT、CLIENT UNBLOCK -1 ERROR、CLIENT UNBLOCK -2、CLIENT UNBLOCK i64::MIN
    // 均被门禁拦截直接返回 :0
    let resp_unblock = a
      .roundtrip(&resp_frame_str(&["CLIENT", "UNBLOCK", "-1"]))
      .await;
    assert_eq!(resp_unblock, b":0\r\n");
    let resp_unblock_timeout = a
      .roundtrip(&resp_frame_str(&["CLIENT", "UNBLOCK", "-1", "TIMEOUT"]))
      .await;
    assert_eq!(resp_unblock_timeout, b":0\r\n");
    let resp_unblock_err = a
      .roundtrip(&resp_frame_str(&["CLIENT", "UNBLOCK", "-1", "ERROR"]))
      .await;
    assert_eq!(resp_unblock_err, b":0\r\n");
    let resp_unblock_neg2 = a
      .roundtrip(&resp_frame_str(&["CLIENT", "UNBLOCK", "-2"]))
      .await;
    assert_eq!(resp_unblock_neg2, b":0\r\n");
    let resp_unblock_min = a
      .roundtrip(&resp_frame_str(&[
        "CLIENT",
        "UNBLOCK",
        "-9223372036854775808",
      ]))
      .await;
    assert_eq!(resp_unblock_min, b":0\r\n");

    // 慢路径等待体不应被解除，超时正常付满 1 秒并回 *-1（RESP2 空数组）
    let started = Instant::now();
    let resp = slow.resolve().await;
    assert_eq!(resp, b"*-1\r\n");
    assert!(
      started.elapsed() >= Duration::from_millis(900),
      "CLIENT UNBLOCK 负数 ID 门禁后，慢路径等待体不得被提前唤醒"
    );
  });
}
