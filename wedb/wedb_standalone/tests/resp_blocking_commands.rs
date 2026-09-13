//! 阻塞命令端到端集成测试（会话 + 存储执行域 + CollectionItemBroker 装配）
//!
//! 对标 garnet/test/standalone/Garnet.test.collections/RespBlockingCollectionTests.cs：
//! 立即可取、真阻塞唤醒（对端推入）、超时空回、FIFO 键序、BLMOVE/BRPOPLPUSH
//! 搬移落库、BLMPOP COUNT 批量、BZPOPMIN、WRONGTYPE、CLIENT UNBLOCK。

use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource,
    resp_server_session::RespServerSessionOptions,
  },
};
use wobject::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};

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
    let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    config.gc.enabled = false;
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

  /// 新建客户端（独立存储会话 + 经纪/配置注入，同单机装配形态）
  fn client(&self, id: u64) -> Client {
    let api = Arc::new(StoreGarnetApi::new(self.store.new_session().unwrap()));
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
    let (consumed, resp) = self.consumer.try_consume_messages(frame);
    assert!(consumed > 0, "命令帧应被完整消费");
    resp
  }

  /// 发一帧并驱动到全部完成（含阻塞命令挂起的 await 续驱与应答写出）
  async fn roundtrip(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut resp = self.feed(frame);
    if let Some(blocked) = self.consumer.take_blocked_wait() {
      let (cmd, result) = blocked.resolve().await;
      let reply = self.consumer.resolve_blocked_wait(cmd, result);
      resp.extend_from_slice(&reply);
    }
    resp
  }

  /// 已 feed 挂起的阻塞命令驱动到完成（网络泵 await 语义的测试等价物）
  async fn resolve_blocked(&mut self) -> Vec<u8> {
    let blocked = self
      .consumer
      .take_blocked_wait()
      .expect("应存在挂起的阻塞等待");
    let (cmd, result) = blocked.resolve().await;
    self.consumer.resolve_blocked_wait(cmd, result)
  }
}

/// 命令参数 → RESP 帧编码
fn frame(args: &[&str]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", args.len()).into_bytes();
  for a in args {
    out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
  }
  out
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

#[test]
fn blpop_immediate_via_broker() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&frame(&["RPUSH", "k", "v"]))
      .assert_eq_bytes(b":1\r\n");

    // 预置数据：BLPOP 挂起后由经纪 InitializeObserver 立即试取指派
    let resp = a.roundtrip(&frame(&["BLPOP", "k", "10"])).await;
    resp.assert_eq_bytes(b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");
  });
}

#[test]
fn blpop_blocks_until_push() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);
    let mut b = h.client(2);

    // A 挂起（真等待：resolve 无人推入则不返回）；50ms 后 B 推入唤醒
    a.feed(&frame(&["BLPOP", "k2", "30"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let resp = b.feed(&frame(&["LPUSH", "k2", "v2"]));
      resp.assert_eq_bytes(b":1\r\n");
    })
    .detach();

    let reply = a.resolve_blocked().await;
    reply.assert_eq_bytes(b"*2\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
  });
}

#[test]
fn blpop_timeout_returns_null_array() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    let start = Instant::now();
    let resp = a.roundtrip(&frame(&["BLPOP", "absent", "0.2"])).await;
    resp.assert_eq_bytes(b"*-1\r\n");
    assert!(
      start.elapsed() >= Duration::from_millis(190),
      "应真实等待至超时，实际 {:?}",
      start.elapsed()
    );
  });
}

/// RespBlockingCollectionTests.cs:ListBlockingPopOrderTest
#[test]
fn blpop_multi_key_fifo_order() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    for i in 1..=5 {
      a.feed(&frame(&["RPUSH", &format!("key{i}"), &format!("value{i}")]))
        .assert_eq_bytes(b":1\r\n");
    }
    for i in 1..=5 {
      let resp = a
        .roundtrip(&frame(&[
          "BLPOP", "key1", "key2", "key3", "key4", "key5", "10",
        ]))
        .await;
      resp.assert_eq_bytes(format!("*2\r\n$4\r\nkey{i}\r\n$6\r\nvalue{i}\r\n").as_bytes());
    }
  });
}

/// RespBlockingCollectionTests.cs:BasicBlockingListMoveTest（立即段）
#[test]
fn blmove_immediate_moves_to_dst() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&frame(&["LPUSH", "src", "v"]))
      .assert_eq_bytes(b":1\r\n");
    let resp = a
      .roundtrip(&frame(&["BLMOVE", "src", "dst", "RIGHT", "LEFT", "10"]))
      .await;
    resp.assert_eq_bytes(b"$1\r\nv\r\n");

    a.roundtrip(&frame(&["LRANGE", "src", "0", "-1"]))
      .await
      .assert_eq_bytes(b"*0\r\n");
    a.roundtrip(&frame(&["LRANGE", "dst", "0", "-1"]))
      .await
      .assert_eq_bytes(b"*1\r\n$1\r\nv\r\n");
  });
}

/// RespBlockingCollectionTests.cs:BasicBlockingListPopPushTest（阻塞段）
#[test]
fn brpoplpush_blocks_until_push() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);
    let mut b = h.client(2);

    a.feed(&frame(&["BRPOPLPUSH", "src2", "dst2", "30"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      b.feed(&frame(&["LPUSH", "src2", "v2"]));
    })
    .detach();

    let reply = a.resolve_blocked().await;
    reply.assert_eq_bytes(b"$2\r\nv2\r\n");

    // 源被弹空、目标持值
    a.roundtrip(&frame(&["LRANGE", "src2", "0", "-1"]))
      .await
      .assert_eq_bytes(b"*0\r\n");
    a.roundtrip(&frame(&["LRANGE", "dst2", "0", "-1"]))
      .await
      .assert_eq_bytes(b"*1\r\n$2\r\nv2\r\n");
  });
}

/// RespBlockingCollectionTests.cs:BlmpopBlockingWithCountTest
#[test]
fn blmpop_blocking_with_count() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);
    let mut b = h.client(2);

    a.feed(&frame(&[
      "BLMPOP", "30", "1", "countkey", "LEFT", "COUNT", "3",
    ]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      b.feed(&frame(&[
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
      .roundtrip(&frame(&[
        "BLMPOP", "30", "1", "countkey", "LEFT", "COUNT", "1",
      ]))
      .await;
    resp.assert_eq_bytes(b"*2\r\n$8\r\ncountkey\r\n*1\r\n$6\r\nvalue4\r\n");
    a.roundtrip(&frame(&["EXISTS", "countkey"]))
      .await
      .assert_eq_bytes(b":0\r\n");
  });
}

/// RespBlockingCollectionTests.cs:BlockingSortedSetPopWrongTypeTests 的
/// String 反例 + 立即可取正例
#[test]
fn bzpopmin_immediate_and_wrongtype() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&frame(&["ZADD", "z", "1.5", "m"]))
      .assert_eq_bytes(b":1\r\n");
    let resp = a.roundtrip(&frame(&["BZPOPMIN", "z", "10"])).await;
    resp.assert_eq_bytes(b"*3\r\n$1\r\nz\r\n$1\r\nm\r\n$3\r\n1.5\r\n");

    // WRONGTYPE：键持字符串
    a.feed(&frame(&["SET", "str", "x"]))
      .assert_eq_bytes(b"+OK\r\n");
    let resp = a.roundtrip(&frame(&["BLPOP", "str", "10"])).await;
    resp
      .assert_eq_bytes(b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n");
  });
}

#[test]
fn bzpopmin_blocks_until_zadd() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);
    let mut b = h.client(2);

    a.feed(&frame(&["BZPOPMIN", "zb", "30"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      b.feed(&frame(&["ZADD", "zb", "2.5", "zm"]));
    })
    .detach();

    let reply = a.resolve_blocked().await;
    reply.assert_eq_bytes(b"*3\r\n$2\r\nzb\r\n$2\r\nzm\r\n$3\r\n2.5\r\n");
  });
}

/// CLIENT UNBLOCK ERROR：被阻塞客户端收到 UNBLOCKED 错误应答
#[test]
fn client_unblock_error_wakes_blocked() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(11);
    let mut b = h.client(22);

    // timeout=0 → 无限等待；期间 B 以 ERROR 形态解除 A
    a.feed(&frame(&["BLPOP", "ub", "0"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let resp = b.feed(&frame(&["CLIENT", "UNBLOCK", "11", "ERROR"]));
      resp.assert_eq_bytes(b":1\r\n");
    })
    .detach();

    let reply = a.resolve_blocked().await;
    reply.assert_eq_bytes(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
  });
}

/// 同键 BLMOVE 同端捷径：列表不变、元素原样返回（C# 同键 no-pop 捷径）
#[test]
fn blmove_same_key_same_end_noop() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&frame(&["RPUSH", "rot", "a", "b"]))
      .assert_eq_bytes(b":2\r\n");
    // 同键 RIGHT RIGHT：恒等搬移，不弹出
    let resp = a
      .roundtrip(&frame(&["BLMOVE", "rot", "rot", "RIGHT", "RIGHT", "10"]))
      .await;
    resp.assert_eq_bytes(b"$1\r\nb\r\n");
    a.roundtrip(&frame(&["LLEN", "rot"]))
      .await
      .assert_eq_bytes(b":2\r\n");
  });
}
