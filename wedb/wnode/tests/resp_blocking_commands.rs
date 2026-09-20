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
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
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
    if let Some(slow) = self.consumer.take_slow_wait() {
      resp.extend_from_slice(&slow.resolve().await);
    }
    if let Some(blocked) = self.consumer.take_blocked_wait() {
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
    let blocked = self
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

#[test]
fn blpop_immediate_via_broker() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&resp_frame_str(&["RPUSH", "k", "v"]))
      .assert_eq_bytes(b":1\r\n");

    // 预置数据：BLPOP 挂起后由经纪 InitializeObserver 立即试取指派
    let resp = a.roundtrip(&resp_frame_str(&["BLPOP", "k", "10"])).await;
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
    a.feed(&resp_frame_str(&["BLPOP", "k2", "30"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let resp = b.feed(&resp_frame_str(&["LPUSH", "k2", "v2"]));
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
    let resp = a
      .roundtrip(&resp_frame_str(&["BLPOP", "absent", "0.2"]))
      .await;
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
  });
}

/// RespBlockingCollectionTests.cs:BasicBlockingListMoveTest（立即段）
#[test]
fn blmove_immediate_moves_to_dst() {
  Runtime::new().unwrap().block_on(async {
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
  });
}

/// RespBlockingCollectionTests.cs:BasicBlockingListPopPushTest（阻塞段）
#[test]
fn brpoplpush_blocks_until_push() {
  Runtime::new().unwrap().block_on(async {
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
  });
}

/// RespBlockingCollectionTests.cs:BlmpopBlockingWithCountTest
#[test]
fn blmpop_blocking_with_count() {
  Runtime::new().unwrap().block_on(async {
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
  });
}

/// RespBlockingCollectionTests.cs:BlockingSortedSetPopWrongTypeTests 的
/// String 反例 + 立即可取正例
#[test]
fn bzpopmin_immediate_and_wrongtype() {
  Runtime::new().unwrap().block_on(async {
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
  });
}

#[test]
fn bzpopmin_blocks_until_zadd() {
  Runtime::new().unwrap().block_on(async {
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
    a.feed(&resp_frame_str(&["BLPOP", "ub", "0"]));

    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let resp = b.feed(&resp_frame_str(&["CLIENT", "UNBLOCK", "11", "ERROR"]));
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
  });
}

/// 活跃分层键上 BLPOP 立即出件（next/qcode.data.md 条 28 差分）：经纪取件源
/// 对分层键恒判不可取，命令层 park 前预探后整体路由慢路径异步臂出件，
/// 不再挂起到超时回空
#[test]
fn blpop_on_tiered_key_pops_immediately() {
  Runtime::new().unwrap().block_on(async {
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
  });
}

/// RESP3 会话下阻塞族与 LMPOP 空回按版本分派为 `_\r\n`（next/qcode.data.md
/// 条 29：C# WriteNull/WriteNullArray 均为版本感知单点）
#[test]
fn blocking_empty_reply_resp3_null() {
  Runtime::new().unwrap().block_on(async {
    let h = Harness::new();
    let mut a = h.client(1);

    a.feed(&resp_frame_str(&["HELLO", "3"]));

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
  });
}
