//! 阻塞族跨租户命名空间隔离集成测试（provider 装配链 + ACL 认证绑域 + 共享经纪）
//!
//! 回归锚点：阻塞命令经经纪注册观察者的键必须以会话域 (ns, db) 折叠，取件
//! 必须在观察者所属域执行（C# 取件随 observer.Session.storageSession，
//! CollectionItemBroker.cs:269/:337；rust 经 CollectionItemSource 单例会话
//! set_context 等价切换）。隔离破坏时的失效形态：他域同名键推入唤醒观察者
//! 并被其越域消费（A 抢走 B 的数据、B 自己反而取不到）。

use std::sync::Arc;

use tempfile::tempdir;
use wacl::AccessControlList;
use wdev::SegmentedDevice;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::{resp_frame_str, test_store_config};

/// 会话装配闭包的统一类型别名（测试 Harness 可命名形态）
type Decorate =
  Box<dyn Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync>;

/// 单客户端驱动面：同步消费 + 阻塞挂起续驱（resp_blocking_commands 同形）
struct Client {
  consumer: RespSessionConsumer,
}

impl Client {
  /// 发一帧并同步取答应（不含阻塞命令的延迟应答）
  fn feed(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut scratch = self.consumer.take_recv_scratch();
    scratch.extend_from_slice(frame);
    self.consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    let remaining = self.consumer.try_consume_messages_into(&mut resp);
    assert!(remaining.is_some(), "命令帧应被完整消费");
    resp
  }

  /// 发一帧并驱动到全部完成（含阻塞命令挂起的 await 续驱）
  async fn roundtrip(&mut self, frame: &[&str]) -> Vec<u8> {
    let mut resp = self.feed(&resp_frame_str(frame));
    // AUTH/HELLO/ACL 族与挂载刷新停车臂闭环（HELLO 协议升级经此面）
    wnode_test::drive_pending_parks_consumer(&mut self.consumer, &mut resp).await;
    if let Some(mut blocked) = self.consumer.take_blocked_wait() {
      let (cmd, result) = blocked.resolve().await;
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

/// 测试装配：ACL 认证可用，消费者经 provider 装配链注入自带共享经纪
///（node_components 装配的经纪取件源与命令会话同源同引擎）
struct Harness {
  provider: StorageSessionProvider<Decorate>,
  _dir: tempfile::TempDir,
}

impl Harness {
  fn new() -> Self {
    let dir = tempdir().unwrap();
    let acl = Arc::new(AccessControlList::new("").unwrap());
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("ns_blocking.db"),
      Box::new(move |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      }) as Decorate,
    )
    .unwrap()
    .with_acl(acl);
    Self {
      provider,
      _dir: dir,
    }
  }

  /// 新建客户端（provider 装配链产出，共享经纪已注入）
  fn client(&self, id: u64) -> Client {
    Client {
      consumer: self.provider.get_session(WireFormat::Ascii, id).unwrap(),
    }
  }
}

/// 跨租户同名阻塞队列互不串扰：ns2 推入的数据必须留在 ns2，ns1 的阻塞观察者
/// 既不被唤醒也不越域取件；各租户自己的域内链路照常闭环
#[compio::test]
async fn blpop_same_key_across_namespaces_is_isolated() {
  let h = Harness::new();

  // ns0 超管建两个租户用户（样板 acl_namespace_admin_tests）
  let mut admin = h.client(1);
  admin
    .roundtrip(&["ACL", "SETUSER", "1#bob", "on", ">bobpw", "+@all"])
    .await
    .assert_eq_bytes(b"+OK\r\n");
  admin
    .roundtrip(&["ACL", "SETUSER", "2#carol", "on", ">carolpw", "+@all"])
    .await
    .assert_eq_bytes(b"+OK\r\n");

  // A 绑 ns1、B 绑 ns2
  let mut a = h.client(2);
  a.roundtrip(&["AUTH", "1#bob", "bobpw"])
    .await
    .assert_eq_bytes(b"+OK\r\n");
  assert_eq!(a.consumer.session().namespace, 1);
  let mut b = h.client(3);
  b.roundtrip(&["AUTH", "2#carol", "carolpw"])
    .await
    .assert_eq_bytes(b"+OK\r\n");
  assert_eq!(b.consumer.session().namespace, 2);

  // A 在 ns1 挂起 BLPOP q（短超时：隔离破坏时会被他域数据提前唤醒）
  a.feed(&resp_frame_str(&["BLPOP", "q", "0.5"]));

  // B 在 ns2 推入同名键并自取：数据必须留在 B 域被 B 消费
  b.feed(&resp_frame_str(&["RPUSH", "q", "x"]))
    .assert_eq_bytes(b":1\r\n");
  let b_reply = b.roundtrip(&["BLPOP", "q", "0.5"]).await;
  b_reply.assert_eq_bytes(b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n");

  // A 未被 B 域唤醒：阻塞至自身超时空回（隔离破坏时此处将收到 [q, x]）
  let a_reply = a.resolve_blocked().await;
  a_reply.assert_eq_bytes(b"*-1\r\n");

  // A 域内链路照常：推入唤醒自己
  a.feed(&resp_frame_str(&["RPUSH", "q", "y"]))
    .assert_eq_bytes(b":1\r\n");
  let a_own = a.roundtrip(&["BLPOP", "q", "0.5"]).await;
  a_own.assert_eq_bytes(b"*2\r\n$1\r\nq\r\n$1\r\ny\r\n");
}

/// 同租户跨库同名阻塞队列互不串扰：折叠键含 db 段，db0 推入的数据不得
/// 唤醒 db1 的阻塞观察者
#[compio::test]
async fn blpop_same_key_across_databases_is_isolated() {
  let h = Harness::new();

  let mut admin = h.client(1);
  admin
    .roundtrip(&["ACL", "SETUSER", "1#bob", "on", ">bobpw", "+@all"])
    .await
    .assert_eq_bytes(b"+OK\r\n");

  // 同一租户两个会话：db0 与 db1
  let mut d0 = h.client(2);
  d0.roundtrip(&["AUTH", "1#bob", "bobpw"])
    .await
    .assert_eq_bytes(b"+OK\r\n");
  let mut d1 = h.client(3);
  d1.roundtrip(&["AUTH", "1#bob", "bobpw"])
    .await
    .assert_eq_bytes(b"+OK\r\n");
  d1.feed(&resp_frame_str(&["SELECT", "1"]))
    .assert_eq_bytes(b"+OK\r\n");
  assert_eq!(d1.consumer.session().active_db_id, 1);

  // db1 挂起 BLPOP q
  d1.feed(&resp_frame_str(&["BLPOP", "q", "0.5"]));

  // db0 推入同名键并自取：数据留在 db0
  d0.feed(&resp_frame_str(&["RPUSH", "q", "x"]))
    .assert_eq_bytes(b":1\r\n");
  let d0_reply = d0.roundtrip(&["BLPOP", "q", "0.5"]).await;
  d0_reply.assert_eq_bytes(b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n");

  // db1 观察者未被跨库唤醒：阻塞至超时空回
  let d1_reply = d1.resolve_blocked().await;
  d1_reply.assert_eq_bytes(b"*-1\r\n");
}
