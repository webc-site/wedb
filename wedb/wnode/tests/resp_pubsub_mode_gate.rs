#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 订阅模式门进出场与 RESP3 数据命令通行（存储会话全管线）
//!
//! 补 wnode/tests/resp_pubsub.rs 会话级白名单表的两处 C# 等价缺口
//! （test/standalone/Garnet.test/RespPubSubTests.cs）：
//! - `PubSubModeViaPsubscribeRejectsCommandsInResp2`：仅经 PSUBSCRIBE 进入
//!   订阅态（无 SUBSCRIBE 参与）同样武装 RESP2 门；PUNSUBSCRIBE 退场后
//!   数据命令恢复真实执行——对标 C# 退场后 GET 回 `$-1\r\n` 存储真值，
//!   非仅「不报错」（白名单表只验拦截臂，未钉 psubscribe 入场与退场恢复）
//! - `PubSubModeAllowsRegularCommandsInResp3`：RESP3 订阅态下 SET/GET
//!   真实执行回正确值（+OK / `$5\r\nmyval`）——门只拦 RESP2，白名单表的
//!   RESP3 臂仅验「无 Can't execute」，未验执行面真值
//!
//! 装配对标生产 thread-per-core 形态：真实存储会话 + 消费者全管线
//! （recv → 解析 → 订阅门 → 存储分派），broker 经 attach_pubsub 接线。

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{drive_pending_parks_consumer, feed, roundtrip};
use wpubsub::subscribe_broker::SubscribeBroker;
use wtest_base::open_test_store;

type TestStore = WedbStore<SegmentedDevice>;

/// 独立连接装配：真实存储会话 + pubsub broker 接线（生产形态）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  c.attach_pubsub(Arc::new(SubscribeBroker::new()));
  c
}

/// RespPubSubTests.cs:PubSubModeViaPsubscribeRejectsCommandsInResp2
///
/// PSUBSCRIBE 单独入场 → RESP2 门拦截 GET（精确错误帧）→ PUNSUBSCRIBE
/// 退场 → GET 恢复真实存储执行（nil bulk `$-1\r\n`）
#[test]
fn pub_sub_mode_via_psubscribe_rejects_commands_in_resp2() {
  let rt = Runtime::new().unwrap();
  let (_dir, store) = open_test_store("pubsub-psub-gate").unwrap();
  let mut c = consumer_on(&store);
  assert!(!c.session().is_subscription_session, "入场前须非订阅态");

  // 仅经 PSUBSCRIBE 进入订阅态（无 SUBSCRIBE 参与）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"PSUBSCRIBE", b"foo*"]),
    &b"*3\r\n$10\r\npsubscribe\r\n$4\r\nfoo*\r\n:1\r\n"[..]
  );
  assert!(
    c.session().is_subscription_session,
    "psubscribe 入场须置订阅态"
  );

  // RESP2 订阅态下 GET 拦截（对标 C# errorResp 精确帧）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"bar"]),
    &b"-ERR Can't execute 'GET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n"[
      ..
    ]
  );

  // PUNSUBSCRIBE 退场（活跃数归零即退出订阅态）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"PUNSUBSCRIBE", b"foo*"]),
    &b"*3\r\n$12\r\npunsubscribe\r\n$4\r\nfoo*\r\n:0\r\n"[..]
  );
  assert!(!c.session().is_subscription_session, "退场须清订阅态");

  // 退场后 GET 恢复真实执行：走存储分派回 nil bulk（对标 C# "$-1\r\n"）
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"bar"]), &b"$-1\r\n"[..]);
}

/// RespPubSubTests.cs:PubSubModeAllowsRegularCommandsInResp3
///
/// 真实 HELLO 3 切协议 → SUBSCRIBE 入场 → 订阅态下 SET/GET 经存储真实
/// 执行回正确值（+OK / `$5\r\nmyval`）→ 退订收场
#[test]
fn pub_sub_mode_allows_regular_commands_in_resp3() {
  let rt = Runtime::new().unwrap();
  let (_dir, store) = open_test_store("pubsub-resp3-regular").unwrap();
  let mut c = consumer_on(&store);

  // 真实 HELLO 路径切 RESP3：无 AUTH 形预筛停车（dispatch_via_garnet_api 的
  // AUTH/HELLO/ACL 预筛），同步段零应答，交泵侧异步臂闭环（产线同款驱动循环）
  rt.block_on(async {
    let sync = feed(&mut c, &[b"HELLO", b"3"]);
    assert!(
      sync.is_empty(),
      "HELLO 3 须停车异步臂，同步段不应有应答: {sync:?}"
    );
    let mut resp_buf = Vec::new();
    drive_pending_parks_consumer(&mut c, &mut resp_buf).await;
    assert!(
      resp_buf.windows(b"proto".len()).any(|w| w == b"proto"),
      "HELLO 3 应答缺 proto 字段: {:?}",
      String::from_utf8_lossy(&resp_buf)
    );
  });
  assert_eq!(c.session().resp_protocol_version, 3);

  // 订阅 foo（RESP3 ack 帧与 RESP2 同形）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SUBSCRIBE", b"foo"]),
    &b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"[..]
  );

  // RESP3 订阅态下 SET/GET 真实执行（对标 C# +OK / $5\r\nmyval\r\n）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"mykey", b"myval"]),
    &b"+OK\r\n"[..]
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"mykey"]),
    &b"$5\r\nmyval\r\n"[..]
  );

  // 退订收场（活跃数归零退出订阅态）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"UNSUBSCRIBE", b"foo"]),
    &b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n"[..]
  );
}
