use std::sync::Arc;

use wnode::{
  ClusterSessionFace,
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::command::RespCommand;

fn session(id: i64) -> RespServerSession {
  let mut s = RespServerSession::new(id, RespServerSessionOptions::default());
  s.attach_pubsub(Arc::new(SubscribeBroker::new()));
  s
}

fn output(s: &mut RespServerSession) -> String {
  let bytes = drain_output(s);
  String::from_utf8_lossy(&bytes).into_owned()
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicSUBSCRIBE
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

#[test]
fn basic_subscribe() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut s = session(1);
  s.attach_pubsub(broker.clone());
  assert!(s.network_subscribe(false, &[b"messages"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$8\r\nmessages\r\n:1\r\n"
  );
  assert_eq!(s.pubsub.num_active_channels(), 1);
  assert!(s.is_subscription_session);
  // broker 全量为 ns 隔离键（会话默认 ns 0 → 前缀 "0:"，见 wpubsub::channel_ns）
  assert_eq!(broker.num_subscriptions(b"0:messages"), 1);

  // Unsubscribe
  assert!(s.network_unsubscribe(&[b"messages"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$8\r\nmessages\r\n:0\r\n"
  );
  assert!(!s.is_subscription_session);
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPSUBSCRIBE
#[test]
fn basic_psubscribe() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut s = session(1);
  s.attach_pubsub(broker.clone());
  let glob = b"messagesA*";
  assert!(s.network_psubscribe(&[glob]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$10\r\npsubscribe\r\n$10\r\nmessagesA*\r\n:1\r\n"
  );

  // publish match（直投 broker 须以隔离键入表；会话面 drain 后应答仍为裸通道名）
  assert_eq!(
    broker.publish_now(b"0:messagesAtest", b"published message"),
    1
  );
  assert_eq!(s.drain_pubsub_frames(), 1);
  assert_eq!(
    output(&mut s),
    "*4\r\n$8\r\npmessage\r\n$10\r\nmessagesA*\r\n$13\r\nmessagesAtest\r\n$17\r\npublished message\r\n"
  );

  // Unsubscribe
  assert!(s.network_punsubscribe(&[glob]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$12\r\npunsubscribe\r\n$10\r\nmessagesA*\r\n:0\r\n"
  );
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPUBSUB_CHANNELS
#[test]
fn basic_pubsub_channels() {
  let mut a = session(1);
  let mut b = session(2);
  let broker = Arc::new(SubscribeBroker::new());
  a.attach_pubsub(broker.clone());
  b.attach_pubsub(broker);

  a.network_subscribe(false, &[b"messagesAtest"]);
  b.network_subscribe(false, &[b"messagesB"]);
  output(&mut a);
  output(&mut b);

  a.network_pubsub_channels(&[]);
  let out = output(&mut a);
  assert!(out.contains("*2\r\n"));
  assert!(out.contains("$13\r\nmessagesAtest\r\n"));
  assert!(out.contains("$9\r\nmessagesB\r\n"));

  // pattern filter
  a.network_pubsub_channels(&[b"messages?test"]);
  let out = output(&mut a);
  assert_eq!(out, "*1\r\n$13\r\nmessagesAtest\r\n");

  a.network_pubsub_channels(&[b"messagesC*"]);
  let out = output(&mut a);
  assert_eq!(out, "*0\r\n");
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPUBSUB_NUMPAT
#[test]
fn basic_pubsub_numpat() {
  let mut s = session(1);

  s.network_pubsub_numpat(&[]);
  assert_eq!(output(&mut s), ":0\r\n");

  s.network_psubscribe(&[b"com.messages.*", b"com.messagesB.*"]);
  output(&mut s);

  s.network_pubsub_numpat(&[]);
  assert_eq!(output(&mut s), ":2\r\n");

  s.network_punsubscribe(&[b"com.messages.*", b"com.messagesB.*"]);
  output(&mut s);

  s.network_pubsub_numpat(&[]);
  assert_eq!(output(&mut s), ":0\r\n");
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPUBSUB_NUMSUB
#[test]
fn basic_pubsub_numsub() {
  let mut s = session(1);

  // empty query
  s.network_pubsub_numsub(&[]);
  assert_eq!(output(&mut s), "*0\r\n");

  // no subscribers yet
  s.network_pubsub_numsub(&[b"messagesA", b"messagesB"]);
  assert_eq!(
    output(&mut s),
    "*4\r\n$9\r\nmessagesA\r\n:0\r\n$9\r\nmessagesB\r\n:0\r\n"
  );

  s.network_subscribe(false, &[b"messagesA", b"messagesB"]);
  output(&mut s);

  s.network_pubsub_numsub(&[b"messagesA", b"messagesB"]);
  assert_eq!(
    output(&mut s),
    "*4\r\n$9\r\nmessagesA\r\n:1\r\n$9\r\nmessagesB\r\n:1\r\n"
  );
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:PubSubModeAllowsValidCommandsInResp2
#[test]
fn pub_sub_mode_allows_valid_commands_in_resp2() {
  let mut s = session(1);

  // Enter subscription mode
  assert!(s.network_subscribe(false, &[b"foo"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"
  );

  // Another SUBSCRIBE
  assert!(s.network_subscribe(false, &[b"bar"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n"
  );

  // PSUBSCRIBE
  assert!(s.network_psubscribe(&[b"baz*"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$10\r\npsubscribe\r\n$4\r\nbaz*\r\n:3\r\n"
  );

  // UNSUBSCRIBE bar
  assert!(s.network_unsubscribe(&[b"bar"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n"
  );

  // PUNSUBSCRIBE baz*
  assert!(s.network_punsubscribe(&[b"baz*"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$12\r\npunsubscribe\r\n$4\r\nbaz*\r\n:1\r\n"
  );

  // UNSUBSCRIBE foo (last channel exits subscription mode)
  assert!(s.network_unsubscribe(&[b"foo"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n"
  );
  assert!(!s.is_subscription_session);
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:PubSubSelfPublishResp3NoLockError
#[test]
fn pub_sub_self_publish_resp3_no_lock_error() {
  let mut s = session(1);
  s.update_resp_protocol_version(3);

  assert!(s.network_subscribe(false, &[b"foo"]));
  output(&mut s);

  // Self-publish
  assert!(s.network_publish(false, &[b"foo", b"bar"]));
  assert_eq!(
    output(&mut s),
    ">3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:1\r\n"
  );

  assert!(s.network_unsubscribe(&[b"foo"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n"
  );
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:PubSubSelfPatternPublishResp3NoLockError
#[test]
fn pub_sub_self_pattern_publish_resp3_no_lock_error() {
  let mut s = session(1);
  s.update_resp_protocol_version(3);

  assert!(s.network_psubscribe(&[b"foo*"]));
  output(&mut s);

  assert!(s.network_publish(false, &[b"foobar" as &[u8], b"baz" as &[u8]]));
  assert_eq!(
    output(&mut s),
    ">4\r\n$8\r\npmessage\r\n$4\r\nfoo*\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n:1\r\n"
  );

  assert!(s.network_punsubscribe(&[b"foo*"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$12\r\npunsubscribe\r\n$4\r\nfoo*\r\n:0\r\n"
  );
}

/// 对标 Garnet PR #1669 / test/standalone/Garnet.test/RespPubSubTests.cs:
/// - PubSubModeAllowsValidCommandsInResp2
/// - PubSubModeRejectsDisallowedCommandsInResp2
#[test]
fn pub_sub_mode_resp2_whitelist_commands() {
  let mut s = session(10);
  assert_eq!(s.resp_protocol_version, 2);
  s.is_subscription_session = true;

  // 1. PING: 允许，响应订阅模式 PONG（对标 C# RespPubSubTests.cs:285 "*2\r\n$4\r\npong\r\n$0\r\n\r\n"）
  let consumed = feed(&mut s, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(consumed, Some(0));
  assert_eq!(output(&mut s), "*2\r\n$4\r\npong\r\n$0\r\n\r\n");

  // 2. RESET: 允许，不报错
  let consumed = feed(&mut s, b"*1\r\n$5\r\nRESET\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("Can't execute"));

  // 3. SUNSUBSCRIBE: 允许，不报错
  let consumed = feed(&mut s, b"*1\r\n$12\r\nSUNSUBSCRIBE\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("Can't execute"));

  // 4. SUBSCRIBE: 允许，不报错
  let consumed = feed(&mut s, b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("Can't execute"));

  // 5. QUIT: 允许，不报错
  let consumed = feed(&mut s, b"*1\r\n$4\r\nQUIT\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("Can't execute"));

  // 6. GET: 非白名单命令，拦截报错并保持连接
  let consumed = feed(&mut s, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR Can't execute 'GET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n"
  );

  // 7. SET: 非白名单命令，拦截报错并保持连接
  let consumed = feed(&mut s, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR Can't execute 'SET': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n"
  );

  // 8. PUBLISH: 非白名单命令，拦截报错并保持连接（对标 PubSubModeRejectsDisallowedCommandsInResp2）
  let consumed = feed(&mut s, b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR Can't execute 'PUBLISH': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n"
  );

  // 9. 保持连接验证：后续允许命令仍正常执行（RESP2 订阅模式仍回订阅 PONG）
  let consumed = feed(&mut s, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(consumed, Some(0));
  assert_eq!(output(&mut s), "*2\r\n$4\r\npong\r\n$0\r\n\r\n");

  // 9. RESP3 模式验证：RESP3 不拦截非白名单命令
  s.resp_protocol_version = 3;
  let consumed = feed(&mut s, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("Can't execute"));
}

/// 集群切面最小桩：SSUBSCRIBE 关闭臂判定只消费 `has_cluster_session` 在场性
/// 以过 CLUSTER_DISABLED 门，不触达任何切面方法（全空体/默认值；
/// 同 resp_server_session_tests 的桩模式）
struct StubClusterSession;

impl ClusterSessionFace for StubClusterSession {
  fn set_read_only_session(&self) {}
  fn set_read_write_session(&self) {}
  fn local_current_epoch(&self) -> i64 {
    0
  }
  fn acquire_current_epoch(&self) {}
  fn release_current_epoch(&self) {}
  fn network_multi_key_slot_verify(
    &self,
    _input: &ClusterSlotVerificationInput<'_>,
    _args: &[&[u8]],
    _output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    SlotVerifyGate::Serve
  }
  fn network_multi_key_slot_verify_no_response(
    &self,
    _input: &ClusterSlotVerificationInput<'_>,
    _args: &[&[u8]],
  ) -> bool {
    false
  }
  fn process_cluster_commands(
    &self,
    _cmd: RespCommand,
    _args: &[&[u8]],
    _output: &mut Vec<u8>,
    _slot: u16,
  ) -> bool {
    true
  }
  fn dispose(&self) {}
}

/// --disable-pubsub（broker 缺位）下订阅族关闭臂整帧锁定
///
/// libs/server/Resp/PubSubCommands.cs:208 NetworkSUBSCRIBE 的 disabledBroker 臂
/// 硬编码 "ERR SUBSCRIBE is disabled, enable it with --pubsub option."，SSUBSCRIBE
/// 经 cmd 参数共用本臂——分片路径同报 SUBSCRIBE 词怪癖（r28-errframe 纠偏点，
/// 此前 rust 侧 SSUBSCRIBE 臂误取精确命令名）；:250 NetworkPSUBSCRIBE 的
/// disabledBroker 臂同串。SSUBSCRIBE 须集群切面在场才抵达关闭臂
/// （CLUSTER_DISABLED 门先行），故挂最小桩
#[test]
fn disabled_broker_rejects_subscribe_family() {
  const DISABLED: &str = "-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n";

  // SUBSCRIBE：无集群切面直达关闭臂
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  assert!(s.network_subscribe(false, &[b"messages"]));
  assert_eq!(output(&mut s), DISABLED);

  // PSUBSCRIBE：C# 怪癖——同报 SUBSCRIBE 词
  let mut s = RespServerSession::new(2, RespServerSessionOptions::default());
  assert!(s.network_psubscribe(&[b"messagesA*"]));
  assert_eq!(output(&mut s), DISABLED);

  // SSUBSCRIBE：集群切面在场（过 CLUSTER_DISABLED 门）后同报 SUBSCRIBE 词
  let mut s = RespServerSession::new(3, RespServerSessionOptions::default());
  s.attach_cluster_session(Arc::new(StubClusterSession));
  assert!(s.network_subscribe(true, &[b"messages"]));
  assert_eq!(output(&mut s), DISABLED);
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:LargeSUBSCRIBE
///
/// 140KB 大载荷经订阅通道推送字节完整：C# 以 140 * 1024 随机字节发布后
/// 在订阅回调逐字节比对，钉住大消息不撕裂、不截断；rust 以确定性模式
/// （i % 251 循环）替代随机源，断言 bulk 长度头与载荷逐字节一致
#[test]
fn large_subscribe() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut s = session(1);
  s.attach_pubsub(broker.clone());
  assert!(s.network_subscribe(false, &[b"messages"]));
  output(&mut s); // 排空 SUBSCRIBE 确认帧

  // 确定性载荷：140KB，逐字节 i % 251 循环（251 质数，模周期覆盖全字节值域）
  let payload: Vec<u8> = (0..140 * 1024).map(|i| (i % 251) as u8).collect();
  assert_eq!(broker.publish_now(b"0:messages", &payload), 1);
  assert_eq!(s.drain_pubsub_frames(), 1);

  // 推送帧：*3 头 + message 字面 + 裸通道名 + 载荷 bulk（长度头 143360）
  let bytes = drain_output(&mut s);
  let head = b"*3\r\n$7\r\nmessage\r\n$8\r\nmessages\r\n$143360\r\n";
  assert_eq!(&bytes[..head.len()], &head[..], "推送帧头不完整或被改写");
  assert_eq!(
    &bytes[head.len()..head.len() + payload.len()],
    &payload[..],
    "大载荷逐字节比对失败（撕裂/截断/改写）"
  );
  assert_eq!(
    &bytes[head.len() + payload.len()..],
    b"\r\n",
    "bulk 收尾缺失"
  );
}
