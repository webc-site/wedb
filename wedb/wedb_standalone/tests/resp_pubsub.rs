use std::sync::Arc;

use wnode::resp::resp_server_session::{RespServerSession, RespServerSessionOptions};
use wpubsub::*;

fn session(id: i64) -> RespServerSession {
  RespServerSession::new(id, RespServerSessionOptions::default())
}

fn wire() -> PubSubSession {
  PubSubSession::new(Arc::new(SubscribeBroker::new(4096)))
}

fn output(s: &mut RespServerSession) -> String {
  let mut bytes = s.take_sent();
  bytes.extend_from_slice(&s.take_output());
  String::from_utf8_lossy(&bytes).into_owned()
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicSUBSCRIBE
#[test]
fn basic_subscribe() {
  let mut s = session(1);
  let mut w = wire();
  assert!(s.network_subscribe(&mut w, false, &[b"messages"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$8\r\nmessages\r\n:1\r\n"
  );
  assert_eq!(w.num_active_channels(), 1);
  assert!(s.is_subscription_session);
  assert_eq!(w.broker().unwrap().num_subscriptions(b"messages"), 1);

  // Unsubscribe
  assert!(s.network_unsubscribe(&mut w, &[b"messages"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$8\r\nmessages\r\n:0\r\n"
  );
  assert!(!s.is_subscription_session);
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPSUBSCRIBE
#[test]
fn basic_psubscribe() {
  let mut s = session(1);
  let mut w = wire();
  let glob = b"messagesA*";
  assert!(s.network_psubscribe(&mut w, &[glob]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$10\r\npsubscribe\r\n$10\r\nmessagesA*\r\n:1\r\n"
  );

  // publish match
  assert_eq!(
    w.broker()
      .unwrap()
      .publish_now(b"messagesAtest", b"published message"),
    1
  );
  assert_eq!(s.drain_pubsub_frames(&w), 1);
  assert_eq!(
    output(&mut s),
    "*4\r\n$8\r\npmessage\r\n$10\r\nmessagesA*\r\n$13\r\nmessagesAtest\r\n$17\r\npublished message\r\n"
  );

  // Unsubscribe
  assert!(s.network_punsubscribe(&mut w, &[glob]));
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
  let broker = Arc::new(SubscribeBroker::new(4096));
  let mut wa = PubSubSession::new(broker.clone());
  let mut wb = PubSubSession::new(broker);

  a.network_subscribe(&mut wa, false, &[b"messagesAtest"]);
  b.network_subscribe(&mut wb, false, &[b"messagesB"]);
  output(&mut a);
  output(&mut b);

  a.network_pubsub_channels(&mut wa, &[]);
  let out = output(&mut a);
  assert!(out.contains("*2\r\n"));
  assert!(out.contains("$13\r\nmessagesAtest\r\n"));
  assert!(out.contains("$9\r\nmessagesB\r\n"));

  // pattern filter
  a.network_pubsub_channels(&mut wa, &[b"messages?test"]);
  let out = output(&mut a);
  assert_eq!(out, "*1\r\n$13\r\nmessagesAtest\r\n");

  a.network_pubsub_channels(&mut wa, &[b"messagesC*"]);
  let out = output(&mut a);
  assert_eq!(out, "*0\r\n");
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPUBSUB_NUMPAT
#[test]
fn basic_pubsub_numpat() {
  let mut s = session(1);
  let mut w = wire();

  s.network_pubsub_numpat(&mut w, &[]);
  assert_eq!(output(&mut s), ":0\r\n");

  s.network_psubscribe(&mut w, &[b"com.messages.*", b"com.messagesB.*"]);
  output(&mut s);

  s.network_pubsub_numpat(&mut w, &[]);
  assert_eq!(output(&mut s), ":2\r\n");

  s.network_punsubscribe(&mut w, &[b"com.messages.*", b"com.messagesB.*"]);
  output(&mut s);

  s.network_pubsub_numpat(&mut w, &[]);
  assert_eq!(output(&mut s), ":0\r\n");
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:BasicPUBSUB_NUMSUB
#[test]
fn basic_pubsub_numsub() {
  let mut s = session(1);
  let mut w = wire();

  // empty query
  s.network_pubsub_numsub(&mut w, &[]);
  assert_eq!(output(&mut s), "*0\r\n");

  // no subscribers yet
  s.network_pubsub_numsub(&mut w, &[b"messagesA", b"messagesB"]);
  assert_eq!(
    output(&mut s),
    "*4\r\n$9\r\nmessagesA\r\n:0\r\n$9\r\nmessagesB\r\n:0\r\n"
  );

  s.network_subscribe(&mut w, false, &[b"messagesA", b"messagesB"]);
  output(&mut s);

  s.network_pubsub_numsub(&mut w, &[b"messagesA", b"messagesB"]);
  assert_eq!(
    output(&mut s),
    "*4\r\n$9\r\nmessagesA\r\n:1\r\n$9\r\nmessagesB\r\n:1\r\n"
  );
}

/// test/standalone/Garnet.test/RespPubSubTests.cs:PubSubModeAllowsValidCommandsInResp2
#[test]
fn pub_sub_mode_allows_valid_commands_in_resp2() {
  let mut s = session(1);
  let mut w = wire();

  // Enter subscription mode
  assert!(s.network_subscribe(&mut w, false, &[b"foo"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"
  );

  // Another SUBSCRIBE
  assert!(s.network_subscribe(&mut w, false, &[b"bar"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n"
  );

  // PSUBSCRIBE
  assert!(s.network_psubscribe(&mut w, &[b"baz*"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$10\r\npsubscribe\r\n$4\r\nbaz*\r\n:3\r\n"
  );

  // UNSUBSCRIBE bar
  assert!(s.network_unsubscribe(&mut w, &[b"bar"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n"
  );

  // PUNSUBSCRIBE baz*
  assert!(s.network_punsubscribe(&mut w, &[b"baz*"]));
  assert_eq!(
    output(&mut s),
    "*3\r\n$12\r\npunsubscribe\r\n$4\r\nbaz*\r\n:1\r\n"
  );

  // UNSUBSCRIBE foo (last channel exits subscription mode)
  assert!(s.network_unsubscribe(&mut w, &[b"foo"]));
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
  let mut w = wire();

  assert!(s.network_subscribe(&mut w, false, &[b"foo"]));
  output(&mut s);

  // Self-publish
  assert!(s.network_publish(&mut w, false, &[b"foo", b"bar"]));
  assert_eq!(
    output(&mut s),
    ">3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:1\r\n"
  );

  assert!(s.network_unsubscribe(&mut w, &[b"foo"]));
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
  let mut w = wire();

  assert!(s.network_psubscribe(&mut w, &[b"foo*"]));
  output(&mut s);

  assert!(s.network_publish(&mut w, false, &[b"foobar", b"baz"]));
  assert_eq!(
    output(&mut s),
    ">4\r\n$8\r\npmessage\r\n$4\r\nfoo*\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n:1\r\n"
  );

  assert!(s.network_punsubscribe(&mut w, &[b"foo*"]));
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

  // 1. PING: 允许，正常响应 +PONG
  let consumed = s.try_consume_messages(b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(consumed, Some(14));
  assert_eq!(output(&mut s), "+PONG\r\n");

  // 2. RESET: 允许，不报错
  let consumed = s.try_consume_messages(b"*1\r\n$5\r\nRESET\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("command not allowed while in subscribe mode"));

  // 3. SUNSUBSCRIBE: 允许，不报错
  let consumed = s.try_consume_messages(b"*1\r\n$12\r\nSUNSUBSCRIBE\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("command not allowed while in subscribe mode"));

  // 4. SUBSCRIBE: 允许，不报错
  let consumed = s.try_consume_messages(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("command not allowed while in subscribe mode"));

  // 5. QUIT: 允许，不报错
  let consumed = s.try_consume_messages(b"*1\r\n$4\r\nQUIT\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("command not allowed while in subscribe mode"));

  // 6. GET: 非白名单命令，拦截报错并保持连接
  let consumed = s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR GET command not allowed while in subscribe mode\r\n"
  );

  // 7. SET: 非白名单命令，拦截报错并保持连接
  let consumed = s.try_consume_messages(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR SET command not allowed while in subscribe mode\r\n"
  );

  // 8. PUBLISH: 非白名单命令，拦截报错并保持连接（对标 PubSubModeRejectsDisallowedCommandsInResp2）
  let consumed = s.try_consume_messages(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  assert!(consumed.is_some());
  assert_eq!(
    output(&mut s),
    "-ERR PUBLISH command not allowed while in subscribe mode\r\n"
  );

  // 9. 保持连接验证：后续允许命令仍正常执行
  let consumed = s.try_consume_messages(b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(consumed, Some(14));
  assert_eq!(output(&mut s), "+PONG\r\n");

  // 9. RESP3 模式验证：RESP3 不拦截非白名单命令
  s.resp_protocol_version = 3;
  let consumed = s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert!(consumed.is_some());
  let out = output(&mut s);
  assert!(!out.contains("command not allowed while in subscribe mode"));
}
