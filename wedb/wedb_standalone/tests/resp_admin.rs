use wconf::{RuntimeServerConfig, ServerConfig};
use wnode::resp::resp_server_session::RespServerSession;

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingTest
#[test]
fn ping_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingMessageTest
#[test]
fn ping_message_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[b"HELLO"], &mut out).unwrap();
  assert_eq!(out, b"$5\r\nHELLO\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingErrorMessageTest
#[test]
fn ping_error_message_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[b"HELLO", b"WORLD"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'PING' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithNoMessageReturnErrorTest
#[test]
fn echo_with_no_message_return_error_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ECHO' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithMessagesReturnErrorTest
#[test]
fn echo_with_messages_return_error_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[b"HELLO", b"WORLD"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ECHO' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithMessageTest
#[test]
fn echo_with_message_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[b"HELLO"], &mut out).unwrap();
  assert_eq!(out, b"$5\r\nHELLO\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:TimeCommandTest
#[test]
fn time_command_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_time(&[], &mut out).unwrap();
  assert!(out.starts_with(b"*2\r\n$"));
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:TimeWithReturnErrorTest
#[test]
fn time_with_return_error_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_time(&[b"X"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'TIME' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:SeSaveTest
#[test]
fn se_save_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_save(&[b"1", b"2"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'SAVE' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:SimpleConfigGet
#[test]
fn config_get_slave_read_only_test() {
  let mut s = ServerConfig;
  let rc = RuntimeServerConfig::with_defaults();
  let mut out = Vec::new();
  s.network_config_get(&[b"slave-read-only"], &rc, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");
}

/// libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT 集成测试
mod admin_collect {
  use std::{sync::Arc, thread, time::Duration};

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wnode::{
    MessageConsumerFace, RespSessionConsumer,
    resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  };

  fn with_session(f: impl FnOnce(&mut RespSessionConsumer)) {
    Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("collect.db")).unwrap());
      let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
      let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);
      f(&mut consumer);
    });
  }

  fn frame(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
      out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
  }

  fn feed(consumer: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
    let (consumed, resp) = consumer.try_consume_messages(&frame(args));
    assert!(consumed > 0);
    resp
  }

  #[test]
  fn hcollect_keeps_live_fields_and_reports_ok() {
    with_session(|c| {
      feed(c, &["HSET", "h", "f1", "v1", "f2", "v2"]);

      feed(c, &["HCOLLECT", "h"]);
      let resp = feed(c, &["HLEN", "h"]);
      assert_eq!(resp, b":2\r\n");
      let resp = feed(c, &["HCOLLECT", "missing"]);
      assert_eq!(resp, b"+OK\r\n");
    });
  }

  #[test]
  fn hcollect_clears_expired_fields() {
    with_session(|c| {
      feed(c, &["HSET", "hx", "keep", "v", "gone", "v"]);
      feed(c, &["HPEXPIRE", "hx", "100", "FIELDS", "1", "gone"]);

      thread::sleep(Duration::from_millis(200));
      let resp = feed(c, &["HCOLLECT", "hx"]);
      assert_eq!(resp, b"+OK\r\n");
      let resp = feed(c, &["HLEN", "hx"]);
      assert_eq!(resp, b":1\r\n");
    });
  }

  #[test]
  fn hcollect_wrongtype_and_star_degrade() {
    with_session(|c| {
      feed(c, &["SET", "str", "x"]);

      let resp = feed(c, &["HCOLLECT", "str"]);
      assert_eq!(
        resp,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
      );

      let resp = feed(c, &["HCOLLECT", "*"]);
      assert!(resp.is_empty(), "同步段仅校验，不残留输出");
      let slow = c.take_slow_wait().expect("HCOLLECT * 应挂起慢路径");
      let resp = Runtime::new().unwrap().block_on(slow.resolve());
      assert_eq!(resp, b"+OK\r\n");
    });
  }

  #[test]
  fn zcollect_reports_ok_and_wrongtype() {
    with_session(|c| {
      feed(c, &["ZADD", "z", "1", "m"]);

      let resp = feed(c, &["ZCOLLECT", "z"]);
      assert_eq!(resp, b"+OK\r\n");
      let resp = feed(c, &["ZCARD", "z"]);
      assert_eq!(resp, b":1\r\n");

      let resp = feed(c, &["ZCOLLECT", "missing"]);
      assert_eq!(resp, b"+OK\r\n");

      feed(c, &["SET", "str", "x"]);
      let resp = feed(c, &["ZCOLLECT", "str"]);
      assert_eq!(
        resp,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
      );
    });
  }
}
