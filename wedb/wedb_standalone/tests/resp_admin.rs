use wnode::{
  config::{runtime_server_config::RuntimeServerConfig, server_config::ServerConfig},
  resp::resp_server_session::RespServerSession,
};

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
