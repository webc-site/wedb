//! redis.call SET/GET 快路径 number 形参集成测试
//! 对标 garnet/libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
//! 的 SET/GET 特例分支（string/number 两臂，其余 ErrBadArg）。

use std::str;

use wlua::{LuaOptions, LuaRunner, ScriptingApi};
use wresp::{
  command::RespCommand,
  read::{try_read_byte_array_with_length_header, try_read_signed_array_length},
};

/// 记录型假会话：登记 set/get 实参与 fallback 面 RESP 命令实参。
#[derive(Default)]
struct MockSession {
  sets: Vec<(Vec<u8>, Vec<u8>)>,
  gets: Vec<Vec<u8>>,
  fallback_args: Vec<Vec<Vec<u8>>>,
}

impl ScriptingApi for MockSession {
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>) {
    let mut cursor: &[u8] = request;
    let mut len = 0i32;
    try_read_signed_array_length(&mut len, &mut cursor).unwrap();
    let mut args: Vec<Vec<u8>> = Vec::new();
    for _ in 0..len {
      let mut arg = Vec::new();
      try_read_byte_array_with_length_header(&mut arg, &mut cursor).unwrap();
      args.push(arg);
    }
    self.fallback_args.push(args);
    response.extend_from_slice(b"+OK\r\n");
  }

  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Vec<u8>> {
    self.gets.push(key.to_vec());
    Ok(Some(b"v12".to_vec()))
  }

  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Vec<u8>> {
    self.sets.push((key.to_vec(), value.to_vec()));
    Ok(())
  }

  fn resp_protocol_version(&self) -> u8 {
    2
  }

  fn update_resp_protocol_version(&mut self, _version: u8) {}

  fn parse_resp_command_buffer(&mut self, _buffer: &[u8]) -> Option<RespCommand> {
    None
  }

  fn check_acl_permissions(&self, _command: RespCommand) -> bool {
    true
  }
}

/// session 模式执行脚本，返回 RESP2 输出（args[0] 为 numkeys）。
fn run_session(script: &str, session: &mut MockSession) -> Vec<u8> {
  let opts = LuaOptions::default();
  let mut runner = LuaRunner::with_options(&opts, script.as_bytes(), "0.0.0.0").unwrap();
  let mut out = Vec::new();
  assert!(
    runner.compile_for_session(&mut out),
    "compile failed: {out:?}"
  );
  out.clear();
  runner.run_for_session(&[b"0".to_vec()], session, &mut out);
  out
}

/// 取 RESP2 单值输出（+ / $ / -）的载荷文本。
fn payload(resp: &[u8]) -> String {
  match resp.first() {
    Some(b'+') | Some(b'-') => {
      let end = resp.iter().position(|b| *b == b'\r').unwrap();
      str::from_utf8(&resp[1..end]).unwrap().to_owned()
    }
    Some(b'$') => {
      let header_end = resp.iter().position(|b| *b == b'\r').unwrap();
      let len: usize = str::from_utf8(&resp[1..header_end])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
      let start = header_end + 2;
      str::from_utf8(&resp[start..start + len])
        .unwrap()
        .to_owned()
    }
    _ => panic!("unexpected RESP frame: {resp:?}"),
  }
}

#[test]
fn fast_path_set_number_args_match_string_args() {
  let mut session = MockSession::default();
  let resp = run_session("return redis.call('SET', 123, 456)", &mut session);
  assert_eq!(payload(&resp), "OK");
  assert_eq!(session.sets, vec![(b"123".to_vec(), b"456".to_vec())]);

  let mut str_session = MockSession::default();
  let str_resp = run_session("return redis.call('SET', '123', '456')", &mut str_session);
  assert_eq!(payload(&str_resp), "OK");
  assert_eq!(str_session.sets, session.sets);
}

#[test]
fn fast_path_get_number_arg_matches_string_arg() {
  let mut session = MockSession::default();
  let resp = run_session("return redis.call('GET', 123)", &mut session);
  assert_eq!(payload(&resp), "v12");
  assert_eq!(session.gets, vec![b"123".to_vec()]);

  let mut str_session = MockSession::default();
  let str_resp = run_session("return redis.call('GET', '123')", &mut str_session);
  assert_eq!(payload(&str_resp), "v12");
  assert_eq!(str_session.gets, session.gets);
}

#[test]
fn fallback_number_args_consistent_with_fast_path() {
  // MSET 非快路径命令：number 形参经 fallback 落为同型文本串（与快路径同口径）。
  let mut session = MockSession::default();
  let resp = run_session("return redis.call('MSET', 123, 456)", &mut session);
  assert_eq!(payload(&resp), "OK");
  assert_eq!(
    session.fallback_args,
    vec![vec![b"MSET".to_vec(), b"123".to_vec(), b"456".to_vec()]]
  );
}

#[test]
fn fast_path_rejects_non_string_number_args() {
  // bool/nil/table 形参仍报 ERR bad argument（C# ErrBadArg 档），且不触达存储/回落面。
  for arg in ["true", "nil", "{}"] {
    let mut session = MockSession::default();
    let script = format!("return tostring(redis.call('SET', {arg}, 'x'))");
    let resp = run_session(&script, &mut session);
    let text = payload(&resp);
    assert_eq!(
      text,
      "ERR Lua redis lib command arguments must be strings or integers"
    );
    assert!(session.sets.is_empty(), "arg {arg} must not reach SET");
    assert!(
      session.fallback_args.is_empty(),
      "arg {arg} must not reach fallback"
    );
  }
}
