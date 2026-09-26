//! redis.call SET/GET 快路径 number 形参集成测试
//! 对标 garnet/libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
//! 的 SET/GET 特例分支（string/number 两臂，其余 ErrBadArg）。

use std::str;

use wlua::{LuaOptions, LuaRunner, ScriptApiError, ScriptingApi};
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
  /// acl_check_cmd 面 parse_resp_command_buffer 收到的成帧缓冲（deviations §112 锁）
  parsed_requests: Vec<Vec<u8>>,
  /// get/set 错误形态注挡（None = 正常臂），模拟重入会话回包解析的两态
  get_err: Option<ScriptApiError>,
  set_err: Option<ScriptApiError>,
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

  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError> {
    self.gets.push(key.to_vec());
    match self.get_err {
      Some(err) => Err(err),
      None => Ok(Some(b"v12".to_vec())),
    }
  }

  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), ScriptApiError> {
    self.sets.push((key.to_vec(), value.to_vec()));
    match self.set_err {
      Some(err) => Err(err),
      None => Ok(()),
    }
  }

  fn resp_protocol_version(&self) -> u8 {
    2
  }

  fn update_resp_protocol_version(&mut self, _version: u8) {}

  fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    self.parsed_requests.push(buffer.to_vec());
    Some(RespCommand::Set)
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
  runner.run_for_session(&[b"0"], session, &mut out);
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
fn fallback_integral_float_args_write_exact_i64_text() {
  // deviations §112 锁：成帧器 number 臂对整值 double 直写精确 i64 文本，
  // 不对齐 C#（标准 Lua 5.4 lua_tolstring）float 子类型的 "%.14g" 形参
  // 转串文本（C# 侧对照：123.0→"123.0"、2^62→"4.6116860184274e+18"、
  // 2^63→"9.2233720368548e+18"、-2^63→"-9.2233720368548e+18"、
  // -0.0→"-0.0"）。rust 期望文本按在册裁决钉死，严禁按 C# 文本形态回改。
  let cases = [
    ("123.0", "123"),
    ("2^62", "4611686018427387904"),
    // 2^63 过 :389 上界（i64::MAX as f64 即 2^63）后 as i64 饱和至 i64::MAX
    ("2^63", "9223372036854775807"),
    ("-2^63", "-9223372036854775808"),
    ("-0.0", "0"),
  ];
  for (expr, text) in cases {
    let mut session = MockSession::default();
    let script = format!("return redis.call('MSET', 'k', {expr})");
    let resp = run_session(&script, &mut session);
    assert_eq!(payload(&resp), "OK", "expr {expr}");
    assert_eq!(
      session.fallback_args,
      vec![vec![
        b"MSET".to_vec(),
        b"k".to_vec(),
        text.as_bytes().to_vec()
      ]],
      "expr {expr} 落参文本（deviations §112）"
    );
  }
}

#[test]
fn acl_check_cmd_float_arg_frames_exact_i64_text() {
  // deviations §112 锁：acl_check_cmd 经 frame_and_acl_check →
  // prepare_and_check_resp_request 同一成帧单点，float 子类型整值形参
  // 成帧文本与 redis.call fallback 面同文（精确 i64，非 C# "%.14g"）。
  let mut session = MockSession::default();
  let resp = run_session(
    "return tostring(redis.acl_check_cmd('SET', 'k', 2^62))",
    &mut session,
  );
  assert_eq!(payload(&resp), "true");
  assert_eq!(session.parsed_requests.len(), 1);
  let frame = str::from_utf8(&session.parsed_requests[0]).unwrap();
  assert!(
    frame.contains("$19\r\n4611686018427387904\r\n"),
    "成帧缓冲应含精确 i64 文本: {frame:?}"
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

#[test]
fn fast_path_get_error_reply_folds_to_false() {
  // C# GET 臂：非 OK status（NOTFOUND/WRONGTYPE 等一切，Functions.cs:3246-3259）
  // 一律 PushBoolean(false) 脚本继续；rust 重入会话产的错误帧按该语义折叠。
  let mut session = MockSession {
    get_err: Some(ScriptApiError::ErrorReply),
    ..MockSession::default()
  };
  let resp = run_session(
    "local v = redis.call('GET', 'wt') if v == false then return 'continued' end return 'wrong'",
    &mut session,
  );
  assert_eq!(resp, b"$9\r\ncontinued\r\n");
  assert_eq!(session.gets, vec![b"wt".to_vec()]);
}

#[test]
fn fast_path_set_error_reply_still_pushes_ok() {
  // C# SET 臂：`_ = api.SET(...)` 显式丢弃 status 恒推 +OK（:3214-3216），
  // 存储面拒绝（错误帧）对脚本不可见。
  let mut session = MockSession {
    set_err: Some(ScriptApiError::ErrorReply),
    ..MockSession::default()
  };
  let resp = run_session("return redis.call('SET', 'k', 'v')", &mut session);
  assert_eq!(resp, b"$2\r\nOK\r\n");
  assert_eq!(session.sets, vec![(b"k".to_vec(), b"v".to_vec())]);
}

#[test]
fn fast_path_protocol_error_raises_lua_error() {
  // Malformed（应答不可解析）无 C# 对应态，保持上抛 Lua 中断脚本——
  // 与 ErrorReply 折叠臂刻意分档，防后审席按折叠形态「统一」吞掉协议损伤。
  let mut get_session = MockSession {
    get_err: Some(ScriptApiError::Protocol),
    ..MockSession::default()
  };
  let resp = run_session(
    "redis.call('GET', 'k') return 'unreachable'",
    &mut get_session,
  );
  // 非 ERR 前缀文案经 finish_err 补 "ERR Lua encountered an error: "（C# LuaRunner.cs:1533 同形）
  assert_eq!(resp, b"-ERR Lua encountered an error: protocol error\r\n");

  let mut set_session = MockSession {
    set_err: Some(ScriptApiError::Protocol),
    ..MockSession::default()
  };
  let resp = run_session(
    "redis.call('SET', 'k', 'v') return 'unreachable'",
    &mut set_session,
  );
  assert_eq!(resp, b"-ERR Lua encountered an error: protocol error\r\n");
}
