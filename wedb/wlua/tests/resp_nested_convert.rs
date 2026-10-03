//! ProcessSingleRespTerm 栈索引回归测试
//!
//! 对标 garnet/libs/server/Lua/LuaRunner.cs:ProcessSingleRespTerm 的
//! `RawSet(recordCount, curTop + 1)` 栈绝对索引契约（C# LuaRunner.cs:615,
//! 988, 1044, 1102, 1111）：`+` `,` `(` `=` 类型嵌套在数组/Map/Set 内时，
//! `{ ok = ... }` 等子表只允许填充自身，不得污染外层容器。

use std::str;

use wlua::{LuaOptions, LuaRunner, ScriptApiError, ScriptingApi};
use wresp::command::RespCommand;

/// 回放型会话：redis.call 走 fallback 派发面，dispatch_resp 原样回放预置 RESP 帧
/// （帧内容即被测的 ProcessRespResponse 输入，对标 C# 测试直接投喂 RESP 的形态）。
struct ReplaySession {
  response: Vec<u8>,
  version: u8,
}

impl ScriptingApi for ReplaySession {
  fn dispatch_resp(&mut self, _request: &[u8], response: &mut Vec<u8>) {
    response.extend_from_slice(&self.response);
  }

  // 本组测试的命令名恒非 SET/GET，快路径两臂不可达。
  fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError> {
    Err(ScriptApiError::Protocol)
  }

  fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), ScriptApiError> {
    Err(ScriptApiError::Protocol)
  }

  fn resp_protocol_version(&self) -> u8 {
    self.version
  }

  fn update_resp_protocol_version(&mut self, _version: u8) {}

  fn parse_resp_command_buffer(&mut self, _buffer: &[u8]) -> Option<RespCommand> {
    None
  }

  fn check_acl_permissions(&self, _command: RespCommand) -> bool {
    true
  }
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
    other => panic!("unexpected RESP frame: {other:?}"),
  }
}

/// 以 session 形态执行脚本（脚本内 assert 校验 RESP 转换结果并返回 'OK'），
/// 断言外层输出即 'OK'；脚本断言失败时透出错误文本便于定位。
fn run_replay(script: &str, frame: &[u8], version: u8) {
  let opts = LuaOptions::default();
  let mut runner = LuaRunner::with_options(&opts, script.as_bytes(), "0.0.0.0").unwrap();
  let mut out = Vec::new();
  assert!(
    runner.compile_for_session(&mut out),
    "compile failed: {out:?}"
  );
  out.clear();
  let mut session = ReplaySession {
    response: frame.to_vec(),
    version,
  };
  runner.run_for_session(&[b"0"], &mut session, &mut out);
  assert_eq!(payload(&out), "OK", "script-side assertions failed");
}

/// 嵌套数组解析：`*2\r\n+OK\r\n+PONG\r\n` → `{ [1]={ok="OK"}, [2]={ok="PONG"} }`，
/// 外层数组表不得出现 ok 污染字段（修复前 raw_set(1) 写入栈底外层表）。
#[test]
fn nested_array_of_simple_strings() {
  run_replay(
    r#"
      local r = redis.call('PING')
      assert(type(r) == 'table')
      assert(#r == 2)
      assert(type(r[1]) == 'table' and r[1].ok == 'OK')
      assert(type(r[2]) == 'table' and r[2].ok == 'PONG')
      assert(rawget(r, 'ok') == nil)
      return 'OK'
    "#,
    b"*2\r\n+OK\r\n+PONG\r\n",
    2,
  );
}

/// RESP3 数组含 double/大整数/逐字字符串：各子表字段完整、外层容器纯净。
#[test]
fn nested_array_of_resp3_typed_terms() {
  run_replay(
    r#"
      local r = redis.call('PING')
      assert(#r == 3)
      assert(r[1].double == 3.14)
      assert(r[2].big_number == '34928903284092385093248509438503948')
      assert(r[3].format == 'txt' and r[3].string == 'abc')
      assert(rawget(r, 'double') == nil)
      assert(rawget(r, 'big_number') == nil)
      assert(rawget(r, 'format') == nil)
      assert(rawget(r, 'string') == nil)
      return 'OK'
    "#,
    b"*3\r\n,3.14\r\n(34928903284092385093248509438503948\r\n=7\r\ntxt:abc\r\n",
    3,
  );
}

/// RESP3 Map 值为嵌套数组：`{ map = { key = { {ok="OK"}, {ok="PONG"} } } }`，
/// 父表与子 map 表均不得被 ok 字段污染。
#[test]
fn nested_map_with_array_of_simple_strings() {
  run_replay(
    r#"
      local r = redis.call('PING')
      assert(type(r.map) == 'table')
      local v = r.map.key
      assert(type(v) == 'table' and #v == 2)
      assert(v[1].ok == 'OK')
      assert(v[2].ok == 'PONG')
      assert(rawget(r, 'ok') == nil)
      assert(rawget(r.map, 'ok') == nil)
      return 'OK'
    "#,
    b"%1\r\n$3\r\nkey\r\n*2\r\n+OK\r\n+PONG\r\n",
    3,
  );
}

/// RESP3 Set 元素为简单字符串：键子表 {ok="OK"} 完整、值 true，外层 {set=...} 表纯净。
#[test]
fn nested_set_of_simple_strings() {
  run_replay(
    r#"
      local r = redis.call('PING')
      assert(type(r.set) == 'table')
      assert(rawget(r, 'ok') == nil)
      local n = 0
      for k, v in pairs(r.set) do
        n = n + 1
        assert(type(k) == 'table' and k.ok == 'OK')
        assert(v == true)
      end
      assert(n == 1)
      return 'OK'
    "#,
    b"~1\r\n+OK\r\n",
    3,
  );
}
