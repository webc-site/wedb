//! cjson 与 cmsgpack 库集成测试。

use std::str;

use wlua::{LuaOptions, LuaRunner, RespObject};

fn new_runner(source: &str) -> LuaRunner {
  let opts = LuaOptions::default();
  LuaRunner::with_options(&opts, source.as_bytes(), "0.0.0.0").unwrap()
}

fn compile_and_run(runner: &mut LuaRunner) -> Result<RespObject, String> {
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out)?;
  runner.run_for_runner(None, None)
}

fn resp_as_str(resp: &RespObject) -> &str {
  match resp {
    RespObject::BulkString(bytes) => str::from_utf8(bytes).unwrap(),
    _ => panic!("Expected bulk string response, got {resp:?}"),
  }
}

#[test]
fn cjson_encode_decode_roundtrip() {
  // 编解码基本类型与嵌套表
  let mut runner = new_runner(
    r#"
    local orig = { name = "wedb", port = 6379, active = true }
    local encoded = cjson.encode(orig)
    local decoded = cjson.decode(encoded)
    return decoded.name .. ":" .. tostring(decoded.port) .. ":" .. tostring(decoded.active)
    "#,
  );
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(resp_as_str(&res), "wedb:6379:true");
}

#[test]
fn cjson_number_formatting() {
  let mut runner = new_runner(
    r#"
    local res = {}
    table.insert(res, cjson.encode({ v = 0 }))
    table.insert(res, cjson.encode({ v = 3 }))
    table.insert(res, cjson.encode({ v = 3.5 }))
    return table.concat(res, ";")
    "#,
  );
  let res = compile_and_run(&mut runner).unwrap();
  let text = resp_as_str(&res);
  assert!(text.contains("{\"v\":0}"));
  assert!(text.contains("{\"v\":3}"));
  assert!(text.contains("{\"v\":3.5}"));
}

#[test]
fn cjson_decode_array() {
  let mut runner = new_runner(
    r#"
    local arr = cjson.decode('[10, 20, 30]')
    return arr[1] + arr[2] + arr[3]
    "#,
  );
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(res, RespObject::Integer(60));
}

#[test]
fn cmsgpack_pack_unpack_primitives() {
  // fixint 42
  let mut runner = new_runner("return cmsgpack.unpack(cmsgpack.pack(42))");
  assert_eq!(
    compile_and_run(&mut runner).unwrap(),
    RespObject::Integer(42)
  );

  // 负 fixint -5
  let mut runner = new_runner("return cmsgpack.unpack(cmsgpack.pack(-5))");
  assert_eq!(
    compile_and_run(&mut runner).unwrap(),
    RespObject::Integer(-5)
  );

  // 字符串 "hi"
  let mut runner = new_runner("return cmsgpack.unpack(cmsgpack.pack('hi'))");
  assert_eq!(
    compile_and_run(&mut runner).unwrap(),
    RespObject::BulkString(b"hi".to_vec())
  );

  // uint16 1000
  let mut runner = new_runner("return cmsgpack.unpack(cmsgpack.pack(1000))");
  assert_eq!(
    compile_and_run(&mut runner).unwrap(),
    RespObject::Integer(1000)
  );
}

#[test]
fn cmsgpack_pack_unpack_array() {
  let mut runner = new_runner(
    r#"
    local packed = cmsgpack.pack({ "apple", "banana", "cherry" })
    local unpacked = cmsgpack.unpack(packed)
    return unpacked[1] .. "," .. unpacked[2] .. "," .. unpacked[3]
    "#,
  );
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(resp_as_str(&res), "apple,banana,cherry");
}

#[test]
fn cmsgpack_unpack_error_handling() {
  // 无效串解码报错
  let mut runner = new_runner(
    r#"
    local ok, err = pcall(function() return cmsgpack.unpack("\203") end)
    if ok then return 1 else return 0 end
    "#,
  );
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(res, RespObject::Integer(0));
}
