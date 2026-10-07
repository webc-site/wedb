#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
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

#[test]
fn cjson_decode_depth_gate_rejects_overshoot() {
  // §206 深度安全门（doc/zh/deviations.md §206）：sonic Value 快路无深度门，
  // cjson.decode 先过 wext_json 单趟预扫，255 层内收、256 层起归
  // too-many-nested 错形（脚本构造 300 层字符串，门先于解析零递归任栈安全）
  let deep = "[".repeat(300);
  let mut runner = new_runner(&format!(
    r#"
    local ok, err = pcall(function() return cjson.decode('{deep}') end)
    if ok then return 1 end
    if string.find(err, "too many nested", 1, true) then return 2 end
    return 3
    "#
  ));
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(
    res,
    RespObject::Integer(2),
    "超门解码须归 too-many-nested 错形"
  );

  // 界内形照常解码（嵌套数组，门不误伤；元素用字符串避开既有整数标量
  // as_f64 解局面——该面与本门无关）
  let mut runner = new_runner(r#"return #cjson.decode('[["a"], ["b", ["c"]]]')"#);
  let res = compile_and_run(&mut runner).unwrap();
  assert_eq!(res, RespObject::Integer(2));
}
