//! 容器变异与翻转命令对类型不匹配节点的 null 占位回归
//!
//! C# GarnetJSON 只注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
//! garnet/test/standalone/Garnet.test.extensions/GarnetJSON 下无同名用例；
//! ARRAPPEND / ARRINSERT / ARRPOP / ARRTRIM / STRAPPEND / TOGGLE 为 wedb 侧
//! 对标 RedisJSON 2.0+ 契约的兼容扩展：JSONPath 匹配命中类型不符的节点时
//! 不改该节点，且应答数组必须与匹配项 1:1 等长，错位槽位回 null 占位。
//!
//! 自研依据: 容器空槽位语义（本仓 JSON 容器实现细节）

use std::str::from_utf8;

use sonic_rs::{JsonContainerTrait, Value};
use wext_json::JsonCommand;

/// RESP 协议版本（resp2 帧，与 wnode 缺省会话一致）
const RESP_VER: u8 = 2;

/// RESP 应答体提取：bulk string 取载荷文本，错误帧去 `-` 前缀
fn resp_text(frame: &[u8]) -> String {
  let s = from_utf8(frame).unwrap();
  if let Some(rest) = s.strip_prefix('$') {
    let head_end = rest.find("\r\n").unwrap();
    let len: usize = rest[..head_end].parse().unwrap();
    let body_start = head_end + 2;
    return rest[body_start..body_start + len].to_string();
  }
  s.trim_end_matches("\r\n")
    .trim_start_matches('-')
    .to_string()
}

/// 走 updater 执行体：回 (是否成功, 应答体, 改写后的载荷)
fn run_updater(cmd: JsonCommand, doc: &str, args: &[&str]) -> (bool, String, String) {
  let mut payload = doc.as_bytes().to_vec();
  let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
  let mut out = Vec::new();
  let ok = (cmd.fns().updater)(&mut payload, &args, &mut out, RESP_VER);
  (ok, resp_text(&out), String::from_utf8(payload).unwrap())
}

/// 应答体按 JSON 解析（比对结果数组内容与顺序，不依赖数字文本形态）
fn resp_json(text: &str) -> Value {
  sonic_rs::from_str::<Value>(text).unwrap()
}

/// 混合类型数组文档（数值/字符串/布尔/数组/对象/null 六路匹配）；
/// 对象键序在 $.* 下不保证稳定，多槽断言一律走数组文档
const MIXED: &str = r#"[1,"foo",true,[2,3],{"a":1},null]"#;

/// JSON.ARRAPPEND $[*] 混合类型：仅数组节点追加，其余槽位回 null 且载荷不被篡改
#[test]
fn arrappend_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::ArrAppend, MIXED, &["$[*]", "7"]);
  assert!(ok);
  let res = resp_json(&out);
  assert_eq!(
    res.as_array().unwrap().len(),
    6,
    "应答须与匹配数 1:1: {out}"
  );
  assert_eq!(res, resp_json("[null,null,null,3,null,null]"));
  assert_eq!(payload, r#"[1,"foo",true,[2,3,7],{"a":1},null]"#);
}

/// 全量类型不匹配时不得回空数组，须逐槽补 null
#[test]
fn arrappend_all_mismatch_returns_all_null() {
  let (ok, out, payload) = run_updater(JsonCommand::ArrAppend, r#"{"a":"x","b":2}"#, &["$.*", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,null]"));
  assert_eq!(resp_json(&payload), resp_json(r#"{"a":"x","b":2}"#));
}

/// 标量路径命中非数组：单槽 null（原为 []）
#[test]
fn arrappend_scalar_path_mismatch_returns_null_slot() {
  let (ok, out, _) = run_updater(JsonCommand::ArrAppend, r#"{"a":"str"}"#, &["$.a", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null]"));
}

/// JSON.ARRINSERT $[*] 混合元素：仅数组且索引合法者插入，其余槽位 null
#[test]
fn arrinsert_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(
    JsonCommand::ArrInsert,
    r#"[1,"foo",true,[2,3],{"a":1}]"#,
    &["$[*]", "0", "9"],
  );
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,null,null,3,null]"));
  assert_eq!(payload, r#"[1,"foo",true,[9,2,3],{"a":1}]"#);
}

/// 数组节点但索引越界：不改值，该位同样补 null
#[test]
fn arrinsert_out_of_range_index_returns_null_slot() {
  let (ok, out, payload) = run_updater(
    JsonCommand::ArrInsert,
    r#"{"a":[1],"b":"s"}"#,
    &["$.*", "9", "7"],
  );
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,null]"));
  // 对象重序列化键序不保证，按 JSON 语义比对载荷未变
  assert_eq!(resp_json(&payload), resp_json(r#"{"a":[1],"b":"s"}"#));
}

/// JSON.ARRPOP $[*] 混合类型：仅数组弹出，非数组槽位 null 且载荷原样
#[test]
fn arrpop_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::ArrPop, MIXED, &["$[*]", "-1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,null,null,3,null,null]"));
  assert_eq!(payload, r#"[1,"foo",true,[2],{"a":1},null]"#);
}

/// 标量路径命中非数组：单槽 null
#[test]
fn arrpop_scalar_path_mismatch_returns_null_slot() {
  let (ok, out, _) = run_updater(JsonCommand::ArrPop, r#"{"a":{"b":1}}"#, &["$.a"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null]"));
}

/// JSON.ARRTRIM $[*] 混合类型：仅数组裁剪，其余槽位 null
#[test]
fn arrtrim_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::ArrTrim, MIXED, &["$[*]", "0", "0"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,null,null,1,null,null]"));
  assert_eq!(payload, r#"[1,"foo",true,[2],{"a":1},null]"#);
}

/// JSON.STRAPPEND $[*] 混合类型：仅字符串追加，其余槽位 null
#[test]
fn strappend_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::StrAppend, MIXED, &["$[*]", r#""!""#]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null,4,null,null,null,null]"));
  assert_eq!(payload, r#"[1,"foo!",true,[2,3],{"a":1},null]"#);
}

/// 标量路径命中非字符串：单槽 null
#[test]
fn strappend_scalar_path_mismatch_returns_null_slot() {
  let (ok, out, _) = run_updater(JsonCommand::StrAppend, r#"{"a":7}"#, &["$.a", "\"x\""]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null]"));
}

/// JSON.TOGGLE $[*] 混合类型：仅布尔翻转，其余槽位 null
#[test]
fn toggle_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::Toggle, MIXED, &["$[*]"]);
  assert!(ok);
  assert_eq!(
    resp_json(&out),
    resp_json("[null,null,false,null,null,null]")
  );
  assert_eq!(payload, r#"[1,"foo",false,[2,3],{"a":1},null]"#);
}

/// 标量路径命中非布尔：单槽 null（原为 []）
#[test]
fn toggle_scalar_path_mismatch_returns_null_slot() {
  let (ok, out, payload) = run_updater(JsonCommand::Toggle, r#"{"a":"s"}"#, &["$.a"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[null]"));
  assert_eq!(payload, r#"{"a":"s"}"#);
}
