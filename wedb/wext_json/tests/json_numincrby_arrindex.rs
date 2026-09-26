//! JSON.NUMINCRBY / JSON.NUMMULTBY / JSON.ARRINDEX 命令执行体用例
//!
//! C# GarnetJSON 只注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
//! garnet/test/standalone/Garnet.test.extensions/GarnetJSON 下无同名用例，
//! 本文件按 RedisJSON 命令语义（非数值/非数组目标回 null 占位、
//! ARRINDEX 的 [start [stop]] 区间检索）核对 wedb 侧扩展执行体。
//!
//! 自研依据: NUMINCRBY 数组索引臂（C# 对应 modules/JSON 实现面）

use std::str::from_utf8;

use sonic_rs::{JsonValueTrait, Value};
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

/// 走 reader 执行体：回 (是否成功, 应答体)
fn run_reader(cmd: JsonCommand, doc: &str, args: &[&str]) -> (bool, String) {
  let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
  let mut out = Vec::new();
  let ok = (cmd.fns().reader)(doc.as_bytes(), &args, &mut out, RESP_VER);
  (ok, resp_text(&out))
}

/// 应答体按 JSON 解析（比对结果数组内容与顺序，不依赖数字文本形态）
fn resp_json(text: &str) -> Value {
  sonic_rs::from_str::<Value>(text).unwrap()
}

/// JSON.NUMINCRBY 非数值节点：不改值、应答位补 null（原回空数组 []）
#[test]
fn numincrby_non_numeric_node_returns_null() {
  for doc in [
    r#"{"a":"hello"}"#,
    r#"{"a":true}"#,
    r#"{"a":{"b":1}}"#,
    r#"{"a":[1,2]}"#,
    r#"{"a":null}"#,
  ] {
    let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, doc, &["$.a", "1"]);
    assert!(ok, "{doc} 应正常应答");
    assert_eq!(resp_json(&out), resp_json("[null]"), "{doc}");
    // 非数值节点不改值，载荷内容原样
    assert_eq!(resp_json(&payload), resp_json(doc), "{doc}");
  }
}

/// JSON.NUMMULTBY 非数值节点：同样回 null 占位
#[test]
fn nummultby_non_numeric_node_returns_null() {
  for doc in [r#"{"a":"hello"}"#, r#"{"a":{"b":1}}"#, r#"{"a":null}"#] {
    let (ok, out, payload) = run_updater(JsonCommand::NumMultBy, doc, &["$.a", "2"]);
    assert!(ok, "{doc} 应正常应答");
    assert_eq!(resp_json(&out), resp_json("[null]"), "{doc}");
    assert_eq!(resp_json(&payload), resp_json(doc), "{doc}");
  }
}

/// 多匹配下结果数组与匹配数等长：非数值匹配占 null 位
#[test]
fn numincrby_mixed_matches_keep_one_slot_each() {
  let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, r#"[1,"x",2]"#, &["$[*]", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[2,null,3]"));
  assert_eq!(payload, r#"[2,"x",3]"#);

  let (ok, out, _) = run_updater(JsonCommand::NumMultBy, r#"[2,"s",null]"#, &["$[*]", "3"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[6,null,null]"));
}

/// 数值节点形态：整数入参加整结果仍整数节点
#[test]
fn numincrby_integer_node_stays_integer() {
  let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, r#"{"a":1}"#, &["$.a", "2"]);
  assert!(ok);
  let res = resp_json(&out);
  assert_eq!(res, resp_json("[3]"));
  assert!(res[0].is_i64(), "整数入参应落整数节点: {out}");
  assert_eq!(payload, r#"{"a":3}"#);
}

/// 浮点入参即便整值结果也保持浮点节点（不漂移成整数）
#[test]
fn numincrby_float_node_keeps_float_shape() {
  let (ok, out, _) = run_updater(JsonCommand::NumIncrBy, r#"{"a":2.0}"#, &["$.a", "1"]);
  assert!(ok);
  let res = resp_json(&out);
  assert_eq!(res, resp_json("[3.0]"));
  assert!(res[0].is_f64(), "浮点入参应保浮点节点: {out}");

  let (ok, out, _) = run_updater(JsonCommand::NumIncrBy, r#"{"a":2.5}"#, &["$.a", "0.5"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[3.0]"));
}

/// 大于 i64::MAX 的大正整数经 as_u64 分支承接，乘加结果不塌成浮点、
/// 也不在 i64 回转中饱和
#[test]
fn numincrby_big_u64_node_keeps_integer_form() {
  let (ok, out, payload) = run_updater(
    JsonCommand::NumIncrBy,
    r#"{"a":10000000000000000000}"#,
    &["$.a", "2048"],
  );
  assert!(ok);
  assert_eq!(out, "[10000000000000002048]");
  let res = resp_json(&out);
  assert_eq!(res[0].as_u64(), Some(10_000_000_000_000_002_048));
  assert_eq!(payload, r#"{"a":10000000000000002048}"#);
}

/// 参数与数值文本校验（arity 4：key path number）
#[test]
fn numincrby_rejects_bad_args() {
  let (ok, out, _) = run_updater(JsonCommand::NumIncrBy, r#"{"a":1}"#, &["$.a"]);
  assert!(!ok);
  assert!(out.contains("wrong number of arguments"), "{out}");

  let (ok, out, _) = run_updater(JsonCommand::NumIncrBy, r#"{"a":1}"#, &["$.a", "abc"]);
  assert!(!ok);
  assert!(out.contains("not a valid float"), "{out}");
}

/// JSON.ARRINDEX 全量检索与未命中
#[test]
fn arrindex_scans_whole_array_by_default() {
  let doc = r#"{"a":[1,2,3]}"#;
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "2"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[1]"));

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "9"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));

  // 标量字符串元素检索
  let (ok, out) = run_reader(
    JsonCommand::ArrIndex,
    r#"{"a":["x","y"]}"#,
    &["$.a", "\"y\""],
  );
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[1]"));
}

/// JSON.ARRINDEX [start [stop]]：stop 为 exclusive 上界
#[test]
fn arrindex_honors_start_and_stop() {
  let doc = r#"{"a":[1,2,3,4]}"#;

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "2", "0", "2"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[1]"));

  // stop=1 时区间为 [0,1)，值 2 落在区间外
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "2", "0", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));

  // start 之后仍可命中
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "4", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[3]"));

  // start 跳过命中位置
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1", "1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));
}

/// stop = 0 是「检索至末尾」约定，而非空区间
#[test]
fn arrindex_stop_zero_searches_to_end() {
  let doc = r#"{"a":[1,2,3,4]}"#;
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "4", "0", "0"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[3]"));

  // 空数组无元素可检索
  let (ok, out) = run_reader(
    JsonCommand::ArrIndex,
    r#"{"a":[]}"#,
    &["$.a", "1", "0", "0"],
  );
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));
}

/// 负索引自末尾起算，越界端点钳入 [0, len]（口径同 C# ArraySliceFilter）
#[test]
fn arrindex_negative_indices_fold_from_end() {
  let doc = r#"{"a":[1,2,3,4]}"#;

  // start=-2 → 区间 [2,4)
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "3", "-2"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[2]"));

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1", "-2"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));

  // start=-4 stop=-1 → 区间 [0,3)：末元素落在 exclusive 上界之外
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "3", "-4", "-1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[2]"));

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "4", "-4", "-1"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1]"));

  // 越界负 start 钳到 0：检索仍自首位起
  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1", "-99"]);
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[0]"));
}

/// start >= stop 时区间为空，恒回 -1
#[test]
fn arrindex_empty_range_returns_minus_one() {
  let doc = r#"{"a":[1,2,3,4]}"#;
  for args in [
    vec!["$.a", "2", "3", "1"],
    vec!["$.a", "2", "1", "1"],
    vec!["$.a", "2", "5", "9"],
  ] {
    let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &args);
    assert!(ok);
    assert_eq!(resp_json(&out), resp_json("[-1]"), "{args:?}");
  }
}

/// 命中节点非数组：应答位回 null（原为静默少一项）
#[test]
fn arrindex_non_array_target_returns_null() {
  for doc in [
    r#"{"a":"hello"}"#,
    r#"{"a":{"b":1}}"#,
    r#"{"a":5}"#,
    r#"{"a":null}"#,
  ] {
    let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1"]);
    assert!(ok, "{doc} 应正常应答");
    assert_eq!(resp_json(&out), resp_json("[null]"), "{doc}");
  }
}

/// 多匹配下结果与匹配数等长，非数组匹配占 null 位
#[test]
fn arrindex_multi_match_keeps_one_slot_each() {
  let (ok, out) = run_reader(
    JsonCommand::ArrIndex,
    r#"[[1,2],"str",[3]]"#,
    &["$[*]", "3"],
  );
  assert!(ok);
  assert_eq!(resp_json(&out), resp_json("[-1,null,0]"));
}

/// 参数个数越界与区间文本非法的应答
#[test]
fn arrindex_rejects_bad_args() {
  let doc = r#"{"a":[1,2,3]}"#;

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a"]);
  assert!(!ok);
  assert!(out.contains("wrong number of arguments"), "{out}");

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1", "0", "1", "9"]);
  assert!(!ok);
  assert!(out.contains("wrong number of arguments"), "{out}");

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "1", "x"]);
  assert!(!ok);
  assert!(out.contains("syntax error"), "{out}");

  let (ok, out) = run_reader(JsonCommand::ArrIndex, doc, &["$.a", "not-json"]);
  assert!(!ok);
  assert!(out.contains("syntax error"), "{out}");
}
