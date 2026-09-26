//! 扫描族过滤器终结命中不重入的变异回归
//!
//! 依据：garnet/modules/GarnetJSON/GarnetJsonObject.cs:Set 为两阶段求值——
//! 先 `jsonPath.Evaluate(...).ToArray()` 物化匹配节点（源码注释
//! "Need ToArray to avoid modifying collection while iterating"），再对列表
//! 逐一 `ReplaceWith`；新插入值内部的容器绝不会被重扫。
//! 修复前 `mutate_recursive` 对终结扫描命中仍以同一 filter_idx 重入下钻，
//! `JSON.SET key $..* [1,2]` 会无限嵌套直至栈溢出（SIGSEGV）崩溃进程。
//! 终结用例的期望文档同时对照 JsonCommandsTest.cs:JsonSetGetTests /
//! JsonSetPathOperations（$..a、$..price 命中替换），并锚定 wedb 扩展
//! JSON.ARRAPPEND / JSON.NUMINCRBY 扫描路径的既有变异语义不回归。
//!
//! 自研依据: 变异/扫描终局面

use std::str::from_utf8;

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use wext_json::{GarnetJsonObject, JsonCommand, JsonPath, SetResult};
use wresp::options::ExistOptions;

fn val(json: &str) -> Value {
  sonic_rs::from_str(json).unwrap()
}

/// RESP 协议版本（resp2 帧，与 wnode 缺省会话一致）
const RESP_VER: u8 = 2;

/// 走 updater 执行体：回 (应答体文本, 改写后的载荷)
fn run_updater(cmd: JsonCommand, doc: &str, args: &[&str]) -> (String, String) {
  let mut payload = doc.as_bytes().to_vec();
  let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
  let mut out = Vec::new();
  assert!((cmd.fns().updater)(&mut payload, &args, &mut out, RESP_VER));
  let s = from_utf8(&out).unwrap();
  let body = if let Some(rest) = s.strip_prefix('$') {
    let head_end = rest.find("\r\n").unwrap();
    let len: usize = rest[..head_end].parse().unwrap();
    rest[head_end + 2..head_end + 2 + len].to_string()
  } else {
    s.trim_end_matches("\r\n").to_string()
  };
  (body, String::from_utf8(payload).unwrap())
}

/// 协议级 JSON.SET：根文档经 set 落进 obj 后与期望文档逐字节等值
fn json_set(path: &str, value: &str, initial: &str) -> Value {
  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", initial.as_bytes(), ExistOptions::None)
    .unwrap();
  let res = obj
    .set(path.as_bytes(), value.as_bytes(), ExistOptions::None)
    .unwrap();
  assert_eq!(res, SetResult::Success, "{path} := {value} 应成功");
  obj.root_node.expect("set 后根文档存在")
}

// ---- 终结命中不重入：崩溃面消除（修复前栈溢出/SIGSEGV） ----

/// $..* 以数组为新值：仅替换原匹配点，新容器 [1,2] 不被重扫；
/// 嵌套原命中（a.b）在 C# 中随外层 ReplaceWith 脱落、不可见
#[test]
fn mutate_wildcard_scan_array_value() {
  let mut root = val(r#"{"a":{"b":1}}"#);
  let path = JsonPath::parse("$..*").unwrap();
  let count = path.replace_matches(&mut root, &val("[1,2]"));
  assert_eq!(count, 1);
  assert_eq!(root, val(r#"{"a":[1,2]}"#));
}

/// $..* 以对象为新值：新对象内部同名键不得被再次通配替换
#[test]
fn mutate_wildcard_scan_object_value() {
  let mut root = val(r#"{"x":1}"#);
  let path = JsonPath::parse("$..*").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"a":1}"#));
  assert_eq!(count, 1);
  assert_eq!(root, val(r#"{"x":{"a":1}}"#));
}

/// 协议级 JSON.SET key $..* [1,2]：工单复现命令，必须收敛应答而非崩溃
#[test]
fn json_set_wildcard_scan_composite_value() {
  let root = json_set("$..*", "[1,2]", r#"{"f1":{"a":1},"f2":2}"#);
  assert_eq!(root, val(r#"{"f1":[1,2],"f2":[1,2]}"#));
}

/// $..a := {"a":7}：命中子树整体落入新值后其内部 "a" 不再被重扫（C# 中
/// 嵌套命中随外层 ReplaceWith 脱落不可见）；未命中的 b 分支继续下钻，
/// b.a 作为独立命中被替换
#[test]
fn mutate_named_scan_object_value() {
  let mut root = val(r#"{"a":{"a":1},"b":{"a":2}}"#);
  let path = JsonPath::parse("$..a").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"a":7}"#));
  assert_eq!(count, 2);
  assert_eq!(root, val(r#"{"a":{"a":7},"b":{"a":{"a":7}}}"#));
}

/// $..[0] := [1,2]：命中元素整体替换，新数组内部下标 0 不被再扫描
#[test]
fn mutate_scan_array_index_composite_value() {
  let mut root = val(r#"{"x":[[9]]}"#);
  let path = JsonPath::parse("$..[0]").unwrap();
  let count = path.replace_matches(&mut root, &val("[1,2]"));
  assert_eq!(count, 1);
  assert_eq!(root, val(r#"{"x":[[1,2]]}"#));
}

/// $..[0,1] := {"a":1}：两命中元素替换后，其内部不再被下标扫描触及
#[test]
fn mutate_scan_array_multiple_index_composite_value() {
  let mut root = val(r#"{"x":[{"p":1},{"q":2}]}"#);
  let path = JsonPath::parse("$..[0,1]").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"a":1}"#));
  assert_eq!(count, 2);
  assert_eq!(root, val(r#"{"x":[{"a":1},{"a":1}]}"#));
}

/// $..[0:2] := [7]：切片命中替换后不重扫新值内部（C# 嵌套命中脱落）；
/// 未命中的 [3] 槽位继续下钻，其元素 0 为可见命中（同 C# 枚举器）
#[test]
fn mutate_scan_array_slice_composite_value() {
  let mut root = val(r#"{"x":[[1],[2],[3]]}"#);
  let path = JsonPath::parse("$..[0:2]").unwrap();
  let count = path.replace_matches(&mut root, &val("[7]"));
  assert_eq!(count, 3);
  assert_eq!(root, val(r#"{"x":[[7],[7],[[7]]]}"#));
}

/// $..['p'] := {"p":0}：多键命中替换后，新对象内同名键 "p" 不得再被命中
/// （C# 脱落）；未命中的 r 分支下钻后 r.p 为独立可见命中
#[test]
fn mutate_scan_multiple_composite_value() {
  let mut root = val(r#"{"p":{"p":1},"r":{"p":2}}"#);
  let path = JsonPath::parse("$..['p']").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"p":0}"#));
  assert_eq!(count, 2);
  assert_eq!(root, val(r#"{"p":{"p":0},"r":{"p":{"p":0}}}"#));
}

/// $..[?(@.price)] := {"price":1}：查询扫描命中替换后不重扫新值内部
#[test]
fn mutate_query_scan_composite_value() {
  let mut root = val(r#"{"a":{"price":5},"b":[{"price":2}]}"#);
  let path = JsonPath::parse("$..[?(@.price)]").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"price":1}"#));
  assert_eq!(count, 2);
  assert_eq!(root, val(r#"{"a":{"price":1},"b":[{"price":1}]}"#));
}

/// 根自身命中：C# Set 对 `rootNode == match` 直接整根换为新值，不触及新值内部
#[test]
fn mutate_query_scan_root_self_hit() {
  let mut root = val(r#"{"price":5}"#);
  let path = JsonPath::parse("$..[?(@.price)]").unwrap();
  let count = path.replace_matches(&mut root, &val(r#"{"price":1}"#));
  assert_eq!(count, 1);
  assert_eq!(root, val(r#"{"price":1}"#));
}

// ---- 既有变异语义不回归（C# 真值 + wedb 扩展锚定） ----

/// 对标 C# JsonSetGetTests：$..a := 3 全部具名命中改值
#[test]
fn json_set_named_scan_scalar_regression() {
  let root = json_set("$..a", "3", r#"{"f1":{"a":1},"f2":{"a":2}}"#);
  assert_eq!(root, val(r#"{"f1":{"a":3},"f2":{"a":3}}"#));
}

/// 对标 C# JsonSetPathOperations：$..a := 42 于 {"x":{"a":1},"y":{"a":2}}
#[test]
fn json_set_named_scan_42_regression() {
  let root = json_set("$..a", "42", r#"{"x":{"a":1},"y":{"a":2}}"#);
  assert_eq!(root, val(r#"{"x":{"a":42},"y":{"a":42}}"#));
}

/// JSON.ARRAPPEND $..[*] 9：通配扫描命中的每个数组追加一次，
/// 标量命中不改值不出列，嵌套数组不因重扫被二次追加
#[test]
fn json_arrappend_terminal_wildcard_scan() {
  let (out, payload) = run_updater(
    JsonCommand::ArrAppend,
    r#"{"a":[[1],[2]]}"#,
    &["$..[*]", "9"],
  );
  assert_eq!(out, "[2,2]");
  assert_eq!(val(&payload), val(r#"{"a":[[1,9],[2,9]]}"#));
}

/// JSON.NUMINCRBY $..a 3：具名扫描全部命中累加，结果数组与匹配数等长。
/// 对象键遍历序按仓库规范随进程随机种子，不做定序断言，只断言多重集
#[test]
fn json_numincrby_named_scan_regression() {
  let (out, payload) = run_updater(
    JsonCommand::NumIncrBy,
    r#"{"f1":{"a":1},"f2":{"a":2}}"#,
    &["$..a", "3"],
  );
  let mut results: Vec<i64> = val(&out)
    .as_array()
    .expect("结果数组")
    .iter()
    .map(|v| v.as_i64().expect("整数"))
    .collect();
  results.sort_unstable();
  assert_eq!(results, vec![4, 5]);
  assert_eq!(val(&payload), val(r#"{"f1":{"a":4},"f2":{"a":5}}"#));
}
