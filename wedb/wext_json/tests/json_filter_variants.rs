//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs
//!
//! 变异/删除状态机对切片（ArraySlice/ScanArraySlice）与扫描过滤器
//! （ScanArrayIndex/ScanArrayMultipleIndex/ScanMultiple/QueryScan）变体的补齐回归。
//! 切片期望下标集取自 JsonCommandsTest.cs:JsonGetArrayOperationsTest 的真实断言口径
//! （$[1:3]→[2,3]、$[-2:]→[3,4]、$[::2]→[1,3] 于 [1,2,3,4]），
//! 查询过滤器期望取自 JsonGetAdvancedFiltersTest 的 `@.price > 20 → 22.99` 口径。
//!
//! 自研依据: JSON 过滤器变体（JSONPath 面）

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use wext_json::JsonPath;

fn val(json: &str) -> Value {
  sonic_rs::from_str(json).unwrap()
}

fn arr_i64(v: &Value, key: &str) -> Vec<i64> {
  v[key]
    .as_array()
    .expect("array expected")
    .iter()
    .map(|e| e.as_i64().expect("int expected"))
    .collect()
}

fn four() -> Value {
  val(r#"{"arr":[1,2,3,4]}"#)
}

/// 对标 ArraySliceFilter.cs:ExecuteFilter 命中集，$[1:3]→[2,3] 同索引集
#[test]
fn mutate_array_slice_positive() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[1:3]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 2);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 9, 9, 4]);
}

/// 对标 JsonCommandsTest.cs 用例 $[-2:]→[3,4]：负下标折算后命中末两元素
#[test]
fn mutate_array_slice_negative_start() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[-2:]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 2);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 2, 9, 9]);
}

/// 对标 JsonCommandsTest.cs 用例 $[::2]→[1,3]：正步进隔一取一
#[test]
fn mutate_array_slice_step() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[::2]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 2);
  assert_eq!(arr_i64(&root, "arr"), vec![9, 2, 9, 4]);
}

/// 反向步进 $[3:0:-1]：C# `for (i=startIndex; IsValid(i,stop,negative); i+=step)`
/// 产出降序 3,2,1，用 +10 增量回调证明逐元素命中而非整体替换
#[test]
fn mutate_array_slice_reverse_step() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[3:0:-1]").unwrap();
  let mut count = 0;
  // 谓词根上下文快照：$ 引用按进入时的文档根求值
  let root_snapshot = root.clone();
  path.mutate_recursive(&root_snapshot, &mut root, 0, &mut |target| {
    let n = target.as_i64().expect("int expected");
    *target = val(&(n + 10).to_string());
    count += 1;
  });
  assert_eq!(count, 3);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 12, 13, 14]);
}

/// step==0 承接 ArraySliceFilter.cs 的异常分支为静默空集，不得 panic
#[test]
fn mutate_array_slice_zero_step_noop() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[1:3:0]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 0);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 2, 3, 4]);
}

#[test]
fn delete_array_slice_positive() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[1:3]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 4]);
}

#[test]
fn delete_array_slice_negative_start() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[-2:]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 2]);
}

#[test]
fn delete_array_slice_step() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[::2]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(arr_i64(&root, "arr"), vec![2, 4]);
}

/// $[::-2]：反向步进命中 3,1，删除前必须折算为升序逆序 remove，
/// 若按产出顺序直接 remove 会因下标偏移删错元素
#[test]
fn delete_array_slice_reverse_step() {
  let mut root = four();
  let path = JsonPath::parse("$.arr[::-2]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(arr_i64(&root, "arr"), vec![1, 3]);
}

/// ScanArrayIndex（$..[0]）：先序遍历每个嵌套数组的折算下标 0，
/// 与 ScanArrayIndexFilter.cs:ExecuteFilter 的 innerArr 命中点一致
#[test]
fn mutate_scan_array_index() {
  let mut root = val(r#"{"a":[1,2],"b":{"c":[3,4]}}"#);
  let path = JsonPath::parse("$..[0]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 2);
  assert_eq!(arr_i64(&root, "a"), vec![9, 2]);
  assert_eq!(arr_i64(&root["b"], "c"), vec![9, 4]);
}

#[test]
fn delete_scan_array_index() {
  let mut root = val(r#"{"a":[1,2],"b":{"c":[3,4]}}"#);
  let path = JsonPath::parse("$..[0]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(arr_i64(&root, "a"), vec![2]);
  assert_eq!(arr_i64(&root["b"], "c"), vec![4]);
}

/// ScanArrayMultipleIndex（$..[0,2,-1,9]）：负下标按各数组长度独立折算、越界丢弃，
/// 外层 a(len4) 命中 0,2,3、内层 c(len2) 命中 0,1(-1)，共 5
#[test]
fn mutate_scan_array_multiple_index() {
  let mut root = val(r#"{"a":[1,2,3,4],"b":{"c":[5,6]}}"#);
  let path = JsonPath::parse("$..[0,2,-1,9]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 5);
  assert_eq!(arr_i64(&root, "a"), vec![9, 2, 9, 9]);
  assert_eq!(arr_i64(&root["b"], "c"), vec![9, 9]);
}

#[test]
fn delete_scan_array_slice() {
  let mut root = val(r#"{"a":[1,2,3,4],"b":{"c":[1,2,3,4]}}"#);
  let path = JsonPath::parse("$..[::2]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 4);
  assert_eq!(arr_i64(&root, "a"), vec![2, 4]);
  assert_eq!(arr_i64(&root["b"], "c"), vec![2, 4]);
}

/// ScanArraySlice 终结删除：命中子树含嵌套数组时逐容器独立求解下标集
#[test]
fn mutate_scan_array_slice() {
  let mut root = val(r#"{"a":[1,2,3,4],"b":{"c":[1,2,3,4]}}"#);
  let path = JsonPath::parse("$..[1:3]").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 4);
  assert_eq!(arr_i64(&root, "a"), vec![1, 9, 9, 4]);
  assert_eq!(arr_i64(&root["b"], "c"), vec![1, 9, 9, 4]);
}

/// ScanMultiple（$..['p','q']）：仅对象成员可命中，数组仅下钻，
/// 与 ScanMultipleFilter.cs:ExecuteFilter 的 objectEnumerator 判定一致
#[test]
fn mutate_scan_multiple() {
  let mut root = val(r#"{"x":{"p":1},"y":{"z":{"p":2,"q":3}}}"#);
  let path = JsonPath::parse("$..['p','q']").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 3);
  assert_eq!(root["x"]["p"].as_i64(), Some(9));
  assert_eq!(root["y"]["z"]["p"].as_i64(), Some(9));
  assert_eq!(root["y"]["z"]["q"].as_i64(), Some(9));
}

#[test]
fn delete_scan_multiple() {
  let mut root = val(r#"{"x":{"p":1},"y":{"z":{"p":2,"q":3}}}"#);
  let path = JsonPath::parse("$..['p','q']").unwrap();
  assert_eq!(path.delete_matches(&mut root), 3);
  assert_eq!(root["x"].as_object().expect("obj").len(), 0);
  assert_eq!(root["y"]["z"].as_object().expect("obj").len(), 0);
  assert!(root["y"].as_object().expect("obj").contains_key(&"z"));
}

fn store_doc() -> Value {
  val(
    r#"{"store":{"book":[{"price":8.95,"title":"a"},{"price":22.99,"title":"b"}],"bicycle":{"price":25.0}}}"#,
  )
}

/// QueryScan 终结删除：对标 JsonGetAdvancedFiltersTest 的 `@.price > 20` 口径，
/// 命中 book[1] 与 bicycle（对象自身按表达式命中），父容器内移除共 2
#[test]
fn delete_query_scan() {
  let mut root = store_doc();
  let path = JsonPath::parse("$..[?(@.price > 20)]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  let book = root["store"]["book"].as_array().expect("array");
  assert_eq!(book.len(), 1);
  assert_eq!(book[0]["price"].as_f64(), Some(8.95));
  assert!(
    !root["store"]
      .as_object()
      .expect("obj")
      .contains_key(&"bicycle")
  );
}

#[test]
fn mutate_query_scan() {
  let mut root = store_doc();
  let path = JsonPath::parse("$..[?(@.price > 20)]").unwrap();
  let count = path.replace_matches(&mut root, &val("99"));
  assert_eq!(count, 2);
  let book = root["store"]["book"].as_array().expect("array");
  assert_eq!(book[1].as_i64(), Some(99));
  assert_eq!(root["store"]["bicycle"].as_i64(), Some(99));
}

/// 读写一致性矩阵：各补齐变体的 evaluate（已由 C# 逐条对齐的只读臂）命中数
/// 必须与变异/删除状态机实际改动数逐一相等，任何臂漏接或重复触达都会红
#[test]
fn read_write_match_count_matrix() {
  let cases: Vec<(&str, &str)> = vec![
    (r#"{"arr":[1,2,3,4,5],"n":{"arr":[1,2]}}"#, "$..[0]"),
    (r#"{"arr":[1,2,3,4,5],"n":{"arr":[1,2]}}"#, "$..[::2]"),
    (r#"{"arr":[1,2,3,4,5],"n":{"arr":[1,2]}}"#, "$..[0,2,-1]"),
    (r#"{"arr":[1,2,3,4]}"#, "$.arr[1:3]"),
    (r#"{"arr":[1,2,3,4]}"#, "$.arr[-2:]"),
    (r#"{"arr":[1,2,3,4]}"#, "$.arr[::-1]"),
    (r#"{"x":{"p":1,"q":2},"y":{"p":3}}"#, "$..['p','q']"),
  ];
  for (json, path_str) in cases {
    let path = JsonPath::parse(path_str).unwrap();
    let doc = val(json);
    let expected = path.evaluate(&doc).len();
    let mut replace_root = doc.clone();
    let replaced = path.replace_matches(&mut replace_root, &val("9"));
    assert_eq!(
      replaced, expected,
      "replace count mismatch for {path_str} on {json}"
    );
    let mut delete_root = doc.clone();
    let deleted = path.delete_matches(&mut delete_root);
    assert_eq!(
      deleted, expected,
      "delete count mismatch for {path_str} on {json}"
    );
  }
}

/// 中间层切片路由：$..[0:1].v 先切片命中数组首元素再向其 .v 字段变异，
/// 证明 ArraySlice/ScanArraySlice 作为非终结过滤器可下传
#[test]
fn mutate_through_intermediate_slice() {
  let mut root = val(r#"{"a":[{"v":1},{"v":2}],"b":[{"v":3}]}"#);
  let path = JsonPath::parse("$..[0].v").unwrap();
  let count = path.replace_matches(&mut root, &val("9"));
  assert_eq!(count, 2);
  assert_eq!(root["a"][0]["v"].as_i64(), Some(9));
  assert_eq!(root["b"][0]["v"].as_i64(), Some(9));
  assert_eq!(root["a"][1]["v"].as_i64(), Some(2));
}

#[test]
fn delete_through_intermediate_slice() {
  let mut root = val(r#"{"a":[{"v":1,"w":7},{"v":2}],"b":[{"v":3}]}"#);
  let path = JsonPath::parse("$.a[0:2].v").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert!(!root["a"][0].as_object().expect("obj").contains_key(&"v"));
  assert!(root["a"][0].as_object().expect("obj").contains_key(&"w"));
  assert!(!root["a"][1].as_object().expect("obj").contains_key(&"v"));
}
