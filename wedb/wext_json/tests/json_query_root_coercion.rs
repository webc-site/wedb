//! 过滤表达式根上下文与比较类型提升回归
//!
//! 对标 garnet/test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/
//! JsonPathExecuteTests.cs:RootInFilter / MultipleQueries 与
//! modules/GarnetJSON/JSONPath/QueryExpression.cs:CompareTo /
//! EqualsWithStringCoercion、JsonPath.cs:ParseOperator 的 <> 等价识别。
//! 求值面（evaluate）已有 root_in_filter 系列覆盖；本文件专测变异/删除面的
//! 根上下文透传、字符串同型字典序、布尔↔字符串对等转换与 <> 操作符。
//!
//! 自研偏差锁: 查询根值聚合（doc/zh/deviations.md JSON 面）

use sonic_rs::{JsonValueTrait, Value};
use wext_json::JsonPath;

fn doc(s: &str) -> Value {
  sonic_rs::from_str(s).unwrap()
}

/// 变异/删除面：中间层 Field 收敛后终结 Query 谓词内的 $ 必须按文档根求值；
/// 伪根（current.clone()）会把 $.threshold 误定位到 outer 子树的 99
#[test]
fn delete_nested_query_predicate_uses_document_root() {
  let mut root = doc(r#"{"threshold":5,"outer":{"threshold":99,"list":[{"v":1},{"v":2}]}}"#);
  let path = JsonPath::parse("$.outer.list[?($.threshold < 10)]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 2);
  assert_eq!(
    root,
    doc(r#"{"threshold":5,"outer":{"threshold":99,"list":[]}}"#)
  );
}

/// 同路径的 replace_matches 变异面：谓词按根求值命中两个列表元素并覆写
#[test]
fn replace_nested_query_predicate_uses_document_root() {
  let mut root = doc(r#"{"threshold":5,"outer":{"threshold":99,"list":[{"v":1}]}}"#);
  let path = JsonPath::parse("$.outer.list[?($.threshold < 10)]").unwrap();
  assert_eq!(path.replace_matches(&mut root, &doc("0")), 1);
  assert_eq!(
    root,
    doc(r#"{"threshold":5,"outer":{"threshold":99,"list":[0]}}"#)
  );
}

/// QueryScan 面：谓词在深层子树求值时伪根会读到子树自身的 far=5 误命中；
/// 真根无 far 字段则不命中，文档应原样保留
#[test]
fn delete_query_scan_predicate_uses_document_root() {
  let mut root = doc(r#"{"a":{"far":5,"arr":[1,2]}}"#);
  let path = JsonPath::parse("$.a..[?($.far < 10)]").unwrap();
  assert_eq!(
    path.delete_matches(&mut root),
    0,
    "真根无 far 字段，谓词恒假"
  );
  assert_eq!(root, doc(r#"{"a":{"far":5,"arr":[1,2]}}"#));
}

/// QueryScan 正例：根命中时整体删除深层命中子值（伪根则改删孙层字段，
/// 结果文档不同）
#[test]
fn delete_query_scan_root_match_removes_matched_child() {
  let mut root = doc(r#"{"far":1,"z":{"inner":{"far":5}}}"#);
  let path = JsonPath::parse("$.z..[?($.far < 10)]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 1);
  assert_eq!(root, doc(r#"{"far":1,"z":{}}"#));
}

/// 字符串同型严格字典序："100" < "20"（Ordinal），不得被数值提升颠倒；
/// 对位 C# CompareTo 的 JsonValueKind.String 同型分支
#[test]
fn string_pair_compares_ordinal_not_numeric() {
  let root = doc(r#"["100","20"]"#);
  let path = JsonPath::parse(r#"$[?(@ < "20")]"#).unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(hits.len(), 1, "字典序下仅 \"100\" < \"20\": {hits:?}");
  assert_eq!(hits[0].as_str(), Some("100"));
}

/// 数值同型与跨型提升：数字对仍按数值比较；数字字符串与数字跨型时
/// 按 C# TryGetAsDouble 提升为数值比较；不可提升的字符串对按 C#
/// CompareTo 末尾 ToJsonString 字典序兜底（"x" > "2" 成立）
#[test]
fn number_pair_and_cross_type_promotion() {
  let root = doc("[1,2,3]");
  let path = JsonPath::parse("$[?(@ > 2)]").unwrap();
  assert_eq!(path.evaluate(&root).len(), 1);

  let root = doc(r#"["10", "x"]"#);
  let path = JsonPath::parse(r#"$[?(@ > 2)]"#).unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(
    hits.len(),
    2,
    "\"10\" 数值提升、\"x\" 走字典序兜底: {hits:?}"
  );
}

/// 布尔↔字符串对等转换：@ == 'true' 命中 true、@ != 'false' 对 false
/// 判等为真（对位 C# EqualsWithStringCoercion 的 bool.TryParse 分支）
#[test]
fn boolean_equals_string_coercion() {
  let root = doc("[true,false]");
  let path = JsonPath::parse("$[?(@ == 'true')]").unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(hits.len(), 1, "仅 true == 'true': {hits:?}");
  assert_eq!(hits[0].as_bool(), Some(true));

  let path = JsonPath::parse("$[?(@ != 'false')]").unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(hits.len(), 1, "false != 'false' 经转换应判等: {hits:?}");
  assert_eq!(hits[0].as_bool(), Some(true));
}

/// ParseOperator：<> 与 != 等价识别（C# JsonPath.cs:ParseOperator）；
/// 修复前 <> 在 < 处截断残留 > 抛 Unexpected character
#[test]
fn angle_not_equals_operator_parses() {
  let path = JsonPath::parse("$[?(@ <> 1)]").expect("<> 应解析为 NotEquals");
  let root = doc("[1,2,3]");
  assert_eq!(path.evaluate(&root).len(), 2);

  // C# JsonPathExecuteTests.cs:921 同款正则文本面
  assert!(JsonPath::parse("[ ?( @..hi <> 'ha' ) ]").is_ok());
}

/// <> 与 != 在级联过滤中等价：[1..9] 经 [?(@ <> 1)][?(@ <> 4)][?(@ < 7)] 引擎级
/// 回 0 条（C# JsonPathExecuteTests.cs:MultipleQueries :941-949 原断言）——第二个
/// 过滤器收到的 current 为标量集合逐元素摊开后的标量节点，C# QueryFilter
/// .ExecuteFilter 对标量无臂恒零产出，整链断流。事实锚为 GarnetJSON 自有引擎
/// （JsonExtensions.cs:67-72 SelectNodes，.NET BCL 无 JsonNode.SelectNodes）
/// 与其测试双证。
#[test]
fn multiple_queries_chained_filter() {
  let root = doc("[1,2,3,4,5,6,7,8,9]");
  let path = JsonPath::parse("[?(@ <> 1)][?(@ <> 4)][?(@ < 7)]").unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(hits.len(), 0, "标量级联断流，引擎级须回 0 条: {hits:?}");
}

/// 变异回调执行期内谓词上下文仍为变异前快照（C# 两阶段求值口径）：
/// 覆写为 0 后列表元素不因根快照刷新而二次命中
#[test]
fn replace_snapshot_is_pre_mutation_state() {
  let mut root = doc(r#"{"threshold":5,"list":[7,8]}"#);
  let path = JsonPath::parse("$.list[?($.threshold < 10)]").unwrap();
  assert_eq!(path.replace_matches(&mut root, &doc("0")), 2);
  assert_eq!(root, doc(r#"{"threshold":5,"list":[0,0]}"#));
}
