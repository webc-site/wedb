//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs
//!
//! 自研依据: 查询表达式求值（C# 对应 JSONPath 过滤表达式）

use sonic_rs::Value;
use wext_json::JsonPath;

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:AndExpressionTest
#[test]
fn and_expression_test() {
  let json = r#"[{"a": 1, "b": 2}, {"a": 1, "b": 3}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1 && @.b == 2)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:OrExpressionTest
#[test]
fn or_expression_test() {
  let json = r#"[{"a": 1}, {"b": 2}, {"c": 3}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1 || @.b == 2)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpression_EqualsOperator
#[test]
fn boolean_expression_equals_operator() {
  let json = r#"[{"active": true}, {"active": false}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.active == true)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_RegexEqualsOperator
#[test]
fn boolean_expression_test_regex_equals_operator() {
  let json = r#"[{"name": "Alice"}, {"name": "Bob"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.name =~ /^A.*/)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_RegexEqualsOperator_CornerCase
#[test]
fn boolean_expression_test_regex_equals_operator_corner_case() {
  let json = r#"[{"name": "Alice"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.name =~ /.*/i)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// 非 ASCII 正则谓词：pattern 须按 UTF-8 原样保留（read_regex_string 禁止
/// 逐字节 as char 的 Latin-1 mojibake，纪律同 read_quoted_string），多字节
/// 字面量可匹配
#[test]
fn regex_predicate_preserves_non_ascii_pattern() {
  let json = r#"[{"name": "北京"}, {"name": "上海"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.name =~ /北京/)]").unwrap();
  assert_eq!(
    path.evaluate(&val).unwrap().len(),
    1,
    "多字节字面量谓词须命中"
  );
  let path = JsonPath::parse("$[?(@.name =~ /^上/)]").unwrap();
  assert_eq!(
    path.evaluate(&val).unwrap().len(),
    1,
    "多字节锚点谓词须命中"
  );
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest
#[test]
fn boolean_expression_test() {
  let json = r#"[{"a": 10}, {"a": 20}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 10)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_GreaterThanOperator
#[test]
fn boolean_expression_test_greater_than_operator() {
  let json = r#"[{"a": 10}, {"a": 20}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a > 15)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_GreaterThanOrEqualsOperator
#[test]
fn boolean_expression_test_greater_than_or_equals_operator() {
  let json = r#"[{"a": 15}, {"a": 10}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a >= 15)]").unwrap();
  assert_eq!(path.evaluate(&val).unwrap().len(), 1);
}

/// 字面正则谓词扫描多元素数组求值
#[test]
fn regex_predicate_evaluates_across_elements() {
  let json = r#"{"items":[{"name":"A1"},{"name":"AB"},{"name":"B"},{"name":"AC"}]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.items[?(@.name =~ /^A/)]").unwrap();
  let matches = path.evaluate(&val).unwrap();
  assert_eq!(matches.len(), 3);
}
