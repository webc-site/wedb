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
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:OrExpressionTest
#[test]
fn or_expression_test() {
  let json = r#"[{"a": 1}, {"b": 2}, {"c": 3}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1 || @.b == 2)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpression_EqualsOperator
#[test]
fn boolean_expression_equals_operator() {
  let json = r#"[{"active": true}, {"active": false}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.active == true)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_RegexEqualsOperator
#[test]
fn boolean_expression_test_regex_equals_operator() {
  let json = r#"[{"name": "Alice"}, {"name": "Bob"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.name =~ /^A.*/)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_RegexEqualsOperator_CornerCase
#[test]
fn boolean_expression_test_regex_equals_operator_corner_case() {
  let json = r#"[{"name": "Alice"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.name =~ /.*/i)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest
#[test]
fn boolean_expression_test() {
  let json = r#"[{"a": 10}, {"a": 20}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 10)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_GreaterThanOperator
#[test]
fn boolean_expression_test_greater_than_operator() {
  let json = r#"[{"a": 10}, {"a": 20}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a > 15)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/QueryExpressionTests.cs:BooleanExpressionTest_GreaterThanOrEqualsOperator
#[test]
fn boolean_expression_test_greater_than_or_equals_operator() {
  let json = r#"[{"a": 15}, {"a": 10}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a >= 15)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}
