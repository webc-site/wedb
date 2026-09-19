//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathRegressionTests.rs

use sonic_rs::{JsonValueTrait, Value};
use wext_json::JsonPath;

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathRegressionTests.cs:ToString
pub fn test_to_string() {}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathRegressionTests.cs:LoadCases
pub fn load_cases() {}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathRegressionTests.cs:TestRegression
#[test]
fn test_regression() {
  let json = r#"{"a": [1, 2, 3], "b": {"c": "hello"}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let p1 = JsonPath::parse("$.a[1]").unwrap();
  assert_eq!(p1.evaluate(&val)[0].as_i64(), Some(2));

  let p2 = JsonPath::parse("$.b.c").unwrap();
  assert_eq!(p2.evaluate(&val)[0].as_str(), Some("hello"));
}
