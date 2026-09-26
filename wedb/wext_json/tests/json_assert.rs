//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonAssert.cs
//!
//! 自研依据: JSON 断言基建（C# 对应 modules/JSON 测试工具面）

use sonic_rs::Value;

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonAssert.cs:AreEqual
pub fn are_equal(expected: &Value, actual: &Value) -> bool {
  expected == actual
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonAssert.cs:DeepEquals
pub fn deep_equals(v1: &Value, v2: &Value) -> bool {
  v1 == v2
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonAssert.cs:Equals
pub fn json_equals(v1: &Value, v2: &Value) -> bool {
  v1 == v2
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonAssert.cs:GetHashCode
pub fn get_hash_code(_v: &Value) -> u64 {
  0
}

#[test]
fn test_json_assert_helpers() {
  let v1: Value = sonic_rs::from_str("{\"a\": 1}").unwrap();
  let v2: Value = sonic_rs::from_str("{\"a\": 1}").unwrap();
  assert!(are_equal(&v1, &v2));
  assert!(deep_equals(&v1, &v2));
  assert!(json_equals(&v1, &v2));
  assert_eq!(get_hash_code(&v1), 0);
}
