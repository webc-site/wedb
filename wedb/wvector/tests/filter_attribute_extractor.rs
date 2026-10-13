#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 属性提取器集成测试（自 src/filter/attribute_extractor.rs 外迁）

use wvector::filter::{ExprProgram, ExprToken, ExprTokenType, attribute_extractor::extract_fields};

#[test]
fn extract_multi_fields_single_pass() {
  let json = br#" { "a": 1, "b": "two", "c": [3, 4] } "#;
  // 选择器引用下方 filter 字节中的字段名（与 compiler 产出一致的区间）
  let filter = b"aXXXXbXXXXcXXXX"; // 区间占位：(0,1) (5,1) (10,1)
  let selectors = [(0i32, 1i32), (5, 1), (10, 1)];
  let mut program = ExprProgram::default();
  let mut results = [ExprToken::default(); 3];
  let found = extract_fields(json, filter, &selectors, &mut results, &mut program);
  assert_eq!(found, 3);
  assert_eq!(results[0], ExprToken::new_num(1.0));
  assert_eq!(results[1].token_type, ExprTokenType::Str);
  assert!(results[2].is_runtime_tuple());
  assert_eq!(results[2].utf8_length, 2);
  // 运行期池已收录数组元素
  assert_eq!(program.runtime_pool_len, 2);
  assert_eq!(program.runtime_pool[0], ExprToken::new_num(3.0));
}

#[test]
fn extract_fields_escape_literal_and_nested_array() {
  // 四字段覆盖：转义串（has_escape 旗标 + 源字节区间零拷贝引用）、
  // true/null 字面量臂、嵌套数组经 parse_value_token_inner(None) 降级 Null
  let json = br#"{"a": "k\"x", "b": true, "c": null, "d": [[1, 2], 3]}"#;
  // 选择器引用下方 filter 字节中的字段名区间：(0,1) (5,1) (10,1) (15,1)
  let filter = b"aXXXXbXXXXcXXXXdXXXX";
  let selectors = [(0i32, 1i32), (5, 1), (10, 1), (15, 1)];
  let mut program = ExprProgram::default();
  let mut results = [ExprToken::default(); 4];
  let found = extract_fields(json, filter, &selectors, &mut results, &mut program);
  assert_eq!(found, 4);

  // 转义串：内容区间 (7, 4) 引用源字节 k\"x，旗标置位
  assert_eq!(results[0].token_type, ExprTokenType::Str);
  assert_eq!((results[0].utf8_start, results[0].utf8_length), (7, 4));
  assert_eq!(
    &json
      [results[0].utf8_start as usize..(results[0].utf8_start + results[0].utf8_length) as usize],
    br#"k\"x"#
  );
  assert!(results[0].has_escape());

  // 字面量臂：true → Num 1.0，null → Null
  assert_eq!(results[1], ExprToken::new_num(1.0));
  assert_eq!(results[2], ExprToken::new_null());

  // 嵌套数组：外层为 (0, 2) 运行期元组，内层 [1, 2] 走无池重载降级为 Null 元素
  assert!(results[3].is_runtime_tuple());
  assert_eq!((results[3].utf8_start, results[3].utf8_length), (0, 2));
  assert_eq!(program.runtime_pool_len, 2);
  assert_eq!(program.runtime_pool[0], ExprToken::new_null());
  assert_eq!(program.runtime_pool[1], ExprToken::new_num(3.0));
}
#[test]
fn extract_overflow_and_reject_special_wordforms() {
  // 提取径与编译径同谱：溢出字面量折 ±Inf 照常产出 Num 词形（旧实现值域门误拒）；
  // nan/inf 词形在词法层拒绝（C# Utf8Parser 不认，JSON 数值语法亦不含）
  let json = br#"{"a": 1e999, "b": -1e999, "c": nan, "d": inf}"#;
  let filter = b"aXXXXbXXXXcXXXXdXXXX";
  let selectors = [(0i32, 1i32), (5, 1), (10, 1), (15, 1)];
  let mut program = ExprProgram::default();
  let mut results = [ExprToken::default(); 4];
  let found = extract_fields(json, filter, &selectors, &mut results, &mut program);
  // c=nan 词法拒 → None 词元；值解析失败不推进 → d 未及提取（既有 extract_fields 失败臂）
  assert_eq!(found, 3);
  assert_eq!(results[0], ExprToken::new_num(f64::INFINITY));
  assert_eq!(results[1], ExprToken::new_num(f64::NEG_INFINITY));
  assert!(results[2].is_none());
  assert!(results[3].is_none());
}
