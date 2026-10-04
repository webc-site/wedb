//! 过滤表达式虚拟机求值测试（自 src/filter/runner.rs 外迁）

use wvector::filter::{
  ExprStack, ExprToken, ExprTokenType,
  attribute_extractor::extract_fields,
  compiler::try_compile,
  default_stack, run,
  runner::{unescape_byte, unescaped_equals},
};

/// 编译 + 提取 + 求值的完整管线。
fn eval(filter: &str, json: &[u8]) -> bool {
  let mut program = try_compile(filter.as_bytes()).unwrap();

  // 收集选择器区间
  let mut selectors: Vec<(i32, i32)> = Vec::new();
  for inst in &program.instructions {
    if inst.token_type == ExprTokenType::Selector {
      let range = (inst.utf8_start, inst.utf8_length);
      if !selectors.contains(&range) {
        selectors.push(range);
      }
    }
  }

  let mut fields = vec![ExprToken::default(); selectors.len().max(1)];
  extract_fields(
    json,
    filter.as_bytes(),
    &selectors,
    &mut fields,
    &mut program,
  );

  let mut stack = default_stack();
  run(
    &program,
    json,
    filter.as_bytes(),
    &selectors,
    &fields,
    &mut stack,
  )
}

#[test]
fn numeric_comparisons() {
  let json = br#"{"year": 2000, "rating": 7.5}"#;
  assert!(eval(".year >= 2000 and .rating > 7", json));
  assert!(!eval(".year > 2000", json));
  assert!(!eval(".rating == 7", json));
  assert!(eval(".rating != 7", json));
  assert!(eval(".rating <= 7.5", json));
  assert!(eval("(.year + 1) * 2 == 4002", json));
  assert!(eval("2 ** 10 == 1024", json));
  assert!(eval("10 % 3 == 1", json));
  assert!(eval("not (.year < 2000)", json));
}

#[test]
fn string_comparisons() {
  let json = br#"{"name": "alice", "tag": "a\"b"}"#;
  assert!(eval(".name == \"alice\"", json));
  assert!(eval(".name != \"bob\"", json));
  // 运行期含转义值 vs 编译期含转义字面量：在线反转义比较
  assert!(eval(".tag == \"a\\\"b\"", json));
  // 子串：a in b 判定 a 是否为 b 的子串
  assert!(eval("\"lic\" in .name", json));
  assert!(!eval(".name in \"lic\"", json));
  assert!(!eval(".name in \"xyz\"", json));
}

#[test]
fn in_operator_with_tuples() {
  let json = br#"{"year": 1999, "tags": ["red", "blue"]}"#;
  // 编译期元组
  assert!(eval(".year in [1998, 1999, 2000]", json));
  assert!(!eval(".year in [2000, 2001]", json));
  // 运行期元组（JSON 数组）
  assert!(eval("\"blue\" in .tags", json));
  assert!(!eval("\"green\" in .tags", json));
}

#[test]
fn null_and_missing_fields() {
  let json = br#"{"x": null, "y": 5}"#;
  assert!(eval(".x == null", json));
  assert!(!eval(".x != null", json));
  // 缺失字段 → 求值失败 → false
  assert!(!eval(".missing == 1", json));
  assert!(!eval(".missing != 1", json));
  // null 为假值
  assert!(!eval(".x", json));
  assert!(eval("not .x", json));
}

#[test]
fn escaped_equality_matrix() {
  // UnescapedEquals 直接矩阵验证
  assert!(unescaped_equals(b"a\\\"b", true, b"a\"b", false));
  assert!(unescaped_equals(b"a\\nb", true, b"a\nb", false));
  assert!(!unescaped_equals(b"a\\tb", true, b"a\\nb", true));
  assert!(!unescaped_equals(b"abc", false, b"ab", false));
  assert_eq!(unescape_byte(b'n'), b'\n');
  assert_eq!(unescape_byte(b'z'), b'z');
}

#[test]
fn stack_push_pop_peek() {
  let mut stack = ExprStack::with_capacity(2);
  assert!(stack.try_push(ExprToken::new_num(1.0)));
  assert!(stack.try_push(ExprToken::new_num(2.0)));
  // 容量满
  assert!(!stack.try_push(ExprToken::new_num(3.0)));
  assert_eq!(stack.count(), 2);
  assert_eq!(stack.peek().num, 2.0);
  assert_eq!(stack.pop().num, 2.0);
  assert_eq!(stack.count(), 1);
  stack.clear();
  assert_eq!(stack.count(), 0);
  assert!(stack.peek().is_none());
}

#[test]
fn truthiness_and_type_coercion() {
  let json = br#"{"n": 0, "s": "3.5", "e": ""}"#;
  // 数字字符串参与算术
  assert!(eval(".s + 0.5 == 4", json));
  // 非空字符串为真值
  assert!(eval(".s and .s", json));
  // 空字符串为假值
  assert!(!eval(".e", json));
  // 0 为假值
  assert!(!eval(".n", json));
}

#[test]
fn nonfinite_numbers_three_paths_agree() {
  // 三径同值：1e999 编译期/提取期/运行期同折 ±Inf 且过滤照常命中；
  // 旧实现编译期误拒使本表达式整条静默全员排除（分 C# 的可观察分叉）
  let json = br#"{"big": 1e999, "neg": -1e999, "r": 7.5, "sb": "1e999", "si": "inf", "sn": "nan"}"#;
  // 提取径属性值 +Inf == 编译径字面量 +Inf
  assert!(eval(".big == 1e999", json));
  assert!(eval(".neg == -1e999", json));
  // C# 契约主场景：`.r <= 1e999` 有限值全员命中
  assert!(eval(".r <= 1e999", json));
  assert!(eval(".big > 1e308", json));
  // 运行期 Str 臂：溢出字符串折 +Inf 原样参与比较（C# ToNum 无有限门，旧实现归 0）
  assert!(eval(".sb == 1e999", json));
  assert!(eval(".sb >= .big", json));
  assert!(!eval(".sb > .big", json));
  // inf/nan 词形：ToNum 词法层拒 → 归 0（与 C# Utf8Parser 失败臂同值）
  assert!(eval(".si == 0", json));
  assert!(eval(".sn == 0", json));
  assert!(!eval(".si == 1e999", json));
}
