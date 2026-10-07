#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 过滤表达式词元与操作符表测试（自 src/filter/expression.rs 外迁）

use wvector::filter::expression::{ExprToken, OP_TABLE, OpCode, get_arity, get_precedence};

#[test]
fn op_table_matches_csharp() {
  // 优先级/元数逐项对齐 C# OpTable
  assert_eq!(get_precedence(OpCode::Or), 0);
  assert_eq!(get_precedence(OpCode::And), 1);
  assert_eq!(get_precedence(OpCode::Gt), 2);
  assert_eq!(get_precedence(OpCode::Eq), 2);
  assert_eq!(get_precedence(OpCode::Add), 3);
  assert_eq!(get_precedence(OpCode::Mul), 4);
  assert_eq!(get_precedence(OpCode::Pow), 5);
  assert_eq!(get_precedence(OpCode::Not), 6);
  assert_eq!(get_precedence(OpCode::OParen), 7);
  // 行序 == 判别值序（判别值下标查表前提），18 项恰覆盖全操作码
  for (i, &(code, precedence, arity)) in OP_TABLE.iter().enumerate() {
    assert_eq!(code as u8, i as u8);
    assert_eq!(get_precedence(code), precedence);
    let expected = match code {
      OpCode::Not => 1,
      OpCode::OParen | OpCode::CParen => 0,
      _ => 2,
    };
    assert_eq!(arity, expected);
    assert_eq!(get_arity(code), arity);
  }
}

#[test]
fn token_flags_roundtrip() {
  let t = ExprToken::new_filter_str(4, 9, true);
  assert!(t.is_filter_origin());
  assert!(t.has_escape());
  assert!(!t.is_none());

  let t = ExprToken::new_str(0, 3, false);
  assert!(!t.is_filter_origin());
  assert!(!t.has_escape());

  let t = ExprToken::new_runtime_tuple(2, 5);
  assert!(t.is_runtime_tuple());
  assert_eq!((t.utf8_start, t.utf8_length), (2, 5));

  assert!(ExprToken::default().is_none());
  assert!(!ExprToken::new_null().is_none());
}
