//! 过滤表达式虚拟机的词法类型（对标 libs/server/Resp/Vector/VectorFilterExpression.cs）
//!
//! 过滤引擎是栈式后缀虚拟机：形如 `.year >= 2000 and .rating > 7` 的过滤串
//! 经 [`crate::resp::vector::expr_compiler::try_compile`] 编译为后缀指令序列，
//! 再由 [`crate::resp::vector::expr_runner::run`] 左到右解释执行。
//!
//! C# 侧 `ExprToken` 是 16 字节显式布局的 blittable 联合体（Num 与
//! Utf8Start/Utf8Length 复用同一 8 字节负载），Rust 侧以普通 struct 承接
//! 同一字段语义（保持零拷贝：字符串仍是外部缓冲区的 (start, length) 区间引用）。

/// 过滤表达式虚拟机的词元类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExprTokenType {
  #[default]
  None = 0,
  Num = 1,
  Str = 2,
  Tuple = 3,
  Selector = 4,
  Op = 5,
  Null = 6,
}

/// 过滤表达式虚拟机的操作符码。
///
/// 优先级与语义对齐 Redis `expr.c ExprOptable[]`；
/// 编译期经 shunting-yard 按优先级重排为中缀转后缀。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum OpCode {
  // 优先级 0
  #[default]
  Or = 0,
  // 优先级 1
  And = 1,
  // 优先级 2
  Gt = 2,
  Gte = 3,
  Lt = 4,
  Lte = 5,
  Eq = 6,
  Neq = 7,
  In = 8,
  // 优先级 3
  Add = 9,
  Sub = 10,
  // 优先级 4
  Mul = 11,
  Div = 12,
  Mod = 13,
  // 优先级 5
  Pow = 14,
  // 优先级 6
  Not = 15,
  // 优先级 7（标记符，非真实操作符）
  OParen = 16,
  CParen = 17,
}

/// 16 字节联合词元的 Rust 承接：tag + 操作符 + 标志位 + 负载。
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ExprToken {
  /// 判别标签。
  pub token_type: ExprTokenType,
  /// 操作符码（仅 `token_type == Op` 时有效）。
  pub op_code: OpCode,
  /// 标志位字节：bit0 HasEscape / bit1 FilterOrigin / bit2 RuntimeTuple。
  pub flags: u8,
  /// 数值负载（bool 以 1.0/0.0 表达）。
  pub num: f64,
  /// 字符串/选择器在源缓冲中的起始偏移；元组时为池内起始下标。
  pub utf8_start: i32,
  /// 字符串字节长度；元组时为元素个数。
  pub utf8_length: i32,
}

const HAS_ESCAPE_FLAG: u8 = 1;
const FILTER_ORIGIN_FLAG: u8 = 2;
const RUNTIME_TUPLE_FLAG: u8 = 4;

impl ExprToken {
  /// 词元是否为默认（未初始化）值。
  #[inline]
  pub fn is_none(&self) -> bool {
    self.token_type == ExprTokenType::None
  }

  /// 字符串区间是否含反斜杠转义序列。
  #[inline]
  pub fn has_escape(&self) -> bool {
    self.flags & HAS_ESCAPE_FLAG != 0
  }

  /// 区间是否引用过滤表达式字节（编译期字面量与选择器）；
  /// 为假时引用 JSON 字节（运行期提取值）。
  #[inline]
  pub fn is_filter_origin(&self) -> bool {
    self.flags & FILTER_ORIGIN_FLAG != 0
  }

  /// 是否为运行期提取的元组（源自 JSON 数组而非编译期字面量）。
  #[inline]
  pub fn is_runtime_tuple(&self) -> bool {
    self.flags & RUNTIME_TUPLE_FLAG != 0
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewNum
  #[inline]
  pub fn new_num(value: f64) -> Self {
    Self {
      token_type: ExprTokenType::Num,
      num: value,
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewStr
  ///
  /// 引用 JSON 缓冲区原始 UTF-8 字节的零分配字符串词元（不含引号）。
  #[inline]
  pub fn new_str(utf8_start: i32, utf8_length: i32, has_escape: bool) -> Self {
    Self {
      token_type: ExprTokenType::Str,
      utf8_start,
      utf8_length,
      flags: u8::from(has_escape),
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewFilterStr
  ///
  /// 引用过滤表达式缓冲区的字符串字面量；runner 据此按 filter 字节解析。
  #[inline]
  pub fn new_filter_str(utf8_start: i32, utf8_length: i32, has_escape: bool) -> Self {
    Self {
      token_type: ExprTokenType::Str,
      utf8_start,
      utf8_length,
      flags: if has_escape {
        FILTER_ORIGIN_FLAG | HAS_ESCAPE_FLAG
      } else {
        FILTER_ORIGIN_FLAG
      },
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewSelector
  ///
  /// 引用过滤表达式字节中字段名（如 `.year > 2000` 的 `year`）的选择器词元。
  #[inline]
  pub fn new_selector(utf8_start: i32, utf8_length: i32) -> Self {
    Self {
      token_type: ExprTokenType::Selector,
      utf8_start,
      utf8_length,
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewOp
  #[inline]
  pub fn new_op(op_code: OpCode) -> Self {
    Self {
      token_type: ExprTokenType::Op,
      op_code,
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewNull
  #[inline]
  pub fn new_null() -> Self {
    Self {
      token_type: ExprTokenType::Null,
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewTuple
  ///
  /// 索引编译期元组池（`[1, "x", 3]` 字面量）。
  #[inline]
  pub fn new_tuple(pool_start: i32, pool_length: i32) -> Self {
    Self {
      token_type: ExprTokenType::Tuple,
      utf8_start: pool_start,
      utf8_length: pool_length,
      ..Self::default()
    }
  }

  /// libs/server/Resp/Vector/VectorFilterExpression.cs:NewRuntimeTuple
  ///
  /// 索引运行期元组池（JSON 数组提取）。
  #[inline]
  pub fn new_runtime_tuple(pool_start: i32, pool_length: i32) -> Self {
    Self {
      token_type: ExprTokenType::Tuple,
      utf8_start: pool_start,
      utf8_length: pool_length,
      flags: RUNTIME_TUPLE_FLAG,
      ..Self::default()
    }
  }
}

/// 操作符元数据（优先级，元数），镜像 Redis ExprOptable，按下标 O(1) 查表。
const OP_TABLE: [(u8, u8); 18] = [
  (0, 2), // Or
  (1, 2), // And
  (2, 2), // Gt
  (2, 2), // Gte
  (2, 2), // Lt
  (2, 2), // Lte
  (2, 2), // Eq
  (2, 2), // Neq
  (2, 2), // In
  (3, 2), // Add
  (3, 2), // Sub
  (4, 2), // Mul
  (4, 2), // Div
  (4, 2), // Mod
  (5, 2), // Pow
  (6, 1), // Not
  (7, 0), // OParen
  (7, 0), // CParen
];

/// libs/server/Resp/Vector/VectorFilterExpression.cs:GetPrecedence
#[inline]
pub fn get_precedence(code: OpCode) -> u8 {
  OP_TABLE[code as usize].0
}

/// libs/server/Resp/Vector/VectorFilterExpression.cs:GetArity
#[inline]
pub fn get_arity(code: OpCode) -> u8 {
  OP_TABLE[code as usize].1
}

/// 编译后的过滤表达式程序。
///
/// C# 为借用调用方缓冲的 ref struct；Rust 侧以 Owned Vec 承接，
/// 容量上限仍由编译器/求值器的常量约束。
#[derive(Debug, Clone, Default)]
pub struct ExprProgram {
  /// 后缀指令序列。
  pub instructions: Vec<ExprToken>,
  /// 编译期元组元素扁平池；Tuple 词元以 (起始下标, 个数) 索引。
  pub tuple_pool: Vec<ExprToken>,
  /// 运行期元组池（JSON 数组提取），逐候选求值前重置。
  pub runtime_pool: Vec<ExprToken>,
  /// 运行期池当前写入位置。
  pub runtime_pool_len: usize,
}

impl ExprProgram {
  /// libs/server/Resp/Vector/VectorFilterExpression.cs:ResetRuntimePool
  #[inline]
  pub fn reset_runtime_pool(&mut self) {
    self.runtime_pool_len = 0;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
    for (i, _) in OP_TABLE.iter().enumerate() {
      let code = OpCode::try_from(i as u8).unwrap();
      let expected = match code {
        OpCode::Not => 1,
        OpCode::OParen | OpCode::CParen => 0,
        _ => 2,
      };
      assert_eq!(get_arity(code), expected);
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
}
