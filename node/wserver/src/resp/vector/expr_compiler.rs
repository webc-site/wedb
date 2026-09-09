//! Shunting-Yard 过滤表达式编译器（对标 libs/server/Resp/Vector/ExprCompiler.cs）
//!
//! 将过滤表达式（UTF-8 字节）词法化并编译为扁平后缀 [`ExprProgram`]。
//! 全部字符串/选择器词元以 (偏移， 长度) 引用原始过滤表达式字节 —— 零字符串分配；
//! 元组元素存入程序级扁平池。

use super::{
  attribute_extractor::{is_digit, is_letter, is_letter_or_digit, trim_white_space},
  vector_filter_expression::{
    ExprProgram, ExprToken, ExprTokenType, OpCode, get_arity, get_precedence,
  },
};

/// 后缀指令上限（溢出 → 编译错误）。128 约支撑 18 组 AND/OR 子句。
pub const MAX_INSTRUCTIONS: usize = 128;
/// 编译期元组池上限（所有 IN [...] 字面量元素合计）。
pub const MAX_TUPLE_POOL: usize = 64;
/// 运行期元组池上限（JSON 数组提取；溢出 → 数组按 Null 降级）。
pub const MAX_RUNTIME_POOL: usize = 64;
/// 唯一字段选择器上限（溢出 → 多余选择器被静默忽略）。
pub const MAX_SELECTORS: usize = 32;
/// 求值栈深度上限（溢出 → TryPush 失败 → 候选被排除）。
pub const STACK_CAPACITY: usize = 16;

/// 编译错误：携带表达式内的错误字节偏移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompileError {
  /// 出错位置（字节偏移；结构性错误统一为 0，对齐 C# errpos=0）。
  pub errpos: usize,
}

/// 以当前位置构造编译错误。
#[inline]
fn err_at(pos: usize) -> CompileError {
  CompileError { errpos: pos }
}

/// libs/server/Resp/Vector/ExprCompiler.cs:TryCompile
///
/// 成功返回编译好的后缀程序；失败返回 [`CompileError`]（对齐 C# 返回 -1 + errpos）。
pub fn try_compile(expr: &[u8]) -> Result<ExprProgram, CompileError> {
  if expr.is_empty() {
    return Err(CompileError { errpos: 0 });
  }

  // 阶段 1：词法化为扁平词元序列
  let mut tokens: Vec<ExprToken> = Vec::with_capacity(16);
  let mut tuple_pool: Vec<ExprToken> = Vec::new();

  let mut pos = 0;
  while pos < expr.len() {
    trim_white_space(expr, &mut pos);
    if pos >= expr.len() {
      break;
    }

    // 判定 '-' 是负号还是减法：上一词元缺失或为操作符（右括号除外）时视为负号
    let minus_is_number = expr[pos] == b'-'
      && pos + 1 < expr.len()
      && (is_digit(expr[pos + 1]) || expr[pos + 1] == b'.')
      && tokens
        .last()
        .is_none_or(|prev| prev.token_type == ExprTokenType::Op && prev.op_code != OpCode::CParen);

    // 数字
    if is_digit(expr[pos]) || (minus_is_number && expr[pos] == b'-') {
      let t = parse_number(expr, &mut pos).ok_or_else(|| err_at(pos))?;
      push_token(&mut tokens, t, || err_at(pos))?;
      continue;
    }

    // 字符串字面量 —— (偏移， 长度) 引用过滤字节
    if expr[pos] == b'"' || expr[pos] == b'\'' {
      let t = parse_string(expr, &mut pos).ok_or_else(|| err_at(pos))?;
      push_token(&mut tokens, t, || err_at(pos))?;
      continue;
    }

    // 选择器（'.' 开头的字段访问）
    if expr[pos] == b'.' && pos + 1 < expr.len() && is_selector_char(expr[pos + 1]) {
      let t = parse_selector(expr, &mut pos);
      push_token(&mut tokens, t, || err_at(pos))?;
      continue;
    }

    // 元组字面量 [1, "foo", 42]
    if expr[pos] == b'[' {
      let t = parse_tuple(expr, &mut tuple_pool, &mut pos).ok_or_else(|| err_at(pos))?;
      push_token(&mut tokens, t, || err_at(pos))?;
      continue;
    }

    // 操作符或字面量关键字（null / true / false / not / and / or / in）
    if is_letter(expr[pos]) || is_operator_special_char(expr[pos]) {
      let t = parse_operator_or_literal(expr, &mut pos).ok_or_else(|| err_at(pos))?;
      push_token(&mut tokens, t, || err_at(pos))?;
      continue;
    }

    return Err(err_at(pos));
  }

  // 阶段 2：Shunting-Yard 编译为后缀
  let mut ops_stack: Vec<ExprToken> = Vec::new();
  let mut instructions: Vec<ExprToken> = Vec::new();
  let mut stack_items = 0usize;

  for token in &tokens {
    match token.token_type {
      ExprTokenType::Num
      | ExprTokenType::Str
      | ExprTokenType::Tuple
      | ExprTokenType::Selector
      | ExprTokenType::Null => {
        ensure_instr_capacity(&instructions)?;
        instructions.push(*token);
        stack_items += 1;
      }
      ExprTokenType::Op => {
        process_operator(*token, &mut instructions, &mut ops_stack, &mut stack_items)?;
      }
      ExprTokenType::None => {}
    }
  }

  while let Some(op) = ops_stack.pop() {
    if op.op_code == OpCode::OParen {
      return Err(CompileError { errpos: 0 });
    }
    let arity = get_arity(op.op_code) as usize;
    if stack_items < arity {
      return Err(CompileError { errpos: 0 });
    }
    ensure_instr_capacity(&instructions)?;
    instructions.push(op);
    stack_items = stack_items - arity + 1;
  }

  if stack_items != 1 {
    return Err(CompileError { errpos: 0 });
  }

  Ok(ExprProgram {
    instructions,
    tuple_pool,
    runtime_pool: Vec::with_capacity(MAX_RUNTIME_POOL),
    runtime_pool_len: 0,
  })
}

/// 词元入带容量约束的缓冲。
fn push_token(
  tokens: &mut Vec<ExprToken>,
  token: ExprToken,
  err: impl FnOnce() -> CompileError,
) -> Result<(), CompileError> {
  if tokens.len() >= MAX_INSTRUCTIONS {
    return Err(err());
  }
  tokens.push(token);
  Ok(())
}

/// 指令缓冲容量检查（对齐 C# instrBuf.Length 上限）。
fn ensure_instr_capacity(instructions: &[ExprToken]) -> Result<(), CompileError> {
  if instructions.len() >= MAX_INSTRUCTIONS {
    return Err(CompileError { errpos: 0 });
  }
  Ok(())
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ProcessOperator
fn process_operator(
  op: ExprToken,
  instructions: &mut Vec<ExprToken>,
  ops_stack: &mut Vec<ExprToken>,
  stack_items: &mut usize,
) -> Result<(), CompileError> {
  if op.op_code == OpCode::OParen {
    if ops_stack.len() >= MAX_INSTRUCTIONS {
      return Err(CompileError { errpos: 0 });
    }
    ops_stack.push(op);
    return Ok(());
  }

  if op.op_code == OpCode::CParen {
    while let Some(top_op) = ops_stack.pop() {
      if top_op.op_code == OpCode::OParen {
        return Ok(());
      }
      let arity = get_arity(top_op.op_code) as usize;
      if *stack_items < arity {
        return Err(CompileError { errpos: 0 });
      }
      ensure_instr_capacity(instructions)?;
      instructions.push(top_op);
      *stack_items = *stack_items - arity + 1;
    }
    // 栈空仍未遇 '('
    return Err(CompileError { errpos: 0 });
  }

  let cur_prec = get_precedence(op.op_code);

  while let Some(top_op) = ops_stack.last().copied() {
    if top_op.op_code == OpCode::OParen {
      break;
    }
    let top_prec = get_precedence(top_op.op_code);
    if top_prec < cur_prec {
      break;
    }
    // Pow 右结合：同级不弹出
    if op.op_code == OpCode::Pow && top_prec <= cur_prec {
      break;
    }
    ops_stack.pop();
    let arity = get_arity(top_op.op_code) as usize;
    if *stack_items < arity {
      return Err(CompileError { errpos: 0 });
    }
    ensure_instr_capacity(instructions)?;
    instructions.push(top_op);
    *stack_items = *stack_items - arity + 1;
  }

  if ops_stack.len() >= MAX_INSTRUCTIONS {
    return Err(CompileError { errpos: 0 });
  }
  ops_stack.push(op);
  Ok(())
}

/// libs/server/Resp/Vector/ExprCompiler.cs:IsOperatorSpecialChar
fn is_operator_special_char(b: u8) -> bool {
  matches!(
    b,
    b'+' | b'-' | b'*' | b'%' | b'/' | b'!' | b'(' | b')' | b'<' | b'>' | b'=' | b'|' | b'&'
  )
}

/// libs/server/Resp/Vector/ExprCompiler.cs:IsSelectorChar
fn is_selector_char(c: u8) -> bool {
  is_letter_or_digit(c) || c == b'_' || c == b'-'
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ParseNumber
///
/// 接受 `[0-9.eE]` 与可选前导负号；要求全量可解析为有限 f64。
fn parse_number(expr: &[u8], pos: &mut usize) -> Option<ExprToken> {
  let start = *pos;
  if expr.get(*pos) == Some(&b'-') {
    *pos += 1;
  }
  while *pos < expr.len() && (is_digit(expr[*pos]) || matches!(expr[*pos], b'.' | b'e' | b'E')) {
    *pos += 1;
  }
  let span = &expr[start..*pos];
  let text = str::from_utf8(span).ok()?;
  let value = text.parse::<f64>().ok().filter(|v| v.is_finite())?;
  Some(ExprToken::new_num(value))
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ParseString
///
/// 字面量以 (偏移， 长度) 指向原始过滤表达式字节（不含引号），零分配。
/// `pos` 指向开引号。
fn parse_string(expr: &[u8], pos: &mut usize) -> Option<ExprToken> {
  let quote = expr[*pos];
  *pos += 1;
  let content_start = *pos;
  let mut has_escape = false;

  while *pos < expr.len() {
    match expr[*pos] {
      b'\\' if *pos + 1 < expr.len() => {
        has_escape = true;
        *pos += 2;
      }
      c if c == quote => {
        let token = ExprToken::new_filter_str(
          content_start as i32,
          (*pos - content_start) as i32,
          has_escape,
        );
        *pos += 1;
        return Some(token);
      }
      _ => *pos += 1,
    }
  }
  None // 未闭合字符串
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ParseSelector
///
/// `.fieldName` → Selector 词元（引用过滤字节，跳过前导点）。
fn parse_selector(expr: &[u8], pos: &mut usize) -> ExprToken {
  *pos += 1; // 跳过 '.'
  let start = *pos;
  while *pos < expr.len() && is_selector_char(expr[*pos]) {
    *pos += 1;
  }
  ExprToken::new_selector(start as i32, (*pos - start) as i32)
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ParseTuple
///
/// 元素存入元组池，词元记录 (池内起始下标, 个数)。
fn parse_tuple(expr: &[u8], tuple_pool: &mut Vec<ExprToken>, pos: &mut usize) -> Option<ExprToken> {
  *pos += 1; // 跳过 '['
  trim_white_space(expr, pos);

  // 空元组 []
  if expr.get(*pos) == Some(&b']') {
    *pos += 1;
    return Some(ExprToken::new_tuple(0, 0));
  }

  let pool_start = tuple_pool.len();
  let mut count = 0i32;

  loop {
    trim_white_space(expr, pos);
    if *pos >= expr.len() || tuple_pool.len() >= MAX_TUPLE_POOL {
      return None;
    }

    let ele = match expr[*pos] {
      c if is_digit(c) || c == b'-' => parse_number(expr, pos),
      b'"' | b'\'' => parse_string(expr, pos),
      _ => return None,
    }?;

    tuple_pool.push(ele);
    count += 1;

    trim_white_space(expr, pos);
    match expr.get(*pos) {
      None => return None,
      Some(b']') => {
        *pos += 1;
        break;
      }
      Some(b',') => *pos += 1,
      _ => return None,
    }
  }

  Some(ExprToken::new_tuple(pool_start as i32, count))
}

/// libs/server/Resp/Vector/ExprCompiler.cs:ParseOperatorOrLiteral
///
/// 贪心匹配：优先最长操作符；`null` / `true` / `false` 为字面量。
fn parse_operator_or_literal(expr: &[u8], pos: &mut usize) -> Option<ExprToken> {
  let start = *pos;
  while *pos < expr.len() && (is_letter(expr[*pos]) || is_operator_special_char(expr[*pos])) {
    *pos += 1;
  }
  let consumed = &expr[start..*pos];
  if consumed.is_empty() {
    return None;
  }

  if consumed == b"null" {
    return Some(ExprToken::new_null());
  }
  if consumed == b"true" {
    return Some(ExprToken::new_num(1.0));
  }
  if consumed == b"false" {
    return Some(ExprToken::new_num(0.0));
  }

  // 最长匹配操作符（多字节优先），对齐 C# TryMatchOp 序列
  static MATCHES: &[(&[u8], OpCode)] = &[
    (b"||", OpCode::Or),
    (b"or", OpCode::Or),
    (b"&&", OpCode::And),
    (b"and", OpCode::And),
    (b"**", OpCode::Pow),
    (b">=", OpCode::Gte),
    (b"<=", OpCode::Lte),
    (b"==", OpCode::Eq),
    (b"!=", OpCode::Neq),
    (b"not", OpCode::Not),
    (b"in", OpCode::In),
    (b"(", OpCode::OParen),
    (b")", OpCode::CParen),
    (b"+", OpCode::Add),
    (b"-", OpCode::Sub),
    (b"*", OpCode::Mul),
    (b"/", OpCode::Div),
    (b"%", OpCode::Mod),
    (b">", OpCode::Gt),
    (b"<", OpCode::Lt),
    (b"!", OpCode::Not),
  ];

  let mut best: Option<(OpCode, usize)> = None;
  for (op_name, op_code) in MATCHES {
    if consumed.len() >= op_name.len()
      && &consumed[..op_name.len()] == *op_name
      && best.is_none_or(|(_, len)| op_name.len() > len)
    {
      best = Some((*op_code, op_name.len()));
    }
  }

  let (op_code, best_len) = best?;
  *pos = start + best_len;
  Some(ExprToken::new_op(op_code))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn compile_ok(expr: &str) -> ExprProgram {
    try_compile(expr.as_bytes()).unwrap_or_else(|e| panic!("{expr}: {e:?}"))
  }

  #[test]
  fn compiles_postfix() {
    // .year >= 2000 and .rating > 7 → 后缀：SEL(year) NUM SEL(rating) NUM Gt And Gte
    let p = compile_ok(".year >= 2000 and .rating > 7");
    let instr = &p.instructions;
    assert_eq!(instr[0], ExprToken::new_selector(1, 4));
    assert_eq!(instr[1], ExprToken::new_num(2000.0));
    assert_eq!(instr[2], ExprToken::new_op(OpCode::Gte));
    assert_eq!(instr[3], ExprToken::new_selector(19, 6));
    assert_eq!(instr[4], ExprToken::new_num(7.0));
    assert_eq!(instr[5], ExprToken::new_op(OpCode::Gt));
    assert_eq!(instr[6], ExprToken::new_op(OpCode::And));
    assert_eq!(instr.len(), 7);
  }

  #[test]
  fn precedence_and_parens() {
    // 1 + 2 * 3 → 1 2 3 * +
    let p = compile_ok("1 + 2 * 3");
    let ops: Vec<_> = p
      .instructions
      .iter()
      .filter(|t| t.token_type == ExprTokenType::Op)
      .collect();
    assert_eq!(
      ops,
      [
        &ExprToken::new_op(OpCode::Mul),
        &ExprToken::new_op(OpCode::Add)
      ]
    );

    // 括号覆盖：(1 + 2) * 3 → 1 2 + 3 *
    let p = compile_ok("(1 + 2) * 3");
    let ops: Vec<_> = p
      .instructions
      .iter()
      .filter(|t| t.token_type == ExprTokenType::Op)
      .collect();
    assert_eq!(
      ops,
      [
        &ExprToken::new_op(OpCode::Add),
        &ExprToken::new_op(OpCode::Mul)
      ]
    );

    // 幂右结合：同级不弹出
    let p = compile_ok("2 ** 3 ** 2");
    let pw: Vec<_> = p
      .instructions
      .iter()
      .filter(|t| t.op_code == OpCode::Pow)
      .collect();
    assert_eq!(pw.len(), 2);
  }

  #[test]
  fn literals_and_tuples() {
    let p = compile_ok("not null and true and .x in [1, \"two\", -3]");
    assert!(p.instructions.iter().any(|t| *t == ExprToken::new_null()));
    let tup = p
      .instructions
      .iter()
      .find(|t| t.token_type == ExprTokenType::Tuple)
      .unwrap();
    assert_eq!(tup.utf8_length, 3);
    assert_eq!(p.tuple_pool[0], ExprToken::new_num(1.0));
    assert!(p.tuple_pool[1].is_filter_origin());
    assert_eq!(p.tuple_pool[2], ExprToken::new_num(-3.0));

    // 空元组
    let p = compile_ok("null in []");
    assert_eq!(p.tuple_pool.len(), 0);
  }

  #[test]
  fn negative_number_disambiguation() {
    // 首个词元 → 负号
    let p = compile_ok("-5 < .x");
    assert_eq!(p.instructions[0], ExprToken::new_num(-5.0));
    // 前一词元为操作符 → 负号
    let p = compile_ok("1 + -2 > 0");
    assert_eq!(p.instructions[1], ExprToken::new_num(-2.0));
    // 前一为操作数 → 减法
    let p = compile_ok("5 - 2 > 0");
    assert!(p.instructions.iter().any(|t| t.op_code == OpCode::Sub));
  }

  #[test]
  fn string_literals_with_escapes() {
    let p = compile_ok(r#".name == "a\"b""#);
    let s = p
      .instructions
      .iter()
      .find(|t| t.token_type == ExprTokenType::Str)
      .unwrap();
    assert!(s.has_escape());
    assert!(s.is_filter_origin());
    assert_eq!(s.utf8_length, 4); // a\"b 含转义序列的原始字节
  }

  #[test]
  fn compile_errors() {
    // 空表达式
    assert!(try_compile(b"").is_err());
    // 未闭合括号
    assert!(try_compile(b"(1 + 2").is_err());
    // 悬空右括号
    assert!(try_compile(b"1 + 2)").is_err());
    // 未闭合字符串
    assert!(try_compile(b".x == \"oops").is_err());
    // 操作数不足
    assert!(try_compile(b"1 +").is_err());
    assert!(try_compile(b"> 5").is_err());
    // 非法字符
    assert!(try_compile(b".x ~ 5").is_err());
  }
}
