#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 过滤表达式编译器集成测试（自 src/filter/compiler.rs 内联测试外迁，零私有依赖）

use wvector::filter::{ExprProgram, ExprToken, ExprTokenType, OpCode, try_compile};

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

#[test]
fn overflow_literals_vs_special_wordforms() {
  // 溢出字面量：C# Utf8Parser 语义折 ±Inf 编译成功（旧实现 is_finite 值域门误拒为 CompileError）
  let p = compile_ok(".a <= 1e999");
  assert_eq!(p.instructions[1], ExprToken::new_num(f64::INFINITY));
  let p = compile_ok("-1e999 < .a");
  assert_eq!(p.instructions[0], ExprToken::new_num(f64::NEG_INFINITY));
  // 元组内溢出同谱
  let p = compile_ok(".x in [1e999, -1e999]");
  assert_eq!(p.tuple_pool[0], ExprToken::new_num(f64::INFINITY));
  assert_eq!(p.tuple_pool[1], ExprToken::new_num(f64::NEG_INFINITY));
  // inf/nan/Infinity 词形三径同拒——编译径于词法层拒（非值域层）
  assert!(try_compile(b".a <= inf").is_err());
  assert!(try_compile(b".a == Infinity").is_err());
  assert!(try_compile(b".a == NaN").is_err());
  assert!(try_compile(b".a <= -Infinity").is_err());
}
