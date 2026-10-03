//! 过滤表达式求值虚拟机（对标 diskann-garnet/ExprRunner.cs）
//!
//! 以栈式解释器执行编译后的 [`ExprProgram`]，针对原始 JSON 属性字节求值。
//! 全部字符串比较工作在原始 UTF-8 字节切片上 —— 零字符串分配。
//! 词元引用两类源缓冲：
//! - `filter_bytes`：编译期字符串字面量与选择器名
//! - `json`：运行期提取的字符串值

use super::{
  compiler::STACK_CAPACITY,
  expression::{ExprProgram, ExprToken, ExprTokenType, OpCode, get_arity},
  parse_f64_exact, slice_at,
};

/// 调用方提供缓冲之上的轻量栈（C# 为 stackalloc Span 承载的 ref struct）。
#[derive(Debug)]
pub struct ExprStack {
  buffer: Vec<ExprToken>,
}

impl ExprStack {
  /// 以给定容量构造。
  pub fn with_capacity(cap: usize) -> Self {
    Self {
      buffer: Vec::with_capacity(cap),
    }
  }

  /// 当前元素数。
  pub fn count(&self) -> usize {
    self.buffer.len()
  }

  /// libs/server/Resp/Vector/ExprRunner.cs:TryPush
  ///
  /// libs/server/Resp/Vector/ExprRunner.cs:Push 合并承接：C# 无容量检查的
  /// 裸 Push 在 rust 无安全对位，统一收敛为容量判定版 try_push（满栈返回
  /// false 由调用方处置，语义覆盖 C# Push 的「缓冲必够」前提）。
  #[inline]
  pub fn try_push(&mut self, t: ExprToken) -> bool {
    if self.buffer.len() >= self.buffer.capacity() {
      return false;
    }
    self.buffer.push(t);
    true
  }

  /// libs/server/Resp/Vector/ExprRunner.cs:Pop
  #[inline]
  pub fn pop(&mut self) -> ExprToken {
    self.buffer.pop().unwrap_or_default()
  }

  /// libs/server/Resp/Vector/ExprRunner.cs:Peek
  #[inline]
  pub fn peek(&self) -> ExprToken {
    self.buffer.last().copied().unwrap_or_default()
  }

  /// libs/server/Resp/Vector/ExprRunner.cs:Clear
  ///
  /// 清空栈。
  pub fn clear(&mut self) {
    self.buffer.clear();
  }
}

/// libs/server/Resp/Vector/ExprRunner.cs:Run
///
/// 使用预提取字段值执行编译后的程序；选择器按字节区间匹配
/// `selector_ranges` 从 `extracted_fields` 取值。
/// 返回栈顶真值（真 = 候选通过过滤）。
pub fn run(
  program: &ExprProgram,
  json: &[u8],
  filter_bytes: &[u8],
  selector_ranges: &[(i32, i32)],
  extracted_fields: &[ExprToken],
  stack: &mut ExprStack,
) -> bool {
  stack.clear();

  for inst in &program.instructions {
    if inst.token_type == ExprTokenType::Selector {
      let selector_name = slice_at(filter_bytes, inst.utf8_start, inst.utf8_length);
      let mut found = false;
      for (j, range) in selector_ranges.iter().enumerate() {
        if *range == (inst.utf8_start, inst.utf8_length)
          || selector_name == slice_at(filter_bytes, range.0, range.1)
        {
          let value = extracted_fields.get(j).copied().unwrap_or_default();
          if value.is_none() || !stack.try_push(value) {
            stack.clear();
            return false;
          }
          found = true;
          break;
        }
      }
      if !found {
        stack.clear();
        return false;
      }
      continue;
    }

    if !execute_instruction(*inst, program, filter_bytes, json, stack) {
      return false;
    }
  }

  let return_value = stack.count() > 0 && to_bool(stack.peek()) != 0.0;

  stack.clear();
  return_value
}

/// libs/server/Resp/Vector/ExprRunner.cs:ExecuteInstruction
///
/// 单条指令执行：值词元入栈，操作符弹栈计算后回推。
fn execute_instruction(
  inst: ExprToken,
  program: &ExprProgram,
  filter_bytes: &[u8],
  json: &[u8],
  stack: &mut ExprStack,
) -> bool {
  if inst.token_type != ExprTokenType::Op {
    if !stack.try_push(inst) {
      stack.clear();
      return false;
    }
    return true;
  }

  let arity = get_arity(inst.op_code) as usize;
  if stack.count() < arity {
    stack.clear();
    return false;
  }

  let b = stack.pop();
  let a = if arity == 2 {
    stack.pop()
  } else {
    ExprToken::default()
  };

  let result = match inst.op_code {
    OpCode::Not => ExprToken::new_num(f64::from(to_bool(b) == 0.0)),
    OpCode::Pow => {
      ExprToken::new_num(to_num(a, filter_bytes, json).powf(to_num(b, filter_bytes, json)))
    }
    OpCode::Mul => {
      ExprToken::new_num(to_num(a, filter_bytes, json) * to_num(b, filter_bytes, json))
    }
    OpCode::Div => {
      ExprToken::new_num(to_num(a, filter_bytes, json) / to_num(b, filter_bytes, json))
    }
    OpCode::Mod => {
      ExprToken::new_num(to_num(a, filter_bytes, json) % to_num(b, filter_bytes, json))
    }
    OpCode::Add => {
      ExprToken::new_num(to_num(a, filter_bytes, json) + to_num(b, filter_bytes, json))
    }
    OpCode::Sub => {
      ExprToken::new_num(to_num(a, filter_bytes, json) - to_num(b, filter_bytes, json))
    }
    OpCode::Gt => ExprToken::new_num(f64::from(
      to_num(a, filter_bytes, json) > to_num(b, filter_bytes, json),
    )),
    OpCode::Gte => ExprToken::new_num(f64::from(
      to_num(a, filter_bytes, json) >= to_num(b, filter_bytes, json),
    )),
    OpCode::Lt => ExprToken::new_num(f64::from(
      to_num(a, filter_bytes, json) < to_num(b, filter_bytes, json),
    )),
    OpCode::Lte => ExprToken::new_num(f64::from(
      to_num(a, filter_bytes, json) <= to_num(b, filter_bytes, json),
    )),
    OpCode::Eq => ExprToken::new_num(f64::from(are_equal(a, b, filter_bytes, json))),
    OpCode::Neq => ExprToken::new_num(f64::from(!are_equal(a, b, filter_bytes, json))),
    OpCode::In => ExprToken::new_num(f64::from(eval_in(
      a,
      b,
      &program.tuple_pool,
      &program.runtime_pool,
      filter_bytes,
      json,
    ))),
    OpCode::And => ExprToken::new_num(f64::from(to_bool(a) != 0.0 && to_bool(b) != 0.0)),
    OpCode::Or => ExprToken::new_num(f64::from(to_bool(a) != 0.0 || to_bool(b) != 0.0)),
    _ => ExprToken::new_num(0.0),
  };

  if !stack.try_push(result) {
    stack.clear();
    return false;
  }
  true
}

// ======================== 类型转换辅助 ========================

/// libs/server/Resp/Vector/ExprRunner.cs:GetStrSpan
///
/// 解析 Str 词元的 UTF-8 字节区间。
/// 编译器产出的词元（is_filter_origin）引用 filter_bytes；
/// 提取器产出的引用 json。
fn get_str_span<'a>(t: &ExprToken, filter_bytes: &'a [u8], json: &'a [u8]) -> &'a [u8] {
  let (buf, start, len) = if t.is_filter_origin() {
    (filter_bytes, t.utf8_start, t.utf8_length)
  } else {
    (json, t.utf8_start, t.utf8_length)
  };
  slice_at(buf, start, len)
}

/// libs/server/Resp/Vector/ExprRunner.cs:ToNum
fn to_num(t: ExprToken, filter_bytes: &[u8], json: &[u8]) -> f64 {
  if t.is_none() {
    return 0.0;
  }
  match t.token_type {
    ExprTokenType::Num => t.num,
    ExprTokenType::Str => {
      // 对齐 C#：Utf8Parser 词法 + consumed 全量校验，解析成功原样返回值
      // （含溢出 ±Inf，无有限性门），失败归 0；inf/nan 词形由词法层拒绝。
      let slice = get_str_span(&t, filter_bytes, json);
      parse_f64_exact(slice).unwrap_or(0.0)
    }
    _ => 0.0,
  }
}

/// libs/server/Resp/Vector/ExprRunner.cs:ToBool
fn to_bool(t: ExprToken) -> f64 {
  if t.is_none() {
    return 0.0;
  }
  match t.token_type {
    ExprTokenType::Num => f64::from(t.num != 0.0),
    ExprTokenType::Str => f64::from(t.utf8_length != 0),
    ExprTokenType::Null => 0.0,
    _ => 1.0,
  }
}

/// libs/server/Resp/Vector/ExprRunner.cs:AreEqual
fn are_equal(a: ExprToken, b: ExprToken, filter_bytes: &[u8], json: &[u8]) -> bool {
  if a.is_none() || b.is_none() {
    return a.is_none() && b.is_none();
  }

  if a.token_type == ExprTokenType::Str && b.token_type == ExprTokenType::Str {
    let a_span = get_str_span(&a, filter_bytes, json);
    let b_span = get_str_span(&b, filter_bytes, json);
    // 任一含转义序列时需要转义感知比较
    if !a.has_escape() && !b.has_escape() {
      return a_span == b_span;
    }
    return unescaped_equals(a_span, a.has_escape(), b_span, b.has_escape());
  }

  if a.token_type == ExprTokenType::Num && b.token_type == ExprTokenType::Num {
    return a.num == b.num;
  }

  if a.token_type == ExprTokenType::Null || b.token_type == ExprTokenType::Null {
    return a.token_type == b.token_type;
  }

  to_num(a, filter_bytes, json) == to_num(b, filter_bytes, json)
}

/// libs/server/Resp/Vector/ExprRunner.cs:EvalIn
fn eval_in(
  a: ExprToken,
  b: ExprToken,
  tuple_pool: &[ExprToken],
  runtime_pool: &[ExprToken],
  filter_bytes: &[u8],
  json: &[u8],
) -> bool {
  if b.is_none() {
    return false;
  }

  // 元组成员判断
  if b.token_type == ExprTokenType::Tuple {
    let pool_start = b.utf8_start as usize;
    let pool_len = b.utf8_length as usize;
    let pool = if b.is_runtime_tuple() {
      runtime_pool
    } else {
      tuple_pool
    };
    return (0..pool_len).any(|i| {
      pool
        .get(pool_start + i)
        .is_some_and(|elem| are_equal(a, *elem, filter_bytes, json))
    });
  }

  // 字符串子串判断
  if !a.is_none() && a.token_type == ExprTokenType::Str && b.token_type == ExprTokenType::Str {
    let needle = get_str_span(&a, filter_bytes, json);
    let haystack = get_str_span(&b, filter_bytes, json);
    if needle.is_empty() {
      return true;
    }
    if needle.len() > haystack.len() {
      return false;
    }
    // 含转义时为近似处理 —— 完整正确需反转义后搜索
    return index_of(haystack, needle).is_some();
  }

  false
}

/// 子串搜索（返回起始下标，对齐 C# Span.IndexOf）。
fn index_of(haystack: &[u8], needle: &[u8]) -> Option<usize> {
  match needle.len() {
    0 => Some(0),
    1 => haystack.iter().position(|&b| b == needle[0]),
    n => haystack.windows(n).position(|w| w == needle),
  }
}

// ======================== 转义感知比较 ========================

/// libs/server/Resp/Vector/ExprRunner.cs:UnescapedEquals
///
/// 比较两个 UTF-8 字节区间，按需在线反转义 JSON 转义序列，不分配内存。
pub fn unescaped_equals(a: &[u8], a_escaped: bool, b: &[u8], b_escaped: bool) -> bool {
  let (mut ai, mut bi) = (0usize, 0usize);
  while ai < a.len() && bi < b.len() {
    let ac = if a_escaped && ai < a.len() - 1 && a[ai] == b'\\' {
      ai += 1;
      unescape_byte(a[ai])
    } else {
      a[ai]
    };

    let bc = if b_escaped && bi < b.len() - 1 && b[bi] == b'\\' {
      bi += 1;
      unescape_byte(b[bi])
    } else {
      b[bi]
    };

    if ac != bc {
      return false;
    }
    ai += 1;
    bi += 1;
  }

  // 两侧必须同时耗尽
  ai == a.len() && bi == b.len()
}

/// libs/server/Resp/Vector/ExprRunner.cs:UnescapeByte
#[inline]
pub const fn unescape_byte(b: u8) -> u8 {
  match b {
    b'n' => b'\n',
    b'r' => b'\r',
    b't' => b'\t',
    b'\\' => b'\\',
    b'"' => b'"',
    b'\'' => b'\'',
    b'/' => b'/',
    _ => b,
  }
}

/// 缺省栈容量（对齐 C# stackalloc `ExprToken[StackCapacity]`）。
pub fn default_stack() -> ExprStack {
  ExprStack::with_capacity(STACK_CAPACITY)
}
