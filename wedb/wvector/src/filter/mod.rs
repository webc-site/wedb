//! 属性过滤引擎（对标 libs/server/Resp/Vector/ExprCompiler.cs 等）

pub mod attribute_extractor;
pub mod compiler;
pub mod expression;
pub mod runner;

pub use compiler::{CompileError, MAX_SELECTORS, try_compile};
pub use expression::{ExprProgram, ExprToken, ExprTokenType, OpCode};
pub use runner::{ExprStack, default_stack, run};

/// 按绝对偏移切片（防越界辅助）。
#[inline]
pub fn slice_at(buf: &[u8], start: i32, len: i32) -> &[u8] {
  if start < 0 || len <= 0 {
    return &[];
  }
  let start = start as usize;
  let len = len as usize;
  buf
    .get(start..)
    .map_or(&[] as &[u8], |tail| &tail[..len.min(tail.len())])
}

/// 编译期（ExprCompiler.cs:ParseNumber）、属性提取期（AttributeExtractor.cs:ParseNumberToken）
/// 与运行期（ExprRunner.cs:ToNum）三处共用的数字解析。
///
/// 对齐 C# `Utf8Parser.TryParse(double)` + `consumed == length` 全量校验语义：
/// - 词法只认十进制语法：可选符号（`+`/`-`）、整数/小数部分（至少一位数字）、
///   可选指数臂（`e`/`E` + 可选符号 + 至少一位数字），并要求全量消费；
/// - `inf`/`nan`/`Infinity` 等词形在词法层直接拒绝（.NET Utf8Parser 不认这些词形）；
/// - 溢出按 IEEE 折 ±Inf 并返回成功（对齐 dotnet/runtime Number.Parsing.cs 的
///   `Scale > MaxDecimalExponent` 臂：`1e999` 解析为 +Inf 而非失败）。
///
/// 不设值域有限性门：有限性职责由词法承担，与 C# 三处口径一致。
#[inline]
pub(crate) fn parse_f64_exact(span: &[u8]) -> Option<f64> {
  let mut i = 0usize;
  // 可选符号
  if matches!(span.first(), Some(&b'+') | Some(&b'-')) {
    i = 1;
  }
  let mut has_digit = false;
  while i < span.len() && span[i].is_ascii_digit() {
    i += 1;
    has_digit = true;
  }
  // 小数点：前后至少一侧有数字（`5.`/`.5` 合法，`.` 非法）
  if i < span.len() && span[i] == b'.' {
    i += 1;
    while i < span.len() && span[i].is_ascii_digit() {
      i += 1;
      has_digit = true;
    }
  }
  if !has_digit {
    return None;
  }
  // 可选指数臂：指数至少一位数字
  if i < span.len() && matches!(span[i], b'e' | b'E') {
    i += 1;
    if i < span.len() && matches!(span[i], b'+' | b'-') {
      i += 1;
    }
    if i >= span.len() || !span[i].is_ascii_digit() {
      return None;
    }
    while i < span.len() && span[i].is_ascii_digit() {
      i += 1;
    }
  }
  // consumed == length 全量校验
  if i != span.len() {
    return None;
  }
  str::from_utf8(span).ok()?.parse::<f64>().ok()
}
