//! 属性过滤引擎（对标 libs/server/Resp/Vector/ExprCompiler.cs 等）

pub mod attribute_extractor;
pub mod compiler;
pub mod expression;
pub mod runner;

pub use compiler::{CompileError, MAX_INSTRUCTIONS, MAX_SELECTORS, try_compile};
pub use expression::{ExprProgram, ExprToken, ExprTokenType, OpCode};
pub use runner::{ExprStack, default_stack, run};

/// 按绝对偏移切片（防越界辅助）。
#[inline]
pub fn slice_at(buf: &[u8], start: i32, len: i32) -> &[u8] {
  let start = start.max(0) as usize;
  let len = len.max(0) as usize;
  buf
    .get(start..)
    .map_or(&[] as &[u8], |tail| &tail[..len.min(tail.len())])
}
