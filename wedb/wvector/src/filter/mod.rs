//! 属性过滤引擎（对标 libs/server/Resp/Vector/ExprCompiler.cs 等）

pub mod attribute_extractor;
pub mod compiler;
pub mod expression;
pub mod runner;

pub use compiler::{CompileError, MAX_INSTRUCTIONS, MAX_SELECTORS, try_compile};
pub use expression::{ExprProgram, ExprToken, ExprTokenType, OpCode};
pub use runner::{ExprStack, default_stack, run};
