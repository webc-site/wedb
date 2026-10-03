//! lua 域错误（错误集中定义；文案即 Lua 层错误串）。

use std::{error, fmt, result};

/// lua 执行/装载错误：携带可直接回灌给脚本或 RESP 的错误文本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
  /// 运行/编译错误消息（lua_pcall 错误对象文本、装载失败原因等）。
  Runtime(String),
  /// 程序性 misuse：非法下标/空栈/名字含 NUL 等宿主侧误用。
  Misuse(&'static str),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Runtime(msg) => f.write_str(msg),
      Self::Misuse(msg) => write!(f, "lua misuse: {msg}"),
    }
  }
}

impl error::Error for Error {}

pub type Result<T> = result::Result<T, Error>;

/// 构造运行时错误。
pub(crate) fn runtime(msg: impl Into<String>) -> Error {
  Error::Runtime(msg.into())
}
