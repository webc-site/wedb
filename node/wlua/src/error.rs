use std::result;

use thiserror::Error;

/// lua 域错误（依赖库错误透明转发）。
#[derive(Error, Debug)]
pub enum Error {
  /// mlua 错误。
  #[error(transparent)]
  Mlua(#[from] mlua::Error),
}

pub type Result<T> = result::Result<T, Error>;
