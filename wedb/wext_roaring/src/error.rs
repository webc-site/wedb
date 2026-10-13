use std::{io, result};

use thiserror::Error;

/// 命令面错误：仅序列化/反序列化底层 I/O 或格式错误（roaring 经 io::Error
/// 上抛）；命令面其余错误文案走 write_error_raw 常量直出，不经本枚举
#[derive(Error, Debug)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] io::Error),
}

pub type Result<T> = result::Result<T, Error>;
