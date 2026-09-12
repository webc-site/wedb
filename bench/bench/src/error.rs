use std::{io, result};

use thiserror::Error;

/// 评测模块统一错误定义
#[derive(Error, Debug)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] io::Error),

  #[error(transparent)]
  Yaml(#[from] serde_yaml::Error),

  #[error(transparent)]
  Json(#[from] sonic_rs::Error),

  #[error("引擎异常: {0}")]
  Engine(String),

  #[error("测试失败: {0}")]
  TestFailed(String),
}

pub type Result<T> = result::Result<T, Error>;
