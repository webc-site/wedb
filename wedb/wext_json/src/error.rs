use std::{io, result};

use thiserror::Error;
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;

/// JSON 模块域错误文案单点（对标 C# modules/GarnetJSON/JsonCmdStrings.cs，
/// 模块文案不上收协议核心 crate）
///
/// garnet/modules/GarnetJSON/JsonCmdStrings.cs:RESP_NEW_OBJECT_AT_ROOT 同名字段
pub const RESP_NEW_OBJECT_AT_ROOT: &str = "ERR new objects must be created at the root";
/// garnet/modules/GarnetJSON/JsonCmdStrings.cs:RESP_WRONG_STATIC_PATH
pub const RESP_WRONG_STATIC_PATH: &str = "Err wrong static path";
/// rust 扩展：C# GarnetJSON 无 "ERR invalid JSONPath" 文案（rg 核实）
pub const ERR_INVALID_JSON_PATH: &str = "ERR invalid JSONPath";
/// rust 扩展：C# 模板 CmdStrings.cs:GenericErrNotAFloat "ERR {0} value is not a valid float"
/// 的 number 实例化，GarnetJSON 内单点使用
pub const ERR_NUMBER_NOT_VALID_FLOAT: &str = "ERR number value is not a valid float";
/// COSCAN（自定义对象扫描）对 JSON 域的裁量错误帧文案：C# GarnetJsonObject.Scan
/// 抛 NotImplementedException（会话级异常，无 RESP 错误帧对位），rust 裁量回
/// 错误帧，文案取 .NET NotImplementedException 默认消息（登记 doc/zh/deviations.md）
pub const RESP_ERR_NOT_IMPLEMENTED: &str = "ERR The method or operation is not implemented.";

/// JSON 模块错误定义
#[derive(Debug, Error)]
pub enum Error {
  #[error("{RESP_NEW_OBJECT_AT_ROOT}")]
  NewObjectAtRoot,

  #[error("{RESP_WRONG_STATIC_PATH}")]
  WrongStaticPath,

  #[error("{ERR_INVALID_JSON_PATH}: {0}")]
  InvalidPath(String),

  #[error("{RESP_ERR_GENERIC_SYNTAX_ERROR}")]
  SyntaxError,

  #[error(transparent)]
  Sonic(#[from] sonic_rs::Error),

  #[error(transparent)]
  Regex(#[from] regex::Error),

  #[error(transparent)]
  Io(#[from] io::Error),
}

pub type Result<T> = result::Result<T, Error>;
