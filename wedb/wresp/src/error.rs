use std::result;

use thiserror::Error;

/// RESP 帧解析违例（C# libs/common/Parsing/RespParsingException.cs 三抛点的
/// rust 形态：C# 直接 throw 由上层断连，rust 以 `Err` 表达违例、
/// `Ok(false)` 表达字节未到齐，三态不得坍缩为单一 false）
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
  /// libs/common/Parsing/RespParsingException.cs:ThrowIntegerOverflow
  ///
  /// 携溢出的 ASCII 数字串（C# `ThrowIntegerOverflow(buffer, length)` 的
  /// `Encoding.ASCII.GetString` 同源载荷，文案消费点须回显原始数字串）
  #[error("RESP 解析整数溢出: {digits}")]
  IntegerOverflow { digits: String },
  /// libs/common/Parsing/RespParsingException.cs:ThrowUnexpectedToken
  #[error("非预期标记: {0}")]
  UnexpectedToken(u8),
  /// libs/common/Parsing/RespParsingException.cs:ThrowInvalidStringLength
  #[error("无效字符串长度: {0}")]
  InvalidStringLength(i32),
}

pub type Result<T> = result::Result<T, Error>;
