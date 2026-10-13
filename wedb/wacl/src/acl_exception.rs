//! ACL 异常族（对标 libs/server/ACL/ACLException.cs）
//!
//! C# 以异常类层级表达（ACLException 基类 + 各子类），rust 侧统一收敛为
//! [`AclError`] 枚举，Display 文案与 C# 各子类异常消息逐字一致，供
//! `ERR {message}` 应答直接复用。
//!
//! 弃迁档：C# ACLUserDoesNotExistException（"A user with name '{0}' does
//! not exist"）不移植——点查架构下用户存在性走存储直查、结构性不可达，
//! 该异常零构造零匹配，无对应错误臂。

use thiserror::Error;

/// ACL 异常（C# 异常族的值化表达）
#[derive(Debug, Error)]
pub enum AclError {
  /// 存储记录 bitcode 编解码失败（记录值二进制格式非法 / 结构损坏）
  #[error("ACL record codec failure: {0}")]
  Codec(#[from] bitcode::Error),
  /// libs/server/ACL/ACLException.cs:ACLException（基类消息档）
  #[error("{0}")]
  Acl(String),
  /// libs/server/ACL/ACLException.cs:ACLParsingException（携带文件与行号上下文）
  #[error("{message}")]
  Parsing {
    /// 解析错误消息
    message: String,
    /// 出错文件名
    filename: String,
    /// 出错行号（-1 表示无行号上下文）
    line: i32,
  },
  /// libs/server/ACL/ACLException.cs:ACLPasswordException
  #[error("{0}")]
  Password(String),
  /// libs/server/ACL/ACLException.cs:ACLUnknownOperationException
  #[error("Unknown operation '{0}'")]
  UnknownOperation(String),
  /// libs/server/ACL/ACLException.cs:ACLCategoryDoesNotExistException
  #[error("ACL Category '{0}' does not exist")]
  CategoryDoesNotExist(String),
  /// libs/server/ACL/ACLException.cs:AclCommandDoesNotExistException
  #[error("Command '{0}' does not exist")]
  CommandDoesNotExist(String),
}
