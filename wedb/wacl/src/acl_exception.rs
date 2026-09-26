//! ACL 异常族（对标 libs/server/ACL/ACLException.cs）
//!
//! C# 以异常类层级表达（ACLException 基类 + 各子类），rust 侧统一收敛为
//! [`AclError`] 枚举，Display 文案与 C# 各子类异常消息逐字一致，供
//! `ERR {message}` 应答直接复用。

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
  /// libs/server/ACL/ACLException.cs:ACLUserDoesNotExistException
  #[error("A user with name '{0}' does not exist")]
  UserDoesNotExist(String),
  /// libs/server/ACL/ACLException.cs:AclCommandDoesNotExistException
  #[error("Command '{0}' does not exist")]
  CommandDoesNotExist(String),
}
