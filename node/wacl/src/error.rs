//! ACL 错误族（对标 libs/server/ACL/ACLException.cs 的值化表达）

use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  /// libs/server/ACL/ACLException.cs:ACLParsingException
  #[error("Malformed ACL rule")]
  Parsing,
  /// libs/server/ACL/ACLException.cs:ACLPasswordException
  #[error("Unable to parse input password hash.")]
  Password,
  /// libs/server/ACL/ACLException.cs:ACLUnknownOperationException
  #[error("Unknown operation '{0}'")]
  UnknownOperation(String),
  /// libs/server/ACL/ACLException.cs:ACLCategoryDoesNotExistException
  #[error("ACL Category '{0}' does not exist")]
  CategoryDoesNotExist(String),
  /// libs/server/ACL/ACLException.cs:ACLUserAlreadyExistsException
  #[error("A user with name '{0}' already exists.")]
  UserAlreadyExists(String),
  /// libs/server/ACL/ACLException.cs:ACLException（其余基类消息档）
  #[error("{0}")]
  Acl(String),
}

pub type Result<T> = result::Result<T, Error>;
