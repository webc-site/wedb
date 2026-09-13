use std::sync::Arc;

use parking_lot::RwLock;

pub mod access_control_list;
pub mod acl_exception;
pub mod acl_parser;
pub mod acl_password;
pub mod auth;
pub mod command_catalog;
pub mod command_catalog_data;
pub mod command_permission_set;
pub mod secrets_utility;
pub mod user;

pub use access_control_list::AccessControlList;
pub use acl_exception::AclError;
pub use acl_parser::AclParser;
pub use acl_password::AclPassword;
pub use auth::*;
pub use command_catalog::RespAclCategories;
pub use command_permission_set::CommandPermissionSet;
pub use secrets_utility::constant_equals;
pub use user::User;

/// 用户并发句柄（直接基于 RwLock<Arc<User>>，消除无意义单字段包装类开销）。
pub type UserHandle = RwLock<Arc<User>>;
