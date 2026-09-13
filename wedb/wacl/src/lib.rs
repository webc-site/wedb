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
pub mod user_handle;

pub use access_control_list::AccessControlList;
pub use acl_exception::AclError;
pub use acl_parser::AclParser;
pub use acl_password::AclPassword;
pub use auth::*;
pub use command_catalog::RespAclCategories;
pub use command_permission_set::CommandPermissionSet;
pub use secrets_utility::constant_equals;
pub use user::User;
pub use user_handle::UserHandleExt;

/// 用户并发句柄（对标 libs/server/ACL/UserHandle.cs:UserHandle —— RwLock 承接
/// C# 引用换新语义；CAS 换新原语见 [`UserHandleExt::try_set_user`]）。
pub type UserHandle = RwLock<Arc<User>>;
