//! 认证设置接口与认证模式（对标 libs/server/Auth/Settings/AuthenticationSettings.cs）

use std::sync::Arc;

use crate::{AccessControlList, AclError, auth::GarnetAuthenticator};

/// 认证模式
///
/// libs/server/Auth/Settings/AuthenticationSettings.cs:GarnetAuthenticationMode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarnetAuthenticationMode {
  /// 免认证：接受任意连接
  NoAuth,
  /// 口令：接受正确口令的连接
  Password,
  /// ACL：按配置的 ACL 用户与访问规则校验连接与命令
  Acl,
}

/// 认证器装配上下文（对标 C# CreateAuthenticator(storeWrapper) 自
/// StoreWrapper 提取 accessControlList 的面；rust 会话域尚未在
/// StoreWrapper 挂载 ACL，缺省以 None 传入由各设置自行裁决）
pub struct AuthSetup {
  /// 已加载的访问控制列表
  pub acl: Option<Arc<AccessControlList>>,
}

/// 认证设置
///
/// libs/server/Auth/Settings/AuthenticationSettings.cs:IAuthenticationSettings
pub trait IAuthenticationSettings: Send + Sync {
  /// 以当前设置创建认证器
  ///
  /// libs/server/Auth/Settings/AuthenticationSettings.cs:CreateAuthenticator
  fn create_authenticator(&self, setup: &AuthSetup) -> Result<GarnetAuthenticator, AclError>;
}
