//! 认证设置接口与认证模式（对标 libs/server/Auth/Settings/AuthenticationSettings.cs）

use std::sync::Arc;

use crate::{
  acl::{AccessControlList, AclError},
  auth::IGarnetAuthenticator,
};

/// 认证模式
///
/// libs/server/Auth/Settings/AuthenticationSettings.cs:GarnetAuthenticationMode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarnetAuthenticationMode {
  /// 免认证：接受任意连接
  NoAuth,
  /// 口令：接受正确口令的连接
  Password,
  /// AAD：接受携带正确 AAD 主体的连接；令牌会过期，客户端须周期性 AUTH 刷新
  Aad,
  /// ACL：按配置的 ACL 用户与访问规则校验连接与命令
  Acl,
  /// ACL + AAD 令牌：用户名须为 ObjectId 或合法组 ObjectId，令牌声明按此校验
  AclWithAad,
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
  fn create_authenticator(
    &self,
    setup: &AuthSetup,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError>;
}
