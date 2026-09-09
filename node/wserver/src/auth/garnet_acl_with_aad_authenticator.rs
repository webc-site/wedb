//! ACL + AAD 认证器（对标 libs/server/Auth/GarnetAclWithAadAuthenticator.cs）

use std::sync::Arc;

use super::{
  garnet_acl_authenticator::GarnetAclAuthenticator, i_garnet_authenticator::IGarnetAuthenticator,
};
use crate::acl::{AccessControlList, user_handle::UserHandle};

/// ACL + AAD 档：口令位置携带 AAD 令牌，经内嵌认证器验证后映射为 ACL 用户
pub struct GarnetAclWithAadAuthenticator {
  /// ACL 认证基座
  pub base: GarnetAclAuthenticator,
  /// AAD 令牌认证器（用户名期望为 ObjectId 或合法组 ObjectId）
  aad: Box<dyn IGarnetAuthenticator>,
}

impl GarnetAclWithAadAuthenticator {
  /// 构造
  pub fn new(acl: Arc<AccessControlList>, aad: Box<dyn IGarnetAuthenticator>) -> Self {
    Self {
      base: GarnetAclAuthenticator::new(acl),
      aad,
    }
  }

  /// 令牌经内嵌认证器验证（用户启用 + 令牌非空 + AAD 认证通过）
  ///
  /// libs/server/Auth/GarnetAclWithAadAuthenticator.cs:AuthenticateInternal
  fn authenticate_internal(
    aad: &mut dyn IGarnetAuthenticator,
    user_handle: &Arc<UserHandle>,
    username: &[u8],
    password: &[u8],
  ) -> bool {
    user_handle.user().is_enabled() && !password.is_empty() && aad.authenticate(password, username)
  }
}

impl IGarnetAuthenticator for GarnetAclWithAadAuthenticator {
  /// 内嵌 AAD 认证与基座句柄须同时有效
  fn is_authenticated(&self) -> bool {
    self.aad.is_authenticated() && self.base.is_authenticated()
  }

  fn can_authenticate(&self) -> bool {
    true
  }

  fn has_acl_support(&self) -> bool {
    true
  }

  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    // 分字段解借用：基座与 AAD 认证器各持可变借用
    let Self { base, aad } = self;
    base.authenticate(username, password, &mut |handle, username, password| {
      Self::authenticate_internal(aad.as_mut(), handle, username, password)
    })
  }
}
