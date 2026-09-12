//! ACL + 口令认证器（对标 libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs）

use std::sync::Arc;

use super::{
  ascii_sanitize, garnet_acl_authenticator::GarnetAclAuthenticator,
  i_garnet_authenticator::IGarnetAuthenticator,
};
use crate::{AccessControlList, AclPassword, user_handle::UserHandle};

/// ACL 口令档：对有效用户校验其注册口令
pub struct GarnetAclWithPasswordAuthenticator {
  /// ACL 认证基座
  pub base: GarnetAclAuthenticator,
}

impl GarnetAclWithPasswordAuthenticator {
  /// 构造
  pub fn new(acl: Arc<AccessControlList>) -> Self {
    Self {
      base: GarnetAclAuthenticator::new(acl),
    }
  }

  /// 口令哈希比对（认证与授权检查均针对生效用户）
  ///
  /// libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs:AuthenticateInternal
  fn authenticate_internal(
    user_handle: &Arc<UserHandle>,
    _username: &[u8],
    password: &[u8],
  ) -> bool {
    // C# 经 Encoding.ASCII 规范口令字节后取 SHA-256 哈希
    let password_hash = AclPassword::from_string(&ascii_sanitize(password));
    let user = user_handle.user();
    user.is_enabled() && user.validate_password(&password_hash)
  }
}

impl IGarnetAuthenticator for GarnetAclWithPasswordAuthenticator {
  fn is_authenticated(&self) -> bool {
    self.base.is_authenticated()
  }

  fn can_authenticate(&self) -> bool {
    true
  }

  fn has_acl_support(&self) -> bool {
    true
  }

  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    self
      .base
      .authenticate(username, password, Self::authenticate_internal)
  }
}
