//! ACL + 口令认证器（对标 libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs）
//!
//! 口令校验抽为自由函数 [`acl_password_check`]：本档与 wnode 会话侧
//! （AUTH 命名用户存储点查失败后回落内存认证器）共用同一实现，
//! 避免同一校验两处落地。

use std::sync::Arc;

use super::{
  ascii_sanitize, garnet_acl_authenticator::GarnetAclAuthenticator,
  i_garnet_authenticator::IGarnetAuthenticator,
};
use crate::{AccessControlList, AclPassword, UserHandle};

/// ACL 口令校验：ascii 规范化后取哈希，比对生效用户的注册口令
///
/// libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs:AuthenticateInternal
pub fn acl_password_check(
  user_handle: &Arc<UserHandle>,
  _username: &[u8],
  password: &[u8],
) -> bool {
  // C# 经 Encoding.ASCII 规范口令字节后取 SHA-256 哈希
  let password_hash = AclPassword::from_string(&ascii_sanitize(password));
  let user = user_handle.user();
  user.is_enabled() && user.validate_password(&password_hash)
}

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
      .authenticate(username, password, acl_password_check)
  }
}
