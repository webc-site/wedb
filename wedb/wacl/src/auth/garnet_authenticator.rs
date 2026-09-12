//! Garnet 统一认证器静态分发枚举（消除堆分配与虚表间接寻址）

use super::{
  GarnetAclWithPasswordAuthenticator, GarnetNoAuthAuthenticator, GarnetPasswordAuthenticator,
  i_garnet_authenticator::IGarnetAuthenticator,
};

/// 统一认证器静态分发枚举
pub enum GarnetAuthenticator {
  /// 免认证
  NoAuth(GarnetNoAuthAuthenticator),
  /// 密码认证
  Password(GarnetPasswordAuthenticator),
  /// ACL + 密码
  AclWithPassword(GarnetAclWithPasswordAuthenticator),
}

impl IGarnetAuthenticator for GarnetAuthenticator {
  #[inline]
  fn is_authenticated(&self) -> bool {
    match self {
      Self::NoAuth(a) => a.is_authenticated(),
      Self::Password(a) => a.is_authenticated(),
      Self::AclWithPassword(a) => a.is_authenticated(),
    }
  }

  #[inline]
  fn can_authenticate(&self) -> bool {
    match self {
      Self::NoAuth(a) => a.can_authenticate(),
      Self::Password(a) => a.can_authenticate(),
      Self::AclWithPassword(a) => a.can_authenticate(),
    }
  }

  #[inline]
  fn has_acl_support(&self) -> bool {
    match self {
      Self::NoAuth(a) => a.has_acl_support(),
      Self::Password(a) => a.has_acl_support(),
      Self::AclWithPassword(a) => a.has_acl_support(),
    }
  }

  #[inline]
  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    match self {
      Self::NoAuth(a) => a.authenticate(password, username),
      Self::Password(a) => a.authenticate(password, username),
      Self::AclWithPassword(a) => a.authenticate(password, username),
    }
  }
}
