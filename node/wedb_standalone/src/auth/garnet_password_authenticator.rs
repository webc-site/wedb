//! 单一固定口令认证器（对标 libs/server/Auth/GarnetPasswordAuthenticator.cs）
//!
//! C# 标注 XXX: Deprecated，应被 ACL 认证器取代。

use super::i_garnet_authenticator::IGarnetAuthenticator;
use crate::acl::secrets_utility::constant_equals;

/// 固定口令档
pub struct GarnetPasswordAuthenticator {
  /// 配置的口令
  pwd: Box<[u8]>,
  /// 是否已通过认证
  authenticated: bool,
}

impl GarnetPasswordAuthenticator {
  /// 以给定口令构造
  pub fn new(pwd: Box<[u8]>) -> Self {
    Self {
      pwd,
      authenticated: false,
    }
  }
}

impl IGarnetAuthenticator for GarnetPasswordAuthenticator {
  fn is_authenticated(&self) -> bool {
    self.authenticated
  }

  fn can_authenticate(&self) -> bool {
    true
  }

  fn has_acl_support(&self) -> bool {
    false
  }

  /// 常量时间比对固定口令
  ///
  /// libs/server/Auth/GarnetPasswordAuthenticator.cs:Authenticate
  fn authenticate(&mut self, password: &[u8], _username: &[u8]) -> bool {
    self.authenticated = constant_equals(&self.pwd, password);
    self.authenticated
  }
}
