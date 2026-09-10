//! 认证器族（对标 libs/server/Auth：IGarnetAuthenticator 及各实现）
//!
//! 固定口令认证器按 C# 口径做常量时间比较；NoAuth 认证器恒拒绝（C#
//! Debug.Fail 的"永不认证"约束以返回 false 承接）。ACL/AAD 组合认证器
//! 需要会话级 ACL 钩子，由 wserver 域的完整认证链承接，此处保留占位。

use crate::acl::constant_equals;

/// libs/server/Auth/IGarnetAuthenticator.cs:IGarnetAuthenticator
pub trait IGarnetAuthenticator {
  /// 当前调用方是否已认证（IsAuthenticated）
  fn is_authenticated(&self) -> bool;
  /// 认证器是否可执行认证（CanAuthenticate）
  fn can_authenticate(&self) -> bool;
  /// 是否支持与 ACL 协同（HasACLSupport）
  fn has_acl_support(&self) -> bool;
  /// 校验 AUTH 命令递交的用户名 / 口令（Authenticate）
  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool;
}

/// libs/server/Auth/GarnetNoAuthAuthenticator.cs:GarnetNoAuthAuthenticator
#[derive(Debug, Clone, Copy, Default)]
pub struct GarnetNoAuthAuthenticator;

impl IGarnetAuthenticator for GarnetNoAuthAuthenticator {
  #[inline]
  fn is_authenticated(&self) -> bool {
    true
  }
  #[inline]
  fn can_authenticate(&self) -> bool {
    false
  }
  #[inline]
  fn has_acl_support(&self) -> bool {
    false
  }
  // C# Debug.Fail："NoAuth 认证器永不参与认证"，恒拒绝
  #[inline]
  fn authenticate(&mut self, _password: &[u8], _username: &[u8]) -> bool {
    false
  }
}

/// libs/server/Auth/GarnetPasswordAuthenticator.cs:GarnetPasswordAuthenticator
///
/// 单一固定口令认证器（C# 标注已废弃，应由 ACL 认证器取代）
#[derive(Debug, Clone)]
pub struct GarnetPasswordAuthenticator {
  /// 固定口令（按字节保存，认证时常量时间比较）
  password: Vec<u8>,
  /// 最近一次认证结果（IsAuthenticated）
  authenticated: bool,
}

impl GarnetPasswordAuthenticator {
  /// libs/server/Auth/GarnetPasswordAuthenticator.cs:GarnetPasswordAuthenticator
  pub fn new(password: Vec<u8>) -> Self {
    Self {
      password,
      authenticated: false,
    }
  }
}

impl IGarnetAuthenticator for GarnetPasswordAuthenticator {
  #[inline]
  fn is_authenticated(&self) -> bool {
    self.authenticated
  }
  #[inline]
  fn can_authenticate(&self) -> bool {
    true
  }
  #[inline]
  fn has_acl_support(&self) -> bool {
    false
  }
  fn authenticate(&mut self, password: &[u8], _username: &[u8]) -> bool {
    // 常量时间比较（对标 SecretsUtility.ConstantEquals）
    self.authenticated = constant_equals(&self.password, password);
    self.authenticated
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn password_authenticator_constant_time_match() {
    let mut auth = GarnetPasswordAuthenticator::new(b"secret".to_vec());
    assert!(!auth.is_authenticated());
    assert!(auth.can_authenticate());
    assert!(!auth.has_acl_support());

    assert!(!auth.authenticate(b"secrat", b""));
    assert!(!auth.is_authenticated());
    assert!(auth.authenticate(b"secret", b""));
    assert!(auth.is_authenticated());

    // 长度不等直接失败
    assert!(!auth.authenticate(b"secret longer", b""));
  }

  #[test]
  fn no_auth_authenticator_never_authenticates() {
    let mut auth = GarnetNoAuthAuthenticator;
    assert!(auth.is_authenticated());
    assert!(!auth.can_authenticate());
    assert!(!auth.authenticate(b"anything", b""));
  }
}
