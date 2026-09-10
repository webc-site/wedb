//! 免认证认证器（对标 libs/server/Auth/GarnetNoAuthAuthenticator.cs）

use super::i_garnet_authenticator::IGarnetAuthenticator;

/// 免认证档：任意连接直接放行
pub struct GarnetNoAuthAuthenticator;

impl IGarnetAuthenticator for GarnetNoAuthAuthenticator {
  /// 恒已认证
  fn is_authenticated(&self) -> bool {
    true
  }

  /// 不参与 AUTH 口令认证
  fn can_authenticate(&self) -> bool {
    false
  }

  fn has_acl_support(&self) -> bool {
    false
  }

  /// 不可达路径：NoAuth 档永不参与认证（对标 C# Debug.Fail 断言）
  ///
  /// libs/server/Auth/GarnetNoAuthAuthenticator.cs:Authenticate
  fn authenticate(&mut self, _password: &[u8], _username: &[u8]) -> bool {
    debug_assert!(false, "No auth authenticator should never authenticate.");
    false
  }
}
