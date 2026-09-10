//! 免认证设置（对标 libs/server/Auth/Settings/NoAuthSettings.cs）

use super::authentication_settings::{AuthSetup, IAuthenticationSettings};
use crate::{
  acl::AclError,
  auth::{GarnetNoAuthAuthenticator, IGarnetAuthenticator},
};

/// 免认证设置
pub struct NoAuthSettings;

impl IAuthenticationSettings for NoAuthSettings {
  /// 创建免认证认证器
  ///
  /// libs/server/Auth/Settings/NoAuthSettings.cs:CreateAuthenticator
  fn create_authenticator(
    &self,
    _setup: &AuthSetup,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    Ok(Box::new(GarnetNoAuthAuthenticator))
  }
}

#[cfg(test)]
mod tests {
  use super::{
    super::authentication_settings::{AuthSetup, IAuthenticationSettings},
    *,
  };

  /// NoAuth 档语义：恒已认证、不参与 AUTH
  #[test]
  fn creates_no_auth_authenticator() {
    let settings = NoAuthSettings;
    let auth = settings
      .create_authenticator(&AuthSetup { acl: None })
      .unwrap();
    assert!(auth.is_authenticated());
    assert!(!auth.can_authenticate());
    assert!(!auth.has_acl_support());
  }
}
