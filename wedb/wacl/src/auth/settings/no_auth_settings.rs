//! 免认证设置（对标 libs/server/Auth/Settings/NoAuthSettings.cs）

use super::authentication_settings::{AuthSetup, IAuthenticationSettings};
use crate::{
  AclError,
  auth::{GarnetAuthenticator, GarnetNoAuthAuthenticator},
};

/// 免认证设置
pub struct NoAuthSettings;

impl IAuthenticationSettings for NoAuthSettings {
  /// 创建免认证认证器
  ///
  /// libs/server/Auth/Settings/NoAuthSettings.cs:CreateAuthenticator
  fn create_authenticator(&self, _setup: &AuthSetup) -> Result<GarnetAuthenticator, AclError> {
    Ok(GarnetAuthenticator::NoAuth(GarnetNoAuthAuthenticator))
  }
}
