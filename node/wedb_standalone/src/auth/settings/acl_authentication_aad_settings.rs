//! ACL + AAD 认证设置（对标 libs/server/Auth/Settings/AclAuthenticationAadSettings.cs）

use std::sync::Arc;

use super::{
  aad_authentication_settings::AadAuthenticationSettings,
  acl_authentication_settings::AclAuthenticationSettings,
  authentication_settings::{AuthSetup, IAuthenticationSettings},
};
use crate::{
  acl::{AccessControlList, AclError},
  auth::{GarnetAclWithAadAuthenticator, IGarnetAuthenticator},
};

/// ACL + AAD 认证设置
pub struct AclAuthenticationAadSettings {
  /// 公共底座
  pub base: AclAuthenticationSettings,
  /// AAD 认证设置
  aad_authentication_settings: AadAuthenticationSettings,
}

impl AclAuthenticationAadSettings {
  /// 构造
  pub fn new(
    acl_configuration_file: Option<String>,
    default_password: String,
    aad_authentication_settings: AadAuthenticationSettings,
  ) -> Self {
    Self {
      base: AclAuthenticationSettings::new(acl_configuration_file, default_password),
      aad_authentication_settings,
    }
  }

  /// 创建 ACL + AAD 认证器（内嵌 AAD 认证器）
  ///
  /// libs/server/Auth/Settings/AclAuthenticationAadSettings.cs:CreateAuthenticatorInternal
  fn create_authenticator_internal(
    &self,
    acl: &Arc<AccessControlList>,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    let aad = self
      .aad_authentication_settings
      .create_authenticator(&AuthSetup {
        acl: Some(Arc::clone(acl)),
      })?;
    Ok(Box::new(GarnetAclWithAadAuthenticator::new(
      Arc::clone(acl),
      aad,
    )))
  }
}

impl IAuthenticationSettings for AclAuthenticationAadSettings {
  /// 模板：解析 ACL 后委托内部创建（对标 C# 基座 CreateAuthenticator）
  ///
  /// libs/server/Auth/Settings/AclAuthenticationAadSettings.cs:CreateAuthenticator
  fn create_authenticator(
    &self,
    setup: &AuthSetup,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    let acl = setup.acl.clone().ok_or_else(|| {
      AclError::Acl("ACL authentication settings require a loaded AccessControlList".into())
    })?;
    self.create_authenticator_internal(&acl)
  }
}
