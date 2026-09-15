//! ACL + 口令认证设置（对标 libs/server/Auth/Settings/AclAuthenticationPasswordSettings.cs）

use std::sync::Arc;

use super::{
  acl_authentication_settings::AclAuthenticationSettings,
  authentication_settings::{AuthSetup, IAuthenticationSettings},
};
use crate::{
  AccessControlList, AclError,
  auth::{GarnetAclWithPasswordAuthenticator, GarnetAuthenticator},
};

/// ACL + 口令认证设置
pub struct AclAuthenticationPasswordSettings {
  /// 公共底座
  pub base: AclAuthenticationSettings,
}

impl AclAuthenticationPasswordSettings {
  /// 构造
  pub fn new(acl_configuration_file: Option<String>, default_password: String) -> Self {
    Self {
      base: AclAuthenticationSettings::new(acl_configuration_file, default_password),
    }
  }

  /// 创建 ACL + 口令认证器
  ///
  /// libs/server/Auth/Settings/AclAuthenticationPasswordSettings.cs:CreateAuthenticatorInternal
  /// libs/server/Auth/Settings/AclAuthenticationSettings.cs:CreateAuthenticatorInternal
  fn create_authenticator_internal(
    &self,
    acl: &Arc<AccessControlList>,
  ) -> Result<GarnetAuthenticator, AclError> {
    Ok(GarnetAuthenticator::AclWithPassword(
      GarnetAclWithPasswordAuthenticator::new(Arc::clone(acl)),
    ))
  }
}

impl IAuthenticationSettings for AclAuthenticationPasswordSettings {
  /// 模板：解析 ACL 后委托内部创建（对标 C# 基座 CreateAuthenticator）
  ///
  /// libs/server/Auth/Settings/AclAuthenticationPasswordSettings.cs:CreateAuthenticator
  fn create_authenticator(&self, setup: &AuthSetup) -> Result<GarnetAuthenticator, AclError> {
    let acl = setup.acl.clone().ok_or_else(|| {
      AclError::Acl("ACL authentication settings require a loaded AccessControlList".into())
    })?;
    self.create_authenticator_internal(&acl)
  }
}
