//! ACL + 口令认证设置（对标 libs/server/Auth/Settings/AclAuthenticationPasswordSettings.cs）

use std::sync::Arc;

use super::{
  acl_authentication_settings::AclAuthenticationSettings,
  authentication_settings::{AuthSetup, IAuthenticationSettings},
};
use crate::{
  acl::{AccessControlList, AclError},
  auth::{GarnetAclWithPasswordAuthenticator, IGarnetAuthenticator},
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
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    Ok(Box::new(GarnetAclWithPasswordAuthenticator::new(
      Arc::clone(acl),
    )))
  }
}

impl IAuthenticationSettings for AclAuthenticationPasswordSettings {
  /// 模板：解析 ACL 后委托内部创建（对标 C# 基座 CreateAuthenticator）
  ///
  /// libs/server/Auth/Settings/AclAuthenticationPasswordSettings.cs:CreateAuthenticator
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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::{
    super::authentication_settings::{AuthSetup, IAuthenticationSettings},
    *,
  };
  use crate::acl::AccessControlList;

  /// ACL 缺席时报错；在场时创建 ACL 口令档
  #[test]
  fn create_requires_acl() {
    let settings = AclAuthenticationPasswordSettings::new(Some("users.acl".into()), String::new());
    assert!(
      settings
        .create_authenticator(&AuthSetup { acl: None })
        .is_err()
    );

    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    let auth = settings
      .create_authenticator(&AuthSetup {
        acl: Some(Arc::clone(&acl)),
      })
      .unwrap();
    assert!(auth.can_authenticate());
    assert!(auth.has_acl_support());
    assert!(!auth.is_authenticated());

    // 底座字段面
    assert_eq!(
      settings.base.acl_configuration_file.as_deref(),
      Some("users.acl")
    );
    assert_eq!(settings.base.default_password, "");
  }
}
