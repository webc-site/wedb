//! AAD 认证设置（对标 libs/server/Auth/Settings/AadAuthenticationSettings.cs）

use std::sync::Arc;

use gxhash::{HashSet, HashSetExt};

use super::authentication_settings::{AuthSetup, IAuthenticationSettings};
use crate::{
  acl::AclError,
  auth::{
    GarnetAadAuthenticator, IGarnetAuthenticator, IssuerSigningTokenProvider,
    garnet_aad_authenticator::AadAuthenticatorConfig,
  },
};

/// AAD 认证设置
pub struct AadAuthenticationSettings {
  /// 白名单应用 ID（小写规范，忽略大小写匹配）
  authorized_app_ids: HashSet<String>,
  /// 合法受众（小写规范）
  audiences: HashSet<String>,
  /// 合法签发者（小写规范）
  issuers: HashSet<String>,
  /// 签名密钥供给器
  signing_token_provider: Arc<IssuerSigningTokenProvider>,
  /// 是否校验用户名（OID / 组声明匹配）
  validate_username: bool,
}

/// 迭代项统一小写规范入集（C# HashSet(OrdinalIgnoreCase) 的承接）
fn lower_set(items: &[String]) -> HashSet<String> {
  let mut set = HashSet::with_capacity(items.len());
  set.extend(items.iter().map(|s| s.to_ascii_lowercase()));
  set
}

impl AadAuthenticationSettings {
  /// 构造（应用 ID / 受众 / 签发者非空；供给器经 Arc 类型保证非空，
  /// 对标 C# 构造异常 "SigningToken provider cannot be null."）
  pub fn new(
    authorized_app_ids: &[String],
    audiences: &[String],
    issuers: &[String],
    signing_token_provider: Arc<IssuerSigningTokenProvider>,
    validate_username: bool,
  ) -> Result<Self, AclError> {
    if authorized_app_ids.is_empty() {
      return Err(AclError::Acl("Authorized app Ids cannot be empty.".into()));
    }
    if audiences.is_empty() {
      return Err(AclError::Acl("Audiences cannot be empty.".into()));
    }
    if issuers.is_empty() {
      return Err(AclError::Acl("Issuers cannot be empty.".into()));
    }
    Ok(Self {
      authorized_app_ids: lower_set(authorized_app_ids),
      audiences: lower_set(audiences),
      issuers: lower_set(issuers),
      signing_token_provider,
      validate_username,
    })
  }
}

impl IAuthenticationSettings for AadAuthenticationSettings {
  /// 创建 AAD 认证器
  ///
  /// libs/server/Auth/Settings/AadAuthenticationSettings.cs:CreateAuthenticator
  fn create_authenticator(
    &self,
    _setup: &AuthSetup,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    Ok(Box::new(GarnetAadAuthenticator::new(
      AadAuthenticatorConfig {
        authorized_app_ids: self.authorized_app_ids.clone(),
        audiences: self.audiences.clone(),
        issuers: self.issuers.clone(),
        signing_token_provider: Arc::clone(&self.signing_token_provider),
        validate_username: self.validate_username,
      },
    )))
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use gxhash::{HashSet, HashSetExt};

  use super::{
    super::authentication_settings::{AuthSetup, IAuthenticationSettings},
    *,
  };
  use crate::auth::{GarnetAadAuthenticator, IssuerSigningTokenProvider};

  fn provider() -> Arc<IssuerSigningTokenProvider> {
    IssuerSigningTokenProvider::create_for_test(Default::default())
  }

  /// 对标 C# 构造异常：白名单 / 受众 / 签发者非空校验
  #[test]
  fn constructor_validations() {
    let app = vec!["app-1".to_string()];
    let aud = vec!["aud".to_string()];
    let iss = vec!["https://sts".to_string()];
    assert!(AadAuthenticationSettings::new(&[], &aud, &iss, provider(), false).is_err());
    assert!(AadAuthenticationSettings::new(&app, &[], &iss, provider(), false).is_err());
    assert!(AadAuthenticationSettings::new(&app, &aud, &[], provider(), false).is_err());
    assert!(AadAuthenticationSettings::new(&app, &aud, &iss, provider(), false).is_ok());
  }

  /// 创建的认证器即 AAD 档（CanAuthenticate / !HasACLSupport）
  #[test]
  fn create_authenticator_builds_aad() {
    let settings = AadAuthenticationSettings::new(
      &["app-1".to_string()],
      &["aud".to_string()],
      &["https://sts".to_string()],
      provider(),
      true,
    )
    .unwrap();
    let auth = settings
      .create_authenticator(&AuthSetup { acl: None })
      .unwrap();
    assert!(auth.can_authenticate());
    assert!(!auth.has_acl_support());
    assert!(!auth.is_authenticated());
    let _ = GarnetAadAuthenticator::new(AadAuthenticatorConfig {
      authorized_app_ids: HashSet::new(),
      audiences: HashSet::new(),
      issuers: HashSet::new(),
      signing_token_provider: provider(),
      validate_username: false,
    });
  }
}
