//! 口令认证设置（对标 libs/server/Auth/Settings/PasswordAuthenticationSettings.cs）

use super::authentication_settings::{AuthSetup, IAuthenticationSettings};
use crate::{
  acl::AclError,
  auth::{GarnetPasswordAuthenticator, IGarnetAuthenticator},
};

/// 口令认证设置
pub struct PasswordAuthenticationSettings {
  /// 口令（ASCII 字节）
  pwd: Box<[u8]>,
}

impl PasswordAuthenticationSettings {
  /// 构造（空口令即报错，对标 C# 构造异常 "Password cannot be null."）
  pub fn new(pwd: &str) -> Result<Self, AclError> {
    if pwd.is_empty() {
      return Err(AclError::Acl("Password cannot be null.".into()));
    }
    Ok(Self {
      pwd: pwd.as_bytes().to_vec().into(),
    })
  }
}

impl IAuthenticationSettings for PasswordAuthenticationSettings {
  /// 创建口令认证器
  ///
  /// libs/server/Auth/Settings/PasswordAuthenticationSettings.cs:CreateAuthenticator
  fn create_authenticator(
    &self,
    _setup: &AuthSetup,
  ) -> Result<Box<dyn IGarnetAuthenticator>, AclError> {
    Ok(Box::new(GarnetPasswordAuthenticator::new(self.pwd.clone())))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 对标 C# 构造异常：空口令拒绝
  #[test]
  fn empty_password_rejected() {
    assert!(PasswordAuthenticationSettings::new("").is_err());
    let settings = PasswordAuthenticationSettings::new("pw").unwrap();
    assert_eq!(settings.pwd.as_ref(), b"pw");
  }
}
