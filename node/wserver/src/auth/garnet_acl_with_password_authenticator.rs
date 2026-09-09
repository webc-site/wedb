//! ACL + 口令认证器（对标 libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs）

use std::sync::Arc;

use super::{
  ascii_sanitize, garnet_acl_authenticator::GarnetAclAuthenticator,
  i_garnet_authenticator::IGarnetAuthenticator,
};
use crate::acl::{AccessControlList, AclPassword, user_handle::UserHandle};

/// ACL 口令档：对有效用户校验其注册口令
pub struct GarnetAclWithPasswordAuthenticator {
  /// ACL 认证基座
  pub base: GarnetAclAuthenticator,
}

impl GarnetAclWithPasswordAuthenticator {
  /// 构造
  pub fn new(acl: Arc<AccessControlList>) -> Self {
    Self {
      base: GarnetAclAuthenticator::new(acl),
    }
  }

  /// 口令哈希比对（认证与授权检查均针对生效用户）
  ///
  /// libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs:AuthenticateInternal
  fn authenticate_internal(
    user_handle: &Arc<UserHandle>,
    _username: &[u8],
    password: &[u8],
  ) -> bool {
    // C# 经 Encoding.ASCII 规范口令字节后取 SHA-256 哈希
    let password_hash = AclPassword::from_string(&ascii_sanitize(password));
    let user = user_handle.user();
    user.is_enabled() && user.validate_password(&password_hash)
  }
}

impl IGarnetAuthenticator for GarnetAclWithPasswordAuthenticator {
  fn is_authenticated(&self) -> bool {
    self.base.is_authenticated()
  }

  fn can_authenticate(&self) -> bool {
    true
  }

  fn has_acl_support(&self) -> bool {
    true
  }

  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    self
      .base
      .authenticate(username, password, &mut |handle, username, password| {
        Self::authenticate_internal(handle, username, password)
      })
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::{
    acl::{AccessControlList, AclParser, AclPassword},
    auth::ascii_sanitize,
    types::RespCommand,
  };

  /// 无 ACL 文件构造：default 用户免密启用
  #[test]
  fn default_user_passwordless_auth() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));

    // 空用户名 → default 用户；免密任意口令通过
    assert!(auth.authenticate(b"whatever", b""));
    let handle = auth.base.get_user_handle().unwrap();
    assert_eq!(handle.user().name, "default");

    // 显式 default 用户名同样通过
    let mut auth2 = GarnetAclWithPasswordAuthenticator::new(acl);
    assert!(auth2.authenticate(b"whatever", b"default"));
    assert!(auth2.base.is_authenticated());
  }

  /// 对标 garnet SetUserTests.EnableAndDisableUsers：未启用用户拒绝认证
  #[test]
  fn disabled_user_rejected() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    AclParser::parse_acl_rule("user alice >pw +@all", Some(&acl)).unwrap();
    let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
    // 默认 off：口令正确也拒绝
    assert!(!auth.authenticate(b"pw", b"alice"));
    assert!(auth.base.get_user_handle().is_none());

    // on 后放行
    AclParser::parse_acl_rule("user alice on", Some(&acl)).unwrap();
    assert!(auth.authenticate(b"pw", b"alice"));
  }

  /// 错误口令 / 未知用户 / 非法 UTF-8 用户名
  #[test]
  fn wrong_password_or_unknown_user() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    AclParser::parse_acl_rule("user alice on >pw", Some(&acl)).unwrap();
    let mut auth = GarnetAclWithPasswordAuthenticator::new(acl);

    assert!(!auth.authenticate(b"wrong", b"alice"));
    assert!(!auth.authenticate(b"pw", b"nobody"));
    assert!(!auth.authenticate(b"pw", b"\xff\xfe"));
  }

  /// 多口令与哈希口令（对标 SetUserTests.AddPasswordFromHash 语义）
  #[test]
  fn hash_password_auth() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    let rule = format!(
      "user bob on #{} >plain",
      "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9"
    );
    AclParser::parse_acl_rule(&rule, Some(&acl)).unwrap();

    let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
    // 哈希口令原文
    assert!(auth.authenticate(b"passw0rd", b"bob"));
    // 明文口令
    assert!(auth.authenticate(b"plain", b"bob"));
    // 其余拒绝
    assert!(!auth.authenticate(b"nope", b"bob"));
  }

  /// 口令认证与命令权限正交（权限经用户权限集判定）
  #[test]
  fn acl_and_permissions_orthogonal() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    AclParser::parse_acl_rule("user carol on >pw +get", Some(&acl)).unwrap();
    let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
    assert!(auth.authenticate(b"pw", b"carol"));

    let user = auth.base.get_user_handle().unwrap().user();
    assert!(user.can_access_command(RespCommand::Get));
    assert!(!user.can_access_command(RespCommand::Set));
  }

  /// 常量时间口令比较（GarnetPasswordAuthenticator 语义在 ACL 侧走哈希比对）
  #[test]
  fn sanitize_non_ascii_bytes_like_csharp() {
    // C# Encoding.ASCII 折 '?' 后哈希：>0x7F 字节等价 '?' 哈希
    let hashed_question = AclPassword::from_string("?");
    let a = AclPassword::from_string(&ascii_sanitize(&[0xC3, 0xA9]));
    assert_eq!(a, AclPassword::from_string("??"));
    assert_eq!(hashed_question, AclPassword::from_string("?"));
    let _ = a;
  }
}
