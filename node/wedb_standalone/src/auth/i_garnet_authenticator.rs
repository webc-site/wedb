//! Garnet 认证器接口（对标 libs/server/Auth/IGarnetAuthenticator.cs）

/// Garnet 认证器
///
/// libs/server/Auth/IGarnetAuthenticator.cs:IGarnetAuthenticator
pub trait IGarnetAuthenticator: Send {
  /// 当前调用方是否已认证
  ///
  /// libs/server/Auth/IGarnetAuthenticator.cs:IsAuthenticated
  fn is_authenticated(&self) -> bool;

  /// 认证器能否参与 AUTH 口令认证
  ///
  /// libs/server/Auth/IGarnetAuthenticator.cs:CanAuthenticate
  fn can_authenticate(&self) -> bool;

  /// 该认证器能否与 ACL 联用
  ///
  /// libs/server/Auth/IGarnetAuthenticator.cs:HasACLSupport
  fn has_acl_support(&self) -> bool;

  /// 以 AUTH 命令送来的用户名 / 口令执行认证（用户名可省略）
  ///
  /// libs/server/Auth/IGarnetAuthenticator.cs:Authenticate
  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool;
}
