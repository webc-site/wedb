//! ACL 认证器基座（对标 libs/server/Auth/GarnetACLAuthenticator.cs）
//!
//! C# 抽象基类：持访问控制列表与已认证用户句柄，Authenticate 为模板
//! 方法（解析用户句柄后委托抽象 AuthenticateInternal）。rust 侧以组合
//! 承接：子档结构体内嵌本基座，模板方法经闭包注入内部认证实现。

use std::sync::Arc;

use crate::{
  AccessControlList, AclPassword, UserHandle, access_control_list::DEFAULT_USER_NAME,
  user::parse_user_namespace,
};

/// ACL 口令校验：ascii 规范化后取哈希，比对生效用户的注册口令
///
/// 会话侧 AUTH（wnode/src/resp/resp_server_session.rs:authenticate_user）与本
/// 基座模板方法共用同一实现，对标原 ACL + 口令子档的内部认证；
/// C# 侧该档类型（GarnetAclWithPasswordAuthenticator）在 rust 无独立落点，
/// 口令校验经本函数注入 [`GarnetAclAuthenticator::authenticate`] 承接
///
/// libs/server/Auth/GarnetAclWithPasswordAuthenticator.cs:AuthenticateInternal
pub fn acl_password_check(
  user_handle: &Arc<UserHandle>,
  _username: &[u8],
  password: &[u8],
) -> bool {
  // C# 经 Encoding.ASCII 规范口令字节后取 SHA-256 哈希
  let password_hash = AclPassword::from_string(&super::ascii_sanitize(password));
  let user = user_handle.user();
  user.is_enabled() && user.validate_password(&password_hash)
}

/// ACL 认证器基座
pub struct GarnetAclAuthenticator {
  /// 认证所依据的访问控制列表
  pub acl: Arc<AccessControlList>,
  /// 已认证用户句柄（未认证为 None）
  pub user_handle: Option<Arc<UserHandle>>,
  /// 当前已认证用户的命名空间（默认为 0）
  pub namespace: u64,
}

impl GarnetAclAuthenticator {
  /// 构造基座
  pub fn new(acl: Arc<AccessControlList>) -> Self {
    Self {
      acl,
      user_handle: None,
      namespace: 0,
    }
  }

  /// 认证模板方法：仅承接引导 default 用户（空用户名 / `default` / `0#default`）
  /// → 委托内部认证（闭包注入，对标子类的 AuthenticateInternal）→ 成功记录句柄
  ///
  /// 命名用户的权威取数已收敛至底层存储点查
  /// （wedb/wnode/src/resp/acl_commands.rs:authenticate_user_via_store 的
  /// `(ns, 0, KeyTag::Acl, user)` 记录），本方法一律返回 false，不再查任何
  /// 用户字典
  ///
  /// C# 侧异常吞噬路径（catch + LogDebug）在 rust 值语义下不存在——
  /// 内部实现以 bool 表达成败，语义等价
  ///
  /// libs/server/Auth/GarnetACLAuthenticator.cs:Authenticate
  /// libs/server/Auth/GarnetACLAuthenticator.cs:AuthenticateInternal
  pub fn authenticate(
    &mut self,
    username: &[u8],
    password: &[u8],
    mut authenticate_internal: impl FnMut(&Arc<UserHandle>, &[u8], &[u8]) -> bool,
  ) -> bool {
    // 用户名经 ASCII 规范化（>0x7F 折 '?'，对标 C# Encoding.ASCII.GetString）
    let uname = super::ascii_sanitize(username);
    let (clean_user, target_ns) = if uname.is_empty() {
      (DEFAULT_USER_NAME, 0)
    } else {
      match parse_user_namespace(&uname) {
        Ok(res) => res,
        Err(_) => return false,
      }
    };
    if target_ns != 0 || clean_user != DEFAULT_USER_NAME {
      return false;
    }
    let user_handle = self.acl.get_default_user_handle();
    if authenticate_internal(&user_handle, username, password) {
      self.user_handle = Some(user_handle);
      self.namespace = target_ns;
      return true;
    }
    false
  }

  /// 当前已认证用户的命名空间
  pub fn get_namespace(&self) -> u64 {
    self.namespace
  }

  /// 当前已认证用户的句柄
  ///
  /// libs/server/Auth/GarnetACLAuthenticator.cs:GetUserHandle
  pub fn get_user_handle(&self) -> Option<&Arc<UserHandle>> {
    self.user_handle.as_ref()
  }

  /// 认证所依据的访问控制列表（全服唯一 ACL 实例）
  ///
  /// libs/server/Auth/GarnetACLAuthenticator.cs:GetAccessControlList
  pub fn get_access_control_list(&self) -> &Arc<AccessControlList> {
    &self.acl
  }
}

/// 已认证位（对标 C# GarnetACLAuthenticator.IsAuthenticated 虚属性）
impl GarnetAclAuthenticator {
  /// 是否已认证
  ///
  /// libs/server/Auth/GarnetACLAuthenticator.cs:IsAuthenticated
  pub fn is_authenticated(&self) -> bool {
    self.user_handle.is_some()
  }
}
