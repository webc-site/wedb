//! ACL 认证器基座（对标 libs/server/Auth/GarnetACLAuthenticator.cs）
//!
//! C# 抽象基类：持访问控制列表与已认证用户句柄，Authenticate 为模板
//! 方法（解析用户句柄后委托抽象 AuthenticateInternal）。rust 侧以组合
//! 承接：子档结构体内嵌本基座，模板方法经闭包注入内部认证实现。

use std::sync::Arc;

use crate::{AccessControlList, user_handle::UserHandle};

/// 内部认证钩子（子档 AuthenticateInternal 的函数指针形态）
pub type AuthenticateInternal = fn(&Arc<UserHandle>, &[u8], &[u8]) -> bool;

/// ACL 认证器基座
pub struct GarnetAclAuthenticator {
  /// 认证所依据的访问控制列表
  pub acl: Arc<AccessControlList>,
  /// 已认证用户句柄（未认证为 None）
  pub user_handle: Option<Arc<UserHandle>>,
}

impl GarnetAclAuthenticator {
  /// 构造基座
  pub fn new(acl: Arc<AccessControlList>) -> Self {
    Self {
      acl,
      user_handle: None,
    }
  }

  /// 认证模板方法：定位用户句柄（用户名缺省取 default 用户）→ 委托
  /// 内部认证（闭包注入，对标子类的 AuthenticateInternal）→ 成功记录句柄
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
    let user_handle = if uname.is_empty() {
      self.acl.get_default_user_handle()
    } else {
      self.acl.get_user_handle(&uname)
    };
    let Some(user_handle) = user_handle else {
      return false;
    };
    if authenticate_internal(&user_handle, username, password) {
      self.user_handle = Some(user_handle);
      return true;
    }
    false
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

/// 子档公共默认面（CanAuthenticate / IsAuthenticated / HasACLSupport）
impl GarnetAclAuthenticator {
  /// 是否已认证
  pub fn is_authenticated(&self) -> bool {
    self.user_handle.is_some()
  }

  /// ACL 档恒可认证（CanAuthenticate = true）
  pub fn can_authenticate(&self) -> bool {
    true
  }

  /// ACL 档恒支持 ACL（HasACLSupport = true）
  pub fn has_acl_support(&self) -> bool {
    true
  }
}
