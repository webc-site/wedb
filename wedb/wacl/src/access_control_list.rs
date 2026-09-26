//! 访问控制列表（对标 libs/server/ACL/AccessControlList.cs）
//!
//! 存储为唯一真源：用户规则以 `KeyTag::Acl` 记录持久化于底层存储
//! （读写见 wedb/wnode/src/resp/acl_store.rs），本类型不再持有任何
//! 用户大字典，仅承接引导 default 用户的内存单例句柄（requirepass /
//! nopass 装配，对标 C# CreateDefaultUserHandle 一条线）。命名用户的
//! 认证 / 管理按需点查存储，内存与用户总量彻底脱钩。

use std::sync::{Arc, OnceLock};

use wresp::catalog::RespAclCategories;

use super::{AclPassword, UserHandle, acl_exception::AclError, user::User};

/// default 用户名（对标 C# DefaultUserName）
pub const DEFAULT_USER_NAME: &str = "default";

/// 访问控制列表（存储为唯一真源，本类型仅承接引导 default 用户）
pub struct AccessControlList {
  /// 引导 default 用户句柄（构造期装配，内存单例，与用户总量无关）
  default_user: Arc<UserHandle>,
}

impl AccessControlList {
  /// 创建访问控制列表并装配引导 default 用户
  ///
  /// libs/server/ACL/AccessControlList.cs:CreateDefaultUserHandle：
  /// default 恒 +@all 且启用；带口令即写入哈希，否则免密
  pub fn new(default_password: &str) -> Result<Self, AclError> {
    // 引导用户在独占可变的构造体上装配，完成后才升为只读共享句柄
    let mut default_user = User::new(DEFAULT_USER_NAME.to_string());
    default_user.add_category(RespAclCategories::ALL)?;
    default_user.set_enabled(true);
    if !default_password.is_empty() {
      default_user.add_password_hash(AclPassword::from_string(default_password));
    } else {
      default_user.set_passwordless(true);
    }
    Ok(Self {
      default_user: Arc::new(UserHandle::new(Arc::new(default_user))),
    })
  }

  /// 引导 default 用户句柄
  ///
  /// libs/server/ACL/AccessControlList.cs:GetDefaultUserHandle
  #[inline]
  pub fn get_default_user_handle(&self) -> Arc<UserHandle> {
    Arc::clone(&self.default_user)
  }

  /// 免认证形态共享的 nopass default 句柄（进程级单例）
  ///
  /// C# NoAuth 档 storeWrapper.accessControlList 依然在场，
  /// AuthenticateUser 兜底臂落 GetDefaultUserHandle（RespServerSession.cs
  /// AuthenticateUser 的 !CanAuthenticate 分支）——rust 免认证档不装配
  /// [`AccessControlList`] 实例，本单例即该兜底句柄的等价承接（nopass
  /// +@all，与 C# CreateDefaultUserHandle 同规格），全会话共享同一 Arc
  pub fn nopass_default_handle() -> Arc<UserHandle> {
    static HANDLE: OnceLock<Arc<UserHandle>> = OnceLock::new();
    Arc::clone(HANDLE.get_or_init(|| {
      let mut user = User::new(DEFAULT_USER_NAME.to_string());
      user
        .add_category(RespAclCategories::ALL)
        .expect("default 用户加 @all 类别不可失败");
      user.set_enabled(true);
      user.set_passwordless(true);
      Arc::new(UserHandle::new(Arc::new(user)))
    }))
  }
}

impl Default for AccessControlList {
  fn default() -> Self {
    Self::new("").expect("failed to create default AccessControlList")
  }
}
