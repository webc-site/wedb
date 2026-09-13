//! 用户句柄扩展原语（对标 libs/server/ACL/UserHandle.cs:UserHandle）
//!
//! [`UserHandle`]（`RwLock<Arc<User>>` 别名，定义于 crate 根）承载会话侧用户引用；
//! 本模块在其上提供 C# `TrySetUser` 的 CAS 换新原语：ACL SETUSER 重建用户对象后
//! 原子换新，持旧引用的会话在下次取用时无感切换，杜绝半更新视图。

use std::sync::Arc;

use super::user::User;

/// 用户句柄的 CAS 换新扩展（对标 libs/server/ACL/UserHandle.cs:TrySetUser）
pub trait UserHandleExt {
  /// 原子换新：当前用户与 `replaced` 为同一实例时替换为 `new_user` 并返回 true
  ///
  /// 对标 C# `Interlocked.CompareExchange(ref user, newUser, replacedUser) == replacedUser`；
  /// 并发竞争失败时调用方应重取当前用户、重建副本后重试
  ///
  /// libs/server/ACL/UserHandle.cs:TrySetUser
  fn try_set_user(&self, new_user: Arc<User>, replaced: &Arc<User>) -> bool;
}

impl UserHandleExt for super::UserHandle {
  fn try_set_user(&self, new_user: Arc<User>, replaced: &Arc<User>) -> bool {
    let mut cur = self.write();
    if !Arc::ptr_eq(&cur, replaced) {
      return false;
    }
    *cur = new_user;
    true
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use parking_lot::RwLock;

  use super::{User, UserHandleExt};

  type UserHandle = RwLock<Arc<User>>;

  /// TrySetUser：实例一致才换新，竞争失败返回 false
  #[test]
  fn try_set_user_cas_semantics() {
    let u0 = Arc::new(User::new("u".into()));
    let handle = UserHandle::new(u0.clone());
    assert!(Arc::ptr_eq(&*handle.read(), &u0));

    // 期望旧实例一致 → 换新成功
    let u1 = Arc::new(User::new("u".into()));
    assert!(handle.try_set_user(u1.clone(), &u0));
    assert!(Arc::ptr_eq(&*handle.read(), &u1));

    // 期望值已过期 → 拒绝换新
    let u2 = Arc::new(User::new("u".into()));
    assert!(!handle.try_set_user(u2, &u0));
    assert!(Arc::ptr_eq(&*handle.read(), &u1));
  }
}
