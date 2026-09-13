//! 用户句柄（对标 libs/server/ACL/UserHandle.cs）

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use super::user::User;

/// 用户引用句柄：读侧无锁快照，写侧 CAS 换新
pub struct UserHandle {
  /// 当前指向的用户（对标 C# UserHandle.user 引用）
  user: ArcSwap<User>,
}

impl UserHandle {
  /// 构造句柄（对标 C# UserHandle 构造；user 为空即 panic，与
  /// ArgumentNullException 语义一致——此处由类型系统保证非空）
  pub fn new(user: Arc<User>) -> Self {
    Self {
      user: ArcSwap::new(user),
    }
  }

  /// 取当前用户（最新版本，对标 C# UserHandle.User 属性）
  #[inline]
  pub fn user(&self) -> Arc<User> {
    self.user.load_full()
  }

  /// 借用当前用户守卫（零引用计数增减，适合瞬态读取）
  #[inline]
  pub fn load(&self) -> Guard<Arc<User>> {
    self.user.load()
  }

  /// CAS 换新：仅当当前用户与 `expected` 同一实例时替换为 `new_user`
  ///
  /// libs/server/ACL/UserHandle.cs:TrySetUser
  pub fn try_set_user(&self, new_user: Arc<User>, expected: &Arc<User>) -> bool {
    let prev = self.user.compare_and_swap(expected, new_user);
    Arc::ptr_eq(&prev, expected)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn try_set_user_swaps_only_matching_instance() {
    let u1 = Arc::new(User::new("a".into()));
    let u2 = Arc::new(User::new("a".into()));
    let h = UserHandle::new(u1.clone());
    assert!(Arc::ptr_eq(&h.user(), &u1));

    // 期望实例不符 → 替换失败
    assert!(!h.try_set_user(u2.clone(), &u2));
    assert!(Arc::ptr_eq(&h.user(), &u1));

    // 期望实例相符 → 替换成功
    assert!(h.try_set_user(u2.clone(), &u1));
    assert!(Arc::ptr_eq(&h.user(), &u2));
  }
}
