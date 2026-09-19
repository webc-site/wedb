//! 用户句柄（对标 libs/server/ACL/UserHandle.cs）

use std::sync::Arc;

use super::user::User;

/// 用户引用句柄：构造即定格的只读快照
///
/// 与 C# 的形态差异：C# 句柄全局共享，跨线程 CAS 换新（TrySetUser）让全部
/// 持有者即时见新权限；rust 按转写规范走「存储单点 + 会话本地句柄」——
/// 句柄连接独占，命令串行处理无并发写者，换代 = 重读存储后整体替换句柄
/// （[`crate::resp`] 侧 set_user_handle），无共享 CAS 域，故 TrySetUser 不移植。
pub struct UserHandle {
  /// 当前指向的用户（对标 C# UserHandle.user 引用）
  user: Arc<User>,
}

impl UserHandle {
  /// 构造句柄（对标 C# UserHandle 构造；user 为空即 panic，与
  /// ArgumentNullException 语义一致——此处由类型系统保证非空）
  pub fn new(user: Arc<User>) -> Self {
    Self { user }
  }

  /// 取当前用户（最新版本，对标 C# UserHandle.User 属性）
  #[inline]
  pub fn user(&self) -> &Arc<User> {
    &self.user
  }

  /// 借用当前用户（零克隆瞬态读取）
  #[inline]
  pub fn load(&self) -> &Arc<User> {
    &self.user
  }
}
