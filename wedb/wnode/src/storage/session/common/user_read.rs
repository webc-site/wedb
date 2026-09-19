//! 用户数据与物理标签域同步读状态抽象
//!
//! 将存储层底层嵌套三态 / 五态折叠为类型化枚举，
//! 消除命令层（GET/GETEX/GETDEL/INCR/BITMAP 等）反复手写的嵌套 match 样板。

use wdev::Device;
use wkv::{BatchStoreSession, StoreResult};
use wval::KeyTag;

use super::ttl_sync::{
  read_adjudicated_tag_sync, read_adjudicated_user_sync, read_adjudicated_user_sync_with_prefix,
};

/// 用户数据双域读结果（String 域与 ObjectEnvelope 域判型折叠）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserRead<R> {
  /// String 域命中（值内容任意）
  Hit(R),
  /// 键缺失（两域皆缺或已过期）
  Missing,
  /// 集合对象键（信封域命中，C# ValueIsObject → WRONGTYPE）
  WrongType,
  /// 磁盘候选 / TTL 待异步裁决，须降级异步
  Deferred,
}

impl<R> UserRead<R> {
  /// 从存储层双域读权威状态枚举转换
  ///
  /// Ok(StoreResult::Success(Ok(v))) -> Ok(UserRead::Hit(v))
  /// Ok(StoreResult::Success(Err(()))) -> Ok(UserRead::WrongType)
  /// Ok(StoreResult::NotFound) -> Ok(UserRead::Missing)
  /// Ok(StoreResult::RecordOnDisk) -> Ok(UserRead::Deferred)
  /// Err(e) -> Err(e)
  #[inline]
  pub fn from_result<E>(res: Result<StoreResult<Result<R, ()>>, E>) -> Result<Self, E> {
    match res {
      Ok(StoreResult::Success(Ok(v))) => Ok(Self::Hit(v)),
      Ok(StoreResult::Success(Err(()))) => Ok(Self::WrongType),
      Ok(StoreResult::NotFound) => Ok(Self::Missing),
      Ok(StoreResult::RecordOnDisk) => Ok(Self::Deferred),
      Err(e) => Err(e),
    }
  }

  /// 映射 Hit 变体包含的值
  #[inline]
  pub fn map<U>(self, f: impl FnOnce(R) -> U) -> UserRead<U> {
    match self {
      Self::Hit(v) => UserRead::Hit(f(v)),
      Self::Missing => UserRead::Missing,
      Self::WrongType => UserRead::WrongType,
      Self::Deferred => UserRead::Deferred,
    }
  }

  /// 是否为 String 域命中
  #[inline]
  pub const fn is_hit(&self) -> bool {
    matches!(self, Self::Hit(_))
  }

  /// 是否为键缺失或过期
  #[inline]
  pub const fn is_missing(&self) -> bool {
    matches!(self, Self::Missing)
  }

  /// 是否为对象键（信封域命中）
  #[inline]
  pub const fn is_wrong_type(&self) -> bool {
    matches!(self, Self::WrongType)
  }

  /// 是否须降级异步
  #[inline]
  pub const fn is_deferred(&self) -> bool {
    matches!(self, Self::Deferred)
  }

  /// 提取 Hit 值，若非 Hit 则返回 None
  #[inline]
  pub fn ok(self) -> Option<R> {
    match self {
      Self::Hit(v) => Some(v),
      _ => None,
    }
  }
}

/// 用户数据双域异步读结果（[`UserRead`] 的异步闭环子集：磁盘候选已在
/// 异步读内核内闭环，无 `Deferred` 降级态）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserReadAsync<R> {
  /// String 域命中（值内容任意）
  Hit(R),
  /// 键缺失（两域皆缺或已过期）
  Missing,
  /// 集合对象键（信封 / 升阶 Meta 域命中，C# ValueIsObject → WRONGTYPE）
  WrongType,
}

/// 物理标签域单域读结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagRead<R> {
  /// 标签域命中
  Hit(R),
  /// 键缺失或已过期
  Missing,
  /// 磁盘候选 / TTL 待异步裁决，须降级异步
  Deferred,
}

impl<R> TagRead<R> {
  /// 从存储层标签域读权威状态枚举转换
  ///
  /// Ok(StoreResult::Success(v)) -> Ok(TagRead::Hit(v))
  /// Ok(StoreResult::NotFound) -> Ok(TagRead::Missing)
  /// Ok(StoreResult::RecordOnDisk) -> Ok(TagRead::Deferred)
  /// Err(e) -> Err(e)
  #[inline]
  pub fn from_result<E>(res: Result<StoreResult<R>, E>) -> Result<Self, E> {
    match res {
      Ok(StoreResult::Success(v)) => Ok(Self::Hit(v)),
      Ok(StoreResult::NotFound) => Ok(Self::Missing),
      Ok(StoreResult::RecordOnDisk) => Ok(Self::Deferred),
      Err(e) => Err(e),
    }
  }

  /// 映射 Hit 变体包含的值
  #[inline]
  pub fn map<U>(self, f: impl FnOnce(R) -> U) -> TagRead<U> {
    match self {
      Self::Hit(v) => TagRead::Hit(f(v)),
      Self::Missing => TagRead::Missing,
      Self::Deferred => TagRead::Deferred,
    }
  }

  /// 是否为标签域命中
  #[inline]
  pub const fn is_hit(&self) -> bool {
    matches!(self, Self::Hit(_))
  }

  /// 是否为键缺失或过期
  #[inline]
  pub const fn is_missing(&self) -> bool {
    matches!(self, Self::Missing)
  }

  /// 是否须降级异步
  #[inline]
  pub const fn is_deferred(&self) -> bool {
    matches!(self, Self::Deferred)
  }

  /// 提取 Hit 值，若非 Hit 则返回 None
  #[inline]
  pub fn ok(self) -> Option<R> {
    match self {
      Self::Hit(v) => Some(v),
      _ => None,
    }
  }
}

/// 带 TTL 裁决的用户数据双域同步读（折叠为 UserRead 四态）
///
/// 附着一致读会话时单键 pre/post 协议在双域回合外触发（超时上抛中止；
/// 两域单回合对位 C# UnifiedStore 一致读）
#[inline]
pub fn read_user_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<UserRead<R>> {
  let res =
    session.with_session_consistent_read(key, || read_adjudicated_user_sync(session, key, f))?;
  UserRead::from_result(res)
}

/// 带 TTL 裁决的用户数据双域同步读的显式前缀变体（循环前缀外提对位，语义与
/// [`read_user_sync`] 一致；rust 工程优化无 c# 对应：批量遍历单次外提
/// `session_prefix()` 消除逐键重读 ns/db 原子变量与重算 Varint）
#[inline]
pub fn read_user_sync_with_prefix<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<UserRead<R>> {
  let res = session.with_session_consistent_read(key, || {
    read_adjudicated_user_sync_with_prefix(session, prefix, key, f)
  })?;
  UserRead::from_result(res)
}

/// 带 TTL 裁决的指定物理标签域同步读（折叠为 TagRead 三态）
///
/// 附着一致读会话时单键 pre/post 协议触发（超时上抛中止）
#[inline]
pub fn read_tag_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: KeyTag,
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<TagRead<R>> {
  let res = session
    .with_session_consistent_read(key, || read_adjudicated_tag_sync(session, key, tag, f))?;
  TagRead::from_result(res)
}

/// 带 TTL 裁决的对象信封域同步读（折叠为 TagRead 三态）
#[inline]
pub fn read_envelope_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<TagRead<R>> {
  read_tag_sync(session, key, KeyTag::ObjectEnvelope, f)
}

#[cfg(test)]
mod tests {
  use wkv::StoreResult;

  use super::{TagRead, UserRead};

  #[test]
  fn user_read_from_result_mappings() {
    assert_eq!(
      UserRead::from_result::<()>(Ok(StoreResult::Success(Ok(42)))),
      Ok(UserRead::Hit(42))
    );
    assert_eq!(
      UserRead::<i32>::from_result::<()>(Ok(StoreResult::Success(Err(())))),
      Ok(UserRead::WrongType)
    );
    assert_eq!(
      UserRead::<i32>::from_result::<()>(Ok(StoreResult::NotFound)),
      Ok(UserRead::Missing)
    );
    assert_eq!(
      UserRead::<i32>::from_result::<()>(Ok(StoreResult::RecordOnDisk)),
      Ok(UserRead::Deferred)
    );
    assert_eq!(
      UserRead::<i32>::from_result::<&str>(Err("disk error")),
      Err("disk error")
    );
  }

  #[test]
  fn user_read_methods() {
    let hit = UserRead::Hit(10);
    assert!(hit.is_hit());
    assert!(!hit.is_missing());
    assert!(!hit.is_wrong_type());
    assert!(!hit.is_deferred());
    assert_eq!(hit.ok(), Some(10));
    assert_eq!(hit.map(|x| x * 2), UserRead::Hit(20));

    let missing = UserRead::<i32>::Missing;
    assert!(missing.is_missing());
    assert_eq!(missing.ok(), None);
    assert_eq!(missing.map(|x| x * 2), UserRead::Missing);

    let wrong = UserRead::<i32>::WrongType;
    assert!(wrong.is_wrong_type());
    assert_eq!(wrong.ok(), None);

    let deferred = UserRead::<i32>::Deferred;
    assert!(deferred.is_deferred());
    assert_eq!(deferred.ok(), None);
  }

  #[test]
  fn tag_read_from_result_mappings() {
    assert_eq!(
      TagRead::from_result::<()>(Ok(StoreResult::Success(42))),
      Ok(TagRead::Hit(42))
    );
    assert_eq!(
      TagRead::<i32>::from_result::<()>(Ok(StoreResult::NotFound)),
      Ok(TagRead::Missing)
    );
    assert_eq!(
      TagRead::<i32>::from_result::<()>(Ok(StoreResult::RecordOnDisk)),
      Ok(TagRead::Deferred)
    );
    assert_eq!(
      TagRead::<i32>::from_result::<&str>(Err("disk error")),
      Err("disk error")
    );
  }

  #[test]
  fn tag_read_methods() {
    let hit = TagRead::Hit(10);
    assert!(hit.is_hit());
    assert!(!hit.is_missing());
    assert!(!hit.is_deferred());
    assert_eq!(hit.ok(), Some(10));
    assert_eq!(hit.map(|x| x * 2), TagRead::Hit(20));

    let missing = TagRead::<i32>::Missing;
    assert!(missing.is_missing());
    assert_eq!(missing.ok(), None);

    let deferred = TagRead::<i32>::Deferred;
    assert!(deferred.is_deferred());
    assert_eq!(deferred.ok(), None);
  }
}
