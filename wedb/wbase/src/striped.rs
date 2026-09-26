//! 键哈希分段/条带读写锁原语
//!
//! 槽位经 [`CachePadded`]（128 字节对齐，独立独占双缓存行）包装各子读写锁，
//! 消除多核心高并发场景下相邻条带锁间的 CPU 伪共享 (False Sharing)；
//! 对齐 64 字节场景可换用 [`crate::align::CachePadded64`] 槽位。
//!
//! 逐条带在堆上就地构造，彻底避免大数组先在栈上成型再整体拷贝导致的栈溢出风险。
//!
//! 自研依据: 分条读写锁（C# 语义参照 libs/common/ReaderWriterLock.cs，按缓存行分条）

use std::{
  fmt::{self, Debug, Formatter},
  marker::PhantomData,
  ops::Deref,
  sync::atomic::{AtomicI64, Ordering},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::align::CachePadded;

/// 缓存行对齐的条带化无锁原子计数器
///
/// 常量泛型 `N` 表示条带数量，编译期强制断言 `N` 为大于 0 且为 2 的幂次方，
/// 从而保证掩码寻址 `idx & (N - 1)` 恒定在 `[0, N)` 范围内，杜绝越界与运行时除法开销。
pub struct StripedCounter<const N: usize = 64> {
  slots: [CachePadded<AtomicI64>; N],
}

impl<const N: usize> Default for StripedCounter<N> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<const N: usize> StripedCounter<N> {
  /// 条带掩码（N - 1）
  pub const STRIPE_MASK: usize = N - 1;

  /// 编译期静态断言：条带数必须大于 0 且必须为 2 的幂次方
  /// 常量构造函数，支持直接用于 static / const 全局上下文
  pub const fn new() -> Self {
    const {
      assert!(
        N > 0 && N.is_power_of_two(),
        "StripedCounter 条带数必须为 2 的正整数次幂"
      );
    }

    Self {
      slots: [const { CachePadded::new(AtomicI64::new(0)) }; N],
    }
  }

  /// 获取指定下标对应的槽位索引（0 分支位掩码回绕）
  #[must_use]
  #[inline(always)]
  pub const fn stripe_index(index: usize) -> usize {
    index & Self::STRIPE_MASK
  }

  /// 在指定条带槽位原子累加
  #[inline]
  pub fn add(&self, stripe_index: usize, delta: i64) {
    let idx = Self::stripe_index(stripe_index);
    // SAFETY: STRIPE_MASK 约束下标必然在 [0, N) 范围内
    unsafe { self.slots.get_unchecked(idx) }.fetch_add(delta, Ordering::Relaxed);
  }

  /// 在指定条带槽位原子递减
  #[inline]
  pub fn sub(&self, stripe_index: usize, delta: i64) {
    let idx = Self::stripe_index(stripe_index);
    // SAFETY: STRIPE_MASK 约束下标必然在 [0, N) 范围内
    unsafe { self.slots.get_unchecked(idx) }.fetch_sub(delta, Ordering::Relaxed);
  }

  /// 获取当前所有条带的累加总和
  #[must_use]
  #[inline]
  pub fn get(&self) -> i64 {
    self.slots.iter().map(|s| s.load(Ordering::Relaxed)).sum()
  }

  /// 获取当前所有条带的非负累加总和（小于 0 时饱和为 0）
  #[must_use]
  #[inline]
  pub fn get_positive(&self) -> usize {
    self.get().max(0) as usize
  }

  /// 将所有条带重置为 0
  #[inline]
  pub fn reset(&self) {
    for slot in &self.slots {
      slot.store(0, Ordering::Relaxed);
    }
  }

  /// 返回条带总数
  #[must_use]
  #[inline(always)]
  pub const fn len(&self) -> usize {
    N
  }

  /// 是否为空（由静态断言保证恒为 false）
  #[must_use]
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    false
  }

  /// 获取底层槽位切片只读借用
  #[must_use]
  #[inline(always)]
  pub fn slots(&self) -> &[CachePadded<AtomicI64>] {
    &self.slots
  }
}

impl<const N: usize> Debug for StripedCounter<N> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("StripedCounter")
      .field("len", &N)
      .field("total", &self.get())
      .finish()
  }
}

/// 缓存行对齐的条带锁单槽包装类型
pub type CacheAlignedLock<T> = CachePadded<RwLock<T>>;

/// 针对键哈希分段的读写条带锁（一套泛型基座）
///
/// 常量泛型 `N` 表示条带数量，编译期强制断言 `N` 为大于 0 且为 2 的幂次方，
/// 从而保证哈希掩码寻址 `hash & (N - 1)` 恒定在 `[0, N)` 范围内，杜绝越界与运行时除法开销。
///
/// 泛型槽位 `W` 决定各条带锁的包装与对齐（默认 [`CacheAlignedLock`]：128 字节
/// 对齐的 parking_lot RwLock；对齐 64 字节场景可传 [`crate::align::CachePadded64`] 槽位），
/// 槽位经 `Deref<Target = RwLock<T>>` 统一加解锁面。
pub struct StripedRwLock<T, const N: usize, W = CacheAlignedLock<T>> {
  stripes: Box<[W]>,
  /// 类型关联标记（T 经槽位 W 内的 RwLock 间接存储，零尺寸占位）
  _marker: PhantomData<T>,
}

impl<T, const N: usize, W> Default for StripedRwLock<T, N, W>
where
  T: Default,
  W: From<RwLock<T>> + Deref<Target = RwLock<T>>,
{
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<T, const N: usize, W> StripedRwLock<T, N, W>
where
  W: Deref<Target = RwLock<T>>,
{
  /// 条带掩码（N - 1）
  pub const STRIPE_MASK: usize = N - 1;

  /// 基于自定义闭包初始化各条带实例（逐条带堆上就地构造，避免栈拷贝）
  pub fn with_initializer<F>(mut init: F) -> Self
  where
    F: FnMut(usize) -> T,
    W: From<RwLock<T>>,
  {
    const {
      assert!(
        N > 0 && N.is_power_of_two(),
        "StripedRwLock 条带数必须为 2 的正整数次幂"
      );
    }

    let mut stripes = Vec::with_capacity(N);
    stripes.extend((0..N).map(|i| W::from(RwLock::new(init(i)))));
    Self {
      stripes: stripes.into_boxed_slice(),
      _marker: PhantomData,
    }
  }

  /// 获取指定键哈希对应的条带下标（0 分支掩码快速寻址）
  #[must_use]
  #[inline(always)]
  pub const fn stripe_index(hash: u64) -> usize {
    (hash as usize) & Self::STRIPE_MASK
  }

  /// 获取指定键哈希对应的共享读锁
  #[inline]
  pub fn read(&self, hash: u64) -> RwLockReadGuard<'_, T> {
    self.read_at(Self::stripe_index(hash))
  }

  /// 获取指定键哈希对应的独占写锁
  #[inline]
  pub fn write(&self, hash: u64) -> RwLockWriteGuard<'_, T> {
    self.write_at(Self::stripe_index(hash))
  }

  /// 获取指定条带下标对应的共享读锁（掩码回绕）
  #[inline]
  pub fn read_at(&self, index: usize) -> RwLockReadGuard<'_, T> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.read()
  }

  /// 获取指定条带下标对应的独占写锁（掩码回绕）
  #[inline]
  pub fn write_at(&self, index: usize) -> RwLockWriteGuard<'_, T> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.write()
  }

  /// 尝试获取指定条带下标对应的共享读锁（掩码回绕）
  #[inline]
  pub fn try_read_at(&self, index: usize) -> Option<RwLockReadGuard<'_, T>> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.try_read()
  }

  /// 尝试获取指定键哈希对应的共享读锁（非阻塞：被占即返回 None）
  #[inline]
  pub fn try_read(&self, hash: u64) -> Option<RwLockReadGuard<'_, T>> {
    self.try_read_at(Self::stripe_index(hash))
  }

  /// 尝试获取指定键哈希对应的独占写锁（非阻塞：被占即返回 None）
  #[inline]
  pub fn try_write(&self, hash: u64) -> Option<RwLockWriteGuard<'_, T>> {
    self.try_write_at(Self::stripe_index(hash))
  }

  /// 尝试获取指定条带下标对应的独占写锁（掩码回绕）
  #[inline]
  pub fn try_write_at(&self, index: usize) -> Option<RwLockWriteGuard<'_, T>> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.try_write()
  }

  /// 返回条带总数
  #[must_use]
  #[inline(always)]
  pub const fn len(&self) -> usize {
    N
  }

  /// 是否为空（由静态断言保证恒为 false）
  #[must_use]
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    false
  }
}

impl<T, const N: usize, W> Debug for StripedRwLock<T, N, W> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("StripedRwLock").field("len", &N).finish()
  }
}

impl<T, const N: usize, W> StripedRwLock<T, N, W>
where
  T: Default,
  W: From<RwLock<T>> + Deref<Target = RwLock<T>>,
{
  /// 创建新的条带锁实例（各条带默认值构造）
  pub fn new() -> Self {
    Self::with_initializer(|_| T::default())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_striped_rwlock_basic() {
    let lock = StripedRwLock::<i32, 16>::new();
    assert_eq!(lock.len(), 16);
    assert!(!lock.is_empty());
    {
      let mut w = lock.write(0);
      *w = 42;
    }
    {
      let r1 = lock.read(0);
      let r2 = lock.read_at(16); // 回绕到 0
      assert_eq!(*r1, 42);
      assert_eq!(*r2, 42);
    }
  }

  #[test]
  fn test_striped_rwlock_try_lock() {
    let lock = StripedRwLock::<(), 16>::new();
    let idx = StripedRwLock::<(), 16>::stripe_index(1);
    let r = lock.try_read_at(idx);
    assert!(r.is_some());
    // 读锁存在时，写锁失败
    assert!(lock.try_write_at(idx).is_none());
    drop(r);
    // 释放后写锁成功
    let w = lock.try_write_at(idx);
    assert!(w.is_some());
    assert!(lock.try_read_at(idx).is_none());
    drop(w);
    assert!(lock.try_read_at(idx).is_some());
  }

  #[test]
  fn test_striped_rwlock_padded64_slot() {
    use crate::align::CachePadded64;

    // 泛型槽位：64 字节对齐 CachePadded64
    let lock = StripedRwLock::<i32, 8, CachePadded64<RwLock<i32>>>::new();
    assert_eq!(lock.len(), 8);
    {
      let mut w = lock.write_at(3);
      *w = 7;
    }
    assert_eq!(*lock.read_at(3), 7);
    // 默认槽位：128 字节对齐 CacheAlignedLock
    let default_slot_lock = StripedRwLock::<(), 8>::new();
    let r = default_slot_lock.try_read_at(2);
    assert!(r.is_some());
    // 读锁存续期间写锁互斥
    assert!(default_slot_lock.try_write_at(2).is_none());
    drop(r);
    // 释放后写锁成功
    assert!(default_slot_lock.try_write_at(2).is_some());
  }
}
