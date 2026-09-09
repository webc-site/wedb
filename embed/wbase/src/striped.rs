//! 键哈希分段/条带读写锁原语
//!
//! 采用 CPU 缓存行 64 字节对齐的 [`CachePadded`] 包装各个子读写锁，
//! 消除多核心高并发场景下相邻条带锁间的 CPU 伪共享 (False Sharing)。
//!
//! 逐条带在堆上就地构造，彻底避免大数组先在栈上成型再整体拷贝导致的栈溢出风险。

use std::{
  fmt::{self, Debug, Formatter},
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
  /// 编译期静态断言：条带数必须大于 0 且必须为 2 的幂次方
  const _ASSERT_POWER_OF_TWO: () = assert!(
    N > 0 && (N & (N - 1)) == 0,
    "StripedCounter 条带数必须为 2 的正整数次幂"
  );

  /// 条带掩码（N - 1）
  pub const STRIPE_MASK: usize = N - 1;

  /// 常量构造函数，支持直接用于 static / const 全局上下文
  pub const fn new() -> Self {
    #[expect(clippy::let_unit_value, reason = "编译期触发静态断言求值")]
    let _ = Self::_ASSERT_POWER_OF_TWO;

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

/// 针对键哈希分段的读写条带锁
///
/// 常量泛型 `N` 表示条带数量，编译期强制断言 `N` 为大于 0 且为 2 的幂次方，
/// 从而保证哈希掩码寻址 `hash & (N - 1)` 恒定在 `[0, N)` 范围内，杜绝越界与运行时除法开销。
pub struct StripedRwLock<T = (), const N: usize = 128> {
  stripes: Box<[CacheAlignedLock<T>]>,
}

impl<T: Default, const N: usize> Default for StripedRwLock<T, N> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<T, const N: usize> StripedRwLock<T, N> {
  /// 编译期静态断言：条带数必须大于 0 且必须为 2 的幂次方
  const _ASSERT_POWER_OF_TWO: () = assert!(
    N > 0 && (N & (N - 1)) == 0,
    "StripedRwLock 条带数必须为 2 的正整数次幂"
  );

  /// 条带掩码（N - 1）
  pub const STRIPE_MASK: usize = N - 1;

  /// 基于自定义闭包初始化各条带实例（逐条带堆上就地构造，避免栈拷贝）
  pub fn with_initializer<F>(mut init: F) -> Self
  where
    F: FnMut(usize) -> T,
  {
    #[expect(clippy::let_unit_value, reason = "编译期触发静态断言求值")]
    let _ = Self::_ASSERT_POWER_OF_TWO;

    let mut stripes = Vec::with_capacity(N);
    stripes.extend((0..N).map(|i| CachePadded::new(RwLock::new(init(i)))));
    Self {
      stripes: stripes.into_boxed_slice(),
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
    let idx = Self::stripe_index(hash);
    // SAFETY: STRIPE_MASK 保证下标严格落在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.0.read()
  }

  /// 获取指定键哈希对应的独占写锁
  #[inline]
  pub fn write(&self, hash: u64) -> RwLockWriteGuard<'_, T> {
    let idx = Self::stripe_index(hash);
    // SAFETY: STRIPE_MASK 保证下标严格落在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.0.write()
  }

  /// 获取指定条带下标对应的共享读锁（按需模除回绕）
  #[inline]
  pub fn read_at(&self, index: usize) -> RwLockReadGuard<'_, T> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.0.read()
  }

  /// 获取指定条带下标对应的独占写锁（按需模除回绕）
  #[inline]
  pub fn write_at(&self, index: usize) -> RwLockWriteGuard<'_, T> {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }.0.write()
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

  /// 获取底层条带切片只读借用
  #[must_use]
  #[inline(always)]
  pub fn stripes(&self) -> &[CacheAlignedLock<T>] {
    &self.stripes
  }
}

impl<T, const N: usize> Debug for StripedRwLock<T, N> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("StripedRwLock").field("len", &N).finish()
  }
}

impl<T: Default, const N: usize> StripedRwLock<T, N> {
  /// 创建新的条带锁实例（各条带默认值构造）
  pub fn new() -> Self {
    Self::with_initializer(|_| T::default())
  }
}
