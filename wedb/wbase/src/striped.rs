//! 键哈希分段/条带读写锁原语
//!
//! 槽位经 [`CachePadded`]（128 字节对齐，独立独占双缓存行）包装各子读写锁，
//! 消除多核心高并发场景下相邻条带锁间的 CPU 伪共享 (False Sharing)；
//! 对齐 64 字节场景可换用 [`crate::align::CachePadded64`] 槽位。
//!
//! 逐条带在堆上就地构造，彻底避免大数组先在栈上成型再整体拷贝导致的栈溢出风险。
//! 该职责与条带数 2 的幂编译期断言、`STRIPE_MASK` 掩码寻址、`CachePadded`
//! 对齐槽一并由 [`StripedTable`] 单点承载（跨仓条带锁域共用，槽位类型参数化）。
//!
//! 自研依据: 分条读写锁（C# 语义参照 libs/common/ReaderWriterLock.cs，按缓存行分条）

use std::{
  fmt::{self, Debug, Formatter},
  marker::PhantomData,
  ops::Deref,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::align::CachePadded;

/// 条带数编译期校验单点（所有条带基座共用：N 必须大于 0 且为 2 的幂，
/// 保证掩码寻址 `idx & (N - 1)` 恒定在 `[0, N)` 范围内，杜绝越界与除法开销）
const fn assert_stripe_count<const N: usize>() {
  assert!(N > 0 && N.is_power_of_two(), "条带数必须为 2 的正整数次幂");
}

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
    const { assert_stripe_count::<N>() }

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

/// 条带表通用基座：条带数组 / 掩码寻址 / 2 的幂编译期断言三件套的单点承载
///
/// 与锁原语解耦：槽位 `SLOT` 类型参数化，本基座不含任何锁语义。wbase 内部
/// [`StripedRwLock`]（parking_lot 同步基座）与跨仓条带锁域（如 wnode 向量域
/// 的 async_lock 异步槽位，守卫跨 `.await` 存活）各自在槽位内承载锁本体，
/// 共享本基座的条带数编译期断言、`STRIPE_MASK` 位与寻址与逐条带堆上就地
/// 构造（杜绝大数组先在栈上成型再整体拷贝导致的栈溢出风险）；槽位对齐面
/// 统一经 [`CachePadded`]（128 字节档，[`crate::align::CachePadded64`] 可换档）。
/// `Arc` 薄指针存储：`size_of::<Self>() == 8`，克隆即共享同一条带表。
pub struct StripedTable<SLOT, const N: usize> {
  stripes: Arc<[SLOT; N]>,
}

impl<SLOT, const N: usize> StripedTable<SLOT, N> {
  /// 条带掩码（N - 1）
  pub const STRIPE_MASK: usize = N - 1;

  /// 逐条带堆上就地构造（`init` 逐槽生成，避免栈上整体成型）
  pub fn with_initializer<F>(init: F) -> Self
  where
    F: FnMut(usize) -> SLOT,
  {
    const { assert_stripe_count::<N>() }

    // Vec 逐槽堆上生成后整块收编为定长 Box（len 恒为 N，不越栈），再入 Arc
    let mut stripes = Vec::with_capacity(N);
    stripes.extend((0..N).map(init));
    let boxed = match Box::<[SLOT; N]>::try_from(stripes) {
      Ok(boxed) => boxed,
      Err(_) => unreachable!("条带数恒为 N（构造序不变量）"),
    };
    Self {
      stripes: Arc::from(boxed),
    }
  }

  /// 获取指定下标对应的条带索引（0 分支位掩码回绕）
  #[must_use]
  #[inline(always)]
  pub const fn stripe_index(index: usize) -> usize {
    index & Self::STRIPE_MASK
  }

  /// 获取指定条带下标的槽位引用（掩码截断后免越界检查）
  #[must_use]
  #[inline(always)]
  pub fn slot_at(&self, index: usize) -> &SLOT {
    let idx = index & Self::STRIPE_MASK;
    // SAFETY: 经 STRIPE_MASK 截断，严格保证下标在 [0, N) 范围内
    unsafe { self.stripes.get_unchecked(idx) }
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

impl<SLOT, const N: usize> Debug for StripedTable<SLOT, N> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("StripedTable").field("len", &N).finish()
  }
}

/// 针对键哈希分段的读写条带锁（一套泛型基座）
///
/// 常量泛型 `N` 表示条带数量，编译期强制断言 `N` 为大于 0 且为 2 的幂次方，
/// 从而保证哈希掩码寻址 `hash & (N - 1)` 恒定在 `[0, N)` 范围内，杜绝越界与运行时除法开销。
///
/// 泛型槽位 `W` 决定各条带锁的包装与对齐（默认 [`CacheAlignedLock`]：128 字节
/// 对齐的 parking_lot RwLock；对齐 64 字节场景可传 [`crate::align::CachePadded64`] 槽位），
/// 槽位经 `Deref<Target = RwLock<T>>` 统一加解锁面。
pub struct StripedRwLock<T, const N: usize, W = CacheAlignedLock<T>> {
  /// 条带基座（数组/掩码/断言三件套由 [`StripedTable`] 单点承载，Arc 薄指针存储）
  stripes: StripedTable<W, N>,
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
  /// 条带掩码（N - 1，[`StripedTable`] 单点）
  pub const STRIPE_MASK: usize = StripedTable::<W, N>::STRIPE_MASK;

  /// 基于自定义闭包初始化各条带实例（逐条带堆上就地构造，避免栈拷贝）
  ///
  /// 生产面零直呼（[`Self::new`] 的内部组合点），跨仓消费仅测试（wbase tests），
  /// `#[doc(hidden)]` 声明测试面：文档不承诺、可见性保留
  #[doc(hidden)]
  pub fn with_initializer<F>(mut init: F) -> Self
  where
    F: FnMut(usize) -> T,
    W: From<RwLock<T>>,
  {
    Self {
      // 条带数 2 的幂编译期断言与堆上就地构造均由 StripedTable 单点承载
      stripes: StripedTable::with_initializer(move |i| W::from(RwLock::new(init(i)))),
      _marker: PhantomData,
    }
  }

  /// 获取指定键哈希对应的条带下标（0 分支掩码快速寻址）
  #[must_use]
  #[inline(always)]
  pub const fn stripe_index(hash: u64) -> usize {
    StripedTable::<W, N>::stripe_index(hash as usize)
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
    self.stripes.slot_at(index).read()
  }

  /// 获取指定条带下标对应的独占写锁（掩码回绕）
  #[inline]
  pub fn write_at(&self, index: usize) -> RwLockWriteGuard<'_, T> {
    self.stripes.slot_at(index).write()
  }

  /// 尝试获取指定条带下标对应的共享读锁（掩码回绕）
  ///
  /// 生产面零直呼（[`Self::try_read`] 的内部组合点），跨仓消费仅测试（wbase tests），
  /// `#[doc(hidden)]` 声明测试面：文档不承诺、可见性保留
  #[doc(hidden)]
  #[inline]
  pub fn try_read_at(&self, index: usize) -> Option<RwLockReadGuard<'_, T>> {
    self.stripes.slot_at(index).try_read()
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
  ///
  /// 生产面零直呼（[`Self::try_write`] 的内部组合点），测试面声明同 [`Self::try_read_at`]
  #[doc(hidden)]
  #[inline]
  pub fn try_write_at(&self, index: usize) -> Option<RwLockWriteGuard<'_, T>> {
    self.stripes.slot_at(index).try_write()
  }

  /// 返回条带总数
  #[must_use]
  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.stripes.len()
  }

  /// 是否为空（由静态断言保证恒为 false）
  #[must_use]
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.stripes.is_empty()
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
