//! 栈优先/堆回退双态字节缓冲区公共基础设施
//!
//! 提供 [`StackHeapBuf`] 常量泛型类型：小字节栈分配、超长自动回退至堆的紧凑二进制缓冲区，
//! 各固容量实例经类型别名固化（如 `type KeyBufRepr = StackHeapBuf<62>;`）。
//! 切片同形：`Deref` / `Borrow` / `AsRef` 到 `[u8]`，`PartialEq` / `Ord` / `Hash` 按内容比对。
//! 当启用 `simd` 特性时，缓冲区内容比对自动启用硬件加速向量比对。

use core::{
  borrow::Borrow,
  cmp::Ordering,
  hash::{Hash, Hasher},
  ops::Deref,
};

#[cfg(feature = "simd")]
use crate::simd::fast_key_eq;

/// 缓冲区切片相等性比对辅助（支持按需启用 SIMD 加速）
#[inline(always)]
fn slice_eq(a: &[u8], b: &[u8]) -> bool {
  #[cfg(feature = "simd")]
  {
    fast_key_eq(a, b)
  }
  #[cfg(not(feature = "simd"))]
  {
    a == b
  }
}

/// 栈优先/堆回退双态字节缓冲区（栈容量 `CAP` <= 255，长度以 u8 承载）
///
/// 变体 `#[non_exhaustive]`：跨 crate 构造统一走 [`from_stack`](Self::from_stack) /
/// [`from_heap`](Self::from_heap) / `From` 系列（均含编译期容量断言），
/// 杜绝绕过断言直接枚举构造；同 crate 内与 `matches!` 模式判定不受限
#[derive(Debug, Clone)]
pub enum StackHeapBuf<const CAP: usize> {
  /// 栈分配（总字节数 <= 上限）
  #[non_exhaustive]
  Stack([u8; CAP], u8),
  /// 堆分配（超长键自动回退）
  #[non_exhaustive]
  Heap(Vec<u8>),
}

impl<const CAP: usize> StackHeapBuf<CAP> {
  /// 编译期校验栈容量可由 u8 长度字节承载（各构造路径统一调用）
  #[inline(always)]
  const fn assert_cap() {
    const { assert!(CAP <= 255, "StackHeapBuf 栈容量不得超过 255 字节") }
  }

  /// 从栈缓冲与长度构造（`len` 应为实际填充字节数，`<= CAP`）
  #[inline(always)]
  pub const fn from_stack(buf: [u8; CAP], len: u8) -> Self {
    Self::assert_cap();
    Self::Stack(buf, len)
  }

  /// 从堆 `Vec` 构造（超长回退路径）
  #[inline(always)]
  pub const fn from_heap(vec: Vec<u8>) -> Self {
    Self::assert_cap();
    Self::Heap(vec)
  }

  /// 获取只读切片借用（零拷贝）
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    self
  }

  /// 获取可变切片借用（零拷贝；仅暴露有效长度区间，杜绝越界写穿填充区）
  #[inline(always)]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    match self {
      Self::Stack(buf, len) => {
        let len = (*len as usize).min(CAP);
        // 安全性保证：len 经 min(CAP) 约束，恒满足 len <= buf.len()
        unsafe { buf.get_unchecked_mut(..len) }
      }
      Self::Heap(vec) => vec.as_mut_slice(),
    }
  }

  /// 获取缓冲区字节长度
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.as_slice().len()
  }

  /// 缓冲区是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 当前是否分配在栈上
  #[inline(always)]
  pub const fn is_stack(&self) -> bool {
    matches!(self, Self::Stack(..))
  }

  /// 当前是否回退到堆分配
  #[inline(always)]
  pub const fn is_heap(&self) -> bool {
    matches!(self, Self::Heap(..))
  }

  /// 消耗自身转换为 Vec<u8>（若原为堆分配则零额外分配转移所有权）
  #[inline]
  pub fn into_vec(self) -> Vec<u8> {
    match self {
      Self::Stack(buf, len) => {
        let len = (len as usize).min(CAP);
        // 安全性保证：len 经 min(CAP) 约束，恒满足 len <= buf.len()
        unsafe { buf.get_unchecked(..len) }.to_vec()
      }
      Self::Heap(vec) => vec,
    }
  }
}

impl<const CAP: usize> Deref for StackHeapBuf<CAP> {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    match self {
      Self::Stack(buf, len) => {
        let len = (*len as usize).min(CAP);
        // 安全性保证：len 经 min(CAP) 约束，恒满足 len <= buf.len()
        unsafe { buf.get_unchecked(..len) }
      }
      Self::Heap(vec) => vec.as_slice(),
    }
  }
}

impl<const CAP: usize> AsRef<[u8]> for StackHeapBuf<CAP> {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl<const CAP: usize> Borrow<[u8]> for StackHeapBuf<CAP> {
  #[inline(always)]
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}

impl<const CAP: usize> PartialEq for StackHeapBuf<CAP> {
  #[inline(always)]
  fn eq(&self, other: &Self) -> bool {
    slice_eq(self.as_slice(), other.as_slice())
  }
}

impl<const CAP: usize> Eq for StackHeapBuf<CAP> {}

impl<const CAP: usize> PartialOrd for StackHeapBuf<CAP> {
  #[inline(always)]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<const CAP: usize> Ord for StackHeapBuf<CAP> {
  #[inline(always)]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}

impl<const CAP: usize> Hash for StackHeapBuf<CAP> {
  #[inline(always)]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state);
  }
}

impl<const CAP: usize> From<Vec<u8>> for StackHeapBuf<CAP> {
  #[inline]
  fn from(vec: Vec<u8>) -> Self {
    Self::assert_cap();
    if vec.len() <= CAP {
      let mut buf = [0u8; CAP];
      buf[..vec.len()].copy_from_slice(&vec);
      Self::Stack(buf, vec.len() as u8)
    } else {
      Self::Heap(vec)
    }
  }
}

impl<const CAP: usize> From<&[u8]> for StackHeapBuf<CAP> {
  #[inline]
  fn from(slice: &[u8]) -> Self {
    Self::assert_cap();
    if slice.len() <= CAP {
      let mut buf = [0u8; CAP];
      buf[..slice.len()].copy_from_slice(slice);
      Self::Stack(buf, slice.len() as u8)
    } else {
      Self::Heap(slice.to_vec())
    }
  }
}
