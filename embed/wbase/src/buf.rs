//! 栈优先/堆回退双态字节缓冲区公共基础设施
//!
//! 提供 `stack_heap_buf!` 宏，一键生成小字节栈分配、超长自动回退至堆的紧凑二进制缓冲区类型。
//! 统一提供 `Deref`、`Borrow`、`AsRef`、`PartialEq`、`Eq`、`PartialOrd`、`Ord`、`Hash` 与 `From` 转换。
//! 当启用 `simd` 特性时，缓冲区内容比对自动启用硬件加速向量比对。

#[cfg(feature = "simd")]
use crate::simd::fast_key_eq;

/// 缓冲区切片相等性比对辅助（支持按需启用 SIMD 加速）
#[inline(always)]
pub fn slice_eq(a: &[u8], b: &[u8]) -> bool {
  #[cfg(feature = "simd")]
  {
    fast_key_eq(a, b)
  }
  #[cfg(not(feature = "simd"))]
  {
    a == b
  }
}

/// 生成 `Stack([u8; CAP], u8) | Heap(Vec<u8>)` 双态缓冲区及其公共 trait 实现
#[macro_export]
macro_rules! stack_heap_buf {
  (
    $(#[$enum_meta:meta])*
    $name:ident, $cap:expr
  ) => {
    $(#[$enum_meta])*
    #[derive(Debug, Clone)]
    pub enum $name {
      /// 栈分配（总字节数 <= 上限）
      Stack([u8; $cap], u8),
      /// 堆分配（超长键自动回退）
      Heap(Vec<u8>),
    }

    const _: () = assert!($cap <= 255, "stack_heap_buf 栈容量不得超过 255 字节");

    impl ::core::default::Default for $name {
      #[inline]
      fn default() -> Self {
        Self::Stack([0u8; $cap], 0)
      }
    }

    impl ::core::ops::Deref for $name {
      type Target = [u8];

      #[inline(always)]
      fn deref(&self) -> &Self::Target {
        match self {
          Self::Stack(buf, len) => {
            let len = (*len as usize).min($cap);
            // 安全性保证：len 经 min($cap) 约束，恒满足 len <= buf.len()
            unsafe { buf.get_unchecked(..len) }
          }
          Self::Heap(vec) => vec.as_slice(),
        }
      }
    }

    impl AsRef<[u8]> for $name {
      #[inline(always)]
      fn as_ref(&self) -> &[u8] {
        self.as_slice()
      }
    }

    impl ::core::borrow::Borrow<[u8]> for $name {
      #[inline(always)]
      fn borrow(&self) -> &[u8] {
        self.as_slice()
      }
    }

    impl PartialEq for $name {
      #[inline(always)]
      fn eq(&self, other: &Self) -> bool {
        $crate::buf::slice_eq(self.as_slice(), other.as_slice())
      }
    }

    impl Eq for $name {}

    impl PartialOrd for $name {
      #[inline(always)]
      fn partial_cmp(&self, other: &Self) -> Option<::core::cmp::Ordering> {
        Some(self.cmp(other))
      }
    }

    impl Ord for $name {
      #[inline(always)]
      fn cmp(&self, other: &Self) -> ::core::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
      }
    }

    impl ::core::hash::Hash for $name {
      #[inline(always)]
      fn hash<H: ::core::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
      }
    }

    impl PartialEq<[u8]> for $name {
      #[inline(always)]
      fn eq(&self, other: &[u8]) -> bool {
        $crate::buf::slice_eq(self.as_slice(), other)
      }
    }

    impl PartialEq<&[u8]> for $name {
      #[inline(always)]
      fn eq(&self, other: &&[u8]) -> bool {
        $crate::buf::slice_eq(self.as_slice(), other)
      }
    }

    impl PartialEq<$name> for [u8] {
      #[inline(always)]
      fn eq(&self, other: &$name) -> bool {
        $crate::buf::slice_eq(self, other.as_slice())
      }
    }

    impl PartialEq<$name> for &[u8] {
      #[inline(always)]
      fn eq(&self, other: &$name) -> bool {
        $crate::buf::slice_eq(self, other.as_slice())
      }
    }

    impl From<Vec<u8>> for $name {
      #[inline]
      fn from(vec: Vec<u8>) -> Self {
        if vec.len() <= $cap {
          let mut buf = [0u8; $cap];
          buf[..vec.len()].copy_from_slice(&vec);
          Self::Stack(buf, vec.len() as u8)
        } else {
          Self::Heap(vec)
        }
      }
    }

    impl From<&[u8]> for $name {
      #[inline]
      fn from(slice: &[u8]) -> Self {
        if slice.len() <= $cap {
          let mut buf = [0u8; $cap];
          buf[..slice.len()].copy_from_slice(slice);
          Self::Stack(buf, slice.len() as u8)
        } else {
          Self::Heap(slice.to_vec())
        }
      }
    }

    impl From<$name> for Vec<u8> {
      #[inline]
      fn from(buf: $name) -> Self {
        buf.into_vec()
      }
    }

    impl $name {
      /// 获取只读切片借用（零拷贝）
      #[inline(always)]
      pub fn as_slice(&self) -> &[u8] {
        self
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
            let len = (len as usize).min($cap);
            // 安全性保证：len 经 min($cap) 约束，恒满足 len <= buf.len()
            unsafe { buf.get_unchecked(..len) }.to_vec()
          }
          Self::Heap(vec) => vec,
        }
      }
    }
  };
}
