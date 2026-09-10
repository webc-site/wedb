//! 字节数组键比较器（对标 libs/server/ByteArrayWrapperComparer.cs）
//!
//! C# 为 IEqualityComparer<ByteArrayWrapper>（字节序列相等 + 字节哈希）；
//! rust 键相等即切片相等，哈希经 gxhash 承接，本结构承接相等判定。

use crate::byte_array_wrapper::ByteArrayWrapper;

/// 字节数组键比较器
#[derive(Debug, Default, Clone, Copy)]
pub struct ByteArrayWrapperComparer;

impl ByteArrayWrapperComparer {
  /// libs/server/ByteArrayWrapperComparer.cs:Equals
  ///
  /// 字节序列逐字节相等（C# ReadOnlySpan.SequenceEqual）
  pub fn equals(left: &ByteArrayWrapper, right: &ByteArrayWrapper) -> bool {
    left.read_only_span() == right.read_only_span()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn equals_is_byte_sequence_equality() {
    let a = ByteArrayWrapper::copy_from(b"key-1");
    let b = ByteArrayWrapper::copy_from(b"key-1");
    let c = ByteArrayWrapper::copy_from(b"key-2");
    assert!(ByteArrayWrapperComparer::equals(&a, &b));
    assert!(!ByteArrayWrapperComparer::equals(&a, &c));
    assert!(ByteArrayWrapperComparer::equals(&a, &a));
  }
}
