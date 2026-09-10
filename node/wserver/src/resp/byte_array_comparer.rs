//! 字节数组比较器（对标 libs/server/Resp/ByteArrayComparer.cs）
//!
//! C# 为 IEqualityComparer<byte[]>（含 ReadOnlySpan 交替比较与字节哈希）；
//! rust 侧字典键直接以 &[u8] 寻址，本结构承接相等判定面。

/// 字节数组比较器
#[derive(Debug, Default, Clone, Copy)]
pub struct ByteArrayComparer;

impl ByteArrayComparer {
  /// libs/server/Resp/ByteArrayComparer.cs:Equals
  ///
  /// 字节序列逐字节相等（C# ReadOnlySpan.SequenceEqual）
  pub fn equals(left: &[u8], right: &[u8]) -> bool {
    left == right
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn equals_is_byte_sequence_equality() {
    assert!(ByteArrayComparer::equals(b"same", b"same"));
    assert!(!ByteArrayComparer::equals(b"same", b"other"));
    assert!(ByteArrayComparer::equals(b"", b""));
    // 长度不同即不等（前缀不等）
    assert!(!ByteArrayComparer::equals(b"pre", b"prefix"));
  }
}
