//! 字节数组键包装（对标 libs/server/ByteArrayWrapper.cs）
//!
//! C# 为 Tsavorite 字典键特化类型：byte[]（可 GC 钉住）+ PinnedSpanByte
//! 双形态。rust 键即自有 `Vec<u8>`，无钉住语义，本结构承接"复制成自有
//! 字节缓冲"的构造面与只读视图。

/// 字节数组键包装
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteArrayWrapper(Vec<u8>);

impl ByteArrayWrapper {
  /// libs/server/ByteArrayWrapper.cs:CopyFrom
  ///
  /// 复制字节进自有缓冲（C# 的 usePinned 钉住分配在 rust 无对应语义，
  /// 统一为普通堆分配）
  pub fn copy_from(bytes: &[u8]) -> Self {
    Self(bytes.to_vec())
  }

  /// 只读字节视图（C# ReadOnlySpan 属性）
  pub fn read_only_span(&self) -> &[u8] {
    &self.0
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn copy_from_clones_bytes() {
    let src = [1u8, 2, 3];
    let wrapper = ByteArrayWrapper::copy_from(&src);
    assert_eq!(wrapper.read_only_span(), &[1, 2, 3]);
    // 自有缓冲：与源分离
    assert_eq!(ByteArrayWrapper::copy_from(&[]).read_only_span(), &[]);
  }
}
