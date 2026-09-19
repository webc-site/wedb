use std::mem::size_of;

/// 在 garnet 中的相对路径:Tsavorite.core/PinnedSpanByte.cs
/// We use ArgSlice to represent PinnedSpanByte.
///
/// C# 持裸指针；rust 侧 offset 化为纯整数对（宿主缓冲槽位区间），
/// 同一解析态全部槽位共享同一宿主缓冲（解析期天然成立，全部来自接收缓冲），
/// 借用安全由 `resolve` 单点绑定宿主缓冲生命周期，无裸指针无 unsafe。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArgSlice {
  /// 宿主缓冲内负载起点偏移
  pub offset: usize,
  /// 负载字节数
  pub length: usize,
}

impl ArgSlice {
  #[inline]
  pub const fn new(offset: usize, length: usize) -> Self {
    Self { offset, length }
  }

  /// 解析为宿主缓冲内的参数切片（对标 C# PinnedSpanByte.Span）
  ///
  /// 空槽（length == 0）返回空切片，偏移不参与解引用
  #[inline]
  pub fn resolve<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
    if self.length == 0 {
      &[]
    } else {
      &buf[self.offset..self.offset + self.length]
    }
  }

  #[inline]
  pub const fn total_size(&self) -> usize {
    self.length + size_of::<u32>()
  }

  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.length == 0
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_arg_slice_resolve() {
    let payload = b"Hello, Garnet ArgSlice!";
    // 负载前预留 4 字节长度前缀位，验证区间解析
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(payload);
    let slice = ArgSlice::new(4, payload.len());
    assert_eq!(slice.resolve(&buf), payload);
    assert_eq!(slice.total_size(), payload.len() + 4);
    assert!(!slice.is_empty());
  }

  #[test]
  fn test_empty_arg_slice() {
    let slice = ArgSlice::new(0, 0);
    assert!(slice.is_empty());
    assert_eq!(slice.resolve(b"anything"), b"");
  }
}
