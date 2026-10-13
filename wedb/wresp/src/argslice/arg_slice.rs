//! 在 garnet 中的相对路径: libs/server/Resp/Parser/ParseUtils.cs(对标 C# ArgSlice 零拷贝切片操作)
use std::mem::size_of;

/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/VarLen/PinnedSpanByte.cs
/// We use ArgSlice to represent PinnedSpanByte.
///
/// C# 持裸指针；rust 侧 offset 化为纯整数对（宿主缓冲槽位区间），
/// 同一解析态全部槽位共享同一宿主缓冲（解析期天然成立，全部来自接收缓冲），
/// 借用安全由 `resolve` 单点绑定宿主缓冲生命周期，无裸指针无 unsafe。
#[derive(Debug, Clone, Copy, PartialEq)]
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
