//! 参数切片向量（对标 libs/server/ArgSlice/ArgSliceVector.cs）
//!
//! C# 以 ScratchBufferBuilder 环形缓冲 + (Offset, Length) 条目批量收集键序列
//! （CLUSTER MIGRATE 键收集面）；rust argslice 域以自有字节缓冲承载切片，
//! 本结构保留"上限防抖的批量收集"语义（达上限即拒绝新增）。

/// 默认最大条目数（C# maxItemNum = 1 << 18）
pub const DEFAULT_MAX_ITEM_NUM: usize = 1 << 18;

/// 参数切片向量
#[derive(Debug, Default)]
pub struct ArgSliceVector {
  /// 条目上限
  max_count: usize,
  /// 已收集条目（自有字节）
  items: Vec<Vec<u8>>,
}

impl ArgSliceVector {
  /// 以默认上限创建（C# 构造子默认 maxItemNum）
  pub fn new() -> Self {
    Self::with_max_items(DEFAULT_MAX_ITEM_NUM)
  }

  /// 以指定上限创建
  pub fn with_max_items(max_item_num: usize) -> Self {
    Self {
      max_count: max_item_num,
      items: Vec::new(),
    }
  }

  /// libs/server/ArgSlice/ArgSliceVector.cs:TryAddItem
  ///
  /// 尝试追加切片：达到上限即拒绝并返回 false（C# `Count + 1 >= maxCount`
  /// 防抖判定，预留 1 条目缓冲）
  pub fn try_add_item(&mut self, item: &[u8]) -> bool {
    if self.items.len() + 1 >= self.max_count {
      return false;
    }
    self.items.push(item.to_vec());
    true
  }

  /// 已收集条目数（C# Count）
  pub fn count(&self) -> usize {
    self.items.len()
  }

  /// 是否为空（C# IsEmpty）
  pub fn is_empty(&self) -> bool {
    self.items.is_empty()
  }

  /// 条目切片序列（枚举面；C# IEnumerable）
  pub fn items(&self) -> &[Vec<u8>] {
    &self.items
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn try_add_item_collects_until_capacity() {
    let mut v = ArgSliceVector::with_max_items(3);
    assert!(v.try_add_item(b"k1"));
    assert!(v.try_add_item(b"k2"));
    // 预留 1 条目缓冲：第 3 条已触界拒绝（C# Count + 1 >= maxCount 口径）
    assert!(!v.try_add_item(b"k3"));
    assert_eq!(v.count(), 2);
    assert_eq!(v.items(), &[b"k1".to_vec(), b"k2".to_vec()]);
  }

  #[test]
  fn default_capacity_matches_csharp() {
    let mut v = ArgSliceVector::new();
    assert_eq!(DEFAULT_MAX_ITEM_NUM, 1 << 18);
    assert!(v.is_empty());
    assert!(v.try_add_item(&[0u8; 64]));
    assert_eq!(v.count(), 1);
  }
}
