//! 范围索引树操作 (RiTreeOps)
//!
//! - Key: `sub_key: &[u8]`
//! - Val: `val: &[u8]`
//! - 接口：`ri_set`, `ri_get`, `ri_get_callback`, `ri_del`, `ri_exists`, `ri_scan`, `ri_range`, `ri_len`
//! - 直接将子键透传至底层独立 BfTree，零拼接开销，单树物理隔离

/// 在 garnet 中的相对路径:libs/server/Storage/Session/MainStore/RangeIndexOps.cs
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField,
};

use crate::error::{CollectionError, CollectionResult};

/// RangeIndex 树操作 Trait
pub trait RiTreeOps {
  /// 插入或更新键值对 (新插入返回 Ok(true)，更新已有键返回 Ok(false))
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> CollectionResult<bool>;

  /// 读取键对应的值 (堆分配拷贝)
  fn ri_get(&self, sub_key: &[u8]) -> CollectionResult<Option<Vec<u8>>> {
    self.ri_get_callback(sub_key, |opt| opt.map(|v| v.to_vec()))
  }

  /// 零拷贝读取键对应的值 (零堆分配回调)
  fn ri_get_callback<R>(
    &self,
    sub_key: &[u8],
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> CollectionResult<R>;

  /// 删除键 (若键存在且被删除返回 Ok(true)，不存在返回 Ok(false))
  fn ri_del(&self, sub_key: &[u8]) -> CollectionResult<bool>;

  /// 获取键数量 (前缀范围内键总数)
  fn ri_len(&self) -> CollectionResult<usize>;

  /// 零拷贝流式扫描键值对 (支持指定投影字段)
  fn ri_scan_with_field<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;

  /// 零拷贝闭区间 [start_key, end_key] 流式范围扫描 (支持指定投影字段)
  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;
}

impl RiTreeOps for BfTreeService {
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> CollectionResult<bool> {
    let exists = self.contains_key(sub_key);
    match self.insert(sub_key, val) {
      BfTreeInsertResult::Success => Ok(!exists),
      BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
      _ => Err(CollectionError::InvalidArgument("ri_set 插入失败")),
    }
  }

  fn ri_get_callback<R>(
    &self,
    sub_key: &[u8],
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> CollectionResult<R> {
    self.read_callback(sub_key, |res, bytes| match res {
      BfTreeReadResult::Found => Ok(f(Some(bytes))),
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(f(None)),
      _ => Err(CollectionError::InvalidArgument("ri_get 读取失败")),
    })
  }

  fn ri_del(&self, sub_key: &[u8]) -> CollectionResult<bool> {
    let exists = self.contains_key(sub_key);
    match self.delete(sub_key) {
      BfTreeDeleteResult::Success => Ok(exists),
      _ => Err(CollectionError::InvalidArgument("ri_del 删除失败")),
    }
  }

  fn ri_len(&self) -> CollectionResult<usize> {
    let mut count = 0;
    self.scan_with_count_callback(&[], usize::MAX, ScanReturnField::Key, |_, _| {
      count += 1;
      true
    })?;
    Ok(count)
  }

  fn ri_scan_with_field<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    mut on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    self
      .scan_with_count_callback(start_key, count, return_field, |k, v| on_entry(k, v))
      .map_err(Into::into)
  }

  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    mut on_entry: F,
  ) -> CollectionResult<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if start_key > end_key {
      return Ok(0);
    }
    self
      .scan_with_end_key_callback(start_key, end_key, return_field, |k, v| on_entry(k, v))
      .map_err(Into::into)
  }
}
