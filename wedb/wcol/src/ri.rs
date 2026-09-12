//! 范围索引树操作 (RiTreeOps)
//!
//! - Key: `[TreePrefix::RangeIndexKey as u8][sub_key]`
//! - Val: `val: &[u8]`
//! - 接口：`ri_set`, `ri_get`, `ri_get_callback`, `ri_del`, `ri_exists`, `ri_scan`, `ri_range`, `ri_len`
//! - 采用 MaybeUninit 栈缓冲组装前缀键，实现零堆分配与数学级防穿透

/// 在 garnet 中的相对路径:libs/server/Storage/Session/MainStore/RangeIndexOps.cs
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField,
};

use crate::{
  CollectionError, Result,
  prefix::{TreePrefix, with_prefixed_key},
};

/// 按投影字段裁剪回调载荷：剥除物理键前缀后回传
///
/// 物理扫描恒为 KeyAndValue（前缀校验与剥离依赖真实键），
/// 投影在回调层完成：Value 投影时键退化为空切片，Key 投影时值退化为空切片
#[inline(always)]
fn project_entry<'a>(
  return_field: ScanReturnField,
  k: &'a [u8],
  v: &'a [u8],
) -> (&'a [u8], &'a [u8]) {
  match return_field {
    ScanReturnField::Value => (&[], v),
    ScanReturnField::Key => (&k[1..], &[]),
    ScanReturnField::KeyAndValue => (&k[1..], v),
  }
}

/// RangeIndex 树操作 Trait
pub trait RiTreeOps {
  /// 插入或更新键值对 (新插入返回 Ok(true)，更新已有键返回 Ok(false))
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> Result<bool>;

  /// 读取键对应的值 (堆分配拷贝)
  fn ri_get(&self, sub_key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.ri_get_callback(sub_key, |opt| opt.map(|v| v.to_vec()))
  }

  /// 零拷贝读取键对应的值 (零堆分配回调)
  fn ri_get_callback<R>(&self, sub_key: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> Result<R>;

  /// 删除键 (若键存在且被删除返回 Ok(true)，不存在返回 Ok(false))
  fn ri_del(&self, sub_key: &[u8]) -> Result<bool>;

  /// 判断键是否存在 (零堆分配)
  fn ri_exists(&self, sub_key: &[u8]) -> Result<bool>;

  /// 获取键数量 (前缀范围内键总数)
  fn ri_len(&self) -> Result<usize>;

  /// 零拷贝流式扫描键值对 (起始键 start_key 为空时从头扫描，遇到跨前缀截断)
  fn ri_scan<F>(&self, start_key: &[u8], count: usize, on_entry: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self.ri_scan_with_field(start_key, count, ScanReturnField::KeyAndValue, on_entry)
  }

  /// 零拷贝流式扫描键值对 (支持指定投影字段)
  fn ri_scan_with_field<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_entry: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;

  /// 零拷贝闭区间 [start_key, end_key] 流式范围扫描
  fn ri_range<F>(&self, start_key: &[u8], end_key: &[u8], on_entry: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self.ri_range_with_field(start_key, end_key, ScanReturnField::KeyAndValue, on_entry)
  }

  /// 零拷贝闭区间 [start_key, end_key] 流式范围扫描 (支持指定投影字段)
  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_entry: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;
}

impl RiTreeOps for BfTreeService {
  fn ri_set(&self, sub_key: &[u8], val: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::RangeIndexKey.as_u8(), sub_key, |k| {
      let exists = self.contains_key(k);
      match self.insert(k, val) {
        BfTreeInsertResult::Success => Ok(!exists),
        BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
        _ => Err(CollectionError::InvalidArgument("ri_set 插入失败")),
      }
    })
  }

  fn ri_get_callback<R>(&self, sub_key: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> Result<R> {
    with_prefixed_key(TreePrefix::RangeIndexKey.as_u8(), sub_key, |k| {
      self.read_callback(k, |res, bytes| match res {
        BfTreeReadResult::Found => Ok(f(Some(bytes))),
        BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(f(None)),
        _ => Err(CollectionError::InvalidArgument("ri_get 读取失败")),
      })
    })
  }

  fn ri_del(&self, sub_key: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::RangeIndexKey.as_u8(), sub_key, |k| {
      let exists = self.contains_key(k);
      match self.delete(k) {
        BfTreeDeleteResult::Success => Ok(exists),
        _ => Err(CollectionError::InvalidArgument("ri_del 删除失败")),
      }
    })
  }

  #[inline]
  fn ri_exists(&self, sub_key: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::RangeIndexKey.as_u8(), sub_key, |k| {
      Ok(self.contains_key(k))
    })
  }

  fn ri_len(&self) -> Result<usize> {
    let prefix_u8 = TreePrefix::RangeIndexKey as u8;
    let start_key = [prefix_u8];
    let mut count = 0;
    self.scan_with_count_callback(&start_key, usize::MAX, ScanReturnField::Key, |k, _| {
      if k.is_empty() || k[0] != prefix_u8 {
        return false;
      }
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
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    let prefix_u8 = TreePrefix::RangeIndexKey.as_u8();
    with_prefixed_key(prefix_u8, start_key, |sk| {
      self
        .scan_with_count_callback(sk, count, ScanReturnField::KeyAndValue, |k, v| {
          if k.is_empty() || k[0] != prefix_u8 {
            return false;
          }
          let (user_k, user_v) = project_entry(return_field, k, v);
          on_entry(user_k, user_v)
        })
        .map_err(Into::into)
    })
  }

  fn ri_range_with_field<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    mut on_entry: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if start_key > end_key {
      return Ok(0);
    }
    let prefix_u8 = TreePrefix::RangeIndexKey.as_u8();
    with_prefixed_key(prefix_u8, start_key, |sk| {
      with_prefixed_key(prefix_u8, end_key, |ek| {
        self
          .scan_with_end_key_callback(sk, ek, ScanReturnField::KeyAndValue, |k, v| {
            if k.is_empty() || k[0] != prefix_u8 {
              return false;
            }
            let (user_k, user_v) = project_entry(return_field, k, v);
            on_entry(user_k, user_v)
          })
          .map_err(Into::into)
      })
    })
  }
}
