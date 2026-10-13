//! 树操作会话：持有底层引擎的保活 Arc，专供高频循环/长事务/批量场景直调底层
//!
//! 相比于经 `BfTreeService` 的单次调用，`TreeSession` 彻底消除逐次
//! `ArcSwap::load` 的原子开销，并支持调用方复用足额缓冲直读。

use std::sync::Arc;

use bf_tree::{BfTree, LeafInsertResult, LeafReadResult};

use crate::{
  error,
  error::Error,
  types::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanReturnField},
};

/// 树操作会话
#[derive(Clone)]
pub struct TreeSession {
  pub(crate) tree: Arc<BfTree>,
  pub(crate) max_record_size: usize,
}

impl TreeSession {
  /// 创建新的会话
  #[inline]
  pub fn new(tree: Arc<BfTree>, max_record_size: usize) -> Self {
    Self {
      tree,
      max_record_size,
    }
  }

  /// 最大记录大小
  #[inline]
  pub fn max_record_size(&self) -> usize {
    self.max_record_size
  }

  /// 底层 BfTree 引用
  #[inline]
  pub fn raw_tree(&self) -> &BfTree {
    &self.tree
  }

  /// 极速原始插入（单指令分支内联）
  #[inline]
  pub fn insert_raw(&self, key: &[u8], value: &[u8]) -> bool {
    matches!(self.tree.insert(key, value), LeafInsertResult::Success)
  }

  /// 极速原始删除
  #[inline]
  pub fn delete_raw(&self, key: &[u8]) {
    self.tree.delete(key);
  }

  /// 插入键值对
  #[inline]
  pub fn insert(&self, key: &[u8], value: &[u8]) -> BfTreeInsertResult {
    if value.is_empty() {
      return BfTreeInsertResult::InvalidKV;
    }
    match self.tree.insert(key, value) {
      LeafInsertResult::Success => BfTreeInsertResult::Success,
      LeafInsertResult::InvalidKV(_) => BfTreeInsertResult::InvalidKV,
    }
  }

  /// 删除键
  #[inline]
  pub fn delete(&self, key: &[u8]) -> BfTreeDeleteResult {
    if key.is_empty() {
      return BfTreeDeleteResult::InvalidArguments;
    }
    self.tree.delete(key);
    BfTreeDeleteResult::Success
  }

  /// 底层直读：调用方提供的 out_buf 长度必须 ≥ max_record_size
  #[inline]
  pub fn read_tree(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    match self.tree.read(key, out_buf) {
      LeafReadResult::Found(n) => (BfTreeReadResult::Found, n as usize),
      LeafReadResult::NotFound => (BfTreeReadResult::NotFound, 0),
      LeafReadResult::Deleted => (BfTreeReadResult::Deleted, 0),
      LeafReadResult::InvalidKey => (BfTreeReadResult::InvalidKey, 0),
    }
  }

  /// 点读：若 out_buf 容量充足直读，否则返回 InvalidArguments
  #[inline]
  pub fn read_into(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    if out_buf.len() >= self.max_record_size {
      self.read_tree(key, out_buf)
    } else {
      (BfTreeReadResult::InvalidArguments, 0)
    }
  }

  /// 基于数量的流式范围扫描回调（复用调用方提供的缓冲区，零栈初始化开销）
  #[inline]
  pub fn scan_with_count_buf<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    buf: &mut [u8],
    mut on_record: F,
  ) -> error::Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    if buf.len() < self.max_record_size {
      return Err(Error::InvalidArgument("buffer too small for scan".into()));
    }
    let mut iter = self
      .tree
      .scan_with_count(start_key, count, return_field.into())
      .map_err(|e| Error::Scan(super::ops::scan_iter_error_to_string(e).to_string()))?;

    let mut scanned = 0;
    match return_field {
      ScanReturnField::KeyAndValue => {
        while let Some((k_len, v_len)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&buf[..k_len], &buf[k_len..k_len + v_len]) {
            break;
          }
        }
      }
      ScanReturnField::Key => {
        while let Some((k_len, _)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&buf[..k_len], &[]) {
            break;
          }
        }
      }
      ScanReturnField::Value => {
        while let Some((k_len, v_len)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&[], &buf[k_len..k_len + v_len]) {
            break;
          }
        }
      }
    }
    Ok(scanned)
  }

  /// 基于数量的流式范围扫描回调（直接在活跃会话持有的 Arc 上执行，零原子克隆）
  #[inline]
  pub fn scan_with_count<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> error::Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let mut stack_buf = [0u8; 8192];
    if self.max_record_size <= 8192 {
      self.scan_with_count_buf(start_key, count, return_field, &mut stack_buf, on_record)
    } else {
      let mut heap_buf = vec![0u8; self.max_record_size];
      self.scan_with_count_buf(start_key, count, return_field, &mut heap_buf, on_record)
    }
  }
}
