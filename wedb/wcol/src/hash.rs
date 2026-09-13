//! Hash 集合操作 (HashTreeOps)
//!
//! - Key: `[TreePrefix::HashField as u8][field]`
//! - Val: `[tag: 1B][payload]` (tag=0 空串，tag=1 非空)
//! - 接口：`hset`, `hget`, `hdel`, `hexists`, `hlen`, `hscan` (零拷贝流式回调)

/// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/HashOps.cs
use std::{mem::MaybeUninit, ptr, slice};

use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField,
};

use crate::{
  CollectionError, Result,
  prefix::{TreePrefix, with_prefixed_key},
};

/// 值标签定义：空串
pub const TAG_EMPTY: u8 = 0;
/// 值标签定义：非空载荷
pub const TAG_NON_EMPTY: u8 = 1;
/// 值标签定义：短值定长填充载荷 (保证 record_size >= 4B 且不破坏值长度)
pub const TAG_PADDED: u8 = 2;

/// 栈缓冲区阈值（1024 字节，避免大部分小值分配堆内存）
const STACK_VAL_BUF_SIZE: usize = 1024;

/// 零拷贝解析 Hash 存储载荷
#[inline]
fn parse_hash_payload(bytes: &[u8]) -> Option<&[u8]> {
  match bytes.first() {
    Some(&TAG_EMPTY) => Some(&[]),
    Some(&TAG_NON_EMPTY) => Some(&bytes[1..]),
    Some(&TAG_PADDED) => {
      if bytes.len() >= 2 {
        let len = bytes[1] as usize;
        if bytes.len() >= 2 + len {
          return Some(&bytes[2..2 + len]);
        }
      }
      None
    }
    _ => None,
  }
}

/// 栈优先构造 Hash 编码值切片
///
/// `key_len` 为包含 TreePrefix 前缀的物理键总长。
/// 当 `key_len + 1 + value.len() < 4` 时，底层 B-Tree 会因整条记录尺寸小于 4 字节限制报错，
/// 此时使用 [`TAG_PADDED`] 定长填充至 4 字节；
/// 正常值根据长度优先走 1024 字节栈分配，超出部分回退到堆。
#[inline]
fn with_hash_val<R>(key_len: usize, value: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
  if value.is_empty() {
    f(&[TAG_EMPTY, 0, 0, 0])
  } else if key_len + 1 + value.len() < 4 {
    let mut buf = [0u8; 4];
    buf[0] = TAG_PADDED;
    buf[1] = value.len() as u8;
    buf[2..2 + value.len()].copy_from_slice(value);
    f(&buf)
  } else if value.len() < STACK_VAL_BUF_SIZE {
    let mut buf = [MaybeUninit::<u8>::uninit(); STACK_VAL_BUF_SIZE];
    let total_len = 1 + value.len();
    unsafe {
      let ptr = buf.as_mut_ptr() as *mut u8;
      *ptr = TAG_NON_EMPTY;
      ptr::copy_nonoverlapping(value.as_ptr(), ptr.add(1), value.len());
      f(slice::from_raw_parts(buf.as_ptr() as *const u8, total_len))
    }
  } else {
    let mut buf = Vec::with_capacity(1 + value.len());
    buf.push(TAG_NON_EMPTY);
    buf.extend_from_slice(value);
    f(&buf)
  }
}

/// Hash 树操作 Trait
///
/// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/HashOps.cs
pub trait HashTreeOps {
  /// 设置字段值 (若为新插入返回 Ok(true)，覆盖已有字段返回 Ok(false))
  fn hset(&self, field: &[u8], value: &[u8]) -> Result<bool>;

  /// 获取字段值
  fn hget(&self, field: &[u8]) -> Result<Option<Vec<u8>>> {
    self.hget_callback(field, |opt| opt.map(|v| v.to_vec()))
  }

  /// 零拷贝借用读取字段值回调 (零堆分配)
  fn hget_callback<R>(&self, field: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> Result<R>;

  /// 删除字段 (若字段存在且被删除返回 Ok(true)，不存在返回 Ok(false))
  fn hdel(&self, field: &[u8]) -> Result<bool>;

  /// 判断字段是否存在 (零堆分配)
  fn hexists(&self, field: &[u8]) -> Result<bool>;

  /// 获取哈希表字段数量 (底层树算子全键扫描；上层物化统计请优先使用 StoreSession::bftree_hlen 的 O(1) 元数据直读)
  fn hlen(&self) -> Result<usize>;

  /// 零拷贝流式扫描字段与值
  fn hscan<F>(&self, start_field: &[u8], count: usize, on_entry: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool;
}

impl HashTreeOps for BfTreeService {
  fn hset(&self, field: &[u8], value: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::HashField.as_u8(), field, |k| {
      let exists = self.contains_key(k);
      let insert_res = with_hash_val(k.len(), value, |val_bytes| self.insert(k, val_bytes));

      match insert_res {
        BfTreeInsertResult::Success => Ok(!exists),
        BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
        _ => Err(CollectionError::InvalidArgument("hset 插入失败")),
      }
    })
  }

  fn hget_callback<R>(&self, field: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> Result<R> {
    with_prefixed_key(TreePrefix::HashField.as_u8(), field, |k| {
      self.read_callback(k, |res, bytes| match res {
        BfTreeReadResult::Found => match parse_hash_payload(bytes) {
          Some(payload) => Ok(f(Some(payload))),
          None => Err(CollectionError::Corrupted("哈希值标签非法")),
        },
        BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(f(None)),
        _ => Err(CollectionError::InvalidArgument("hget 读取失败")),
      })
    })
  }

  fn hdel(&self, field: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::HashField.as_u8(), field, |k| {
      let exists = self.contains_key(k);
      if !exists {
        return Ok(false);
      }
      match self.delete(k) {
        BfTreeDeleteResult::Success => Ok(true),
        _ => Err(CollectionError::InvalidArgument("hdel 删除失败")),
      }
    })
  }

  #[inline]
  fn hexists(&self, field: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::HashField.as_u8(), field, |k| {
      Ok(self.contains_key(k))
    })
  }

  fn hlen(&self) -> Result<usize> {
    let prefix_u8 = TreePrefix::HashField as u8;
    let start_key = [prefix_u8];
    let mut count = 0;
    self.scan_with_count_callback(&start_key, usize::MAX, ScanReturnField::Key, |k, _val| {
      if k.is_empty() || k[0] != prefix_u8 {
        return false;
      }
      count += 1;
      true
    })?;
    Ok(count)
  }

  fn hscan<F>(&self, start_field: &[u8], count: usize, mut on_entry: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    let prefix_u8 = TreePrefix::HashField as u8;
    let mut corrupted = false;
    let scanned = with_prefixed_key(prefix_u8, start_field, |sk| {
      self.scan_with_count_callback(sk, count, ScanReturnField::KeyAndValue, |k, v| {
        if k.is_empty() || k[0] != prefix_u8 {
          return false;
        }
        match parse_hash_payload(v) {
          Some(payload) => on_entry(&k[1..], payload),
          None => {
            corrupted = true;
            false
          }
        }
      })
    })?;

    if corrupted {
      return Err(CollectionError::Corrupted("哈希值标签非法"));
    }
    Ok(scanned)
  }
}
