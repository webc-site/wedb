//! Set 集合操作 (SetTreeOps)
//!
//! - Key: `[TreePrefix::SetMember as u8][member]`
//! - Val: `[0x00; 4]` (4 字节占位符)
//! - 接口：`sadd`, `srem`, `sismember`, `scard`, `sscan` (流式回调)

use wbftree::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeService, ScanReturnField};

use crate::{
  CollectionError, Result,
  prefix::{TreePrefix, with_prefixed_key},
};

/// 集合成员占位值 (4 字节 0x00，确保与任意短键相加均满足底层 min_record_size >= 4B 约束)
pub const SET_VAL_PLACEHOLDER: &[u8] = &[0x00; 4];

/// Set 树操作 Trait
///
/// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/SetOps.cs
pub trait SetTreeOps {
  /// 添加成员 (若为新添加返回 Ok(true)，已存在返回 Ok(false))
  fn sadd(&self, member: &[u8]) -> Result<bool>;

  /// 移除成员 (若存在且被移除返回 Ok(true)，不存在返回 Ok(false))
  fn srem(&self, member: &[u8]) -> Result<bool>;

  /// 判断成员是否存在 (零堆分配)
  fn sismember(&self, member: &[u8]) -> Result<bool>;

  /// 获取集合基数 (成员总数，注：上层存储会话优先通过 MetaValue.size 达到 O(1) 直读，本方法用于无元数据纯树校验)
  fn scard(&self) -> Result<usize>;

  /// 流式扫描成员
  fn sscan<F>(&self, start_member: &[u8], count: usize, on_member: F) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool;

  /// 收集全部成员列表 (以 Vec 返回)
  fn smembers(&self) -> Result<Vec<Vec<u8>>> {
    let mut members = Vec::new();
    self.sscan(b"", usize::MAX, |m| {
      members.push(m.to_vec());
      true
    })?;
    Ok(members)
  }
}

impl SetTreeOps for BfTreeService {
  fn sadd(&self, member: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::SetMember.as_u8(), member, |k| {
      let exists = self.contains_key(k);
      if exists {
        return Ok(false);
      }
      match self.insert(k, SET_VAL_PLACEHOLDER) {
        BfTreeInsertResult::Success => Ok(true),
        BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
        _ => Err(CollectionError::InvalidArgument("sadd 插入失败")),
      }
    })
  }

  fn srem(&self, member: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::SetMember.as_u8(), member, |k| {
      let exists = self.contains_key(k);
      if !exists {
        return Ok(false);
      }
      match self.delete(k) {
        BfTreeDeleteResult::Success => Ok(true),
        _ => Err(CollectionError::InvalidArgument("srem 删除失败")),
      }
    })
  }

  #[inline]
  fn sismember(&self, member: &[u8]) -> Result<bool> {
    with_prefixed_key(TreePrefix::SetMember.as_u8(), member, |k| {
      Ok(self.contains_key(k))
    })
  }

  fn scard(&self) -> Result<usize> {
    let prefix_u8 = TreePrefix::SetMember.as_u8();
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

  fn sscan<F>(&self, start_member: &[u8], count: usize, mut on_member: F) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    let prefix_u8 = TreePrefix::SetMember.as_u8();
    with_prefixed_key(prefix_u8, start_member, |sk| {
      self
        .scan_with_count_callback(sk, count, ScanReturnField::Key, |k, _| {
          if k.is_empty() || k[0] != prefix_u8 {
            return false;
          }
          on_member(&k[1..])
        })
        .map_err(Into::into)
    })
  }
}
