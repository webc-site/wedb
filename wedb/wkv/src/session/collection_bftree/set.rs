//! BfTree Set 集合操作
//!
//! 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/SetOps.cs

use wcol::SetTreeOps;
use wdev::Device;
use wval::CollectionType;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 添加集合成员 (新添加返回 Ok(true)，已存在返回 Ok(false))
  pub async fn bftree_sadd(&self, key: &[u8], member: &[u8]) -> Result<bool> {
    self.bftree_sadd_batch(key, &[member]).await.map(|n| n > 0)
  }

  /// 批量添加集合成员 (返回实际新增的成员数量)
  pub async fn bftree_sadd_batch(&self, key: &[u8], members: &[impl AsRef<[u8]>]) -> Result<usize> {
    if members.is_empty() {
      return Ok(0);
    }
    self
      .with_bftree_write_or_create(key, CollectionType::Set, |tree| {
        let mut added = 0u64;
        for m in members {
          if tree.sadd(m.as_ref())? {
            added += 1;
          }
        }
        Ok((added, added as usize))
      })
      .await
  }

  /// 移除集合成员 (成员存在且被移除返回 Ok(true)，不存在返回 Ok(false)；删空触发严格释放)
  pub async fn bftree_srem(&self, key: &[u8], member: &[u8]) -> Result<bool> {
    self.bftree_srem_batch(key, &[member]).await.map(|n| n > 0)
  }

  /// 批量移除集合成员 (返回实际移除的成员数量；删空触发严格释放)
  pub async fn bftree_srem_batch(&self, key: &[u8], members: &[impl AsRef<[u8]>]) -> Result<usize> {
    if members.is_empty() {
      return Ok(0);
    }
    self
      .with_bftree_remove(
        key,
        CollectionType::Set,
        || 0,
        |tree| {
          let mut removed = 0u64;
          for m in members {
            if tree.srem(m.as_ref())? {
              removed += 1;
            }
          }
          Ok((removed, removed as usize))
        },
      )
      .await
  }

  /// 判断成员是否存在于集合中 (零堆分配)
  pub async fn bftree_sismember(&self, key: &[u8], member: &[u8]) -> Result<bool> {
    self
      .with_bftree_read(
        key,
        CollectionType::Set,
        || false,
        |tree| tree.sismember(member).map_err(Into::into),
      )
      .await
  }

  /// 批量判断成员是否存在于集合中 (零堆分配判断)
  pub async fn bftree_smismember(
    &self,
    key: &[u8],
    members: &[impl AsRef<[u8]>],
  ) -> Result<Vec<bool>> {
    self
      .with_bftree_read(
        key,
        CollectionType::Set,
        || vec![false; members.len()],
        |tree| {
          let mut results = Vec::with_capacity(members.len());
          for m in members {
            results.push(tree.sismember(m.as_ref())?);
          }
          Ok(results)
        },
      )
      .await
  }

  /// 获取集合基数 (O(1) 读取元数据)
  pub async fn bftree_scard(&self, key: &[u8]) -> Result<usize> {
    self.bftree_card(key, CollectionType::Set).await
  }

  /// 获取集合所有成员
  pub async fn bftree_smembers(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
    self.bftree_sscan(key, b"", usize::MAX).await
  }

  /// 扫描集合成员 (返回 Vec 集合)
  pub async fn bftree_sscan(
    &self,
    key: &[u8],
    start_member: &[u8],
    count: usize,
  ) -> Result<Vec<Vec<u8>>> {
    let mut members = Vec::with_capacity(count.min(1024));
    self
      .bftree_sscan_stream(key, start_member, count, |m| {
        members.push(m.to_vec());
        true
      })
      .await?;
    Ok(members)
  }

  /// 零拷贝流式扫描集合成员
  pub async fn bftree_sscan_stream<F>(
    &self,
    key: &[u8],
    start_member: &[u8],
    count: usize,
    on_member: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool,
  {
    self
      .with_bftree_read(
        key,
        CollectionType::Set,
        || 0,
        |tree| {
          tree
            .sscan(start_member, count, on_member)
            .map_err(Into::into)
        },
      )
      .await
  }
}
