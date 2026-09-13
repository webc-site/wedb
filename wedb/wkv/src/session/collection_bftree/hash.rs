//! BfTree Hash 集合操作
//!
//! 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/HashOps.cs

use wcol::HashTreeOps;
use wdev::Device;
use wval::CollectionType;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 设置哈希字段值 (新插入返回 Ok(true)，更新已有字段返回 Ok(false))
  pub async fn bftree_hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<bool> {
    self
      .with_bftree_write_or_create(key, CollectionType::Hash, |tree| {
        let is_new = tree.hset(field, value)?;
        Ok((if is_new { 1 } else { 0 }, is_new))
      })
      .await
  }

  /// 读取哈希字段值
  pub async fn bftree_hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
    self
      .with_bftree_read(
        key,
        CollectionType::Hash,
        || None,
        |tree| tree.hget(field).map_err(Into::into),
      )
      .await
  }

  /// 零拷贝借用读取哈希字段值（闭包直出切片视图，零堆分配）
  pub async fn bftree_hget_with<R>(
    &self,
    key: &[u8],
    field: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self
      .with_bftree_read(
        key,
        CollectionType::Hash,
        || None,
        |tree| {
          tree
            .hget_callback(field, |opt| opt.map(f))
            .map_err(Into::into)
        },
      )
      .await
  }

  /// 判断哈希表中指定字段是否存在 (零堆分配)
  pub async fn bftree_hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
    self
      .with_bftree_read(
        key,
        CollectionType::Hash,
        || false,
        |tree| tree.hexists(field).map_err(Into::into),
      )
      .await
  }

  /// 删除哈希字段 (字段存在且被删除返回 Ok(true)，不存在返回 Ok(false)；删空触发严格释放)
  pub async fn bftree_hdel(&self, key: &[u8], field: &[u8]) -> Result<bool> {
    self
      .with_bftree_remove(
        key,
        CollectionType::Hash,
        || false,
        |tree| {
          let deleted = tree.hdel(field)?;
          Ok((if deleted { 1 } else { 0 }, deleted))
        },
      )
      .await
  }

  /// 获取哈希表字段总数 (O(1) 直读主存元数据 size，严禁扫树)
  pub async fn bftree_hlen(&self, key: &[u8]) -> Result<usize> {
    self.bftree_card(key, CollectionType::Hash).await
  }

  /// 扫描哈希表字段与值 (返回 Vec 集合)
  pub async fn bftree_hscan(
    &self,
    key: &[u8],
    start_field: &[u8],
    count: usize,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut records = Vec::with_capacity(count.min(1024));
    self
      .bftree_hscan_stream(key, start_field, count, |k, v| {
        records.push((k.to_vec(), v.to_vec()));
        true
      })
      .await?;
    Ok(records)
  }

  /// 零拷贝流式扫描哈希表字段与值
  pub async fn bftree_hscan_stream<F>(
    &self,
    key: &[u8],
    start_field: &[u8],
    count: usize,
    on_entry: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self
      .with_bftree_read(
        key,
        CollectionType::Hash,
        || 0,
        |tree| tree.hscan(start_field, count, on_entry).map_err(Into::into),
      )
      .await
  }
}
