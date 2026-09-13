//! BfTree List 列表操作
//!
//! 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/ListOps.cs

use wcol::{CollectionError, ListTreeOps};
use wdev::Device;
use wval::CollectionType;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 左侧推入元素 (返回推入后的列表总长度)
  pub async fn bftree_lpush(&self, key: &[u8], element: &[u8]) -> Result<usize> {
    self
      .with_bftree_list_push(key, |tree, stub| Ok(tree.lpush(stub, element)?))
      .await
  }

  /// 右侧推入元素 (返回推入后的列表总长度)
  pub async fn bftree_rpush(&self, key: &[u8], element: &[u8]) -> Result<usize> {
    self
      .with_bftree_list_push(key, |tree, stub| Ok(tree.rpush(stub, element)?))
      .await
  }

  /// 左侧弹出元素 (空列表返回 Ok(None)；删空触发严格释放)
  pub async fn bftree_lpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self
      .with_bftree_list_pop_or_trim(
        key,
        || None,
        |tree, stub| {
          let elem = tree.lpop(stub)?;
          let changed = elem.is_some();
          Ok((changed, elem))
        },
      )
      .await
  }

  /// 右侧弹出元素 (空列表返回 Ok(None)；删空触发严格释放)
  pub async fn bftree_rpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self
      .with_bftree_list_pop_or_trim(
        key,
        || None,
        |tree, stub| {
          let elem = tree.rpop(stub)?;
          let changed = elem.is_some();
          Ok((changed, elem))
        },
      )
      .await
  }

  /// 按索引读取列表元素 (支持负数倒数索引)
  pub async fn bftree_lindex(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>> {
    self
      .with_bftree_read_list(
        key,
        || None,
        |tree, stub| tree.lindex(stub, index).map_err(Into::into),
      )
      .await
  }

  /// 按范围读取列表元素 (闭区间，支持负数倒数索引与越界截断)
  pub async fn bftree_lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    self
      .with_bftree_read_list(key, Vec::new, |tree, stub| {
        tree.lrange(stub, start, stop).map_err(Into::into)
      })
      .await
  }

  /// 按索引修改列表元素 (LSET)
  pub async fn bftree_lset(&self, key: &[u8], index: i64, element: &[u8]) -> Result<()> {
    let loaded = self
      .load_bftree_meta_stub(key, CollectionType::List)
      .await?;
    let Some((_, stub, Some(list_stub))) = loaded else {
      return Err(CollectionError::InvalidArgument("ERR no such key").into());
    };
    let tree = self.acquire_tree_read(key, &stub).await?;
    tree.lset(&list_stub, index, element).map_err(Into::into)
  }

  /// 裁剪列表仅保留指定区间元素 (LTRIM)
  pub async fn bftree_ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<usize> {
    self
      .with_bftree_list_pop_or_trim(
        key,
        || 0,
        |tree, stub| {
          let remaining = tree.ltrim(stub, start, stop)?;
          Ok((true, remaining))
        },
      )
      .await
  }

  /// 获取列表总长度 (O(1) 读取元数据)
  pub async fn bftree_llen(&self, key: &[u8]) -> Result<usize> {
    self.bftree_card(key, CollectionType::List).await
  }
}
