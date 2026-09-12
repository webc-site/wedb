//! BfTree ZSet 有序集合操作
//!
//! 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/SortedSetOps.cs

use wcol::{ZRangeByScoreOpt, ZSetTreeOps};
use wdev::Device;
use wval::CollectionType;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  // =========================================================================
  // ZSet 有序集合 API
  // =========================================================================

  /// 添加或更新有序集合成员分值 (新添加返回 Ok(true)，更新已有成员返回 Ok(false))
  ///
  /// NaN 拦截由 wcol::ZSetTreeOps::zadd 算子层统一执行（对标 C# SortedSetOps
  /// 在 ObjectStore 层拦截后转交对象实现，单一真值源）
  pub async fn bftree_zadd(&self, key: &[u8], member: &[u8], score: f64) -> Result<bool> {
    self
      .with_bftree_write_or_create(key, CollectionType::ZSET, |tree| {
        let is_new = tree.zadd(member, score)?;
        Ok((if is_new { 1 } else { 0 }, is_new))
      })
      .await
  }

  /// 移除有序集合成员 (成员存在且被移除返回 Ok(true)，不存在返回 Ok(false)；删空触发严格释放)
  pub async fn bftree_zrem(&self, key: &[u8], member: &[u8]) -> Result<bool> {
    self
      .with_bftree_remove(
        key,
        CollectionType::ZSET,
        || false,
        |tree| {
          let removed = tree.zrem(member)?;
          Ok((if removed { 1 } else { 0 }, removed))
        },
      )
      .await
  }

  /// 读取有序集合成员分值
  pub async fn bftree_zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>> {
    self
      .with_bftree_read(
        key,
        CollectionType::ZSET,
        || None,
        |tree| tree.zscore(member).map_err(Into::into),
      )
      .await
  }

  /// 按分值范围读取有序集合成员与分值 (返回 Vec 集合)
  pub async fn bftree_zrange_by_score(
    &self,
    key: &[u8],
    min: f64,
    max: f64,
  ) -> Result<Vec<(Vec<u8>, f64)>> {
    let mut items = Vec::with_capacity(32);
    self
      .bftree_zrange_by_score_stream(key, min, max, |member, score| {
        items.push((member.to_vec(), score));
        true
      })
      .await?;
    Ok(items)
  }

  /// 零拷贝流式按分值范围扫描有序集合成员与分值
  pub async fn bftree_zrange_by_score_stream<F>(
    &self,
    key: &[u8],
    min: f64,
    max: f64,
    on_item: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    self
      .with_bftree_read(
        key,
        CollectionType::ZSET,
        || 0,
        |tree| tree.zrange_by_score(min, max, on_item).map_err(Into::into),
      )
      .await
  }

  /// 按分值选项读取有序集合成员与分值 (返回 Vec 集合，支持开闭区间控制与 LIMIT 截断)
  pub async fn bftree_zrange_by_score_ext(
    &self,
    key: &[u8],
    opt: ZRangeByScoreOpt,
  ) -> Result<Vec<(Vec<u8>, f64)>> {
    let cap = if opt.limit != usize::MAX {
      opt.limit.min(1024)
    } else {
      32
    };
    let mut items = Vec::with_capacity(cap);
    self
      .bftree_zrange_by_score_ext_stream(key, opt, |member, score| {
        items.push((member.to_vec(), score));
        true
      })
      .await?;
    Ok(items)
  }

  /// 零拷贝流式按分值选项扫描有序集合成员与分值
  pub async fn bftree_zrange_by_score_ext_stream<F>(
    &self,
    key: &[u8],
    opt: ZRangeByScoreOpt,
    on_item: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    self
      .with_bftree_read(
        key,
        CollectionType::ZSET,
        || 0,
        |tree| tree.zrange_by_score_ext(opt, on_item).map_err(Into::into),
      )
      .await
  }

  /// 按下标排名范围读取有序集合成员与分值 [start, stop]
  pub async fn bftree_zrange_by_index(
    &self,
    key: &[u8],
    start: usize,
    stop: usize,
  ) -> Result<Vec<(Vec<u8>, f64)>> {
    let cap = if start <= stop {
      (stop - start + 1).min(1024)
    } else {
      0
    };
    let mut items = Vec::with_capacity(cap);
    self
      .bftree_zrange_by_index_stream(key, start, stop, |member, score| {
        items.push((member.to_vec(), score));
        true
      })
      .await?;
    Ok(items)
  }

  /// 零拷贝流式按下标排名范围扫描有序集合成员与分值 [start, stop]
  pub async fn bftree_zrange_by_index_stream<F>(
    &self,
    key: &[u8],
    start: usize,
    stop: usize,
    on_item: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    self
      .with_bftree_read(
        key,
        CollectionType::ZSET,
        || 0,
        |tree| {
          tree
            .zrange_by_index(start, stop, on_item)
            .map_err(Into::into)
        },
      )
      .await
  }

  /// 统计区间 [min, max] 内的有序集合成员总数
  pub async fn bftree_zcount(&self, key: &[u8], min: f64, max: f64) -> Result<usize> {
    self.bftree_zcount_ext(key, min, true, max, true).await
  }

  /// 统计分值区间内的有序集合成员总数（支持开闭区间控制，全覆盖短路 O(1)）
  pub async fn bftree_zcount_ext(
    &self,
    key: &[u8],
    min: f64,
    min_inc: bool,
    max: f64,
    max_inc: bool,
  ) -> Result<usize> {
    let loaded = self
      .load_bftree_meta_stub(key, CollectionType::ZSET)
      .await?;
    let Some((meta, stub, _)) = loaded else {
      return Ok(0);
    };
    if min.is_infinite()
      && min.is_sign_negative()
      && min_inc
      && max.is_infinite()
      && max.is_sign_positive()
      && max_inc
    {
      return Ok(meta.size as usize);
    }
    let tree = self.acquire_tree_read(key, &stub).await?;
    tree
      .zcount_ext(min, min_inc, max, max_inc)
      .map_err(Into::into)
  }

  /// 获取有序集合总基数 (O(1) 读取元数据)
  pub async fn bftree_zcard(&self, key: &[u8]) -> Result<usize> {
    self.bftree_card(key, CollectionType::ZSET).await
  }

  // =========================================================================
}
