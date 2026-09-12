//! ZSet 有序集合操作 (ZSetTreeOps)
//!
//! 采用单树双前缀设计：
//! - `0x01` 主分值索引：Key = `[TreePrefix::ZSetScore as u8: 1B][order_score: 8B be][member]`, Val = `[0x00]`
//! - `0x02` 反查索引：Key = `[TreePrefix::ZSetMember as u8: 1B][member]`, Val = `[order_score: 8B be]`
//! - 浮点数保序转换：
//!   ```ignore
//!   let bits = score.to_bits();
//!   let order = if (bits & (1 << 63)) == 0 { bits ^ (1 << 63) } else { !bits };
//!   let order_score = order.to_be_bytes();
//!   ```
//! - 接口：`zadd`, `zrem`, `zscore`, `zcount`, `zrange_by_score` (流式回调)

use wbase::float;
use wbftree::{BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField};

use crate::{
  CollectionError, Result,
  prefix::{TreePrefix, with_prefixed_key, with_prefixed_key2},
};

/// 主分值索引前缀 (0x01)
pub const PREFIX_SCORE: u8 = TreePrefix::ZSetScore as u8;
/// 成员反查索引前缀 (0x02)
pub const PREFIX_MEMBER: u8 = TreePrefix::ZSetMember as u8;

/// ZSet 主分值索引占位值 (1 字节 0x00，主键长度 ≥ 9B 满足底层 min_record_size 约束)
const ZSET_VAL_PLACEHOLDER: &[u8] = &[0x00];

/// 浮点数转换为大端保序字节序列 (IEEE 754 全序单调映射，零分支位运算)
#[inline(always)]
pub const fn encode_order_score(score: f64) -> [u8; 8] {
  float::encode_f64(score)
}

/// 大端保序字节序列解码为浮点数 (IEEE 754 全序单调映射，零分支位运算)
#[inline(always)]
pub const fn decode_order_score(order_score: [u8; 8]) -> f64 {
  float::decode_f64(order_score)
}

/// 栈优先构造分值索引键 `[0x01][order_score: 8B][member]`
#[inline]
fn with_score_key<R>(order_score: &[u8; 8], member: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
  with_prefixed_key2(PREFIX_SCORE, order_score, member, f)
}

/// 栈优先构造成员反查索引键 `[0x02][member]`
#[inline]
fn with_member_key<R>(member: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
  with_prefixed_key(PREFIX_MEMBER, member, f)
}

/// 归一化分值区间边界并编码为保序字节序对（zcount_ext / zrange_by_score_ext 共用）
///
/// 返回 None 表示空区间（NaN / 反向 / 单点开区间 / 编码后倒序）；
/// 区间下端展开到 -0.0、上端展开到 +0.0 编码，确保 ±0.0 成员均落入扫描范围
#[inline]
fn score_bounds(min: f64, min_inc: bool, max: f64, max_inc: bool) -> Option<([u8; 8], [u8; 8])> {
  if min.is_nan() || max.is_nan() || min > max {
    return None;
  }
  if min == max && (!min_inc || !max_inc) {
    return None;
  }
  let min_order = if min == 0.0 {
    encode_order_score(-0.0)
  } else {
    encode_order_score(min)
  };
  let max_order = if max == 0.0 {
    encode_order_score(0.0)
  } else {
    encode_order_score(max)
  };
  (min_order <= max_order).then_some((min_order, max_order))
}

/// 构造分值索引扫描起始键 `[0x01][min_order: 8B]`
#[inline]
fn score_start_key(min_order: [u8; 8]) -> [u8; 9] {
  let mut start_key = [0u8; 9];
  start_key[0] = PREFIX_SCORE;
  start_key[1..9].copy_from_slice(&min_order);
  start_key
}

/// ZRange 分值范围查询选项
///
/// 对标 C# libs/server/Objects/SortedSet/SortedSetObjectImpl.cs 中的 GetElementsInRangeByScore
#[derive(Debug, Clone, Copy)]
pub struct ZRangeByScoreOpt {
  pub min: f64,
  pub min_inc: bool,
  pub max: f64,
  pub max_inc: bool,
  pub offset: usize,
  pub limit: usize,
}

impl ZRangeByScoreOpt {
  /// 创建基础闭区间选项 [min, max]
  #[inline]
  pub const fn inclusive(min: f64, max: f64) -> Self {
    Self {
      min,
      min_inc: true,
      max,
      max_inc: true,
      offset: 0,
      limit: usize::MAX,
    }
  }

  /// 创建全配置选项
  #[inline]
  pub const fn new(
    min: f64,
    min_inc: bool,
    max: f64,
    max_inc: bool,
    offset: usize,
    limit: usize,
  ) -> Self {
    Self {
      min,
      min_inc,
      max,
      max_inc,
      offset,
      limit,
    }
  }
}

/// ZSet 树操作 Trait
///
/// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/SortedSetOps.cs
/// 在 garnet 中的相对路径:libs/server/Objects/SortedSet/SortedSetObjectImpl.cs
pub trait ZSetTreeOps {
  /// 添加或更新成员分值 (若为新添加返回 Ok(true)，更新已有成员返回 Ok(false))
  ///
  /// 对标 SortedSetOps.cs 中的 SortedSetAdd 与 SortedSetObjectImpl.cs 中的 SortedSetAdd
  fn zadd(&self, member: &[u8], score: f64) -> Result<bool>;

  /// 移除成员 (若存在且被移除返回 Ok(true)，不存在返回 Ok(false))
  ///
  /// 对标 SortedSetOps.cs 中的 SortedSetRemove 与 SortedSetObjectImpl.cs 中的 SortedSetRemove
  fn zrem(&self, member: &[u8]) -> Result<bool>;

  /// 查询成员分值
  ///
  /// 对标 SortedSetOps.cs 中的 SortedSetScore 与 SortedSetObjectImpl.cs 中的 SortedSetScore
  fn zscore(&self, member: &[u8]) -> Result<Option<f64>>;

  /// 统计区间 [min, max] 内的成员数量 (使用纯键零载荷扫描)
  ///
  /// 对标 SortedSetOps.cs 中的 SortedSetCount 与 SortedSetObjectImpl.cs 中的 SortedSetCount
  fn zcount(&self, min: f64, max: f64) -> Result<usize> {
    self.zcount_ext(min, true, max, true)
  }

  /// 统计分值区间内的成员数量 (支持开闭区间控制，使用纯键零载荷扫描)
  ///
  /// 对标 SortedSetObjectImpl.cs 中的 GetElementsInRangeByScore (count 分支)
  fn zcount_ext(&self, min: f64, min_inc: bool, max: f64, max_inc: bool) -> Result<usize>;

  /// 按分值范围流式扫描成员与分值
  ///
  /// 对标 SortedSetObjectImpl.cs 中的 GetElementsInRangeByScore 与 SortedSetOps.cs 中的 SortedSetRange
  fn zrange_by_score<F>(&self, min: f64, max: f64, on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool;

  /// 按分值范围流式扫描（支持开闭区间控制与 LIMIT offset count）
  ///
  /// 对标 SortedSetObjectImpl.cs 中的 GetElementsInRangeByScore (支持 minExclusive/maxExclusive 与 limit.offset/count)
  fn zrange_by_score_ext<F>(&self, opt: ZRangeByScoreOpt, on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool;

  /// 按下标排名范围流式扫描成员与分值 [start, stop]
  ///
  /// 对标 SortedSetObjectImpl.cs 中的 SortedSetRange (byIndex 分支，跳过 minIndex 并截取 n 项)
  fn zrange_by_index<F>(&self, start: usize, stop: usize, on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool;

  /// 全量遍历所有成员与分值 (按反查索引遍历，供自动降级合并使用)
  ///
  /// 对标 SortedSetObject.cs 中的 CopyDiff / 全量枚举器
  fn ziter_all<F>(&self, on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool;

  /// 获取有序集合总基数 (所有成员总数)
  ///
  /// 对标 SortedSetOps.cs 中的 SortedSetLength 与 SortedSetObject.cs 中的 Count
  fn zcard(&self) -> Result<usize> {
    self.zcount(f64::NEG_INFINITY, f64::INFINITY)
  }
}

impl ZSetTreeOps for BfTreeService {
  fn zadd(&self, member: &[u8], score: f64) -> Result<bool> {
    if score.is_nan() {
      return Err(CollectionError::InvalidArgument("score 不能为 NaN"));
    }
    let score = if score == 0.0 { 0.0 } else { score };
    let new_order = encode_order_score(score);
    let mut old_buf = [0u8; 8];

    with_member_key(member, |mem_key| {
      let (res, len) = self.read_into(mem_key, &mut old_buf);
      let exists = match res {
        BfTreeReadResult::Found if len == 8 => true,
        BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => false,
        BfTreeReadResult::Found => return Err(CollectionError::Corrupted("ZSet 反查索引长度异常")),
        _ => return Err(CollectionError::InvalidArgument("zadd 读取反查索引失败")),
      };

      if exists {
        if old_buf == new_order {
          return Ok(false);
        }
        with_score_key(&old_buf, member, |k| {
          self.delete(k);
        });
      }

      // 写入主分值索引
      let insert_res = with_score_key(&new_order, member, |k| self.insert(k, ZSET_VAL_PLACEHOLDER));
      if insert_res != BfTreeInsertResult::Success {
        return Err(CollectionError::InvalidArgument("zadd 写入分值索引失败"));
      }

      // 写入反查索引 (复用已构造好的 mem_key)
      let member_res = self.insert(mem_key, &new_order);
      if member_res != BfTreeInsertResult::Success {
        return Err(CollectionError::InvalidArgument("zadd 写入反查索引失败"));
      }

      Ok(!exists)
    })
  }

  fn zrem(&self, member: &[u8]) -> Result<bool> {
    let mut order_buf = [0u8; 8];
    with_member_key(member, |mem_key| {
      let (res, len) = self.read_into(mem_key, &mut order_buf);
      match res {
        BfTreeReadResult::Found if len == 8 => {
          with_score_key(&order_buf, member, |score_key| {
            self.delete(score_key);
          });
          self.delete(mem_key);
          Ok(true)
        }
        BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(false),
        BfTreeReadResult::Found => Err(CollectionError::Corrupted("ZSet 反查索引长度异常")),
        _ => Err(CollectionError::InvalidArgument("zrem 读取失败")),
      }
    })
  }

  fn zscore(&self, member: &[u8]) -> Result<Option<f64>> {
    let mut order_buf = [0u8; 8];
    with_member_key(member, |k| {
      let (res, len) = self.read_into(k, &mut order_buf);
      match res {
        BfTreeReadResult::Found if len == 8 => {
          let score = decode_order_score(order_buf);
          Ok(Some(if score == 0.0 { 0.0 } else { score }))
        }
        BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(None),
        BfTreeReadResult::Found => Err(CollectionError::Corrupted("ZSet 反查索引长度异常")),
        _ => Err(CollectionError::InvalidArgument("zscore 读取失败")),
      }
    })
  }

  fn zcount_ext(&self, min: f64, min_inc: bool, max: f64, max_inc: bool) -> Result<usize> {
    let Some((min_order, max_order)) = score_bounds(min, min_inc, max, max_inc) else {
      return Ok(0);
    };
    let start_key = score_start_key(min_order);

    let mut count = 0;
    self.scan_with_count_callback(&start_key, usize::MAX, ScanReturnField::Key, |k, _| {
      if k.len() < 9 || k[0] != PREFIX_SCORE {
        return false;
      }
      let order_bytes: [u8; 8] = unsafe { k.get_unchecked(1..9).try_into().unwrap_unchecked() };
      if order_bytes > max_order {
        return false;
      }
      let score = decode_order_score(order_bytes);
      let score = if score == 0.0 { 0.0 } else { score };

      if !min_inc && score == min {
        return true;
      }
      if !max_inc && score == max {
        return false;
      }

      count += 1;
      true
    })?;
    Ok(count)
  }

  fn zrange_by_score<F>(&self, min: f64, max: f64, on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    self.zrange_by_score_ext(ZRangeByScoreOpt::inclusive(min, max), on_item)
  }

  fn zrange_by_score_ext<F>(&self, opt: ZRangeByScoreOpt, mut on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    let (min, min_inc, max, max_inc, offset, limit) = (
      opt.min,
      opt.min_inc,
      opt.max,
      opt.max_inc,
      opt.offset,
      opt.limit,
    );
    if limit == 0 {
      return Ok(0);
    }
    let Some((min_order, max_order)) = score_bounds(min, min_inc, max, max_inc) else {
      return Ok(0);
    };
    let start_key = score_start_key(min_order);

    let mut skipped = 0;
    let mut count = 0;
    self.scan_with_count_callback(&start_key, usize::MAX, ScanReturnField::Key, |k, _| {
      if k.len() < 9 || k[0] != PREFIX_SCORE {
        return false;
      }
      // SAFETY: k.len() >= 9 已验证，k[1..9] 长度恒为 8
      let order_bytes: [u8; 8] = unsafe { k.get_unchecked(1..9).try_into().unwrap_unchecked() };
      if order_bytes > max_order {
        return false;
      }
      let score = decode_order_score(order_bytes);
      let score = if score == 0.0 { 0.0 } else { score };

      if !min_inc && score == min {
        return true;
      }
      if !max_inc && score == max {
        return false;
      }

      if skipped < offset {
        skipped += 1;
        return true;
      }

      count += 1;
      let member = &k[9..];
      !(!on_item(member, score) || count >= limit)
    })?;
    Ok(count)
  }

  fn zrange_by_index<F>(&self, start: usize, stop: usize, mut on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    if start > stop {
      return Ok(0);
    }
    let start_key = [PREFIX_SCORE];
    let mut current_idx = 0;
    let mut count = 0;
    self.scan_with_count_callback(&start_key, usize::MAX, ScanReturnField::Key, |k, _| {
      if k.len() < 9 || k[0] != PREFIX_SCORE {
        return false;
      }
      if current_idx < start {
        current_idx += 1;
        return true;
      }
      if current_idx > stop {
        return false;
      }
      current_idx += 1;
      count += 1;
      let order_bytes: [u8; 8] = unsafe { k.get_unchecked(1..9).try_into().unwrap_unchecked() };
      let score = decode_order_score(order_bytes);
      let score = if score == 0.0 { 0.0 } else { score };
      let member = &k[9..];
      let cont = on_item(member, score);
      cont && current_idx <= stop
    })?;
    Ok(count)
  }

  fn ziter_all<F>(&self, mut on_item: F) -> Result<usize>
  where
    F: FnMut(&[u8], f64) -> bool,
  {
    let start_key = [PREFIX_MEMBER];
    let mut count = 0;
    self.scan_with_count_callback(
      &start_key,
      usize::MAX,
      ScanReturnField::KeyAndValue,
      |k, v| {
        if k.is_empty() || k[0] != PREFIX_MEMBER {
          return false;
        }
        if v.len() < 8 {
          return false;
        }
        let order_bytes: [u8; 8] = unsafe { v.get_unchecked(..8).try_into().unwrap_unchecked() };
        let score = decode_order_score(order_bytes);
        let score = if score == 0.0 { 0.0 } else { score };
        let member = &k[1..];
        count += 1;
        on_item(member, score)
      },
    )?;
    Ok(count)
  }
}
