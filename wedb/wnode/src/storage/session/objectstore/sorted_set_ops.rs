//! 有序集合对象操作（对标 libs/server/Storage/Session/ObjectStore/SortedSetOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 [`SortedSetObject`]（dict +
//! tree 双索引）；排序视图按 (score, member) 字典序现算。空集合整键回收。

use gxhash::HashMap as GxHashMap;
use wdev::Device;

use super::{
  super::storage_session::StorageSession,
  common::{GarnetObjectPayload, RmwOutcome},
};
use crate::{
  objects::{parse_utils::try_parse_with_infinity, sortedset::sorted_set_object::SortedSetObject},
  types::GarnetStatus,
};

/// 聚合方式（ZUNION/ZINTER 权重合并语义）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZSetAggregate {
  /// 求和
  Sum,
  /// 取最小
  Min,
  /// 取最大
  Max,
}

/// 移除区间种类（C# SortedSetRemoveRange 按 RangeType 分发）
#[derive(Debug, Clone, Copy)]
pub enum ZSetRemoveRange<'k> {
  /// 按排名闭区间
  Rank(i64, i64),
  /// 按分值区间（Redis 语法文本：`(5` / `[5` / `-inf` / `+inf`）
  Score(&'k [u8], &'k [u8]),
  /// 按字典序区间（`[a` / `(a` / `-` / `+`）
  Lex(&'k [u8], &'k [u8]),
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 装载有序集合（缺失/类型不符快速出口；读路径专用）
  pub(crate) async fn zset_load(
    &self,
    key: &[u8],
  ) -> wkv::Result<Result<Option<SortedSetObject>, GarnetStatus>> {
    self.typed_load::<SortedSetObject>(key).await
  }

  /// ZADD：添加或更新成员分值
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetAdd
  pub async fn sorted_set_add(
    &self,
    key: &[u8],
    members: &[(&[u8], f64)],
    nx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let added = self
      .typed_rmw::<SortedSetObject, i64>(key, true, |obj| {
        let mut count = 0i64;
        let mut modified = false;
        for &(member, score) in members {
          match obj.sorted_set_dict.get(member).copied() {
            Some(_prev) if nx => {}
            Some(prev) => {
              if score == prev {
                continue;
              }
              let update = (gt && score > prev) || (lt && score < prev) || (!gt && !lt);
              if update {
                obj.add(member, score);
                modified = true;
                if ch {
                  count += 1;
                }
              }
            }
            None => {
              obj.add(member, score);
              modified = true;
              count += 1;
            }
          }
        }
        if !modified { None } else { Some(count) }
      })
      .await?;
    match added {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      RmwOutcome::Aborted => Ok((GarnetStatus::Ok, 0)),
      RmwOutcome::Written(count) => Ok((GarnetStatus::Ok, count)),
    }
  }

  /// ZREM：批量移除成员
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZREM)=false，不物化空有序集合）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRemove
  pub async fn sorted_set_remove(
    &self,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove::<SortedSetObject, i64>(key, 0, |obj| {
        let mut n = 0i64;
        for m in members {
          if obj.rem(m).is_some() {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// ZREMRANGEBYLEX：按字典序区间移除
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZREMRANGEBYLEX)=false）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRemoveRangeByLex
  pub async fn sorted_set_remove_range_by_lex(
    &self,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove::<SortedSetObject, i64>(key, 0, |obj| {
        let to_remove: Vec<Vec<u8>> = obj
          .sorted_set
          .iter()
          .filter(|e| lex_in_range(&e.member, min, max))
          .map(|e| e.member.clone())
          .collect();
        let mut n = 0i64;
        for m in to_remove {
          if obj.rem(&m).is_some() {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// ZREMRANGEBYSCORE：按分值区间移除（端点开闭语义见 `parse_score_bound`）
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZREMRANGEBYSCORE)=false）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRemoveRangeByScore
  pub async fn sorted_set_remove_range_by_score(
    &self,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let (Ok(min_b), Ok(max_b)) = (parse_score_bound(min), parse_score_bound(max)) else {
      return Ok((GarnetStatus::WrongType, 0));
    };
    self
      .typed_remove::<SortedSetObject, i64>(key, 0, |obj| {
        let to_remove: Vec<Vec<u8>> = obj
          .sorted_set
          .iter()
          .filter(|e| score_in_range(e.score, min_b, max_b))
          .map(|e| e.member.clone())
          .collect();
        let mut n = 0i64;
        for m in to_remove {
          if obj.rem(&m).is_some() {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// ZREMRANGEBYRANK：按排名闭区间移除
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZREMRANGEBYRANK)=false）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRemoveRangeByRank
  pub async fn sorted_set_remove_range_by_rank(
    &self,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove::<SortedSetObject, i64>(key, 0, |obj| {
        let (lo, hi) = clamp_rank_range(start, stop, obj.sorted_set.len());
        if lo > hi {
          return Some(0);
        }
        let to_remove: Vec<Vec<u8>> = obj
          .sorted_set
          .iter()
          .skip(lo)
          .take(hi - lo + 1)
          .map(|e| e.member.clone())
          .collect();
        let mut n = 0i64;
        for m in to_remove {
          if obj.rem(&m).is_some() {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// ZPOPMIN/ZPOPMAX：按分值端点弹出
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate=false，RESP 层据此写空数组）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetPop
  pub async fn sorted_set_pop(
    &self,
    key: &[u8],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    self
      .typed_remove::<SortedSetObject, Vec<(Vec<u8>, f64)>>(key, Vec::new(), |obj| {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
          let item = if min { obj.pop_min() } else { obj.pop_max() };
          match item {
            Some((m, s)) => out.push((m, s)),
            None => break,
          }
        }
        Some(out)
      })
      .await
  }

  /// ZINCRBY / ZADD INCR：分值增减，返回新分值
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIncrement
  pub async fn sorted_set_increment(
    &self,
    key: &[u8],
    member: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    let outcome = self
      .typed_rmw::<SortedSetObject, f64>(key, true, |obj| {
        let score = obj.incr_by(member, delta);
        if score.is_nan() { None } else { Some(score) }
      })
      .await?;
    match outcome {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, None)),
      RmwOutcome::Aborted => Ok((GarnetStatus::Ok, None)),
      RmwOutcome::Written(score) => Ok((GarnetStatus::Ok, Some(score))),
    }
  }

  /// ZCARD：成员数
  ///
  /// 键缺失返回 NOTFOUND（C# SortedSetLength → ReadObjectStoreOperation，RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetLength
  pub async fn sorted_set_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    self
      .typed_query::<SortedSetObject, _>(key, 0, |obj| obj.count())
      .await
  }

  /// ZRANGE/ZREVRANGE：排名区间（`with_scores` 附带分值）
  ///
  /// 键缺失返回 NOTFOUND（C# SortedSetRange → ReadObjectStoreOperation）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRange
  pub async fn sorted_set_range(
    &self,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    match self.zset_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::NotFound, Vec::new())),
      Ok(Some(obj)) => {
        let len = obj.sorted_set.len();
        let (lo, hi) = clamp_rank_range(start, stop, len);
        if lo > hi {
          return Ok((GarnetStatus::Ok, Vec::new()));
        }
        let count = hi - lo + 1;
        let out: Vec<(Vec<u8>, Option<f64>)> = if rev {
          obj
            .sorted_set
            .iter()
            .rev()
            .skip(lo)
            .take(count)
            .map(|e| (e.member.clone(), with_scores.then_some(e.score)))
            .collect()
        } else {
          obj
            .sorted_set
            .iter()
            .skip(lo)
            .take(count)
            .map(|e| (e.member.clone(), with_scores.then_some(e.score)))
            .collect()
        };
        Ok((GarnetStatus::Ok, out))
      }
    }
  }

  /// ZDIFF：多集合差集（首键减其余；错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetDifference
  pub async fn sorted_set_difference(
    &self,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    let Some(first) = keys.first() else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let base = match self.zset_load(first).await? {
      Err(s) => return Ok((s, Vec::new())),
      Ok(None) => return Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(o)) => o,
    };
    let mut result = sorted_view(&base);
    for key in &keys[1..] {
      match self.zset_load(key).await? {
        Err(s) => return Ok((s, Vec::new())),
        Ok(Some(other)) => {
          let dict = &other.sorted_set_dict;
          result.retain(|(m, _)| !dict.contains_key(m));
        }
        Ok(None) => {}
      }
      if result.is_empty() {
        break;
      }
    }
    Ok((GarnetStatus::Ok, result))
  }

  /// ZDIFFSTORE：差集写入目标键
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetDifferenceStore
  pub async fn sorted_set_difference_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, entries) = self.sorted_set_difference(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    self.zset_overwrite(dest, &entries).await
  }

  /// ZRANK/ZREVRANK：成员排名（0 基，缺失 None）
  ///
  /// 键缺失返回 NOTFOUND（RESP 层同答 null）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRank
  pub async fn sorted_set_rank(
    &self,
    key: &[u8],
    member: &[u8],
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, Option<i64>)> {
    match self.zset_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) => Ok((GarnetStatus::NotFound, None)),
      Ok(Some(obj)) => {
        let pos = if rev {
          obj
            .sorted_set
            .iter()
            .rev()
            .position(|e| e.member.as_slice() == member)
        } else {
          obj
            .sorted_set
            .iter()
            .position(|e| e.member.as_slice() == member)
        };
        Ok((GarnetStatus::Ok, pos.map(|p| p as i64)))
      }
    }
  }

  /// ZRANGESTORE：排名区间切片写入目标键
  ///
  /// 源键缺失：等价空区间——删除目标键后返回 (Ok, 0)（对齐 C#
  /// SortedSetRangeStore 的 NOTFOUND 分支：EXPIRE(dst, TimeSpan.Zero)）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRangeStore
  pub async fn sorted_set_range_store(
    &self,
    dest: &[u8],
    src: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    // 须带分值读取：with_scores=false 时分值恒 None，下方 filter_map 会把
    // 全部成员丢弃（ZRANGESTORE 空写的存量缺陷）
    let (status, range) = self.sorted_set_range(src, start, stop, rev, true).await?;
    let entries: Vec<(Vec<u8>, f64)> = match status {
      GarnetStatus::Ok => range
        .into_iter()
        .filter_map(|(m, s)| s.map(|score| (m, score)))
        .collect(),
      // 源缺失：删除目标键、按 0 成功返回（C# NOTFOUND 分支）
      GarnetStatus::NotFound => Vec::new(),
      _ => return Ok((status, 0)),
    };
    self.zset_overwrite(dest, &entries).await
  }

  /// ZSCORE：单成员分值
  ///
  /// 键缺失返回 NOTFOUND（RESP 层同答 null）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetScore
  pub async fn sorted_set_score(
    &self,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    self
      .typed_query::<SortedSetObject, _>(key, None, |obj| obj.sorted_set_dict.get(member).copied())
      .await
  }

  /// ZMSCORE：多成员分值（缺失占位 None）
  ///
  /// 键缺失返回 NOTFOUND（载荷仍按成员数占位 None，对齐 RESP 渲染）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetScores
  pub async fn sorted_set_scores(
    &self,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<f64>>)> {
    match self.zset_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::NotFound, vec![None; members.len()])),
      Ok(Some(obj)) => {
        let dict = &obj.sorted_set_dict;
        Ok((
          GarnetStatus::Ok,
          members.iter().map(|m| dict.get(*m).copied()).collect(),
        ))
      }
    }
  }

  /// ZCOUNT：分值区间成员数
  ///
  /// 键缺失返回 NOTFOUND（RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetCount
  pub async fn sorted_set_count(
    &self,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let (Ok(min_b), Ok(max_b)) = (parse_score_bound(min), parse_score_bound(max)) else {
      return Ok((GarnetStatus::WrongType, 0));
    };
    match self.zset_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::NotFound, 0)),
      Ok(Some(obj)) => {
        let n = obj
          .sorted_set
          .iter()
          .filter(|e| score_in_range(e.score, min_b, max_b))
          .count();
        Ok((GarnetStatus::Ok, n as i64))
      }
    }
  }

  /// ZLEXCOUNT：字典序区间成员数
  ///
  /// 键缺失返回 NOTFOUND。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetLengthByValue
  pub async fn sorted_set_length_by_value(
    &self,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    match self.zset_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::NotFound, 0)),
      Ok(Some(obj)) => {
        let n = obj
          .sorted_set
          .iter()
          .filter(|e| lex_in_range(&e.member, min, max))
          .count();
        Ok((GarnetStatus::Ok, n as i64))
      }
    }
  }

  /// ZREMRANGE 统一分发入口（按排名 / 分值 / 字典序）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRemoveRange
  pub async fn sorted_set_remove_range(
    &self,
    key: &[u8],
    range: ZSetRemoveRange<'_>,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    match range {
      ZSetRemoveRange::Rank(start, stop) => {
        self.sorted_set_remove_range_by_rank(key, start, stop).await
      }
      ZSetRemoveRange::Score(min, max) => {
        self.sorted_set_remove_range_by_score(key, min, max).await
      }
      ZSetRemoveRange::Lex(min, max) => self.sorted_set_remove_range_by_lex(key, min, max).await,
    }
  }

  /// ZRANDMEMBER：随机取样成员
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRandomMember
  pub async fn sorted_set_random_member(
    &self,
    key: &[u8],
    count: i64,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    let (status, entries) = self
      .sorted_set_range(key, 0, -1, false, with_scores)
      .await?;
    if status != GarnetStatus::Ok || entries.is_empty() {
      return Ok((status, Vec::new()));
    }
    let out = if count < 0 {
      let n = count.unsigned_abs();
      (0..n)
        .map(|_| entries[fastrand::usize(..entries.len())].clone())
        .collect()
    } else {
      let mut pool = entries;
      let n = count.unsigned_abs().min(pool.len() as u64) as usize;
      let mut out = Vec::with_capacity(n);
      for _ in 0..n {
        let idx = fastrand::usize(..pool.len());
        out.push(pool.swap_remove(idx));
      }
      out
    };
    Ok((GarnetStatus::Ok, out))
  }

  /// ZSCAN：成员增量扫描（游标 = 上次返回的最后一个成员）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetScan
  pub async fn sorted_set_scan(
    &self,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    let members_of = |payload: &[u8]| -> Option<Vec<Vec<u8>>> {
      Some(
        SortedSetObject::deserialize_from_slice(payload)
          .sorted_set
          .iter()
          .map(|e| e.member.clone())
          .collect(),
      )
    };
    self
      .object_scan(
        key,
        wval::GarnetObjectType::SortedSet as u8,
        pattern,
        cursor,
        count,
        members_of,
      )
      .await
  }

  /// ZUNION：多集合并集（权重 + 聚合；错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetUnion
  pub async fn sorted_set_union(
    &self,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    self.zset_combine(keys, weights, aggregate, false).await
  }

  /// ZUNIONSTORE：并集写入目标键
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetUnionStore
  pub async fn sorted_set_union_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, combined) = self.zset_combine(keys, weights, aggregate, false).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    self.zset_overwrite(dest, &combined).await
  }

  /// ZMPOP：依次寻找首个非空集合并弹出端点成员
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetMPop
  pub async fn sorted_set_m_pop(
    &self,
    keys: &[&[u8]],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Option<(Vec<u8>, Vec<(Vec<u8>, f64)>)>)> {
    for key in keys {
      let (status, popped) = self.sorted_set_pop(key, count, min).await?;
      if !popped.is_empty() {
        return Ok((GarnetStatus::Ok, Some(((*key).to_vec(), popped))));
      }
      if status != GarnetStatus::Ok && status != GarnetStatus::NotFound {
        // 非 OK 且非缺失（如 WRONGTYPE）立即传播（对齐 C# SortedSetMPop）
        return Ok((status, None));
      }
    }
    Ok((GarnetStatus::Ok, None))
  }

  /// ZINTERCARD：交集基数
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersectLength
  pub async fn sorted_set_intersect_length(
    &self,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, entries) = self.zset_combine(keys, weights, aggregate, true).await?;
    Ok((status, entries.len()))
  }

  /// ZINTERSTORE：交集写入目标键
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersectStore
  pub async fn sorted_set_intersect_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, entries) = self.zset_combine(keys, weights, aggregate, true).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    self.zset_overwrite(dest, &entries).await
  }

  /// ZINTER：多集合交集（权重 + 聚合；错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersect
  pub async fn sorted_set_intersect(
    &self,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    self.zset_combine(keys, weights, aggregate, true).await
  }

  /// 交集计算（ZINTER 纯计算视图）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersection
  pub async fn sorted_set_intersection(
    &self,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    self.zset_combine(keys, weights, aggregate, true).await
  }

  /// ZEXPIRE/ZPEXPIRE：相对时长过期（整键级，TimeSpan 口径 ticks）
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZEXPIRE)=false，RMW 缺键直返
  /// NOTFOUND；RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetExpire
  pub async fn sorted_set_expire(
    &self,
    key: &[u8],
    ttl_ticks: i64,
  ) -> wkv::Result<(GarnetStatus, bool)> {
    let set = self.expire_in_ticks(key, ttl_ticks).await?;
    Ok(if set == 1 {
      (GarnetStatus::Ok, true)
    } else {
      (GarnetStatus::NotFound, false)
    })
  }

  /// ZPTTL：剩余生存毫秒
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetTimeToLive
  pub async fn sorted_set_time_to_live(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, i64)> {
    let pttl = self.pttl_ms(key).await?;
    Ok((
      if pttl == -2 {
        GarnetStatus::NotFound
      } else {
        GarnetStatus::Ok
      },
      pttl,
    ))
  }

  /// ZPERSIST：移除过期
  ///
  /// 键缺失返回 NOTFOUND（C# RMW 缺键口径）；键在但无 TTL 为 (Ok, false)
  /// ——wkv persist 对"缺键"与"无 TTL"同返 0，故以 pttl 先行辨缺键。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetPersist
  pub async fn sorted_set_persist(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, bool)> {
    if self.pttl_ms(key).await? == -2 {
      return Ok((GarnetStatus::NotFound, false));
    }
    let removed = self.persist_key(key).await?;
    Ok((GarnetStatus::Ok, removed == 1))
  }

  /// 对象回收统计（键内成员数）
  ///
  /// 缺口说明：C# 侧 SortedSetCollect 由对象回收任务统计/驱逐堆对象；
  /// wkv 对象生命周期由引擎 GC 统一管理，此处退化为成员数统计。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetCollect
  pub async fn sorted_set_collect(&self, key: &[u8]) -> wkv::Result<usize> {
    Ok(self.sorted_set_length(key).await?.1)
  }

  /// 多键组合内核（并/交统一：`intersect` 选择交语义）
  ///
  /// 错误类型键传播 WRONGTYPE；缺键按空集参与（交语义短路为空）。
  /// 哈希累加 + 末次排序：单遍 O(n) 折叠，产出 (score, member) 排名序。
  /// 交语义聚合出 NaN 时归零（C# 缺陷兼容，见保留分支内注释）。
  async fn zset_combine(
    &self,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
    intersect: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    let mut acc: GxHashMap<Vec<u8>, f64> = GxHashMap::default();
    for (i, key) in keys.iter().enumerate() {
      // 交集已空：与空集相交恒空，提前结束
      if intersect && i > 0 && acc.is_empty() {
        break;
      }
      let w = weights.get(i).copied().unwrap_or(1.0);
      let obj = match self.zset_load(key).await? {
        Err(s) => return Ok((s, Vec::new())),
        // 缺键按空集：并语义跳过；交语义短路为空（对齐 C# SortedSetIntersection
        // 的 NOTFOUND 分支：pairs 清空后立即返回，不得携带前序键的累加结果）
        Ok(None) => {
          if intersect {
            return Ok((GarnetStatus::Ok, Vec::new()));
          }
          continue;
        }
        Ok(Some(o)) => o,
      };
      let dict = &obj.sorted_set_dict;
      if i == 0 {
        acc.reserve(dict.len());
      }
      if i == 0 || !intersect {
        // 并语义（含首键）：插入或聚合累加
        for (m, &s) in dict.iter() {
          let ws = s * w;
          match acc.get_mut(m) {
            Some(slot) => *slot = apply_aggregate(aggregate, *slot, ws),
            None => {
              acc.insert(m.clone(), ws);
            }
          }
        }
      } else {
        // 交语义：仅保留本键也有的成员并聚合分值
        acc.retain(|m, s| match dict.get(m) {
          Some(&other) => {
            *s = apply_aggregate(aggregate, *s, other * w);
            // NaN → 0：兼容 C# SortedSetIntersection 的显式缺陷行为
            //（"That's what the references do. Arguably we're doing bug
            // compatible behaviour here."，Sum 遇 +inf/-inf 相加产生 NaN 时
            // 归零；ZUNION 路径无此逻辑，不在此套用）
            if s.is_nan() {
              *s = 0.0;
            }
            true
          }
          None => false,
        });
      }
    }
    let mut out: Vec<(Vec<u8>, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    Ok((GarnetStatus::Ok, out))
  }

  /// 覆写目标键为给定成员集合（先删后写，空集回收）
  async fn zset_overwrite(
    &self,
    dest: &[u8],
    entries: &[(Vec<u8>, f64)],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let _ = self.delete_string(dest).await?;
    if entries.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let refs: Vec<(&[u8], f64)> = entries.iter().map(|(m, s)| (m.as_slice(), *s)).collect();
    let (..) = self
      .sorted_set_add(dest, &refs, false, false, false, false)
      .await?;
    Ok((GarnetStatus::Ok, entries.len()))
  }
}

/// (score, member) 排序视图（Redis 排名序：分值升序、同分按成员字典序）
///
/// 直接迭代 SortedSetObject 双索引中的 BTreeSet 有序树（条目序即 (score, member) 序），
/// 免去字典快照 + O(N log N) 重排序；树由 add/rem/pop_min/pop_max 全路径同步维护，
/// 恒与字典一致，O(N) 单遍导出
fn sorted_view(obj: &SortedSetObject) -> Vec<(Vec<u8>, f64)> {
  obj
    .sorted_set
    .iter()
    .map(|e| (e.member.clone(), e.score))
    .collect()
}

/// 聚合两分值
fn apply_aggregate(agg: ZSetAggregate, a: f64, b: f64) -> f64 {
  match agg {
    ZSetAggregate::Sum => a + b,
    ZSetAggregate::Min => a.min(b),
    ZSetAggregate::Max => a.max(b),
  }
}

/// 排名区间归一化（负数自尾计数，返回非负闭区间 [lo, hi]，空集为 (1, 0)）
fn clamp_rank_range(start: i64, stop: i64, len: usize) -> (usize, usize) {
  if len == 0 {
    return (1, 0);
  }
  let len_i = len as i64;
  let s = if start < 0 { len_i + start } else { start }.max(0);
  let e = if stop < 0 { len_i + stop } else { stop }.min(len_i - 1);
  if s > e {
    (1, 0)
  } else {
    (s as usize, e as usize)
  }
}

/// 解析分值区间端点：`(5` / `[5` / `-inf` / `+inf` → (值, 是否含端点)
fn parse_score_bound(b: &[u8]) -> Result<(f64, bool), ()> {
  if b.is_empty() {
    return Err(());
  }
  match b[0] {
    // 数值部分统一走 NumUtils.TryParseWithInfinity 单一实现
    //（inf/+inf/-inf 词形白名单，nan/Infinity 词形拒绝，溢出 ±inf 保留）
    b'(' => try_parse_with_infinity(&b[1..])
      .map(|v| (v, false))
      .ok_or(()),
    b'[' => try_parse_with_infinity(&b[1..])
      .map(|v| (v, true))
      .ok_or(()),
    _ => try_parse_with_infinity(b).map(|v| (v, true)).ok_or(()),
  }
}

/// 分值是否落在区间内（含端点标记）
fn score_in_range(s: f64, min: (f64, bool), max: (f64, bool)) -> bool {
  let ge = if min.1 { s >= min.0 } else { s > min.0 };
  let le = if max.1 { s <= max.0 } else { s < max.0 };
  ge && le
}

/// 成员是否落在字典序区间内（`[x` 含、`(x` 不含、`-`/`+` 开区间）
fn lex_in_range(member: &[u8], min: &[u8], max: &[u8]) -> bool {
  let ge_min = match min {
    b"-" => true,
    [b'(', rest @ ..] => member > rest,
    [b'[', rest @ ..] => member >= rest,
    _ => true,
  };
  let le_max = match max {
    b"+" => true,
    [b'(', rest @ ..] => member < rest,
    [b'[', rest @ ..] => member <= rest,
    _ => true,
  };
  ge_min && le_max
}
