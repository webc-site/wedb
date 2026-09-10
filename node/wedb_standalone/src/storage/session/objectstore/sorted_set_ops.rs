//! 有序集合对象操作（对标 libs/server/Storage/Session/ObjectStore/SortedSetOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 wobject [`SortedSetObject`]（dict +
//! tree 双索引）；排序视图按 (score, member) 字典序现算。空集合整键回收。

use std::io::Cursor;

use gxhash::HashMap as GxHashMap;
use wdev::Device;
use wobject::sorted_set::sorted_set_object::{SortedSetObject, SortedSetOperation};

use super::{
  super::storage_session::StorageSession,
  common::{ObjState, RmwOutcome},
};
use crate::{api::garnet_status::GarnetStatus, objects::parse_utils::try_parse_with_infinity};

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

impl<'a, D: Device> StorageSession<'a, D> {
  /// 有序集合读-改-写：载荷解码为 SortedSetObject 后交闭包变更，返回前自动回写
  ///
  /// `create` 为假时键缺失直接 Aborted（不物化空有序集合信封）
  async fn zset_rmw<R>(
    &self,
    key: &[u8],
    create: bool,
    f: impl FnOnce(&mut SortedSetObject) -> Option<R>,
  ) -> wkv::Result<RmwOutcome<R>> {
    self
      .rmw_object_store_operation(key, super::common::OBJ_TAG_SORTED_SET, |payload| {
        let mut obj = match payload {
          Some(bytes) => SortedSetObject::deserialize(&mut Cursor::new(bytes)).unwrap_or_default(),
          None if create => SortedSetObject::new(),
          None => return None,
        };
        let r = f(&mut obj)?;
        let mut out = Vec::new();
        obj.serialize(&mut out).ok()?;
        Some((out, r))
      })
      .await
  }

  /// 装载有序集合（缺失/类型不符快速出口；读路径专用）
  pub(crate) async fn zset_load(
    &self,
    key: &[u8],
  ) -> wkv::Result<Result<Option<SortedSetObject>, GarnetStatus>> {
    Ok(
      match self
        .obj_load(key, super::common::OBJ_TAG_SORTED_SET)
        .await?
      {
        ObjState::Absent => Ok(None),
        ObjState::WrongType => Err(GarnetStatus::WrongType),
        ObjState::Present(p) => Ok(Some(
          SortedSetObject::deserialize(&mut Cursor::new(p)).unwrap_or_default(),
        )),
      },
    )
  }

  /// ZADD：批量添加/更新（NX 仅新增、GT/LT 阈值更新、CH 统计变更），返回计数
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
      .zset_rmw(key, true, |obj| {
        let mut count = 0i64;
        for &(member, score) in members {
          match obj.dict.pin().get(member).copied() {
            Some(_prev) if nx => {}
            Some(prev) => {
              let update = (gt && score > prev) || (lt && score < prev) || (!gt && !lt);
              if update {
                obj.operate(SortedSetOperation::Zadd, member, score);
                if ch {
                  count += 1;
                }
              }
            }
            None => {
              obj.operate(SortedSetOperation::Zadd, member, score);
              count += 1;
            }
          }
        }
        // 计数为 0 时不写回：键缺失则放弃物化空集合，键已存在则载荷不变
        (count > 0).then_some(count)
      })
      .await?;
    match added {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      outcome => Ok((GarnetStatus::Ok, outcome.unwrap_or(0))),
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
    let removed = self
      .zset_rmw(key, false, |obj| {
        let mut n = 0i64;
        for m in members {
          if obj.operate(SortedSetOperation::Zrem, m, 0.0).is_some() {
            n += 1;
          }
        }
        Some((n, obj.dict.pin().is_empty()))
      })
      .await?;
    match removed {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      // 键缺失：NOTFOUND（C# NeedToCreate(ZREM)=false）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, 0)),
      outcome => {
        let n = self.finalize_removal(key, outcome, 0).await?;
        Ok((GarnetStatus::Ok, n))
      }
    }
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
    let removed = self
      .zset_rmw(key, false, |obj| {
        let entries = sorted_view(obj);
        let mut n = 0i64;
        for (m, _s) in entries {
          if lex_in_range(&m, min, max) && obj.operate(SortedSetOperation::Zrem, &m, 0.0).is_some()
          {
            n += 1;
          }
        }
        Some((n, obj.dict.pin().is_empty()))
      })
      .await?;
    match removed {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      // 键缺失：NOTFOUND（C# NeedToCreate=false）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, 0)),
      outcome => {
        let n = self.finalize_removal(key, outcome, 0).await?;
        Ok((GarnetStatus::Ok, n))
      }
    }
  }

  /// ZREMRANGEBYSCORE：按分值区间移除（端点开闭语义见 [`parse_score_bound`]）
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
    let removed = self
      .zset_rmw(key, false, |obj| {
        let entries = sorted_view(obj);
        let mut n = 0i64;
        for (m, s) in entries {
          if score_in_range(s, min_b, max_b)
            && obj.operate(SortedSetOperation::Zrem, &m, 0.0).is_some()
          {
            n += 1;
          }
        }
        Some((n, obj.dict.pin().is_empty()))
      })
      .await?;
    match removed {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      // 键缺失：NOTFOUND（C# NeedToCreate=false）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, 0)),
      outcome => {
        let n = self.finalize_removal(key, outcome, 0).await?;
        Ok((GarnetStatus::Ok, n))
      }
    }
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
    let removed = self
      .zset_rmw(key, false, |obj| {
        let entries = sorted_view(obj);
        let (lo, hi) = clamp_rank_range(start, stop, entries.len());
        let mut n = 0i64;
        for (m, _s) in entries.into_iter().take(hi + 1).skip(lo) {
          if obj.operate(SortedSetOperation::Zrem, &m, 0.0).is_some() {
            n += 1;
          }
        }
        Some((n, obj.dict.pin().is_empty()))
      })
      .await?;
    match removed {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      // 键缺失：NOTFOUND（C# NeedToCreate=false）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, 0)),
      outcome => {
        let n = self.finalize_removal(key, outcome, 0).await?;
        Ok((GarnetStatus::Ok, n))
      }
    }
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
    let popped = self
      .zset_rmw(key, false, |obj| {
        let mut entries = sorted_view(obj);
        let mut out = Vec::with_capacity(count.min(entries.len()));
        for _ in 0..count {
          let item = if min {
            entries.first().cloned()
          } else {
            entries.last().cloned()
          };
          match item {
            Some((m, s)) => {
              obj.operate(SortedSetOperation::Zrem, &m, 0.0);
              if min {
                entries.remove(0);
              } else {
                entries.pop();
              }
              out.push((m, s));
            }
            None => break,
          }
        }
        Some((out, obj.dict.pin().is_empty()))
      })
      .await?;
    match popped {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, Vec::new())),
      // 键缺失：NOTFOUND（C# NeedToCreate(ZPOPMIN/ZPOPMAX)=false）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, Vec::new())),
      outcome => {
        let out = self.finalize_removal(key, outcome, Vec::new()).await?;
        Ok((GarnetStatus::Ok, out))
      }
    }
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
    // Zincrby 语义：传入增量，返回新分值（见 wobject operate；闭包 None 即放弃）
    let outcome = self
      .zset_rmw(key, true, |obj| {
        obj
          .operate(SortedSetOperation::Zincrby, member, delta)
          .map(Some)
      })
      .await?;
    match outcome {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, None)),
      RmwOutcome::Aborted => Ok((GarnetStatus::Ok, None)),
      RmwOutcome::Written(score) => Ok((GarnetStatus::Ok, score)),
    }
  }

  /// ZCARD：成员数
  ///
  /// 键缺失返回 NOTFOUND（C# SortedSetLength → ReadObjectStoreOperation，RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetLength
  pub async fn sorted_set_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    match self.zset_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::NotFound, 0)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.count())),
    }
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
        let mut entries = sorted_view(&obj);
        if rev {
          entries.reverse();
        }
        let (lo, hi) = clamp_rank_range(start, stop, entries.len());
        Ok((
          GarnetStatus::Ok,
          entries
            .into_iter()
            .take(hi + 1)
            .skip(lo)
            .map(|(m, s)| (m, with_scores.then_some(s)))
            .collect(),
        ))
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
          let pin = other.dict.pin();
          result.retain(|(m, _)| !pin.contains_key(m));
        }
        Ok(None) => {}
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
        let mut entries = sorted_view(&obj);
        if rev {
          entries.reverse();
        }
        Ok((
          GarnetStatus::Ok,
          entries
            .iter()
            .position(|(m, _)| m == member)
            .map(|p| p as i64),
        ))
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
    match self.zset_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) => Ok((GarnetStatus::NotFound, None)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.dict.pin().get(member).copied())),
    }
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
      Ok(None) => Ok((
        GarnetStatus::NotFound,
        members.iter().map(|_| None).collect(),
      )),
      Ok(Some(obj)) => {
        let pin = obj.dict.pin();
        Ok((
          GarnetStatus::Ok,
          members.iter().map(|m| pin.get(*m).copied()).collect(),
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
        let n = sorted_view(&obj)
          .into_iter()
          .filter(|(_, s)| score_in_range(*s, min_b, max_b))
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
        let n = sorted_view(&obj)
          .into_iter()
          .filter(|(m, _)| lex_in_range(m, min, max))
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
        let last = pool.len() - 1;
        let idx = fastrand::usize(..pool.len());
        pool.swap(idx, last);
        out.push(pool.pop().unwrap_or_default());
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
      SortedSetObject::deserialize(&mut Cursor::new(payload.to_vec()))
        .ok()
        .map(|o| sorted_view(&o).into_iter().map(|(m, _)| m).collect())
    };
    self
      .object_scan(
        key,
        super::common::OBJ_TAG_SORTED_SET,
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

  /// ZEXPIRE/ZPEXPIRE：相对毫秒过期（整键级）
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(ZEXPIRE)=false，RMW 缺键直返
  /// NOTFOUND；RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetExpire
  pub async fn sorted_set_expire(
    &self,
    key: &[u8],
    ttl_ms: u64,
  ) -> wkv::Result<(GarnetStatus, bool)> {
    let set = self.expire_in_ms(key, ttl_ms).await?;
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
      let pin = obj.dict.pin();
      if i == 0 || !intersect {
        // 并语义（含首键）：插入或聚合累加
        for (m, &s) in pin.iter() {
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
        acc.retain(|m, s| match pin.get(m) {
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
/// 直接迭代 wobject 双索引中的 BTreeSet 有序树（条目序即 (score, member) 序），
/// 免去字典快照 + O(N log N) 重排序；树由 operate/pop_min/pop_max 全路径同步维护，
/// 恒与字典一致，O(N) 单遍导出
fn sorted_view(obj: &SortedSetObject) -> Vec<(Vec<u8>, f64)> {
  obj
    .tree
    .lock()
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
