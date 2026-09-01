use rapidhash::{HashSetExt, RapidHashMap as HashMap, RapidHashSet as HashSet};
use std::cmp::Ordering;
use std::str::from_utf8;
use std::sync::Arc;

use super::context::{
  ConnectionContext, KeyComposer, RangeLexSpec, RangeScoreSpec, ZSetMeta, decode_sortable_f64,
  encode_sortable_f64, matches_glob_bytes, normalize_range,
};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::{int_to_blob, score_to_blob};
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

/// 8 字节十六进制解码为字节数组
#[inline]
fn decode_hex_8(s: &[u8]) -> Option<[u8; 8]> {
  if s.len() != 16 {
    return None;
  }
  let mut out = [0u8; 8];
  for (i, slot) in out.iter_mut().enumerate() {
    let hi = match s[i * 2] {
      b'0'..=b'9' => s[i * 2] - b'0',
      b'a'..=b'f' => s[i * 2] - b'a' + 10,
      b'A'..=b'F' => s[i * 2] - b'A' + 10,
      _ => return None,
    };
    let lo = match s[i * 2 + 1] {
      b'0'..=b'9' => s[i * 2 + 1] - b'0',
      b'a'..=b'f' => s[i * 2 + 1] - b'a' + 10,
      b'A'..=b'F' => s[i * 2 + 1] - b'A' + 10,
      _ => return None,
    };
    *slot = (hi << 4) | lo;
  }
  Some(out)
}

/// 解析 Score 索引键中的 (score, member) 切片
#[inline]
fn parse_score_item(score_prefix_len: usize, raw_key: &[u8]) -> Option<(f64, &[u8])> {
  if raw_key.len() < score_prefix_len + 17 {
    return None;
  }
  let remain = &raw_key[score_prefix_len..];
  if remain[16] != b':' {
    return None;
  }
  let hex_bytes = &remain[..16];
  let byte_arr = decode_hex_8(hex_bytes)?;
  let score = decode_sortable_f64(byte_arr);
  let member = &remain[17..];
  Some((score, member))
}

/// 聚合计算方式枚举（支持 SUM / MIN / MAX）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Aggregate {
  Sum,
  Min,
  Max,
}

impl Aggregate {
  fn parse(s: &str) -> Self {
    match s.to_ascii_uppercase().as_str() {
      "MIN" => Self::Min,
      "MAX" => Self::Max,
      _ => Self::Sum,
    }
  }

  #[inline]
  fn apply(&self, current: f64, new_val: f64) -> f64 {
    let res = match self {
      Self::Sum => current + new_val,
      Self::Min => current.min(new_val),
      Self::Max => current.max(new_val),
    };
    if res.is_nan() { 0.0 } else { res }
  }
}

/// 读取指定 member 的当前 score
pub(crate) async fn get_member_score(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  member: &[u8],
) -> Result<Option<f64>> {
  let z_k = kc.zset_key_bytes(key, member);
  let val = node
    .read(GetKVReq {
      key: String::from_utf8_lossy(&z_k).to_string(),
    })
    .await?;
  match val {
    Some(b) => {
      if b.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&b);
        Ok(Some(decode_sortable_f64(arr)))
      } else if let Ok(s) = from_utf8(&b)
        && let Ok(score) = s.parse::<f64>()
      {
        Ok(Some(score))
      } else {
        Ok(None)
      }
    }
    None => Ok(None),
  }
}

/// 获取全部 (score, member)，已按 (score ASC, member ASC) 严格保序排列
async fn get_all_member_scores(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<Vec<(f64, Vec<u8>)>> {
  let sm = node.state_machine();
  let raw_k = kc.raw_key(key);
  let now = now_millis();
  if sm.is_expired(&raw_k) {
    return Ok(Vec::new());
  }
  if let Some(meta_bytes) = node
    .read(GetKVReq {
      key: kc.zset_meta(key),
    })
    .await?
    && let Some(meta) = ZSetMeta::decode(&meta_bytes)
    && (meta.is_expired(now) || meta.is_empty())
  {
    return Ok(Vec::new());
  }

  let score_prefix = kc.zset_score_prefix(key);
  let items = node
    .scan_prefix(ScanPrefixReq {
      prefix: score_prefix.clone(),
    })
    .await?;

  if !items.is_empty() {
    let mut pairs = Vec::with_capacity(items.len());
    for (k, _) in items {
      if let Some((score, member)) = parse_score_item(score_prefix.len(), &k) {
        pairs.push((score, member.to_vec()));
      }
    }
    return Ok(pairs);
  }

  // 回退兼容：从 member 索引前缀扫描
  let prefix = kc.zset_prefix(key);
  let items = node
    .scan_prefix(ScanPrefixReq {
      prefix: prefix.clone(),
    })
    .await?;
  let mut pairs = Vec::with_capacity(items.len());
  for (k, v) in items {
    let member = k[prefix.len()..].to_vec();
    let score = if v.len() == 8 {
      let mut arr = [0u8; 8];
      arr.copy_from_slice(&v);
      decode_sortable_f64(arr)
    } else if let Ok(s) = from_utf8(&v)
      && let Ok(score) = s.parse::<f64>()
    {
      score
    } else {
      0.0
    };
    pairs.push((score, member));
  }
  pairs.sort_unstable_by(|a, b| {
    a.0
      .partial_cmp(&b.0)
      .unwrap_or(Ordering::Equal)
      .then_with(|| a.1.cmp(&b.1))
  });
  Ok(pairs)
}

/// 覆盖写入目标有序集合（对标 Kvrocks ZSet::Overwrite）
async fn overwrite_zset(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  dst: &str,
  mscores: impl IntoIterator<Item = (f64, Vec<u8>)>,
) -> Result<i64> {
  let prefix = kc.zset_prefix(dst);
  let old_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
  let score_prefix = kc.zset_score_prefix(dst);
  let old_score_items = node
    .scan_prefix(ScanPrefixReq {
      prefix: score_prefix,
    })
    .await?;

  let mut entries = Vec::with_capacity(old_items.len() + old_score_items.len() + 16);
  for (k, _) in old_items {
    entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).to_string()));
  }
  for (k, _) in old_score_items {
    entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).to_string()));
  }
  entries.push(UpsertKV::delete(kc.zset_meta(dst)));

  let mut count = 0u64;
  for (score, member) in mscores {
    let z_k = kc.zset_key_bytes(dst, &member);
    let zs_k = kc.zset_score_key_bytes(dst, score, &member);
    entries.push(UpsertKV::insert(
      String::from_utf8_lossy(&z_k).to_string(),
      encode_sortable_f64(score).to_vec(),
    ));
    entries.push(UpsertKV::insert(
      String::from_utf8_lossy(&zs_k).to_string(),
      Vec::new(),
    ));
    count += 1;
  }

  if count > 0 {
    let meta = ZSetMeta::new(0, 0, count);
    entries.push(UpsertKV::insert(kc.zset_meta(dst), meta.encode().to_vec()));
  }

  if !entries.is_empty() {
    node.batch_write(BatchWriteReq { entries }).await?;
  }
  Ok(count as i64)
}

/// 处理 ZSet 有序集合全套 Redis 命令
pub async fn handle_zset(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let ns = ctx.namespace.clone();
  let db = ctx.db;
  let kc = KeyComposer::new_with_db(&ns, db);
  let sm = node.state_machine();
  let now = now_millis();

  match cmd {
    // ================= 1. 元素添加与更新 (ZADD) =================
    RedisCommand::ZAdd {
      key,
      nx,
      xx,
      gt,
      lt,
      ch,
      incr,
      members,
    } => {
      let raw_k = kc.raw_key(&key);
      let mut metadata = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.zset_meta(&key),
        })
        .await?
        && let Some(m) = ZSetMeta::decode(&meta_bytes)
      {
        if m.is_expired(now) || sm.is_expired(&raw_k) {
          ZSetMeta::new(0, m.base.version + 1, 0)
        } else {
          m
        }
      } else {
        ZSetMeta::new(0, 0, 0)
      };

      let mut entries = Vec::with_capacity(members.len() * 2 + 1);
      let mut added_count = 0u64;
      let mut changed_count = 0u64;
      let mut incr_score = 0.0;
      let mut incr_updated = false;

      // 按输入逆序去重保留最新（对标 Kvrocks）
      let mut seen_members = HashSet::with_capacity_and_hasher(members.len(), Default::default());
      let mut deduped_members = Vec::with_capacity(members.len());
      for (score, member) in members.into_iter().rev() {
        if seen_members.insert(member.clone()) {
          deduped_members.push((score, member));
        }
      }

      for (score, member) in deduped_members {
        let old_score_opt = get_member_score(node, &kc, &key, &member).await?;

        if let Some(old_score) = old_score_opt {
          if nx {
            continue;
          }
          let new_score = if incr { old_score + score } else { score };
          if new_score.is_nan() {
            return Err(Error::invalid_data(
              "ERR resulting score is not a number (NaN)",
            ));
          }

          if !incr {
            if gt && new_score <= old_score {
              continue;
            }
            if lt && new_score >= old_score {
              continue;
            }
          }

          if incr {
            incr_score = new_score;
            incr_updated = true;
          }

          if new_score != old_score {
            changed_count += 1;
            let old_score_k = kc.zset_score_key_bytes(&key, old_score, &member);
            let m_k = kc.zset_key_bytes(&key, &member);
            let new_score_k = kc.zset_score_key_bytes(&key, new_score, &member);
            entries.push(UpsertKV::delete(
              String::from_utf8_lossy(&old_score_k).to_string(),
            ));
            entries.push(UpsertKV::insert(
              String::from_utf8_lossy(&m_k).to_string(),
              encode_sortable_f64(new_score).to_vec(),
            ));
            entries.push(UpsertKV::insert(
              String::from_utf8_lossy(&new_score_k).to_string(),
              Vec::new(),
            ));
          }
        } else {
          if xx {
            continue;
          }
          if incr {
            incr_score = score;
            incr_updated = true;
          }
          added_count += 1;
          let m_k = kc.zset_key_bytes(&key, &member);
          let s_k = kc.zset_score_key_bytes(&key, score, &member);
          entries.push(UpsertKV::insert(
            String::from_utf8_lossy(&m_k).to_string(),
            encode_sortable_f64(score).to_vec(),
          ));
          entries.push(UpsertKV::insert(
            String::from_utf8_lossy(&s_k).to_string(),
            Vec::new(),
          ));
        }
      }

      if added_count > 0 {
        metadata.base.size += added_count;
        entries.push(UpsertKV::insert(
          kc.zset_meta(&key),
          metadata.encode().to_vec(),
        ));
      }

      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      if incr {
        if incr_updated {
          Ok(score_to_blob(incr_score))
        } else {
          Ok(RespValue::Null)
        }
      } else if ch {
        Ok(RespValue::Int((added_count + changed_count) as i64))
      } else {
        Ok(RespValue::Int(added_count as i64))
      }
    }

    // ================= 2. 元素删除与基数查询 =================
    RedisCommand::ZRem(key, members) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      let mut metadata = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.zset_meta(&key),
        })
        .await?
        && let Some(m) = ZSetMeta::decode(&meta_bytes)
      {
        if m.is_expired(now) {
          return Ok(RespValue::Int(0));
        }
        m
      } else {
        ZSetMeta::new(0, 0, 0)
      };

      let mut unique_members = HashSet::with_capacity_and_hasher(members.len(), Default::default());
      for m in members {
        unique_members.insert(m);
      }

      let mut entries = Vec::with_capacity(unique_members.len() * 2 + 1);
      let mut removed_count = 0u64;

      for m in unique_members {
        if let Some(old_score) = get_member_score(node, &kc, &key, &m).await? {
          removed_count += 1;
          let m_k = kc.zset_key_bytes(&key, &m);
          let s_k = kc.zset_score_key_bytes(&key, old_score, &m);
          entries.push(UpsertKV::delete(String::from_utf8_lossy(&m_k).to_string()));
          entries.push(UpsertKV::delete(String::from_utf8_lossy(&s_k).to_string()));
        }
      }

      if removed_count > 0 {
        if metadata.base.size > removed_count {
          metadata.base.size -= removed_count;
          entries.push(UpsertKV::insert(
            kc.zset_meta(&key),
            metadata.encode().to_vec(),
          ));
        } else {
          entries.push(UpsertKV::delete(kc.zset_meta(&key)));
        }
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(removed_count as i64))
    }

    RedisCommand::ZCard(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.zset_meta(&key),
        })
        .await?
        && let Some(meta) = ZSetMeta::decode(&meta_bytes)
      {
        if meta.is_expired(now) {
          return Ok(RespValue::Int(0));
        }
        return Ok(RespValue::Int(meta.size() as i64));
      }

      let score_prefix = kc.zset_score_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: score_prefix,
        })
        .await?;
      Ok(RespValue::Int(items.len() as i64))
    }

    // ================= 3. 分值与增量操作 =================
    RedisCommand::ZScore(key, member) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Null);
      }
      if let Some(score) = get_member_score(node, &kc, &key, &member).await? {
        Ok(score_to_blob(score))
      } else {
        Ok(RespValue::Null)
      }
    }

    RedisCommand::ZMScore(key, members) => {
      let raw_k = kc.raw_key(&key);
      let expired = sm.is_expired(&raw_k);
      let mut results = Vec::with_capacity(members.len());
      for m in members {
        if expired {
          results.push(RespValue::Null);
        } else if let Some(score) = get_member_score(node, &kc, &key, &m).await? {
          results.push(score_to_blob(score));
        } else {
          results.push(RespValue::Null);
        }
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::ZIncrBy(key, delta, member) => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZAdd {
          key,
          nx: false,
          xx: false,
          gt: false,
          lt: false,
          ch: false,
          incr: true,
          members: vec![(delta, member)],
        },
      ))
      .await
    }

    // ================= 4. 区间统计与排名 (ZCOUNT / ZLEXCOUNT / ZRANK / ZREVRANK) =================
    RedisCommand::ZCount(key, min_str, max_str) => {
      let score_spec = RangeScoreSpec::from_bounds(&min_str, &max_str, 0, None)
        .map_err(|e| Error::invalid_data(e.to_string()))?;

      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }
      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.zset_meta(&key),
        })
        .await?
        && let Some(meta) = ZSetMeta::decode(&meta_bytes)
        && (meta.is_expired(now) || meta.is_empty())
      {
        return Ok(RespValue::Int(0));
      }

      let score_prefix = kc.zset_score_prefix(&key);
      let score_items = node
        .scan_prefix(ScanPrefixReq {
          prefix: score_prefix.clone(),
        })
        .await?;

      let mut count = 0i64;
      for (k, _) in score_items {
        if let Some((score, _)) = parse_score_item(score_prefix.len(), &k)
          && score_spec.check(score)
        {
          count += 1;
        }
      }
      Ok(RespValue::Int(count))
    }

    RedisCommand::ZLexCount(key, min_str, max_str) => {
      let lex_spec = RangeLexSpec::from_bounds(min_str.as_bytes(), max_str.as_bytes(), 0, None)
        .map_err(|e| Error::invalid_data(e.to_string()))?;

      let items = get_all_member_scores(node, &kc, &key).await?;
      let count = items.iter().filter(|(_, m)| lex_spec.check(m)).count() as i64;
      Ok(RespValue::Int(count))
    }

    RedisCommand::ZRank {
      key,
      member,
      with_score,
    } => {
      let items = get_all_member_scores(node, &kc, &key).await?;
      for (idx, (score, m)) in items.into_iter().enumerate() {
        if m == member {
          if with_score {
            return Ok(RespValue::Arr(vec![
              RespValue::Int(idx as i64),
              score_to_blob(score),
            ]));
          } else {
            return Ok(RespValue::Int(idx as i64));
          }
        }
      }
      Ok(RespValue::Null)
    }

    RedisCommand::ZRevRank {
      key,
      member,
      with_score,
    } => {
      let items = get_all_member_scores(node, &kc, &key).await?;
      let total = items.len();
      for (idx, (score, m)) in items.into_iter().enumerate() {
        if m == member {
          let rev_rank = (total - 1 - idx) as i64;
          if with_score {
            return Ok(RespValue::Arr(vec![
              RespValue::Int(rev_rank),
              score_to_blob(score),
            ]));
          } else {
            return Ok(RespValue::Int(rev_rank));
          }
        }
      }
      Ok(RespValue::Null)
    }

    // ================= 5. 范围检索 (ZRANGE 全参数统一架构) =================
    RedisCommand::ZRange {
      key,
      min,
      max,
      by_score,
      by_lex,
      rev,
      offset,
      count,
      with_scores,
    } => {
      let mut all_pairs = get_all_member_scores(node, &kc, &key).await?;

      let filtered_pairs: Vec<(f64, Vec<u8>)> = if by_score {
        let score_spec = RangeScoreSpec::from_bounds(&min, &max, 0, None)
          .map_err(|e| Error::invalid_data(e.to_string()))?;
        let mut pairs: Vec<(f64, Vec<u8>)> = all_pairs
          .into_iter()
          .filter(|(s, _)| score_spec.check(*s))
          .collect();
        if rev {
          pairs.reverse();
        }
        pairs
      } else if by_lex {
        let lex_spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), 0, None)
          .map_err(|e| Error::invalid_data(e.to_string()))?;
        let mut pairs: Vec<(f64, Vec<u8>)> = all_pairs
          .into_iter()
          .filter(|(_, m)| lex_spec.check(m))
          .collect();
        pairs.sort_unstable_by(|a, b| a.1.cmp(&b.1));
        if rev {
          pairs.reverse();
        }
        pairs
      } else {
        if rev {
          all_pairs.reverse();
        }
        let start = min.parse::<i64>().unwrap_or(0);
        let stop = max.parse::<i64>().unwrap_or(-1);
        let len = all_pairs.len() as i64;
        let (s, e) = normalize_range(start, stop, len);
        if s > e || s >= len {
          Vec::new()
        } else {
          all_pairs[s as usize..=e as usize].to_vec()
        }
      };

      let selected: Vec<(f64, Vec<u8>)> = if by_score || by_lex || count.is_some() {
        let limit = count.unwrap_or(filtered_pairs.len());
        filtered_pairs
          .into_iter()
          .skip(offset)
          .take(limit)
          .collect()
      } else {
        filtered_pairs
      };

      let mut elements = Vec::with_capacity(selected.len() * (if with_scores { 2 } else { 1 }));
      for (s, m) in selected {
        elements.push(RespValue::Blob(m));
        if with_scores {
          elements.push(score_to_blob(s));
        }
      }
      Ok(RespValue::Arr(elements))
    }

    RedisCommand::ZRevRange(key, start, stop, with_scores) => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key,
          min: format!("{start}"),
          max: format!("{stop}"),
          by_score: false,
          by_lex: false,
          rev: true,
          offset: 0,
          count: None,
          with_scores,
        },
      ))
      .await
    }

    RedisCommand::ZRangeByScore {
      key,
      min,
      max,
      with_scores,
      offset,
      count,
    } => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key,
          min,
          max,
          by_score: true,
          by_lex: false,
          rev: false,
          offset,
          count,
          with_scores,
        },
      ))
      .await
    }

    RedisCommand::ZRevRangeByScore {
      key,
      max,
      min,
      with_scores,
      offset,
      count,
    } => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key,
          min,
          max,
          by_score: true,
          by_lex: false,
          rev: true,
          offset,
          count,
          with_scores,
        },
      ))
      .await
    }

    RedisCommand::ZRangeByLex {
      key,
      min,
      max,
      offset,
      count,
    } => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key,
          min,
          max,
          by_score: false,
          by_lex: true,
          rev: false,
          offset,
          count,
          with_scores: false,
        },
      ))
      .await
    }

    RedisCommand::ZRevRangeByLex {
      key,
      max,
      min,
      offset,
      count,
    } => {
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key,
          min,
          max,
          by_score: false,
          by_lex: true,
          rev: true,
          offset,
          count,
          with_scores: false,
        },
      ))
      .await
    }

    // ================= 6. 弹出操作 (ZPOPMIN / ZPOPMAX / BZPOPMIN / BZPOPMAX / ZMPOP / BZMPOP) =================
    RedisCommand::ZPopMin(key, count_opt) => {
      let c = count_opt.unwrap_or(1);
      if c == 0 {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let pairs = get_all_member_scores(node, &kc, &key).await?;
      if pairs.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let to_pop: Vec<(f64, Vec<u8>)> = pairs.into_iter().take(c).collect();
      let members: Vec<Vec<u8>> = to_pop.iter().map(|(_, m)| m.clone()).collect();
      Box::pin(handle_zset(node, ctx, RedisCommand::ZRem(key, members))).await?;

      let mut results = Vec::with_capacity(to_pop.len() * 2);
      for (score, member) in to_pop {
        results.push(RespValue::Blob(member));
        results.push(score_to_blob(score));
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::ZPopMax(key, count_opt) => {
      let c = count_opt.unwrap_or(1);
      if c == 0 {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let pairs = get_all_member_scores(node, &kc, &key).await?;
      if pairs.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let to_pop: Vec<(f64, Vec<u8>)> = pairs.into_iter().rev().take(c).collect();
      let members: Vec<Vec<u8>> = to_pop.iter().map(|(_, m)| m.clone()).collect();
      Box::pin(handle_zset(node, ctx, RedisCommand::ZRem(key, members))).await?;

      let mut results = Vec::with_capacity(to_pop.len() * 2);
      for (score, member) in to_pop {
        results.push(RespValue::Blob(member));
        results.push(score_to_blob(score));
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::BZPopMin(keys, _) => {
      for k in keys {
        let popped = Box::pin(handle_zset(
          node,
          ctx,
          RedisCommand::ZPopMin(k.clone(), Some(1)),
        ))
        .await?;
        if let RespValue::Arr(arr) = popped
          && arr.len() >= 2
        {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            arr[0].clone(),
            arr[1].clone(),
          ]));
        }
      }
      Ok(RespValue::Null)
    }

    RedisCommand::BZPopMax(keys, _) => {
      for k in keys {
        let popped = Box::pin(handle_zset(
          node,
          ctx,
          RedisCommand::ZPopMax(k.clone(), Some(1)),
        ))
        .await?;
        if let RespValue::Arr(arr) = popped
          && arr.len() >= 2
        {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            arr[0].clone(),
            arr[1].clone(),
          ]));
        }
      }
      Ok(RespValue::Null)
    }

    RedisCommand::ZMPop { keys, min, count }
    | RedisCommand::BZMPop {
      keys,
      min,
      count,
      timeout: _,
    } => {
      for k in keys {
        let popped = if min {
          Box::pin(handle_zset(
            node,
            ctx,
            RedisCommand::ZPopMin(k.clone(), Some(count)),
          ))
          .await?
        } else {
          Box::pin(handle_zset(
            node,
            ctx,
            RedisCommand::ZPopMax(k.clone(), Some(count)),
          ))
          .await?
        };
        if let RespValue::Arr(arr) = popped
          && !arr.is_empty()
        {
          let mut nested_pairs = Vec::with_capacity(arr.len() / 2);
          for chunk in arr.chunks(2) {
            nested_pairs.push(RespValue::Arr(vec![chunk[0].clone(), chunk[1].clone()]));
          }
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Arr(nested_pairs),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    // ================= 7. 范围删除 (ZREMRANGEBYRANK / SCORE / LEX) =================
    RedisCommand::ZRemRangeByRank(key, start, stop) => {
      let pairs = get_all_member_scores(node, &kc, &key).await?;
      let len = pairs.len() as i64;
      let (s, e) = normalize_range(start, stop, len);
      if s > e || s >= len {
        return Ok(RespValue::Int(0));
      }
      let to_del: Vec<Vec<u8>> = pairs[s as usize..=e as usize]
        .iter()
        .map(|(_, m)| m.clone())
        .collect();
      let count = to_del.len() as i64;
      Box::pin(handle_zset(node, ctx, RedisCommand::ZRem(key, to_del))).await?;
      Ok(RespValue::Int(count))
    }

    RedisCommand::ZRemRangeByScore(key, min_str, max_str) => {
      let score_spec = RangeScoreSpec::from_bounds(&min_str, &max_str, 0, None)
        .map_err(|e| Error::invalid_data(e.to_string()))?;

      let pairs = get_all_member_scores(node, &kc, &key).await?;
      let to_del: Vec<Vec<u8>> = pairs
        .into_iter()
        .filter(|(s, _)| score_spec.check(*s))
        .map(|(_, m)| m)
        .collect();
      let count = to_del.len() as i64;
      if count > 0 {
        Box::pin(handle_zset(node, ctx, RedisCommand::ZRem(key, to_del))).await?;
      }
      Ok(RespValue::Int(count))
    }

    RedisCommand::ZRemRangeByLex(key, min_str, max_str) => {
      let lex_spec = RangeLexSpec::from_bounds(min_str.as_bytes(), max_str.as_bytes(), 0, None)
        .map_err(|e| Error::invalid_data(e.to_string()))?;

      let pairs = get_all_member_scores(node, &kc, &key).await?;
      let to_del: Vec<Vec<u8>> = pairs
        .into_iter()
        .filter(|(_, m)| lex_spec.check(m))
        .map(|(_, m)| m)
        .collect();
      let count = to_del.len() as i64;
      if count > 0 {
        Box::pin(handle_zset(node, ctx, RedisCommand::ZRem(key, to_del))).await?;
      }
      Ok(RespValue::Int(count))
    }

    // ================= 8. 随机采样 (ZRANDMEMBER) =================
    RedisCommand::ZRandMember {
      key,
      count: count_opt,
      with_scores,
    } => {
      let items = get_all_member_scores(node, &kc, &key).await?;
      if items.is_empty() {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      match count_opt {
        None => {
          let idx = fastrand::usize(..items.len());
          let (score, member) = &items[idx];
          if with_scores {
            Ok(RespValue::Arr(vec![
              RespValue::Blob(member.clone()),
              score_to_blob(*score),
            ]))
          } else {
            Ok(RespValue::Blob(member.clone()))
          }
        }
        Some(0) => Ok(RespValue::Arr(Vec::new())),
        Some(count) if count > 0 => {
          let c = (count as usize).min(items.len());
          let indices: Vec<usize> = if c == 1 {
            vec![fastrand::usize(..items.len())]
          } else if c < 64 && c * 4 < items.len() {
            let mut picked = HashSet::with_capacity(c);
            while picked.len() < c {
              picked.insert(fastrand::usize(..items.len()));
            }
            picked.into_iter().collect()
          } else {
            let mut idxs: Vec<usize> = (0..items.len()).collect();
            for i in 0..c {
              let j = fastrand::usize(i..items.len());
              idxs.swap(i, j);
            }
            idxs.truncate(c);
            idxs
          };
          let mut results = Vec::with_capacity(c * (if with_scores { 2 } else { 1 }));
          for idx in indices {
            let (score, member) = &items[idx];
            results.push(RespValue::Blob(member.clone()));
            if with_scores {
              results.push(score_to_blob(*score));
            }
          }
          Ok(RespValue::Arr(results))
        }
        Some(count) => {
          let c = count.unsigned_abs() as usize;
          let mut results = Vec::with_capacity(c * (if with_scores { 2 } else { 1 }));
          for _ in 0..c {
            let idx = fastrand::usize(..items.len());
            let (score, member) = &items[idx];
            results.push(RespValue::Blob(member.clone()));
            if with_scores {
              results.push(score_to_blob(*score));
            }
          }
          Ok(RespValue::Arr(results))
        }
      }
    }

    // ================= 9. 范围结果转存 (ZRANGESTORE) =================
    RedisCommand::ZRangeStore {
      dst,
      src,
      min,
      max,
      by_score,
      by_lex,
      rev,
      offset,
      count,
    } => {
      let range_res = Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZRange {
          key: src,
          min,
          max,
          by_score,
          by_lex,
          rev,
          offset,
          count,
          with_scores: true,
        },
      ))
      .await?;

      if let RespValue::Arr(items) = range_res {
        let mut mscores = Vec::with_capacity(items.len() / 2);
        for chunk in items.chunks(2) {
          if let (RespValue::Blob(m), RespValue::Blob(s_bytes)) = (&chunk[0], &chunk[1]) {
            let score = from_utf8(s_bytes)
              .ok()
              .and_then(|s| s.parse::<f64>().ok())
              .unwrap_or(0.0);
            mscores.push((score, m.clone()));
          }
        }
        let stored = overwrite_zset(node, &kc, &dst, mscores).await?;
        Ok(RespValue::Int(stored))
      } else {
        let stored = overwrite_zset(node, &kc, &dst, Vec::new()).await?;
        Ok(RespValue::Int(stored))
      }
    }

    // ================= 10. 集合运算 (ZINTER / ZUNION / ZDIFF 全套) =================
    RedisCommand::ZInter {
      keys,
      weights,
      aggregate,
      with_scores,
    } => {
      if keys.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let agg = Aggregate::parse(&aggregate);
      let mut member_scores: HashMap<Vec<u8>, (f64, usize)> = HashMap::default();

      for (idx, k) in keys.iter().enumerate() {
        let items = get_all_member_scores(node, &kc, k).await?;
        if items.is_empty() {
          return Ok(RespValue::Arr(Vec::new()));
        }
        let w = weights.get(idx).copied().unwrap_or(1.0);

        let mut cur_keys = HashMap::with_capacity_and_hasher(items.len(), Default::default());
        for (s, m) in items {
          let weighted_score = s * w;
          let score = if weighted_score.is_nan() {
            0.0
          } else {
            weighted_score
          };
          cur_keys.insert(m, score);
        }

        if idx == 0 {
          for (m, s) in cur_keys {
            member_scores.insert(m, (s, 1));
          }
        } else {
          for (m, (cur_s, count)) in member_scores.iter_mut() {
            if let Some(&s) = cur_keys.get(m) {
              *count += 1;
              *cur_s = agg.apply(*cur_s, s);
            }
          }
        }
      }

      let num_keys = keys.len();
      let mut final_pairs: Vec<(f64, Vec<u8>)> = member_scores
        .into_iter()
        .filter_map(|(m, (s, c))| if c == num_keys { Some((s, m)) } else { None })
        .collect();

      final_pairs.sort_unstable_by(|a, b| {
        a.0
          .partial_cmp(&b.0)
          .unwrap_or(Ordering::Equal)
          .then_with(|| a.1.cmp(&b.1))
      });
      let mut elements = Vec::with_capacity(final_pairs.len() * (if with_scores { 2 } else { 1 }));
      for (s, m) in final_pairs {
        elements.push(RespValue::Blob(m));
        if with_scores {
          elements.push(score_to_blob(s));
        }
      }
      Ok(RespValue::Arr(elements))
    }

    RedisCommand::ZInterStore {
      dst,
      keys,
      weights,
      aggregate,
    } => {
      let inter = match Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZInter {
          keys,
          weights,
          aggregate,
          with_scores: true,
        },
      ))
      .await?
      {
        RespValue::Arr(arr) => arr,
        _ => Vec::new(),
      };
      let mut mscores = Vec::with_capacity(inter.len() / 2);
      for chunk in inter.chunks(2) {
        if let (RespValue::Blob(m), RespValue::Blob(s_bytes)) = (&chunk[0], &chunk[1]) {
          let score = from_utf8(s_bytes)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
          mscores.push((score, m.clone()));
        }
      }
      let stored = overwrite_zset(node, &kc, &dst, mscores).await?;
      Ok(RespValue::Int(stored))
    }

    RedisCommand::ZInterCard { keys, limit } => {
      if keys.is_empty() {
        return Ok(RespValue::Int(0));
      }
      let mut all_sets = Vec::with_capacity(keys.len());
      for k in &keys {
        let items = get_all_member_scores(node, &kc, k).await?;
        if items.is_empty() {
          return Ok(RespValue::Int(0));
        }
        let member_set: HashSet<Vec<u8>> = items.into_iter().map(|(_, m)| m).collect();
        all_sets.push(member_set);
      }

      all_sets.sort_unstable_by_key(|s| s.len());
      let base_set = &all_sets[0];
      let other_sets = &all_sets[1..];

      let mut count = 0usize;
      for item in base_set {
        let mut in_all = true;
        for other in other_sets {
          if !other.contains(item) {
            in_all = false;
            break;
          }
        }
        if in_all {
          count += 1;
          if limit > 0 && count >= limit {
            break;
          }
        }
      }
      Ok(RespValue::Int(count as i64))
    }

    RedisCommand::ZUnion {
      keys,
      weights,
      aggregate,
      with_scores,
    } => {
      let agg = Aggregate::parse(&aggregate);
      let mut member_scores: HashMap<Vec<u8>, f64> = HashMap::default();

      for (idx, k) in keys.iter().enumerate() {
        let items = get_all_member_scores(node, &kc, k).await?;
        let w = weights.get(idx).copied().unwrap_or(1.0);

        for (s, m) in items {
          let weighted_score = s * w;
          let score = if weighted_score.is_nan() {
            0.0
          } else {
            weighted_score
          };
          member_scores
            .entry(m)
            .and_modify(|cur_s| *cur_s = agg.apply(*cur_s, score))
            .or_insert(score);
        }
      }

      let mut final_pairs: Vec<(f64, Vec<u8>)> =
        member_scores.into_iter().map(|(m, s)| (s, m)).collect();
      final_pairs.sort_unstable_by(|a, b| {
        a.0
          .partial_cmp(&b.0)
          .unwrap_or(Ordering::Equal)
          .then_with(|| a.1.cmp(&b.1))
      });
      let mut elements = Vec::with_capacity(final_pairs.len() * (if with_scores { 2 } else { 1 }));
      for (s, m) in final_pairs {
        elements.push(RespValue::Blob(m));
        if with_scores {
          elements.push(score_to_blob(s));
        }
      }
      Ok(RespValue::Arr(elements))
    }

    RedisCommand::ZUnionStore {
      dst,
      keys,
      weights,
      aggregate,
    } => {
      let union = match Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZUnion {
          keys,
          weights,
          aggregate,
          with_scores: true,
        },
      ))
      .await?
      {
        RespValue::Arr(arr) => arr,
        _ => Vec::new(),
      };
      let mut mscores = Vec::with_capacity(union.len() / 2);
      for chunk in union.chunks(2) {
        if let (RespValue::Blob(m), RespValue::Blob(s_bytes)) = (&chunk[0], &chunk[1]) {
          let score = from_utf8(s_bytes)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
          mscores.push((score, m.clone()));
        }
      }
      let stored = overwrite_zset(node, &kc, &dst, mscores).await?;
      Ok(RespValue::Int(stored))
    }

    RedisCommand::ZDiff { keys, with_scores } => {
      if keys.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }
      let first_items = get_all_member_scores(node, &kc, &keys[0]).await?;
      if first_items.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let mut member_scores: HashMap<Vec<u8>, f64> =
        HashMap::with_capacity_and_hasher(first_items.len(), Default::default());
      for (s, m) in first_items {
        member_scores.insert(m, s);
      }

      for k in keys.iter().skip(1) {
        let items = get_all_member_scores(node, &kc, k).await?;
        for (_, m) in items {
          member_scores.remove(&m);
        }
        if member_scores.is_empty() {
          break;
        }
      }

      let mut final_pairs: Vec<(f64, Vec<u8>)> =
        member_scores.into_iter().map(|(m, s)| (s, m)).collect();
      final_pairs.sort_unstable_by(|a, b| {
        a.0
          .partial_cmp(&b.0)
          .unwrap_or(Ordering::Equal)
          .then_with(|| a.1.cmp(&b.1))
      });
      let mut elements = Vec::with_capacity(final_pairs.len() * (if with_scores { 2 } else { 1 }));
      for (s, m) in final_pairs {
        elements.push(RespValue::Blob(m));
        if with_scores {
          elements.push(score_to_blob(s));
        }
      }
      Ok(RespValue::Arr(elements))
    }

    RedisCommand::ZDiffStore { dst, keys } => {
      let diff = match Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZDiff {
          keys,
          with_scores: true,
        },
      ))
      .await?
      {
        RespValue::Arr(arr) => arr,
        _ => Vec::new(),
      };
      let mut mscores = Vec::with_capacity(diff.len() / 2);
      for chunk in diff.chunks(2) {
        if let (RespValue::Blob(m), RespValue::Blob(s_bytes)) = (&chunk[0], &chunk[1]) {
          let score = from_utf8(s_bytes)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
          mscores.push((score, m.clone()));
        }
      }
      let stored = overwrite_zset(node, &kc, &dst, mscores).await?;
      Ok(RespValue::Int(stored))
    }

    // ================= 11. 游标迭代扫描 (ZSCAN) =================
    RedisCommand::ZScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let items = get_all_member_scores(node, &kc, &key).await?;

      let pat_str = pattern.unwrap_or_else(|| "*".to_string());
      let pat_bytes = pat_str.as_bytes();
      let limit = count.unwrap_or(10);

      let mut matched = Vec::new();
      for (score, member) in items {
        if matches_glob_bytes(pat_bytes, &member) {
          matched.push(RespValue::Blob(member));
          matched.push(score_to_blob(score));
        }
      }

      let start = (cursor as usize) * 2;
      let end = (start + limit * 2).min(matched.len());
      let next_cursor = if end >= matched.len() {
        0
      } else {
        (end / 2) as u64
      };
      let slice = if start < matched.len() {
        matched[start..end].to_vec()
      } else {
        Vec::new()
      };

      Ok(RespValue::Arr(vec![
        int_to_blob(next_cursor),
        RespValue::Arr(slice),
      ]))
    }

    _ => Err(Error::redis("Command not matched in handle_zset")),
  }
}
