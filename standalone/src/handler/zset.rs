use crate::handler::resp_util::{member_scores_to_arr, score_to_blob};
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Aggregate, Error, RangeLexSpec, RangeScoreSpec, Result, WeDb, ZAdd};
use wedb_resp::RespValue;

/// 处理所有有序集合 (ZSet) 命令
pub async fn handle_zset(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::ZAdd {
      key,
      members,
      nx,
      xx,
      gt,
      lt,
      ch,
      incr,
    } => {
      let m: Vec<(f64, &[u8])> = members.iter().map(|(s, m)| (*s, m.as_slice())).collect();
      let mut flags = Vec::new();
      if nx {
        flags.push(ZAdd::Nx);
      }
      if xx {
        flags.push(ZAdd::Xx);
      }
      if gt {
        flags.push(ZAdd::Gt);
      }
      if lt {
        flags.push(ZAdd::Lt);
      }
      if ch {
        flags.push(ZAdd::Ch);
      }
      if incr {
        flags.push(ZAdd::Incr);
      }
      let count = db.zadd(key.as_bytes(), &m, &flags)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZScore(key, member) => match db.zscore(key.as_bytes(), &member)? {
      Some(s) => Ok(score_to_blob(s)),
      None => Ok(RespValue::Null),
    },
    Cmd::ZMScore(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
      let scores = db.zmscore(key.as_bytes(), &m)?;
      let arr = scores
        .into_iter()
        .map(|opt| match opt {
          Some(s) => score_to_blob(s),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::ZCard(key) => {
      let count = db.zcard(key.as_bytes())?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZRem(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
      let count = db.zrem(key.as_bytes(), &m)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZIncrBy(key, delta, member) => {
      let new_score = db.zincrby(key.as_bytes(), delta, &member)?;
      Ok(score_to_blob(new_score))
    }
    Cmd::ZRank {
      key,
      member,
      with_score,
    } => {
      if with_score {
        match db.zrank_with_score(key.as_bytes(), &member)? {
          Some((rank, score)) => Ok(RespValue::Arr(vec![
            RespValue::Int(rank as i64),
            score_to_blob(score),
          ])),
          None => Ok(RespValue::Null),
        }
      } else {
        match db.zrank(key.as_bytes(), &member)? {
          Some(rank) => Ok(RespValue::Int(rank as i64)),
          None => Ok(RespValue::Null),
        }
      }
    }
    Cmd::ZRevRank {
      key,
      member,
      with_score,
    } => {
      if with_score {
        match db.zrevrank_with_score(key.as_bytes(), &member)? {
          Some((rank, score)) => Ok(RespValue::Arr(vec![
            RespValue::Int(rank as i64),
            score_to_blob(score),
          ])),
          None => Ok(RespValue::Null),
        }
      } else {
        match db.zrevrank(key.as_bytes(), &member)? {
          Some(rank) => Ok(RespValue::Int(rank as i64)),
          None => Ok(RespValue::Null),
        }
      }
    }
    Cmd::ZRange {
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
      if by_score {
        let spec = RangeScoreSpec::from_bounds(&min, &max, offset, count)?;
        let items = if rev {
          db.zrevrangebyscore(key.as_bytes(), &spec)?
        } else {
          db.zrangebyscore(key.as_bytes(), &spec)?
        };
        Ok(member_scores_to_arr(items, with_scores))
      } else if by_lex {
        let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)?;
        let members = if rev {
          db.zrevrangebylex(key.as_bytes(), &spec)?
        } else {
          db.zrangebylex(key.as_bytes(), &spec)?
        };
        Ok(RespValue::Arr(
          members.into_iter().map(RespValue::Blob).collect(),
        ))
      } else {
        let start = min.parse::<i64>().unwrap_or(0);
        let stop = max.parse::<i64>().unwrap_or(-1);
        let items = if rev {
          db.zrevrange(key.as_bytes(), start, stop)?
        } else {
          db.zrange(key.as_bytes(), start, stop)?
        };
        Ok(member_scores_to_arr(items, with_scores))
      }
    }
    Cmd::ZRevRange(key, start, stop, with_scores) => {
      let items = db.zrevrange(key.as_bytes(), start, stop)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZRangeByScore {
      key,
      min,
      max,
      with_scores,
      offset,
      count,
    } => {
      let spec = RangeScoreSpec::from_bounds(&min, &max, offset, count)?;
      let items = db.zrangebyscore(key.as_bytes(), &spec)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZRevRangeByScore {
      key,
      max,
      min,
      with_scores,
      offset,
      count,
    } => {
      let spec = RangeScoreSpec::from_bounds(&min, &max, offset, count)?;
      let items = db.zrevrangebyscore(key.as_bytes(), &spec)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZRangeByLex {
      key,
      min,
      max,
      offset,
      count,
    } => {
      let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)?;
      let items = db.zrangebylex(key.as_bytes(), &spec)?;
      Ok(RespValue::Arr(
        items.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::ZRevRangeByLex {
      key,
      max,
      min,
      offset,
      count,
    } => {
      let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)?;
      let items = db.zrevrangebylex(key.as_bytes(), &spec)?;
      Ok(RespValue::Arr(
        items.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::ZCount(key, min, max) => {
      let spec = RangeScoreSpec::from_bounds(&min, &max, 0, None)?;
      let count = db.zcount(key.as_bytes(), &spec)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZLexCount(key, min, max) => {
      let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), 0, None)?;
      let count = db.zlexcount(key.as_bytes(), &spec)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZPopMin(key, count) => {
      let c = count.unwrap_or(1);
      let items = db.zpopmin(key.as_bytes(), c)?;
      Ok(member_scores_to_arr(items, true))
    }
    Cmd::ZPopMax(key, count) => {
      let c = count.unwrap_or(1);
      let items = db.zpopmax(key.as_bytes(), c)?;
      Ok(member_scores_to_arr(items, true))
    }
    Cmd::BZPopMin(keys, _) => {
      for k in &keys {
        let items = db.zpopmin(k.as_bytes(), 1)?;
        if let Some((m, s)) = items.into_iter().next() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Blob(m),
            score_to_blob(s),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::BZPopMax(keys, _) => {
      for k in &keys {
        let items = db.zpopmax(k.as_bytes(), 1)?;
        if let Some((m, s)) = items.into_iter().next() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Blob(m),
            score_to_blob(s),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::ZMPop { keys, min, count }
    | Cmd::BZMPop {
      keys, min, count, ..
    } => {
      for k in &keys {
        let items = if min {
          db.zpopmin(k.as_bytes(), count)?
        } else {
          db.zpopmax(k.as_bytes(), count)?
        };
        if !items.is_empty() {
          let mut elements = Vec::with_capacity(items.len());
          for (m, s) in items {
            elements.push(RespValue::Arr(vec![RespValue::Blob(m), score_to_blob(s)]));
          }
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Arr(elements),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::ZInter {
      keys,
      weights,
      aggregate,
      with_scores,
    } => {
      let keys_weights: Vec<(&[u8], f64)> = if weights.is_empty() {
        keys.iter().map(|k| (k.as_bytes(), 1.0)).collect()
      } else {
        keys
          .iter()
          .zip(weights.iter())
          .map(|(k, w)| (k.as_bytes(), *w))
          .collect()
      };
      let agg = Aggregate::parse(&aggregate);
      let items = db.zinter(&keys_weights, agg)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZInterStore {
      dst,
      keys,
      weights,
      aggregate,
    } => {
      let keys_weights: Vec<(&[u8], f64)> = if weights.is_empty() {
        keys.iter().map(|k| (k.as_bytes(), 1.0)).collect()
      } else {
        keys
          .iter()
          .zip(weights.iter())
          .map(|(k, w)| (k.as_bytes(), *w))
          .collect()
      };
      let agg = Aggregate::parse(&aggregate);
      let count = db.zinterstore(dst.as_bytes(), &keys_weights, agg)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZInterCard { keys, limit } => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let count = db.zintercard(&k_refs, limit)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZUnion {
      keys,
      weights,
      aggregate,
      with_scores,
    } => {
      let keys_weights: Vec<(&[u8], f64)> = if weights.is_empty() {
        keys.iter().map(|k| (k.as_bytes(), 1.0)).collect()
      } else {
        keys
          .iter()
          .zip(weights.iter())
          .map(|(k, w)| (k.as_bytes(), *w))
          .collect()
      };
      let agg = Aggregate::parse(&aggregate);
      let items = db.zunion(&keys_weights, agg)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZUnionStore {
      dst,
      keys,
      weights,
      aggregate,
    } => {
      let keys_weights: Vec<(&[u8], f64)> = if weights.is_empty() {
        keys.iter().map(|k| (k.as_bytes(), 1.0)).collect()
      } else {
        keys
          .iter()
          .zip(weights.iter())
          .map(|(k, w)| (k.as_bytes(), *w))
          .collect()
      };
      let agg = Aggregate::parse(&aggregate);
      let count = db.zunionstore(dst.as_bytes(), &keys_weights, agg)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZDiff { keys, with_scores } => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let items = db.zdiff(&k_refs)?;
      Ok(member_scores_to_arr(items, with_scores))
    }
    Cmd::ZDiffStore { dst, keys } => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let count = db.zdiffstore(dst.as_bytes(), &k_refs)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZRemRangeByRank(key, start, stop) => {
      let count = db.zremrangebyrank(key.as_bytes(), start, stop)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZRemRangeByScore(key, min, max) => {
      let spec = RangeScoreSpec::from_bounds(&min, &max, 0, None)?;
      let count = db.zremrangebyscore(key.as_bytes(), &spec)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZRemRangeByLex(key, min, max) => {
      let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), 0, None)?;
      let count = db.zremrangebylex(key.as_bytes(), &spec)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::ZRandMember {
      key,
      count,
      with_scores,
    } => {
      let c = count.unwrap_or(1);
      let items = db.zrandmember(key.as_bytes(), c)?;
      if count.is_none() {
        if let Some((m, _)) = items.into_iter().next() {
          Ok(RespValue::Blob(m))
        } else {
          Ok(RespValue::Null)
        }
      } else {
        Ok(member_scores_to_arr(items, with_scores))
      }
    }
    Cmd::ZRangeStore {
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
      let items = if by_score {
        let spec = RangeScoreSpec::from_bounds(&min, &max, offset, count)?;
        if rev {
          db.zrevrangebyscore(src.as_bytes(), &spec)?
        } else {
          db.zrangebyscore(src.as_bytes(), &spec)?
        }
      } else if by_lex {
        let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)?;
        let members = if rev {
          db.zrevrangebylex(src.as_bytes(), &spec)?
        } else {
          db.zrangebylex(src.as_bytes(), &spec)?
        };
        let mut list = Vec::with_capacity(members.len());
        for m in members {
          list.push((m, 0.0));
        }
        list
      } else {
        let start = min.parse::<i64>().unwrap_or(0);
        let stop = max.parse::<i64>().unwrap_or(-1);
        if rev {
          db.zrevrange(src.as_bytes(), start, stop)?
        } else {
          db.zrange(src.as_bytes(), start, stop)?
        }
      };
      db.del(&[dst.as_bytes()])?;
      if !items.is_empty() {
        let pairs: Vec<(f64, &[u8])> = items.iter().map(|(m, s)| (*s, m.as_slice())).collect();
        db.zadd(dst.as_bytes(), &pairs, [])?;
      }
      Ok(RespValue::Int(items.len() as i64))
    }
    Cmd::ZScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let (next_cursor, items) = db.zscan(
        key.as_bytes(),
        cursor,
        pattern.as_deref().map(str::as_bytes),
        count,
      )?;
      let members_arr = member_scores_to_arr(items, true);
      let mut itoa_buf = itoa::Buffer::new();
      Ok(RespValue::Arr(vec![
        RespValue::Blob(itoa_buf.format(next_cursor).as_bytes().to_vec()),
        members_arr,
      ]))
    }
    _ => Err(Error::internal("unsupported zset command")),
  }
}
