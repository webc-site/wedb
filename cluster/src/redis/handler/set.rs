use rapidhash::{HashSetExt, RapidHashSet as HashSet};
use std::sync::Arc;

use super::context::{ConnectionContext, KeyComposer, SetMeta, matches_glob_bytes};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::int_to_blob;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

/// 获取集合全部成员并转化为 Hash 集合（带过期校验）
async fn get_set_members(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<HashSet<Vec<u8>>> {
  let sm = node.state_machine();
  let raw_k = kc.raw_key(key);
  let now = now_millis();
  if sm.is_expired(&raw_k) {
    return Ok(HashSet::default());
  }

  if let Some(meta_bytes) = node
    .read(GetKVReq {
      key: kc.set_meta(key),
    })
    .await?
    && let Some(meta) = SetMeta::decode(&meta_bytes)
    && (meta.is_expired(now) || meta.is_empty())
  {
    return Ok(HashSet::default());
  }

  let prefix = kc.set_prefix(key);
  let items = node
    .scan_prefix(ScanPrefixReq {
      prefix: prefix.clone(),
    })
    .await?;
  let mut set = HashSet::with_capacity_and_hasher(items.len(), Default::default());
  for (k, _) in items {
    set.insert(k[prefix.len()..].to_vec());
  }
  Ok(set)
}

/// 覆盖写入目标集合（对标 Apache Kvrocks Set::Overwrite）
async fn overwrite_set(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  dst: &str,
  members: impl IntoIterator<Item = Vec<u8>>,
) -> Result<i64> {
  let prefix = kc.set_prefix(dst);
  let old_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
  let mut entries = Vec::with_capacity(old_items.len() + 16);
  for (k, _) in old_items {
    entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).to_string()));
  }
  entries.push(UpsertKV::delete(kc.set_meta(dst)));

  let mut count = 0u64;
  for m in members {
    let s_k = kc.set_item_bytes(dst, &m);
    entries.push(UpsertKV::insert(s_k, Vec::new()));
    count += 1;
  }

  if count > 0 {
    let meta = SetMeta::new(0, 0, count);
    entries.push(UpsertKV::insert(kc.set_meta(dst), meta.encode().to_vec()));
  }

  if !entries.is_empty() {
    node.batch_write(BatchWriteReq { entries }).await?;
  }
  Ok(count as i64)
}

/// 处理 Set 集合全套 Redis 命令
pub async fn handle_set(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();
  let sm = node.state_machine();
  let now = now_millis();

  match cmd {
    // ================= 1. 基础 CRUD =================
    RedisCommand::SAdd(key, members) => {
      let raw_k = kc.raw_key(&key);
      let mut metadata = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(m) = SetMeta::decode(&meta_bytes)
      {
        if m.is_expired(now) || sm.is_expired(&raw_k) {
          // 已过期，重置元数据
          SetMeta::new(0, m.base.version + 1, 0)
        } else {
          m
        }
      } else {
        SetMeta::new(0, 0, 0)
      };

      // 去重输入参数中的重复成员
      let mut unique_members = HashSet::with_capacity_and_hasher(members.len(), Default::default());
      for m in members {
        unique_members.insert(m);
      }

      let mut entries = Vec::with_capacity(unique_members.len() + 1);
      let mut added_count = 0u64;

      for m in unique_members {
        let s_k = kc.set_item_bytes(&key, &m);
        if node.read(GetKVReq { key: s_k.clone() }).await?.is_none() {
          added_count += 1;
          entries.push(UpsertKV::insert(s_k, Vec::new()));
        }
      }

      if added_count > 0 {
        metadata.base.size += added_count;
        entries.push(UpsertKV::insert(
          kc.set_meta(&key),
          metadata.encode().to_vec(),
        ));
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(added_count as i64))
    }

    RedisCommand::SRem(key, members) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
      {
        SetMeta::decode(&meta_bytes)
      } else {
        None
      };

      if let Some(meta) = meta_opt
        && meta.is_expired(now)
      {
        return Ok(RespValue::Int(0));
      }

      let mut unique_members = HashSet::with_capacity_and_hasher(members.len(), Default::default());
      for m in members {
        unique_members.insert(m);
      }

      let mut entries = Vec::with_capacity(unique_members.len() + 1);
      let mut removed_count = 0u64;

      for m in unique_members {
        let s_k = kc.set_item_bytes(&key, &m);
        if node.read(GetKVReq { key: s_k.clone() }).await?.is_some() {
          removed_count += 1;
          entries.push(UpsertKV::delete(s_k));
        }
      }

      if removed_count > 0 {
        if let Some(mut meta) = meta_opt {
          if meta.base.size > removed_count {
            meta.base.size -= removed_count;
            entries.push(UpsertKV::insert(kc.set_meta(&key), meta.encode().to_vec()));
          } else {
            entries.push(UpsertKV::delete(kc.set_meta(&key)));
          }
        } else {
          let prefix = kc.set_prefix(&key);
          let rem_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          if rem_items.len() > removed_count as usize {
            let new_size = (rem_items.len() - removed_count as usize) as u64;
            let meta = SetMeta::new(0, 0, new_size);
            entries.push(UpsertKV::insert(kc.set_meta(&key), meta.encode().to_vec()));
          } else {
            entries.push(UpsertKV::delete(kc.set_meta(&key)));
          }
        }

        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(removed_count as i64))
    }

    RedisCommand::SCard(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
      {
        if meta.is_expired(now) {
          return Ok(RespValue::Int(0));
        }
        return Ok(RespValue::Int(meta.size() as i64));
      }

      // 回退兼容：前缀扫描
      let prefix = kc.set_prefix(&key);
      let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      Ok(RespValue::Int(items.len() as i64))
    }

    RedisCommand::SIsMember(key, member) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
        && meta.is_expired(now)
      {
        return Ok(RespValue::Int(0));
      }

      let s_k = kc.set_item_bytes(&key, &member);
      let exists = node.read(GetKVReq { key: s_k }).await?.is_some();
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }

    RedisCommand::SMIsMember(key, members) => {
      let raw_k = kc.raw_key(&key);
      let expired = sm.is_expired(&raw_k)
        || (if let Some(meta_bytes) = node
          .read(GetKVReq {
            key: kc.set_meta(&key),
          })
          .await?
          && let Some(meta) = SetMeta::decode(&meta_bytes)
        {
          meta.is_expired(now)
        } else {
          false
        });

      if expired {
        let results = vec![RespValue::Int(0); members.len()];
        return Ok(RespValue::Arr(results));
      }

      let mut results = Vec::with_capacity(members.len());
      for m in members {
        let s_k = kc.set_item_bytes(&key, &m);
        let exists = node.read(GetKVReq { key: s_k }).await?.is_some();
        results.push(RespValue::Int(if exists { 1 } else { 0 }));
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::SMembers(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
        && meta.is_expired(now)
      {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let prefix = kc.set_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut results = Vec::with_capacity(items.len());
      for (k, _) in items {
        let member = k[prefix.len()..].to_vec();
        results.push(RespValue::Blob(member));
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::SPop(key, count_opt) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
        && meta.is_expired(now)
      {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      let prefix = kc.set_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      if items.is_empty() {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      let c = count_opt.unwrap_or(1).min(items.len());
      if c == 0 {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      let c = count_opt.unwrap_or(1).min(items.len());
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

      let mut popped = Vec::with_capacity(c);
      let mut entries = Vec::with_capacity(c + 1);

      for &idx in &indices {
        let (k, _) = &items[idx];
        let member = k[prefix.len()..].to_vec();
        popped.push(RespValue::Blob(member));
        entries.push(UpsertKV::delete(String::from_utf8_lossy(k).to_string()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(mut meta) = SetMeta::decode(&meta_bytes)
      {
        if meta.base.size > c as u64 {
          meta.base.size -= c as u64;
          entries.push(UpsertKV::insert(kc.set_meta(&key), meta.encode().to_vec()));
        } else {
          entries.push(UpsertKV::delete(kc.set_meta(&key)));
        }
      } else if items.len() > c {
        let new_size = (items.len() - c) as u64;
        let meta = SetMeta::new(0, 0, new_size);
        entries.push(UpsertKV::insert(kc.set_meta(&key), meta.encode().to_vec()));
      } else {
        entries.push(UpsertKV::delete(kc.set_meta(&key)));
      }

      node.batch_write(BatchWriteReq { entries }).await?;

      if count_opt.is_some() {
        Ok(RespValue::Arr(popped))
      } else {
        Ok(popped.into_iter().next().unwrap_or(RespValue::Null))
      }
    }

    RedisCommand::SRandMember(key, count_opt) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
        && meta.is_expired(now)
      {
        return Ok(if count_opt.is_some() {
          RespValue::Arr(Vec::new())
        } else {
          RespValue::Null
        });
      }

      let prefix = kc.set_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
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
          let member = items[idx].0[prefix.len()..].to_vec();
          Ok(RespValue::Blob(member))
        }
        Some(0) => Ok(RespValue::Arr(Vec::new())),
        Some(count) if count > 0 => {
          // 正数：无放回非重复随机采样
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
          let list = indices
            .into_iter()
            .map(|idx| RespValue::Blob(items[idx].0[prefix.len()..].to_vec()))
            .collect();
          Ok(RespValue::Arr(list))
        }
        Some(count) => {
          // 负数：有放回可重复随机采样
          let c = count.unsigned_abs() as usize;
          let mut list = Vec::with_capacity(c);
          for _ in 0..c {
            let idx = fastrand::usize(..items.len());
            list.push(RespValue::Blob(items[idx].0[prefix.len()..].to_vec()));
          }
          Ok(RespValue::Arr(list))
        }
      }
    }

    RedisCommand::SMove { src, dst, member } => {
      if src == dst {
        let s_k = kc.set_item_bytes(&src, &member);
        let exists = node.read(GetKVReq { key: s_k }).await?.is_some();
        return Ok(RespValue::Int(if exists { 1 } else { 0 }));
      }

      let src_k = kc.set_item_bytes(&src, &member);
      if node.read(GetKVReq { key: src_k.clone() }).await?.is_none() {
        return Ok(RespValue::Int(0));
      }

      let dst_k = kc.set_item_bytes(&dst, &member);
      let in_dst = node.read(GetKVReq { key: dst_k.clone() }).await?.is_some();

      let mut entries = Vec::with_capacity(4);
      entries.push(UpsertKV::delete(src_k));

      // 更新 src 元数据
      if let Some(src_meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&src),
        })
        .await?
        && let Some(mut meta) = SetMeta::decode(&src_meta_bytes)
      {
        if meta.base.size > 1 {
          meta.base.size -= 1;
          entries.push(UpsertKV::insert(kc.set_meta(&src), meta.encode().to_vec()));
        } else {
          entries.push(UpsertKV::delete(kc.set_meta(&src)));
        }
      }

      // 更新 dst 元数据及插入成员
      if !in_dst {
        entries.push(UpsertKV::insert(dst_k, Vec::new()));
        let mut dst_meta = if let Some(dst_meta_bytes) = node
          .read(GetKVReq {
            key: kc.set_meta(&dst),
          })
          .await?
          && let Some(m) = SetMeta::decode(&dst_meta_bytes)
        {
          m
        } else {
          SetMeta::new(0, 0, 0)
        };
        dst_meta.base.size += 1;
        entries.push(UpsertKV::insert(
          kc.set_meta(&dst),
          dst_meta.encode().to_vec(),
        ));
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }

    // ================= 2. 集合运算（ACM 级优化实现） =================
    RedisCommand::SUnion(keys) => {
      let mut union_set = HashSet::default();
      for k in keys {
        let cur_members = get_set_members(node, &kc, &k).await?;
        for m in cur_members {
          union_set.insert(m);
        }
      }
      let list = union_set.into_iter().map(RespValue::Blob).collect();
      Ok(RespValue::Arr(list))
    }

    RedisCommand::SUnionStore(dst, keys) => {
      let mut union_set = HashSet::default();
      for k in keys {
        let cur_members = get_set_members(node, &kc, &k).await?;
        for m in cur_members {
          union_set.insert(m);
        }
      }
      let count = overwrite_set(node, &kc, &dst, union_set).await?;
      Ok(RespValue::Int(count))
    }

    RedisCommand::SInter(keys) => {
      if keys.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let mut all_sets = Vec::with_capacity(keys.len());
      for k in keys {
        let cur_set = get_set_members(node, &kc, &k).await?;
        if cur_set.is_empty() {
          // 遇空集合快速短路返回
          return Ok(RespValue::Arr(Vec::new()));
        }
        all_sets.push(cur_set);
      }

      // 按集合基数升序排序，以最小集合为主体过滤
      all_sets.sort_unstable_by_key(|s| s.len());
      let mut iter = all_sets.into_iter();
      let mut inter_set = iter.next().unwrap_or_default();
      for next_set in iter {
        inter_set.retain(|m| next_set.contains(m));
        if inter_set.is_empty() {
          break;
        }
      }
      let list = inter_set.into_iter().map(RespValue::Blob).collect();
      Ok(RespValue::Arr(list))
    }

    RedisCommand::SInterStore(dst, keys) => {
      if keys.is_empty() {
        let count = overwrite_set(node, &kc, &dst, Vec::new()).await?;
        return Ok(RespValue::Int(count));
      }

      let mut all_sets = Vec::with_capacity(keys.len());
      for k in keys {
        let cur_set = get_set_members(node, &kc, &k).await?;
        if cur_set.is_empty() {
          let count = overwrite_set(node, &kc, &dst, Vec::new()).await?;
          return Ok(RespValue::Int(count));
        }
        all_sets.push(cur_set);
      }

      all_sets.sort_unstable_by_key(|s| s.len());
      let mut iter = all_sets.into_iter();
      let mut inter_set = iter.next().unwrap_or_default();
      for next_set in iter {
        inter_set.retain(|m| next_set.contains(m));
        if inter_set.is_empty() {
          break;
        }
      }

      let count = overwrite_set(node, &kc, &dst, inter_set).await?;
      Ok(RespValue::Int(count))
    }

    RedisCommand::SInterCard { keys, limit } => {
      if keys.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let mut all_sets = Vec::with_capacity(keys.len());
      for k in keys {
        let cur_set = get_set_members(node, &kc, &k).await?;
        if cur_set.is_empty() {
          return Ok(RespValue::Int(0));
        }
        all_sets.push(cur_set);
      }

      if all_sets.len() == 1 {
        let count = all_sets[0].len();
        let res = if limit > 0 { count.min(limit) } else { count };
        return Ok(RespValue::Int(res as i64));
      }

      // 按集合大小升序排序，以最小集合为主体进行判断，遇 limit 提前终止
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

    RedisCommand::SDiffCard { keys, limit } => {
      if keys.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let first_set = get_set_members(node, &kc, &keys[0]).await?;
      if first_set.is_empty() || keys.len() == 1 {
        let count = first_set.len();
        let res = if limit > 0 { count.min(limit) } else { count };
        return Ok(RespValue::Int(res as i64));
      }

      let mut exclude_sets = Vec::with_capacity(keys.len() - 1);
      for k in &keys[1..] {
        let s = get_set_members(node, &kc, k).await?;
        if !s.is_empty() {
          exclude_sets.push(s);
        }
      }

      let mut count = 0usize;
      for item in &first_set {
        let in_other = exclude_sets.iter().any(|s| s.contains(item));
        if !in_other {
          count += 1;
          if limit > 0 && count >= limit {
            break;
          }
        }
      }

      Ok(RespValue::Int(count as i64))
    }

    RedisCommand::SUnionCard { keys, limit } => {
      if keys.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let mut union_set = HashSet::default();
      for k in &keys {
        let cur_set = get_set_members(node, &kc, k).await?;
        for item in cur_set {
          union_set.insert(item);
          if limit > 0 && union_set.len() >= limit {
            return Ok(RespValue::Int(limit as i64));
          }
        }
      }

      let count = union_set.len();
      let res = if limit > 0 { count.min(limit) } else { count };
      Ok(RespValue::Int(res as i64))
    }

    RedisCommand::SDiff(keys) => {
      if keys.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let mut diff_set = get_set_members(node, &kc, &keys[0]).await?;
      if diff_set.is_empty() {
        return Ok(RespValue::Arr(Vec::new()));
      }

      for k in &keys[1..] {
        let target_set = get_set_members(node, &kc, k).await?;
        for member in target_set {
          diff_set.remove(&member);
        }
        if diff_set.is_empty() {
          break;
        }
      }

      let list = diff_set.into_iter().map(RespValue::Blob).collect();
      Ok(RespValue::Arr(list))
    }

    RedisCommand::SDiffStore(dst, keys) => {
      if keys.is_empty() {
        let count = overwrite_set(node, &kc, &dst, Vec::new()).await?;
        return Ok(RespValue::Int(count));
      }

      let mut diff_set = get_set_members(node, &kc, &keys[0]).await?;
      if !diff_set.is_empty() {
        for k in &keys[1..] {
          let target_set = get_set_members(node, &kc, k).await?;
          for member in target_set {
            diff_set.remove(&member);
          }
          if diff_set.is_empty() {
            break;
          }
        }
      }

      let count = overwrite_set(node, &kc, &dst, diff_set).await?;
      Ok(RespValue::Int(count))
    }

    // ================= 3. 遍历与匹配 =================
    RedisCommand::SScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(vec![
          RespValue::Blob(b"0".to_vec()),
          RespValue::Arr(Vec::new()),
        ]));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&meta_bytes)
        && meta.is_expired(now)
      {
        return Ok(RespValue::Arr(vec![
          RespValue::Blob(b"0".to_vec()),
          RespValue::Arr(Vec::new()),
        ]));
      }

      let prefix = kc.set_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let pat_str = pattern.unwrap_or_else(|| "*".to_string());
      let pat_bytes = pat_str.as_bytes();
      let limit = count.unwrap_or(10);

      let mut matched = Vec::new();
      for (k, _) in items {
        let member_bytes = &k[prefix.len()..];
        if matches_glob_bytes(pat_bytes, member_bytes) {
          matched.push(RespValue::Blob(member_bytes.to_vec()));
        }
      }

      let start = cursor as usize;
      let end = (start + limit).min(matched.len());
      let next_cursor = if end >= matched.len() { 0 } else { end as u64 };
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

    _ => Err(Error::redis("Command not matched in handle_set")),
  }
}
