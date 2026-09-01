use std::sync::Arc;

use super::context::{ConnectionContext, KeyComposer, ListMeta};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

/// 获取列表元数据并进行有效性校验（过滤已过期或空列表）
#[inline]
async fn get_list_meta(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<Option<ListMeta>> {
  let meta_k = kc.list_meta(key);
  let raw = node.read(GetKVReq { key: meta_k }).await?;
  match raw {
    Some(b) => {
      if let Some(meta) = ListMeta::decode(&b) {
        let now = now_millis();
        if meta.is_expired(now) || meta.base.size == 0 {
          Ok(None)
        } else {
          Ok(Some(meta))
        }
      } else {
        Ok(None)
      }
    }
    None => Ok(None),
  }
}

/// 双端插入实现（支持多元素一次性批量写入）
async fn list_push(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  elements: Vec<Vec<u8>>,
  create_if_missing: bool,
  left: bool,
) -> Result<i64> {
  if elements.is_empty() {
    let meta = get_list_meta(node, kc, key).await?;
    return Ok(meta.map_or(0, |m| m.base.size as i64));
  }

  let meta_opt = get_list_meta(node, kc, key).await?;
  if meta_opt.is_none() && !create_if_missing {
    return Ok(0);
  }

  let meta_k = kc.list_meta(key);
  let mut meta = meta_opt.unwrap_or_else(|| ListMeta::new(0, now_millis()));

  let elem_count = elements.len();
  let mut entries = Vec::with_capacity(elem_count + 1);

  if left {
    let mut idx = meta.head;
    for el in elements {
      idx = idx.wrapping_sub(1);
      entries.push(UpsertKV::insert(kc.list_item(key, idx), el));
    }
    meta.head = meta.head.wrapping_sub(elem_count as u64);
  } else {
    let mut idx = meta.tail;
    for el in elements {
      entries.push(UpsertKV::insert(kc.list_item(key, idx), el));
      idx = idx.wrapping_add(1);
    }
    meta.tail = meta.tail.wrapping_add(elem_count as u64);
  }

  meta.base.size += elem_count as u64;
  entries.push(UpsertKV::insert(meta_k, meta.encode().to_vec()));

  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(meta.base.size as i64)
}

/// 双端弹出实现（支持批量弹出与批量删除）
async fn list_pop(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  count_opt: Option<usize>,
  left: bool,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let mut meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Null),
  };

  let requested_count = count_opt.unwrap_or(1);
  if requested_count == 0 {
    return if count_opt.is_some() {
      Ok(RespValue::Arr(Vec::new()))
    } else {
      Ok(RespValue::Null)
    };
  }

  let to_pop = requested_count.min(meta.base.size as usize);
  let mut popped = Vec::with_capacity(to_pop);
  let mut entries = Vec::with_capacity(to_pop + 1);

  if left {
    for i in 0..to_pop {
      let idx = meta.head.wrapping_add(i as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node
        .read(GetKVReq {
          key: item_k.clone(),
        })
        .await?
      {
        popped.push(v);
        entries.push(UpsertKV::delete(item_k));
      }
    }
    meta.head = meta.head.wrapping_add(to_pop as u64);
  } else {
    for i in 0..to_pop {
      let idx = meta.tail.wrapping_sub(1 + i as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node
        .read(GetKVReq {
          key: item_k.clone(),
        })
        .await?
      {
        popped.push(v);
        entries.push(UpsertKV::delete(item_k));
      }
    }
    meta.tail = meta.tail.wrapping_sub(to_pop as u64);
  }

  meta.base.size -= to_pop as u64;
  let meta_k = kc.list_meta(key);
  if meta.base.size == 0 {
    entries.push(UpsertKV::delete(meta_k));
  } else {
    entries.push(UpsertKV::insert(meta_k, meta.encode().to_vec()));
  }

  node.batch_write(BatchWriteReq { entries }).await?;

  if count_opt.is_none() {
    if let Some(first) = popped.into_iter().next() {
      Ok(RespValue::Blob(first))
    } else {
      Ok(RespValue::Null)
    }
  } else {
    Ok(RespValue::Arr(
      popped.into_iter().map(RespValue::Blob).collect(),
    ))
  }
}

/// 列表长度获取
#[inline]
async fn list_len(node: &Arc<RaftNode>, kc: &KeyComposer<'_>, key: &str) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let len = meta_opt.map_or(0, |m| m.base.size as i64);
  Ok(RespValue::Int(len))
}

/// 获取指定索引处元素
async fn list_index(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  idx: i64,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Null),
  };

  let len = meta.base.size as i64;
  let real_idx = if idx < 0 { len + idx } else { idx };
  if real_idx < 0 || real_idx >= len {
    return Ok(RespValue::Null);
  }

  let target_idx = meta.head.wrapping_add(real_idx as u64);
  let item_k = kc.list_item(key, target_idx);
  let val = node.read(GetKVReq { key: item_k }).await?;
  match val {
    Some(v) => Ok(RespValue::Blob(v)),
    None => Ok(RespValue::Null),
  }
}

/// 设置指定索引处元素值
async fn list_set(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  idx: i64,
  elem: Vec<u8>,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Err(Error::invalid_data("ERR no such key")),
  };

  let len = meta.base.size as i64;
  let real_idx = if idx < 0 { len + idx } else { idx };
  if real_idx < 0 || real_idx >= len {
    return Err(Error::invalid_data("ERR index out of range"));
  }

  let target_idx = meta.head.wrapping_add(real_idx as u64);
  let item_k = kc.list_item(key, target_idx);
  let entries = vec![UpsertKV::insert(item_k, elem)];
  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(RespValue::ok())
}

/// 范围读取（对标 Kvrocks Range 标准化逻辑）
async fn list_range(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  start: i64,
  stop: i64,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Arr(Vec::new())),
  };

  let len = meta.base.size as i64;
  let mut s = if start < 0 { len + start } else { start };
  let mut e = if stop < 0 { len + stop } else { stop };

  if s > len || e < 0 || s > e {
    return Ok(RespValue::Arr(Vec::new()));
  }

  if s < 0 {
    s = 0;
  }
  if e >= len {
    e = len - 1;
  }

  let count = (e - s + 1) as usize;
  let mut results = Vec::with_capacity(count);

  for i in s..=e {
    let idx = meta.head.wrapping_add(i as u64);
    let item_k = kc.list_item(key, idx);
    if let Some(v) = node.read(GetKVReq { key: item_k }).await? {
      results.push(RespValue::Blob(v));
    }
  }
  Ok(RespValue::Arr(results))
}

/// 修剪列表（移除指定范围外的元素）
async fn list_trim(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  start: i64,
  stop: i64,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let mut meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::ok()),
  };

  let len = meta.base.size as i64;
  let mut s = if start < 0 { len + start } else { start };
  let mut e = if stop < 0 { len + stop } else { stop };

  if s > e || s >= len || e < 0 {
    // 清空整个列表
    let mut entries = Vec::with_capacity(meta.base.size as usize + 1);
    for i in 0..meta.base.size {
      let idx = meta.head.wrapping_add(i);
      entries.push(UpsertKV::delete(kc.list_item(key, idx)));
    }
    entries.push(UpsertKV::delete(kc.list_meta(key)));
    node.batch_write(BatchWriteReq { entries }).await?;
    return Ok(RespValue::ok());
  }

  if s < 0 {
    s = 0;
  }
  if e >= len {
    e = len - 1;
  }

  let mut entries = Vec::new();
  // 删除左侧超出范围项 [0..s)
  for i in 0..s {
    let idx = meta.head.wrapping_add(i as u64);
    entries.push(UpsertKV::delete(kc.list_item(key, idx)));
  }
  // 删除右侧超出范围项 [e+1..len)
  for i in (e + 1)..len {
    let idx = meta.head.wrapping_add(i as u64);
    entries.push(UpsertKV::delete(kc.list_item(key, idx)));
  }

  let new_size = (e - s + 1) as u64;
  meta.head = meta.head.wrapping_add(s as u64);
  meta.tail = meta.head.wrapping_add(new_size);
  meta.base.size = new_size;

  let meta_k = kc.list_meta(key);
  if new_size == 0 {
    entries.push(UpsertKV::delete(meta_k));
  } else {
    entries.push(UpsertKV::insert(meta_k, meta.encode().to_vec()));
  }

  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(RespValue::ok())
}

/// 移除与指定值匹配的元素（对标 Kvrocks 最小位移量算法）
async fn list_rem(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  count: i64,
  elem: &[u8],
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let mut meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Int(0)),
  };

  let len = meta.base.size as usize;
  let target_del_limit = if count == 0 {
    usize::MAX
  } else {
    count.unsigned_abs() as usize
  };

  // 扫描匹配项的相对偏移
  let mut to_delete_offsets = Vec::new();
  if count >= 0 {
    for offset in 0..len {
      let idx = meta.head.wrapping_add(offset as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node.read(GetKVReq { key: item_k }).await?
        && v == elem
      {
        to_delete_offsets.push(offset);
        if to_delete_offsets.len() >= target_del_limit {
          break;
        }
      }
    }
  } else {
    for step in 0..len {
      let offset = len - 1 - step;
      let idx = meta.head.wrapping_add(offset as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node.read(GetKVReq { key: item_k }).await?
        && v == elem
      {
        to_delete_offsets.push(offset);
        if to_delete_offsets.len() >= target_del_limit {
          break;
        }
      }
    }
    to_delete_offsets.reverse(); // 保持升序
  }

  if to_delete_offsets.is_empty() {
    return Ok(RespValue::Int(0));
  }

  let del_cnt = to_delete_offsets.len();
  let meta_k = kc.list_meta(key);

  // 如果全部元素被删除
  if del_cnt == len {
    let mut entries = Vec::with_capacity(len + 1);
    for offset in 0..len {
      let idx = meta.head.wrapping_add(offset as u64);
      entries.push(UpsertKV::delete(kc.list_item(key, idx)));
    }
    entries.push(UpsertKV::delete(meta_k));
    node.batch_write(BatchWriteReq { entries }).await?;
    return Ok(RespValue::Int(del_cnt as i64));
  }

  // 比较左右两侧移动代价，选择较小一端进行位移
  let min_del_offset = to_delete_offsets[0];
  let max_del_offset = to_delete_offsets[del_cnt - 1];
  let left_cost = max_del_offset;
  let right_cost = len - 1 - min_del_offset;

  let mut entries = Vec::new();

  if left_cost <= right_cost {
    // 左侧向右移覆盖
    let mut target_offset = max_del_offset;
    let mut del_idx_cursor = del_cnt;

    for offset in (0..=max_del_offset).rev() {
      if del_idx_cursor > 0 && to_delete_offsets[del_idx_cursor - 1] == offset {
        del_idx_cursor -= 1;
      } else {
        if target_offset != offset {
          let from_idx = meta.head.wrapping_add(offset as u64);
          let to_idx = meta.head.wrapping_add(target_offset as u64);
          if let Some(v) = node
            .read(GetKVReq {
              key: kc.list_item(key, from_idx),
            })
            .await?
          {
            entries.push(UpsertKV::insert(kc.list_item(key, to_idx), v));
          }
        }
        target_offset = target_offset.saturating_sub(1);
      }
    }

    // 删除左端空出的条目
    for offset in 0..del_cnt {
      let idx = meta.head.wrapping_add(offset as u64);
      entries.push(UpsertKV::delete(kc.list_item(key, idx)));
    }

    meta.head = meta.head.wrapping_add(del_cnt as u64);
  } else {
    // 右侧向左移覆盖
    let mut target_offset = min_del_offset;
    let mut del_idx_cursor = 0;

    for offset in min_del_offset..len {
      if del_idx_cursor < del_cnt && to_delete_offsets[del_idx_cursor] == offset {
        del_idx_cursor += 1;
      } else {
        if target_offset != offset {
          let from_idx = meta.head.wrapping_add(offset as u64);
          let to_idx = meta.head.wrapping_add(target_offset as u64);
          if let Some(v) = node
            .read(GetKVReq {
              key: kc.list_item(key, from_idx),
            })
            .await?
          {
            entries.push(UpsertKV::insert(kc.list_item(key, to_idx), v));
          }
        }
        target_offset += 1;
      }
    }

    // 删除右端空出的条目
    for offset in (len - del_cnt)..len {
      let idx = meta.head.wrapping_add(offset as u64);
      entries.push(UpsertKV::delete(kc.list_item(key, idx)));
    }

    meta.tail = meta.tail.wrapping_sub(del_cnt as u64);
  }

  meta.base.size -= del_cnt as u64;
  entries.push(UpsertKV::insert(meta_k, meta.encode().to_vec()));
  node.batch_write(BatchWriteReq { entries }).await?;

  Ok(RespValue::Int(del_cnt as i64))
}

/// 插入元素到指定基准元素前后（对标 Kvrocks 最小位移量算法）
async fn list_insert(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  before: bool,
  pivot: &[u8],
  element: Vec<u8>,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let mut meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Int(0)),
  };

  let len = meta.base.size as usize;
  let mut pivot_offset = None;

  for offset in 0..len {
    let idx = meta.head.wrapping_add(offset as u64);
    let item_k = kc.list_item(key, idx);
    if let Some(v) = node.read(GetKVReq { key: item_k }).await?
      && v == pivot
    {
      pivot_offset = Some(offset);
      break;
    }
  }

  let p_off = match pivot_offset {
    Some(off) => off,
    None => return Ok(RespValue::Int(-1)),
  };

  let target_insert_offset = if before { p_off } else { p_off + 1 };
  let left_cost = target_insert_offset;
  let right_cost = len - target_insert_offset;

  let mut entries = Vec::new();

  if left_cost <= right_cost {
    // 左端向左位移 1 个单位
    for offset in 0..target_insert_offset {
      let from_idx = meta.head.wrapping_add(offset as u64);
      let to_idx = from_idx.wrapping_sub(1);
      if let Some(v) = node
        .read(GetKVReq {
          key: kc.list_item(key, from_idx),
        })
        .await?
      {
        entries.push(UpsertKV::insert(kc.list_item(key, to_idx), v));
      }
    }
    let insert_idx = meta
      .head
      .wrapping_add(target_insert_offset as u64)
      .wrapping_sub(1);
    entries.push(UpsertKV::insert(kc.list_item(key, insert_idx), element));
    meta.head = meta.head.wrapping_sub(1);
  } else {
    // 右端向右位移 1 个单位
    for offset in (target_insert_offset..len).rev() {
      let from_idx = meta.head.wrapping_add(offset as u64);
      let to_idx = from_idx.wrapping_add(1);
      if let Some(v) = node
        .read(GetKVReq {
          key: kc.list_item(key, from_idx),
        })
        .await?
      {
        entries.push(UpsertKV::insert(kc.list_item(key, to_idx), v));
      }
    }
    let insert_idx = meta.head.wrapping_add(target_insert_offset as u64);
    entries.push(UpsertKV::insert(kc.list_item(key, insert_idx), element));
    meta.tail = meta.tail.wrapping_add(1);
  }

  meta.base.size += 1;
  let meta_k = kc.list_meta(key);
  entries.push(UpsertKV::insert(meta_k, meta.encode().to_vec()));
  node.batch_write(BatchWriteReq { entries }).await?;

  Ok(RespValue::Int(meta.base.size as i64))
}

/// 元素位置检索（完整支持 RANK, COUNT, MAXLEN 选项，对标 Kvrocks PosSpec）
async fn list_pos(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  element: &[u8],
  rank: Option<i64>,
  count: Option<usize>,
  max_len: Option<usize>,
) -> Result<RespValue> {
  if rank == Some(0) {
    return Err(Error::invalid_data(
      "ERR RANK can't be zero: must be 1, 2, 3, ... or -1, -2, -3...",
    ));
  }

  let meta_opt = get_list_meta(node, kc, key).await?;
  let meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => {
      return if count.is_some() {
        Ok(RespValue::Arr(Vec::new()))
      } else {
        Ok(RespValue::Null)
      };
    }
  };

  let len = meta.base.size as usize;
  let req_rank = rank.unwrap_or(1);
  let reversed = req_rank < 0;
  let target_rank = req_rank.unsigned_abs() as usize;
  let limit = max_len.map(|m| m.min(len)).unwrap_or(len);

  let mut matches = Vec::new();
  let mut match_count = 0;

  if !reversed {
    for offset in 0..limit {
      let idx = meta.head.wrapping_add(offset as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node.read(GetKVReq { key: item_k }).await?
        && v == element
      {
        match_count += 1;
        if match_count >= target_rank {
          matches.push(offset as i64);
          if let Some(c) = count
            && c > 0
            && matches.len() >= c
          {
            break;
          }
          if count.is_none() {
            break;
          }
        }
      }
    }
  } else {
    for step in 0..limit {
      let offset = len - 1 - step;
      let idx = meta.head.wrapping_add(offset as u64);
      let item_k = kc.list_item(key, idx);
      if let Some(v) = node.read(GetKVReq { key: item_k }).await?
        && v == element
      {
        match_count += 1;
        if match_count >= target_rank {
          matches.push(offset as i64);
          if let Some(c) = count
            && c > 0
            && matches.len() >= c
          {
            break;
          }
          if count.is_none() {
            break;
          }
        }
      }
    }
  }

  if count.is_some() {
    Ok(RespValue::Arr(
      matches.into_iter().map(RespValue::Int).collect(),
    ))
  } else if let Some(first) = matches.into_iter().next() {
    Ok(RespValue::Int(first))
  } else {
    Ok(RespValue::Null)
  }
}

/// 列表移动（原子迁移单个元素，支持单列表与双列表）
async fn list_move(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  src: &str,
  dst: &str,
  src_left: bool,
  dst_left: bool,
) -> Result<RespValue> {
  if src == dst {
    list_move_single(node, kc, src, src_left, dst_left).await
  } else {
    list_move_two(node, kc, src, dst, src_left, dst_left).await
  }
}

/// 单列表内部元素轮转
async fn list_move_single(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  src_left: bool,
  dst_left: bool,
) -> Result<RespValue> {
  let meta_opt = get_list_meta(node, kc, key).await?;
  let mut meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Null),
  };

  let curr_idx = if src_left {
    meta.head
  } else {
    meta.tail.wrapping_sub(1)
  };
  let curr_k = kc.list_item(key, curr_idx);
  let elem = match node
    .read(GetKVReq {
      key: curr_k.clone(),
    })
    .await?
  {
    Some(v) => v,
    None => return Ok(RespValue::Null),
  };

  if src_left == dst_left || meta.base.size == 1 {
    return Ok(RespValue::Blob(elem));
  }

  let mut entries = Vec::with_capacity(3);
  entries.push(UpsertKV::delete(curr_k));

  if src_left {
    let new_tail_idx = meta.tail;
    entries.push(UpsertKV::insert(
      kc.list_item(key, new_tail_idx),
      elem.clone(),
    ));
    meta.head = meta.head.wrapping_add(1);
    meta.tail = meta.tail.wrapping_add(1);
  } else {
    let new_head_idx = meta.head.wrapping_sub(1);
    entries.push(UpsertKV::insert(
      kc.list_item(key, new_head_idx),
      elem.clone(),
    ));
    meta.head = meta.head.wrapping_sub(1);
    meta.tail = meta.tail.wrapping_sub(1);
  }

  entries.push(UpsertKV::insert(kc.list_meta(key), meta.encode().to_vec()));
  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(RespValue::Blob(elem))
}

/// 双列表间元素原子迁移
async fn list_move_two(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  src: &str,
  dst: &str,
  src_left: bool,
  dst_left: bool,
) -> Result<RespValue> {
  let src_meta_opt = get_list_meta(node, kc, src).await?;
  let mut src_meta = match src_meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Null),
  };

  let dst_meta_opt = get_list_meta(node, kc, dst).await?;
  let mut dst_meta = dst_meta_opt.unwrap_or_else(|| ListMeta::new(0, now_millis()));

  let src_idx = if src_left {
    src_meta.head
  } else {
    src_meta.tail.wrapping_sub(1)
  };
  let src_k = kc.list_item(src, src_idx);
  let elem = match node.read(GetKVReq { key: src_k.clone() }).await? {
    Some(v) => v,
    None => return Ok(RespValue::Null),
  };

  let mut entries = Vec::with_capacity(4);

  // 从 src 弹出
  entries.push(UpsertKV::delete(src_k));
  if src_left {
    src_meta.head = src_meta.head.wrapping_add(1);
  } else {
    src_meta.tail = src_meta.tail.wrapping_sub(1);
  }
  src_meta.base.size -= 1;

  let src_meta_k = kc.list_meta(src);
  if src_meta.base.size == 0 {
    entries.push(UpsertKV::delete(src_meta_k));
  } else {
    entries.push(UpsertKV::insert(src_meta_k, src_meta.encode().to_vec()));
  }

  // 压入 dst
  let dst_idx = if dst_left {
    let idx = dst_meta.head.wrapping_sub(1);
    dst_meta.head = idx;
    idx
  } else {
    let idx = dst_meta.tail;
    dst_meta.tail = dst_meta.tail.wrapping_add(1);
    idx
  };
  entries.push(UpsertKV::insert(kc.list_item(dst, dst_idx), elem.clone()));
  dst_meta.base.size += 1;
  entries.push(UpsertKV::insert(
    kc.list_meta(dst),
    dst_meta.encode().to_vec(),
  ));

  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(RespValue::Blob(elem))
}

/// LMOVEM / BLMOVEM 批量移动参数
#[derive(Debug, Clone, Copy)]
struct ListMoveOpts<'a> {
  src: &'a str,
  dst: &'a str,
  src_left: bool,
  dst_left: bool,
  count: Option<usize>,
  exactly: Option<usize>,
}

/// 批量列表元素移动 (LMOVEM / BLMOVEM)
async fn list_move_m(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  opts: ListMoveOpts<'_>,
) -> Result<RespValue> {
  let ListMoveOpts {
    src,
    dst,
    src_left,
    dst_left,
    count,
    exactly,
  } = opts;
  let meta_opt = get_list_meta(node, kc, src).await?;
  let src_meta = match meta_opt {
    Some(m) if m.base.size > 0 => m,
    _ => return Ok(RespValue::Arr(Vec::new())),
  };

  let req_cnt = if let Some(exact) = exactly {
    if (src_meta.base.size as usize) < exact {
      return Ok(RespValue::Null);
    }
    exact
  } else {
    count.unwrap_or(1).min(src_meta.base.size as usize)
  };

  if req_cnt == 0 {
    return Ok(RespValue::Arr(Vec::new()));
  }

  let mut moved = Vec::with_capacity(req_cnt);
  for _ in 0..req_cnt {
    let res = list_move(node, kc, src, dst, src_left, dst_left).await?;
    if let RespValue::Blob(b) = res {
      moved.push(RespValue::Blob(b));
    }
  }
  Ok(RespValue::Arr(moved))
}

/// 列表命令主处理器调度
pub async fn handle_list(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::LPush(key, elements) => {
      let res = list_push(node, &kc, &key, elements, true, true).await?;
      Ok(RespValue::Int(res))
    }
    RedisCommand::RPush(key, elements) => {
      let res = list_push(node, &kc, &key, elements, true, false).await?;
      Ok(RespValue::Int(res))
    }
    RedisCommand::LPushX(key, elements) => {
      let res = list_push(node, &kc, &key, elements, false, true).await?;
      Ok(RespValue::Int(res))
    }
    RedisCommand::RPushX(key, elements) => {
      let res = list_push(node, &kc, &key, elements, false, false).await?;
      Ok(RespValue::Int(res))
    }
    RedisCommand::LPop(key, count_opt) => list_pop(node, &kc, &key, count_opt, true).await,
    RedisCommand::RPop(key, count_opt) => list_pop(node, &kc, &key, count_opt, false).await,
    RedisCommand::LLen(key) => list_len(node, &kc, &key).await,
    RedisCommand::LIndex(key, idx) => list_index(node, &kc, &key, idx).await,
    RedisCommand::LSet(key, idx, val) => list_set(node, &kc, &key, idx, val).await,
    RedisCommand::LRange(key, start, stop) => list_range(node, &kc, &key, start, stop).await,
    RedisCommand::LTrim(key, start, stop) => list_trim(node, &kc, &key, start, stop).await,
    RedisCommand::LRem(key, count, element) => list_rem(node, &kc, &key, count, &element).await,
    RedisCommand::LInsert {
      key,
      before,
      pivot,
      element,
    } => list_insert(node, &kc, &key, before, &pivot, element).await,
    RedisCommand::LMove {
      src,
      dst,
      src_left,
      dst_left,
    } => list_move(node, &kc, &src, &dst, src_left, dst_left).await,
    RedisCommand::LMoveM {
      src,
      dst,
      src_left,
      dst_left,
      count,
      exactly,
    } => {
      list_move_m(
        node,
        &kc,
        ListMoveOpts {
          src: &src,
          dst: &dst,
          src_left,
          dst_left,
          count,
          exactly,
        },
      )
      .await
    }
    RedisCommand::RPopLPush(src, dst) => list_move(node, &kc, &src, &dst, false, true).await,
    RedisCommand::LPos {
      key,
      element,
      rank,
      count,
      max_len,
    } => list_pos(node, &kc, &key, &element, rank, count, max_len).await,
    RedisCommand::BLPop(keys, _) => {
      for k in keys {
        let popped = list_pop(node, &kc, &k, None, true).await?;
        if let RespValue::Blob(b) = popped {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Blob(b),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    RedisCommand::BRPop(keys, _) => {
      for k in keys {
        let popped = list_pop(node, &kc, &k, None, false).await?;
        if let RespValue::Blob(b) = popped {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Blob(b),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    RedisCommand::BLMove {
      src,
      dst,
      src_left,
      dst_left,
      timeout: _,
    } => list_move(node, &kc, &src, &dst, src_left, dst_left).await,
    RedisCommand::BLMoveM {
      src,
      dst,
      src_left,
      dst_left,
      count,
      exactly,
      timeout: _,
    } => {
      list_move_m(
        node,
        &kc,
        ListMoveOpts {
          src: &src,
          dst: &dst,
          src_left,
          dst_left,
          count,
          exactly,
        },
      )
      .await
    }
    RedisCommand::LMPop { keys, left, count }
    | RedisCommand::BLMPop {
      keys,
      left,
      count,
      timeout: _,
    } => {
      for k in keys {
        let popped = list_pop(node, &kc, &k, Some(count), left).await?;
        if let RespValue::Arr(arr) = popped
          && !arr.is_empty()
        {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Arr(arr),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    _ => Err(Error::redis("Command not matched in handle_list")),
  }
}
