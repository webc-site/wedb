use rapidhash::{RapidHashMap, RapidHashSet};
use std::sync::Arc;

use super::context::{BloomChainMeta, ConnectionContext, CuckooChainMeta, KeyComposer};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::bloom::{
  BlockSplitBloomFilter, BloomFilterAddResult, BloomFilterInsertOptions, CuckooFilterHelper,
};
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

/// 布隆过滤器插入通用底层逻辑
async fn bloom_insert_common(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  items: &[Vec<u8>],
  opt: &BloomFilterInsertOptions,
) -> Result<Vec<BloomFilterAddResult>> {
  let bf_meta_k = kc.bf_meta(key);
  let mut meta = match node
    .read(GetKVReq {
      key: bf_meta_k.clone(),
    })
    .await?
  {
    Some(bytes) => {
      BloomChainMeta::decode(&bytes).ok_or_else(|| Error::internal("corrupted bloom metadata"))?
    }
    None => {
      if !opt.auto_create {
        return Err(Error::invalid_data("ERR not found"));
      }
      let bloom_bytes =
        BlockSplitBloomFilter::optimal_num_of_bytes(opt.capacity, opt.error_rate) as u32;
      BloomChainMeta::new(
        opt.capacity,
        opt.error_rate,
        opt.expansion,
        0,
        0,
        bloom_bytes,
      )
    }
  };

  let mut sub_filters = Vec::with_capacity(meta.n_filters as usize);
  let mut dirty_filters = RapidHashSet::default();
  for i in 0..meta.n_filters {
    let sub_k = kc.bf_item(key, i);
    let cap = meta.sub_filter_capacity(i);
    let expected_bytes = BlockSplitBloomFilter::optimal_num_of_bytes(cap, meta.error_rate);
    let data = match node.read(GetKVReq { key: sub_k }).await? {
      Some(mut d) => {
        if d.len() < expected_bytes {
          d.resize(expected_bytes, 0);
        }
        d
      }
      None => vec![0u8; expected_bytes],
    };
    sub_filters.push(data);
  }

  let origin_size = meta.base.size;
  let mut results = Vec::with_capacity(items.len());

  for item in items {
    let h = BlockSplitBloomFilter::hash(item);
    let mut exists = false;
    for data in sub_filters.iter().rev() {
      if BlockSplitBloomFilter::find_hash(data, h) {
        exists = true;
        break;
      }
    }

    if exists {
      results.push(BloomFilterAddResult::Exist);
    } else {
      if meta.base.size + 1 > meta.get_capacity() as u64 {
        if meta.is_scaling() {
          let new_filter_idx = meta.n_filters;
          let new_cap = meta.sub_filter_capacity(new_filter_idx);
          let new_bytes = BlockSplitBloomFilter::optimal_num_of_bytes(new_cap, meta.error_rate);
          meta.n_filters += 1;
          meta.bloom_bytes += new_bytes as u32;
          sub_filters.push(vec![0u8; new_bytes]);
          dirty_filters.insert(new_filter_idx);
        } else {
          results.push(BloomFilterAddResult::Full);
          continue;
        }
      }

      let last_idx = (meta.n_filters - 1) as usize;
      BlockSplitBloomFilter::insert_hash(&mut sub_filters[last_idx], h);
      dirty_filters.insert(last_idx as u16);
      meta.base.size += 1;
      results.push(BloomFilterAddResult::Ok);
    }
  }

  if meta.base.size != origin_size || !dirty_filters.is_empty() {
    let mut entries = Vec::with_capacity(1 + dirty_filters.len());
    entries.push(UpsertKV::insert(bf_meta_k, meta.encode().to_vec()));
    for idx in dirty_filters {
      let sub_k = kc.bf_item(key, idx);
      entries.push(UpsertKV::insert(sub_k, sub_filters[idx as usize].clone()));
    }
    node.batch_write(BatchWriteReq { entries }).await?;
  }

  Ok(results)
}

/// 布谷鸟过滤器分页读写缓存
struct CuckooPageCache<'a> {
  node: &'a Arc<RaftNode>,
  kc: &'a KeyComposer<'a>,
  key: &'a str,
  bucket_size: u8,
  page_size: u32,
  pages: RapidHashMap<(u16, u32), (Vec<u8>, bool)>,
}

impl<'a> CuckooPageCache<'a> {
  fn new(
    node: &'a Arc<RaftNode>,
    kc: &'a KeyComposer<'a>,
    key: &'a str,
    bucket_size: u8,
    page_size: u32,
  ) -> Self {
    Self {
      node,
      kc,
      key,
      bucket_size,
      page_size,
      pages: RapidHashMap::default(),
    }
  }

  fn get_bucket_location(&self, bucket_idx: u32) -> (u32, usize) {
    let buckets_per_page = (self.page_size / self.bucket_size as u32).max(1);
    let page_idx = bucket_idx / buckets_per_page;
    let offset = ((bucket_idx % buckets_per_page) as usize) * (self.bucket_size as usize);
    (page_idx, offset)
  }

  async fn get_page(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    page_idx: u32,
  ) -> Result<&mut [u8]> {
    if !self.pages.contains_key(&(filter_idx, page_idx)) {
      let buckets_per_page = (self.page_size / self.bucket_size as u32).max(1);
      let first_bucket = page_idx * buckets_per_page;
      let page_bucket_count = buckets_per_page.min(num_buckets.saturating_sub(first_bucket));
      let expected_size = (page_bucket_count as usize) * (self.bucket_size as usize);

      let page_key = self.kc.cf_page(self.key, filter_idx, page_idx);
      let data = match self.node.read(GetKVReq { key: page_key }).await? {
        Some(mut d) => {
          if d.len() < expected_size {
            d.resize(expected_size, 0);
          }
          d
        }
        None => vec![0u8; expected_size],
      };
      self.pages.insert((filter_idx, page_idx), (data, false));
    }
    self
      .pages
      .get_mut(&(filter_idx, page_idx))
      .map(|(data, _)| data.as_mut_slice())
      .ok_or_else(|| Error::internal("Failed to retrieve cuckoo filter page"))
  }

  async fn try_insert_in_bucket(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    fp: u8,
  ) -> Result<bool> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let bs = self.bucket_size as usize;
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    for slot in 0..bs {
      if page[offset + slot] == 0 {
        page[offset + slot] = fp;
        if let Some(entry) = self.pages.get_mut(&(filter_idx, page_idx)) {
          entry.1 = true;
        }
        return Ok(true);
      }
    }
    Ok(false)
  }

  async fn get_bucket_slot(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    slot: usize,
  ) -> Result<u8> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    Ok(page[offset + slot])
  }

  async fn set_bucket_slot(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    slot: usize,
    fp: u8,
  ) -> Result<()> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    page[offset + slot] = fp;
    if let Some(entry) = self.pages.get_mut(&(filter_idx, page_idx)) {
      entry.1 = true;
    }
    Ok(())
  }

  async fn contains_in_bucket(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    fp: u8,
  ) -> Result<bool> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let bs = self.bucket_size as usize;
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    for slot in 0..bs {
      if page[offset + slot] == fp {
        return Ok(true);
      }
    }
    Ok(false)
  }

  async fn count_in_bucket(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    fp: u8,
  ) -> Result<usize> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let bs = self.bucket_size as usize;
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    let mut count = 0;
    for slot in 0..bs {
      if page[offset + slot] == fp {
        count += 1;
      }
    }
    Ok(count)
  }

  async fn delete_from_bucket(
    &mut self,
    filter_idx: u16,
    num_buckets: u32,
    bucket_idx: u32,
    fp: u8,
  ) -> Result<bool> {
    let (page_idx, offset) = self.get_bucket_location(bucket_idx);
    let bs = self.bucket_size as usize;
    let page = self.get_page(filter_idx, num_buckets, page_idx).await?;
    for slot in 0..bs {
      if page[offset + slot] == fp {
        page[offset + slot] = 0;
        if let Some(entry) = self.pages.get_mut(&(filter_idx, page_idx)) {
          entry.1 = true;
        }
        return Ok(true);
      }
    }
    Ok(false)
  }

  fn discard_dirty(&mut self) {
    self.pages.clear();
  }

  fn collect_dirty_entries(&self) -> Vec<UpsertKV> {
    let mut entries = Vec::new();
    for (&(filter_idx, page_idx), (data, is_dirty)) in &self.pages {
      if *is_dirty {
        let page_key = self.kc.cf_page(self.key, filter_idx, page_idx);
        entries.push(UpsertKV::insert(page_key, data.clone()));
      }
    }
    entries
  }
}

async fn try_kickout_insert(
  page_cache: &mut CuckooPageCache<'_>,
  filter_idx: u16,
  num_buckets: u32,
  bucket_size: u8,
  max_iterations: u16,
  hash: u64,
  fp: u8,
) -> Result<bool> {
  let mut cur_i = (hash % (num_buckets as u64)) as u32;
  let mut cur_fp = fp;

  for _ in 0..max_iterations {
    let slot = fastrand::usize(..) % (bucket_size as usize);
    let old_fp = page_cache
      .get_bucket_slot(filter_idx, num_buckets, cur_i, slot)
      .await?;
    page_cache
      .set_bucket_slot(filter_idx, num_buckets, cur_i, slot, cur_fp)
      .await?;

    cur_fp = old_fp;
    cur_i = CuckooFilterHelper::get_alt_bucket_index(cur_i, cur_fp, num_buckets);

    if page_cache
      .try_insert_in_bucket(filter_idx, num_buckets, cur_i, cur_fp)
      .await?
    {
      return Ok(true);
    }
  }

  page_cache.discard_dirty();
  Ok(false)
}

async fn cuckoo_add_single(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  item: &[u8],
  default_capacity: u64,
) -> Result<bool> {
  let cf_meta_k = kc.cf_meta(key);
  let mut meta = match node
    .read(GetKVReq {
      key: cf_meta_k.clone(),
    })
    .await?
  {
    Some(bytes) => {
      CuckooChainMeta::decode(&bytes).ok_or_else(|| Error::internal("corrupted cuckoo metadata"))?
    }
    None => CuckooChainMeta::new(
      default_capacity,
      CuckooFilterHelper::DEFAULT_BUCKET_SIZE,
      CuckooFilterHelper::DEFAULT_MAX_ITERATIONS,
      CuckooFilterHelper::DEFAULT_EXPANSION,
      CuckooFilterHelper::DEFAULT_PAGE_SIZE,
      0,
      0,
    ),
  };

  let hash = CuckooFilterHelper::hash(item);
  let fp = CuckooFilterHelper::generate_fingerprint(hash);

  let mut page_cache = CuckooPageCache::new(node, kc, key, meta.bucket_size, meta.page_size);

  let mut inserted = false;
  for filter_idx in (0..meta.n_filters).rev() {
    let num_buckets = meta.sub_filter_num_buckets(filter_idx)?;
    let b1 = (hash % (num_buckets as u64)) as u32;
    let b2 = CuckooFilterHelper::get_alt_bucket_index(b1, fp, num_buckets);

    if page_cache
      .try_insert_in_bucket(filter_idx, num_buckets, b1, fp)
      .await?
    {
      inserted = true;
      break;
    }
    if b1 != b2
      && page_cache
        .try_insert_in_bucket(filter_idx, num_buckets, b2, fp)
        .await?
    {
      inserted = true;
      break;
    }
  }

  if !inserted {
    let last_filter_idx = meta.n_filters - 1;
    let num_buckets = meta.sub_filter_num_buckets(last_filter_idx)?;
    inserted = try_kickout_insert(
      &mut page_cache,
      last_filter_idx,
      num_buckets,
      meta.bucket_size,
      meta.max_iterations,
      hash,
      fp,
    )
    .await?;
  }

  if !inserted && meta.is_scaling() && meta.n_filters < u16::MAX {
    let new_filter_idx = meta.n_filters;
    meta.n_filters += 1;
    let new_buckets = meta.sub_filter_num_buckets(new_filter_idx)?;
    let b1 = (hash % (new_buckets as u64)) as u32;
    let ok = page_cache
      .try_insert_in_bucket(new_filter_idx, new_buckets, b1, fp)
      .await?;
    if ok {
      inserted = true;
    }
  }

  if !inserted {
    return Err(Error::invalid_data("ERR filter is full"));
  }

  meta.base.size += 1;
  let mut entries = page_cache.collect_dirty_entries();
  entries.push(UpsertKV::insert(cf_meta_k, meta.encode().to_vec()));
  node.batch_write(BatchWriteReq { entries }).await?;

  Ok(true)
}

async fn cuckoo_exists(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  item: &[u8],
) -> Result<bool> {
  let cf_meta_k = kc.cf_meta(key);
  let meta_bytes = match node.read(GetKVReq { key: cf_meta_k }).await? {
    Some(b) => b,
    None => return Ok(false),
  };
  let meta = CuckooChainMeta::decode(&meta_bytes)
    .ok_or_else(|| Error::internal("corrupted cuckoo metadata"))?;

  let hash = CuckooFilterHelper::hash(item);
  let fp = CuckooFilterHelper::generate_fingerprint(hash);

  let mut page_cache = CuckooPageCache::new(node, kc, key, meta.bucket_size, meta.page_size);

  for filter_idx in 0..meta.n_filters {
    let num_buckets = meta.sub_filter_num_buckets(filter_idx)?;
    let b1 = (hash % (num_buckets as u64)) as u32;
    let b2 = CuckooFilterHelper::get_alt_bucket_index(b1, fp, num_buckets);

    if page_cache
      .contains_in_bucket(filter_idx, num_buckets, b1, fp)
      .await?
    {
      return Ok(true);
    }
    if b1 != b2
      && page_cache
        .contains_in_bucket(filter_idx, num_buckets, b2, fp)
        .await?
    {
      return Ok(true);
    }
  }

  Ok(false)
}

async fn cuckoo_del(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  item: &[u8],
) -> Result<bool> {
  let cf_meta_k = kc.cf_meta(key);
  let meta_bytes = match node
    .read(GetKVReq {
      key: cf_meta_k.clone(),
    })
    .await?
  {
    Some(b) => b,
    None => return Ok(false),
  };
  let mut meta = CuckooChainMeta::decode(&meta_bytes)
    .ok_or_else(|| Error::internal("corrupted cuckoo metadata"))?;

  let hash = CuckooFilterHelper::hash(item);
  let fp = CuckooFilterHelper::generate_fingerprint(hash);

  let mut page_cache = CuckooPageCache::new(node, kc, key, meta.bucket_size, meta.page_size);

  let mut deleted = false;
  for filter_idx in 0..meta.n_filters {
    let num_buckets = meta.sub_filter_num_buckets(filter_idx)?;
    let b1 = (hash % (num_buckets as u64)) as u32;
    let b2 = CuckooFilterHelper::get_alt_bucket_index(b1, fp, num_buckets);

    if page_cache
      .delete_from_bucket(filter_idx, num_buckets, b1, fp)
      .await?
    {
      deleted = true;
      break;
    }
    if b1 != b2
      && page_cache
        .delete_from_bucket(filter_idx, num_buckets, b2, fp)
        .await?
    {
      deleted = true;
      break;
    }
  }

  if deleted {
    meta.base.size = meta.base.size.saturating_sub(1);
    meta.num_deleted_items += 1;
    let mut entries = page_cache.collect_dirty_entries();
    entries.push(UpsertKV::insert(cf_meta_k, meta.encode().to_vec()));
    node.batch_write(BatchWriteReq { entries }).await?;
    Ok(true)
  } else {
    Ok(false)
  }
}

async fn cuckoo_count(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  item: &[u8],
) -> Result<usize> {
  let cf_meta_k = kc.cf_meta(key);
  let meta_bytes = match node.read(GetKVReq { key: cf_meta_k }).await? {
    Some(b) => b,
    None => return Ok(0),
  };
  let meta = CuckooChainMeta::decode(&meta_bytes)
    .ok_or_else(|| Error::internal("corrupted cuckoo metadata"))?;

  let hash = CuckooFilterHelper::hash(item);
  let fp = CuckooFilterHelper::generate_fingerprint(hash);

  let mut page_cache = CuckooPageCache::new(node, kc, key, meta.bucket_size, meta.page_size);

  let mut total_count = 0;
  for filter_idx in 0..meta.n_filters {
    let num_buckets = meta.sub_filter_num_buckets(filter_idx)?;
    let b1 = (hash % (num_buckets as u64)) as u32;
    let b2 = CuckooFilterHelper::get_alt_bucket_index(b1, fp, num_buckets);

    total_count += page_cache
      .count_in_bucket(filter_idx, num_buckets, b1, fp)
      .await?;
    if b1 != b2 {
      total_count += page_cache
        .count_in_bucket(filter_idx, num_buckets, b2, fp)
        .await?;
    }
  }

  Ok(total_count)
}

/// 布隆与布谷鸟过滤器命令主调度处理器
pub async fn handle_bloom(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::BfReserve {
      key,
      error_rate,
      capacity,
      expansion,
      nonscaling,
    } => {
      let bf_meta_k = kc.bf_meta(&key);
      if node
        .read(GetKVReq {
          key: bf_meta_k.clone(),
        })
        .await?
        .is_some()
      {
        return Err(Error::invalid_data("ERR item exists"));
      }
      let exp = if nonscaling { 0 } else { expansion };
      let bloom_bytes = BlockSplitBloomFilter::optimal_num_of_bytes(capacity, error_rate) as u32;
      let meta = BloomChainMeta::new(capacity, error_rate, exp, 0, 0, bloom_bytes);
      let entries = vec![
        UpsertKV::insert(bf_meta_k, meta.encode().to_vec()),
        UpsertKV::insert(kc.bf_item(&key, 0), vec![0u8; bloom_bytes as usize]),
      ];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::BfAdd(key, item) => {
      let results = bloom_insert_common(
        node,
        &kc,
        &key,
        &[item],
        &BloomFilterInsertOptions::default(),
      )
      .await?;
      match results.first() {
        Some(BloomFilterAddResult::Ok) => Ok(RespValue::Int(1)),
        Some(BloomFilterAddResult::Exist) => Ok(RespValue::Int(0)),
        Some(BloomFilterAddResult::Full) => {
          Err(Error::invalid_data("ERR nonscaling filter is full"))
        }
        None => Ok(RespValue::Int(0)),
      }
    }
    RedisCommand::BfMAdd(key, items) => {
      let results = bloom_insert_common(
        node,
        &kc,
        &key,
        &items,
        &BloomFilterInsertOptions::default(),
      )
      .await?;
      let mut list = Vec::with_capacity(results.len());
      for r in results {
        match r {
          BloomFilterAddResult::Ok => list.push(RespValue::Int(1)),
          BloomFilterAddResult::Exist => list.push(RespValue::Int(0)),
          BloomFilterAddResult::Full => {
            list.push(RespValue::error("ERR nonscaling filter is full"));
          }
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::BfInsert {
      key,
      capacity,
      error_rate,
      expansion,
      nocreate,
      nonscaling,
      items,
    } => {
      let opt = BloomFilterInsertOptions {
        capacity: capacity.unwrap_or(100),
        error_rate: error_rate.unwrap_or(0.01),
        expansion: if nonscaling {
          0
        } else {
          expansion.unwrap_or(2)
        },
        auto_create: !nocreate,
      };
      let results = bloom_insert_common(node, &kc, &key, &items, &opt).await?;
      let mut list = Vec::with_capacity(results.len());
      for r in results {
        match r {
          BloomFilterAddResult::Ok => list.push(RespValue::Int(1)),
          BloomFilterAddResult::Exist => list.push(RespValue::Int(0)),
          BloomFilterAddResult::Full => {
            list.push(RespValue::error("ERR nonscaling filter is full"));
          }
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::BfExists(key, item) => {
      let bf_meta_k = kc.bf_meta(&key);
      let meta_bytes = match node.read(GetKVReq { key: bf_meta_k }).await? {
        Some(b) => b,
        None => return Ok(RespValue::Int(0)),
      };
      let meta = BloomChainMeta::decode(&meta_bytes)
        .ok_or_else(|| Error::internal("corrupted bloom metadata"))?;
      let h = BlockSplitBloomFilter::hash(&item);
      for i in (0..meta.n_filters).rev() {
        let data = node
          .read(GetKVReq {
            key: kc.bf_item(&key, i),
          })
          .await?
          .unwrap_or_default();
        if BlockSplitBloomFilter::find_hash(&data, h) {
          return Ok(RespValue::Int(1));
        }
      }
      Ok(RespValue::Int(0))
    }
    RedisCommand::BfMExists(key, items) => {
      let bf_meta_k = kc.bf_meta(&key);
      let meta_bytes = match node.read(GetKVReq { key: bf_meta_k }).await? {
        Some(b) => b,
        None => return Ok(RespValue::Arr(vec![RespValue::Int(0); items.len()])),
      };
      let meta = BloomChainMeta::decode(&meta_bytes)
        .ok_or_else(|| Error::internal("corrupted bloom metadata"))?;
      let mut sub_filters = Vec::with_capacity(meta.n_filters as usize);
      for i in 0..meta.n_filters {
        let data = node
          .read(GetKVReq {
            key: kc.bf_item(&key, i),
          })
          .await?
          .unwrap_or_default();
        sub_filters.push(data);
      }
      let mut results = Vec::with_capacity(items.len());
      for item in items {
        let h = BlockSplitBloomFilter::hash(&item);
        let mut exists = false;
        for data in sub_filters.iter().rev() {
          if BlockSplitBloomFilter::find_hash(data, h) {
            exists = true;
            break;
          }
        }
        results.push(RespValue::Int(if exists { 1 } else { 0 }));
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::BfInfo { key, sub_cmd } => {
      let bf_meta_k = kc.bf_meta(&key);
      let meta_bytes = match node.read(GetKVReq { key: bf_meta_k }).await? {
        Some(b) => b,
        None => return Err(Error::invalid_data("ERR not found")),
      };
      let meta = BloomChainMeta::decode(&meta_bytes)
        .ok_or_else(|| Error::internal("corrupted bloom metadata"))?;
      match sub_cmd
        .as_deref()
        .map(|s| s.to_ascii_uppercase())
        .as_deref()
      {
        Some("CAPACITY") => Ok(RespValue::Int(meta.get_capacity() as i64)),
        Some("SIZE") => Ok(RespValue::Int(meta.bloom_bytes as i64)),
        Some("FILTERS") => Ok(RespValue::Int(meta.n_filters as i64)),
        Some("ITEMS") => Ok(RespValue::Int(meta.base.size as i64)),
        Some("EXPANSION") => Ok(if meta.expansion == 0 {
          RespValue::Null
        } else {
          RespValue::Int(meta.expansion as i64)
        }),
        None => {
          let info = vec![
            RespValue::Simple("Capacity".to_string()),
            RespValue::Int(meta.get_capacity() as i64),
            RespValue::Simple("Size".to_string()),
            RespValue::Int(meta.bloom_bytes as i64),
            RespValue::Simple("Number of filters".to_string()),
            RespValue::Int(meta.n_filters as i64),
            RespValue::Simple("Number of items inserted".to_string()),
            RespValue::Int(meta.base.size as i64),
            RespValue::Simple("Expansion rate".to_string()),
            if meta.expansion == 0 {
              RespValue::Null
            } else {
              RespValue::Int(meta.expansion as i64)
            },
          ];
          Ok(RespValue::Arr(info))
        }
        Some(_) => Err(Error::invalid_data("Invalid info argument")),
      }
    }
    RedisCommand::BfCard(key) => {
      let bf_meta_k = kc.bf_meta(&key);
      let meta_bytes = match node.read(GetKVReq { key: bf_meta_k }).await? {
        Some(b) => b,
        None => return Ok(RespValue::Int(0)),
      };
      let meta = BloomChainMeta::decode(&meta_bytes)
        .ok_or_else(|| Error::internal("corrupted bloom metadata"))?;
      Ok(RespValue::Int(meta.base.size as i64))
    }
    RedisCommand::CfReserve {
      key,
      capacity,
      bucket_size,
      max_iterations,
      expansion,
    } => {
      let cf_meta_k = kc.cf_meta(&key);
      if node
        .read(GetKVReq {
          key: cf_meta_k.clone(),
        })
        .await?
        .is_some()
      {
        return Err(Error::invalid_data("ERR item exists"));
      }
      let bs = bucket_size.unwrap_or(2);
      let mi = max_iterations.unwrap_or(20);
      let exp = expansion
        .map(CuckooFilterHelper::normalize_expansion)
        .unwrap_or(1);
      let meta = CuckooChainMeta::new(
        capacity,
        bs,
        mi,
        exp,
        CuckooFilterHelper::DEFAULT_PAGE_SIZE,
        0,
        0,
      );
      let entries = vec![UpsertKV::insert(cf_meta_k, meta.encode().to_vec())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::CfAdd(key, item) => {
      cuckoo_add_single(node, &kc, &key, &item, 1024).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::CfAddNx(key, item) => {
      let exists = cuckoo_exists(node, &kc, &key, &item).await?;
      if exists {
        Ok(RespValue::Int(0))
      } else {
        cuckoo_add_single(node, &kc, &key, &item, 1024).await?;
        Ok(RespValue::Int(1))
      }
    }
    RedisCommand::CfInsert {
      key,
      capacity,
      nocreate,
      items,
    } => {
      let cf_meta_k = kc.cf_meta(&key);
      let default_cap = capacity.unwrap_or(1024);
      if nocreate && node.read(GetKVReq { key: cf_meta_k }).await?.is_none() {
        return Err(Error::invalid_data("ERR not found"));
      }
      let mut list = Vec::with_capacity(items.len());
      for item in items {
        match cuckoo_add_single(node, &kc, &key, &item, default_cap).await {
          Ok(_) => list.push(RespValue::Int(1)),
          Err(e) => list.push(RespValue::error(e.to_string())),
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::CfInsertNx {
      key,
      capacity,
      nocreate,
      items,
    } => {
      let cf_meta_k = kc.cf_meta(&key);
      let default_cap = capacity.unwrap_or(1024);
      if nocreate && node.read(GetKVReq { key: cf_meta_k }).await?.is_none() {
        return Err(Error::invalid_data("ERR not found"));
      }
      let mut list = Vec::with_capacity(items.len());
      for item in items {
        let exists = cuckoo_exists(node, &kc, &key, &item).await?;
        if exists {
          list.push(RespValue::Int(0));
        } else {
          match cuckoo_add_single(node, &kc, &key, &item, default_cap).await {
            Ok(_) => list.push(RespValue::Int(1)),
            Err(e) => list.push(RespValue::error(e.to_string())),
          }
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::CfExists(key, item) => {
      let exists = cuckoo_exists(node, &kc, &key, &item).await?;
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    RedisCommand::CfMExists(key, items) => {
      let mut list = Vec::with_capacity(items.len());
      for item in items {
        let exists = cuckoo_exists(node, &kc, &key, &item).await?;
        list.push(RespValue::Int(if exists { 1 } else { 0 }));
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::CfDel(key, item) => {
      let deleted = cuckoo_del(node, &kc, &key, &item).await?;
      Ok(RespValue::Int(if deleted { 1 } else { 0 }))
    }
    RedisCommand::CfCount(key, item) => {
      let count = cuckoo_count(node, &kc, &key, &item).await?;
      Ok(RespValue::Int(count as i64))
    }
    RedisCommand::CfInfo(key) => {
      let cf_meta_k = kc.cf_meta(&key);
      let meta_bytes = match node.read(GetKVReq { key: cf_meta_k }).await? {
        Some(b) => b,
        None => return Err(Error::invalid_data("ERR not found")),
      };
      let meta = CuckooChainMeta::decode(&meta_bytes)
        .ok_or_else(|| Error::internal("corrupted cuckoo metadata"))?;
      let mut total_buckets = 0u32;
      let mut total_bytes = 0u32;
      for i in 0..meta.n_filters {
        let buckets = meta.sub_filter_num_buckets(i)?;
        total_buckets += buckets;
        let buckets_per_page = (meta.page_size / meta.bucket_size as u32).max(1);
        let num_pages = buckets.div_ceil(buckets_per_page);
        total_bytes += num_pages * meta.page_size;
      }
      let info = vec![
        RespValue::Simple("Size".to_string()),
        RespValue::Int(total_bytes as i64),
        RespValue::Simple("Number of buckets".to_string()),
        RespValue::Int(total_buckets as i64),
        RespValue::Simple("Number of filters".to_string()),
        RespValue::Int(meta.n_filters as i64),
        RespValue::Simple("Number of items inserted".to_string()),
        RespValue::Int(meta.base.size as i64),
        RespValue::Simple("Number of items deleted".to_string()),
        RespValue::Int(meta.num_deleted_items as i64),
        RespValue::Simple("Bucket size".to_string()),
        RespValue::Int(meta.bucket_size as i64),
        RespValue::Simple("Expansion rate".to_string()),
        if meta.expansion == 0 {
          RespValue::Null
        } else {
          RespValue::Int(meta.expansion as i64)
        },
        RespValue::Simple("Max iterations".to_string()),
        RespValue::Int(meta.max_iterations as i64),
      ];
      Ok(RespValue::Arr(info))
    }
    _ => Err(Error::internal("unsupported bloom/cuckoo command")),
  }
}
