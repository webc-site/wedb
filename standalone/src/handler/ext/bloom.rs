use crate::handler::resp_util::bools_to_arr;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::bloom::{
  DEFAULT_BF_CAPACITY, DEFAULT_BF_ERROR_RATE, DEFAULT_BF_EXPANSION, DEFAULT_CF_CAPACITY,
};
use wedb_embed::{
  BloomFilterAddResult, BloomFilterInsertOptions, CuckooFilterInsertOptions, Error, Result, WeDb,
};
use wedb_resp::RespValue;

/// 处理所有 Bloom & Cuckoo Filter (布隆与布谷鸟过滤器) 命令
pub async fn handle_bloom(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::BfReserve {
      key,
      error_rate,
      capacity,
      expansion,
      ..
    } => {
      db.bf_reserve(key.as_bytes(), error_rate, capacity, expansion)?;
      Ok(RespValue::ok())
    }
    Cmd::BfAdd(key, item) => {
      let added = db.bf_add(key.as_bytes(), &item)?;
      Ok(RespValue::Int(if added { 1 } else { 0 }))
    }
    Cmd::BfMAdd(key, items) => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let results = db.bf_madd(key.as_bytes(), &item_bytes)?;
      Ok(bools_to_arr(results))
    }
    Cmd::BfInsert {
      key,
      items,
      capacity,
      error_rate,
      expansion,
      nocreate,
      nonscaling: _,
    } => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let opts = BloomFilterInsertOptions {
        capacity: capacity.unwrap_or(DEFAULT_BF_CAPACITY),
        error_rate: error_rate.unwrap_or(DEFAULT_BF_ERROR_RATE),
        expansion: expansion.unwrap_or(DEFAULT_BF_EXPANSION),
        auto_create: !nocreate,
      };
      let results = db.bf_insert(key.as_bytes(), &item_bytes, &opts)?;
      let arr = results
        .into_iter()
        .map(|r| {
          RespValue::Int(if matches!(r, BloomFilterAddResult::Ok) {
            1
          } else {
            0
          })
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::BfExists(key, item) => {
      let exists = db.bf_exists(key.as_bytes(), &item)?;
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    Cmd::BfMExists(key, items) => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let results = db.bf_mexists(key.as_bytes(), &item_bytes)?;
      Ok(bools_to_arr(results))
    }
    Cmd::BfInfo { key, .. } => {
      let info = db.bf_info(key.as_bytes())?;
      Ok(RespValue::Arr(vec![
        RespValue::Simple("Capacity".to_string()),
        RespValue::Int(info.capacity as i64),
        RespValue::Simple("Size".to_string()),
        RespValue::Int(info.size as i64),
        RespValue::Simple("Number of filters".to_string()),
        RespValue::Int(info.n_filters as i64),
        RespValue::Simple("Number of items inserted".to_string()),
        RespValue::Int(info.bloom_bytes as i64),
        RespValue::Simple("Expansion rate".to_string()),
        RespValue::Int(info.expansion as i64),
      ]))
    }
    Cmd::BfCard(key) => {
      let val = db.bf_card(key.as_bytes())?;
      Ok(RespValue::Int(val as i64))
    }
    Cmd::CfReserve {
      key,
      capacity,
      bucket_size,
      max_iterations,
      expansion,
    } => {
      db.cf_reserve_ext(
        key.as_bytes(),
        capacity,
        bucket_size.unwrap_or(2),
        max_iterations.unwrap_or(20),
        expansion.unwrap_or(1),
        1024,
      )?;
      Ok(RespValue::ok())
    }
    Cmd::CfAdd(key, item) => {
      db.cf_add(key.as_bytes(), &item)?;
      Ok(RespValue::Int(1))
    }
    Cmd::CfAddNx(key, item) => {
      let added = db.cf_addnx(key.as_bytes(), &item)?;
      Ok(RespValue::Int(if added { 1 } else { 0 }))
    }
    Cmd::CfInsert {
      key,
      items,
      capacity,
      nocreate,
    } => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let opts = CuckooFilterInsertOptions {
        capacity: capacity.unwrap_or(DEFAULT_CF_CAPACITY),
        auto_create: !nocreate,
        nx: false,
        ..Default::default()
      };
      let results = db.cf_insert(key.as_bytes(), &item_bytes, &opts)?;
      Ok(bools_to_arr(results))
    }
    Cmd::CfInsertNx {
      key,
      items,
      capacity,
      nocreate,
    } => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let opts = CuckooFilterInsertOptions {
        capacity: capacity.unwrap_or(DEFAULT_CF_CAPACITY),
        auto_create: !nocreate,
        nx: true,
        ..Default::default()
      };
      let results = db.cf_insert(key.as_bytes(), &item_bytes, &opts)?;
      Ok(bools_to_arr(results))
    }
    Cmd::CfExists(key, item) => {
      let exists = db.cf_exists(key.as_bytes(), &item)?;
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    Cmd::CfMExists(key, items) => {
      let item_bytes: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      let results = db.cf_mexists(key.as_bytes(), &item_bytes)?;
      Ok(bools_to_arr(results))
    }
    Cmd::CfDel(key, item) => {
      let deleted = db.cf_del(key.as_bytes(), &item)?;
      Ok(RespValue::Int(if deleted { 1 } else { 0 }))
    }
    Cmd::CfCount(key, item) => {
      let count = db.cf_count(key.as_bytes(), &item)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::CfInfo(key) => {
      let info = db.cf_info(key.as_bytes())?;
      Ok(RespValue::Arr(vec![
        RespValue::Simple("Size".to_string()),
        RespValue::Int(info.size as i64),
        RespValue::Simple("Number of buckets".to_string()),
        RespValue::Int(info.num_buckets as i64),
        RespValue::Simple("Number of filters".to_string()),
        RespValue::Int(info.num_filters as i64),
        RespValue::Simple("Number of items inserted".to_string()),
        RespValue::Int(info.num_items_inserted as i64),
        RespValue::Simple("Number of items deleted".to_string()),
        RespValue::Int(info.num_items_deleted as i64),
        RespValue::Simple("Bucket size".to_string()),
        RespValue::Int(info.bucket_size as i64),
        RespValue::Simple("Expansion rate".to_string()),
        RespValue::Int(info.expansion as i64),
        RespValue::Simple("Max iterations".to_string()),
        RespValue::Int(info.max_iterations as i64),
      ]))
    }
    _ => Err(Error::internal("unsupported bloom command")),
  }
}
