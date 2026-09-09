//! WATCH 包装 API（对标 libs/server/API/GarnetWatchApi.cs:GarnetWatchApi）
//!
//! C# 侧 GarnetWatchApi\<TGarnetApi\> 在每个读操作前先 WATCH(key, StoreType)；
//! Rust 侧以 [`GarnetWatchApi`] 关联函数实现同序：先登记 [`StorageSession`]
//! 监视表（写日志尾地址版本代理，见 storage_session 域注释），再委托读操作。

use wdev::Device;

use crate::{
  api::{garnet_status::GarnetStatus, hash_fields, hash_or_set_members, set_members},
  storage::session::{
    mainstore::main_store_ops::LcsResult,
    objectstore::{
      common::{OBJ_TAG_HASH, OBJ_TAG_SET},
      sorted_set_geo_ops::{GeoCenter, GeoCmd},
      sorted_set_ops::ZSetAggregate,
    },
    storage_session::StorageSession,
  },
};

/// WATCH 包装 API
pub struct GarnetWatchApi;

impl GarnetWatchApi {
  /// libs/server/API/GarnetWatchApi.cs:GETForMemoryResult
  pub async fn get_for_memory_result<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.watch_key(key);
    ss.read_main_store(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:LCS
  pub async fn lcs<D: Device>(
    ss: &StorageSession<'_, D>,
    key1: &[u8],
    key2: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<LcsResult>)> {
    ss.watch_key(key1);
    ss.watch_key(key2);
    ss.lcs(key1, key2).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GETRANGE
  pub async fn getrange<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    end: i64,
  ) -> wkv::Result<Vec<u8>> {
    ss.watch_key(key);
    ss.getrange(key, start, end).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetLength
  pub async fn sorted_set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.watch_key(key);
    ss.sorted_set_length(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetCount
  pub async fn sorted_set_count<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.watch_key(key);
    ss.sorted_set_count(key, min, max).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetLengthByValue
  pub async fn sorted_set_length_by_value<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.watch_key(key);
    ss.sorted_set_length_by_value(key, min, max).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetRandomMember
  pub async fn sorted_set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    ss.watch_key(key);
    ss.sorted_set_random_member(key, count, with_scores).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetRange
  pub async fn sorted_set_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    ss.watch_key(key);
    ss.sorted_set_range(key, start, stop, rev, with_scores)
      .await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetScore
  pub async fn sorted_set_score<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.watch_key(key);
    ss.sorted_set_score(key, member).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetScores
  pub async fn sorted_set_scores<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<f64>>)> {
    ss.watch_key(key);
    ss.sorted_set_scores(key, members).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetRank
  pub async fn sorted_set_rank<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, Option<i64>)> {
    ss.watch_key(key);
    ss.sorted_set_rank(key, member, rev).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetDifference
  pub async fn sorted_set_difference<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.sorted_set_difference(keys).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetUnion
  pub async fn sorted_set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.sorted_set_union(keys, weights, aggregate).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GeoCommands
  pub async fn geo_commands<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cmd: GeoCmd<'_>,
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.watch_key(key);
    ss.geo_commands(key, cmd).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GeoSearchReadOnly
  pub async fn geo_search_read_only<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.watch_key(key);
    ss.geo_search_read_only(key, center, radius_m).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetScan
  pub async fn sorted_set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.sorted_set_scan(key, cursor, pattern, count).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetIntersect
  pub async fn sorted_set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.sorted_set_intersect(keys, weights, aggregate).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetIntersectLength
  pub async fn sorted_set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.sorted_set_intersect_length(keys, weights, aggregate)
      .await
  }

  /// libs/server/API/GarnetWatchApi.cs:SortedSetTimeToLive
  pub async fn sorted_set_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.watch_key(key);
    ss.sorted_set_time_to_live(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:ListLength
  pub async fn list_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.watch_key(key);
    ss.list_length(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:ListRange
  pub async fn list_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.list_range(key, start, stop).await
  }

  /// libs/server/API/GarnetWatchApi.cs:ListIndex
  pub async fn list_index<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    index: i64,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.watch_key(key);
    ss.list_index(key, index).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetLength
  pub async fn set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.watch_key(key);
    ss.set_length(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetMembers
  pub async fn set_members<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.set_members(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetIsMember
  pub async fn set_is_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.watch_key(key);
    ss.set_is_member(key, member).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetScan
  pub async fn set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.object_scan(key, OBJ_TAG_SET, pattern, cursor, count, set_members)
      .await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetUnion
  pub async fn set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.set_union(keys).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetIntersect
  pub async fn set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.set_intersect(keys).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetDiff
  pub async fn set_diff<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.set_diff(keys).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetIntersectLength
  pub async fn set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    for k in keys {
      ss.watch_key(k);
    }
    ss.set_intersect_length(keys).await
  }

  /// libs/server/API/GarnetWatchApi.cs:SetRandomMember
  pub async fn set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.set_random_member(key, count).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashGet
  pub async fn hash_get<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.watch_key(key);
    ss.hash_get(key, field).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashGetMultiple
  pub async fn hash_get_multiple<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.watch_key(key);
    ss.hash_get_multiple(key, fields).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashGetAll
  pub async fn hash_get_all<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Vec<u8>)>)> {
    ss.watch_key(key);
    ss.hash_get_all(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashLength
  pub async fn hash_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.watch_key(key);
    ss.hash_length(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashExists
  pub async fn hash_exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.watch_key(key);
    ss.hash_exists(key, field).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashStrLength
  pub async fn hash_str_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.watch_key(key);
    ss.hash_str_length(key, field).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashRandomField
  pub async fn hash_random_field<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_values: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    ss.watch_key(key);
    ss.hash_random_field(key, count, with_values).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashKeys
  pub async fn hash_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.hash_keys(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashVals
  pub async fn hash_vals<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.hash_vals(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashScan
  pub async fn hash_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.object_scan(key, OBJ_TAG_HASH, pattern, cursor, count, hash_fields)
      .await
  }

  /// libs/server/API/GarnetWatchApi.cs:HashTimeToLive
  pub async fn hash_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.watch_key(key);
    ss.hash_time_to_live(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GET
  pub async fn get<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.watch_key(key);
    ss.read_main_store(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:TTL
  pub async fn ttl<D: Device>(ss: &StorageSession<'_, D>, key: &[u8]) -> wkv::Result<Option<i64>> {
    ss.watch_key(key);
    ss.handle_ttl(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:EXPIRETIME
  pub async fn expiretime<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<Option<i64>> {
    ss.watch_key(key);
    ss.handle_expire_time(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:DbScan
  pub async fn db_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    pattern: &[u8],
    all_keys: bool,
    cursor: &[u8],
    count: usize,
  ) -> wkv::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    ss.db_scan(pattern, all_keys, cursor, count).await
  }

  /// libs/server/API/GarnetWatchApi.cs:DeleteSlotKeys
  pub async fn delete_slot_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    slots: &[u16],
  ) -> wkv::Result<u64> {
    ss.delete_slot_keys(slots).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GetDbKeys
  pub async fn get_db_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    pattern: &[u8],
  ) -> wkv::Result<Vec<Vec<u8>>> {
    ss.db_keys(pattern).await
  }

  /// libs/server/API/GarnetWatchApi.cs:GetDbSize
  pub async fn get_db_size<D: Device>(ss: &StorageSession<'_, D>) -> wkv::Result<usize> {
    ss.db_size().await
  }

  /// libs/server/API/GarnetWatchApi.cs:HyperLogLogLength
  pub async fn hyper_log_log_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, f64)> {
    ss.watch_key(key);
    ss.hyper_log_log_length(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:IterateStore
  pub async fn iterate_store<D: Device>(
    ss: &StorageSession<'_, D>,
    on_record: impl FnMut(&[u8], &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    ss.iterate_store(on_record).await
  }

  /// libs/server/API/GarnetWatchApi.cs:ObjectScan
  pub async fn object_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    tag: u8,
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.watch_key(key);
    ss.object_scan(key, tag, pattern, cursor, count, hash_or_set_members)
      .await
  }

  /// libs/server/API/GarnetWatchApi.cs:ResetScratchBuffer
  pub fn reset_scratch_buffer() {
    // Rust 输出缓冲随作用域回收，无共享 scratch 需要重置
  }

  /// libs/server/API/GarnetWatchApi.cs:StringBitCount
  pub async fn string_bit_count<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, u64)> {
    ss.watch_key(key);
    ss.string_bit_count(key, start, end, bit_mode).await
  }

  /// libs/server/API/GarnetWatchApi.cs:StringBitFieldReadOnly
  pub async fn string_bit_field_read_only<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    gets: &[(bool, u8, u64)],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    ss.watch_key(key);
    ss.string_bit_field_read_only(key, gets).await
  }

  /// libs/server/API/GarnetWatchApi.cs:StringBitPosition
  pub async fn string_bit_position<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    bit: u8,
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.watch_key(key);
    ss.string_bit_position(key, bit, start, end, bit_mode).await
  }

  /// libs/server/API/GarnetWatchApi.cs:StringGetBit
  pub async fn string_get_bit<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    offset: u64,
  ) -> wkv::Result<(GarnetStatus, u8)> {
    ss.watch_key(key);
    ss.string_get_bit(key, offset).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetCardinality
  pub async fn vector_set_cardinality<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_cardinality(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetDimensions
  pub async fn vector_set_dimensions<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_dimensions(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetElementSimilarity
  pub async fn vector_set_element_similarity<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_element_similarity(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetEmbedding
  pub async fn vector_set_embedding<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_embedding(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetGetAttribute
  pub async fn vector_set_get_attribute<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_get_attribute(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetInfo
  pub async fn vector_set_info<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_info(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetIsMember
  pub async fn vector_set_is_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_is_member(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetLinks
  pub async fn vector_set_links<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_links(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetRandomMembers
  pub async fn vector_set_random_members<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_random_members(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetRawEmbedding
  pub async fn vector_set_raw_embedding<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_raw_embedding(key).await
  }

  /// libs/server/API/GarnetWatchApi.cs:VectorSetValueSimilarity
  pub async fn vector_set_value_similarity<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.watch_key(key);
    ss.vector_set_value_similarity(key).await
  }
}
