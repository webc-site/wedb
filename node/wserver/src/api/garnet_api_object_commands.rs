//! 对象命令 API 实现（对标 libs/server/API/GarnetApiObjectCommands.cs，C# 为 GarnetApi partial）
//!
//! C# 侧该 partial 实现 IGarnetApi 的对象族方法；Rust 侧以
//! [`GarnetApiObjectCommands`] 关联函数统一委托对象存操作面。

use wdev::Device;
use wobject::list::list_object::OperationDirection;

use crate::{
  api::{garnet_status::GarnetStatus, hash_fields, set_members},
  storage::session::{
    objectstore::{
      common::{OBJ_TAG_HASH, OBJ_TAG_SET},
      sorted_set_geo_ops::{GeoCenter, GeoCmd},
      sorted_set_ops::{ZSetAggregate, ZSetRemoveRange},
    },
    storage_session::StorageSession,
  },
};

/// 对象命令 API 实现
pub struct GarnetApiObjectCommands;

impl GarnetApiObjectCommands {
  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetAdd
  pub async fn sorted_set_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[(&[u8], f64)],
    nx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_add(key, members, nx, gt, lt, ch).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRangeStore
  pub async fn sorted_set_range_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    src: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_range_store(dest, src, start, stop, rev).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRemove
  pub async fn sorted_set_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove(key, members).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetLength
  pub async fn sorted_set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_length(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRange
  pub async fn sorted_set_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    ss.sorted_set_range(key, start, stop, rev, with_scores)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetScore
  pub async fn sorted_set_score<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.sorted_set_score(key, member).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetScores
  pub async fn sorted_set_scores<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<f64>>)> {
    ss.sorted_set_scores(key, members).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetPop
  pub async fn sorted_set_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_pop(key, count, min).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetMPop
  pub async fn sorted_set_m_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Option<(Vec<u8>, Vec<(Vec<u8>, f64)>)>)> {
    ss.sorted_set_m_pop(keys, count, min).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetCount
  pub async fn sorted_set_count<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_count(key, min, max).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetLengthByValue
  pub async fn sorted_set_length_by_value<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_length_by_value(key, min, max).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRemoveRangeByLex
  pub async fn sorted_set_remove_range_by_lex<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_lex(key, min, max).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRemoveRangeByScore
  pub async fn sorted_set_remove_range_by_score<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_score(key, min, max).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRemoveRangeByRank
  pub async fn sorted_set_remove_range_by_rank<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_rank(key, start, stop).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetIncrement
  pub async fn sorted_set_increment<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.sorted_set_increment(key, member, delta).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRank
  pub async fn sorted_set_rank<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, Option<i64>)> {
    ss.sorted_set_rank(key, member, rev).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRandomMember
  pub async fn sorted_set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    ss.sorted_set_random_member(key, count, with_scores).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetDifference
  pub async fn sorted_set_difference<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_difference(keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetDifferenceStore
  pub async fn sorted_set_difference_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_difference_store(dest, keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetUnion
  pub async fn sorted_set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_union(keys, weights, aggregate).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetUnionStore
  pub async fn sorted_set_union_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_union_store(dest, keys, weights, aggregate)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetScan
  pub async fn sorted_set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.sorted_set_scan(key, cursor, pattern, count).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetIntersect
  pub async fn sorted_set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_intersect(keys, weights, aggregate).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetIntersectStore
  pub async fn sorted_set_intersect_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_intersect_store(dest, keys, weights, aggregate)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetIntersectLength
  pub async fn sorted_set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_intersect_length(keys, weights, aggregate)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetIntersection
  pub async fn sorted_set_intersection<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_intersection(keys, weights, aggregate).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetRemoveRange
  pub async fn sorted_set_remove_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    range: ZSetRemoveRange<'_>,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range(key, range).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetExpire
  pub async fn sorted_set_expire<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    ttl_ms: u64,
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.sorted_set_expire(key, ttl_ms).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetTimeToLive
  pub async fn sorted_set_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_time_to_live(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetPersist
  pub async fn sorted_set_persist<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.sorted_set_persist(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SortedSetCollect
  pub async fn sorted_set_collect<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<usize> {
    ss.sorted_set_collect(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetAdd
  pub async fn set_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.set_add(key, members).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetRemove
  pub async fn set_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.set_remove(key, members).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetLength
  pub async fn set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_length(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetMembers
  pub async fn set_members<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_members(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetPop
  pub async fn set_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_pop(key, count).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetMove
  pub async fn set_move<D: Device>(
    ss: &StorageSession<'_, D>,
    src: &[u8],
    dest: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.set_move(src, dest, member).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetRandomMember
  pub async fn set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_random_member(key, count).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetIsMember
  pub async fn set_is_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.set_is_member(key, member).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetScan
  pub async fn set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.object_scan(key, OBJ_TAG_SET, pattern, cursor, count, set_members)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetUnion
  pub async fn set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_union(keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetIntersect
  pub async fn set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_intersect(keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetDiff
  pub async fn set_diff<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_diff(keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetIntersectStore
  pub async fn set_intersect_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_intersect_store(dest, keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetUnionStore
  pub async fn set_union_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_union_store(dest, keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetDiffStore
  pub async fn set_diff_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_diff_store(dest, keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:SetIntersectLength
  pub async fn set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_intersect_length(keys).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListPush
  pub async fn list_push<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    values: &[&[u8]],
    direction: OperationDirection,
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_push(key, values, direction, only_if_exists).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListLeftPush
  pub async fn list_left_push<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    values: &[&[u8]],
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_push(key, values, OperationDirection::Left, only_if_exists)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListRightPush
  pub async fn list_right_push<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    values: &[&[u8]],
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_push(key, values, OperationDirection::Right, only_if_exists)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListLeftPop
  pub async fn list_left_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_pop(key, OperationDirection::Left).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListRightPop
  pub async fn list_right_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_pop(key, OperationDirection::Right).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListPop
  pub async fn list_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    direction: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_pop(key, direction).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListPopMultiple
  pub async fn list_pop_multiple<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: usize,
    direction: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.list_pop_multiple(key, count, direction).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListLength
  pub async fn list_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.list_length(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListMove
  pub async fn list_move<D: Device>(
    ss: &StorageSession<'_, D>,
    src: &[u8],
    dest: &[u8],
    src_dir: OperationDirection,
    dest_dir: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_move(src, dest, src_dir, dest_dir).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListTrim
  pub async fn list_trim<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<GarnetStatus> {
    ss.list_trim(key, start, stop).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListPosition
  pub async fn list_position<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    element: &[u8],
    rank: i64,
    maxlen: Option<usize>,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_position(key, element, rank, maxlen).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListRange
  pub async fn list_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.list_range(key, start, stop).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListInsert
  pub async fn list_insert<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    pivot: &[u8],
    element: &[u8],
    before: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_insert(key, pivot, element, before).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListIndex
  pub async fn list_index<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    index: i64,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_index(key, index).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListRemove
  pub async fn list_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    element: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.list_remove(key, element, count).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:ListSet
  pub async fn list_set<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    index: i64,
    element: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.list_set(key, index, element).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashSet
  pub async fn hash_set<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_set(key, fields, nx).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashSetWhenNotExists
  pub async fn hash_set_when_not_exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_set(key, fields, true).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashDelete
  pub async fn hash_delete<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_delete(key, fields).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashGet
  pub async fn hash_get<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.hash_get(key, field).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashGetMultiple
  pub async fn hash_get_multiple<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.hash_get_multiple(key, fields).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashGetAll
  pub async fn hash_get_all<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Vec<u8>)>)> {
    ss.hash_get_all(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashLength
  pub async fn hash_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.hash_length(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashStrLength
  pub async fn hash_str_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.hash_str_length(key, field).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashExists
  pub async fn hash_exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.hash_exists(key, field).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashIncrement
  pub async fn hash_increment<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
    delta: &[u8],
    float: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.hash_increment(key, field, delta, float).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashRandomField
  pub async fn hash_random_field<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_values: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    ss.hash_random_field(key, count, with_values).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashKeys
  pub async fn hash_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.hash_keys(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashVals
  pub async fn hash_vals<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.hash_vals(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashScan
  pub async fn hash_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.object_scan(key, OBJ_TAG_HASH, pattern, cursor, count, hash_fields)
      .await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:HashTimeToLive
  pub async fn hash_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_time_to_live(key).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:GeoAdd
  pub async fn geo_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    items: &[(f64, f64, &[u8])],
    nx: bool,
    ch: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.geo_add(key, items, nx, ch).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:GeoCommands
  pub async fn geo_commands<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cmd: GeoCmd<'_>,
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.geo_commands(key, cmd).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:GeoSearchReadOnly
  pub async fn geo_search_read_only<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.geo_search_read_only(key, center, radius_m).await
  }

  /// libs/server/API/GarnetApiObjectCommands.cs:GeoSearchStore
  pub async fn geo_search_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    src: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.geo_search_store(dest, src, center, radius_m).await
  }
}
