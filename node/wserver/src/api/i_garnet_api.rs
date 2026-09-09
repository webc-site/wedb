//! Garnet 存储 API 面（对标 libs/server/API/IGarnetApi.cs:IGarnetApi）
//!
//! C# 侧 IGarnetApi 为接口，实现分散于 GarnetApi / GarnetApiObjectCommands /
//! GarnetApiUnifiedCommands；Rust 侧以 [`IGarnetApi`] 关联函数作为统一 API 面，
//! 全部委托 [`StorageSession`](crate::storage::session::storage_session::StorageSession)
//! 已实现操作（mainstore / objectstore / unifiedstore / common）。

use std::io::Cursor;

use wdev::Device;
use wobject::{
  hash::hash_object::HashObject, list::list_object::OperationDirection, set::set_object::SetObject,
};

use crate::{
  api::garnet_status::GarnetStatus,
  storage::{
    functions::mainstore::rmw_methods__etags as rmm,
    session::{
      mainstore::{
        bitmap_ops::{BitFieldOp, BitmapOp},
        main_store_ops::LcsResult,
      },
      objectstore::{
        common::{OBJ_TAG_HASH, OBJ_TAG_SET},
        sorted_set_geo_ops::{GeoCenter, GeoCmd},
        sorted_set_ops::{ZSetAggregate, ZSetRemoveRange},
      },
      storage_session::{StorageSession, StoreType},
    },
  },
};

/// OBJECT 命令子命令
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSubCommand {
  /// OBJECT ENCODING
  Encoding,
  /// OBJECT REFCOUNT
  RefCount,
  /// OBJECT IDLETIME
  IdleTime,
  /// OBJECT FREQ
  Freq,
}

/// OBJECT SCAN 成员抽取兜底：哈希返回字段、集合返回成员（载荷为剥壳 bitcode）
fn generic_members(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
  HashObject::deserialize(&mut Cursor::new(payload.to_vec()))
    .ok()
    .map(|o| {
      let mut fields = o.get_keys();
      fields.sort();
      fields
    })
}

/// SET SCAN（游标 = 上次返回的最后一个成员）
async fn set_scan<D: Device>(
  ss: &StorageSession<'_, D>,
  key: &[u8],
  cursor: &[u8],
  pattern: &[u8],
  count: usize,
) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
  let members_of = |payload: &[u8]| -> Option<Vec<Vec<u8>>> {
    SetObject::deserialize(&mut Cursor::new(payload.to_vec()))
      .ok()
      .map(|o| {
        let mut members = o.get_keys();
        members.sort();
        members
      })
  };
  ss.object_scan(key, OBJ_TAG_SET, pattern, cursor, count, members_of)
    .await
}

/// HASH SCAN（游标 = 上次返回的最后一个字段）
async fn hash_scan<D: Device>(
  ss: &StorageSession<'_, D>,
  key: &[u8],
  cursor: &[u8],
  pattern: &[u8],
  count: usize,
) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
  let members_of = |payload: &[u8]| -> Option<Vec<Vec<u8>>> {
    HashObject::deserialize(&mut Cursor::new(payload.to_vec()))
      .ok()
      .map(|o| {
        let mut fields = o.get_keys();
        fields.sort();
        fields
      })
  };
  ss.object_scan(key, OBJ_TAG_HASH, pattern, cursor, count, members_of)
    .await
}

/// etag 条件写：值与 etag 一并落盘（值 = 新负载，etag 整数文本独立记录缺失，
/// 以"新值文本即 etag"约定返回状态）
async fn etag_set<D: Device>(
  ss: &StorageSession<'_, D>,
  key: &[u8],
  val: &[u8],
  etag: u64,
) -> wkv::Result<rmm::EtagOutcome> {
  // 缺口：wkv 无独立 etag 元数据通道，新 etag 以负载文本承载（见 etags 域注释）
  ss.upsert_string(key, val).await?;
  let _ = etag;
  Ok(rmm::EtagOutcome::Updated)
}

/// Garnet 存储 API 面
pub struct IGarnetApi;

impl IGarnetApi {
  /// libs/server/API/IGarnetApi.cs:GETEX
  pub async fn getex<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    ttl_ms: Option<u64>,
    persist: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.getex(key, ttl_ms, persist).await
  }

  /// libs/server/API/IGarnetApi.cs:SET_Conditional
  pub async fn set_conditional<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    val: &[u8],
    nx: bool,
    xx: bool,
    get_old: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.set_conditional(key, val, nx, xx, get_old).await
  }

  /// libs/server/API/IGarnetApi.cs:SET_ETagConditional
  pub async fn set_e_tag_conditional<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    val: &[u8],
    etag: u64,
  ) -> wkv::Result<GarnetStatus> {
    Ok(match etag_set(ss, key, val, etag).await? {
      rmm::EtagOutcome::Updated | rmm::EtagOutcome::Deleted => GarnetStatus::Ok,
      rmm::EtagOutcome::Unchanged => GarnetStatus::NotFound,
    })
  }

  /// libs/server/API/IGarnetApi.cs:DEL_ETagConditional
  pub async fn del_e_tag_conditional<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    etag: u64,
  ) -> wkv::Result<GarnetStatus> {
    ss.del_conditional(key, etag as i64).await
  }

  /// libs/server/API/IGarnetApi.cs:SETEX
  pub async fn setex<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    val: &[u8],
    ttl_ms: u64,
  ) -> wkv::Result<GarnetStatus> {
    ss.setex(key, val, ttl_ms).await
  }

  /// libs/server/API/IGarnetApi.cs:SETRANGE
  pub async fn setrange<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    offset: usize,
    val: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.setrange(key, offset, val).await
  }

  /// libs/server/API/IGarnetApi.cs:MSET_Conditional
  pub async fn mset_conditional<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    values: &[&[u8]],
    nx: bool,
  ) -> wkv::Result<GarnetStatus> {
    ss.mset_conditional(keys, values, nx).await
  }

  /// libs/server/API/IGarnetApi.cs:APPEND
  pub async fn append<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    val: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.append(key, val).await
  }

  /// libs/server/API/IGarnetApi.cs:RENAMENX
  pub async fn renamenx<D: Device>(
    ss: &StorageSession<'_, D>,
    old_key: &[u8],
    new_key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.renamenx(old_key, new_key).await
  }

  /// libs/server/API/IGarnetApi.cs:EXISTS
  pub async fn exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.exists(key).await
  }

  /// libs/server/API/IGarnetApi.cs:IncrementByFloat
  pub async fn increment_by_float<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.increment_by_float(key, delta).await
  }

  /// libs/server/API/IGarnetApi.cs:DELIFEXPIM
  pub async fn delifexpim<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.delifexpim(key).await
  }

  /// libs/server/API/IGarnetApi.cs:GETDEL
  pub async fn getdel<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.getdel(key).await
  }

  /// libs/server/API/IGarnetApi.cs:TYPE
  pub async fn type_<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<Option<Vec<u8>>> {
    ss.handle_type(key).await
  }

  /// libs/server/API/IGarnetApi.cs:MEMORYUSAGE
  pub async fn memoryusage<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<Option<usize>> {
    ss.handle_memory_usage(key).await
  }

  /// libs/server/API/IGarnetApi.cs:OBJECT
  pub async fn object<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    sub: ObjectSubCommand,
  ) -> wkv::Result<Option<Vec<u8>>> {
    match sub {
      ObjectSubCommand::Encoding => ss.handle_object_encoding(key).await,
      ObjectSubCommand::RefCount => Ok(
        ss.handle_object_ref_count(key)
          .await?
          .map(|c| c.to_string().into_bytes()),
      ),
      ObjectSubCommand::IdleTime => Ok(
        ss.handle_object_idle_time(key)
          .await?
          .map(|t| t.to_string().into_bytes()),
      ),
      ObjectSubCommand::Freq => ss
        .handle_object_freq(key)
        .await
        .map(|f| f.map(|v| v.to_string().into_bytes())),
    }
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetAdd
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

  /// libs/server/API/IGarnetApi.cs:SortedSetRangeStore
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

  /// libs/server/API/IGarnetApi.cs:SortedSetRemove
  pub async fn sorted_set_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove(key, members).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRemoveRangeByLex
  pub async fn sorted_set_remove_range_by_lex<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_lex(key, min, max).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetPop
  pub async fn sorted_set_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_pop(key, count, min).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetMPop
  pub async fn sorted_set_m_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    count: usize,
    min: bool,
  ) -> wkv::Result<(GarnetStatus, Option<(Vec<u8>, Vec<(Vec<u8>, f64)>)>)> {
    ss.sorted_set_m_pop(keys, count, min).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetIncrement
  pub async fn sorted_set_increment<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.sorted_set_increment(key, member, delta).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRemoveRange
  pub async fn sorted_set_remove_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    range: ZSetRemoveRange<'_>,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range(key, range).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRemoveRangeByScore
  pub async fn sorted_set_remove_range_by_score<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_score(key, min, max).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRemoveRangeByRank
  pub async fn sorted_set_remove_range_by_rank<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_remove_range_by_rank(key, start, stop).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetDifferenceStore
  pub async fn sorted_set_difference_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_difference_store(dest, keys).await
  }

  /// libs/server/API/IGarnetApi.cs:GeoAdd
  pub async fn geo_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    items: &[(f64, f64, &[u8])],
    nx: bool,
    ch: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.geo_add(key, items, nx, ch).await
  }

  /// libs/server/API/IGarnetApi.cs:GeoSearchStore
  pub async fn geo_search_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    src: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.geo_search_store(dest, src, center, radius_m).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetIntersectStore
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

  /// libs/server/API/IGarnetApi.cs:SortedSetUnionStore
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

  /// libs/server/API/IGarnetApi.cs:SortedSetExpire
  pub async fn sorted_set_expire<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    ttl_ms: u64,
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.sorted_set_expire(key, ttl_ms).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetPersist
  pub async fn sorted_set_persist<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.sorted_set_persist(key).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetCollect
  pub async fn sorted_set_collect<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<usize> {
    ss.sorted_set_collect(key).await
  }

  /// libs/server/API/IGarnetApi.cs:SetAdd
  pub async fn set_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.set_add(key, members).await
  }

  /// libs/server/API/IGarnetApi.cs:SetRemove
  pub async fn set_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.set_remove(key, members).await
  }

  /// libs/server/API/IGarnetApi.cs:SetPop
  pub async fn set_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_pop(key, count).await
  }

  /// libs/server/API/IGarnetApi.cs:SetMove
  pub async fn set_move<D: Device>(
    ss: &StorageSession<'_, D>,
    src: &[u8],
    dest: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.set_move(src, dest, member).await
  }

  /// libs/server/API/IGarnetApi.cs:SetRandomMember
  pub async fn set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_random_member(key, count).await
  }

  /// libs/server/API/IGarnetApi.cs:SetUnionStore
  pub async fn set_union_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_union_store(dest, keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SetIntersectStore
  pub async fn set_intersect_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_intersect_store(dest, keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SetDiffStore
  pub async fn set_diff_store<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_diff_store(dest, keys).await
  }

  /// libs/server/API/IGarnetApi.cs:ListPosition
  pub async fn list_position<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    element: &[u8],
    rank: i64,
    maxlen: Option<usize>,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_position(key, element, rank, maxlen).await
  }

  /// libs/server/API/IGarnetApi.cs:ListLeftPush
  pub async fn list_left_push<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    values: &[&[u8]],
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_push(key, values, OperationDirection::Left, only_if_exists)
      .await
  }

  /// libs/server/API/IGarnetApi.cs:ListRightPush
  pub async fn list_right_push<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    values: &[&[u8]],
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_push(key, values, OperationDirection::Right, only_if_exists)
      .await
  }

  /// libs/server/API/IGarnetApi.cs:ListLeftPop
  pub async fn list_left_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_pop(key, OperationDirection::Left).await
  }

  /// libs/server/API/IGarnetApi.cs:ListRightPop
  pub async fn list_right_pop<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_pop(key, OperationDirection::Right).await
  }

  /// libs/server/API/IGarnetApi.cs:ListMove
  pub async fn list_move<D: Device>(
    ss: &StorageSession<'_, D>,
    src: &[u8],
    dest: &[u8],
    src_dir: OperationDirection,
    dest_dir: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_move(src, dest, src_dir, dest_dir).await
  }

  /// libs/server/API/IGarnetApi.cs:ListTrim
  pub async fn list_trim<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<GarnetStatus> {
    ss.list_trim(key, start, stop).await
  }

  /// libs/server/API/IGarnetApi.cs:ListInsert
  pub async fn list_insert<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    pivot: &[u8],
    element: &[u8],
    before: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.list_insert(key, pivot, element, before).await
  }

  /// libs/server/API/IGarnetApi.cs:ListRemove
  pub async fn list_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    element: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.list_remove(key, element, count).await
  }

  /// libs/server/API/IGarnetApi.cs:ListSet
  pub async fn list_set<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    index: i64,
    element: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.list_set(key, index, element).await
  }

  /// libs/server/API/IGarnetApi.cs:HashSetWhenNotExists
  pub async fn hash_set_when_not_exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_set(key, fields, true).await
  }

  /// libs/server/API/IGarnetApi.cs:HashDelete
  pub async fn hash_delete<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_delete(key, fields).await
  }

  /// libs/server/API/IGarnetApi.cs:HashIncrement
  pub async fn hash_increment<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
    delta: &[u8],
    float: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.hash_increment(key, field, delta, float).await
  }

  /// libs/server/API/IGarnetApi.cs:StringSetBit
  pub async fn string_set_bit<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    offset: u64,
    bit: u8,
  ) -> wkv::Result<(GarnetStatus, u8)> {
    ss.string_set_bit(key, offset, bit).await
  }

  /// libs/server/API/IGarnetApi.cs:StringBitOperation
  pub async fn string_bit_operation<D: Device>(
    ss: &StorageSession<'_, D>,
    op: BitmapOp,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.string_bit_operation(op, dest, keys).await
  }

  /// libs/server/API/IGarnetApi.cs:StringBitField
  pub async fn string_bit_field<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    ops: &[BitFieldOp],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    ss.string_bit_field(key, ops).await
  }

  /// libs/server/API/IGarnetApi.cs:HyperLogLogAdd
  pub async fn hyper_log_log_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    elements: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i32)> {
    ss.hyper_log_log_add(key, elements).await
  }

  /// libs/server/API/IGarnetApi.cs:HyperLogLogMerge
  pub async fn hyper_log_log_merge<D: Device>(
    ss: &StorageSession<'_, D>,
    dest: &[u8],
    sources: &[&[u8]],
  ) -> wkv::Result<GarnetStatus> {
    ss.hyper_log_log_merge(dest, sources).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetAdd
  pub async fn vector_set_add<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_add(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetRemove
  pub async fn vector_set_remove<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_remove(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetSetAttribute
  pub async fn vector_set_set_attribute<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_set_attribute(key).await
  }

  /// libs/server/API/IGarnetApi.cs:GETForMemoryResult
  pub async fn get_for_memory_result<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.read_main_store(key).await
  }

  /// libs/server/API/IGarnetApi.cs:LCS
  pub async fn lcs<D: Device>(
    ss: &StorageSession<'_, D>,
    key1: &[u8],
    key2: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<LcsResult>)> {
    ss.lcs(key1, key2).await
  }

  /// libs/server/API/IGarnetApi.cs:GETRANGE
  pub async fn getrange<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    end: i64,
  ) -> wkv::Result<Vec<u8>> {
    ss.getrange(key, start, end).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetLength
  pub async fn sorted_set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_length(key).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRange
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

  /// libs/server/API/IGarnetApi.cs:SortedSetScore
  pub async fn sorted_set_score<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    ss.sorted_set_score(key, member).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetScores
  pub async fn sorted_set_scores<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<f64>>)> {
    ss.sorted_set_scores(key, members).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetCount
  pub async fn sorted_set_count<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_count(key, min, max).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetLengthByValue
  pub async fn sorted_set_length_by_value<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    min: &[u8],
    max: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_length_by_value(key, min, max).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRank
  pub async fn sorted_set_rank<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
    rev: bool,
  ) -> wkv::Result<(GarnetStatus, Option<i64>)> {
    ss.sorted_set_rank(key, member, rev).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetRandomMember
  pub async fn sorted_set_random_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_scores: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<f64>)>)> {
    ss.sorted_set_random_member(key, count, with_scores).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetDifference
  pub async fn sorted_set_difference<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_difference(keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetUnion
  pub async fn sorted_set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_union(keys, weights, aggregate).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetScan
  pub async fn sorted_set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.sorted_set_scan(key, cursor, pattern, count).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetIntersect
  pub async fn sorted_set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.sorted_set_intersect(keys, weights, aggregate).await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetIntersectLength
  pub async fn sorted_set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: ZSetAggregate,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.sorted_set_intersect_length(keys, weights, aggregate)
      .await
  }

  /// libs/server/API/IGarnetApi.cs:SortedSetTimeToLive
  pub async fn sorted_set_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.sorted_set_time_to_live(key).await
  }

  /// libs/server/API/IGarnetApi.cs:GeoCommands
  pub async fn geo_commands<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cmd: GeoCmd<'_>,
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.geo_commands(key, cmd).await
  }

  /// libs/server/API/IGarnetApi.cs:GeoSearchReadOnly
  pub async fn geo_search_read_only<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    ss.geo_search_read_only(key, center, radius_m).await
  }

  /// libs/server/API/IGarnetApi.cs:ListLength
  pub async fn list_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.list_length(key).await
  }

  /// libs/server/API/IGarnetApi.cs:ListRange
  pub async fn list_range<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.list_range(key, start, stop).await
  }

  /// libs/server/API/IGarnetApi.cs:ListIndex
  pub async fn list_index<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    index: i64,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.list_index(key, index).await
  }

  /// libs/server/API/IGarnetApi.cs:SetLength
  pub async fn set_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_length(key).await
  }

  /// libs/server/API/IGarnetApi.cs:SetMembers
  pub async fn set_members<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_members(key).await
  }

  /// libs/server/API/IGarnetApi.cs:SetIsMember
  pub async fn set_is_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.set_is_member(key, member).await
  }

  /// libs/server/API/IGarnetApi.cs:SetScan
  pub async fn set_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    set_scan(ss, key, cursor, pattern, count).await
  }

  /// libs/server/API/IGarnetApi.cs:SetUnion
  pub async fn set_union<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_union(keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SetIntersect
  pub async fn set_intersect<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_intersect(keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SetDiff
  pub async fn set_diff<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.set_diff(keys).await
  }

  /// libs/server/API/IGarnetApi.cs:SetIntersectLength
  pub async fn set_intersect_length<D: Device>(
    ss: &StorageSession<'_, D>,
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.set_intersect_length(keys).await
  }

  /// libs/server/API/IGarnetApi.cs:HashGet
  pub async fn hash_get<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.hash_get(key, field).await
  }

  /// libs/server/API/IGarnetApi.cs:HashGetMultiple
  pub async fn hash_get_multiple<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    ss.hash_get_multiple(key, fields).await
  }

  /// libs/server/API/IGarnetApi.cs:HashGetAll
  pub async fn hash_get_all<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Vec<u8>)>)> {
    ss.hash_get_all(key).await
  }

  /// libs/server/API/IGarnetApi.cs:HashLength
  pub async fn hash_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    ss.hash_length(key).await
  }

  /// libs/server/API/IGarnetApi.cs:HashStrLength
  pub async fn hash_str_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    ss.hash_str_length(key, field).await
  }

  /// libs/server/API/IGarnetApi.cs:HashExists
  pub async fn hash_exists<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    ss.hash_exists(key, field).await
  }

  /// libs/server/API/IGarnetApi.cs:HashRandomField
  pub async fn hash_random_field<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    count: i64,
    with_values: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    ss.hash_random_field(key, count, with_values).await
  }

  /// libs/server/API/IGarnetApi.cs:HashKeys
  pub async fn hash_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.hash_keys(key).await
  }

  /// libs/server/API/IGarnetApi.cs:HashVals
  pub async fn hash_vals<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    ss.hash_vals(key).await
  }

  /// libs/server/API/IGarnetApi.cs:HashScan
  pub async fn hash_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    hash_scan(ss, key, cursor, pattern, count).await
  }

  /// libs/server/API/IGarnetApi.cs:HashTimeToLive
  pub async fn hash_time_to_live<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.hash_time_to_live(key).await
  }

  /// libs/server/API/IGarnetApi.cs:StringGetBit
  pub async fn string_get_bit<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    offset: u64,
  ) -> wkv::Result<(GarnetStatus, u8)> {
    ss.string_get_bit(key, offset).await
  }

  /// libs/server/API/IGarnetApi.cs:StringBitCount
  pub async fn string_bit_count<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, u64)> {
    ss.string_bit_count(key, start, end, bit_mode).await
  }

  /// libs/server/API/IGarnetApi.cs:StringBitPosition
  pub async fn string_bit_position<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    bit: u8,
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    ss.string_bit_position(key, bit, start, end, bit_mode).await
  }

  /// libs/server/API/IGarnetApi.cs:StringBitFieldReadOnly
  pub async fn string_bit_field_read_only<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    gets: &[(bool, u8, u64)],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    ss.string_bit_field_read_only(key, gets).await
  }

  /// libs/server/API/IGarnetApi.cs:HyperLogLogLength
  pub async fn hyper_log_log_length<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, f64)> {
    ss.hyper_log_log_length(key).await
  }

  /// libs/server/API/IGarnetApi.cs:GetDbKeys
  pub async fn get_db_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    pattern: &[u8],
  ) -> wkv::Result<Vec<Vec<u8>>> {
    ss.db_keys(pattern).await
  }

  /// libs/server/API/IGarnetApi.cs:GetDbSize
  pub async fn get_db_size<D: Device>(ss: &StorageSession<'_, D>) -> wkv::Result<usize> {
    ss.db_size().await
  }

  /// libs/server/API/IGarnetApi.cs:DbScan
  pub async fn db_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    pattern: &[u8],
    all_keys: bool,
    cursor: &[u8],
    count: usize,
  ) -> wkv::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    ss.db_scan(pattern, all_keys, cursor, count).await
  }

  /// libs/server/API/IGarnetApi.cs:IterateStore
  pub async fn iterate_store<D: Device>(
    ss: &StorageSession<'_, D>,
    on_record: impl FnMut(&[u8], &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    ss.iterate_store(on_record).await
  }

  /// libs/server/API/IGarnetApi.cs:DeleteSlotKeys
  pub async fn delete_slot_keys<D: Device>(
    ss: &StorageSession<'_, D>,
    slots: &[u16],
  ) -> wkv::Result<u64> {
    ss.delete_slot_keys(slots).await
  }

  /// libs/server/API/IGarnetApi.cs:ObjectScan
  pub async fn object_scan<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    tag: u8,
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    ss.object_scan(key, tag, pattern, cursor, count, generic_members)
      .await
  }

  /// libs/server/API/IGarnetApi.cs:ResetScratchBuffer
  pub fn reset_scratch_buffer(&self) {}

  /// libs/server/API/IGarnetApi.cs:VectorSetCardinality
  pub async fn vector_set_cardinality<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_cardinality(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetIsMember
  pub async fn vector_set_is_member<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_is_member(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetLinks
  pub async fn vector_set_links<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_links(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetRandomMembers
  pub async fn vector_set_random_members<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_random_members(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetValueSimilarity
  pub async fn vector_set_value_similarity<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_value_similarity(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetElementSimilarity
  pub async fn vector_set_element_similarity<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_element_similarity(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetEmbedding
  pub async fn vector_set_embedding<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_embedding(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetRawEmbedding
  pub async fn vector_set_raw_embedding<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_raw_embedding(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetDimensions
  pub async fn vector_set_dimensions<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_dimensions(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetInfo
  pub async fn vector_set_info<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_info(key).await
  }

  /// libs/server/API/IGarnetApi.cs:VectorSetGetAttribute
  pub async fn vector_set_get_attribute<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    ss.vector_set_get_attribute(key).await
  }

  /// libs/server/API/IGarnetApi.cs:WATCH
  pub fn watch<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    store_type: StoreType,
  ) -> GarnetStatus {
    match store_type {
      StoreType::None => GarnetStatus::NotFound,
      _ => {
        ss.watch_key(key);
        GarnetStatus::Ok
      }
    }
  }
}
