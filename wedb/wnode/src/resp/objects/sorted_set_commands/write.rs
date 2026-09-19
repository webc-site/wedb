//! 有序集合写操作与集合运算命令（ZADD/ZREM/ZINCRBY/ZREMRANGE/ZRANGESTORE/
//! ZDIFF/ZINTER/ZUNION/ZINTERCARD/ZRANDMEMBER/ZEXPIRE/ZTTL/ZPERSIST，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 写命令与集合运算段）

use gxhash::HashMap;
use wbase::num::{strict_f64, strict_i32};
use wcol::zset::sorted_set_object::{SortedSetObject, SortedSetOperation, SortedSetRangeOpts};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
  options::SortedSetAggregateType as ZSetAggregate,
};

use super::{Rmw, ZsetLoad, parse_pairs_payload, run_operate, zset_load_sync, zset_save_or_gc};
use crate::{
  resp::{
    objects::object_store_utils::{
      ElementHeaderKind, IntersectCardKind, RespRmwDone, parse_elements_only_args,
      parse_expire_elements_args, parse_intersect_card_args, parse_random_member_args,
      write_random_member_missing,
    },
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::del_ttl_sync,
};

/// ZREMRANGE 变体
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveRangeKind {
  Rank,
  Score,
  Lex,
}

/// 集合运算种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineKind {
  Intersect,
  Union,
}

/// 集合运算参数解析产物
pub struct CombineArgs<'p> {
  pub keys: Vec<&'p [u8]>,
  pub weights: Vec<f64>,
  pub aggregate: ZSetAggregate,
  pub with_scores: bool,
}

impl RespServerSession {
  /// ZADD key [NX|XX|GT|LT|CH|INCR] score member [score member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd
  pub fn sorted_set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3.., output, "ZADD");

    let key = parse_state[0];
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zadd,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      Rmw::WrongType | Rmw::Missing => Ok(true),
      // 写成功 → 唤醒该键的阻塞观察者（C# SortedSetAdd 后 HandleCollectionUpdate）
      Rmw::Present(_) => {
        self.notify_collection_update(key);
        Ok(true)
      }
    }
  }

  /// ZREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemove
  pub fn sorted_set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "ZREM");

    let key = parse_state[0];
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zrem,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# SortedSetRemove 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// ZINCRBY key increment member
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIncrement
  pub fn sorted_set_increment<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZINCRBY");

    let key = parse_state[0];
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zincrby,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemoveRange
  pub fn sorted_set_remove_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    range_kind: RemoveRangeKind,
  ) -> wresp::Result<bool> {
    let (name, op) = match range_kind {
      RemoveRangeKind::Rank => ("ZREMRANGEBYRANK", SortedSetOperation::Zremrangebyrank),
      RemoveRangeKind::Score => ("ZREMRANGEBYSCORE", SortedSetOperation::Zremrangebyscore),
      RemoveRangeKind::Lex => ("ZREMRANGEBYLEX", SortedSetOperation::Zremrangebylex),
    };
    check_arg_count!(parse_state, 3, output, name);

    let key = parse_state[0];
    let payload_start = output.len();
    match self.zset_rmw(store, key, op, &parse_state[1..], (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => return Ok(true),
      // ZREMRANGEBYLEX 仅回填 result1（int.MaxValue=参数错误、int.MinValue=部分执行）
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if range_kind == RemoveRangeKind::Lex && !payload_written {
          if result1 == i32::MAX as i64 {
            output.truncate(payload_start);
            cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
          } else if output.len() == payload_start && result1 != i32::MIN as i64 {
            output.write_resp_int(result1);
          }
        }
      }
    }
    Ok(true)
  }

  /// ZRANGESTORE dst src min max \[BYSCORE|BYLEX\] \[REV\] \[LIMIT offset count\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRangeStore
  pub fn sorted_set_range_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4..=9, output, "ZRANGESTORE");

    let dst_key = parse_state[0];
    let src_key = parse_state[1];

    // 源集合范围读取（Store 选项：强制 WITHSCORES + RESP2 成对负载）
    let mut src_obj = match zset_load_sync(store, src_key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）
        match zset_save_or_gc(store, dst_key, &SortedSetObject::new()) {
          Ok(true) => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
          Ok(false) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut src_obj,
      SortedSetOperation::Zrange,
      &parse_state[2..],
      0,
      SortedSetRangeOpts::STORE.bits() as i32,
      self.resp_protocol_version,
    );

    // result1 = -1 表示范围参数被拒（错误已写入负载）
    if obj_out.result1 == -1 {
      output.extend_from_slice(&obj_out.payload);
      return Ok(true);
    }

    // 成对负载 → 目标集合（from_entries：双索引 + 内存记账，ProcessRespArrayOutputAsPairs 语义）
    let dst = SortedSetObject::from_entries(parse_pairs_payload(&obj_out.payload));

    // STORE 族目标键为 SET 语义（清既有 key 级 TTL）：对标 C# SortedSetRangeStore
    // 先统一面 Delete dst 再 RMW ZADD（ObjectStore/SortedSetOps.cs），信封域
    // upsert 默认保留 TTL，故显式清退
    match del_ttl_sync(store, dst_key) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match zset_save_or_gc(store, dst_key, &dst) {
      Ok(true) => {
        output.write_resp_int(dst.sorted_set_dict.len() as i64);
        self.notify_collection_update(dst_key);
      }
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// ZDIFF numkeys key [key ...] \[WITHSCORES\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetDifference
  pub fn sorted_set_difference<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let (keys, with_scores) = match parse_diff_args(parse_state, "ZDIFF", output) {
      Some(v) => v,
      None => return Ok(true),
    };

    // 第一集合 − 其余集合（CopyDiff 逐个收缩，复用 diff_sets）
    let objs = match load_many(store, &keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = diff_sets(&objs);

    write_zset_entries(
      Some(&result),
      with_scores,
      output,
      self.resp_protocol_version,
    );
    Ok(true)
  }

  /// ZDIFFSTORE dst numkeys key [key ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetDifferenceStore
  pub fn sorted_set_difference_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3.., output, "ZDIFFSTORE");

    let dst = parse_state[0];
    let Some((keys, _)) = parse_diff_args(&parse_state[1..], "ZDIFFSTORE", output) else {
      return Ok(true);
    };

    let objs = match load_many(store, &keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let mut result = diff_sets(&objs);
    let count = result.count();
    // STORE 族目标键为 SET 语义（清既有 key 级 TTL，对标 C#
    // SortedSetDifferenceStore 的 SET 收尾），信封域 upsert 默认保留须显式清退
    match del_ttl_sync(store, dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match zset_save_or_gc(store, dst, &result) {
      Ok(true) => {
        output.write_resp_int(count as i64);
        self.notify_collection_update(dst);
      }
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] \[WITHSCORES\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersect
  pub fn sorted_set_intersect<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_combine_args(parse_state, "ZINTER", output) else {
      return Ok(true);
    };

    let objs = match load_many(store, &args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = combine_sets(&objs, &args.weights, args.aggregate, CombineKind::Intersect);
    write_zset_entries(
      Some(&result),
      args.with_scores,
      output,
      self.resp_protocol_version,
    );
    Ok(true)
  }

  /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectLength
  pub fn sorted_set_intersect_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；zset 族帧见 IntersectCardKind）
    let Some(args) = parse_intersect_card_args(IntersectCardKind::SortedSet, parse_state, output)
    else {
      return Ok(true);
    };
    // LIMIT 仅正值参与钳制（0 与未给同效）
    let limit_v = args.limit.filter(|&v| v > 0);

    let mut objs = match load_many(store, args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    // ZINTERCARD 只需基数：取最小集合遍历查集，命中 limit 提前跳出，
    // 彻底消除字典 clone 与成员字符串分配（空间 O(1)）
    let card = if objs.is_empty() {
      0
    } else if objs.len() == 1 {
      objs[0].count() as i64
    } else if let Some((min_idx, min_obj)) = objs
      .iter()
      .enumerate()
      .min_by_key(|(_, o)| o.sorted_set_dict.len())
    {
      if min_obj.sorted_set_dict.is_empty() {
        0
      } else {
        let other_objs: Vec<_> = objs
          .iter()
          .enumerate()
          .filter_map(|(i, o)| (i != min_idx).then_some(&o.sorted_set_dict))
          .collect();
        let mut count = 0_i64;
        for member in min_obj.sorted_set_dict.keys() {
          if other_objs.iter().all(|dict| dict.contains_key(member)) {
            count += 1;
            if let Some(v) = limit_v
              && count >= i64::from(v)
            {
              break;
            }
          }
        }
        count
      }
    } else {
      0
    };
    output.write_resp_int(match limit_v {
      Some(v) => card.min(i64::from(v)),
      None => card,
    });
    Ok(true)
  }

  /// ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectStore
  pub fn sorted_set_intersect_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 目标键写成功后唤醒其阻塞观察者（C# SortedSetIntersectStore 同径）
    let notify_dst = |dst: &[u8]| self.notify_collection_update(dst);
    sorted_set_combine_store(
      parse_state,
      store,
      output,
      CombineKind::Intersect,
      &notify_dst,
    )
  }

  /// ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] \[WITHSCORES\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetUnion
  pub fn sorted_set_union<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_combine_args(parse_state, "ZUNION", output) else {
      return Ok(true);
    };

    let objs = match load_many(store, &args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = combine_sets(&objs, &args.weights, args.aggregate, CombineKind::Union);
    write_zset_entries(
      Some(&result),
      args.with_scores,
      output,
      self.resp_protocol_version,
    );
    Ok(true)
  }

  /// ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetUnionStore
  pub fn sorted_set_union_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 目标键写成功后唤醒其阻塞观察者（C# SortedSetUnionStore 同径）
    let notify_dst = |dst: &[u8]| self.notify_collection_update(dst);
    sorted_set_combine_store(parse_state, store, output, CombineKind::Union, &notify_dst)
  }

  /// ZRANDMEMBER key \[count \[WITHSCORES\]\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRandomMember
  pub fn sorted_set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；count 钳至有符号 30 位，
    // arg1 打包 (count << 1 | includedCount) << 1 | withScores；判定序对齐
    // C# 先解析后触达后端）
    let Some(args) = parse_random_member_args("ZRANDMEMBER", parse_state, cs::WITHSCORES, output)
    else {
      return Ok(true);
    };

    let key = parse_state[0];

    // count 为 0 不触达后端（对齐 C#；应答与缺失态同形单源）
    if args.param_count == 0 {
      write_random_member_missing(output, args.included_count, self.resp_protocol_version);
      return Ok(true);
    }

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        write_random_member_missing(output, args.included_count, self.resp_protocol_version);
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zrandmember,
      &[],
      args.arg1,
      fastrand::i32(..),
      self.resp_protocol_version,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZEXPIRE / ZEXPIREAT / ZPEXPIRE / ZPEXPIREAT key seconds [NX|XX|GT|LT] MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetExpire
  pub fn sorted_set_expire<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &'static str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；ticks 换算与 word 打包收口内核）
    let Some(args) = parse_expire_elements_args(
      cmd_name,
      parse_state,
      ElementHeaderKind::Members,
      is_milliseconds,
      is_timestamp,
      output,
    ) else {
      return Ok(true);
    };

    match self.zset_rmw(
      store,
      args.key,
      SortedSetOperation::Zexpire,
      args.elements,
      args.args12,
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZTTL / ZPTTL / ZEXPIRETIME / ZPEXPIRETIME key MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetTimeToLive
  pub fn sorted_set_time_to_live<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &'static str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, members)) =
      parse_elements_only_args(cmd_name, parse_state, ElementHeaderKind::Members, output)
    else {
      return Ok(true);
    };

    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zttl,
      members,
      (i32::from(is_milliseconds), i32::from(is_timestamp)),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZPERSIST key MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPersist
  pub fn sorted_set_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, members)) =
      parse_elements_only_args("ZPERSIST", parse_state, ElementHeaderKind::Members, output)
    else {
      return Ok(true);
    };

    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zpersist,
      members,
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }
}

/// ZINTERSTORE / ZUNIONSTORE 公共入口
fn sorted_set_combine_store<'s, D: wdev::Device>(
  parse_state: &[&[u8]],
  store: &wkv::BatchStoreSession<'s, D>,
  output: &mut Vec<u8>,
  kind: CombineKind,
  notify: &impl Fn(&[u8]),
) -> wresp::Result<bool> {
  let name = if kind == CombineKind::Intersect {
    "ZINTERSTORE"
  } else {
    "ZUNIONSTORE"
  };
  check_arg_count!(parse_state, 3.., output, name);
  let dst = parse_state[0];
  let Some(args) = parse_combine_args(&parse_state[1..], name, output) else {
    return Ok(true);
  };

  let objs = match load_many(store, &args.keys, output) {
    Ok(Some(objs)) => objs,
    Ok(None) => return Ok(false),
    Err(()) => return Ok(true),
  };
  let mut result = combine_sets(&objs, &args.weights, args.aggregate, kind);
  let count = result.count();
  // STORE 族目标键为 SET 语义（清既有 key 级 TTL，对标 C# ZINTERSTORE/
  // ZUNIONSTORE 的 SET 收尾），信封域 upsert 默认保留须显式清退
  match del_ttl_sync(store, dst) {
    Ok(true) => {}
    Ok(false) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  match zset_save_or_gc(store, dst, &result) {
    Ok(true) => {
      output.write_resp_int(count as i64);
      notify(dst);
    }
    Ok(false) => return Ok(false),
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  Ok(true)
}

/// ZDIFF 参数解析：numkeys key... [WITHSCORES]
pub(crate) fn parse_diff_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
) -> Option<(Vec<&'p [u8]>, bool)> {
  check_arg_count!(parse_state, 2.., output, name, return None);

  // C# TryGetInt（int32）：非整数（含溢出）报 NOT_INTEGER（SortedSetDifference :921）
  let Some(n_keys) = strict_i32(parse_state[0]) else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };

  let remaining = parse_state.len() - 1;
  let n = n_keys as usize;
  if remaining != n && remaining != n + 1 {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }

  let mut with_scores = false;
  if remaining > n {
    let last = parse_state[remaining];
    if !last.eq_ignore_ascii_case(cs::WITHSCORES) {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
    with_scores = true;
  }

  Some((parse_state[1..=n].to_vec(), with_scores))
}

/// ZINTER/ZUNION 参数解析：numkeys key... [WEIGHTS w...] [AGGREGATE agg] [WITHSCORES]
pub(crate) fn parse_combine_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
) -> Option<CombineArgs<'p>> {
  check_arg_count!(parse_state, 2.., output, name, return None);

  // C# TryGetInt（int32）：非整数（含溢出）报 NOT_INTEGER（SortedSetIntersect :1061 / Union :1356）
  let Some(n_keys) = strict_i32(parse_state[0]) else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };
  if n_keys < 1 {
    output.extend_from_slice(
      format!("-ERR at least 1 input key is needed for '{name}' command\r\n").as_bytes(),
    );
    return None;
  }
  if parse_state.len() < n_keys as usize + 1 {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }

  let keys: Vec<&[u8]> = parse_state[1..=n_keys as usize].to_vec();
  let mut weights = vec![1.0_f64; keys.len()];
  let mut aggregate = ZSetAggregate::Sum;
  let mut with_scores = false;

  let mut idx = n_keys as usize + 1;
  while idx < parse_state.len() {
    let token = parse_state[idx];
    if token.eq_ignore_ascii_case(cs::WEIGHTS) {
      idx += 1;
      // C# 两段式：先判数量够不够（:1097/:1289/:1388/:1512），再逐值判浮点
      if idx + keys.len() > parse_state.len() {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      let mut parsed = Vec::with_capacity(keys.len());
      for &raw in &parse_state[idx..idx + keys.len()] {
        match strict_f64(raw, true) {
          Some(w) => parsed.push(w),
          None => {
            // C# GenericErrNotAFloat 替换 {0}="weight"（SortedSetCommands.cs:1107）
            cs::abort_with_error_message(output, cs::GENERIC_ERR_NOT_A_FLOAT_WEIGHT);
            return None;
          }
        }
      }
      idx += keys.len();
      weights = parsed;
    } else if token.eq_ignore_ascii_case(b"AGGREGATE") {
      idx += 1;
      let Some(agg) = parse_state.get(idx).and_then(|t| {
        if t.eq_ignore_ascii_case(b"SUM") {
          Some(ZSetAggregate::Sum)
        } else if t.eq_ignore_ascii_case(b"MIN") {
          Some(ZSetAggregate::Min)
        } else if t.eq_ignore_ascii_case(b"MAX") {
          Some(ZSetAggregate::Max)
        } else {
          None
        }
      }) else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      };
      aggregate = agg;
      idx += 1;
    } else if token.eq_ignore_ascii_case(cs::WITHSCORES) {
      with_scores = true;
      idx += 1;
    } else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
  }

  Some(CombineArgs {
    keys,
    weights,
    aggregate,
    with_scores,
  })
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
///
/// 聚合面成员级 TTL：装载即堆序 purge 过期成员（对位 C# 各聚合命令经
/// SortedSetObject.Dictionary getter / TryGetScore / CopyDiff / InPlaceDiff
/// 的存活视图口径，libs/server/Objects/SortedSet/SortedSetObject.cs:237）；
/// 装载产物为本请求私有副本，就地 purge 与 C# 非破坏过滤行为等价，
/// 且与 count() 复用同一谓词源 delete_expired_items，勿另建第二套口径
fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SortedSetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(None),
      ZsetLoad::WrongType => return Err(()),
      ZsetLoad::Missing => objs.push(SortedSetObject::new()),
      ZsetLoad::Present(mut o) => {
        o.delete_expired_items();
        objs.push(o);
      }
    }
  }
  Ok(Some(objs))
}

/// 字典 → 有序集合（保持 (score, member) 双索引）
fn dict_to_zset(dict: HashMap<Vec<u8>, f64>) -> SortedSetObject {
  let entries: Vec<(Vec<u8>, f64)> = dict.into_iter().collect();
  SortedSetObject::from_entries(entries)
}

/// ZDIFF 语义计算：单次遍历 first，过滤掉在 rest 中出现的项，消灭多轮复制与重构
pub(crate) fn diff_sets(objs: &[SortedSetObject]) -> SortedSetObject {
  let Some((first, rest)) = objs.split_first() else {
    return SortedSetObject::default();
  };
  if rest.is_empty() {
    return first.clone();
  }
  let entries: Vec<(Vec<u8>, f64)> = first
    .sorted_set
    .iter()
    .filter(|e| {
      !rest
        .iter()
        .any(|o| o.sorted_set_dict.contains_key(&e.member))
    })
    .map(|e| (e.member.clone(), e.score))
    .collect();
  SortedSetObject::from_entries(entries)
}

/// ZINTER/ZUNION 语义计算（权重 + 聚合）
pub(crate) fn combine_sets(
  objs: &[SortedSetObject],
  weights: &[f64],
  aggregate: ZSetAggregate,
  kind: CombineKind,
) -> SortedSetObject {
  if kind == CombineKind::Intersect {
    // 交集：任何一个集合为空，结果必为空；
    // 选出最小集合作为基准遍历，仅克隆最终交集元素，消灭临时 HashMap 与无效克隆
    if objs.is_empty() || objs.iter().any(|o| o.sorted_set_dict.is_empty()) {
      return SortedSetObject::default();
    }
    let (min_idx, min_obj) = objs
      .iter()
      .enumerate()
      // 选最小集合仅决定遍历顺序（基数直读 raw len；过期成员已由
      // load_many 装载口堆序 purge 剔净，遍历即存活视图）
      .min_by_key(|(_, o)| o.sorted_set_dict.len())
      .unwrap();

    let mut entries = Vec::new();
    'outer: for (member, min_score) in &min_obj.sorted_set_dict {
      let mut agg_score = min_score * weights.get(min_idx).copied().unwrap_or(1.0);
      for (i, obj) in objs.iter().enumerate() {
        if i == min_idx {
          continue;
        }
        let Some(&score) = obj.sorted_set_dict.get(member) else {
          continue 'outer;
        };
        let w = weights.get(i).copied().unwrap_or(1.0);
        let s = score * w;
        agg_score = aggregate.apply(agg_score, s);
      }
      entries.push((member.clone(), agg_score));
    }
    return SortedSetObject::from_entries(entries);
  }

  // 并集：预分配总容量
  let total_cap = objs.iter().map(|o| o.sorted_set_dict.len()).sum();
  let mut combined: HashMap<Vec<u8>, f64> = HashMap::default();
  combined.reserve(total_cap);
  for (i, obj) in objs.iter().enumerate() {
    let weight = weights.get(i).copied().unwrap_or(1.0);
    for (member, score) in &obj.sorted_set_dict {
      let weighted = score * weight;
      match combined.get_mut(member.as_slice()) {
        Some(existing) => {
          *existing = aggregate.apply(*existing, weighted);
        }
        None => {
          combined.insert(member.clone(), weighted);
        }
      }
    }
  }

  dict_to_zset(combined)
}

/// 结果集合的 RESP 输出（WITHSCORES 双协议形态：RESP2 扁平 *2n + bulk 分值；
/// RESP3 头 *n + 逐成员前插 *2 + `,num` 分值）
///
/// 对标 SortedSetCommands.cs:966-988（ZDIFF）/1135-1165（ZINTER）/1446-1462（ZUNION）
/// 的 WriteArrayLength + WriteDoubleNumeric 组合；空结果恒 TryWriteEmptyArray(*0)
pub(crate) fn write_zset_entries(
  obj: Option<&SortedSetObject>,
  with_scores: bool,
  output: &mut Vec<u8>,
  resp_version: u8,
) {
  let Some(obj) = obj else {
    // C# 空结果 → TryWriteEmptyArray（不分版本的 *0）
    output.extend_from_slice(b"*0\r\n");
    return;
  };

  // 有序视图单次遍历流式写出：免 collect 与逐元素 clone
  let n = obj.sorted_set.len();
  // C# :966/:1135/:1446：头长 RESP2+WITHSCORES 为扁平 2n，RESP3 为 n
  output.write_resp_array_len(if with_scores && resp_version < 3 {
    n * 2
  } else {
    n
  });
  for e in &obj.sorted_set {
    if with_scores && resp_version >= 3 {
      // RESP3 逐成员前插成员对包装
      output.write_resp_array_len(2);
    }
    output.write_resp_bulk_string(&e.member);
    if with_scores {
      // 分值版本分派单点（RESP2 bulk / RESP3 `,num`）
      cs::write_double_numeric(output, e.score, resp_version);
    }
  }
}

#[cfg(test)]
mod write_zset_entries_tests {
  use wcol::zset::sorted_set_object::SortedSetObject;

  use super::write_zset_entries;

  fn obj() -> SortedSetObject {
    SortedSetObject::from_entries(vec![(b"a".to_vec(), 1.5), (b"b".to_vec(), 2.5)])
  }

  /// WITHSCORES：RESP2 扁平 *2n + bulk 分值；RESP3 头 *n + 逐条 *2 + `,num`
  ///（C# SortedSetCommands.cs:966-988 / 1135-1165 / 1446-1462）
  #[test]
  fn with_scores_dual_protocol() {
    let mut out2 = Vec::new();
    write_zset_entries(Some(&obj()), true, &mut out2, 2);
    assert_eq!(
      out2,
      b"*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$3\r\n2.5\r\n"
    );

    let mut out3 = Vec::new();
    write_zset_entries(Some(&obj()), true, &mut out3, 3);
    assert_eq!(
      out3,
      b"*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$1\r\nb\r\n,2.5\r\n"
    );
  }

  /// 不带分值：双版本同形
  #[test]
  fn without_scores_dual_protocol() {
    let mut out2 = Vec::new();
    write_zset_entries(Some(&obj()), false, &mut out2, 2);
    assert_eq!(out2, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

    let mut out3 = Vec::new();
    write_zset_entries(Some(&obj()), false, &mut out3, 3);
    assert_eq!(out3, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
  }

  /// 空结果恒 *0（C# TryWriteEmptyArray，不分版本）
  #[test]
  fn empty_result_star0() {
    for ver in [2_u8, 3] {
      let mut out = Vec::new();
      write_zset_entries(None, true, &mut out, ver);
      assert_eq!(out, b"*0\r\n");
    }
  }
}
