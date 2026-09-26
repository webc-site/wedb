//! 有序集合写操作与集合运算命令（ZADD/ZREM/ZINCRBY/ZREMRANGE/ZRANGESTORE/
//! ZDIFF/ZINTER/ZUNION/ZINTERCARD/ZRANDMEMBER/ZEXPIRE/ZTTL/ZPERSIST，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 写命令与集合运算段）

use wbase::{map::HashMap, num::strict_f64};
use wcol::zset::sorted_set_object::{SortedSetObject, SortedSetOperation, SortedSetRangeOpts};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i32_arg},
  cmd_strings as cs,
  ext::{RespVecExt, is_resp3},
  options::{SortedSetAggregateType as ZSetAggregate, try_get_sorted_set_aggregate_type},
};

use super::{
  OptCursor, Rmw, ZsetLoad, parse_pairs_payload, run_operate, store_overwrite, zset_load_sync,
};
use crate::resp::{
  objects::object_store_utils::{
    ElementHeaderKind, IntersectCardKind, RespRmwDone, parse_elements_only_args,
    parse_expire_elements_args, parse_intersect_card_args, parse_random_member_args,
    write_random_member_missing, write_rmw_aof_fail_frame, write_rmw_reply,
  },
  resp_server_session::RespServerSession,
};

/// 对象层范围类 result1 哨兵（ZLEXCOUNT / ZREMRANGEBYLEX 回填约定）
const BAD_RANGE: i64 = i32::MAX as i64;
const PARTIAL: i64 = i32::MIN as i64;

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
  pub keys: &'p [&'p [u8]],
  pub weights: Vec<f64>,
  pub aggregate: ZSetAggregate,
  pub with_scores: bool,
}

impl RespServerSession {
  /// ZADD key [NX|XX|GT|LT|CH|INCR] score member [score member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd
  pub fn sorted_set_add<'a, D: Device>(
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
      // AofFail：ZADD 树内/信封写已生效、增量条目入队失败，撤帧后落错误帧
      // 拒绝（AofEnqueue 契约），禁计数帧假成功；不唤醒观察者（应答未确认）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        Ok(true)
      }
    }
  }

  /// ZREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemove
  pub fn sorted_set_remove<'a, D: Device>(
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
      // C# SortedSetRemove 仅回填 result1，整数回复由 RESP 层写出（补写臂单源）
      Rmw::Present(done) => write_rmw_reply(done, output),
      // AofFail：写已生效、入账失败，撤帧落错误帧拒绝（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
    }
    Ok(true)
  }

  /// ZINCRBY key increment member
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIncrement
  pub fn sorted_set_increment<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZINCRBY");

    zset_rmw_read_or_bail!(
      self,
      store,
      SortedSetOperation::Zincrby,
      parse_state,
      output
    )
  }

  /// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRemoveRange
  pub fn sorted_set_remove_range<'a, D: Device>(
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
      // AofFail：写已生效、入账失败，撤帧落错误帧拒绝（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      // ZREMRANGEBYLEX 仅回填 result1（哨兵语义见 write_lex_result）
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if range_kind == RemoveRangeKind::Lex && !payload_written {
          if result1 == BAD_RANGE {
            output.truncate(payload_start);
            cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
          } else if output.len() == payload_start {
            write_lex_result(result1, output);
          }
        }
      }
    }
    Ok(true)
  }

  /// ZRANGESTORE dst src min max \[BYSCORE|BYLEX\] \[REV\] \[LIMIT offset count\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRangeStore
  pub fn sorted_set_range_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4..=9, output, "ZRANGESTORE");

    let dst_key = parse_state[0];
    let src_key = parse_state[1];

    // 源集合范围读取（Store 选项：强制 WITHSCORES + RESP2 成对负载）
    let mut src_obj = zset_load_or_bail!(store, src_key, output, {
      // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）——
      // 回收臂即 STORE 覆写族目标键双保护·同步档单源 store_overwrite（持目标
      // 键窗并按开窗时刻存活域复验，对面 DEL/SET 交叠即拒写降级）
      if let Some(ret) = store_overwrite(store, dst_key, &SortedSetObject::new(), output).terminal()
      {
        return Ok(ret);
      }
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });

    // 解析消费非回显：负载挂本地 sink（错误臂冷路径透传一次）
    let mut sink = Vec::new();
    let result1 = run_operate(
      &mut src_obj,
      SortedSetOperation::Zrange,
      &parse_state[2..],
      0,
      SortedSetRangeOpts::STORE.bits() as i32,
      self.resp_protocol_version,
      &mut sink,
    )
    .result1;

    // result1 = -1 表示范围参数被拒（错误已写入负载）
    if result1 == -1 {
      output.append(&mut sink);
      return Ok(true);
    }

    // 成对负载 → 目标集合（from_entries：双索引 + 内存记账，ProcessRespArrayOutputAsPairs 语义）
    let dst = SortedSetObject::from_entries(parse_pairs_payload(&sink));

    // STORE 族目标键为 SET 语义（清既有 key 级 TTL，对标 C# SortedSetRangeStore
    // 先统一面 Delete dst 再 RMW ZADD，ObjectStore/SortedSetOps.cs）：信封域
    // upsert 默认保留 TTL，故写回成功后随写显式清退；覆写族目标键双保护·同步档
    //（持目标键窗跨「写回 → 清退」、落笔前复验归属）与三序纪律（写回先行、清退
    // 随后、失败臂零清退即原态）皆收口于 store_overwrite 单源
    //（票 zcode-r122c-setstore1 / load-type-rmw-window zset 留尾）
    if let Some(ret) = store_overwrite(store, dst_key, &dst, output).terminal() {
      return Ok(ret);
    }
    output.write_resp_int(dst.sorted_set_dict.len() as i64);
    self.notify_collection_update(dst_key);
    Ok(true)
  }

  /// ZDIFF numkeys key [key ...] \[WITHSCORES\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetDifference
  pub fn sorted_set_difference<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((keys, with_scores)) = parse_diff_args(parse_state, "ZDIFF", true, output) else {
      return Ok(true);
    };

    // 第一集合 − 其余集合（CopyDiff 逐个收缩，复用 diff_sets）
    let objs = load_many_or_bail!(store, keys, output);
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
  pub fn sorted_set_difference_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3.., output, "ZDIFFSTORE");

    let dst = parse_state[0];
    let Some((keys, _)) = parse_diff_args(&parse_state[1..], "ZDIFFSTORE", false, output) else {
      return Ok(true);
    };

    let objs = load_many_or_bail!(store, keys, output);
    let mut result = diff_sets(&objs);
    let count = result.purge_expired_len();
    // STORE 族 SET 语义 + 目标键双保护·同步档：收口于 store_overwrite 单源
    //（与 ZRANGESTORE / Z*STORE 同径，票 zcode-r122c-setstore1、
    // load-type-rmw-window zset 留尾）
    if let Some(ret) = store_overwrite(store, dst, &result, output).terminal() {
      return Ok(ret);
    }
    output.write_resp_int(count as i64);
    self.notify_collection_update(dst);
    Ok(true)
  }

  fn sorted_set_combine<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    cmd_name: &str,
    kind: CombineKind,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_combine_args(parse_state, cmd_name, true, output) else {
      return Ok(true);
    };

    let objs = load_many_or_bail!(store, args.keys, output);
    let result = combine_sets(&objs, &args.weights, args.aggregate, kind);
    write_zset_entries(
      Some(&result),
      args.with_scores,
      output,
      self.resp_protocol_version,
    );
    Ok(true)
  }

  /// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] \[WITHSCORES\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersect
  pub fn sorted_set_intersect<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.sorted_set_combine(parse_state, store, output, "ZINTER", CombineKind::Intersect)
  }

  /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectLength
  pub fn sorted_set_intersect_length<'a, D: Device>(
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
    // LIMIT 仅正值参与钳制（0 与未给同效），换算收口 intersect_card 单源
    let mut objs = load_many_or_bail!(store, args.keys, output);
    output.write_resp_int(intersect_card(&mut objs, args.limit));
    Ok(true)
  }

  /// ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetIntersectStore
  pub fn sorted_set_intersect_store<'a, D: Device>(
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
  pub fn sorted_set_union<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.sorted_set_combine(parse_state, store, output, "ZUNION", CombineKind::Union)
  }

  /// ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetUnionStore
  pub fn sorted_set_union_store<'a, D: Device>(
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
  pub fn sorted_set_random_member<'a, D: Device>(
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

    let mut obj = zset_load_or_bail!(store, key, output, {
      write_random_member_missing(output, args.included_count, self.resp_protocol_version);
      return Ok(true);
    });

    run_operate(
      &mut obj,
      SortedSetOperation::Zrandmember,
      &[],
      args.arg1,
      fastrand::i32(..),
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// ZEXPIRE / ZEXPIREAT / ZPEXPIRE / ZPEXPIREAT key seconds [NX|XX|GT|LT] MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetExpire
  pub fn sorted_set_expire<'a, D: Device>(
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
      // AofFail：出账/持久化写已生效、入账失败，撤帧落错误帧拒绝
      //（AofEnqueue 契约；ZEXPIRE/ZPERSIST 为写臂，禁 `_` 吞态冒答成功）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        Ok(true)
      }
      _ => Ok(true),
    }
  }

  /// ZTTL / ZPTTL / ZEXPIRETIME / ZPEXPIRETIME key MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetTimeToLive
  pub fn sorted_set_time_to_live<'a, D: Device>(
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
      // AofFail：出账/持久化写已生效、入账失败，撤帧落错误帧拒绝
      //（AofEnqueue 契约；ZEXPIRE/ZPERSIST 为写臂，禁 `_` 吞态冒答成功）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        Ok(true)
      }
      _ => Ok(true),
    }
  }

  /// ZPERSIST key MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPersist
  pub fn sorted_set_persist<'a, D: Device>(
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
      // AofFail：出账/持久化写已生效、入账失败，撤帧落错误帧拒绝
      //（AofEnqueue 契约；ZEXPIRE/ZPERSIST 为写臂，禁 `_` 吞态冒答成功）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        Ok(true)
      }
      _ => Ok(true),
    }
  }
}

/// ZINTERSTORE / ZUNIONSTORE 公共入口
fn sorted_set_combine_store<'s, D: Device>(
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
  let Some(args) = parse_combine_args(&parse_state[1..], name, false, output) else {
    return Ok(true);
  };

  let objs = load_many_or_bail!(store, args.keys, output);
  let mut result = combine_sets(&objs, &args.weights, args.aggregate, kind);
  let count = result.purge_expired_len();
  // STORE 族 SET 语义 + 目标键双保护·同步档：收口于 store_overwrite 单源
  //（与 ZDIFFSTORE / ZRANGESTORE 同径，票 zcode-r122c-setstore1、
  // load-type-rmw-window zset 留尾）
  if let Some(ret) = store_overwrite(store, dst, &result, output).terminal() {
    return Ok(ret);
  }
  output.write_resp_int(count as i64);
  notify(dst);
  Ok(true)
}

/// ZDIFF 参数解析：numkeys key... [WITHSCORES]
///
/// `allow_with_scores` 为 false 时为 STORE 严格模式（ZDIFFSTORE，对标 C#
/// SortedSetCommands.cs:1023 `parseState.Count - 2 != nKeys` 恒报语法错误，
/// 绝无 WITHSCORES 分支）
pub(crate) fn parse_diff_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  allow_with_scores: bool,
  output: &mut Vec<u8>,
) -> Option<(&'p [&'p [u8]], bool)> {
  check_arg_count!(parse_state, 2.., output, name, return None);

  let n_keys = parse_i32_arg(parse_state[0], output)?;
  let Ok(n) = usize::try_from(n_keys) else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  };

  let remaining = parse_state.len() - 1;
  if remaining != n && !(allow_with_scores && remaining == n + 1) {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  };

  let mut with_scores = false;
  // 尾词元只可能是 WITHSCORES（多出一参的形态已由上方计数门住）
  if remaining > n {
    if !parse_state[remaining].eq_ignore_ascii_case(cs::WITHSCORES) {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
    with_scores = true;
  }

  Some((&parse_state[1..1 + n], with_scores))
}

/// ZINTER/ZUNION 参数解析：numkeys key... [WEIGHTS w...] [AGGREGATE agg] [WITHSCORES]
///
/// `allow_with_scores` 为 false 时为 STORE 严格模式（ZINTERSTORE/ZUNIONSTORE，
/// 对标 C# SortedSetCommands.cs:1282-1322 / :1506-1545 选项循环仅识别
/// WEIGHTS 与 AGGREGATE，其他词元含 WITHSCORES 恒报语法错误）
pub(crate) fn parse_combine_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  allow_with_scores: bool,
  output: &mut Vec<u8>,
) -> Option<CombineArgs<'p>> {
  check_arg_count!(parse_state, 2.., output, name, return None);

  let n_keys = parse_i32_arg(parse_state[0], output)?;
  if n_keys < 1 {
    // 对标 C# SortedSetCommands.cs:AbortWithErrorMessage(GenericErrAtLeastOneKey, name)
    cs::abort_with_at_least_one_key(output, name);
    return None;
  }
  // n_keys ≥ 1，窄化无损
  let n = n_keys as usize;
  if parse_state.len().saturating_sub(1) < n {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }

  let keys: &'p [&'p [u8]] = &parse_state[1..=n];
  let mut weights = Vec::new();
  let mut aggregate = ZSetAggregate::Sum;
  let mut with_scores = false;

  // 选项段单次前向扫描（游标单源，见 OptCursor）
  let mut cur = OptCursor::new(&parse_state[1 + n..]);
  while !cur.done() {
    if cur.eat(cs::WEIGHTS) {
      // C# 两段式：先判数量够不够（:1097/:1289/:1388/:1512），再逐值判浮点
      let Some(raw) = cur.take(keys.len()) else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      };
      let Some(parsed) = raw
        .iter()
        .map(|&w| strict_f64(w, true))
        .collect::<Option<Vec<_>>>()
      else {
        // C# GenericErrNotAFloat 替换 {0}="weight"（SortedSetCommands.cs:1107）
        cs::abort_with_error_message(output, cs::GENERIC_ERR_NOT_A_FLOAT_WEIGHT);
        return None;
      };
      weights = parsed;
    } else if cur.eat(b"AGGREGATE") {
      let Some(agg) = cur.one().and_then(try_get_sorted_set_aggregate_type) else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      };
      aggregate = agg;
    } else if allow_with_scores && cur.eat(cs::WITHSCORES) {
      with_scores = true;
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
  store: &wkv::BatchStoreSession<impl Device>,
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

/// ZINTERCARD 基数内核（快慢路径共用）：只算基数不建结果集——单键走堆序 purge
/// 计数；多键以最小集合为遍历基准、余集逐个查集，命中 limit 提前跳出，彻底消除
/// 字典 clone 与成员字符串分配（空间 O(1)）。`limit` 仅正值参与钳制
///（0 与未给同效）
pub(crate) fn intersect_card(objs: &mut [SortedSetObject], limit: Option<i32>) -> i64 {
  let limit = limit.filter(|&v| v > 0);
  let card = match objs.len() {
    0 => 0,
    1 => objs[0].purge_expired_len() as i64,
    _ => {
      let Some((min_idx, min_obj)) = objs
        .iter()
        .enumerate()
        .min_by_key(|(_, o)| o.sorted_set_dict.len())
      else {
        return 0;
      };
      if min_obj.sorted_set_dict.is_empty() {
        return 0;
      }
      // 过期成员已由装载口 purge 剔净，遍历即存活视图
      let others = objs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != min_idx)
        .map(|(_, o)| &o.sorted_set_dict);
      let mut count = 0_i64;
      for member in min_obj.sorted_set_dict.keys() {
        if others.clone().all(|dict| dict.contains_key(member)) {
          count += 1;
          if limit.is_some_and(|v| count >= i64::from(v)) {
            break;
          }
        }
      }
      count
    }
  };
  limit.map_or(card, |v| card.min(i64::from(v)))
}

/// ZLEXCOUNT / ZREMRANGEBYLEX 的 result1 收尾应答（对象层不写负载、RESP 层落帧；
/// 快慢路径共用）：范围参数非法（`BAD_RANGE`）→ min/max 错误帧，
/// 部分执行（`PARTIAL`）→ 静默零应答
pub(crate) fn write_lex_result(result1: i64, output: &mut Vec<u8>) {
  if result1 == BAD_RANGE {
    cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
  } else if result1 != PARTIAL {
    output.write_resp_int(result1);
  }
}

/// 字典 → 有序集合（保持 (score, member) 双索引）
fn dict_to_zset(dict: HashMap<Vec<u8>, f64>) -> SortedSetObject {
  let entries: Vec<(Vec<u8>, f64)> = dict.into_iter().collect();
  SortedSetObject::from_entries(entries)
}

/// ZDIFF 语义计算：单次遍历 first，过滤掉在 rest 中出现的项，消灭多轮复制与重构
/// libs/server/Objects/SortedSet/SortedSetObject.cs:CopyDiff
///
/// 单键形态同样经 entries 重建（票 zcode-r15-zset 发现四）：C# CopyDiff 产物为
/// 纯 Dictionary（SortedSetObject.cs:537，成员级过期不随行），clone 会连带
/// ExpiryLedger 使 ZDIFFSTORE 单键落键的成员级 TTL 非法随行；first 已由
/// load_many 装载口 purge 过期，重建即存活视图，与多键形态及其余三个 STORE
/// 命令同口径
pub(crate) fn diff_sets(objs: &[SortedSetObject]) -> SortedSetObject {
  let Some((first, rest)) = objs.split_first() else {
    return SortedSetObject::default();
  };
  if rest.is_empty() {
    let entries: Vec<(Vec<u8>, f64)> = first
      .sorted_set
      .iter()
      .map(|e| (e.member.to_vec(), e.score))
      .collect();
    return SortedSetObject::from_entries(entries);
  }
  let entries: Vec<(Vec<u8>, f64)> = first
    .sorted_set
    .iter()
    .filter(|e| {
      !rest
        .iter()
        .any(|o| o.sorted_set_dict.contains_key(&e.member))
    })
    .map(|e| (e.member.to_vec(), e.score))
    .collect();
  SortedSetObject::from_entries(entries)
}

/// 集合运算分值特值归一：NaN → +0.0
///
/// 依据 doc/zh/deviations.md §2 输入侧恒拒 nan 词形之不变式延伸（§2 保证入集分值恒非 NaN，
/// 聚合产 NaN 只来自 0×±inf 之类运算）：
/// 交集侧的聚合步归一同时逐字复刻 C#
/// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetIntersection:1572-1577
/// （`aggregateType switch` 之后紧跟 `if (double.IsNaN(pairs[kvp.Key])) pairs[kvp.Key] = 0;`，
/// 源码自陈 "Arguably we're doing bug compatible behaviour"，对位 Redis
/// zunionInterGenericCommand 的 isnan 归零）。
///
/// 归一点即工单枚举的五处产点——交集 seed 加权、交集逐步加权、交集聚合结果，
/// 并集加权与并集聚合结果。C# 仅在交集聚合步有此检查（并集与乘积点均无），
/// 本仓扩到全产点，属已登记的刻意偏差（doc/zh/deviations.md §104 a，严禁按 C# 回改）；
/// 聚合器 `ZSetAggregate::apply`
/// 保持纯运算，集合不变式只由写入路径承担
#[inline(always)]
fn nan0(v: f64) -> f64 {
  if v.is_nan() { 0.0 } else { v }
}

/// ZINTER/ZUNION 语义计算（权重 + 聚合）
///
/// 分值经 [`nan0`] 归一后入集，NaN 恒不落 entries/combined
pub(crate) fn combine_sets(
  objs: &[SortedSetObject],
  weights: &[f64],
  aggregate: ZSetAggregate,
  kind: CombineKind,
) -> SortedSetObject {
  if kind == CombineKind::Intersect {
    // 交集：任何一个集合为空，结果必为空；
    // 选出最小集合作为基准遍历，仅克隆最终交集元素，消灭临时 HashMap 与无效克隆
    // ——最小集仅作遍历基准、逐集合查集，消除 C# 交集步 foreach 体内
    // pairs.Remove 迭代删字典必抛 InvalidOperationException 掐连接的异常面，
    // 系已登记的防御性偏离（doc/zh/deviations.md §104 b，严禁按 C# 回改）
    if objs.is_empty() || objs.iter().any(|o| o.sorted_set_dict.is_empty()) {
      return SortedSetObject::default();
    }
    let min_obj = objs
      .iter()
      // 选最小集合仅决定遍历顺序（基数直读 raw len；过期成员已由
      // load_many 装载口堆序 purge 剔净，遍历即存活视图）
      .min_by_key(|o| o.sorted_set_dict.len())
      .unwrap();

    let mut entries = Vec::new();
    'outer: for member in min_obj.sorted_set_dict.keys() {
      // C# SortedSetIntersection（SortedSetOps.cs:1521-1585）恒以 keys[0] 加权
      // 分值为种子、按索引序逐步累加聚合（IEEE 754 加法无结合律，累加序须与
      // C# 键序同序，如三键 {1e308, 1, -1e308} SUM 应得 0）；最小集仅作成员
      // 遍历基准（空间优化不动），命中判定逐集合查集，objs[0] 缺席即非交集
      let Some(&score0) = objs[0].sorted_set_dict.get(member) else {
        continue 'outer;
      };
      let mut agg_score = nan0(score0 * weights.first().copied().unwrap_or(1.0));
      for (i, obj) in objs.iter().enumerate().skip(1) {
        let Some(&score) = obj.sorted_set_dict.get(member) else {
          continue 'outer;
        };
        let w = weights.get(i).copied().unwrap_or(1.0);
        agg_score = nan0(aggregate.apply(agg_score, nan0(score * w)));
      }
      entries.push((member.to_vec(), agg_score));
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
      let weighted = nan0(*score * weight);
      match combined.get_mut(member.as_ref()) {
        Some(existing) => {
          *existing = nan0(aggregate.apply(*existing, weighted));
        }
        None => {
          combined.insert(member.to_vec(), weighted);
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
    output.write_resp_array_len(0);
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
    if with_scores && is_resp3(resp_version) {
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
