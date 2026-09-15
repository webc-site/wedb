//! 有序集合 RESP 命令（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::zset::sorted_set_object::SortedSetObject`] 的
//! operate/ObjectInput 通道（与 C# GarnetObjectBase.Operate 分层一致），
//! 存取经与 storage 会话域共享的 `[类型标签][载荷]` 信封
//! （见 [`crate::resp::objects::object_store_utils`]）。

use std::str;

use gxhash::HashMap;
use itoa::{self, Buffer as ItoaBuffer};
use memchr::memmem;
use wbase::num::{strict_f64, strict_i32, strict_i64};
use wcol::{
  types::object_output::ObjectOutput,
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation, SortedSetRangeOpts},
};
use wresp::{
  ExpirationWithOption, ExpireOption, RespCommand, RespSliceExt, RespVecExt,
  SortedSetAggregateType as ZSetAggregate, check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  format_double, try_get_expire_option,
};
use wval::GarnetObjectType;
use zmij::Buffer;

use crate::{
  resp::{
    objects::object_store_utils::{
      ElementHeaderKind, ObjLoad, RespRmwOutcome, SyncRmwCmd, SyncRmwHandlers, make_object_input,
      obj_load_typed_sync, obj_save_or_gc, parse_elements_header, run_sync_rmw, zset_from_blob,
      zset_to_blob,
    },
    resp_server_session::RespServerSession,
  },
  session_parse_state_extensions::try_get_timeout_bytes,
};

pub(crate) type ZsetLoad = ObjLoad<SortedSetObject>;
type Rmw = RespRmwOutcome;

fn write_popped_pairs(key: &[u8], popped: &[(f64, Vec<u8>)], output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  output.write_resp_array_len(popped.len());
  let mut buf = Buffer::new();
  for (score, member) in popped {
    output.write_resp_array_len(2);
    output.write_resp_bulk_string(member);
    output.write_resp_bulk_string(format_double(*score, &mut buf).as_bytes());
  }
}

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
fn run_operate(
  obj: &mut SortedSetObject,
  op: SortedSetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
) -> ObjectOutput {
  let input = make_object_input(GarnetObjectType::SortedSet, op as u8, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, resp_version);
  obj_out
}

/// 同步装载有序集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn zset_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ZsetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::SortedSet as u8,
    output,
    zset_from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与 set 命令域收尾）
#[inline]
pub(crate) fn zset_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SortedSetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::SortedSet as u8,
    obj,
    obj.sorted_set_dict.is_empty(),
    zset_to_blob,
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作（ZREM/ZREMRANGEBYLEX）以移除计数为准。
fn should_write_back(
  op: SortedSetOperation,
  out: &ObjectOutput,
  obj: &SortedSetObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.has_wrong_type()
    || out.payload.first() == Some(&b'-')
    || (!existed && obj.sorted_set_dict.is_empty())
  {
    return false;
  }
  match op {
    SortedSetOperation::Zrem | SortedSetOperation::Zremrangebylex => {
      out.result1 > 0 && out.result1 != i32::MAX as i64
    }
    _ => !out.payload.is_empty(),
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zcard
      | SortedSetOperation::Zscore
      | SortedSetOperation::Zmscore
      | SortedSetOperation::Zcount
      | SortedSetOperation::Zrange
      | SortedSetOperation::Zrank
      | SortedSetOperation::Zrevrank
      | SortedSetOperation::Zlexcount
      | SortedSetOperation::Zrandmember
      | SortedSetOperation::Zttl
      | SortedSetOperation::Zscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn zset_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: SortedSetOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    output: &mut Vec<u8>,
  ) -> Rmw {
    let (arg1, arg2) = args12;
    let resp_version = self.resp_protocol_version;
    run_sync_rmw(
      store,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::SortedSet as u8,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        zset_from_blob,
        SortedSetObject::new,
        |o: &SortedSetObject| o.sorted_set_dict.is_empty(),
        zset_to_blob,
        |obj, op, args| run_operate(obj, op, args, arg1, arg2, resp_version),
        should_write_back,
      ),
    )
  }
  /// ZADD key [NX|XX|GT|LT|CH|INCR] score member [score member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd
  pub fn sorted_set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 3, output, "ZADD");

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
      Rmw::Error => Ok(true),
      // 写成功 → 唤醒该键的阻塞观察者（C# SortedSetAdd 后 HandleCollectionUpdate）
      Rmw::Done { .. } => {
        self.notify_collection_update(key);
        Ok(true)
      }
    }
  }

  /// ZSCORE key member
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScore
  pub fn sorted_set_score<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "ZSCORE");

    let key = parse_state[0];
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zscore,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
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
    check_arg_count!(parse_state, >= 2, output, "ZREM");

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
      Rmw::Error => {}
      // C# SortedSetRemove 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// ZCARD key
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLength
  pub fn sorted_set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "ZCARD");

    let key = parse_state[0];
    match self.zset_rmw(store, key, SortedSetOperation::Zcard, &[], (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // C# SortedSetLength 仅回填 result1
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// ZPOPMIN / ZPOPMAX key \[count\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetPop
  pub fn sorted_set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      parse_state,
      1..=2,
      output,
      if is_min { "ZPOPMIN" } else { "ZPOPMAX" }
    );

    let key = parse_state[0];
    let op = if is_min {
      SortedSetOperation::Zpopmin
    } else {
      SortedSetOperation::Zpopmax
    };

    // count 缺省形态传 -1（无外层数组头）
    let arg1: i32 = match parse_state.get(1) {
      None => -1,
      Some(c) => match strict_i32(c) {
        Some(v) if v >= 0 => v,
        _ => {
          // C# popCount < 0 → RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE（含句点）
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
      },
    };

    match self.zset_rmw(store, key, op, &[], (arg1, 0), output) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRange
  pub fn sorted_set_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    range_opts: SortedSetRangeOpts,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 3, output, "ZRANGE");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.extend_from_slice(b"*0\r\n");
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zrange,
      &parse_state[1..],
      0,
      range_opts.bits() as i32,
      self.resp_protocol_version,
    );
    output.extend_from_slice(&obj_out.payload);
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
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）
        match zset_save_or_gc(store, dst_key, &SortedSetObject::new()) {
          Ok(true) => output.extend_from_slice(b":0\r\n"),
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

    match zset_save_or_gc(store, dst_key, &dst) {
      Ok(true) => {
        let mut buf = ItoaBuffer::new();
        output.extend_from_slice(b":");
        output.extend_from_slice(buf.format(dst.sorted_set_dict.len()).as_bytes());
        output.extend_from_slice(b"\r\n");
        self.notify_collection_update(dst_key);
      }
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// ZMSCORE key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScores
  pub fn sorted_set_scores<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 2, output, "ZMSCORE");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 键缺失：全 null 数组
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in 1..parse_state.len() {
          output.write_resp_null();
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zmscore,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetMPop
  pub fn sorted_set_m_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 3, output, "ZMPOP");

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    // C# 校验序：numkeys < 1 → NOT_INTEGER；参数不足以容纳 numkeys+MIN/MAX → SYNTAX_ERROR
    if num_keys < 1 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    }
    if parse_state.len() < num_keys as usize + 2 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let order_arg = parse_state[num_keys as usize + 1];
    let low_scores_first = if order_arg.eq_ignore_ascii_case(b"MIN") {
      true
    } else if order_arg.eq_ignore_ascii_case(b"MAX") {
      false
    } else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let mut count = 1_i64;
    if parse_state.len() > num_keys as usize + 2 {
      if parse_state.len() != num_keys as usize + 4
        || !parse_state[num_keys as usize + 2].eq_ignore_ascii_case(b"COUNT")
      {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      match parse_state[num_keys as usize + 3].try_parse_i64() {
        Some(v) if v >= 1 => count = v,
        _ => {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        }
      }
    }

    // 逐键尝试弹出第一个非空集合
    for key in &parse_state[1..=num_keys as usize] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if obj.count() == 0 {
        continue;
      }

      let max_k = (count as usize).min(obj.count());
      let mut popped = Vec::with_capacity(max_k);
      for _ in 0..count {
        if let Some(pair) = obj.pop_min_or_max(!low_scores_first) {
          popped.push(pair);
        } else {
          break;
        }
      }
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }

      // 回复：[key, [[member, score], ...]]
      write_popped_pairs(key, &popped, output);
      return Ok(true);
    }

    output.write_resp_null();
    Ok(true)
  }

  /// ZCOUNT key min max
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetCount
  pub fn sorted_set_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZCOUNT");

    let key = parse_state[0];
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zcount,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }

  /// ZLEXCOUNT key min max
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLengthByValue
  pub fn sorted_set_length_by_value<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZLEXCOUNT");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.extend_from_slice(b":0\r\n");
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zlexcount,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
    );
    // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
    // （C# SortedSetRemoveOrCountRangeByLex 仅回填 result1，RESP 层负责写整数）
    if obj_out.result1 == i32::MAX as i64 {
      output.clear();
      output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
    } else if obj_out.result1 != i32::MIN as i64 {
      output.clear();
      output.write_resp_int(obj_out.result1);
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

  /// ZRANK / ZREVRANK key member \[WITHSCORE\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRank
  pub fn sorted_set_rank<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    ascending: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 2, output, "ZRANK");

    let key = parse_state[0];
    let with_score = parse_state.len() > 2 && parse_state[2].eq_ignore_ascii_case(b"WITHSCORE");

    let op = if ascending {
      SortedSetOperation::Zrank
    } else {
      SortedSetOperation::Zrevrank
    };

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        output.write_resp_null();
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let obj_out = run_operate(
      &mut obj,
      op,
      &parse_state[1..2],
      if with_score { 1 } else { 0 },
      0,
      self.resp_protocol_version,
    );
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
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
      Rmw::Error => return Ok(true),
      // ZREMRANGEBYLEX 仅回填 result1（int.MaxValue=参数错误、int.MinValue=部分执行）
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if range_kind == RemoveRangeKind::Lex && !payload_written {
          if result1 == i32::MAX as i64 {
            output.truncate(payload_start);
            output.extend_from_slice(b"-ERR min or max not valid string range item\r\n");
          } else if output.len() == payload_start && result1 != i32::MIN as i64 {
            output.write_resp_int(result1);
          }
        }
      }
    }
    Ok(true)
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
    check_arg_count!(parse_state, 1..=3, output, "ZRANDMEMBER");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        if parse_state.len() > 1 {
          output.extend_from_slice(b"*0\r\n");
        } else {
          output.write_resp_null();
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    // 参数打包：arg1 = (count << 1 | includedCount) << 1 | withScores
    // C# paramCount 缺省为 1（ZRANDMEMBER key 回 1 个成员）
    let mut param_count = 1_i64;
    let mut included_count = false;
    let mut with_scores = false;

    if let Some(c) = parse_state.get(1) {
      let Some(v) = c.try_parse_i64() else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      param_count = v.min(i32::MAX as i64 >> 2);
      included_count = true;

      if let Some(ws) = parse_state.get(2) {
        if !ws.eq_ignore_ascii_case(b"WITHSCORES") {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        with_scores = true;
      }
    }

    let arg1 = (((param_count << 1) | included_count as i64) << 1) | with_scores as i64;
    // count = 0 不触达后端（对齐 C#）
    if param_count == 0 {
      output.extend_from_slice(b"*0\r\n");
      return Ok(true);
    }

    let obj_out = run_operate(
      &mut obj,
      SortedSetOperation::Zrandmember,
      &[],
      arg1 as i32,
      fastrand::i32(..),
      self.resp_protocol_version,
    );
    output.extend_from_slice(&obj_out.payload);
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

    write_zset_entries(Some(&result), with_scores, output);
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
    check_arg_count!(parse_state, >= 3, output, "ZDIFFSTORE");

    let dst = parse_state[0];
    let Some((keys, _)) = parse_diff_args(&parse_state[1..], "ZDIFFSTORE", output) else {
      return Ok(true);
    };

    let objs = match load_many(store, &keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };
    let result = diff_sets(&objs);
    let count = result.count();
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
    write_zset_entries(Some(&result), args.with_scores, output);
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
    check_arg_count!(parse_state, >= 2, output, "ZINTERCARD");

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if num_keys < 1 {
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    }

    let mut limit = 0_i64;
    let idx = num_keys as usize + 1;
    if parse_state.len() == idx + 2 {
      if !parse_state[idx].eq_ignore_ascii_case(b"LIMIT") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      match parse_state[idx + 1].try_parse_i64() {
        Some(v) if v >= 0 => limit = v,
        _ => {
          output.extend_from_slice(b"-ERR limit is negative\r\n");
          return Ok(true);
        }
      }
    } else if parse_state.len() != idx {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let keys = &parse_state[1..=num_keys as usize];
    let objs = match load_many(store, keys, output) {
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
    } else if let Some((min_idx, min_obj)) = objs.iter().enumerate().min_by_key(|(_, o)| o.count())
    {
      if min_obj.count() == 0 {
        0
      } else {
        let other_objs: Vec<_> = objs
          .iter()
          .enumerate()
          .filter_map(|(i, o)| {
            if i != min_idx {
              Some(&o.sorted_set_dict)
            } else {
              None
            }
          })
          .collect();
        let mut count = 0_i64;
        for member in min_obj.sorted_set_dict.keys() {
          if other_objs.iter().all(|dict| dict.contains_key(member)) {
            count += 1;
            if limit > 0 && count >= limit {
              break;
            }
          }
        }
        count
      }
    } else {
      0
    };
    output.write_resp_int(if limit > 0 { card.min(limit) } else { card });
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
    write_zset_entries(Some(&result), args.with_scores, output);
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

  /// BZPOPMIN / BZPOPMAX key [key ...] timeout
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingPop
  ///
  /// 经纪注入时挂起等待（立即试取由经纪主循环 InitializeObserver 承担）；
  /// 未注入经纪保留立即可取路径
  pub fn sorted_set_blocking_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    let command = if is_min {
      RespCommand::Bzpopmin
    } else {
      RespCommand::Bzpopmax
    };
    let cmd_name = if is_min { "BZPOPMIN" } else { "BZPOPMAX" };
    check_arg_count!(parse_state, >= 2, output, cmd_name);
    let timeout = match try_get_timeout_bytes(parse_state[parse_state.len() - 1]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };

    // 经纪挂起路径（C# GetCollectionItemAsync(command, keysBytes, this, timeout)）
    if self.park_broker_wait(
      command,
      timeout,
      || {
        parse_state[..parse_state.len() - 1]
          .iter()
          .map(|k| k.to_vec())
          .collect()
      },
      Vec::new,
    ) {
      return Ok(true);
    }

    // ---- 立即可取路径（经纪未注入的独立会话域）----
    for key in &parse_state[..parse_state.len() - 1] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if let Some((score, member)) = obj.pop_min_or_max(!is_min) {
        match zset_save_or_gc(store, key, &obj) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
        output.write_resp_array_len(3);
        output.write_resp_bulk_string(key);
        output.write_resp_bulk_string(&member);
        let s = ObjectOutput::format_double(score);
        output.write_resp_bulk_string(s.as_bytes());
        return Ok(true);
      }
    }

    output.write_resp_null();
    Ok(true)
  }

  /// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetBlockingMPop
  ///
  /// 经纪注入时挂起等待（cmd_args = [lowScoresFirst(1B), popCount(i32 LE 4B)]，
  /// C# SortedSetBlockingMPop 同编码）；未注入经纪保留立即可取路径
  pub fn sorted_set_blocking_m_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 4, output, "BZMPOP");

    let timeout = match try_get_timeout_bytes(parse_state[0]) {
      Ok(timeout) => timeout,
      Err(error) => {
        cs::abort_with_error_message(output, error);
        return Ok(true);
      }
    };
    let Some(num_keys) = parse_state[1].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if num_keys < 1 || parse_state.len() < num_keys as usize + 3 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    let order_arg = parse_state[num_keys as usize + 2];
    let low_scores_first = if order_arg.eq_ignore_ascii_case(b"MIN") {
      true
    } else if order_arg.eq_ignore_ascii_case(b"MAX") {
      false
    } else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let mut count = 1_i64;
    if parse_state.len() > num_keys as usize + 3 {
      if parse_state.len() != num_keys as usize + 5
        || !parse_state[num_keys as usize + 3].eq_ignore_ascii_case(b"COUNT")
      {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      match parse_state[num_keys as usize + 4].try_parse_i64() {
        Some(v) if v >= 1 => count = v,
        _ => {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        }
      }
    }

    // 经纪挂起路径（MIN/MAX 布尔 + count 编码进 cmd_args）
    if self.park_broker_wait(
      RespCommand::Bzmpop,
      timeout,
      || {
        parse_state[2..=num_keys as usize + 1]
          .iter()
          .map(|k| k.to_vec())
          .collect()
      },
      || {
        vec![
          vec![u8::from(low_scores_first)],
          (count as i32).to_le_bytes().to_vec(),
        ]
      },
    ) {
      return Ok(true);
    }

    // ---- 立即可取路径（经纪未注入的独立会话域）----
    for key in &parse_state[2..=num_keys as usize + 1] {
      let mut obj = match zset_load_sync(store, key, output) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => return Ok(true),
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };
      if obj.count() == 0 {
        continue;
      }

      let max_k = (count as usize).min(obj.count());
      let mut popped = Vec::with_capacity(max_k);
      for _ in 0..count {
        if let Some(pair) = obj.pop_min_or_max(!low_scores_first) {
          popped.push(pair);
        } else {
          break;
        }
      }
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }

      write_popped_pairs(key, &popped, output);
      return Ok(true);
    }

    output.write_resp_null();
    Ok(true)
  }

  /// ZEXPIRE / ZEXPIREAT / ZPEXPIRE / ZPEXPIREAT key seconds [NX|XX|GT|LT] MEMBERS nummembers member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetExpire
  pub fn sorted_set_expire<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 5, output, "ZEXPIRE");

    let key = parse_state[0];
    let Some(expiration_base) = strict_i64(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if expiration_base < 0 {
      cs::abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
      return Ok(true);
    }

    let mut curr_idx = 2;
    let mut expire_option = ExpireOption::NONE;
    if let Some(opt) = try_get_expire_option(parse_state[curr_idx]) {
      expire_option = opt;
      curr_idx = 3;
    }

    let Some((members_start, _num_members)) =
      parse_elements_header(parse_state, curr_idx, ElementHeaderKind::Members, output)
    else {
      return Ok(true);
    };

    // .NET Ticks 目标时刻（saturating，与 hash 域 compute_expiration_ticks 同口径）
    let expiration_ticks =
      Self::compute_expiration_ticks(expiration_base, is_milliseconds, is_timestamp);

    let e = ExpirationWithOption::new(expiration_ticks, expire_option);

    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zexpire,
      &parse_state[members_start..],
      ((e.word() >> 32) as i32, e.word() as i32),
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
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 4, output, "ZTTL");

    let key = parse_state[0];
    let Some((members_start, _num_members)) =
      parse_elements_header(parse_state, 1, ElementHeaderKind::Members, output)
    else {
      return Ok(true);
    };

    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zttl,
      &parse_state[members_start..],
      (
        if is_milliseconds { 1 } else { 0 },
        if is_timestamp { 1 } else { 0 },
      ),
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
    check_arg_count!(parse_state, >= 4, output, "ZPERSIST");

    let key = parse_state[0];
    let Some((members_start, _num_members)) =
      parse_elements_header(parse_state, 1, ElementHeaderKind::Members, output)
    else {
      return Ok(true);
    };

    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zpersist,
      &parse_state[members_start..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      _ => Ok(true),
    }
  }
}

/// ZREMRANGE 变体
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveRangeKind {
  Rank,
  Score,
  Lex,
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
  check_arg_count!(parse_state, >= 3, output, name);
  let dst = parse_state[0];
  let Some(args) = parse_combine_args(&parse_state[1..], name, output) else {
    return Ok(true);
  };

  let objs = match load_many(store, &args.keys, output) {
    Ok(Some(objs)) => objs,
    Ok(None) => return Ok(false),
    Err(()) => return Ok(true),
  };
  let result = combine_sets(&objs, &args.weights, args.aggregate, kind);
  let count = result.count();
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

/// 成对负载解析（ZRANGESTORE/GEOSEARCHSTORE 回读：member score member score ...）
///
/// 兼容两种形态：RESP2 扁平序列；GEOSEARCHSTORE 的每项前置 `*2` 嵌套数组头
pub(crate) fn parse_pairs_payload(payload: &[u8]) -> Vec<(Vec<u8>, f64)> {
  let mut pairs = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头 *<n>

  if payload.first() == Some(&b'*')
    && let Some(line_end) = find_crlf(payload, 0)
  {
    pos = line_end + 2;
  }

  while pos < payload.len() {
    // 项间嵌套数组头跳过（*<n>\r\n）
    if payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    // $<len>\r\n<bytes>\r\n
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let member = payload[start..end].to_vec();
    pos = end + 2;

    // 第二项：bulk string 形式的分值（前置嵌套数组头同样跳过）
    if pos < payload.len()
      && payload[pos] == b'*'
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
    if pos >= payload.len() || payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    let score = str::from_utf8(&payload[start..end])
      .unwrap_or("")
      .parse::<f64>()
      .unwrap_or(0.0);
    pos = end + 2;

    pairs.push((member, score));
  }
  pairs
}

fn find_crlf(payload: &[u8], from: usize) -> Option<usize> {
  let slice = payload.get(from..)?;
  memmem::find(slice, b"\r\n").map(|pos| from + pos)
}

/// ZDIFF 参数解析：numkeys key... [WITHSCORES]
fn parse_diff_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
) -> Option<(Vec<&'p [u8]>, bool)> {
  check_arg_count!(parse_state, >= 2, output, name, return None);

  let Some(n_keys) = parse_state[0].try_parse_i64() else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };

  if parse_state.len() - 1 != n_keys as usize && parse_state.len() - 1 != n_keys as usize + 1 {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }

  let mut with_scores = false;
  if parse_state.len() - 1 > n_keys as usize {
    let last = parse_state[parse_state.len() - 1];
    if !last.eq_ignore_ascii_case(b"WITHSCORES") {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
    with_scores = true;
  }

  Some((parse_state[1..=n_keys as usize].to_vec(), with_scores))
}

/// 集合运算参数解析产物
pub struct CombineArgs<'p> {
  pub keys: Vec<&'p [u8]>,
  pub weights: Vec<f64>,
  pub aggregate: ZSetAggregate,
  pub with_scores: bool,
}

/// ZINTER/ZUNION 参数解析：numkeys key... [WEIGHTS w...] [AGGREGATE agg] [WITHSCORES]
fn parse_combine_args<'p>(
  parse_state: &'p [&'p [u8]],
  name: &str,
  output: &mut Vec<u8>,
) -> Option<CombineArgs<'p>> {
  check_arg_count!(parse_state, >= 2, output, name, return None);

  let Some(n_keys) = parse_state[0].try_parse_i64() else {
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
    if token.eq_ignore_ascii_case(b"WEIGHTS") {
      idx += 1;
      let mut parsed = Vec::with_capacity(keys.len());
      while parsed.len() < keys.len() && idx < parse_state.len() {
        match strict_f64(parse_state[idx], true) {
          Some(w) => parsed.push(w),
          None => {
            // C# GenericErrNotAFloat 替换 {0}="weight"（SortedSetCommands.cs:1107）
            cs::abort_with_error_message(output, cs::GENERIC_ERR_NOT_A_FLOAT_WEIGHT);
            return None;
          }
        }
        idx += 1;
      }
      if parsed.len() != keys.len() {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
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
    } else if token.eq_ignore_ascii_case(b"WITHSCORES") {
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

/// 集合运算种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineKind {
  Intersect,
  Union,
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SortedSetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(None),
      ZsetLoad::Error => return Err(()),
      ZsetLoad::Missing => objs.push(SortedSetObject::new()),
      ZsetLoad::Present(o) => objs.push(o),
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
fn diff_sets(objs: &[SortedSetObject]) -> SortedSetObject {
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
fn combine_sets(
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
      .min_by_key(|(_, o)| o.count())
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
  let total_cap = objs.iter().map(|o| o.count()).sum();
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

/// 结果集合的 RESP 输出（WITHSCORES 双协议形态，RESP2 扁平）
fn write_zset_entries(obj: Option<&SortedSetObject>, with_scores: bool, output: &mut Vec<u8>) {
  let Some(obj) = obj else {
    output.extend_from_slice(b"*0\r\n");
    return;
  };

  // 有序视图流式写出：免 collect 与逐元素 clone
  let n = obj.sorted_set.len();
  output.write_resp_array_len(if with_scores { n * 2 } else { n });
  for e in &obj.sorted_set {
    output.write_resp_bulk_string(&e.member);
    if with_scores {
      let s = ObjectOutput::format_double(e.score);
      output.write_resp_bulk_string(s.as_bytes());
    }
  }
}
