//! 有序集合只读命令（ZRANGE/ZRANK/ZREVRANK/ZSCORE/ZMSCORE/ZCOUNT/ZLEXCOUNT/ZCARD，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 查询与范围读取命令段）

use wcol::zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{Rmw, ZsetLoad, run_operate, zset_load_sync};
use crate::resp::{
  objects::object_store_utils::{ObjLoad, obj_length_sync},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
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
    check_arg_count!(parse_state, 3.., output, "ZRANGE");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        output.write_resp_array_len(0);
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    run_operate(
      &mut obj,
      SortedSetOperation::Zrange,
      &parse_state[1..],
      0,
      range_opts.bits() as i32,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
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
    match obj_length_sync(store, key, GarnetObjectType::SortedSet, output) {
      ObjLoad::Degrade => Ok(false),
      ObjLoad::WrongType => Ok(true),
      // C# NOTFOUND → :0
      ObjLoad::Missing => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        Ok(true)
      }
      ObjLoad::Present(len) => {
        output.write_resp_int(len as i64);
        Ok(true)
      }
    }
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
    check_arg_count!(parse_state, 2.., output, "ZMSCORE");

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        // 键缺失：全 null 数组
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in &parse_state[1..] {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    run_operate(
      &mut obj,
      SortedSetOperation::Zmscore,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
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
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
    // （C# SortedSetRemoveOrCountRangeByLex 仅回填 result1，RESP 层负责写整数；
    // 对象层不写负载段，整数应答在挂载点之后落帧）
    let result1 = run_operate(
      &mut obj,
      SortedSetOperation::Zlexcount,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    )
    .result1;
    if result1 == i32::MAX as i64 {
      cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
    } else if result1 != i32::MIN as i64 {
      output.write_resp_int(result1);
    }
    Ok(true)
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
    let cmd_name = if ascending { "ZRANK" } else { "ZREVRANK" };
    check_arg_count!(parse_state, 2.., output, cmd_name);

    // C# 仅 Count==3 时校验 WITHSCORE（大小写不敏感，非法即 syntax error）；
    // Count>3 静默忽略多余参数（includeWithScore 保持 false）
    let with_score = if parse_state.len() == 3 {
      if parse_state[2].eq_ignore_ascii_case(cs::WITHSCORE) {
        true
      } else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    } else {
      false
    };

    let key = parse_state[0];

    let op = if ascending {
      SortedSetOperation::Zrank
    } else {
      SortedSetOperation::Zrevrank
    };

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        output.write_resp_null_ver(self.resp_protocol_version);
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    run_operate(
      &mut obj,
      op,
      &parse_state[1..2],
      if with_score { 1 } else { 0 },
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }
}
