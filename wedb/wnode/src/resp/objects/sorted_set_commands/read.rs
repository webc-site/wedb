//! 有序集合只读命令（ZRANGE/ZRANK/ZREVRANK/ZSCORE/ZMSCORE/ZCOUNT/ZLEXCOUNT/ZCARD，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 查询与范围读取命令段）

use wcol::zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{Rmw, ZsetLoad, parse_rank_with_score, run_operate, zset_load_sync};
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
      // 信封水位越线（成员级 TTL 已有成员到期，头部计数失真）：落物化 Zcard
      // 矫正臂（臂内复核仍降级——分层态/磁盘候选/rmw 窗口争用——转异步慢路径）
      ObjLoad::Degrade => self.sorted_set_length_purged(store, key, output),
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

  /// ZCARD 物化矫正臂：rmw 通道执行 Zcard——对象层
  /// [`SortedSetObject::purge_expired_len`] 堆序惰性剔除为唯一剔除内核
  ///（collection.md §6.3），剔除实际发生经 mutated_by_ttl 升格写回一次矫正
  /// 并触发删空自愈
  fn sorted_set_length_purged<'a, D: wdev::Device>(
    &self,
    store: &wkv::BatchStoreSession<'a, D>,
    key: &[u8],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    match self.zset_rmw(
      store,
      key,
      SortedSetOperation::Zcard,
      &[] as &[&[u8]],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => Ok(false),
      Rmw::WrongType => Ok(true),
      // C# NOTFOUND → :0
      Rmw::Missing => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        Ok(true)
      }
      Rmw::Present(done) => {
        if !done.payload_written {
          output.write_resp_int(done.result1);
        }
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

    // WITHSCORE 词元推导单源（快慢共用，失败帧已写出；仅 len==3 校验，
    // len>3 静默忽略）
    let Some(with_score) = parse_rank_with_score(cmd_name, parse_state, output) else {
      return Ok(true);
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
      i32::from(with_score),
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }
}
