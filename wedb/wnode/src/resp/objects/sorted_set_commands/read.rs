//! 有序集合只读命令（ZRANGE/ZRANK/ZREVRANK/ZSCORE/ZMSCORE/ZCOUNT/ZLEXCOUNT/ZCARD，
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 查询与范围读取命令段）

use wcol::zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, unpack_args},
  cmd_strings as cs,
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{Rmw, parse_rank_with_score, run_operate, write_lex_result, zset_load_sync};
use crate::resp::{
  objects::object_store_utils::{
    obj_length_sync, reply_obj_length, write_rmw_aof_fail_frame, write_rmw_reply,
  },
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGEBYLEX
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRange
  pub fn sorted_set_range<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    range_opts: SortedSetRangeOpts,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3.., output, "ZRANGE");

    let key = parse_state[0];
    let mut obj = zset_load_or_bail!(store, key, output, {
      output.write_resp_array_len(0);
      return Ok(true);
    });

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
  pub fn sorted_set_score<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "ZSCORE");

    zset_rmw_read_or_bail!(self, store, SortedSetOperation::Zscore, parse_state, output)
  }

  /// ZCARD key
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLength
  pub fn sorted_set_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "ZCARD") else {
      return Ok(true);
    };
    reply_obj_length(
      obj_length_sync(store, key, GarnetObjectType::SortedSet, output),
      output,
      |out| self.sorted_set_length_purged(store, key, out),
    )
  }

  /// ZCARD 物化矫正臂：rmw 通道执行 Zcard——对象层
  /// [`SortedSetObject::purge_expired_len`] 堆序惰性剔除为唯一剔除内核
  ///（collection.md §6.3），剔除实际发生经 mutated_by_ttl 升格写回一次矫正
  /// 并触发删空自愈
  fn sorted_set_length_purged<'a, D: Device>(
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
        write_rmw_reply(done, output);
        Ok(true)
      }
      // AofFail：到期剔除写已生效、入账失败，撤帧落错误帧拒绝（AofEnqueue
      // 契约；ZCARD 剔除升格写臂可至本态，禁假成功）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        Ok(true)
      }
    }
  }

  /// ZMSCORE key member [member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetScores
  pub fn sorted_set_scores<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "ZMSCORE");

    let key = parse_state[0];
    let mut obj = zset_load_or_bail!(store, key, output, {
      // 键缺失：全 null 数组
      output.write_resp_array_len(parse_state.len() - 1);
      for _ in &parse_state[1..] {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      return Ok(true);
    });

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
  ///
  /// 装载先行（与同文件 ZLEXCOUNT 同形）：C# 键缺席经 ReadObjectStoreOperation
  ///（SortedSetOps.cs:984 → Common.cs:56）Tsavorite Read 回调不执行，对象层
  /// SortedSetCount（SortedSetObjectImpl.cs:286）的 min/max 解析不运行，
  /// NOTFOUND 直写 :0——故 Missing 短路回 :0，Present 才 run_operate 求值
  ///（键存在 + 非法参数仍回错误帧，判定序对齐真 Redis zcountCommand
  /// lookupKeyReadOrReply 缺键先回 0）
  pub fn sorted_set_count<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZCOUNT");

    let key = parse_state[0];
    let mut obj = zset_load_or_bail!(store, key, output, {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });

    run_operate(
      &mut obj,
      SortedSetOperation::Zcount,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// ZLEXCOUNT key min max
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetLengthByValue
  pub fn sorted_set_length_by_value<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "ZLEXCOUNT");

    let key = parse_state[0];
    let mut obj = zset_load_or_bail!(store, key, output, {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });

    // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
    // （C# SortedSetRemoveOrCountRangeByLex 仅回填 result1，RESP 层负责写整数；
    // 对象层不写负载段，整数应答在挂载点之后落帧——单源见 write_lex_result）
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
    write_lex_result(result1, output);
    Ok(true)
  }

  /// ZRANK / ZREVRANK key member \[WITHSCORE\]
  ///
  /// libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRank
  pub fn sorted_set_rank<'a, D: Device>(
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

    let mut obj = zset_load_or_bail!(store, key, output, {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    });

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
