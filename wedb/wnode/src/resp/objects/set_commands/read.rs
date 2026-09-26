//! 集合只读命令实现（SCARD, SMEMBERS, SISMEMBER, SMISMEMBER, SRANDMEMBER, SINTER, SINTERCARD, SUNION, SDIFF）

use wbase::map::HashSet;
use wcol::set::{
  set_object::{SetObject, SetOperation},
  set_object_impl::NO_COUNT,
};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i32_arg, unpack_args},
  cmd_strings as cs,
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{
  run_operate, set_load_sync,
  write::{diff_sets, intersect_sets, load_many, union_sets},
  write_set_members,
};
use crate::resp::{
  objects::object_store_utils::{
    IntersectCardKind, obj_length_sync, parse_intersect_card_args, reply_obj_length,
  },
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// SCARD key
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetLength
  pub fn set_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "SCARD") else {
      return Ok(true);
    };
    reply_obj_length(
      obj_length_sync(store, key, GarnetObjectType::Set, output),
      output,
      |_| Ok(false),
    )
  }

  /// SMEMBERS key
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetMembers
  pub fn set_members<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "SMEMBERS");
    let key = parse_state[0];
    let mut obj = set_load_or_bail!(store, key, output, {
      // C# NOTFOUND → WriteEmptySet（版本分派：RESP2 *0 / RESP3 ~0）
      cs::write_set_len(output, 0, self.resp_protocol_version);
      return Ok(true);
    });
    run_operate(
      &mut obj,
      SetOperation::Smembers,
      &[],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// SISMEMBER key member
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIsMember
  pub fn set_is_member<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "SISMEMBER");
    let key = parse_state[0];
    let mut obj = set_load_or_bail!(store, key, output, {
      // C# NOTFOUND → :0
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });
    run_operate(
      &mut obj,
      SetOperation::Sismember,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// C# SetIsMember 多值判定形态（SMISMEMBER；精确锚点见本文件 298 行）
  ///
  /// SMISMEMBER 入口（对应 C# SetIsMember 多值判定形态）
  pub fn set_multi_is_member<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SMISMEMBER");
    let key = parse_state[0];
    let mut obj = set_load_or_bail!(store, key, output, {
      // C# NOTFOUND：count-1 个 :0 数组
      output.write_resp_array_len(parse_state.len() - 1);
      for _ in 1..parse_state.len() {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      }
      return Ok(true);
    });
    run_operate(
      &mut obj,
      SetOperation::Smismember,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// SRANDMEMBER key \[count\]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRandomMember
  pub fn set_random_member<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1..=2, output, "SRANDMEMBER");

    let key = parse_state[0];

    let count_parameter = if parse_state.len() == 2 {
      let Some(c) = parse_i32_arg(parse_state[1], output) else {
        return Ok(true);
      };
      c
    } else {
      NO_COUNT
    };

    // C# countParameter == 0 → 空数组（不触达后端）
    if count_parameter == 0 {
      output.extend_from_slice(cs::RESP_EMPTYLIST);
      return Ok(true);
    }

    // Create a random seed（C# Random.Shared.Next()）
    let seed = fastrand::i32(..);

    let mut obj = set_load_or_bail!(store, key, output, {
      // C# NOTFOUND：带 count → 空数组；无 count → null
      if parse_state.len() == 2 {
        output.extend_from_slice(cs::RESP_EMPTYLIST);
      } else {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      return Ok(true);
    });
    run_operate(
      &mut obj,
      SetOperation::Srandmember,
      &[],
      count_parameter,
      seed,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  fn set_combine<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    cmd_name: &str,
    combine: impl FnOnce(&[SetObject]) -> HashSet<Vec<u8>>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, cmd_name);

    let objs = load_many_or_bail!(store, parse_state, output);

    // 裸集直出帧（对位 C# SetCommands foreach 借用枚举直写，零中转零计账）
    write_set_members(&combine(&objs), output, self.resp_protocol_version);
    Ok(true)
  }

  /// SINTER key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersect
  pub fn set_intersect<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine(parse_state, store, output, "SINTER", intersect_sets)
  }

  /// SINTERCARD numkeys key [key ...] [LIMIT limit]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectLength
  pub fn set_intersect_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；set 族帧见 IntersectCardKind）
    let Some(args) = parse_intersect_card_args(IntersectCardKind::Set, parse_state, output) else {
      return Ok(true);
    };

    let objs = load_many_or_bail!(store, args.keys, output);

    // 基数标量直读裸集 len，读臂零计账零出帧中转
    let mut card = intersect_sets(&objs).len() as i64;
    if let Some(limit) = args.limit.filter(|&v| v > 0) {
      card = card.min(i64::from(limit));
    }
    output.write_resp_int(card);
    Ok(true)
  }

  /// SUNION key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnion
  pub fn set_union<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine(parse_state, store, output, "SUNION", union_sets)
  }

  /// SDIFF key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiff
  pub fn set_diff<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine(parse_state, store, output, "SDIFF", diff_sets)
  }
}
