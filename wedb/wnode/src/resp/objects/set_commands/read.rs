//! 集合只读命令实现（SCARD, SMEMBERS, SISMEMBER, SMISMEMBER, SRANDMEMBER, SINTER, SINTERCARD, SUNION, SDIFF）

use wcol::set::{set_object::SetOperation, set_object_impl::NO_COUNT};
use wresp::{
  check_args::check_arg_count,
  cmd_strings as cs,
  ext::{RespSliceExt, RespVecExt},
};
use wval::GarnetObjectType;

use super::{
  SetLoad, run_operate, set_load_sync,
  write::{diff_sets, intersect_sets, load_many, union_sets},
  write_set_members,
};
use crate::resp::{
  objects::object_store_utils::{
    IntersectCardKind, ObjLoad, obj_length_sync, parse_intersect_card_args,
  },
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// SCARD key
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetLength
  pub fn set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "SCARD");
    let key = parse_state[0];
    match obj_length_sync(store, key, GarnetObjectType::Set, output) {
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

  /// SMEMBERS key
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetMembers
  pub fn set_members<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "SMEMBERS");
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND → WriteEmptySet（版本分派：RESP2 *0 / RESP3 ~0）
      SetLoad::Missing => cs::write_set_len(output, 0, self.resp_protocol_version),
      SetLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          SetOperation::Smembers,
          &[],
          0,
          0,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// SISMEMBER key member
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIsMember
  pub fn set_is_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "SISMEMBER");
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND → :0
      SetLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      SetLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          SetOperation::Sismember,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// C# SetIsMember 多值判定形态（SMISMEMBER；精确锚点见本文件 298 行）
  ///
  /// SMISMEMBER 入口（对应 C# SetIsMember 多值判定形态）
  pub fn set_multi_is_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SMISMEMBER");
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND：count-1 个 :0 数组
      SetLoad::Missing => {
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in 1..parse_state.len() {
          output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        }
      }
      SetLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          SetOperation::Smismember,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// SRANDMEMBER key \[count\]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRandomMember
  pub fn set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1..=2, output, "SRANDMEMBER");

    let key = parse_state[0];

    let count_parameter = if parse_state.len() == 2 {
      match parse_state[1].try_parse_i64() {
        Some(c) if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&c) => c as i32,
        _ => {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        }
      }
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

    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND：带 count → 空数组；无 count → null
      SetLoad::Missing => {
        if parse_state.len() == 2 {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
      SetLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          SetOperation::Srandmember,
          &[],
          count_parameter,
          seed,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// SINTER key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersect
  pub fn set_intersect<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "SINTER");

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = intersect_sets(&objs);
    write_set_members(&result, output, self.resp_protocol_version);
    Ok(true)
  }

  /// SINTERCARD numkeys key [key ...] [LIMIT limit]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectLength
  pub fn set_intersect_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；set 族帧见 IntersectCardKind）
    let Some(args) = parse_intersect_card_args(IntersectCardKind::Set, parse_state, output) else {
      return Ok(true);
    };

    let objs = match load_many(store, args.keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let mut card = intersect_sets(&objs).set.len() as i64;
    if let Some(limit) = args.limit.filter(|&v| v > 0) {
      card = card.min(i64::from(limit));
    }
    output.write_resp_int(card);
    Ok(true)
  }

  /// SUNION key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnion
  pub fn set_union<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "SUNION");

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = union_sets(&objs);
    write_set_members(&result, output, self.resp_protocol_version);
    Ok(true)
  }

  /// SDIFF key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiff
  pub fn set_diff<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "SDIFF");

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = diff_sets(&objs);
    write_set_members(&result, output, self.resp_protocol_version);
    Ok(true)
  }
}
