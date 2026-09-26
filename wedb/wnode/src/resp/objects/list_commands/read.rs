//! 列表只读命令实现（LLEN, LRANGE, LINDEX, LPOS）

use wcol::list::list_object::ListOperation;
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i32_arg, unpack_args},
  cmd_strings as cs,
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{list_load_sync, parse_i32_pair_args, run_operate};
use crate::resp::{
  objects::object_store_utils::{obj_length_sync, reply_obj_length},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// LLEN key
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListLength
  pub fn list_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "LLEN") else {
      return Ok(true);
    };
    reply_obj_length(
      obj_length_sync(store, key, GarnetObjectType::List, output),
      output,
      |_| Ok(false),
    )
  }

  /// LRANGE key start stop
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRange
  pub fn list_range<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // start/stop 参数推导单源（快慢共用，失败帧已写出）
    let Some((start, stop)) = parse_i32_pair_args("LRANGE", parse_state, output) else {
      return Ok(true);
    };
    let key = parse_state[0];

    let mut obj = list_load_or_bail!(store, key, output, {
      output.extend_from_slice(cs::RESP_EMPTYLIST);
      return Ok(true);
    });
    run_operate(
      &mut obj,
      ListOperation::Lrange,
      &[],
      start,
      stop,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// LINDEX key index
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListIndex
  pub fn list_index<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "LINDEX");
    let key = parse_state[0];
    let Some(index) = parse_i32_arg(parse_state[1], output) else {
      return Ok(true);
    };

    let mut obj = list_load_or_bail!(store, key, output, {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    });
    // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
    let result1 = run_operate(
      &mut obj,
      ListOperation::Lindex,
      &[],
      index,
      0,
      self.resp_protocol_version,
      output,
    )
    .result1;
    if result1 == -1 {
      output.write_resp_null_ver(self.resp_protocol_version);
    }
    Ok(true)
  }

  /// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPosition
  pub fn list_position<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "LPOS");

    let key = parse_state[0];

    let mut obj = list_load_or_bail!(store, key, output, {
      // C# NOTFOUND：参数中含 COUNT → 空数组，否则 null（eq_ignore_ascii_case 本身大小写不敏感）
      let count = parse_state[2..]
        .iter()
        .any(|t| t.eq_ignore_ascii_case(cs::COUNT));
      if count {
        output.extend_from_slice(cs::RESP_EMPTYLIST);
      } else {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      return Ok(true);
    });
    // 对象层 Lpos result1 = 命中数恒 ≥0（未命中负载由对象层自写 null/空
    // 数组），无 LINDEX 的 result1==-1 未写负载形
    run_operate(
      &mut obj,
      ListOperation::Lpos,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }
}
