//! 列表只读命令实现（LLEN, LRANGE, LINDEX, LPOS）

use wbase::num::strict_i32;
use wcol::list::list_object::ListOperation;
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{ListLoad, list_load_sync, parse_i32_pair_args, run_operate};
use crate::resp::{
  objects::object_store_utils::{ObjLoad, obj_length_sync},
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// LLEN key
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListLength
  pub fn list_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "LLEN");
    let key = parse_state[0];
    match obj_length_sync(store, key, GarnetObjectType::List, output) {
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

  /// LRANGE key start stop
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListRange
  pub fn list_range<'a, D: wdev::Device>(
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

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      ListLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      ListLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          ListOperation::Lrange,
          &[],
          start,
          stop,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// LINDEX key index
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListIndex
  pub fn list_index<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "LINDEX");
    let key = parse_state[0];
    let Some(index) = strict_i32(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      // C# NOTFOUND → null
      ListLoad::Missing => output.write_resp_null_ver(self.resp_protocol_version),
      ListLoad::Present(mut obj) => {
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
      }
    }
    Ok(true)
  }

  /// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
  ///
  /// libs/server/Resp/Objects/ListCommands.cs:ListPosition
  pub fn list_position<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "LPOS");

    let key = parse_state[0];

    match list_load_sync(store, key, output) {
      ListLoad::Degrade => return Ok(false),
      ListLoad::WrongType => {}
      ListLoad::Missing => {
        // C# NOTFOUND：参数中含 COUNT → 空数组，否则 null（eq_ignore_ascii_case 本身大小写不敏感）
        let count = parse_state[2..]
          .iter()
          .any(|t| t.eq_ignore_ascii_case(cs::COUNT));
        if count {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
      ListLoad::Present(mut obj) => {
        // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
        let result1 = run_operate(
          &mut obj,
          ListOperation::Lpos,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        )
        .result1;
        if result1 == -1 {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
    }
    Ok(true)
  }
}
