//! 哈希只读命令实现（HGET, HGETALL, HMGET, HLEN, HEXISTS, HKEYS, HVALS, HRANDFIELD, HSTRLEN, HTTL 族）

use wcol::hash::hash_object::HashOperation;
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{HashLoad, Rmw, hash_load_sync, run_operate};
use crate::resp::{
  objects::object_store_utils::{
    ElementHeaderKind, ObjLoad, obj_length_sync, parse_elements_only_args,
    parse_random_member_args, write_random_member_missing,
  },
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// HGET key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGet
  pub fn hash_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "HGET");
    let key = parse_state[0];
    if let Rmw::Degrade = self.hash_rmw(
      store,
      key,
      HashOperation::Hget,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      return Ok(false);
    }
    Ok(true)
  }

  /// HGETALL key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGetAll
  pub fn hash_get_all<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "HGETALL");
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      HashLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      HashLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          HashOperation::Hgetall,
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

  /// HMGET key field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGetMultiple
  pub fn hash_get_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "HMGET");
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      // C# NOTFOUND：count-1 元素全 null 数组
      HashLoad::Missing => {
        write_null_array(output, parse_state.len() - 1, self.resp_protocol_version);
      }
      HashLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          HashOperation::Hmget,
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

  /// HLEN key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashLength
  pub fn hash_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "HLEN");
    let key = parse_state[0];
    match obj_length_sync(store, key, GarnetObjectType::Hash, output) {
      // 信封水位越线（字段级 TTL 已有成员到期，头部计数失真）：落物化 Hlen
      // 矫正臂（臂内复核仍降级——分层态/磁盘候选/rmw 窗口争用——转异步慢路径）
      ObjLoad::Degrade => self.hash_length_purged(store, key, output),
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

  /// HLEN 物化矫正臂：rmw 通道执行 Hlen——对象层
  /// [`HashObject::purge_expired_len`] 堆序惰性剔除为唯一剔除内核（与
  /// HGETALL/HKEYS 同源，collection.md §6.3），剔除实际发生经 mutated_by_ttl
  /// 升格写回一次矫正并触发删空自愈
  fn hash_length_purged<'a, D: wdev::Device>(
    &self,
    store: &wkv::BatchStoreSession<'a, D>,
    key: &[u8],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    match self.hash_rmw(store, key, HashOperation::Hlen, &[], (0, 0), output) {
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

  /// HEXISTS key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExists
  pub fn hash_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "HEXISTS");
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let result1 = run_operate(
          &mut obj,
          HashOperation::Hexists,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        )
        .result1;
        output.write_resp_int(result1);
      }
    }
    Ok(true)
  }

  /// HKEYS / HVALS key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashKeys
  pub fn hash_keys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_keys: bool,
  ) -> wresp::Result<bool> {
    let name = if is_keys { "HKEYS" } else { "HVALS" };
    check_arg_count!(parse_state, 1, output, name);
    let key = parse_state[0];
    let op = if is_keys {
      HashOperation::Hkeys
    } else {
      HashOperation::Hvals
    };
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      // C# NOTFOUND → 空数组
      HashLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      HashLoad::Present(mut obj) => {
        run_operate(&mut obj, op, &[], 0, 0, self.resp_protocol_version, output);
      }
    }
    Ok(true)
  }

  /// HVALS key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashVals
  ///
  /// HVALS 入口，调用 hash_keys(..., false)
  pub fn hash_vals<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_keys(parse_state, store, output, false)
  }

  /// HRANDFIELD key \[count \[WITHVALUES\]\]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashRandomField
  pub fn hash_random_field<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出；count 上限钳至有符号 30 位，
    // arg1 打包 (count << 1 | includedCount) << 1 | withValues）
    let Some(args) = parse_random_member_args("HRANDFIELD", parse_state, cs::WITHVALUES, output)
    else {
      return Ok(true);
    };
    let key = parse_state[0];

    // Create a random seed（C# Random.Shared.Next()；负数由对象层按无符号取模吸收）
    let seed = fastrand::i32(..);

    // count 为 0 不触达后端（对齐 C#；应答与缺失态同形单源）
    if args.param_count == 0 {
      write_random_member_missing(output, args.included_count, self.resp_protocol_version);
      return Ok(true);
    }

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      HashLoad::Missing => {
        write_random_member_missing(output, args.included_count, self.resp_protocol_version);
      }
      HashLoad::Present(mut obj) => {
        run_operate(
          &mut obj,
          HashOperation::Hrandfield,
          &[],
          args.arg1,
          seed,
          self.resp_protocol_version,
          output,
        );
      }
    }
    Ok(true)
  }

  /// HSTRLEN key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashStrLength
  pub fn hash_str_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "HSTRLEN");
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::WrongType => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let result1 = run_operate(
          &mut obj,
          HashOperation::Hstrlen,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
          output,
        )
        .result1;
        output.write_resp_int(result1);
      }
    }
    Ok(true)
  }

  /// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashTimeToLive
  ///
  /// 对象层 HashTimeToLive 会 DeleteExpiredItems 物理剔除（C#
  /// HashObjectImpl.cs:HashTimeToLive 同），走 rmw 骨架使剔除结果经
  /// should_write_back 的 mutated_by_ttl 判定落盘（C# 常驻对象经 checkpoint
  /// 序列化落盘的等价物）
  pub fn hash_time_to_live<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &'static str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, fields)) =
      parse_elements_only_args(cmd_name, parse_state, ElementHeaderKind::Fields, output)
    else {
      return Ok(true);
    };

    match self.hash_rmw(
      store,
      key,
      HashOperation::Httl,
      fields,
      (i32::from(is_milliseconds), i32::from(is_timestamp)),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType => {}
      // C# NOTFOUND：对象层以空对象执行，逐字段 -2 数组（payload 经骨架透写）
      Rmw::Missing | Rmw::Present(_) => {}
    }
    Ok(true)
  }
}

/// nil 元素数组应答（HMGET 键缺失时的逐字段占位）
///
/// 元素帧随会话协议（调用 write_resp_null_ver 逐元素）
pub(super) fn write_null_array(output: &mut Vec<u8>, len: usize, resp_version: u8) {
  output.reserve(len * 5 + 16);
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.write_resp_null_ver(resp_version);
  }
}
