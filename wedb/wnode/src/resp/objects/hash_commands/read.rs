//! 哈希只读命令实现（HGET, HGETALL, HMGET, HLEN, HEXISTS, HKEYS, HVALS, HRANDFIELD, HSTRLEN, HTTL 族）
//!
//! 同根待裁面登记（票 wnode-hgetall-envelope-ttl-purge-not-solidified §5，
//! 本票最小案不扩面）：HMGET/HEXISTS/HSTRLEN 三臂维持 hash_load_sync 装载即
//! 弃形态，装载期剔除（deserialize_from_slice 置 mutated_by_ttl）同样未固化，
//! 与已修复的 HGETALL/HKEYS/HVALS 三臂同根；HRANDFIELD 面 §12 既判不复报。
//! 后续裁决若改接 rmw 通道，可直接复用 `hash_rmw_missing`/on_missing 形制
//!（慢臂对偶 `hash_rmw_cold_missing`），勿另立第二套 Missing 短路。

use wcol::hash::hash_object::HashOperation;
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, unpack_args},
  cmd_strings as cs,
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::{HashRmwParams, Rmw, hash_load_sync, run_operate};
use crate::resp::{
  objects::object_store_utils::{
    ElementHeaderKind, obj_length_sync, parse_elements_only_args, parse_random_member_args,
    reply_obj_length, write_random_member_missing, write_rmw_aof_fail_frame,
  },
  resp_server_session::RespServerSession,
};

/// HGETALL/HKEYS/HVALS 缺键短路 rmw 单源（本域两处同形收口，仅 op 参数化）：
/// `hash_rmw_missing` 挂常量空数组 `on_missing`（写 `*0` 帧），Degrade 转 `Ok(false)`
/// 异步重放、AofFail 落错误帧闭环（读臂 should_write 恒假本态不可达，防御
/// 口径与存储硬失败同帧，AofEnqueue 契约单点），其余态（错误帧/缺键帧/已透写
/// payload）经骨架闭环回 `Ok(true)`
macro_rules! hash_rmw_missing_or_bail {
  ($self:expr, $store:expr, $key:expr, $op:expr, $output:expr) => {{
    match $self.hash_rmw_missing(
      $store,
      HashRmwParams {
        key: $key,
        op: $op,
        args: &[],
        args12: (0, 0),
      },
      $output,
      Some(super::write_missing_empty_array),
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::AofFail => {
        crate::resp::objects::object_store_utils::write_rmw_aof_fail_frame($output);
        return Ok(true);
      }
      _ => {}
    }
    Ok(true)
  }};
}

impl RespServerSession {
  /// HGET key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGet
  pub fn hash_get<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "HGET");
    let key = parse_state[0];
    match self.hash_rmw(
      store,
      key,
      HashOperation::Hget,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      // AofFail 防御臂：读骨架不入账本态不可达，同帧口径收口（见宏注）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      _ => {}
    }
    Ok(true)
  }

  /// HGETALL key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGetAll
  ///
  /// 走 rmw 固化通道（HTTL 既定模式，见本文件 hash_time_to_live 注）：装载期/
  /// 出帧期物理剔除经 should_write_back 的 mutated_by_ttl 升格写回固化，全剔
  /// 空载荷经 obj_save_or_gc_raw→try_delete_sync 删空自愈；缺键经 on_missing
  /// 钩子短路常量空数组帧（C# NOTFOUND → RESP_EMPTYLIST，HashCommands.cs:148）
  pub fn hash_get_all<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1, output, "HGETALL");
    let key = parse_state[0];
    hash_rmw_missing_or_bail!(self, store, key, HashOperation::Hgetall, output)
  }

  /// HMGET key field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashGetMultiple
  pub fn hash_get_multiple<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "HMGET");
    let key = parse_state[0];
    let mut obj = hash_load_or_bail!(store, key, output, {
      write_null_array(output, parse_state.len() - 1, self.resp_protocol_version);
      return Ok(true);
    });
    run_operate(
      &mut obj,
      HashOperation::Hmget,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// HLEN key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashLength
  pub fn hash_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "HLEN") else {
      return Ok(true);
    };
    reply_obj_length(
      obj_length_sync(store, key, GarnetObjectType::Hash, output),
      output,
      |_| Ok(false),
    )
  }

  fn hash_field_int_op<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    cmd_name: &str,
    op: HashOperation,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, cmd_name);
    let key = parse_state[0];
    let mut obj = hash_load_or_bail!(store, key, output, {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });
    let result1 = run_operate(
      &mut obj,
      op,
      &parse_state[1..],
      0,
      0,
      self.resp_protocol_version,
      output,
    )
    .result1;
    output.write_resp_int(result1);
    Ok(true)
  }

  /// HEXISTS key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExists
  pub fn hash_exists<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_field_int_op(
      parse_state,
      store,
      output,
      "HEXISTS",
      HashOperation::Hexists,
    )
  }

  /// HKEYS / HVALS key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashKeys
  ///
  /// 走 rmw 固化通道（与 [`Self::hash_get_all`] 同形，HTTL 既定模式）；缺键
  /// 经 on_missing 钩子短路常量空数组帧（C# NOTFOUND → TryWriteEmptyArray，
  /// HashCommands.cs:513）
  pub fn hash_keys<'a, D: Device>(
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
    hash_rmw_missing_or_bail!(self, store, key, op, output)
  }

  /// HVALS key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashVals
  ///
  /// HVALS 入口，调用 hash_keys(..., false)
  pub fn hash_vals<'a, D: Device>(
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
  pub fn hash_random_field<'a, D: Device>(
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

    let mut obj = hash_load_or_bail!(store, key, output, {
      write_random_member_missing(output, args.included_count, self.resp_protocol_version);
      return Ok(true);
    });
    run_operate(
      &mut obj,
      HashOperation::Hrandfield,
      &[],
      args.arg1,
      seed,
      self.resp_protocol_version,
      output,
    );
    Ok(true)
  }

  /// HSTRLEN key field
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashStrLength
  pub fn hash_str_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_field_int_op(
      parse_state,
      store,
      output,
      "HSTRLEN",
      HashOperation::Hstrlen,
    )
  }

  /// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashTimeToLive
  ///
  /// 对象层 HashTimeToLive 会 DeleteExpiredItems 物理剔除（C#
  /// HashObjectImpl.cs:HashTimeToLive 同），走 rmw 骨架使剔除结果经
  /// should_write_back 的 mutated_by_ttl 判定落盘（C# 常驻对象经 checkpoint
  /// 序列化落盘的等价物）
  pub fn hash_time_to_live<'a, D: Device>(
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

    // C# NOTFOUND：对象层以空对象执行，逐字段 -2 数组（payload 经骨架透写），非降级臂均已闭环
    match self.hash_rmw(
      store,
      key,
      HashOperation::Httl,
      fields,
      (i32::from(is_milliseconds), i32::from(is_timestamp)),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      // AofFail 防御臂：读骨架不入账本态不可达，同帧口径收口（见宏注）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      _ => {}
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
