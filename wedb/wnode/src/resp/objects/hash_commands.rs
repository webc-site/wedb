//! 哈希命令（对标 libs/server/Resp/Objects/HashCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wobject::hash::hash_object::HashObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。

use wbase::{convert::UNIX_EPOCH_TICKS, time::now_ticks};
use wobject::{
  hash::hash_object::{HashObject, HashOperation},
  parse_utils::{try_get_int, try_get_long},
  sortedset::sorted_set_object::ExpirationWithOption,
  types::object_output::ObjectOutput,
};
use wresp::{ExpireOption, RespVecExt, check_arg_count, cmd_strings as cs, try_get_expire_option};
use wval::GarnetObjectType;

use crate::resp::{
  objects::object_store_utils::{
    ObjLoad, RespRmwOutcome, SyncRmwCmd, SyncRmwHandlers, hash_from_blob, hash_to_blob,
    make_object_input, obj_load_typed_sync, run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

/// HSET ... F.n 语义下 numFields 非法时的校验文案（本域多处复用）。
const ERR_NUM_FIELDS_POSITIVE: &str = "ERR Parameter `numFields` should be greater than 0";

/// 本命令面统一按会话协商协议版本输出（C# respProtocolVersion 由会话
/// `UpdateRespProtocolVersion` 下发，命令层经 `resp_protocol_version` 透传）
///
/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut HashObject,
  op: HashOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
) -> ObjectOutput {
  let input = make_object_input(GarnetObjectType::Hash, op as u8, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, resp_version);
  obj_out
}

pub(crate) type HashLoad = ObjLoad<HashObject>;
type Rmw = RespRmwOutcome;

/// 同步装载哈希（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn hash_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> HashLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::Hash as u8,
    output,
    hash_from_blob,
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除类操作（HDEL）以移除计数为准。
fn should_write_back(
  op: HashOperation,
  out: &ObjectOutput,
  obj: &HashObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.has_wrong_type()
    || out.payload.first() == Some(&b'-')
    || (!existed && obj.hash.is_empty())
  {
    return false;
  }
  match op {
    HashOperation::Hdel => out.result1 > 0,
    _ => true,
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hget
      | HashOperation::Hmget
      | HashOperation::Hgetall
      | HashOperation::Hlen
      | HashOperation::Hstrlen
      | HashOperation::Hexists
      | HashOperation::Hkeys
      | HashOperation::Hvals
      | HashOperation::Hrandfield
      | HashOperation::Httl
      | HashOperation::Hscan
  )
}

/// HEXPIRE 命令参数面。
#[derive(Debug, Clone)]
struct HashExpireArgs<'a> {
  key: &'a [u8],
  expiration: i64,
  expire_option: ExpireOption,
  fields: &'a [&'a [u8]],
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn hash_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: HashOperation,
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
        tag: GarnetObjectType::Hash as u8,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        hash_from_blob,
        HashObject::new,
        |o: &HashObject| o.is_empty(),
        hash_to_blob,
        |obj, op, args| run_operate(obj, op, args, arg1, arg2, resp_version),
        should_write_back,
      ),
    )
  }
  /// HSET key field value [field value ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashSet（HSET 形态，回复新增字段数）
  pub fn hash_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hset)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashSetNx
  ///
  /// HSETNX 入口，调用 hash_set_by_command(HashOperation::Hsetnx)
  pub fn hash_set_nx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hsetnx)
  }

  /// HMSET key field value [field value ...]（废弃别名，回复 +OK）
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashSetMap
  ///
  /// HMSET 入口，调用 hash_set_by_command(HashOperation::Hmset)
  pub fn hash_set_map<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hmset)
  }

  /// HSET/HSETNX/HMSET 公共体
  fn hash_set_by_command<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    op: HashOperation,
  ) -> wresp::Result<bool> {
    let cmd_name = match op {
      HashOperation::Hsetnx => "HSETNX",
      HashOperation::Hmset => "HMSET",
      _ => "HSET",
    };
    let valid = if op == HashOperation::Hsetnx {
      parse_state.len() == 3
    } else {
      parse_state.len() > 1 && parse_state.len() % 2 == 1
    };
    check_arg_count!(valid, output, cmd_name);

    let key = parse_state[0];
    let args = &parse_state[1..];
    match self.hash_rmw(store, key, op, args, (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // HMSET 回复 +OK；其余回新增字段数（仅 result1，无负载）
      Rmw::Done {
        result1,
        payload_written,
      } => {
        if !payload_written {
          if op == HashOperation::Hmset {
            output.extend_from_slice(cs::RESP_OK);
          } else {
            output.write_resp_int(result1);
          }
        }
      }
    }
    Ok(true)
  }

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
      HashLoad::Error => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      HashLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hgetall,
          &[],
          0,
          0,
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
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
    check_arg_count!(parse_state, >= 2, output, "HMGET");
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：count-1 元素全 null 数组
      HashLoad::Missing => {
        write_null_array(output, parse_state.len() - 1);
      }
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hmget,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
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
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hlen,
          &[],
          0,
          0,
          self.resp_protocol_version,
        );
        output.write_resp_int(obj_out.result1);
      }
    }
    Ok(true)
  }

  /// HDEL key field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashDelete
  pub fn hash_delete<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 2, output, "HDEL");
    let key = parse_state[0];
    match self.hash_rmw(
      store,
      key,
      HashOperation::Hdel,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
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
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hexists,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
        );
        output.write_resp_int(obj_out.result1);
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
      HashLoad::Error => {}
      // C# NOTFOUND → 空数组
      HashLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, op, &[], 0, 0, self.resp_protocol_version);
        output.extend_from_slice(&obj_out.payload);
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
    check_arg_count!(parse_state, 1..=3, output, "HRANDFIELD");

    let key = parse_state[0];

    let mut param_count = 1_i64;
    let mut with_values = false;
    let mut included_count = false;

    if parse_state.len() >= 2 {
      // C# parseState.TryGetInt：32 位整型（越界即 VALUE_IS_NOT_INTEGER）
      let Some(v) = try_get_int(parse_state[1]) else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      param_count = i64::from(v);
      included_count = true;

      // Read WITHVALUES
      if parse_state.len() == 3 && !parse_state[2].eq_ignore_ascii_case(b"WITHVALUES") {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      with_values = parse_state.len() == 3;
    }

    // arg1 打包 (count << 1 | includedCount) << 1 | withValues，count 上限
    // 受有符号 30 位约束（对齐 C# Math.Min(paramCount, int.MaxValue >> 2)）
    param_count = param_count.min(i32::MAX as i64 >> 2);
    let count_with_metadata =
      (((param_count << 1) | i64::from(included_count)) << 1) | i64::from(with_values);

    // Create a random seed（C# Random.Shared.Next()；负数由对象层按无符号取模吸收）
    let seed = fastrand::i32(..);

    // This prevents going to the backend if HRANDFIELD is called with a count of 0
    if param_count == 0 {
      if included_count {
        output.extend_from_slice(cs::RESP_EMPTYLIST);
      } else {
        output.write_resp_null();
      }
      return Ok(true);
    }

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      HashLoad::Missing => {
        if included_count {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null();
        }
      }
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hrandfield,
          &[],
          count_with_metadata as i32,
          seed,
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
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
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hstrlen,
          &parse_state[1..],
          0,
          0,
          self.resp_protocol_version,
        );
        output.write_resp_int(obj_out.result1);
      }
    }
    Ok(true)
  }

  /// HINCRBY key field increment / HINCRBYFLOAT key field increment
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashIncrement
  pub fn hash_increment<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_float: bool,
  ) -> wresp::Result<bool> {
    let name = if is_float { "HINCRBYFLOAT" } else { "HINCRBY" };
    check_arg_count!(parse_state, 3, output, name);

    let key = parse_state[0];
    let op = if is_float {
      HashOperation::Hincrbyfloat
    } else {
      HashOperation::Hincrby
    };
    if let Rmw::Degrade = self.hash_rmw(store, key, op, &parse_state[1..], (0, 0), output) {
      return Ok(false);
    }
    Ok(true)
  }

  fn parse_hash_expire_args<'a>(
    parse_state: &'a [&'a [u8]],
    output: &mut Vec<u8>,
  ) -> Option<HashExpireArgs<'a>> {
    check_arg_count!(parse_state, >= 5, output, "HEXPIRE", return None);

    let key = parse_state[0];

    let Some(expiration) = try_get_long(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    if expiration < 0 {
      cs::abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
      return None;
    }

    let mut curr_idx = 2;
    let mut expire_option = ExpireOption::NONE;
    if let Some(opt) = try_get_expire_option(parse_state[curr_idx]) {
      expire_option = opt;
      curr_idx += 1;
    }

    // FIELDS numfields 头校验（公共体，见 parse_fields_header）
    let (fields_start, _) = parse_fields_header(parse_state, curr_idx, output)?;

    Some(HashExpireArgs {
      key,
      expiration,
      expire_option,
      fields: &parse_state[fields_start..],
    })
  }

  /// 过期时长 → .NET Ticks 目标时刻（saturating，防大时长溢出；
  /// is_timestamp 对 UNIX_EPOCH，否则对当前时刻）
  pub(crate) fn compute_expiration_ticks(
    expiration: i64,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> i64 {
    let step: i64 = if is_milliseconds { 10_000 } else { 10_000_000 };
    let offset = expiration.saturating_mul(step);
    if is_timestamp {
      UNIX_EPOCH_TICKS.saturating_add(offset)
    } else {
      now_ticks().saturating_add(offset)
    }
  }

  /// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExpire
  pub fn hash_expire<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    let Some(args) = Self::parse_hash_expire_args(parse_state, output) else {
      return Ok(true);
    };

    let expiration_ticks =
      Self::compute_expiration_ticks(args.expiration, is_milliseconds, is_timestamp);
    let e = ExpirationWithOption::new(expiration_ticks, args.expire_option);

    match self.hash_rmw(
      store,
      args.key,
      HashOperation::Hexpire,
      args.fields,
      ((e.word() >> 32) as i32, e.word() as i32),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // C# NOTFOUND：numFields 个 -2 数组
      Rmw::Done { .. } => {}
    }
    // 键缺失时对象层不可达（rmw Missing 分支以空对象执行，逐字段回 -2 语义一致）
    Ok(true)
  }

  /// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashTimeToLive
  pub fn hash_time_to_live<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 4, output, "HTTL");

    let key = parse_state[0];
    let Some((fields_start, num_fields)) = parse_fields_header(parse_state, 1, output) else {
      return Ok(true);
    };

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：numFields 个 -2 数组
      HashLoad::Missing => write_n2_array(output, num_fields),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Httl,
          &parse_state[fields_start..],
          i32::from(is_milliseconds),
          i32::from(is_timestamp),
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// HPERSIST key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashPersist
  pub fn hash_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 4, output, "HPERSIST");

    let key = parse_state[0];
    let Some((fields_start, num_fields)) = parse_fields_header(parse_state, 1, output) else {
      return Ok(true);
    };

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：numFields 个 -2 数组
      HashLoad::Missing => write_n2_array(output, num_fields),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Hpersist,
          &parse_state[fields_start..],
          0,
          0,
          self.resp_protocol_version,
        );
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }
}

/// 解析 `FIELDS numfields field [field...]` 公共头部，返回 `(fields_start_idx, num_fields)`
fn parse_fields_header(
  parse_state: &[&[u8]],
  curr_idx: usize,
  output: &mut Vec<u8>,
) -> Option<(usize, usize)> {
  if curr_idx >= parse_state.len() || !parse_state[curr_idx].eq_ignore_ascii_case(b"FIELDS") {
    cs::abort_with_error_message(
      output,
      "Mandatory argument FIELDS is missing or not at the right position",
    );
    return None;
  }

  let num_idx = curr_idx + 1;
  let Some(num_fields) = parse_state.get(num_idx).and_then(|raw| try_get_int(raw)) else {
    cs::abort_with_error_message(output, ERR_NUM_FIELDS_POSITIVE);
    return None;
  };

  if num_fields < 1 {
    cs::abort_with_error_message(output, ERR_NUM_FIELDS_POSITIVE);
    return None;
  }

  let num_fields = num_fields as usize;
  let fields_start = num_idx + 1;
  if parse_state.len() != fields_start + num_fields {
    cs::abort_with_error_message(
      output,
      "The `numFields` parameter must match the number of arguments",
    );
    return None;
  }

  Some((fields_start, num_fields))
}

/// -2 元素数组应答（键缺失时的逐字段占位）
fn write_n2_array(output: &mut Vec<u8>, len: usize) {
  output.reserve(len * cs::RESP_RETURN_VAL_N2.len() + 16);
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.extend_from_slice(cs::RESP_RETURN_VAL_N2);
  }
}

/// nil 元素数组应答（HMGET 键缺失时的逐字段占位）
pub(super) fn write_null_array(output: &mut Vec<u8>, len: usize) {
  output.reserve(len * 5 + 16);
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.write_resp_null();
  }
}
