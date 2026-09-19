//! 哈希命令（对标 libs/server/Resp/Objects/HashCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::hash::hash_object::HashObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。

use wcol::{
  ObjectOutput,
  hash::hash_object::{HashObject, HashOperation},
};
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use crate::resp::{
  objects::object_store_utils::{
    ElementHeaderKind, GarnetObjectPayload, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers,
    obj_length_sync, obj_load_typed_sync, parse_elements_only_args, parse_expire_elements_args,
    parse_random_member_args, run_sync_rmw, write_random_member_missing,
  },
  resp_server_session::RespServerSession,
};

/// 本命令面统一按会话协商协议版本输出（C# respProtocolVersion 由会话
/// `UpdateRespProtocolVersion` 下发，命令层经 `resp_protocol_version` 透传）
///
/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate<'o>(
  obj: &mut HashObject,
  op: HashOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
  output: &'o mut Vec<u8>,
) -> ObjectOutput<'o> {
  let mut obj_out = ObjectOutput::mount(output);
  obj.operate(op as u8, args, arg1, arg2, &mut obj_out, resp_version);
  obj_out
}

pub(crate) type HashLoad = ObjLoad<HashObject>;
type Rmw = ObjLoad<RespRmwDone>;

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
    GarnetObjectType::Hash,
    output,
    HashObject::from_blob,
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；例外：对象层已发生 TTL 惰性剔除（mutated_by_ttl）时
///   升格写回——C# 对象常驻 Tsavorite 对象缓存（HashOps.HashTimeToLive →
///   ReadObjectStoreOperation 的就地剔除经 checkpoint 序列化落盘），Rust 信封
///   无常驻对象层，以剔除后回写等价闭环，杜绝已剔除字段重装载复活；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除类操作（HDEL）以移除计数为准。
fn should_write_back(
  op: HashOperation,
  out: &ObjectOutput,
  obj: &HashObject,
  existed: bool,
) -> bool {
  if (is_read_only(op) && !obj.mutated_by_ttl())
    || out.payload_view().first() == Some(&b'-')
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
        tag: GarnetObjectType::Hash,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        HashObject::from_blob,
        HashObject::new,
        |o: &HashObject| o.is_empty(),
        |o: &HashObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
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

  /// C# HashSet 共用入口（HSETNX 命令分支；精确锚点见本文件 158 行）
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
  /// C# HashSet 共用入口（HMSET 命令分支；精确锚点见本文件 158 行）
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
      Rmw::WrongType | Rmw::Missing => {}
      // HMSET 回复 +OK；其余回新增字段数（仅 result1，无负载）
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
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

  /// HDEL key field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashDelete
  pub fn hash_delete<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# HashDelete 仅 Count < 1（:395）：HDEL key 零字段合法，两态回 :0
    check_arg_count!(parse_state, 1.., output, "HDEL");
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
      Rmw::WrongType | Rmw::Missing => {}
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
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

  /// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExpire
  pub fn hash_expire<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &'static str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some(args) = parse_expire_elements_args(
      cmd_name,
      parse_state,
      ElementHeaderKind::Fields,
      is_milliseconds,
      is_timestamp,
      output,
    ) else {
      return Ok(true);
    };

    match self.hash_rmw(
      store,
      args.key,
      HashOperation::Hexpire,
      args.elements,
      args.args12,
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# NOTFOUND：numFields 个 -2 数组
      Rmw::Present(_) => {}
    }
    // 键缺失时对象层不可达（rmw Missing 分支以空对象执行，逐字段回 -2 语义一致）
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

  /// HPERSIST key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashPersist
  ///
  /// C# HashPersist → RMWObjectStoreOperation（HashOps.cs:HashPersist），
  /// 对象层 HashPersist 先 DeleteExpiredItems 物理剔除，走 rmw 骨架使
  /// 剔除与持久化移除一并落盘
  pub fn hash_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, fields)) =
      parse_elements_only_args("HPERSIST", parse_state, ElementHeaderKind::Fields, output)
    else {
      return Ok(true);
    };

    match self.hash_rmw(store, key, HashOperation::Hpersist, fields, (0, 0), output) {
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
/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/HashCommands.cs 各命令经 Tsavorite pending
/// 读 CompletePending 后重放的异步形态：磁盘候选经 StorageSession 异步读
/// 闭环后按同步入口同款语义求值（Missing 短路与 RMW 新建矩阵逐一对位），
/// 写回经异步段对象写回唯一漏斗（StorageSession::obj_save / delete_string，
/// 信封整值入账对标 C# WriteLogUpsert）。`Err(())` 为存储 IO 失败，由
/// exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wcol::hash::hash_object::HashObject;
  use wdev::Device;
  use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
  use wval::GarnetObjectType;

  use super::{HashOperation, Rmw, run_operate, should_write_back, write_null_array};
  use crate::{
    resp::objects::{
      object_store_utils::{
        ElementHeaderKind, GarnetObjectPayload, SyncRmwCmd, SyncRmwHandlers,
        parse_elements_only_args, parse_expire_elements_args, parse_random_member_args,
        run_async_rmw, slow_load_eval, try_tiered_arm, write_random_member_missing,
        write_rmw_reply,
      },
      tiered_collection_ops::{TieredCollectionArgs, exec_tiered_hash},
    },
    storage::session::storage_session::StorageSession,
  };

  /// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
  async fn hash_rmw_cold(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    op: HashOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<Rmw, ()> {
    let (arg1, arg2) = args12;
    run_async_rmw(
      storage,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::Hash,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        HashObject::from_blob,
        HashObject::new,
        |o: &HashObject| o.is_empty(),
        |o: &HashObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
    .await
  }

  /// HSCAN 以外的哈希命令统一慢路径分派（HSCAN 走 shared 慢路径扫描）
  pub(crate) async fn hash(
    storage: &StorageSession<'_, impl Device>,
    cmd: RespCommand,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let resp_version = storage.resp_protocol_version();
    let key = refs.first().copied().unwrap_or(&[]);
    let args = refs.get(1..).unwrap_or(&[]);

    // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
    // HDEL 不入表：删除重命令一律走下方对象层通道（物化求值 + 整值重灌），
    // 杜绝向分层树逐成员落删除墓碑（栈深不变量，见 tiered_collection_ops 头注）
    let op_opt = match cmd {
      RespCommand::Hset => Some(HashOperation::Hset),
      RespCommand::Hsetnx => Some(HashOperation::Hsetnx),
      RespCommand::Hmset => Some(HashOperation::Hmset),
      RespCommand::Hget => Some(HashOperation::Hget),
      RespCommand::Hmget => Some(HashOperation::Hmget),
      RespCommand::Hexists => Some(HashOperation::Hexists),
      RespCommand::Hlen => Some(HashOperation::Hlen),
      RespCommand::Hstrlen => Some(HashOperation::Hstrlen),
      RespCommand::Hgetall => Some(HashOperation::Hgetall),
      RespCommand::Hkeys => Some(HashOperation::Hkeys),
      RespCommand::Hvals => Some(HashOperation::Hvals),
      RespCommand::Hincrby => Some(HashOperation::Hincrby),
      RespCommand::Hincrbyfloat => Some(HashOperation::Hincrbyfloat),
      _ => None,
    };
    if try_tiered_arm(
      storage,
      key,
      GarnetObjectType::Hash,
      op_opt,
      output,
      async move |ctx, op, output| {
        exec_tiered_hash(
          &storage.batch,
          key,
          ctx,
          TieredCollectionArgs::new(op, (0, 0), args, resp_version),
          output,
        )
        .await
      },
    )
    .await?
    {
      return Ok(());
    }

    // HEXPIRE 族：word 打包对位同步段 hash_expire
    if matches!(
      cmd,
      RespCommand::Hexpire
        | RespCommand::Hpexpire
        | RespCommand::Hexpireat
        | RespCommand::Hpexpireat
    ) {
      let (is_ms, is_ts) = match cmd {
        RespCommand::Hexpire => (false, false),
        RespCommand::Hpexpire => (true, false),
        RespCommand::Hexpireat => (false, true),
        _ => (true, true),
      };
      // 参数推导单源（快慢共用，失败帧已写出）
      let Some(args) = parse_expire_elements_args(
        cmd.into(),
        refs,
        ElementHeaderKind::Fields,
        is_ms,
        is_ts,
        output,
      ) else {
        return Ok(());
      };
      hash_rmw_cold(
        storage,
        args.key,
        HashOperation::Hexpire,
        args.elements,
        args.args12,
        resp_version,
        output,
      )
      .await?;
      return Ok(());
    }
    // HTTL / HPERSIST 族
    if matches!(
      cmd,
      RespCommand::Httl
        | RespCommand::Hpttl
        | RespCommand::Hexpiretime
        | RespCommand::Hpexpiretime
        | RespCommand::Hpersist
    ) {
      let op = match cmd {
        RespCommand::Hpersist => HashOperation::Hpersist,
        _ => HashOperation::Httl,
      };
      let args12 = match cmd {
        RespCommand::Httl => (0, 0),
        RespCommand::Hpttl => (1, 0),
        RespCommand::Hexpiretime => (0, 1),
        RespCommand::Hpexpiretime => (1, 1),
        _ => (0, 0),
      };
      // 参数推导单源（快慢共用，失败帧已写出）
      let Some((key, fields)) =
        parse_elements_only_args(cmd.into(), refs, ElementHeaderKind::Fields, output)
      else {
        return Ok(());
      };
      hash_rmw_cold(storage, key, op, fields, args12, resp_version, output).await?;
      return Ok(());
    }

    // RMW 形态（同步段 Missing 走空对象求值矩阵，骨架对位）
    let rmw_spec = match cmd {
      RespCommand::Hset => Some((HashOperation::Hset, (0, 0))),
      RespCommand::Hsetnx => Some((HashOperation::Hsetnx, (0, 0))),
      RespCommand::Hmset => Some((HashOperation::Hmset, (0, 0))),
      RespCommand::Hget => Some((HashOperation::Hget, (0, 0))),
      RespCommand::Hdel => Some((HashOperation::Hdel, (0, 0))),
      RespCommand::Hincrby => Some((HashOperation::Hincrby, (0, 0))),
      RespCommand::Hincrbyfloat => Some((HashOperation::Hincrbyfloat, (0, 0))),
      _ => None,
    };
    if let Some((op, args12)) = rmw_spec {
      let done = hash_rmw_cold(storage, key, op, args, args12, resp_version, output).await?;
      if let Rmw::Present(done) = done {
        // HMSET 恒回 +OK；HSET/HSETNX/HDEL 补整数；其余负载已透写
        if op == HashOperation::Hmset {
          if !done.payload_written {
            output.extend_from_slice(cs::RESP_OK);
          }
        } else if matches!(
          op,
          HashOperation::Hset | HashOperation::Hsetnx | HashOperation::Hdel
        ) {
          write_rmw_reply(done, output);
        }
      }
      return Ok(());
    }

    // 装载 + operate 形态（同步段 Missing 短路应答逐一对位；HLEN 由
    // exec_slow O(1) 计数直读臂承接）
    let load_spec = match cmd {
      RespCommand::Hgetall => Some((HashOperation::Hgetall, 0, 0, ReplyOnMissing::EmptyList)),
      RespCommand::Hmget => Some((HashOperation::Hmget, 0, 0, ReplyOnMissing::NullArray)),
      RespCommand::Hexists => Some((HashOperation::Hexists, 0, 0, ReplyOnMissing::Zero)),
      RespCommand::Hstrlen => Some((HashOperation::Hstrlen, 0, 0, ReplyOnMissing::Zero)),
      RespCommand::Hkeys => Some((HashOperation::Hkeys, 0, 0, ReplyOnMissing::EmptyList)),
      RespCommand::Hvals => Some((HashOperation::Hvals, 0, 0, ReplyOnMissing::EmptyList)),
      RespCommand::Hrandfield => None, // 需参数打包，独立分支
      _ => None,
    };
    if let Some((op, arg1, arg2, missing)) = load_spec {
      return slow_load_eval(
        storage,
        key,
        GarnetObjectType::Hash,
        output,
        HashObject::from_blob,
        move |output: &mut Vec<u8>| match missing {
          ReplyOnMissing::Zero => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
          ReplyOnMissing::EmptyList => output.extend_from_slice(cs::RESP_EMPTYLIST),
          ReplyOnMissing::NullArray => write_null_array(output, refs.len() - 1, resp_version),
        },
        async move |obj: &mut super::HashObject, output: &mut Vec<u8>| {
          let result1 = run_operate(obj, op, args, arg1, arg2, resp_version, output).result1;
          if matches!(op, HashOperation::Hexists | HashOperation::Hstrlen) {
            output.write_resp_int(result1);
          }
        },
      )
      .await;
    }

    // HRANDFIELD：count/WITHVALUES 打包 + seed 对位同步段
    if cmd == RespCommand::Hrandfield {
      // 参数推导单源（快慢共用，失败帧已写出；第三词元大小写门与快侧同口径）
      let Some(rand_args) = parse_random_member_args("HRANDFIELD", refs, cs::WITHVALUES, output)
      else {
        return Ok(());
      };
      // count 为 0 不触达后端（应答与缺失态同形单源）
      if rand_args.param_count == 0 {
        write_random_member_missing(output, rand_args.included_count, resp_version);
        return Ok(());
      }
      let arg1 = rand_args.arg1;
      return slow_load_eval(
        storage,
        key,
        GarnetObjectType::Hash,
        output,
        HashObject::from_blob,
        |output: &mut Vec<u8>| {
          write_random_member_missing(output, rand_args.included_count, resp_version);
        },
        async move |obj: &mut super::HashObject, output: &mut Vec<u8>| {
          run_operate(
            obj,
            HashOperation::Hrandfield,
            &[],
            arg1,
            fastrand::i32(..),
            resp_version,
            output,
          );
        },
      )
      .await;
    }

    // 兜底臂：不应抵达慢路径的未接线命令形态（快路径参数校验已拦截）
    cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
    Ok(())
  }

  /// 装载形态 Missing 短路应答类别
  #[derive(Clone, Copy)]
  enum ReplyOnMissing {
    Zero,
    EmptyList,
    NullArray,
  }
}
