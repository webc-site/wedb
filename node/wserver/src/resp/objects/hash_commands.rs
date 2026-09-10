//! 哈希命令（对标 libs/server/Resp/Objects/HashCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`crate::objects::hash::hash_object::HashObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致），存取经与 storage 会话域
//! 共享的 `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]），
//! 载荷为 wobject bitcode `(field, value)`；携带成员级过期时落 C#
//! BinaryWriter 线格式。

use std::io::Cursor;

use wobject::hash::hash_object::HashObject as WoHashObject;

use crate::{
  arg_slice::ArgSlice,
  input_header::RespInputHeader,
  inputs::ObjectInput,
  objects::{
    hash::hash_object::{ExpireOption, HashObject, HashOperation},
    parse_utils::{now_ticks, try_get_expire_option, try_get_int, try_get_long},
    sortedset::sorted_set_object::ExpirationWithOption,
    types::object_output::ObjectOutput,
  },
  resp::{
    cmd_strings as cs,
    cmd_strings::write_error_raw,
    objects::object_store_utils::{OBJ_TAG_HASH, SyncObj, obj_load_sync, obj_save_or_gc_sync},
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  session_parse_state::SessionParseState,
  types::{GarnetObjectType, RespInputFlags},
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

/// 从 wkv 信封载荷装载哈希对象
///
/// 载荷双格式：默认 wobject bitcode（与 storage 会话域兼容）；携带成员级过期时
/// 落 C# BinaryWriter 线格式（bitcode 无过期槽位），读取先试 bitcode 再回退
pub(crate) fn hash_from_blob(raw: &[u8]) -> HashObject {
  if let Ok(wo) = WoHashObject::deserialize(&mut Cursor::new(raw)) {
    return HashObject::from_pairs(wo.hash_get_all());
  }
  // C# 线格式回退（成员级过期）
  HashObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化回 wkv 信封载荷（带成员过期时用 C# 线格式承载）
pub(crate) fn hash_to_blob(obj: &HashObject) -> Vec<u8> {
  if obj.has_expirable_items() {
    let mut csharp_format = Vec::new();
    let _ = obj.clone().serialize(&mut csharp_format);
    return csharp_format;
  }

  let wo = WoHashObject::new();
  {
    let pin = wo.hash.pin();
    for (key, value) in obj.to_pairs() {
      pin.insert(key, value);
    }
  }
  let mut out = Vec::new();
  if wo.serialize(&mut out).is_err() {
    for (key, value) in obj.to_pairs() {
      out.extend_from_slice(&(key.len() as u32).to_le_bytes());
      out.extend_from_slice(&key);
      out.extend_from_slice(&(value.len() as u32).to_le_bytes());
      out.extend_from_slice(&value);
    }
  }
  out
}

/// 构造 ObjectInput（backing 与 input 同生命周期存活）
fn make_input(
  op: HashOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> (ObjectInput, Vec<Vec<u8>>) {
  let backing: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  let slices: Vec<ArgSlice> = backing
    .iter()
    .map(|b| ArgSlice::new(b.as_ptr(), b.len()))
    .collect();
  let mut parse_state = SessionParseState::new();
  parse_state.initialize_with_args(&slices);

  let mut header = RespInputHeader::new_with_type(GarnetObjectType::Hash, RespInputFlags::empty());
  header.set_sub_id(op as u8);
  (
    ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
    backing,
  )
}

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut HashObject,
  op: HashOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let (input, _backing) = make_input(op, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  obj_out
}

/// 哈希键同步装载结果
pub(crate) enum HashLoad {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// WrongType / 存储错误（错误行已写入输出）
  Error,
  /// 键缺失（可按空对象求值，但不得落库创建）
  Missing,
  /// 命中（信封载荷已解码）
  Present(HashObject),
}

/// 同步装载哈希（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
pub(crate) fn hash_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> HashLoad {
  match obj_load_sync(store, key, OBJ_TAG_HASH) {
    Ok(None) => HashLoad::Degrade,
    Ok(Some(SyncObj::Missing)) => HashLoad::Missing,
    Ok(Some(SyncObj::WrongType)) => {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
      HashLoad::Error
    }
    Ok(Some(SyncObj::Present(p))) => HashLoad::Present(hash_from_blob(&p)),
    Err(_) => {
      output.write_resp_error("generic error");
      HashLoad::Error
    }
  }
}

/// 变更回写：空哈希整键回收（对齐 storage 层 hash_gc_if_empty 与命令域收尾）
///
/// 返回 `Ok(false)` 表示磁盘侧须降级异步重放；`Err(())` 为存储层错误（由调用方写错误行）
pub(crate) fn hash_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &HashObject,
) -> Result<bool, ()> {
  let payload = hash_to_blob(obj);
  obj_save_or_gc_sync(store, key, OBJ_TAG_HASH, &payload, obj.hash.is_empty()).map_err(|_| ())
}

/// rmw 结果
enum Rmw {
  /// 磁盘候选降级（未写任何输出）
  Degrade,
  /// 错误行已写出，调用方不得追加回复
  Error,
  /// 已闭环：RESP 负载已随 rmw 写出；payload_written=false 时 result1 供调用方回执
  Done { result1: i64, payload_written: bool },
}

/// 读-改-写骨架：装载 → operate → 变更回写 → 负载输出
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: HashOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
  let (mut obj, existed) = match hash_load_sync(store, key, output) {
    HashLoad::Degrade => return Rmw::Degrade,
    HashLoad::Error => return Rmw::Error,
    HashLoad::Missing => (HashObject::new(), false),
    HashLoad::Present(o) => (o, true),
  };

  let obj_out = run_operate(&mut obj, op, args, arg1, arg2);
  let result1 = obj_out.result1;

  // 回写须先于回复输出：降级时保持输出零污染，交由异步重放整体重写
  if should_write_back(op, &obj_out, &obj, existed) {
    match hash_save_or_gc(store, key, &obj) {
      Ok(true) => {}
      Ok(false) => return Rmw::Degrade,
      Err(()) => {
        output.write_resp_error("generic error");
        return Rmw::Error;
      }
    }
  }
  output.extend_from_slice(&obj_out.payload);

  Rmw::Done {
    result1,
    payload_written: !obj_out.payload.is_empty(),
  }
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

impl RespServerSession {
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

  /// HSETNX key field value
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashSet（HSETNX 形态）
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
  /// libs/server/Resp/Objects/HashCommands.cs:HashSet（HMSET 形态）
  pub fn hash_set_map<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hmset)
  }

  /// HSET/HSETNX/HMSET 公共体
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashSet
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
    if (op != HashOperation::Hsetnx && (parse_state.len() == 1 || parse_state.len() % 2 != 1))
      || (op == HashOperation::Hsetnx && parse_state.len() != 3)
    {
      cs::abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }

    let key = parse_state[0];
    let args = &parse_state[1..];
    match rmw(store, key, op, args, 0, 0, output) {
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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "HGET");
      return Ok(true);
    }
    let key = parse_state[0];
    if let Rmw::Degrade = rmw(
      store,
      key,
      HashOperation::Hget,
      &parse_state[1..],
      0,
      0,
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
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, "HGETALL");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → RESP_EMPTYLIST
      HashLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hgetall, &[], 0, 0);
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
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "HMGET");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：count-1 元素全 null 数组
      HashLoad::Missing => {
        write_null_array(output, parse_state.len() - 1);
      }
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hmget, &parse_state[1..], 0, 0);
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
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, "HLEN");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hlen, &[], 0, 0);
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
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "HDEL");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hdel, &parse_state[1..], 0, 0);
        if obj_out.result1 > 0 {
          match hash_save_or_gc(store, key, &obj) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(()) => {
              output.write_resp_error("generic error");
              return Ok(true);
            }
          }
        }
        // C# 仅回填 result1，整数回复由 RESP 层写出
        output.write_resp_int(obj_out.result1);
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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "HEXISTS");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hexists, &parse_state[1..], 0, 0);
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
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, name);
      return Ok(true);
    }
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
        let obj_out = run_operate(&mut obj, op, &[], 0, 0);
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// HVALS key
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashKeys（HVALS 共体）
  pub fn hash_vals<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_keys(parse_state, store, output, false)
  }

  /// HRANDFIELD key [count [WITHVALUES]]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashRandomField
  pub fn hash_random_field<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 3 {
      cs::abort_with_wrong_number_of_arguments(output, "HRANDFIELD");
      return Ok(true);
    }

    let key = parse_state[0];

    let mut param_count = 1_i64;
    let mut with_values = false;
    let mut included_count = false;

    if parse_state.len() >= 2 {
      let Some(v) = parse_state[1].try_parse_i64() else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      param_count = v;
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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "HSTRLEN");
      return Ok(true);
    }
    let key = parse_state[0];
    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND → :0
      HashLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hstrlen, &parse_state[1..], 0, 0);
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
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, name);
      return Ok(true);
    }

    let key = parse_state[0];
    let op = if is_float {
      HashOperation::Hincrbyfloat
    } else {
      HashOperation::Hincrby
    };
    if let Rmw::Degrade = rmw(store, key, op, &parse_state[1..], 0, 0, output) {
      return Ok(false);
    }
    Ok(true)
  }

  /// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExpire
  #[allow(clippy::too_many_lines)]
  pub fn hash_expire<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() <= 4 {
      cs::abort_with_wrong_number_of_arguments(output, "HEXPIRE");
      return Ok(true);
    }

    let key = parse_state[0];

    let Some(expiration) = try_get_long(parse_state[1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if expiration < 0 {
      cs::abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
      return Ok(true);
    }

    let mut curr_idx = 2;
    let mut expire_option = ExpireOption::NONE;
    if let Some(opt) = try_get_expire_option(parse_state[curr_idx]) {
      expire_option = opt;
      curr_idx += 1;
    }

    if !parse_state[curr_idx].eq_ignore_ascii_case(b"FIELDS") {
      cs::abort_with_error_message(
        output,
        "Mandatory argument FIELDS is missing or not at the right position",
      );
      return Ok(true);
    }
    curr_idx += 1;

    let Some(num_fields) = try_get_int(parse_state[curr_idx]) else {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    };
    curr_idx += 1;

    if num_fields < 1 {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    }
    if parse_state.len() != curr_idx + num_fields as usize {
      cs::abort_with_error_message(
        output,
        "The `numFields` parameter must match the number of arguments",
      );
      return Ok(true);
    }

    // .NET Ticks 目标时刻
    const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
    let step: i64 = if is_milliseconds { 10_000 } else { 10_000_000 };
    let expiration_ticks = if is_timestamp {
      UNIX_EPOCH_TICKS + expiration * step
    } else {
      now_ticks() + expiration * step
    };

    let e = ExpirationWithOption::new(expiration_ticks, expire_option);

    let args: Vec<&[u8]> = parse_state[curr_idx..].to_vec();
    match rmw(
      store,
      key,
      HashOperation::Hexpire,
      &args,
      (e.word() >> 32) as i32,
      e.word() as i32,
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
    if parse_state.len() <= 3 {
      cs::abort_with_wrong_number_of_arguments(output, "HTTL");
      return Ok(true);
    }

    let key = parse_state[0];

    if !parse_state[1].eq_ignore_ascii_case(b"FIELDS") {
      cs::abort_with_error_message(
        output,
        "Mandatory argument FIELDS is missing or not at the right position",
      );
      return Ok(true);
    }

    let Some(num_fields) = try_get_int(parse_state[2]) else {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    };
    if num_fields < 1 {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    }
    if parse_state.len() != 3 + num_fields as usize {
      cs::abort_with_error_message(
        output,
        "The `numFields` parameter must match the number of arguments",
      );
      return Ok(true);
    }

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：numFields 个 -2 数组
      HashLoad::Missing => {
        output.write_resp_array_len(num_fields as usize);
        for _ in 0..num_fields {
          output.extend_from_slice(cs::RESP_RETURN_VAL_N2);
        }
      }
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          HashOperation::Httl,
          &parse_state[3..],
          i32::from(is_milliseconds),
          i32::from(is_timestamp),
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
    if parse_state.len() <= 3 {
      cs::abort_with_wrong_number_of_arguments(output, "HPERSIST");
      return Ok(true);
    }

    let key = parse_state[0];

    if !parse_state[1].eq_ignore_ascii_case(b"FIELDS") {
      cs::abort_with_error_message(
        output,
        "Mandatory argument FIELDS is missing or not at the right position",
      );
      return Ok(true);
    }

    let Some(num_fields) = try_get_int(parse_state[2]) else {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    };
    if num_fields < 1 {
      cs::abort_with_error_message(output, "ERR Parameter `numFields` should be greater than 0");
      return Ok(true);
    }
    if parse_state.len() != 3 + num_fields as usize {
      cs::abort_with_error_message(
        output,
        "The `numFields` parameter must match the number of arguments",
      );
      return Ok(true);
    }

    match hash_load_sync(store, key, output) {
      HashLoad::Degrade => return Ok(false),
      HashLoad::Error => {}
      // C# NOTFOUND：numFields 个 -2 数组
      HashLoad::Missing => {
        output.write_resp_array_len(num_fields as usize);
        for _ in 0..num_fields {
          output.extend_from_slice(cs::RESP_RETURN_VAL_N2);
        }
      }
      HashLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, HashOperation::Hpersist, &parse_state[3..], 0, 0);
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }
}

/// nil 元素数组应答（HMGET 键缺失时的逐字段占位）
pub(super) fn write_null_array(output: &mut Vec<u8>, len: usize) {
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.write_resp_null();
  }
}

#[cfg(test)]
mod tests {
  use std::{io::Cursor, sync::Arc};

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wobject::hash::hash_object::HashObject as WoHashObject;

  use super::{
    super::object_store_utils::{OBJ_TAG_HASH, obj_encode},
    *,
  };

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  /// WRONGTYPE 错误应答帧
  const WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

  /// rmw 回写契约：HSET 变更落库、HDEL 清空回收键、错误不建幻键、
  /// WRONGTYPE 不覆盖异类键、信封与 storage 会话域互通
  #[test]
  fn rmw_writeback_contract() {
    let (_dir, _store, session) = fixture("hashwb.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // HSET 基础：新增计数落库
    sess
      .hash_set(&[b"h", b"f1", b"v1", b"f2", b"v2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // 覆写不计数
    out.clear();
    sess
      .hash_set(&[b"h", b"f1", b"v9"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // HGET 读回；缺失字段 nil；键缺失 nil
    out.clear();
    sess.hash_get(&[b"h", b"f1"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\nv9\r\n");
    out.clear();
    sess.hash_get(&[b"h", b"nx"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();
    sess.hash_get(&[b"nk", b"f"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    // HDEL 删空 → 整键回收；再删 → :0
    out.clear();
    sess
      .hash_delete(&[b"h", b"f1", b"f2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
    assert!(
      batch
        .try_read_sync(b"h", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );
    out.clear();
    sess.hash_delete(&[b"h", b"f1"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // HSET 非法 arity：不建幻键
    out.clear();
    sess.hash_set(&[b"ghost"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"-ERR wrong number of arguments for 'HSET' command\r\n"));
    assert!(
      batch
        .try_read_sync(b"ghost", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // 字符串键 → WRONGTYPE 且原值保留
    let _ = batch.try_upsert_sync(b"str", b"plain-value");
    out.clear();
    sess
      .hash_set(&[b"str", b"f", b"v"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, WRONGTYPE);
    assert_eq!(
      batch
        .try_read_sync(b"str", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten(),
      Some(b"plain-value".to_vec())
    );

    // 信封互通（r2 契约）：RESP 写入 = [OBJ_TAG_HASH][wobject bitcode]
    sess
      .hash_set(&[b"env", b"f", b"v"], &batch, &mut out)
      .unwrap();
    let raw = batch
      .try_read_sync(b"env", |v| v.to_vec())
      .ok()
      .flatten()
      .flatten()
      .expect("envelope value");
    assert_eq!(raw[0], OBJ_TAG_HASH);
    let from_storage = WoHashObject::deserialize(&mut Cursor::new(&raw[1..])).unwrap();
    assert_eq!(
      from_storage.hash.pin().get(b"f".as_slice()).cloned(),
      Some(b"v".to_vec())
    );

    // 反向：storage 会话域写入的同构信封 RESP 层可读
    let ext = WoHashObject::new();
    ext.hash.pin().insert(b"ext".to_vec(), b"pv".to_vec());
    let mut payload = Vec::new();
    ext.serialize(&mut payload).unwrap();
    let _ = batch.try_upsert_sync(b"fromstore", &obj_encode(OBJ_TAG_HASH, &payload));
    out.clear();
    sess
      .hash_get(&[b"fromstore", b"ext"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\npv\r\n");
  }

  /// HSETNX / HMSET 应答形态
  #[test]
  fn hsetnx_and_hmset_forms() {
    let (_dir, _store, session) = fixture("hashnx.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // HSETNX 新字段 → :1
    sess
      .hash_set_nx(&[b"h", b"f", b"v"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // HSETNX 已存在 → :0 且不覆写
    out.clear();
    sess
      .hash_set_nx(&[b"h", b"f", b"other"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess.hash_get(&[b"h", b"f"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");

    // HMSET → +OK；arity 错误
    out.clear();
    sess
      .hash_set_map(&[b"h", b"g", b"w"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    out.clear();
    sess.hash_set_map(&[b"h"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"-ERR wrong number of arguments for 'HMSET' command\r\n"));
    out.clear();
    sess.hash_set_nx(&[b"h", b"f"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"-ERR wrong number of arguments for 'HSETNX' command\r\n"));
  }

  /// HGETALL / HKEYS / HVALS / HMGET / HLEN / HEXISTS / HSTRLEN
  #[test]
  fn read_commands() {
    let (_dir, _store, session) = fixture("hashread.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .hash_set(&[b"h", b"a", b"1", b"b", b"22"], &batch, &mut out)
      .unwrap();
    out.clear();

    // HLEN
    sess.hash_length(&[b"h"], &batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    sess.hash_length(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // HGETALL：RESP2 扁平（迭代序随散列，排序比对）
    out.clear();
    sess.hash_get_all(&[b"h"], &batch, &mut out).unwrap();
    let items = parse_bulk_array(&out);
    let mut sorted = items.clone();
    sorted.sort();
    assert_eq!(
      sorted,
      vec![b"1".to_vec(), b"22".to_vec(), b"a".to_vec(), b"b".to_vec()]
    );
    out.clear();
    sess.hash_get_all(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");

    // HKEYS / HVALS
    out.clear();
    sess.hash_keys(&[b"h"], &batch, &mut out, true).unwrap();
    let mut keys = parse_bulk_array(&out);
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    out.clear();
    sess.hash_vals(&[b"h"], &batch, &mut out).unwrap();
    let mut vals = parse_bulk_array(&out);
    vals.sort();
    assert_eq!(vals, vec![b"1".to_vec(), b"22".to_vec()]);
    out.clear();
    sess.hash_keys(&[b"nk"], &batch, &mut out, true).unwrap();
    assert_eq!(out, b"*0\r\n");

    // HMGET：命中 + 缺失；键缺失全 null
    out.clear();
    sess
      .hash_get_multiple(&[b"h", b"a", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\n1\r\n$-1\r\n");
    out.clear();
    sess
      .hash_get_multiple(&[b"nk", b"f1", b"f2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$-1\r\n$-1\r\n");

    // HEXISTS
    out.clear();
    sess.hash_exists(&[b"h", b"a"], &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess.hash_exists(&[b"h", b"nx"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess.hash_exists(&[b"nk", b"f"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // HSTRLEN
    out.clear();
    sess
      .hash_str_length(&[b"h", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    sess
      .hash_str_length(&[b"h", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess
      .hash_str_length(&[b"nk", b"f"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  }

  /// HINCRBY / HINCRBYFLOAT 浮点文本语义端到端
  #[test]
  fn increment_commands() {
    let (_dir, _store, session) = fixture("hashincr.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // HINCRBY 新字段
    sess
      .hash_increment(&[b"h", b"f", b"10"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":10\r\n");

    // 累加负数
    out.clear();
    sess
      .hash_increment(&[b"h", b"f", b"-3"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":7\r\n");

    // HINCRBYFLOAT
    out.clear();
    sess
      .hash_increment(&[b"h", b"g", b"10.5"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.5\r\n");
    out.clear();
    sess
      .hash_increment(&[b"h", b"g", b"0.1"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.6\r\n");

    // 存量非整型 / 非浮点错误
    sess
      .hash_set(&[b"h", b"s", b"abc"], &batch, &mut Vec::new())
      .unwrap();
    out.clear();
    sess
      .hash_increment(&[b"h", b"s", b"1"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"-ERR hash value is not an integer.\r\n");
    out.clear();
    sess
      .hash_increment(&[b"h", b"s", b"1.5"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR hash value is not a float.\r\n");

    // 非法增量
    out.clear();
    sess
      .hash_increment(&[b"h", b"f", b"1.5"], &batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    out.clear();
    sess
      .hash_increment(&[b"h", b"f", b"inf"], &batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");

    // arity
    out.clear();
    sess
      .hash_increment(&[b"h", b"f"], &batch, &mut out, false)
      .unwrap();
    assert!(out.starts_with(b"-ERR wrong number of arguments for 'HINCRBY' command\r\n"));
  }

  /// HEXPIRE / HTTL / HPERSIST 家族端到端
  #[test]
  fn expire_family_commands() {
    let (_dir, _store, session) = fixture("hashexp.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .hash_set(&[b"h", b"a", b"1", b"b", b"2"], &batch, &mut out)
      .unwrap();
    out.clear();

    // HEXPIRE 3600 FIELDS 2 a zz
    sess
      .hash_expire(
        &[b"h", b"3600", b"FIELDS", b"2", b"a", b"zz"],
        &batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:-2\r\n");

    // HTTL 剩余秒（3590..3600）
    out.clear();
    sess
      .hash_time_to_live(
        &[b"h", b"FIELDS", b"1", b"a"],
        &batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .unwrap()
      .trim_start_matches(':')
      .parse()
      .unwrap();
    assert!((3590..=3600).contains(&ttl), "ttl = {payload}");

    // HTTL 键缺失 → 全 -2
    out.clear();
    sess
      .hash_time_to_live(
        &[b"nk", b"FIELDS", b"2", b"a", b"b"],
        &batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*2\r\n:-2\r\n:-2\r\n");

    // HPERSIST
    out.clear();
    sess
      .hash_persist(&[b"h", b"FIELDS", b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:-1\r\n");

    // FIELDS 词元缺失
    out.clear();
    sess
      .hash_time_to_live(&[b"h", b"NOPE", b"1", b"a"], &batch, &mut out, false, false)
      .unwrap();
    assert!(out.starts_with(b"-Mandatory argument FIELDS is missing"));

    // numFields 不匹配
    out.clear();
    sess
      .hash_persist(&[b"h", b"FIELDS", b"2", b"a"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-The `numFields` parameter must match the number of arguments"));

    // HEXPIRE 负过期
    out.clear();
    sess
      .hash_expire(
        &[b"h", b"-1", b"FIELDS", b"1", b"a"],
        &batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"-ERR invalid expire time, must be >= 0\r\n");
  }

  /// HRANDFIELD：无 count / count / WITHVALUES / count=0 / 键缺失
  #[test]
  fn random_field_command() {
    let (_dir, _store, session) = fixture("hashrand.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .hash_set(
        &[b"h", b"a", b"1", b"b", b"2", b"c", b"3"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    // 无 count：单 bulk
    sess.hash_random_field(&[b"h"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"$1\r\n"));

    // count=2：数组
    out.clear();
    sess
      .hash_random_field(&[b"h", b"2"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*2\r\n"));

    // count=2 WITHVALUES：RESP2 扁平 4 项
    out.clear();
    sess
      .hash_random_field(&[b"h", b"2", b"WITHVALUES"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*4\r\n"));

    // count 超集：钳制到 3
    out.clear();
    sess
      .hash_random_field(&[b"h", b"9"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*3\r\n"));

    // count=0：不触达后端
    out.clear();
    sess
      .hash_random_field(&[b"h", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // 键缺失：无 count → nil；带 count → 空数组
    out.clear();
    sess.hash_random_field(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();
    sess
      .hash_random_field(&[b"nk", b"2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // WITHVALUES 词形错误
    out.clear();
    sess
      .hash_random_field(&[b"h", b"2", b"WITHSCORES"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");
  }

  /// 解析 RESP 批量字符串数组帧为 Vec<Vec<u8>>（测试辅助）
  fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut items = Vec::new();
    let mut pos = frame.iter().position(|&b| b == b'\n').unwrap() + 1;
    while pos < frame.len() {
      assert_eq!(frame[pos], b'$');
      let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
      let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
        .unwrap()
        .parse()
        .unwrap();
      let start = len_end + 1;
      items.push(frame[start..start + len].to_vec());
      pos = start + len + 2;
    }
    items
  }
}
