//! 集合命令（对标 libs/server/Resp/Objects/SetCommands.cs）
//!
//! 命令层只做参数校验与编解码：单键语义全部下沉到
//! [`crate::objects::set::set_object::SetObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致）；SINTER/SUNION/SDIFF
//! 族为多键聚合，对标 libs/server/Storage/Session/ObjectStore/SetOps.cs
//! 的装载-折叠语义在命令层就地求值。存取经与 storage 会话域共享的
//! `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]），载荷为
//! wobject bitcode `Vec<u8>` 数组。

use std::{collections::HashSet, io::Cursor};

use wobject::set::set_object::SetObject as WoSetObject;

use crate::{
  arg_slice::ArgSlice,
  input_header::RespInputHeader,
  inputs::ObjectInput,
  objects::{
    set::{
      set_object::{SetObject, SetOperation},
      set_object_impl::NO_COUNT,
    },
    types::object_output::ObjectOutput,
  },
  resp::{
    cmd_strings as cs,
    cmd_strings::write_error_raw,
    objects::object_store_utils::{OBJ_TAG_SET, SyncObj, obj_load_sync, obj_save_or_gc_sync},
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  session_parse_state::SessionParseState,
  types::{GarnetObjectType, RespInputFlags},
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

/// 从 wkv 信封载荷装载集合对象
///
/// 载荷双格式：默认 wobject bitcode（与 storage 会话域兼容）；C# BinaryWriter
/// 线格式（count 前缀 + 定长条目）作回退
pub(crate) fn set_from_blob(raw: &[u8]) -> SetObject {
  if let Ok(wo) = WoSetObject::deserialize(&mut Cursor::new(raw)) {
    return SetObject::from_members(wo.get_keys());
  }
  SetObject::deserialize(&mut Cursor::new(raw)).unwrap_or_default()
}

/// 序列化回 wkv 信封载荷
pub(crate) fn set_to_blob(obj: &SetObject) -> Vec<u8> {
  let wo = WoSetObject::new();
  {
    let pin = wo.set.pin();
    for member in obj.to_members() {
      pin.insert(member);
    }
  }
  let mut out = Vec::new();
  if wo.serialize(&mut out).is_err() {
    for member in obj.to_members() {
      out.extend_from_slice(&(member.len() as u32).to_le_bytes());
      out.extend_from_slice(&member);
    }
  }
  out
}

/// 构造 ObjectInput（backing 与 input 同生命周期存活）
fn make_input(
  op: SetOperation,
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

  let mut header = RespInputHeader::new_with_type(GarnetObjectType::Set, RespInputFlags::empty());
  header.set_sub_id(op as u8);
  (
    ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
    backing,
  )
}

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut SetObject,
  op: SetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let (input, _backing) = make_input(op, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  obj_out
}

/// 集合键同步装载结果
pub(crate) enum SetLoad {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// WrongType / 存储错误（错误行已写入输出）
  Error,
  /// 键缺失（可按空对象求值，但不得落库创建）
  Missing,
  /// 命中（信封载荷已解码）
  Present(SetObject),
}

/// 同步装载集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
pub(crate) fn set_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> SetLoad {
  match obj_load_sync(store, key, OBJ_TAG_SET) {
    Ok(None) => SetLoad::Degrade,
    Ok(Some(SyncObj::Missing)) => SetLoad::Missing,
    Ok(Some(SyncObj::WrongType)) => {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
      SetLoad::Error
    }
    Ok(Some(SyncObj::Present(p))) => SetLoad::Present(set_from_blob(&p)),
    Err(_) => {
      output.write_resp_error("generic error");
      SetLoad::Error
    }
  }
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
///
/// 返回 `Ok(false)` 表示磁盘侧须降级异步重放；`Err(())` 为存储层错误（由调用方写错误行）
pub(crate) fn set_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SetObject,
) -> Result<bool, ()> {
  let payload = set_to_blob(obj);
  obj_save_or_gc_sync(store, key, OBJ_TAG_SET, &payload, obj.set.is_empty()).map_err(|_| ())
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
  op: SetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
  let (mut obj, existed) = match set_load_sync(store, key, output) {
    SetLoad::Degrade => return Rmw::Degrade,
    SetLoad::Error => return Rmw::Error,
    SetLoad::Missing => (SetObject::new(), false),
    SetLoad::Present(o) => (o, true),
  };

  let obj_out = run_operate(&mut obj, op, args, arg1, arg2);
  let result1 = obj_out.result1;

  // 回写须先于回复输出：降级时保持输出零污染，交由异步重放整体重写
  if should_write_back(op, &obj_out, &obj, existed) {
    match set_save_or_gc(store, key, &obj) {
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
/// - 仅回填 result1 的删除类操作（SREM）以移除计数为准。
fn should_write_back(op: SetOperation, out: &ObjectOutput, obj: &SetObject, existed: bool) -> bool {
  if is_read_only(op)
    || out.has_wrong_type()
    || out.payload.first() == Some(&b'-')
    || (!existed && obj.set.is_empty())
  {
    return false;
  }
  match op {
    SetOperation::Srem => out.result1 > 0,
    _ => true,
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: SetOperation) -> bool {
  matches!(
    op,
    SetOperation::Scard
      | SetOperation::Smembers
      | SetOperation::Sismember
      | SetOperation::Smismember
      | SetOperation::Srandmember
      | SetOperation::Sscan
  )
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(None),
      SetLoad::Error => return Err(()),
      SetLoad::Missing => objs.push(SetObject::new()),
      SetLoad::Present(o) => objs.push(o),
    }
  }
  Ok(Some(objs))
}

impl RespServerSession {
  /// SADD key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetAdd
  pub fn set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SADD");
      return Ok(true);
    }

    let key = parse_state[0];
    match rmw(
      store,
      key,
      SetOperation::Sadd,
      &parse_state[1..],
      0,
      0,
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::Error => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出
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

  /// SREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRemove
  pub fn set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SREM");
      return Ok(true);
    }

    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND → :0
      SetLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Srem, &parse_state[1..], 0, 0);
        if obj_out.result1 > 0 {
          match set_save_or_gc(store, key, &obj) {
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

  /// SCARD key
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetLength
  pub fn set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, "SCARD");
      return Ok(true);
    }
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND → :0
      SetLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Scard, &[], 0, 0);
        output.write_resp_int(obj_out.result1);
      }
    }
    Ok(true)
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
    if parse_state.len() != 1 {
      cs::abort_with_wrong_number_of_arguments(output, "SMEMBERS");
      return Ok(true);
    }
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND → WriteEmptySet（RESP2 退化为 *0）
      SetLoad::Missing => output.extend_from_slice(cs::RESP_EMPTYLIST),
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Smembers, &[], 0, 0);
        output.extend_from_slice(&obj_out.payload);
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
    if parse_state.len() != 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SISMEMBER");
      return Ok(true);
    }
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND → :0
      SetLoad::Missing => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Sismember, &parse_state[1..], 0, 0);
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// SMISMEMBER key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIsMember（SMISMEMBER 共体）
  pub fn set_multi_is_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SMISMEMBER");
      return Ok(true);
    }
    let key = parse_state[0];
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND：count-1 个 :0 数组
      SetLoad::Missing => {
        output.write_resp_array_len(parse_state.len() - 1);
        for _ in 1..parse_state.len() {
          output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        }
      }
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Smismember, &parse_state[1..], 0, 0);
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// SPOP key [count]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetPop
  ///
  /// 弹空后整键回收
  pub fn set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SPOP");
      return Ok(true);
    }

    let key = parse_state[0];

    let count_parameter = if parse_state.len() == 2 {
      match parse_state[1].try_parse_i64() {
        // C#：非整数或负数 → VALUE_IS_NOT_INTEGER
        Some(c) if (0..=i64::from(i32::MAX)).contains(&c) => c as i32,
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

    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND → WriteNull
      SetLoad::Missing => output.write_resp_null(),
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Spop, &[], count_parameter, 0);
        match set_save_or_gc(store, key, &obj) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(()) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// SRANDMEMBER key [count]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRandomMember
  pub fn set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SRANDMEMBER");
      return Ok(true);
    }

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
      SetLoad::Error => {}
      // C# NOTFOUND：带 count → 空数组；无 count → null
      SetLoad::Missing => {
        if parse_state.len() == 2 {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null();
        }
      }
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Srandmember,
          &[],
          count_parameter,
          seed,
        );
        output.extend_from_slice(&obj_out.payload);
      }
    }
    Ok(true)
  }

  /// SMOVE source destination member
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetMove
  /// （存储侧语义对标 libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMove）
  pub fn set_move<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      cs::abort_with_wrong_number_of_arguments(output, "SMOVE");
      return Ok(true);
    }

    let source_key = parse_state[0];
    let destination_key = parse_state[1];
    let member = parse_state[2];

    let mut src = match set_load_sync(store, source_key, output) {
      SetLoad::Degrade => return Ok(false),
      // C# NOTFOUND → :0
      SetLoad::Missing => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        return Ok(true);
      }
      SetLoad::Error => return Ok(true),
      SetLoad::Present(o) => o,
    };

    // If the keys are the same, no operation is performed.
    if source_key == destination_key {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    }

    let mut dst = match set_load_sync(store, destination_key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => return Ok(true),
      SetLoad::Missing => SetObject::new(),
      SetLoad::Present(o) => o,
    };

    if !src.set.remove(member) {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    }
    src.update_size(member, false);

    dst.set.insert(member.to_vec());
    dst.update_size(member, true);

    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    match set_save_or_gc(store, source_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(()) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match set_save_or_gc(store, destination_key, &dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(()) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    output.extend_from_slice(cs::RESP_RETURN_VAL_1);
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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "SINTER");
      return Ok(true);
    }

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = intersect_sets(&objs);
    write_set_members(&result, output);
    Ok(true)
  }

  /// SINTERSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectStore
  pub fn set_intersect_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SINTERSTORE");
      return Ok(true);
    }

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = intersect_sets(&objs);
    combine_store(self, dst, &result, store, output)
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
    // Need at least numkeys + 1 key
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SINTERCARD");
      return Ok(true);
    }

    let Some(num_keys) = parse_state[0].try_parse_i64() else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if num_keys < 1 {
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
      return Ok(true);
    }
    if parse_state.len() < num_keys as usize + 1 {
      cs::abort_with_error_message(output, "ERR numkeys should be greater than 0");
      return Ok(true);
    }

    // Optional LIMIT argument
    let mut limit: Option<i64> = None;
    if parse_state.len() > num_keys as usize + 1 {
      if !parse_state[num_keys as usize + 1].eq_ignore_ascii_case(b"LIMIT")
        || parse_state.len() != num_keys as usize + 3
      {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      let Some(limit_val) = parse_state[num_keys as usize + 2].try_parse_i64() else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      if limit_val < 0 {
        cs::abort_with_error_message(output, "ERR LIMIT can't be negative");
        return Ok(true);
      }
      limit = Some(limit_val);
    }

    let keys = &parse_state[1..=num_keys as usize];
    let objs = match load_many(store, keys, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let mut card = intersect_sets(&objs).set.len() as i64;
    if let Some(limit) = limit
      && limit > 0
    {
      card = card.min(limit);
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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "SUNION");
      return Ok(true);
    }

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let mut result = SetObject::new();
    for obj in &objs {
      for member in obj.to_members() {
        result.set.insert(member);
      }
    }
    write_set_members(&result, output);
    Ok(true)
  }

  /// SUNIONSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnionStore
  pub fn set_union_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SUNIONSTORE");
      return Ok(true);
    }

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let mut result = SetObject::new();
    for obj in &objs {
      for member in obj.to_members() {
        result.set.insert(member);
      }
    }
    combine_store(self, dst, &result, store, output)
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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "SDIFF");
      return Ok(true);
    }

    let objs = match load_many(store, parse_state, output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = diff_sets(&objs);
    write_set_members(&result, output);
    Ok(true)
  }

  /// SDIFFSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiffStore
  pub fn set_diff_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "SDIFFSTORE");
      return Ok(true);
    }

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = diff_sets(&objs);
    combine_store(self, dst, &result, store, output)
  }
}

/// 集合求交（首集复制后逐集收缩；缺失键视为空集 → 空结果）
///
/// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersect
fn intersect_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  for member in first.to_members() {
    result.set.insert(member);
  }

  for obj in &objs[1..] {
    // intersection of anything with empty set is empty set
    if result.set.is_empty() {
      break;
    }
    let members: HashSet<&[u8]> = obj.set.iter().map(|m| m.as_slice()).collect();
    result.set.retain(|m| members.contains(m.as_slice()));
  }
  result
}

/// 集合求差（首集减去其余各集）
///
/// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff
fn diff_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  for member in first.to_members() {
    result.set.insert(member);
  }

  for obj in &objs[1..] {
    let members: HashSet<&[u8]> = obj.set.iter().map(|m| m.as_slice()).collect();
    result.set.retain(|m| !members.contains(m.as_slice()));
  }
  result
}

/// SINTER/SUNION/SDIFF 的 *STORE 公共收尾：空结果回收目标键，否则写回并回基数
fn combine_store<'a, D: wdev::Device>(
  _session: &mut RespServerSession,
  dst: &[u8],
  result: &SetObject,
  store: &wkv::BatchStoreSession<'a, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  match set_save_or_gc(store, dst, result) {
    Ok(true) => output.write_resp_int(result.set.len() as i64),
    Ok(false) => return Ok(false),
    Err(()) => output.write_resp_error("generic error"),
  }
  Ok(true)
}

/// 结果集合的 RESP 输出（RESP2 等长数组）
fn write_set_members(result: &SetObject, output: &mut Vec<u8>) {
  let members = result.to_members();
  output.write_resp_array_len(members.len());
  for member in members {
    output.write_resp_bulk_string(&member);
  }
}

#[cfg(test)]
mod tests {
  use std::{io::Cursor, str, sync::Arc};

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wobject::set::set_object::SetObject as WoSetObject;

  use super::{
    super::object_store_utils::{OBJ_TAG_SET, obj_encode},
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

  /// rmw 回写契约：SADD 计数落库、SREM 清空回收、幻键防护、WRONGTYPE、信封互通
  #[test]
  fn rmw_writeback_contract() {
    let (_dir, _store, session) = fixture("setwb.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    // SADD a b a：新增 2
    sess
      .set_add(&[b"st", b"a", b"b", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // SCARD / SISMEMBER
    out.clear();
    sess.set_length(&[b"st"], &batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    sess
      .set_is_member(&[b"st", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .set_is_member(&[b"st", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    // 键缺失 SISMEMBER → :0
    out.clear();
    sess
      .set_is_member(&[b"nk", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // SMISMEMBER
    out.clear();
    sess
      .set_multi_is_member(&[b"st", b"a", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:0\r\n");
    // 键缺失 → 全 0 数组
    out.clear();
    sess
      .set_multi_is_member(&[b"nk", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:0\r\n:0\r\n");

    // SMEMBERS（迭代序随散列，排序比对）
    out.clear();
    sess.set_members(&[b"st"], &batch, &mut out).unwrap();
    let items = parse_bulk_array(&out);
    let mut sorted = items.clone();
    sorted.sort();
    assert_eq!(sorted, vec![b"a".to_vec(), b"b".to_vec()]);
    out.clear();
    sess.set_members(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");

    // SREM 部分移除
    out.clear();
    sess
      .set_remove(&[b"st", b"a", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    // SREM 清空 → 整键回收
    out.clear();
    sess.set_remove(&[b"st", b"b"], &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    assert!(
      batch
        .try_read_sync(b"st", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );
    // SREM 键缺失 → :0
    out.clear();
    sess.set_remove(&[b"st", b"b"], &batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 字符串键 → WRONGTYPE 不覆写
    let _ = batch.try_upsert_sync(b"str", b"plain-value");
    out.clear();
    sess.set_add(&[b"str", b"m"], &batch, &mut out).unwrap();
    assert_eq!(out, WRONGTYPE);
    assert_eq!(
      batch
        .try_read_sync(b"str", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten(),
      Some(b"plain-value".to_vec())
    );

    // 信封互通：RESP 写入 = [OBJ_TAG_SET][wobject bitcode]
    sess
      .set_add(&[b"env", b"m"], &batch, &mut Vec::new())
      .unwrap();
    let raw = batch
      .try_read_sync(b"env", |v| v.to_vec())
      .ok()
      .flatten()
      .flatten()
      .expect("envelope value");
    assert_eq!(raw[0], OBJ_TAG_SET);
    let from_storage = WoSetObject::deserialize(&mut Cursor::new(&raw[1..])).unwrap();
    assert!(from_storage.set.pin().contains(b"m".as_slice()));

    // 反向：storage 会话域信封 RESP 层可读
    let ext = WoSetObject::new();
    ext.set.pin().insert(b"pv".to_vec());
    let mut payload = Vec::new();
    ext.serialize(&mut payload).unwrap();
    let _ = batch.try_upsert_sync(b"fromstore", &obj_encode(OBJ_TAG_SET, &payload));
    out.clear();
    sess
      .set_is_member(&[b"fromstore", b"pv"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  }

  /// SPOP / SRANDMEMBER 形态端到端
  #[test]
  fn pop_and_random() {
    let (_dir, _store, session) = fixture("setpop.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .set_add(&[b"st", b"a", b"b"], &batch, &mut Vec::new())
      .unwrap();

    // count=0 → 空数组；负 count / 非整数 → 错误
    out.clear();
    sess.set_pop(&[b"st", b"0"], &batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");
    out.clear();
    sess.set_pop(&[b"st", b"-1"], &batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // 带 count：数组形态；弹空整键回收
    out.clear();
    sess.set_pop(&[b"st", b"10"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"*2\r\n"));
    assert!(
      batch
        .try_read_sync(b"st", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // 键缺失：无 count / 带 count 均 nil（C# NOTFOUND → WriteNull）
    out.clear();
    sess.set_pop(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();
    sess.set_pop(&[b"nk", b"5"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    // SRANDMEMBER
    sess
      .set_add(&[b"r", b"m1", b"m2", b"m3"], &batch, &mut Vec::new())
      .unwrap();
    // count=0 → 空数组
    out.clear();
    sess
      .set_random_member(&[b"r", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
    // 正 count：不弹出
    out.clear();
    sess
      .set_random_member(&[b"r", b"2"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*2\r\n"));
    out.clear();
    sess.set_length(&[b"r"], &batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");
    // 负 count：可重复
    out.clear();
    sess
      .set_random_member(&[b"r", b"-4"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*4\r\n"));
    // 无 count：单 bulk（成员均为 2 字节）
    out.clear();
    sess.set_random_member(&[b"r"], &batch, &mut out).unwrap();
    assert!(out.starts_with(b"$2\r\n"));
    // 键缺失：无 count → nil；带 count → 空数组
    out.clear();
    sess.set_random_member(&[b"nk"], &batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();
    sess
      .set_random_member(&[b"nk", b"2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
  }

  /// SMOVE / SINTER / SINTERCARD / SUNION / SDIFF 及 *STORE 端到端
  #[test]
  fn multi_key_ops() {
    let (_dir, _store, session) = fixture("setmulti.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();

    sess
      .set_add(&[b"a", b"m1", b"m2", b"m3"], &batch, &mut Vec::new())
      .unwrap();
    sess
      .set_add(&[b"b", b"m2", b"m4"], &batch, &mut Vec::new())
      .unwrap();

    // SINTER 2 a b → m2
    out.clear();
    sess.set_intersect(&[b"a", b"b"], &batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$2\r\nm2\r\n");

    // SINTERCARD
    out.clear();
    sess
      .set_intersect_length(&[b"2", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    // LIMIT 0 视为不限（C#: limit > 0 才取 min）
    out.clear();
    sess
      .set_intersect_length(&[b"2", b"a", b"b", b"LIMIT", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    // LIMIT 钳制到 0 上限之外的正值
    out.clear();
    sess
      .set_intersect_length(&[b"2", b"a", b"b", b"LIMIT", b"9"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    // LIMIT 词形错误
    out.clear();
    sess
      .set_intersect_length(&[b"2", b"a", b"b", b"LIM", b"0"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");
    // numkeys < 1
    out.clear();
    sess
      .set_intersect_length(&[b"0", b"a"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR numkeys should be greater than 0\r\n");

    // SUNION → 4 成员
    out.clear();
    sess.set_union(&[b"a", b"b"], &batch, &mut out).unwrap();
    let items = parse_bulk_array(&out);
    let mut sorted = items.clone();
    sorted.sort();
    assert_eq!(
      sorted,
      vec![
        b"m1".to_vec(),
        b"m2".to_vec(),
        b"m3".to_vec(),
        b"m4".to_vec()
      ]
    );

    // SDIFF a b → m1 m3
    out.clear();
    sess.set_diff(&[b"a", b"b"], &batch, &mut out).unwrap();
    let items = parse_bulk_array(&out);
    let mut sorted = items.clone();
    sorted.sort();
    assert_eq!(sorted, vec![b"m1".to_vec(), b"m3".to_vec()]);

    // *STORE 形态
    out.clear();
    sess
      .set_intersect_store(&[b"i_dst", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .set_union_store(&[b"u_dst", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");
    out.clear();
    sess
      .set_diff_store(&[b"d_dst", b"a", b"b"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    sess.set_length(&[b"i_dst"], &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    // 空结果 *STORE：目标回收（集合自身与自身求差 → 空）
    sess
      .set_add(&[b"only_x", b"x"], &batch, &mut Vec::new())
      .unwrap();
    out.clear();
    sess
      .set_diff_store(&[b"i_dst", b"only_x", b"only_x"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    assert!(
      batch
        .try_read_sync(b"i_dst", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // SMOVE：命中搬移 + 源空回收
    sess
      .set_add(&[b"s1", b"m", b"keep"], &batch, &mut Vec::new())
      .unwrap();
    out.clear();
    sess
      .set_move(&[b"s1", b"s2", b"m"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .set_is_member(&[b"s2", b"m"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // SMOVE 源空回收后，剩余成员保留
    out.clear();
    sess
      .set_move(&[b"s1", b"s2", b"keep"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert!(
      batch
        .try_read_sync(b"s1", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten()
        .is_none()
    );

    // SMOVE 成员不在源 → :0；源缺失 → :0；同键 → :0
    out.clear();
    sess
      .set_move(&[b"s2", b"s3", b"nx"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess
      .set_move(&[b"nk", b"s2", b"m"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess
      .set_move(&[b"s2", b"s2", b"m"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  }

  /// 解析 RESP 批量字符串数组帧（测试辅助）
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
