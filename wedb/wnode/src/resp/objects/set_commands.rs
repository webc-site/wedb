//! 集合命令（对标 libs/server/Resp/Objects/SetCommands.cs）
//!
//! 命令层只做参数校验与编解码：单键语义全部下沉到
//! [`crate::objects::set::set_object::SetObject`] 的 operate/ObjectInput
//! 通道（与 C# GarnetObjectBase.Operate 分层一致）；SINTER/SUNION/SDIFF
//! 族为多键聚合，对标 libs/server/Storage/Session/ObjectStore/SetOps.cs
//! 的装载-折叠语义在命令层就地求值。存取经与 storage 会话域共享的
//! `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
use wresp::{RespSliceExt, RespVecExt, cmd_strings as cs};

use crate::{
  objects::{
    set::{
      set_object::{SetObject, SetOperation},
      set_object_impl::NO_COUNT,
    },
    types::object_output::ObjectOutput,
  },
  resp::{
    objects::object_store_utils::{
      ObjLoad, RmwOutcome, SyncRmwCmd, SyncRmwHandlers, make_object_input, obj_load_typed_sync,
      obj_save_or_gc, run_sync_rmw, set_from_blob, set_to_blob,
    },
    resp_server_session::RespServerSession,
  },
  types::GarnetObjectType,
};

/// 本命令面统一按 RESP2 协议输出（C# respProtocolVersion 由会话下发，
/// 会话层接线时替换为实际协商版本）
const RESP_VERSION: u8 = 2;

pub(crate) type SetLoad = ObjLoad<SetObject>;
type Rmw = RmwOutcome;

/// 经对象层 operate 通道执行操作，返回结构化输出
fn run_operate(
  obj: &mut SetObject,
  op: SetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
) -> ObjectOutput {
  let input = make_object_input(GarnetObjectType::Set, op as u8, args, arg1, arg2);
  let mut obj_out = ObjectOutput::new();
  obj.operate(&input, &mut obj_out, RESP_VERSION);
  obj_out
}

/// 同步装载集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn set_load_sync(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> SetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::Set as u8,
    output,
    set_from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
#[inline]
pub(crate) fn set_save_or_gc(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  obj: &SetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::Set as u8,
    obj,
    obj.set.is_empty(),
    set_to_blob,
  )
}

/// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
#[inline]
fn rmw(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  op: SetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  output: &mut Vec<u8>,
) -> Rmw {
  run_sync_rmw(
    store,
    SyncRmwCmd {
      key,
      tag: GarnetObjectType::Set as u8,
      op,
      args,
      arg1,
      arg2,
    },
    output,
    SyncRmwHandlers::new(
      set_from_blob,
      SetObject::new,
      |o: &SetObject| o.set.is_empty(),
      set_to_blob,
      |obj, op, args| run_operate(obj, op, args, arg1, arg2),
      should_write_back,
    ),
  )
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
    match rmw(
      store,
      key,
      SetOperation::Srem,
      &parse_state[1..],
      0,
      0,
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
  /// SMISMEMBER 入口（对应 C# SetIsMember 多值判定形态）
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

  /// SPOP key \[count\]
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
    let (key, count_parameter) = match parse_state {
      [key] => (*key, NO_COUNT),
      [key, arg] => {
        let count = match arg.try_parse_i64() {
          // C#：非整数或负数 → VALUE_IS_NOT_INTEGER
          Some(c) if (0..=i64::from(i32::MAX)).contains(&c) => c as i32,
          _ => {
            cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return Ok(true);
          }
        };
        (*key, count)
      }
      _ => {
        cs::abort_with_wrong_number_of_arguments(output, "SPOP");
        return Ok(true);
      }
    };

    // C# countParameter == 0 → 空数组（不触达后端）
    if count_parameter == 0 {
      output.extend_from_slice(cs::RESP_EMPTYLIST);
      return Ok(true);
    }

    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::Error => {}
      // C# NOTFOUND: parseState.Count == 2 (有 count) → WriteEmptySet (*0\r\n)，否则 WriteNull ($-1\r\n)
      SetLoad::Missing => {
        if count_parameter != NO_COUNT {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
        } else {
          output.write_resp_null();
        }
      }
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(&mut obj, SetOperation::Spop, &[], count_parameter, 0);
        match set_save_or_gc(store, key, &obj) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(_) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
        output.extend_from_slice(&obj_out.payload);
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
  /// （存储侧语义对标 SetOps.SetMove）
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

    let Some(item) = src.set.take(member) else {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    };
    src.update_size(member, false);

    dst.update_size(member, true);
    dst.set.insert(item);

    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    match set_save_or_gc(store, source_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    match set_save_or_gc(store, destination_key, &dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
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
    combine_store(dst, &result, store, output)
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
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
      return Ok(true);
    }
    if parse_state.len() < num_keys as usize + 1 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS);
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
      for member in &obj.set {
        result.set.insert(member.clone());
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
      for member in &obj.set {
        result.set.insert(member.clone());
      }
    }
    combine_store(dst, &result, store, output)
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
    combine_store(dst, &result, store, output)
  }
}

/// 集合求交（首集复制后逐集收缩；缺失键视为空集 → 空结果）
///
/// 对应 SetOps.cs:SetIntersect 算法的本地集合求交
fn intersect_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  result.set = first.set.clone();

  for obj in &objs[1..] {
    // intersection of anything with empty set is empty set
    if result.set.is_empty() {
      break;
    }
    result.set.retain(|m| obj.contains(m.as_slice()));
  }
  result
}

/// 集合求差（首集减去其余各集）
///
/// 对应 SetOps.cs:SetDiff 算法的本地集合求差
fn diff_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  result.set = first.set.clone();

  for obj in &objs[1..] {
    result.set.retain(|m| !obj.contains(m.as_slice()));
  }
  result
}

/// SINTER/SUNION/SDIFF 的 *STORE 公共收尾：空结果回收目标键，否则写回并回基数
fn combine_store<'a, D: wdev::Device>(
  dst: &[u8],
  result: &SetObject,
  store: &wkv::BatchStoreSession<'a, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  match set_save_or_gc(store, dst, result) {
    Ok(true) => output.write_resp_int(result.set.len() as i64),
    Ok(false) => return Ok(false),
    Err(_) => output.write_resp_error("generic error"),
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
