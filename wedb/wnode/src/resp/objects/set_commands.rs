//! 集合命令（对标 libs/server/Resp/Objects/SetCommands.cs）
//!
//! 命令层只做参数校验与编解码：单键语义全部下沉到
//! [`wcol::set::set_object::SetObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致）；SINTER/SUNION/SDIFF
//! 族为多键聚合，对标 libs/server/Storage/Session/ObjectStore/SetOps.cs
//! 的装载-折叠语义在命令层就地求值。存取经与 storage 会话域共享的
//! `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
use wbase::num::strict_i32;
use wcol::{
  ObjectOutput,
  set::{
    set_object::{SetObject, SetOperation},
    set_object_impl::NO_COUNT,
  },
};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::{RespSliceExt, RespVecExt},
};
use wval::GarnetObjectType;

use crate::{
  resp::{
    objects::object_store_utils::{
      GarnetObjectPayload, IntersectCardKind, ObjLoad, RespRmwDone, SyncRmwCmd, SyncRmwHandlers,
      obj_length_sync, obj_load_typed_sync, obj_save_or_gc, parse_intersect_card_args,
      run_sync_rmw,
    },
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::del_ttl_sync,
};

pub(crate) type SetLoad = ObjLoad<SetObject>;
type Rmw = ObjLoad<RespRmwDone>;

/// SPOP key \[count\] 参数推导单源（快慢路径共用；解析失败时已写出错误应答
/// 并返回 None），返回 (key, count；缺省 NO_COUNT)
///
/// 判定序对标 C# SetCommands.cs 的 SetPop：arity 1..=2 → count 非整数
/// （含溢出）或负数同报 NOT_INTEGER
pub(crate) fn parse_set_pop_args<'a>(
  parse_state: &'a [&'a [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'a [u8], i32)> {
  check_arg_count!(parse_state, 1..=2, output, "SPOP", return None);
  let count = match parse_state.get(1) {
    None => NO_COUNT,
    // C#：非整数（含溢出）或负数 → VALUE_IS_NOT_INTEGER
    Some(raw) => match strict_i32(raw) {
      Some(c) if c >= 0 => c,
      _ => {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return None;
      }
    },
  };
  Some((parse_state[0], count))
}

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
fn run_operate(
  obj: &mut SetObject,
  op: SetOperation,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
) -> ObjectOutput {
  let mut obj_out = ObjectOutput::new();
  obj.operate(op as u8, args, arg1, arg2, &mut obj_out, resp_version);
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
    GarnetObjectType::Set,
    output,
    SetObject::from_blob,
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
    GarnetObjectType::Set,
    obj,
    obj.set.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的删除类操作（SREM）以移除计数为准。
fn should_write_back(op: SetOperation, out: &ObjectOutput, obj: &SetObject, existed: bool) -> bool {
  if is_read_only(op) || out.payload.first() == Some(&b'-') || (!existed && obj.set.is_empty()) {
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
      SetLoad::WrongType => return Err(()),
      SetLoad::Missing => objs.push(SetObject::new()),
      SetLoad::Present(o) => objs.push(o),
    }
  }
  Ok(Some(objs))
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn set_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl wdev::Device>,
    key: &[u8],
    op: SetOperation,
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
        tag: GarnetObjectType::Set,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        SetObject::from_blob,
        SetObject::new,
        |o: &SetObject| o.set.is_empty(),
        |o: &SetObject| o.to_blob(),
        |obj, op, args| run_operate(obj, op, args, arg1, arg2, resp_version),
        should_write_back,
      ),
    )
  }
  /// SADD key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetAdd
  pub fn set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SADD");

    let key = parse_state[0];
    match self.set_rmw(
      store,
      key,
      SetOperation::Sadd,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出
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

  /// SREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRemove
  pub fn set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SREM");

    let key = parse_state[0];
    match self.set_rmw(
      store,
      key,
      SetOperation::Srem,
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
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Smembers,
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
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Sismember,
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
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Smismember,
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
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, count_parameter)) = parse_set_pop_args(parse_state, output) else {
      return Ok(true);
    };

    // C# countParameter == 0 → 空集合（WriteEmptySet 版本分派，不触达后端）
    if count_parameter == 0 {
      cs::write_set_len(output, 0, self.resp_protocol_version);
      return Ok(true);
    }

    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND: parseState.Count == 2 (有 count) → WriteEmptySet（版本分派），否则 WriteNull ($-1\r\n)
      SetLoad::Missing => {
        if count_parameter != NO_COUNT {
          cs::write_set_len(output, 0, self.resp_protocol_version);
        } else {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
      SetLoad::Present(mut obj) => {
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Spop,
          &[],
          count_parameter,
          0,
          self.resp_protocol_version,
        );
        match set_save_or_gc(store, key, &obj) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
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
        let obj_out = run_operate(
          &mut obj,
          SetOperation::Srandmember,
          &[],
          count_parameter,
          seed,
          self.resp_protocol_version,
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
    check_arg_count!(parse_state, 3, output, "SMOVE");

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
      SetLoad::WrongType => return Ok(true),
      SetLoad::Present(o) => o,
    };

    // If the keys are the same, no operation is performed.
    if source_key == destination_key {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    }

    let mut dst = match set_load_sync(store, destination_key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => return Ok(true),
      SetLoad::Missing => SetObject::new(),
      SetLoad::Present(o) => o,
    };

    let Some(item) = src.set.take(member) else {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    };
    src.update_size(member, false);

    // C# SetMove → SetAdd 条件记账：目标已含成员时不重复添加、不虚增堆记账
    if dst.set.insert(item) {
      dst.update_size(member, true);
    }

    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    match set_save_or_gc(store, source_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match set_save_or_gc(store, destination_key, &dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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

  /// SINTERSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectStore
  pub fn set_intersect_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SINTERSTORE");

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

  /// SUNIONSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnionStore
  pub fn set_union_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SUNIONSTORE");

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = union_sets(&objs);
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

  /// SDIFFSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiffStore
  pub fn set_diff_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SDIFFSTORE");

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

/// 集合求并（逐集并入，extend 单次插入免逐元素判重分支）
fn union_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  for obj in objs {
    result.set.extend(obj.set.iter().cloned());
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
///
/// 目标键为 SET 语义（清既有 key 级 TTL，对标 C# SetOps 的 SET 收尾），信封域
/// upsert 默认保留 TTL，非空写回前显式清退；空结果走 set_save_or_gc 删空臂
///（try_delete_sync 级联清 TTL，对齐 C# EXPIRE key 0）
fn combine_store<'a, D: wdev::Device>(
  dst: &[u8],
  result: &SetObject,
  store: &wkv::BatchStoreSession<'a, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  if !result.is_empty() {
    match del_ttl_sync(store, dst) {
      Ok(true) => {}
      // 环形页翻转：整体降级
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  match set_save_or_gc(store, dst, result) {
    Ok(true) => output.write_resp_int(result.set.len() as i64),
    Ok(false) => return Ok(false),
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  Ok(true)
}

/// 结果集合的 RESP 输出（集合头版本分派：RESP2 *N / RESP3 ~N）
///
/// 写出 set 成员列表（内部调用 cs::write_set_len 单点）
fn write_set_members(result: &SetObject, output: &mut Vec<u8>, resp_version: u8) {
  let members = result.to_members();
  cs::write_set_len(output, members.len(), resp_version);
  for member in members {
    output.write_resp_bulk_string(&member);
  }
}
/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/SetCommands.cs 各命令经 Tsavorite pending
/// 读 CompletePending 后重放的异步形态；装载/折叠核与同步段单源复用
/// （load_many_async / intersect_sets / union_sets / diff_sets /
/// write_set_members），写回经异步段唯一漏斗。`Err(())` 为存储 IO 失败，
/// 由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wcol::{
    ObjLoad as WcolObjLoad,
    set::{
      set_object::{SetObject, SetOperation},
      set_object_impl::NO_COUNT,
    },
  };
  use wdev::Device;
  use wresp::{
    cmd_strings as cs,
    command::RespCommand,
    ext::{RespSliceExt, RespVecExt},
  };
  use wval::GarnetObjectType;

  use super::{
    Rmw, diff_sets, intersect_sets, parse_set_pop_args, run_operate, should_write_back, union_sets,
    write_set_members,
  };
  use crate::{
    resp::objects::{
      object_store_utils::{
        GarnetObjectPayload, IntersectCardKind, SyncRmwCmd, SyncRmwHandlers, obj_load_typed_async,
        obj_writeback_tiered, parse_intersect_card_args, retire_tiered_dest, run_async_rmw,
        slow_load_eval, try_tiered_arm, write_rmw_reply,
      },
      tiered_collection_ops::{exec_tiered_set, tiered_materialize_blob},
    },
    storage::session::storage_session::StorageSession,
  };

  /// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
  async fn set_rmw_cold(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    op: SetOperation,
    args: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<Rmw, ()> {
    run_async_rmw(
      storage,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::Set,
        op,
        args,
        arg1: 0,
        arg2: 0,
      },
      output,
      SyncRmwHandlers::new(
        SetObject::from_blob,
        SetObject::new,
        |o: &SetObject| o.set.is_empty(),
        |o: &SetObject| o.to_blob(),
        |obj, op, args| run_operate(obj, op, args, 0, 0, resp_version),
        should_write_back,
      ),
    )
    .await
  }

  /// 多键异步装载（缺失按空集合）
  ///
  /// 对位同步段 [`super::load_many`]：`Ok(None)` = WRONGTYPE 错误行已写出
  /// （同步段 Ok(None) 降级臂在异步域不存在）；`Err(())` 为存储 IO 失败
  async fn load_many_async(
    storage: &StorageSession<'_, impl Device>,
    keys: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<Option<Vec<SetObject>>, ()> {
    let mut objs = Vec::with_capacity(keys.len());
    for key in keys {
      match obj_load(storage, key, output).await? {
        None => return Ok(None),
        Some(o) => objs.push(o),
      }
    }
    Ok(Some(objs))
  }

  /// *STORE 公共收尾慢路径对位（空结果回收目标键，否则清 TTL 后写回并回基数）
  async fn combine_store_cold(
    storage: &StorageSession<'_, impl Device>,
    dst: &[u8],
    result: &SetObject,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    if !result.is_empty() {
      // SET 语义清既有 key 级 TTL（对标 C# SetOps 的 SET 收尾）
      storage.persist_key(dst).await.map_err(|_| ())?;
    }
    if result.is_empty() {
      storage
        .delete_string(dst)
        .await
        .map(|_| ())
        .map_err(|_| ())?;
    } else {
      storage
        .obj_save(dst, GarnetObjectType::Set, &result.to_blob())
        .await
        .map_err(|_| ())?;
    }
    // 目标键若原为分层态：信封已接管（或删空），清退残留树
    retire_tiered_dest(storage, dst).await?;
    output.write_resp_int(result.set.len() as i64);
    Ok(())
  }

  /// SSCAN 以外的集合命令统一慢路径分派
  pub(crate) async fn set(
    storage: &StorageSession<'_, impl Device>,
    cmd: RespCommand,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let resp_version = storage.resp_protocol_version();
    let key = refs.first().copied().unwrap_or(&[]);
    let args = refs.get(1..).unwrap_or(&[]);

    // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
    // SREM / SPOP 不入表：删除重命令一律走下方对象层通道（物化求值 + 整值
    // 重灌），杜绝向分层树逐成员落删除墓碑（栈深不变量，见 tiered_collection_ops 头注）
    let op_opt = match cmd {
      RespCommand::Sadd => Some(SetOperation::Sadd),
      RespCommand::Smembers => Some(SetOperation::Smembers),
      RespCommand::Sismember => Some(SetOperation::Sismember),
      RespCommand::Smismember => Some(SetOperation::Smismember),
      RespCommand::Scard => Some(SetOperation::Scard),
      RespCommand::Srandmember => Some(SetOperation::Srandmember),
      _ => None,
    };
    if try_tiered_arm(
      storage,
      key,
      GarnetObjectType::Set,
      op_opt,
      output,
      async move |ctx, op, output| {
        exec_tiered_set(&storage.batch, key, ctx, op, args, output, resp_version).await
      },
    )
    .await?
    {
      return Ok(());
    }

    // RMW 形态（SADD/SREM）
    if matches!(cmd, RespCommand::Sadd | RespCommand::Srem) {
      let op = if cmd == RespCommand::Sadd {
        SetOperation::Sadd
      } else {
        SetOperation::Srem
      };
      let done = set_rmw_cold(storage, key, op, args, resp_version, output).await?;
      if let Rmw::Present(done) = done {
        write_rmw_reply(done, output);
      }
      return Ok(());
    }

    // 装载 + operate 形态（Missing 短路逐一对位同步段；SCARD 由 exec_slow
    // O(1) 计数直读臂承接）
    match cmd {
      RespCommand::Smembers => {
        return slow_load_eval(
          storage,
          key,
          GarnetObjectType::Set,
          output,
          SetObject::from_blob,
          |output: &mut Vec<u8>| cs::write_set_len(output, 0, resp_version),
          async move |obj: &mut SetObject, output: &mut Vec<u8>| {
            output.extend_from_slice(
              &run_operate(obj, SetOperation::Smembers, &[], 0, 0, resp_version).payload,
            );
          },
        )
        .await;
      }
      RespCommand::Sismember => {
        return slow_load_eval(
          storage,
          key,
          GarnetObjectType::Set,
          output,
          SetObject::from_blob,
          |output: &mut Vec<u8>| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
          async move |obj: &mut SetObject, output: &mut Vec<u8>| {
            output.extend_from_slice(
              &run_operate(obj, SetOperation::Sismember, args, 0, 0, resp_version).payload,
            );
          },
        )
        .await;
      }
      RespCommand::Smismember => {
        return slow_load_eval(
          storage,
          key,
          GarnetObjectType::Set,
          output,
          SetObject::from_blob,
          |output: &mut Vec<u8>| {
            output.write_resp_array_len(refs.len() - 1);
            for _ in 1..refs.len() {
              output.extend_from_slice(cs::RESP_RETURN_VAL_0);
            }
          },
          async move |obj: &mut SetObject, output: &mut Vec<u8>| {
            output.extend_from_slice(
              &run_operate(obj, SetOperation::Smismember, args, 0, 0, resp_version).payload,
            );
          },
        )
        .await;
      }
      RespCommand::Srandmember => {
        let count_parameter = match refs.get(1) {
          Some(c) => match c.try_parse_i64() {
            Some(v) if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&v) => v as i32,
            // 快路径已拦截非法 count，防御臂写明错误不静默
            _ => {
              cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
              return Ok(());
            }
          },
          None => NO_COUNT,
        };
        if count_parameter == 0 {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
          return Ok(());
        }
        return slow_load_eval(
          storage,
          key,
          GarnetObjectType::Set,
          output,
          SetObject::from_blob,
          |output: &mut Vec<u8>| {
            if refs.len() == 2 {
              output.extend_from_slice(cs::RESP_EMPTYLIST);
            } else {
              output.write_resp_null_ver(resp_version);
            }
          },
          async move |obj: &mut SetObject, output: &mut Vec<u8>| {
            output.extend_from_slice(
              &run_operate(
                obj,
                SetOperation::Srandmember,
                &[],
                count_parameter,
                fastrand::i32(..),
                resp_version,
              )
              .payload,
            );
          },
        )
        .await;
      }
      RespCommand::Spop => {
        return spop_cold(storage, refs, resp_version, output).await;
      }
      RespCommand::Smove => {
        return smove_cold(storage, refs, output).await;
      }
      RespCommand::Sinter | RespCommand::Sunion | RespCommand::Sdiff => {
        let Some(objs) = load_many_async(storage, refs, output).await? else {
          return Ok(());
        };
        let result = match cmd {
          RespCommand::Sinter => intersect_sets(&objs),
          RespCommand::Sunion => union_sets(&objs),
          _ => diff_sets(&objs),
        };
        write_set_members(&result, output, resp_version);
        return Ok(());
      }
      RespCommand::Sinterstore | RespCommand::Sunionstore | RespCommand::Sdiffstore => {
        let Some(objs) = load_many_async(storage, refs.get(1..).unwrap_or(&[]), output).await?
        else {
          return Ok(());
        };
        let result = match cmd {
          RespCommand::Sinterstore => intersect_sets(&objs),
          RespCommand::Sunionstore => union_sets(&objs),
          _ => diff_sets(&objs),
        };
        return combine_store_cold(storage, key, &result, output).await;
      }
      RespCommand::Sintercard => {
        // 参数推导单源（快慢共用，失败帧已写出；负 LIMIT 与快侧同帧拒）
        let Some(args) = parse_intersect_card_args(IntersectCardKind::Set, refs, output) else {
          return Ok(());
        };
        let Some(objs) = load_many_async(storage, args.keys, output).await? else {
          return Ok(());
        };
        let mut card = intersect_sets(&objs).set.len() as i64;
        if let Some(limit) = args.limit.filter(|&v| v > 0) {
          card = card.min(i64::from(limit));
        }
        output.write_resp_int(card);
        return Ok(());
      }
      _ => {}
    }

    cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
    Ok(())
  }

  /// SPOP 慢路径对位（Present 臂 operate 后异步删空/写回）
  async fn spop_cold(
    storage: &StorageSession<'_, impl Device>,
    refs: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, count_parameter)) = parse_set_pop_args(refs, output) else {
      return Ok(());
    };
    if count_parameter == 0 {
      cs::write_set_len(output, 0, resp_version);
      return Ok(());
    }
    // Missing 短路对位同步段：有 count → 空集合（版本分派）；无 count → null
    let with_count = refs.len() == 2;
    let Some(mut obj) = obj_load_shortcircuit(storage, key, output, |output| {
      if with_count {
        cs::write_set_len(output, 0, resp_version);
      } else {
        output.write_resp_null_ver(resp_version);
      }
    })
    .await?
    else {
      return Ok(());
    };
    let obj_out = run_operate(
      &mut obj,
      SetOperation::Spop,
      &[],
      count_parameter,
      0,
      resp_version,
    );
    save_or_gc(storage, key, &obj).await?;
    output.extend_from_slice(&obj_out.payload);
    Ok(())
  }

  /// SMOVE 慢路径对位（双键异步装载 + 移动 + 双写）
  async fn smove_cold(
    storage: &StorageSession<'_, impl Device>,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let Some((source_key, destination_key, member)) = refs.get(0..3).map(|r| (r[0], r[1], r[2]))
    else {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      return Ok(());
    };
    let Some(mut src) = obj_load(storage, source_key, output).await? else {
      return Ok(());
    };
    if src.set.is_empty() {
      // C# NOTFOUND → :0
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(());
    }
    if source_key == destination_key {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(());
    }
    let mut dst = match obj_load(storage, destination_key, output).await? {
      Some(o) => o,
      None => return Ok(()),
    };
    let Some(item) = src.set.take(member) else {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(());
    };
    src.update_size(member, false);
    // C# SetAdd 条件记账：目标已含成员时跳过 size 递增
    if dst.set.insert(item) {
      dst.update_size(member, true);
    }

    save_or_gc(storage, source_key, &src).await?;
    save_or_gc(storage, destination_key, &dst).await?;
    output.extend_from_slice(cs::RESP_RETURN_VAL_1);
    Ok(())
  }

  /// 单键异步装载（None = WRONGTYPE 错误行已写出；缺失按空集合，
  /// 与同步段 Missing → 空对象矩阵对位）
  async fn obj_load(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    output: &mut Vec<u8>,
  ) -> Result<Option<SetObject>, ()> {
    match obj_load_typed_async(
      storage,
      key,
      GarnetObjectType::Set,
      output,
      SetObject::from_blob,
    )
    .await
    .map_err(|_| ())?
    {
      // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存对象
      WcolObjLoad::Degrade => {
        let Some(blob) =
          tiered_materialize_blob(&storage.batch, key, GarnetObjectType::Set).await?
        else {
          return Err(());
        };
        // 物化载荷解码 fail-fast：畸形落错中止，不回退空对象销毁原键
        match SetObject::from_blob(&blob) {
          Some(obj) => Ok(Some(obj)),
          None => {
            log::error!(
              "set obj_load: corrupted materialized payload, key='{}'",
              String::from_utf8_lossy(key)
            );
            Err(())
          }
        }
      }
      WcolObjLoad::WrongType => Ok(None),
      WcolObjLoad::Missing => Ok(Some(SetObject::new())),
      WcolObjLoad::Present(o) => Ok(Some(o)),
    }
  }

  /// 单键异步装载（None = WRONGTYPE/MISSING 短路已按同步段口径应答）
  async fn obj_load_shortcircuit(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    output: &mut Vec<u8>,
    on_missing: impl FnOnce(&mut Vec<u8>),
  ) -> Result<Option<SetObject>, ()> {
    match obj_load_typed_async(
      storage,
      key,
      GarnetObjectType::Set,
      output,
      SetObject::from_blob,
    )
    .await
    .map_err(|_| ())?
    {
      // 分层键：物化回内存对象（缺失短路应答仅在真缺失时触发）
      WcolObjLoad::Degrade => {
        let Some(blob) =
          tiered_materialize_blob(&storage.batch, key, GarnetObjectType::Set).await?
        else {
          return Err(());
        };
        // 物化载荷解码 fail-fast：畸形落错中止，不回退空对象销毁原键
        match SetObject::from_blob(&blob) {
          Some(obj) => Ok(Some(obj)),
          None => {
            log::error!(
              "set obj_load: corrupted materialized payload, key='{}'",
              String::from_utf8_lossy(key)
            );
            Err(())
          }
        }
      }
      WcolObjLoad::WrongType => Ok(None),
      WcolObjLoad::Missing => {
        on_missing(output);
        Ok(None)
      }
      WcolObjLoad::Present(o) => Ok(Some(o)),
    }
  }

  /// 删空回收或信封写回（对标 sync set_save_or_gc 的异步臂）
  async fn save_or_gc(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    obj: &SetObject,
  ) -> Result<(), ()> {
    obj_writeback_tiered(storage, key, GarnetObjectType::Set, obj).await
  }
}

#[cfg(test)]
mod write_set_members_tests {
  use wcol::set::set_object::SetObject;

  use super::write_set_members;

  /// SINTER/SUNION/SDIFF 结果集合头版本分派测试（RESP2 *N / RESP3 ~N）
  #[test]
  fn set_head_resp2_array_resp3_set() {
    let mut obj = SetObject::new();
    obj.set.insert(b"a".to_vec());

    let mut out2 = Vec::new();
    write_set_members(&obj, &mut out2, 2);
    assert_eq!(out2, b"*1\r\n$1\r\na\r\n");

    let mut out3 = Vec::new();
    write_set_members(&obj, &mut out3, 3);
    assert_eq!(out3, b"~1\r\n$1\r\na\r\n");
  }

  /// 空集位点（SMEMBERS 缺键 / SPOP count==0 / SPOP 缺键带 count）
  /// 对位 C# RespServerSessionOutput.cs:100 WriteEmptySet
  #[test]
  fn empty_set_resp2_star_resp3_tilde() {
    let obj = SetObject::new();

    let mut out2 = Vec::new();
    write_set_members(&obj, &mut out2, 2);
    assert_eq!(out2, b"*0\r\n");

    let mut out3 = Vec::new();
    write_set_members(&obj, &mut out3, 3);
    assert_eq!(out3, b"~0\r\n");
  }
}
