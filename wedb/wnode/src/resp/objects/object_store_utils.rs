//! RESP 对象命令层：同步信封读写辅助 + 命令中止工具
//!
//! 信封存储：记录挂 `KeyTag::ObjectEnvelope` 物理键（带外类型通道，对标 C#
//! LogRecord.DataHeader.ValueIsObject），值为 [1 字节类型标签][bitcode 载荷]；
//! 编码与 storage 层一处定义共享（`storage/session/objectstore/common.rs`，对标
//! libs/server/Storage/Session/ObjectStore/Common.cs），RESP 同步快路径与异步
//! storage 会话两条链路读写同一格式，杜绝跨层 WRONGTYPE 误判与裸载荷覆盖。
//!
//! 降级约定与命令层一致：磁盘候选 / 环形页翻转须异步裁决时读侧返回 `Ok(None)`、
//! 写侧返回 `Ok(false)`，由调用方整体转异步重放。
//!
//! 中止工具对标 libs/server/Resp/Objects/ObjectStoreUtils.cs（C# 为 RespServerSession
//! partial）：`AbortWithWrongNumberOfArgumentsOrUnknownSubcommand` 已由
//! resp::admin_commands 在 RespServerSession 上实现（同映射注释），此处不再重复定义。

use core::str;
use std::marker::PhantomData;

use wbase::{num::strict_i32, time::now_ticks};
pub(crate) use wcol::object_store_utils::{
  hash_from_blob, hash_to_blob, list_from_blob, list_to_blob, make_object_input, set_from_blob,
  set_to_blob, zset_from_blob, zset_to_blob,
};
use wcol::object_store_utils::{obj_decode, obj_encode};
pub use wcol::types::object_output::ObjectOutput;
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  RespVecExt,
  cmd_strings::{
    self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_wrong_number_of_arguments,
    write_error_raw,
  },
};
use wval::KeyTag;

use crate::resp::resp_server_session::RespServerSession;

/// 集合元素头部类型（FIELDS 或 MEMBERS）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementHeaderKind {
  Fields,
  Members,
}

impl ElementHeaderKind {
  pub const MANDATORY_FIELDS_MISSING: &str =
    "Mandatory argument FIELDS is missing or not at the right position";
  pub const MANDATORY_MEMBERS_MISSING: &str =
    "Mandatory argument MEMBERS is missing or not at the right position";
  pub const PARAM_FIELDS_POSITIVE: &str = "ERR Parameter `numFields` should be greater than 0";
  pub const PARAM_MEMBERS_POSITIVE: &str = "ERR Parameter `numMembers` should be greater than 0";
  pub const PARAM_FIELDS_MATCH_ARGS: &str =
    "The `numFields` parameter must match the number of arguments";
  pub const PARAM_MEMBERS_MATCH_ARGS: &str =
    "The `numMembers` parameter must match the number of arguments";

  #[inline]
  pub const fn token_bytes(self) -> &'static [u8] {
    match self {
      Self::Fields => b"FIELDS",
      Self::Members => b"MEMBERS",
    }
  }

  #[inline]
  pub const fn err_mandatory_missing(self) -> &'static str {
    match self {
      Self::Fields => Self::MANDATORY_FIELDS_MISSING,
      Self::Members => Self::MANDATORY_MEMBERS_MISSING,
    }
  }

  #[inline]
  pub const fn err_param_positive(self) -> &'static str {
    match self {
      Self::Fields => Self::PARAM_FIELDS_POSITIVE,
      Self::Members => Self::PARAM_MEMBERS_POSITIVE,
    }
  }

  #[inline]
  pub const fn err_param_match_args(self) -> &'static str {
    match self {
      Self::Fields => Self::PARAM_FIELDS_MATCH_ARGS,
      Self::Members => Self::PARAM_MEMBERS_MATCH_ARGS,
    }
  }
}

/// 解析 `FIELDS/MEMBERS count elem [elem...]` 公共头部，返回 `(elements_start_idx, num_elements)`
///
/// 零堆分配、O(1) 状态校验
pub fn parse_elements_header(
  parse_state: &[&[u8]],
  curr_idx: usize,
  kind: ElementHeaderKind,
  output: &mut Vec<u8>,
) -> Option<(usize, usize)> {
  if curr_idx >= parse_state.len()
    || !parse_state[curr_idx].eq_ignore_ascii_case(kind.token_bytes())
  {
    cs::abort_with_error_message(output, kind.err_mandatory_missing());
    return None;
  }

  let num_idx = curr_idx + 1;
  let Some(num_elements) = parse_state.get(num_idx).and_then(|raw| strict_i32(raw)) else {
    cs::abort_with_error_message(output, kind.err_param_positive());
    return None;
  };

  if num_elements < 1 {
    cs::abort_with_error_message(output, kind.err_param_positive());
    return None;
  }

  let num_elements = num_elements as usize;
  let elements_start = num_idx + 1;
  if parse_state.len() != elements_start + num_elements {
    cs::abort_with_error_message(output, kind.err_param_match_args());
    return None;
  }

  Some((elements_start, num_elements))
}

/// -2 元素数组应答（键缺失时的逐字段/成员占位）
#[inline]
pub fn write_n2_array(output: &mut Vec<u8>, len: usize) {
  output.reserve(len * cs::RESP_RETURN_VAL_N2.len() + 16);
  output.write_resp_array_len(len);
  for _ in 0..len {
    output.extend_from_slice(cs::RESP_RETURN_VAL_N2);
  }
}

impl RespServerSession {
  /// 参数数量错误中止（始终消费完整命令，返回 true）
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithWrongNumberOfArguments
  pub fn abort_with_wrong_number_of_arguments(
    &mut self,
    cmd_name: &str,
    output: &mut Vec<u8>,
  ) -> bool {
    self.command_error_written = true;
    abort_with_wrong_number_of_arguments(output, cmd_name);
    true
  }

  /// 以给定错误信息中止
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithErrorMessage
  /// （C# 置 commandErrorWritten 后经 RespWriteUtils 写错误帧）
  pub fn abort_with_error_message(&mut self, error_message: &[u8], output: &mut Vec<u8>) -> bool {
    self.command_error_written = true;
    let msg_str = str::from_utf8(error_message).unwrap_or("");
    write_error_raw(output, msg_str);
    true
  }
}
/// 对象同步装载三态（四类对象共用）
pub enum ObjLoad<T> {
  /// 磁盘候选：命令须降级异步重放（未写任何输出）
  Degrade,
  /// WrongType / 存储错误（错误行已写入输出）
  Error,
  /// 键缺失（可按空对象求值，但不得落库创建）
  Missing,
  /// 命中（信封载荷已解码）
  Present(T),
}

/// 对象读改写命令的 RESP 回执三态（四类对象共用；与会话侧落库判定
/// [`crate::storage::session::objectstore::common::RmwOutcome`] 分层区分）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespRmwOutcome {
  /// 磁盘候选降级（未写任何输出）
  Degrade,
  /// 错误行已写出，调用方不得追加回复
  Error,
  /// 已闭环：RESP 负载已随 rmw 写出；payload_written=false 时 result1 供调用方回执
  Done { result1: i64, payload_written: bool },
}

/// 通用同步对象装载
///
/// 读取走 KeyTag::ObjectEnvelope 物理域，信封剥壳后零拷贝直喂反序列化器；
/// 信封域未命中时反探 String 域——命中即用户字符串键（WRONGTYPE），两域
/// 皆缺为键缺失
pub fn obj_load_typed_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  deserialize: impl FnOnce(&[u8]) -> T,
) -> ObjLoad<T> {
  // 读侧降级约定：Ok(None) 磁盘候选 / TTL 待裁决；Some(None) 域内键缺失
  match store.try_read_tag_sync(key, KeyTag::ObjectEnvelope, |raw| {
    match obj_decode(raw, tag) {
      None => Err(()),
      Some(p) => Ok(deserialize(p)),
    }
  }) {
    // 信封域确认缺失（内存墓碑/无候选）：反探 String 域——命中即用户字符串键
    //（WRONGTYPE），两域皆缺为键缺失
    Ok(Some(None)) => {
      match store.try_read_tag_sync(key, KeyTag::String, |raw| raw.first().copied()) {
        Ok(Some(Some(_))) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE);
          ObjLoad::Error
        }
        Ok(_) => ObjLoad::Missing,
        Err(_) => {
          RespVecExt::write_resp_error(output, RESP_ERR_GENERIC);
          ObjLoad::Error
        }
      }
    }
    Ok(Some(Some(Err(())))) => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      ObjLoad::Error
    }
    Ok(Some(Some(Ok(obj)))) => ObjLoad::Present(obj),
    Ok(None) => ObjLoad::Degrade,
    Err(_) => {
      RespVecExt::write_resp_error(output, RESP_ERR_GENERIC);
      ObjLoad::Error
    }
  }
}

/// 同步写对象信封（记录挂 KeyTag::ObjectEnvelope 物理域）
///
/// `Ok(true)` 已闭环；`Ok(false)` 须降级异步；`Err` 存储层错误
pub(super) fn obj_save_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
) -> wkv::Result<bool> {
  let val = obj_encode(tag, payload);
  Ok(
    store
      .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &val)?
      .is_ok(),
  )
}

/// 信封整值写回 + 入账：写成功后同栈触发信封整值写通知（ObjectStoreUpsert
/// 全量条目，对标 C# WriteLogUpsert：libs/server/Storage/Functions/ObjectStore/
/// PrivateMethods.cs:WriteLogUpsert）
///
/// resp 层同步快路径散点（HCOLLECT/ZCOLLECT 单键、GEOADD/ZUNIONSTORE、
/// SPOP/LPOP 族、集合项经纪取件、自定义对象命令同步臂）统一收口——这些点
/// 携带的命令上下文各异，增量条目（ObjectStoreRMW）无法在此层一处合成，
/// 以全量收敛等价闭环；增量条目仅由 [`run_sync_rmw`] 经
/// [`notify_object_rmw_raw`] + 显式 ObjectStoreRMW 通知单独承接，不重复入账
pub(super) fn obj_save_notified<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
) -> wkv::Result<bool> {
  let val = obj_encode(tag, payload);
  let saved = store
    .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &val)?
    .is_ok();
  if saved {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, key);
    store
      .store
      .notify_envelope_upsert(raw_key.as_slice(), val.as_slice());
  }
  Ok(saved)
}

/// 移除族收尾（无入账内核）：对象被删空时整键回收，否则写回新载荷
///
/// 供 [`run_sync_rmw`] 复用（其增量条目另行显式通知，杜绝双份入账）。
/// 删除经 wkv 双域删除内核（信封墓碑由写监听入队 StoreDelete）。
fn obj_save_or_gc_raw<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
  now_empty: bool,
) -> wkv::Result<bool> {
  if now_empty {
    return Ok(store.try_delete_sync(key)?.is_ok());
  }
  obj_save_sync(store, key, tag, payload)
}

/// 移除族收尾：对象被删空时整键回收，否则写回新载荷并自动入账
///
/// 对齐 storage 层 `finalize_removal`（对象为空即回收键，不留空对象信封）。
/// 返回约定同 [`obj_save_sync`]；非空写回成功后经 [`obj_save_notified`]
/// 同栈入账（删空臂的信封墓碑由写监听 StoreDelete 承接）
pub(super) fn obj_save_or_gc_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
  now_empty: bool,
) -> wkv::Result<bool> {
  if now_empty {
    return Ok(store.try_delete_sync(key)?.is_ok());
  }
  obj_save_notified(store, key, tag, payload)
}

/// 通用对象变更写回或 GC（空集合整键回收，否则更新信封载荷；自动入账）
pub fn obj_save_or_gc<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  obj: &T,
  is_empty: bool,
  serialize: impl FnOnce(&T) -> Vec<u8>,
) -> wkv::Result<bool> {
  let payload = if is_empty { Vec::new() } else { serialize(obj) };
  obj_save_or_gc_sync(store, key, tag, &payload, is_empty)
}

/// 同步对象 RMW 命令输入参数
pub struct SyncRmwCmd<'a, Op> {
  pub key: &'a [u8],
  pub tag: u8,
  pub op: Op,
  pub args: &'a [&'a [u8]],
  pub arg1: i32,
  pub arg2: i32,
}

/// 同步对象 RMW 处理策略集合
pub struct SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite> {
  pub deserialize: Deser,
  pub default_obj: Def,
  pub is_empty: IsEmpty,
  pub serialize: Ser,
  pub run_op: RunOp,
  pub should_write: ShouldWrite,
  pub phantom: PhantomData<fn() -> (Obj, Op)>,
}

impl<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>
  SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>
where
  Deser: FnOnce(&[u8]) -> Obj,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: FnOnce(&mut Obj, Op, &[&[u8]]) -> ObjectOutput,
  ShouldWrite: FnOnce(Op, &ObjectOutput, &Obj, bool) -> bool,
{
  #[inline]
  pub fn new(
    deserialize: Deser,
    default_obj: Def,
    is_empty: IsEmpty,
    serialize: Ser,
    run_op: RunOp,
    should_write: ShouldWrite,
  ) -> Self {
    Self {
      deserialize,
      default_obj,
      is_empty,
      serialize,
      run_op,
      should_write,
      phantom: PhantomData,
    }
  }
}

/// 通用同步对象 RMW 执行骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
pub fn run_sync_rmw<
  Obj,
  Op: Copy + Into<u8>,
  D: Device,
  Deser,
  Def,
  IsEmpty,
  Ser,
  RunOp,
  ShouldWrite,
>(
  store: &BatchStoreSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  handlers: SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>,
) -> RespRmwOutcome
where
  Deser: FnOnce(&[u8]) -> Obj,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: FnOnce(&mut Obj, Op, &[&[u8]]) -> ObjectOutput,
  ShouldWrite: FnOnce(Op, &ObjectOutput, &Obj, bool) -> bool,
{
  let (mut obj, existed) =
    match obj_load_typed_sync(store, cmd.key, cmd.tag, output, handlers.deserialize) {
      ObjLoad::Degrade => return RespRmwOutcome::Degrade,
      ObjLoad::Error => return RespRmwOutcome::Error,
      ObjLoad::Missing => ((handlers.default_obj)(), false),
      ObjLoad::Present(o) => (o, true),
    };

  let obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed) {
    let empty = (handlers.is_empty)(&obj);
    // 写回走无入账内核（payload 先行编码一次，删空臂传空载荷）：增量条目
    // ObjectStoreRMW 由下方显式通知单独承接（对标 C# WriteLogRMW），与信封
    // 整值写通知（obj_save_notified 收口）互斥，杜绝双份入账
    let payload = if empty {
      Vec::new()
    } else {
      (handlers.serialize)(&obj)
    };
    match obj_save_or_gc_raw(store, cmd.key, cmd.tag, &payload, empty) {
      Ok(true) => {
        // 事件时间戳：真 .NET Ticks（与 wkv::ObjectRmwNotification 契约同域，
        // 对标 Garnet 对象 RMW 输入的时间戳）；key 为信封物理键
        //（KeyTag::ObjectEnvelope），与存储记录域一致
        let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, cmd.key);
        let notif = wkv::ObjectRmwNotification {
          key: &raw_key,
          obj_type: cmd.tag,
          op_code: cmd.op.into(),
          timestamp_ticks: now_ticks(),
          arg1: cmd.arg1,
          arg2: cmd.arg2,
          args: cmd.args,
        };
        store.notify_object_rmw(&notif);
      }
      Ok(false) => return RespRmwOutcome::Degrade,
      Err(_) => {
        RespVecExt::write_resp_error(output, RESP_ERR_GENERIC);
        return RespRmwOutcome::Error;
      }
    }
  }
  output.extend_from_slice(&obj_out.payload);

  RespRmwOutcome::Done {
    result1,
    payload_written: !obj_out.payload.is_empty(),
  }
}

#[cfg(test)]
mod abort_tests {
  use super::*;

  #[test]
  fn abort_frames_match_csharp_text() {
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();
    assert!(sess.abort_with_wrong_number_of_arguments("ZADD", &mut out));
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZADD' command\r\n"
    );

    out.clear();
    assert!(sess.abort_with_error_message(b"ERR custom", &mut out));
    assert_eq!(out, b"-ERR custom\r\n");
  }

  #[test]
  fn test_parse_elements_header_fields_and_members() {
    let mut out = Vec::new();

    // 正常 FIELDS
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"2", b"f1", b"f2"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, Some((3, 2)));
    assert!(out.is_empty());

    // 正常 MEMBERS
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, Some((3, 1)));
    assert!(out.is_empty());

    // 缺失 FIELDS
    out.clear();
    let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-Mandatory argument FIELDS is missing or not at the right position\r\n"
    );

    // 缺失 MEMBERS
    out.clear();
    let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // num <= 0
    out.clear();
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"0"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-ERR Parameter `numMembers` should be greater than 0\r\n"
    );

    // num 不是数字
    out.clear();
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"abc"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-ERR Parameter `numFields` should be greater than 0\r\n"
    );

    // 参数数量不匹配
    out.clear();
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"2", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );

    // write_n2_array 测试
    out.clear();
    write_n2_array(&mut out, 2);
    assert_eq!(out, b"*2\r\n:-2\r\n:-2\r\n");
  }
}
