//! RESP 对象命令层：同步信封读写辅助 + 命令中止工具
//!
//! 信封编码（[1 字节类型标签][bitcode 载荷]）与 storage 层一处定义共享
//! （`storage/session/objectstore/common.rs`，对标 libs/server/Storage/Session/
//! ObjectStore/Common.cs），RESP 同步快路径与异步 storage 会话两条链路读写同一
//! 格式，杜绝跨层 WRONGTYPE 误判与裸载荷覆盖。
//!
//! 降级约定与命令层一致：磁盘候选 / 环形页翻转须异步裁决时读侧返回 `Ok(None)`、
//! 写侧返回 `Ok(false)`，由调用方整体转异步重放。
//!
//! 中止工具对标 libs/server/Resp/Objects/ObjectStoreUtils.cs（C# 为 RespServerSession
//! partial）：`AbortWithWrongNumberOfArgumentsOrUnknownSubcommand` 已由
//! resp::admin_commands 在 RespServerSession 上实现（同映射注释），此处不再重复定义。

use core::str;
use std::marker::PhantomData;

use wbase::time::now_ticks;
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  RespVecExt,
  cmd_strings::{RESP_ERR_WRONG_TYPE, abort_with_wrong_number_of_arguments, write_error_raw},
};

pub use crate::objects::object_store_utils::{
  hash_from_blob, hash_to_blob, is_object_envelope, list_from_blob, list_to_blob,
  make_object_input, object_type_name, set_from_blob, set_to_blob, zset_from_blob, zset_to_blob,
};
pub(crate) use crate::storage::session::objectstore::common::{obj_decode, obj_encode};
use crate::{
  objects::types::object_output::ObjectOutput, resp::resp_server_session::RespServerSession,
};

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
pub(super) enum SyncObj {
  /// 键不存在
  Missing,
  /// 存在但非本类型信封
  WrongType,
  /// 命中，返回剥壳后的 bitcode 载荷
  Present(Vec<u8>),
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

/// 对象读改写执行结果（四类对象共用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwOutcome {
  /// 磁盘候选降级（未写任何输出）
  Degrade,
  /// 错误行已写出，调用方不得追加回复
  Error,
  /// 已闭环：RESP 负载已随 rmw 写出；payload_written=false 时 result1 供调用方回执
  Done { result1: i64, payload_written: bool },
}

/// 同步读对象信封（零 I/O 快路径）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步裁决；`Err` 为存储层错误
pub(super) fn obj_load_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
) -> wkv::Result<Option<SyncObj>> {
  Ok(Some(match store.try_read_sync(key, |v| v.to_vec())? {
    // 磁盘候选 / TTL 待裁决：降级
    None => return Ok(None),
    Some(None) => SyncObj::Missing,
    Some(Some(raw)) => match obj_decode(&raw, tag) {
      None => SyncObj::WrongType,
      Some(p) => SyncObj::Present(p.to_vec()),
    },
  }))
}

/// 通用同步对象装载
pub fn obj_load_typed_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  deserialize: impl FnOnce(&[u8]) -> T,
) -> ObjLoad<T> {
  match obj_load_sync(store, key, tag) {
    Ok(None) => ObjLoad::Degrade,
    Ok(Some(SyncObj::Missing)) => ObjLoad::Missing,
    Ok(Some(SyncObj::WrongType)) => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      ObjLoad::Error
    }
    Ok(Some(SyncObj::Present(p))) => ObjLoad::Present(deserialize(&p)),
    Err(_) => {
      RespVecExt::write_resp_error(output, "generic error");
      ObjLoad::Error
    }
  }
}

/// 同步写对象信封
///
/// `Ok(true)` 已闭环；`Ok(false)` 须降级异步；`Err` 存储层错误
pub(super) fn obj_save_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
) -> wkv::Result<bool> {
  let val = obj_encode(tag, payload);
  Ok(store.try_upsert_sync(key, &val)?.is_ok())
}

/// 移除族收尾：对象被删空时整键回收，否则写回新载荷
///
/// 对齐 storage 层 `finalize_removal`（对象为空即回收键，不留空对象信封）。
/// 返回约定同 [`obj_save_sync`]
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
  obj_save_sync(store, key, tag, payload)
}

/// 通用对象变更写回或 GC（空集合整键回收，否则更新信封载荷）
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
) -> RmwOutcome
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
      ObjLoad::Degrade => return RmwOutcome::Degrade,
      ObjLoad::Error => return RmwOutcome::Error,
      ObjLoad::Missing => ((handlers.default_obj)(), false),
      ObjLoad::Present(o) => (o, true),
    };

  let obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed) {
    let empty = (handlers.is_empty)(&obj);
    match obj_save_or_gc(store, cmd.key, cmd.tag, &obj, empty, handlers.serialize) {
      Ok(true) => {
        // 事件时间戳：真 .NET Ticks（与 wkv::ObjectRmwNotification 契约同域，
        // 对标 Garnet 对象 RMW 输入的时间戳）
        let raw_key = store.session_string_key(cmd.key);
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
      Ok(false) => return RmwOutcome::Degrade,
      Err(_) => {
        RespVecExt::write_resp_error(output, "generic error");
        return RmwOutcome::Error;
      }
    }
  }
  output.extend_from_slice(&obj_out.payload);

  RmwOutcome::Done {
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
}
