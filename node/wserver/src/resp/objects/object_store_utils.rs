//! RESP 对象命令层：同步信封读写辅助 + 命令中止工具
//!
//! 信封编码（[1 字节类型标签][wobject bitcode 载荷]）与 storage 层一处定义共享
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

use wdev::Device;
use wkv::BatchStoreSession;

use crate::resp::{
  cmd_strings as cs, cmd_strings::write_error_raw, parser::resp_ext::RespVecExt,
  resp_server_session::RespServerSession,
};
pub(crate) use crate::storage::session::objectstore::common::{
  OBJ_TAG_HASH, OBJ_TAG_LIST, OBJ_TAG_SET, obj_decode, obj_encode,
};

/// ERR wrong number of arguments for '{0}' command
const GENERIC_ERR_WRONG_NUM_ARGS: &str = "ERR wrong number of arguments for '{name}' command";

impl RespServerSession {
  /// 参数数量错误中止（始终消费完整命令，返回 true）
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithWrongNumberOfArguments
  pub fn abort_with_wrong_number_of_arguments(
    &mut self,
    cmd_name: &str,
    output: &mut Vec<u8>,
  ) -> bool {
    write_error_raw(
      output,
      &GENERIC_ERR_WRONG_NUM_ARGS.replace("{name}", cmd_name),
    );
    true
  }

  /// 以给定错误信息中止
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithErrorMessage
  /// （C# 置 commandErrorWritten 后经 RespWriteUtils 写错误帧）
  pub fn abort_with_error_message(&mut self, error_message: &[u8], output: &mut Vec<u8>) -> bool {
    output.push(b'-');
    output.extend_from_slice(error_message);
    output.extend_from_slice(b"\r\n");
    true
  }
}
pub(super) enum SyncObj {
  /// 键不存在
  Missing,
  /// 存在但非本类型信封
  WrongType,
  /// 命中，返回剥壳后的 wobject 载荷
  Present(Vec<u8>),
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

/// 读命令统一应答：命中载荷交 `f` 处理，缺失走 `missing`，
/// WrongType / 存储错误直接写出错误行
///
/// 返回 `false` 表示须降级异步（命令层回 `Ok(false)`），否则已完整写出应答
pub(super) fn read_object_or_reply<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  missing: impl FnOnce(&mut Vec<u8>),
  f: impl FnOnce(Vec<u8>, &mut Vec<u8>),
) -> bool {
  match obj_load_sync(store, key, tag) {
    Ok(None) => return false,
    Ok(Some(SyncObj::Missing)) => missing(output),
    Ok(Some(SyncObj::WrongType)) => write_error_raw(output, cs::RESP_ERR_WRONG_TYPE),
    Ok(Some(SyncObj::Present(p))) => f(p, output),
    Err(_) => output.write_resp_error("generic error"),
  }
  true
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

#[cfg(test)]
mod abort_tests {
  use super::*;

  #[test]
  fn abort_frames_match_csharp_text() {
    let mut sess = RespServerSession;
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
