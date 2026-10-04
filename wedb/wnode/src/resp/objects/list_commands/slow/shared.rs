//! 慢臂共享 helper 单源：编译期分派表（op_of / tiered_op / is_push）、错误
//! 帧骨架（err_async）、等待面 × timeout 词元（face_with_timeout）、双端
//! 队列端点算子（peek_end / pop_end / push_end）、多键弹出出帧骨架
//! （write_key_item_frame / write_key_items_frame）、rmw 骨架
//! （list_rmw_cold）与装载求值公共体（load_eval_write）。入口分派与各族
//! 臂文件共用，禁另起副本

use std::collections::VecDeque;

use wcol::list::list_object::{ListObject, ListOperation, OperationDirection};
use wdev::Device;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{
  super::{Rmw, should_write_back},
  face::BlockWaitFace,
};
use crate::{
  resp::objects::{
    object_store_utils::{SyncRmwCmd, SyncRmwOutcome, load_sealed_tri, run_async_rmw},
    rmw_helpers::collection_rmw_handlers,
  },
  session_parse_state_extensions::try_get_timeout_bytes,
  storage::session::storage_session::StorageSession,
};

// ============ 族内编译期分派表与出帧/错误帧骨架（慢臂同形复制收口） ============

/// BLMOVE / BRPOPLPUSH 的 timeout 词元在 refs 中的下标
pub(super) const BLMOVE_TIMEOUT_IDX: usize = 4;
pub(super) const BRPOPLPUSH_TIMEOUT_IDX: usize = 2;

/// cmd → 对象层操作单源（原分层探测表与推入/弹出三臂的
/// `if cmd == X {A} else {B}` 同形复制收口；仅作分派，AOF 值序由
/// wcol::ListOperation 钉死，严禁增删项）
pub(super) const fn op_of(cmd: RespCommand) -> Option<ListOperation> {
  Some(match cmd {
    RespCommand::Lpush => ListOperation::Lpush,
    RespCommand::Rpush => ListOperation::Rpush,
    RespCommand::Lpushx => ListOperation::Lpushx,
    RespCommand::Rpushx => ListOperation::Rpushx,
    RespCommand::Lpop => ListOperation::Lpop,
    RespCommand::Rpop => ListOperation::Rpop,
    RespCommand::Llen => ListOperation::Llen,
    RespCommand::Lrange => ListOperation::Lrange,
    RespCommand::Lindex => ListOperation::Lindex,
    RespCommand::Lpos => ListOperation::Lpos,
    _ => return None,
  })
}

/// 分层快速通道探测集 = [`op_of`] 除弹出族：LPOP/RPOP 弹出即删除重命令，与
/// LREM/LTRIM 同径走对象层通道（物化求值 + 整值重灌），杜绝向分层树头尾逐成员
/// 落删除墓碑（栈深不变量见 tiered_collection_ops 头注）；LPOS 只读入表，
/// 走共享读锁树内流式臂，claim 在册回退快照照常出读
pub(super) const fn tiered_op(cmd: RespCommand) -> Option<ListOperation> {
  match cmd {
    RespCommand::Lpop | RespCommand::Rpop => None,
    _ => op_of(cmd),
  }
}

/// 推入族判据单源（写成功后唤醒阻塞观察者，对标同步段 notify 点）
pub(super) const fn is_push(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Lpush | RespCommand::Rpush | RespCommand::Lpushx | RespCommand::Rpushx
  )
}

/// 慢臂「参数须由快路径/异步臂承接」统一错误帧 + 该分支应答终值（原八处同形
/// 复制收口，帧文本与写出序不变；`ok` = 分派臂 `()`、tiered 求值臂 `true`）
#[inline]
pub(super) fn err_async<T>(output: &mut Vec<u8>, ok: T) -> Result<T, ()> {
  cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
  Ok(ok)
}

/// 等待面 × refs 中的 timeout 词元（阻塞族三臂同形收口：经纪未注入 / 词元
/// 缺失或不可解析 → None，调用方各回自身空值形态；词元判定与快路径同源）
#[inline]
pub(super) fn face_with_timeout<'a, 'b>(
  block: Option<&'a BlockWaitFace<'b>>,
  token: Option<&[u8]>,
) -> Option<(&'a BlockWaitFace<'b>, f64)> {
  let face = block?;
  let timeout = token.and_then(|t| try_get_timeout_bytes(t).ok())?;
  Some((face, timeout))
}

/// `RIGHT` → 尾、其余（`LEFT`）→ 头的双端队列端点算子单源（慢臂原七处同形
/// if/else 收口，内联零成本；Unknown 词元快路径已拦，此处沿用原判序极性）
#[inline]
pub(super) fn peek_end(list: &VecDeque<Vec<u8>>, dir: OperationDirection) -> Option<&Vec<u8>> {
  if dir == OperationDirection::Right {
    list.back()
  } else {
    list.front()
  }
}

#[inline]
pub(super) fn pop_end(list: &mut VecDeque<Vec<u8>>, dir: OperationDirection) -> Option<Vec<u8>> {
  if dir == OperationDirection::Right {
    list.pop_back()
  } else {
    list.pop_front()
  }
}

#[inline]
pub(super) fn push_end(list: &mut VecDeque<Vec<u8>>, dir: OperationDirection, item: Vec<u8>) {
  if dir == OperationDirection::Left {
    list.push_front(item)
  } else {
    list.push_back(item)
  }
}

/// 回复 `[key, item]`（BLPOP/BRPOP 冷臂命中帧；骨架 count 恒 1 故 items 一元）
pub(super) fn write_key_item_frame(key: &[u8], items: &[Vec<u8>], output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  for item in items {
    output.write_resp_bulk_string(item);
  }
}

/// 回复 `[key, [element, ...]]`（LMPOP/BLMPOP 命中帧）
pub(super) fn write_key_items_frame(key: &[u8], items: &[Vec<u8>], output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  output.write_resp_array_len(items.len());
  for item in items {
    output.write_resp_bulk_string(item);
  }
}

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
pub(super) async fn list_rmw_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  op: ListOperation,
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
      tag: GarnetObjectType::List,
      op,
      args,
      arg1,
      arg2,
    },
    output,
    collection_rmw_handlers(arg1, arg2, resp_version, should_write_back),
  )
  .await
  .map(SyncRmwOutcome::from)
}

/// 装载 + 求值公共体·写回臂版（票 load-type-rmw-window 异步档）：
/// 求值前取 rmw 窗跨「装载 → 求值 → 写回」全程（与 run_async_rmw 同锁源，
/// 让核等待），`Err(())` 窗预算耗尽按存储忙统一应答（fail-closed 重试）；
/// eval 闭包上抛 `Result`，落笔域复验内聚于 [`obj_writeback_rechecked_async`]
pub(super) async fn load_eval_write(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
  on_missing: impl FnOnce(&mut Vec<u8>),
  eval: impl AsyncFnOnce(ListObject, bool, &mut Vec<u8>) -> Result<(), ()>,
) -> Result<(), ()> {
  let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
  match load_sealed_tri::<ListObject, _>(storage, key, output).await? {
    None => Ok(()),
    Some(None) => {
      on_missing(output);
      Ok(())
    }
    Some(Some((obj, swap_in_window))) => eval(obj, swap_in_window.is_some(), output).await,
  }
}
