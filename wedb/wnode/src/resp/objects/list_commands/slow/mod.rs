//! 列表命令慢路径执行臂（exec_slow 冷键分派）
//!
//! 对标 libs/server/Resp/Objects/ListCommands.cs 各命令经 Tsavorite pending
//! 读 CompletePending 后重放的异步形态。阻塞族（BLPOP/BRPOP/BLMOVE/
//! BRPOPLPUSH/BLMPOP）在冷键/分层键降级至此后：装载可出件即直接出件，
//! 未取到则经经纪登记观察者在执行域内联竞速超时（`block_wait_cold`，C#
//! `AsyncUtils.BlockingWait` 内联阻塞的 compio 投影——键态冷热与阻塞语义
//! 解耦，timeout=0 无限等待），出件或超时后经 [`write_collection_item_result`]
//! 统一出帧，与快路径 park_broker_wait 挂起体同一应答单源。慢路径观察者 ID 取自
//! usize::MAX 递减专用域，CLIENT UNBLOCK 经 client_id < 0 单点门禁隔离不命中本域
//! （偏差登记见 doc/zh/deviations.md §63）。经纪未注入的
//! 独立会话域无等待面，维持立即可取语义回空值（偏差登记见 doc/zh/deviations.md §24）。
//! `Err(())` 为存储 IO 失败，由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
//!
//! 目录化拆分：[`face`] 等待面（BlockWaitFace）、[`shared`] 共享 helper 单源、
//! [`push_pop`] 推入/弹出族、[`single_key`] 单键装载求值族、[`lmove`] 双键
//! 移动族、[`multi_pop`] 多键弹出族；本文件持分层快速通道与 match 分派

mod face;
mod lmove;
mod multi_pop;
mod push_pop;
mod shared;
mod single_key;

pub(crate) use face::BlockWaitFace;
use shared::{err_async, is_push, tiered_op};
use wbase::num::strict_i32;
use wdev::Device;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

use super::parse_i32_pair_args;
use crate::{
  resp::objects::{
    object_store_utils::try_tiered_arm,
    rmw_helpers::split_refs,
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_list, list_needs_write},
  },
  storage::session::storage_session::StorageSession,
};

/// 列表命令统一慢路径分派（LSCAN 走 shared 慢路径扫描）
pub(crate) async fn list(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  block: Option<&BlockWaitFace<'_>>,
  cmd: RespCommand,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_version;
  let (key, args) = split_refs(refs);

  // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透），
  // 探测集与不入表理由单源见 [`tiered_op`]
  let op_opt = tiered_op(cmd);
  if try_tiered_arm(
    storage,
    key,
    GarnetObjectType::List,
    op_opt,
    |op| list_needs_write(*op),
    output,
    async move |ctx, op, output| {
      // arg1/arg2 通道（对齐 SyncRmwCmd 与 C# 对象层 ObjectInput 约定）：
      // LINDEX = index，LRANGE = (start, stop)；推入族元素走 args
      let (arg1, arg2) = match cmd {
        RespCommand::Lindex => match refs.get(1).and_then(|v| strict_i32(v)) {
          Some(index) => (index, 0),
          None => return err_async(output, true),
        },
        RespCommand::Lrange => match parse_i32_pair_args("LRANGE", refs, output) {
          Some((start, stop)) => (start, stop),
          None => return Ok(true),
        },
        _ => (0, 0),
      };
      let handled = exec_tiered_list(
        &storage.batch,
        key,
        ctx,
        TieredCollectionArgs::new(op, (arg1, arg2), args, resp_version),
        output,
      )
      .await?;
      // 写命令唤醒阻塞观察者（对标同步段 notify 点，判据单源见 [`is_push`]）
      if handled && is_push(cmd) {
        notify(key);
      }
      Ok(handled)
    },
  )
  .await?
  {
    return Ok(());
  }

  // LPUSH/RPUSH/LPUSHX/RPUSHX/LPOP/RPOP 慢臂（判据 = op_of 六臂联合，
  // 与原前置 if 链同源；三段序与臂体见 push_pop）
  if matches!(
    cmd,
    RespCommand::Lpush
      | RespCommand::Rpush
      | RespCommand::Lpushx
      | RespCommand::Rpushx
      | RespCommand::Lpop
      | RespCommand::Rpop
  ) {
    return push_pop::push_pop_cold(storage, notify, cmd, key, args, resp_version, output).await;
  }

  // LLEN 由 exec_slow O(1) 计数直读臂承接
  match cmd {
    RespCommand::Ltrim => single_key::ltrim_cold(storage, key, refs, resp_version, output).await,
    RespCommand::Lrange => single_key::lrange_cold(storage, key, refs, resp_version, output).await,
    RespCommand::Lindex => single_key::lindex_cold(storage, key, refs, resp_version, output).await,
    RespCommand::Lpos => {
      single_key::lpos_cold(storage, key, args, refs, resp_version, output).await
    }
    RespCommand::Linsert => {
      single_key::linsert_cold(storage, notify, key, args, resp_version, output).await
    }
    RespCommand::Lrem => single_key::lrem_cold(storage, key, refs, resp_version, output).await,
    RespCommand::Lset => single_key::lset_cold(storage, key, refs, resp_version, output).await,
    RespCommand::Lmove | RespCommand::Rpoplpush | RespCommand::Blmove | RespCommand::Brpoplpush => {
      lmove::lmove_cold(storage, notify, block, cmd, refs, resp_version, output).await
    }
    RespCommand::Lmpop | RespCommand::Blmpop => {
      multi_pop::mpop_cold(storage, block, cmd, refs, resp_version, output).await
    }
    RespCommand::Blpop | RespCommand::Brpop => {
      multi_pop::bpop_cold(storage, block, cmd, refs, resp_version, output).await
    }
    _ => err_async(output, ()),
  }
}
