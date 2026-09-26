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

use std::{
  collections::VecDeque,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use wbase::num::strict_i32;
use wcol::{
  itembroker::item_broker_face::{BlockedWait, ItemBrokerFinisher},
  list::list_object::{ListObject, ListOperation, OperationDirection},
};
use wdev::Device;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{
  Rmw, parse_i32_pair_args, parse_lmpop_args, run_operate, should_write_back,
  write_collection_item_result,
};
use crate::{
  resp::objects::{
    object_store_utils::{
      GarnetObjectPayload, SyncRmwCmd, SyncRmwHandlers, SyncRmwOutcome, load_sealed_tri,
      obj_writeback_recheck_async, obj_writeback_tiered, rmw_window_pair_async, run_async_rmw,
      slow_load_eval, try_tiered_arm, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_list, list_needs_write},
  },
  session_parse_state_extensions::{
    operation_direction_from_token as parse_direction, try_get_timeout_bytes,
  },
  storage::session::storage_session::StorageSession,
};

/// 慢路径阻塞等待面（经纪句柄 + 发起会话域；`list()` 与 `move_core_cold`
/// 的阻塞族臂据此在装载未取到时闭环等待，None = 经纪未注入的独立会话域）
pub(crate) struct BlockWaitFace<'a> {
  pub(crate) broker: &'a Arc<dyn ItemBrokerFinisher>,
  pub(crate) domain: (u64, u64),
}

impl BlockWaitFace<'_> {
  /// 慢路径阻塞等待闭环（C# BlockingWait 内联阻塞的 compio 投影）：
  /// 登记观察者后在执行域内竞速超时，出件/超时经应答单源统一出帧。
  ///
  /// 观察者 id 取慢路径专用递减域（自 `usize::MAX` 递减，与 i64 会话 id
  /// 空间不相交）：执行域无会话可达面，专用域保证并发等待互不顶替映射；
  /// 且 CLIENT UNBLOCK 命令对 client_id < 0 设单点门禁恒回 0，不命中本域
  /// （偏差登记见 doc/zh/deviations.md §63；C# 侧慢路径重放后仍持原会话可
  /// 解除，rust 慢路径等待体由超时/出件/注销守卫自治收口）。
  ///
  /// 执行体注销收口由 [`ObserverDropGuard`] 承担：慢执行体未闭环即被丢弃
  /// （网络泵终止广播胜出 / 会话 dispose 收口 / 脚本挂起槽就地取消）时经
  /// 经纪 `handle_session_disposed` 摘除观察者——对位 C# RespServerSession
  /// Dispose :408 `itemBroker?.HandleSessionDisposed(this)` 的无条件注销，
  /// 杜绝观察者滞留经纪等待队列成僵尸（新写入元素被误弹出后写回 BrokenPipe
  /// 丢弃，真数据丢失）
  pub(crate) async fn wait(
    &self,
    cmd: RespCommand,
    timeout: f64,
    keys: Vec<Vec<u8>>,
    cmd_args: Vec<Vec<u8>>,
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    static NEXT_ID: AtomicU64 = AtomicU64::new(usize::MAX as u64);
    let session_id = NEXT_ID.fetch_sub(1, Ordering::Relaxed) as usize;
    let observer = self
      .broker
      .start_wait(cmd, keys, session_id, cmd_args, self.domain);
    let mut guard = ObserverDropGuard {
      broker: Arc::clone(self.broker),
      session_id,
      done: false,
    };
    let mut wait = BlockedWait::new(Arc::clone(self.broker), observer, cmd, timeout);
    let (_, result) = wait.resolve().await;
    // 正常闭环（出件/超时）：resolve 内 finish_wait 已摘会话映射，
    // drop 守卫为 no-op
    guard.done = true;
    write_collection_item_result(cmd, &result, resp_version, output);
  }
}

/// 慢路径阻塞观察者注销守卫（内嵌于 `BlockWaitFace::wait` 执行体）：执行体
/// 未闭环即被丢弃时经经纪摘除观察者。守卫随执行体走，泵终止/dispose/脚本
/// 槽取消全部丢弃点单一覆盖，与阻塞臂 `BlockedWait::abort` 同一经纪单点，
/// 不新增第二套注销机制
struct ObserverDropGuard {
  broker: Arc<dyn ItemBrokerFinisher>,
  session_id: usize,
  done: bool,
}

impl Drop for ObserverDropGuard {
  fn drop(&mut self) {
    if !self.done {
      self.broker.handle_session_disposed(self.session_id);
    }
  }
}

// ============ 族内编译期分派表与出帧/错误帧骨架（慢臂同形复制收口） ============

/// BLMOVE / BRPOPLPUSH 的 timeout 词元在 refs 中的下标
const BLMOVE_TIMEOUT_IDX: usize = 4;
const BRPOPLPUSH_TIMEOUT_IDX: usize = 2;

/// cmd → 对象层操作单源（原分层探测表与推入/弹出三臂的
/// `if cmd == X {A} else {B}` 同形复制收口；仅作分派，AOF 值序由
/// wcol::ListOperation 钉死，严禁增删项）
const fn op_of(cmd: RespCommand) -> Option<ListOperation> {
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
const fn tiered_op(cmd: RespCommand) -> Option<ListOperation> {
  match cmd {
    RespCommand::Lpop | RespCommand::Rpop => None,
    _ => op_of(cmd),
  }
}

/// 推入族判据单源（写成功后唤醒阻塞观察者，对标同步段 notify 点）
const fn is_push(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Lpush | RespCommand::Rpush | RespCommand::Lpushx | RespCommand::Rpushx
  )
}

/// 慢臂「参数须由快路径/异步臂承接」统一错误帧 + 该分支应答终值（原八处同形
/// 复制收口，帧文本与写出序不变；`ok` = 分派臂 `()`、tiered 求值臂 `true`）
#[inline]
fn err_async<T>(output: &mut Vec<u8>, ok: T) -> Result<T, ()> {
  cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
  Ok(ok)
}

/// 等待面 × refs 中的 timeout 词元（阻塞族三臂同形收口：经纪未注入 / 词元
/// 缺失或不可解析 → None，调用方各回自身空值形态；词元判定与快路径同源）
#[inline]
fn face_with_timeout<'a, 'b>(
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
fn peek_end(list: &VecDeque<Vec<u8>>, dir: OperationDirection) -> Option<&Vec<u8>> {
  if dir == OperationDirection::Right {
    list.back()
  } else {
    list.front()
  }
}

#[inline]
fn pop_end(list: &mut VecDeque<Vec<u8>>, dir: OperationDirection) -> Option<Vec<u8>> {
  if dir == OperationDirection::Right {
    list.pop_back()
  } else {
    list.pop_front()
  }
}

#[inline]
fn push_end(list: &mut VecDeque<Vec<u8>>, dir: OperationDirection, item: Vec<u8>) {
  if dir == OperationDirection::Left {
    list.push_front(item)
  } else {
    list.push_back(item)
  }
}

/// 回复 `[key, item]`（BLPOP/BRPOP 冷臂命中帧；骨架 count 恒 1 故 items 一元）
fn write_key_item_frame(key: &[u8], items: &[Vec<u8>], output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  for item in items {
    output.write_resp_bulk_string(item);
  }
}

/// 回复 `[key, [element, ...]]`（LMPOP/BLMPOP 命中帧）
fn write_key_items_frame(key: &[u8], items: &[Vec<u8>], output: &mut Vec<u8>) {
  output.write_resp_array_len(2);
  output.write_resp_bulk_string(key);
  output.write_resp_array_len(items.len());
  for item in items {
    output.write_resp_bulk_string(item);
  }
}

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
async fn list_rmw_cold(
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
    SyncRmwHandlers::new(
      ListObject::from_blob,
      ListObject::new,
      |o: &ListObject| o.list.is_empty(),
      |o: &ListObject| o.to_blob(),
      |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
      should_write_back,
    ),
  )
  .await
  .map(SyncRmwOutcome::from)
}

/// 装载 + 求值公共体·写回臂版（票 load-type-rmw-window 异步档）：
/// 求值前取 rmw 窗跨「装载 → 求值 → 写回」全程（与 run_async_rmw 同锁源，
/// 让核等待），`Err(())` 窗预算耗尽按存储忙统一应答（fail-closed 重试）；
/// eval 闭包上抛 `Result`，落笔域复验内聚于 [`save_or_gc`]
async fn load_eval_write(
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

/// 删空回收或信封写回（对标 sync list_save_or_gc 的异步臂）：
/// 非封窗（信封域）落笔前先按装载态复验域归属（票 load-type-rmw-window
/// 异步档，与 run_async_rmw 落笔复验同核），窗内 DEL/SET 交叠即 `Err(())`
/// 按存储忙拒写；封窗臂（SwapInWindowGuard）物化语义域已钉死，免复验
async fn save_or_gc(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  obj: &ListObject,
  sealed: bool,
  existed: bool,
) -> Result<(), ()> {
  if !sealed {
    obj_writeback_recheck_async(storage, key, existed).await?;
  }
  obj_writeback_tiered(storage, key, GarnetObjectType::List, obj, sealed).await
}

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
  let key = refs.first().copied().unwrap_or(&[]);
  let args = refs.get(1..).unwrap_or(&[]);

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

  // LPUSH/RPUSH（RMW；写成功唤醒阻塞观察者，对标同步段 notify 点）
  if let Some(op @ (ListOperation::Lpush | ListOperation::Rpush)) = op_of(cmd) {
    let done = list_rmw_cold(storage, key, op, args, (0, 0), resp_version, output).await?;
    if let Rmw::Present(done) = done {
      write_rmw_reply(done, output);
      notify(key);
    }
    return Ok(());
  }
  // LPUSHX/RPUSHX（缺失不物化回 :0；写成功唤醒阻塞观察者；装载前取 rmw 窗
  // 跨全程由 load_eval_write 单源承担，对位同步臂同款双保护·异步档）
  if let Some(op @ (ListOperation::Lpushx | ListOperation::Rpushx)) = op_of(cmd) {
    return load_eval_write(
      storage,
      key,
      output,
      // MISSING：C# 键缺失不创建，回复 :0
      |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
        // LPUSH 族仅回填 result1（无负载段），写回失败由外层统一落错
        let result1 = run_operate(&mut obj, op, args, 0, 0, resp_version, output).result1;
        save_or_gc(storage, key, &obj, sealed, true).await?;
        output.write_resp_int(result1);
        notify(key);
        Ok(())
      },
    )
    .await;
  }
  // LPOP/RPOP（RMW，arg1 = pop_count）
  if let Some(op @ (ListOperation::Lpop | ListOperation::Rpop)) = op_of(cmd) {
    let pop_count = match refs.get(1) {
      // 缺省 count = 1（无外层数组形态）
      None => 1,
      Some(c) => match strict_i32(c) {
        Some(v) if v >= 0 => v,
        _ => return err_async(output, ()),
      },
    };
    list_rmw_cold(storage, key, op, &[], (pop_count, 0), resp_version, output).await?;
    return Ok(());
  }

  // LLEN 由 exec_slow O(1) 计数直读臂承接
  match cmd {
    RespCommand::Ltrim => {
      // start/stop 参数推导单源（快慢共用，失败帧已写出）
      let Some((start, stop)) = parse_i32_pair_args("LTRIM", refs, output) else {
        return Ok(());
      };
      load_eval_write(
        storage,
        key,
        output,
        // C# NOTFOUND → OK（无对象可裁剪，仍回 OK）
        |output| output.extend_from_slice(cs::RESP_OK),
        async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
          run_operate(
            &mut obj,
            ListOperation::Ltrim,
            &[],
            start,
            stop,
            resp_version,
            output,
          );
          save_or_gc(storage, key, &obj, sealed, true).await?;
          output.extend_from_slice(cs::RESP_OK);
          Ok(())
        },
      )
      .await
    }
    RespCommand::Lrange => {
      // start/stop 参数推导单源（快慢共用，失败帧已写出）
      let Some((start, stop)) = parse_i32_pair_args("LRANGE", refs, output) else {
        return Ok(());
      };
      // 只读臂走非封窗纯物化通道（与 hash/set/zset 三族读臂同机制，
      // eval 零写回不封窗，读面忙拒面不扩大）
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::List,
        output,
        ListObject::from_blob,
        |output| output.extend_from_slice(cs::RESP_EMPTYLIST),
        async move |obj: &mut ListObject, output: &mut Vec<u8>| {
          run_operate(
            obj,
            ListOperation::Lrange,
            &[],
            start,
            stop,
            resp_version,
            output,
          );
        },
      )
      .await
    }
    RespCommand::Lindex => {
      let Some(index) = refs.get(1).and_then(|v| strict_i32(v)) else {
        return err_async(output, ());
      };
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::List,
        output,
        ListObject::from_blob,
        |output| output.write_resp_null_ver(resp_version),
        async move |obj: &mut ListObject, output: &mut Vec<u8>| {
          // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
          let result1 = run_operate(
            obj,
            ListOperation::Lindex,
            &[],
            index,
            0,
            resp_version,
            output,
          )
          .result1;
          if result1 == -1 {
            output.write_resp_null_ver(resp_version);
          }
        },
      )
      .await
    }
    RespCommand::Lpos => {
      // 只读臂走非封窗纯物化通道（与三族读臂同机制）：分层键已注册树内
      // 流式臂（tiered_list_arm Lpos），本臂仅承接信封态与装载竞态降级形；
      // 对象层 Lpos result1 = 命中数恒 ≥0，无 LINDEX 的 -1 未写负载形
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::List,
        output,
        ListObject::from_blob,
        // C# NOTFOUND：参数含 COUNT → 空数组，否则 null
        |output| {
          if refs[2..].iter().any(|t| t.eq_ignore_ascii_case(cs::COUNT)) {
            output.extend_from_slice(cs::RESP_EMPTYLIST);
          } else {
            output.write_resp_null_ver(resp_version);
          }
        },
        async move |obj: &mut ListObject, output: &mut Vec<u8>| {
          run_operate(obj, ListOperation::Lpos, args, 0, 0, resp_version, output);
        },
      )
      .await
    }
    RespCommand::Linsert => {
      load_eval_write(
        storage,
        key,
        output,
        |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
          let result1 = run_operate(
            &mut obj,
            ListOperation::Linsert,
            args,
            0,
            0,
            resp_version,
            output,
          )
          .result1;
          if result1 > 0 {
            save_or_gc(storage, key, &obj, sealed, true).await?;
            notify(key);
          }
          output.write_resp_int(result1);
          Ok(())
        },
      )
      .await
    }
    RespCommand::Lrem => {
      let Some(count) = refs.get(1).and_then(|v| strict_i32(v)) else {
        return err_async(output, ());
      };
      let element = refs.get(2).copied().unwrap_or(&[]);
      load_eval_write(
        storage,
        key,
        output,
        |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
          let result1 = run_operate(
            &mut obj,
            ListOperation::Lrem,
            &[element],
            count,
            0,
            resp_version,
            output,
          )
          .result1;
          if result1 > 0 {
            save_or_gc(storage, key, &obj, sealed, true).await?;
          }
          output.write_resp_int(result1);
          Ok(())
        },
      )
      .await
    }
    RespCommand::Lset => {
      let (Some(idx_val), Some(element)) = (refs.get(1).copied(), refs.get(2).copied()) else {
        return err_async(output, ());
      };
      load_eval_write(
        storage,
        key,
        output,
        // C# NOTFOUND → ERR no such key
        |output| cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY),
        async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
          let mut obj_out = run_operate(
            &mut obj,
            ListOperation::Lset,
            &[idx_val, element],
            0,
            0,
            resp_version,
            output,
          );
          if obj_out.payload_view().first() == Some(&b'+')
            && let Err(e) = save_or_gc(storage, key, &obj, sealed, true).await
          {
            // 回退挂载点清场后上抛（+OK 负载不得与忙帧拼帧，统一应答兜底）
            obj_out.reset();
            return Err(e);
          }
          // +OK / 对象层错误负载均已直写会话输出尾段
          Ok(())
        },
      )
      .await
    }
    RespCommand::Lmove | RespCommand::Rpoplpush | RespCommand::Blmove | RespCommand::Brpoplpush => {
      // LMOVE/RPOPLPUSH 直取；BLMOVE/BRPOPLPUSH 为阻塞族（源未取到时经
      // 等待面闭环，方向定式对位同步段）
      let (src_key, dst_key, src_dir, dst_dir) = match cmd {
        RespCommand::Rpoplpush | RespCommand::Brpoplpush => (
          key,
          refs.get(1).copied().unwrap_or(&[]),
          OperationDirection::Right,
          OperationDirection::Left,
        ),
        _ => {
          // 方向词元成对判定（任一元缺失/非 LEFT|RIGHT 同报，对位原两处同形分支）
          let (Some(src_dir), Some(dst_dir)) = (
            refs.get(2).copied().and_then(parse_direction),
            refs.get(3).copied().and_then(parse_direction),
          ) else {
            return err_async(output, ());
          };
          (key, refs.get(1).copied().unwrap_or(&[]), src_dir, dst_dir)
        }
      };
      // 阻塞族超时词元（快路径已校验同源；非阻塞族恒无等待面）
      let block = match cmd {
        RespCommand::Blmove => face_with_timeout(block, refs.get(BLMOVE_TIMEOUT_IDX).copied()),
        RespCommand::Brpoplpush => {
          face_with_timeout(block, refs.get(BRPOPLPUSH_TIMEOUT_IDX).copied())
        }
        _ => None,
      };
      move_core_cold(
        resp_version,
        storage,
        notify,
        block,
        MoveColdArgs {
          src_key,
          dst_key,
          src_dir,
          dst_dir,
        },
        output,
      )
      .await
    }
    RespCommand::Lmpop | RespCommand::Blmpop => {
      // 参数推导单源（快慢共用，失败帧已写出；BLMPOP 的 timeout 词元由
      // 快路径先行校验，冷键降级时不再复检）
      let Some((keys, pop_direction, pop_count)) =
        parse_lmpop_args(refs, cmd == RespCommand::Blmpop, output)
      else {
        return Ok(());
      };
      let handled = pop_first_cold(
        storage,
        keys,
        pop_direction,
        pop_count as usize,
        output,
        write_key_items_frame,
      )
      .await?;
      if !handled {
        // LMPOP NOTFOUND → WriteNullArray（非阻塞语义恒立即回）；BLMPOP
        // 未取到 → 经等待面闭环（cmd_args = [popDir, popCount]，与快路径
        // park 同编码；C# ListBlockingPopMultiple 尾部 BlockingWait 同位），
        // 出件/超时经应答单源出帧；经纪未注入域回空值
        if cmd == RespCommand::Lmpop {
          output.write_resp_null_array_ver(resp_version);
        } else if let Some((face, timeout)) = face_with_timeout(block, refs.first().copied()) {
          face
            .wait(
              cmd,
              timeout,
              keys.iter().map(|k| k.to_vec()).collect(),
              vec![vec![pop_direction as u8], pop_count.to_le_bytes().to_vec()],
              resp_version,
              output,
            )
            .await;
        } else {
          output.write_resp_null_ver(resp_version);
        }
      }
      Ok(())
    }
    RespCommand::Blpop | RespCommand::Brpop => {
      // 阻塞族冷臂：逐键立即可取（弹取出件复用 [`pop_first_cold`] 骨架，count
      // 恒 1、回复 [key, item]；对位同步段 blocking.rs list_blocking_pop 的三态
      // 匹配，C# ListBlockingPop 遇 IsTypeMismatch 写错误行即终止整条命令，后续
      // 键不被触碰）；全部键缺失或空列表即经等待面闭环（C# ListBlockingPop :284
      // 无条件 BlockingWait 的键态解耦语义），出件/超时经应答单源出帧
      let dir = if cmd == RespCommand::Blpop {
        OperationDirection::Left
      } else {
        OperationDirection::Right
      };
      let keys = &refs[..refs.len().saturating_sub(1)];
      if pop_first_cold(storage, keys, dir, 1, output, write_key_item_frame).await? {
        return Ok(());
      }
      // 未取到：经纪注入域等待闭环（timeout 词元快路径已校验）；未注入
      // 域回 C# !result.Found 同款空数组（会话版本分派，RESP3 为 `_\r\n`）
      if let Some((face, timeout)) = face_with_timeout(block, refs.last().copied()) {
        face
          .wait(
            cmd,
            timeout,
            keys.iter().map(|k| k.to_vec()).collect(),
            Vec::new(),
            resp_version,
            output,
          )
          .await;
      } else {
        output.write_resp_null_array_ver(resp_version);
      }
      Ok(())
    }
    _ => err_async(output, ()),
  }
}

/// LMOVE 双键移动慢路径入参（收敛入参，消除 too-many-arguments）
struct MoveColdArgs<'a> {
  src_key: &'a [u8],
  dst_key: &'a [u8],
  src_dir: OperationDirection,
  dst_dir: OperationDirection,
}

/// 源未取到的阻塞闭环（C# ListBlockingMove 尾部 BlockingWait 后
/// !result.Found → WriteNull：出件由经纪 BLMOVE 臂完整执行弹+推，超时经
/// 应答单源回空值；经纪未注入域维持立即可取回 null）。入参收敛为
/// [`MoveColdArgs`]，两个源不可出件分支（缺失 / 空列表）同形复用
async fn wait_src_or_null(
  block: Option<(&BlockWaitFace<'_>, f64)>,
  resp_version: u8,
  args: &MoveColdArgs<'_>,
  output: &mut Vec<u8>,
) {
  match block {
    Some((face, timeout)) => {
      face
        .wait(
          RespCommand::Blmove,
          timeout,
          vec![args.src_key.to_vec()],
          vec![
            args.dst_key.to_vec(),
            vec![args.src_dir as u8],
            vec![args.dst_dir as u8],
          ],
          resp_version,
          output,
        )
        .await;
    }
    None => output.write_resp_null_ver(resp_version),
  }
}

/// LMOVE 双键移动慢路径对位（对标同步段 list_move_core；写成功唤醒阻塞
/// 观察者。`block` 为阻塞族等待面：BLMOVE/BRPOPLPUSH 源未取到时不立即
/// 回空值，登记观察者在执行域内联等待出件——C# ListBlockingMove
/// :373-376 无条件 BlockingWait 的键态解耦语义；`timeout` 仅阻塞族消费）
async fn move_core_cold(
  resp_version: u8,
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  block: Option<(&BlockWaitFace<'_>, f64)>,
  args: MoveColdArgs<'_>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let MoveColdArgs {
    src_key,
    dst_key,
    src_dir,
    dst_dir,
  } = args;
  // 双键装载型写臂双保护·异步档：键组桶升序单机制双窗杜绝环等待与双序对撞
  //（同键/同桶折叠仅持一窗，票 zcode-r135c-lockorder 案一），跨 src/dst 装载、
  // 求值与双写回全程；窗预算耗尽按存储忙统一应答（fail-closed）
  let windows = rmw_window_pair_async(storage, src_key, dst_key)
    .await
    .map_err(|_| ())?;
  // 源未取到的阻塞闭环（C# ListBlockingMove 尾部 BlockingWait 后
  // !result.Found → WriteNull：出件由经纪 BLMOVE 臂完整执行弹+推，超时经
  // 应答单源回空值；经纪未注入域维持立即可取回 null）。等待臂须先释放
  // 双窗：阻塞等待期间唤醒方（LPUSH 等写命令）须能取得窗写入源键，持窗
  // 等待即自我锁死至超时（装载快照已定，窗的保护使命随之结束）
  let (mut src, src_window) =
    match load_sealed_tri::<ListObject, _>(storage, src_key, output).await? {
      // WRONGTYPE：错误帧已由装载底层写出，终止整条命令
      None => return Ok(()),
      // C# src 缺失 → null element（与同步 list_move_core 的 Missing 分支同形；
      // 阻塞族转等待闭环）
      Some(None) => {
        drop(windows);
        wait_src_or_null(block, resp_version, &args, output).await;
        return Ok(());
      }
      Some(Some(loaded)) => loaded,
    };
  if src.list.is_empty() {
    // C# src 缺失/空 → OK + null element（会话版本分派，RESP3 为 `_\r\n`；
    // 阻塞族转等待闭环，同上先释放双窗）
    drop(windows);
    wait_src_or_null(block, resp_version, &args, output).await;
    return Ok(());
  }
  let _keep = windows;

  let same_key = src_key == dst_key;
  if same_key && (src_dir == dst_dir || src.list.len() == 1) {
    // C# 同键同向 或 单元素：窥视元素返回，严禁先 pop 再 push（防 TTL 丢失）
    match peek_end(&src.list, src_dir) {
      Some(item) => output.write_resp_bulk_string(item),
      None => output.write_resp_null_ver(resp_version),
    }
    return Ok(());
  }

  // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截）
  let mut dst_loaded = ListObject::new();
  let mut dst_window = None;
  // dst 装载态快照（新建写回复验要求域仍缺席，票 load-type-rmw-window）
  let mut dst_existed = false;
  if !same_key {
    match load_sealed_tri(storage, dst_key, output).await? {
      None => return Ok(()),
      Some(None) => {}
      // 封窗守卫存活至 dst 写回收尾（函数尾），杜绝窗内并发写被顶替
      Some(Some((o, w))) => {
        dst_loaded = o;
        dst_window = w;
        dst_existed = true;
      }
    }
  }

  let Some(element) = pop_end(&mut src.list, src_dir) else {
    output.write_resp_null_ver(resp_version);
    return Ok(());
  };

  if same_key {
    push_end(&mut src.list, dst_dir, element);
    save_or_gc(storage, src_key, &src, src_window.is_some(), true).await?;
    notify(src_key);
    if let Some(elem) = peek_end(&src.list, dst_dir) {
      output.write_resp_bulk_string(elem);
    }
    return Ok(());
  }

  src.update_size(&element, false);
  dst_loaded.update_size(&element, true);
  push_end(&mut dst_loaded.list, dst_dir, element);
  // 写回序先目标后源（与同步段 list_move_core 及 set_commands/slow.rs 同款补偿
  // 序，对齐 set_move 成文纪律）：目标写回失败（迁移窗忙错/升阶未成超页
  // fail-closed/存储 IO）时源零变异零写入，错误帧后数据无损，重试自完整初态
  // 收敛；目标成功而源失败时列表 push 非幂等，残留为「元素双份」可重试收敛
  // （集合侧 insert 幂等自收敛，差异注见同步段）
  save_or_gc(
    storage,
    dst_key,
    &dst_loaded,
    dst_window.is_some(),
    dst_existed,
  )
  .await?;
  // dst 提交即唤醒，src 写回失败不回撤事件（对齐快臂 list_move_core dst save
  // Ok(true) 贴发位与 C# ListOps.cs:299 提交后唤醒之本仓投影：本仓两笔 save
  // 独立，唤醒随 dst 自身提交；notify 严禁漂回下方 src save `?` 之后——src
  // fail-closed 错误帧会把已持久 dst 元素落成「已提交漏发」永悬态，
  // timeout=0 观察者悬至该键下一写事件（票 zcode-r145c-lblpop2 案一）。
  // 观察者即时取走该元素落 §100 在册「元素双份」可重试容忍形，快臂现形
  // 即此口径运行，零新增险面
  notify(dst_key);
  // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
  save_or_gc(storage, src_key, &src, src_window.is_some(), true).await?;
  if let Some(elem) = peek_end(&dst_loaded.list, dst_dir) {
    output.write_resp_bulk_string(elem);
  }
  Ok(())
}

/// 逐键弹出第一个非空列表的慢路径骨架（LMPOP/BLMPOP 与 BLPOP/BRPOP 冷臂
/// 共用，对标同步段 pop_first_nonempty 的 Some(true)/None 分派；出帧形态由
/// `frame` 决定，C# ListPopMultiple / ListBlockingPop 尾部应答差异）
///
/// WRONGTYPE 判定序单源：装载底层已写错误行即返回终结，严禁续扫后续键
/// （C# 遇 IsTypeMismatch 终止整条命令）；MISSING / 空列表续扫且不写回。
///
/// 返回 `true` = 已生成终结性应答（WRONGTYPE 错误行写出，或命中并出帧），
/// 调用方不得追加任何空值应答；`false` = 全部键缺失或空列表，由调用方补写
/// null 形态应答
async fn pop_first_cold(
  storage: &StorageSession<'_, impl Device>,
  keys: &[&[u8]],
  pop_direction: OperationDirection,
  pop_count: usize,
  output: &mut Vec<u8>,
  frame: impl Fn(&[u8], &[Vec<u8>], &mut Vec<u8>),
) -> Result<bool, ()> {
  for key in keys {
    // 装载型取件臂双保护·异步档：逐键装载前取 rmw 窗跨弹出与写回
    let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
    let (mut obj, swap_in_window) =
      match load_sealed_tri::<ListObject, _>(storage, key, output).await? {
        // WRONGTYPE：错误帧已写出，返回 true 告知调用方应答已终结
        None => return Ok(true),
        // MISSING：键不存在，继续扫描下一键
        Some(None) => continue,
        Some(Some(loaded)) => loaded,
      };
    let mut popped = Vec::with_capacity(pop_count.min(obj.list.len()));
    while popped.len() < pop_count {
      let Some(item) = pop_end(&mut obj.list, pop_direction) else {
        break;
      };
      obj.update_size(&item, false);
      popped.push(item);
    }
    // 空列表（无可弹元素）→ 续扫，不写回
    if popped.is_empty() {
      continue;
    }
    save_or_gc(storage, key, &obj, swap_in_window.is_some(), true).await?;
    frame(key, &popped, output);
    return Ok(true);
  }
  Ok(false)
}
