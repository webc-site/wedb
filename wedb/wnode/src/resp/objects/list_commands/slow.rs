//! 列表命令慢路径执行臂（exec_slow 冷键分派）
//!
//! 对标 libs/server/Resp/Objects/ListCommands.cs 各命令经 Tsavorite pending
//! 读 CompletePending 后重放的异步形态。阻塞族在经纪注入时已在快路径
//! park_broker_wait 挂起不降级，降级仅发生于经纪未注入的独立会话域，
//! 异步臂对位立即可取路径（取不到按同步段口径回空值，不做等待循环）。
//! `Err(())` 为存储 IO 失败，由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE

use wbase::num::strict_i32;
use wcol::list::list_object::{ListObject, ListOperation, OperationDirection};
use wkv::SwapInWindowGuard;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{Rmw, parse_i32_pair_args, parse_lmpop_args, run_operate, should_write_back};
use crate::{
  resp::objects::{
    object_store_utils::{
      GarnetObjectPayload, SealedLoad, SyncRmwCmd, SyncRmwHandlers, load_typed_sealed,
      obj_writeback_tiered, run_async_rmw, try_tiered_arm, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_list},
  },
  session_parse_state_extensions::operation_direction_from_token as parse_direction,
  storage::session::storage_session::StorageSession,
};

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
async fn list_rmw_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
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
}

/// 单键异步装载：`Ok(None)` = WRONGTYPE 错误行已写出；
/// `Ok(Some(None))` = MISSING（调用方定短路应答）；
/// `Ok(Some(Some((obj, window))))` = 已装载（`window` = 分层物化封窗守卫，
/// 信封域装载为 `None`——守卫须存活至写回收尾，封写语义见
/// [`load_typed_sealed`]）
async fn load_typed(
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<(ListObject, Option<SwapInWindowGuard>)>>, ()> {
  Ok(
    match load_typed_sealed(storage, key, GarnetObjectType::List, output).await? {
      SealedLoad::WrongType => None,
      SealedLoad::Missing => Some(None),
      SealedLoad::Present(o, window) => Some(Some((o, window))),
    },
  )
}

/// 装载 + 求值公共体：WRONGTYPE 已闭环；MISSING 短路应答；
/// PRESENT 交异步 eval（求值 + 按需写回；分层物化封窗守卫存活至 eval 完成，
/// 即写回收尾之后）
async fn load_eval(
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
  on_missing: impl FnOnce(&mut Vec<u8>),
  eval: impl AsyncFnOnce(ListObject, bool, &mut Vec<u8>),
) -> Result<(), ()> {
  match load_typed(storage, key, output).await? {
    None => Ok(()),
    Some(None) => {
      on_missing(output);
      Ok(())
    }
    Some(Some((obj, swap_in_window))) => {
      eval(obj, swap_in_window.is_some(), output).await;
      Ok(())
    }
  }
}

/// 删空回收或信封写回（对标 sync list_save_or_gc 的异步臂）
async fn save_or_gc(
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
  obj: &ListObject,
  sealed: bool,
) -> Result<(), ()> {
  obj_writeback_tiered(storage, key, GarnetObjectType::List, obj, sealed).await
}

/// 列表命令统一慢路径分派（LSCAN 走 shared 慢路径扫描）
pub(crate) async fn list(
  storage: &StorageSession<'_, impl wdev::Device>,
  notify: &impl Fn(&[u8]),
  cmd: RespCommand,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_version;
  let key = refs.first().copied().unwrap_or(&[]);
  let args = refs.get(1..).unwrap_or(&[]);

  // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
  // LPOP/RPOP 不入表：弹出即删除重命令，与 LREM/LTRIM 同径走下方对象层通道
  //（物化求值 + 整值重灌），杜绝向分层树头/尾逐成员落删除墓碑
  //（栈深不变量，见 tiered_collection_ops 头注）
  let op_opt = match cmd {
    RespCommand::Lpush => Some(ListOperation::Lpush),
    RespCommand::Rpush => Some(ListOperation::Rpush),
    RespCommand::Lpushx => Some(ListOperation::Lpushx),
    RespCommand::Rpushx => Some(ListOperation::Rpushx),
    RespCommand::Llen => Some(ListOperation::Llen),
    RespCommand::Lrange => Some(ListOperation::Lrange),
    RespCommand::Lindex => Some(ListOperation::Lindex),
    _ => None,
  };
  if try_tiered_arm(
    storage,
    key,
    GarnetObjectType::List,
    op_opt,
    output,
    async move |ctx, op, output| {
      // arg1/arg2 通道（对齐 SyncRmwCmd 与 C# 对象层 ObjectInput 约定）：
      // LINDEX = index，LRANGE = (start, stop)；推入族元素走 args
      let (arg1, arg2) = match cmd {
        RespCommand::Lindex => match refs.get(1).and_then(|v| strict_i32(v)) {
          Some(index) => (index, 0),
          None => {
            cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
            return Ok(true);
          }
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
      // 写命令唤醒阻塞观察者（对标同步段 notify 点）
      if handled
        && matches!(
          cmd,
          RespCommand::Lpush | RespCommand::Rpush | RespCommand::Lpushx | RespCommand::Rpushx
        )
      {
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
  if matches!(cmd, RespCommand::Lpush | RespCommand::Rpush) {
    let op = if cmd == RespCommand::Lpush {
      ListOperation::Lpush
    } else {
      ListOperation::Rpush
    };
    let done = list_rmw_cold(storage, key, op, args, (0, 0), resp_version, output).await?;
    if let Rmw::Present(done) = done {
      write_rmw_reply(done, output);
      notify(key);
    }
    return Ok(());
  }
  // LPUSHX/RPUSHX（缺失不物化回 :0；写成功唤醒阻塞观察者）
  if matches!(cmd, RespCommand::Lpushx | RespCommand::Rpushx) {
    let op = if cmd == RespCommand::Lpushx {
      ListOperation::Lpushx
    } else {
      ListOperation::Rpushx
    };
    return match load_typed(storage, key, output).await? {
      // MISSING：C# 键缺失不创建，回复 :0
      Some(None) => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        Ok(())
      }
      None => Ok(()),
      Some(Some((mut obj, swap_in_window))) => {
        // LPUSH 族仅回填 result1（无负载段），写回失败由外层统一落错
        let result1 = run_operate(&mut obj, op, args, 0, 0, resp_version, output).result1;
        save_or_gc(storage, key, &obj, swap_in_window.is_some()).await?;
        output.write_resp_int(result1);
        notify(key);
        Ok(())
      }
    };
  }
  // LPOP/RPOP（RMW，arg1 = pop_count）
  if matches!(cmd, RespCommand::Lpop | RespCommand::Rpop) {
    let op = if cmd == RespCommand::Lpop {
      ListOperation::Lpop
    } else {
      ListOperation::Rpop
    };
    let pop_count = match refs.get(1) {
      // 缺省 count = 1（无外层数组形态）
      None => 1,
      Some(c) => match strict_i32(c) {
        Some(v) if v >= 0 => v,
        _ => {
          cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
          return Ok(());
        }
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
      load_eval(
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
          match save_or_gc(storage, key, &obj, sealed).await {
            Ok(()) => output.extend_from_slice(cs::RESP_OK),
            Err(()) => output.write_resp_error(cs::RESP_ERR_GENERIC),
          }
        },
      )
      .await
    }
    RespCommand::Lrange => {
      // start/stop 参数推导单源（快慢共用，失败帧已写出）
      let Some((start, stop)) = parse_i32_pair_args("LRANGE", refs, output) else {
        return Ok(());
      };
      load_eval(
        storage,
        key,
        output,
        |output| output.extend_from_slice(cs::RESP_EMPTYLIST),
        async move |mut obj: ListObject, _sealed: bool, output: &mut Vec<u8>| {
          run_operate(
            &mut obj,
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
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      load_eval(
        storage,
        key,
        output,
        |output| output.write_resp_null_ver(resp_version),
        async move |mut obj: ListObject, _sealed: bool, output: &mut Vec<u8>| {
          // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
          let result1 = run_operate(
            &mut obj,
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
      load_eval(
        storage,
        key,
        output,
        // C# NOTFOUND：参数含 COUNT → 空数组，否则 null
        |output| {
          if refs[2..].iter().any(|t| t.eq_ignore_ascii_case(cs::COUNT)) {
            output.extend_from_slice(cs::RESP_EMPTYLIST);
          } else {
            output.write_resp_null_ver(resp_version);
          }
        },
        async move |mut obj: ListObject, _sealed: bool, output: &mut Vec<u8>| {
          // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
          let result1 = run_operate(
            &mut obj,
            ListOperation::Lpos,
            args,
            0,
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
    RespCommand::Linsert => {
      load_eval(
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
            if save_or_gc(storage, key, &obj, sealed).await.is_err() {
              output.write_resp_error(cs::RESP_ERR_GENERIC);
              return;
            }
            notify(key);
          }
          output.write_resp_int(result1);
        },
      )
      .await
    }
    RespCommand::Lrem => {
      let Some(count) = refs.get(1).and_then(|v| strict_i32(v)) else {
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      let element = refs.get(2).copied().unwrap_or(&[]);
      load_eval(
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
          if result1 > 0 && save_or_gc(storage, key, &obj, sealed).await.is_err() {
            output.write_resp_error(cs::RESP_ERR_GENERIC);
            return;
          }
          output.write_resp_int(result1);
        },
      )
      .await
    }
    RespCommand::Lset => {
      let (Some(idx_val), Some(element)) = (refs.get(1).copied(), refs.get(2).copied()) else {
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      load_eval(
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
            && let Err(()) = save_or_gc(storage, key, &obj, sealed).await
          {
            // 回退挂载点改写错误帧（+OK 负载不得与错误帧拼帧）
            obj_out.reset();
            output.write_resp_error(cs::RESP_ERR_GENERIC);
          }
          // +OK / 对象层错误负载均已直写会话输出尾段
        },
      )
      .await
    }
    RespCommand::Lmove | RespCommand::Rpoplpush | RespCommand::Blmove | RespCommand::Brpoplpush => {
      // LMOVE/RPOPLPUSH 直取；BLMOVE/BRPOPLPUSH 为经纪未注入域的立即取
      //（方向定式对位同步段）
      let (src_key, dst_key, src_dir, dst_dir) = match cmd {
        RespCommand::Rpoplpush | RespCommand::Brpoplpush => (
          key,
          refs.get(1).copied().unwrap_or(&[]),
          OperationDirection::Right,
          OperationDirection::Left,
        ),
        _ => {
          let Some(src_dir) = refs.get(2).copied().and_then(parse_direction) else {
            cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
            return Ok(());
          };
          let Some(dst_dir) = refs.get(3).copied().and_then(parse_direction) else {
            cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
            return Ok(());
          };
          (key, refs.get(1).copied().unwrap_or(&[]), src_dir, dst_dir)
        }
      };
      move_core_cold(
        resp_version,
        storage,
        notify,
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
      let handled =
        pop_first_nonempty_cold(storage, keys, pop_direction, pop_count, output).await?;
      if !handled {
        // LMPOP NOTFOUND → WriteNullArray；BLMPOP !result.Found → WriteNull
        //（会话版本分派，RESP3 为 `_\r\n`）
        if cmd == RespCommand::Lmpop {
          output.write_resp_null_array_ver(resp_version);
        } else {
          output.write_resp_null_ver(resp_version);
        }
      }
      Ok(())
    }
    RespCommand::Blpop | RespCommand::Brpop => {
      // 阻塞族独立会话域语义：逐键立即可取（对位同步段立即可取路径）
      for k in &refs[..refs.len().saturating_sub(1)] {
        let Some(Some((mut obj, swap_in_window))) = load_typed(storage, k, output).await? else {
          continue;
        };
        let item = if cmd == RespCommand::Blpop {
          obj.list.pop_front()
        } else {
          obj.list.pop_back()
        };
        let Some(item) = item else {
          continue;
        };
        obj.update_size(&item, false);
        save_or_gc(storage, k, &obj, swap_in_window.is_some()).await?;
        // 回复：[key, item]
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(k);
        output.write_resp_bulk_string(&item);
        return Ok(());
      }
      // C# !result.Found → WriteNullArray（会话版本分派，RESP3 为 `_\r\n`）
      output.write_resp_null_array_ver(resp_version);
      Ok(())
    }
    _ => {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// LMOVE 双键移动慢路径入参（收敛入参，消除 too-many-arguments）
struct MoveColdArgs<'a> {
  src_key: &'a [u8],
  dst_key: &'a [u8],
  src_dir: OperationDirection,
  dst_dir: OperationDirection,
}

/// LMOVE 双键移动慢路径对位（对标同步段 list_move_core；写成功唤醒阻塞
/// 观察者）
async fn move_core_cold(
  resp_version: u8,
  storage: &StorageSession<'_, impl wdev::Device>,
  notify: &impl Fn(&[u8]),
  args: MoveColdArgs<'_>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let MoveColdArgs {
    src_key,
    dst_key,
    src_dir,
    dst_dir,
  } = args;
  let Some(Some((mut src, src_window))) = load_typed(storage, src_key, output).await? else {
    return Ok(());
  };
  if src.list.is_empty() {
    // C# src 缺失/空 → OK + null element（会话版本分派，RESP3 为 `_\r\n`）
    output.write_resp_null_ver(resp_version);
    return Ok(());
  }

  let same_key = src_key == dst_key;
  if same_key && (src_dir == dst_dir || src.list.len() == 1) {
    // C# 同键同向 或 单元素：窥视元素返回，严禁先 pop 再 push（防 TTL 丢失）
    let item = if src_dir == OperationDirection::Right {
      src.list.back()
    } else {
      src.list.front()
    };
    match item {
      Some(item) => output.write_resp_bulk_string(item),
      None => output.write_resp_null_ver(resp_version),
    }
    return Ok(());
  }

  // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截）
  let mut dst_loaded = ListObject::new();
  let mut dst_window = None;
  if !same_key {
    match load_typed(storage, dst_key, output).await? {
      None => return Ok(()),
      Some(None) => {}
      // 封窗守卫存活至 dst 写回收尾（函数尾），杜绝窗内并发写被顶替
      Some(Some((o, w))) => {
        dst_loaded = o;
        dst_window = w;
      }
    }
  }

  let popped = if src_dir == OperationDirection::Right {
    src.list.pop_back()
  } else {
    src.list.pop_front()
  };
  let Some(element) = popped else {
    output.write_resp_null_ver(resp_version);
    return Ok(());
  };

  if same_key {
    if dst_dir == OperationDirection::Left {
      src.list.push_front(element);
    } else {
      src.list.push_back(element);
    }
    save_or_gc(storage, src_key, &src, src_window.is_some()).await?;
    notify(src_key);
    let elem_ref = if dst_dir == OperationDirection::Left {
      src.list.front()
    } else {
      src.list.back()
    };
    if let Some(elem) = elem_ref {
      output.write_resp_bulk_string(elem);
    }
    return Ok(());
  }

  src.update_size(&element, false);
  dst_loaded.update_size(&element, true);
  if dst_dir == OperationDirection::Left {
    dst_loaded.list.push_front(element);
  } else {
    dst_loaded.list.push_back(element);
  }
  // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
  save_or_gc(storage, src_key, &src, src_window.is_some()).await?;
  save_or_gc(storage, dst_key, &dst_loaded, dst_window.is_some()).await?;
  notify(dst_key);
  let elem_ref = if dst_dir == OperationDirection::Left {
    dst_loaded.list.front()
  } else {
    dst_loaded.list.back()
  };
  if let Some(elem) = elem_ref {
    output.write_resp_bulk_string(elem);
  }
  Ok(())
}

/// 逐键弹出第一个非空列表的慢路径对位（对标同步段 pop_first_nonempty）
///
/// 返回 `Ok(false)` = WRONGTYPE 错误行已写出
async fn pop_first_nonempty_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  keys: &[&[u8]],
  pop_direction: OperationDirection,
  pop_count: i32,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let is_left = pop_direction == OperationDirection::Left;
  for key in keys {
    let Some(Some((mut obj, swap_in_window))) = load_typed(storage, key, output).await? else {
      continue;
    };
    if obj.list.is_empty() {
      continue;
    }
    let count = (pop_count as usize).min(obj.list.len());
    let mut popped = Vec::with_capacity(count);
    for _ in 0..count {
      let item = if is_left {
        obj.list.pop_front()
      } else {
        obj.list.pop_back()
      };
      let Some(item) = item else {
        break;
      };
      obj.update_size(&item, false);
      popped.push(item);
    }
    save_or_gc(storage, key, &obj, swap_in_window.is_some()).await?;
    // 回复：[key, [element, ...]]
    output.write_resp_array_len(2);
    output.write_resp_bulk_string(key);
    output.write_resp_array_len(popped.len());
    for element in &popped {
      output.write_resp_bulk_string(element);
    }
    return Ok(true);
  }
  Ok(false)
}
