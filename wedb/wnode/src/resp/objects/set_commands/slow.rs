/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/SetCommands.cs 各命令经 Tsavorite pending
/// 读 CompletePending 后重放的异步形态；装载/折叠核与同步段单源复用
/// （load_many_async / intersect_sets / union_sets / diff_sets /
/// write_set_members），写回经异步段唯一漏斗。`Err(())` 为存储 IO 失败，
/// 由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
use wcol::set::{
  set_object::{SetObject, SetOperation},
  set_object_impl::NO_COUNT,
};
use wdev::Device;
use wkv::SwapInWindowGuard;
use wresp::{
  cmd_strings as cs,
  command::RespCommand,
  ext::{RespSliceExt, RespVecExt},
};
use wval::GarnetObjectType;

use super::{
  Rmw, parse_set_pop_args, run_operate, should_write_back,
  write::{diff_sets, intersect_sets, union_sets},
  write_set_members,
};
use crate::{
  resp::objects::{
    object_store_utils::{
      GarnetObjectPayload, IntersectCardKind, SealedLoad, SyncRmwCmd, SyncRmwHandlers,
      load_typed_sealed, obj_writeback_tiered, parse_intersect_card_args, retire_tiered_dest,
      run_async_rmw, slow_load_eval, try_tiered_arm, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_set},
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
      |obj, op, args, output| run_operate(obj, op, args, 0, 0, resp_version, output),
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
      // 聚合流源键只读（结果写回仅落 dst），封窗守卫解构即释
      Some((o, _swap_in_window)) => objs.push(o),
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
  let resp_version = storage.resp_version;
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
      exec_tiered_set(
        &storage.batch,
        key,
        ctx,
        TieredCollectionArgs::new(op, (0, 0), args, resp_version),
        output,
      )
      .await
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
          run_operate(obj, SetOperation::Smembers, &[], 0, 0, resp_version, output);
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
          run_operate(
            obj,
            SetOperation::Sismember,
            args,
            0,
            0,
            resp_version,
            output,
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
          run_operate(
            obj,
            SetOperation::Smismember,
            args,
            0,
            0,
            resp_version,
            output,
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
          run_operate(
            obj,
            SetOperation::Srandmember,
            &[],
            count_parameter,
            fastrand::i32(..),
            resp_version,
            output,
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
      let Some(objs) = load_many_async(storage, refs.get(1..).unwrap_or(&[]), output).await? else {
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
  let Some((mut obj, swap_in_window)) = obj_load_shortcircuit(storage, key, output, |output| {
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
  let mut obj_out = run_operate(
    &mut obj,
    SetOperation::Spop,
    &[],
    count_parameter,
    0,
    resp_version,
    output,
  );
  // 写回失败回退挂载点再落错（慢路径统一应答前清场）
  if save_or_gc(storage, key, &obj, swap_in_window.is_some())
    .await
    .is_err()
  {
    obj_out.reset();
    return Err(());
  }
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
  let Some((mut src, src_window)) = obj_load(storage, source_key, output).await? else {
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
  let (mut dst, dst_window) = match obj_load(storage, destination_key, output).await? {
    Some((o, w)) => (o, w),
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

  // 写回序先目标后源（与同步段 set_move 同款补偿序，对标 C# SetMove 两键
  // 事务原子性）：目标写失败时源零变异，错误帧后数据无损；目标成功而源失败
  // 时 member 双份存在于源与目标，无丢失，重试幂等收敛
  save_or_gc(storage, destination_key, &dst, dst_window.is_some()).await?;
  save_or_gc(storage, source_key, &src, src_window.is_some()).await?;
  output.extend_from_slice(cs::RESP_RETURN_VAL_1);
  Ok(())
}

/// 单键异步装载（None = WRONGTYPE 错误行已写出；缺失按空集合，
/// 与同步段 Missing → 空对象矩阵对位）；物化降级装配单点转引
/// [`load_typed_sealed`]，守卫随对象交调用方持跨求值与写回收尾
/// （信封域装载守卫为 None）
async fn obj_load(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<(SetObject, Option<SwapInWindowGuard>)>, ()> {
  match load_typed_sealed(storage, key, GarnetObjectType::Set, output).await? {
    SealedLoad::WrongType => Ok(None),
    SealedLoad::Missing => Ok(Some((SetObject::new(), None))),
    SealedLoad::Present(o, window) => Ok(Some((o, window))),
  }
}

/// 单键异步装载（None = WRONGTYPE/MISSING 短路已按同步段口径应答）；
/// 物化降级装配单点转引 [`load_typed_sealed`]，守卫随对象交调用方
/// 持跨求值与写回收尾
async fn obj_load_shortcircuit(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
  on_missing: impl FnOnce(&mut Vec<u8>),
) -> Result<Option<(SetObject, Option<SwapInWindowGuard>)>, ()> {
  match load_typed_sealed(storage, key, GarnetObjectType::Set, output).await? {
    SealedLoad::WrongType => Ok(None),
    // 缺失短路应答仅在真缺失时触发
    SealedLoad::Missing => {
      on_missing(output);
      Ok(None)
    }
    SealedLoad::Present(o, window) => Ok(Some((o, window))),
  }
}

/// 删空回收或信封写回（对标 sync set_save_or_gc 的异步臂）
async fn save_or_gc(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  obj: &SetObject,
  sealed: bool,
) -> Result<(), ()> {
  obj_writeback_tiered(storage, key, GarnetObjectType::Set, obj, sealed).await
}
