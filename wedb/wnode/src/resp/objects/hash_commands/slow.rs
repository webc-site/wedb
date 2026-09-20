/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/HashCommands.cs 各命令经 Tsavorite pending
/// 读 CompletePending 后重放的异步形态：磁盘候选经 StorageSession 异步读
/// 闭环后按同步入口同款语义求值（Missing 短路与 RMW 新建矩阵逐一对位），
/// 写回经异步段对象写回唯一漏斗（StorageSession::obj_save / delete_string，
/// 信封整值入账对标 C# WriteLogUpsert）。`Err(())` 为存储 IO 失败，由
/// exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE
use wcol::hash::hash_object::HashObject;
use wdev::Device;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{HashOperation, Rmw, read::write_null_array, run_operate, should_write_back};
use crate::{
  resp::objects::{
    object_store_utils::{
      ElementHeaderKind, GarnetObjectPayload, SyncRmwCmd, SyncRmwHandlers,
      parse_elements_only_args, parse_expire_elements_args, parse_random_member_args,
      run_async_rmw, slow_load_eval, try_tiered_arm, write_random_member_missing, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_hash},
  },
  storage::session::storage_session::StorageSession,
};

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源）
async fn hash_rmw_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  op: HashOperation,
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
      tag: GarnetObjectType::Hash,
      op,
      args,
      arg1,
      arg2,
    },
    output,
    SyncRmwHandlers::new(
      HashObject::from_blob,
      HashObject::new,
      |o: &HashObject| o.is_empty(),
      |o: &HashObject| o.to_blob(),
      |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
      should_write_back,
    ),
  )
  .await
}

/// HSCAN 以外的哈希命令统一慢路径分派（HSCAN 走 shared 慢路径扫描）
pub(crate) async fn hash(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_version;
  let key = refs.first().copied().unwrap_or(&[]);
  let args = refs.get(1..).unwrap_or(&[]);

  // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
  // HDEL 不入表：删除重命令一律走下方对象层通道（物化求值 + 整值重灌），
  // 杜绝向分层树逐成员落删除墓碑（栈深不变量，见 tiered_collection_ops 头注）
  let op_opt = match cmd {
    RespCommand::Hset => Some(HashOperation::Hset),
    RespCommand::Hsetnx => Some(HashOperation::Hsetnx),
    RespCommand::Hmset => Some(HashOperation::Hmset),
    RespCommand::Hget => Some(HashOperation::Hget),
    RespCommand::Hmget => Some(HashOperation::Hmget),
    RespCommand::Hexists => Some(HashOperation::Hexists),
    RespCommand::Hlen => Some(HashOperation::Hlen),
    RespCommand::Hstrlen => Some(HashOperation::Hstrlen),
    RespCommand::Hgetall => Some(HashOperation::Hgetall),
    RespCommand::Hkeys => Some(HashOperation::Hkeys),
    RespCommand::Hvals => Some(HashOperation::Hvals),
    RespCommand::Hincrby => Some(HashOperation::Hincrby),
    RespCommand::Hincrbyfloat => Some(HashOperation::Hincrbyfloat),
    _ => None,
  };
  if try_tiered_arm(
    storage,
    key,
    GarnetObjectType::Hash,
    op_opt,
    output,
    async move |ctx, op, output| {
      exec_tiered_hash(
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

  // HEXPIRE 族：word 打包对位同步段 hash_expire
  if matches!(
    cmd,
    RespCommand::Hexpire | RespCommand::Hpexpire | RespCommand::Hexpireat | RespCommand::Hpexpireat
  ) {
    let (is_ms, is_ts) = match cmd {
      RespCommand::Hexpire => (false, false),
      RespCommand::Hpexpire => (true, false),
      RespCommand::Hexpireat => (false, true),
      _ => (true, true),
    };
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some(args) = parse_expire_elements_args(
      cmd.into(),
      refs,
      ElementHeaderKind::Fields,
      is_ms,
      is_ts,
      output,
    ) else {
      return Ok(());
    };
    hash_rmw_cold(
      storage,
      args.key,
      HashOperation::Hexpire,
      args.elements,
      args.args12,
      resp_version,
      output,
    )
    .await?;
    return Ok(());
  }
  // HTTL / HPERSIST 族
  if matches!(
    cmd,
    RespCommand::Httl
      | RespCommand::Hpttl
      | RespCommand::Hexpiretime
      | RespCommand::Hpexpiretime
      | RespCommand::Hpersist
  ) {
    let op = match cmd {
      RespCommand::Hpersist => HashOperation::Hpersist,
      _ => HashOperation::Httl,
    };
    let args12 = match cmd {
      RespCommand::Httl => (0, 0),
      RespCommand::Hpttl => (1, 0),
      RespCommand::Hexpiretime => (0, 1),
      RespCommand::Hpexpiretime => (1, 1),
      _ => (0, 0),
    };
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, fields)) =
      parse_elements_only_args(cmd.into(), refs, ElementHeaderKind::Fields, output)
    else {
      return Ok(());
    };
    hash_rmw_cold(storage, key, op, fields, args12, resp_version, output).await?;
    return Ok(());
  }

  // RMW 形态（同步段 Missing 走空对象求值矩阵，骨架对位）
  let rmw_spec = match cmd {
    RespCommand::Hset => Some((HashOperation::Hset, (0, 0))),
    RespCommand::Hsetnx => Some((HashOperation::Hsetnx, (0, 0))),
    RespCommand::Hmset => Some((HashOperation::Hmset, (0, 0))),
    RespCommand::Hget => Some((HashOperation::Hget, (0, 0))),
    RespCommand::Hdel => Some((HashOperation::Hdel, (0, 0))),
    RespCommand::Hincrby => Some((HashOperation::Hincrby, (0, 0))),
    RespCommand::Hincrbyfloat => Some((HashOperation::Hincrbyfloat, (0, 0))),
    _ => None,
  };
  if let Some((op, args12)) = rmw_spec {
    let done = hash_rmw_cold(storage, key, op, args, args12, resp_version, output).await?;
    if let Rmw::Present(done) = done {
      // HMSET 恒回 +OK；HSET/HSETNX/HDEL 补整数；其余负载已透写
      if op == HashOperation::Hmset {
        if !done.payload_written {
          output.extend_from_slice(cs::RESP_OK);
        }
      } else if matches!(
        op,
        HashOperation::Hset | HashOperation::Hsetnx | HashOperation::Hdel
      ) {
        write_rmw_reply(done, output);
      }
    }
    return Ok(());
  }

  // 装载 + operate 形态（同步段 Missing 短路应答逐一对位；HLEN 由
  // exec_slow O(1) 计数直读臂承接）
  let load_spec = match cmd {
    RespCommand::Hgetall => Some((HashOperation::Hgetall, 0, 0, ReplyOnMissing::EmptyList)),
    RespCommand::Hmget => Some((HashOperation::Hmget, 0, 0, ReplyOnMissing::NullArray)),
    RespCommand::Hexists => Some((HashOperation::Hexists, 0, 0, ReplyOnMissing::Zero)),
    RespCommand::Hstrlen => Some((HashOperation::Hstrlen, 0, 0, ReplyOnMissing::Zero)),
    RespCommand::Hkeys => Some((HashOperation::Hkeys, 0, 0, ReplyOnMissing::EmptyList)),
    RespCommand::Hvals => Some((HashOperation::Hvals, 0, 0, ReplyOnMissing::EmptyList)),
    RespCommand::Hrandfield => None, // 需参数打包，独立分支
    _ => None,
  };
  if let Some((op, arg1, arg2, missing)) = load_spec {
    return slow_load_eval(
      storage,
      key,
      GarnetObjectType::Hash,
      output,
      HashObject::from_blob,
      move |output: &mut Vec<u8>| match missing {
        ReplyOnMissing::Zero => output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        ReplyOnMissing::EmptyList => output.extend_from_slice(cs::RESP_EMPTYLIST),
        ReplyOnMissing::NullArray => write_null_array(output, refs.len() - 1, resp_version),
      },
      async move |obj: &mut super::HashObject, output: &mut Vec<u8>| {
        let result1 = run_operate(obj, op, args, arg1, arg2, resp_version, output).result1;
        if matches!(op, HashOperation::Hexists | HashOperation::Hstrlen) {
          output.write_resp_int(result1);
        }
      },
    )
    .await;
  }

  // HRANDFIELD：count/WITHVALUES 打包 + seed 对位同步段
  if cmd == RespCommand::Hrandfield {
    // 参数推导单源（快慢共用，失败帧已写出；第三词元大小写门与快侧同口径）
    let Some(rand_args) = parse_random_member_args("HRANDFIELD", refs, cs::WITHVALUES, output)
    else {
      return Ok(());
    };
    // count 为 0 不触达后端（应答与缺失态同形单源）
    if rand_args.param_count == 0 {
      write_random_member_missing(output, rand_args.included_count, resp_version);
      return Ok(());
    }
    let arg1 = rand_args.arg1;
    return slow_load_eval(
      storage,
      key,
      GarnetObjectType::Hash,
      output,
      HashObject::from_blob,
      |output: &mut Vec<u8>| {
        write_random_member_missing(output, rand_args.included_count, resp_version);
      },
      async move |obj: &mut super::HashObject, output: &mut Vec<u8>| {
        run_operate(
          obj,
          HashOperation::Hrandfield,
          &[],
          arg1,
          fastrand::i32(..),
          resp_version,
          output,
        );
      },
    )
    .await;
  }

  // 兜底臂：不应抵达慢路径的未接线命令形态（快路径参数校验已拦截）
  cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
  Ok(())
}

/// 装载形态 Missing 短路应答类别
#[derive(Clone, Copy)]
enum ReplyOnMissing {
  Zero,
  EmptyList,
  NullArray,
}
