//! 有序集合慢路径执行臂（exec_slow 冷键分派）
//!
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 各命令经 Tsavorite
//! pending 读 CompletePending 后重放的异步形态。阻塞族（BZPOPMIN/BZPOPMAX/
//! BZMPOP）在经纪注入时已在快路径挂起不降级，降级仅发生于经纪未注入的
//! 独立会话域，异步臂对位立即可取路径。`Err(())` 为存储 IO 失败，由
//! exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE

use wbase::num::{strict_i32, strict_i64};
use wcol::{
  ObjLoad as WcolObjLoad,
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation, SortedSetRangeOpts},
};
use wresp::{
  cmd_strings as cs,
  command::RespCommand,
  ext::RespVecExt,
  options::{ExpirationWithOption, ExpireOption, try_get_expire_option},
};
use wval::GarnetObjectType;

use super::{
  CombineKind, RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM, Rmw, combine_sets, diff_sets,
  parse_combine_args, parse_diff_args, parse_pairs_payload, run_operate, should_write_back,
  write_popped_pairs, write_zset_entries,
};
use crate::{
  resp::objects::{
    object_store_utils::{
      ElementHeaderKind, GarnetObjectPayload, SyncRmwCmd, SyncRmwHandlers,
      compute_expiration_ticks, obj_load_typed_async, obj_writeback_tiered, parse_elements_header,
      retire_tiered_dest, run_async_rmw, slow_load_eval, try_tiered_arm, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_zset, tiered_materialize_blob},
  },
  storage::session::storage_session::StorageSession,
};

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源；
/// 事务过程存储视图冷键臂同函数复用，一处定义）
pub(crate) async fn zset_rmw_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
  op: SortedSetOperation,
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
      tag: GarnetObjectType::SortedSet,
      op,
      args,
      arg1,
      arg2,
    },
    output,
    SyncRmwHandlers::new(
      SortedSetObject::from_blob,
      SortedSetObject::new,
      |o: &SortedSetObject| o.sorted_set_dict.is_empty(),
      |o: &SortedSetObject| o.to_blob(),
      |obj, op, args| run_operate(obj, op, args, arg1, arg2, resp_version),
      should_write_back,
    ),
  )
  .await
}

/// 单键异步装载：`Ok(None)` = WRONGTYPE 错误行已写出；
/// `Ok(Some(None))` = MISSING（调用方定短路应答）
async fn load_typed(
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<SortedSetObject>>, ()> {
  let loaded = obj_load_typed_async(
    storage,
    key,
    GarnetObjectType::SortedSet,
    output,
    SortedSetObject::from_blob,
  )
  .await
  .map_err(|_| ())?;
  Ok(match loaded {
    // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存对象
    WcolObjLoad::Degrade => {
      let Some(blob) =
        tiered_materialize_blob(&storage.batch, key, GarnetObjectType::SortedSet).await?
      else {
        return Err(());
      };
      // 物化载荷解码 fail-fast：畸形落错中止，不回退空对象销毁原键
      match SortedSetObject::from_blob(&blob) {
        Some(obj) => Some(Some(obj)),
        None => {
          log::error!(
            "zset load_typed: corrupted materialized payload, key='{}'",
            String::from_utf8_lossy(key)
          );
          return Err(());
        }
      }
    }
    WcolObjLoad::WrongType => None,
    WcolObjLoad::Missing => Some(None),
    WcolObjLoad::Present(o) => Some(Some(o)),
  })
}

/// 多键异步装载（缺失按空集合；`Ok(None)` = WRONGTYPE 错误行已写出）
async fn load_many_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SortedSetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match load_typed(storage, key, output).await? {
      None => return Ok(None),
      Some(None) => objs.push(SortedSetObject::new()),
      Some(Some(o)) => objs.push(o),
    }
  }
  Ok(Some(objs))
}

/// STORE 族目标键异步收尾：SET 语义清 TTL（对标 C# 先统一面 Delete dst
/// 再 RMW ZADD，信封域 upsert 默认保留须显式清退）后删空回收或写回
async fn store_dest_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  dst: &[u8],
  result: &SortedSetObject,
) -> Result<usize, ()> {
  storage.persist_key(dst).await.map_err(|_| ())?;
  let count = result.sorted_set_dict.len();
  if result.sorted_set_dict.is_empty() {
    storage
      .delete_string(dst)
      .await
      .map(|_| ())
      .map_err(|_| ())?;
  } else {
    storage
      .obj_save(dst, GarnetObjectType::SortedSet, &result.to_blob())
      .await
      .map_err(|_| ())?;
  }
  // 目标键若原为分层态：信封已接管（或删空），清退残留树
  retire_tiered_dest(storage, dst).await?;
  Ok(count)
}

/// ZEXPIRE 族慢路径解析元组：(key, members 切片, (expiration_hi, expiration_lo))
type ExpireArgs<'a> = (&'a [u8], &'a [&'a [u8]], (i32, i32));

/// ZEXPIRE 族慢路径参数重推导：(key, members 切片, expiration word)
fn parse_expire_args<'a>(
  refs: &'a [&'a [u8]],
  is_milliseconds: bool,
  is_timestamp: bool,
) -> Option<ExpireArgs<'a>> {
  let key = refs.first().copied()?;
  let expiration = strict_i64(refs.get(1).copied()?)?;
  if expiration < 0 {
    return None;
  }
  let mut curr_idx = 2;
  let mut expire_option = ExpireOption::NONE;
  if let Some(opt) = refs.get(curr_idx).copied().and_then(try_get_expire_option) {
    expire_option = opt;
    curr_idx += 1;
  }
  let (members_start, _) =
    parse_elements_header(refs, curr_idx, ElementHeaderKind::Members, &mut Vec::new())?;
  let ticks = compute_expiration_ticks(expiration, is_milliseconds, is_timestamp);
  let e = ExpirationWithOption::new(ticks, expire_option);
  Some((
    key,
    &refs[members_start..],
    ((e.word() >> 32) as i32, e.word() as i32),
  ))
}

/// ZTTL / ZPERSIST 族慢路径参数重推导：(key, members 切片)
fn parse_members_args<'a>(refs: &'a [&'a [u8]]) -> Option<(&'a [u8], &'a [&'a [u8]])> {
  let key = refs.first().copied()?;
  let (members_start, _) =
    parse_elements_header(refs, 1, ElementHeaderKind::Members, &mut Vec::new())?;
  Some((key, &refs[members_start..]))
}

/// ZRANGE 族命令 → arg2 选项位（对象层 `run_operate` 与分层树内臂共用同一约定，
/// 一处定义；libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetRangeOpts）
fn range_opts_of(cmd: RespCommand) -> SortedSetRangeOpts {
  match cmd {
    RespCommand::Zrevrange => SortedSetRangeOpts::REVERSE,
    RespCommand::Zrangebylex => SortedSetRangeOpts::BY_LEX,
    RespCommand::Zrevrangebylex => SortedSetRangeOpts::BY_LEX.union(SortedSetRangeOpts::REVERSE),
    RespCommand::Zrangebyscore => SortedSetRangeOpts::BY_SCORE,
    RespCommand::Zrevrangebyscore => {
      SortedSetRangeOpts::BY_SCORE.union(SortedSetRangeOpts::REVERSE)
    }
    // ZRANGE 本体与其余命令：无选项位
    _ => SortedSetRangeOpts::NONE,
  }
}

/// ZRANK / ZREVRANK 的 WITHSCORE 位（对位快路径与 C# 同口径：仅 Count == 3 校验
/// 该词元，Count > 3 静默忽略）；词元非法 → `None`，由调用方回既有错误行
fn zrank_with_score(refs: &[&[u8]]) -> Option<i32> {
  match refs.len() {
    0..=2 => Some(0),
    3 if refs[2].eq_ignore_ascii_case(b"WITHSCORE") => Some(1),
    _ if refs.len() > 3 => Some(0),
    _ => None,
  }
}

/// 有序集合命令统一慢路径分派（ZSCAN 走 shared 慢路径扫描）
pub(crate) async fn sorted_set(
  storage: &StorageSession<'_, impl wdev::Device>,
  notify: &impl Fn(&[u8]),
  cmd: RespCommand,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_protocol_version();
  let key = refs.first().copied().unwrap_or(&[]);
  let args = refs.get(1..).unwrap_or(&[]);

  // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
  // ZRANGE 族 / ZLEXCOUNT / ZRANK 族经 arg12 通道下同原生臂：范围与排名语义在
  // 树内流式求值（wcol 参数解析与结果负载单源复用），不再走装载型物化通道
  // ZREM 不入表：删除重命令与 ZPOPMIN / ZREMRANGE* 同径走对象层通道（物化求值
  // + 整值重灌），杜绝向分层树逐成员落删除墓碑（见 tiered_collection_ops 头注）
  // ZCOUNT 不入表：已在下方 rmw_spec 内，run_async_rmw 的树内臂优先接手同一函数
  let op_opt = match cmd {
    RespCommand::Zadd => Some((SortedSetOperation::Zadd, (0, 0))),
    RespCommand::Zscore => Some((SortedSetOperation::Zscore, (0, 0))),
    RespCommand::Zmscore => Some((SortedSetOperation::Zmscore, (0, 0))),
    RespCommand::Zcard => Some((SortedSetOperation::Zcard, (0, 0))),
    RespCommand::Zincrby => Some((SortedSetOperation::Zincrby, (0, 0))),
    RespCommand::Zrange
    | RespCommand::Zrevrange
    | RespCommand::Zrangebylex
    | RespCommand::Zrevrangebylex
    | RespCommand::Zrangebyscore
    | RespCommand::Zrevrangebyscore => Some((
      SortedSetOperation::Zrange,
      (0, range_opts_of(cmd).bits() as i32),
    )),
    RespCommand::Zlexcount => Some((SortedSetOperation::Zlexcount, (0, 0))),
    RespCommand::Zrank => zrank_with_score(refs).map(|ws| (SortedSetOperation::Zrank, (ws, 0))),
    RespCommand::Zrevrank => {
      zrank_with_score(refs).map(|ws| (SortedSetOperation::Zrevrank, (ws, 0)))
    }
    _ => None,
  };
  if try_tiered_arm(
    storage,
    key,
    GarnetObjectType::SortedSet,
    op_opt,
    output,
    async move |ctx, (op, args12), output| {
      let handled = exec_tiered_zset(
        &storage.batch,
        key,
        ctx,
        TieredCollectionArgs::new(op, args12, args, resp_version),
        output,
      )
      .await?;
      if handled && matches!(cmd, RespCommand::Zadd | RespCommand::Zincrby) {
        // 写命令唤醒该键的阻塞观察者（对标同步段 ZADD 后 HandleCollectionUpdate）
        notify(key);
      }
      Ok(handled)
    },
  )
  .await?
  {
    return Ok(());
  }

  // ---- ZEXPIRE 族（word 打包对位同步段 sorted_set_expire）
  if matches!(
    cmd,
    RespCommand::Zexpire | RespCommand::Zpexpire | RespCommand::Zexpireat | RespCommand::Zpexpireat
  ) {
    let (is_ms, is_ts) = match cmd {
      RespCommand::Zexpire => (false, false),
      RespCommand::Zpexpire => (true, false),
      RespCommand::Zexpireat => (false, true),
      _ => (true, true),
    };
    let Some((key, members, args12)) = parse_expire_args(refs, is_ms, is_ts) else {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      return Ok(());
    };
    zset_rmw_cold(
      storage,
      key,
      SortedSetOperation::Zexpire,
      members,
      args12,
      resp_version,
      output,
    )
    .await?;
    return Ok(());
  }
  // ---- ZTTL / ZPERSIST 族
  if matches!(
    cmd,
    RespCommand::Zttl
      | RespCommand::Zpttl
      | RespCommand::Zexpiretime
      | RespCommand::Zpexpiretime
      | RespCommand::Zpersist
  ) {
    let op = match cmd {
      RespCommand::Zpersist => SortedSetOperation::Zpersist,
      _ => SortedSetOperation::Zttl,
    };
    let args12 = match cmd {
      RespCommand::Zttl => (0, 0),
      RespCommand::Zpttl => (1, 0),
      RespCommand::Zexpiretime => (0, 1),
      RespCommand::Zpexpiretime => (1, 1),
      _ => (0, 0),
    };
    let Some((key, members)) = parse_members_args(refs) else {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      return Ok(());
    };
    zset_rmw_cold(storage, key, op, members, args12, resp_version, output).await?;
    return Ok(());
  }

  // ---- RMW 形态（ZCARD 由 exec_slow O(1) 计数直读臂承接）
  let rmw_spec = match cmd {
    RespCommand::Zadd => Some((SortedSetOperation::Zadd, (0, 0))),
    RespCommand::Zscore => Some((SortedSetOperation::Zscore, (0, 0))),
    RespCommand::Zrem => Some((SortedSetOperation::Zrem, (0, 0))),
    RespCommand::Zcount => Some((SortedSetOperation::Zcount, (0, 0))),
    RespCommand::Zincrby => Some((SortedSetOperation::Zincrby, (0, 0))),
    RespCommand::Zpopmin | RespCommand::Zpopmax => {
      // count 缺省形态传 -1（无外层数组头）
      let arg1 = match refs.get(1) {
        None => -1,
        Some(c) => match strict_i32(c) {
          Some(v) if v >= 0 => v,
          _ => {
            cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
            return Ok(());
          }
        },
      };
      Some((
        if cmd == RespCommand::Zpopmin {
          SortedSetOperation::Zpopmin
        } else {
          SortedSetOperation::Zpopmax
        },
        (arg1, 0),
      ))
    }
    RespCommand::Zremrangebyrank => Some((SortedSetOperation::Zremrangebyrank, (0, 0))),
    RespCommand::Zremrangebyscore => Some((SortedSetOperation::Zremrangebyscore, (0, 0))),
    RespCommand::Zremrangebylex => Some((SortedSetOperation::Zremrangebylex, (0, 0))),
    _ => None,
  };
  if let Some((op, args12)) = rmw_spec {
    let done = zset_rmw_cold(storage, key, op, args, args12, resp_version, output).await?;
    if let Rmw::Present(done) = done {
      // ZREM 补整数；ZREMRANGEBYLEX 仅回填 result1（int.MaxValue =
      // 参数错误、int.MinValue = 部分执行）；其余负载已透写
      match op {
        SortedSetOperation::Zrem => write_rmw_reply(done, output),
        SortedSetOperation::Zremrangebylex if !done.payload_written => {
          if done.result1 == i32::MAX as i64 {
            output.extend_from_slice(RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM);
          } else if done.result1 != i32::MIN as i64 {
            output.write_resp_int(done.result1);
          }
        }
        _ => {}
      }
      // 写成功唤醒该键的阻塞观察者（对标同步段 ZADD 后 HandleCollectionUpdate）
      if op == SortedSetOperation::Zadd {
        notify(key);
      }
    }
    return Ok(());
  }

  // ---- 装载 + operate 形态
  match cmd {
    RespCommand::Zrange
    | RespCommand::Zrevrange
    | RespCommand::Zrangebylex
    | RespCommand::Zrevrangebylex
    | RespCommand::Zrangebyscore
    | RespCommand::Zrevrangebyscore => {
      // 选项位换算单点见 range_opts_of（与上方分层原生臂同一处）
      let opts = range_opts_of(cmd);
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        |output| output.extend_from_slice(b"*0\r\n"),
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          let obj_out = run_operate(
            obj,
            SortedSetOperation::Zrange,
            args,
            0,
            opts.bits() as i32,
            resp_version,
          );
          output.extend_from_slice(&obj_out.payload);
        },
      )
      .await
    }
    RespCommand::Zmscore => {
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        // 键缺失：全 null 数组
        |output| {
          output.write_resp_array_len(refs.len() - 1);
          for _ in 1..refs.len() {
            output.write_resp_null_ver(resp_version);
          }
        },
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          output.extend_from_slice(
            &run_operate(obj, SortedSetOperation::Zmscore, args, 0, 0, resp_version).payload,
          );
        },
      )
      .await
    }
    RespCommand::Zlexcount => {
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          // 解析失败标记（int.MaxValue）→ 错误回复；否则以 result1 作整数回复
          let result1 =
            run_operate(obj, SortedSetOperation::Zlexcount, args, 0, 0, resp_version).result1;
          if result1 == i32::MAX as i64 {
            output.extend_from_slice(RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM);
          } else if result1 != i32::MIN as i64 {
            output.write_resp_int(result1);
          }
        },
      )
      .await
    }
    RespCommand::Zrank | RespCommand::Zrevrank => {
      let op = if cmd == RespCommand::Zrank {
        SortedSetOperation::Zrank
      } else {
        SortedSetOperation::Zrevrank
      };
      // 对齐快路径：C# 仅 Count==3 校验 WITHSCORE，Count>3 静默忽略（换算单点
      // 见 zrank_with_score，与上方分层原生臂同一处）
      let Some(with_score) = zrank_with_score(refs) else {
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      let member = refs.get(1).copied().unwrap_or(&[]);
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        |output| output.write_resp_null_ver(resp_version),
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          let obj_out = run_operate(obj, op, &[member], with_score, 0, resp_version);
          output.extend_from_slice(&obj_out.payload);
        },
      )
      .await
    }
    RespCommand::Zrandmember => {
      // 参数打包：arg1 = (count << 1 | includedCount) << 1 | withScores
      let mut param_count = 1_i32;
      let mut included_count = false;
      let mut with_scores = false;
      if let Some(c) = refs.get(1) {
        let Some(v) = strict_i32(c) else {
          cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
          return Ok(());
        };
        param_count = v.min(i32::MAX >> 2);
        included_count = true;
        if let Some(ws) = refs.get(2) {
          if !ws.eq_ignore_ascii_case(b"WITHSCORES") {
            cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
            return Ok(());
          }
          with_scores = true;
        }
      }
      if param_count == 0 {
        output.extend_from_slice(b"*0\r\n");
        return Ok(());
      }
      let arg1 = (((param_count << 1) | i32::from(included_count)) << 1) | i32::from(with_scores);
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        |output: &mut Vec<u8>| {
          if refs.len() > 1 {
            output.extend_from_slice(b"*0\r\n");
          } else {
            output.write_resp_null_ver(resp_version);
          }
        },
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          let obj_out = run_operate(
            obj,
            SortedSetOperation::Zrandmember,
            &[],
            arg1,
            fastrand::i32(..),
            resp_version,
          );
          output.extend_from_slice(&obj_out.payload);
        },
      )
      .await
    }
    RespCommand::Zrangestore => zrangestore_cold(storage, notify, refs, resp_version, output).await,
    RespCommand::Zdiff => {
      let Some((keys, with_scores)) = parse_diff_args(refs, "ZDIFF", output) else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, &keys, output).await? else {
        return Ok(());
      };
      let result = diff_sets(&objs);
      write_zset_entries(Some(&result), with_scores, output, resp_version);
      Ok(())
    }
    RespCommand::Zdiffstore => {
      let dst = key;
      let Some((keys, _)) = parse_diff_args(refs.get(1..).unwrap_or(&[]), "ZDIFFSTORE", output)
      else {
        return Ok(());
      };
      combine_store_cold(storage, notify, dst, &keys, ZSetCombineKind::Diff, output).await
    }
    RespCommand::Zinter | RespCommand::Zunion => {
      let name = if cmd == RespCommand::Zinter {
        "ZINTER"
      } else {
        "ZUNION"
      };
      let Some(parsed) = parse_combine_args(refs, name, output) else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, &parsed.keys, output).await? else {
        return Ok(());
      };
      let result = combine_sets(
        &objs,
        &parsed.weights,
        parsed.aggregate,
        if cmd == RespCommand::Zinter {
          CombineKind::Intersect
        } else {
          CombineKind::Union
        },
      );
      write_zset_entries(Some(&result), parsed.with_scores, output, resp_version);
      Ok(())
    }
    RespCommand::Zinterstore | RespCommand::Zunionstore => {
      let name = if cmd == RespCommand::Zinterstore {
        "ZINTERSTORE"
      } else {
        "ZUNIONSTORE"
      };
      let kind = if cmd == RespCommand::Zinterstore {
        ZSetCombineKind::Inter
      } else {
        ZSetCombineKind::Union
      };
      let Some(parsed) = parse_combine_args(refs.get(1..).unwrap_or(&[]), name, output) else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, &parsed.keys, output).await? else {
        return Ok(());
      };
      let result = combine_sets(
        &objs,
        &parsed.weights,
        parsed.aggregate,
        if kind == ZSetCombineKind::Inter {
          CombineKind::Intersect
        } else {
          CombineKind::Union
        },
      );
      let count = store_dest_cold(storage, key, &result).await?;
      output.write_resp_int(count as i64);
      notify(key);
      Ok(())
    }
    RespCommand::Zintercard => {
      let Some((keys, limit)) = parse_zintercard_args(refs) else {
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      let Some(mut objs) = load_many_cold(storage, &keys, output).await? else {
        return Ok(());
      };
      // 基数取最小集合遍历查集（对位同步段，命中 limit 提前跳出）
      let card = if objs.is_empty() {
        0
      } else if objs.len() == 1 {
        objs[0].count() as i64
      } else if let Some((min_idx, min_obj)) = objs
        .iter()
        .enumerate()
        .min_by_key(|(_, o)| o.sorted_set_dict.len())
      {
        if min_obj.sorted_set_dict.is_empty() {
          0
        } else {
          let others: Vec<_> = objs
            .iter()
            .enumerate()
            .filter_map(|(i, o)| (i != min_idx).then_some(&o.sorted_set_dict))
            .collect();
          let mut count = 0_i64;
          for member in min_obj.sorted_set_dict.keys() {
            if others.iter().all(|dict| dict.contains_key(member)) {
              count += 1;
              if limit > 0 && count >= i64::from(limit) {
                break;
              }
            }
          }
          count
        }
      } else {
        0
      };
      output.write_resp_int(if limit > 0 {
        card.min(i64::from(limit))
      } else {
        card
      });
      Ok(())
    }
    RespCommand::Zmpop | RespCommand::Bzmpop => {
      let Some((keys, low_scores_first, count)) =
        parse_zmpop_common(refs, cmd == RespCommand::Bzmpop)
      else {
        cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
        return Ok(());
      };
      let handled = zset_pop_first_nonempty_cold(
        storage,
        &keys,
        low_scores_first,
        count,
        output,
        resp_version,
      )
      .await?;
      if !handled {
        // 同步段 ZMPOP/BZMPOP NOTFOUND → WriteNull（会话版本分派，RESP3 为 `_\r\n`）
        output.write_resp_null_ver(resp_version);
      }
      Ok(())
    }
    RespCommand::Bzpopmin | RespCommand::Bzpopmax => {
      // 阻塞族独立会话域语义：逐键立即可取（对位同步段立即可取路径）
      let is_max = cmd == RespCommand::Bzpopmax;
      for k in &refs[..refs.len().saturating_sub(1)] {
        let Some(Some(mut obj)) = load_typed(storage, k, output).await? else {
          continue;
        };
        let Some((score, member)) = obj.pop_min_or_max(is_max) else {
          continue;
        };
        if zset_save_or_gc_local(storage, &obj, k).await.is_err() {
          return Err(());
        }
        output.write_resp_array_len(3);
        output.write_resp_bulk_string(k);
        output.write_resp_bulk_string(&member);
        // 分值版本分派（C# SortedSetBlockingPop :1619 WriteDoubleNumeric）
        cs::write_double_numeric(output, score, resp_version);
        return Ok(());
      }
      // C# !result.Found → WriteNull（会话版本分派，RESP3 为 `_\r\n`）
      output.write_resp_null_ver(resp_version);
      Ok(())
    }
    _ => {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// STORE 族运算类别（ZDIFFSTORE / ZINTERSTORE / ZUNIONSTORE 分派用）
#[derive(Clone, Copy, PartialEq, Eq)]
enum ZSetCombineKind {
  Diff,
  Inter,
  Union,
}

/// ZDIFFSTORE 慢路径（first − rest 折叠后落目标键）
async fn combine_store_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  notify: &impl Fn(&[u8]),
  dst: &[u8],
  keys: &[&[u8]],
  kind: ZSetCombineKind,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some(objs) = load_many_cold(storage, keys, output).await? else {
    return Ok(());
  };
  let result = match kind {
    ZSetCombineKind::Diff => diff_sets(&objs),
    // ZINTERSTORE/ZUNIONSTORE 走 parse_combine_args 通道，不经过此臂
    _ => return Err(()),
  };
  let count = store_dest_cold(storage, dst, &result).await?;
  output.write_resp_int(count as i64);
  notify(dst);
  Ok(())
}

/// ZRANGESTORE 慢路径对位（读源范围 → 成对负载落目标键）
async fn zrangestore_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  notify: &impl Fn(&[u8]),
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let dst_key = refs.first().copied().unwrap_or(&[]);
  let src_key = refs.get(1).copied().unwrap_or(&[]);
  let range_args = refs.get(2..).unwrap_or(&[]);

  // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）后回 :0
  let mut src_obj = match load_typed(storage, src_key, output).await? {
    Some(None) => {
      store_dest_cold(storage, dst_key, &SortedSetObject::new()).await?;
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(());
    }
    Some(Some(o)) => o,
    None => return Ok(()),
  };
  let obj_out = run_operate(
    &mut src_obj,
    SortedSetOperation::Zrange,
    range_args,
    0,
    SortedSetRangeOpts::STORE.bits() as i32,
    resp_version,
  );
  // result1 = -1 表示范围参数被拒（错误已写入负载）
  if obj_out.result1 == -1 {
    output.extend_from_slice(&obj_out.payload);
    return Ok(());
  }
  let dst = SortedSetObject::from_entries(parse_pairs_payload(&obj_out.payload));
  let count = store_dest_cold(storage, dst_key, &dst).await?;
  output.write_resp_int(count as i64);
  notify(dst_key);
  Ok(())
}

/// 删空回收或信封写回（对标 sync zset_save_or_gc 的异步臂；分层感知）
async fn zset_save_or_gc_local(
  storage: &StorageSession<'_, impl wdev::Device>,
  obj: &SortedSetObject,
  key: &[u8],
) -> Result<(), ()> {
  obj_writeback_tiered(storage, key, GarnetObjectType::SortedSet, obj).await
}

/// ZINTERCARD 参数重推导：(键切片, LIMIT)
fn parse_zintercard_args<'a>(refs: &'a [&'a [u8]]) -> Option<(Vec<&'a [u8]>, i32)> {
  let num_keys = strict_i32(refs.first().copied()?)?;
  if num_keys < 1 {
    return None;
  }
  let idx = num_keys as usize + 1;
  let mut limit = 0_i32;
  if refs.len() == idx + 2 {
    if !refs[idx].eq_ignore_ascii_case(b"LIMIT") {
      return None;
    }
    let v = strict_i32(refs[idx + 1])?;
    if v < 0 {
      return None;
    }
    limit = v;
  } else if refs.len() != idx {
    return None;
  }
  Some((refs[1..=num_keys as usize].to_vec(), limit))
}

/// ZMPOP/BZMPOP 参数重推导：(键切片, 低分优先, count)
///
/// ZMPOP: numkeys key [key ...] MIN|MAX [COUNT count]
/// BZMPOP: timeout numkeys key [key ...] MIN|MAX [COUNT count]
fn parse_zmpop_common<'a>(
  refs: &'a [&'a [u8]],
  is_blocking: bool,
) -> Option<(Vec<&'a [u8]>, bool, i32)> {
  let base = usize::from(is_blocking);
  let num_keys = strict_i32(refs.get(base).copied()?)?;
  if num_keys < 1 {
    return None;
  }
  let order_idx = base + 1 + num_keys as usize;
  // 定长形态：order 必带；COUNT 形态恰多 2 参
  if refs.len() != order_idx + 1 && refs.len() != order_idx + 3 {
    return None;
  }
  let keys = refs[base + 1..order_idx].to_vec();
  let order = refs.get(order_idx).copied()?;
  let low_scores_first = if order.eq_ignore_ascii_case(b"MIN") {
    true
  } else if order.eq_ignore_ascii_case(b"MAX") {
    false
  } else {
    return None;
  };
  let mut count = 1_i32;
  if refs.len() == order_idx + 3 {
    if !refs[order_idx + 1].eq_ignore_ascii_case(b"COUNT") {
      return None;
    }
    let c = strict_i32(refs[order_idx + 2])?;
    if c < 1 {
      return None;
    }
    count = c;
  }
  Some((keys, low_scores_first, count))
}

/// 逐键弹出第一个非空有序集合的慢路径对位
///（对标同步段 zset_pop_first_nonempty；返回 `Ok(false)` = WRONGTYPE 已写出）
async fn zset_pop_first_nonempty_cold(
  storage: &StorageSession<'_, impl wdev::Device>,
  keys: &[&[u8]],
  low_scores_first: bool,
  pop_count: i32,
  output: &mut Vec<u8>,
  resp_version: u8,
) -> Result<bool, ()> {
  for key in keys {
    let Some(Some(mut obj)) = load_typed(storage, key, output).await? else {
      continue;
    };
    if obj.count() == 0 {
      continue;
    }
    let max_k = (pop_count.max(0) as usize).min(obj.count());
    let mut popped = Vec::with_capacity(max_k);
    for _ in 0..max_k {
      if let Some(pair) = obj.pop_min_or_max(!low_scores_first) {
        popped.push(pair);
      } else {
        break;
      }
    }
    zset_save_or_gc_local(storage, &obj, key).await?;
    // 回复：[key, [member, score], ...]（与同步段 write_popped_pairs 单源）
    write_popped_pairs(key, &popped, output, resp_version);
    return Ok(true);
  }
  Ok(false)
}
