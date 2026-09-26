//! 有序集合慢路径执行臂（exec_slow 冷键分派）
//!
//! 对标 libs/server/Resp/Objects/SortedSetCommands.cs 各命令经 Tsavorite
//! pending 读 CompletePending 后重放的异步形态。阻塞族（BZPOPMIN/BZPOPMAX/
//! BZMPOP）快路径 park 前预探任一键同步不可出件（活跃分层键 / 磁盘候选）即
//! 整体降级至此（C# 无分层概念、任何键态无条件 BlockingWait 的键态解耦投影）：
//! 装载可出件即直接出件，全键未取到则经经纪等待面（复用列表族
//! `BlockWaitFace` 单源，禁另起机制）在执行域内联竞速超时，出件/超时经
//! `write_collection_item_result` 应答单源出帧；经纪未注入的独立会话域
//! 无等待面，维持立即可取语义回空值。
//! `Err(())` 为存储 IO 失败，由 exec_slow 统一应答 RESP_ERR_SLOW_PATH_STORAGE

use wbase::num::strict_i32;
use wcol::zset::sorted_set_object::{SortedSetObject, SortedSetOperation, SortedSetRangeOpts};
use wdev::Device;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{
  CombineKind, Rmw, combine_sets, diff_sets, intersect_card, parse_combine_args, parse_diff_args,
  parse_pairs_payload, parse_rank_with_score, parse_zmpop_args, pop_up_to, run_operate,
  should_write_back, write_lex_result, write_popped_pairs, write_zset_entries,
};
use crate::{
  resp::objects::{
    list_commands::slow::BlockWaitFace,
    object_store_utils::{
      ElementHeaderKind, GarnetObjectPayload, IntersectCardKind, SyncRmwCmd, SyncRmwHandlers,
      SyncRmwOutcome, load_sealed_tri, obj_writeback_recheck_async, obj_writeback_tiered,
      parse_elements_only_args, parse_expire_elements_args, parse_intersect_card_args,
      parse_random_member_args, run_async_rmw, slow_load_eval, store_dest_cold_common,
      try_tiered_arm, write_random_member_missing, write_rmw_reply,
    },
    tiered_collection_ops::{TieredCollectionArgs, exec_tiered_zset, zset_needs_write},
  },
  session_parse_state_extensions::try_get_timeout_bytes,
  storage::session::storage_session::StorageSession,
};

/// rmw 骨架的慢路径对位（复用家族 should_write_back / run_operate 单源；
/// 事务过程存储视图冷键臂同函数复用，一处定义）
pub(crate) async fn zset_rmw_cold(
  storage: &StorageSession<'_, impl Device>,
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
      |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
      should_write_back,
    ),
  )
  .await
  .map(SyncRmwOutcome::from)
}

/// 多键异步装载（缺失按空集合；`Ok(None)` = WRONGTYPE 错误行已写出）
///
/// 聚合面成员级 TTL：装载即堆序 purge 过期成员，对位同步段 [`load_many`]
/// 与 C# Dictionary getter / TryGetScore / CopyDiff / InPlaceDiff 存活视图口径
async fn load_many_cold(
  storage: &StorageSession<'_, impl Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SortedSetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match load_sealed_tri::<SortedSetObject, _>(storage, key, output).await? {
      None => return Ok(None),
      Some(None) => objs.push(SortedSetObject::new()),
      // 聚合流源键只读（结果写回仅落 dst），封窗守卫解构即释
      Some(Some((mut o, _swap_in_window))) => {
        o.delete_expired_items();
        objs.push(o);
      }
    }
  }
  Ok(Some(objs))
}

/// STORE 族目标键异步收尾：SET 语义清 TTL（对标 C# 先统一面 Delete dst
/// 再 RMW ZADD，信封域 upsert 默认保留须显式清退）后删空回收或写回
///
/// zset STORE 族（ZRANGESTORE / Z*STORE / GEOSEARCHSTORE / GEORADIUS STORE 族）
/// 慢路径收尾单点：geo 臂（票 zcode-r18-geo 发现一）与 zset 臂共用本函数，
/// 禁另造第二套收尾。窗序单点转引 [`store_dest_cold_common`]（票
/// wnode-store-cold-window-ttl-clear-outsides-critical-section /
/// zcode-r32-retirematrix：与 set 臂一处定义，开窗存活为 String 时在持窗写
/// 临界区内先清退旧 String 域，TTL 清退随写落笔，禁窗外裸清）；目标键 rmw 窗
/// 句柄由调用方预取承接（同键同单窗禁双取，装载前预取窗直接传入）
pub(crate) async fn store_dest_cold<D: Device>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
  result: &SortedSetObject,
  window: wkv::RmwWindow<'_, '_, D>,
) -> Result<usize, ()> {
  let count = result.sorted_set_dict.len();
  store_dest_cold_common(storage, dst, result, window).await?;
  Ok(count)
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

/// 有序集合命令统一慢路径分派（ZSCAN 走 shared 慢路径扫描）
///
/// `block` 为阻塞族（BZPOPMIN/BZPOPMAX/BZMPOP）等待面（复用列表族单源，
/// None = 经纪未注入的独立会话域），全键未取到时内联等待出件/超时
pub(crate) async fn sorted_set(
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

  // 分层快速通道（骨架单点收口 try_tiered_arm：探测/WRONGTYPE 门/穿透）
  // ZRANGE 族 / ZLEXCOUNT / ZCOUNT / ZRANK 族经 arg12 通道下同原生臂：范围、
  // 计数与排名语义在树内流式求值（wcol 参数解析与结果负载单源复用），不再走
  // 装载型物化通道
  // ZREM 不入表：删除重命令与 ZPOPMIN / ZREMRANGE* 同径走对象层通道（物化求值
  // + 整值重灌），杜绝向分层树逐成员落删除墓碑（见 tiered_collection_ops 头注）
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
    RespCommand::Zcount => Some((SortedSetOperation::Zcount, (0, 0))),
    // ZRANK / ZREVRANK 的 WITHSCORE 位经族内单源推导（与下方内存臂同一函数，
    // 失败帧已写出即短路，杜绝第二套词元判定）
    RespCommand::Zrank | RespCommand::Zrevrank => {
      let op = if matches!(cmd, RespCommand::Zrank) {
        SortedSetOperation::Zrank
      } else {
        SortedSetOperation::Zrevrank
      };
      let Some(with_score) = parse_rank_with_score(cmd.into(), refs, output) else {
        return Ok(());
      };
      Some((op, (i32::from(with_score), 0)))
    }
    _ => None,
  };
  if try_tiered_arm(
    storage,
    key,
    GarnetObjectType::SortedSet,
    op_opt,
    |(op, _)| zset_needs_write(*op),
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
      if handled && cmd == RespCommand::Zadd {
        // 写命令唤醒该键的阻塞观察者（对标同步段 ZADD 后 HandleCollectionUpdate）；
        // 仅 Zadd 单义（票 zcode-r15-zset 发现三）：C# SortedSetIncrement 走 RMW
        // 无经纪通知，内存态快路径与慢路径 rmw_spec 臂同口径，ZINCRBY 在分层态
        // 不再单态独走唤醒
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
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some(args) = parse_expire_elements_args(
      cmd.into(),
      refs,
      ElementHeaderKind::Members,
      is_ms,
      is_ts,
      output,
    ) else {
      return Ok(());
    };
    zset_rmw_cold(
      storage,
      args.key,
      SortedSetOperation::Zexpire,
      args.elements,
      args.args12,
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
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, members)) =
      parse_elements_only_args(cmd.into(), refs, ElementHeaderKind::Members, output)
    else {
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
          write_lex_result(done.result1, output);
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
        |output| output.write_resp_array_len(0),
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          run_operate(
            obj,
            SortedSetOperation::Zrange,
            args,
            0,
            opts.bits() as i32,
            resp_version,
            output,
          );
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
          run_operate(
            obj,
            SortedSetOperation::Zmscore,
            args,
            0,
            0,
            resp_version,
            output,
          );
        },
      )
      .await
    }
    RespCommand::Zcount => {
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        // C# NOTFOUND 直写 :0（键缺席 min/max 不校验，与快路径同判定序）
        |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          run_operate(
            obj,
            SortedSetOperation::Zcount,
            args,
            0,
            0,
            resp_version,
            output,
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
          let result1 = run_operate(
            obj,
            SortedSetOperation::Zlexcount,
            args,
            0,
            0,
            resp_version,
            output,
          )
          .result1;
          write_lex_result(result1, output);
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
      // WITHSCORE 词元推导单源（快慢共用，失败帧已写出）
      let Some(with_score) = parse_rank_with_score(cmd.into(), refs, output) else {
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
          run_operate(
            obj,
            op,
            &[member],
            i32::from(with_score),
            0,
            resp_version,
            output,
          );
        },
      )
      .await
    }
    RespCommand::Zrandmember => {
      // 参数推导单源（快慢共用，失败帧已写出；arg1 打包
      // (count << 1 | includedCount) << 1 | withScores）
      let Some(args) = parse_random_member_args("ZRANDMEMBER", refs, cs::WITHSCORES, output) else {
        return Ok(());
      };
      // count 为 0 不触达后端（应答与缺失态同形单源）
      if args.param_count == 0 {
        write_random_member_missing(output, args.included_count, resp_version);
        return Ok(());
      }
      let arg1 = args.arg1;
      let included_count = args.included_count;
      slow_load_eval(
        storage,
        key,
        GarnetObjectType::SortedSet,
        output,
        SortedSetObject::from_blob,
        move |output: &mut Vec<u8>| {
          write_random_member_missing(output, included_count, resp_version);
        },
        async move |obj: &mut SortedSetObject, output: &mut Vec<u8>| {
          run_operate(
            obj,
            SortedSetOperation::Zrandmember,
            &[],
            arg1,
            fastrand::i32(..),
            resp_version,
            output,
          );
        },
      )
      .await
    }
    RespCommand::Zrangestore => zrangestore_cold(storage, notify, refs, resp_version, output).await,
    RespCommand::Zdiff => {
      let Some((keys, with_scores)) = parse_diff_args(refs, "ZDIFF", true, output) else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, keys, output).await? else {
        return Ok(());
      };
      let result = diff_sets(&objs);
      write_zset_entries(Some(&result), with_scores, output, resp_version);
      Ok(())
    }
    RespCommand::Zdiffstore => {
      let dst = key;
      let Some((keys, _)) =
        parse_diff_args(refs.get(1..).unwrap_or(&[]), "ZDIFFSTORE", false, output)
      else {
        return Ok(());
      };
      zdiffstore_cold(storage, notify, dst, keys, output).await
    }
    RespCommand::Zinter | RespCommand::Zunion => {
      let (name, kind) = if cmd == RespCommand::Zinter {
        ("ZINTER", CombineKind::Intersect)
      } else {
        ("ZUNION", CombineKind::Union)
      };
      let Some(parsed) = parse_combine_args(refs, name, true, output) else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, parsed.keys, output).await? else {
        return Ok(());
      };
      let result = combine_sets(&objs, &parsed.weights, parsed.aggregate, kind);
      write_zset_entries(Some(&result), parsed.with_scores, output, resp_version);
      Ok(())
    }
    RespCommand::Zinterstore | RespCommand::Zunionstore => {
      let (name, kind) = if cmd == RespCommand::Zinterstore {
        ("ZINTERSTORE", CombineKind::Intersect)
      } else {
        ("ZUNIONSTORE", CombineKind::Union)
      };
      let Some(parsed) = parse_combine_args(refs.get(1..).unwrap_or(&[]), name, false, output)
      else {
        return Ok(());
      };
      let Some(objs) = load_many_cold(storage, parsed.keys, output).await? else {
        return Ok(());
      };
      let result = combine_sets(&objs, &parsed.weights, parsed.aggregate, kind);
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let count = store_dest_cold(storage, key, &result, window).await?;
      output.write_resp_int(count as i64);
      notify(key);
      Ok(())
    }
    RespCommand::Zintercard => {
      // 参数推导单源（快慢共用，失败帧已写出）
      let Some(args) = parse_intersect_card_args(IntersectCardKind::SortedSet, refs, output) else {
        return Ok(());
      };
      let Some(mut objs) = load_many_cold(storage, args.keys, output).await? else {
        return Ok(());
      };
      // 基数内核与快路径同单源（最小集遍历、命中 limit 提前跳出）
      output.write_resp_int(intersect_card(&mut objs, args.limit));
      Ok(())
    }
    RespCommand::Zmpop | RespCommand::Bzmpop => {
      // 参数推导单源（快慢共用，失败帧已写出；BZMPOP 的 timeout 词元由
      // 快路径先行校验，冷键降级时不再复检）
      let Some((keys, low_scores_first, count)) =
        parse_zmpop_args(refs, cmd == RespCommand::Bzmpop, output)
      else {
        return Ok(());
      };
      let handled =
        zset_pop_first_nonempty_cold(storage, keys, low_scores_first, count, output, resp_version)
          .await?;
      if !handled {
        // ZMPOP NOTFOUND → WriteNull（非阻塞语义恒立即回）；BZMPOP 未取到 →
        // 经等待面闭环（cmd_args = [lowScoresFirst(1B), popCount(i32 LE 4B)]，
        // 与快路径 park 同编码；C# SortedSetBlockingMPop 尾部 BlockingWait
        // 同位），出件/超时经应答单源出帧；此时装载窗均已随冷臂循环作用域
        // 释放（对照列表族等待前 drop 双窗纪律），唤醒方写入不被窗死锁；
        // 经纪未注入域回空值
        if cmd == RespCommand::Zmpop {
          output.write_resp_null_ver(resp_version);
        } else if let Some((face, timeout)) = block.zip(
          refs
            .first()
            .copied()
            .and_then(|t| try_get_timeout_bytes(t).ok()),
        ) {
          face
            .wait(
              cmd,
              timeout,
              keys.iter().map(|k| k.to_vec()).collect(),
              vec![
                vec![u8::from(low_scores_first)],
                count.to_le_bytes().to_vec(),
              ],
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
    RespCommand::Bzpopmin | RespCommand::Bzpopmax => {
      // 阻塞族冷臂：逐键立即可取（对位同步段立即可取路径）
      let is_max = cmd == RespCommand::Bzpopmax;
      let keys = &refs[..refs.len().saturating_sub(1)];
      for k in keys {
        // 装载型取件臂双保护·异步档（对照 blpop 冷臂形态）：逐键装载前取 rmw 窗
        // 跨「装载 → 弹出 → 写回」全程，窗预算耗尽按存储忙上抛（fail-closed）
        let _window = storage.batch.rmw_window(k).await.map_err(|_| ())?;
        let (mut obj, swap_in_window) =
          match load_sealed_tri::<SortedSetObject, _>(storage, k, output).await? {
            // WRONGTYPE：错误帧已由装载底层写出，终结应答，严禁 continue
            None => return Ok(()),
            // MISSING：键不存在，继续扫描下一键
            Some(None) => continue,
            Some(Some(loaded)) => loaded,
          };
        let Some((score, member)) = obj.pop_min_or_max(is_max) else {
          // 删空自愈（对标 C# SortedSetObject.Operate :452-453 REMOVE_KEY）：
          // 弹出落空但全成员已到期剔空时，须写删空墓碑清退幽灵空键
          if obj.mutated_by_ttl() {
            zset_save_or_gc_local(storage, &obj, k, swap_in_window.is_some(), true).await?;
          }
          continue;
        };
        if zset_save_or_gc_local(storage, &obj, k, swap_in_window.is_some(), true)
          .await
          .is_err()
        {
          return Err(());
        }
        output.write_resp_array_len(3);
        output.write_resp_bulk_string(k);
        output.write_resp_bulk_string(&member);
        // 分值版本分派（C# SortedSetBlockingPop :1619 WriteDoubleNumeric）
        cs::write_double_numeric(output, score, resp_version);
        return Ok(());
      }
      // 全键未取到：经纪注入域等待闭环（timeout 词元快路径已校验；C#
      // SortedSetBlockingPop 尾部 BlockingWait 的键态解耦语义，cmd_args 空
      // 表与快路径 park 同编码，出件/超时经应答单源出帧）；未注入域回
      // C# !result.Found 同款空值（会话版本分派，RESP3 为 `_\r\n`）。
      // 逐键装载窗均已随循环作用域释放，唤醒方写入不被窗死锁
      if let Some((face, timeout)) = block.zip(
        refs
          .last()
          .copied()
          .and_then(|t| try_get_timeout_bytes(t).ok()),
      ) {
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
        output.write_resp_null_ver(resp_version);
      }
      Ok(())
    }
    _ => {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// ZDIFFSTORE 慢路径（first − rest 折叠后落目标键）
async fn zdiffstore_cold(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  dst: &[u8],
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some(objs) = load_many_cold(storage, keys, output).await? else {
    return Ok(());
  };
  let result = diff_sets(&objs);
  let window = storage.batch.rmw_window(dst).await.map_err(|_| ())?;
  let count = store_dest_cold(storage, dst, &result, window).await?;
  output.write_resp_int(count as i64);
  notify(dst);
  Ok(())
}

/// ZRANGESTORE 慢路径对位（读源范围 → 成对负载落目标键）
async fn zrangestore_cold(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let dst_key = refs.first().copied().unwrap_or(&[]);
  let src_key = refs.get(1).copied().unwrap_or(&[]);
  let range_args = refs.get(2..).unwrap_or(&[]);

  // 源缺失：目标回收（空结果删目标键，对齐 Redis ZRANGESTORE 语义）后回 :0
  let mut src_obj = match load_sealed_tri::<SortedSetObject, _>(storage, src_key, output).await? {
    Some(None) => {
      let window = storage.batch.rmw_window(dst_key).await.map_err(|_| ())?;
      store_dest_cold(storage, dst_key, &SortedSetObject::new(), window).await?;
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(());
    }
    // 源键只读（结果仅落 dst），封窗守卫解构即释
    Some(Some((o, _swap_in_window))) => o,
    None => return Ok(()),
  };
  // 解析消费非回显：负载挂本地 sink（错误臂冷路径透传一次）
  let mut sink = Vec::new();
  let result1 = run_operate(
    &mut src_obj,
    SortedSetOperation::Zrange,
    range_args,
    0,
    SortedSetRangeOpts::STORE.bits() as i32,
    resp_version,
    &mut sink,
  )
  .result1;
  // result1 = -1 表示范围参数被拒（错误已写入负载）
  if result1 == -1 {
    output.append(&mut sink);
    return Ok(());
  }
  let dst = SortedSetObject::from_entries(parse_pairs_payload(&sink));
  let window = storage.batch.rmw_window(dst_key).await.map_err(|_| ())?;
  let count = store_dest_cold(storage, dst_key, &dst, window).await?;
  output.write_resp_int(count as i64);
  notify(dst_key);
  Ok(())
}

/// 删空回收或信封写回（对标 sync zset_save_or_gc 的异步臂；分层感知）：
/// 非封窗（信封域）落笔前先按装载态复验域归属（票 load-type-rmw-window 异步档
/// zset 留尾，与 [`run_async_rmw`] 落笔复验同核、list 侧 `save_or_gc` 同形），
/// 窗内 DEL/SET 交叠即 `Err(())` 按存储忙拒写；封窗臂（SwapInWindowGuard）
/// 物化语义域已钉死，免复验
async fn zset_save_or_gc_local(
  storage: &StorageSession<'_, impl Device>,
  obj: &SortedSetObject,
  key: &[u8],
  sealed: bool,
  existed: bool,
) -> Result<(), ()> {
  if !sealed {
    obj_writeback_recheck_async(storage, key, existed).await?;
  }
  obj_writeback_tiered(storage, key, GarnetObjectType::SortedSet, obj, sealed).await
}

/// 逐键弹出第一个非空有序集合的慢路径对位（对标同步段 zset_pop_first_nonempty 的
/// Some(true)/None 分派，C# SortedSetMPop 遇 WRONGTYPE 立即终止不续扫）
///
/// 返回 `Ok(true)` = 已生成终结性应答（WRONGTYPE 错误行写出，或命中并写出
/// [key, [member, score], ...] 帧），调用方不得追加任何空值应答；
/// `Ok(false)` = 全部键缺失或空集合，由调用方补写 null 形态应答
async fn zset_pop_first_nonempty_cold(
  storage: &StorageSession<'_, impl Device>,
  keys: &[&[u8]],
  low_scores_first: bool,
  pop_count: i32,
  output: &mut Vec<u8>,
  resp_version: u8,
) -> Result<bool, ()> {
  for key in keys {
    // 装载型取件臂双保护·异步档（与 BZPOPMIN 冷臂同规格，对照 pop_first_nonempty_cold
    // 的 list 形态）：逐键装载前取 rmw 窗跨「装载 → 弹出 → 写回」全程
    let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
    let (mut obj, swap_in_window) =
      match load_sealed_tri::<SortedSetObject, _>(storage, key, output).await? {
        // WRONGTYPE：错误帧已写出，返回 true 告知调用方应答已终结
        None => return Ok(true),
        // MISSING：键不存在，继续扫描下一键
        Some(None) => continue,
        Some(Some(loaded)) => loaded,
      };
    if obj.purge_expired_len() == 0 {
      // 删空自愈（对标 C# SortedSetObject.Operate :452-453 REMOVE_KEY）：
      // 全成员到期剔空时须物理写删空墓碑清退幽灵空键，慢路径异步臂同规格补齐
      if obj.mutated_by_ttl() {
        zset_save_or_gc_local(storage, &obj, key, swap_in_window.is_some(), true).await?;
      }
      continue;
    }
    let popped = pop_up_to(&mut obj, !low_scores_first, pop_count);
    zset_save_or_gc_local(storage, &obj, key, swap_in_window.is_some(), true).await?;
    // 回复：[key, [member, score], ...]（与同步段 write_popped_pairs 单源）
    write_popped_pairs(key, &popped, output, resp_version);
    return Ok(true);
  }
  Ok(false)
}
