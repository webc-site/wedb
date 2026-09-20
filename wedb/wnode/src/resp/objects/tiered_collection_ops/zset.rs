use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
  mem::swap,
  ops::Range,
};

use wbase::{num::strict_f64, time::now_ticks};
use wbftree::{BfTreeReadResult, BfTreeService, ScanReturnField};
use wcol::{
  types::member_ttl::{decode_member, member_expired_at},
  zset::{
    comparer::SortedSetComparer,
    sorted_set_object::{SortedSetEntry, SortedSetObject, SortedSetOperation, SortedSetRangeOpts},
    sorted_set_object_impl::{
      RangeArgError, SpecialRanges, parse_range_options, write_sorted_set_result_payload,
    },
  },
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::common::{
  TieredCollectionArgs, TieredCtx, TieredMirror, emit_tiered_mirror, finish_tiered_arm,
  save_tiered_meta, scan_count, tiered_count, tiered_guard, tiered_precheck, tiered_write_mirror,
  tree_member_state, tree_put_ok, tree_put_rejected,
};

/// 有序集合族写面判定（一处定义）：树内稳态写命令取独占写锁；纯读
///（Zscore/Zmscore）、计数臂 Zcard（稳态共享读锁 O(1) 直读，水位越过经
/// [`tiered_count`] 升级写锁出账）、ZREM、ZEXPIRE / ZTTL / ZPERSIST 与其余
/// 穿透臂维持共享读锁（经物化降级整值重灌）
fn zset_needs_write(op: SortedSetOperation) -> bool {
  matches!(op, SortedSetOperation::Zadd | SortedSetOperation::Zincrby)
}

/// 有序集合族树内稳态写命令判定（AOF 命令镜像面一处定义，刻意窄于
/// [`zset_needs_write`] 锁面）：仅写语义命令产镜像事件；Zcard 是读计数臂，
/// 水位越过出账的树变更已由 promote 重灌流整树镜像，命令本身无写语义
/// 不产命令记录
fn tiered_zset_writes(op: SortedSetOperation) -> bool {
  matches!(op, SortedSetOperation::Zadd | SortedSetOperation::Zincrby)
}

/// 执行分层态有序集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_zset<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let frame_base = output.len();
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  let mirror = tiered_write_mirror(
    GarnetObjectType::SortedSet,
    op,
    args12,
    args,
    tiered_zset_writes,
  );
  let handled = tiered_zset_arm(
    session,
    key,
    ctx,
    TieredCollectionArgs::new(op, args12, args, resp_protocol_version),
    output,
    frame_base,
    mirror,
  )
  .await;
  let res = finish_tiered_arm(session, key, ctx, handled);
  if res.is_err() {
    output.truncate(frame_base);
  }
  res
}

/// 分层态有序集合命令树内主体（读写臂分派，见 [`exec_tiered_zset`]）
async fn tiered_zset_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
  frame_base: usize,
  mirror: Option<TieredMirror<'_>>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    // arg1 为 ZRANK / ZREVRANK 的 WITHSCORE 位、arg2 为 ZRANGE 族选项位（见下方
    // 范围与排名臂）；成员级 TTL 面（ZEXPIRE 族）已穿透物化降级，其压缩字仍由
    // run_async_rmw 的 run_op 闭包捕获透传对象层，本臂不再消费
    args12,
    args,
    resp_protocol_version,
  } = call;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, zset_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      SortedSetOperation::Zadd => {
        use wresp::options::{SortedSetAddOption, try_get_sorted_set_add_option};

        // ---- GetOptions 选项段（
        // C# SortedSetObjectImpl.GetOptions：XX&NX 互斥、NX/GT/LT 两两互斥、
        // INCR 仅单对、剩余 token 非空且成对）
        // 尾段检查经 vendored C# 行实核对为 1:1 同在（agy r6-data 条 4 复核：
        // SortedSetObjectImpl.cs:80-87，upstream #644 起）——选项后剩余段为空/
        // 奇数（如 XX + 单成员）在 C# 同回 syntax error（对齐 Redis 语义；RESP
        // 层仅选项形态先被 ZADD arity -4 门拦下，两实现同表），撤销反而与
        // C#、Redis 及信封对象层三方分叉，故保留
        let mut options = SortedSetAddOption::NONE;
        let mut curr = 0usize;
        while curr < args.len()
          && let Some(opt) = try_get_sorted_set_add_option(args[curr])
        {
          options |= opt;
          curr += 1;
        }
        let options_error: Option<&'static str> =
          if options.contains(SortedSetAddOption::XX) && options.contains(SortedSetAddOption::NX) {
            Some(cs::RESP_ERR_XX_NX_NOT_COMPATIBLE)
          } else if (options.contains(SortedSetAddOption::GT)
            && options.contains(SortedSetAddOption::LT))
            || ((options.contains(SortedSetAddOption::GT)
              || options.contains(SortedSetAddOption::LT))
              && options.contains(SortedSetAddOption::NX))
          {
            Some(cs::RESP_ERR_GT_LT_NX_NOT_COMPATIBLE)
          } else if options.contains(SortedSetAddOption::INCR) && args.len() - curr > 2 {
            Some(cs::RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR)
          } else {
            None
          };
        if let Some(err) = options_error {
          cs::write_error_raw(output, err);
          break 'arm Ok(true);
        }
        if curr == args.len() || !(args.len() - curr).is_multiple_of(2) {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          break 'arm Ok(true);
        }

        // 预校验先于任何写入（RI 批量口径：任一成员越契约即整体失败、零树内副作用；
        // (score, member) 成对排布，成员即 pair[1]，分值恒 8B 裸记录落树，成员长度
        // 决定契约；分值解析失败仍由主循环保有原口径）
        for pair in args[curr..].as_chunks::<2>().0 {
          if !tiered_precheck(ctx, pair[1], size_of::<f64>(), None, output) {
            break 'arm Ok(true);
          }
        }

        // ---- SortedSetAdd 主循环（C# SortedSetObjectImpl.SortedSetAdd：
        // 新增恒计数 / CH 计变更 / NX 过滤已存在 / GT/LT 分值比较 /
        // INCR 增量回 bulk string）；「插成功才计数」判据统一走 tree_put_ok
        let mut added_or_changed = 0_i64;
        let mut incr_result = 0_f64;
        let mut new_members = 0u64;
        let mut put_rejected = false;
        let now = now_ticks();
        // ZADD 收尾记账单点：新成员计数入账与元记录回写一处判定，提前出口与正常
        // 出口共用——保证任一出口处「计数与树内实存一一对应」（解析/NaN 等命令
        // 中途错误提前回包时，已落树的成员计数不得随早退丢失，否则 size 虚减
        // 反噬删空自愈判据）
        macro_rules! commit_new_members {
          () => {
            if new_members > 0 {
              ctx.meta.size += new_members;
              // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见
              // save_tiered_meta），早退臂已落树的成员计数不随失败丢失
              if save_tiered_meta(session, key, ctx).await.is_err() {
                break 'arm Err(());
              }
            }
          };
        }
        while curr < args.len() {
          let Some(score) = strict_f64(args[curr], true) else {
            commit_new_members!();
            cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
            break 'arm Ok(true);
          };
          curr += 1;
          let member = args[curr];
          curr += 1;

          // 到期成员视同不存在（C# SortedSetAdd 入口先 DeleteExpiredItems）；
          // 物理覆盖对到期旧记录等价先删后加（size 不变），对存活旧记录任一
          // 写入分支都清字段 TTL（C# TryRemoveExpiration，含同分值分支）
          let state = tree_member_state(tree, member, now);
          let mut alive_score = None;
          if matches!(state, Some((_, false))) {
            tree.read_callback(member, |res, raw| {
              if res == BfTreeReadResult::Found {
                let (_, payload) = decode_member(raw);
                if payload.len() == 8
                  && let Ok(arr) = <[u8; 8]>::try_from(payload)
                {
                  alive_score = Some(f64::from_be_bytes(arr));
                }
              }
              res == BfTreeReadResult::Found
            });
          }

          match alive_score {
            None => {
              // 真新成员 / 已到期旧记录：XX 置位则不新增（到期视同不存在）
              if options.contains(SortedSetAddOption::XX) {
                continue;
              }
              incr_result = score;
              if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
                if state.is_none() {
                  new_members += 1;
                }
                added_or_changed += 1;
              } else {
                put_rejected = true;
              }
            }
            Some(old_score) => {
              let mut score = score;
              if options.contains(SortedSetAddOption::INCR) {
                score += old_score;
                incr_result = score;
                if score.is_nan() {
                  commit_new_members!();
                  cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SCORE_NAN);
                  break 'arm Ok(true);
                }
              }
              if score == old_score {
                // 同分值：仅清 TTL（零计数变更，C# 相同分支 TryRemoveExpiration）
                if !tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
                  put_rejected = true;
                }
                continue;
              }
              // NX 置位，或 GT/LT 置位且现存分值高于/低于新分值 → 不更新
              if options.contains(SortedSetAddOption::NX)
                || (options.contains(SortedSetAddOption::GT) && old_score > score)
                || (options.contains(SortedSetAddOption::LT) && old_score < score)
              {
                if options.contains(SortedSetAddOption::INCR) {
                  commit_new_members!();
                  output.write_resp_null_ver(resp_protocol_version);
                  break 'arm Ok(true);
                }
                continue;
              }
              if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
                if options.contains(SortedSetAddOption::CH) {
                  added_or_changed += 1;
                }
              } else {
                put_rejected = true;
              }
            }
          }
        }

        commit_new_members!();
        if put_rejected {
          tree_put_rejected(output);
          break 'arm Ok(true);
        }
        if options.contains(SortedSetAddOption::INCR) {
          // 分值数值（对标 C# SortedSetObjectImpl.SortedSetAdd INCR 分支
          // WriteDoubleNumeric，RESP3 `,val`、RESP2 bulk）
          cs::write_double_numeric(output, incr_result, resp_protocol_version);
        } else {
          output.write_resp_int(added_or_changed);
        }
        Ok(true)
      }

      SortedSetOperation::Zscore => {
        if args.is_empty() {
          break 'arm Err(());
        }
        let member = args[0];
        let now = now_ticks();
        let mut score_opt = None;
        tree.read_callback(member, |res, raw| {
          if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
            let (_, payload) = decode_member(raw);
            if payload.len() == 8
              && let Ok(arr) = <[u8; 8]>::try_from(payload)
            {
              score_opt = Some(f64::from_be_bytes(arr));
            }
          }
          res == BfTreeReadResult::Found
        });
        if let Some(score) = score_opt {
          cs::write_double_numeric(output, score, resp_protocol_version);
        } else {
          output.write_resp_null_ver(resp_protocol_version);
        }
        Ok(true)
      }

      SortedSetOperation::Zmscore => {
        let now = now_ticks();
        output.write_resp_array_len(args.len());
        for &member in args {
          let mut score_opt = None;
          tree.read_callback(member, |res, raw| {
            if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
              let (_, payload) = decode_member(raw);
              if payload.len() == 8
                && let Ok(arr) = <[u8; 8]>::try_from(payload)
              {
                score_opt = Some(f64::from_be_bytes(arr));
              }
            }
            res == BfTreeReadResult::Found
          });
          if let Some(score) = score_opt {
            cs::write_double_numeric(output, score, resp_protocol_version);
          } else {
            output.write_resp_null_ver(resp_protocol_version);
          }
        }
        Ok(true)
      }

      SortedSetOperation::Zcard => {
        // 计数（同分层 Hlen 臂，共 [`tiered_count`] 内核）：稳态水位内共享读锁
        // 锁内重读元记录 O(1) 直读 size；水位越过升级写锁物理出账（树内零墓碑，
        // 有到期才重灌），O(N) 每到期纪元至多一次
        let size = tiered_count(session, key, ctx, tree_guard).await?;
        output.write_resp_int(size as i64);
        Ok(true)
      }

      SortedSetOperation::Zincrby => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        // 入参增量解析失败（进树前前置校验）：C# SortedSetIncrement 的
        // parseState.TryGetDouble（canBeInfinite 默认 true，±inf 词形合法放行）
        // 失败 → RESP_ERR_NOT_VALID_FLOAT（libs/server/Objects/SortedSet/
        // SortedSetObjectImpl.cs:331-334，信封层 sorted_set_increment 同面），
        // 严禁折叠成慢路径存储错误与信封臂分叉
        let Some(incr) = strict_f64(args[0], true) else {
          cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          break 'arm Ok(true);
        };
        let member = args[1];
        let now = now_ticks();
        let mut cur_score = 0.0f64;
        let mut is_new = true;
        // 存活成员增量保留既有 TTL（C# SortedSetIncrby 不动 expiration）；到期
        // 旧记录视同不存在（C# 增量入口先 DeleteExpiredItems）
        let mut old_expiry = None;
        // 到期命中独立标志（三态判据：真缺席 / 到期在树 / 存活，与 ZADD None
        // 分支收敛同一判据面，见 Zincrby 尾部记账文注）
        let mut expired_hit = false;
        tree.read_callback(member, |res, raw| {
          if res == BfTreeReadResult::Found {
            let (expiry, payload) = decode_member(raw);
            if member_expired_at(raw, now) {
              expired_hit = true;
              return true;
            }
            is_new = false;
            old_expiry = expiry;
            if payload.len() == 8
              && let Ok(arr) = <[u8; 8]>::try_from(payload)
            {
              cur_score = f64::from_be_bytes(arr);
            }
          }
          res == BfTreeReadResult::Found
        });
        let new_score = cur_score + incr;
        let score_record = new_score.to_be_bytes();
        // 预校验先于写入（RI 单点口径，零副作用）；编码记录长度随 TTL 头形态换算
        if !tiered_precheck(ctx, member, score_record.len(), old_expiry, output) {
          break 'arm Ok(true);
        }
        // 「插成功才计数」：写成才计新成员并入账回分值（C#
        // SortedSetObjectImpl.SortedSetIncrement 的等价判据）
        if !tree_put_ok(ctx, tree, member, &score_record, old_expiry) {
          tree_put_rejected(output);
          break 'arm Ok(true);
        }
        // 真缺席才计数（is_new 初值真；到期命中已由物理覆盖承接零计数——成员
        // 已在 size 中，+1 即永久虚增且覆盖后 sweep 不再数入出账。C# 净零由
        // 入口 DeleteExpiredItems 先摘后加承担，三态判据与 ZADD None 分支同面）
        if is_new && !expired_hit {
          ctx.meta.size += 1;
          // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
          if save_tiered_meta(session, key, ctx).await.is_err() {
            break 'arm Err(());
          }
        }
        // 分值数值（对标 C# SortedSetObjectImpl.SortedSetIncrement WriteDoubleNumeric）
        cs::write_double_numeric(output, new_score, resp_protocol_version);
        Ok(true)
      }

      // ZCOUNT 树内读臂（libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:
      // SortedSetCount）：单趟流式扫描计数，内存 O(1)（原经 slow_load_eval 全量
      // 物化 → 千万级集合一次 O(N) 内存抖动）
      SortedSetOperation::Zcount => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
          SortedSetObject::try_parse_parameter(args[0]),
          SortedSetObject::try_parse_parameter(args[1]),
        ) else {
          cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
          break 'arm Ok(true);
        };
        let bounds = ZScoreBounds {
          min: min_value,
          min_excl: min_exclusive,
          max: max_value,
          max_excl: max_exclusive,
        };
        let scan = zset_scan_select(tree, now_ticks(), ZSetWindow::Count, |score, _| {
          bounds.pass(score)
        })?;
        // C# 外层守卫 `minValue <= sortedSet.Max.Score`：raw `<=` 且 Max **含到期
        // 成员**（对象层该臂不先 DeleteExpiredItems），NaN 面唯一差异处
        // （min 为 NaN 时守卫短路为 0，谓词本身会把 NaN 成员计入）
        let count = match scan.last_score {
          Some(last) if min_value <= last => scan.matched,
          _ => 0,
        };
        output.write_resp_int(count as i64);
        Ok(true)
      }

      // ZLEXCOUNT 树内读臂（C# SortedSetObjectImpl.SortedSetRemoveOrCountRangeByLex
      // 的 ZLEXCOUNT 分支：`GetElementsInRangeByLex(min, max, false, false, (0,0))`
      // 命中数，零删除）
      SortedSetOperation::Zlexcount => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        let Some(bounds) = ZLexBounds::parse(args[0], args[1], false) else {
          // 对象层由调用方按 result1 == int.MaxValue 回同一错误文本
          cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
          break 'arm Ok(true);
        };
        let (_picked, _range, matched) = zset_lex_select(
          tree,
          now_ticks(),
          bounds,
          ZSetWindow::Count,
          false,
          0,
          usize::MAX,
        )?;
        output.write_resp_int(matched as i64);
        Ok(true)
      }

      // ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX /
      // ZREVRANGEBYLEX 树内读臂（arg2 = SortedSetRangeOpts 位，与对象层 run_operate
      // 同约定；C# SortedSetObjectImpl.SortedSetRange 三形态逐臂对位）：
      // 参数段解析与结果负载均复用 wcol 单源函数，选择走 [`zset_scan_select`]
      // 流式内核，内存只随窗口增长，不再全量物化
      SortedSetOperation::Zrange => {
        let range_opts = SortedSetRangeOpts::from_bits_truncate(args12.1 as u8);
        let (options, resp_ver) = match parse_range_options(args, range_opts, resp_protocol_version)
        {
          Ok(v) => v,
          // 命令层 arity 保证 min/max 两段 → Incomplete 不可达；分层侧不得输出
          // 半帧（对象层该臂零输出回包），防御性走存储错误面
          Err(RangeArgError::Incomplete) => break 'arm Err(()),
          Err(err) => {
            err.write_reply(output);
            break 'arm Ok(true);
          }
        };
        let (min_span, max_span) = (args[0], args[1]);
        let now = now_ticks();

        // ---- 区间块 1：byIndex 或 byScore（C# 同段进入条件 !byLex || byScore）
        if (!options.by_score && !options.by_lex) || options.by_score {
          let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
            SortedSetObject::try_parse_parameter(min_span),
            SortedSetObject::try_parse_parameter(max_span),
          ) else {
            cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
            break 'arm Ok(true);
          };
          let (picked, range) = if options.by_score {
            let mut bounds = ZScoreBounds {
              min: min_value,
              min_excl: min_exclusive,
              max: max_value,
              max_excl: max_exclusive,
            };
            // C# do_reverse 先交换边界再正序扫、后 reverse 输出（等价于按窗口
            // 方向直接取序最大 k 条）
            if options.reverse {
              swap(&mut bounds.min, &mut bounds.max);
              swap(&mut bounds.min_excl, &mut bounds.max_excl);
            }
            let (window, off, take) =
              zset_limit_window(options.reverse, options.valid_limit, options.limit);
            let scan = zset_scan_select(tree, now, window, |score, _| bounds.pass(score))?;
            zset_windowed_pick(scan.picked, window, options.reverse, off, take)
          } else {
            // byIndex：LIMIT 不支持（C# 同臂），负索引归一与钳制以存活总数为
            // 基准，故除 `0 -1` 全量快速路径外需先走一趟计数
            if options.valid_limit {
              cs::write_error_raw(output, cs::RESP_ERR_LIMIT_NOT_SUPPORTED);
              break 'arm Ok(true);
            }
            if min_value == 0.0 && max_value == -1.0 {
              let scan = zset_scan_select(tree, now, ZSetWindow::All, |_, _| true)?;
              zset_windowed_pick(scan.picked, ZSetWindow::All, options.reverse, 0, usize::MAX)
            } else {
              let set_count = zset_scan_select(tree, now, ZSetWindow::Count, |_, _| true)?.alive;
              if min_value > (set_count as f64) - 1.0 {
                (Vec::new(), 0..0)
              } else {
                let (mut min_index, mut max_index) = (min_value as i64, max_value as i64);
                if min_index < 0 {
                  min_index += set_count as i64;
                }
                if max_index < 0 {
                  max_index += set_count as i64;
                } else if max_index >= set_count as i64 {
                  max_index = set_count as i64 - 1;
                }
                if (min_index < 0 && max_index < 0) || min_index > max_index {
                  (Vec::new(), 0..0)
                } else {
                  let min_index = min_index.max(0);
                  let n = (max_index - min_index + 1) as usize;
                  let window = if options.reverse {
                    ZSetWindow::Tail(min_index as usize + n)
                  } else {
                    ZSetWindow::Head(min_index as usize + n)
                  };
                  let scan = zset_scan_select(tree, now, window, |_, _| true)?;
                  zset_windowed_pick(scan.picked, window, options.reverse, min_index as usize, n)
                }
              }
            }
          };
          write_sorted_set_result_payload(
            output,
            options.with_scores,
            range.len(),
            resp_ver,
            picked[range].iter().map(|e| (e.score, e.member.as_slice())),
          );
        }

        // ---- 区间块 2：byLex（与块 1 相互独立，BYSCORE+BYLEX 并置时 C# 写两份
        // 回复；本块解析失败须回退本命令负载起点重写错误，对标 writer.ResetPosition）
        if options.by_lex {
          let out = match ZLexBounds::parse(min_span, max_span, options.reverse) {
            Some(bounds) => {
              let (window, off, take) =
                zset_limit_window(options.reverse, options.valid_limit, options.limit);
              match zset_lex_select(tree, now, bounds, window, options.reverse, off, take) {
                Ok(out) => out,
                Err(()) => {
                  output.truncate(frame_base);
                  cs::write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
                  break 'arm Ok(true);
                }
              }
            }
            None => {
              output.truncate(frame_base);
              cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
              break 'arm Ok(true);
            }
          };
          let (picked, range, _) = out;
          write_sorted_set_result_payload(
            output,
            options.with_scores,
            range.len(),
            resp_ver,
            picked[range].iter().map(|e| (e.score, e.member.as_slice())),
          );
        }
        Ok(true)
      }

      // ZRANK / ZREVRANK 树内读臂（C# SortedSetObjectImpl.SortedSetRank）：
      // arg1 == 1 附带分值；名次 = 序在目标之前的存活成员数，反向以
      // `存活总数 - 名次 - 1` 换算（C# Count() 同基准，两侧皆不含到期成员）
      SortedSetOperation::Zrank | SortedSetOperation::Zrevrank => {
        let with_score = args12.0 == 1;
        // 与冷路径同口径取成员（命令层保证段数，缺失防御性按空成员）
        let member = args.first().copied().unwrap_or(&[]);
        let now = now_ticks();
        // C# TryGetScore：到期成员视同不存在（先点读定位分值，再单趟流式计名次）；
        // 扫描 Err 与「成员不存在」分态：Err 上抛（应答尚未落帧），仅 None 回 null
        let Some(score) = tree_member_score(tree, member, now)? else {
          output.write_resp_null_ver(resp_protocol_version);
          break 'arm Ok(true);
        };
        let target = (score, member);
        let scan = zset_scan_select(tree, now, ZSetWindow::Count, |s, m| {
          SortedSetComparer::compare((&s, m), (&target.0, target.1)) == Ordering::Less
        })?;
        let mut rank = scan.matched as i64;
        if op == SortedSetOperation::Zrevrank {
          rank = scan.alive as i64 - rank - 1;
        }
        if with_score {
          output.write_resp_array_len(2);
          output.write_resp_int(rank);
          cs::write_double_numeric(output, score, resp_protocol_version);
        } else {
          output.write_resp_int(rank);
        }
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答——ZPOPMIN / ZPOPMAX / ZREMRANGEBY*
      // / GEOADD / ZRANGESTORE 等写族经此落 wcol 对象层单源真实删改，WATCH 推进
      // 同臂由 apply_rmw_post_operate 承接（旧兜底臂把它们应答成整表 ZRANGE 形态
      // 且零树删除，客户端见成功而数据未动，是比漏栅栏更重的语义缺陷）。
      // ZREM 与成员级 TTL 面（ZEXPIRE / ZTTL / ZPERSIST 族，原树内逐成员出账臂
      // 已删）同在此穿透：删除与到期出账一律走整值重灌，树内零墓碑（见模块头注）。
      // ZRANDMEMBER / ZDIFF / ZUNION / ZINTER / ZRANGESTORE 等多键与随机采样面
      // 亦维持物化通道（非本票射程，代价与限流口径见 doc/zh/collection.md）。
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾：树守卫存续窗口内入账（锁内 emit，AOF 序与树内提交
  // 序严格一致，见 emit_tiered_mirror 文注）；早退臂未走写漏斗不置脏自然跳过，
  // 落盘失败臂经 save_tiered_meta 以 break 'arm 落本收尾——镜像先行再上抛
  //（树已变更 ⟺ 镜像已入账，见 emit_tiered_mirror 头注不变式）
  emit_tiered_mirror(session, key, ctx, mirror);
  result
}

/// 分层 zset 树内流式留存窗口（[`zset_scan_select`] 的内存上界）
///
/// 判序单点复用 wcol [`SortedSetEntry`] 的 `Ord`（委托
/// [`SortedSetComparer`]，.NET `Double.CompareTo` 口径），与对象层内存
/// `SortedSet` 同序——树内只存 member → 分值，无分序索引，故「序」只能在
/// 扫描侧由同一比较器重建，严禁另立第二套排序结构。
#[derive(Debug, Clone, Copy)]
enum ZSetWindow {
  /// 全量留存后排序：应答本身即 O(N) 的形态（`ZRANGE k 0 -1`、无 LIMIT 的
  /// ZRANGEBYSCORE / ZRANGEBYLEX）
  All,
  /// 只留序最小 k 条，升序回（有界窗口正向；k == 1 即「序最小者」= 断点求解）
  Head(usize),
  /// 只留序最大 k 条，降序回（有界窗口 REV / 反向形态）
  Tail(usize),
  /// 不留存，仅计数（ZCOUNT / ZRANK / ZLEXCOUNT 与 byIndex 的存活计数趟）
  Count,
}

/// [`zset_scan_select`] 回传束
struct ZSetScanOut {
  /// 留存条目：Head/All 升序、Tail 降序、Count 恒空
  picked: Vec<SortedSetEntry>,
  /// 存活且命中谓词的条目数（Count 形态的应答值）
  matched: u64,
  /// 存活条目总数（与谓词无关：byIndex 的 `set_count` 与 ZREVRANK 的
  /// `Count - rank - 1` 同基准）
  alive: u64,
  /// 全树序最大条目的分值，**含到期成员**（对位对象层 `sorted_set.last()`：
  /// C# 该守卫不过滤到期，ZCOUNT 外层守卫唯一取值面）
  last_score: Option<f64>,
}

/// 分值判序单点（[`SortedSetComparer`] 的空成员回退臂，等价 .NET
/// `Double.CompareTo`：NaN 小于一切非 NaN，±0.0 相等）
#[inline]
fn zset_score_order(a: f64, b: f64) -> Ordering {
  const EMPTY: &[u8] = &[];
  SortedSetComparer::compare((&a, EMPTY), (&b, EMPTY))
}

/// 分值区间界（解析单点复用 wcol [`SortedSetObject::try_parse_parameter`]）
///
/// 谓词口径逐字对位 C# `GetElementsInRangeByScore` / `SortedSetCount` 循环：
/// 下界 = `GetViewBetween((minValue, null), …)` 哨兵的序裁剪（按 CompareTo，
/// 非 raw `>=`：±0.0 与 NaN 面不等价），上界 = 循环内 raw `>` / `==` 断点判据。
#[derive(Debug, Clone, Copy)]
struct ZScoreBounds {
  min: f64,
  min_excl: bool,
  max: f64,
  max_excl: bool,
}

impl ZScoreBounds {
  fn pass(&self, score: f64) -> bool {
    zset_score_order(score, self.min) != Ordering::Less
      && !(self.min_excl && score == self.min)
      && !(score > self.max || (self.max_excl && score == self.max))
  }
}

/// 字典序区间界（解析单点复用 wcol [`SortedSetObject::try_parse_lex_parameter`]，
/// REV 交换对位 C# `GetElementsInRangeByLex` 首段三换）
#[derive(Debug, Clone, Copy)]
struct ZLexBounds<'a> {
  min: &'a [u8],
  min_excl: bool,
  min_inf: SpecialRanges,
  max: &'a [u8],
  max_excl: bool,
  max_inf: SpecialRanges,
}

impl<'a> ZLexBounds<'a> {
  /// 两界解析 + REV 交换；任一界词形非法 → None（C# 以 i32::MAX 上抛）
  fn parse(min_span: &'a [u8], max_span: &'a [u8], reverse: bool) -> Option<Self> {
    let ((min, min_excl, min_inf), (max, max_excl, max_inf)) = (
      SortedSetObject::try_parse_lex_parameter(min_span)?,
      SortedSetObject::try_parse_lex_parameter(max_span)?,
    );
    Some(if reverse {
      Self {
        min: max,
        min_excl: max_excl,
        min_inf: max_inf,
        max: min,
        max_excl: min_excl,
        max_inf: min_inf,
      }
    } else {
      Self {
        min,
        min_excl,
        min_inf,
        max,
        max_excl,
        max_inf,
      }
    })
  }

  /// C# 早空判据：min 为 `+`、max 为 `-`
  fn always_empty(&self) -> bool {
    self.min_inf == SpecialRanges::InfiniteMax || self.max_inf == SpecialRanges::InfiniteMin
  }

  /// 下界过滤（成员字节序，对位 C# `SequenceCompareTo`；`-∞` 恒真）
  fn pass_min(&self, member: &[u8]) -> bool {
    if self.min_inf == SpecialRanges::InfiniteMin {
      return true;
    }
    let ord = member.cmp(self.min);
    !(ord == Ordering::Less || (ord == Ordering::Equal && self.min_excl))
  }

  /// 上界过滤（C# `take_while` 的判据面；`+∞` 恒真 ⇒ 不存在断点）
  fn pass_max(&self, member: &[u8]) -> bool {
    if self.max_inf == SpecialRanges::InfiniteMax {
      return true;
    }
    let ord = member.cmp(self.max);
    !(ord == Ordering::Greater || (ord == Ordering::Equal && self.max_excl))
  }
}

/// 分层 zset 树内单趟流式遍历内核：范围选择、区间计数、名次计数三族共用
///
/// 树内记录以 member 为键（member → 8B f64 大端分值 + 可选 TTL 头），本内核自
/// 树头一趟线性扫过全部记录，按 `(分值, 成员)` 序留存至多 `window` 条，
/// **内存随窗口增长而非随键基数增长**——这正是本票消除的「一条 ZRANGE 触发
/// 千万级成员全量反序列化 + 重建 `SortedSetObject`」。计数形态（[`ZSetWindow::
/// Count`]）内存 O(1)。
///
/// 到期成员只过滤不出产（C# 循环首臂 `IsExpired → continue`），本内核**不落
/// 任何删除记录**：维持「树内零墓碑」写形不变量（见本模块头注；物理出账归
/// ZCARD / ZCOLLECT 臂的 expire_sweep_or_rebuild，锁内单趟扫描 + 有到期才
/// 整值重灌，树内同样零墓碑），故本族读臂一律走共享读锁、
/// `dirty` 恒假、不推进 WATCH 栅栏。但 [`ZSetScanOut::last_score`] 含到期成员，
/// 与对象层 `sorted_set.last()` 同基准。
///
/// 栈深口径同 [`exec_tiered_scan`]：底层游标对墓碑的尾递归连跑不受本臂截断
/// 约束，安全性来自写形不变量而非本扫描的窗口。
///
/// 分值载荷非 8B = 编码损坏 → fail-fast `Err(())`（与 [`tiered_materialize_blob`]
/// 的 zset 臂同口径，严禁静默剔除成员后照常应答）。
fn zset_scan_select(
  tree: &BfTreeService,
  now: i64,
  window: ZSetWindow,
  mut pred: impl FnMut(f64, &[u8]) -> bool,
) -> Result<ZSetScanOut, ()> {
  let mut picked: Vec<SortedSetEntry> = Vec::new();
  // Head 用最大堆（超容量弹最大 ⇒ 恒留序最小 k 条），Tail 用反序堆（同构造
  // 对偶 ⇒ 恒留序最大 k 条，`into_sorted_vec` 即降序）
  let mut head: BinaryHeap<SortedSetEntry> = BinaryHeap::new();
  let mut tail: BinaryHeap<Reverse<SortedSetEntry>> = BinaryHeap::new();
  let mut matched = 0_u64;
  let mut alive = 0_u64;
  let mut last_score: Option<f64> = None;
  let mut corrupt = false;
  // 扫描 Err 经 [`scan_count`] 上抛（ZCOUNT/ZRANK/ZRANGE 各臂应答尚未落帧），
  // 严禁折叠——计数臂折叠会把存储故障答成 0 计数
  scan_count(tree.scan_with_count_callback(
    &[0u8],
    usize::MAX,
    ScanReturnField::KeyAndValue,
    |k, v| {
      let (expiry, payload) = decode_member(v);
      let Ok(arr) = <[u8; 8]>::try_from(payload) else {
        log::error!(
          "zset_scan_select: corrupted zset score payload, member='{}'",
          String::from_utf8_lossy(k)
        );
        corrupt = true;
        return false;
      };
      let score = f64::from_be_bytes(arr);
      if last_score.is_none_or(|cur| zset_score_order(score, cur) == Ordering::Greater) {
        last_score = Some(score);
      }
      if expiry.is_some_and(|ticks| ticks < now) {
        return true;
      }
      alive += 1;
      if !pred(score, k) {
        return true;
      }
      matched += 1;
      match window {
        ZSetWindow::All => picked.push(SortedSetEntry {
          score,
          member: k.to_vec(),
        }),
        ZSetWindow::Head(cap) if cap > 0 => {
          head.push(SortedSetEntry {
            score,
            member: k.to_vec(),
          });
          if head.len() > cap {
            head.pop();
          }
        }
        ZSetWindow::Tail(cap) if cap > 0 => {
          tail.push(Reverse(SortedSetEntry {
            score,
            member: k.to_vec(),
          }));
          if tail.len() > cap {
            tail.pop();
          }
        }
        _ => {}
      }
      true
    },
  ))?;
  if corrupt {
    return Err(());
  }
  let picked = match window {
    ZSetWindow::Head(_) => head.into_sorted_vec(),
    ZSetWindow::Tail(_) => tail.into_sorted_vec().into_iter().map(|e| e.0).collect(),
    // All：树内扫描序为 member 序，须按 (分值, 成员) 单源 Ord 重排，与内存态
    // `SortedSetObject`（C# 内存 `SortedSet` 同序）及本内核 Head/Tail 堆序一致；
    // Count 形态 picked 恒空，排序无副作用
    _ => {
      picked.sort();
      picked
    }
  };
  Ok(ZSetScanOut {
    picked,
    matched,
    alive,
    last_score,
  })
}

/// REV / LIMIT → （留存窗口, 跳数, 取数）单点换算
///
/// 对位 C# 两区间块的同一段：`offset < 0 || count == 0` → 空结果；`count < 0`
/// → 取到末尾（窗口退化为全量）；否则正向取序最小 `offset + count` 条、反向取
/// 序最大 `offset + count` 条，再跳过 `offset` 条——与 C#「全量收集后
/// `skip(offset).take(count)`」逐条同序，堆留存集是它的前缀。
fn zset_limit_window(
  reverse: bool,
  valid_limit: bool,
  limit: (i64, i64),
) -> (ZSetWindow, usize, usize) {
  if !valid_limit {
    return (ZSetWindow::All, 0, usize::MAX);
  }
  if limit.0 < 0 || limit.1 == 0 {
    return (ZSetWindow::Head(0), 0, 0);
  }
  let off = limit.0 as usize;
  if limit.1 < 0 {
    return (ZSetWindow::All, off, usize::MAX);
  }
  let take = limit.1 as usize;
  let window = if reverse {
    ZSetWindow::Tail(off.saturating_add(take))
  } else {
    ZSetWindow::Head(off.saturating_add(take))
  };
  (window, off, take)
}

/// 留存集 → 输出方向与 `skip/take` 切片（免二次拷贝）
///
/// Head/All 升序、Tail 降序已由 [`zset_scan_select`] 保证；仅全量形态需按 REV
/// 显式倒置（C# `scored_elements.reverse()` / `all.reverse()` 同臂）。
fn zset_windowed_pick(
  mut picked: Vec<SortedSetEntry>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> (Vec<SortedSetEntry>, Range<usize>) {
  if reverse && matches!(window, ZSetWindow::All) {
    picked.reverse();
  }
  let start = off.min(picked.len());
  let end = start.saturating_add(take).min(picked.len());
  (picked, start..end)
}

/// 字典序区间树内流式选择（ZRANGEBYLEX 族与 ZLEXCOUNT 共用内核）
///
/// C# `GetElementsInRangeByLex` 的上界是 `take_while`（真 break），而树内扫描序
/// 是 member 序、输出口径是 `(分值, 成员)` 序：同一条目「成员越界」与「分值
/// 靠后」互相交错，越界断点**不可**表达为局部谓词（反例 `{("z",1),("a",2)}`
/// 取 `[a`/`[y`：C# 在 (1,"z") 处 break，(2,"a") 虽在字典窗口内亦不得出现）。
/// 故先以 [`ZSetWindow::Head(1)`] 一趟求出断点 `= 序最小者 ∈ {存活 ∧ 过下界 ∧
/// 未过上界}`（内存 O(1)），第二趟把「序 < 断点」并入谓词选择；上界为 `+`
/// 时 break 永不触发，断点趟直接跳过。两趟均为页级顺序扫，无成员级堆分配。
///
/// 返回 `(留存集, 输出区间, 命中总数)`：区间供范围命令切片，命中总数供
/// ZLEXCOUNT（[`ZSetWindow::Count`] 形态下区间恒空）。
fn zset_lex_select(
  tree: &BfTreeService,
  now: i64,
  bounds: ZLexBounds<'_>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> Result<(Vec<SortedSetEntry>, Range<usize>, u64), ()> {
  if bounds.always_empty() {
    return Ok((Vec::new(), 0..0, 0));
  }
  let barrier = if bounds.max_inf == SpecialRanges::InfiniteMax {
    None
  } else {
    zset_scan_select(tree, now, ZSetWindow::Head(1), |_, member| {
      bounds.pass_min(member) && !bounds.pass_max(member)
    })?
    .picked
    .into_iter()
    .next()
  };
  let scan = zset_scan_select(tree, now, window, |score, member| {
    bounds.pass_min(member)
      && bounds.pass_max(member)
      && barrier.as_ref().is_none_or(|t| {
        SortedSetComparer::compare((&score, member), (&t.score, &t.member)) == Ordering::Less
      })
  })?;
  let matched = scan.matched;
  let (picked, range) = zset_windowed_pick(scan.picked, window, reverse, off, take);
  Ok((picked, range, matched))
}

/// 成员分值点读（到期视同不存在，对位 C# `SortedSetObject.TryGetScore`）
///
/// 与分层 ZSCORE / ZMSCORE 臂同一解码口径（8B f64 大端 + 可选 TTL 头）。以树内
/// 顺序扫描（member 为升序键，命中或越过即停）替代 `read_callback` 点读，与本
/// 模块其余读臂同一 `scan_with_count_callback` 入口——ZRANK 臂随后即接一次计数
/// 扫描，点读 + 扫描在同一服务上交错会命中 bf-tree 游标定位的 mini-page 合并
/// 缺陷，故全程只用扫描入口。`Ok(None)` = 成员不存在；扫描 Err 经 [`scan_count`]
/// 上抛（与「成员不存在」严格分态，不得折叠成 null 应答）
fn tree_member_score(tree: &BfTreeService, member: &[u8], now: i64) -> Result<Option<f64>, ()> {
  let mut score_opt = None;
  scan_count(tree.scan_with_count_callback(
    &[0u8],
    usize::MAX,
    ScanReturnField::KeyAndValue,
    |k, v| {
      match k.cmp(member) {
        // 键升序：越过目标即判不存在，停止扫描
        Ordering::Greater => false,
        // 命中：解码分值（到期视同不存在），停止扫描
        Ordering::Equal => {
          let (expiry, payload) = decode_member(v);
          if expiry.is_none_or(|ticks| ticks >= now)
            && let Ok(arr) = <[u8; 8]>::try_from(payload)
          {
            score_opt = Some(f64::from_be_bytes(arr));
          }
          false
        }
        Ordering::Less => true,
      }
    },
  ))?;
  Ok(score_opt)
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use tempfile::tempdir;
  use wcol::{
    types::member_ttl::encode_member,
    zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts},
  };
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wval::GarnetObjectType;

  use super::*;
  use crate::resp::objects::tiered_collection_ops::common::{TieredCollectionArgs, TieredCtx};

  #[test]
  fn test_tiered_zset_bylex_syntax_error_truncate() {
    let dir = tempdir().unwrap();
    let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
    let device =
      Arc::new(SegmentedDevice::single_file(dir.path().join("zset_truncate.db")).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let rt = Runtime::new().unwrap();
    let sess = store.new_session().unwrap();
    let key = b"tiered_zset_key";

    // 灌入条目并升阶为分层态
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
      .map(|i| {
        let member = format!("m{i:02}").into_bytes();
        let val = encode_member(&((i as f64).to_be_bytes()), None);
        (member, val)
      })
      .collect();

    rt.block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::SortedSet,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();

    let batch = sess.enter_batch();
    let (mut meta, mut stub) = rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .unwrap();

    // 1. 单命令场景：output 为空，语法错误写出错误帧
    {
      let mut ctx = TieredCtx::new(&mut meta, &mut stub);
      let mut output = Vec::new();
      let args: &[&[u8]] = &[b"invalid_lex", b"[m05"];
      let range_opts = SortedSetRangeOpts::BY_LEX;
      let call = TieredCollectionArgs::new(
        SortedSetOperation::Zrange,
        (0, range_opts.bits() as i32),
        args,
        2,
      );
      let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
      assert_eq!(res, Ok(true));
      assert_eq!(output, b"-ERR min or max not valid string range item\r\n");
    }

    // 2. 流水线（Pipeline）场景：output 包含先前命令已写应答，严禁被 clear 冲刷
    {
      let mut ctx = TieredCtx::new(&mut meta, &mut stub);
      let mut output = Vec::new();
      let prefix = b"+PONG\r\n:1\r\n";
      output.extend_from_slice(prefix);

      let args: &[&[u8]] = &[b"invalid_lex", b"[m05"];
      let range_opts = SortedSetRangeOpts::BY_LEX;
      let call = TieredCollectionArgs::new(
        SortedSetOperation::Zrange,
        (0, range_opts.bits() as i32),
        args,
        2,
      );
      let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
      assert_eq!(res, Ok(true));
      assert_eq!(
        &output[..prefix.len()],
        prefix,
        "前置流水线响应必须被完整保留，严禁调用 output.clear()"
      );
      assert_eq!(
        output,
        b"+PONG\r\n:1\r\n-ERR min or max not valid string range item\r\n"
      );
    }

    // 3. BYSCORE 与 BYLEX 并置场景：BYSCORE 写出后 BYLEX 语法失败，truncate 仅撤回本命令输出
    {
      let mut ctx = TieredCtx::new(&mut meta, &mut stub);
      let mut output = Vec::new();
      let prefix = b"+OK\r\n";
      output.extend_from_slice(prefix);

      let args: &[&[u8]] = &[b"0", b"5", b"BYSCORE", b"BYLEX"];
      let range_opts = SortedSetRangeOpts::BY_SCORE | SortedSetRangeOpts::BY_LEX;
      let call = TieredCollectionArgs::new(
        SortedSetOperation::Zrange,
        (0, range_opts.bits() as i32),
        args,
        2,
      );
      let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
      assert_eq!(res, Ok(true));
      assert_eq!(
        output, b"+OK\r\n-ERR min or max not valid string range item\r\n",
        "BYLEX 失败后应回滚 BYSCORE 输出，仅留存前置应答与错误帧"
      );
    }
  }
}
