//! 分层 zset 树内计数/范围/排名臂：ZCOUNT / ZLEXCOUNT / ZRANGE 族 / ZRANK /
//! ZREVRANK（tiered_zset_arm 的读命令臂提取，扫描内核见 [`super::scan`]）

use std::{cmp::Ordering, mem::swap, ops::Range};

use wbase::time::now_ticks;
use wbftree::BfTreeService;
use wcol::zset::{
  comparer::SortedSetComparer,
  sorted_set_object::{SortedSetEntry, SortedSetOperation, SortedSetRangeOpts},
  sorted_set_object_impl::{RangeArgError, parse_range_options, write_sorted_set_result_payload},
};
use wresp::{cmd_strings as cs, ext::RespVecExt};

use super::scan::{
  ZLexBounds, ZScoreBounds, ZSetWindow, tree_member_score, zset_lex_select, zset_limit_window,
  zset_scan_select, zset_windowed_pick,
};

/// ZRANGE 族结果负载出帧单点（byIndex/byScore 块与 byLex 块同一负载写法，
/// 逐字委托 wcol 单源 [`write_sorted_set_result_payload`]，本层不自拼协议字节）
#[inline]
fn write_zset_range_reply(
  output: &mut Vec<u8>,
  with_scores: bool,
  resp_ver: u8,
  picked: &[SortedSetEntry],
  range: Range<usize>,
) {
  write_sorted_set_result_payload(
    output,
    with_scores,
    range.len(),
    resp_ver,
    picked[range].iter().map(|e| (e.score, e.member.as_ref())),
  );
}

/// ZCOUNT 树内读臂（libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:
/// SortedSetCount）：单趟流式扫描计数，内存 O(1)（原经 slow_load_eval 全量
/// 物化 → 千万级集合一次 O(N) 内存抖动）。原 `tiered_zset_arm` 的
/// `SortedSetOperation::Zcount` 臂逐字迁出
pub(super) fn zset_zcount_arm(
  tree: &BfTreeService,
  output: &mut Vec<u8>,
  args: &[&[u8]],
) -> Result<bool, ()> {
  if args.len() < 2 {
    return Err(());
  }
  let Some(bounds) = ZScoreBounds::parse(args[0], args[1]) else {
    cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
    return Ok(true);
  };
  let scan = zset_scan_select(tree, now_ticks(), ZSetWindow::Count, |score, _| {
    bounds.pass(score)
  })?;
  // C# 外层守卫 `minValue <= sortedSet.Max.Score`：raw `<=` 且 Max **含到期
  // 成员**（对象层该臂不先 DeleteExpiredItems），NaN 面唯一差异处
  //（min 为 NaN 时守卫短路为 0，谓词本身会把 NaN 成员计入）
  let count = match scan.last_score {
    Some(last) if bounds.min <= last => scan.matched,
    _ => 0,
  };
  output.write_resp_int(count as i64);
  Ok(true)
}

/// ZLEXCOUNT 树内读臂（C# SortedSetObjectImpl.SortedSetRemoveOrCountRangeByLex
/// 的 ZLEXCOUNT 分支：`GetElementsInRangeByLex(min, max, false, false, (0,0))`
/// 命中数，零删除）。原 `tiered_zset_arm` 的 `SortedSetOperation::Zlexcount`
/// 臂逐字迁出
pub(super) fn zset_zlexcount_arm(
  tree: &BfTreeService,
  output: &mut Vec<u8>,
  args: &[&[u8]],
) -> Result<bool, ()> {
  if args.len() < 2 {
    return Err(());
  }
  let Some(bounds) = ZLexBounds::parse(args[0], args[1], false) else {
    // 对象层由调用方按 result1 == int.MaxValue 回同一错误文本
    cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
    return Ok(true);
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

/// ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX /
/// ZREVRANGEBYLEX 树内读臂（arg2 = SortedSetRangeOpts 位，与对象层 run_operate
/// 同约定；C# SortedSetObjectImpl.SortedSetRange 三形态逐臂对位）：
/// 参数段解析与结果负载均复用 wcol 单源函数，选择走 [`zset_scan_select`]
/// 流式内核，内存只随窗口增长，不再全量物化。原 `tiered_zset_arm` 的
/// `SortedSetOperation::Zrange` 臂逐字迁出（byLex 块解析失败撤帧回到
/// `frame_base` 重写错误应答，对标 C# writer.ResetPosition）
pub(super) fn zset_zrange_arm(
  tree: &BfTreeService,
  output: &mut Vec<u8>,
  args: &[&[u8]],
  args12: (i32, i32),
  resp_protocol_version: u8,
  frame_base: usize,
) -> Result<bool, ()> {
  let range_opts = SortedSetRangeOpts::from_bits_truncate(args12.1 as u8);
  let (options, resp_ver) = match parse_range_options(args, range_opts, resp_protocol_version) {
    Ok(v) => v,
    // 命令层 arity 保证 min/max 两段 → Incomplete 不可达；分层侧不得输出
    // 半帧（对象层该臂零输出回包），防御性走存储错误面
    Err(RangeArgError::Incomplete) => return Err(()),
    Err(err) => {
      err.write_reply(output);
      return Ok(true);
    }
  };
  let (min_span, max_span) = (args[0], args[1]);
  let now = now_ticks();

  // ---- 区间块 1：byIndex 或 byScore（C# 同段进入条件 !byLex || byScore）
  if (!options.by_score && !options.by_lex) || options.by_score {
    let Some(mut bounds) = ZScoreBounds::parse(min_span, max_span) else {
      cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
      return Ok(true);
    };
    let (picked, range) = if options.by_score {
      // C# do_reverse 先交换边界再正序扫、后 reverse 输出（等价于按窗口
      // 方向直接取序最大 k 条）
      if options.reverse {
        swap(&mut bounds.min, &mut bounds.max);
        swap(&mut bounds.min_excl, &mut bounds.max_excl);
      }
      let (window, off, take) =
        zset_limit_window(options.reverse, options.valid_limit, options.limit);
      // LIMIT 折位早退：对位 C# GetElementsInRangeByScore 的 validLimit &&
      // (offset < 0 || count == 0) 即时回空（libs/server/Objects/SortedSet/
      // SortedSetObjectImpl.cs:1095-1099）与内存信封臂
      // wcol sorted_set_object_impl.rs:1392-1394——契约恒空不触树内容：
      // 免一趟全树空扫，损坏载荷键上亦不随扫描 fail-fast 把空应答放大成
      // 存储错误（双态契约恒空不分叉）
      if take == 0 {
        (Vec::new(), 0..0)
      } else {
        let scan = zset_scan_select(tree, now, window, |score, _| bounds.pass(score))?;
        zset_windowed_pick(scan.picked, window, options.reverse, off, take)
      }
    } else {
      // byIndex：LIMIT 不支持（C# 同臂），负索引归一与钳制以存活总数为
      // 基准，故除 `0 -1` 全量快速路径外需先走一趟计数
      if options.valid_limit {
        cs::write_error_raw(output, cs::RESP_ERR_LIMIT_NOT_SUPPORTED);
        return Ok(true);
      }
      if bounds.min == 0.0 && bounds.max == -1.0 {
        let scan = zset_scan_select(tree, now, ZSetWindow::All, |_, _| true)?;
        zset_windowed_pick(scan.picked, ZSetWindow::All, options.reverse, 0, usize::MAX)
      } else {
        let set_count = zset_scan_select(tree, now, ZSetWindow::Count, |_, _| true)?.alive;
        if bounds.min > (set_count as f64) - 1.0 {
          (Vec::new(), 0..0)
        } else {
          // 下标 f64→i64 饱和加宽不复刻 C# (int) unchecked 截断回绕（宽向
          // 修复登记 deviations.md §202，与内存态同口径）
          let (mut min_index, mut max_index) = (bounds.min as i64, bounds.max as i64);
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
    write_zset_range_reply(output, options.with_scores, resp_ver, &picked, range);
  }

  // ---- 区间块 2：byLex（与块 1 相互独立，BYSCORE+BYLEX 并置时 C# 写两份
  // 回复；本块解析失败须回退本命令负载起点重写错误，对标 writer.ResetPosition）
  if options.by_lex {
    let out = match ZLexBounds::parse(min_span, max_span, options.reverse) {
      Some(bounds) => {
        let (window, off, take) =
          zset_limit_window(options.reverse, options.valid_limit, options.limit);
        // 恒空界与 LIMIT 折位并入同一短路臂——对位 C# GetElementsInRangeByLex
        // :1004-1010 单臂形态与内存信封臂 sorted_set_object_impl.rs:1307-1312
        // 三条件同臂短路；take == 0 为 zset_limit_window 折位唯一签名（见其
        // 文注），契约恒空不触树内容，损坏载荷键上不得答成 SLOW_PATH_STORAGE
        // 错误帧（双态契约恒空不分叉）。早退落本命令臂，勿下沉 zset_lex_select
        // 共享核——ZLEXCOUNT 复用该核且无 limit 输入，下沉即污染
        if take == 0 || bounds.always_empty() {
          (Vec::new(), 0..0, 0)
        } else {
          match zset_lex_select(tree, now, bounds, window, options.reverse, off, take) {
            Ok(out) => out,
            Err(()) => {
              output.truncate(frame_base);
              cs::write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
              return Ok(true);
            }
          }
        }
      }
      None => {
        output.truncate(frame_base);
        cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
        return Ok(true);
      }
    };
    let (picked, range, _) = out;
    write_zset_range_reply(output, options.with_scores, resp_ver, &picked, range);
  }
  Ok(true)
}

/// ZRANK / ZREVRANK 树内读臂（C# SortedSetObjectImpl.SortedSetRank）：
/// arg1 == 1 附带分值；名次 = 序在目标之前的存活成员数，反向以
/// `存活总数 - 名次 - 1` 换算（C# Count() 同基准，两侧皆不含到期成员）。
/// 原 `tiered_zset_arm` 的 `SortedSetOperation::Zrank | Zrevrank` 臂逐字迁出
pub(super) fn zset_zrank_arm(
  tree: &BfTreeService,
  output: &mut Vec<u8>,
  args: &[&[u8]],
  args12: (i32, i32),
  resp_protocol_version: u8,
  op: SortedSetOperation,
) -> Result<bool, ()> {
  let with_score = args12.0 == 1;
  // 与冷路径同口径取成员（命令层保证段数，缺失防御性按空成员）
  let member = args.first().copied().unwrap_or(&[]);
  let now = now_ticks();
  // C# TryGetScore：到期成员视同不存在（先点读定位分值，再单趟流式计名次）；
  // 扫描 Err 与「成员不存在」分态：Err 上抛（应答尚未落帧），仅 None 回 null
  let Some(score) = tree_member_score(tree, member, now)? else {
    output.write_resp_null_ver(resp_protocol_version);
    return Ok(true);
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
