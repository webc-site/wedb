//! 有序集合 RESP 语义操作（对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs，
//! C# 为 SortedSetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use std::{cmp::Ordering, mem::swap};

use fastrand::Rng;
use wbase::{
  convert::{
    milliseconds_from_diff_ticks, seconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks,
    unix_time_in_seconds_from_ticks,
  },
  num::strict_i32,
  time::now_ticks,
};
use wresp::{
  cmd_strings::{
    LIMIT, RESP_ERR_GENERIC_SCORE_NAN, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_GT_LT_NX_NOT_COMPATIBLE,
    RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR, RESP_ERR_LIMIT_NOT_SUPPORTED,
    RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, RESP_ERR_MIN_MAX_NOT_VALID_STRING, RESP_ERR_NOT_VALID_FLOAT,
    RESP_ERR_XX_NX_NOT_COMPATIBLE, WITHSCORES,
  },
  options::{
    ExpirationWithOption, SortedSetAddOption, equals_ignore_case, try_get_sorted_set_add_option,
  },
  resp_memory_writer::RespWriter,
};

use super::sorted_set_object::{
  SortedSetEntry, SortedSetObject, SortedSetOperation, SortedSetRangeOpts,
};
use crate::{
  parse_utils::try_parse_with_infinity,
  resp::output::{write_double_numeric, write_null},
  types::{ObjectOutput, read_scan_input},
};

/// [`sorted_set_range`] 出错标记：range 回复不可能为负，ZRANGESTORE 借此区分
/// 错误与正常空结果
///
/// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:RangeError
pub(crate) const RANGE_ERROR: i64 = -1;

/// ZRANGE 选项束
///
/// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:ZRangeOptions
#[derive(Debug, Clone, Copy, Default)]
struct ZRangeOptions {
  by_score: bool,
  by_lex: bool,
  reverse: bool,
  with_scores: bool,
  valid_limit: bool,
  limit: (i64, i64),
}

/// 双向迭代静态分发枚举（消除堆分配）
enum IterDir<F, R> {
  Forward(F),
  Reverse(R),
}

impl<T, F: Iterator<Item = T>, R: Iterator<Item = T>> Iterator for IterDir<F, R> {
  type Item = T;

  #[inline(always)]
  fn next(&mut self) -> Option<T> {
    match self {
      Self::Forward(f) => f.next(),
      Self::Reverse(r) => r.next(),
    }
  }

  #[inline(always)]
  fn size_hint(&self) -> (usize, Option<usize>) {
    match self {
      Self::Forward(f) => f.size_hint(),
      Self::Reverse(r) => r.size_hint(),
    }
  }
}

/// 按分值范围检索有序集合元素的查询参数
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScoreRangeQuery {
  pub min_value: f64,
  pub max_value: f64,
  pub min_exclusive: bool,
  pub max_exclusive: bool,
  pub do_reverse: bool,
  pub valid_limit: bool,
  pub limit: (i64, i64),
}

/// 字典序特殊区间（-/+, 即无穷小/无穷大）
///
/// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SpecialRanges
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpecialRanges {
  #[default]
  None = 0,
  InfiniteMin = 1,
  InfiniteMax = 2,
}

impl SortedSetObject {
  /// 解析并校验 ZADD 选项组合；失败时写错误并返回 None
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions
  pub(crate) fn get_options(
    &self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    curr_token_idx: &mut usize,
  ) -> Option<SortedSetAddOption> {
    let mut options = SortedSetAddOption::NONE;

    while *curr_token_idx < args.len() {
      let Some(curr_option) = try_get_sorted_set_add_option(args[*curr_token_idx]) else {
        break;
      };
      options |= curr_option;
      *curr_token_idx += 1;
    }

    // XX 与 NX 互斥
    let mut options_error: &[u8] = &[];
    if options.contains(SortedSetAddOption::XX) && options.contains(SortedSetAddOption::NX) {
      options_error = RESP_ERR_XX_NX_NOT_COMPATIBLE.as_bytes();
    }

    // NX、GT、LT 两两互斥
    if options.contains(SortedSetAddOption::GT) && options.contains(SortedSetAddOption::LT)
      || ((options.contains(SortedSetAddOption::GT) || options.contains(SortedSetAddOption::LT))
        && options.contains(SortedSetAddOption::NX))
    {
      options_error = RESP_ERR_GT_LT_NX_NOT_COMPATIBLE.as_bytes();
    }

    // INCR 仅支持单对 score-element
    if options.contains(SortedSetAddOption::INCR) && args.len() - *curr_token_idx > 2 {
      options_error = RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR.as_bytes();
    }

    if !options_error.is_empty() {
      RespWriter::new_ref(&mut output.payload).write_error_bytes(options_error);
      return None;
    }

    // 剩余 token 须为正数对（偶数个）
    if *curr_token_idx == args.len() || !(args.len() - *curr_token_idx).is_multiple_of(2) {
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      return None;
    }

    Some(options)
  }

  /// ZADD：带 XX/NX/GT/LT/CH/INCR 全选项的批量添加
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd
  pub(crate) fn sorted_set_add(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    let mut added_or_changed = 0_i64;
    let mut incr_result = 0_f64;

    let mut options = SortedSetAddOption::NONE;
    let mut curr_token_idx = 0;
    let mut parsed_options = false;

    let count = args.len();

    while curr_token_idx < count {
      // 先尝试解析 score；非 score 则先吃掉选项段
      let Some(score) = try_parse_with_infinity(args[curr_token_idx]) else {
        if !parsed_options {
          parsed_options = true;
          let Some(opts) = self.get_options(args, output, &mut curr_token_idx) else {
            return;
          };
          options = opts;
          continue; // 选项解析完重试当前 token
        }
        RespWriter::new_ref(&mut output.payload)
          .write_error_bytes(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
        return;
      };

      parsed_options = true;
      curr_token_idx += 1;

      // member（命令层保证 score-member 成对；奇数尾巴防御性截断，C# 为越界读）
      if curr_token_idx >= count {
        break;
      }
      let member = args[curr_token_idx].to_vec();
      curr_token_idx += 1;

      match self.sorted_set_dict.get(&member).copied() {
        // 新增成员
        None => {
          // XX 时不新增
          if options.contains(SortedSetAddOption::XX) {
            continue;
          }

          incr_result = score;
          self.sorted_set_dict.insert(member.clone(), score);
          if self.sorted_set.insert(SortedSetEntry {
            score,
            member: member.clone(),
          }) {
            added_or_changed += 1;
          }

          self.update_size(&member, true);
        }
        // 更新既有成员
        Some(score_stored) => {
          let mut score = score;
          // INCR：新分值叠加在既有分值上
          if options.contains(SortedSetAddOption::INCR) {
            score += score_stored;
            incr_result = score;

            if score.is_nan() {
              RespWriter::new_ref(&mut output.payload)
                .write_error_bytes(RESP_ERR_GENERIC_SCORE_NAN.as_bytes());
              return;
            }
          }

          // 分值未变：仅清除过期
          if score == score_stored {
            self.try_remove_expiration(&member);
            continue;
          }

          // NX / GT(旧分值更高) / LT(旧分值更低) 时拒绝更新
          if options.contains(SortedSetAddOption::NX)
            || (options.contains(SortedSetAddOption::GT) && score_stored > score)
            || (options.contains(SortedSetAddOption::LT) && score_stored < score)
          {
            if options.contains(SortedSetAddOption::INCR) {
              write_null(output, resp_protocol_version);
              return;
            }
            continue;
          }

          self.sorted_set_dict.insert(member.clone(), score);
          self.sorted_set.remove(&SortedSetEntry {
            score: score_stored,
            member: member.clone(),
          });
          self.sorted_set.insert(SortedSetEntry {
            score,
            member: member.clone(),
          });
          self.try_remove_expiration(&member);

          // CH 时变更计入返回值
          if options.contains(SortedSetAddOption::CH) {
            added_or_changed += 1;
          }
        }
      }
    }

    if options.contains(SortedSetAddOption::INCR) {
      write_double_numeric(output, incr_result, resp_protocol_version);
    } else {
      RespWriter::new_ref(&mut output.payload).write_int64(added_or_changed);
    }
  }

  /// ZREM：批量移除成员
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemove
  pub(crate) fn sorted_set_remove(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    let mut removed = 0_i64;

    for &value in args {
      let Some((member, score)) = self.sorted_set_dict.remove_entry(value) else {
        continue;
      };

      removed += 1;
      self.sorted_set.remove(&SortedSetEntry { score, member });
      self.try_remove_expiration(value);

      self.update_size(value, false);
    }

    output.result1 = removed;
  }

  /// ZCARD：成员计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetLength
  pub(crate) fn sorted_set_length(&mut self, output: &mut ObjectOutput<'_>) {
    // Check both objects
    debug_assert_eq!(
      self.sorted_set_dict.len(),
      self.sorted_set.len(),
      "SortedSet object is not in sync."
    );
    output.result1 = self.count() as i64;
  }

  /// ZSCORE：单成员分值
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetScore
  pub(crate) fn sorted_set_score(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let member = args[0];

    match self.try_get_score(member) {
      None => write_null(output, resp_protocol_version),
      Some(score) => write_double_numeric(output, score, resp_protocol_version),
    }
    output.result1 = 1;
  }

  /// ZMSCORE：多成员分值
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetScores
  pub(crate) fn sorted_set_scores(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let count = args.len();

    RespWriter::new_ref(&mut output.payload).write_array_length(count);

    for &member in args {
      match self.try_get_score(member) {
        None => write_null(output, resp_protocol_version),
        Some(score) => write_double_numeric(output, score, resp_protocol_version),
      }
    }

    output.result1 = count as i64;
  }

  /// ZCOUNT：分值区间成员计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetCount
  pub(crate) fn sorted_set_count(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    let min_param = args[0];
    let max_param = args[1];

    let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
      Self::try_parse_parameter(min_param),
      Self::try_parse_parameter(max_param),
    ) else {
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    let mut count = 0_i64;
    if let Some(max_entry) = self.sorted_set.last()
      && min_value <= max_entry.score
    {
      let start = SortedSetEntry {
        score: min_value,
        member: Vec::new(),
      };
      for item in self.sorted_set.range(start..) {
        if self.is_expired(&item.member) {
          continue;
        }
        if item.score > max_value || (max_exclusive && item.score == max_value) {
          break;
        }
        if min_exclusive && item.score == min_value {
          continue;
        }
        count += 1;
      }
    }

    RespWriter::new_ref(&mut output.payload).write_int64(count);
  }

  /// ZINCRBY：分值增量
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetIncrement
  pub(crate) fn sorted_set_increment(
    &mut self,
    args: &[&[u8]],
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    // RESP2 兼容读回场景的协议覆盖（C# arg2 > 0 时强制）
    let resp_protocol_version = if arg2 > 0 {
      arg2 as u8
    } else {
      resp_protocol_version
    };

    let Some(incr_value) = try_parse_with_infinity(args[0]) else {
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    let member = args[1].to_vec();

    let new_score = match self.sorted_set_dict.get(&member).copied() {
      Some(score) => {
        let result = score + incr_value;
        if result.is_nan() {
          RespWriter::new_ref(&mut output.payload)
            .write_error_bytes(RESP_ERR_GENERIC_SCORE_NAN.as_bytes());
          return;
        }

        self.sorted_set_dict.insert(member.clone(), result);
        self.sorted_set.remove(&SortedSetEntry {
          score,
          member: member.clone(),
        });
        self.sorted_set.insert(SortedSetEntry {
          score: result,
          member: member.clone(),
        });
        result
      }
      None => {
        self.sorted_set_dict.insert(member.clone(), incr_value);
        self.sorted_set.insert(SortedSetEntry {
          score: incr_value,
          member: member.clone(),
        });
        self.update_size(&member, true);
        incr_value
      }
    };

    write_double_numeric(output, new_score, resp_protocol_version);
  }

  /// ZRANGE / ZRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGE 族统一入口
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRange
  pub(crate) fn sorted_set_range(
    &mut self,
    args: &[&[u8]],
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let range_opts = SortedSetRangeOpts::from_bits_truncate(arg2 as u8);
    let count = args.len();

    // C# 两个区间块均要求 count >= 2，不足时无任何输出（命令层保证 ≥2）
    if count < 2 {
      return;
    }

    let mut curr_idx = 0;

    let min_span = args[curr_idx];
    curr_idx += 1;
    let max_span = args[curr_idx];
    curr_idx += 1;

    // ZRANGESTORE 需要成对负载读回，协议固定为 RESP2
    let mut resp_protocol_version = resp_protocol_version;
    let mut options = ZRangeOptions {
      by_score: range_opts.contains(SortedSetRangeOpts::BY_SCORE),
      by_lex: range_opts.contains(SortedSetRangeOpts::BY_LEX),
      reverse: range_opts.contains(SortedSetRangeOpts::REVERSE),
      with_scores: range_opts.contains(SortedSetRangeOpts::WITH_SCORES)
        || range_opts.contains(SortedSetRangeOpts::STORE),
      ..Default::default()
    };

    if resp_protocol_version >= 3 && range_opts.contains(SortedSetRangeOpts::STORE) {
      resp_protocol_version = 2;
    }

    if count > 2 {
      while curr_idx < count {
        let token = args[curr_idx];
        curr_idx += 1;

        if equals_ignore_case(token, b"BYSCORE") {
          options.by_score = true;
        } else if equals_ignore_case(token, b"BYLEX") {
          options.by_lex = true;
        } else if equals_ignore_case(token, b"REV") {
          options.reverse = true;
        } else if equals_ignore_case(token, LIMIT) {
          // LIMIT 后须有 offset count 两个 token
          if args.len() - curr_idx < 2 {
            RespWriter::new_ref(&mut output.payload)
              .write_error_bytes(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
            output.result1 = RANGE_ERROR;
            return;
          }

          let (Some(offset), Some(count_limit)) = (
            strict_i32(args[curr_idx]).map(|v| v as i64),
            strict_i32(args[curr_idx + 1]).map(|v| v as i64),
          ) else {
            RespWriter::new_ref(&mut output.payload)
              .write_error_bytes(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
            output.result1 = RANGE_ERROR;
            return;
          };
          curr_idx += 2;

          options.limit = (offset, count_limit);
          options.valid_limit = true;
        } else if equals_ignore_case(token, WITHSCORES) {
          options.with_scores = true;
        }
      }
    }

    if count >= 2 && ((!options.by_score && !options.by_lex) || options.by_score) {
      let Some((min_value, min_exclusive)) = Self::try_parse_parameter(min_span) else {
        RespWriter::new_ref(&mut output.payload)
          .write_error_bytes(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT.as_bytes());
        output.result1 = RANGE_ERROR;
        return;
      };
      let Some((max_value, max_exclusive)) = Self::try_parse_parameter(max_span) else {
        RespWriter::new_ref(&mut output.payload)
          .write_error_bytes(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT.as_bytes());
        output.result1 = RANGE_ERROR;
        return;
      };

      if options.by_score {
        let scored_elements = self.get_elements_in_range_by_score(ScoreRangeQuery {
          min_value,
          max_value,
          min_exclusive,
          max_exclusive,
          do_reverse: options.reverse,
          valid_limit: options.valid_limit,
          limit: options.limit,
        });
        let n = scored_elements.len();
        self.write_sorted_set_result(
          options.with_scores,
          n,
          resp_protocol_version,
          scored_elements.into_iter(),
          output,
        );
      } else {
        // byIndex
        let set_count = self.count();
        let mut min_index = min_value as i64;
        let mut max_index = max_value as i64;
        if options.valid_limit {
          RespWriter::new_ref(&mut output.payload)
            .write_error_bytes(RESP_ERR_LIMIT_NOT_SUPPORTED.as_bytes());
          output.result1 = RANGE_ERROR;
          return;
        } else if min_value > (set_count as f64) - 1.0 {
          // 空结果
          RespWriter::new_ref(&mut output.payload).write_empty_array();
          return;
        } else {
          // 负索引从尾部偏移
          if min_index < 0 {
            min_index += set_count as i64;
          }
          if max_index < 0 {
            max_index += set_count as i64;
          } else if max_index >= set_count as i64 {
            max_index = set_count as i64 - 1;
          }

          // 双双越界或 min > max：空结果
          if (min_index < 0 && max_index < 0) || min_index > max_index {
            RespWriter::new_ref(&mut output.payload).write_empty_array();
            return;
          }

          // 钳制 minIndex
          let min_index = min_index.max(0);

          let n = (max_index - min_index + 1) as usize;
          let direction = if options.reverse {
            IterDir::Reverse(self.sorted_set.iter().rev())
          } else {
            IterDir::Forward(self.sorted_set.iter())
          };

          let picked: Vec<(f64, &[u8])> = direction
            .filter(|x| !self.is_expired(&x.member))
            .skip(min_index as usize)
            .take(n)
            .map(|x| (x.score, x.member.as_slice()))
            .collect();

          let count = picked.len();
          self.write_sorted_set_result(
            options.with_scores,
            count,
            resp_protocol_version,
            picked.into_iter(),
            output,
          );
        }
      }
    }

    // byLex
    if count >= 2 && options.by_lex {
      let (elements_in_lex, error_code) = self.get_elements_in_range_by_lex(
        min_span,
        max_span,
        options.reverse,
        options.valid_limit,
        options.limit,
      );

      if error_code == i32::MAX {
        // BYSCORE 与 BYLEX 相互独立："ZRANGE k 1 3 BYSCORE BYLEX" 时上方已写过
        // 数组回复，须回退到本命令负载起点再写错误（两份回复会令 RESP 流失步；
        // 对标 writer.ResetPosition）
        output.reset();
        RespWriter::new_ref(&mut output.payload)
          .write_error_bytes(RESP_ERR_MIN_MAX_NOT_VALID_STRING.as_bytes());
        output.result1 = RANGE_ERROR;
      } else {
        let n = elements_in_lex.len();
        self.write_sorted_set_result(
          options.with_scores,
          n,
          resp_protocol_version,
          elements_in_lex.into_iter(),
          output,
        );
      }
    }
  }

  /// 范围结果统一 RESP 输出（RESP3 成对嵌套 / RESP2 扁平）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:WriteSortedSetResult
  pub(crate) fn write_sorted_set_result<B: AsRef<[u8]>>(
    &self,
    with_scores: bool,
    count: usize,
    resp_protocol_version: u8,
    iterator: impl Iterator<Item = (f64, B)>,
    output: &mut ObjectOutput<'_>,
  ) {
    if with_scores && resp_protocol_version >= 3 {
      RespWriter::new_ref(&mut output.payload).write_array_length(count);

      for (score, element) in iterator {
        RespWriter::new_ref(&mut output.payload).write_array_length(2);
        RespWriter::new_ref(&mut output.payload).write_bulk_string(element.as_ref());
        write_double_numeric(output, score, resp_protocol_version);
      }
    } else {
      RespWriter::new_ref(&mut output.payload).write_array_length(if with_scores {
        count * 2
      } else {
        count
      });

      for (score, element) in iterator {
        RespWriter::new_ref(&mut output.payload).write_bulk_string(element.as_ref());
        if with_scores {
          RespWriter::new_ref(&mut output.payload).write_double_bulk_string(score);
        }
      }
    }
  }

  /// ZREMRANGEBYRANK：按排名区间移除
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveRangeByRank
  pub(crate) fn sorted_set_remove_range_by_rank(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
  ) {
    self.delete_expired_items();

    let (Some(start), Some(stop)) = (
      strict_i32(args[0]).map(|v| v as i64),
      strict_i32(args[1]).map(|v| v as i64),
    ) else {
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return;
    };

    let count = self.sorted_set_dict.len() as i64;

    // 负索引平移（对齐 Redis t_zset.c zremrangeGenericCommand）
    let mut start = start;
    let mut stop = stop;
    if start < 0 {
      start += count;
    }
    if stop < 0 {
      stop += count;
    }
    if start < 0 {
      start = 0;
    }

    // start 非负，故 start > stop 覆盖 stop 仍为负的情形
    if start > stop || start >= count {
      RespWriter::new_ref(&mut output.payload).write_int64(0);
      return;
    }

    let stop = if stop >= count { count - 1 } else { stop };

    let element_count = (stop - start + 1) as usize;

    // 先收集后删除，规避迭代中修改
    let doomed: Vec<SortedSetEntry> = self
      .sorted_set
      .iter()
      .skip(start as usize)
      .take(element_count)
      .cloned()
      .collect();

    for item in doomed {
      if self.sorted_set_dict.remove(&item.member).is_some() {
        self.sorted_set.remove(&item);
        self.update_size(&item.member, false);
      }
      self.try_remove_expiration(&item.member);
    }

    RespWriter::new_ref(&mut output.payload).write_int64(element_count as i64);
  }

  /// ZREMRANGEBYSCORE：按分值区间移除
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveRangeByScore
  pub(crate) fn sorted_set_remove_range_by_score(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
  ) {
    self.delete_expired_items();

    let min_param = args[0];
    let max_param = args[1];

    let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
      Self::try_parse_parameter(min_param),
      Self::try_parse_parameter(max_param),
    ) else {
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    let hits = self.get_elements_in_range_by_score(ScoreRangeQuery {
      min_value,
      max_value,
      min_exclusive,
      max_exclusive,
      do_reverse: false,
      valid_limit: false,
      limit: (0, 0),
    });

    // rem 删除语义移出扫描函数：借用收集命中即止，仅对命中 member 做一次 to_vec，
    // 删除循环经 dict.remove_entry 归还所有权后直接构造 BTreeSet 条目 remove（零额外克隆）
    let doomed: Vec<Vec<u8>> = hits.into_iter().map(|(_, m)| m.to_vec()).collect();
    let removed = doomed.len() as i64;
    for member in doomed {
      if let Some((m, score)) = self.sorted_set_dict.remove_entry(&member) {
        self.sorted_set.remove(&SortedSetEntry { score, member: m });
        self.try_remove_expiration(&member);
        self.update_size(&member, false);
      }
    }

    RespWriter::new_ref(&mut output.payload).write_int64(removed);
  }

  /// ZRANDMEMBER：随机成员（arg1 打包 count/withScores/includedCount，arg2 为种子）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRandomMember
  pub(crate) fn sorted_set_random_member(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let mut count = (arg1 >> 2) as i64;
    let with_scores = (arg1 & 1) == 1;
    let included_count = ((arg1 >> 1) & 1) == 1;
    let seed = arg2 as u32;
    let sorted_set_count = self.count() as i64;

    if count > 0 && count > sorted_set_count {
      count = sorted_set_count;
    }

    // count 可为负，但数组长度不能
    let array_length = (if with_scores && resp_protocol_version == 2 {
      count * 2
    } else {
      count
    })
    .abs();
    if array_length > 1 || (array_length == 1 && included_count) {
      RespWriter::new_ref(&mut output.payload).write_array_length(array_length as usize);
    }
    let index_count = count.unsigned_abs() as usize;

    // 随机下标采样：count > 0 不放回（Fisher-Yates 部分洗牌，对标
    // RandomUtils.PickKRandomIndexes 的 unique 路径），负数放回重复抽取
    let mut rng = Rng::with_seed(u64::from(seed));
    let universe = sorted_set_count.max(0) as usize;
    let mut indexes = Vec::with_capacity(index_count);
    if count > 0 {
      let mut perm: Vec<usize> = (0..universe).collect();
      for i in 0..index_count.min(universe) {
        let j = rng.usize(i..perm.len());
        perm.swap(i, j);
        indexes.push(perm[i]);
      }
    } else if universe > 0 {
      // 刻意差异（对照 C#）：Random.Next(0) 在空集上抛 ArgumentOutOfRangeException，
      // 此处按无结果处理（空对象实际不可达：删空即 RemoveKey）
      for _ in 0..index_count {
        indexes.push(rng.usize(..universe));
      }
    }

    for idx in indexes {
      let Some((element, score)) = self.element_at(idx) else {
        continue;
      };

      if with_scores && resp_protocol_version >= 3 {
        RespWriter::new_ref(&mut output.payload).write_array_length(2);
      }

      RespWriter::new_ref(&mut output.payload).write_bulk_string(&element);

      if with_scores {
        write_double_numeric(output, score, resp_protocol_version);
      }
    }

    output.result1 = count;
  }

  /// ZREMRANGEBYLEX / ZLEXCOUNT：字典序区间移除或计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveOrCountRangeByLex
  pub(crate) fn sorted_set_remove_or_count_range_by_lex(
    &mut self,
    args: &[&[u8]],
    output: &mut ObjectOutput<'_>,
    op: SortedSetOperation,
  ) {
    // 以 i32::MIN 标记部分执行（resp 层据此中止）
    output.result1 = i32::MIN as i64;

    let min_param = args[0];
    let max_param = args[1];

    let is_remove = op == SortedSetOperation::Zremrangebylex;

    if is_remove {
      self.delete_expired_items();
    }

    let (hits, error_code) =
      self.get_elements_in_range_by_lex(min_param, max_param, false, false, (0, 0));

    output.result1 = error_code as i64;
    if error_code == 0 {
      let count = hits.len() as i64;
      // rem 删除语义移出扫描函数：借用收集命中即止（ZLEXCOUNT 仅计数不删），
      // ZREMRANGEBYLEX 仅对命中 member 做一次 to_vec 后删除，经 remove_entry 归还所有权
      if is_remove {
        let doomed: Vec<Vec<u8>> = hits.into_iter().map(|(_, m)| m.to_vec()).collect();
        for member in doomed {
          if let Some((m, score)) = self.sorted_set_dict.remove_entry(&member) {
            self.sorted_set.remove(&SortedSetEntry { score, member: m });
            self.try_remove_expiration(&member);
            self.update_size(&member, false);
          }
        }
      }
      output.result1 = count;
    }
  }

  /// ZRANK / ZREVRANK（arg1 == 1 时附带分值）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRank
  pub(crate) fn sorted_set_rank(
    &mut self,
    args: &[&[u8]],
    arg1: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
    ascending: bool,
  ) {
    let with_score = arg1 == 1;

    let member = args[0].to_vec();

    let Some(score) = self.try_get_score(&member) else {
      write_null(output, resp_protocol_version);
      return;
    };

    let mut rank = 0_i64;
    for item in &self.sorted_set {
      if self.is_expired(&item.member) {
        continue;
      }
      if item.member == member {
        break;
      }
      rank += 1;
    }

    if !ascending {
      rank = self.count() as i64 - rank - 1;
    }

    if with_score {
      RespWriter::new_ref(&mut output.payload).write_array_length(2);
      RespWriter::new_ref(&mut output.payload).write_int64(rank);
      write_double_numeric(output, score, resp_protocol_version);
    } else {
      RespWriter::new_ref(&mut output.payload).write_int64(rank);
    }
  }

  /// 弹出最低/最高分成员
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:PopMinOrMax
  pub fn pop_min_or_max(&mut self, pop_max_score_element: bool) -> Option<(f64, Vec<u8>)> {
    self.delete_expired_items();

    let element = if pop_max_score_element {
      self.sorted_set.pop_last()?
    } else {
      let first = self.sorted_set.first()?.clone();
      self.sorted_set.remove(&first);
      first
    };

    self.sorted_set_dict.remove(&element.member);
    self.try_remove_expiration(&element.member);
    self.update_size(&element.member, false);

    Some((element.score, element.member))
  }

  /// ZPOPMIN / ZPOPMAX（含 COUNT 形态；arg1 = -1 表示无计数的单元素形态）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetPopMinOrMaxCount
  pub(crate) fn sorted_set_pop_min_or_max_count(
    &mut self,
    _args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
    op: SortedSetOperation,
  ) {
    self.delete_expired_items();

    let mut count = arg1 as i64;
    let mut count_done = 0_i64;
    let mut with_header = true;

    if count == -1 {
      with_header = false;
      count = 1;
    }

    if (self.sorted_set.len() as i64) < count {
      count = self.sorted_set.len() as i64;
    }

    let resp_protocol_version = if arg2 > 0 {
      arg2 as u8
    } else {
      resp_protocol_version
    };

    if count == 0 {
      RespWriter::new_ref(&mut output.payload).write_empty_array();
      output.result1 = 0;
      return;
    }

    if with_header {
      if resp_protocol_version >= 3 {
        RespWriter::new_ref(&mut output.payload).write_array_length(count as usize);
      } else {
        RespWriter::new_ref(&mut output.payload).write_array_length((count * 2) as usize);
      }
    }

    let pop_max = op == SortedSetOperation::Zpopmax;
    while count > 0 {
      let Some((score, member)) = self.pop_min_or_max(pop_max) else {
        break;
      };

      if !with_header || resp_protocol_version >= 3 {
        RespWriter::new_ref(&mut output.payload).write_array_length(2);
      }

      RespWriter::new_ref(&mut output.payload).write_bulk_string(&member);
      write_double_numeric(output, score, resp_protocol_version);

      count_done += 1;
      count -= 1;
    }

    output.result1 = count_done;
  }

  /// ZPERSIST：批量清除成员过期
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetPersist
  pub(crate) fn sorted_set_persist(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    let num_fields = args.len();

    RespWriter::new_ref(&mut output.payload).write_array_length(num_fields);

    for &arg in args {
      let result = self.persist(arg);
      RespWriter::new_ref(&mut output.payload).write_int64(result as i64);
    }

    output.result1 = num_fields as i64;
  }

  /// ZTTL / ZEXPIRETIME（arg1 = 毫秒标记，arg2 = 时间戳标记）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetTimeToLive
  pub(crate) fn sorted_set_time_to_live(
    &mut self,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    let is_milliseconds = arg1 == 1;
    let is_timestamp = arg2 == 1;
    let num_fields = args.len();

    RespWriter::new_ref(&mut output.payload).write_array_length(num_fields);

    let now = now_ticks();
    for &member in args {
      let mut result = self.get_expiration(member);

      // 已成过去的过期视同不存在
      if result > 0 && self.is_expired(member) {
        result = -2;
      }

      if result >= 0 {
        result = if is_timestamp {
          if is_milliseconds {
            unix_time_in_milliseconds_from_ticks(result)
          } else {
            unix_time_in_seconds_from_ticks(result)
          }
        } else if is_milliseconds {
          milliseconds_from_diff_ticks(result, now)
        } else {
          seconds_from_diff_ticks(result, now)
        };
      }

      RespWriter::new_ref(&mut output.payload).write_int64(result);
    }

    output.result1 = num_fields as i64;
  }

  /// ZEXPIRE：批量设置成员过期（arg1/arg2 为 ExpirationWithOption 压缩字的高低半部）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetExpire
  pub(crate) fn sorted_set_expire(
    &mut self,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
  ) {
    self.delete_expired_items();

    let expiration_with_option = ExpirationWithOption::from_word_head_tail(arg1, arg2);

    RespWriter::new_ref(&mut output.payload).write_array_length(args.len());

    for &arg in args {
      let result = self.set_expiration(
        arg,
        expiration_with_option.expiration_time_in_ticks(),
        expiration_with_option.expire_option(),
      );
      RespWriter::new_ref(&mut output.payload).write_int64(result as i64);
    }

    output.result1 = args.len() as i64;
  }

  /// ZCOLLECT：占位收集操作（清除过期后确认存活）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetCollect
  pub fn sorted_set_collect(&mut self, output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    output.result1 = 1;
  }

  // ---- Common Methods ----

  /// 字典序区间取元素（纯查询：借用收集命中、range 下界裁剪去全量快照、正向 limit 迭代级
  /// 短路；rem 删除语义已移至调用点）
  ///
  /// 返回 (元素借用列表, 错误码)：解析失败 → `i32::MAX`；成功 → 0
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetElementsInRangeByLex
  pub(crate) fn get_elements_in_range_by_lex<'a>(
    &'a self,
    min_param: &[u8],
    max_param: &[u8],
    do_reverse: bool,
    valid_limit: bool,
    limit: (i64, i64),
  ) -> (Vec<(f64, &'a [u8])>, i32) {
    // 解析边界
    let (
      Some((mut min_value_chars, mut min_value_exclusive, mut min_value_infinity)),
      Some((mut max_value_chars, mut max_value_exclusive, mut max_value_infinity)),
    ) = (
      self.try_parse_lex_parameter(min_param),
      self.try_parse_lex_parameter(max_param),
    )
    else {
      return (Vec::new(), i32::MAX);
    };

    if do_reverse {
      swap(&mut min_value_chars, &mut max_value_chars);
      swap(&mut min_value_infinity, &mut max_value_infinity);
      swap(&mut min_value_exclusive, &mut max_value_exclusive);
    }

    if min_value_infinity == SpecialRanges::InfiniteMax
      || max_value_infinity == SpecialRanges::InfiniteMin
      || (valid_limit && (limit.0 < 0 || limit.1 == 0))
    {
      return (Vec::new(), 0);
    }

    let mut elements_in_lex: Vec<(f64, &'a [u8])> = Vec::new();
    // 空集合直接空结果（C# GetViewBetween 在空集/下界越集时抛 ArgumentException 被捕获，
    // 亦返回空列表）
    if let Some(first) = self.sorted_set.first() {
      // 对标 C# GetViewBetween((Min.Score, minValueChars), Max)：以 (first.score,
      // min_value_chars) 哨兵作 range 下界裁剪。条目序 (score, member) 与
      // SortedSetComparer 一致，被裁掉的 score==Min.Score 低成员必被下方 min 过滤
      // continue，逐条对位，结果不变（min 为无穷小时哨兵退化为 (Min.Score, 空) 不裁剪）。
      let start = SortedSetEntry {
        score: first.score,
        member: min_value_chars.to_vec(),
      };
      // 过滤链严格对位 C# 循环：过期 continue → min continue → max break
      let stream = self
        .sorted_set
        .range(start..)
        .filter(|x| !self.is_expired(&x.member))
        .filter(|x| {
          if min_value_infinity == SpecialRanges::InfiniteMin {
            return true;
          }
          let in_range = x.member.as_slice().cmp(min_value_chars);
          !(in_range == Ordering::Less || (in_range == Ordering::Equal && min_value_exclusive))
        })
        .take_while(|x| {
          if max_value_infinity == SpecialRanges::InfiniteMax {
            return true;
          }
          let out_range = x.member.as_slice().cmp(max_value_chars);
          !(out_range == Ordering::Greater || (out_range == Ordering::Equal && max_value_exclusive))
        })
        .map(|x| (x.score, x.member.as_slice()));

      let offset = limit.0.max(0) as usize;
      let take = if limit.1 >= 0 {
        limit.1 as usize
      } else {
        usize::MAX
      };

      elements_in_lex = if do_reverse {
        // 升序扫描无法在 reverse 输出上短路：先收集、倒序，再按 limit 切片（C# 同序）
        let mut all: Vec<(f64, &'a [u8])> = stream.collect();
        all.reverse();
        if valid_limit {
          all.into_iter().skip(offset).take(take).collect()
        } else {
          all
        }
      } else if valid_limit {
        // 正向 LIMIT：迭代级 skip/take 短路，无需全量收集
        stream.skip(offset).take(take).collect()
      } else {
        stream.collect()
      };
    }

    (elements_in_lex, 0)
  }

  /// 分值区间取元素（纯查询：借用收集命中、去逐命中 clone；rem 删除语义已移至调用点；
  /// do_reverse 交换边界并倒序输出）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetElementsInRangeByScore
  pub(crate) fn get_elements_in_range_by_score<'a>(
    &'a self,
    query: ScoreRangeQuery,
  ) -> Vec<(f64, &'a [u8])> {
    let mut min_value = query.min_value;
    let mut max_value = query.max_value;
    let mut min_exclusive = query.min_exclusive;
    let mut max_exclusive = query.max_exclusive;
    if query.do_reverse {
      swap(&mut min_value, &mut max_value);
      swap(&mut min_exclusive, &mut max_exclusive);
    }

    let mut scored_elements: Vec<(f64, &'a [u8])> = Vec::new();
    if query.valid_limit && (query.limit.0 < 0 || query.limit.1 == 0) {
      return scored_elements;
    }
    if let Some(max_entry) = self.sorted_set.last()
      && max_entry.score < min_value
    {
      return scored_elements;
    }

    // 对标 C# GetViewBetween((minValue, null), Max)：(min_value, 空 member) 哨兵作
    // range 下界；条目序 (score, member) 与 SortedSetComparer 一致，被裁项必被下方
    // min 过滤 continue，逐条对位，结果不变。
    let start = SortedSetEntry {
      score: min_value,
      member: Vec::new(),
    };
    for item in self.sorted_set.range(start..) {
      if self.is_expired(&item.member) {
        continue;
      }
      if item.score > max_value || (max_exclusive && item.score == max_value) {
        break;
      }
      if min_exclusive && item.score == min_value {
        continue;
      }
      scored_elements.push((item.score, item.member.as_slice()));
    }

    if query.do_reverse {
      scored_elements.reverse();
    }

    if query.valid_limit {
      let offset = if query.limit.0 > 0 {
        query.limit.0 as usize
      } else {
        0
      };
      let take = if query.limit.1 >= 0 {
        query.limit.1 as usize
      } else {
        scored_elements.len()
      };
      scored_elements = scored_elements
        .into_iter()
        .skip(offset)
        .take(take)
        .collect();
    }

    scored_elements
  }

  // ---- Helper Methods ----

  /// 解析分值区间参数：`(5` → 独占；支持 ±inf
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:TryParseParameter
  pub(crate) fn try_parse_parameter(val: &[u8]) -> Option<(f64, bool)> {
    let mut val = val;
    let mut exclusive = false;

    // 独占前缀
    if val.first() == Some(&b'(') {
      val = &val[1..];
      exclusive = true;
    }

    if let Some(value) = try_parse_with_infinity(val) {
      // ±inf 的独占语义退化为普通边界
      let exclusive = exclusive && !value.is_infinite();
      return Some((value, exclusive));
    }

    None
  }

  /// 解析字典序区间参数：`[a` 闭 / `(a` 开 / `-` 无穷小 / `+` 无穷大
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:TryParseLexParameter
  pub(crate) fn try_parse_lex_parameter<'p>(
    &self,
    val: &'p [u8],
  ) -> Option<(&'p [u8], bool, SpecialRanges)> {
    let mut limit_chars: &[u8] = &[];
    let mut limit_exclusive = false;
    let mut infinity = SpecialRanges::None;

    match val.first() {
      Some(b'-') => return Some((limit_chars, limit_exclusive, SpecialRanges::InfiniteMin)),
      Some(b'+') => return Some((limit_chars, limit_exclusive, SpecialRanges::InfiniteMax)),
      Some(b'[') => {
        limit_chars = &val[1..];
        limit_exclusive = false;
      }
      Some(b'(') => {
        limit_chars = &val[1..];
        limit_exclusive = true;
      }
      _ => return None,
    }

    // Redis 容忍 "[+" / "[-"，实际按无穷小处理
    if limit_chars.len() == 1 && (limit_chars[0] == b'-' || limit_chars[0] == b'+') {
      infinity = SpecialRanges::InfiniteMin;
      limit_chars = &[];
    }

    Some((limit_chars, limit_exclusive, infinity))
  }

  // ---- Scan（ZSCAN 分派） ----

  /// ZSCAN 的对象层入口（参数解析走 GarnetObjectBase::ReadScanInput 单点后
  /// 走 [`Self::scan`]；分值可空项的 null 回写与 HSCAN/SSCAN 分叉，故独立成体）。
  pub(crate) fn scan_operate(
    &mut self,
    args: &[&[u8]],
    limit_count_in_output: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) {
    let params = match read_scan_input(args, limit_count_in_output) {
      Ok(params) => params,
      Err(msg) => {
        RespWriter::new_ref(&mut output.payload).write_error_bytes(msg);
        return;
      }
    };

    let (items, cursor_output) = self.scan(
      params.cursor,
      params.count,
      params.pattern,
      params.is_no_value,
    );
    let items_len = items.len();

    RespWriter::new_ref(&mut output.payload).write_array_length(2);
    RespWriter::new_ref(&mut output.payload).write_int64_as_bulk_string(cursor_output);

    if items.is_empty() {
      RespWriter::new_ref(&mut output.payload).write_empty_array();
    } else {
      RespWriter::new_ref(&mut output.payload).write_array_length(items.len());
      for item in items {
        match item {
          Some(bytes) => RespWriter::new_ref(&mut output.payload).write_bulk_string(&bytes),
          // 对标 C#:Utf8Formatter 失败的 null 项回写
          None => write_null(output, resp_protocol_version),
        }
      }
    }

    output.result1 = items_len as i64;
  }
}
