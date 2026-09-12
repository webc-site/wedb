//! 有序集合 RESP 语义操作（对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs，
//! C# 为 SortedSetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use std::{cmp::Ordering, mem::swap};

use wbase::{
  convert::{
    milliseconds_from_diff_ticks, seconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks,
    unix_time_in_seconds_from_ticks,
  },
  time::now_ticks,
};

use crate::{
  inputs::ObjectInput,
  objects::{
    parse_utils::{
      equals_ignore_case, try_get_int, try_get_long, try_get_sorted_set_add_option,
      try_parse_with_infinity,
    },
    sortedset::sorted_set_object::{
      ExpirationWithOption, SortedSetAddOption, SortedSetEntry, SortedSetObject,
      SortedSetOperation, SortedSetRangeOpts,
    },
    types::object_output::ObjectOutput,
  },
};

// ---- CmdStrings 中 ZRANGE 族专用错误串（cmd_strings.rs 不在本周期改动范围） ----

/// ERR XX and NX options at the same time are not compatible
pub(crate) const RESP_ERR_XX_NX_NOT_COMPATIBLE: &[u8] =
  b"ERR XX and NX options at the same time are not compatible";
/// ERR GT, LT, and/or NX options at the same time are not compatible
pub(crate) const RESP_ERR_GT_LT_NX_NOT_COMPATIBLE: &[u8] =
  b"ERR GT, LT, and/or NX options at the same time are not compatible";
/// ERR INCR option supports a single increment-element pair
pub(crate) const RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR: &[u8] =
  b"ERR INCR option supports a single increment-element pair";
/// ERR min or max is not a float
pub(crate) const RESP_ERR_MIN_MAX_NOT_VALID_FLOAT: &[u8] = b"ERR min or max is not a float";
/// ERR min or max not valid string range item
pub(crate) const RESP_ERR_MIN_MAX_NOT_VALID_STRING: &[u8] =
  b"ERR min or max not valid string range item";
/// ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX
pub(crate) const RESP_ERR_LIMIT_NOT_SUPPORTED: &[u8] =
  b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX";
/// ERR resulting score is not a number (NaN)
pub(crate) const RESP_ERR_GENERIC_SCORE_NAN: &[u8] = b"ERR resulting score is not a number (NaN)";

use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_NOT_VALID_FLOAT,
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
  pub rem: bool,
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

/// ZSCAN 解析结果
#[derive(Debug, Clone, Default)]
struct ScanParams<'p> {
  cursor: i64,
  pattern: &'p [u8],
  count: i64,
  is_no_value: bool,
}

/// 取第 i 个参数字节
///
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

impl SortedSetObject {
  /// 解析并校验 ZADD 选项组合；失败时写错误并返回 None
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions
  pub(crate) fn get_options(
    &self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    curr_token_idx: &mut usize,
  ) -> Option<SortedSetAddOption> {
    let mut options = SortedSetAddOption::NONE;

    while *curr_token_idx < input.parse_state.count {
      let Some(curr_option) = try_get_sorted_set_add_option(arg(input, *curr_token_idx)) else {
        break;
      };
      options |= curr_option;
      *curr_token_idx += 1;
    }

    // XX 与 NX 互斥
    let mut options_error: &[u8] = &[];
    if options.contains(SortedSetAddOption::XX) && options.contains(SortedSetAddOption::NX) {
      options_error = RESP_ERR_XX_NX_NOT_COMPATIBLE;
    }

    // NX、GT、LT 两两互斥
    if options.contains(SortedSetAddOption::GT) && options.contains(SortedSetAddOption::LT)
      || ((options.contains(SortedSetAddOption::GT) || options.contains(SortedSetAddOption::LT))
        && options.contains(SortedSetAddOption::NX))
    {
      options_error = RESP_ERR_GT_LT_NX_NOT_COMPATIBLE;
    }

    // INCR 仅支持单对 score-element
    if options.contains(SortedSetAddOption::INCR) && input.parse_state.count - *curr_token_idx > 2 {
      options_error = RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR;
    }

    if !options_error.is_empty() {
      output.write_error(options_error);
      return None;
    }

    // 剩余 token 须为正数对（偶数个）
    if *curr_token_idx == input.parse_state.count
      || !(input.parse_state.count - *curr_token_idx).is_multiple_of(2)
    {
      output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      return None;
    }

    Some(options)
  }

  /// ZADD：带 XX/NX/GT/LT/CH/INCR 全选项的批量添加
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd
  pub(crate) fn sorted_set_add(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    let mut added_or_changed = 0_i64;
    let mut incr_result = 0_f64;

    let mut options = SortedSetAddOption::NONE;
    let mut curr_token_idx = 0;
    let mut parsed_options = false;

    let count = input.parse_state.count;

    while curr_token_idx < count {
      // 先尝试解析 score；非 score 则先吃掉选项段
      let Some(score) = try_parse_with_infinity(arg(input, curr_token_idx)) else {
        if !parsed_options {
          parsed_options = true;
          let Some(opts) = self.get_options(input, output, &mut curr_token_idx) else {
            return;
          };
          options = opts;
          continue; // 选项解析完重试当前 token
        }
        output.write_error(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
        return;
      };

      parsed_options = true;
      curr_token_idx += 1;

      // member（命令层保证 score-member 成对；奇数尾巴防御性截断，C# 为越界读）
      if curr_token_idx >= count {
        break;
      }
      let member = arg(input, curr_token_idx).to_vec();
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
              output.write_error(RESP_ERR_GENERIC_SCORE_NAN);
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
              output.write_null(resp_protocol_version);
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
      output.write_double_numeric(incr_result, resp_protocol_version);
    } else {
      output.write_int64(added_or_changed);
    }
  }

  /// ZREM：批量移除成员
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemove
  pub(crate) fn sorted_set_remove(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    self.delete_expired_items();

    let mut removed = 0_i64;

    for i in 0..input.parse_state.count {
      let value = arg(input, i);
      let Some(score) = self.sorted_set_dict.remove(value) else {
        continue;
      };

      removed += 1;
      self.sorted_set.remove(&SortedSetEntry {
        score,
        member: value.to_vec(),
      });
      self.try_remove_expiration(value);

      self.update_size(value, false);
    }

    output.result1 = removed;
  }

  /// ZCARD：成员计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetLength
  pub(crate) fn sorted_set_length(&mut self, output: &mut ObjectOutput) {
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
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let member = arg(input, 0);

    match self.try_get_score(member) {
      None => output.write_null(resp_protocol_version),
      Some(score) => output.write_double_numeric(score, resp_protocol_version),
    }
    output.result1 = 1;
  }

  /// ZMSCORE：多成员分值
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetScores
  pub(crate) fn sorted_set_scores(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let count = input.parse_state.count;

    output.write_array_length(count);

    for i in 0..count {
      let member = arg(input, i);
      match self.try_get_score(member) {
        None => output.write_null(resp_protocol_version),
        Some(score) => output.write_double_numeric(score, resp_protocol_version),
      }
    }

    output.result1 = count as i64;
  }

  /// ZCOUNT：分值区间成员计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetCount
  pub(crate) fn sorted_set_count(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let min_param = arg(input, 0);
    let max_param = arg(input, 1);

    let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
      Self::try_parse_parameter(min_param),
      Self::try_parse_parameter(max_param),
    ) else {
      output.write_error(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
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

    output.write_int64(count);
  }

  /// ZINCRBY：分值增量
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetIncrement
  pub(crate) fn sorted_set_increment(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    self.delete_expired_items();

    // RESP2 兼容读回场景的协议覆盖（C# input.arg2 > 0 时强制）
    let resp_protocol_version = if input.arg2 > 0 {
      input.arg2 as u8
    } else {
      resp_protocol_version
    };

    let Some(incr_value) = try_parse_with_infinity(arg(input, 0)) else {
      output.write_error(RESP_ERR_NOT_VALID_FLOAT.as_bytes());
      return;
    };

    let member = arg(input, 1).to_vec();

    let new_score = match self.sorted_set_dict.get(&member).copied() {
      Some(score) => {
        let result = score + incr_value;
        if result.is_nan() {
          output.write_error(RESP_ERR_GENERIC_SCORE_NAN);
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

    output.write_double_numeric(new_score, resp_protocol_version);
  }

  /// ZRANGE / ZRANGEBYSCORE / ZRANGEBYLEX / ZREVRANGE 族统一入口
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRange
  pub(crate) fn sorted_set_range(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let range_opts = SortedSetRangeOpts::from_bits_truncate(input.arg2 as u8);
    let count = input.parse_state.count;

    // C# 两个区间块均要求 count >= 2，不足时无任何输出（命令层保证 ≥2）
    if count < 2 {
      return;
    }

    let mut curr_idx = 0;

    let min_span = arg(input, curr_idx);
    curr_idx += 1;
    let max_span = arg(input, curr_idx);
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
        let token = arg(input, curr_idx);
        curr_idx += 1;

        if equals_ignore_case(token, b"BYSCORE") {
          options.by_score = true;
        } else if equals_ignore_case(token, b"BYLEX") {
          options.by_lex = true;
        } else if equals_ignore_case(token, b"REV") {
          options.reverse = true;
        } else if equals_ignore_case(token, b"LIMIT") {
          // LIMIT 后须有 offset count 两个 token
          if input.parse_state.count - curr_idx < 2 {
            output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
            output.result1 = RANGE_ERROR;
            return;
          }

          let (Some(offset), Some(count_limit)) = (
            try_get_int(arg(input, curr_idx)).map(|v| v as i64),
            try_get_int(arg(input, curr_idx + 1)).map(|v| v as i64),
          ) else {
            output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
            output.result1 = RANGE_ERROR;
            return;
          };
          curr_idx += 2;

          options.limit = (offset, count_limit);
          options.valid_limit = true;
        } else if equals_ignore_case(token, b"WITHSCORES") {
          options.with_scores = true;
        }
      }
    }

    if count >= 2 && ((!options.by_score && !options.by_lex) || options.by_score) {
      let Some((min_value, min_exclusive)) = Self::try_parse_parameter(min_span) else {
        output.write_error(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
        output.result1 = RANGE_ERROR;
        return;
      };
      let Some((max_value, max_exclusive)) = Self::try_parse_parameter(max_span) else {
        output.write_error(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
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
          rem: false,
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
          output.write_error(RESP_ERR_LIMIT_NOT_SUPPORTED);
          output.result1 = RANGE_ERROR;
          return;
        } else if min_value > (set_count as f64) - 1.0 {
          // 空结果
          output.write_empty_array();
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
            output.write_empty_array();
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
        false,
        options.limit,
      );

      if error_code == i32::MAX {
        // BYSCORE 与 BYLEX 相互独立："ZRANGE k 1 3 BYSCORE BYLEX" 时上方已写过
        // 数组回复，须回退到本命令负载起点再写错误（两份回复会令 RESP 流失步；
        // 对标 writer.ResetPosition）
        output.payload.clear();
        output.write_error(RESP_ERR_MIN_MAX_NOT_VALID_STRING);
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
    output: &mut ObjectOutput,
  ) {
    if with_scores && resp_protocol_version >= 3 {
      output.write_array_length(count);

      for (score, element) in iterator {
        output.write_array_length(2);
        output.write_bulk_string(element.as_ref());
        output.write_double_numeric(score, resp_protocol_version);
      }
    } else {
      output.write_array_length(if with_scores { count * 2 } else { count });

      for (score, element) in iterator {
        output.write_bulk_string(element.as_ref());
        if with_scores {
          output.write_double_bulk_string(score);
        }
      }
    }
  }

  /// ZREMRANGEBYRANK：按排名区间移除
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveRangeByRank
  pub(crate) fn sorted_set_remove_range_by_rank(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
  ) {
    self.delete_expired_items();

    let (Some(start), Some(stop)) = (
      try_get_int(arg(input, 0)).map(|v| v as i64),
      try_get_int(arg(input, 1)).map(|v| v as i64),
    ) else {
      output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
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
      output.write_int64(0);
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

    output.write_int64(element_count as i64);
  }

  /// ZREMRANGEBYSCORE：按分值区间移除
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveRangeByScore
  pub(crate) fn sorted_set_remove_range_by_score(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
  ) {
    self.delete_expired_items();

    let min_param = arg(input, 0);
    let max_param = arg(input, 1);

    let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
      Self::try_parse_parameter(min_param),
      Self::try_parse_parameter(max_param),
    ) else {
      output.write_error(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
      return;
    };

    let removed = self.get_elements_in_range_by_score(ScoreRangeQuery {
      min_value,
      max_value,
      min_exclusive,
      max_exclusive,
      do_reverse: false,
      valid_limit: false,
      rem: true,
      limit: (0, 0),
    });

    output.write_int64(removed.len() as i64);
  }

  /// ZRANDMEMBER：随机成员（arg1 打包 count/withScores/includedCount，arg2 为种子）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRandomMember
  pub(crate) fn sorted_set_random_member(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let mut count = (input.arg1 >> 2) as i64;
    let with_scores = (input.arg1 & 1) == 1;
    let included_count = ((input.arg1 >> 1) & 1) == 1;
    let seed = input.arg2 as u32;
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
      output.write_array_length(array_length as usize);
    }
    let index_count = count.unsigned_abs() as usize;

    // 随机下标采样：count > 0 不放回（Fisher-Yates 部分洗牌，对标
    // RandomUtils.PickKRandomIndexes 的 unique 路径），负数放回重复抽取
    let mut rng = fastrand::Rng::with_seed(u64::from(seed));
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
        output.write_array_length(2);
      }

      output.write_bulk_string(&element);

      if with_scores {
        output.write_double_numeric(score, resp_protocol_version);
      }
    }

    output.result1 = count;
  }

  /// ZREMRANGEBYLEX / ZLEXCOUNT：字典序区间移除或计数
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemoveOrCountRangeByLex
  pub(crate) fn sorted_set_remove_or_count_range_by_lex(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    op: SortedSetOperation,
  ) {
    // 以 i32::MIN 标记部分执行（resp 层据此中止）
    output.result1 = i32::MIN as i64;

    let min_param = arg(input, 0);
    let max_param = arg(input, 1);

    let is_remove = op == SortedSetOperation::Zremrangebylex;

    if is_remove {
      self.delete_expired_items();
    }

    let (rem, error_code) =
      self.get_elements_in_range_by_lex(min_param, max_param, false, false, is_remove, (0, 0));

    output.result1 = error_code as i64;
    if error_code == 0 {
      output.result1 = rem.len() as i64;
    }
  }

  /// ZRANK / ZREVRANK（arg1 == 1 时附带分值）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRank
  pub(crate) fn sorted_set_rank(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
    ascending: bool,
  ) {
    let with_score = input.arg1 == 1;

    let member = arg(input, 0).to_vec();

    let Some(score) = self.try_get_score(&member) else {
      output.write_null(resp_protocol_version);
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
      output.write_array_length(2);
      output.write_int64(rank);
      output.write_double_numeric(score, resp_protocol_version);
    } else {
      output.write_int64(rank);
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
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
    op: SortedSetOperation,
  ) {
    self.delete_expired_items();

    let mut count = input.arg1 as i64;
    let mut count_done = 0_i64;
    let mut with_header = true;

    if count == -1 {
      with_header = false;
      count = 1;
    }

    if (self.sorted_set.len() as i64) < count {
      count = self.sorted_set.len() as i64;
    }

    let resp_protocol_version = if input.arg2 > 0 {
      input.arg2 as u8
    } else {
      resp_protocol_version
    };

    if count == 0 {
      output.write_empty_array();
      output.result1 = 0;
      return;
    }

    if with_header {
      if resp_protocol_version >= 3 {
        output.write_array_length(count as usize);
      } else {
        output.write_array_length((count * 2) as usize);
      }
    }

    let pop_max = op == SortedSetOperation::Zpopmax;
    while count > 0 {
      let Some((score, member)) = self.pop_min_or_max(pop_max) else {
        break;
      };

      if !with_header || resp_protocol_version >= 3 {
        output.write_array_length(2);
      }

      output.write_bulk_string(&member);
      output.write_double_numeric(score, resp_protocol_version);

      count_done += 1;
      count -= 1;
    }

    output.result1 = count_done;
  }

  /// ZPERSIST：批量清除成员过期
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetPersist
  pub(crate) fn sorted_set_persist(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    self.delete_expired_items();

    let num_fields = input.parse_state.count;

    output.write_array_length(num_fields);

    for i in 0..num_fields {
      let result = self.persist(arg(input, i));
      output.write_int64(result as i64);
    }

    output.result1 = num_fields as i64;
  }

  /// ZTTL / ZEXPIRETIME（arg1 = 毫秒标记，arg2 = 时间戳标记）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetTimeToLive
  pub(crate) fn sorted_set_time_to_live(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    let is_milliseconds = input.arg1 == 1;
    let is_timestamp = input.arg2 == 1;
    let num_fields = input.parse_state.count;

    output.write_array_length(num_fields);

    let now = now_ticks();
    for i in 0..num_fields {
      let member = arg(input, i);
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

      output.write_int64(result);
    }

    output.result1 = num_fields as i64;
  }

  /// ZEXPIRE：批量设置成员过期（arg1/arg2 为 ExpirationWithOption 压缩字的高低半部）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetExpire
  pub(crate) fn sorted_set_expire(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    self.delete_expired_items();

    let expiration_with_option = ExpirationWithOption::from_word_head_tail(input.arg1, input.arg2);

    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let result = self.set_expiration(
        arg(input, i),
        expiration_with_option.expiration_time_in_ticks(),
        expiration_with_option.expire_option(),
      );
      output.write_int64(result as i64);
    }

    output.result1 = input.parse_state.count as i64;
  }

  /// ZCOLLECT：占位收集操作（清除过期后确认存活）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetCollect
  pub(crate) fn sorted_set_collect(&mut self, output: &mut ObjectOutput) {
    self.delete_expired_items();

    output.result1 = 1;
  }

  // ---- Common Methods ----

  /// 字典序区间取元素（rem=true 时移除）
  ///
  /// 返回 (元素列表, 错误码)：解析失败 → `i32::MAX`；成功 → 0
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetElementsInRangeByLex
  pub(crate) fn get_elements_in_range_by_lex(
    &mut self,
    min_param: &[u8],
    max_param: &[u8],
    do_reverse: bool,
    valid_limit: bool,
    rem: bool,
    limit: (i64, i64),
  ) -> (Vec<(f64, Vec<u8>)>, i32) {
    let mut elements_in_lex = Vec::new();

    // 解析边界
    let (
      Some((mut min_value_chars, mut min_value_exclusive, mut min_value_infinity)),
      Some((mut max_value_chars, mut max_value_exclusive, mut max_value_infinity)),
    ) = (
      self.try_parse_lex_parameter(min_param),
      self.try_parse_lex_parameter(max_param),
    )
    else {
      return (elements_in_lex, i32::MAX);
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
      return (elements_in_lex, 0);
    }

    // C# 以 GetViewBetween + ArgumentException 表达空集合/下界越集；等价地全序扫描
    if !self.sorted_set.is_empty() {
      let snapshot: Vec<SortedSetEntry> = self.sorted_set.iter().cloned().collect();
      for item in snapshot {
        if self.is_expired(&item.member) {
          continue;
        }

        if min_value_infinity != SpecialRanges::InfiniteMin {
          let in_range = item.member.as_slice().cmp(min_value_chars);
          if in_range == Ordering::Less || (in_range == Ordering::Equal && min_value_exclusive) {
            continue;
          }
        }

        if max_value_infinity != SpecialRanges::InfiniteMax {
          let out_range = item.member.as_slice().cmp(max_value_chars);
          if out_range == Ordering::Greater || (out_range == Ordering::Equal && max_value_exclusive)
          {
            break;
          }
        }

        if rem && self.sorted_set_dict.remove(&item.member).is_some() {
          self.sorted_set.remove(&item);
          self.try_remove_expiration(&item.member);
          self.update_size(&item.member, false);
        }
        elements_in_lex.push((item.score, item.member));
      }
    }

    if do_reverse {
      elements_in_lex.reverse();
    }

    if valid_limit {
      let offset = if limit.0 > 0 { limit.0 as usize } else { 0 };
      let take = if limit.1 >= 0 {
        limit.1 as usize
      } else {
        elements_in_lex.len()
      };
      elements_in_lex = elements_in_lex
        .into_iter()
        .skip(offset)
        .take(take)
        .collect();
    }

    (elements_in_lex, 0)
  }

  /// 分值区间取元素（rem=true 时移除；do_reverse 交换边界并倒序输出）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetElementsInRangeByScore
  pub(crate) fn get_elements_in_range_by_score(
    &mut self,
    query: ScoreRangeQuery,
  ) -> Vec<(f64, Vec<u8>)> {
    let mut min_value = query.min_value;
    let mut max_value = query.max_value;
    let mut min_exclusive = query.min_exclusive;
    let mut max_exclusive = query.max_exclusive;
    if query.do_reverse {
      swap(&mut min_value, &mut max_value);
      swap(&mut min_exclusive, &mut max_exclusive);
    }

    let mut scored_elements = Vec::new();
    if query.valid_limit && (query.limit.0 < 0 || query.limit.1 == 0) {
      return scored_elements;
    }
    if let Some(max_entry) = self.sorted_set.last()
      && max_entry.score < min_value
    {
      return scored_elements;
    }

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
      scored_elements.push((item.score, item.member.clone()));
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

    if query.rem {
      for (score, member) in scored_elements.clone() {
        if self.sorted_set_dict.remove(&member).is_some() {
          self.sorted_set.remove(&SortedSetEntry {
            score,
            member: member.clone(),
          });
          self.try_remove_expiration(&member);
          self.update_size(&member, false);
        }
      }
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

  /// ZSCAN 的对象层入口（解析光标/MATCH/COUNT/NOVALUES 后走 [`Self::scan`]）。
  pub(crate) fn scan_operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    // 单轮最多返回的条目数由调用方经 arg2 下发
    let limit_count_in_output = input.arg2 as i64;

    // 默认 COUNT
    let mut params = ScanParams {
      cursor: 0,
      pattern: &[],
      count: 10,
      is_no_value: false,
    };

    if input.parse_state.count > 0 {
      match try_get_long(arg(input, 0)) {
        Some(c) if c >= 0 => params.cursor = c,
        _ => {
          output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
          return;
        }
      }
    } else {
      output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
      return;
    }

    let mut curr_token_idx = 1;
    while curr_token_idx < input.parse_state.count {
      let param = arg(input, curr_token_idx);
      curr_token_idx += 1;

      if equals_ignore_case(param, b"MATCH") {
        if curr_token_idx >= input.parse_state.count {
          output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
          return;
        }
        params.pattern = arg(input, curr_token_idx);
        curr_token_idx += 1;
      } else if equals_ignore_case(param, b"COUNT") {
        if curr_token_idx >= input.parse_state.count {
          output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
          return;
        }
        match try_get_int(arg(input, curr_token_idx)) {
          Some(c) => {
            curr_token_idx += 1;
            params.count = c as i64;
            // 无条件钳制到输出上限（对标 C# countInInput > limitCountInOutput）
            if params.count > limit_count_in_output {
              params.count = limit_count_in_output;
            }
          }
          None => {
            output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
            return;
          }
        }
      } else if equals_ignore_case(param, b"NOVALUES") {
        params.is_no_value = true;
      }
    }

    let (items, cursor_output) = self.scan(
      params.cursor,
      params.count,
      params.pattern,
      params.is_no_value,
    );
    let items_len = items.len();

    output.write_array_length(2);
    output.write_int64_as_bulk_string(cursor_output);

    if items.is_empty() {
      output.write_empty_array();
    } else {
      output.write_array_length(items.len());
      for item in items {
        match item {
          Some(bytes) => output.write_bulk_string(&bytes),
          // 对标 C#:Utf8Formatter 失败的 null 项回写
          None => output.write_null(resp_protocol_version),
        }
      }
    }

    output.result1 = items_len as i64;
  }
}
