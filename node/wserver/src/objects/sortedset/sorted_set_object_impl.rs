//! 有序集合 RESP 语义操作（对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs，
//! C# 为 SortedSetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use std::cmp::Ordering;
use std::mem::swap;

use crate::{
  inputs::ObjectInput,
  objects::{
    parse_utils::{
      equals_ignore_case, now_ticks, try_get_int, try_get_long, try_get_sorted_set_add_option,
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
/// ERR invalid cursor
pub(crate) const RESP_ERR_GENERIC_INVALIDCURSOR: &[u8] = b"ERR invalid cursor";

use crate::resp::cmd_strings::{
  RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_NOT_VALID_FLOAT,
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
  pub(crate) fn sorted_set_count(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
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
        let scored_elements = self.get_elements_in_range_by_score(
          min_value,
          max_value,
          min_exclusive,
          max_exclusive,
          options.reverse,
          options.valid_limit,
          false,
          options.limit,
        );
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
          let direction: Box<dyn Iterator<Item = &SortedSetEntry>> = if options.reverse {
            Box::new(self.sorted_set.iter().rev())
          } else {
            Box::new(self.sorted_set.iter())
          };

          let picked: Vec<(f64, Vec<u8>)> = direction
            .filter(|x| !self.is_expired(&x.member))
            .skip(min_index as usize)
            .take(n)
            .map(|x| (x.score, x.member.clone()))
            .collect();

          self.write_sorted_set_result(
            options.with_scores,
            n,
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
  pub(crate) fn write_sorted_set_result(
    &self,
    with_scores: bool,
    count: usize,
    resp_protocol_version: u8,
    iterator: impl Iterator<Item = (f64, Vec<u8>)>,
    output: &mut ObjectOutput,
  ) {
    if with_scores && resp_protocol_version >= 3 {
      output.write_array_length(count);

      for (score, element) in iterator {
        output.write_array_length(2);
        output.write_bulk_string(&element);
        output.write_double_numeric(score, resp_protocol_version);
      }
    } else {
      output.write_array_length(if with_scores { count * 2 } else { count });

      for (score, element) in iterator {
        output.write_bulk_string(&element);
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
    _resp_protocol_version: u8,
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
    _resp_protocol_version: u8,
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

    let removed = self.get_elements_in_range_by_score(
      min_value,
      max_value,
      min_exclusive,
      max_exclusive,
      false,
      false,
      true,
      (0, 0),
    );

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
  pub(crate) fn sorted_set_persist(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
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
  pub(crate) fn sorted_set_time_to_live(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
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
        // .NET Ticks → Unix 时间（Unix 纪元在 .NET Ticks 轴上为 621355968000000000）
        const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
        result = if is_timestamp {
          // 对标 ConvertUtils.UnixTimeIn{Milliseconds,Seconds}FromTicks：非正入参 → -1
          if result > 0 {
            if is_milliseconds {
              (result - UNIX_EPOCH_TICKS) / 10_000
            } else {
              (result - UNIX_EPOCH_TICKS) / 10_000_000
            }
          } else {
            -1
          }
        } else {
          // 对标 ConvertUtils.{Milliseconds,Seconds}FromDiffUtcNowTicks：
          // 差值非正 → -1；秒级四舍五入（+ TicksPerSecond/2 再除）
          let diff = result - now;
          if diff > 0 {
            if is_milliseconds {
              diff / 10_000
            } else {
              (diff + 5_000_000) / 10_000_000
            }
          } else {
            -1
          }
        };
      }

      output.write_int64(result);
    }

    output.result1 = num_fields as i64;
  }

  /// ZEXPIRE：批量设置成员过期（arg1/arg2 为 ExpirationWithOption 压缩字的高低半部）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetExpire
  pub(crate) fn sorted_set_expire(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    _resp_protocol_version: u8,
  ) {
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
          if in_range == Ordering::Less
            || (in_range == Ordering::Equal && min_value_exclusive)
          {
            continue;
          }
        }

        if max_value_infinity != SpecialRanges::InfiniteMax {
          let out_range = item.member.as_slice().cmp(max_value_chars);
          if out_range == Ordering::Greater
            || (out_range == Ordering::Equal && max_value_exclusive)
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
  ///
  /// C# 原签名 9 参（含未用的 withScore），1:1 保留形参规模
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn get_elements_in_range_by_score(
    &mut self,
    mut min_value: f64,
    mut max_value: f64,
    mut min_exclusive: bool,
    mut max_exclusive: bool,
    do_reverse: bool,
    valid_limit: bool,
    rem: bool,
    limit: (i64, i64),
  ) -> Vec<(f64, Vec<u8>)> {
    if do_reverse {
      swap(&mut min_value, &mut max_value);
      swap(&mut min_exclusive, &mut max_exclusive);
    }

    let mut scored_elements = Vec::new();
    if valid_limit && (limit.0 < 0 || limit.1 == 0) {
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

    if do_reverse {
      scored_elements.reverse();
    }

    if valid_limit {
      let offset = if limit.0 > 0 { limit.0 as usize } else { 0 };
      let take = if limit.1 >= 0 {
        limit.1 as usize
      } else {
        scored_elements.len()
      };
      scored_elements = scored_elements
        .into_iter()
        .skip(offset)
        .take(take)
        .collect();
    }

    if rem {
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

  /// ZSCAN 的对象层入口（解析光标/MATCH/COUNT/NOVALUES 后走 [`Self::scan`]）
  ///
  /// libs/server/Objects/Types/GarnetObjectBase.cs:Scan(ref ObjectInput, ...)
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
          output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR);
          return;
        }
      }
    } else {
      output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR);
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
      params.count.max(0) as usize,
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::objects::sortedsetgeo::geo_hash::{GeoDistanceUnitType, GeoHash};
  use crate::objects::sortedsetgeo::sorted_set_geo_object_impl::{
    GeoOriginType, GeoSearchOptions, GeoSearchType,
  };
  use crate::{
    arg_slice::ArgSlice,
    input_header::RespInputHeader,
    objects::sortedset::sorted_set_object::ExpireOption,
    session_parse_state::SessionParseState,
    types::{GarnetObjectType, RespInputFlags},
  };

  /// 构造 ObjectInput（backing 须与 input 同生命周期存活）
  fn make_input(
    op: SortedSetOperation,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
  ) -> (ObjectInput, Vec<Vec<u8>>) {
    let backing: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
    let slices: Vec<ArgSlice> = backing
      .iter()
      .map(|b| ArgSlice::new(b.as_ptr(), b.len()))
      .collect();
    let mut parse_state = SessionParseState::new();
    parse_state.initialize_with_args(&slices);

    let mut header =
      RespInputHeader::new_with_type(GarnetObjectType::SortedSet, RespInputFlags::empty());
    header.set_sub_id(op as u8);
    (
      ObjectInput::new_with_state(header, &mut parse_state, arg1, arg2),
      backing,
    )
  }

  fn seed(obj: &mut SortedSetObject, members: &[(&str, f64)]) {
    for (m, s) in members {
      obj.sorted_set_dict.insert(m.as_bytes().to_vec(), *s);
      obj.sorted_set.insert(SortedSetEntry {
        score: *s,
        member: m.as_bytes().to_vec(),
      });
    }
  }

  fn members_of(obj: &SortedSetObject) -> Vec<String> {
    let mut v: Vec<String> = obj
      .sorted_set_dict
      .keys()
      .map(|k| String::from_utf8(k.clone()).unwrap())
      .collect();
    v.sort();
    v
  }

  /// ZADD NX/GT/LT/CH/INCR 全组合语义
  #[test]
  fn zadd_option_matrix() {
    let mut obj = SortedSetObject::new();

    // 基础添加：返回新增数
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"10", b"a", b"20", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");

    // XX：只更新既有（c 不新增），CH 统计变更数
    let (input, _b) = make_input(
      SortedSetOperation::Zadd,
      &[b"XX", b"CH", b"15", b"a", b"99", b"c"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b":1\r\n");
    assert_eq!(obj.try_get_score(b"a"), Some(15.0));
    assert_eq!(obj.try_get_score(b"c"), None);

    // NX：不更新既有（a 保持 15），新增 d
    let (input, _b) = make_input(
      SortedSetOperation::Zadd,
      &[b"NX", b"99", b"a", b"5", b"d"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b":1\r\n");
    assert_eq!(obj.try_get_score(b"a"), Some(15.0));

    // GT：新分值更大才更新（15 → 20 更新；→ 10 拒绝）
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"GT", b"20", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(obj.try_get_score(b"a"), Some(20.0));

    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"GT", b"10", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(obj.try_get_score(b"a"), Some(20.0));

    // LT：新分值更小才更新
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"LT", b"10", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(obj.try_get_score(b"a"), Some(10.0));

    // 选项组合校验：XX+NX / GT+LT / INCR 多对
    for (args, expect) in [
      (
        vec![b"XX".to_vec(), b"NX".to_vec(), b"1".to_vec(), b"x".to_vec()],
        RESP_ERR_XX_NX_NOT_COMPATIBLE,
      ),
      (
        vec![b"GT".to_vec(), b"LT".to_vec(), b"1".to_vec(), b"x".to_vec()],
        RESP_ERR_GT_LT_NX_NOT_COMPATIBLE,
      ),
      (
        vec![b"NX".to_vec(), b"GT".to_vec(), b"1".to_vec(), b"x".to_vec()],
        RESP_ERR_GT_LT_NX_NOT_COMPATIBLE,
      ),
      (
        vec![
          b"INCR".to_vec(),
          b"1".to_vec(),
          b"x".to_vec(),
          b"2".to_vec(),
          b"y".to_vec(),
        ],
        RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR,
      ),
    ] {
      let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
      let (input, _b) = make_input(SortedSetOperation::Zadd, &refs, 0, 0);
      let mut out = ObjectOutput::new();
      obj.sorted_set_add(&input, &mut out, 2);
      assert_eq!(&out.payload[1..1 + expect.len()], expect);
    }

    // INCR 单对：返回叠加后的分值（RESP2 bulk string）
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"INCR", b"2.5", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b"$4\r\n12.5\r\n");

    // INCR 作用在不存在成员上：以增量值新增
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"INCR", b"3", b"new"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b"$1\r\n3\r\n");
    assert_eq!(obj.try_get_score(b"new"), Some(3.0));

    // INCR + NX 拒绝更新时返回 null
    let (input, _b) = make_input(
      SortedSetOperation::Zadd,
      &[b"INCR", b"NX", b"3", b"a"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b"$-1\r\n");

    // 非法 score
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"zzz", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_add(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR value is not a valid float\r\n");
  }

  /// ZSCORE / ZMSCORE / ZCARD / ZREM / ZINCRBY
  #[test]
  fn basic_ops() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0)]);

    let (input, _b) = make_input(SortedSetOperation::Zscore, &[b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_score(&input, &mut out, 2);
    assert_eq!(out.payload, b"$1\r\n2\r\n");

    let (input, _b) = make_input(SortedSetOperation::Zmscore, &[b"b", b"zz", b"c"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_scores(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n$1\r\n2\r\n$-1\r\n$1\r\n3\r\n");

    let (_input, _b) = make_input(SortedSetOperation::Zcard, &[], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_length(&mut out);
    assert_eq!(out.result1, 3);

    let (input, _b) = make_input(SortedSetOperation::Zrem, &[b"a", b"zz"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_remove(&input, &mut out);
    assert_eq!(out.result1, 1);
    assert_eq!(members_of(&obj), ["b", "c"]);

    let (input, _b) = make_input(SortedSetOperation::Zincrby, &[b"1.5", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b"$3\r\n3.5\r\n");
  }

  /// ZRANGE byRank/byScore/byLex + REV + LIMIT
  #[test]
  fn zrange_variants() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);

    // byRank 0 2
    let (input, _b) = make_input(SortedSetOperation::Zrange, &[b"0", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    // byRank 负索引 1 -1
    let (input, _b) = make_input(SortedSetOperation::Zrange, &[b"1", b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");

    // REV + WITHSCORES
    let (input, _b) = make_input(
      SortedSetOperation::Zrange,
      &[b"0", b"1", b"REV", b"WITHSCORES"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_range(&input, &mut out, 2);
    assert_eq!(
      out.payload,
      b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );

    // BYSCORE + LIMIT
    let (input, _b) = make_input(
      SortedSetOperation::Zrange,
      &[b"1", b"4", b"BYSCORE", b"LIMIT", b"1", b"2"],
      0,
      SortedSetRangeOpts::BY_SCORE.bits() as i32,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_range(&input, &mut out, 2);
    // [1..4] 全员 → LIMIT 1 2 跳过 a 取 b,c
    assert_eq!(out.payload, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");

    // BYLEX（同分字典序）
    let mut lex = SortedSetObject::new();
    seed(
      &mut lex,
      &[("apple", 0.0), ("banana", 0.0), ("cherry", 0.0)],
    );
    let (input, _b) = make_input(
      SortedSetOperation::Zrange,
      &[b"[a", b"(c", b"BYLEX"],
      0,
      SortedSetRangeOpts::BY_LEX.bits() as i32,
    );
    let mut out = ObjectOutput::new();
    lex.sorted_set_range(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n$5\r\napple\r\n$6\r\nbanana\r\n");

    // BYLEX 非法参数 → 回退后写错误，result1 = RangeError
    let (input, _b) = make_input(
      SortedSetOperation::Zrange,
      &[b"foo", b"bar", b"BYLEX"],
      0,
      SortedSetRangeOpts::BY_LEX.bits() as i32,
    );
    let mut out = ObjectOutput::new();
    lex.sorted_set_range(&input, &mut out, 2);
    assert_eq!(out.result1, RANGE_ERROR);
    assert!(
      out
        .payload
        .starts_with(b"-ERR min or max not valid string range item")
    );

    // byRank + LIMIT → LIMIT_NOT_SUPPORTED
    let (input, _b) = make_input(
      SortedSetOperation::Zrange,
      &[b"0", b"1", b"LIMIT", b"0", b"1"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_range(&input, &mut out, 2);
    assert_eq!(out.result1, RANGE_ERROR);
    assert!(
      out
        .payload
        .starts_with(b"-ERR syntax error, LIMIT is only supported")
    );
  }

  /// ZCOUNT / ZRANK / ZPOPMIN / ZPOPMAX
  #[test]
  fn count_rank_pop() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);

    let (input, _b) = make_input(SortedSetOperation::Zcount, &[b"(1", b"3"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_count(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");

    // ZRANK ascending
    let (input, _b) = make_input(SortedSetOperation::Zrank, &[b"c"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_rank(&input, &mut out, 2, true);
    assert_eq!(out.payload, b":2\r\n");

    // ZREVRANK：c 逆序排名第 1
    let (input, _b) = make_input(SortedSetOperation::Zrevrank, &[b"c"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_rank(&input, &mut out, 2, false);
    assert_eq!(out.payload, b":1\r\n");

    // ZPOPMIN（单元素形态：arg1 = -1）
    let (input, _b) = make_input(SortedSetOperation::Zpopmin, &[], -1, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_pop_min_or_max_count(&input, &mut out, 2, SortedSetOperation::Zpopmin);
    assert_eq!(out.payload, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");

    // ZPOPMAX 带 COUNT
    let (input, _b) = make_input(SortedSetOperation::Zpopmax, &[], 2, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_pop_min_or_max_count(&input, &mut out, 2, SortedSetOperation::Zpopmax);
    assert_eq!(
      out.payload,
      b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert!(obj.equals(&{
      let mut o = SortedSetObject::new();
      seed(&mut o, &[("b", 2.0)]);
      o
    }));
  }

  /// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZLEXCOUNT / ZREMRANGEBYLEX
  #[test]
  fn remove_ranges() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);

    // ZREMRANGEBYRANK 1 2
    let (input, _b) = make_input(SortedSetOperation::Zremrangebyrank, &[b"1", b"2"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_remove_range_by_rank(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");
    assert_eq!(members_of(&obj), ["a", "d"]);

    // 负索引全删：0 -1
    let (input, _b) = make_input(SortedSetOperation::Zremrangebyrank, &[b"0", b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_remove_range_by_rank(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");
    assert!(obj.sorted_set_dict.is_empty());

    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0)]);
    let (input, _b) = make_input(SortedSetOperation::Zremrangebyscore, &[b"(1", b"3"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_remove_range_by_score(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");
    assert_eq!(members_of(&obj), ["a"]);

    // ZLEXCOUNT
    let mut lex = SortedSetObject::new();
    seed(
      &mut lex,
      &[("apple", 0.0), ("banana", 0.0), ("cherry", 0.0)],
    );
    let (input, _b) = make_input(SortedSetOperation::Zlexcount, &[b"[a", b"[c"], 0, 0);
    let mut out = ObjectOutput::new();
    lex.sorted_set_remove_or_count_range_by_lex(&input, &mut out, SortedSetOperation::Zlexcount);
    assert_eq!(out.result1, 2);

    // ZREMRANGEBYLEX
    let (input, _b) = make_input(
      SortedSetOperation::Zremrangebylex,
      &[b"[a", b"[banana"],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    lex.sorted_set_remove_or_count_range_by_lex(
      &input,
      &mut out,
      SortedSetOperation::Zremrangebylex,
    );
    assert_eq!(out.result1, 2);
    assert_eq!(members_of(&lex), ["cherry"]);
  }

  /// ZINCRBY NaN、ZTTL/ZEXPIRE/ZPERSIST 家族
  #[test]
  fn expire_family() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0)]);

    // NaN 增量报错
    let (input, _b) = make_input(SortedSetOperation::Zincrby, &[b"inf", b"a"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_increment(&input, &mut out, 2);
    assert_eq!(out.payload, b"$3\r\ninf\r\n"); // inf + 1 = inf（合法）

    // ZEXPIRE：expiration+option 压缩字
    let exp = now_ticks() + 1_000_000;
    let e = ExpirationWithOption::new(
      exp,
      ExpireOption::NONE,
    );
    let (input, _b) = make_input(
      SortedSetOperation::Zexpire,
      &[b"a", b"zz"],
      (e.word() >> 32) as i32,
      e.word() as i32,
    );
    let mut out = ObjectOutput::new();
    obj.sorted_set_expire(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:1\r\n:-2\r\n");

    // ZTTL：剩余毫秒（arg1=1 毫秒）
    let (input, _b) = make_input(SortedSetOperation::Zttl, &[b"a"], 1, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_time_to_live(&input, &mut out, 2);
    // 1e6 ticks = 100ms（±1ms 抖动）
    let payload = String::from_utf8_lossy(&out.payload);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .and_then(|l| l.trim_start_matches(':').parse().ok())
      .unwrap_or(-999);
    assert!((90..=100).contains(&ttl), "{payload}");

    // ZPERSIST
    let (input, _b) = make_input(SortedSetOperation::Zpersist, &[b"a", b"b"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_persist(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n:1\r\n:-1\r\n");
    assert!(!obj.has_expirable_items());
  }

  /// ZRANDMEMBER（确定性种子）
  #[test]
  fn zrandmember() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);

    // arg1 = (count << 2) | (includedCount << 1) | withScores
    let count = 2_i64;
    let included_count = true;
    let with_scores = false;
    let arg1 = (((count << 1) | included_count as i64) << 1) | with_scores as i64;
    let (input, _b) = make_input(SortedSetOperation::Zrandmember, &[], arg1 as i32, 42);
    let mut out = ObjectOutput::new();
    obj.sorted_set_random_member(&input, &mut out, 2);
    // 数组头 + 2 个不重复成员
    assert!(out.payload.starts_with(b"*2\r\n"));
    assert!(out.payload.windows(4).any(|w| w == b"$1\r\n"));
  }

  /// ZSCAN 光标与 MATCH/COUNT/NOVALUES
  #[test]
  fn zscan_flow() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("one", 1.0), ("two", 2.0), ("three", 3.0)]);

    let (input, _b) = make_input(SortedSetOperation::Zscan, &[b"0", b"MATCH", b"t*"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.scan_operate(&input, &mut out, 2);
    // 匹配顺序随散列迭代序（与 C# Dictionary 一致），仅校验内容完整性
    let payload = String::from_utf8_lossy(&out.payload);
    assert!(payload.starts_with("*2\r\n$1\r\n0\r\n*4\r\n"), "{payload}");
    for (member, score) in [("two", "2"), ("three", "3")] {
      assert!(
        payload.contains(&format!("${}\r\n{}\r\n", member.len(), member)),
        "{payload}"
      );
      assert!(
        payload.contains(&format!("$1\r\n{}\r\n", score)),
        "{payload}"
      );
    }

    // 非法光标
    let (input, _b) = make_input(SortedSetOperation::Zscan, &[b"-1"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.scan_operate(&input, &mut out, 2);
    assert_eq!(out.payload, b"-ERR invalid cursor\r\n");
  }

  /// GEOADD / GEOHASH / GEODIST / GEOPOS 对象层语义
  #[test]
  fn geo_ops() {
    let mut obj = SortedSetObject::new();

    // GEOADD lon lat member ...
    let (input, _b) = make_input(
      SortedSetOperation::Geoadd,
      &[
        b"-122.4194",
        b"37.7749",
        b"sf",
        b"2.3522",
        b"48.8566",
        b"paris",
      ],
      0,
      0,
    );
    let mut out = ObjectOutput::new();
    obj.geo_add(&input, &mut out, 2);
    assert_eq!(out.payload, b":2\r\n");

    // GEOHASH sf → 9q8yy 开头（last char 恒为 '0'）
    let (input, _b) = make_input(SortedSetOperation::Geohash, &[b"sf", b"missing"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.geo_hash(&input, &mut out, 2);
    assert_eq!(out.payload, b"*2\r\n$11\r\n9q8yyk8ytp0\r\n$-1\r\n");

    // GEODIST sf paris（km）≈ 8967 km
    let (input, _b) = make_input(SortedSetOperation::Geodist, &[b"sf", b"paris", b"km"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.geo_distance(&input, &mut out, 2);
    let payload = String::from_utf8_lossy(&out.payload);
    let dist: f64 = payload.lines().nth(1).unwrap().parse().unwrap();
    assert!((dist - 8967.0).abs() < 30.0, "{dist}");

    // GEOPOS
    let (input, _b) = make_input(SortedSetOperation::Geopos, &[b"sf"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.geo_position(&input, &mut out, 2);
    let payload = String::from_utf8(out.payload.clone()).unwrap();
    assert!(payload.contains("-122.4"), "{payload}");

    // GEOSEARCH：SF 半径 100km 内只有 sf 自己
    let mut opts = GeoSearchOptions {
      search_type:
        GeoSearchType::ByRadius,
      unit: GeoDistanceUnitType::Km,
      radius: 100.0,
      origin: GeoOriginType::FromLonLat,
      lon: -122.4194,
      lat: 37.7749,
      with_dist: true,
      ..Default::default()
    };
    let mut out = ObjectOutput::new();
    obj.geo_search(&mut opts, &mut out, 2, true);
    // 分值经 GeoHash 量化，圆心到自身距离为厘米级（量化重建误差），非精确 0
    let payload = String::from_utf8_lossy(&out.payload);
    assert!(
      payload.starts_with("*1\r\n*2\r\n$2\r\nsf\r\n$"),
      "{payload}"
    );
    let dist: f64 = payload.lines().nth(5).unwrap().parse().unwrap();
    assert!(dist < 0.001, "{dist}");

    // FROMMEMBER 缺失成员 → 错误
    opts.origin =
      GeoOriginType::FromMember;
    opts.from_member = b"missing".to_vec();
    let mut out = ObjectOutput::new();
    obj.geo_search(&mut opts, &mut out, 2, true);
    assert_eq!(
      out.payload,
      b"-ERR could not decode requested zset member\r\n"
    );

    // 分值即 GeoHash 整数：解码坐标与原始输入差在量化误差内
    let sf_score = obj.try_get_score(b"sf").unwrap() as i64;
    let (lat, lon) = GeoHash::get_coordinates_from_long(sf_score);
    assert!((lat - 37.7749).abs() < 1e-4, "{lat}");
    assert!((lon - -122.4194).abs() < 1e-4, "{lon}");
  }

  /// operate 分派冒烟：ZADD → 空集合 REMOVE_KEY 标记
  #[test]
  fn operate_dispatch_smoke() {
    let mut obj = SortedSetObject::new();
    let (input, _b) = make_input(SortedSetOperation::Zadd, &[b"1", b"x"], 0, 0);
    let mut out = ObjectOutput::new();
    assert!(obj.operate(&input, &mut out, 2));
    assert_eq!(out.payload, b":1\r\n");

    // 删空后 REMOVE_KEY
    let (input, _b) = make_input(SortedSetOperation::Zrem, &[b"x"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.operate(&input, &mut out, 2);
    assert!(out.has_remove_key());
  }

  /// CopyDiff/InPlaceDiff 已在对象层测试覆盖；此处补 ZCARD 与 score 解析的 ±inf
  #[test]
  fn infinity_score_parsing() {
    let mut obj = SortedSetObject::new();
    seed(&mut obj, &[("x", 5.0)]);

    let (input, _b) = make_input(SortedSetOperation::Zcount, &[b"-inf", b"+inf"], 0, 0);
    let mut out = ObjectOutput::new();
    obj.sorted_set_count(&input, &mut out, 2);
    assert_eq!(out.payload, b":1\r\n");
  }
}
