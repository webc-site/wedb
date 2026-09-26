//! 有序集合 RESP 语义操作（对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs，
//! C# 为 SortedSetObject 的 partial 分片；Rust 侧以同 crate 跨模块 impl 承载）
//!
//! RESP 负载经 [`ObjectOutput`] 输出；操作计数经 `result1` 回传。

use std::{cmp::Ordering, mem::swap, sync::Arc};

use wbase::{
  num::{strict_f64, strict_i32},
  time::now_ticks,
};
use wresp::{
  cmd_strings::{
    self, RESP_ERR_GENERIC_SCORE_NAN, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_GT_LT_NX_NOT_COMPATIBLE,
    RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR, RESP_ERR_LIMIT_NOT_SUPPORTED,
    RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, RESP_ERR_MIN_MAX_NOT_VALID_STRING, RESP_ERR_NOT_VALID_FLOAT,
    RESP_ERR_XX_NX_NOT_COMPATIBLE,
  },
  ext::{RespVecExt, is_resp3},
  options::{ExpirationWithOption, SortedSetAddOption, try_get_sorted_set_add_option},
  resp_memory_writer::{RespWriter, format_double as wresp_format_double},
};
use zmij::Buffer as ZmijBuffer;

use super::sorted_set_object::{
  EXPIRY_FLOOR, SortedSetEntry, SortedSetObject, SortedSetOperation, SortedSetRangeOpts,
};
use crate::{
  resp::output::{write_double_numeric, write_null},
  types::{ObjectOutput, format_member_ttl, pick_k_random_indexes, scan_operate_shared},
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
pub struct ZRangeOptions {
  pub by_score: bool,
  pub by_lex: bool,
  pub reverse: bool,
  pub with_scores: bool,
  pub valid_limit: bool,
  pub limit: (i64, i64),
}

/// [`parse_range_options`] 的参数段出错标记（对象层与分层树内臂共用应答单源）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeArgError {
  /// `ZRANGE key <min>` 条目不足：C# 两区间块均要求 count >= 2，不足时零输出
  Incomplete,
  /// LIMIT 缺 offset/count 两 token
  Syntax,
  /// LIMIT 两 token 非整数
  NotInteger,
}

impl RangeArgError {
  /// 错误应答负载单写点：写出与 C# 同款错误行并回 `result1` 标记
  ///
  /// 对位 C# SortedSetObjectImpl.cs 的 SortedSetRange 选项段（锚点归
  /// [`sorted_set_range`]，此处为其错误应答段）
  pub fn write_reply(self, payload: &mut Vec<u8>) -> i64 {
    match self {
      Self::Incomplete => RANGE_ERROR,
      Self::Syntax => {
        write_err(payload, RESP_ERR_GENERIC_SYNTAX_ERROR);
        RANGE_ERROR
      }
      Self::NotInteger => {
        write_err(payload, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        RANGE_ERROR
      }
    }
  }
}

/// ZRANGE 族参数段解析单点（对象层 [`sorted_set_range`] 求值与 wnode 分层树内
/// 臂共用，杜绝第二套 token 翻译）
///
/// 返回 `(选项束, 生效协议版本)`：STORE 形态强制带分值且 RESP3 回退 RESP2 成对
/// 负载读回（C# 同段口径）。参数段非法 → [`RangeArgError`]，调用方经
/// [`RangeArgError::write_reply`] 落同款应答。
///
/// 对位 C# SortedSetObjectImpl.cs 的 SortedSetRange 选项段（锚点归
/// [`sorted_set_range`]，本函数为该选项段的单源实现，与分层树内臂共用）
#[rustfmt::skip]
pub fn parse_range_options(
  args: &[&[u8]],
  range_opts: SortedSetRangeOpts,
  resp_protocol_version: u8,
) -> Result<(ZRangeOptions, u8), RangeArgError> {
  let count = args.len();
  // C# 两个区间块均要求 count >= 2，不足时零输出（命令层保证 ≥2）
  if count < 2 {
    return Err(RangeArgError::Incomplete);
  }

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

  if is_resp3(resp_protocol_version) && range_opts.contains(SortedSetRangeOpts::STORE) {
    resp_protocol_version = 2;
  }

  let mut curr_idx = 2;
  while curr_idx < count {
    let token = args[curr_idx];
    if token.eq_ignore_ascii_case(b"BYSCORE") {
      options.by_score = true;
    } else if token.eq_ignore_ascii_case(b"BYLEX") {
      options.by_lex = true;
    } else if token.eq_ignore_ascii_case(b"REV") {
      options.reverse = true;
    } else if token.eq_ignore_ascii_case(b"WITHSCORES") {
      options.with_scores = true;
    } else if token.eq_ignore_ascii_case(b"LIMIT") {
      let [_, offset_raw, count_raw, ..] = args[curr_idx..] else {
        return Err(RangeArgError::Syntax);
      };
      let (Some(offset), Some(count_limit)) = (
        strict_i32(offset_raw).map(|v| v as i64),
        strict_i32(count_raw).map(|v| v as i64),
      ) else {
        return Err(RangeArgError::NotInteger);
      };
      curr_idx += 2;
      options.limit = (offset, count_limit);
      options.valid_limit = true;
    }
    curr_idx += 1;
  }
  Ok((options, resp_protocol_version))
}

/// 范围/集合结果统一 RESP 输出负载单点（对象层 [`SortedSetObject::write_sorted_set_result`]
/// 与 wnode 分层树内臂共用，RESP3 成对嵌套 / RESP2 扁平）
///
/// 对位 C# SortedSetObjectImpl.cs 的 WriteSortedSetResult（锚点归
/// [`SortedSetObject::write_sorted_set_result`]，本函数为其负载单源）
pub fn write_sorted_set_result_payload<B: AsRef<[u8]>>(
  payload: &mut Vec<u8>,
  with_scores: bool,
  count: usize,
  resp_protocol_version: u8,
  iterator: impl Iterator<Item = (f64, B)>,
) {
  if with_scores && is_resp3(resp_protocol_version) {
    RespWriter::new_ref(payload).write_array_length(count);

    for (score, element) in iterator {
      RespWriter::new_ref(payload).write_array_length(2);
      RespWriter::new_ref(payload).write_bulk_string(element.as_ref());
      cmd_strings::write_double_numeric(payload, score, resp_protocol_version);
    }
  } else {
    RespWriter::new_ref(payload).write_array_length(if with_scores { count * 2 } else { count });

    for (score, element) in iterator {
      RespWriter::new_ref(payload).write_bulk_string(element.as_ref());
      if with_scores {
        payload.write_resp_double_bulk_string(score);
      }
    }
  }
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

/// ZADD 选项段判定单源（内存态对象层与分层树内臂双态共用，杜绝第二套翻译）：
/// 消费前缀选项段 + 组合互斥校验 + 尾段偶数校验，`Err` 为错误帧词（调用方
/// 原样写出，各自按自身输出通道落帧）
///
/// 三条互斥校验（XX&NX、GT/LT/NX、INCR 多对）为「局部变量顺位覆写」非短路序，
/// 不是首中即返：后一条命中即覆盖前一条已写入的错误帧词，末位命中者胜出，
/// 最终单点落帧——对符 C# GetOptions:53-77（optionsError 依次赋值、:73-77 单次
/// writer.WriteError）。改回逐条提前 return 即与 C# 在复合违规输入下错误帧字节
/// 分叉（如 NX XX GT 应回 GT_LT_NX 帧而非 XX_NX 帧），勿复归。
///
/// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions
pub fn sorted_set_add_get_options(
  args: &[&[u8]],
  curr_token_idx: &mut usize,
) -> Result<SortedSetAddOption, &'static str> {
  let mut options = SortedSetAddOption::NONE;

  while *curr_token_idx < args.len() {
    let Some(curr_option) = try_get_sorted_set_add_option(args[*curr_token_idx]) else {
      break;
    };
    options |= curr_option;
    *curr_token_idx += 1;
  }

  // 组合互斥校验：C# GetOptions:53-77 局部变量 optionsError 顺位覆写（后写覆盖、
  // 末位命中胜出），无短路；单点 Err 落帧由调用方按自身输出通道写出
  let mut options_error: Option<&'static str> = None;

  // XX 与 NX 互斥
  if options.contains(SortedSetAddOption::XX) && options.contains(SortedSetAddOption::NX) {
    options_error = Some(RESP_ERR_XX_NX_NOT_COMPATIBLE);
  }

  // NX、GT、LT 两两互斥（覆写规则一）
  if options.contains(SortedSetAddOption::GT) && options.contains(SortedSetAddOption::LT)
    || ((options.contains(SortedSetAddOption::GT) || options.contains(SortedSetAddOption::LT))
      && options.contains(SortedSetAddOption::NX))
  {
    options_error = Some(RESP_ERR_GT_LT_NX_NOT_COMPATIBLE);
  }

  // INCR 仅支持单对 score-element（覆写前两条）
  if options.contains(SortedSetAddOption::INCR) && args.len() - *curr_token_idx > 2 {
    options_error = Some(RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR);
  }

  if let Some(err) = options_error {
    return Err(err);
  }

  // 剩余 token 须为正数对（偶数个）。经 vendored C# 行实核对为 1:1 同在
  //（agy r6-data 条 4 复核：SortedSetObjectImpl.cs:80-87，upstream #644 起）
  // ——选项后剩余段为空/奇数（如 XX + 单成员）在 C# 同回 RESP_SYNTAX_ERROR
  //（对齐 Redis 语义），非「:0 / NOT_VALID_FLOAT」；撤销即与 C# 三方分叉，
  // 故保留
  if *curr_token_idx == args.len() || !(args.len() - *curr_token_idx).is_multiple_of(2) {
    return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
  }

  Ok(options)
}

/// 错误帧单写点：向负载追加与 C# 同款错误行（消 `RespWriter::new_ref(..)
/// .write_error_bytes(..)` 重复样板）
fn write_err(payload: &mut Vec<u8>, frame: &str) {
  RespWriter::new_ref(payload).write_error_bytes(frame.as_bytes());
}

/// 负下标换算单源（Redis 语义 `i < 0 → len + i`，非负原样）：ZRANGE byIndex 闭区间
/// 与 ZREMRANGEBYRANK 起止两处平移共用（与 list 侧同名设施同形，跨文件提升另行统一）
#[inline]
const fn norm(i: i64, len: i64) -> i64 {
  if i < 0 { len + i } else { i }
}

/// LIMIT 段换算单源（byLex / byScore 两臂共用）：offset 负值钳 0，count 负值取全量
/// 上界 `all`
#[inline]
const fn limit_span(limit: (i64, i64), all: usize) -> (usize, usize) {
  let offset = if limit.0 > 0 { limit.0 as usize } else { 0 };
  let take = if limit.1 >= 0 { limit.1 as usize } else { all };
  (offset, take)
}

impl SortedSetObject {
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
      let Some(score) = strict_f64(args[curr_token_idx], true) else {
        if !parsed_options {
          parsed_options = true;
          match sorted_set_add_get_options(args, &mut curr_token_idx) {
            Ok(opts) => options = opts,
            // 选项段判定单源（分层树内臂共用），错误帧词由调用方写出
            Err(frame) => {
              write_err(output.payload, frame);
              return;
            }
          }
          continue; // 选项解析完重试当前 token
        }
        // 出帧即整体丢弃：本臂已落进 obj 的部分变更（前序合法对）不落库——
        // 家族回写门按载荷首字节 `-` 整臂拒写（deviations §142，C# 参照实现
        // 为部分提交留痕形，禁按其回改）
        write_err(output.payload, RESP_ERR_NOT_VALID_FLOAT);
        return;
      };

      parsed_options = true;
      curr_token_idx += 1;

      // member（命令层保证 score-member 成对；奇数尾巴防御性截断，C# 为主循环
      // GetArgSliceByRef 越界读 UB——deviations §151，禁按 C# 形改写为越界读）
      if curr_token_idx >= count {
        break;
      }
      let member = args[curr_token_idx];
      curr_token_idx += 1;

      // 到期守卫窄窗：delete_expired_items 与字典判定各自采样 now_ticks，跨
      // tick 恰到期成员漏摘；对位分层三态判据——到期成员视同缺席，先摘
      // （rem 摘双索引 + 退账 + 清账本）再落新增臂，NX 不拦新增、
      // GT/LT 按缺席语义新增，与 hash_set 守卫同形
      if self.is_expired(member) {
        self.rem(member);
      }

      // 单查同时取回字典内存活句柄与原分值（句柄缺失即落新增臂，判定序不变）
      match self
        .sorted_set_dict
        .get_key_value(member)
        .map(|(k, v)| (k.clone(), *v))
      {
        // 新增成员
        None => {
          // XX 时不新增
          if options.contains(SortedSetAddOption::XX) {
            continue;
          }

          incr_result = score;
          // 一处分配多处持有：散列与有序视图共享同一 Arc（消成员字节双份驻留）
          let m = Arc::from(member);
          self.update_size(&m, true);
          self.sorted_set_dict.insert(m.clone(), score);
          if self.sorted_set.insert(SortedSetEntry { score, member: m }) {
            added_or_changed += 1;
          }
        }
        // 更新既有成员
        Some((m, score_stored)) => {
          let mut score = score;
          // INCR：新分值叠加在既有分值上
          if options.contains(SortedSetAddOption::INCR) {
            score += score_stored;
            incr_result = score;

            if score.is_nan() {
              write_err(output.payload, RESP_ERR_GENERIC_SCORE_NAN);
              return;
            }
          }

          // 分值未变：仅清除过期
          if score == score_stored {
            self.drop_expiration(member);
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

          // 句柄经字典取回复用（引用计数自增、零字节复制）：与散列旧键同一 Arc，
          // 双索引真共享（对位 C# sortedSetDict 保留旧键引用）
          self.sorted_set_dict.insert(m.clone(), score);
          self.sorted_set.remove(&SortedSetEntry {
            score: score_stored,
            member: m.clone(),
          });
          self.drop_expiration(&m);
          self.sorted_set.insert(SortedSetEntry { score, member: m });

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
      RespWriter::new_ref(output.payload).write_int64(added_or_changed);
    }
  }

  /// ZREM：批量移除成员
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRemove
  pub(crate) fn sorted_set_remove(&mut self, args: &[&[u8]], output: &mut ObjectOutput<'_>) {
    self.delete_expired_items();

    let mut removed = 0_i64;

    for &value in args {
      // 双索引剔除单源 `rem`（散列摘出 + 有序视图同步移除 + 退账 + 清过期账本）
      if self.rem(value).is_some() {
        removed += 1;
      }
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
    output.result1 = self.purge_expired_len() as i64;
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

    RespWriter::new_ref(output.payload).write_array_length(count);

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
      write_err(output.payload, RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
      return;
    };

    // 区间判定与 byScore 收集臂共用扫描内核（分值入参经 strict_f64 收敛，恒非
    // NaN，故「上界分值 < 下界即空集」早退与原「下界 <= 上界分值才扫描」等价）
    let mut count = 0_i64;
    self.scan_score_range(min_value, max_value, min_exclusive, max_exclusive, |_| {
      count += 1
    });

    RespWriter::new_ref(output.payload).write_int64(count);
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

    let Some(incr_value) = strict_f64(args[0], true) else {
      write_err(output.payload, RESP_ERR_NOT_VALID_FLOAT);
      return;
    };

    let member = args[1];

    // 到期守卫窄窗：delete_expired_items 与 is_expired 各自采样 now_ticks，
    // 跨 tick 时恰到期成员漏摘；先摘后按新增臂落 incr_value，杜绝残留旧刻度写回滤除面
    if self.is_expired(member) {
      self.rem(member);
    }

    // 单查同时取回字典内存活句柄与原分值（句柄缺失即落新增臂）
    let new_score = match self
      .sorted_set_dict
      .get_key_value(member)
      .map(|(k, v)| (k.clone(), *v))
    {
      Some((m, score)) => {
        let result = score + incr_value;
        if result.is_nan() {
          write_err(output.payload, RESP_ERR_GENERIC_SCORE_NAN);
          return;
        }

        // 句柄经字典取回复用（引用计数自增、零字节复制）：与散列旧键同一 Arc，
        // 双索引真共享
        self.sorted_set_dict.insert(m.clone(), result);
        self.sorted_set.remove(&SortedSetEntry {
          score,
          member: m.clone(),
        });
        self.sorted_set.insert(SortedSetEntry {
          score: result,
          member: m,
        });
        result
      }
      None => {
        let m = Arc::<[u8]>::from(member);
        self.update_size(&m, true);
        self.sorted_set_dict.insert(m.clone(), incr_value);
        self.sorted_set.insert(SortedSetEntry {
          score: incr_value,
          member: m,
        });
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

    // 参数段解析单点（与分层树内臂共用，见 [`parse_range_options`]）
    let (options, resp_protocol_version) =
      match parse_range_options(args, range_opts, resp_protocol_version) {
        Ok(v) => v,
        // C# 两个区间块均要求 count >= 2，不足时无任何输出（命令层保证 ≥2）
        Err(RangeArgError::Incomplete) => return,
        Err(e) => {
          output.result1 = e.write_reply(output.payload);
          return;
        }
      };
    let (min_span, max_span) = (args[0], args[1]);

    if count >= 2 && ((!options.by_score && !options.by_lex) || options.by_score) {
      let Some((min_value, min_exclusive)) = Self::try_parse_parameter(min_span) else {
        write_err(output.payload, RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
        output.result1 = RANGE_ERROR;
        return;
      };
      let Some((max_value, max_exclusive)) = Self::try_parse_parameter(max_span) else {
        write_err(output.payload, RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
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
        let set_count = self.purge_expired_len() as i64;
        if options.valid_limit {
          write_err(output.payload, RESP_ERR_LIMIT_NOT_SUPPORTED);
          output.result1 = RANGE_ERROR;
          return;
        } else if min_value > (set_count as f64) - 1.0 {
          // 空结果
          RespWriter::new_ref(output.payload).write_empty_array();
          return;
        } else {
          // 闭区间换算：负下标自尾部平移，上界钳 len-1（原「非负且 >= len 才钳」
          // 分支等价——负下标平移后必 <= len-1，故统一取 min）
          let min_index = norm(min_value as i64, set_count);
          let max_index = norm(max_value as i64, set_count).min(set_count - 1);

          // 双双越界或 min > max：空结果
          if (min_index < 0 && max_index < 0) || min_index > max_index {
            RespWriter::new_ref(output.payload).write_empty_array();
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
            .map(|x| (x.score, x.member.as_ref()))
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
        write_err(output.payload, RESP_ERR_MIN_MAX_NOT_VALID_STRING);
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
    write_sorted_set_result_payload(
      output.payload,
      with_scores,
      count,
      resp_protocol_version,
      iterator,
    );
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
      write_err(output.payload, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return;
    };

    let count = self.sorted_set_dict.len() as i64;

    // 负索引平移 + 下界钳 0（对齐 Redis t_zset.c zremrangeGenericCommand）
    let start = norm(start, count).max(0);
    let stop = norm(stop, count);

    // start 非负，故 start > stop 覆盖 stop 仍为负的情形
    if start > stop || start >= count {
      RespWriter::new_ref(output.payload).write_int64(0);
      // 零命中回填计数（信封 RMW 写回门控以 result1 为准，杜绝零变更伪写回）
      output.result1 = 0;
      return;
    }

    // 上界钳 len-1
    let stop = stop.min(count - 1);
    let element_count = (stop - start + 1) as usize;

    // 先收集后删除，规避迭代中修改
    let doomed: Vec<SortedSetEntry> = self
      .sorted_set
      .iter()
      .skip(start as usize)
      .take(element_count)
      .cloned()
      .collect();

    // 逐枚经 `rem` 单源剔除（散列 + 有序视图 + 账本 + 记账四步同序）
    for item in doomed {
      self.rem(item.member.as_ref());
    }

    RespWriter::new_ref(output.payload).write_int64(element_count as i64);
    // 实际移除数回填（信封 RMW 写回门控以 result1 为准）
    output.result1 = element_count as i64;
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
      write_err(output.payload, RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
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
    // 删除循环逐枚转 `rem` 单源四步剔除（散列 + 有序视图 + 账本 + 记账）
    let doomed: Vec<Vec<u8>> = hits.into_iter().map(|(_, m)| m.to_vec()).collect();
    let removed = doomed.len() as i64;
    for member in doomed {
      self.rem(&member);
    }

    RespWriter::new_ref(output.payload).write_int64(removed);
    // 实际移除数回填（信封 RMW 写回门控以 result1 为准，杜绝零命中伪写回）
    output.result1 = removed;
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
    let seed = arg2;
    let sorted_set_count = self.purge_expired_len() as i64;

    if count > 0 && count > sorted_set_count {
      count = sorted_set_count;
    }

    // 存活数零早退（与 hash_random_field / set_random_member 同构守卫，族内唯
    // 本臂缺项）：成员级 TTL 全到期键上 pick_k_random_indexes 的 n=0 空域零产出，
    // 负 count 的头部声明数大于实写数即 RESP 流永久错位、正 count 钳 0 与无 count
    // 形整帧零字节挂读；带 count 落空数组、无 count 落 null。C# 同形态在
    // Random.Next(0) 抛断流（deviations.md §12 已登记原型危险面），「声明数恒等
    // 实写数」（§53）不受该缺陷豁免
    if sorted_set_count == 0 {
      if included_count {
        RespWriter::new_ref(output.payload).write_empty_array();
      } else {
        write_null(output, resp_protocol_version);
      }
      output.result1 = 0;
      return;
    }

    // count 可为负，但数组长度不能
    let array_length = (if with_scores && resp_protocol_version == 2 {
      count * 2
    } else {
      count
    })
    .abs();
    if array_length > 1 || (array_length == 1 && included_count) {
      RespWriter::new_ref(output.payload).write_array_length(array_length as usize);
    }
    let index_count = count.unsigned_abs() as usize;

    // 随机下标采样共用单源（count > 0 不放回 / 负 count 可重复，
    // 与 HRANDFIELD、SRANDMEMBER 同口，见 pick_k_random_indexes 的 k/n 阈值分派）；
    // 下标流式 sink 直写应答：负 count 的 |k| 与基数脱钩，放回臂零存储
    //（C# new int[indexCount] 为连接级 OOM 面，预分配即 GB 级单命令分配）
    pick_k_random_indexes(
      sorted_set_count.max(0) as usize,
      index_count,
      seed,
      count > 0,
      |idx| {
        let Some((element, score)) = self.element_at(idx) else {
          return;
        };

        if with_scores && is_resp3(resp_protocol_version) {
          RespWriter::new_ref(output.payload).write_array_length(2);
        }

        RespWriter::new_ref(output.payload).write_bulk_string(element);

        if with_scores {
          write_double_numeric(output, score, resp_protocol_version);
        }
      },
    );

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
      // ZREMRANGEBYLEX 仅对命中 member 做一次 to_vec 后逐枚转 `rem` 单源四步剔除
      if is_remove {
        let doomed: Vec<Vec<u8>> = hits.into_iter().map(|(_, m)| m.to_vec()).collect();
        for member in doomed {
          self.rem(&member);
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

    // 直接查验输入切片，零临时堆分配（成员匹配同用切片比较）
    let member = args[0];

    let Some(score) = self.try_get_score(member) else {
      write_null(output, resp_protocol_version);
      return;
    };

    let mut rank = 0_i64;
    for item in &self.sorted_set {
      if self.is_expired(&item.member) {
        continue;
      }
      if item.member.as_ref() == member {
        break;
      }
      rank += 1;
    }

    if !ascending {
      rank = self.purge_expired_len() as i64 - rank - 1;
    }

    if with_score {
      RespWriter::new_ref(output.payload).write_array_length(2);
      RespWriter::new_ref(output.payload).write_int64(rank);
      write_double_numeric(output, score, resp_protocol_version);
    } else {
      RespWriter::new_ref(output.payload).write_int64(rank);
    }
  }

  /// 弹出最低/最高分成员（环前剔除过期后弹出，树空返回 None）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:PopMinOrMax
  pub fn pop_min_or_max(&mut self, pop_max_score_element: bool) -> Option<(f64, Arc<[u8]>)> {
    self.delete_expired_items();
    self.pop_min_or_max_no_purge(pop_max_score_element)
  }

  /// 免水位重采样摘除内核：直摘最低/最高分成员，四步摘除（成员摘除/字典
  /// 移除/账本 remove_expiration/记账）原样，不触碰到期水位
  ///
  /// 对位 C# PopMinOrMax 去 DeleteExpiredItems 的裸摘除体（SortedSetPopMinOrMaxCount
  /// 循环体 :849-866 同形直摘）：环内到期成员视存活原样弹出并计入声明，与 C#
  /// 逐字节全等（裁决依 doc/zh/deviations.md §53 族先例），到期账随摘除一并结清
  fn pop_min_or_max_no_purge(&mut self, pop_max_score_element: bool) -> Option<(f64, Arc<[u8]>)> {
    // pop_first/pop_last 直接摘除并归还所有权（O(log n) 单次树查找），
    // 消除先 peek 再 remove 的二次查找与 Arc 克隆
    let element = if pop_max_score_element {
      self.sorted_set.pop_last()?
    } else {
      self.sorted_set.pop_first()?
    };

    self.sorted_set_dict.remove(&element.member);
    self.drop_expiration(&element.member);
    self.update_size(&element.member, false);

    Some((element.score, element.member))
  }

  /// ZPOPMIN / ZPOPMAX（含 COUNT 形态；arg1 = -1 表示无计数的单元素形态）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetPopMinOrMaxCount
  ///
  /// 环前一次剔除、环内免重采样（弹出循环直调免采样内核，环内到期成员视
  /// 存活原样弹出并计入声明，与 C# 逐字节全等），裁决依 deviations :752-753
  /// 族先例
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
      RespWriter::new_ref(output.payload).write_empty_array();
      output.result1 = 0;
      return;
    }

    if with_header {
      if is_resp3(resp_protocol_version) {
        RespWriter::new_ref(output.payload).write_array_length(count as usize);
      } else {
        RespWriter::new_ref(output.payload).write_array_length((count * 2) as usize);
      }
    }

    let pop_max = op == SortedSetOperation::Zpopmax;
    while count > 0 {
      // 钳后 count ≤ 环前剔除后的存活基数，免采样内核每轮必摘除一枚、
      // 不空返（C# 循环体直摘 Min/Max 同一无检查形），故无 break 早停臂
      let (score, member) = self.pop_min_or_max_no_purge(pop_max).unwrap();

      if !with_header || is_resp3(resp_protocol_version) {
        RespWriter::new_ref(output.payload).write_array_length(2);
      }

      RespWriter::new_ref(output.payload).write_bulk_string(&member);
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

    RespWriter::new_ref(output.payload).write_array_length(num_fields);

    for &arg in args {
      let result = self.persist(arg);
      RespWriter::new_ref(output.payload).write_int64(result as i64);
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

    RespWriter::new_ref(output.payload).write_array_length(num_fields);

    let now = now_ticks();
    for &member in args {
      let mut result = self.get_expiration(member);

      // 已成过去的过期视同不存在
      if result > 0 && self.is_expired(member) {
        result = -2;
      }

      let formatted = format_member_ttl(result, is_timestamp, is_milliseconds, now);
      RespWriter::new_ref(output.payload).write_int64(formatted);
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

    RespWriter::new_ref(output.payload).write_array_length(args.len());

    for &arg in args {
      let result = self.set_expiration(
        arg,
        expiration_with_option.expiration_time_in_ticks(),
        expiration_with_option.expire_option(),
      );
      RespWriter::new_ref(output.payload).write_int64(result as i64);
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
      Self::try_parse_lex_parameter(min_param),
      Self::try_parse_lex_parameter(max_param),
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
        member: Arc::from(min_value_chars),
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
          let in_range = x.member.as_ref().cmp(min_value_chars);
          !(in_range == Ordering::Less || (in_range == Ordering::Equal && min_value_exclusive))
        })
        .take_while(|x| {
          if max_value_infinity == SpecialRanges::InfiniteMax {
            return true;
          }
          let out_range = x.member.as_ref().cmp(max_value_chars);
          !(out_range == Ordering::Greater || (out_range == Ordering::Equal && max_value_exclusive))
        })
        .map(|x| (x.score, x.member.as_ref()));

      // LIMIT 段换算单源（负 count 取 usize::MAX 即全量，与 byScore 同形）
      let (offset, take) = limit_span(limit, usize::MAX);

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

  /// 分值区间扫描内核：过期跳过 → 上界出界即止 → 下界独占即跳过，命中项交 `visit`
  /// 消费。ZCOUNT 计数与 [`Self::get_elements_in_range_by_score`] 收集臂共用，
  /// 杜绝第二套区间判定。
  ///
  /// 对标 C# GetViewBetween((minValue, null), Max)：以 (min_value, 空 member) 哨兵作
  /// range 下界；条目序 (score, member) 与 SortedSetComparer 一致，被裁项必被下方
  /// min 过滤 continue，逐条对位，结果不变。
  fn scan_score_range<'a>(
    &'a self,
    min_value: f64,
    max_value: f64,
    min_exclusive: bool,
    max_exclusive: bool,
    mut visit: impl FnMut(&'a SortedSetEntry),
  ) {
    // 全集最大分值仍低于下界：区间必空（等价于 C# GetViewBetween 的下界越集守卫）
    let Some(max_entry) = self.sorted_set.last() else {
      return;
    };
    if max_entry.score < min_value {
      return;
    }

    let start = SortedSetEntry {
      score: min_value,
      member: Arc::from(&[][..]),
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
      visit(item);
    }
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

    self.scan_score_range(min_value, max_value, min_exclusive, max_exclusive, |item| {
      scored_elements.push((item.score, item.member.as_ref()))
    });

    if query.do_reverse {
      scored_elements.reverse();
    }

    if query.valid_limit {
      // LIMIT 段换算单源（负 count 取实收全长，与 byLex 同形）
      let (offset, take) = limit_span(query.limit, scored_elements.len());
      scored_elements = scored_elements
        .into_iter()
        .skip(offset)
        .take(take)
        .collect();
    }

    scored_elements
  }

  // ---- Helper Methods ----

  /// 清除成员级过期账本条目（`ledger.remove_expiration` + 记账字节收敛单点）
  fn drop_expiration(&mut self, member: &[u8]) {
    self
      .ledger
      .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, member);
  }

  // 定点摘除内核即 `Self::rem`（散列摘出 + 有序视图同步移除 + 清过期账本 + 退账，
  // 四步同序），本文件不另立私有形态；ZREM / ZREMRANGEBYRANK / ZREMRANGEBYSCORE /
  // ZREMRANGEBYLEX 四处移除臂共用该单源

  /// 解析分值区间参数：`(5` → 独占；支持 ±inf
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:TryParseParameter
  ///
  /// 【有意偏差登记】ZCOUNT/ZRANGEBYSCORE 空串边界：当 `val` 为空串时，
  /// C# 侧无长度校验，硬读 `val[0]` 会因 `IndexOutOfRangeException` 掐断连接；
  /// Rust 侧由于采用 `first()` 安全访问并回落到后续解析，遇空串将正常返回 `None`，
  /// 上层进而回 `min or max is not a float` 错误帧。这是崩溃防御，而非文案改写。
  pub fn try_parse_parameter(val: &[u8]) -> Option<(f64, bool)> {
    let mut val = val;
    let mut exclusive = false;

    // 独占前缀
    if val.first() == Some(&b'(') {
      val = &val[1..];
      exclusive = true;
    }

    if let Some(value) = strict_f64(val, true) {
      // ±inf 的独占语义退化为普通边界
      let exclusive = exclusive && !value.is_infinite();
      return Some((value, exclusive));
    }

    None
  }

  /// 解析字典序区间参数：`[a` 闭 / `(a` 开 / `-` 无穷小 / `+` 无穷大
  ///
  /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:TryParseLexParameter
  pub fn try_parse_lex_parameter(val: &[u8]) -> Option<(&[u8], bool, SpecialRanges)> {
    let mut limit_chars: &[u8] = &[];
    let mut limit_exclusive = false;
    let mut infinity = SpecialRanges::None;

    // 空串形有意偏差自陈：C# `switch (val[0])`（SortedSetObjectImpl.cs:1174）无长度
    // 守卫，空串裸读即越界掐连（裸调用先于 try 段、catch 救不到此形）；rust first()
    // 守卫把空串与非法首字符一律折 None 恒回 not-valid-string 帧，登记见 doc/zh/deviations.md §138
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

  /// ZSCAN 的对象层入口，转发至 [`scan_operate_shared`]（总量即
  /// sorted_set_dict.len()，供游标/条目帧位预留估宽）。分值经
  /// [`wresp_format_double`] 文本化后走 sink（与 write_resp_double_bulk_string
  /// 逐字节同形：非有限值输出 "inf"/"-inf" 与 ZRANGE 对齐，见
  /// doc/zh/deviations.md §80），每成员恒发 2 项：成员 + 分值。
  pub(crate) fn scan_operate(
    &mut self,
    args: &[&[u8]],
    limit_count_in_output: i32,
    output: &mut ObjectOutput<'_>,
    _resp_protocol_version: u8,
  ) {
    scan_operate_shared(
      args,
      limit_count_in_output,
      output,
      self.sorted_set_dict.len(),
      |cursor, count, pattern, is_no_value, sink| {
        self.scan(cursor, count, pattern, is_no_value, |member, score| {
          sink(member);
          let mut fbuf = ZmijBuffer::new();
          sink(wresp_format_double(score, &mut fbuf).as_bytes());
          2
        })
      },
    );
  }
}
