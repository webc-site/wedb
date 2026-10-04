//! RESP 命令层参数校验与解构工具（消除机械样板代码）
//!
//! 在 garnet 中的对位: 命令参数校验散布于 C# 各命令处理器(BasicCommands.cs 等的 AbortWithWrongNumberOfArguments;RespCommandArgument.cs 仅参数元数据树,无单一校验文件)

use std::{
  array::from_fn,
  ops::{RangeFrom, RangeInclusive, RangeToInclusive},
};

pub use crate::check_arg_count;
use crate::cmd_strings::abort_with_wrong_number_of_arguments;

/// 参数数量匹配器（支持精确数值或区间范围）
///
/// pub 保持：`check_arg_count!` 为导出宏，展开体跨 crate 引用
/// [`check_args_len`]，匹配器随签名一并处于宏的公开契约面
pub trait ArgCountMatcher {
  fn matches(&self, count: usize) -> bool;
}

impl ArgCountMatcher for usize {
  #[inline]
  fn matches(&self, count: usize) -> bool {
    *self == count
  }
}

impl ArgCountMatcher for RangeInclusive<usize> {
  #[inline]
  fn matches(&self, count: usize) -> bool {
    self.contains(&count)
  }
}

impl ArgCountMatcher for RangeFrom<usize> {
  #[inline]
  fn matches(&self, count: usize) -> bool {
    self.contains(&count)
  }
}

impl ArgCountMatcher for RangeToInclusive<usize> {
  #[inline]
  fn matches(&self, count: usize) -> bool {
    self.contains(&count)
  }
}

use wbase::num::{DbIndexError, parse_db_index, strict_i32, strict_i64};

use crate::cmd_strings::{
  RESP_ERR_DB_INDEX_OUT_OF_RANGE, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, abort_with_error_message,
};

/// 校验参数数量是否符合匹配规则（纯泛型函数）
///
/// 校验失败时自动向 output 写入 `-ERR wrong number of arguments for '...' command\r\n` 并返回 false
#[inline]
pub fn check_args_len(
  args: &[&[u8]],
  matcher: impl ArgCountMatcher,
  output: &mut Vec<u8>,
  cmd: &str,
) -> bool {
  if !matcher.matches(args.len()) {
    abort_with_wrong_number_of_arguments(output, cmd);
    return false;
  }
  true
}

/// 从参数字节切片严格解析 i64，失败时向 output 写入整数错误帧并返回 None
#[inline]
pub fn parse_i64_arg(arg: &[u8], output: &mut Vec<u8>) -> Option<i64> {
  if let Some(v) = strict_i64(arg) {
    Some(v)
  } else {
    abort_with_error_message(output, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    None
  }
}

/// 从参数字节切片严格解析 i32，失败时向 output 写入整数错误帧并返回 None
#[inline]
pub fn parse_i32_arg(arg: &[u8], output: &mut Vec<u8>) -> Option<i32> {
  if let Some(v) = strict_i32(arg) {
    Some(v)
  } else {
    abort_with_error_message(output, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    None
  }
}

/// 从参数字节切片解析数据库索引（C# int32 档，值域 ≥ 0），失败时向 output 写入相应错误帧并返回 None
#[inline]
pub fn parse_db_index_arg(
  raw: &[u8],
  not_integer_msg: &'static str,
  output: &mut Vec<u8>,
) -> Option<u64> {
  match parse_db_index(raw) {
    Ok(idx) => Some(idx as u64),
    Err(DbIndexError::NotInteger) => {
      abort_with_error_message(output, not_integer_msg);
      None
    }
    Err(DbIndexError::OutOfRange) => {
      abort_with_error_message(output, RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      None
    }
  }
}

/// RESP 命令参数数量断言宏
///
/// 未通过时自动写出 `-ERR wrong number of arguments for '...' command\r\n`。
/// 默认 `return Ok(true)`，可自定义返回表达式（如 `, return Ok(false)` 或 `, return false`）。
///
/// # 语法示例
/// - `check_arg_count!(parse_state, 1, output, "GET");` (要求恰好 1 个参数)
/// - `check_arg_count!(parse_state, 2.., output, "MGET");` (要求至少 2 个参数)
/// - `check_arg_count!(parse_state, ..=6, output, "HELLO");` (要求最多 6 个参数)
/// - `check_arg_count!(parse_state, 1.., output, "KEYS");` (非空)
/// - `check_arg_count!(parse_state, ..=0, output, "FLUSHALL");` (空)
/// - `check_arg_count!(parse_state, 1..=2, output, "AUTH");` (范围包含)
/// - `check_arg_count!(count == 1 || count == 3, output, "MEMORY|USAGE");` (任意布尔条件)
#[macro_export]
macro_rules! check_arg_count {
  // 任意布尔条件 默认 (3 参数)
  ($cond:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($cond, $output, $cmd, return Ok(true));
  };
  // 任意布尔条件 带 return (4 参数且末尾为 return $ret)
  ($cond:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if !($cond) {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };

  // matcher (usize 精确值或 RangeBounds 区间) 默认 (4 参数)
  ($args:expr, $matcher:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, $matcher, $output, $cmd, return Ok(true));
  };
  // matcher 带 return（委托泛型 check_args_len）
  ($args:expr, $matcher:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if !$crate::check_args::check_args_len($args, $matcher, $output, $cmd) {
      return $ret;
    }
  };
}

/// RESP 命令参数切片解构（const 泛型）：严格校验参数数量并解构前缀参数
///
/// 数量不符时写出 `-ERR wrong number of arguments for '...' command\r\n` 并返回 `None`，
/// 配合 let-else 提前返回：
/// `let Some([key, val]) = unpack_args(parse_state, output, "SET") else { return Ok(true) };`
#[inline]
pub fn unpack_args<'a, const N: usize>(
  args: &[&'a [u8]],
  output: &mut Vec<u8>,
  cmd: &str,
) -> Option<[&'a [u8]; N]> {
  if args.len() != N {
    abort_with_wrong_number_of_arguments(output, cmd);
    return None;
  }
  // SAFETY：len == N 已严格校验，索引恒在界内
  Some(from_fn(|i| unsafe { *args.get_unchecked(i) }))
}

/// 前缀参数数组 + 剩余切片二元组（[`unpack_args_rest`] 返回值）
pub(crate) type ArgsWithRest<'a, const N: usize> = ([&'a [u8]; N], &'a [&'a [u8]]);

/// 前缀 + 剩余切片解构（const 泛型）：校验至少 N 个参数，返回前缀参数数组与剩余切片
///
/// 不足 N 个时写出错误应答并返回 `None`，配合 let-else 提前返回：
/// `let Some(([key], rest)) = unpack_args_rest(parse_state, output, "MGET") else { return Ok(true) };`
#[inline]
pub fn unpack_args_rest<'a, const N: usize>(
  args: &'a [&'a [u8]],
  output: &mut Vec<u8>,
  cmd: &str,
) -> Option<ArgsWithRest<'a, N>> {
  if args.len() < N {
    abort_with_wrong_number_of_arguments(output, cmd);
    return None;
  }
  let (prefix, rest) = args.split_at(N);
  // SAFETY：split_at 后 prefix.len() == N，索引恒在界内
  Some((from_fn(|i| unsafe { *prefix.get_unchecked(i) }), rest))
}
