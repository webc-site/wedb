//! RESP 命令层参数校验与解构工具（消除机械样板代码）

use std::{
  array::from_fn,
  ops::{RangeFrom, RangeInclusive, RangeToInclusive},
};

pub use crate::check_arg_count;
use crate::cmd_strings::abort_with_wrong_number_of_arguments;

/// 参数数量匹配器（支持精确数值或区间范围）
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
  // matcher 带 return
  ($args:expr, $matcher:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if !$crate::check_args::ArgCountMatcher::matches(&($matcher), $args.len()) {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
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
pub type ArgsWithRest<'a, const N: usize> = ([&'a [u8]; N], &'a [&'a [u8]]);

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_check_arg_count_macro() {
    let mut out = Vec::new();

    fn run_exact(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 2, out, "CMD");
      Ok(false)
    }

    let a: &[&[u8]] = &[b"k", b"v"];
    assert_eq!(run_exact(a, &mut out), Ok(false));

    let b: &[&[u8]] = &[b"k"];
    assert_eq!(run_exact(b, &mut out), Ok(true));
    assert!(out.starts_with(b"-ERR wrong number of arguments"));

    out.clear();
    fn run_min(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 2.., out, "CMD");
      Ok(false)
    }
    assert_eq!(run_min(b, &mut out), Ok(true));
    assert_eq!(run_min(a, &mut out), Ok(false));

    out.clear();
    fn run_range(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 1..=2, out, "AUTH");
      Ok(false)
    }
    assert_eq!(run_range(a, &mut out), Ok(false));
    let c: &[&[u8]] = &[b"1", b"2", b"3"];
    assert_eq!(run_range(c, &mut out), Ok(true));

    out.clear();
    fn run_custom_ret(args: &[&[u8]], out: &mut Vec<u8>) -> bool {
      check_arg_count!(args, 1.., out, "CMD", return false);
      true
    }
    let empty: &[&[u8]] = &[];
    assert!(!run_custom_ret(empty, &mut out));
    assert!(run_custom_ret(a, &mut out));
  }

  #[test]
  fn test_unpack_args() {
    let mut out = Vec::new();

    fn run_unpack(args: &[&[u8]], out: &mut Vec<u8>) -> Result<(&'static str, usize), ()> {
      let Some([k, v]) = unpack_args(args, out, "SET") else {
        return Err(());
      };
      assert_eq!(k, b"key");
      assert_eq!(v, b"val");
      Ok(("ok", 2))
    }

    let valid: &[&[u8]] = &[b"key", b"val"];
    assert_eq!(run_unpack(valid, &mut out), Ok(("ok", 2)));

    let invalid: &[&[u8]] = &[b"key"];
    assert_eq!(run_unpack(invalid, &mut out), Err(()));
    assert!(out.starts_with(b"-ERR wrong number of arguments"));

    out.clear();
    fn run_unpack_rest(args: &[&[u8]], out: &mut Vec<u8>) -> Result<(&'static str, usize), ()> {
      let Some(([k], rest)) = unpack_args_rest(args, out, "MGET") else {
        return Err(());
      };
      assert_eq!(k, b"key");
      Ok(("ok", rest.len()))
    }

    let multi: &[&[u8]] = &[b"key", b"v1", b"v2"];
    assert_eq!(run_unpack_rest(multi, &mut out), Ok(("ok", 2)));
    assert_eq!(run_unpack_rest(invalid, &mut out), Ok(("ok", 0)));
    assert_eq!(run_unpack_rest(&[], &mut out), Err(()));
  }
}
