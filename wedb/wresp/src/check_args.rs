//! RESP 命令层参数校验与解构工具（消除机械样板代码）

use std::ops::{Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

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

impl ArgCountMatcher for Range<usize> {
  #[inline]
  fn matches(&self, count: usize) -> bool {
    self.contains(&count)
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

impl ArgCountMatcher for RangeTo<usize> {
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

/// 校验确切参数数量：不匹配则写出错误应答并返回 false
#[inline]
pub fn check_exact_arg_count(
  actual: usize,
  expected: usize,
  output: &mut Vec<u8>,
  cmd: &str,
) -> bool {
  if actual != expected {
    abort_with_wrong_number_of_arguments(output, cmd);
    false
  } else {
    true
  }
}

/// 校验最小参数数量：不足则写出错误应答并返回 false
#[inline]
pub fn check_min_arg_count(actual: usize, min: usize, output: &mut Vec<u8>, cmd: &str) -> bool {
  if actual < min {
    abort_with_wrong_number_of_arguments(output, cmd);
    false
  } else {
    true
  }
}

/// RESP 命令参数数量断言宏
///
/// 支持多种条件语法，未通过时自动写出 `-ERR wrong number of arguments for '...' command\r\n`。
/// 默认 `return Ok(true)`，可自定义返回表达式（如 `, return Ok(false)` 或 `, return false`）。
///
/// # 语法示例
/// - `check_arg_count!(parse_state, 1, output, "GET");` (要求恰好 1 个参数)
/// - `check_arg_count!(parse_state, >= 2, output, "MGET");` (要求至少 2 个参数)
/// - `check_arg_count!(parse_state, <= 6, output, "HELLO");` (要求最多 6 个参数)
/// - `check_arg_count!(parse_state, > 0, output, "PING");`
/// - `check_arg_count!(parse_state, < 3, output, "CMD");`
/// - `check_arg_count!(parse_state, 1..=2, output, "AUTH");` (范围包含)
/// - `check_arg_count!(parse_state, !empty, output, "KEYS");` (非空)
/// - `check_arg_count!(parse_state, empty, output, "FLUSHALL");` (空)
/// - `check_arg_count!(count == 1 || count == 3, output, "MEMORY|USAGE");` (任意布尔条件)
#[macro_export]
macro_rules! check_arg_count {
  // 1. !empty 带 return
  ($args:expr, !empty, $output:expr, $cmd:expr, return $ret:expr) => {
    if $args.is_empty() {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 1. !empty 默认
  ($args:expr, !empty, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, !empty, $output, $cmd, return Ok(true));
  };

  // 2. empty 带 return
  ($args:expr, empty, $output:expr, $cmd:expr, return $ret:expr) => {
    if !$args.is_empty() {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 2. empty 默认
  ($args:expr, empty, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, empty, $output, $cmd, return Ok(true));
  };

  // 3. >= 最小数量 带 return
  ($args:expr, >= $min:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if $args.len() < $min {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 3. >= 最小数量 默认
  ($args:expr, >= $min:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, >= $min, $output, $cmd, return Ok(true));
  };

  // 4. <= 最大数量 带 return
  ($args:expr, <= $max:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if $args.len() > $max {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 4. <= 最大数量 默认
  ($args:expr, <= $max:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, <= $max, $output, $cmd, return Ok(true));
  };

  // 5. > 大于数量 带 return
  ($args:expr, > $min:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if $args.len() <= $min {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 5. > 大于数量 默认
  ($args:expr, > $min:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, > $min, $output, $cmd, return Ok(true));
  };

  // 6. < 小于数量 带 return
  ($args:expr, < $max:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if $args.len() >= $max {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 6. < 小于数量 默认
  ($args:expr, < $max:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, < $max, $output, $cmd, return Ok(true));
  };

  // 7. 任意布尔条件 默认 (3 参数)
  ($cond:expr, $output:expr, $cmd:expr) => {
    if !($cond) {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return Ok(true);
    }
  };
  // 7. 任意布尔条件 带 return (4 参数且末尾为 return $ret)
  ($cond:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if !($cond) {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };

  // 8. matcher (usize 或 RangeBounds) 带 return
  ($args:expr, $matcher:expr, $output:expr, $cmd:expr, return $ret:expr) => {
    if !$crate::check_args::ArgCountMatcher::matches(&($matcher), $args.len()) {
      $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
      return $ret;
    }
  };
  // 8. matcher 默认 (4 参数)
  ($args:expr, $matcher:expr, $output:expr, $cmd:expr) => {
    $crate::check_arg_count!($args, $matcher, $output, $cmd, return Ok(true));
  };
}

/// RESP 命令参数切片解构宏
///
/// 利用切片模式匹配，同时完成参数数量严格校验与变量绑定（自动解引用外层引用）。
///
/// # 语法示例
/// - `unpack_args!(parse_state, output, "GET", [key]);`
/// - `unpack_args!(parse_state, output, "SET", [key, val]);`
/// - `unpack_args!(parse_state, output, "SETRANGE", [key, offset, val]);`
/// - `unpack_args!(parse_state, output, "MGET", [key], rest);`
/// - `unpack_args!(parse_state, output, "MSET", [k1, v1], rest);`
#[macro_export]
macro_rules! unpack_args {
  // 固定数量解构：[v1, v2, ...] 带 return
  ($args:expr, $output:expr, $cmd:expr, [$($var:ident),+ $(,)?], return $ret:expr) => {
    let [$($var),+] = match $args {
      [$($var),+] => [$(*$var),+],
      _ => {
        $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
        return $ret;
      }
    };
  };

  // 固定数量解构：[v1, v2, ...] 默认 return Ok(true)
  ($args:expr, $output:expr, $cmd:expr, [$($var:ident),+ $(,)?]) => {
    $crate::unpack_args!($args, $output, $cmd, [$($var),+], return Ok(true));
  };

  // 前缀 + 剩余切片解构：[v1, ...], rest 带 return
  ($args:expr, $output:expr, $cmd:expr, [$($var:ident),+ $(,)?], $rest:ident, return $ret:expr) => {
    let ($($var,)+ $rest) = match $args {
      [$($var,)+ rest @ ..] => ($(*$var,)+ rest),
      _ => {
        $crate::cmd_strings::abort_with_wrong_number_of_arguments($output, $cmd);
        return $ret;
      }
    };
  };

  // 前缀 + 剩余切片解构：[v1, ...], rest 默认 return Ok(true)
  ($args:expr, $output:expr, $cmd:expr, [$($var:ident),+ $(,)?], $rest:ident) => {
    $crate::unpack_args!($args, $output, $cmd, [$($var),+], $rest, return Ok(true));
  };
}

#[cfg(test)]
mod tests {
  use std::str::from_utf8;

  use super::*;

  #[test]
  fn test_check_exact_and_min() {
    let mut out = Vec::new();
    assert!(check_exact_arg_count(2, 2, &mut out, "TEST"));
    assert!(out.is_empty());

    assert!(!check_exact_arg_count(1, 2, &mut out, "TEST"));
    assert_eq!(
      from_utf8(&out).unwrap(),
      "-ERR wrong number of arguments for 'TEST' command\r\n"
    );

    out.clear();
    assert!(check_min_arg_count(2, 2, &mut out, "TEST"));
    assert!(!check_min_arg_count(1, 2, &mut out, "TEST"));
  }

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
      check_arg_count!(args, >= 2, out, "CMD");
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
      check_arg_count!(args, !empty, out, "CMD", return false);
      true
    }
    let empty: &[&[u8]] = &[];
    assert!(!run_custom_ret(empty, &mut out));
    assert!(run_custom_ret(a, &mut out));
  }

  #[test]
  fn test_unpack_args_macro() {
    let mut out = Vec::new();

    fn run_unpack(args: &[&[u8]], out: &mut Vec<u8>) -> Result<(&'static str, usize), ()> {
      unpack_args!(args, out, "SET", [k, v], return Err(()));
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
      unpack_args!(args, out, "MGET", [k], rest, return Err(()));
      assert_eq!(k, b"key");
      Ok(("ok", rest.len()))
    }

    let multi: &[&[u8]] = &[b"key", b"v1", b"v2"];
    assert_eq!(run_unpack_rest(multi, &mut out), Ok(("ok", 2)));
    assert_eq!(run_unpack_rest(invalid, &mut out), Ok(("ok", 0)));
    assert_eq!(run_unpack_rest(&[], &mut out), Err(()));
  }
}
