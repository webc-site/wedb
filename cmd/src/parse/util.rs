use std::str::from_utf8;
use wedb_embed::{Error, Result};
use wedb_resp::{parse_f64_fast, parse_i64_fast, parse_u64_fast};

use crate::types::ExpireCondition;

/// 标准 Redis 参数数量错误
#[inline]
pub fn err_wrong_args(cmd_name: &str) -> Error {
  Error::invalid_data(format!(
    "ERR wrong number of arguments for '{cmd_name}' command"
  ))
}

/// 标准语法错误
#[inline]
pub fn err_syntax() -> Error {
  Error::invalid_data("ERR syntax error")
}

/// 标准整数超出范围错误
#[inline]
pub fn err_not_integer() -> Error {
  Error::invalid_data("ERR value is not an integer or out of range")
}

/// 标准浮点数错误
#[inline]
pub fn err_not_float() -> Error {
  Error::invalid_data("ERR value is not a valid float")
}

/// 检查最少参数数量
#[inline]
pub fn check_min_args(cmd_name: &str, args: &[&[u8]], min: usize) -> Result<()> {
  if args.len() < min {
    Err(err_wrong_args(cmd_name))
  } else {
    Ok(())
  }
}

/// 检查精确参数数量
#[inline]
pub fn check_exact_args(cmd_name: &str, args: &[&[u8]], exact: usize) -> Result<()> {
  if args.len() != exact {
    Err(err_wrong_args(cmd_name))
  } else {
    Ok(())
  }
}

/// 零拷贝将切片转换为 &str（非法 UTF-8 返回空串）
#[inline]
pub fn arg_str(b: &[u8]) -> &str {
  from_utf8(b).unwrap_or("")
}

/// 将切片转换为拥有所有权的 String
#[inline]
pub fn arg_string(b: &[u8]) -> String {
  match from_utf8(b) {
    Ok(s) => s.to_string(),
    Err(_) => String::from_utf8_lossy(b).into_owned(),
  }
}

/// 快速解析 i64
#[inline]
pub fn arg_i64(b: &[u8]) -> Option<i64> {
  parse_i64_fast(b)
}

/// 快速解析 u64
#[inline]
pub fn arg_u64(b: &[u8]) -> Option<u64> {
  parse_u64_fast(b)
}

/// 快速解析 f64
#[inline]
pub fn arg_f64(b: &[u8]) -> Option<f64> {
  parse_f64_fast(b)
}

/// 快速解析 usize
#[inline]
pub fn arg_usize(b: &[u8]) -> Option<usize> {
  parse_u64_fast(b).map(|v| v as usize)
}

/// 快速解析 u32
#[inline]
pub fn arg_u32(b: &[u8]) -> Option<u32> {
  parse_u64_fast(b).and_then(|v| u32::try_from(v).ok())
}

/// 快速解析 u16
#[inline]
pub fn arg_u16(b: &[u8]) -> Option<u16> {
  parse_u64_fast(b).and_then(|v| u16::try_from(v).ok())
}

/// 快速解析 u8
#[inline]
pub fn arg_u8(b: &[u8]) -> Option<u8> {
  parse_u64_fast(b).and_then(|v| u8::try_from(v).ok())
}

/// 快速解析浮点数字段（失败返回 ERR value is not a valid float）
#[inline]
pub fn parse_float_strict(b: &[u8]) -> Result<f64> {
  match parse_f64_fast(b) {
    Some(f) if !f.is_nan() => Ok(f),
    _ => Err(err_not_float()),
  }
}

/// 快速解析整型字段（失败返回 ERR value is not an integer or out of range）
#[inline]
pub fn parse_i64_strict(b: &[u8]) -> Result<i64> {
  parse_i64_fast(b).ok_or_else(err_not_integer)
}

/// 快速解析无符号整型字段（失败返回 ERR value is not an integer or out of range）
#[inline]
pub fn parse_u64_strict(b: &[u8]) -> Result<u64> {
  parse_u64_fast(b).ok_or_else(err_not_integer)
}

/// 快速解析 usize 字段（失败返回 ERR value is not an integer or out of range）
#[inline]
pub fn parse_usize_strict(b: &[u8]) -> Result<usize> {
  parse_u64_fast(b)
    .map(|v| v as usize)
    .ok_or_else(err_not_integer)
}

/// 快速解析字段过期条件 (NX / XX / GT / LT)
#[inline]
pub fn parse_expire_condition(opt: &[u8]) -> Option<ExpireCondition> {
  if opt.eq_ignore_ascii_case(b"nx") {
    Some(ExpireCondition::NX)
  } else if opt.eq_ignore_ascii_case(b"xx") {
    Some(ExpireCondition::XX)
  } else if opt.eq_ignore_ascii_case(b"gt") {
    Some(ExpireCondition::GT)
  } else if opt.eq_ignore_ascii_case(b"lt") {
    Some(ExpireCondition::LT)
  } else {
    None
  }
}

/// 游标扫描类选项解析器 [MATCH pattern] [COUNT count]
#[derive(Default, Debug, Clone)]
pub struct ScanOptions {
  pub pattern: Option<String>,
  pub count: Option<usize>,
}

impl ScanOptions {
  pub fn parse(args: &[&[u8]], start_idx: usize) -> Self {
    let mut opts = Self::default();
    let mut i = start_idx;
    while i < args.len() {
      let opt = args[i];
      if opt.eq_ignore_ascii_case(b"match") && i + 1 < args.len() {
        opts.pattern = Some(arg_string(args[i + 1]));
        i += 2;
      } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
        opts.count = arg_usize(args[i + 1]);
        i += 2;
      } else {
        i += 1;
      }
    }
    opts
  }
}

/// 浮点数边界解析（支持 '(' 开区间与无穷大）
#[inline]
pub fn parse_score_boundary(s: &str) -> (f64, bool) {
  parse_score_boundary_bytes(s.as_bytes())
}

/// 字节切片版浮点数边界解析
#[inline]
pub fn parse_score_boundary_bytes(s: &[u8]) -> (f64, bool) {
  if s.is_empty() {
    return (0.0, false);
  }
  if s.eq_ignore_ascii_case(b"-inf") || s.eq_ignore_ascii_case(b"-infinity") {
    (f64::NEG_INFINITY, false)
  } else if s.eq_ignore_ascii_case(b"+inf")
    || s.eq_ignore_ascii_case(b"inf")
    || s.eq_ignore_ascii_case(b"+infinity")
    || s.eq_ignore_ascii_case(b"infinity")
  {
    (f64::INFINITY, false)
  } else if s[0] == b'(' {
    (parse_f64_fast(&s[1..]).unwrap_or(0.0), true)
  } else {
    (parse_f64_fast(s).unwrap_or(0.0), false)
  }
}
