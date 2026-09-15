//! RESP 会话解析态的类型化读取扩展
//! （对标 libs/server/Resp/Parser/SessionParseState.cs + ParseUtils.cs + RespReadUtils.cs）
//!
//! C# `SessionParseState` 为单一 struct，参数缓冲与类型化 getter 同体；
//! Rust 侧参数缓冲本体落在本 crate 根 [`SessionParseState`](crate::session_parse_state)
//! （被 inputs 等域引用），本文件承接同一 C# 类型的剩余成员：
//! 变参初始化 / 按位写入 / int·long·double·float·string·bool 严格解析族。
//! 以 `pub use` 保持 `resp::parser::SessionParseState` 路径可用。
//!
//! C# 同名成员中参数缓冲 / 序列化骨架（SessionParseState.cs:GetSerializedLength /
//! SerializeTo / DeserializeFrom / GetArgSliceByRef / Initialize / Slice）已由
//! crate 根 `session_parse_state` 域实现并公开同形方法，本文件不再重复。
//!
//! 严格数值解析族逐项对齐 C#：
//! - 整数（TryReadInt32Safe/TryReadInt64Safe, allowLeadingZeros: false）：
//!   可选 +/- 号、拒绝前导零（单独 0 允许）、纯数字整体消费、值域校验；
//! - 浮点（Utf8Parser.TryParse + TryReadInfinity 白名单）：NaN 恒拒绝，
//!   inf 字面量白名单 inf/+inf/-inf/infinity/+infinity/-infinity
//!   （大小写不敏感）且须 canBeInfinite；实现单一落 [`wbase::num`]
//!   （strict_i32/strict_i64/strict_f32/strict_f64），本文件仅承接
//!   `SessionParseState` 类型化读取的薄适配。

use std::{ptr::null, str::from_utf8};

use wbase::num::{strict_f32, strict_f64, strict_i32, strict_i64};
use wresp::{ArgSlice, MAX_ARGUMENT_LENGTH_BYTES as MAX_ARG_LEN, SessionParseState};

pub const MAX_ARGUMENT_LENGTH_BYTES: usize = MAX_ARG_LEN as usize;

/// libs/server/Resp/Parser/SessionParseState.cs:SetArgument
///
/// 写入指定下标的参数；下标越过 Count 时推进 Count（i >= Count → Count = i + 1）
pub fn set_argument(state: &mut SessionParseState, i: usize, arg: ArgSlice) {
  if i >= state.root_buffer.len() {
    state.root_buffer.resize(i + 1, ArgSlice::new(null(), 0));
  }
  state.root_buffer[i] = arg;
  if i >= state.count {
    state.count = i + 1;
  }
}

/// libs/server/Resp/Parser/SessionParseState.cs:SetArguments
///
/// 自 start 下标起连续写入参数（C# 调用方保证容量充足）
pub fn set_arguments(state: &mut SessionParseState, start: usize, args: &[ArgSlice]) {
  debug_assert!(start + args.len() <= state.count);
  let dst = &mut state.root_buffer[start..start + args.len()];
  for (slot, &arg) in dst.iter_mut().zip(args) {
    *slot = arg;
  }
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetInt
///
/// C# 抛 RespParsingException（调用方约定已校验）；Rust 侧以 None 表达
/// 解析失败，调用方按协议错误处置
#[inline]
pub fn get_int(state: &SessionParseState, i: usize) -> Option<i32> {
  try_get_int(state, i)
}

/// libs/server/Resp/Parser/SessionParseState.cs:TryGetInt
#[inline]
pub fn try_get_int(state: &SessionParseState, i: usize) -> Option<i32> {
  strict_i32(state.get_arg_slice_by_ref(i).as_slice())
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetLong
#[inline]
pub fn get_long(state: &SessionParseState, i: usize) -> Option<i64> {
  try_get_long(state, i)
}

/// libs/server/Resp/Parser/SessionParseState.cs:TryGetLong
#[inline]
pub fn try_get_long(state: &SessionParseState, i: usize) -> Option<i64> {
  strict_i64(state.get_arg_slice_by_ref(i).as_slice())
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetDouble
#[inline]
pub fn get_double(state: &SessionParseState, i: usize, can_be_infinite: bool) -> Option<f64> {
  try_get_double(state, i, can_be_infinite)
}

/// libs/server/Resp/Parser/SessionParseState.cs:TryGetDouble
#[inline]
pub fn try_get_double(state: &SessionParseState, i: usize, can_be_infinite: bool) -> Option<f64> {
  strict_f64(state.get_arg_slice_by_ref(i).as_slice(), can_be_infinite)
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetFloat
#[inline]
pub fn get_float(state: &SessionParseState, i: usize, can_be_infinite: bool) -> Option<f32> {
  try_get_float(state, i, can_be_infinite)
}

/// libs/server/Resp/Parser/SessionParseState.cs:TryGetFloat
#[inline]
pub fn try_get_float(state: &SessionParseState, i: usize, can_be_infinite: bool) -> Option<f32> {
  strict_f32(state.get_arg_slice_by_ref(i).as_slice(), can_be_infinite)
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetString
///
/// ASCII 字符串参数；非 UTF-8 视为 null（C# ReadString 失败返回 null）
#[inline]
pub fn get_string(state: &SessionParseState, i: usize) -> Option<&str> {
  from_utf8(state.get_arg_slice_by_ref(i).as_slice()).ok()
}

/// libs/server/Resp/Parser/SessionParseState.cs:GetBool
///
/// 单字节 '1'/'0'（对齐 C# ParseUtils.ReadBool；非法值抛异常 → None）
#[inline]
pub fn get_bool(state: &SessionParseState, i: usize) -> Option<bool> {
  try_get_bool(state, i)
}

/// libs/server/Resp/Parser/SessionParseState.cs:TryGetBool
#[inline]
pub fn try_get_bool(state: &SessionParseState, i: usize) -> Option<bool> {
  match state.get_arg_slice_by_ref(i).as_slice() {
    [b'1'] => Some(true),
    [b'0'] => Some(false),
    _ => None,
  }
}

/// libs/server/Resp/Parser/SessionParseState.cs:Read
///
/// 自接收缓冲读出第 `i` 个参数（`$len\r\n` 头 + 负载 + \r\n），
/// 写入解析态缓冲；`ptr`/`end` 为接收缓冲游标。头部非法、负长度（C#
/// ThrowInvalidStringLength）、超 [`MAX_ARGUMENT_LENGTH_BYTES`] 或负载未
/// 完整到达均返回 false；尾部非 \r\n 在 C# 抛异常断连，此处按 false 降级
/// 由调用方按协议错误处置
pub fn read(
  state: &mut SessionParseState,
  i: usize,
  buffer: &[u8],
  ptr: &mut usize,
  end: usize,
) -> bool {
  // 长度头：$ [+-] 数字 \r\n（C# TryReadSignedLengthHeader 要求 ptr+3 <= end）
  if *ptr + 3 > end || buffer[*ptr] != b'$' {
    return false;
  }
  *ptr += 1;
  // 单次查表解析可选 +/- 号（每条 bulk 参数头部均走此路径）
  let negative = match buffer[*ptr] {
    b'-' => {
      *ptr += 1;
      true
    }
    b'+' => {
      *ptr += 1;
      false
    }
    _ => false,
  };
  let digits_start = *ptr;
  let mut length: i64 = 0;
  while *ptr < end && buffer[*ptr].is_ascii_digit() {
    length = length
      .saturating_mul(10)
      .saturating_add(i64::from(buffer[*ptr] - b'0'));
    *ptr += 1;
  }
  // 无数字或结尾非 \r\n → 头不完整/非法
  if *ptr == digits_start || *ptr + 2 > end || &buffer[*ptr..*ptr + 2] != b"\r\n" {
    return false;
  }
  *ptr += 2;
  if negative || length < 0 || length > MAX_ARGUMENT_LENGTH_BYTES as i64 {
    // C#：负长度抛 ThrowInvalidStringLength；超限返回 false —— 统一 false 降级
    return false;
  }
  let length = length as usize;

  // 负载 + '\r\n' 须完整到达（C# slice.Set 后 ptr += len + 2 越界检查）
  if *ptr + length + 2 > end {
    return false;
  }
  if &buffer[*ptr + length..*ptr + length + 2] != b"\r\n" {
    return false;
  }
  if i >= state.root_buffer.len() {
    state.root_buffer.resize(i + 1, ArgSlice::new(null(), 0));
  }
  state.root_buffer[i] = ArgSlice::new(buffer[*ptr..].as_ptr(), length);
  if i >= state.count {
    state.count = i + 1;
  }
  *ptr += length + 2;
  true
}

#[cfg(test)]
mod tests {
  use super::*;

  fn state_of(args: &[&[u8]]) -> SessionParseState {
    let slices: Vec<ArgSlice> = args
      .iter()
      .map(|a| ArgSlice::new(a.as_ptr(), a.len()))
      .collect();
    let mut state = SessionParseState::new();
    state.initialize_with_args(&slices);
    state
  }

  #[test]
  fn typed_getters_roundtrip() {
    let state = state_of(&[b"42", b"-7", b"3.5", b"1", b"abc", b"9223372036854775807"]);

    assert_eq!(try_get_int(&state, 0), Some(42));
    assert_eq!(try_get_int(&state, 1), Some(-7));
    // i64 量级溢出 i32 → None，i64 通道可用
    assert_eq!(try_get_int(&state, 5), None);
    assert_eq!(try_get_long(&state, 5), Some(i64::MAX));
    assert_eq!(try_get_long(&state, 4), None);

    assert_eq!(try_get_double(&state, 2, false), Some(3.5));
    assert_eq!(try_get_double(&state, 4, false), None);
    assert_eq!(try_get_bool(&state, 3), Some(true));
    assert_eq!(try_get_bool(&state, 0), None);
    assert_eq!(get_string(&state, 1), Some("-7"));
  }

  #[test]
  fn strict_int_rejects_leading_zeros_and_allows_sign() {
    // 前导零拒绝（C# allowLeadingZeros: false）
    assert_eq!(strict_i64(b"007"), None);
    assert_eq!(strict_i64(b"-007"), None);
    assert_eq!(strict_i32(b"01"), None);
    // 单独 0 / -0 / +0 合法
    assert_eq!(strict_i64(b"0"), Some(0));
    assert_eq!(strict_i64(b"-0"), Some(0));
    assert_eq!(strict_i64(b"+0"), Some(0));
    // 可选符号
    assert_eq!(strict_i64(b"+5"), Some(5));
    assert_eq!(strict_i64(b"-5"), Some(-5));
    // i64::MIN 精确可达（C# u64 中转语义）
    assert_eq!(strict_i64(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(strict_i64(b"-9223372036854775809"), None);
    assert_eq!(strict_i64(b"9223372036854775808"), None);
    // 尾随垃圾 / 空串 / 非数字
    assert_eq!(strict_i64(b"5 "), None);
    assert_eq!(strict_i64(b""), None);
    assert_eq!(strict_i64(b"1x"), None);
    // i32 值域
    assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
    assert_eq!(strict_i32(b"2147483648"), None);
    assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
  }

  #[test]
  fn infinite_gate_and_nan_rejection() {
    let state = state_of(&[b"inf", b"-inf", b"nan", b"1e3", b"+inf", b"Infinity"]);
    // INF 白名单（大小写不敏感、+INF 同号）
    assert_eq!(try_get_double(&state, 0, true), Some(f64::INFINITY));
    assert_eq!(try_get_double(&state, 0, false), None);
    assert_eq!(try_get_double(&state, 1, true), Some(f64::NEG_INFINITY));
    assert_eq!(try_get_double(&state, 4, true), Some(f64::INFINITY));
    // NaN 恒拒绝；infinity 全拼不在 TryReadInfinity 白名单（仅 inf/+inf/-inf）
    assert_eq!(try_get_double(&state, 2, true), None);
    assert_eq!(try_get_double(&state, 5, true), None);
    assert_eq!(try_get_float(&state, 3, false), Some(1000.0));
    // 数值溢出至 inf（"1e999"）：C# Utf8Parser 首分支接受，不受白名单约束
    assert_eq!(strict_f64(b"1e999", false), Some(f64::INFINITY));
  }

  #[test]
  fn read_parses_bulk_argument_and_advances() {
    // *2\r\n$3\r\nSET\r\n$2\r\nv1\r\n 的参数段（命令名已被快路径消费）
    let buffer = b"$3\r\nSET\r\n$2\r\nv1\r\n";
    let mut state = SessionParseState::new();
    state.initialize(2);
    let mut ptr = 0usize;
    assert!(read(&mut state, 0, buffer, &mut ptr, buffer.len()));
    assert_eq!(state.get_arg_slice_by_ref(0).as_slice(), b"SET");
    assert_eq!(ptr, 9);
    assert!(read(&mut state, 1, buffer, &mut ptr, buffer.len()));
    assert_eq!(state.get_arg_slice_by_ref(1).as_slice(), b"v1");
    assert_eq!(ptr, buffer.len());

    // 负载未完整到达 → false 且游标停在负载起点（可重试）
    let partial = b"$5\r\nab";
    let mut ptr = 0usize;
    assert!(!read(&mut state, 0, partial, &mut ptr, partial.len()));

    // 空串参数 $0\r\n\r\n
    let empty = b"$0\r\n\r\n";
    let mut ptr = 0usize;
    assert!(read(&mut state, 0, empty, &mut ptr, empty.len()));
    assert_eq!(state.get_arg_slice_by_ref(0).as_slice(), b"");

    // 负长度（C# ThrowInvalidStringLength）→ false
    let neg = b"$-1\r\n";
    let mut ptr = 0usize;
    assert!(!read(&mut state, 0, neg, &mut ptr, neg.len()));

    // 头不完整
    let mut ptr = 0usize;
    assert!(!read(&mut state, 0, b"$1", &mut ptr, 2));
  }

  #[test]
  fn set_argument_grows_count() {
    let mut state = SessionParseState::new();
    state.initialize(1);
    set_argument(&mut state, 2, ArgSlice::new(b"k".as_ptr(), 1));
    assert_eq!(state.count, 3);
    assert_eq!(state.get_arg_slice_by_ref(2).as_slice(), b"k");

    set_arguments(&mut state, 0, &[ArgSlice::new(b"x".as_ptr(), 1)]);
    assert_eq!(state.get_arg_slice_by_ref(0).as_slice(), b"x");
  }
}
