use std::cmp::max;

use smallvec::SmallVec;

use crate::{argslice::ArgSlice, read::MAX_ARGUMENT_LENGTH_BYTES as MAX_ARG_LEN};

/// 内联参数缓冲槽位数（覆盖 1~8 参高频命令，杜绝高频堆分配）
pub const INLINE_PARAMS: usize = 8;
pub const MAX_ARGUMENT_LENGTH_BYTES: usize = MAX_ARG_LEN as usize;

/// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:SessionParseState
///
/// 槽位为 [`ArgSlice`] 纯整数对（宿主缓冲区间），同一解析态全部槽位共享
/// 同一宿主缓冲（解析期天然成立，全部来自接收缓冲）；字节视图经
/// [`Self::arg_in`] / [`ArgSlice::resolve`] 绑定宿主缓冲借用获取
///
/// 本类型只承载解析态装配面（len/get/arg_in/serialize/slice），不设 C# 的
/// Get* 取参族：参数转数值单点为 `wbase::num` 严格解析面，RESP 参数视图薄封装为
/// [`crate::ext::RespSliceExt`]，同一参数不得有两套入口与两种严格性口径
#[derive(Debug, Clone)]
pub struct SessionParseState {
  pub count: usize,
  pub root_buffer: SmallVec<[ArgSlice; INLINE_PARAMS]>,
  pub offset: usize,
}

impl Default for SessionParseState {
  fn default() -> Self {
    Self::new()
  }
}

impl SessionParseState {
  pub const MIN_PARAMS: usize = 5;

  #[inline]
  pub fn new() -> Self {
    Self {
      count: 0,
      root_buffer: SmallVec::new(),
      offset: 0,
    }
  }

  #[inline]
  pub fn initialize(&mut self, count: usize) {
    self.count = count;
    self.offset = 0;
    let cap = max(count, Self::MIN_PARAMS);
    self.root_buffer.clear();
    self.root_buffer.resize(cap, ArgSlice::new(0, 0));
  }

  #[inline]
  pub fn slice(&self, idx_offset: usize) -> Self {
    let new_count = self.count.saturating_sub(idx_offset);
    let start = self.offset + idx_offset;
    let end = (start + new_count).min(self.root_buffer.len());
    let mut root_buffer = SmallVec::new();
    if start < end {
      root_buffer.extend_from_slice(&self.root_buffer[start..end]);
    }
    Self {
      count: new_count,
      root_buffer,
      offset: 0,
    }
  }

  #[inline]
  pub fn get_arg_slice_by_ref(&self, i: usize) -> ArgSlice {
    debug_assert!(i < self.count);
    self.root_buffer[self.offset + i]
  }

  /// 槽位区间 + 宿主缓冲 → 参数视图（`GetArgSliceByRef(i).Span` 的安全承接）
  #[inline]
  pub fn arg_in<'a>(&self, buf: &'a [u8], i: usize) -> &'a [u8] {
    self.get_arg_slice_by_ref(i).resolve(buf)
  }

  /// libs/server/Resp/Parser/SessionParseState.cs:Read
  ///
  /// 自接收缓冲读出第 `i` 个参数（`$len\r\n` 头 + 负载 + \r\n），
  /// 写入解析态缓冲；`ptr`/`end` 为接收缓冲游标。头部非法、负长度（C#
  /// ThrowInvalidStringLength）、超 [`MAX_ARGUMENT_LENGTH_BYTES`] 或负载未
  /// 完整到达均返回 false；尾部非 \r\n 在 C# 抛异常断连，此处按 false 降级
  /// 由调用方按协议错误处置
  pub fn read(&mut self, i: usize, buffer: &[u8], ptr: &mut usize, end: usize) -> bool {
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
    if i >= self.root_buffer.len() {
      self.root_buffer.resize(i + 1, ArgSlice::new(0, 0));
    }
    self.root_buffer[i] = ArgSlice::new(*ptr, length);
    if i >= self.count {
      self.count = i + 1;
    }
    *ptr += length + 2;
    true
  }

  #[inline]
  pub fn get_serialized_length(&self) -> usize {
    size_of::<i32>()
      + self.root_buffer[self.offset..self.offset + self.count]
        .iter()
        .map(|arg| arg.total_size())
        .sum::<usize>()
  }

  /// 将参数数组序列化为 `[count i32][每参数 4B 长度前缀 + 数据]` 布局
  ///
  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:SerializeTo
  ///
  /// `buf` 为槽位宿主缓冲；返回写入 `dest` 的字节数（调用方按
  /// [`Self::get_serialized_length`] 预留容量）
  pub fn serialize_to(&self, buf: &[u8], dest: &mut [u8]) -> usize {
    let mut curr = 0usize;
    let count = self.count as i32;
    dest[curr..curr + 4].copy_from_slice(&count.to_le_bytes());
    curr += 4;

    for arg in &self.root_buffer[self.offset..self.offset + self.count] {
      let len = arg.length as u32;
      dest[curr..curr + 4].copy_from_slice(&len.to_le_bytes());
      curr += 4;
      dest[curr..curr + arg.length].copy_from_slice(arg.resolve(buf));
      curr += arg.length;
    }
    debug_assert!(curr <= dest.len(), "写入字节数超出预留容量上限");
    curr
  }
}

#[cfg(test)]
mod tests {
  use wbase::num::{strict_i32, strict_i64};

  use super::*;

  /// 以宿主缓冲连续排布构造解析态（返回状态与宿主缓冲）
  fn state_with_args(args: &[&[u8]]) -> (SessionParseState, Vec<u8>) {
    let mut buf = Vec::new();
    let mut slices = Vec::with_capacity(args.len());
    for arg in args {
      slices.push(ArgSlice::new(buf.len(), arg.len()));
      buf.extend_from_slice(arg);
    }
    let mut state = SessionParseState::new();
    // initialize_with_args 已随零消费口删除：字段 pub 直填（一处初始化形态）
    state.initialize(slices.len());
    state.root_buffer[..slices.len()].copy_from_slice(&slices);
    (state, buf)
  }

  #[test]
  fn serialize_roundtrip_layout() {
    let (state, buf) = state_with_args(&[b"SET", b"k", b"v"]);
    let len = state.get_serialized_length();
    let mut dest = vec![0u8; len];
    let written = state.serialize_to(&buf, &mut dest);
    assert_eq!(written, len);

    // 布局：[count i32][每参数 4B 长度前缀 + 数据]
    assert_eq!(&dest[..4], &3i32.to_le_bytes());
    assert_eq!(&dest[4..8], &3u32.to_le_bytes());
    assert_eq!(&dest[8..11], b"SET");
    assert_eq!(&dest[11..15], &1u32.to_le_bytes());
    assert_eq!(&dest[15..16], b"k");
    assert_eq!(&dest[16..20], &1u32.to_le_bytes());
    assert_eq!(&dest[20..21], b"v");
  }

  #[test]
  fn slice_and_arg_in() {
    let (state, buf) = state_with_args(&[b"k", b"v1", b"v2"]);
    assert_eq!(state.arg_in(&buf, 0), b"k");

    let sliced = state.slice(1);
    assert_eq!(sliced.count, 2);
    assert_eq!(sliced.arg_in(&buf, 0), b"v1");
    assert_eq!(sliced.arg_in(&buf, 1), b"v2");
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
  fn read_parses_bulk_argument_and_advances() {
    // *2\r\n$3\r\nSET\r\n$2\r\nv1\r\n 的参数段（命令名已被快路径消费）
    let buffer = b"$3\r\nSET\r\n$2\r\nv1\r\n";
    let mut state = SessionParseState::new();
    state.initialize(2);
    let mut ptr = 0usize;
    assert!(state.read(0, buffer, &mut ptr, buffer.len()));
    assert_eq!(state.arg_in(buffer, 0), b"SET");
    assert_eq!(ptr, 9);
    assert!(state.read(1, buffer, &mut ptr, buffer.len()));
    assert_eq!(state.arg_in(buffer, 1), b"v1");
    assert_eq!(ptr, buffer.len());

    // 负载未完整到达 → false 且游标停在负载起点（可重试）
    let partial = b"$5\r\nab";
    let mut ptr = 0usize;
    assert!(!state.read(0, partial, &mut ptr, partial.len()));

    // 空串参数 $0\r\n\r\n
    let empty = b"$0\r\n\r\n";
    let mut ptr = 0usize;
    assert!(state.read(0, empty, &mut ptr, empty.len()));
    assert_eq!(state.arg_in(empty, 0), b"");

    // 负长度（C# ThrowInvalidStringLength）→ false
    let neg = b"$-1\r\n";
    let mut ptr = 0usize;
    assert!(!state.read(0, neg, &mut ptr, neg.len()));

    // 头不完整
    let mut ptr = 0usize;
    assert!(!state.read(0, b"$1", &mut ptr, 2));
  }
}
