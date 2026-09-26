//! 在 garnet 中的相对路径: libs/server/Resp/RespSession? 对标 C# SessionParseState
use std::cmp::max;

use smallvec::SmallVec;

use crate::{
  Error, Result,
  argslice::ArgSlice,
  read::{MAX_ARGUMENT_LENGTH_BYTES as MAX_ARG_LEN, try_read_unsigned_length_header},
};

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

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:Initialize
  ///
  /// 对位 C# `Initialize(int count)` 的容量早退：只更新计数与视图起点，
  /// 容量满足（`root_buffer.len() >= cap`）时一个槽位都不覆写，消除每命令
  /// 一次的 clear+resize 写放大。清零对本结构无正确性作用——读面
  /// [`Self::parameters`] / [`Self::get_arg_slice_by_ref`] / [`Self::arg_in`]
  /// 与写面 [`Self::read`] 全部以 `count` 为界，越出 `count` 的陈旧槽位不可达；
  /// 命令解析的写循环 `0..count` 在读取前必覆旧值。不变式
  /// `root_buffer.len() >= count` 由「仅在 `len < cap` 时扩容」维持。
  #[inline]
  pub fn initialize(&mut self, count: usize) {
    self.count = count;
    self.offset = 0;
    let cap = max(count, Self::MIN_PARAMS);
    if self.root_buffer.len() < cap {
      self.root_buffer.resize(cap, ArgSlice::new(0, 0));
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.count
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.count == 0
  }

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:Slice
  ///
  /// C# 单参 `Slice(int idxOffset)`（:219）1:1：视图起点 +idx_offset、计数
  /// -idx_offset。复用底层 `root_buffer` 杜绝堆分配与内存拷贝。
  #[inline]
  pub fn slice(&self, idx_offset: usize) -> Self {
    let offset = self.offset.saturating_add(idx_offset);
    let count = self.count.saturating_sub(idx_offset);
    Self {
      count,
      root_buffer: self.root_buffer.clone(),
      offset,
    }
  }

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:GetArgSliceByRef
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

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:Read
  ///
  /// 自接收缓冲读出第 `i` 个参数（`$len\r\n` 头 + 负载 + \r\n），写入解析态
  /// 缓冲；`ptr`/`end` 为接收缓冲游标。头解码复用
  /// [`crate::read::try_read_unsigned_length_header`]（C# :350 同一函数），
  /// 判定次序与 C# 三态逐臂对齐：
  /// - `Ok(true)`：参数装配完成，`ptr` 推进至下一参数起点
  /// - `Ok(false)`：字节未到齐（头不足 3 字节 / 负载或尾部 \r\n 未达 /
  ///   长度超 [`MAX_ARGUMENT_LENGTH_BYTES`]——C# :350 头消费后仅返回 false），
  ///   调用方回退游标等待后续字节
  /// - `Err(Error)`：协议违例（非 `$` sigil / 无数字 / 头尾与值尾终止符不符
  ///   → UnexpectedToken；负长度含 `_\r\n` 与 `$-1\r\n` → InvalidStringLength；
  ///   数值溢出 → IntegerOverflow），C# 于此处 throw RespParsingException 由
  ///   上层写 `ERR Protocol Error` 后断连，调用方置会话违例载体，
  ///   绝不得按半包等待（真畸形帧若退化为 false 会令会话游标永不前进）
  pub fn read(&mut self, i: usize, buffer: &[u8], ptr: &mut usize, end: usize) -> Result<bool> {
    // 长度头单点解码（C# TryReadUnsignedLengthHeader → TryReadSignedLengthHeader）
    let mut head = &buffer[*ptr..end];
    let mut length = 0;
    if !try_read_unsigned_length_header(&mut length, &mut head, b'$')? {
      return Ok(false);
    }
    // 超 512MB 上限：C# Read 头消费后仅返回 false，同口径按未到齐处置
    if length as usize > MAX_ARGUMENT_LENGTH_BYTES {
      return Ok(false);
    }
    *ptr = end - head.len();
    let length = length as usize;

    // 负载 + '\r\n' 须完整到达（C# slice.Set 后 ptr += len + 2 越界检查）
    if *ptr + length + 2 > end {
      return Ok(false);
    }
    // 值尾终止符不符：C# :359 ThrowUnexpectedToken
    if &buffer[*ptr + length..*ptr + length + 2] != b"\r\n" {
      return Err(Error::UnexpectedToken(buffer[*ptr + length]));
    }
    if i >= self.root_buffer.len() {
      self.root_buffer.resize(i + 1, ArgSlice::new(0, 0));
    }
    self.root_buffer[i] = ArgSlice::new(*ptr, length);
    if i >= self.count {
      self.count = i + 1;
    }
    *ptr += length + 2;
    Ok(true)
  }

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:Parameters
  ///
  /// 当前视图窗口对应的参数槽位切片（对标 C# `ReadOnlySpan<PinnedSpanByte> Parameters`）
  #[inline]
  pub fn parameters(&self) -> &[ArgSlice] {
    if self.count == 0 {
      &[]
    } else {
      let start = self.offset;
      let end = start.saturating_add(self.count).min(self.root_buffer.len());
      if start >= end {
        &[]
      } else {
        &self.root_buffer[start..end]
      }
    }
  }

  /// 在 garnet 中的相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:GetSerializedLength
  ///
  /// 序列化预留容量预算（C# `sizeof(int) + Σ TotalSize` 同式，槽位窗口
  /// [offset, offset+count)）；消费点 [`crate::session_parse_state::SessionParseState::serialize_to`]
  /// 与 wnode 慢日志快照封装 `serialize_snapshot`
  #[inline]
  pub fn get_serialized_length(&self) -> usize {
    size_of::<i32>()
      + self
        .parameters()
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
    let count = self.parameters().len() as i32;
    dest[curr..curr + 4].copy_from_slice(&count.to_le_bytes());
    curr += 4;

    for arg in self.parameters() {
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
  fn multi_level_slice_and_serialize() {
    let (state, buf) = state_with_args(&[b"CMD", b"k1", b"v1", b"k2", b"v2"]);
    assert_eq!(state.count, 5);
    assert_eq!(state.offset, 0);
    assert_eq!(state.arg_in(&buf, 0), b"CMD");

    // 第 1 层切片：跳过 CMD
    let s1 = state.slice(1);
    assert_eq!(s1.count, 4);
    assert_eq!(s1.offset, 1);
    assert_eq!(s1.arg_in(&buf, 0), b"k1");
    assert_eq!(s1.arg_in(&buf, 3), b"v2");

    // 第 2 层切片：再跳过 k1, v1
    let s2 = s1.slice(2);
    assert_eq!(s2.count, 2);
    assert_eq!(s2.offset, 3);
    assert_eq!(s2.arg_in(&buf, 0), b"k2");
    assert_eq!(s2.arg_in(&buf, 1), b"v2");

    // 验证切片后的序列化正确性
    let len = s2.get_serialized_length();
    let mut dest = vec![0u8; len];
    let written = s2.serialize_to(&buf, &mut dest);
    assert_eq!(written, len);
    assert_eq!(&dest[..4], &2i32.to_le_bytes());
    assert_eq!(&dest[4..8], &2u32.to_le_bytes());
    assert_eq!(&dest[8..10], b"k2");
    assert_eq!(&dest[10..14], &2u32.to_le_bytes());
    assert_eq!(&dest[14..16], b"v2");

    // 切片超出范围：count 为 0
    let s3 = s2.slice(5);
    assert_eq!(s3.count, 0);
    assert_eq!(s3.offset, 8);
    assert_eq!(s3.get_serialized_length(), 4);
    let mut dest_empty = vec![0u8; 4];
    assert_eq!(s3.serialize_to(&buf, &mut dest_empty), 4);
    assert_eq!(&dest_empty[..4], &0i32.to_le_bytes());
  }

  #[test]
  fn strict_int_rejects_leading_zeros_and_allows_sign() {
    // 前导零拒绝（前导零拒收系 rust 统一严格收口，C# TryReadInt64Safe 拒绝但 TryReadInt32Safe 因死参放行，见 doc/zh/deviations.md §32）
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
    assert!(state.read(0, buffer, &mut ptr, buffer.len()).unwrap());
    assert_eq!(state.arg_in(buffer, 0), b"SET");
    assert_eq!(ptr, 9);
    assert!(state.read(1, buffer, &mut ptr, buffer.len()).unwrap());
    assert_eq!(state.arg_in(buffer, 1), b"v1");
    assert_eq!(ptr, buffer.len());

    // 负载未完整到达 → Ok(false) 且游标停在负载起点（可重试）
    let partial = b"$5\r\nab";
    let mut ptr = 0usize;
    assert!(!state.read(0, partial, &mut ptr, partial.len()).unwrap());

    // 空串参数 $0\r\n\r\n
    let empty = b"$0\r\n\r\n";
    let mut ptr = 0usize;
    assert!(state.read(0, empty, &mut ptr, empty.len()).unwrap());
    assert_eq!(state.arg_in(empty, 0), b"");

    // 负长度（C# ThrowInvalidStringLength）→ 协议违例
    let neg = b"$-1\r\n";
    let mut ptr = 0usize;
    assert_eq!(
      state.read(0, neg, &mut ptr, neg.len()),
      Err(Error::InvalidStringLength(-1))
    );

    // 头不完整
    let mut ptr = 0usize;
    assert!(!state.read(0, b"$1", &mut ptr, 2).unwrap());
  }
}
