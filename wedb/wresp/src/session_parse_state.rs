//! 在 garnet 中的相对路径: libs/server/Resp/Parser/SessionParseState.cs(对标 C# SessionParseState)
use std::cmp::max;

use smallvec::SmallVec;

use crate::{
  Error, Result,
  argslice::ArgSlice,
  read::{MAX_ARGUMENT_LENGTH_BYTES as MAX_ARG_LEN, try_read_unsigned_length_header},
};

/// 内联参数缓冲槽位数（覆盖 1~8 参高频命令，杜绝高频堆分配）
pub(crate) const INLINE_PARAMS: usize = 8;

/// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:SessionParseState
///
/// 槽位为 [`ArgSlice`] 纯整数对（宿主缓冲区间），同一解析态全部槽位共享
/// 同一宿主缓冲（解析期天然成立，全部来自接收缓冲）；字节视图经
/// [`Self::arg_in`] / [`ArgSlice::resolve`] 绑定宿主缓冲借用获取
///
/// 本类型只承载解析态装配面（len/get/arg_in/serialize），不设 C# 的
/// Get* 取参族：参数转数值单点为 `wbase::num` 严格解析面，RESP 参数视图薄封装为
/// [`crate::ext::RespSliceExt`]，同一参数不得有两套入口与两种严格性口径
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
  /// 解析态槽位容量下界（高频小命令免堆扩容；crate 内单点消费）
  const MIN_PARAMS: usize = 5;

  #[inline]
  pub fn new() -> Self {
    Self {
      count: 0,
      root_buffer: SmallVec::new(),
      offset: 0,
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:Initialize
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

  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:GetArgSliceByRef
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

  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:Read
  ///
  /// 自接收缓冲读出第 `i` 个参数（`$len\r\n` 头 + 负载 + \r\n），写入解析态
  /// 缓冲；`ptr`/`end` 为接收缓冲游标。头解码复用
  /// [`crate::read::try_read_unsigned_length_header`]（C# :350 同一函数），
  /// 判定次序与 C# 三态逐臂对齐：
  /// - `Ok(true)`：参数装配完成，`ptr` 推进至下一参数起点
  /// - `Ok(false)`：字节未到齐（头不足 3 字节 / 负载或尾部 \r\n 未达 /
  ///   长度超 [`MAX_ARG_LEN`]——C# :350 头消费后仅返回 false），
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
    if length > MAX_ARG_LEN {
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

  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:Parameters
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

  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:GetSerializedLength
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
  /// 在 garnet 中的相对路径:libs/server/Resp/Parser/SessionParseState.cs:SerializeTo
  ///
  /// `buf` 为槽位宿主缓冲；返回写入 `dest` 的字节数（调用方按
  /// [`Self::get_serialized_length`] 预留容量）
  pub fn serialize_to(&self, buf: &[u8], dest: &mut [u8]) -> usize {
    let mut curr = 0usize;
    let params = self.parameters();
    let count = params.len() as i32;
    dest[curr..curr + 4].copy_from_slice(&count.to_le_bytes());
    curr += 4;

    for arg in params {
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
