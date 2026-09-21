use wbase::num::strict_i64;

use crate::manager::{length_in_bytes, try_validate_bitfield_offset};

/// BITFIELD 符号位（C# BitFieldSign：UNSIGNED = 0x0，SIGNED = 0x80）
pub const BIT_FIELD_SIGN_SIGNED: u8 = 0x80;

/// BITFIELD 子命令（C# RespCommand.GET / SET / INCRBY）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldSecondaryCommand {
  /// 读取
  Get,
  /// 写入并回旧值
  Set,
  /// 自增并回新值
  IncrBy,
}

/// BITFIELD 溢出策略字节值（C# BitmapCommands.cs:BitFieldOverflow 的 byte 序：
/// WRAP=0 / SAT=1 / FAIL=2，一处定义供命令域与解析器共享）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BitFieldOverflow {
  /// 回绕
  Wrap = 0,
  /// 饱和
  Sat = 1,
  /// 失败（回吐 nil 且不落盘）
  Fail = 2,
}

/// 解析溢出策略切片（大小写不敏感匹配 WRAP / SAT / FAIL）
///
/// libs/server/SessionParseStateExtensions.cs:TryGetBitFieldOverflow
/// （C# 的 BitmapCommands / PrivateMethods 调用点均走该解析态扩展口，全仓单点）
pub fn parse_bitfield_overflow_slice(raw: &[u8]) -> Option<BitFieldOverflow> {
  if raw.eq_ignore_ascii_case(b"WRAP") {
    Some(BitFieldOverflow::Wrap)
  } else if raw.eq_ignore_ascii_case(b"SAT") {
    Some(BitFieldOverflow::Sat)
  } else if raw.eq_ignore_ascii_case(b"FAIL") {
    Some(BitFieldOverflow::Fail)
  } else {
    None
  }
}

/// 解析位域编码（u<位宽> / i<位宽>）
///
/// libs/server/SessionParseStateExtensions.cs:TryGetBitfieldEncoding
/// （C# 定义在解析态扩展口，BitmapCommands 与 MainStore/PrivateMethods 均为调用点）
pub fn parse_bitfield_encoding(encoding: &[u8]) -> Option<(u8, bool)> {
  if encoding.len() <= 1 {
    return None;
  }
  let signed = match encoding[0] {
    b'i' => true,
    b'u' => false,
    _ => return None,
  };
  let bit_count = strict_i64(&encoding[1..])?;
  (bit_count > 0
    && if signed {
      bit_count <= 64
    } else {
      bit_count < 64
    })
  .then_some((bit_count as u8, signed))
}

/// 位域 offset 解析：`#<n>` 倍乘形式或裸位偏移，须 ≥ 0
///
/// libs/server/SessionParseStateExtensions.cs:TryGetBitfieldOffset
pub fn parse_bitfield_offset(raw: &[u8]) -> Option<(i64, bool)> {
  let (digits, multiply_offset) = match raw {
    [b'#', rest @ ..] if !rest.is_empty() => (rest, true),
    _ => (raw, false),
  };
  let offset = strict_i64(digits)?;
  (offset >= 0).then_some((offset, multiply_offset))
}

/// 位域解析期联查：encoding + offset 校验并给出 typeInfo / 归一化 offset
///
/// C# 侧为 TryGetBitfieldEncoding + TryGetBitfieldOffset +
/// BitmapManager.TryValidateBitfieldOffset 三步；返回（typeInfo，offset）
pub fn parse_bitfield_type_offset(encoding: &[u8], offset_raw: &[u8]) -> Option<(u8, i64)> {
  let (bit_count, signed) = parse_bitfield_encoding(encoding)?;
  let (offset, multiply_offset) = parse_bitfield_offset(offset_raw)?;
  let (normalized_offset, _) = try_validate_bitfield_offset(offset, bit_count, multiply_offset)?;
  let type_info = if signed { 0x80 | bit_count } else { bit_count };
  Some((type_info, normalized_offset))
}

/// BITFIELD 命令参数（C# BitFieldCmdArgs；C# 以 FieldOffset 布局打包，
/// Rust 侧平铺字段，逻辑面一致）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitFieldCmdArgs {
  /// 子命令
  pub secondary_command: BitFieldSecondaryCommand,
  /// 编码信息：低 7 位为位宽，最高位（0x80）为符号
  pub type_info: u8,
  /// 位偏移（`#` 倍乘已归一化）
  pub offset: i64,
  /// SET 的写入值 / INCRBY 的增量
  pub value: i64,
  /// 溢出策略（BitFieldOverflow 的 byte 值）
  pub overflow_type: u8,
}

impl BitFieldCmdArgs {
  /// libs/server/Resp/Bitmap/BitmapCommands.cs:BitFieldCmdArgs..ctor
  pub fn new(
    secondary_command: BitFieldSecondaryCommand,
    type_info: u8,
    offset: i64,
    value: i64,
    overflow_type: u8,
  ) -> Self {
    Self {
      secondary_command,
      type_info,
      offset,
      value,
      overflow_type,
    }
  }
}

/// 位域所需字节数（offset 为原始位偏移）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:LengthFromType
#[inline]
pub fn length_from_type(args: &BitFieldCmdArgs) -> i32 {
  let offset = args.offset;
  let bit_count = args.type_info & 0x7F;
  // 调用方已在解析期经 TryValidateBitfieldOffset 校验（C# DEBUG 断言同口径）
  let end_offset = offset + i64::from(bit_count) - 1;
  length_in_bytes(end_offset).unwrap_or(0)
}

/// BITFIELD 写路径的分配尺寸
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:NewBlockAllocLengthFromType
#[inline]
pub fn new_block_alloc_length_from_type(args: &BitFieldCmdArgs, value_len: i32) -> i32 {
  let length_in_bytes = length_from_type(args);
  if value_len > length_in_bytes {
    value_len
  } else {
    length_in_bytes
  }
}
