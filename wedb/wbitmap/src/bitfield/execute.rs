use super::parse::{
  BIT_FIELD_SIGN_SIGNED, BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand,
};
use crate::manager::{index, try_validate_bitfield_offset};

/// 检查位域操作是否溢出，返回（结果值，是否溢出）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckBitfieldOverflow
///
/// 仅 FAIL 策略的溢出会置位溢出标记（WRAP/SAT 已就地消化，无需回吐 nil）。
pub fn check_bitfield_overflow(
  value: i64,
  incr_by: i64,
  bit_count: u8,
  overflow_type: u8,
  signed: bool,
) -> (i64, bool) {
  let (new_value, overflow) = if signed {
    check_signed_bitfield_overflow(value, incr_by, bit_count, overflow_type)
  } else {
    let (nv, overflow) =
      check_unsigned_bitfield_overflow(value as u64, incr_by, bit_count, overflow_type);
    (nv as i64, overflow)
  };

  // WRAP/SAT 忽略溢出标记（无需回 nil 或跳过写入）
  let overflow = overflow_type == BitFieldOverflow::Fail as u8 && overflow;
  (new_value, overflow)
}

/// 无符号位域溢出检查，返回（回绕/饱和后的结果，是否溢出）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckUnsignedBitfieldOverflow
pub fn check_unsigned_bitfield_overflow(
  value: u64,
  incr_by: i64,
  bit_count: u8,
  overflow_type: u8,
) -> (u64, bool) {
  let max_val: u64 = if bit_count == 64 {
    u64::MAX
  } else {
    (1u64 << bit_count) - 1
  };
  let max_add = max_val.wrapping_sub(value);

  let neg = incr_by < 0;
  // 增量绝对值
  let abs_incr_by = if incr_by < 0 {
    (!incr_by as u64).wrapping_add(1)
  } else {
    incr_by as u64
  };
  // 绝对增量超过 maxVal 与当前值之差即溢出
  let overflow = abs_incr_by > max_add;
  // 负增量绝对值大于当前值即下溢
  let underflow = abs_incr_by > value && neg;

  let mut result = if neg {
    value.wrapping_sub(abs_incr_by)
  } else {
    value.wrapping_add(abs_incr_by)
  };
  result &= max_val;
  match overflow_type {
    x if x == BitFieldOverflow::Wrap as u8 => {
      if overflow || underflow {
        return (result, true);
      }
      (result, false)
    }
    x if x == BitFieldOverflow::Sat as u8 => {
      if overflow {
        (max_val, true)
      } else if underflow {
        (0, true)
      } else {
        (result, false)
      }
    }
    x if x == BitFieldOverflow::Fail as u8 => {
      if overflow || underflow {
        (0, true)
      } else {
        (result, false)
      }
    }
    _ => {
      // C# 抛 GarnetException("Invalid overflow type")；解析器保证不可达
      (0, true)
    }
  }
}

/// 有符号位域溢出检查，返回（回绕/饱和后的结果，是否溢出）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckSignedBitfieldOverflow
pub fn check_signed_bitfield_overflow(
  value: i64,
  incr_by: i64,
  bit_count: u8,
  overflow_type: u8,
) -> (i64, bool) {
  let signbit: i64 = 1 << (bit_count - 1);
  let mask: i64 = if bit_count == 64 { -1 } else { signbit - 1 };

  let result = value.wrapping_add(incr_by);
  // 双负操作数可能下溢：符号位为 0 即下溢
  let underflow = (result & signbit) == 0 && value < 0 && incr_by < 0;
  // 双正操作数可能上溢：位宽之上高位有 1 即上溢（64 位宽时无多余位，
  // 上溢表现为符号位翻负）
  let overflow = if bit_count == 64 {
    result < 0 && value >= 0 && incr_by > 0
  } else {
    ((result & !mask) as u64) > 0 && value >= 0 && incr_by > 0
  };

  match overflow_type {
    x if x == BitFieldOverflow::Wrap as u8 => {
      if underflow || overflow {
        let mut res = result as u64;
        if bit_count < 64 {
          let msb = signbit as u64;
          let smask = mask as u64;
          res = if (res & msb) > 0 {
            res | !smask
          } else {
            res & smask
          };
        }
        return (res as i64, true);
      }
      (result, false)
    }
    x if x == BitFieldOverflow::Sat as u8 => {
      let max_val: i64 = if bit_count == 64 {
        i64::MAX
      } else {
        signbit - 1
      };
      if overflow {
        (max_val, true)
      } else if underflow {
        // 下溢饱和到有符号最小值
        (max_val.wrapping_neg().wrapping_sub(1), true)
      } else {
        (result, false)
      }
    }
    x if x == BitFieldOverflow::Fail as u8 => {
      if underflow || overflow {
        (0, true)
      } else {
        (result, false)
      }
    }
    _ => {
      // C# 抛 GarnetException("Invalid overflow type")；解析器保证不可达
      (0, true)
    }
  }
}

/// 位域读取规格
#[derive(Debug, Clone, Copy)]
struct BitFieldSpec {
  offset: i64,
  encoding: u8,
  signed: bool,
}

/// 位域写回规格
#[derive(Debug, Clone, Copy)]
struct BitFieldWriteSpec {
  bitmap_start: usize,
  offset: i64,
  encoding: i32,
  unaligned_bits: i32,
  new_value: i64,
}

/// 从位图读取位域值
///
/// `curr` 为读取起始字节下标，读完推进到下一未读字节；`cend` 为域内字节
/// 界（min(域末端，位图末端)），`vend` 为位图末端。
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:GetValue
fn get_value(
  buf: &mut [u8; 8],
  value: &[u8],
  curr: &mut usize,
  cend: usize,
  vend: usize,
  spec: BitFieldSpec,
) -> i64 {
  // 8 字节草稿自高向低填充域内字节
  for slot in (0..8).rev() {
    if *curr < cend {
      buf[slot] = value[*curr];
      *curr += 1;
    }
  }
  let mut return_value = i64::from_le_bytes(*buf);

  // 裁掉域前导位
  let left = (spec.offset - ((spec.offset >> 3) << 3)) as u32;
  return_value <<= left;

  // 64 位草稿不够容纳域时再补一个字节（拼到低位）
  if (64 - left) < u32::from(spec.encoding) {
    let lsb = if *curr < vend {
      value[*curr] >> (8 - left)
    } else {
      0
    };
    return_value |= i64::from(lsb);
  }

  // 移位到域位宽
  let right = 64 - u32::from(spec.encoding);
  if spec.signed {
    return_value >> right
  } else {
    ((return_value as u64) >> right) as i64
  }
}

/// 把位域值写回位图
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:SetValue
fn set_value(buf: &[u8; 8], value: &mut [u8], curr: usize, cend: usize, spec: BitFieldWriteSpec) {
  let byte_index_start = (spec.offset >> 3) as usize;
  let encoding = spec.encoding as u32;
  let mut tmp = spec.new_value as u64
    & if encoding == 64 {
      u64::MAX
    } else {
      (1u64 << encoding) - 1
    };
  // 假设域整体落在 offset 起的 64 位内
  let mut pbits = encoding; // 前缀位
  let mut sbits: u32 = 0; // 后缀位（第 9 字节）

  if encoding > spec.unaligned_bits as u32 {
    sbits = encoding - spec.unaligned_bits as u32; // 需写入第 9 字节的位数
    let smask = (1u64 << sbits) - 1; // 提取第 9 字节位的掩码
    let keep = 8 - sbits; // 第 9 字节保留低位数
    let msb = ((tmp & smask) << keep) as u8; // 后缀位左对齐到第 9 字节
    let b9 = value[curr] & ((1u16 << keep) - 1) as u8; // 保留第 9 字节原低位
    value[curr] = msb | b9;

    pbits = encoding - sbits; // 剩余写入 byteIndexStart..byteIndexEnd 的位数
    tmp &= !smask; // 清掉已存入第 9 字节的位
  }

  let shf = spec.unaligned_bits - spec.encoding; // 剩余值最低位位置
  let mut mask: u64 = if encoding == 64 {
    u64::MAX
  } else {
    (1u64 << pbits) - 1
  };
  if shf < 0 {
    // 剩余位偏左：值右移；掩码按第 9 字节位数左移后再右移定位
    tmp >>= -shf;
    mask = !((mask << sbits) >> -shf);
  } else {
    tmp <<= shf;
    mask = !(mask << shf);
  }

  let old_v = u64::from_le_bytes(*buf);
  let tmp = (old_v & mask) | tmp;
  let curr = spec.bitmap_start + byte_index_start;
  if curr < cend {
    let write_len = (cend - curr).min(8);
    let bytes = tmp.to_be_bytes();
    value[curr..curr + write_len].copy_from_slice(&bytes[..write_len]);
  }
}

/// BITFIELD GET 子操作
///
/// 偏移校验失败时 C# 抛 GarnetException，此处以 None 表达（解析期已校验）。
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:GetBitfield
pub fn get_bitfield(
  bitmap: &[u8],
  bitmap_length: i64,
  offset: i64,
  encoding: u8,
  signed: bool,
) -> Option<i64> {
  let (offset, end_offset) = try_validate_bitfield_offset(offset, encoding, false)?;

  let byte_index_start = index(offset)?;
  let byte_index_end = index(end_offset)? + 1;
  let mut buf = [0u8; 8];

  // 域整体落在当前值之外恒 0
  if byte_index_start as i64 >= bitmap_length {
    return Some(0);
  }

  let vend = bitmap_length as usize;
  let mut curr = byte_index_start;
  let cend = (byte_index_end).min(vend);
  Some(get_value(
    &mut buf,
    bitmap,
    &mut curr,
    cend,
    vend,
    BitFieldSpec {
      offset,
      encoding,
      signed,
    },
  ))
}

/// BITFIELD SET 子操作，返回（旧值，是否溢出放弃）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:SetBitfield
pub fn set_bitfield(
  bitmap: &mut [u8],
  bitmap_length: i64,
  args: &BitFieldCmdArgs,
) -> Option<(i64, bool)> {
  let bit_count = args.type_info & 0x7F;
  let signed = (args.type_info & BIT_FIELD_SIGN_SIGNED) > 0;
  let (offset, end_offset) = try_validate_bitfield_offset(args.offset, bit_count, false)?;

  let byte_index_start = index(offset)?;
  let byte_index_end = index(end_offset)? + 1;
  let mut buf = [0u8; 8];

  // 域起点落在当前值之外（C# 抛 GarnetException：会话层须先增长值）
  if byte_index_start as i64 >= bitmap_length {
    return None;
  }

  // 读旧值
  let vend = bitmap_length as usize;
  let mut curr = byte_index_start;
  let cend = byte_index_end.min(vend);
  let old_value = get_value(
    &mut buf,
    bitmap,
    &mut curr,
    cend,
    vend,
    BitFieldSpec {
      offset,
      encoding: bit_count,
      signed,
    },
  );

  // FAIL 策略下旧值已越界即放弃
  if args.overflow_type == BitFieldOverflow::Fail as u8 {
    let (_, overflow) =
      check_bitfield_overflow(old_value, 0, bit_count, args.overflow_type, signed);
    if overflow {
      return Some((0, true));
    }
  }

  // 写入新值
  let left = (offset - ((offset >> 3) << 3)) as i32;
  let unaligned_bits = 64 - left;
  set_value(
    &buf,
    bitmap,
    curr,
    cend,
    BitFieldWriteSpec {
      bitmap_start: 0,
      offset,
      encoding: i32::from(bit_count),
      unaligned_bits,
      new_value: args.value,
    },
  );

  Some((old_value, false))
}

/// BITFIELD INCRBY 子操作，返回（新值，是否溢出放弃）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:IncrementBitfield
pub fn increment_bitfield(
  value: &mut [u8],
  val_len: i64,
  args: &BitFieldCmdArgs,
) -> Option<(i64, bool)> {
  let bit_count = args.type_info & 0x7F;
  let signed = (args.type_info & BIT_FIELD_SIGN_SIGNED) > 0;
  let (offset, end_offset) = try_validate_bitfield_offset(args.offset, bit_count, false)?;

  let byte_index_start = index(offset)?;
  let byte_index_end = index(end_offset)? + 1;
  let mut buf = [0u8; 8];

  // 域起点落在当前值之外（C# 抛 GarnetException：会话层须先增长值）
  if byte_index_start as i64 >= val_len {
    return None;
  }

  // 读旧值
  let vend = val_len as usize;
  let mut curr = byte_index_start;
  let cend = byte_index_end.min(vend);
  let old_value = get_value(
    &mut buf,
    value,
    &mut curr,
    cend,
    vend,
    BitFieldSpec {
      offset,
      encoding: bit_count,
      signed,
    },
  );

  // 增量 + 溢出检查
  let (new_value, overflow) =
    check_bitfield_overflow(old_value, args.value, bit_count, args.overflow_type, signed);

  // 写入（溢出放弃时 C# 同样写入 newValue，仅由上层以 nil 替代应答）
  let left = (offset - ((offset >> 3) << 3)) as i32;
  let unaligned_bits = 64 - left;
  set_value(
    &buf,
    value,
    curr,
    cend,
    BitFieldWriteSpec {
      bitmap_start: 0,
      offset,
      encoding: i32::from(bit_count),
      unaligned_bits,
      new_value,
    },
  );

  Some((new_value, overflow))
}

/// 执行 BITFIELD 子操作（GET/SET/INCRBY）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:BitFieldExecute
pub fn bit_field_execute(args: &BitFieldCmdArgs, value: &mut [u8]) -> Option<(i64, bool)> {
  let bit_count = args.type_info & 0x7F;
  let signed = (args.type_info & BIT_FIELD_SIGN_SIGNED) > 0;
  let val_len = value.len() as i64;

  match args.secondary_command {
    BitFieldSecondaryCommand::Set => set_bitfield(value, val_len, args),
    BitFieldSecondaryCommand::IncrBy => increment_bitfield(value, val_len, args),
    BitFieldSecondaryCommand::Get => Some((
      get_bitfield(value, val_len, args.offset, bit_count, signed)?,
      false,
    )),
  }
}

/// 执行只读 BITFIELD_RO 子操作（仅 GET）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:BitFieldExecute_RO
pub fn bit_field_execute_ro(args: &BitFieldCmdArgs, value: &[u8]) -> Option<i64> {
  let bit_count = args.type_info & 0x7F;
  let signed = (args.type_info & BIT_FIELD_SIGN_SIGNED) > 0;

  match args.secondary_command {
    BitFieldSecondaryCommand::Get => {
      get_bitfield(value, value.len() as i64, args.offset, bit_count, signed)
    }
    _ => None,
  }
}
