//! `struct.pack/unpack/size` 库的编码层
//! （对标 libs/server/Lua/LuaRunner.Functions.Struct.cs:LuaRunner_Functions_Struct）。
//!
//! 支持格式符：`b/B/h/H/i/I/l/L/j/J`（整型族）、`f/d/n`（浮点族）、
//! `s/z/c n`（字符串族）、`x`（填充）、`</>/=`（端序）、`!`（对齐）。

use std::array;

/// 端序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endianness {
  /// 小端。
  Little,
  /// 大端。
  Big,
}

/// 解析后的格式选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatOptions {
  /// 端序。
  pub endianness: Endianness,
  /// 强制对齐（`!`）；`struct` 库默认非对齐打包。
  pub align: bool,
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:DefaultOptions
pub fn default_options() -> FormatOptions {
  FormatOptions {
    endianness: Endianness::Little,
    align: false,
  }
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryGetFormat
///
/// 前导端序/对齐控制符解析。
pub fn try_get_format(bytes: &[u8]) -> Option<(FormatOptions, usize)> {
  let mut options = default_options();
  let mut consumed = 0;
  while consumed < bytes.len() {
    match bytes[consumed] {
      b'<' => options.endianness = Endianness::Little,
      b'>' => options.endianness = Endianness::Big,
      b'=' => options.endianness = Endianness::Little,
      b'!' => options.align = true,
      _ => break,
    }
    consumed += 1;
  }
  Some((options, consumed))
}

/// 单个格式符的尺寸（None = 非定长项）。
fn item_size(token: u8, count: usize) -> Option<usize> {
  Some(match token {
    b'b' | b'B' => count.max(1),
    b'h' | b'H' => 2 * count.max(1),
    b'i' | b'I' | b'l' | b'L' | b'f' => 4 * count.max(1),
    b'j' | b'J' | b'd' | b'n' => 8 * count.max(1),
    b's' | b'z' => count.max(1),
    b'x' => count.max(1),
    b'c' => count,
    _ => return None,
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructSize
///
/// 计算格式串的打包尺寸。
pub fn struct_size(format: &[u8]) -> Option<usize> {
  let (options, mut pos): (FormatOptions, usize) = try_get_format(format)?;
  let mut total: usize = 0;
  while pos < format.len() {
    let token = format[pos];
    pos += 1;
    let (count, next) = read_count(format, pos);
    pos = next;
    if options.align
      && token.is_ascii_lowercase()
      && !matches!(token, b's' | b'z' | b'c' | b'x')
      && let Some(size) = item_size(token, 1)
    {
      total = total.div_ceil(size) * size;
    }
    let size = item_size(token, count)?;
    total += size;
  }
  Some(total)
}

/// 读取数字前缀计数。
fn read_count(format: &[u8], mut pos: usize) -> (usize, usize) {
  let mut count = 0;
  let mut any = false;
  while pos < format.len() && format[pos].is_ascii_digit() {
    count = count * 10 + usize::from(format[pos] - b'0');
    pos += 1;
    any = true;
  }
  if any { (count, pos) } else { (0, pos) }
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeInteger
///
/// 按宽度与端序编码整型（补符号位由调用方的值域保证）。
pub fn try_encode_integer(
  out: &mut Vec<u8>,
  value: i64,
  width: usize,
  endianness: Endianness,
) -> bool {
  let bytes = value.to_le_bytes();
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes[..width]);
  } else {
    // 大端：取低 width 字节后逆序。
    out.extend(bytes[..width].iter().rev());
  }
  true
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeSingle
pub fn try_encode_single(out: &mut Vec<u8>, value: f32, endianness: Endianness) -> bool {
  let bytes = value.to_le_bytes();
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes);
  } else {
    out.extend(bytes.iter().rev());
  }
  true
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeDouble
pub fn try_encode_double(out: &mut Vec<u8>, value: f64, endianness: Endianness) -> bool {
  let bytes = value.to_le_bytes();
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes);
  } else {
    out.extend(bytes.iter().rev());
  }
  true
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeInteger
pub fn try_decode_integer(
  input: &[u8],
  offset: usize,
  width: usize,
  endianness: Endianness,
) -> Option<i64> {
  let slot = input.get(offset..offset + width)?;
  let mut bytes = [0u8; 8];
  if endianness == Endianness::Little {
    bytes[..width].copy_from_slice(slot);
  } else {
    bytes[8 - width..].copy_from_slice(slot);
  }
  Some(match width {
    1 => bytes[0] as i8 as i64,
    2 => i16::from_le_bytes(array::from_fn(|i| bytes[i])) as i64,
    4 => i32::from_le_bytes(array::from_fn(|i| bytes[i])) as i64,
    _ => i64::from_le_bytes(bytes),
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeSingle
pub fn try_decode_single(input: &[u8], offset: usize, endianness: Endianness) -> Option<f32> {
  let slot = input.get(offset..offset + 4)?;
  let bytes: [u8; 4] = slot.try_into().ok()?;
  Some(if endianness == Endianness::Little {
    f32::from_le_bytes(bytes)
  } else {
    f32::from_be_bytes(bytes)
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeDouble
pub fn try_decode_double(input: &[u8], offset: usize, endianness: Endianness) -> Option<f64> {
  let slot = input.get(offset..offset + 8)?;
  let bytes: [u8; 8] = slot.try_into().ok()?;
  Some(if endianness == Endianness::Little {
    f64::from_le_bytes(bytes)
  } else {
    f64::from_be_bytes(bytes)
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeCharacter
pub fn try_decode_character(input: &[u8], offset: usize, count: usize) -> Option<Vec<u8>> {
  input.get(offset..offset + count).map(<[u8]>::to_vec)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeString
///
/// `z`（NUL 终止）解码。
pub fn try_decode_string(input: &[u8], offset: usize) -> Option<Vec<u8>> {
  let tail = input.get(offset..)?;
  let len = tail.iter().position(|&b| b == 0)?;
  Some(tail[..len].to_vec())
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructPack
///
/// 极简打包：仅处理当前 Redis 用到路径（整型族/浮点/`x` 填充），输入
/// 与格式逐项对应。
pub fn struct_pack(format: &[u8], values: &[i64]) -> Option<Vec<u8>> {
  let (options, mut pos) = try_get_format(format)?;
  let mut out = Vec::new();
  let mut next_value = values.iter();
  while pos < format.len() {
    let token = format[pos];
    pos += 1;
    let (_, next) = read_count(format, pos);
    pos = next;
    let encoded: Option<()> = match token {
      b'x' => {
        out.push(0);
        Some(())
      }
      b'b' | b'B' => {
        let value = *next_value.next()?;
        out.push(value.clamp(i64::from(i8::MIN), i64::from(u8::MAX)) as u8);
        Some(())
      }
      b'h' | b'H' => {
        try_encode_integer(&mut out, *next_value.next()?, 2, options.endianness).then_some(())
      }
      b'i' | b'I' | b'l' | b'L' => {
        try_encode_integer(&mut out, *next_value.next()?, 4, options.endianness).then_some(())
      }
      b'j' | b'J' => {
        try_encode_integer(&mut out, *next_value.next()?, 8, options.endianness).then_some(())
      }
      b'f' => {
        try_encode_single(&mut out, *next_value.next()? as f32, options.endianness).then_some(())
      }
      b'd' | b'n' => {
        try_encode_double(&mut out, *next_value.next()? as f64, options.endianness).then_some(())
      }
      _ => None,
    };
    encoded?;
  }
  Some(out)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructUnpack
///
/// 极简解包：与 `struct_pack` 对称。
pub fn struct_unpack(format: &[u8], data: &[u8]) -> Option<Vec<i64>> {
  let (options, mut pos) = try_get_format(format)?;
  let mut offset = 0;
  let mut out = Vec::new();
  while pos < format.len() {
    let token = format[pos];
    pos += 1;
    let (_, next) = read_count(format, pos);
    pos = next;
    match token {
      b'x' => offset += 1,
      b'b' => out.push(i64::from(*data.get(offset)? as i8)),
      b'B' => out.push(i64::from(*data.get(offset)?)),
      b'h' => out.push(i64::from(
        try_decode_integer(data, offset, 2, options.endianness)? as i16,
      )),
      b'H' => out.push(try_decode_integer(data, offset, 2, options.endianness)? & 0xffff),
      b'i' | b'l' => out.push(try_decode_integer(data, offset, 4, options.endianness)?),
      b'I' | b'L' => {
        out.push(try_decode_integer(data, offset, 4, options.endianness)? & 0xffff_ffff)
      }
      b'j' | b'J' => out.push(try_decode_integer(data, offset, 8, options.endianness)?),
      b'f' => out.push(try_decode_single(data, offset, options.endianness)? as i64),
      b'd' | b'n' => out.push(try_decode_double(data, offset, options.endianness)? as i64),
      _ => return None,
    }
    offset += item_size(token, 1).unwrap_or(0);
  }
  Some(out)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:AddAlignmentPadding
///
/// 追加填充至对齐边界。
pub fn add_alignment_padding(out: &mut Vec<u8>, alignment: usize) {
  let pad = (alignment - out.len() % alignment) % alignment;
  out.resize(out.len() + pad, 0);
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:GetToAlign
///
/// 距下一对齐边界的字节数。
pub fn get_to_align(len: usize, alignment: usize) -> usize {
  (alignment - len % alignment) % alignment
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryGetOptSize
///
/// 数字前缀解析（可选尺寸）。
pub fn try_get_opt_size(format: &[u8], pos: usize) -> (Option<usize>, usize) {
  let (count, next) = read_count(format, pos);
  (count.checked_mul(1).filter(|_| count > 0), next)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryGetNum
///
/// 十进制数字序列解析。
pub fn try_get_num(format: &[u8], pos: usize) -> Option<(usize, usize)> {
  let (count, next) = read_count(format, pos);
  (count > 0).then_some((count, next))
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryParseControlOptions
pub fn try_parse_control_options(format: &[u8]) -> Option<(FormatOptions, usize)> {
  try_get_format(format)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn pack_unpack_roundtrip() {
    // 小端 i32 + i8。
    let packed = struct_pack(b"<ib", &[0x1234_5678, -1]).unwrap();
    assert_eq!(packed.len(), 5);
    assert_eq!(packed[..4], 0x1234_5678i32.to_le_bytes());
    assert_eq!(packed[4], 0xff);

    let unpacked = struct_unpack(b"<ib", &packed).unwrap();
    assert_eq!(unpacked, vec![0x1234_5678, -1]);
  }

  #[test]
  fn big_endian_doubles() {
    let mut out = Vec::new();
    assert!(try_encode_double(&mut out, 1.5f64, Endianness::Big));
    assert_eq!(out, 1.5f64.to_be_bytes());
    assert_eq!(try_decode_double(&out, 0, Endianness::Big), Some(1.5));
  }

  #[test]
  fn struct_size_and_align() {
    assert_eq!(struct_size(b"<ibB"), Some(6));
    assert_eq!(struct_size(b"x"), Some(1));
    assert_eq!(struct_size(b"c10"), Some(10));
    assert_eq!(get_to_align(5, 4), 3);
    let mut padded = vec![1u8; 5];
    add_alignment_padding(&mut padded, 4);
    assert_eq!(padded.len(), 8);
  }

  #[test]
  fn string_family() {
    let data = b"abc\0rest";
    assert_eq!(try_decode_string(data, 0).unwrap(), b"abc");
    assert_eq!(try_decode_character(data, 0, 3).unwrap(), b"abc");
  }
}
