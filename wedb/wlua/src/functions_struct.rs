//! `struct.pack/unpack/size` 库的编解码层
//! （对标 libs/server/Lua/LuaRunner.Functions.Struct.cs:LuaRunnerFunctions_Struct）。
//!
//! 格式符与 C# 一致：`b/B`（1 字节）、`h/H`（2 字节）、`l/L`/`T`（8 字节）、
//! `i/I`（默认 4 字节，数字后缀 = 字节尺寸，上限 32）、`f`/`d`（浮点）、
//! `c`（定长字节串，后缀 = 长度）、`s`（打包原串 / 解包 NUL 终结串）、
//! `x`（1 字节填充）、`</>`（端序）、`!`（2 的幂对齐，缺省 8）、` `（透传）。
//!
//! 对 C# 的有据偏差（C# 对非 4 字节 `i` 的写入为"写 4 字节跳 size"的残缺
//! 实现、对 `i0`/负 `c0` 解包会产出垃圾或崩溃，Rust 取宽度补码语义并报错）：
//! - `i/I` 任意合法宽度按二进制补码截断/符号扩展读写；
//! - 解包宽度不在 1..=8 内视为非法格式。

/// 端序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endianness {
  /// 小端。
  Little,
  /// 大端。
  Big,
}

/// 格式头：端序 + 对齐（C# Header）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatOptions {
  /// 端序。
  pub endianness: Endianness,
  /// 对齐边界（1 = 不对齐；`!` 置入 2 的幂）。
  pub align: usize,
}

/// `!` 缺省对齐（C# MAXALIGN = Marshal.SizeOf(cD) - sizeof(double)）。
const DEFAULT_ALIGN: usize = 8;

/// `i/I` 后缀上限（C# MAXINTSIZE）。
const MAX_INT_SIZE: usize = 32;

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:DefaultOptions
///
/// 原生端序（x86 为小端）、紧缩打包（不对齐）。
pub const fn default_options() -> FormatOptions {
  FormatOptions {
    endianness: Endianness::Little,
    align: 1,
  }
}

/// 打包/解包的参数值（C# 栈上 Lua 值的数值/字节串二态）。
#[derive(Debug, Clone, PartialEq)]
pub enum StructValue {
  /// 数值。
  Number(f64),
  /// 字节串。
  Bytes(Vec<u8>),
}

impl StructValue {
  /// 数值视图：字节串按 lua_tonumber 语义强转（非数值串为 0）。
  fn as_number(&self) -> f64 {
    match self {
      Self::Number(n) => *n,
      Self::Bytes(b) => str::from_utf8(b)
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(0.0),
    }
  }
}

/// 解包结果：值序列 + 消费位置。
#[derive(Debug, Clone, PartialEq)]
pub struct StructUnpackOut {
  /// 解码值序列。
  pub values: Vec<StructValue>,
  /// 消费后的字节偏移（C# 循环尾 pos，含对齐填充与变长项）。
  pub consumed: usize,
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryGetNum
///
/// token 后的可选十进制后缀（无后缀取 `default`；溢出为非法格式）。
fn try_get_num(format: &[u8], pos: &mut usize, default: usize) -> Option<usize> {
  let Some(&first) = format.get(*pos) else {
    return Some(default);
  };
  if !first.is_ascii_digit() {
    return Some(default);
  }
  let mut result = 0usize;
  while let Some(&b) = format.get(*pos) {
    if !b.is_ascii_digit() {
      break;
    }
    let digit = usize::from(b - b'0');
    result = result.checked_mul(10)?.checked_add(digit)?;
    *pos += 1;
  }
  Some(result)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryGetOptSize
///
/// 单格式符解析：返回 (token, 字节尺寸)，`pos` 越过 token 与数字后缀。
/// `s` 与控制符尺寸为 0。
fn try_get_opt_size(format: &[u8], pos: &mut usize) -> Option<(u8, usize)> {
  let token = *format.get(*pos)?;
  *pos += 1;
  let size = match token {
    b'b' | b'B' => 1,
    b'h' | b'H' => 2,
    b'l' | b'L' | b'T' => 8,
    b'f' => 4,
    b'd' => 8,
    b'x' => 1,
    b'c' => try_get_num(format, pos, 1)?,
    b'i' | b'I' => {
      let n = try_get_num(format, pos, 4)?;
      if n > MAX_INT_SIZE {
        return None;
      }
      n
    }
    _ => 0,
  };
  Some((token, size))
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryParseControlOptions
///
/// 控制符解析：` ` 透传、`<`/`>` 端序、`!` 对齐（可带 2 的幂参数）。
fn try_parse_control_options(
  token: u8,
  format: &[u8],
  pos: &mut usize,
  header: &mut FormatOptions,
) -> Option<()> {
  match token {
    b' ' => {}
    b'<' => header.endianness = Endianness::Little,
    b'>' => header.endianness = Endianness::Big,
    b'!' => {
      let a = try_get_num(format, pos, DEFAULT_ALIGN)?;
      if !a.is_power_of_two() {
        return None;
      }
      header.align = a;
    }
    _ => return None,
  }
  Some(())
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:GetToAlign
///
/// 距下一对齐边界的填充字节数（`size == 0` 与 `c` 不对齐；
/// C# 位运算形态原样承接，`size` 非 2 的幂时按位与截断）。
fn get_to_align(len: usize, alignment: usize, token: u8, size: usize) -> usize {
  if size == 0 || token == b'c' {
    return 0;
  }
  let size = size.min(alignment);
  (size - (len & (size.wrapping_sub(1)))) & size.wrapping_sub(1)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeInteger
///
/// 按宽度与端序写入二进制补码（宽度截断到 1..=8 字节）。
fn try_encode_integer(out: &mut Vec<u8>, value: i64, width: usize, endianness: Endianness) {
  let bytes = value.to_le_bytes();
  let width = width.clamp(1, 8);
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes[..width]);
  } else {
    // 大端：取低 width 字节后逆序。
    out.extend(bytes[..width].iter().rev());
  }
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeSingle
fn try_encode_single(out: &mut Vec<u8>, value: f32, endianness: Endianness) {
  let bytes = value.to_le_bytes();
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes);
  } else {
    out.extend(bytes.iter().rev());
  }
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryEncodeDouble
fn try_encode_double(out: &mut Vec<u8>, value: f64, endianness: Endianness) {
  let bytes = value.to_le_bytes();
  if endianness == Endianness::Little {
    out.extend_from_slice(&bytes);
  } else {
    out.extend(bytes.iter().rev());
  }
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeInteger
///
/// 宽度 `width`（1..=8）整数读出：小写 token 符号扩展，大写零扩展。
fn try_decode_integer(
  input: &[u8],
  offset: usize,
  width: usize,
  endianness: Endianness,
  signed: bool,
) -> Option<f64> {
  debug_assert!((1..=8).contains(&width));
  let slot = input.get(offset..offset + width)?;
  let mut l = 0u64;
  match endianness {
    Endianness::Little => {
      for &b in slot.iter().rev() {
        l = (l << 8) | u64::from(b);
      }
    }
    Endianness::Big => {
      for &b in slot {
        l = (l << 8) | u64::from(b);
      }
    }
  }
  let bits = width * 8;
  if signed && bits < 64 && l & (1 << (bits - 1)) != 0 {
    l |= u64::MAX << bits;
  }
  Some(if signed { l as i64 as f64 } else { l as f64 })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeSingle
fn try_decode_single(input: &[u8], offset: usize, endianness: Endianness) -> Option<f32> {
  let slot: [u8; 4] = input.get(offset..offset + 4)?.try_into().ok()?;
  Some(match endianness {
    Endianness::Little => f32::from_le_bytes(slot),
    Endianness::Big => f32::from_be_bytes(slot),
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:TryDecodeDouble
fn try_decode_double(input: &[u8], offset: usize, endianness: Endianness) -> Option<f64> {
  let slot: [u8; 8] = input.get(offset..offset + 8)?.try_into().ok()?;
  Some(match endianness {
    Endianness::Little => f64::from_le_bytes(slot),
    Endianness::Big => f64::from_be_bytes(slot),
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructPack
///
/// 格式串 + 值序列 → 二进制串；非法格式 / 值缺失 / 串过短返回 None。
pub fn struct_pack(format: &[u8], values: &[StructValue]) -> Option<Vec<u8>> {
  let mut header = default_options();
  let mut out = Vec::new();
  let mut pos = 0usize;
  let mut next = values.iter();
  while pos < format.len() {
    let (token, size) = try_get_opt_size(format, &mut pos)?;
    let pad = get_to_align(out.len(), header.align, token, size);
    out.resize(out.len() + pad, 0);
    match token {
      // 数值经 i64 截断再收窄（C# (long)numRaw 后按宽度截位；f64→u8 直转
      // 会饱和，负值须回绕为低位字节）。
      b'b' | b'B' => out.push(next.next()?.as_number() as i64 as u8),
      b'h' | b'H' => try_encode_integer(
        &mut out,
        next.next()?.as_number() as i64,
        2,
        header.endianness,
      ),
      b'l' | b'L' | b'T' => {
        try_encode_integer(
          &mut out,
          next.next()?.as_number() as i64,
          8,
          header.endianness,
        );
      }
      b'i' | b'I' => {
        if size == 0 || size > 8 {
          return None;
        }
        try_encode_integer(
          &mut out,
          next.next()?.as_number() as i64,
          size,
          header.endianness,
        );
      }
      b'f' => try_encode_single(&mut out, next.next()?.as_number() as f32, header.endianness),
      b'd' => try_encode_double(&mut out, next.next()?.as_number(), header.endianness),
      b'x' => out.push(0),
      // c/s：字节串参数（size == 0 时取整串；过短即错；s 追加 1 字节填充）。
      b'c' | b's' => {
        let StructValue::Bytes(data) = next.next()? else {
          return None;
        };
        let n = if size == 0 { data.len() } else { size };
        if data.len() < n {
          return None;
        }
        out.extend_from_slice(&data[..n]);
        if token == b's' {
          out.push(0);
        }
      }
      _ => try_parse_control_options(token, format, &mut pos, &mut header)?,
    }
  }
  Some(out)
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructUnpack
///
/// 二进制串按格式解包：返回值序列与消费位置（`data` 过短 / 格式非法返回 None）。
pub fn struct_unpack(format: &[u8], data: &[u8]) -> Option<StructUnpackOut> {
  let mut header = default_options();
  let mut pos = 0usize;
  let mut offset = 0usize;
  let mut values = Vec::new();
  while pos < format.len() {
    let (token, mut size) = try_get_opt_size(format, &mut pos)?;
    offset += get_to_align(offset, header.align, token, size);
    if size > data.len() || offset > data.len() - size {
      // 数据串过短
      return None;
    }
    match token {
      b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
        if size == 0 || size > 8 {
          return None;
        }
        let signed = token.is_ascii_lowercase();
        let value = try_decode_integer(data, offset, size, header.endianness, signed)?;
        values.push(StructValue::Number(value));
      }
      b'x' => {}
      b'f' => values.push(StructValue::Number(f64::from(try_decode_single(
        data,
        offset,
        header.endianness,
      )?))),
      b'd' => values.push(StructValue::Number(try_decode_double(
        data,
        offset,
        header.endianness,
      )?)),
      b'c' => {
        // c0：尺寸取自上一个已解码数值（C# TryDecodeCharacter 形态）。
        if size == 0 {
          match values.pop()? {
            StructValue::Number(n) if n >= 0.0 => size = n as usize,
            _ => return None,
          }
          if size > data.len() - offset {
            return None;
          }
        }
        values.push(StructValue::Bytes(data[offset..offset + size].to_vec()));
      }
      b's' => {
        // NUL 终结串：消费量含 NUL，推入值不含 NUL（C# TryDecodeString）。
        let nul = data[offset..].iter().position(|&b| b == 0)?;
        values.push(StructValue::Bytes(data[offset..offset + nul].to_vec()));
        size = nul + 1;
      }
      _ => try_parse_control_options(token, format, &mut pos, &mut header)?,
    }
    offset += size;
  }
  Some(StructUnpackOut {
    values,
    consumed: offset,
  })
}

/// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructSize
///
/// 格式串的打包尺寸；`s` 与 `c0` 为非法格式。
pub fn struct_size(format: &[u8]) -> Option<usize> {
  let mut header = default_options();
  let mut pos = 0usize;
  let mut total = 0usize;
  while pos < format.len() {
    let (token, size) = try_get_opt_size(format, &mut pos)?;
    total += get_to_align(total, header.align, token, size);
    if token == b's' {
      return None;
    }
    if token == b'c' && size == 0 {
      return None;
    }
    if !token.is_ascii_alphanumeric() {
      try_parse_control_options(token, format, &mut pos, &mut header)?;
    }
    total += size;
  }
  Some(total)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn pack_unpack_roundtrip() {
    // 小端 i32 + i8。
    let packed = struct_pack(
      b"<ib",
      &[
        StructValue::Number(305_419_896.0),
        StructValue::Number(-1.0),
      ],
    )
    .unwrap();
    assert_eq!(packed.len(), 5);
    assert_eq!(packed[..4], 0x1234_5678i32.to_le_bytes());
    assert_eq!(packed[4], 0xff);

    let out = struct_unpack(b"<ib", &packed).unwrap();
    assert_eq!(
      out.values,
      vec![
        StructValue::Number(305_419_896.0),
        StructValue::Number(-1.0)
      ]
    );
    assert_eq!(out.consumed, 5);
  }

  #[test]
  fn sized_integer_suffix_is_byte_width() {
    // C# TryGetNum：`i8` 后缀为字节尺寸（非元素个数）。
    assert_eq!(struct_size(b"<i8"), Some(8));
    assert_eq!(struct_size(b"i"), Some(4));
    assert_eq!(struct_size(b"i33"), None, "超过 MAXINTSIZE=32 报错");

    let packed = struct_pack(b">I2", &[StructValue::Number(4660.0)]).unwrap();
    assert_eq!(packed, vec![0x12, 0x34]);
    let out = struct_unpack(b">I2", &packed).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(4660.0)]);
  }

  #[test]
  fn sign_extension_by_token_case() {
    // 小写符号扩展 / 大写零扩展。
    let out = struct_unpack(b"B", &[0xff]).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(255.0)]);
    let out = struct_unpack(b"h", &[0xff, 0xff]).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(-1.0)]);
    let out = struct_unpack(b"H", &[0xff, 0xff]).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(65535.0)]);
  }

  #[test]
  fn double_preserves_fraction() {
    let packed = struct_pack(b"d", &[StructValue::Number(1.5)]).unwrap();
    let out = struct_unpack(b"d", &packed).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(1.5)]);
  }

  #[test]
  fn string_family() {
    // c 定长：写入原样、不足报错。
    let packed = struct_pack(b"c4", &[StructValue::Bytes(b"abcd".to_vec())]).unwrap();
    assert_eq!(packed, b"abcd");
    assert!(struct_pack(b"c4", &[StructValue::Bytes(b"ab".to_vec())]).is_none());
    // c0 解包：尺寸取自上一个已解码数值（数值本身被消费，C# TryDecodeCharacter
    // 的 Remove 形态）。
    let out = struct_unpack(b"Ic0", &[4, 0, 0, 0, b'a', b'b', b'c', b'd']).unwrap();
    assert_eq!(out.values, vec![StructValue::Bytes(b"abcd".to_vec())]);
    assert_eq!(out.consumed, 8);
    // s 解包：NUL 终结，消费量含 NUL。
    let out = struct_unpack(b"s", b"abc\0rest").unwrap();
    assert_eq!(out.values, vec![StructValue::Bytes(b"abc".to_vec())]);
    assert_eq!(out.consumed, 4);
    // s 打包：整串 + 1 字节填充。
    let packed = struct_pack(b"s", &[StructValue::Bytes(b"ab".to_vec())]).unwrap();
    assert_eq!(packed, b"ab\0");
    // struct.size 不支持 s / c0。
    assert_eq!(struct_size(b"s"), None);
    assert_eq!(struct_size(b"c0"), None);
  }

  #[test]
  fn struct_size_and_align() {
    assert_eq!(struct_size(b"<ibB"), Some(6));
    assert_eq!(struct_size(b"x"), Some(1));
    assert_eq!(struct_size(b"c10"), Some(10));
    // `!` 对齐：b(1B) 后 i 补 3 字节至 4 边界。
    assert_eq!(struct_size(b"!4 bi"), Some(8));
    // 端序控制符。
    assert_eq!(struct_size(b">"), Some(0));
    // 未知字母（C# StructSize 对 size 0 的字母静默跳过）。
    assert_eq!(struct_size(b"z"), Some(0));
  }

  #[test]
  fn unpack_reports_short_data() {
    assert!(struct_unpack(b"i", &[1, 2, 3]).is_none());
    assert!(struct_unpack(b"s", b"no-null").is_none());
    assert!(struct_unpack(b"q", &[0]).is_none(), "未知字母报错");
  }

  #[test]
  fn big_endian_doubles() {
    let mut out = Vec::new();
    try_encode_double(&mut out, 1.5, Endianness::Big);
    assert_eq!(out, 1.5f64.to_be_bytes());
    assert_eq!(try_decode_double(&out, 0, Endianness::Big), Some(1.5));
  }

  #[test]
  fn alignment_padding_positions() {
    // `!4`：i(4B) + c(2B) 后 d 对齐至 8 偏移（6 → 8 填 2 字节）。
    let format = b"!4 icd";
    assert_eq!(struct_size(format), Some(16));
    let packed = struct_pack(
      format,
      &[
        StructValue::Number(1.0),
        StructValue::Bytes(b"ab".to_vec()),
        StructValue::Number(1.5),
      ],
    )
    .unwrap();
    assert_eq!(packed.len(), 16);
    let out = struct_unpack(format, &packed).unwrap();
    assert_eq!(out.consumed, 16);
    assert_eq!(out.values[2], StructValue::Number(1.5));
  }

  #[test]
  fn get_to_align_matches_csharp_bit_form() {
    // C# GetToAlign：(size - (len & (size-1))) & (size-1)。
    assert_eq!(get_to_align(0, 4, b'i', 4), 0);
    assert_eq!(get_to_align(5, 4, b'i', 4), 3);
    assert_eq!(get_to_align(5, 4, b'c', 3), 0, "c 不对齐");
    assert_eq!(get_to_align(5, 1, b'i', 4), 0, "默认不对齐");
  }

  #[test]
  fn array_decimal_reads() {
    // i4 = 4 字节整数（非 4 个 i）。
    let packed = struct_pack(b"i4", &[StructValue::Number(-2.0)]).unwrap();
    assert_eq!(packed, (-2i32).to_le_bytes());
    let out = struct_unpack(b"i4", &packed).unwrap();
    assert_eq!(out.values, vec![StructValue::Number(-2.0)]);
    assert_eq!(out.consumed, 4);
  }
}
