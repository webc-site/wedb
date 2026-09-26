//! 十六进制微工具（跨模块公共件，一处定义；对标 C# 各处散落的 hex 折值内联）
//!
//! 自研依据: 十六进制编解码单点化（C# 分散于 Convert.ToHexString/FromHexString 调用点）

/// 小写十六进制字符映射表
pub const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

/// 大写十六进制字符映射表
pub const HEX_CHARS_UPPER: &[u8; 16] = b"0123456789ABCDEF";
/// 单个十六进制字符折值（大小写均可）
#[inline]
pub const fn hex_val(c: u8) -> Option<u8> {
  match c {
    b'0'..=b'9' => Some(c - b'0'),
    b'a'..=b'f' => Some(c - b'a' + 10),
    b'A'..=b'F' => Some(c - b'A' + 10),
    _ => None,
  }
}

/// 定长 20 字节（如 SHA1）小写十六进制编码：输出 40 字节定长数组（零堆分配）
pub const fn hex_encode_20(src: &[u8; 20]) -> [u8; 40] {
  let mut out = [0u8; 40];
  let mut i = 0;
  while i < 20 {
    let b = src[i];
    out[i * 2] = HEX_CHARS_LOWER[(b >> 4) as usize];
    out[i * 2 + 1] = HEX_CHARS_LOWER[(b & 0x0f) as usize];
    i += 1;
  }
  out
}

/// 定长 32 字节（如 SHA256）小写十六进制编码：输出 64 字节定长数组（零堆分配）
pub const fn hex_encode_32(src: &[u8; 32]) -> [u8; 64] {
  let mut out = [0u8; 64];
  let mut i = 0;
  while i < 32 {
    let b = src[i];
    out[i * 2] = HEX_CHARS_LOWER[(b >> 4) as usize];
    out[i * 2 + 1] = HEX_CHARS_LOWER[(b & 0x0f) as usize];
    i += 1;
  }
  out
}

/// 切片小写十六进制编码到目标切片（零堆分配）
pub fn hex_encode_into(src: &[u8], dst: &mut [u8]) {
  assert!(dst.len() >= src.len() * 2, "destination buffer too small");
  for (chunk, &b) in dst.as_chunks_mut::<2>().0.iter_mut().zip(src.iter()) {
    chunk[0] = HEX_CHARS_LOWER[(b >> 4) as usize];
    chunk[1] = HEX_CHARS_LOWER[(b & 0x0f) as usize];
  }
}

/// 任意长度小写十六进制编码为 String
pub fn hex_encode(src: &[u8]) -> String {
  let mut out = vec![0u8; src.len() * 2];
  hex_encode_into(src, &mut out);
  // SAFETY: HEX_CHARS_LOWER 仅包含 ASCII 十六进制字符，为合法 UTF-8
  unsafe { String::from_utf8_unchecked(out) }
}

/// u128 小写十六进制编码：输出 32 字节定长数组（大端，零堆分配）
pub const fn hex_encode_u128(src: u128) -> [u8; 32] {
  let mut out = [0u8; 32];
  let mut i = 0;
  while i < 16 {
    // 大端序逐字节取出：第 i 字节即 (120 - 8i) 位起 8 位
    let b = (src >> (120 - 8 * i)) as u8;
    out[i * 2] = HEX_CHARS_LOWER[(b >> 4) as usize];
    out[i * 2 + 1] = HEX_CHARS_LOWER[(b & 0x0f) as usize];
    i += 1;
  }
  out
}

/// u128 小写十六进制编码为 String（32 字符）
#[inline]
pub fn hex_str_u128(src: u128) -> String {
  let mut out = vec![0u8; 32];
  out.copy_from_slice(&hex_encode_u128(src));
  // SAFETY: HEX_CHARS_LOWER 仅包含 ASCII 十六进制字符，为合法 UTF-8
  unsafe { String::from_utf8_unchecked(out) }
}

/// 恰好 32 字符的十六进制串折为 u128（大小写均可，零堆分配）；
/// 长度不符或含非法字符返回 None
pub const fn hex_u128(src: &[u8]) -> Option<u128> {
  if src.len() != 32 {
    return None;
  }
  let mut bytes = [0u8; 16];
  let mut i = 0;
  while i < 32 {
    let v = match hex_val(src[i]) {
      Some(v) => v,
      None => return None,
    };
    bytes[i / 2] = if i & 1 == 0 { v << 4 } else { bytes[i / 2] | v };
    i += 1;
  }
  Some(u128::from_be_bytes(bytes))
}

/// 定长解码：恰好 2N 长度的 hex 串折为 N 字节（零堆分配）；
/// 长度不符或含非法字符返回 None
pub const fn hex_decode<const N: usize>(src: &[u8]) -> Option<[u8; N]> {
  if src.len() != N * 2 {
    return None;
  }
  let mut out = [0u8; N];
  let mut i = 0;
  while i < src.len() {
    let hi = match hex_val(src[i]) {
      Some(v) => v,
      None => return None,
    };
    let lo = match hex_val(src[i + 1]) {
      Some(v) => v,
      None => return None,
    };
    out[i / 2] = (hi << 4) | lo;
    i += 2;
  }
  Some(out)
}

/// 生成 40 字符十六进制随机身份串（对标 C# Generator.CreateHexId(40)；
/// 无状态独立随机填充，每次独立生成，不共享全局计数器状态）
pub fn generate_hex_id() -> String {
  let mut buf = [0u8; 20];
  fastrand::fill(&mut buf);
  let enc = hex_encode_20(&buf);
  // SAFETY: hex_encode_20 仅产出 ASCII '0'-'9','a'-'f'，为合法 UTF-8
  unsafe { String::from_utf8_unchecked(enc.to_vec()) }
}

#[cfg(test)]
mod tests {
  use gxhash::{HashSet, HashSetExt};

  use super::{
    HEX_CHARS_LOWER, HEX_CHARS_UPPER, generate_hex_id, hex_decode, hex_encode, hex_encode_20,
    hex_encode_32, hex_encode_into, hex_encode_u128, hex_str_u128, hex_u128, hex_val,
  };

  #[test]
  fn hex_chars_len() {
    assert_eq!(HEX_CHARS_LOWER.len(), 16);
    assert_eq!(HEX_CHARS_UPPER.len(), 16);
  }

  #[test]
  fn hex_val_cases() {
    assert_eq!(hex_val(b'0'), Some(0));
    assert_eq!(hex_val(b'9'), Some(9));
    assert_eq!(hex_val(b'a'), Some(10));
    assert_eq!(hex_val(b'f'), Some(15));
    assert_eq!(hex_val(b'A'), Some(10));
    assert_eq!(hex_val(b'F'), Some(15));
    assert_eq!(hex_val(b'g'), None);
    assert_eq!(hex_val(b'/'), None);
  }

  #[test]
  fn hex_decode_roundtrip() {
    let bytes = hex_decode::<4>(b"0aFf0102").unwrap();
    assert_eq!(bytes, [0x0a, 0xff, 0x01, 0x02]);

    // 长度不符
    assert!(hex_decode::<4>(b"0aFf01").is_none());
    // 非法字符
    assert!(hex_decode::<2>(b"0g").is_none());
    // 空输入定长 0
    assert_eq!(hex_decode::<0>(b"").unwrap(), [0u8; 0]);
  }

  #[test]
  fn hex_encode_helpers() {
    let raw20 = [0xabu8; 20];
    let enc20 = hex_encode_20(&raw20);
    assert_eq!(&enc20[..4], b"abab");
    assert_eq!(hex_decode::<20>(&enc20).unwrap(), raw20);

    let raw32 = [0x0fu8; 32];
    let enc32 = hex_encode_32(&raw32);
    assert_eq!(&enc32[..4], b"0f0f");
    assert_eq!(hex_decode::<32>(&enc32).unwrap(), raw32);

    let mut buf = [0u8; 8];
    hex_encode_into(&[1, 2, 3, 4], &mut buf);
    assert_eq!(&buf, b"01020304");

    let s = hex_encode(&[0x12, 0x34]);
    assert_eq!(s, "1234");
  }

  #[test]
  fn hex_u128_roundtrip() {
    let v = 0x0123_4567_89ab_cdef_0fed_cba9_8765_4321u128;
    let enc = hex_encode_u128(v);
    assert_eq!(&enc[..2], b"01");
    assert_eq!(&enc[30..], b"21");
    assert_eq!(hex_u128(&enc), Some(v));
    assert_eq!(hex_str_u128(v), "0123456789abcdef0fedcba987654321");

    // 大写可解码
    assert_eq!(hex_u128(b"0123456789ABCDEF0FEDCBA987654321"), Some(v));
    // 零
    assert_eq!(hex_str_u128(0), "00000000000000000000000000000000");
    // 长度不符与非法字符
    assert!(hex_u128(b"01").is_none());
    assert!(hex_u128(b"0g23456789abcdef0fedcba987654321").is_none());
  }

  #[test]
  fn test_generate_hex_id() {
    let id1 = generate_hex_id();
    let id2 = generate_hex_id();
    assert_eq!(id1.len(), 40);
    assert_eq!(id2.len(), 40);
    assert!(
      id1
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert!(
      id2
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_ne!(id1, id2, "独立随机取样不得相同");
  }

  #[test]
  fn test_generate_hex_id_independent_no_shared_counter() {
    let mut ids = HashSet::new();
    for _ in 0..100 {
      assert!(ids.insert(generate_hex_id()));
    }
  }
}
