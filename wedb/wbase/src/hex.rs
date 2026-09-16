//! 十六进制微工具（跨模块公共件，一处定义；对标 C# 各处散落的 hex 折值内联）

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

#[cfg(test)]
mod tests {
  use super::{hex_decode, hex_val};

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
    assert_eq!(hex_decode::<0>(b"").unwrap(), []);
  }
}
