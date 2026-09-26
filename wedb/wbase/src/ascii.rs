//! ASCII 规范化与折叠原语（对标 C# ASCIIEncoding.GetString）

use std::{borrow::Cow, str::from_utf8_unchecked};

/// 字节串按 ASCII 规范化（>0x7F 逐字节折 '?'，对标 C# Encoding.ASCII.GetString）
///
/// 纯 ASCII（含空）输入零拷贝借用；含高位字节时逐字节折 '?'——
/// 多字节 UTF-8 序列同样逐字节各折一个 '?'，与 C# ASCIIEncoding.GetString 的
/// 逐字节替换语义全等（并非按字符折一个 '?'）。
#[inline]
pub fn ascii_sanitize(bytes: &[u8]) -> Cow<'_, str> {
  if bytes.is_ascii() {
    // SAFETY: bytes.is_ascii() 保证所有字节 <= 127，为合法的 UTF-8 子集
    Cow::Borrowed(unsafe { from_utf8_unchecked(bytes) })
  } else {
    let buf: Vec<u8> = bytes
      .iter()
      .map(|&b| if b.is_ascii() { b } else { b'?' })
      .collect();
    // SAFETY: 输出字节全部 <= 127（原 ASCII 字节或 b'?' 0x3F），恒为合法的 UTF-8 字节序列
    Cow::Owned(unsafe { String::from_utf8_unchecked(buf) })
  }
}

/// 比较两字节切片在 ASCII 意义下是否忽略大小写相等（零分配，支持 const 上下文）
#[inline]
pub const fn eq_ascii_case(a: &[u8], b: &[u8]) -> bool {
  if a.len() != b.len() {
    return false;
  }
  let mut i = 0;
  while i < a.len() {
    let x = a[i];
    let y = b[i];
    if x != y && !x.eq_ignore_ascii_case(&y) {
      return false;
    }
    i += 1;
  }
  true
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ascii_sanitize() {
    // 纯 ASCII 零拷贝借用
    assert_eq!(ascii_sanitize(b""), "");
    assert_eq!(ascii_sanitize(b"abc"), "abc");
    assert_eq!(ascii_sanitize(b"HELLO 123"), "HELLO 123");

    // 单字节 0xFF 折为 '?'
    assert_eq!(ascii_sanitize(&[0xFF]), "?");

    // 混合非 ASCII
    assert_eq!(ascii_sanitize(b"foo\xffbar"), "foo?bar");

    // 6 字节中文逐字节折为 6 个 '?'
    assert_eq!(ascii_sanitize("错误".as_bytes()), "??????");
    assert_eq!(ascii_sanitize("未知".as_bytes()), "??????");
  }

  #[test]
  fn test_eq_ascii_case() {
    assert!(eq_ascii_case(b"", b""));
    assert!(eq_ascii_case(b"abc", b"ABC"));
    assert!(eq_ascii_case(b"Hello-World_123", b"hELLO-wORLD_123"));
    assert!(!eq_ascii_case(b"abc", b"abcd"));
    assert!(!eq_ascii_case(b"abcd", b"abc"));
    assert!(!eq_ascii_case(b"abc", b"abd"));
    assert!(!eq_ascii_case(b"\xff", b"\xfe"));
    assert!(eq_ascii_case(b"\xff", b"\xff"));

    // 编译期 const 验证
    const {
      assert!(eq_ascii_case(b"GET", b"get"));
      assert!(!eq_ascii_case(b"GET", b"set"));
    }
  }
}
