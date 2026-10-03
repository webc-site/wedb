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
    let mut buf = bytes.to_vec();
    for b in &mut buf {
      if !b.is_ascii() {
        *b = b'?';
      }
    }
    // SAFETY: 输出字节全部 <= 127（原 ASCII 字节或 b'?' 0x3F），恒为合法的 UTF-8 字节序列
    Cow::Owned(unsafe { String::from_utf8_unchecked(buf) })
  }
}

/// 比较两字节切片在 ASCII 意义下是否忽略大小写相等（运行时标准库 SIMD 加速）
#[inline]
pub fn eq_ascii_case(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

/// 比较两字节切片在 ASCII 意义下是否忽略大小写相等（编译期 const 上下文可用）
#[inline]
pub const fn eq_ascii_case_const(a: &[u8], b: &[u8]) -> bool {
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
