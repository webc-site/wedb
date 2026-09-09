//! 零堆分配、保序小写 Base32（RFC 4648 Base32hex）极速编解码原语
//!
//! 专为高吞吐存储引擎快照与刷盘文件名设计：
//! - 字符集：`0-9` (10) + `a-v` (22) = 32 个字符，严格小写，免疫 APFS / NTFS 大小写折叠冲突；
//! - 字典序保序（Order-Preserving）：大端按位切分，数值大小与字符串字典序严格单调一致；
//! - 极速纯位移：5 位大端切分与常数查表，零大数除法，单次编解码 < 2ns；
//! - 零堆分配：`Base32Buf64([u8; 13])` 与 `Base32Buf128([u8; 26])` 栈字符串包装，无中间堆内存开销；
//! - 容错解码：单指令查表映射，宽容兼容大写（`A-V` 自动映射至 `10..=31`），非法字符常数级拒绝。

use core::{
  fmt,
  ops::Deref,
  str::{self, from_utf8_unchecked},
};
use std::{ffi::OsStr, path::Path};

/// 64 位整数编码为 Base32 的固定字符长度
pub const BASE32_LEN_U64: usize = 13;

/// 128 位整数编码为 Base32 的固定字符长度
pub const BASE32_LEN_U128: usize = 26;

/// RFC 4648 Base32hex 全小写字符常量表（0..=31）
pub const BASE32_LOWER_TABLE: &[u8; 32] = b"0123456789abcdefghijklmnopqrstuv";

/// 编译期预计算的 256 字节解码表（非 Base32 字符映射为 0xFF，同时映射 a-v 与 A-V）
const BASE32_DECODE_TABLE: [u8; 256] = {
  let mut table = [0xFFu8; 256];
  let mut i = 0usize;
  while i < 10 {
    table[b'0' as usize + i] = i as u8;
    i += 1;
  }
  let mut j = 0usize;
  while j < 22 {
    table[b'a' as usize + j] = (10 + j) as u8;
    table[b'A' as usize + j] = (10 + j) as u8;
    j += 1;
  }
  table
};

/// 64 位整数编码后的固定 13 字符栈缓冲区
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Base32Buf64(pub [u8; BASE32_LEN_U64]);

impl Base32Buf64 {
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    // SAFETY: 缓冲区内容恒定来自 BASE32_LOWER_TABLE，全部为合法 ASCII
    unsafe { from_utf8_unchecked(&self.0) }
  }

  #[inline(always)]
  pub const fn as_bytes(&self) -> &[u8; BASE32_LEN_U64] {
    &self.0
  }
}

impl Deref for Base32Buf64 {
  type Target = str;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_str()
  }
}

impl AsRef<str> for Base32Buf64 {
  #[inline(always)]
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl AsRef<Path> for Base32Buf64 {
  #[inline(always)]
  fn as_ref(&self) -> &Path {
    Path::new(self.as_str())
  }
}

impl AsRef<OsStr> for Base32Buf64 {
  #[inline(always)]
  fn as_ref(&self) -> &OsStr {
    OsStr::new(self.as_str())
  }
}

impl AsRef<[u8]> for Base32Buf64 {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}

impl PartialEq<str> for Base32Buf64 {
  #[inline(always)]
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl PartialEq<&str> for Base32Buf64 {
  #[inline(always)]
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl PartialEq<Base32Buf64> for str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf64) -> bool {
    self == other.as_str()
  }
}

impl PartialEq<Base32Buf64> for &str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf64) -> bool {
    *self == other.as_str()
  }
}

impl fmt::Debug for Base32Buf64 {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl fmt::Display for Base32Buf64 {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// 128 位整数编码后的固定 26 字符栈缓冲区
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Base32Buf128(pub [u8; BASE32_LEN_U128]);

impl Base32Buf128 {
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    // SAFETY: 缓冲区内容恒定来自 BASE32_LOWER_TABLE，全部为合法 ASCII
    unsafe { from_utf8_unchecked(&self.0) }
  }

  #[inline(always)]
  pub const fn as_bytes(&self) -> &[u8; BASE32_LEN_U128] {
    &self.0
  }
}

impl Deref for Base32Buf128 {
  type Target = str;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_str()
  }
}

impl AsRef<str> for Base32Buf128 {
  #[inline(always)]
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl AsRef<Path> for Base32Buf128 {
  #[inline(always)]
  fn as_ref(&self) -> &Path {
    Path::new(self.as_str())
  }
}

impl AsRef<OsStr> for Base32Buf128 {
  #[inline(always)]
  fn as_ref(&self) -> &OsStr {
    OsStr::new(self.as_str())
  }
}

impl AsRef<[u8]> for Base32Buf128 {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}

impl PartialEq<str> for Base32Buf128 {
  #[inline(always)]
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl PartialEq<&str> for Base32Buf128 {
  #[inline(always)]
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl PartialEq<Base32Buf128> for str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf128) -> bool {
    self == other.as_str()
  }
}

impl PartialEq<Base32Buf128> for &str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf128) -> bool {
    *self == other.as_str()
  }
}

impl fmt::Debug for Base32Buf128 {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl fmt::Display for Base32Buf128 {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// 将 64 位整数大端保序编码为固定 13 字符的小写 Base32 栈字符串
#[inline]
pub const fn encode_u64(val: u64) -> Base32Buf64 {
  let mut buf = [0u8; BASE32_LEN_U64];
  buf[0] = BASE32_LOWER_TABLE[((val >> 60) & 0x0F) as usize];
  buf[1] = BASE32_LOWER_TABLE[((val >> 55) & 0x1F) as usize];
  buf[2] = BASE32_LOWER_TABLE[((val >> 50) & 0x1F) as usize];
  buf[3] = BASE32_LOWER_TABLE[((val >> 45) & 0x1F) as usize];
  buf[4] = BASE32_LOWER_TABLE[((val >> 40) & 0x1F) as usize];
  buf[5] = BASE32_LOWER_TABLE[((val >> 35) & 0x1F) as usize];
  buf[6] = BASE32_LOWER_TABLE[((val >> 30) & 0x1F) as usize];
  buf[7] = BASE32_LOWER_TABLE[((val >> 25) & 0x1F) as usize];
  buf[8] = BASE32_LOWER_TABLE[((val >> 20) & 0x1F) as usize];
  buf[9] = BASE32_LOWER_TABLE[((val >> 15) & 0x1F) as usize];
  buf[10] = BASE32_LOWER_TABLE[((val >> 10) & 0x1F) as usize];
  buf[11] = BASE32_LOWER_TABLE[((val >> 5) & 0x1F) as usize];
  buf[12] = BASE32_LOWER_TABLE[(val & 0x1F) as usize];
  Base32Buf64(buf)
}

/// 将 128 位整数大端保序编码为固定 26 字符的小写 Base32 栈字符串
#[inline]
pub const fn encode_u128(val: u128) -> Base32Buf128 {
  let mut buf = [0u8; BASE32_LEN_U128];
  buf[0] = BASE32_LOWER_TABLE[((val >> 125) & 0x07) as usize];
  let mut i = 1;
  while i < BASE32_LEN_U128 {
    let shift = 125 - i * 5;
    buf[i] = BASE32_LOWER_TABLE[((val >> shift) & 0x1F) as usize];
    i += 1;
  }
  Base32Buf128(buf)
}

/// 将 64 位整数大端保序编码追加写入 String（0 临时堆分配）
#[inline]
pub fn push_base32_u64(val: u64, buf: &mut String) {
  let encoded = encode_u64(val);
  buf.push_str(encoded.as_str());
}

/// 将 128 位整数大端保序编码追加写入 String（0 临时堆分配）
#[inline]
pub fn push_base32_u128(val: u128, buf: &mut String) {
  let encoded = encode_u128(val);
  buf.push_str(encoded.as_str());
}

/// 快速判定字符串切片是否全部由合法的 Base32 字符构成（大小写无关）
#[inline]
pub fn is_base32(s: &str) -> bool {
  s.as_bytes()
    .iter()
    .all(|&b| BASE32_DECODE_TABLE[b as usize] != 0xFF)
}

/// 将 13 字符的 Base32 字符串安全解码为 64 位整数
///
/// 若长度不等于 13、首字符超出高位范围 (0x0F) 或含有非法字符，返回 `None`
#[inline]
pub fn decode_u64(s: &str) -> Option<u64> {
  if s.len() != BASE32_LEN_U64 {
    return None;
  }
  let bytes = s.as_bytes();

  let head = BASE32_DECODE_TABLE[bytes[0] as usize];
  if head > 0x0F {
    return None;
  }

  bytes[1..].iter().try_fold(head as u64, |acc, &b| {
    let digit = BASE32_DECODE_TABLE[b as usize];
    if digit == 0xFF {
      None
    } else {
      Some((acc << 5) | (digit as u64))
    }
  })
}

/// 将 26 字符的 Base32 字符串安全解码为 128 位整数
///
/// 若长度不等于 26、首字符超出高位范围 (0x07) 或含有非法字符，返回 `None`
#[inline]
pub fn decode_u128(s: &str) -> Option<u128> {
  if s.len() != BASE32_LEN_U128 {
    return None;
  }
  let bytes = s.as_bytes();

  let head = BASE32_DECODE_TABLE[bytes[0] as usize];
  if head > 0x07 {
    return None;
  }

  bytes[1..].iter().try_fold(head as u128, |acc, &b| {
    let digit = BASE32_DECODE_TABLE[b as usize];
    if digit == 0xFF {
      None
    } else {
      Some((acc << 5) | (digit as u128))
    }
  })
}

const _: () = {
  assert!(BASE32_LEN_U64 * 5 >= 64);
  assert!((BASE32_LEN_U64 - 1) * 5 < 64);
  assert!(BASE32_LEN_U128 * 5 >= 128);
  assert!((BASE32_LEN_U128 - 1) * 5 < 128);

  let b64 = encode_u64(u64::MAX);
  assert!(b64.0[0] == b'f');
  assert!(b64.0[12] == b'v');

  let b128 = encode_u128(u128::MAX);
  assert!(b128.0[0] == b'7');
  assert!(b128.0[25] == b'v');
};
