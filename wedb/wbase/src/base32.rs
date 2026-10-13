//! 零堆分配、保序小写 Base32（RFC 4648 Base32hex）极速编解码原语
//!
//! 专为高吞吐存储引擎快照与刷盘文件名设计：
//! - 字符集：`0-9` (10) + `a-v` (22) = 32 个字符，严格小写，免疫 APFS / NTFS 大小写折叠冲突；
//! - 字典序保序（Order-Preserving）：大端按位切分，数值大小与字符串字典序严格单调一致；
//! - 极速纯位移：5 位大端切分与常数查表，零大数除法，单次编解码 < 2ns；
//! - 零堆分配：`Base32Buf64` 与 `Base32Buf128` 栈缓冲区包装（内部 `[u8; N]`，无堆、无中间分配）；
//! - 容错解码：单指令查表映射，宽容兼容大写（`A-V` 自动映射至 `10..=31`），非法字符常数级拒绝。

use core::{fmt, ops::Deref, str::from_utf8_unchecked};
use std::{ffi::OsStr, path::Path};

/// 64 位整数编码为 Base32 的固定字符长度（4 + 12×5 = 64 位）
pub const BASE32_LEN_U64: usize = 13;

/// 128 位整数编码为 Base32 的固定字符长度（3 + 25×5 = 128 位）
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

/// 定长 Base32 栈字符串缓冲：长度由 const 泛型参数 `N` 决定，`repr(transparent)` 包裹 `[u8; N]`
///
/// 各长度形态（64 / 128 位）行为完全一致，统一由 `impl<const N: usize>` 覆盖，无需宏展开
///
/// 字段私有：唯一构造入口为 [`encode_u64`] / [`encode_u128`]，写入内容恒为合法 ASCII
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Base32Buf<const N: usize>([u8; N]);

/// 64 位整数编码后的固定 13 字符栈缓冲区
pub type Base32Buf64 = Base32Buf<BASE32_LEN_U64>;

/// 128 位整数编码后的固定 26 字符栈缓冲区
pub type Base32Buf128 = Base32Buf<BASE32_LEN_U128>;

impl<const N: usize> Base32Buf<N> {
  /// 只读 str 借用（零拷贝）
  #[inline(always)]
  pub const fn as_str(&self) -> &str {
    // SAFETY: 缓冲区内容恒定来自 BASE32_LOWER_TABLE，全部为合法 ASCII
    unsafe { from_utf8_unchecked(&self.0) }
  }

  /// 只读字节数组借用
  #[inline(always)]
  pub const fn as_bytes(&self) -> &[u8; N] {
    &self.0
  }
}

impl<const N: usize> Deref for Base32Buf<N> {
  type Target = str;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_str()
  }
}

impl<const N: usize> AsRef<str> for Base32Buf<N> {
  #[inline(always)]
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl<const N: usize> AsRef<Path> for Base32Buf<N> {
  #[inline(always)]
  fn as_ref(&self) -> &Path {
    Path::new(self.as_str())
  }
}

impl<const N: usize> AsRef<OsStr> for Base32Buf<N> {
  #[inline(always)]
  fn as_ref(&self) -> &OsStr {
    OsStr::new(self.as_str())
  }
}

impl<const N: usize> AsRef<[u8]> for Base32Buf<N> {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}

impl<const N: usize> PartialEq<str> for Base32Buf<N> {
  #[inline(always)]
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl<const N: usize> PartialEq<&str> for Base32Buf<N> {
  #[inline(always)]
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl<const N: usize> PartialEq<Base32Buf<N>> for str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf<N>) -> bool {
    self == other.as_str()
  }
}

impl<const N: usize> PartialEq<Base32Buf<N>> for &str {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf<N>) -> bool {
    *self == other.as_str()
  }
}

impl<const N: usize> PartialEq<String> for Base32Buf<N> {
  #[inline(always)]
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other.as_str()
  }
}

impl<const N: usize> PartialEq<Base32Buf<N>> for String {
  #[inline(always)]
  fn eq(&self, other: &Base32Buf<N>) -> bool {
    self.as_str() == other.as_str()
  }
}

impl<const N: usize> PartialEq<&Base32Buf<N>> for String {
  #[inline(always)]
  fn eq(&self, other: &&Base32Buf<N>) -> bool {
    self.as_str() == other.as_str()
  }
}

impl<const N: usize> PartialEq<String> for &Base32Buf<N> {
  #[inline(always)]
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other.as_str()
  }
}

impl<const N: usize> fmt::Debug for Base32Buf<N> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl<const N: usize> fmt::Display for Base32Buf<N> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// 定长 Base32 大端保序编码核心（64/128 位共享单一真源）
///
/// 位宽 `BITS` 与字符数 `N` 满足 `(N-1)*5 < BITS <= N*5`：首字符承载高位
/// `BITS - (N-1)*5` 位（4/3 位），其余每字符 5 位大端递减；
/// 调用方须保证 `val` 有效位不超过 `BITS`（本模块仅 encode_u64 / encode_u128）
const fn encode_bits<const BITS: u32, const N: usize>(val: u128) -> Base32Buf<N> {
  const {
    assert!(
      BITS > (N as u32 - 1) * 5 && BITS <= N as u32 * 5,
      "Base32 编码位宽与字符数不匹配"
    )
  };
  let mut buf = [0u8; N];
  let head_shift = (N as u32 - 1) * 5;
  // 断言保证首字符载荷 ≤ 5 位，索引恒在表内
  buf[0] = BASE32_LOWER_TABLE[(val >> head_shift) as usize];
  let mut i = 1;
  while i < N {
    buf[i] = BASE32_LOWER_TABLE[((val >> (head_shift - i as u32 * 5)) & 0x1F) as usize];
    i += 1;
  }
  Base32Buf(buf)
}

/// 将 64 位整数大端保序编码为固定 13 字符的小写 Base32 栈字符串
///
/// 首字符载荷 4 位（60..64），其余每字符 5 位大端递减
#[inline]
pub const fn encode_u64(val: u64) -> Base32Buf64 {
  encode_bits::<64, BASE32_LEN_U64>(val as u128)
}

/// 将 128 位整数大端保序编码为固定 26 字符的小写 Base32 栈字符串
///
/// 首字符载荷 3 位（125..128），其余每字符 5 位大端递减
#[inline]
pub const fn encode_u128(val: u128) -> Base32Buf128 {
  encode_bits::<128, BASE32_LEN_U128>(val)
}

/// 定长 Base32 解码核心（64/128 位共享单一真源）
///
/// 校验总长度恰为 `expect_len`、首字符载荷不超过 `head_bits` 位、其余字符全部合法，
/// 查表逐字符累积至 u128 后返回
fn decode_base32(s: &str, expect_len: usize, head_bits: u32) -> Option<u128> {
  if s.len() != expect_len {
    return None;
  }
  let bytes = s.as_bytes();

  let head = BASE32_DECODE_TABLE[bytes[0] as usize];
  if head >= (1 << head_bits) {
    return None;
  }

  let mut acc = head as u128;
  for &b in &bytes[1..] {
    let digit = BASE32_DECODE_TABLE[b as usize];
    if digit == 0xFF {
      return None;
    }
    acc = (acc << 5) | digit as u128;
  }
  Some(acc)
}

/// 将 13 字符的 Base32 字符串安全解码为 64 位整数
///
/// 若长度不等于 13、首字符载荷超过 4 位或含有非法字符，返回 `None`
#[inline]
pub fn decode_u64(s: &str) -> Option<u64> {
  u64::try_from(decode_base32(s, BASE32_LEN_U64, 4)?).ok()
}

/// 将 26 字符的 Base32 字符串安全解码为 128 位整数
///
/// 若长度不等于 26、首字符载荷超过 3 位或含有非法字符，返回 `None`
#[inline]
pub fn decode_u128(s: &str) -> Option<u128> {
  decode_base32(s, BASE32_LEN_U128, 3)
}

/// 编译期断言：长度常量与位宽严格匹配、极值编码首尾字符正确
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
