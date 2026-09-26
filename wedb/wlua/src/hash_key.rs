//! 脚本哈希键：脚本 SHA1 摘要的规范化存储
//! （对标 libs/server/Lua/ScriptHashKey.cs:ScriptHashKey）。

use std::{
  fmt::{self, Display, Formatter},
  ops::Deref,
  str,
};

use wbase::hex::hex_encode_20;

/// SHA1 十六进制长度。
pub(crate) const SHA1_HEX_LEN: usize = 40;

/// 脚本摘要键：20 字节 SHA1 的 40 字符小写十六进制串（定长缓冲，零分配）。
///
/// 桶散布承接：libs/server/Lua/ScriptHashKey.cs:GetHashCode（取首 4 字节）
/// 由 `#[derive(Hash)]` 单点派生（gxhash Hasher 全缓冲混洗），无独立散布
/// 函数面；等值判定 `PartialEq` 同为派生（C# Equals 的 40 字节向量比对，
/// 摘要键下等价）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScriptHashKey {
  /// 摘要缓冲（小写十六进制 ASCII，恒 40 字节有效）。
  buf: [u8; SHA1_HEX_LEN],
}

impl Default for ScriptHashKey {
  #[inline]
  fn default() -> Self {
    Self {
      buf: [b'0'; SHA1_HEX_LEN],
    }
  }
}

impl ScriptHashKey {
  /// libs/server/Lua/ScriptHashKey.cs:ScriptHashKey（构造，sha1 → 小写 hex）。
  pub const fn new(sha1_digest: &[u8; 20]) -> Self {
    Self {
      buf: hex_encode_20(sha1_digest),
    }
  }

  /// 构造自 40 字符 hex 切片 (ScriptHashKey span 形态)
  ///
  /// EVALSHA / SCRIPT EXISTS 收到的摘要即为 hex 文本；长度或字符不合法返回 None。
  pub fn from_hex(hex: &[u8]) -> Option<Self> {
    if hex.len() != SHA1_HEX_LEN {
      return None;
    }
    let mut buf = [0u8; SHA1_HEX_LEN];
    for (i, &b) in hex.iter().enumerate() {
      if !b.is_ascii_hexdigit() {
        return None;
      }
      buf[i] = b.to_ascii_lowercase();
    }
    Some(Self { buf })
  }

  /// 摘要字符串视图（hex 为 ASCII，必然合法 UTF-8）。
  #[inline]
  #[must_use]
  pub fn as_str(&self) -> &str {
    // SAFETY: buf 仅包含合法 ASCII 十六进制字符，恒为合法 UTF-8。
    unsafe { str::from_utf8_unchecked(&self.buf) }
  }

  /// 获取字节切片视图
  #[inline]
  #[must_use]
  pub const fn as_bytes(&self) -> &[u8; SHA1_HEX_LEN] {
    &self.buf
  }

  /// libs/server/Lua/ScriptHashKey.cs:Equals
  ///
  /// 与另一摘要键逐字节相等。
  ///
  /// 同款 SIMD 取舍（本仓 SIMD 单点 `wbase::simd`，feature `simd` 启用）：C#
  /// 双载 `Vector256`（0..32 与 8..40 重叠覆盖定长 40B）是裸指针形态的省指令
  /// 写法，本类型自带定长缓冲、整段比较即等价，宽度交编译期自动向量化承接；
  /// fearless_simd 选型面只覆盖变长键比对（`fast_key_eq`），此处不引向量臂。
  #[inline]
  #[must_use]
  pub fn equals(&self, other: &ScriptHashKey) -> bool {
    self.buf == other.buf
  }
}

impl Deref for ScriptHashKey {
  type Target = str;
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.as_str()
  }
}

impl AsRef<str> for ScriptHashKey {
  #[inline]
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl AsRef<[u8]> for ScriptHashKey {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    &self.buf
  }
}

impl Display for ScriptHashKey {
  #[inline]
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

#[cfg(test)]
mod tests {
  use super::{SHA1_HEX_LEN, ScriptHashKey};

  #[test]
  fn digest_to_hex() {
    let digest = [0xabu8; 20];
    let key = ScriptHashKey::new(&digest);
    assert_eq!(key.as_str(), &"ab".repeat(20));
    assert_eq!(key.as_str().len(), SHA1_HEX_LEN);
  }

  #[test]
  fn equals_matches_content() {
    let a = ScriptHashKey::new(&[1u8; 20]);
    let b = ScriptHashKey::new(&[1u8; 20]);
    let c = ScriptHashKey::new(&[2u8; 20]);
    assert!(a.equals(&b));
    assert!(!a.equals(&c));
  }
}
