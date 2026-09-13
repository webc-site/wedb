//! 脚本哈希键：脚本 SHA1 摘要的规范化存储
//! （对标 libs/server/Lua/ScriptHashKey.cs:ScriptHashKey）。

use std::{
  fmt::{self, Display, Formatter},
  ops::Deref,
  str,
};

/// SHA1 十六进制长度。
pub const SHA1_HEX_LEN: usize = 40;

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// 脚本摘要键：20 字节 SHA1 的 40 字符小写十六进制串（定长缓冲，零分配）。
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
    let mut buf = [0u8; SHA1_HEX_LEN];
    let mut i = 0;
    while i < 20 {
      let b = sha1_digest[i];
      buf[i * 2] = HEX_CHARS[(b >> 4) as usize];
      buf[i * 2 + 1] = HEX_CHARS[(b & 0xf) as usize];
      i += 1;
    }
    Self { buf }
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
    // SAFETY: buf 仅包含 HEX_CHARS 或合法 ASCII 十六进制字符，恒为合法 UTF-8。
    unsafe { str::from_utf8_unchecked(&self.buf) }
  }

  /// 获取字节切片视图
  #[inline]
  #[must_use]
  pub const fn as_bytes(&self) -> &[u8; SHA1_HEX_LEN] {
    &self.buf
  }

  /// libs/server/Lua/ScriptHashKey.cs:CopyTo
  ///
  /// 复制摘要到目标缓冲（目标须 >= 40 字节）。
  pub fn copy_to(&self, destination: &mut [u8]) -> bool {
    if let Some(dest) = destination.get_mut(..SHA1_HEX_LEN) {
      dest.copy_from_slice(&self.buf);
      true
    } else {
      false
    }
  }

  /// libs/server/Lua/ScriptHashKey.cs:Equals
  ///
  /// 与另一摘要键逐字节相等。
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
  use super::ScriptHashKey;

  #[test]
  fn digest_to_hex_and_copy() {
    let digest = [0xabu8; 20];
    let key = ScriptHashKey::new(&digest);
    assert_eq!(key.as_str(), &"ab".repeat(20));
    assert_eq!(key.as_str().len(), 40);

    let mut dst = [0u8; 40];
    assert!(key.copy_to(&mut dst));
    assert_eq!(&dst, key.as_str().as_bytes());
    assert!(!key.copy_to(&mut [0u8; 39]));
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
