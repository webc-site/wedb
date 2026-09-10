//! ACL 口令（对标 libs/server/ACL/ACLPassword.cs）
//!
//! 口令以 SHA-256 哈希存储（32 字节），比较走常量时间路径。

use std::{
  fmt,
  hash::{Hash, Hasher},
};

use sha2::{Digest, Sha256};

use super::{acl_exception::AclError, secrets_utility::constant_equals};

/// 每条哈希的字节数（SHA-256）
pub const NUM_HASH_BYTES: usize = 32;

/// ACL 口令（SHA-256 哈希值）
#[derive(Debug, Clone)]
pub struct AclPassword {
  /// 口令哈希
  pub password_hash: [u8; NUM_HASH_BYTES],
}

impl AclPassword {
  /// 由明文口令构造（SHA-256(UTF-8(明文))）
  ///
  /// libs/server/ACL/ACLPassword.cs:ACLPasswordFromString
  pub fn from_string(password: &str) -> Self {
    Self {
      password_hash: Sha256::digest(password.as_bytes()).into(),
    }
  }

  /// 由十六进制哈希串构造（64 个十六进制字符）
  ///
  /// libs/server/ACL/ACLPassword.cs:ACLPasswordFromHash
  pub fn from_hash(hash_string: &str) -> Result<Self, AclError> {
    let bytes = hash_string.as_bytes();
    if bytes.len() != NUM_HASH_BYTES * 2 {
      return Err(AclError::Password(
        "Unable to parse input password hash. The input is of wrong length.".into(),
      ));
    }
    let mut password_hash = [0u8; NUM_HASH_BYTES];
    // 单遍解析：每两个十六进制字符折一个字节
    for (i, slot) in password_hash.iter_mut().enumerate() {
      let hi = hex_val(bytes[i * 2]);
      let lo = hex_val(bytes[i * 2 + 1]);
      match (hi, lo) {
        (Some(hi), Some(lo)) => *slot = hi << 4 | lo,
        _ => {
          return Err(AclError::Password(
            "Unable to parse input password hash. The input is not of the correct format.".into(),
          ));
        }
      }
    }
    Ok(Self { password_hash })
  }
}

/// 单个十六进制字符折值（大小写均可）
#[inline]
const fn hex_val(c: u8) -> Option<u8> {
  match c {
    b'0'..=b'9' => Some(c - b'0'),
    b'a'..=b'f' => Some(c - b'a' + 10),
    b'A'..=b'F' => Some(c - b'A' + 10),
    _ => None,
  }
}

/// 常量时间比较（口令哈希相等性）
///
/// libs/server/ACL/ACLPassword.cs:Equals
impl PartialEq for AclPassword {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    constant_equals(&self.password_hash, &other.password_hash)
  }
}

impl Eq for AclPassword {}

/// 加速索引的短哈希（仅取首字节）
///
/// libs/server/ACL/ACLPassword.cs:GetHashCode
impl Hash for AclPassword {
  #[inline]
  fn hash<H: Hasher>(&self, state: &mut H) {
    state.write_u8(self.password_hash[0]);
  }
}

/// 小写十六进制输出
///
/// libs/server/ACL/ACLPassword.cs:ToString
impl fmt::Display for AclPassword {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for b in self.password_hash {
      write!(f, "{b:02x}")?;
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// garnet/test/standalone/Garnet.test.acl/Resp/ACL/AclTest.cs:DummyPasswordHash
  const DUMMY_PASSWORD_HASH: &str =
    "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";

  #[test]
  fn from_string_sha256_hex() {
    let p = AclPassword::from_string("passw0rd");
    assert_eq!(p.to_string(), DUMMY_PASSWORD_HASH);
  }

  #[test]
  fn from_hash_roundtrip() {
    let p = AclPassword::from_hash(DUMMY_PASSWORD_HASH).unwrap();
    assert_eq!(p, AclPassword::from_string("passw0rd"));

    // 大写十六进制同样接受
    let upper = AclPassword::from_hash(&DUMMY_PASSWORD_HASH.to_uppercase()).unwrap();
    assert_eq!(upper, p);
  }

  #[test]
  fn from_hash_rejects_bad_input() {
    assert!(matches!(
      AclPassword::from_hash("abcd"),
      Err(AclError::Password(_))
    ));
    assert!(matches!(
      AclPassword::from_hash(&"z".repeat(64)),
      Err(AclError::Password(_))
    ));
  }
}
