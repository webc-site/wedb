//! RDB DUMP/RESTORE 校验和（对标 libs/common/Crc64.cs，移植 redis crc64）
//!
//! 单一实现为 `wbase::crc64`（编译期查表位精确实现），本域仅按 DUMP/RESTORE
//! 的字节输出形态转接。

use wbase::crc64;

/// libs/common/Crc64.cs:Hash
///
/// Computes crc64（小端 8 字节输出，与 C# BitConverter.GetBytes 一致）
pub fn hash(data: &[u8]) -> [u8; 8] {
  crc64::hash(data)
}

#[cfg(test)]
mod tests {
  use super::{crc64, hash};

  #[test]
  fn empty_and_properties() {
    // 空输入：crc=0，reflect 后仍为 0
    assert_eq!(hash(b""), [0u8; 8]);
    // 确定性 + 长度敏感性
    assert_eq!(hash(b"abc"), hash(b"abc"));
    assert_ne!(hash(b"abc"), hash(b"abd"));
    assert_ne!(hash(b"a"), hash(b"aa"));
    // 与 wbase 查表实现一致（单一出处对拍）
    assert_eq!(hash(b"123456789"), crc64::hash(b"123456789"));
  }
}
