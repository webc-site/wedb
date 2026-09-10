//! RDB DUMP/RESTORE 校验和（复用 wbase::crc64，对标 libs/common/Crc64.cs）

pub use wbase::crc64::hash;

#[cfg(test)]
mod tests {
  use super::hash;

  #[test]
  fn empty_and_properties() {
    // 空输入：crc=0，reflect 后仍为 0
    assert_eq!(hash(b""), [0u8; 8]);
    // 确定性 + 长度敏感性
    assert_eq!(hash(b"abc"), hash(b"abc"));
    assert_ne!(hash(b"abc"), hash(b"abd"));
    assert_ne!(hash(b"a"), hash(b"aa"));
  }

  #[test]
  fn known_vector() {
    let data = b"123456789";
    // Redis/Garnet CRC64 (Jones poly 0xad93d23594c935a9): 0xe9c6d914c4b8d9ca
    assert_eq!(hash(data), [202, 217, 184, 196, 20, 217, 198, 233]);
  }
}
