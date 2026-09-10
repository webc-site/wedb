//! RDB DUMP/RESTORE 校验和（对标 libs/common/Crc64.cs，移植 redis crc64）

/// libs/common/Crc64.cs:POLY
///
/// Polynomial (same as redis)
const POLY: u64 = 0xad93d23594c935a9;

/// libs/common/Crc64.cs:Reflect64
///
/// Reverse all bits in a 64-bit value (bit reflection)
#[inline]
const fn reflect64(mut data: u64) -> u64 {
  // swap odd/even bits
  data = ((data >> 1) & 0x5555555555555555) | ((data & 0x5555555555555555) << 1);
  // swap consecutive pairs
  data = ((data >> 2) & 0x3333333333333333) | ((data & 0x3333333333333333) << 2);
  // swap nibbles
  data = ((data >> 4) & 0x0F0F0F0F0F0F0F0F) | ((data & 0x0F0F0F0F0F0F0F0F) << 4);
  // swap bytes, then 2-byte pairs, then 4-byte pairs
  data.swap_bytes()
}

/// libs/common/Crc64.cs:Crc64Bitwise
///
/// A direct bit-by-bit CRC64 calculation (like _crc64 in C)
fn crc64_bitwise(data: &[u8]) -> u64 {
  let mut crc: u64 = 0;

  for &c in data {
    let mut i: u8 = 1;
    while i != 0 {
      // interpret the top bit of 'crc' and current bit of 'c'
      let mut bit_set = (crc & 0x8000000000000000) != 0;
      let c_bit = (c & i) != 0;

      // if cbit flips the sense, invert bitSet
      if c_bit {
        bit_set = !bit_set;
      }

      // shift
      crc <<= 1;

      // apply polynomial if needed
      if bit_set {
        crc ^= POLY;
      }

      i <<= 1;
    }
  }

  // reflect and XOR, per standard
  reflect64(crc)
}

/// libs/common/Crc64.cs:Hash
///
/// Computes crc64（小端 8 字节输出，与 C# BitConverter.GetBytes 一致）
pub fn hash(data: &[u8]) -> [u8; 8] {
  crc64_bitwise(data).to_le_bytes()
}

#[cfg(test)]
mod tests {
  use super::{crc64_bitwise, hash};

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
  fn little_endian_output_matches_bitwise() {
    // Hash 输出为位运算结果的小端 8 字节
    let data = b"123456789";
    let le = crc64_bitwise(data).to_le_bytes();
    assert_eq!(hash(data), le);
  }
}
