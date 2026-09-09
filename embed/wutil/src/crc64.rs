/// garnet/libs/common/Crc64.cs:POLY
const POLY: u64 = 0xad93d23594c935a9;

/// garnet/libs/common/Crc64.cs:Reflect64
#[inline]
fn reflect64(mut data: u64) -> u64 {
  data = ((data >> 1) & 0x5555555555555555) | ((data & 0x5555555555555555) << 1);
  data = ((data >> 2) & 0x3333333333333333) | ((data & 0x3333333333333333) << 2);
  data = ((data >> 4) & 0x0F0F0F0F0F0F0F0F) | ((data & 0x0F0F0F0F0F0F0F0F) << 4);
  data.swap_bytes()
}

/// garnet/libs/common/Crc64.cs:Crc64Bitwise
#[inline]
fn crc64_bitwise(data: &[u8]) -> u64 {
  let mut crc: u64 = 0;

  for &c in data {
    let mut i: u8 = 1;
    while i != 0 {
      let mut bit_set = (crc & 0x8000000000000000) != 0;
      let cbit = (c & i) != 0;

      if cbit {
        bit_set = !bit_set;
      }

      crc <<= 1;

      if bit_set {
        crc ^= POLY;
      }

      i <<= 1;
    }
  }

  reflect64(crc)
}

/// garnet/libs/common/Crc64.cs:Hash
pub fn hash(data: &[u8]) -> [u8; 8] {
  let bitwise_crc = crc64_bitwise(data);
  bitwise_crc.to_le_bytes() // BitConverter.GetBytes returns little-endian on most architectures
}
