//! garnet/libs/common/Crc64.cs 的查表实现
//!
//! 与 C# 逐位参考实现完全等价：寄存器 MSB 左移、输入字节按 LSB-first 位序进入、
//! 输出经 Reflect64 反射。查表形式将逐字节 8 轮迭代折叠为单次查表，吞吐提升约 8 倍；
//! 256 项查找表（含输入位反转）在编译期 const 求值构造，运行时零初始化开销。

/// garnet/libs/common/Crc64.cs:POLY
const POLY: u64 = 0xad93d23594c935a9;

/// 编译期构造标准 MSB-first 查表：`table[d]` = 寄存器高字节为 0 时，位 `d7..d0`
/// 依次进入寄存器（MSB-first）8 轮迭代后的寄存器值
///
/// C# 逐位参考实现按 LSB-first 位序读入输入字节，对应标准形式需先将字节位反转
/// （`u8::reverse_bits`，单指令），故运行时以 `高字节 ^ c.reverse_bits()` 查本表
const CRC64_TABLE: [u64; 256] = {
  let mut table = [0u64; 256];
  let mut i = 0usize;
  while i < 256 {
    let mut crc = (i as u64) << 56;
    let mut round = 0;
    while round < 8 {
      let bit_set = (crc & 0x8000_0000_0000_0000) != 0;
      crc <<= 1;
      if bit_set {
        crc ^= POLY;
      }
      round += 1;
    }
    table[i] = crc;
    i += 1;
  }
  table
};

/// garnet/libs/common/Crc64.cs:Reflect64
#[inline]
fn reflect64(mut data: u64) -> u64 {
  data = ((data >> 1) & 0x5555_5555_5555_5555) | ((data & 0x5555_5555_5555_5555) << 1);
  data = ((data >> 2) & 0x3333_3333_3333_3333) | ((data & 0x3333_3333_3333_3333) << 2);
  data = ((data >> 4) & 0x0F0F_0F0F_0F0F_0F0F) | ((data & 0x0F0F_0F0F_0F0F_0F0F) << 4);
  data.swap_bytes()
}

/// 与 C# 逐位参考实现 (`crc64_bitwise`) 的逐字节查表等价形式
///
/// C# 逐位实现 LSB-first 读入字节，此处以 `reverse_bits`（编译为单指令）对齐其位序
fn crc64_table(data: &[u8]) -> u64 {
  let mut crc: u64 = 0;
  for &c in data {
    crc = (crc << 8) ^ CRC64_TABLE[(((crc >> 56) as u8) ^ c.reverse_bits()) as usize];
  }
  reflect64(crc)
}

/// garnet/libs/common/Crc64.cs:Hash
pub fn hash(data: &[u8]) -> [u8; 8] {
  // C# BitConverter.GetBytes 在小端平台返回小端字节序
  crc64_table(data).to_le_bytes()
}

#[cfg(test)]
mod tests {
  use super::*;

  /// C# Crc64Bitwise 的逐位参考实现（仅测试期对拍，保证查表实现 bit-exact）
  fn crc64_bitwise(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &c in data {
      let mut i: u8 = 1;
      while i != 0 {
        let mut bit_set = (crc & 0x8000_0000_0000_0000) != 0;
        if (c & i) != 0 {
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

  #[test]
  fn 查表实现与逐位参考对拍() {
    let samples: [&[u8]; 9] = [
      b"",
      b"1",
      b"123456789",
      b"\x00",
      b"\xff\xff\xff\xff\xff\xff\xff\xff",
      b"12345678",
      b"1234567890123456",
      b"The quick brown fox jumps over the lazy dog",
      &[0x80u8, 0x7f, 0x01, 0xfe, 0x55, 0xaa, 0x00, 0xff, 0x10, 0x20],
    ];
    for s in samples {
      assert_eq!(crc64_table(s), crc64_bitwise(s), "输入 {s:?} 不一致");
    }
  }
}
