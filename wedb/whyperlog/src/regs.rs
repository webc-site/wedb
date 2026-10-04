//! 寄存器读写与位操作（对标 HyperLogLog.cs 寄存器访问）。

use crate::{HyperLogLog, REG_BITS, REG_BITS_MSK};

impl HyperLogLog {
  /// 哈希值 → 寄存器下标
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:RegIdx
  #[inline]
  pub fn reg_idx(&self, hv: u64) -> u16 {
    (hv & (self.mcnt as u64 - 1)) as u16
  }

  /// 哈希值剩余位的前导零计数（封顶 qbit + 1）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:clz
  #[inline]
  pub fn clz(&self, hv: u64) -> u8 {
    (hv.leading_zeros() as u8).min(self.qbit) + 1
  }

  /// 读 6 位寄存器（LSB 起跨字节打包）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:_get_register
  #[inline]
  pub fn get_register(&self, reg: &[u8], idx: u16) -> u8 {
    let m = idx as usize * REG_BITS as usize;
    let b0 = m >> 3;
    let lsb = (m & 0x7) as u32;

    let v0 = reg[b0] >> lsb;
    let b1 = reg.get(b0 + 1).copied().unwrap_or(0);
    let v1 = ((b1 as u16) << (8 - lsb)) as u8;

    (v0 | v1) & REG_BITS_MSK
  }

  /// 写 6 位寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:_set_register
  #[inline]
  pub fn set_register(&self, reg: &mut [u8], idx: u16, val: u8) {
    let m = idx as usize * REG_BITS as usize;
    let b0 = m >> 3;
    let lsb = (m & 0x7) as u32;
    let msb = 8 - lsb;

    debug_assert!(b0 < (self.mcnt * REG_BITS as usize) / 8);

    reg[b0] &= !(((REG_BITS_MSK as u16) << lsb) as u8);
    reg[b0] |= ((val as u16) << lsb) as u8;

    // 末寄存器（idx = mcnt-1, lsb = 2）不跨字节：C# 对 b0+1 的写恒为
    // "保持全位 |= 0"（val >> 6 == 0），Rust 以边界短路等价实现
    if let Some(next_byte) = reg.get_mut(b0 + 1) {
      *next_byte &= !(((REG_BITS_MSK as u16) >> msb) as u8);
      *next_byte |= ((val as u16) >> msb) as u8;
    }
  }
}
