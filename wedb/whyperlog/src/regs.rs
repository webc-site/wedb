//! 寄存器读写、位操作与诊断导出（对标 HyperLogLog.cs 寄存器访问与比对）。

#[cfg(debug_assertions)]
use crate::{HLL_HEADER_BYTES, HllDtype};
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

  // ---- 调试比对与导出（C# HyperLogLog.cs:1100-1298 的 #if DEBUG 段） ----
  //
  // 三件均无生产读者，门控后 release 面不再导出。

  /// 按类型导出原始字节
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpRawBytes
  #[cfg(debug_assertions)]
  pub fn dump_raw_bytes(&self, ptr: &[u8]) -> String {
    match Self::get_type(ptr) {
      x if x == HllDtype::Sparse as u8 => self.dump_sparse_raw_bytes(ptr),
      _ => String::from("dense"),
    }
  }

  /// 稀疏与稠密寄存器一致性校验
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CompareSparseToDense
  #[cfg(debug_assertions)]
  pub fn compare_sparse_to_dense(&self, dense: &[u8], sparse: &[u8]) -> Result<(), String> {
    let regs = &dense[HLL_HEADER_BYTES..];
    let rle_size = self.get_sparse_rle_size(sparse) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    let mut offset = 0_usize;

    for &op in &sparse[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };

      if !iszero {
        let val = Self::get_non_zero(op);
        if offset >= self.mcnt {
          return Err(format!("FAILED: Nonzero not contained: {offset} = {val}"));
        }
        let dense_val = self.get_register(regs, offset as u16);
        if dense_val == 0 {
          return Err(format!("FAILED: Nonzero not contained: {offset} = {val}"));
        }
        if dense_val != val {
          return Err(format!(
            "FAILED: Nonzero wrong nonzero value: {offset} = {val}"
          ));
        }
      }
      offset += clen;
    }
    Ok(())
  }

  /// 按类型导出非零寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpRegs
  #[cfg(debug_assertions)]
  pub fn dump_regs(&self, ptr: &[u8]) -> String {
    match Self::get_type(ptr) {
      x if x == HllDtype::Sparse as u8 => self.dump_sparse_regs(ptr),
      _ => self.dump_dense_regs(ptr),
    }
  }
}
