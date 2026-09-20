//! 稠密编码域：定长 6 位寄存器数组的初始化、更新、合并与导出（对标 HyperLogLog.cs 稠密分支）。

#[cfg(debug_assertions)]
use std::fmt::Write;

use super::{HLL_HEADER_BYTES, HllDtype, HyperLogLog};

impl HyperLogLog {
  /// 初始化稠密载荷（寄存器清零）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:InitDense
  pub fn init_dense(&self, ptr: &mut [u8]) {
    ptr[..self.dense_bytes].fill(0);

    self.set_prefix(ptr);
    Self::set_type(ptr, HllDtype::Dense);
    self.set_card(ptr, i64::MIN);
  }

  /// 更新稠密寄存器（单哈希入口）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateDense
  #[inline]
  pub fn update_dense(&self, ptr: &mut [u8], hv: u64) -> bool {
    let idx = self.reg_idx(hv);
    let cntlz = self.clz(hv);

    debug_assert!(idx < self.mcnt as u16);
    debug_assert!(cntlz < self.qbit + 1);

    self.update_dense_register(ptr, idx, cntlz)
  }

  /// 稠密寄存器择大更新，变更时失效基数缓存
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateDenseRegister
  #[inline]
  pub fn update_dense_register(&self, ptr: &mut [u8], idx: u16, cntlz: u8) -> bool {
    let start = HLL_HEADER_BYTES;
    if cntlz > self.get_register(&ptr[start..], idx) {
      // 失效先前计算的基数
      self.set_card(ptr, i64::MIN);
      self.set_register(&mut ptr[start..], idx, cntlz);
      return true;
    }
    false
  }

  /// 稠密 ← 稠密 逐寄存器取最大合并
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseToDense
  pub fn dense_to_dense(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let mut f_updated = false;
    let start = HLL_HEADER_BYTES;
    for idx in 0..self.mcnt as u16 {
      let src_lz = self.get_register(&src[start..], idx);
      let dst_lz = self.get_register(&dst[start..], idx);
      if src_lz > dst_lz {
        self.set_register(&mut dst[start..], idx, src_lz);
        f_updated = true;
      }
    }
    self.set_card(dst, i64::MIN);
    f_updated
  }

  /// 稠密非零寄存器导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpDenseRegs
  #[cfg(debug_assertions)]
  pub fn dump_dense_regs(&self, dense: &[u8]) -> String {
    let regs = &dense[HLL_HEADER_BYTES..];
    let mut out = String::new();
    for i in 0..self.mcnt as u16 {
      let lz = self.get_register(regs, i);
      if lz != 0 {
        let _ = writeln!(out, "{i} = {lz}");
      }
    }
    out
  }
}
