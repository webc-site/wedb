//! 多实例合并与融合逻辑（对标 HyperLogLog.cs 合并分支）。

use crate::{HllDtype, HyperLogLog};

impl HyperLogLog {
  /// 原位合并：目标稠密可直接并入；目标稀疏须确认空间充足
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:TryMerge
  pub fn try_merge(&self, src: &[u8], dst: &mut [u8], dst_len: usize) -> bool {
    let dtype_dst = Self::get_type(dst);
    if dtype_dst == HllDtype::Dense as u8 {
      self.merge(src, dst);
      self.set_card(dst, i64::MIN);
      return true;
    }

    // 目标稀疏
    let dtype_src = Self::get_type(src);
    if dtype_src == HllDtype::Sparse as u8 {
      // 原位合并容纳判定与 PFADD 原位臂同一谓词（源非零寄存器逐枚经
      // UpdateSparseReg 插入，每枚最坏 +2B 计一单元）
      if self.sparse_fits(
        self.sparse_current_size_in_bytes(dst),
        self.sparse_count_non_zero(src),
        dst_len,
      ) {
        self.merge(src, dst);
        self.set_card(dst, i64::MIN);
        return true;
      }

      return false;
    }

    // 稠密→稀疏恒失败
    false
  }

  /// 合并分派（按两侧编码选择路径）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Merge
  pub fn merge(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let dtype_src = Self::get_type(src);
    let dtype_dst = Self::get_type(dst);

    match (dtype_src, dtype_dst) {
      (s, d) if d == HllDtype::Dense as u8 => {
        if s == HllDtype::Sparse as u8 {
          self.sparse_to_dense(src, dst)
        } else {
          self.dense_to_dense(src, dst)
        }
      }
      (s, d) if s == HllDtype::Sparse as u8 && d == HllDtype::Sparse as u8 => {
        self.sparse_to_sparse(src, dst)
      }
      _ => {
        debug_assert!(false, "Merge exception");
        false
      }
    }
  }
}
