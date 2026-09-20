//! 基数估计域：NC 估计器与 τ/σ 校正函数（对标 HyperLogLog.cs Count 系列）。

use super::{ALPHA, HLL_HEADER_BYTES, HllDtype, HyperLogLog};

impl HyperLogLog {
  /// 主计数入口（优先返回未失效的缓存基数）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Count
  pub fn count(&self, ptr: &mut [u8]) -> i64 {
    let dtype = Self::get_type(ptr);

    // 缓存未失效直接返回
    if Self::is_valid_card(ptr) {
      return Self::get_card(ptr);
    }

    let e = match dtype {
      x if x == HllDtype::Sparse as u8 => self.count_sparse_nc_estimator(ptr),
      x if x == HllDtype::Dense as u8 => self.count_dense_nc_estimator(ptr),
      _ => {
        debug_assert!(false, "HyperLogLog Count invalid data structure type");
        0
      }
    };
    self.set_card(ptr, e);
    e
  }

  /// 大值校正函数 τ
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:cTau
  pub fn c_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
      return 0.0;
    }
    let mut prev_z;
    let mut y = 1.0;
    let mut z = 1.0 - x;
    let mut x = x;
    loop {
      x = x.sqrt();
      prev_z = z;
      y *= 0.5;
      z -= (1.0 - x).powi(2) * y;
      if prev_z == z {
        break;
      }
    }
    z / 3.0
  }

  /// 小值校正函数 σ
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:cSigma
  pub fn c_sigma(x: f64) -> f64 {
    if x == 1.0 {
      return f64::INFINITY;
    }
    let mut prev_z;
    let mut y = 1.0;
    let mut z = x;
    let mut x = x;
    loop {
      x *= x;
      prev_z = z;
      z += x * y;
      y += y;
      if prev_z == z {
        break;
      }
    }
    z
  }

  /// 稀疏 NC 估计器（寄存器直方图 + τ/σ 修正）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CountSparseNCEstimator
  pub fn count_sparse_nc_estimator(&self, ptr: &[u8]) -> i64 {
    let mut rhisto = [0_usize; 64];
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;

    for &op in &ptr[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };
      let lz = if iszero {
        0
      } else {
        Self::get_non_zero(op) as usize
      };
      rhisto[lz] += clen;
    }

    self.nc_estimator_from_histogram(&rhisto)
  }

  /// 稠密 NC 估计器（3 字节展开 4 寄存器）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CountDenseNCEstimator
  pub fn count_dense_nc_estimator(&self, ptr: &[u8]) -> i64 {
    let mut rhisto = [0_usize; 64];
    let regs = &ptr[HLL_HEADER_BYTES..];
    let total_bytes = (self.mcnt * 6) >> 3;

    for &[b0, b1, b2] in regs[..total_bytes].as_chunks::<3>().0 {
      rhisto[(b0 & 63) as usize] += 1;
      rhisto[(((b0 >> 6) | (b1 << 2)) & 63) as usize] += 1;
      rhisto[(((b1 >> 4) | (b2 << 4)) & 63) as usize] += 1;
      rhisto[((b2 >> 2) & 63) as usize] += 1;
    }

    self.nc_estimator_from_histogram(&rhisto)
  }

  /// 直方图 → NC 估计（稀疏/稠密共用尾部）
  fn nc_estimator_from_histogram(&self, rhisto: &[usize; 64]) -> i64 {
    let mcnt = self.mcnt as f64;
    let mut z = mcnt * Self::c_tau((self.mcnt - rhisto[self.qbit as usize + 1]) as f64 / mcnt);

    for j in (1..=self.qbit as usize).rev() {
      z += rhisto[j] as f64;
      z *= 0.5;
    }
    z += mcnt * Self::c_sigma(rhisto[0] as f64 / mcnt);
    let e = ALPHA * mcnt * mcnt / z;

    round_estimate(e)
  }
}

/// 估计值出口舍入
///
/// 对标 C#: (long)Math.Round(E)（CountSparse/CountDenseNCEstimator 两处内联
/// 舍入的 rust 共用出口）。.NET Math.Round 默认中点舍入为
/// MidpointRounding.ToEven（银行家舍入），逐字对位用 round_ties_even；
/// f64::round 为 half-away-from-zero，E 恰落 x.5 时会差 ±1，不得混用。
#[inline]
fn round_estimate(e: f64) -> i64 {
  e.round_ties_even() as i64
}

#[cfg(test)]
mod tests {
  use super::round_estimate;

  /// x.5 中点落点：银行家舍入取偶（C# Math.Round 默认口径；
  /// half-away-from-zero 会在 2.5/4.5 处给出 3/5）
  #[test]
  fn midpoint_ties_round_to_even() {
    assert_eq!(round_estimate(0.5), 0);
    assert_eq!(round_estimate(1.5), 2);
    assert_eq!(round_estimate(2.5), 2);
    assert_eq!(round_estimate(3.5), 4);
    assert_eq!(round_estimate(4.5), 4);
    assert_eq!(round_estimate(5.5), 6);
  }

  /// 非中点落点与 f64::round 行为一致（舍入口径仅在中点分叉）
  #[test]
  fn non_midpoint_unaffected() {
    assert_eq!(round_estimate(2.4), 2);
    assert_eq!(round_estimate(2.6), 3);
    assert_eq!(round_estimate(0.0), 0);
    assert_eq!(round_estimate(1024.5), 1024);
    assert_eq!(round_estimate(1025.5), 1026);
  }
}
