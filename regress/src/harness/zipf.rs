//! Zipf 键采样器：对标 C# KV.benchmark/YCSB.benchmark 的 `ZipfConstants` + `ZipfGenerator`
//! （garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/ZipfGenerator.cs）。
//! 逐项对齐：zeta 常数和、alpha=1/(1-θ)、cutoff2=0.5^θ、eta 修正项，next 走
//! 「u·zetaN 分段 + 幂函数」同式，θ 缺省 0.99（对齐 KV.benchmark Options.ZipfTheta 默认档）。

/// 预计算 Zipf 分布常数（O(N) 调和和，须与键基数同量级，仅构造一次跨线程/跨采样复用）
pub struct ZipfConstants {
  size: f64,
  zeta_n: f64,
  alpha: f64,
  cutoff2: f64,
  eta: f64,
}

/// θ 缺省档位（对齐 KV.benchmark `--zipf-theta` Default=0.99）
pub const DEFAULT_ZIPF_THETA: f64 = 0.99;

impl ZipfConstants {
  /// 以键基数与偏斜参数 θ 构造常数表（θ∈(0,1)，θ→1 头部热点越集中）
  pub fn new(size: u64, theta: f64) -> Self {
    let size_f = size as f64;
    let zeta_n = zeta(size, theta);
    let alpha = 1.0 / (1.0 - theta);
    let cutoff2 = 0.5_f64.powf(theta);
    let zeta_2 = zeta(2, theta);
    let eta = (1.0 - (2.0 / size_f).powf(1.0 - theta)) / (1.0 - zeta_2 / zeta_n);
    Self {
      size: size_f,
      zeta_n,
      alpha,
      cutoff2,
      eta,
    }
  }

  /// 依均匀样本 u∈[0,1) 反变换采样键下标，结果截断至 [0, size-1]（同 C# `ZipfGenerator.Next`）
  #[inline]
  pub fn sample(&self, u: f64) -> u64 {
    let uz = u * self.zeta_n;
    if uz < 1.0 {
      return 0;
    }
    if uz < 1.0 + self.cutoff2 {
      return 1;
    }
    let k = self.size * (self.eta * u - self.eta + 1.0).powf(self.alpha);
    (k as u64).min(self.size as u64 - 1)
  }
}

/// 调和部分和 Σ_{i=1..count} i^{-θ}（对标 C# `ZipfConstants.Zeta`）
fn zeta(count: u64, theta: f64) -> f64 {
  let mut sum = 0.0;
  for i in 1..=count {
    sum += 1.0 / (i as f64).powf(theta);
  }
  sum
}
