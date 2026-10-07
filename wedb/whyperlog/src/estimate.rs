//! 基数估计域：NC 估计器与 τ/σ 校正函数（对标 HyperLogLog.cs Count 系列）。
//!
//! 自研依据: HLL 基数估计（C# 对应 HyperLogLogOps.cs 估计函数）

use super::{ALPHA, HLL_HEADER_BYTES, HllDtype, HyperLogLog};

impl HyperLogLog {
  /// 主计数入口（优先返回未失效的缓存基数）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Count
  ///
  /// 非法 dtype 臂（伪造载荷直面 count 的威胁模型，tests/hyperloglog 伪造
  /// 用例在册）回 i64::MIN 负哨兵且不写缓存——0 是合法基数（is_valid_card
  /// 按 >= 0 判真），落 0 即把垃圾钉成权威 0；负哨兵恒失效与 round_estimate
  /// 越界档（deviations §197）同一「垃圾估计绝不入合法缓存」不变式
  pub fn count(&self, ptr: &mut [u8]) -> i64 {
    let dtype = Self::get_type(ptr);

    // 缓存未失效直接返回
    if Self::is_valid_card(ptr) {
      return Self::get_card(ptr);
    }

    let e = match HllDtype::try_from(dtype) {
      Ok(HllDtype::Sparse) => self.count_sparse_nc_estimator(ptr),
      Ok(HllDtype::Dense) => self.count_dense_nc_estimator(ptr),
      // 非法 dtype：负哨兵恒失效（绝不写卡），对位 C# throw 的 fail-fast
      Err(()) => {
        debug_assert!(false, "HyperLogLog Count invalid data structure type");
        i64::MIN
      }
    };
    if e != i64::MIN {
      self.set_card(ptr, e);
    }
    e
  }

  /// 大值校正函数 τ
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:cTau
  fn c_tau(x: f64) -> f64 {
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
  fn c_sigma(x: f64) -> f64 {
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
  fn count_sparse_nc_estimator(&self, ptr: &[u8]) -> i64 {
    let mut rhisto = [0_usize; 64];
    for (is_zero, clen, lz) in self.sparse_ops(ptr) {
      rhisto[if is_zero { 0 } else { lz as usize }] += clen;
    }

    self.nc_estimator_from_histogram(&rhisto)
  }

  /// 稠密 NC 估计器（3 字节展开 4 寄存器）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CountDenseNCEstimator
  fn count_dense_nc_estimator(&self, ptr: &[u8]) -> i64 {
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
///
/// 越界档（非有限，或舍入后达 i64 值域边界）返回 `i64::MIN`，复刻 C# 的
/// **可观测不变式**而非 rust 的饱和转换语义：
/// - C# 侧 `(long)` 处于 unchecked 上下文，对越界/`+inf` 的 `double → long`
///   是未定义转换，x64 实践由 `cvttsd2si` 得 `long.MinValue`（**负**），
///   该负值经 C# HyperLogLog.cs 的 Count 方法之
///   `SetCard(ptr, E)` 写进载荷后，`::IsValidCard`（`GetCard >= 0`）恒判**失效**
///   ⇒ 下一轮 `::Count` 必重算，越界估计永不入缓存。
/// - rust 的 `f64 as i64` 是**饱和**转换（`+inf` 与有限越界皆得 `i64::MAX`，
///   正极大 ⇒ `crate::frame::is_valid_card` 转真），照字面直转会把越界垃圾基数
///   钉成权威缓存基数、令 `::HyperLogLog::count` 的缓存短路每轮直返该值，
///   并随 `crate::merge` 路径的写回持久落盘——与 C# 的缓存裁决方向相反。
/// - 平台差异申报：arm64 的 `fcvtzs` 对越界同为饱和（得 `long.MaxValue`），
///   故 C# 侧的负值本身是 x64 实践位型而非跨平台常量；本处锚的是「越界估计
///   绝不作为合法基数」这一不变式，收口形态取 C# x64 实践值 `i64::MIN`
///   （负 ⇒ 缓存恒失效 ⇒ 每轮重算，与 C# 主战场行为一致），不在
///   `::is_valid_card` 另加特判，避免同一不变式散成两处真值源。
///
/// 越界估计的可达面：`::HyperLogLog::nc_estimator_from_histogram` 的 rhisto
/// 无条件收录全部 64 档寄存器值，估计式却只消费 `rhisto[0]`、`rhisto[1..=qbit]`、
/// `rhisto[qbit + 1]`，落在 `qbit + 2..=` 死区的寄存器对 z 零贡献；注入式稠密
/// 载荷（寄存器值全为死区值）⇒ z 归零 ⇒ `E = +inf`，仅少数寄存器落在
/// `1..=qbit` ⇒ z 极小 ⇒ `E` 有限越界。两形皆经 `::HyperLogLog::count` 的
/// `set_card` 入口，故须在本出口收口。
/// 对位 C# HyperLogLog.cs 内 CountSparseNCEstimator 与 CountDenseNCEstimator 的 Math.Round 出口
const MIN_I64_F64: f64 = i64::MIN as f64;
const MAX_I64_F64: f64 = i64::MAX as f64;

#[inline]
pub fn round_estimate(e: f64) -> i64 {
  let r = e.round_ties_even();
  // 上界取 `>=`：i64::MAX as f64 即 2^63，已达上界即越界（该值本身不可由
  // i64 表示）；下界 `i64::MIN as f64`（-2^63）恰可表示，故不入越界档。
  // 界一律由 i64::MIN/MAX as f64 编译期导出，禁写魔法字面量。
  if !r.is_finite() || r < MIN_I64_F64 || r >= MAX_I64_F64 {
    return i64::MIN;
  }
  // 界内转换无饱和、无歧义
  r as i64
}
