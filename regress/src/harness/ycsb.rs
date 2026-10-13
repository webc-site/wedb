//! YCSB 风格读写混合基准夹具：对标 C# KV.benchmark `KvBenchmark.Worker.cs` 的
//! RUMD 混合工况热环（zipf/uniform 键采样 + 独立掷币选操作 + 删除自动回插）与
//! `KvBenchmark.Validate.cs` 的回读校验。wkv 无 RMW 原语，故 RUMD 折算为 RUD
//! （读/写/删），rmw 比例并入写侧（KV.benchmark 默认档 rmw=0，折算无损）。
//!
//! 工况对齐 KV.benchmark 默认档：θ=0.99、删除回插、值图案按键派生可回读复算。

use fastrand::Rng;

use super::{DEFAULT_KEY_SIZE, DEFAULT_VALUE_SIZE, WkvHarness, make_num_key, zipf::ZipfConstants};
use crate::harness::zipf::DEFAULT_ZIPF_THETA;

/// 键空间前缀（与现有 regress 基准命名空间隔离）
const YCSB_KEY_PREFIX: &[u8] = b"wkv:ycsb:k:";

/// 单条基准键值图案：byte[i] = (key_id*31 + i) & 0xFF（对标 C# `writerThread*31+i` 图案，
/// 单逻辑写者以 key_id 代 threadIdx），使 validate 回读可纯依 key_id 复算期望值
#[inline]
pub fn pattern_value(key_id: u64) -> [u8; DEFAULT_VALUE_SIZE] {
  let mut val = [0u8; DEFAULT_VALUE_SIZE];
  let base = key_id.wrapping_mul(31);
  for (i, b) in val.iter_mut().enumerate() {
    *b = (base + i as u64) as u8;
  }
  val
}

#[inline]
fn ycsb_key(key_id: u64) -> [u8; DEFAULT_KEY_SIZE] {
  make_num_key::<DEFAULT_KEY_SIZE>(YCSB_KEY_PREFIX, key_id as usize)
}

/// 混合工况参数（比例和须为 100）
#[derive(Debug, Clone, Copy)]
pub struct WorkloadParams {
  /// 唯一键基数
  pub key_count: u64,
  /// 读百分比 (R)
  pub read_pct: u32,
  /// 写百分比 (U)
  pub upsert_pct: u32,
  /// 删百分比 (D)，>0 时删除自动回插（同 KV.benchmark `RumdHasDeletes`）
  pub delete_pct: u32,
  /// Some(θ) 走 zipf 采样，None 走 uniform（对齐 KV.benchmark `--distribution`）
  pub zipf_theta: Option<f64>,
  /// 基准随机种子（对齐 KV.benchmark `--seed` 语义，键采样与选币两路派生独立子种）
  pub seed: u64,
}

impl WorkloadParams {
  /// YCSB core workload A：50 读 / 50 更新、zipf θ=0.99（KV.benchmark 读写混合锚档）
  pub fn ycsb_a(key_count: u64) -> Self {
    Self {
      key_count,
      read_pct: 50,
      upsert_pct: 50,
      delete_pct: 0,
      zipf_theta: Some(DEFAULT_ZIPF_THETA),
      seed: 211,
    }
  }
}

/// 单次混合步执行后的操作计数
#[derive(Debug, Default, Clone, Copy)]
pub struct OpCounts {
  pub reads: u64,
  pub writes: u64,
  pub deletes: u64,
}

/// YCSB 混合工况夹具：托管 wkv 存储 + 键采样器 + 选币掷币器
pub struct YcsbHarness {
  pub wkv: WkvHarness,
  params: WorkloadParams,
  zipf: Option<ZipfConstants>,
  /// 键采样 RNG（RNG#1）
  key_rng: Rng,
  /// 选操作 RNG（RNG#2，独立于键采样——zipf 非均匀消耗键 RNG，选币须独立，同 C# 强制约束）
  op_rng: Rng,
  buf: Vec<u8>,
}

impl YcsbHarness {
  pub fn new(params: WorkloadParams) -> aok::Result<Self> {
    let wkv = WkvHarness::default_bench(params.key_count.max(1) as usize)?;
    let zipf = params
      .zipf_theta
      .map(|t| ZipfConstants::new(params.key_count, t));
    Ok(Self {
      wkv,
      params,
      zipf,
      key_rng: Rng::with_seed(params.seed),
      op_rng: Rng::with_seed(params.seed ^ 0x9E37_79B9),
      buf: vec![0u8; DEFAULT_VALUE_SIZE],
    })
  }

  /// 装载阶段：顺序 upsert key_count 个键（键值图案派生自 key_id，对标 KV.benchmark load 分区）
  pub fn load(&mut self) {
    for id in 0..self.params.key_count {
      let key = ycsb_key(id);
      let val = pattern_value(id);
      self.wkv.upsert_sync(&key, &val);
    }
  }

  #[inline]
  fn next_key_id(&mut self) -> u64 {
    let u = self.key_rng.f64();
    match &self.zipf {
      Some(z) => z.sample(u),
      None => ((u * self.params.key_count as f64) as u64).min(self.params.key_count - 1),
    }
  }

  /// 依 RUD 比例掷币选一操作并执行 n 次（不做计时，供上层 warmup / 采样窗复用）
  pub fn run_ops(&mut self, n: u64) -> OpCounts {
    // 预计算 32 位选币阈值：读 < readCutoff，写 < upsertCutoff，余为删（同 C# cutoff 比较）
    let read_cut = ((self.params.read_pct as u64) << 32) / 100;
    let upsert_cut = (((self.params.read_pct + self.params.upsert_pct) as u64) << 32) / 100;
    let mut counts = OpCounts::default();
    for _ in 0..n {
      let id = self.next_key_id();
      let coin = self.op_rng.u32(0..u32::MAX) as u64;
      let key = ycsb_key(id);
      if coin < read_cut {
        let len = self.wkv.get_sync(&key, &mut self.buf);
        counts.reads += 1;
        // 读须真命中：读回空即工况失真（删除回插保证键常驻）
        debug_assert!(len.is_some(), "ycsb 读落空: key_id={id}");
      } else if coin < upsert_cut {
        let val = pattern_value(id);
        self.wkv.upsert_sync(&key, &val);
        counts.writes += 1;
      } else {
        self.wkv.delete_sync(&key);
        counts.deletes += 1;
        if self.params.delete_pct > 0 {
          // 删除自动回插，保持键常驻（同 KV.benchmark `deleteReinsert`），计入写侧
          let val = pattern_value(id);
          self.wkv.upsert_sync(&key, &val);
          counts.writes += 1;
        }
      }
    }
    counts
  }

  /// warmup + 测量：先跑 warmup_ops 丢弃预热窗，再返回供上层计时的测量窗句柄
  /// （对齐 KV.benchmark `--warmup-sec` 结果剔除语义；调用方对 `run_ops(meas_ops)` 计时）
  pub fn warmup(&mut self, warmup_ops: u64) {
    self.run_ops(warmup_ops);
  }

  /// 全量回读校验：逐键读回并比对图案前缀，返回 (回读落空数, 图案失配数)
  /// （对标 C# `KvBenchmark.Validate` 的 mismatches/readMisses）
  pub fn validate(&mut self) -> (u64, u64) {
    let mut misses = 0u64;
    let mut mismatches = 0u64;
    for id in 0..self.params.key_count {
      let key = ycsb_key(id);
      match self.wkv.get_sync(&key, &mut self.buf) {
        Some(len) => {
          let expect = pattern_value(id);
          if self.buf[..len] != expect[..len] {
            mismatches += 1;
          }
        }
        None => misses += 1,
      }
    }
    (mismatches, misses)
  }
}
