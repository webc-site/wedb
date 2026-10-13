//! 评测负载参数：默认逐项对齐 redb-bench 的常量，
//! 另提供缩放与按物理内存收敛缓存的入口（CI 小内存 runner 用）。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// redb-bench 的默认缓存：4 GiB
pub const REDB_CACHE_SIZE: usize = 4 * 1024 * 1024 * 1024;

/// 缩放前用于折算超时的负载字节数（redb 标准档）
const BASELINE_BYTES: u64 = 3_872_000_000; // 5M × (24 + 150) + 写读扫描等的近似总量

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
  /// 「random reads」重复轮数，取中位数
  pub read_iterations: usize,
  /// 批量装载条目数
  pub bulk_elements: usize,
  /// 有序装载条目数
  pub sorted_elements: usize,
  /// 单条独立提交次数
  pub individual_writes: usize,
  /// nosync 档位下的单条提交次数
  pub nosync_writes: usize,
  /// 小批次事务数
  pub batch_writes: usize,
  /// 每批条目数
  pub batch_size: usize,
  /// 范围读重复轮数，取中位数
  pub scan_iterations: usize,
  /// 随机点读次数
  pub num_reads: usize,
  /// 随机范围读次数
  pub num_scans: usize,
  /// 每次范围读步进的对象数
  pub scan_len: usize,
  /// pop 段目标弹出条数
  pub pop_removals: usize,
  /// pop 段慢速判定采样条数
  pub pop_sample_removals: usize,
  /// 采样在多久内完成才视为快路径（毫秒）
  pub slow_pop_sample_limit_ms: u64,
  /// 键长
  pub key_size: usize,
  /// 值长
  pub value_size: usize,
  /// 确定性随机种子
  pub rng_seed: u64,
  /// 引擎缓存预算（字节）
  pub cache_size: usize,
  /// 多线程读段线程数
  pub thread_counts: Vec<usize>,
}

impl Default for Workload {
  fn default() -> Self {
    Self {
      read_iterations: 3,
      bulk_elements: 5_000_000,
      sorted_elements: 1_000_000,
      individual_writes: 1_000,
      nosync_writes: 50_000,
      batch_writes: 100,
      batch_size: 1_000,
      scan_iterations: 3,
      num_reads: 1_000_000,
      num_scans: 500_000,
      scan_len: 10,
      pop_removals: 500_000,
      pop_sample_removals: 5_000,
      slow_pop_sample_limit_ms: 1_000,
      key_size: 24,
      value_size: 150,
      rng_seed: 3,
      cache_size: REDB_CACHE_SIZE,
      thread_counts: vec![4, 8, 16, 32],
    }
  }
}

impl Workload {
  /// redb 标准档
  pub fn redb_standard() -> Self {
    Self::default()
  }

  /// 按 `scale` 缩放条目量级（bulk/sorted/reads/scans/pop），
  /// 小量级写段（individual/nosync/batch）保持原样以免测不出差异。
  ///
  /// 读段用与装载同一种子重放键序列，因此 `num_reads`、`num_scans`
  /// 必须始终不超过表内条目数：这里保持与 redb 相同的比例关系。
  pub fn scaled(scale: f64) -> Self {
    let scale = if scale.is_finite() && scale > 0.0 {
      scale
    } else {
      1.0
    };
    let mut w = Self::default();
    w.bulk_elements = ((5_000_000_f64) * scale).max(1.0) as usize;
    w.sorted_elements = ((1_000_000_f64) * scale).max(1.0) as usize;
    w.num_reads = ((1_000_000_f64) * scale).max(1.0) as usize;
    w.num_scans = ((500_000_f64) * scale).max(1.0) as usize;
    w.pop_removals = ((500_000_f64) * scale).max(2.0) as usize;
    w.pop_sample_removals = w.pop_removals.min(5_000);
    w
  }

  /// 缓存预算收敛到物理内存的一半以内，避免小内存 runner 直接 OOM。
  /// 返回是否发生收敛，调用方据此在表格外打印告警。
  pub fn cap_cache_to_memory(&mut self, total_memory_gib: f64) -> bool {
    if total_memory_gib <= 0.0 {
      return false;
    }
    let limit = (total_memory_gib * 0.5 * 1024.0 * 1024.0 * 1024.0) as usize;
    if self.cache_size > limit {
      self.cache_size = limit;
      return true;
    }
    false
  }

  pub fn slow_pop_sample_limit(&self) -> Duration {
    Duration::from_millis(self.slow_pop_sample_limit_ms)
  }

  /// 装载完成后表内条目总数：bulk + 单条写 + 小批 + nosync 段的写入量
  pub fn loaded_elements(&self) -> usize {
    self.bulk_elements
      + self.individual_writes
      + self.batch_size * self.batch_writes
      + self.nosync_writes
  }

  /// 本档位的近似工作量（字节），用于折算默认超时
  pub fn workload_bytes(&self) -> u64 {
    let pair = (self.key_size + self.value_size) as u64;
    let write_units = self.loaded_elements() as u64 + self.sorted_elements as u64;
    let read_units = (self.num_reads
      + self.num_scans * self.scan_len
      + self
        .thread_counts
        .iter()
        .map(|t| self.loaded_elements() / t * t)
        .sum::<usize>()) as u64;
    let mutate_units = (self.loaded_elements() as u64 / 2)
      * 4 // removals / retain / extract_if / pop 四段同量级
      ;
    (write_units + read_units + mutate_units) * pair
  }

  /// 默认单引擎超时：以 redb 标准档 1 小时为基线按字节量折算，下限 15 分钟
  pub fn derived_timeout_secs(&self) -> u64 {
    let bytes = self.workload_bytes();
    let scaled = 3600.0 * (bytes as f64 / BASELINE_BYTES as f64);
    (scaled.max(900.0)) as u64
  }
}
