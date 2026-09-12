use std::time::Duration;

use crate::sys_info::MachineInfo;

/// 评测参数配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BenchmarkConfig {
  pub key_size: usize,
  pub value_size: usize,
  pub cache_size: usize,
  pub bulk_elements: usize,
  pub individual_writes: usize,
  pub batch_writes: usize,
  pub batch_size: usize,
  pub nosync_writes: usize,
  pub num_reads: usize,
  pub read_iterations: usize,
  pub num_scans: usize,
  pub scan_len: usize,
  pub scan_iterations: usize,
  pub removals: usize,
  pub rng_seed: u64,
}

/// 统一内存预算管理与分配
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct MemoryBudget {
  /// 总分配内存字节数
  pub total_bytes: usize,
  /// 读缓存 (Block Cache / Buffer Pool) 预算 (占 60%)
  pub read_cache_bytes: usize,
  /// 写缓冲 (MemTable / WriteBuffer) 预算 (占 40%)
  pub write_buffer_bytes: usize,
  /// 单个 MemTable 字节数
  pub memtable_size: usize,
  /// 最大 MemTable 数量
  pub max_memtable_count: usize,
}

impl MemoryBudget {
  /// 从总内存预算构建细分配置：
  /// - 读缓存占比 60%
  /// - 写缓冲占比 40%
  /// - 单个 memtable 限制在 write_buffer_bytes / 2，保证至少容纳 2 个 memtable 且能及时触发 flush
  pub fn new(total_bytes: usize) -> Self {
    let read_cache_bytes = (total_bytes * 6) / 10;
    let write_buffer_bytes = total_bytes.saturating_sub(read_cache_bytes);
    let memtable_size = (write_buffer_bytes / 2).max(64 * 1024);
    let max_memtable_count = (write_buffer_bytes / memtable_size).clamp(2, 4);

    Self {
      total_bytes,
      read_cache_bytes,
      write_buffer_bytes,
      memtable_size,
      max_memtable_count,
    }
  }
}

impl Default for BenchmarkConfig {
  fn default() -> Self {
    Self {
      key_size: 24,
      value_size: 150,
      cache_size: 128 * 1024 * 1024, // 128MB 内存缓存
      bulk_elements: 6_000_000,      // 600万条数据 (~1.04GB 原始写入，磁盘约 1.2~1.5GB)
      individual_writes: 1_000,
      batch_writes: 100,
      batch_size: 1_000,
      nosync_writes: 50_000,
      num_reads: 100_000,
      read_iterations: 3,
      num_scans: 5_000,
      scan_len: 10,
      scan_iterations: 3,
      removals: 50_000,
      rng_seed: 3,
    }
  }
}

impl BenchmarkConfig {
  /// 快速调试模式（减少数据规模，保证磁盘内容大于内存 16 倍）
  pub fn quick() -> Self {
    Self {
      key_size: 24,
      value_size: 150,
      cache_size: 4 * 1024 * 1024, // 4MB 缓存
      bulk_elements: 380_000,      // ~66MB 原始数据，磁盘约 75~90MB，为缓存 18~22 倍
      individual_writes: 100,
      batch_writes: 10,
      batch_size: 100,
      nosync_writes: 5_000,
      num_reads: 10_000,
      read_iterations: 3,
      num_scans: 1_000,
      scan_len: 10,
      scan_iterations: 3,
      removals: 10_000,
      rng_seed: 3,
    }
  }

  /// 标准对齐 redb 500万条规模配置
  pub fn standard_5m() -> Self {
    Self {
      key_size: 24,
      value_size: 150,
      cache_size: 48 * 1024 * 1024, // 48MB 缓存
      bulk_elements: 5_000_000,     // 500万条 (~870MB 原始数据，磁盘约 1GB+，为缓存 20+ 倍)
      individual_writes: 1_000,
      batch_writes: 100,
      batch_size: 1_000,
      nosync_writes: 50_000,
      num_reads: 1_000_000,
      read_iterations: 3,
      num_scans: 500_000,
      scan_len: 10,
      scan_iterations: 3,
      removals: 2_500_000,
      rng_seed: 3,
    }
  }
}

/// 评测单项产出数据
#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ResultType {
  /// 实际磁盘数据吞吐率统计 (总传输字节数, 耗时)
  Throughput { bytes: u64, duration: Duration },
  /// 单次操作或阶段耗时
  Latency(Duration),
  /// 存储物理字节大小
  SizeInBytes(u64),
  /// 不支持/不适用
  NA,
}

impl ResultType {
  pub fn throughput(bytes: u64, duration: Duration) -> Self {
    Self::Throughput { bytes, duration }
  }

  pub fn rate(&self) -> f64 {
    match self {
      Self::Throughput { bytes, duration } => {
        let secs = duration.as_secs_f64();
        if secs > 0.0 {
          *bytes as f64 / secs
        } else {
          0.0
        }
      }
      _ => 0.0,
    }
  }

  pub fn duration(&self) -> Option<Duration> {
    match self {
      Self::Throughput { duration, .. } | Self::Latency(duration) => Some(*duration),
      _ => None,
    }
  }

  /// 比较两个指标谁更优（吞吐越大越好；延迟/耗时越小越好；磁盘占用越小越好）
  pub fn is_better_than(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Throughput { .. }, Self::Throughput { .. }) => self.rate() > other.rate(),
      (Self::Latency(a), Self::Latency(b)) => a < b,
      (Self::SizeInBytes(a), Self::SizeInBytes(b)) => a < b,
      _ => false,
    }
  }

  /// 格式化为输出字符串（如 1.25 G/s, 506 M/s, 1.25 GiB）
  pub fn format_value(&self) -> String {
    match self {
      Self::NA => "N/A".to_string(),
      Self::Throughput { .. } => format_disk_throughput(self.rate()),
      Self::Latency(d) => {
        let micros = d.as_micros();
        if micros < 1000 {
          format!("{micros}µs")
        } else {
          let millis = (d.as_nanos() + 500_000) / 1_000_000;
          format!("{millis}ms")
        }
      }
      Self::SizeInBytes(bytes) => format_bytes(*bytes),
    }
  }

  /// 控制台打印时的带耗时详细字符串
  pub fn with_rate_detail(&self) -> String {
    match self {
      Self::Throughput { duration, .. } => {
        let millis = (duration.as_nanos() + 500_000) / 1_000_000;
        let rate_str = format_disk_throughput(self.rate());
        format!("{rate_str} ({millis}ms)")
      }
      _ => self.format_value(),
    }
  }
}

/// 格式化实际磁盘吞吐率 (B/s, K/s, M/s, G/s)
pub fn format_disk_throughput(bytes_per_sec: f64) -> String {
  const UNITS: [&str; 4] = ["B/s", "K/s", "M/s", "G/s"];
  let mut val = bytes_per_sec;
  let mut unit_idx = 0;
  while val >= 1024.0 && unit_idx + 1 < UNITS.len() {
    val /= 1024.0;
    unit_idx += 1;
  }
  let precision = if val < 10.0 {
    2
  } else if val < 100.0 {
    1
  } else {
    0
  };
  let unit = UNITS[unit_idx];
  format!("{val:.precision$} {unit}")
}

/// 格式化字节大小为合适单位（B, KiB, MiB, GiB）
pub fn format_bytes(bytes: u64) -> String {
  const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
  let mut size = bytes as f64;
  let mut unit_idx = 0;
  while size >= 1024.0 && unit_idx + 1 < UNITS.len() {
    size /= 1024.0;
    unit_idx += 1;
  }
  if unit_idx == 0 {
    format!("{bytes} B")
  } else {
    format!("{size:.2} {}", UNITS[unit_idx])
  }
}

/// JSON 导出单项指标结构
#[derive(Clone, Debug, serde::Serialize)]
pub struct JsonMetric {
  pub key: String,
  pub r#type: &'static str,
  pub formatted: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub bytes: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub duration_ms: Option<f64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub rate: Option<f64>,
}

impl JsonMetric {
  pub fn from_result(key: &str, res: &ResultType) -> Self {
    match res {
      ResultType::Throughput { bytes, duration } => {
        let rate = res.rate();
        Self {
          key: key.to_string(),
          r#type: "throughput",
          formatted: res.format_value(),
          bytes: Some(*bytes),
          duration_ms: Some(duration.as_secs_f64() * 1000.0),
          rate: Some(rate),
        }
      }
      ResultType::Latency(d) => Self {
        key: key.to_string(),
        r#type: "latency",
        formatted: res.format_value(),
        bytes: None,
        duration_ms: Some(d.as_secs_f64() * 1000.0),
        rate: None,
      },
      ResultType::SizeInBytes(bytes) => Self {
        key: key.to_string(),
        r#type: "size",
        formatted: res.format_value(),
        bytes: Some(*bytes),
        duration_ms: None,
        rate: None,
      },
      ResultType::NA => Self {
        key: key.to_string(),
        r#type: "na",
        formatted: "N/A".to_string(),
        bytes: None,
        duration_ms: None,
        rate: None,
      },
    }
  }
}

/// JSON 导出引擎评测结果
#[derive(Clone, Debug, serde::Serialize)]
pub struct JsonEngineResult {
  pub name: String,
  pub url: String,
  pub metrics: Vec<JsonMetric>,
}

/// JSON 导出完整评测数据集
#[derive(Clone, Debug, serde::Serialize)]
pub struct JsonBenchmarkData {
  pub machine: MachineInfo,
  pub config: BenchmarkConfig,
  pub engines: Vec<JsonEngineResult>,
}
