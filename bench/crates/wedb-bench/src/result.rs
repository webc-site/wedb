//! 结果值与其呈现：单位、比较方向、以及 redb 一致的三档格式化。

use std::{fmt, time::Duration};

/// 一次吞吐测量的计数单位
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ThroughputUnit {
  /// 单个键值对的插入/读取/删除
  Key,
  /// 单次已提交的写事务
  Transaction,
  /// 单次范围读（覆盖若干键）
  Scan,
}

impl ThroughputUnit {
  pub fn abbreviation(self) -> &'static str {
    match self {
      ThroughputUnit::Key => "key/s",
      ThroughputUnit::Transaction => "txn/s",
      ThroughputUnit::Scan => "scan/s",
    }
  }

  /// JSON 侧机读单位名
  pub fn slug(self) -> &'static str {
    match self {
      ThroughputUnit::Key => "key_per_sec",
      ThroughputUnit::Transaction => "txn_per_sec",
      ThroughputUnit::Scan => "scan_per_sec",
    }
  }
}

#[derive(Copy, Clone, Debug)]
pub enum ResultType {
  /// `count` 个工作在 `duration` 内完成；按速率呈现，使工作量不同的段之间也能对比
  Throughput {
    count: u64,
    duration: Duration,
    unit: ThroughputUnit,
  },
  /// 单次调用而非循环的耗时
  Latency(Duration),
  SizeInBytes(u64),
  /// 引擎不支持该段，或该段在超时/崩溃中被跳过
  NA,
}

impl ResultType {
  pub fn keys(count: usize, duration: Duration) -> Self {
    ResultType::Throughput {
      count: count as u64,
      duration,
      unit: ThroughputUnit::Key,
    }
  }

  pub fn txns(count: usize, duration: Duration) -> Self {
    ResultType::Throughput {
      count: count as u64,
      duration,
      unit: ThroughputUnit::Transaction,
    }
  }

  pub fn scans(count: usize, duration: Duration) -> Self {
    ResultType::Throughput {
      count: count as u64,
      duration,
      unit: ThroughputUnit::Scan,
    }
  }

  pub fn rate(&self) -> f64 {
    match self {
      ResultType::Throughput {
        count, duration, ..
      } => {
        let secs = duration.as_secs_f64();
        if secs > 0.0 {
          *count as f64 / secs
        } else {
          0.0
        }
      }
      _ => 0.0,
    }
  }

  /// 行标题里补充的单位后缀；自带单位的值返回 None
  pub fn unit_label(&self) -> Option<&'static str> {
    match self {
      ResultType::Throughput { unit, .. } => Some(unit.abbreviation()),
      ResultType::Latency(_) | ResultType::SizeInBytes(_) | ResultType::NA => None,
    }
  }

  /// 吞吐越高越好，时延与占用越低越好；N/A 永不占优，同一行不混 kinds
  pub fn is_better_than(&self, other: &ResultType) -> bool {
    match (self, other) {
      (ResultType::Throughput { .. }, ResultType::Throughput { .. }) => self.rate() > other.rate(),
      (ResultType::Latency(a), ResultType::Latency(b)) => a < b,
      (ResultType::SizeInBytes(a), ResultType::SizeInBytes(b)) => a < b,
      _ => false,
    }
  }

  /// 与 `Display` 相同，但把单位写进文本——用于没有行标题的逐段进度输出
  pub fn with_unit(&self) -> String {
    match self {
      ResultType::Throughput { unit, .. } => format!("{self} {}", unit.abbreviation()),
      _ => self.to_string(),
    }
  }

  /// JSON 的 kind 字段
  pub fn kind(&self) -> &'static str {
    match self {
      ResultType::Throughput { .. } => "throughput",
      ResultType::Latency(_) => "latency",
      ResultType::SizeInBytes(_) => "size",
      ResultType::NA => "na",
    }
  }
}

impl fmt::Display for ResultType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    use byte_unit::{Byte, UnitType};

    match self {
      ResultType::NA => write!(f, "N/A"),
      ResultType::Throughput { .. } => write!(f, "{}", format_rate(self.rate())),
      ResultType::Latency(d) => write!(f, "{}", format_duration(*d)),
      ResultType::SizeInBytes(s) => {
        let b = Byte::from_u64(*s).get_appropriate_unit(UnitType::Binary);
        write!(f, "{b:.2}")
      }
    }
  }
}

/// 三位有效数字加 SI 后缀，如 "938"、"9.20K"、"293K"、"1.09M"，保持结果表窄到可读
pub fn format_rate(rate: f64) -> String {
  const SUFFIXES: [&str; 4] = ["", "K", "M", "G"];

  let mut value = rate;
  let mut suffix = 0;
  while value >= 1000.0 && suffix + 1 < SUFFIXES.len() {
    value /= 1000.0;
    suffix += 1;
  }
  let precision = if value < 10.0 {
    2
  } else if value < 100.0 {
    1
  } else {
    0
  };
  let suffix = SUFFIXES[suffix];

  format!("{value:.precision$}{suffix}")
}

/// 四舍五入到整毫秒
pub fn format_duration(duration: Duration) -> String {
  let millis = (duration.as_nanos() + 500_000) / 1_000_000;
  format!("{millis}ms")
}

/// 行名到稳定机读键：小写、非字母数字折成下划线
pub fn metric_key(name: &str) -> String {
  let mut key = String::with_capacity(name.len());
  let mut last_underscore = false;
  for ch in name.chars() {
    if ch.is_ascii_alphanumeric() {
      key.push(ch.to_ascii_lowercase());
      last_underscore = false;
    } else if !last_underscore {
      key.push('_');
      last_underscore = true;
    }
  }
  while key.ends_with('_') {
    key.pop();
  }
  key
}
