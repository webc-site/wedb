//! 机读结果侧车：与 markdown 表同源，供 CI 汇总、历史累积与网站渲染。
//!
//! 往返无损：`ResultType` 的全部信息（kind / unit / count / duration_ns / bytes）
//! 都写进 JSON，合并后的报表与单进程直出的表逐字节一致。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::{
  config::Workload,
  machine::MachineInfo,
  result::{ResultType, ThroughputUnit, metric_key},
};

pub const JSON_SCHEMA: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRow {
  /// 稳定机读键，如 `bulk_load`、`random_reads_4_threads`
  pub key: String,
  /// 表内行名，与 redb 逐字一致
  pub name: String,
  /// throughput | latency | size | na
  pub kind: String,
  pub unit: Option<String>,
  pub count: Option<u64>,
  pub duration_ns: Option<u128>,
  pub bytes: Option<u64>,
  /// 表格里的显示文本（不含加粗）
  pub formatted: String,
  /// 每秒速率，仅吞吐行有
  pub rate: Option<f64>,
  /// 同平台同行内的最优（含并列）
  #[serde(default)]
  pub winner: bool,
}

impl JsonRow {
  pub fn from_result(name: &str, result: &ResultType) -> Self {
    let (count, duration_ns, bytes, rate) = match result {
      ResultType::Throughput {
        count,
        duration,
        unit: _,
      } => (
        Some(*count),
        Some(duration.as_nanos()),
        None,
        Some(result.rate()),
      ),
      ResultType::Latency(d) => (None, Some(d.as_nanos()), None, None),
      ResultType::SizeInBytes(b) => (None, None, Some(*b), None),
      ResultType::NA => (None, None, None, None),
    };

    Self {
      key: metric_key(name),
      name: name.to_string(),
      kind: result.kind().to_string(),
      unit: match result {
        ResultType::Throughput { unit, .. } => Some(unit.abbreviation().to_string()),
        _ => None,
      },
      count,
      duration_ns,
      bytes,
      formatted: result.to_string(),
      rate,
      winner: false,
    }
  }

  /// 无损还原表格渲染所需的结果值
  pub fn to_result(&self) -> ResultType {
    match self.kind.as_str() {
      "throughput" => {
        let duration = Duration::from_nanos(self.duration_ns.unwrap_or(0) as u64);
        let unit = match self.unit.as_deref().unwrap_or("key/s") {
          "txn/s" => ThroughputUnit::Transaction,
          "scan/s" => ThroughputUnit::Scan,
          _ => ThroughputUnit::Key,
        };
        ResultType::Throughput {
          count: self.count.unwrap_or(0),
          duration,
          unit,
        }
      }
      "latency" => ResultType::Latency(Duration::from_nanos(self.duration_ns.unwrap_or(0) as u64)),
      "size" => ResultType::SizeInBytes(self.bytes.unwrap_or(0)),
      _ => ResultType::NA,
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonEngine {
  /// 表列名：hash / bftree / fjall / rocksdb / sqlite
  pub name: String,
  /// 被测实现位置或上游主页
  pub source: String,
  /// ok | crashed | timeout | skipped
  pub status: String,
  /// status 非 ok 时的说明（退出码、超时预算等）
  pub detail: Option<String>,
  /// 该引擎峰值常驻内存（字节），表外信息
  pub peak_memory_bytes: Option<u64>,
  pub rows: Vec<JsonRow>,
}

/// 一个平台一次评测的完整记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRun {
  pub schema: u32,
  pub generated_at_unix: u64,
  pub commit: String,
  pub branch: String,
  pub platform: String,
  pub machine: MachineInfo,
  pub workload: Workload,
  /// 呈现口径说明，如「缓存已按物理内存收敛到 3.2 GiB」
  pub notes: Vec<String>,
  pub engines: Vec<JsonEngine>,
}

impl JsonRun {
  pub fn unix_now() -> u64 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_secs())
      .unwrap_or(0)
  }

  /// 同平台每行标出最优（并列全标），规则与表格一致
  pub fn mark_winners(&mut self) {
    let Some(first) = self.engines.first() else {
      return;
    };
    let row_count = first.rows.len();
    if self.engines.len() < 2 {
      return;
    }

    for i in 0..row_count {
      let row_results: Vec<ResultType> =
        self.engines.iter().map(|e| e.rows[i].to_result()).collect();
      let mut best: Option<usize> = None;
      for (j, result) in row_results.iter().enumerate() {
        if matches!(result, ResultType::NA) {
          continue;
        }
        if best.is_none_or(|previous| result.is_better_than(&row_results[previous])) {
          best = Some(j);
        }
      }
      if let Some(winner_index) = best {
        let winner_text = row_results[winner_index].to_string();
        for engine in self.engines.iter_mut() {
          if engine.rows[i].formatted == winner_text {
            engine.rows[i].winner = true;
          }
        }
      }
    }
  }

  /// 还原成 `print_results_table` 的入参形态
  pub fn to_table_results(&self) -> Vec<(String, Vec<(String, ResultType)>)> {
    self
      .engines
      .iter()
      .map(|engine| {
        (
          engine.name.clone(),
          engine
            .rows
            .iter()
            .map(|row| (row.name.clone(), row.to_result()))
            .collect(),
        )
      })
      .collect()
  }
}

/// 跨平台合并后的报表（CI report 任务与网站数据的载体）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonReport {
  pub schema: u32,
  pub generated_at_unix: u64,
  pub runs: Vec<JsonRun>,
}

impl JsonReport {
  pub fn new(runs: Vec<JsonRun>) -> Self {
    Self {
      schema: JSON_SCHEMA,
      generated_at_unix: JsonRun::unix_now(),
      runs,
    }
  }
}

/// 跨提交累积的历史：网站趋势图的数据源，随站点静态资源发布
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonHistory {
  pub schema: u32,
  pub generated_at_unix: u64,
  /// 按时间升序，同一 commit 只保留最后写入的一次
  pub reports: Vec<JsonReport>,
}

impl JsonHistory {
  pub fn empty() -> Self {
    Self {
      schema: JSON_SCHEMA,
      generated_at_unix: JsonRun::unix_now(),
      reports: Vec::new(),
    }
  }
}
