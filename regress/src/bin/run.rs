use std::{
  collections::BTreeMap,
  fs,
  hint::black_box,
  process::Command,
  time::{Duration, Instant},
};

use aok::OK;
use fastrand::Rng;
use regress::harness::{
  DEFAULT_BULK_ELEMENTS, DEFAULT_KEY_SIZE, DEFAULT_NUM_READS, DEFAULT_NUM_SCANS,
  DEFAULT_READ_ITERATIONS, DEFAULT_REMOVALS, DEFAULT_RNG_SEED, DEFAULT_SCAN_ITERATIONS,
  DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, WbftreeHarness, WkvHarness, fill_pair,
};
use sonic_rs::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetricValue {
  pub ops: f64,
  pub latency_us: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommitRecord {
  pub commit: String,
  pub message: String,
  pub date: String,
  pub author: String,
  pub metrics: BTreeMap<String, MetricValue>,
}

fn get_git_info() -> (String, String, String, String) {
  let out = Command::new("git")
    .args([
      "log",
      "-1",
      "--date=format:%Y-%m-%d %H:%M:%S",
      "--pretty=format:%H%x00%s%x00%cd%x00%an",
    ])
    .output()
    .ok();

  if let Some(out) = out
    && let Ok(text) = String::from_utf8(out.stdout)
  {
    let parts: Vec<&str> = text.split('\0').collect();
    if parts.len() >= 4 {
      let commit = parts[0].get(..8).unwrap_or(parts[0]).to_string();
      let message = parts[1].to_string();
      let date = parts[2].to_string();
      let author = parts[3].to_string();
      return (commit, message, date, author);
    }
  }

  let commit = Command::new("git")
    .args(["rev-parse", "--short=8", "HEAD"])
    .output()
    .ok()
    .and_then(|o| String::from_utf8(o.stdout).ok())
    .map(|s| s.trim().to_string())
    .unwrap_or_else(|| "unknown".into());

  (commit, String::new(), String::new(), String::new())
}

#[inline]
fn median_duration(durations: &mut [Duration]) -> Duration {
  durations.sort_unstable();
  durations[durations.len() / 2]
}

fn main() -> aok::Result<()> {
  let (commit, message, date, author) = get_git_info();
  let mut metrics = BTreeMap::new();

  let mut key = vec![0u8; DEFAULT_KEY_SIZE];
  let mut val = vec![0u8; DEFAULT_VALUE_SIZE];
  let mut buf = [0u8; 512];

  // 1. 评估 wkv (配置完全对标 bench: 128MB 缓存, 64MB 段大小, 600万条/约1.04GB数据)
  let wkv = WkvHarness::default_bench(DEFAULT_BULK_ELEMENTS)?;

  // wkv_upsert (Bulk 写入 6,000,000 项)
  {
    let mut rng = Rng::with_seed(DEFAULT_RNG_SEED);
    let start = Instant::now();
    for _ in 0..DEFAULT_BULK_ELEMENTS {
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = wkv.upsert_sync(&key, &val);
    }
    let duration = start.elapsed();
    let secs = duration.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_BULK_ELEMENTS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_BULK_ELEMENTS as f64);
    metrics.insert("wkv_upsert".into(), MetricValue { ops, latency_us });
  }

  // wkv_get_hot (对标 bench: 100,000 项随机点查, 3 次采样取中位数)
  {
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let mut read_rng = Rng::with_seed(DEFAULT_RNG_SEED);
      let start = Instant::now();
      for _ in 0..DEFAULT_NUM_READS {
        fill_pair(&mut read_rng, &mut key, &mut val);
        let len = wkv.get_sync(&key, &mut buf);
        black_box(len);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_NUM_READS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_NUM_READS as f64);
    metrics.insert("wkv_get_hot".into(), MetricValue { ops, latency_us });
  }

  // wkv_delete (对标 bench: 50,000 项删除)
  {
    let mut del_rng = Rng::with_seed(DEFAULT_RNG_SEED);
    let start = Instant::now();
    for _ in 0..DEFAULT_REMOVALS {
      fill_pair(&mut del_rng, &mut key, &mut val);
      let _ = wkv.delete_sync(&key);
    }
    let duration = start.elapsed();
    let secs = duration.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_REMOVALS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_REMOVALS as f64);
    metrics.insert("wkv_delete".into(), MetricValue { ops, latency_us });
  }

  // 2. 评估 wbftree (配置完全对标 bench: 128MB 缓存, 4 线程并发, 600万条/约1.04GB数据)
  let wbftree = WbftreeHarness::default_bench()?;

  // wbftree_insert (Bulk 写入 6,000,000 项)
  {
    let mut rng = Rng::with_seed(DEFAULT_RNG_SEED);
    let start = Instant::now();
    for _ in 0..DEFAULT_BULK_ELEMENTS {
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = wbftree.insert(&key, &val);
    }
    let duration = start.elapsed();
    let secs = duration.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_BULK_ELEMENTS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_BULK_ELEMENTS as f64);
    metrics.insert("wbftree_insert".into(), MetricValue { ops, latency_us });
  }

  // wbftree_read (对标 bench: 100,000 项随机点查, 3 次采样取中位数)
  {
    // 读缓冲对齐引擎直读门槛 cb_max_record_size(4096)：对标 C# BfTreeService.ReadByPtr
    // 的 stackalloc 4096 中转口径，走 read_into 直读快路径（512 会触发双拷贝中转）
    let mut wbf_buf = [0u8; 4096];
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let mut read_rng = Rng::with_seed(DEFAULT_RNG_SEED);
      let start = Instant::now();
      for _ in 0..DEFAULT_NUM_READS {
        fill_pair(&mut read_rng, &mut key, &mut val);
        let res = wbftree.read(&key, &mut wbf_buf);
        black_box(res);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_NUM_READS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_NUM_READS as f64);
    metrics.insert("wbftree_read".into(), MetricValue { ops, latency_us });
  }

  // wbftree_scan_10 (对标 bench: 5,000 项范围扫描, 步长 10, 3 次采样取中位数)
  {
    let mut durations = vec![Duration::ZERO; DEFAULT_SCAN_ITERATIONS];
    for d in &mut durations {
      let mut scan_rng = Rng::with_seed(DEFAULT_RNG_SEED);
      let start = Instant::now();
      for _ in 0..DEFAULT_NUM_SCANS {
        fill_pair(&mut scan_rng, &mut key, &mut val);
        let scanned = wbftree.scan(&key, DEFAULT_SCAN_LEN);
        black_box(scanned);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_NUM_SCANS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_NUM_SCANS as f64);
    metrics.insert("wbftree_scan_10".into(), MetricValue { ops, latency_us });
  }

  // wbftree_delete (对标 bench: 50,000 项删除)
  {
    let mut del_rng = Rng::with_seed(DEFAULT_REMOVALS as u64);
    let start = Instant::now();
    for _ in 0..DEFAULT_REMOVALS {
      fill_pair(&mut del_rng, &mut key, &mut val);
      let _ = wbftree.delete(&key);
    }
    let duration = start.elapsed();
    let secs = duration.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_REMOVALS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_REMOVALS as f64);
    metrics.insert("wbftree_delete".into(), MetricValue { ops, latency_us });
  }

  let record = CommitRecord {
    commit,
    message,
    date,
    author,
    metrics,
  };

  let json = sonic_rs::to_string_pretty(&record)?;
  let _ = fs::create_dir_all("data");
  let _ = fs::write("data/latest.json", &json);
  println!("{json}");
  OK
}
