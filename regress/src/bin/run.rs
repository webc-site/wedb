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
  DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, READ_BUF_LEN, WbftreeHarness, WkvHarness, WorkloadParams,
  YcsbHarness, fill_pair,
};
use sonic_rs::{Deserialize, Serialize};

/// wbftree_scan_50 场景扫描长度（对齐 benches/regress.rs wbftree_scan_50）
const SCAN_50_LEN: usize = 50;

/// YCSB 混合工况键基数（对标 KV.benchmark 读写混合档，缩样以适配基线单轮时长）
const YCSB_KEY_COUNT: u64 = 100_000;
/// YCSB 预热窗操作数（丢弃，对标 KV.benchmark --warmup-sec）
const YCSB_WARMUP_OPS: u64 = 100_000;
/// YCSB 测量窗操作数（对标 KV.benchmark 30s run 窗，缩样为定长操作数）
const YCSB_MEAS_OPS: u64 = 300_000;

/// 每段最低限度抽检样本数（防写入/删除全失败仍产出虚高指标）
const SAMPLE_CHECKS: usize = 4;

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

/// 重放种子序列生成第 idx 个键值对（键生成串行依赖，须前缀重放）
#[inline]
fn fill_pair_at(idx: usize, key: &mut [u8], val: &mut [u8]) {
  let mut rng = Rng::with_seed(DEFAULT_RNG_SEED);
  for _ in 0..idx {
    fill_pair(&mut rng, key, val);
  }
  fill_pair(&mut rng, key, val);
}

/// 抽检断言：均匀取 SAMPLE_CHECKS 个样本，断言读回状态符合预期
fn assert_samples(
  total: usize,
  expect: Option<usize>,
  label: &str,
  key: &mut [u8],
  val: &mut [u8],
  buf: &mut [u8],
  read: impl Fn(&[u8], &mut [u8]) -> Option<usize>,
) {
  for i in 0..SAMPLE_CHECKS {
    let idx = i * total / SAMPLE_CHECKS;
    fill_pair_at(idx, key, val);
    assert_eq!(
      read(key, buf),
      expect,
      "{label} 抽检失败: 第 {idx} 键读回状态异常"
    );
  }
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
  // 抽检:均匀抽样断言写入真实落库
  assert_samples(
    DEFAULT_BULK_ELEMENTS,
    Some(DEFAULT_VALUE_SIZE),
    "wkv_upsert",
    &mut key,
    &mut val,
    &mut buf,
    |k, b| wkv.get_sync(k, b),
  );

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

  // wkv_delete (对标 bench: 50,000 项删除, 与 insert 同序列删前 5 万存在键)
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
  // 抽检:均匀抽样断言删除真实生效
  assert_samples(
    DEFAULT_REMOVALS,
    None,
    "wkv_delete",
    &mut key,
    &mut val,
    &mut buf,
    |k, b| wkv.get_sync(k, b),
  );

  // wkv_get_immutable (对标 bench: flush+封区后不可变区点查, 100,000 项, 3 次采样取中位数;
  // 生产 flush 后只读区稳态主工况)
  wkv.seal_immutable();
  {
    // 工况预校验:封区后未删键必须可读,杜绝基准测到空数据(对标 C# BfTreeOperations
    // GlobalSetup 的 Debug.Assert 预校验口径);读键跳过已删前段
    let mut probe_rng = Rng::with_seed(DEFAULT_RNG_SEED);
    for _ in 0..=DEFAULT_REMOVALS {
      fill_pair(&mut probe_rng, &mut key, &mut val);
    }
    assert_eq!(
      wkv.get_sync(&key, &mut buf),
      Some(DEFAULT_VALUE_SIZE),
      "wkv_get_immutable 工况失真: 封区后键不可读"
    );
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let mut read_rng = Rng::with_seed(DEFAULT_RNG_SEED);
      for _ in 0..DEFAULT_REMOVALS {
        fill_pair(&mut read_rng, &mut key, &mut val);
      }
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
    metrics.insert("wkv_get_immutable".into(), MetricValue { ops, latency_us });
  }

  // 2. YCSB 风格读写混合工况 (对标 C# KV.benchmark RUMD 混合热环 + zipf + warmup + validate;
  // wkv 无 RMW 原语, RUMD 折算为 RUD 读/写/删, rmw 并入写侧, 默认档 rmw=0 折算无损)
  {
    let mut ycsb = YcsbHarness::new(WorkloadParams::ycsb_a(YCSB_KEY_COUNT))?;
    ycsb.load();
    // 工况预校验: 装载后全量回读, 杜绝基线测到空/错数据 (对标 C# Validate 口径)
    let (mismatches, misses) = ycsb.validate();
    assert_eq!(
      (mismatches, misses),
      (0, 0),
      "wkv_ycsb_mixed 工况失真: 装载回读校验未过 (失配 {mismatches}/落空 {misses})"
    );
    // 预热窗丢弃后再计时 (对标 KV.benchmark --warmup-sec 结果剔除语义)
    ycsb.warmup(YCSB_WARMUP_OPS);
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      let cnt = ycsb.run_ops(YCSB_MEAS_OPS);
      black_box(cnt.reads + cnt.writes + cnt.deletes);
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let ops = (YCSB_MEAS_OPS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (YCSB_MEAS_OPS as f64);
    metrics.insert("wkv_ycsb_mixed".into(), MetricValue { ops, latency_us });
    // 运行后二次校验: 删除回插保证键常驻, 混合跑完仍须全命中
    let (m2, s2) = ycsb.validate();
    assert_eq!(
      (m2, s2),
      (0, 0),
      "wkv_ycsb_mixed 运行后回读失真 (失配 {m2}/落空 {s2})"
    );
  }

  // 3. 评估 wbftree (配置完全对标 bench: 128MB 缓存, 4 线程并发, 600万条/约1.04GB数据)
  let wbftree = WbftreeHarness::default_bench()?;
  // 读缓冲取 READ_BUF_LEN(cb_max_record_size=4097):引擎要求 out_buf 容量 >=
  // cb_max_record_size 才走 read_into 直读快路径,低于门槛(4096/512 均触发)
  // 走 fallback 双拷贝中转;对标 C# BfTreeService.ReadByPtr 的 stackalloc 4096 中转口径
  let mut wbf_buf = [0u8; READ_BUF_LEN];

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
  // 抽检:均匀抽样断言写入真实落库(顺带覆盖直读路径命中)
  assert_samples(
    DEFAULT_BULK_ELEMENTS,
    Some(DEFAULT_VALUE_SIZE),
    "wbftree_insert",
    &mut key,
    &mut val,
    &mut wbf_buf,
    |k, b| wbftree.read(k, b),
  );

  // wbftree_read (对标 bench: 100,000 项随机点查, 3 次采样取中位数)
  {
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

  // wbftree_scan_50 (对标 bench: 5,000 项范围扫描, 步长 50, 3 次采样取中位数)
  {
    let mut durations = vec![Duration::ZERO; DEFAULT_SCAN_ITERATIONS];
    for d in &mut durations {
      let mut scan_rng = Rng::with_seed(DEFAULT_RNG_SEED);
      let start = Instant::now();
      for _ in 0..DEFAULT_NUM_SCANS {
        fill_pair(&mut scan_rng, &mut key, &mut val);
        let scanned = wbftree.scan(&key, SCAN_50_LEN);
        black_box(scanned);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let ops = (DEFAULT_NUM_SCANS as f64) / secs;
    let latency_us = (secs * 1_000_000.0) / (DEFAULT_NUM_SCANS as f64);
    metrics.insert("wbftree_scan_50".into(), MetricValue { ops, latency_us });
  }

  // wbftree_delete (对标 bench: 50,000 项删除, 与 insert 同序列删前 5 万存在键)
  {
    let mut del_rng = Rng::with_seed(DEFAULT_RNG_SEED);
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
  // 抽检:均匀抽样断言删除真实生效
  assert_samples(
    DEFAULT_REMOVALS,
    None,
    "wbftree_delete",
    &mut key,
    &mut val,
    &mut wbf_buf,
    |k, b| wbftree.read(k, b),
  );

  // 4. RESP 端到端解析/编码微基准 (对标 C# Resp.benchmark 吞吐 + BDN/Parsing, wresp 关键协议路径)
  {
    use regress::harness::resp;
    const PIPE: usize = 128;
    const KEY_LEN: usize = 16;
    const VAL_LEN: usize = 32;
    // 工况预校验: 解析命令名/参数数、编码结构长度精确
    resp::validate(PIPE, KEY_LEN, VAL_LEN);
    let pipeline = resp::build_set_pipeline(PIPE, KEY_LEN, VAL_LEN);

    // resp_parse: 解析流水线 (以命令数计 ops, 3 次采样取中位数)
    let rounds = 5_000u64;
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      for _ in 0..rounds {
        let (cmds, bytes) = resp::parse_pipeline(&pipeline);
        black_box(cmds + bytes);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    let total_cmds = (rounds as usize * PIPE) as f64;
    metrics.insert(
      "resp_parse".into(),
      MetricValue {
        ops: total_cmds / secs,
        latency_us: (secs * 1_000_000.0) / total_cmds,
      },
    );

    // resp_encode: 编码 bulk 应答 (以应答条数计 ops)
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      for _ in 0..rounds {
        black_box(resp::encode_bulks(PIPE, VAL_LEN));
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    metrics.insert(
      "resp_encode".into(),
      MetricValue {
        ops: total_cmds / secs,
        latency_us: (secs * 1_000_000.0) / total_cmds,
      },
    );
  }

  // 5. wepoch/whlog 关键路径微基准 (对标 C# LightEpoch / AllocatorBase 环形页缓冲, 现仓零守护)
  {
    use regress::harness::micro::{EpochHarness, PageHarness};
    // wepoch 受保护域进出
    let epoch = EpochHarness::bench();
    epoch.validate();
    let eops = 3_000_000u64;
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      let mut acc = 0u64;
      for _ in 0..eops {
        acc += epoch.protect_once();
      }
      black_box(acc);
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    metrics.insert(
      "wepoch_protect".into(),
      MetricValue {
        ops: (eops as f64) / secs,
        latency_us: (secs * 1_000_000.0) / (eops as f64),
      },
    );

    // whlog 页装载回读
    let page = PageHarness::bench()?;
    let page_data = vec![b'p'; page.page_size];
    page.validate_roundtrip(0);
    let pops = 200_000u64;
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      for i in 0..pops {
        page.load(i % page.num_pages as u64, &page_data);
        black_box(page.read_head(i % page.num_pages as u64));
      }
      *d = start.elapsed();
    }
    // 装载后二次校验, 杜绝测到脏页
    page.validate_roundtrip(0);
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    metrics.insert(
      "whlog_page_load".into(),
      MetricValue {
        ops: (pops as f64) / secs,
        latency_us: (secs * 1_000_000.0) / (pops as f64),
      },
    );
  }

  // 6. wdev 设备层直读写吞吐 (对标 C# Device.benchmark/BenchWorker.cs, 全引擎物理 I/O 底座)
  {
    use regress::harness::wdev::WdevHarness;
    const WDEV_SLOTS: u64 = 1_024;
    let dev = WdevHarness::bench()?;
    // 工况预校验: 直写后回读逐字节命中、传输计数写满整页
    dev.validate();
    let wops = 20_000u64;
    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      for i in 0..wops {
        black_box(dev.write_page(i % WDEV_SLOTS));
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    metrics.insert(
      "wdev_write_page".into(),
      MetricValue {
        ops: (wops as f64) / secs,
        latency_us: (secs * 1_000_000.0) / (wops as f64),
      },
    );

    let mut durations = vec![Duration::ZERO; DEFAULT_READ_ITERATIONS];
    for d in &mut durations {
      let start = Instant::now();
      for i in 0..wops {
        black_box(dev.read_page(i % WDEV_SLOTS));
      }
      *d = start.elapsed();
    }
    // 读侧二次校验由 validate 覆盖 (写入页回读一致), 此处仅记录吞吐
    let median = median_duration(&mut durations);
    let secs = median.as_secs_f64().max(0.000_000_001);
    metrics.insert(
      "wdev_read_page".into(),
      MetricValue {
        ops: (wops as f64) / secs,
        latency_us: (secs * 1_000_000.0) / (wops as f64),
      },
    );
  }

  let record = CommitRecord {
    commit,
    message,
    date,
    author,
    metrics,
  };

  let json = sonic_rs::to_string_pretty(&record)?;
  // 编译期锚定 regress crate 根,写侧与 report.js 读侧(import.meta.dirname)同锚,不随 cwd 漂移
  fs::create_dir_all(concat!(env!("CARGO_MANIFEST_DIR"), "/data"))?;
  fs::write(
    concat!(env!("CARGO_MANIFEST_DIR"), "/data/latest.json"),
    &json,
  )?;
  println!("{json}");
  OK
}
