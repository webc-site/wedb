//! range 段每记录成本探针：复刻 harness 第 4-6 段装载序（bulk sync →
//! individual sync → batch sync → nosync）后跑 harness 第 7 段同形的 range
//! 消费环（make_rng 同种子键序列、PairBufs fill 同序、scan_len 步进）。
//!
//! 用途：range 游标驱动形态的 A/B 归因（WEDB_RANGE_BATCH=0 逐条 next_ref
//! 对 K>0 批量 for_each_ref refill）——不进对基口径。
//! `--elements 5000000 --scans 500000` 即 1.0 档 range 段量级。

use std::{
  env,
  fs::create_dir_all,
  path::PathBuf,
  time::{Duration, Instant},
};

use fastrand::Rng;
use wedb_bench::{Workload, config::REDB_CACHE_SIZE, engines::hash_engine::HashEngine, traits::*};

struct PairBufs {
  key: Vec<u8>,
  value: Vec<u8>,
}

impl PairBufs {
  fn new(w: &Workload) -> Self {
    Self {
      key: vec![0u8; w.key_size],
      value: vec![0u8; w.value_size],
    }
  }

  /// harness PairBufs 同序：先 fill(key) 再 fill(value)
  #[inline]
  fn fill(&mut self, rng: &mut Rng) {
    rng.fill(&mut self.key);
    rng.fill(&mut self.value);
  }
}

fn main() {
  let mut elements = 5_000_000usize;
  let mut scans = 500_000usize;
  let mut data_path = PathBuf::from("/tmp/range_probe_data");
  let mut cache = REDB_CACHE_SIZE;
  let mut rounds = 3usize;
  let mut args = env::args().skip(1);
  while let Some(arg) = args.next() {
    match arg.as_str() {
      "--elements" => elements = args.next().unwrap().parse().unwrap(),
      "--scans" => scans = args.next().unwrap().parse().unwrap(),
      "--data-path" => data_path = PathBuf::from(args.next().unwrap()),
      "--cache-mb" => cache = args.next().unwrap().parse::<usize>().unwrap() * 1024 * 1024,
      "--rounds" => rounds = args.next().unwrap().parse().unwrap(),
      other => panic!("未知参数 {other}"),
    }
  }
  create_dir_all(&data_path).unwrap();

  let mut w = Workload::redb_standard();
  w.bulk_elements = elements;
  w.num_scans = scans;
  w.cache_size = cache;

  let db = HashEngine::open(&data_path, &w).expect("打开引擎失败");

  // harness 装载序复刻：bulk sync → individual sync → batch sync → nosync
  let t = "batch=K10(固化形态)".to_string();
  let mut conn = db.connect();
  let mut rng = Rng::with_seed(w.rng_seed);
  let mut kb = vec![0u8; w.key_size];
  let mut vb = vec![0u8; w.value_size];

  let s = Instant::now();
  {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      for _ in 0..w.bulk_elements {
        rng.fill(&mut kb);
        rng.fill(&mut vb);
        ins.insert(&kb, &vb).unwrap();
      }
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] bulk {} in {}ms",
    w.bulk_elements,
    s.elapsed().as_millis()
  );

  let s = Instant::now();
  for _ in 0..w.individual_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      ins.insert(&kb, &vb).unwrap();
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] individual {} in {}ms",
    w.individual_writes,
    s.elapsed().as_millis()
  );

  let s = Instant::now();
  for _ in 0..w.batch_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      for _ in 0..w.batch_size {
        rng.fill(&mut kb);
        rng.fill(&mut vb);
        ins.insert(&kb, &vb).unwrap();
      }
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] batch {}x{} in {}ms",
    w.batch_writes,
    w.batch_size,
    s.elapsed().as_millis()
  );

  conn.set_sync(false);
  let s = Instant::now();
  for _ in 0..w.nosync_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      ins.insert(&kb, &vb).unwrap();
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] nosync {} in {}ms",
    w.nosync_writes,
    s.elapsed().as_millis()
  );

  // harness 第 7 段同形 range 消费环 × rounds 轮取中位
  let mut durations = vec![Duration::ZERO; rounds];
  for (i, d) in durations.iter_mut().enumerate() {
    let mut rng = Rng::with_seed(w.rng_seed);
    let txn = conn.read_transaction();
    let mut reader = txn.get_reader();
    let mut bufs = PairBufs::new(&w);
    let start = Instant::now();
    let mut value_sum = 0u64;
    let mut steps = 0u64;
    for _ in 0..w.num_scans {
      bufs.fill(&mut rng);
      let mut iter = reader.range_from(&bufs.key);
      for _ in 0..w.scan_len {
        if let Some((_, value)) = iter.next() {
          value_sum += value[0] as u64;
          steps += 1;
        } else {
          break;
        }
      }
    }
    assert!(value_sum > 0);
    *d = start.elapsed();
    println!(
      "[{t}] range round {}：{} scans x {} in {}ms ({:.2}M scan/s, steps={steps})",
      i + 1,
      w.num_scans,
      w.scan_len,
      d.as_millis(),
      w.num_scans as f64 / d.as_secs_f64() / 1e6,
    );
  }
  durations.sort();
  let med = durations[rounds / 2];
  println!(
    "[{t}] median: {} scans in {}ms ({:.2}M scan/s)",
    w.num_scans,
    med.as_millis(),
    w.num_scans as f64 / med.as_secs_f64() / 1e6,
  );
}
