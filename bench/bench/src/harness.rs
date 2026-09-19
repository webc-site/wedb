use std::{
  fs,
  path::Path,
  sync::Barrier,
  thread,
  time::{Duration, Instant},
};

use walkdir::WalkDir;

use crate::{
  alloc::GLOBAL_ALLOC,
  sys_info::get_process_physical_memory,
  traits::*,
  types::{BenchmarkConfig, ResultType},
};

/// 填充可复现确定性随机键值对（零分配复用缓冲区）
#[inline]
pub fn fill_pair(rng: &mut fastrand::Rng, key: &mut [u8], val: &mut [u8]) {
  rng.fill(key);
  rng.fill(val);
}

/// 计算多个耗时采样的中位数
pub fn median_duration(durations: &mut [Duration]) -> Duration {
  durations.sort_unstable();
  durations[durations.len() / 2]
}

/// 递归统计数据库文件或目录的物理总字节数
pub fn database_size(path: &Path) -> u64 {
  if path.is_file() {
    return fs::metadata(path).map(|m| m.len()).unwrap_or(0);
  }
  WalkDir::new(path)
    .into_iter()
    .filter_map(|e| e.ok())
    .filter(|e| e.file_type().is_file())
    .filter_map(|e| e.metadata().ok())
    .map(|m| m.len())
    .sum()
}

/// 为多线程测试生成分片 RNG，保持整体序列与单线程读取一致
fn make_rng_shards(
  shards: usize,
  elements: usize,
  seed: u64,
  key_size: usize,
  val_size: usize,
) -> Vec<fastrand::Rng> {
  let elements_per_shard = elements / shards;
  let mut rngs = Vec::with_capacity(shards);
  let mut key_buf = vec![0u8; key_size];
  let mut val_buf = vec![0u8; val_size];
  let mut rng = fastrand::Rng::with_seed(seed);
  for i in 0..shards {
    if i > 0 {
      for _ in 0..elements_per_shard {
        fill_pair(&mut rng, &mut key_buf, &mut val_buf);
      }
    }
    rngs.push(rng.clone());
  }
  rngs
}

/// 统一 benchmark 驱动执行器
pub fn run_benchmark<T: BenchDatabase>(
  mut db: T,
  path: &Path,
  cfg: &BenchmarkConfig,
) -> Vec<(String, ResultType)> {
  let mut results = Vec::new();
  let db_name = T::name();

  let mut conn = db.connect();
  let item_bytes = (cfg.key_size + cfg.value_size) as u64;
  let mut rng = fastrand::Rng::with_seed(cfg.rng_seed);
  let mut key = vec![0u8; cfg.key_size];
  let mut val = vec![0u8; cfg.value_size];

  // 1. Bulk Load (大批量导入)
  let start = Instant::now();
  {
    let mut txn = conn.write_transaction();
    for _ in 0..cfg.bulk_elements {
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = txn.insert(&key, &val);
    }
    let _ = txn.commit();
  }
  let duration = start.elapsed();
  let bytes = cfg.bulk_elements as u64 * item_bytes;
  let res = ResultType::throughput(bytes, duration);
  println!(
    "{db_name}: bulk load {} 项，吞吐 {}",
    cfg.bulk_elements,
    res.with_rate_detail()
  );
  results.push(("bulk_load".to_string(), res));

  // 2. Individual Writes (单条写单独事务)
  let start = Instant::now();
  {
    for _ in 0..cfg.individual_writes {
      let mut txn = conn.write_transaction();
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = txn.insert(&key, &val);
      let _ = txn.commit();
    }
  }
  let duration = start.elapsed();
  let bytes = cfg.individual_writes as u64 * item_bytes;
  let res = ResultType::throughput(bytes, duration);
  println!(
    "{db_name}: individual writes {} 项，吞吐 {}",
    cfg.individual_writes,
    res.with_rate_detail()
  );
  results.push(("individual_writes".to_string(), res));

  // 3. Batch Writes (小批次批量写)
  let start = Instant::now();
  {
    for _ in 0..cfg.batch_writes {
      let mut txn = conn.write_transaction();
      for _ in 0..cfg.batch_size {
        fill_pair(&mut rng, &mut key, &mut val);
        let _ = txn.insert(&key, &val);
      }
      let _ = txn.commit();
    }
  }
  let duration = start.elapsed();
  let total_batch_items = cfg.batch_writes * cfg.batch_size;
  let bytes = total_batch_items as u64 * item_bytes;
  let res = ResultType::throughput(bytes, duration);
  println!(
    "{db_name}: batch writes {} 项，吞吐 {}",
    total_batch_items,
    res.with_rate_detail()
  );
  results.push(("batch_writes".to_string(), res));

  // 4. Nosync Writes (无落盘异步写)
  if conn.set_sync(false) {
    let start = Instant::now();
    for _ in 0..cfg.nosync_writes {
      let mut txn = conn.write_transaction();
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = txn.insert(&key, &val);
      let _ = txn.commit();
    }
    let duration = start.elapsed();
    let bytes = cfg.nosync_writes as u64 * item_bytes;
    let res = ResultType::throughput(bytes, duration);
    println!(
      "{db_name}: nosync writes {} 项，吞吐 {}",
      cfg.nosync_writes,
      res.with_rate_detail()
    );
    results.push(("nosync_writes".to_string(), res));
    conn.set_sync(false);
  } else {
    // 引擎不支持设置 nosync，补齐写以对齐后续数量
    let mut txn = conn.write_transaction();
    for _ in 0..cfg.nosync_writes {
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = txn.insert(&key, &val);
    }
    let _ = txn.commit();
    results.push(("nosync_writes".to_string(), ResultType::NA));
  }

  // 5. len()
  {
    let start = Instant::now();
    let mut reader = conn.read_transaction();
    let count = reader.len();
    let duration = start.elapsed();
    let res = ResultType::Latency(duration);
    println!(
      "{db_name}: len() 统计结果为 {count}，耗时 {}",
      res.format_value()
    );
    results.push(("len".to_string(), res));
  }

  // 6. Random Reads (3次中位数)
  {
    let mut durations = vec![Duration::ZERO; cfg.read_iterations];
    for d in &mut durations {
      let mut read_rng = fastrand::Rng::with_seed(cfg.rng_seed);
      let mut reader = conn.read_transaction();
      let start = Instant::now();
      for _ in 0..cfg.num_reads {
        fill_pair(&mut read_rng, &mut key, &mut val);
        let _ = reader.get(&key);
      }
      *d = start.elapsed();
    }
    let median = median_duration(&mut durations);
    let bytes = cfg.num_reads as u64 * item_bytes;
    let res = ResultType::throughput(bytes, median);
    println!(
      "{db_name}: random reads {} 项 (中位数)，吞吐 {}",
      cfg.num_reads,
      res.with_rate_detail()
    );
    results.push(("random_reads".to_string(), res));
  }

  // 7. Random Range Reads (3次中位数)
  {
    let mut durations = vec![Duration::ZERO; cfg.scan_iterations];
    let mut supported = true;
    for d in &mut durations {
      let mut scan_rng = fastrand::Rng::with_seed(cfg.rng_seed);
      let mut reader = conn.read_transaction();
      let start = Instant::now();
      let mut total_scanned = 0;
      for _ in 0..cfg.num_scans {
        fill_pair(&mut scan_rng, &mut key, &mut val);
        let (scanned, _) = reader.range_scan(&key, cfg.scan_len);
        total_scanned += scanned;
      }
      if total_scanned == 0 {
        supported = false;
        break;
      }
      *d = start.elapsed();
    }

    if supported {
      let median = median_duration(&mut durations);
      let bytes = (cfg.num_scans * cfg.scan_len) as u64 * item_bytes;
      let res = ResultType::throughput(bytes, median);
      println!(
        "{db_name}: random range reads {} 次 (中位数)，吞吐 {}",
        cfg.num_scans,
        res.with_rate_detail()
      );
      results.push(("random_range_reads".to_string(), res));
    } else {
      results.push(("random_range_reads".to_string(), ResultType::NA));
    }
  }

  // 8. Multi-threaded Random Reads (4, 8, 16, 32 线程)
  let total_elements =
    cfg.bulk_elements + cfg.individual_writes + total_batch_items + cfg.nosync_writes;
  for threads in [4, 8, 16, 32] {
    let barrier = Barrier::new(threads);
    let mut rng_shards = make_rng_shards(
      threads,
      total_elements,
      cfg.rng_seed,
      cfg.key_size,
      cfg.value_size,
    );
    let items_per_thread = (cfg.num_reads / threads).max(100);
    let total_thread_reads = items_per_thread * threads;

    let start = Instant::now();
    thread::scope(|s| {
      for _ in 0..threads {
        let thread_conn = db.connect();
        let mut t_rng = rng_shards
          .pop()
          .unwrap_or_else(|| fastrand::Rng::with_seed(cfg.rng_seed));
        let k_size = cfg.key_size;
        let v_size = cfg.value_size;
        let barrier = &barrier;

        s.spawn(move || {
          barrier.wait();
          let mut reader = thread_conn.read_transaction();
          let mut thread_key = vec![0u8; k_size];
          let mut thread_val = vec![0u8; v_size];
          for _ in 0..items_per_thread {
            fill_pair(&mut t_rng, &mut thread_key, &mut thread_val);
            let _ = reader.get(&thread_key);
          }
        });
      }
    });
    let duration = start.elapsed();
    let bytes = total_thread_reads as u64 * item_bytes;
    let res = ResultType::throughput(bytes, duration);
    println!(
      "{db_name}: random reads ({threads} 线程) {} 项，吞吐 {}",
      total_thread_reads,
      res.with_rate_detail()
    );
    results.push((format!("random_reads_{threads}"), res));
  }

  // 9. Removals (删除数据)
  let start = Instant::now();
  {
    let mut del_rng = fastrand::Rng::with_seed(cfg.rng_seed);
    let mut txn = conn.write_transaction();
    for _ in 0..cfg.removals {
      fill_pair(&mut del_rng, &mut key, &mut val);
      let _ = txn.remove(&key);
    }
    let _ = txn.commit();
  }
  let duration = start.elapsed();
  let bytes = cfg.removals as u64 * item_bytes;
  let res = ResultType::throughput(bytes, duration);
  println!(
    "{db_name}: removals {} 项，吞吐 {}",
    cfg.removals,
    res.with_rate_detail()
  );
  results.push(("removals".to_string(), res));

  // 10. Uncompacted Size (整理前文件大小)
  drop(conn);
  db.flush();
  let uncompacted = database_size(path);
  let res = ResultType::SizeInBytes(uncompacted);
  println!("{db_name}: uncompacted size 为 {}", res.format_value());
  results.push(("uncompacted_size".to_string(), res));

  // 11. Compacted Size (整理后文件大小)
  if db.compact() {
    let compacted = database_size(path);
    let res = ResultType::SizeInBytes(compacted);
    println!("{db_name}: compacted size 为 {}", res.format_value());
    results.push(("compacted_size".to_string(), res));
  } else {
    results.push(("compacted_size".to_string(), ResultType::NA));
  }

  // 12. 结束时内存占用 (End Memory: 统一精准测量进程真实物理常驻内存 Footprint/RSS)
  db.flush();
  let phys_bytes = get_process_physical_memory();
  let cur_allocated = GLOBAL_ALLOC.current_allocated() as u64;
  let mem_bytes = if phys_bytes > 0 {
    phys_bytes
  } else {
    cur_allocated
  };
  let res = ResultType::SizeInBytes(mem_bytes);
  println!(
    "{db_name}: memory 为 {} (物理常驻: {}, 堆分配: {})",
    res.format_value(),
    ResultType::SizeInBytes(phys_bytes).format_value(),
    ResultType::SizeInBytes(cur_allocated).format_value()
  );
  results.push(("memory".to_string(), res));

  results
}
