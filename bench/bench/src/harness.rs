use std::{
  fs,
  hint::black_box,
  path::Path,
  sync::Barrier,
  thread,
  time::{Duration, Instant},
};

use walkdir::WalkDir;

use crate::{
  sys_info::get_process_physical_memory,
  traits::*,
  types::{
    BenchmarkConfig, KEY_BATCH_WRITES, KEY_BULK_LOAD, KEY_COMPACTED_SIZE, KEY_INDIVIDUAL_WRITES,
    KEY_LEN, KEY_MEMORY, KEY_NOSYNC_WRITES, KEY_RANDOM_RANGE_READS, KEY_RANDOM_READS,
    KEY_RANDOM_READS_4, KEY_RANDOM_READS_8, KEY_RANDOM_READS_16, KEY_RANDOM_READS_32, KEY_REMOVALS,
    KEY_UNCOMPACTED_SIZE, ResultType,
  },
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
  results.push((KEY_BULK_LOAD.to_string(), res));

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
  results.push((KEY_INDIVIDUAL_WRITES.to_string(), res));

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
  results.push((KEY_BATCH_WRITES.to_string(), res));

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
    results.push((KEY_NOSYNC_WRITES.to_string(), res));
  } else {
    // 引擎不支持设置 nosync，补齐写以对齐后续数量
    let mut txn = conn.write_transaction();
    for _ in 0..cfg.nosync_writes {
      fill_pair(&mut rng, &mut key, &mut val);
      let _ = txn.insert(&key, &val);
    }
    let _ = txn.commit();
    results.push((KEY_NOSYNC_WRITES.to_string(), ResultType::NA));
  }
  // nosync 段结束后恢复持久写，保证后续 removals 等段落回归默认 sync 口径
  // 对标上游 redb-bench crates/redb-bench/src/lib.rs:216 (nosync 段后无条件 set_sync(true))
  conn.set_sync(true);

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
    results.push((KEY_LEN.to_string(), res));
  }

  // 6. Random Reads (3次中位数)
  // 消费屏障（对标上游 redb-bench crates/redb-bench/src/lib.rs:236-246）：读循环内
  // 逐条累加 get 返回值首字节校验和与写入期同序列期望校验和，循环外 black_box 消费
  // 并断言相等，杜绝 lto=fat + codegen-units=1 下整条读链被优化消除测到空转
  {
    let mut durations = vec![Duration::ZERO; cfg.read_iterations];
    for d in &mut durations {
      let mut read_rng = fastrand::Rng::with_seed(cfg.rng_seed);
      let mut reader = conn.read_transaction();
      let start = Instant::now();
      let mut checksum = 0u64;
      let mut expected_checksum = 0u64;
      let mut hits = 0usize;
      for _ in 0..cfg.num_reads {
        fill_pair(&mut read_rng, &mut key, &mut val);
        if let Some(got) = reader.get(&key) {
          hits += 1;
          checksum += got.first().copied().unwrap_or(0) as u64;
        }
        expected_checksum += val.first().copied().unwrap_or(0) as u64;
      }
      let (checksum, expected_checksum) = black_box((checksum, expected_checksum));
      assert_eq!(
        hits, cfg.num_reads,
        "{db_name}: 随机点查命中数异常（读链未真实生效或数据丢失）"
      );
      assert_eq!(
        checksum, expected_checksum,
        "{db_name}: 随机点查校验和不匹配（读链被优化消除或数据损坏）"
      );
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
    results.push((KEY_RANDOM_READS.to_string(), res));
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
      // range_scan 校验和双消费（traits.rs「供防优化校验」契约兑现），
      // 对标上游 redb-bench crates/redb-bench/src/lib.rs:266-277 非零断言口径
      let mut value_sum = 0u64;
      for _ in 0..cfg.num_scans {
        fill_pair(&mut scan_rng, &mut key, &mut val);
        let (scanned, sum) = reader.range_scan(&key, cfg.scan_len);
        total_scanned += scanned;
        value_sum += sum;
      }
      if total_scanned == 0 {
        supported = false;
        break;
      }
      assert!(
        black_box(value_sum) > 0,
        "{db_name}: 范围扫描校验和为 0（扫描结果未被消费或读链被优化消除）"
      );
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
      results.push((KEY_RANDOM_RANGE_READS.to_string(), res));
    } else {
      results.push((KEY_RANDOM_RANGE_READS.to_string(), ResultType::NA));
    }
  }

  // 8. Multi-threaded Random Reads (4, 8, 16, 32 线程)
  // 对标上游 redb-bench crates/redb-bench/src/lib.rs:284-316：每线程读量为 total_elements/threads。
  // 建连、会话创建与线程本地 runtime 预热全部移出计时窗，双 Barrier 对齐后仅读循环计时。
  let total_elements =
    cfg.bulk_elements + cfg.individual_writes + total_batch_items + cfg.nosync_writes;
  for (threads, section_key) in [
    (4, KEY_RANDOM_READS_4),
    (8, KEY_RANDOM_READS_8),
    (16, KEY_RANDOM_READS_16),
    (32, KEY_RANDOM_READS_32),
  ] {
    // warm_done: workers+主线程共 threads+1 个参与方，宣告预热完成；start_gate 统一放行读窗
    let warm_done = Barrier::new(threads + 1);
    let start_gate = Barrier::new(threads + 1);
    let mut rng_shards = make_rng_shards(
      threads,
      total_elements,
      cfg.rng_seed,
      cfg.key_size,
      cfg.value_size,
    );
    let items_per_thread = (total_elements / threads).max(1);
    let total_thread_reads = items_per_thread * threads;

    let mut begin: Option<Instant> = None;
    // 每线程校验和线程本地累加，join 汇总主线程统一 black_box + 断言
    // （对标上游 redb-bench crates/redb-bench/src/lib.rs:301-316 线程内断言的汇总形态）
    let mut totals = (0u64, 0u64, 0usize);
    // move 闭包只捕获共享引用，避免 db 被逐个 worker 移动
    let db_ref = &db;
    thread::scope(|s| {
      let mut handles = Vec::with_capacity(threads);
      for _ in 0..threads {
        let mut t_rng = rng_shards
          .pop()
          .unwrap_or_else(|| fastrand::Rng::with_seed(cfg.rng_seed));
        let k_size = cfg.key_size;
        let v_size = cfg.value_size;
        let warm_done = &warm_done;
        let start_gate = &start_gate;

        handles.push(s.spawn(move || {
          // 线程内建连（含 wkv new_session）+ 预热 + 一次空读走通读路径，均在计时窗外
          let mut conn = db_ref.connect();
          conn.warm_up();
          let mut reader = conn.read_transaction();
          let mut thread_key = vec![0u8; k_size];
          let mut thread_val = vec![0u8; v_size];
          // 空读用独立克隆 rng，不消耗计时读的样本序列，保持分片覆盖与上游一致；
          // 结果 black_box 消费，杜绝 lto=fat 消除预热读使固定成本回灌计时窗
          let mut warm_rng = t_rng.clone();
          fill_pair(&mut warm_rng, &mut thread_key, &mut thread_val);
          black_box(reader.get(&thread_key));
          warm_done.wait();
          // 等主线程起表后统一放行，仅读循环计入耗时
          start_gate.wait();
          let mut checksum = 0u64;
          let mut expected_checksum = 0u64;
          let mut hits = 0usize;
          for _ in 0..items_per_thread {
            fill_pair(&mut t_rng, &mut thread_key, &mut thread_val);
            if let Some(got) = reader.get(&thread_key) {
              hits += 1;
              checksum += got.first().copied().unwrap_or(0) as u64;
            }
            expected_checksum += thread_val.first().copied().unwrap_or(0) as u64;
          }
          (checksum, expected_checksum, hits)
        }));
      }
      warm_done.wait();
      begin = Some(Instant::now());
      start_gate.wait();
      for h in handles {
        let (c, e, n) = h.join().expect("多线程随机读 worker 异常退出");
        totals.0 += c;
        totals.1 += e;
        totals.2 += n;
      }
    });
    let (checksum, expected_checksum, hits) = black_box(totals);
    assert_eq!(
      hits, total_thread_reads,
      "{db_name}: ({threads} 线程) 随机读命中数异常（读链未真实生效或数据丢失）"
    );
    assert_eq!(
      checksum, expected_checksum,
      "{db_name}: ({threads} 线程) 随机读校验和不匹配（读链被优化消除或数据损坏）"
    );
    let duration = begin.unwrap_or_else(Instant::now).elapsed();
    let bytes = total_thread_reads as u64 * item_bytes;
    let res = ResultType::throughput(bytes, duration);
    println!(
      "{db_name}: random reads ({threads} 线程) {} 项，吞吐 {}",
      total_thread_reads,
      res.with_rate_detail()
    );
    results.push((section_key.to_string(), res));
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
  results.push((KEY_REMOVALS.to_string(), res));

  // 10. Uncompacted Size (整理前文件大小)
  drop(conn);
  db.flush();
  let uncompacted = database_size(path);
  let res = ResultType::SizeInBytes(uncompacted);
  println!("{db_name}: uncompacted size 为 {}", res.format_value());
  results.push((KEY_UNCOMPACTED_SIZE.to_string(), res));

  // 11. Compacted Size (整理后文件大小)
  if db.compact() {
    let compacted = database_size(path);
    let res = ResultType::SizeInBytes(compacted);
    println!("{db_name}: compacted size 为 {}", res.format_value());
    results.push((KEY_COMPACTED_SIZE.to_string(), res));
  } else {
    results.push((KEY_COMPACTED_SIZE.to_string(), ResultType::NA));
  }

  // 12. 结束时内存占用 (End Memory: 统一精准测量进程真实物理常驻内存 Footprint/RSS)
  db.flush();
  let phys_bytes = get_process_physical_memory();
  let res = ResultType::SizeInBytes(phys_bytes);
  println!("{db_name}: memory 为 {}", res.format_value());
  results.push((KEY_MEMORY.to_string(), res));

  results
}
