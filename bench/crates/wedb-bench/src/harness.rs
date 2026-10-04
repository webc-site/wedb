//! 18 段 workload：与 redb-bench 的 `benchmark()` 同名、同序、同单位。
//!
//! 与 redb 的两处有意偏差：
//! - 参数从 `Workload` 取，而非编译期常量（CI 需要按 runner 内存/时间缩放）；
//! - `retain` / `extract_if` / `pop` 在不支持的引擎上记 N/A 而不是 panic，
//!   其余段（键值读写、提交）失败即中止该引擎，由外层汇总为整列 N/A。

use std::{
  ops::Bound,
  path::Path,
  sync::{Arc, Barrier},
  thread,
  time::{Duration, Instant},
};

use fastrand::Rng;
use walkdir::WalkDir;

use crate::{config::Workload, result::ResultType, traits::*};

/// 返回一对确定性随机键值：与 redb 同种子同序列，读段据此重放装载时的键
#[inline]
fn random_pair(rng: &mut Rng, key_size: usize, value_size: usize) -> (Vec<u8>, Vec<u8>) {
  let mut key = vec![0u8; key_size];
  rng.fill(&mut key);
  let mut value = vec![0u8; value_size];
  rng.fill(&mut value);
  (key, value)
}

fn make_rng(w: &Workload) -> Rng {
  Rng::with_seed(w.rng_seed)
}

/// 第 i 个分片把种子序列快进 i 个元素，使各线程读到的键互不重叠且都在表内
fn make_rng_shards(w: &Workload, shards: usize, elements: usize) -> Vec<Rng> {
  let mut rngs = vec![];
  let elements_per_shard = elements / shards;
  for i in 0..shards {
    let mut rng = make_rng(w);
    for _ in 0..(i * elements_per_shard) {
      let _ = random_pair(&mut rng, w.key_size, w.value_size);
    }
    rngs.push(rng);
  }
  rngs
}

/// 取重复轮数的中位数，避开首轮冷缓存与离群值；偶数样本取靠后的中间值
fn median_duration(durations: &mut [Duration]) -> Duration {
  durations.sort_unstable();
  durations[durations.len() / 2]
}

#[inline(never)]
fn nosync_writes<T: BenchDatabase + Send + Sync>(
  connection: &T::C<'_>,
  rng: &mut Rng,
  w: &Workload,
) -> ResultType {
  let start = Instant::now();
  {
    for _ in 0..w.nosync_writes {
      let mut txn = connection.write_transaction();
      let mut inserter = txn.get_inserter();
      let (key, value) = random_pair(rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
      drop(inserter);
      txn.commit().unwrap();
    }
  }

  let duration = start.elapsed();
  let result = ResultType::txns(w.nosync_writes, duration);
  println!(
    "{}: Wrote {} individual items in {}ms ({})，nosync",
    T::db_type_name(),
    w.nosync_writes,
    duration.as_millis(),
    result.with_unit()
  );

  result
}

/// 逐段驱动引擎，返回 `[(行名, 结果)]`；行名与 redb 结果表逐字一致
pub fn benchmark<T: BenchDatabase + Send + Sync>(
  mut db: T,
  path: &Path,
  w: &Workload,
) -> Vec<(String, ResultType)> {
  let mut rng = make_rng(w);
  let mut results = Vec::new();
  let mut connection = db.connect();

  // 1. bulk load
  let start = Instant::now();
  {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..w.bulk_elements {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }
  let duration = start.elapsed();
  let result = ResultType::keys(w.bulk_elements, duration);
  println!(
    "{}: Bulk loaded {} items in {}ms ({})",
    T::db_type_name(),
    w.bulk_elements,
    duration.as_millis(),
    result.with_unit()
  );
  results.push(("bulk load".to_string(), result));

  // 2. individual writes
  let start = Instant::now();
  for _ in 0..w.individual_writes {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
    inserter.insert(&key, &value).unwrap();
    drop(inserter);
    txn.commit().unwrap();
  }
  let duration = start.elapsed();
  let result = ResultType::txns(w.individual_writes, duration);
  println!(
    "{}: Wrote {} individual items in {}ms ({})",
    T::db_type_name(),
    w.individual_writes,
    duration.as_millis(),
    result.with_unit()
  );
  results.push(("individual writes".to_string(), result));

  // 3. small batch writes
  let start = Instant::now();
  for _ in 0..w.batch_writes {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..w.batch_size {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }
  let duration = start.elapsed();
  // 批次足够大，这段度量的是键写入速率而非提交速率
  let result = ResultType::keys(w.batch_writes * w.batch_size, duration);
  println!(
    "{}: Wrote {} batches of {} items in {}ms ({})",
    T::db_type_name(),
    w.batch_writes,
    w.batch_size,
    duration.as_millis(),
    result.with_unit()
  );
  results.push(("small batch writes".to_string(), result));
  // 有序装载必须最后跑，以免污染尺寸测量；但结果行要放回写段之间
  let sorted_inserts_row = results.len();

  // 4. nosync writes
  if connection.set_sync(false) {
    let result = nosync_writes::<T>(&connection, &mut rng, w);
    results.push(("nosync writes".to_string(), result));
  } else {
    // 不支持 nosync 也要把这批数据写进去，否则后续段落在全表扫描上被少算
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..w.nosync_writes {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
    results.push(("nosync writes".to_string(), ResultType::NA));
  }
  connection.set_sync(true);

  let elements = w.loaded_elements();
  let txn = connection.read_transaction();
  {
    // 5. len()
    let start = Instant::now();
    let len = txn.get_reader().len();
    assert_eq!(len, elements as u64);
    let duration = start.elapsed();
    let result = ResultType::Latency(duration);
    println!("{}: len() in {}", T::db_type_name(), result.with_unit());
    results.push(("len()".to_string(), result));

    // 6. random reads
    let mut read_durations = vec![Duration::ZERO; w.read_iterations];
    for read_duration in &mut read_durations {
      let mut rng = make_rng(w);
      let start = Instant::now();
      let mut checksum = 0u64;
      let mut expected_checksum = 0u64;
      let mut reader = txn.get_reader();
      for _ in 0..w.num_reads {
        let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
        let got = reader.get(&key).unwrap();
        checksum += got.as_ref()[0] as u64;
        expected_checksum += value[0] as u64;
      }
      assert_eq!(checksum, expected_checksum);
      *read_duration = start.elapsed();
    }
    let duration = median_duration(&mut read_durations);
    let result = ResultType::keys(w.num_reads, duration);
    println!(
      "{}: Random read {} items in {}ms ({}), median of {} runs",
      T::db_type_name(),
      w.num_reads,
      duration.as_millis(),
      result.with_unit(),
      w.read_iterations
    );
    results.push(("random reads".to_string(), result));

    // 7. random range reads
    let mut scan_durations = vec![Duration::ZERO; w.scan_iterations];
    for scan_duration in &mut scan_durations {
      let mut rng = make_rng(w);
      let start = Instant::now();
      let mut reader = txn.get_reader();
      // redb 原文这里写成 `= 0`，被推成 u8：bench 档关了溢出检查看不出来，
      // dev/check 档跑到 255 次累加就 panic，所以显式取 u64
      let mut value_sum = 0u64;
      for _ in 0..w.num_scans {
        let (key, _value) = random_pair(&mut rng, w.key_size, w.value_size);
        let mut iter = reader.range_from(&key);
        for _ in 0..w.scan_len {
          if let Some((_, value)) = iter.next() {
            value_sum += value.as_ref()[0] as u64;
          } else {
            break;
          }
        }
      }
      assert!(value_sum > 0);
      *scan_duration = start.elapsed();
    }
    // 按范围读次数而非键次数计速率：首个键之后都是步进而非查找
    let duration = median_duration(&mut scan_durations);
    let result = ResultType::scans(w.num_scans, duration);
    println!(
      "{}: Random range read {} x {} elements in {}ms ({}), median of {} runs",
      T::db_type_name(),
      w.num_scans,
      w.scan_len,
      duration.as_millis(),
      result.with_unit(),
      w.scan_iterations
    );
    results.push(("random range reads".to_string(), result));
  }
  drop(txn);

  // 8-11. random reads (N threads)
  for &num_threads in &w.thread_counts {
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut rngs = make_rng_shards(w, num_threads, elements);
    let start = Instant::now();

    thread::scope(|s| {
      for _ in 0..num_threads {
        let barrier = barrier.clone();
        let connection = db.connect();
        let rng = rngs.pop().unwrap();
        s.spawn(move || {
          barrier.wait();
          let txn = connection.read_transaction();
          let mut checksum = 0u64;
          let mut expected_checksum = 0u64;
          let mut reader = txn.get_reader();
          let mut rng = rng.clone();
          for _ in 0..(elements / num_threads) {
            let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
            let got = reader.get(&key).unwrap();
            checksum += got.as_ref()[0] as u64;
            expected_checksum += value[0] as u64;
          }
          assert_eq!(checksum, expected_checksum);
        });
      }
    });

    let duration = start.elapsed();
    let result = ResultType::keys(elements, duration);
    println!(
      "{}: Random read ({} threads) {} items in {}ms ({})",
      T::db_type_name(),
      num_threads,
      elements,
      duration.as_millis(),
      result.with_unit()
    );
    results.push((format!("random reads ({num_threads} threads)"), result));
  }

  // 12. removals
  let deletes = elements / 2;
  let start = Instant::now();
  {
    let mut rng = make_rng(w);
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..deletes {
      let (key, _value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.remove(&key).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }
  let duration = start.elapsed();
  let result = ResultType::keys(deletes, duration);
  println!(
    "{}: Removed {} items in {}ms ({})",
    T::db_type_name(),
    deletes,
    duration.as_millis(),
    result.with_unit()
  );
  results.push(("removals".to_string(), result));

  // 13. retain：每隔一条丢一条，随后回填等量随机条目，使尺寸段看到的表形与前一致
  let start = Instant::now();
  let (removed, retain_result) = {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    let mut counter: u64 = 0;
    match inserter.retain(|_, _| {
      let keep = counter.is_multiple_of(2);
      counter += 1;
      keep
    }) {
      Ok(removed) => {
        drop(inserter);
        txn.commit().unwrap();
        let duration = start.elapsed();
        let result = ResultType::keys(removed as usize, duration);
        println!(
          "{}: Retain removed {} items in {}ms ({})",
          T::db_type_name(),
          removed,
          duration.as_millis(),
          result.with_unit()
        );
        (removed, result)
      }
      Err(_) => {
        // 不支持 retain：放弃本段，不做回填，表形保持删除后的状态
        drop(inserter);
        txn.commit().unwrap();
        println!("{}: retain unsupported，记 N/A", T::db_type_name());
        (0, ResultType::NA)
      }
    }
  };
  results.push(("retain".to_string(), retain_result));

  if removed > 0 {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..removed {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }

  // 14. extract_if：遍历约 1/3 键空间（首字节 [0x55,0xAA)）并摘除奇数条，随后回填
  let extract_start: Vec<u8> = vec![0x55; w.key_size];
  let extract_end: Vec<u8> = vec![0xAA; w.key_size];
  let start = Instant::now();
  let (extracted, extract_result) = {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    let mut counter: u64 = 0;
    // extract_if 返回的迭代器借用 inserter；redb 此处直接 unwrap 续用，本处要为
    // 不支持的引擎留 N/A 分支。迭代器先就地折叠成计数，避免 Result<Iter,_> 的
    // drop 汇合点把这层借用延长到整个 match 体、挡住 inserter/txn 的释放。
    let counted = inserter
      .extract_if(
        (
          Bound::Included(extract_start.as_slice()),
          Bound::Excluded(extract_end.as_slice()),
        ),
        |_, _| {
          let extract = counter % 2 == 1;
          counter += 1;
          extract
        },
      )
      .map(|mut extract_iter| {
        let mut extracted = 0u64;
        while let Some((_key, _value)) = extract_iter.next() {
          extracted += 1;
        }
        extracted
      });
    drop(inserter);
    txn.commit().unwrap();
    let duration = start.elapsed();
    match counted {
      Ok(extracted) => {
        let result = ResultType::keys(extracted as usize, duration);
        println!(
          "{}: extract_if removed {} items in {}ms ({})",
          T::db_type_name(),
          extracted,
          duration.as_millis(),
          result.with_unit()
        );
        (extracted, result)
      }
      Err(_) => {
        println!("{}: extract_if unsupported，记 N/A", T::db_type_name());
        (0, ResultType::NA)
      }
    }
  };
  results.push(("extract_if".to_string(), extract_result));

  if extracted > 0 {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..extracted {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }

  // 15. pop：交替弹出两端；慢速后端（BtreeMap 式 pop）先采 1% 样本再外推
  let pop_table_len = {
    let txn = connection.read_transaction();
    txn.get_reader().len() as usize
  };
  let (_timed_pops, applied_pops, pop_result) = {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    let start = Instant::now();
    let mut duration = None;
    let mut timed_pops = 0u64;
    let mut unsupported = false;
    for i in 0..w.pop_removals {
      let popped = if i % 2 == 0 {
        inserter.pop_first()
      } else {
        inserter.pop_last()
      };
      match popped {
        Ok(Some(_entry)) => timed_pops += 1,
        Ok(None) => break,
        Err(_) => {
          unsupported = true;
          break;
        }
      }
      if i + 1 == w.pop_sample_removals {
        let sample_duration = start.elapsed();
        if sample_duration > w.slow_pop_sample_limit() {
          duration = Some(
            sample_duration
              .checked_mul((w.pop_removals / w.pop_sample_removals.max(1)) as u32)
              .unwrap_or(Duration::MAX),
          );
          break;
        }
      }
    }
    let duration = duration.unwrap_or_else(|| start.elapsed());
    if timed_pops != w.pop_removals as u64 && !unsupported {
      // 用 retain 施加剩余的同形边缘删除，不把另一 API 混进本段的计时
      let timed_pops = timed_pops as usize;
      let remaining_front_pops = w.pop_removals.div_ceil(2) - timed_pops.div_ceil(2);
      let remaining_back_pops = w.pop_removals / 2 - timed_pops / 2;
      let remaining_len = pop_table_len - timed_pops;
      let mut index = 0usize;
      let removed = inserter
        .retain(|_, _| {
          let keep = index >= remaining_front_pops && index < remaining_len - remaining_back_pops;
          index += 1;
          keep
        })
        .unwrap();
      assert_eq!(index, remaining_len);
      assert_eq!(removed as usize, remaining_front_pops + remaining_back_pops);
    }
    drop(inserter);
    txn.commit().unwrap();
    let applied_pops = if timed_pops == w.pop_removals as u64 {
      timed_pops
    } else {
      w.pop_removals as u64
    };
    let result = if unsupported {
      println!("{}: pop unsupported，记 N/A", T::db_type_name());
      ResultType::NA
    } else {
      let result = ResultType::keys(w.pop_removals, duration);
      if timed_pops == w.pop_removals as u64 {
        println!(
          "{}: Popped {} items in {}ms ({})",
          T::db_type_name(),
          timed_pops,
          duration.as_millis(),
          result.with_unit()
        );
      } else {
        println!(
          "{}: Popped {} sampled items, estimated {} items in {}ms ({})",
          T::db_type_name(),
          timed_pops,
          w.pop_removals,
          duration.as_millis(),
          result.with_unit()
        );
      }
      result
    };
    // 采样外推时 retain 已施加全部删除，回填量按 applied_pops 计
    let applied_pops = if unsupported { 0 } else { applied_pops };
    (timed_pops, applied_pops, result)
  };
  results.push(("pop".to_string(), pop_result));

  if applied_pops > 0 {
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..applied_pops {
      let (key, value) = random_pair(&mut rng, w.key_size, w.value_size);
      inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();
  }

  // 16. uncompacted size
  let uncompacted_size = database_size(path);
  results.push((
    "uncompacted size".to_string(),
    ResultType::SizeInBytes(uncompacted_size),
  ));

  // 17. compacted size
  let start = Instant::now();
  drop(connection);
  if db.compact() {
    let duration = start.elapsed();
    println!(
      "{}: Compacted in {}ms",
      T::db_type_name(),
      duration.as_millis()
    );
    let compacted_size = database_size(path);
    results.push((
      "compacted size".to_string(),
      ResultType::SizeInBytes(compacted_size),
    ));
  } else {
    results.push(("compacted size".to_string(), ResultType::NA));
  }

  // 18. sorted inserts：键严格递增且大于所有既有键，配对预生成，计时只覆盖装载
  let sorted_pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..w.sorted_elements as u64)
    .map(|i| {
      let mut key = vec![0xFFu8; w.key_size + 8];
      key[w.key_size..].copy_from_slice(&i.to_be_bytes());
      let mut value = vec![0u8; w.value_size];
      rng.fill(&mut value);
      (key, value)
    })
    .collect();
  let connection = db.connect();
  let start = Instant::now();
  let mut txn = connection.write_transaction();
  let mut inserter = txn.get_inserter();
  inserter
    .insert_sorted(
      sorted_pairs
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap();
  drop(inserter);
  txn.commit().unwrap();
  let duration = start.elapsed();
  let result = ResultType::keys(w.sorted_elements, duration);
  println!(
    "{}: Loaded {} sorted items in {}ms ({})",
    T::db_type_name(),
    w.sorted_elements,
    duration.as_millis(),
    result.with_unit()
  );
  results.insert(sorted_inserts_row, ("sorted inserts".to_string(), result));

  results
}

/// 目录树递归字节总和（含 WAL/段文件等旁路存储），与 redb 同口径
pub fn database_size(path: &Path) -> u64 {
  let mut size = 0u64;
  for result in WalkDir::new(path) {
    let entry = result.unwrap();
    size += entry.metadata().unwrap().len();
  }
  size
}

/// 结果表的行名与顺序：与 redb 输出逐字一致（sorted inserts 归在写段之间）
pub fn row_names(w: &Workload) -> Vec<String> {
  let mut names: Vec<String> = [
    "bulk load",
    "individual writes",
    "small batch writes",
    "sorted inserts",
    "nosync writes",
    "len()",
    "random reads",
    "random range reads",
  ]
  .iter()
  .map(|s| s.to_string())
  .collect();
  for num_threads in &w.thread_counts {
    names.push(format!("random reads ({num_threads} threads)"));
  }
  names.extend(
    [
      "removals",
      "retain",
      "extract_if",
      "pop",
      "uncompacted size",
      "compacted size",
    ]
    .iter()
    .map(|s| s.to_string()),
  );
  names
}

/// 整列 N/A 的垫行：引擎崩溃或超时时保持表形不变
pub fn na_rows(w: &Workload) -> Vec<(String, ResultType)> {
  row_names(w)
    .into_iter()
    .map(|name| (name, ResultType::NA))
    .collect()
}
