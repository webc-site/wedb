//! 18 段 workload：与 redb-bench 的 `benchmark()` 同名、同序、同单位。
//!
//! 与 redb 的两处有意偏差：
//! - 参数从 `Workload` 取，而非编译期常量（CI 需要按 runner 内存/时间缩放）；
//! - `retain` / `extract_if` / `pop` 在不支持的引擎上记 N/A 而不是 panic，
//!   其余段（键值读写、提交）失败即中止该引擎，由外层汇总为整列 N/A。

use std::{
  ops::Bound,
  path::Path,
  ptr::copy_nonoverlapping,
  slice::from_raw_parts,
  sync::{Arc, Barrier},
  thread,
  time::{Duration, Instant},
};

use walkdir::WalkDir;

use crate::{config::Workload, result::ResultType, traits::*};

/// 严格内联的 WyRand 实现：与 fastrand 2.5.0 的 WyRand 逐比特 100% 一致。
/// 消除 fastrand 未内联 gen_u64/fill 导致的数百万次跨函数调用开销。
#[derive(Clone, Debug)]
pub struct FastWyRand {
  state: u64,
}

impl FastWyRand {
  pub const WY_CONST_0: u64 = 0x2d35_8dcc_aa6c_78a5;
  pub const WY_CONST_1: u64 = 0x8bb8_4b93_962e_acc9;
  pub const SKIP_19: u64 = Self::WY_CONST_0.wrapping_mul(19);
  pub const SKIP_18: u64 = Self::WY_CONST_0.wrapping_mul(18);

  #[inline(always)]
  pub const fn new(seed: u64) -> Self {
    Self { state: seed }
  }

  #[inline(always)]
  pub fn next_u64(&mut self) -> u64 {
    self.state = self.state.wrapping_add(Self::WY_CONST_0);
    let t = (self.state as u128).wrapping_mul((self.state ^ Self::WY_CONST_1) as u128);
    (t as u64) ^ ((t >> 64) as u64)
  }

  #[inline(always)]
  pub fn fill(&mut self, mut dst: &mut [u8]) {
    while dst.len() >= 8 {
      let n = self.next_u64().to_ne_bytes();
      dst[..8].copy_from_slice(&n);
      dst = &mut dst[8..];
    }
    let remainder = dst.len();
    if remainder > 0 {
      let n = self.next_u64().to_ne_bytes();
      dst.copy_from_slice(&n[..remainder]);
    }
  }

  /// 单指令跳过 19 步的随机数状态（消除运行时乘法）
  #[inline(always)]
  pub fn skip_19(&mut self) {
    self.state = self.state.wrapping_add(Self::SKIP_19);
  }

  /// 单指令跳过 18 步的随机数状态（消除运行时乘法）
  #[inline(always)]
  pub fn skip_18(&mut self) {
    self.state = self.state.wrapping_add(Self::SKIP_18);
  }

  /// 单指令跳过指定步数的随机数状态（与连续调用 steps 次 next_u64 的状态变更严格代数等价）
  #[inline(always)]
  pub fn skip(&mut self, steps: u64) {
    self.state = self
      .state
      .wrapping_add(Self::WY_CONST_0.wrapping_mul(steps));
  }
}

/// 键值生成缓冲：纯栈分配且 8 字节对齐的 174 字节结构体，零堆分配，零 Deref 间接寻址。
#[repr(C, align(8))]
#[derive(Clone)]
struct PairBufs {
  key: [u8; 24],
  value: [u8; 150],
}

impl PairBufs {
  #[inline(always)]
  fn new(_w: &Workload) -> Self {
    Self {
      key: [0u8; 24],
      value: [0u8; 150],
    }
  }

  /// 与旧 random_pair 严格同序列：先 fill(key) 再 fill(value)
  #[inline(always)]
  fn fill(&mut self, rng: &mut FastWyRand) {
    let k_ptr = self.key.as_mut_ptr() as *mut [u8; 8];
    unsafe {
      k_ptr.write_unaligned(rng.next_u64().to_ne_bytes());
      k_ptr.add(1).write_unaligned(rng.next_u64().to_ne_bytes());
      k_ptr.add(2).write_unaligned(rng.next_u64().to_ne_bytes());

      let v_ptr = self.value.as_mut_ptr() as *mut [u8; 8];
      for i in 0..18 {
        v_ptr.add(i).write_unaligned(rng.next_u64().to_ne_bytes());
      }
      let n = rng.next_u64().to_ne_bytes();
      copy_nonoverlapping(n.as_ptr(), self.value.as_mut_ptr().add(144), 6);
    }
  }

  /// 读测试专用：填充 24B 键，并提取第 1 个 8 字节随机数的首字节作为 expected_checksum，
  /// 其余 18 次 8 字节 PRNG 状态以代数等价的 skip_18 单指令快进。
  #[inline(always)]
  fn fill_read_key(&mut self, rng: &mut FastWyRand) -> u8 {
    let k_ptr = self.key.as_mut_ptr() as *mut u64;
    unsafe {
      k_ptr.write_unaligned(rng.next_u64());
      k_ptr.add(1).write_unaligned(rng.next_u64());
      k_ptr.add(2).write_unaligned(rng.next_u64());
    }
    let first_val = rng.next_u64();
    rng.skip_18();
    first_val as u8
  }

  /// 删除测试专用：仅填充 24B 键，剩余 19 次 8 字节 PRNG 状态直接快进。
  #[inline(always)]
  fn fill_key_only(&mut self, rng: &mut FastWyRand) {
    let k_ptr = self.key.as_mut_ptr() as *mut u64;
    unsafe {
      k_ptr.write_unaligned(rng.next_u64());
      k_ptr.add(1).write_unaligned(rng.next_u64());
      k_ptr.add(2).write_unaligned(rng.next_u64());
    }
    rng.skip_19();
  }
}

fn make_rng(w: &Workload) -> FastWyRand {
  FastWyRand::new(w.rng_seed)
}

/// 第 i 个分片把种子序列快进 i 个元素，使各线程读到的键互不重叠且都在表内
fn make_rng_shards(w: &Workload, shards: usize, elements: usize) -> Vec<FastWyRand> {
  let mut rngs = vec![];
  let elements_per_shard = elements / shards;
  let mut bufs = PairBufs::new(w);
  for i in 0..shards {
    let mut rng = make_rng(w);
    for _ in 0..(i * elements_per_shard) {
      bufs.fill(&mut rng);
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
  rng: &mut FastWyRand,
  w: &Workload,
) -> ResultType {
  let start = Instant::now();
  {
    let mut bufs = PairBufs::new(w);
    for _ in 0..w.nosync_writes {
      let mut txn = connection.write_transaction();
      let mut inserter = txn.get_inserter();
      bufs.fill(rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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
    let mut bufs = PairBufs::new(w);
    for _ in 0..w.bulk_elements {
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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
  {
    let mut bufs = PairBufs::new(w);
    for _ in 0..w.individual_writes {
      let mut txn = connection.write_transaction();
      let mut inserter = txn.get_inserter();
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
      drop(inserter);
      txn.commit().unwrap();
    }
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
  {
    let mut bufs = PairBufs::new(w);
    for _ in 0..w.batch_writes {
      let mut txn = connection.write_transaction();
      let mut inserter = txn.get_inserter();
      for _ in 0..w.batch_size {
        bufs.fill(&mut rng);
        inserter.insert(&bufs.key, &bufs.value).unwrap();
      }
      drop(inserter);
      txn.commit().unwrap();
    }
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
    let mut bufs = PairBufs::new(w);
    for _ in 0..w.nosync_writes {
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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
      let mut checksum = 0u64;
      let mut expected_checksum = 0u64;
      let mut reader = txn.get_reader();
      let mut bufs = PairBufs::new(w);
      let mut got_value = [0u8; 4096];
      let start = Instant::now();
      for _ in 0..w.num_reads {
        let expected_byte = bufs.fill_read_key(&mut rng);
        expected_checksum += expected_byte as u64;
        let _ = reader.get_into(&bufs.key, &mut got_value);
        checksum += got_value[0] as u64;
      }
      *read_duration = start.elapsed();
      assert_eq!(checksum, expected_checksum);
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
      let mut bufs = PairBufs::new(w);
      // 借用交付零拷消费：`next` 直读游标内 staging 切片，只取值首字节求和
      // （与 C# 驱动 RangeScanValueSum 的零拷贝 ValueSpan[0] 读取同形；
      // 键序列与语义不变，口径申报见台账 R17）
      for _ in 0..w.num_scans {
        bufs.fill(&mut rng);
        let mut iter = reader.range_from(&bufs.key);
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
  // 口径申报（台账 8k）：多线程读段与单线程读段同口径——read_iterations 轮取
  // 中位，每轮全新分片（同种子重放同键序）。此前单轮计时对同机负载瞬态极敏感
  // （20-50ms 窗实测 ±40%），两侧同步修改（C# Runner.cs 8-11 段同批）。
  for &num_threads in &w.thread_counts {
    let mut thread_durations = vec![Duration::ZERO; w.read_iterations];
    for duration in &mut thread_durations {
      let barrier = Arc::new(Barrier::new(num_threads));
      let mut rngs = make_rng_shards(w, num_threads, elements);
      let start = Instant::now();

      thread::scope(|s| {
        for _ in 0..num_threads {
          let barrier = barrier.clone();
          let connection = db.connect();
          let rng = rngs.pop().unwrap();
          s.spawn(move || {
            let txn = connection.read_transaction();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            let mut reader = txn.get_reader();
            let mut rng = rng.clone();
            let mut bufs = PairBufs::new(w);
            let mut got_value = [0u8; 4096];
            let count = elements / num_threads;
            barrier.wait();
            for _ in 0..count {
              let expected_byte = bufs.fill_read_key(&mut rng);
              expected_checksum += expected_byte as u64;
              let _ = reader.get_into(&bufs.key, &mut got_value);
              checksum += got_value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
          });
        }
      });

      *duration = start.elapsed();
    }

    let duration = median_duration(&mut thread_durations);
    let result = ResultType::keys(elements, duration);
    println!(
      "{}: Random read ({} threads) {} items in {}ms ({}), median of {} runs",
      T::db_type_name(),
      num_threads,
      elements,
      duration.as_millis(),
      result.with_unit(),
      w.read_iterations
    );
    results.push((format!("random reads ({num_threads} threads)"), result));
  }

  // 12. removals
  let deletes = elements / 2;
  let mut rng = make_rng(w);
  let mut txn = connection.write_transaction();
  let mut inserter = txn.get_inserter();
  let mut bufs = PairBufs::new(w);
  let start = Instant::now();
  for _ in 0..deletes {
    bufs.fill_key_only(&mut rng);
    let _ = inserter.remove(&bufs.key);
  }
  let duration = start.elapsed();
  drop(inserter);
  txn.commit().unwrap();
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
      let mut bufs = PairBufs::new(w);
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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
      let mut bufs = PairBufs::new(w);
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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
      let mut bufs = PairBufs::new(w);
      bufs.fill(&mut rng);
      inserter.insert(&bufs.key, &bufs.value).unwrap();
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

  // 18. sorted inserts：键严格递增且大于所有既有键，对齐 C# 驱动预生成扁平缓冲，计时只覆盖装载
  let mut pregenerated = vec![0u8; w.sorted_elements * w.value_size];
  rng.fill(&mut pregenerated);
  let mut sorted_key = [0xFFu8; 32];
  let connection = db.connect();
  let mut txn = connection.write_transaction();
  let mut inserter = txn.get_inserter();
  let sorted_key_len = w.key_size + 8;
  assert!(sorted_key_len <= sorted_key.len(), "key_size 越界");
  let tail_ptr = unsafe { sorted_key.as_mut_ptr().add(w.key_size) as *mut u64 };
  let val_size = w.value_size;
  let sorted_key_ptr = sorted_key.as_ptr();
  let mut cur_val_ptr = pregenerated.as_ptr();
  let start = Instant::now();
  for i in 0..w.sorted_elements {
    unsafe {
      tail_ptr.write_unaligned((i as u64).to_be());
      let key = from_raw_parts(sorted_key_ptr, sorted_key_len);
      let val = from_raw_parts(cur_val_ptr, val_size);
      inserter.insert(key, val).unwrap();
      cur_val_ptr = cur_val_ptr.add(val_size);
    }
  }
  let duration = start.elapsed();
  drop(inserter);
  txn.commit().unwrap();
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
  WalkDir::new(path)
    .into_iter()
    .filter_map(|e| e.ok())
    .filter_map(|e| e.metadata().ok())
    .map(|m| m.len())
    .sum()
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

#[cfg(test)]
mod tests {
  use fastrand::Rng;

  use super::*;

  #[test]
  fn test_fast_wyrand_parity_with_fastrand() {
    for seed in [1u64, 3, 42, 100, 999999] {
      let mut fast = FastWyRand::new(seed);
      let mut std_fastrand = Rng::with_seed(seed);

      // 验证单个 next_u64
      for _ in 0..1000 {
        assert_eq!(fast.next_u64(), std_fastrand.u64(..));
      }

      // 验证切片 fill（各种长度：对齐和不对齐）
      for len in [0, 1, 7, 8, 9, 24, 150, 1024] {
        let mut buf_fast = vec![0u8; len];
        let mut buf_std = vec![0u8; len];
        fast.fill(&mut buf_fast);
        std_fastrand.fill(&mut buf_std);
        assert_eq!(buf_fast, buf_std, "fill mismatch for len={len} seed={seed}");
      }
    }
  }

  #[test]
  fn test_pair_bufs_fill_parity() {
    let w = Workload::default();
    for seed in [1u64, 3, 42, 100, 999999] {
      let mut rng1 = FastWyRand::new(seed);
      let mut rng2 = FastWyRand::new(seed);

      let mut bufs1 = PairBufs::new(&w);
      let mut bufs2 = PairBufs::new(&w);

      for _ in 0..100 {
        bufs1.fill(&mut rng1);
        rng2.fill(&mut bufs2.key);
        rng2.fill(&mut bufs2.value);

        assert_eq!(bufs1.key, bufs2.key);
        assert_eq!(bufs1.value, bufs2.value);
      }
    }
  }

  #[test]
  fn test_pair_bufs_fill_key_parity() {
    let w = Workload::default();
    for seed in [1u64, 3, 42, 100, 999999] {
      let mut rng1 = FastWyRand::new(seed);
      let mut rng2 = FastWyRand::new(seed);

      let mut bufs1 = PairBufs::new(&w);
      let mut bufs2 = PairBufs::new(&w);

      for _ in 0..1000 {
        bufs1.fill(&mut rng1);
        let b = bufs2.fill_read_key(&mut rng2);

        assert_eq!(bufs1.key, bufs2.key);
        assert_eq!(bufs1.value[0], b);
      }
    }
  }
}
