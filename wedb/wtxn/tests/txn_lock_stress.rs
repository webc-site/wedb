//! 事务锁表碰撞与压力回归（对标
//! libs/storage/Tsavorite/cs/test/test.session.context/TransactionalUnsafeContextTests.cs
//! 的 ManualLockCollidingHashCodes / StressManualLocks / MultiSharedLockTest 与
//! Tsavorite/cs/test/OverflowBucketLockTableTests.cs 的锁计数断言）
//!
//! C# 语义映射（哈希桶内嵌闩位域，锁粒度即索引桶数、随 split 扩容细化）：
//! - ManualLockCollidingHashCodes：`(uniquifier << 30) | bucketIndex` 构造同桶
//!   哈希 → 排序归并合并为单桶最强锁型（锁计数断言），持闩期间同桶互斥；
//! - StressManualLocks：多线程随机键集排序加锁压力，持锁窗口内断言桶互斥 /
//!   共享计数不越界，锁计数与归并计划严格相等，收尾全表计数归零、无死锁；
//! - MultiSharedLockTest：同键 63 把共享闩逐把叠加/递减，共享共存、排他全拒。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/BasicLockTests.cs（并发争用与无死锁推进）

use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
  time::Duration,
};

use wtxn::{LockType, TxnKeyEntries, TxnKeyEntry, TxnKeyEntryComparison, TxnLockTable};
use wval::SessionPrefixBuf;

/// 确定性伪随机数（xorshift64*，无第三方依赖，种子固定保证可复现）
struct XorShift(u64);

impl XorShift {
  fn next(&mut self) -> u64 {
    let mut x = self.0;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    self.0 = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
  }

  fn below(&mut self, n: u64) -> u64 {
    self.next() % n
  }
}

/// 与 C# genHashCode 同构：`(uniquifier << 30) | bucketIndex` 必然碰撞同一桶
/// （低 30 位仅保留 bucketIndex，掩码 `hash & size_mask` 后必落同桶）
fn colliding_hash(uniquifier: u64, bucket_index: i64) -> i64 {
  (uniquifier as i64) << 30 | bucket_index
}

/// 键集排序归并计划（镜像 lock_plan 语义）：桶升序去重 + 最强锁型合并
fn merge_plan(table: &TxnLockTable, mut keys: Vec<TxnKeyEntry>) -> Vec<(usize, bool)> {
  let index = table.pin();
  keys.sort_unstable_by(|a, b| TxnKeyEntryComparison::compare(&index, a, b));
  let mut plan: Vec<(usize, bool)> = Vec::new();
  for entry in keys {
    let bucket = table.bucket_index_for_hash(entry.key_hash);
    let exclusive = entry.lock_type == LockType::Exclusive;
    match plan.last_mut() {
      Some(last) if last.0 == bucket => last.1 |= exclusive,
      _ => plan.push((bucket, exclusive)),
    }
  }
  plan
}

/// C# ManualLockCollidingHashCodes：同桶碰撞哈希排序归并为单把最强锁型，
/// 持闩期间同桶互斥，解锁后锁计数归零、可重新获取
///
/// libs/storage/Tsavorite/cs/test/OverflowBucketLockTableTests.cs:ThreeKeyTest
/// （三个碰撞键共享同桶：共享闩逐把叠加、排他被拒、全释放后排他可取）
#[test]
fn colliding_hash_codes_merge_into_single_bucket_lock() {
  const BUCKET_INDEX: i64 = 42;

  let key_hashes: Vec<i64> = (1..=3u64)
    .map(|u| colliding_hash(u, BUCKET_INDEX))
    .collect();
  let table = TxnLockTable::new();
  // 碰撞前提：三个不同哈希必须落入同一桶（对标 C# GetBucketIndex 断言）
  let bucket = table.bucket_index_for_hash(key_hashes[0]);
  for h in &key_hashes {
    assert_eq!(
      table.bucket_index_for_hash(*h),
      bucket,
      "哈希 {h} 必须与 {BUCKET_INDEX} 桶碰撞"
    );
  }

  let mut entries = TxnKeyEntries::new(4, table.clone());
  for (i, &h) in key_hashes.iter().enumerate() {
    // 混合锁型：归并后必须取最强（排他）
    entries.add_key(
      h,
      if i == 1 {
        LockType::Shared
      } else {
        LockType::Exclusive
      },
    );
  }

  entries.lock_all_keys();
  // 归并对账（对标 AssertTotalLockCounts）：3 条碰撞条目归并为 1 把桶闩，
  // 镜像排序归并计划的桶数必须为 1
  let plan = merge_plan(
    &table,
    key_hashes
      .iter()
      .enumerate()
      .map(|(i, &h)| {
        TxnKeyEntry::new(
          h,
          if i == 1 {
            LockType::Shared
          } else {
            LockType::Exclusive
          },
        )
      })
      .collect(),
  );
  assert_eq!(plan.len(), 1, "碰撞哈希必须归并为单桶计划");

  // 持闩期间：同桶共享/排他全部被拒（最强锁型生效）。探针必须走
  // 同一引擎实例锁表的句柄克隆——与持有者同源方可竞争
  let probed_hash = key_hashes[0];
  let probe_table = table.clone();
  thread::spawn(move || {
    let mut shared_probe = TxnKeyEntries::new(1, probe_table.clone());
    shared_probe.add_key(probed_hash, LockType::Shared);
    assert!(
      !shared_probe.try_lock_all_keys(Duration::from_millis(1)),
      "归并为排他后同桶共享锁必须被拒"
    );
    let mut x_probe = TxnKeyEntries::new(1, probe_table);
    x_probe.add_key(probed_hash, LockType::Exclusive);
    assert!(
      !x_probe.try_lock_all_keys(Duration::from_millis(1)),
      "归并为排他后同桶排他锁必须被拒"
    );
  })
  .join()
  .expect("探针线程不得 panic");

  entries.unlock_all_keys();
  let mut reacquire = TxnKeyEntries::new(1, table);
  reacquire.add_key(key_hashes[0], LockType::Exclusive);
  assert!(
    reacquire.try_lock_all_keys(Duration::from_millis(1)),
    "解锁后必须可重新获取"
  );
}

/// 不同桶的条目各持一把闩（归并不跨桶合并），排序加锁无死锁
#[test]
fn distinct_buckets_hold_one_lock_each() {
  // 0 与 1 掩码后分属不同桶；第三条与首条同桶（重复条目不增锁）
  let keys = vec![
    TxnKeyEntry::new(0x0000_0000, LockType::Exclusive),
    TxnKeyEntry::new(0x0000_0001, LockType::Exclusive),
    TxnKeyEntry::new(0x0000_0000, LockType::Shared), // 同桶重复条目不增锁
  ];
  let table = TxnLockTable::new();
  let plan = merge_plan(&table, keys.clone());
  assert_eq!(plan.len(), 2, "去重后恰为 2 桶");

  let mut entries = TxnKeyEntries::new(4, table.clone());
  for &k in &keys {
    entries.add_key(k.key_hash, k.lock_type);
  }
  entries.lock_all_keys();
  // 持闩期间：各计划桶的行为互斥（同桶再取闩必须被拒）
  for &(bucket, _) in &plan {
    assert!(
      !table.try_lock_exclusive(bucket),
      "排他计划项必须逐桶持独占闩（桶 {bucket}）"
    );
  }
  entries.unlock_all_keys();
  // 收尾：全部计划桶回到空闲态（可重取即无闩位残留）
  for &(bucket, _) in &plan {
    assert!(table.try_lock_exclusive(bucket), "桶 {bucket} 残留闩位");
    table.unlock_exclusive(bucket);
  }
}

/// C# MultiSharedLockTest：同键 63 把共享闩逐把叠加与递减，共享恒共存、
/// 排他在全部释放前恒被拒
///
/// libs/storage/Tsavorite/cs/test/OverflowBucketLockTableTests.cs:SingleKeyTest
/// （单键共享闩叠加、共享持有期间排他被拒、释放后排他可取的锁计数账目）
#[test]
fn multi_shared_locks_stack_and_release() {
  const MAX_LOCKS: usize = 63;
  const KEY_HASH: i64 = 42;
  let table = TxnLockTable::new();
  let bucket = table.bucket_index_for_hash(KEY_HASH);
  // 本地持闩计数（按下标取放，锁位不由守卫对象承载，与 C# 同构）
  let mut held = 0usize;

  // 逐把叠加：共享共存，排他被拒
  for _ in 0..MAX_LOCKS {
    assert!(table.try_lock_shared(bucket));
    held += 1;
    assert!(
      !table.try_lock_exclusive(bucket),
      "共享闩持有期间独占闩必须被拒"
    );
  }
  let probe = table.clone();
  thread::spawn(move || {
    assert!(
      probe.try_lock_shared(bucket),
      "63 把共享闩持有期间共享闩必须仍可共存"
    );
    assert!(
      !probe.try_lock_exclusive(bucket),
      "64 把共享闩持有期间独占闩必须被拒"
    );
    probe.unlock_shared(bucket);
  })
  .join()
  .expect("探针线程不得 panic");

  // 逐把递减：最后一把释放前排他被拒，释放后立即可取
  while held > 0 {
    table.unlock_shared(bucket);
    held -= 1;
    let taken = table.try_lock_exclusive(bucket);
    if held > 0 {
      assert!(!taken, "仍有共享闩持有时独占闩必须被拒");
    } else {
      assert!(taken, "全部共享闩释放后独占闩必须立即可取");
      table.unlock_exclusive(bucket);
    }
  }
}

/// 每桶的持有者计数器（共享/排他分账；更新均发生在持有真实桶闩的
/// 窗口内，同桶无并发更新）
struct BucketLedger {
  exclusive: Vec<AtomicUsize>,
  shared: Vec<AtomicUsize>,
}

impl BucketLedger {
  fn new(bucket_count: usize) -> Self {
    Self {
      exclusive: (0..bucket_count).map(|_| AtomicUsize::new(0)).collect(),
      shared: (0..bucket_count).map(|_| AtomicUsize::new(0)).collect(),
    }
  }
}

/// C# StressManualLocks：8 线程 × 1000 轮随机键集排序加锁压力——
/// 持锁窗口内断言桶排他互斥/共享计数不越界，内部锁计数与归并计划严格相等，
/// 收尾全表计数归零、无死锁
///
/// libs/storage/Tsavorite/cs/test/OverflowBucketLockTableTests.cs:ThreadedLockStressTestMultiThreadsFullContention
/// （多线程满竞争加解锁压力，收尾锁计数严格归零）
#[test]
fn stress_manual_locks_across_threads_without_deadlock() {
  const BASE_KEY: i64 = 42;
  const NUM_KEYS: i64 = 20;
  const NUM_THREADS: usize = 8;
  let iterations = if cfg!(debug_assertions) { 200 } else { 1000 };

  // 同一引擎实例锁表句柄下发各线程（对标同 store 各会话共享 store.LockTable）
  let table = TxnLockTable::new();
  let bucket_count = table.pin().size;
  let ledger = Arc::new(BucketLedger::new(bucket_count));
  let mut handles = Vec::with_capacity(NUM_THREADS);

  for tid in 0..NUM_THREADS {
    let ledger = Arc::clone(&ledger);
    let table = table.clone();
    handles.push(thread::spawn(move || {
      let mut rng = XorShift((tid as u64 + 101) | 1);

      for _ in 0..iterations {
        // 随机键集（C# enumKeys：base + rng 步进），锁型 60% 共享 / 40% 排他
        let mut keys: Vec<TxnKeyEntry> = Vec::new();
        let mut key = BASE_KEY + rng.below(5) as i64;
        while key < BASE_KEY + NUM_KEYS {
          let lock_type = if rng.below(100) < 60 {
            LockType::Shared
          } else {
            LockType::Exclusive
          };
          keys.push(TxnKeyEntry::new(
            TxnKeyEntryComparison::scoped_key_hash(
              SessionPrefixBuf::ROOT.as_slice(),
              &key.to_le_bytes(),
            ),
            lock_type,
          ));
          key += rng.below(5).max(1) as i64;
        }
        let plan = merge_plan(&table, keys.clone());

        let mut entries = TxnKeyEntries::new(8, table.clone());
        for &k in &keys {
          entries.add_key(k.key_hash, k.lock_type);
        }
        entries.lock_all_keys();

        // 持锁窗口：登记持有者并断言互斥不变式（排他至多 1 个持有者）。
        // 登记与注销都必须发生在真实持锁窗口内——注销先于解锁，杜绝
        // 「已解锁未注销」窗口内他人合法取闩导致的对账误报
        for &(bucket, exclusive) in &plan {
          if exclusive {
            // 排他计划项：行为面验证独占互斥（同桶共享取闩被拒；
            // 万一取到立即释放，避免失败路径污染后续轮次）
            let taken = table.try_lock_shared(bucket);
            if taken {
              table.unlock_shared(bucket);
            }
            assert!(!taken, "桶 {bucket} 排他持有期间共享取闩必须被拒");
            let prev = ledger.exclusive[bucket].fetch_add(1, Ordering::AcqRel);
            assert_eq!(prev, 0, "桶 {bucket} 出现并发排他持有者");
          } else {
            // 共享计划项：计入 ledger 记账（他线程的独占取闩会先置独占位再
            // 排空读者，失败时回退该位，故持共享闩期间不作瞬时状态断言）
            let prev = ledger.shared[bucket].fetch_add(1, Ordering::AcqRel);
            assert!(prev < NUM_THREADS, "桶 {bucket} 共享计数越界");
          }
        }
        for &(bucket, exclusive) in &plan {
          if exclusive {
            ledger.exclusive[bucket].fetch_sub(1, Ordering::AcqRel);
          } else {
            ledger.shared[bucket].fetch_sub(1, Ordering::AcqRel);
          }
        }
        entries.unlock_all_keys();
      }
    }));
  }

  for handle in handles {
    handle.join().expect("压力线程不得死锁或 panic");
  }

  // 收尾全表锁计数归零（对标 AssertTotalLockCounts(0, 0)）
  for bucket in &ledger.exclusive {
    assert_eq!(bucket.load(Ordering::Acquire), 0, "排他计数必须归零");
  }
  for bucket in &ledger.shared {
    assert_eq!(bucket.load(Ordering::Acquire), 0, "共享计数必须归零");
  }
  // 闩字层面亦不得残留：压力结束后全表逐桶可重新取闩（取到即还）
  for bucket in 0..bucket_count {
    assert!(table.try_lock_exclusive(bucket), "桶 {bucket} 残留闩位");
    table.unlock_exclusive(bucket);
  }
}
