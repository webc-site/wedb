//! wreviv 端到端冒烟测试套件
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs（复活池策略）

use std::{
  sync::{Arc, Barrier, Mutex, atomic::Ordering},
  thread,
};

use aok::{OK, Void};
use log::info;
use wreviv::{
  BEST_FIT_SCAN_ALL, DEFAULT_BIN_SIZES, FreeRecord, FreeRecordBin, FreeRecordPool, SetStatus,
};

/// 端到端冒烟测试：FreeRecordPool 完整生命周期、松弛填充分配、统计与清空复用
#[test]
fn smoke_pool_lifecycle_and_slack_allocation() -> Void {
  info!("> smoke_pool_lifecycle_and_slack_allocation");

  let pool = FreeRecordPool::new(true);
  assert!(pool.is_empty());
  assert_eq!(pool.bins.len(), DEFAULT_BIN_SIZES.len());
  assert_eq!(pool.bins.iter().map(|b| b.len()).sum::<usize>(), 0);

  // 1. 存入不同分桶区间的记录
  assert!(pool.put(0x1000, 64, 0x1000));
  assert!(pool.put(0x2000, 120, 0x1000));
  assert!(pool.put(0x3000, 256, 0x1000));
  assert_eq!(pool.bins.iter().map(|b| b.len()).sum::<usize>(), 3);
  assert_eq!(pool.put_count.load(Ordering::Relaxed), 3);

  // 2. 精确分配 (Exact match: 64B -> 64B)
  assert_eq!(pool.take(64, 0x1000, 0x1000), Some((0x1000, 64)));

  // 3. 向上跨桶取出：100B 需求命中 120B 槽位（内部松弛填充 20B）
  assert_eq!(pool.take(100, 0x1000, 0x1000), Some((0x2000, 120)));

  // 4. 向上跨桶普通 take 取出 (命中 256B 槽位)
  let taken = pool.take(150, 0x1000, 0x1000);
  assert_eq!(taken, Some((0x3000, 256)));
  assert!(pool.is_empty());

  // 5. 统计指标验证（直读 pub 原子字段，与生产 wkv 同口径）
  assert_eq!(pool.put_count.load(Ordering::Relaxed), 3);
  assert_eq!(pool.take_count.load(Ordering::Relaxed), 3);
  assert_eq!(pool.hit_count.load(Ordering::Relaxed), 3);
  assert_eq!(
    pool.take_count.load(Ordering::Relaxed) - pool.hit_count.load(Ordering::Relaxed),
    0,
    "复活申请失败次数为零"
  );

  // 6. 清空与复用验证
  assert!(pool.put(0x5000, 64, 0x1000));
  assert_eq!(pool.bins.iter().map(|b| b.len()).sum::<usize>(), 1);
  pool.clear();
  assert!(pool.is_empty());
  assert_eq!(pool.take(64, 0x1000, 0x1000), None);

  OK
}

/// 计数复位与清槽职责分立：`reset_stats` 归零四计数而不动槽位，
/// `clear` 清槽而账目原样保留
/// 验证 RevivificationStats.Reset 的账目复位语义（INFO RESETSTAT 的 reviv 臂终点，不经清池）
#[test]
fn reset_stats_zeroes_counters_and_clear_keeps_them() -> Void {
  info!("> reset_stats_zeroes_counters_and_clear_keeps_them");

  let pool = FreeRecordPool::new(true);
  assert!(pool.put(0x1000, 64, 0x1000));
  assert!(pool.put(0x2000, 120, 0x1000));
  assert_eq!(pool.take(64, 0x1000, 0x1000), Some((0x1000, 64)));
  assert_eq!(
    (
      pool.put_count.load(Ordering::Relaxed),
      pool.take_count.load(Ordering::Relaxed),
      pool.hit_count.load(Ordering::Relaxed)
    ),
    (2, 1, 1)
  );

  // clear 只清槽：账目原样保留（INFO RESETSTAT 绝不误用本口当复位）
  pool.clear();
  assert_eq!(
    (
      pool.put_count.load(Ordering::Relaxed),
      pool.take_count.load(Ordering::Relaxed),
      pool.hit_count.load(Ordering::Relaxed),
      pool.drop_count.load(Ordering::Relaxed)
    ),
    (2, 1, 1, 0),
    "clear 只清槽不动账目"
  );
  assert!(pool.is_empty());

  // reset_stats 只清账：可复活槽位原样保留，复位后继续记账
  assert!(pool.put(0x3000, 64, 0x1000));
  pool.reset_stats();
  assert_eq!(
    (
      pool.put_count.load(Ordering::Relaxed),
      pool.take_count.load(Ordering::Relaxed),
      pool.hit_count.load(Ordering::Relaxed),
      pool.drop_count.load(Ordering::Relaxed)
    ),
    (0, 0, 0, 0),
    "reset_stats 应归零四计数"
  );
  assert_eq!(
    pool.bins.iter().map(|b| b.len()).sum::<usize>(),
    1,
    "reset_stats 不得清槽"
  );
  assert_eq!(pool.take(64, 0x1000, 0x1000), Some((0x3000, 64)));
  assert_eq!(
    (
      pool.take_count.load(Ordering::Relaxed),
      pool.hit_count.load(Ordering::Relaxed)
    ),
    (1, 1),
    "复位后新流量继续从零点记账"
  );

  OK
}

/// 端到端冒烟测试：FreeRecordBin 单桶直接操作与存取
#[test]
fn smoke_bin_direct_operations() -> Void {
  info!("> smoke_bin_direct_operations");

  let bin = FreeRecordBin::with_scan_limit(128, 8, BEST_FIT_SCAN_ALL);
  assert_eq!(bin.max_size, 128);
  assert_eq!(bin.slots.len(), 8);
  assert_eq!(bin.len(), 0);
  assert!(bin.is_empty());

  let status = bin.put(0x1000, 80, 0x1000);
  assert_eq!(status, SetStatus::InsertedEmpty);
  assert_eq!(bin.len(), 1);
  assert!(!bin.is_empty());

  let (res, purged) = bin.take_best_fit(70, 0x1000, 0x1000);
  assert_eq!(res, Some((0x1000, 80)));
  assert_eq!(purged, 0);
  assert_eq!(bin.len(), 0);
  assert!(bin.is_empty());

  bin.clear();
  assert!(bin.is_empty());

  OK
}

/// 端到端冒烟测试：FreeRecord 原子槽位布局打包与状态机
#[test]
fn smoke_free_record_slot_primitive() -> Void {
  info!("> smoke_free_record_slot_primitive");

  let empty = FreeRecord::empty();
  assert!(empty.is_empty());
  assert_eq!(empty.raw(), 0);
  assert_eq!(empty.to_string(), "FreeRecord(empty)");

  // 48 位地址与 16 位尺寸打包解包
  let (addr, size) = (0x1234_5678_ABCDu64, 4096u32);
  let packed = FreeRecord::pack(addr, size);
  let (unpacked_addr, unpacked_size) = FreeRecord::unpack(packed);
  assert_eq!(unpacked_addr, addr);
  assert_eq!(unpacked_size, size);

  // 存入有效槽位
  let record = FreeRecord::empty();
  assert_eq!(record.set(0x1000, 128, 0x1000), SetStatus::InsertedEmpty);
  assert_eq!(FreeRecord::unpack(record.raw()), (0x1000, 128));

  // 占用拒绝
  assert_eq!(record.set(0x2000, 256, 0x1000), SetStatus::Occupied);

  // 主动淘汰清零
  assert!(record.try_purge_below(0x2000));
  assert!(record.is_empty());

  record.clear();
  assert!(record.is_empty());

  OK
}

/// 端到端冒烟测试：基础多线程并发存取冒烟验证
#[test]
fn smoke_multithread_basic_concurrency() -> Void {
  info!("> smoke_multithread_basic_concurrency");

  let thread_count = 4;
  let ops_per_thread = 200;
  let pool = Arc::new(FreeRecordPool::with_bin_sizes_and_scan_limit(
    &DEFAULT_BIN_SIZES,
    512,
    BEST_FIT_SCAN_ALL,
  )?);
  let barrier = Arc::new(Barrier::new(thread_count * 2));

  // 4 个并发生产者
  let mut put_handles = Vec::with_capacity(thread_count);
  for t in 0..thread_count {
    let pool = Arc::clone(&pool);
    let barrier = Arc::clone(&barrier);
    put_handles.push(thread::spawn(move || {
      barrier.wait();
      let mut put_ok = 0;
      for i in 0..ops_per_thread {
        let addr = ((t * ops_per_thread + i + 1) * 64) as u64;
        if pool.put(addr, 48, 0) {
          put_ok += 1;
        }
      }
      put_ok
    }));
  }

  // 4 个并发消费者
  let taken_records = Arc::new(Mutex::new(Vec::new()));
  let mut take_handles = Vec::with_capacity(thread_count);
  for _ in 0..thread_count {
    let pool = Arc::clone(&pool);
    let barrier = Arc::clone(&barrier);
    let taken_records = Arc::clone(&taken_records);
    take_handles.push(thread::spawn(move || {
      barrier.wait();
      let mut local = Vec::new();
      for _ in 0..ops_per_thread {
        if let Some((addr, size)) = pool.take(48, 0, 0) {
          local.push((addr, size));
        }
        thread::yield_now();
      }
      taken_records.lock().unwrap().extend(local);
    }));
  }

  let mut total_put = 0;
  for h in put_handles {
    total_put += h.join().unwrap();
  }
  for h in take_handles {
    h.join().unwrap();
  }

  let taken = taken_records.lock().unwrap();
  assert!(total_put > 0);
  assert!(!taken.is_empty());

  OK
}
