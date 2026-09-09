//! 高并发争抢与压力测试，对标 C# RevivificationTests.cs
//!
//! 覆盖：
//! - ArtificialThreadContentionOnOneRecordTest (单记录极高并发争抢 CAS 击穿保护)
//! - ArtificialFreeBinThreadStressTest (多线程 FreeBin 综合压力测试)
//! - exact_match_contention_fallback (精确匹配 CAS 冲突自动回退机制)
//! - concurrent_put_take_purge_stress (生产者、消费者与主动清理极限并发)
//! - active_count_invariant_under_contention (活跃计数并发不变量：CAS 背书增减，绝无下溢或泄漏)

use std::{
  mem,
  sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use gxhash::{HashSet, HashSetExt};
use log::info;
use wreviv::{FreeRecordBin, FreeRecordPool};

use super::support::ADDRESS_INCREMENT;

/// 验证单条记录在多线程高频争抢下的互斥性与状态完整性
/// 对标 C# `ArtificialThreadContentionOnOneRecordTest`
///
/// 8 个高并发线程同时对池中唯一的单条记录进行激烈的竞争争抢：
/// - 每个线程循环 10,000 次；
/// - 若成功取出该记录，全局地址置有效，局部计数器 -1；
/// - 若全局地址有效，尝试原子置 0 并将记录再次 put 回回收池，局部计数器 +1；
/// - 最终累加所有线程的计数器，断言净值为 0（或持有状态为 -1），且槽位数据绝无并发丢失或重复。
#[test]
fn thread_contention_on_single_record() -> Void {
  info!("> thread_contention_on_single_record [对标 C# ArtificialThreadContentionOnOneRecordTest]");

  let pool = Arc::new(FreeRecordPool::with_bin_sizes(&[64], 32)?);
  let test_address = ADDRESS_INCREMENT;
  let min_address = ADDRESS_INCREMENT - 10;
  let small_size = 32u32;

  // 初始存入该唯一记录
  assert!(pool.put(test_address, small_size, min_address));

  let global_address = Arc::new(AtomicU64::new(0));
  let net_counter = Arc::new(AtomicI64::new(0));
  let num_threads = 8;
  let num_iterations = 10_000;

  let mut handles = Vec::with_capacity(num_threads);

  for _ in 0..num_threads {
    let pool = Arc::clone(&pool);
    let global_address = Arc::clone(&global_address);
    let net_counter = Arc::clone(&net_counter);

    handles.push(thread::spawn(move || {
      let mut local_counter: i64 = 0;
      for _ in 0..num_iterations {
        if let Some((addr, _)) = pool.take(small_size, min_address) {
          assert_eq!(addr, test_address);
          global_address.store(test_address, Ordering::Release);
          local_counter -= 1;
        } else if global_address.load(Ordering::Acquire) == test_address
          && global_address
            .compare_exchange(test_address, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
          assert!(pool.put(test_address, small_size, min_address));
          local_counter += 1;
        }
        thread::yield_now();
      }
      net_counter.fetch_add(local_counter, Ordering::Relaxed);
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // 检查槽位是否在池中或在 global_address 中
  let in_pool = pool.take(small_size, min_address).is_some();
  let in_global = global_address.load(Ordering::Acquire) == test_address;
  assert!(
    in_pool ^ in_global,
    "记录必须恰好存在于池中或持有于 global_address 中"
  );

  let final_net = net_counter.load(Ordering::Relaxed);
  if in_pool {
    assert_eq!(final_net, 0, "并发争夺单记录的最终计数器净值必须为 0");
  } else {
    assert_eq!(
      final_net, -1,
      "并发争夺单记录被持有时的最终计数器净值必须为 -1"
    );
  }

  OK
}

/// 多生产者、多消费者深度并发压力测试
/// 对标 C# `ArtificialFreeBinThreadStressTest`
///
/// 验证多线程组合下，每条添加的记录恰好被取出一次，无任何遗留标志或槽位。
#[test]
fn free_bin_multithread_stress() -> Void {
  info!("> free_bin_multithread_stress [对标 C# ArtificialFreeBinThreadStressTest]");

  let test_cases = [(1, 1), (5, 10), (10, 5), (8, 8)];
  let num_records_per_thread = 500usize;
  let add_record_size = 48u32;
  let removed_base = 10_000i64;

  for &(num_add_threads, num_take_threads) in &test_cases {
    let max_records = num_records_per_thread * num_add_threads;
    let pool = Arc::new(FreeRecordPool::with_bin_sizes(&[64], max_records)?);
    let flags = Arc::new(
      (0..max_records)
        .map(|_| AtomicI64::new(0))
        .collect::<Vec<_>>(),
    );
    let total_taken = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(num_add_threads + num_take_threads));

    // 生产者线程
    let mut add_handles = Vec::with_capacity(num_add_threads);
    for t in 0..num_add_threads {
      let pool = Arc::clone(&pool);
      let flags = Arc::clone(&flags);
      let barrier = Arc::clone(&barrier);

      add_handles.push(thread::spawn(move || {
        barrier.wait();
        for i in 0..num_records_per_thread {
          let addr_base = i + t * num_records_per_thread;
          let prev_flag = flags[addr_base].swap(1, Ordering::AcqRel);
          assert_eq!(prev_flag, 0, "槽位状态应当未添加");
          let logical_addr = (addr_base as u64) + ADDRESS_INCREMENT;
          assert!(
            pool.put(logical_addr, add_record_size, 0),
            "添加记录必须成功"
          );
        }
      }));
    }

    // 消费者线程
    let mut take_handles = Vec::with_capacity(num_take_threads);
    for t in 0..num_take_threads {
      let pool = Arc::clone(&pool);
      let flags = Arc::clone(&flags);
      let total_taken = Arc::clone(&total_taken);
      let barrier = Arc::clone(&barrier);

      take_handles.push(thread::spawn(move || {
        barrier.wait();
        let tid = t as i64;
        while total_taken.load(Ordering::Acquire) < max_records {
          if let Some((addr, _)) = pool.take(add_record_size, 0) {
            let addr_base = (addr - ADDRESS_INCREMENT) as usize;
            let prev_flag = flags[addr_base].compare_exchange(
              1,
              removed_base + tid,
              Ordering::AcqRel,
              Ordering::Acquire,
            );
            assert_eq!(
              prev_flag,
              Ok(1),
              "取出记录必须来自于已添加的槽位且不可重复取出"
            );
            total_taken.fetch_add(1, Ordering::Release);
          } else {
            thread::yield_now();
          }
        }
      }));
    }

    for h in add_handles {
      h.join().unwrap();
    }
    for h in take_handles {
      h.join().unwrap();
    }

    assert_eq!(total_taken.load(Ordering::Acquire), max_records);
    for (i, flag) in flags.iter().enumerate() {
      let v = flag.load(Ordering::Acquire);
      assert!(
        v >= removed_base,
        "槽位 {i} 状态异常: v={v}, 预期已完全消费"
      );
    }
    assert_eq!(pool.total_active_records(), 0);
  }

  OK
}

/// 验证精确匹配高频争抢 CAS 冲突时的回退与重试
///
/// 当多个线程高频争夺精确匹配槽位且发生 CAS 碰撞时，
/// 未抢到精确槽位的线程不会直接放弃，而是折半扫描上限继续重试或回退至备选最优槽位。
#[test]
fn exact_match_contention_fallback() -> Void {
  info!("> exact_match_contention_fallback [精确匹配竞争回退]");

  let bin_size = 64u32;
  let pool = Arc::new(FreeRecordPool::with_bin_sizes_and_scan_limit(
    &[bin_size],
    16,
    4,
  )?);

  // 存入 2 个 40B（精确目标）与 2 个 48B（备选最优）
  assert!(pool.put(0x1000, 40, 0));
  assert!(pool.put(0x2000, 40, 0));
  assert!(pool.put(0x3000, 48, 0));
  assert!(pool.put(0x4000, 48, 0));
  assert_eq!(pool.total_active_records(), 4);

  let taken_records = Arc::new(Mutex::new(Vec::new()));
  let barrier = Arc::new(Barrier::new(4));
  let mut handles = Vec::new();

  for _ in 0..4 {
    let pool = Arc::clone(&pool);
    let barrier = Arc::clone(&barrier);
    let taken_records = Arc::clone(&taken_records);
    handles.push(thread::spawn(move || {
      barrier.wait();
      if let Some((addr, size)) = pool.take(40, 0) {
        let mut lock = taken_records.lock().unwrap();
        lock.push((addr, size));
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  let taken = mem::take(&mut *taken_records.lock().unwrap());
  assert_eq!(taken.len(), 4, "所有 4 条槽位均应被成功复活取走");
  assert_eq!(pool.total_active_records(), 0);

  let mut seen_addrs = HashSet::with_capacity(taken.len());
  for (addr, size) in taken {
    assert!(seen_addrs.insert(addr));
    assert!(size == 40 || size == 48);
  }

  OK
}

/// 生产者、消费者与冷区主动清理并发极限对抗测试
///
/// 使用原子计数器实现完全确定性同步，杜绝无意义 sleep。
/// 验证在激烈的并发交织下无死锁、无重复、无取到过期地址。
#[test]
fn concurrent_put_take_purge_stress() -> Void {
  info!("> concurrent_put_take_purge_stress [并发存取淘汰极限压力对抗]");

  let pool = Arc::new(FreeRecordPool::with_capacity(512)?);
  let min_addr_global = Arc::new(AtomicU64::new(0x1000));
  let producers_done = Arc::new(AtomicBool::new(false));
  let total_produced = Arc::new(AtomicUsize::new(0));

  let num_producers = 4;
  let num_consumers = 4;
  let ops_per_producer = 1000;
  let total_ops = num_producers * ops_per_producer;

  let barrier = Arc::new(Barrier::new(num_producers + num_consumers + 1));
  let mut handles = Vec::new();

  // 生产者线程
  for p in 0..num_producers {
    let pool = Arc::clone(&pool);
    let min_addr = Arc::clone(&min_addr_global);
    let barrier = Arc::clone(&barrier);
    let total_produced = Arc::clone(&total_produced);

    handles.push(thread::spawn(move || {
      barrier.wait();
      for i in 0..ops_per_producer {
        let cur_min = min_addr.load(Ordering::Acquire);
        let addr = cur_min + (p as u64 * 100_000) + (i as u64 * 8) + 8;
        let size = (32 + (i % 8) * 16) as u32;
        let _ = pool.put(addr, size, cur_min);
        total_produced.fetch_add(1, Ordering::Relaxed);
        if i % 50 == 0 {
          thread::yield_now();
        }
      }
    }));
  }

  // 消费者线程
  let taken_records = Arc::new(Mutex::new(Vec::new()));
  for _ in 0..num_consumers {
    let pool = Arc::clone(&pool);
    let min_addr = Arc::clone(&min_addr_global);
    let producers_done = Arc::clone(&producers_done);
    let taken_records = Arc::clone(&taken_records);
    let barrier = Arc::clone(&barrier);

    handles.push(thread::spawn(move || {
      barrier.wait();
      while !producers_done.load(Ordering::Acquire) || pool.total_active_records() > 0 {
        let cur_min = min_addr.load(Ordering::Acquire);
        if let Some((addr, _size)) = pool.take(32, cur_min) {
          assert!(
            addr >= cur_min,
            "取出的地址 {addr:#x} 绝不能低于当时的 min_address {cur_min:#x}"
          );
          let mut lock = taken_records.lock().unwrap();
          lock.push(addr);
        } else {
          thread::yield_now();
        }
      }
    }));
  }

  // 清理者线程：基于生产者产出进度确定性推进 min_address 并调用 purge_below
  let cleaner_pool = Arc::clone(&pool);
  let cleaner_min = Arc::clone(&min_addr_global);
  let cleaner_produced = Arc::clone(&total_produced);
  let cleaner_done = Arc::clone(&producers_done);
  let cleaner_barrier = Arc::clone(&barrier);
  let cleaner_handle = thread::spawn(move || {
    cleaner_barrier.wait();
    let total_steps = 20;
    let step_threshold = total_ops / total_steps;

    for step in 1..=total_steps {
      let target_produced = step * step_threshold;
      while cleaner_produced.load(Ordering::Relaxed) < target_produced
        && !cleaner_done.load(Ordering::Acquire)
      {
        thread::yield_now();
      }
      let new_min = 0x1000 + (step as u64 * 2000);
      cleaner_min.store(new_min, Ordering::Release);
      cleaner_pool.purge_below(new_min);
    }
  });

  // 等待生产者结束
  for h in handles.drain(..num_producers) {
    h.join().unwrap();
  }
  producers_done.store(true, Ordering::Release);
  cleaner_handle.join().unwrap();

  // 等待消费者结束
  for h in handles {
    h.join().unwrap();
  }

  let taken = taken_records.lock().unwrap();
  let mut seen = HashSet::with_capacity(taken.len());
  for &addr in taken.iter() {
    assert!(seen.insert(addr), "检测到重复取出的地址: {addr:#x}");
  }

  info!(
    "高压存取淘汰测试完成: 成功取出 {} 条独立槽位, 丢弃/淘汰 {} 条",
    taken.len(),
    pool.drop_count()
  );

  OK
}

/// 验证分桶活跃计数在并发存取与淘汰风暴下的不变量
///
/// 活跃计数 `len()` 的每次增减均由一次成功的 CAS 背书（存入 +1 / 取出或淘汰 -1），
/// 因此并发冲击后必须精确归零，且与真实非空槽位数一致，绝无下溢或泄漏。
#[test]
fn active_count_invariant_under_contention() -> Void {
  info!("> active_count_invariant_under_contention [活跃计数并发不变量]");

  let bin = Arc::new(FreeRecordBin::new(64, 16));
  assert_eq!(bin.len(), 0);
  assert!(bin.is_empty());

  let threads = 8;
  let iterations = 10_000;
  let barrier = Arc::new(Barrier::new(threads * 2));
  let next_addr = Arc::new(AtomicU64::new(ADDRESS_INCREMENT));

  // 4 个线程并发存入（地址递增保证唯一，超出容量的存入被拒绝属预期）
  let mut put_handles = Vec::new();
  for _ in 0..threads {
    let bin = Arc::clone(&bin);
    let barrier = Arc::clone(&barrier);
    let next_addr = Arc::clone(&next_addr);
    put_handles.push(thread::spawn(move || {
      barrier.wait();
      for _ in 0..iterations {
        let addr = next_addr.fetch_add(1, Ordering::Relaxed);
        let _ = bin.put(addr, 48, ADDRESS_INCREMENT);
      }
    }));
  }

  // 4 个线程并发取出与淘汰
  let mut take_handles = Vec::new();
  for _ in 0..threads {
    let bin = Arc::clone(&bin);
    let barrier = Arc::clone(&barrier);
    take_handles.push(thread::spawn(move || {
      barrier.wait();
      for _ in 0..iterations {
        let _ = bin.take_best_fit(48, ADDRESS_INCREMENT);
        bin.purge_below(u64::MAX);
      }
    }));
  }

  for h in put_handles {
    h.join().unwrap();
  }
  for h in take_handles {
    h.join().unwrap();
  }

  // 主线程兜底清理（此时已无并发），消除取放线程收尾交织的不确定性
  bin.purge_below(u64::MAX);

  // 不变量：计数归零、与逐槽扫描的真实非空槽位数一致
  assert_eq!(bin.len(), 0, "并发风暴后活跃计数必须精确归零");
  assert!(bin.is_empty());
  let occupied = bin.slots.iter().filter(|s| !s.is_empty()).count();
  assert_eq!(occupied, 0, "逐槽扫描不允许残留非空槽位");

  OK
}
