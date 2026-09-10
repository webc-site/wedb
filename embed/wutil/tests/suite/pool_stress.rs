//! BufferPool 并发压力与生命周期竞态测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolStressTests.cs
//! （[Explicit] 压力测试的轻量化等价）与 SectorAlignedBufferPoolTests.cs 的
//! `ConcurrentFreeDuringActiveTrafficIsSafe`、`FreeAfterCrossThreadReturnsIsCleanAndBudgetZero`、
//! `MultiplePoolsDifferentSectorSizesNoCorruption`、`RecycledSlotIsIsolatedFromStalePoolShard`、
//! `DeadThreadFinalizerReclaimsPermits`、`ManyShortLivedPoolsDoNotAccumulatePerThreadState`。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
    mpsc::channel,
  },
  thread,
  time::Duration,
};

use aok::{OK, Void};
use log::info;
use wutil::{AlignedBuf, BufferPool, DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE};

/// 多线程高并发混合尺寸获取与归还压力
#[test]
fn concurrent_get_return_stress_mixed_sizes() -> Void {
  info!("对标压力测试轻量化：8 线程 × 200 次混合尺寸与混合清零策略的 Get/Return");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let mut handles = Vec::new();

  for thread_id in 0..8 {
    let p = pool.clone();
    handles.push(thread::spawn(move || -> Void {
      let test_sizes = [512, 1024, 4096, 8192, 65536];
      for i in 0..200 {
        let size = test_sizes[(thread_id + i) % test_sizes.len()];
        let mut buf = p.get_with_policy(size, i % 2 == 0)?;
        assert!(buf.capacity() >= size);
        assert!(buf.is_ptr_aligned());
        buf.as_allocated_slice_mut()[0] = (thread_id as u8).wrapping_add(i as u8);
        drop(buf);
      }
      OK
    }));
  }

  for h in handles {
    h.join().expect("线程运行成功")?;
  }

  OK
}

/// 归还洪峰进行中执行 free()：竞态结束后所有预算许可严格归零
#[test]
fn concurrent_free_during_active_traffic_is_safe() -> Void {
  info!("对标 ConcurrentFreeDuringActiveTrafficIsSafe：free() 与跨线程归还竞态不得滞留配额");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let (tx, rx) = channel::<AlignedBuf>();
  let stop = Arc::new(AtomicBool::new(false));

  // 生产者：持续租借并发送（属主 = 生产者线程），覆盖小容量与 1MB 大容量两类 class
  let mut producers = Vec::new();
  for _ in 0..4 {
    let p = pool.clone();
    let t = tx.clone();
    let stop = stop.clone();
    producers.push(thread::spawn(move || {
      let sizes = [4096, 8192, 65536, 1024 * 1024];
      let mut i = 0usize;
      while !stop.load(Relaxed) && !p.is_closed() {
        if let Ok(buf) = p.get_with_policy(sizes[i % sizes.len()], false) {
          let _ = t.send(buf);
        }
        i += 1;
      }
    }));
  }
  drop(tx); // 释放主线程持有的发送端句柄

  // 消费者：跨线程归还，触发 inbox / Depot 路由与 free() 的竞态窗口
  let rx = Arc::new(parking_lot::Mutex::new(rx));
  let mut consumers = Vec::new();
  for _ in 0..4 {
    let rx = rx.clone();
    consumers.push(thread::spawn(move || {
      loop {
        let got = {
          let lock = rx.lock();
          lock.recv_timeout(Duration::from_millis(20)).ok()
        };
        match got {
          Some(buf) => drop(buf),
          None => break,
        }
      }
    }));
  }

  // 归还洪峰进行中执行 pool.free()（同步多线程测试允许 std::thread::sleep）
  thread::sleep(Duration::from_millis(10));
  pool.free();
  stop.store(true, Relaxed);

  for h in producers {
    let _ = h.join();
  }
  for h in consumers {
    let _ = h.join();
  }

  // 清空通道中剩余遗留缓冲（属主线程已退出，归还走密封回退路径）
  {
    let lock = rx.lock();
    while let Ok(buf) = lock.try_recv() {
      drop(buf);
    }
  }

  assert_eq!(
    pool.reserved_bytes(),
    0,
    "free 与并发归还竞态结束后所有预算许可必须严格归零，不得滞留"
  );

  OK
}

/// 跨线程生产/消费完成后执行 free()，配额必须干净归零
#[test]
fn free_after_cross_thread_returns_is_clean_and_budget_zero() -> Void {
  info!("对标 FreeAfterCrossThreadReturnsIsCleanAndBudgetZero：并发高压释放后配额归零");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let (tx, rx) = channel::<AlignedBuf>();

  let mut producers = Vec::new();
  for _ in 0..4 {
    let p = pool.clone();
    let t = tx.clone();
    producers.push(thread::spawn(move || {
      for _ in 0..100 {
        if let Ok(buf) = p.get_with_policy(4096, false) {
          let _ = t.send(buf);
        }
      }
    }));
  }
  drop(tx); // 释放主线程持有的发送端句柄

  let rx = Arc::new(parking_lot::Mutex::new(rx));
  let mut consumers = Vec::new();
  for _ in 0..4 {
    let rx_clone = rx.clone();
    consumers.push(thread::spawn(move || {
      loop {
        let buf_opt = {
          let lock = rx_clone.lock();
          lock.recv_timeout(Duration::from_millis(20)).ok()
        };
        match buf_opt {
          Some(buf) => drop(buf),
          None => break,
        }
      }
    }));
  }

  for h in producers {
    let _ = h.join();
  }
  for h in consumers {
    let _ = h.join();
  }

  // 清空通道中剩余遗留缓冲
  {
    let lock = rx.lock();
    while let Ok(buf) = lock.try_recv() {
      drop(buf);
    }
  }

  pool.free();

  assert_eq!(
    pool.reserved_bytes(),
    0,
    "并发高压释放后池预留配额必须严格归零"
  );

  OK
}

/// 不同扇区大小的多个池交错借还互不污染
#[test]
fn multiple_pools_different_sector_sizes_no_corruption() -> Void {
  info!("对标 MultiplePoolsDifferentSectorSizesNoCorruption：512/4096 双池交错 1000 轮");

  let pool_a = BufferPool::new(MIN_SECTOR_SIZE)?;
  let pool_b = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  for _ in 0..1000 {
    let mut a = pool_a.get_with_policy(2000, false)?;
    let mut b = pool_b.get_with_policy(2000, false)?;

    assert_eq!(
      a.as_allocated_slice().as_ptr() as usize % MIN_SECTOR_SIZE,
      0
    );
    assert_eq!(
      b.as_allocated_slice().as_ptr() as usize % DEFAULT_SECTOR_SIZE,
      0
    );
    assert!(a.capacity() >= 2000);
    assert!(b.capacity() >= 2000);

    a.as_allocated_slice_mut()[1999] = 1;
    b.as_allocated_slice_mut()[1999] = 1;

    drop(a);
    drop(b);
  }

  OK
}

/// 顺序创建销毁的池必须与陈旧 TLS 缓存条目严格隔离
#[test]
fn recycled_pool_is_isolated_from_stale_tls_shard() -> Void {
  info!("对标 RecycledSlotIsIsolatedFromStalePoolShard：新池不得命中旧池 512 对齐的陈旧缓冲");

  for round in 0..25 {
    // 旧池：驻留本线程 TLS 缓存后释放
    let old = BufferPool::new(MIN_SECTOR_SIZE)?;
    for _ in 0..8 {
      drop(old.get_with_policy(2000, false)?);
    }
    old.free();

    // 新池大概率复用旧池的槽位：必须签发 4096 对齐的新缓冲，绝无 512 对齐旧货
    let new = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    for _ in 0..8 {
      let mut p = new.get_with_policy(2000, false)?;
      assert_eq!(
        p.as_allocated_slice().as_ptr() as usize % DEFAULT_SECTOR_SIZE,
        0,
        "第 {round} 轮: 复用槽位的新池不得提供旧池 512 对齐缓冲"
      );
      assert!(p.capacity() >= 2000);
      p.as_allocated_slice_mut()[1999] = 1;
    }

    assert_eq!(
      old.reserved_bytes(),
      0,
      "第 {round} 轮: 旧池释放后不得持有预算"
    );
  }

  OK
}

/// 工作线程退出时 TLS RAII 确定性回收缓存并将许可归还池
#[test]
fn dead_thread_exit_reclaims_permits() -> Void {
  info!("对标 DeadThreadFinalizerReclaimsPermits：Rust 无 GC，TLS RAII 在线程退出时确定性回池");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  for _ in 0..4 {
    let p = pool.clone();
    thread::spawn(move || {
      for _ in 0..32 {
        drop(p.get_with_policy(4096, false).expect("租借成功")); // 同线程归还，暂存 TLS 私有栈
      }
    })
    .join()
    .expect("工作线程运行成功");
  }

  // 线程退出时 TLS RAII 将局部暂存缓冲转移至 Depot（或释放），许可仍由池持有
  assert!(
    pool.reserved_bytes() > 0,
    "已退出线程的缓存转入 Depot 后许可必须仍可核算"
  );

  pool.free();
  assert_eq!(
    pool.reserved_bytes(),
    0,
    "工作线程退出且池关闭后，所有配额必须严格归零"
  );

  OK
}

/// 单线程连续创建销毁大量短生命周期池，配额必须逐池完全释放
#[test]
fn many_short_lived_pools_do_not_accumulate_per_thread_state() -> Void {
  info!("对标 ManyShortLivedPoolsDoNotAccumulatePerThreadState：100 个顺序池的配额完全回卷");

  for iter in 0..100 {
    let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
    for _ in 0..8 {
      let mut buf = pool.get_with_policy(4096, false)?;
      buf.as_allocated_slice_mut()[0] = 0x5A;
      drop(buf);
    }
    assert!(
      pool.reserved_bytes() > 0,
      "运行期间池必须持有有效缓存预留配额"
    );
    pool.free();
    assert_eq!(
      pool.reserved_bytes(),
      0,
      "迭代 {iter}: 池 Free 后预留配额必须严格归零"
    );
  }

  OK
}
