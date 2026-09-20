//! FreeRecord::set 单次 CAS 语义测试，对标 C# FreeRecordPool.cs:FreeRecord.Set
//!
//! 覆盖：
//! - empty_slot_arm_inserts_once（空槽臂：一次 CAS 写入空槽）
//! - occupied_slot_arm_returns_occupied（占用臂：槽内地址有效即 Occupied，不改写槽位）
//! - expired_slot_arm_replaces_once（过期槽臂：一次 CAS 覆盖失效地址）
//! - occupied_slot_set_never_blocks（同槽占用下归还侧有界返回，不被钉在单槽）
//! - concurrent_set_on_empty_slot_yields_single_winner（同槽竞争失败即 Occupied，绝无二次写入）
//! - bin_put_when_all_slots_occupied_returns_occupied（分桶满：FreeRecordBin::put 返回 Occupied）
//!
//! 与 C# 单次 CompareExchange 同形（失败即由调用侧分桶扫描换槽），本组用例钉住
//! 「单槽无自旋」的可观测后果：返回值合法、槽位不被改写、调用必然终止。

use std::{
  sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use log::info;
use wreviv::{BEST_FIT_SCAN_ALL, FreeRecord, FreeRecordBin, FreeRecordPool, SetStatus};

/// 空槽臂：一次 CAS 写入空槽位并原样解包
#[test]
fn empty_slot_arm_inserts_once() -> Void {
  info!("> empty_slot_arm_inserts_once [空槽臂]");

  let slot = FreeRecord::empty();
  assert_eq!(slot.set(0x1000, 128, 0x1000), SetStatus::InsertedEmpty);
  assert_eq!(slot.address(), 0x1000);
  assert_eq!(slot.size(), 128);

  OK
}

/// 占用臂：槽内地址不低于 min_address 时返回 Occupied，且绝不改写既有槽位
#[test]
fn occupied_slot_arm_returns_occupied() -> Void {
  info!("> occupied_slot_arm_returns_occupied [占用臂]");

  let slot = FreeRecord::empty();
  assert_eq!(slot.set(0x1000, 128, 0x1000), SetStatus::InsertedEmpty);

  // 更高地址、更大尺寸也进不去：槽位仍持有首条记录
  assert_eq!(slot.set(0x9000, 64, 0x1000), SetStatus::Occupied);
  assert_eq!(slot.get(), (0x1000, 128), "占用臂不得改写既有槽位");

  // 自减的 min_address 不改变判定：只要槽内地址 >= min_address 即 Occupied
  assert_eq!(slot.set(0x9000, 64, 0x800), SetStatus::Occupied);
  assert_eq!(slot.get(), (0x1000, 128));

  OK
}

/// 过期槽臂：槽内地址低于新 min_address 时一次 CAS 覆盖替换
#[test]
fn expired_slot_arm_replaces_once() -> Void {
  info!("> expired_slot_arm_replaces_once [过期槽臂]");

  let slot = FreeRecord::empty();
  assert_eq!(slot.set(0x1000, 128, 0x1000), SetStatus::InsertedEmpty);

  // min_address 推进使 0x1000 滑入失效区，本次写入为覆盖替换
  assert_eq!(slot.set(0x3000, 96, 0x2000), SetStatus::ReplacedExpired);
  assert_eq!(slot.get(), (0x3000, 96));

  OK
}

/// 同槽被有效地址占用时，归还侧必须立即返回而非自旋等待
///
/// 无迭代上限的同槽自旋会把写者线程钉在该槽位上，本用例钉住终止性与幂等外观：
/// 竞争线程对同一占用槽位连打十万次 set，槽位内容始终不变且十万次全部返回 Occupied。
#[test]
fn occupied_slot_set_never_blocks() -> Void {
  info!("> occupied_slot_set_never_blocks [同槽占用下归还侧有界返回]");

  let slot = Arc::new(FreeRecord::empty());
  assert_eq!(slot.set(0x1000, 128, 0x1000), SetStatus::InsertedEmpty);

  let worker = {
    let slot = Arc::clone(&slot);
    thread::spawn(move || {
      let mut occupied = 0usize;
      for i in 1..=100_000u64 {
        if slot.set(0x9000 + i, 32, 0x1000) == SetStatus::Occupied {
          occupied += 1;
        }
      }
      occupied
    })
  };

  let occupied = worker
    .join()
    .expect("同槽占用下 set 必须有界返回，绝不被钉在单槽");

  assert_eq!(occupied, 100_000, "全部十万次均须返回 Occupied");
  assert_eq!(slot.get(), (0x1000, 128), "十万次竞争尝试不得改写槽位");

  OK
}

/// 同一空槽位的并发归还：恰有一个写入成功，另一个必须以 Occupied 让路
///
/// 对标 C# `Interlocked.CompareExchange` 失败即 false 的单次尝试语义。
#[test]
fn concurrent_set_on_empty_slot_yields_single_winner() -> Void {
  info!("> concurrent_set_on_empty_slot_yields_single_winner [同槽竞争失败即让路]");

  const ROUNDS: usize = 2_000;
  let slot = Arc::new(FreeRecord::empty());
  let barrier = Arc::new(Barrier::new(2));
  let inserted = Arc::new(AtomicUsize::new(0));
  let yielded = Arc::new(AtomicUsize::new(0));

  let mut handles = Vec::with_capacity(2);
  for t in 0..2u64 {
    let slot = Arc::clone(&slot);
    let barrier = Arc::clone(&barrier);
    let inserted = Arc::clone(&inserted);
    let yielded = Arc::clone(&yielded);
    handles.push(thread::spawn(move || {
      let own = |round: usize| 0x1_0000 + t * 0x1000 + round as u64;
      let peer = |round: usize| 0x1_0000 + (1 - t) * 0x1000 + round as u64;
      for round in 0..ROUNDS {
        // 三道栅栏夹住一轮竞争：进入首道时槽位必为空（上一轮末尾由 t=0 归零），
        // 末道之后才允许清零，保证断言期内无人改写槽位
        if t == 0 && round > 0 {
          slot.clear();
        }
        barrier.wait();
        match slot.set(own(round), 48, 0) {
          SetStatus::InsertedEmpty => {
            inserted.fetch_add(1, Ordering::Release);
          }
          SetStatus::Occupied => {
            yielded.fetch_add(1, Ordering::Release);
          }
          SetStatus::ReplacedExpired => unreachable!("min_address 为 0，不存在过期槽"),
        }
        barrier.wait();
        let addr = slot.address();
        assert!(
          addr == own(round) || addr == peer(round),
          "第 {round} 轮槽内地址 {addr:#x} 既非己方也非对端写入"
        );
        assert_eq!(slot.size(), 48, "第 {round} 轮槽内尺寸被破坏");
        barrier.wait();
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  assert_eq!(
    inserted.load(Ordering::Acquire),
    ROUNDS,
    "每轮恰有一个槽位写入成功"
  );
  assert_eq!(
    yielded.load(Ordering::Acquire),
    ROUNDS,
    "竞争失败方一律 Occupied 让路，绝无二次写入"
  );
  assert!(!slot.is_empty(), "末轮胜者写入必须留在槽位内");

  OK
}

/// 分桶满：全部槽位被有效记录占用时 `FreeRecordBin::put` 返回 Occupied
#[test]
fn bin_put_when_all_slots_occupied_returns_occupied() -> Void {
  info!("> bin_put_when_all_slots_occupied_returns_occupied [分桶满换槽扫描]");

  let capacity = 4;
  let bin = FreeRecordBin::with_scan_limit(64, capacity, BEST_FIT_SCAN_ALL);
  for i in 1..=capacity {
    assert_eq!(
      bin.put(0x1000 * i as u64, 48, 0x1000),
      SetStatus::InsertedEmpty
    );
  }
  assert_eq!(bin.len(), capacity);

  // 逐槽扫描至末槽仍被有效地址挡住：分桶满，返回 Occupied 而非阻塞
  assert_eq!(bin.put(0x9000, 48, 0x1000), SetStatus::Occupied);
  assert_eq!(bin.len(), capacity, "满载拒绝不得改动活跃计数");

  // 池层同路径：唯一分桶满载后 put 返回 false 并计入 drop
  let pool = FreeRecordPool::with_bin_sizes_and_scan_limit(&[64], capacity, BEST_FIT_SCAN_ALL)?;
  for i in 1..=capacity {
    assert!(pool.put(0x1000 * i as u64, 48, 0x1000));
  }
  assert!(!pool.put(0x9000, 48, 0x1000));
  assert_eq!(pool.stats().drop_count, 1);

  OK
}
