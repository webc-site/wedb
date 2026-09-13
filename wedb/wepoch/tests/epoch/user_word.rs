//! 用户字 (UserWord) 字段生命周期、并发分配与全局折叠测试

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
  },
  thread::{spawn, yield_now},
};

use aok::{OK, Void};
use log::info;
use parking_lot::Mutex;
use wepoch::{Error, LightEpoch};

use super::support::join_all;

/// 验证用户字生命周期、线程隔离与全局最小值折叠
#[test]
fn user_word_lifecycle_and_min_fold() -> Void {
  info!("验证用户字生命周期、线程隔离与全局最小值折叠");

  let epoch = Arc::new(LightEpoch::new(16));
  assert_eq!(epoch.test_hook_max_user_words(), 5);

  // 分配第一个用户字槽位，初值为 100
  let word_idx = epoch.allocate_user_word(100)?;
  assert_eq!(word_idx, 0);

  // 在受保护作用域内读写用户字
  let _scope = epoch.protected_scope();
  assert_eq!(epoch.this_thread_user_word(word_idx)?, 100);
  epoch.set_this_thread_user_word(word_idx, 42)?;
  assert_eq!(epoch.this_thread_user_word(word_idx)?, 42);

  // 验证多线程并发修改与隔离
  let epoch2 = Arc::clone(&epoch);
  let handle = spawn(move || {
    let _scope = epoch2.protected_scope();
    assert_eq!(epoch2.this_thread_user_word(word_idx).unwrap(), 100);
    epoch2.set_this_thread_user_word(word_idx, 15).unwrap();
    assert_eq!(epoch2.this_thread_user_word(word_idx).unwrap(), 15);
    epoch2
      .this_thread_user_word_atomic(word_idx)
      .unwrap()
      .fetch_add(5, Ordering::SeqCst);
    assert_eq!(epoch2.this_thread_user_word(word_idx).unwrap(), 20);
  });
  handle.join().unwrap();

  // 验证主线程用户字未被子线程覆盖（隔离性保证）
  assert_eq!(epoch.this_thread_user_word(word_idx)?, 42);
  drop(_scope);

  // 释放用户字槽位
  epoch.release_user_word(word_idx)?;

  OK
}

/// 验证用户字槽位容量上限与错误类型防御
#[test]
fn user_word_capacity_limits_and_errors() -> Void {
  info!("验证用户字槽位容量上限与错误类型防御");

  let epoch = LightEpoch::new(16);
  let max_words = epoch.test_hook_max_user_words();
  let mut allocated = Vec::with_capacity(max_words);

  for _ in 0..max_words {
    let idx = epoch.allocate_user_word(0)?;
    allocated.push(idx);
  }

  // 槽位已满，再次分配应返回 ExceededMaxUserWords
  assert!(matches!(
    epoch.allocate_user_word(0),
    Err(Error::ExceededMaxUserWords(cap)) if cap == max_words
  ));

  // 越界访问应返回 InvalidUserWordIndex
  const OUT_OF_BOUNDS_IDX: usize = 99;
  assert!(matches!(
    epoch.get_min_user_word(OUT_OF_BOUNDS_IDX),
    Err(Error::InvalidUserWordIndex(idx)) if idx == OUT_OF_BOUNDS_IDX
  ));

  // 释放一个槽位后可再次成功分配
  epoch.release_user_word(allocated.pop().unwrap())?;
  let reallocated = epoch.allocate_user_word(123)?;
  assert!(reallocated < max_words);

  OK
}

/// 验证通过 Participant 句柄访问与操作用户字
#[test]
fn user_word_via_participant() -> Void {
  info!("验证通过 Participant 句柄访问与操作用户字");

  const CAPACITY: usize = 8;
  let epoch = Arc::new(LightEpoch::new(CAPACITY));
  let word_idx = epoch.allocate_user_word(999)?;

  let p1 = epoch.register()?;
  assert_eq!(p1.user_word(word_idx)?, 999);

  p1.set_user_word(word_idx, 555)?;
  assert_eq!(p1.user_word(word_idx)?, 555);

  p1.user_word_atomic(word_idx)?
    .fetch_add(45, Ordering::SeqCst);
  assert_eq!(p1.user_word(word_idx)?, 600);

  OK
}

/// 验证多线程并发抢注用户字槽位，位掩码 CAS 恰好分配唯一索引
#[test]
fn concurrent_user_word_allocation_race() -> Void {
  info!("验证多线程并发抢注用户字槽位，位掩码 CAS 恰好分配唯一索引");

  const THREADS: usize = 8;
  let epoch = Arc::new(LightEpoch::new(THREADS));
  let max_words = epoch.test_hook_max_user_words();
  let start = Arc::new(AtomicBool::new(false));
  let ok_count = Arc::new(AtomicU32::new(0));
  let full_count = Arc::new(AtomicU32::new(0));
  let granted = Arc::new(Mutex::new(Vec::with_capacity(max_words)));

  let handles: Vec<_> = (0..THREADS)
    .map(|_| {
      let epoch_clone = Arc::clone(&epoch);
      let start_clone = Arc::clone(&start);
      let ok_clone = Arc::clone(&ok_count);
      let full_clone = Arc::clone(&full_count);
      let granted_clone = Arc::clone(&granted);

      spawn(move || {
        while !start_clone.load(Ordering::Acquire) {
          yield_now();
        }
        match epoch_clone.allocate_user_word(7) {
          Ok(idx) => {
            ok_clone.fetch_add(1, Ordering::SeqCst);
            granted_clone.lock().push(idx);
          }
          Err(Error::ExceededMaxUserWords(_)) => {
            full_clone.fetch_add(1, Ordering::SeqCst);
          }
          Err(e) => panic!("非预期错误: {e}"),
        }
      })
    })
    .collect();

  start.store(true, Ordering::Release);
  join_all(handles);

  // 恰好 MAX_USER_WORDS 个线程成功，其余报容量上限；索引互不重复且覆盖 0..MAX
  assert_eq!(ok_count.load(Ordering::SeqCst), max_words as u32);
  assert_eq!(
    (THREADS - max_words) as u32,
    full_count.load(Ordering::SeqCst)
  );
  let mut got = granted.lock().clone();
  got.sort_unstable();
  let expected: [usize; 5] = [0, 1, 2, 3, 4];
  assert_eq!(&got[..], &expected);

  OK
}

/// 验证用户字极端值（负数与 i64::MIN）及动态折叠
#[test]
fn user_word_negative_and_extreme_values() -> Void {
  info!("验证用户字极端值（负数与 i64::MIN）及动态折叠");

  let epoch = Arc::new(LightEpoch::new(8));
  let word_idx = epoch.allocate_user_word(100)?;

  let p1 = epoch.register()?;
  let p2 = epoch.register()?;

  p1.set_user_word(word_idx, -500)?;
  assert_eq!(epoch.get_min_user_word(word_idx)?, -500);

  p2.set_user_word(word_idx, i64::MIN)?;
  assert_eq!(epoch.get_min_user_word(word_idx)?, i64::MIN);

  // 恢复 p2 为 0，最小值重新变为 -500
  p2.set_user_word(word_idx, 0)?;
  assert_eq!(epoch.get_min_user_word(word_idx)?, -500);

  // 恢复 p1 为 200，最小值重新变为 0
  p1.set_user_word(word_idx, 200)?;
  assert_eq!(epoch.get_min_user_word(word_idx)?, 0);

  epoch.release_user_word(word_idx)?;
  OK
}
