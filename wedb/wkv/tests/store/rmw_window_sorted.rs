//! 多键读改写原子窗口（rmw_window_sorted）折叠与计划缓存回归测试
//!
//! 验证：
//! 1. 同桶多键折叠计数断言（闩获取次数 = distinct 桶数）
//! 2. rmw_window_sorted 争用轮次内 plan 仅首轮构建一次的断言
//! 3. 索引版本推进时失效重建断言

use std::{
  collections::HashMap,
  iter,
  sync::{Arc, atomic::Ordering},
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wkv::{RMW_PLAN_ACQUIRE_MISS, RMW_PLAN_BUILD_COUNT, RMW_PLAN_PINNED_INDEX, store::ResizePhase};
use wval::SessionPrefixBuf;

use crate::support::{config, finish_resize_window, open_store, stage_resize};

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// 有界握手：等待计划可观测点达成，超时即点名未达成的编排前提
/// （nextest 不给测试设默认超时，无界忙等会把一次编排失效升级成整条闸门挂死）
fn wait_for(what: &str, reached: impl Fn() -> bool) {
  const DEADLINE: Duration = Duration::from_secs(20);
  let start = Instant::now();
  while !reached() {
    assert!(
      start.elapsed() <= DEADLINE,
      "rmw 计划编排握手超时未达成: {what}"
    );
    thread::yield_now();
  }
}

/// 辅助生成落入指定桶的测试键（窗口寻桶 scoped 口径：会话物理前缀种子，
/// 与 `try_rmw_window`/`rmw_window_sorted` 的 `whasher::scoped_hash` 单点同构）
fn find_keys_for_buckets(
  store: &wkv::WedbStore<impl wdev::Device>,
  prefix: &SessionPrefixBuf,
) -> (usize, Vec<Vec<u8>>, usize, Vec<Vec<u8>>) {
  let index = store.active_index();
  let mut buckets = HashMap::<usize, Vec<Vec<u8>>>::new();
  for i in 0..10_000 {
    let key = format!("k_{i}").into_bytes();
    let b = index.bucket_index_for_hash(whasher::scoped_hash(prefix.as_slice(), &key));
    let list = buckets.entry(b).or_default();
    list.push(key);
    if list.len() >= 3 && buckets.len() >= 2 {
      // 找到两个桶，一个有 >=3 键，另一个有 >=2 键
      let mut found_b0 = None;
      let mut found_b1 = None;
      for (&b_idx, k_list) in &buckets {
        if k_list.len() >= 3 && found_b0.is_none() {
          found_b0 = Some((b_idx, k_list.clone()));
        } else if k_list.len() >= 2 && found_b1.is_none() {
          found_b1 = Some((b_idx, k_list.clone()));
        }
      }
      if let (Some(b0), Some(b1)) = (found_b0, found_b1) {
        return (b0.0, b0.1, b1.0, b1.1);
      }
    }
  }
  panic!("未能生成落入目标桶的键");
}

/// 同桶多键折叠计数断言（闩获取次数 = distinct 桶数）
#[test]
fn test_rmw_window_sorted_bucket_folding() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store("rmw_sorted_folding", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let (b0, k_b0, b1, k_b1) = find_keys_for_buckets(&store, &SessionPrefixBuf::ROOT);
    let index = store.active_index();

    // 1. 同一桶 3 个键：必须折叠为 1 个窗口，且仅取 1 次排他闩
    let keys_same_bucket = [&k_b0[0][..], &k_b0[1][..], &k_b0[2][..]];
    let windows = batch
      .try_rmw_window_sorted(keys_same_bucket)
      .expect("try_rmw_window_sorted 同桶 3 键必成功");
    assert_eq!(
      windows.len(),
      1,
      "同桶 3 键必须折叠为 1 个窗口（distinct 桶数 = 1）"
    );
    assert_eq!(
      windows[0].held.as_ref().map(|(_, b)| *b),
      Some(b0),
      "窗口持有的桶号必须与目标桶一致"
    );
    // 验证此时该桶排他闩已被占用
    assert!(
      !index.bucket(b0).try_lock_exclusive(),
      "窗口持有期间该桶排他闩不可复取"
    );
    drop(windows);
    // 验证退窗后排他闩已释放
    assert!(
      index.bucket(b0).try_lock_exclusive(),
      "退窗后该桶排他闩必须已释放"
    );
    index.bucket(b0).unlock_exclusive();

    // 2. 跨 2 个桶 5 个键（桶 0 有 3 键，桶 1 有 2 键）：必须折叠为 2 个窗口
    let keys_two_buckets = [
      &k_b0[0][..],
      &k_b1[0][..],
      &k_b0[1][..],
      &k_b1[1][..],
      &k_b0[2][..],
    ];
    let windows = batch
      .try_rmw_window_sorted(keys_two_buckets)
      .expect("try_rmw_window_sorted 跨 2 桶 5 键必成功");
    assert_eq!(
      windows.len(),
      2,
      "跨 2 桶 5 键必须折叠为 2 个窗口（distinct 桶数 = 2）"
    );
    // 验证桶序升序定序
    let bucket_held_0 = windows[0].held.as_ref().unwrap().1;
    let bucket_held_1 = windows[1].held.as_ref().unwrap().1;
    assert!(
      bucket_held_0 < bucket_held_1,
      "窗口持有桶序必须按桶下标严格升序"
    );
    // 验证两桶排他闩均被占用
    assert!(!index.bucket(b0).try_lock_exclusive());
    assert!(!index.bucket(b1).try_lock_exclusive());
    drop(windows);
    // 验证两桶排他闩均已释放
    assert!(index.bucket(b0).try_lock_exclusive());
    index.bucket(b0).unlock_exclusive();
    assert!(index.bucket(b1).try_lock_exclusive());
    index.bucket(b1).unlock_exclusive();

    // 3. 单键直接内联路径
    let single = batch
      .try_rmw_window_sorted([&k_b0[0][..]])
      .expect("单键取窗必成");
    assert_eq!(single.len(), 1);
    drop(single);

    // 4. 空键直接返回空
    let empty = batch
      .try_rmw_window_sorted(iter::empty::<&[u8]>())
      .expect("空键取窗必成");
    assert_eq!(empty.len(), 0);

    OK
  })
}

/// rmw_window_sorted 争用轮次内 plan 仅首轮构建一次的断言
///
/// 争用与放行全由计划可观测点握手裁定，不靠裸 sleep 赌时序：单轮取闩的自旋预算
/// （1024 次）在 CPU 超售下实测可拖到数十毫秒，sleep 划出的「先争用后放行」两段
/// 序会塌缩成首轮即取闩成功，争用面根本不被触达
#[test]
fn test_rmw_window_sorted_cached_plan_under_contention() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_sorted_plan_cached",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let (_b0, k_b0, b1, k_b1) = find_keys_for_buckets(&store, &SessionPrefixBuf::ROOT);
    let index = store.active_index();
    let pinned_old = Arc::as_ptr(&index) as usize;

    // 预先占住 b1 桶的排他闩，制造争用
    assert!(
      index.bucket(b1).try_lock_exclusive(),
      "预占 b1 桶排他闩必须成功"
    );

    RMW_PLAN_BUILD_COUNT.store(0, Ordering::Relaxed);
    RMW_PLAN_ACQUIRE_MISS.store(0, Ordering::Relaxed);
    RMW_PLAN_PINNED_INDEX.store(0, Ordering::Relaxed);

    let index_clone = index.clone();
    // 后台线程仅在确证「计划钉在本旧表且已失闩至少一个整轮」后放闩
    let handle = thread::spawn(move || {
      wait_for("首轮计划钉旧表并失闩一轮", || {
        RMW_PLAN_PINNED_INDEX.load(Ordering::Relaxed) == pinned_old
          && RMW_PLAN_ACQUIRE_MISS.load(Ordering::Relaxed) >= 1
      });
      index_clone.bucket(b1).unlock_exclusive();
    });

    let keys = [&k_b0[0][..], &k_b1[0][..]];
    let windows = batch
      .rmw_window_sorted(keys)
      .await
      .expect("争用解除后异步持窗必成");
    handle.join().unwrap();

    assert_eq!(windows.len(), 2, "获取到两个桶的窗口");

    let plan_builds = RMW_PLAN_BUILD_COUNT.load(Ordering::Relaxed);
    // 铁律断言：经历争用轮次重试的情况下 plan 仅构建 1 次（循环内零重哈希、
    // 零重排、零重建），且争用确凿发生而非首轮即成的空转通过
    assert_eq!(
      plan_builds, 1,
      "争用轮次内 plan 必须仅在首轮构建一次，实际构建次数: {plan_builds}"
    );
    assert!(
      RMW_PLAN_ACQUIRE_MISS.load(Ordering::Relaxed) >= 1,
      "争用必须至少让出一个整轮"
    );

    drop(windows);

    OK
  })
}

/// 索引版本推进（扩容）时计划失效重建断言
///
/// 编排全走可观测点握手：先确证首轮计划钉在被替换前的旧表上（此刻旧表 b1 桶闩
/// 仍由本测试占住，该计划只可能失闩重试），才切上 2 倍容量新表；再确证重试循环
/// 按新表重建了计划，才放旧表闩。裸 sleep 在负载下两头都塌：扩容若落在首轮构建
/// 之前，计划自始钉新表（新表桶闩全空，预占旧表桶根本不构成交争），若落在持闩
/// 自旋中途，则 1024 次自旋预算实测可跨过数十毫秒，失闩期间以旧表计划取闩成功
#[test]
fn test_rmw_window_sorted_plan_rebuild_on_version_advance() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_sorted_plan_rebuild",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let (_b0, k_b0, b1, k_b1) = find_keys_for_buckets(&store, &SessionPrefixBuf::ROOT);
    let old_index = store.active_index();
    let pinned_old = Arc::as_ptr(&old_index) as usize;

    // 预占旧表 b1 桶排他闩，且直至计划重建确证后才放——旧表计划绝无不争用而
    // 交回的路径，交回即必为新表计划
    assert!(
      old_index.bucket(b1).try_lock_exclusive(),
      "预占旧表 b1 桶排他闩必须成功"
    );

    RMW_PLAN_BUILD_COUNT.store(0, Ordering::Relaxed);
    RMW_PLAN_PINNED_INDEX.store(0, Ordering::Relaxed);

    let store_clone = store.clone();
    let index_clone = old_index.clone();
    let handle = thread::spawn(move || {
      wait_for("首轮计划钉旧表", || {
        RMW_PLAN_PINNED_INDEX.load(Ordering::Relaxed) == pinned_old
      });
      // 推进索引版本：切上 2 倍容量新表并装配在途分块迁移窗
      stage_resize(&store_clone, true, ResizePhase::InProgressGrow);
      let pinned_new = Arc::as_ptr(&store_clone.active_index()) as usize;
      wait_for("版本推进后计划失效重建", || {
        RMW_PLAN_PINNED_INDEX.load(Ordering::Relaxed) == pinned_new
      });
      index_clone.bucket(b1).unlock_exclusive();
    });

    let keys = [&k_b0[0][..], &k_b1[0][..]];
    let windows = batch
      .rmw_window_sorted(keys)
      .await
      .expect("版本推进后持窗必成");
    handle.join().unwrap();

    assert_eq!(windows.len(), 2, "跨 2 桶 2 键必须折叠为 2 个窗口");
    let plan_builds = RMW_PLAN_BUILD_COUNT.load(Ordering::Relaxed);
    assert!(
      plan_builds >= 2,
      "版本推进后 plan 必须失效并重建一次，实际构建次数: {plan_builds}"
    );
    // 重建的确证判据：交回的桶闩全钉在当前活跃表上——旧表桶闩在半迁移期对同键
    // 新表持闩者根本不构成互斥，落在旧表即等于裸写
    let cur_index = store.active_index();
    assert!(
      !Arc::ptr_eq(&cur_index, &old_index),
      "装配扩容窗后活跃表必已换代"
    );
    for window in &windows {
      let (pinned, _) = window.held.as_ref().expect("非事务会话取窗必持本桶排他闩");
      assert!(
        Arc::ptr_eq(pinned, &cur_index),
        "失效重建后的窗口必须钉在当前活跃表上"
      );
    }

    drop(windows);
    finish_resize_window(&store);

    OK
  })
}
