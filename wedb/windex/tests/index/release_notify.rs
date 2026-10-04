//! 放闩通知位单点判据（票 wnode-collect-arm-rmw-writer-latch-starvation-locktimeout）
//!
//! 锁定契约：[`HashBucket::unlock_exclusive`] 放闩必置第 62 位通知位，
//! [`HashBucket::consume_release_notify`] 见位即清并返回 true——等闩者预算环
//! 据此「见位不让核立即重试、未见才让核」，重试相位锚定放闩时刻，取代盲采样
//! （同键 collect 高频重入令闩空闲窗仅纳秒级占比时，盲采成败取决于采样相位，
//! 写者概率性饿死即根因）。本套件钉死三面：置位-下轮必见的确定性、位与共享
//! 计数/溢出链同字并存的位安全、跨线程等闩环的活性。真原子无 mock。

use std::{sync::Arc, thread, time::Duration};

use aok::{OK, Void};
use windex::{HashBucket, HashIndex};

/// 共享计数满员值（13 位掩码上限，第 61 位划归等闩让渡登记位、第 62 位放闩通知位；
/// 位宽回扩或高位被进位吞没即此炸出）
const SHARED_LATCH_FULL: usize = 8_191;

/// 置位-消费确定性：放闩必置位、等闩者下一查必见、消费即清、可重复置位
#[test]
fn test_release_notify_set_on_unlock_and_consumed_once() -> Void {
  let bucket = HashBucket::new();
  assert!(
    !bucket.consume_release_notify(),
    "空桶无放闩必无通知（零误报）"
  );

  assert!(bucket.try_lock_exclusive());
  assert!(
    !bucket.consume_release_notify(),
    "持闩期未放闩不得有通知（位只由放闩侧置位）"
  );
  bucket.unlock_exclusive();
  assert!(
    bucket.consume_release_notify(),
    "放闩置位，等闩者下一查必见（确定性判据本体）"
  );
  assert!(
    !bucket.consume_release_notify(),
    "通知位单粒度信号，消费即清"
  );

  assert!(bucket.try_lock_exclusive());
  bucket.unlock_exclusive();
  assert!(bucket.consume_release_notify(), "再次放闩再次置位");

  OK
}

/// 位安全：共享计数满员增减、溢出链安装均不得进位吞通知位；通知位不算持闩
#[test]
fn test_notify_bit_survives_neighbor_word_ops() -> Void {
  // 1. 共享计数压满 14 位上限后整环释放——进位吞位即此炸出
  let bucket = HashBucket::new();
  assert!(bucket.try_lock_exclusive());
  bucket.unlock_exclusive();
  let mut readers = 0;
  while bucket.try_lock_shared() {
    readers += 1;
  }
  assert_eq!(
    readers, SHARED_LATCH_FULL,
    "共享计数上限 16383（14 位），位宽回扩即位布局回归"
  );
  for _ in 0..readers {
    bucket.unlock_shared();
  }
  assert!(
    bucket.consume_release_notify(),
    "共享计数满员增减不得进位吞通知位"
  );
  assert!(!bucket.consume_release_notify());

  // 2. 溢出链安装/复取与通知位同字并存
  let bucket = HashBucket::new();
  assert!(bucket.try_lock_exclusive());
  bucket.unlock_exclusive();
  assert!(bucket.set_overflow_index(7));
  assert_eq!(bucket.overflow_index(), 7);
  assert!(!bucket.set_overflow_index(8), "已有溢出桶再装必拒");
  // 判据必须在消费前置位在时取证：位在而持闩判定面为净，方钉死「通知位不算持闩」
  //（消费后取证探不出 LATCH_MASK 误并通知位的布局回归）
  assert!(
    !bucket.is_latched() && bucket.num_latched_shared() == 0,
    "通知位不是持闩态，持闩判定面（is_latched/共享计数）不受污染"
  );
  assert!(
    bucket.consume_release_notify(),
    "溢出链 CAS 保留通知位（同字 OR 保留高位）"
  );
  assert!(
    !bucket.is_latched() && bucket.num_latched_shared() == 0,
    "消费清位不触持闩判定面"
  );

  OK
}

/// 键闩守卫放闩面：KeyLatch Drop 经 unlock_exclusive 单点放闩必置位——
/// rmw 窗口两臂、wtxn 事务键锁、TTL 键闩同守卫同放闩点，一处验证全 face 生效
#[test]
fn test_key_latch_guard_drop_sets_notify() -> Void {
  let index = HashIndex::new(16)?;
  let key = b"release_notify_key";
  let hash = HashIndex::hash_key(key);
  let bucket_idx = index.bucket_index_for_hash(hash);

  {
    let _latch = index
      .try_lock_key_hash_exclusive(hash)
      .expect("空闲桶必取键闩");
    assert!(!index.get_bucket(bucket_idx).consume_release_notify());
  }
  assert!(
    index.get_bucket(bucket_idx).consume_release_notify(),
    "键闩守卫 Drop 放闩必置位，等闩者下一查必见"
  );

  OK
}

/// 跨线程活性：等闩者「见通知位优先重试、未见让核」预算环，放闩后必在界内
/// 取得闩——真原子无 mock（钉闩下预算耗尽回 LockTimeout 的 fail-closed 面
/// 由 wkv rmw_window_single_key_latch.rs 承接，此处只证通知路径活性）
#[test]
fn test_waiter_notify_retry_liveness() -> Void {
  let bucket = Arc::new(HashBucket::new());
  assert!(bucket.try_lock_exclusive(), "夹具前提：桶须空闲");

  let waiter = {
    let bucket = Arc::clone(&bucket);
    thread::spawn(move || {
      // 等闩者预算环同形：见位即试、未见让核；轮数为活性界
      let mut rounds = 0usize;
      loop {
        if bucket.consume_release_notify() && bucket.try_lock_exclusive() {
          return rounds;
        }
        rounds += 1;
        assert!(rounds < 1_000_000, "放闩后等闩者未在界内获得重试权");
        thread::yield_now();
      }
    })
  };

  thread::sleep(Duration::from_millis(20));
  bucket.unlock_exclusive();
  let rounds = waiter.join().expect("等闩者线程必正常收敛");
  assert!(rounds >= 1, "持闩期等闩者必至少让核一轮");
  bucket.unlock_exclusive();

  OK
}

/// 等闩让渡登记位（第 61 位）契约：在册期间门控臂让位、绕开臂（登记人重试通道）
/// 仍可取闩、出册后门控臂恢复；与共享计数/通知位/独占标记同字并存互不吞没
#[test]
fn test_handoff_defers_gated_and_bypass_wins() -> Void {
  let bucket = HashBucket::new();
  assert!(!bucket.handoff_pending(), "空桶无在册等闩者");
  assert!(bucket.try_lock_exclusive(), "无在册者门控臂即裸臂");
  bucket.unlock_exclusive();

  bucket.set_handoff();
  assert!(bucket.handoff_pending());
  assert!(
    !bucket.try_lock_exclusive(),
    "新取闩者见册让位（不进入自旋，单次判定即回绝）"
  );
  assert!(
    bucket.try_lock_exclusive_now(),
    "登记人自身走绕开臂不被自家位阻挡（自见自让失败形的修正判据）"
  );
  // 持闩期共享读者按既有语义被拒；高位置位态不并入持闩判定面
  assert!(
    !bucket.try_lock_shared(),
    "持独占闩期共享读者必拒（既有语义不因位段调整而变）"
  );
  assert!(!bucket.try_lock_exclusive(), "持闩+在册仍让位");
  bucket.unlock_exclusive();
  assert!(
    bucket.handoff_pending(),
    "放闩不消登记位：出册只由登记人守卫执行"
  );
  assert!(
    bucket.consume_release_notify(),
    "放闩通知位与让渡登记位同字并存"
  );

  bucket.clear_handoff();
  assert!(!bucket.handoff_pending());
  assert!(bucket.try_lock_exclusive(), "出册后门控臂恢复放行");
  bucket.unlock_exclusive();

  OK
}

/// HashBucketEntry 位段引用面保持：通知位不侵占条目 tag/tentative 位段
///（槽位 0..7 数据条目与本通知位分属不同槽字，防位布局交叉回归）
#[test]
fn test_data_entry_layout_unaffected() -> Void {
  let bucket = HashBucket::new();
  assert!(bucket.try_insert(0, 0xABCD, 0x1234));
  assert!(bucket.try_lock_exclusive());
  bucket.unlock_exclusive();
  assert_eq!(
    bucket.find_tag_address(0xABCD),
    Some(0x1234),
    "数据槽位寻址不受溢出槽字位段调整影响"
  );
  assert!(bucket.consume_release_notify());

  OK
}
