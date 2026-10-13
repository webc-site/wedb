use std::{sync::Arc, thread};

use waof::SequenceNumberGenerator;
use wbase::map::HashSet;

#[test]
fn monotonic_and_offset_lift() {
  let generator = SequenceNumberGenerator::new(0);
  let first = generator.get_sequence_number();
  let second = generator.get_sequence_number();
  let third = generator.get_sequence_number();
  assert!(
    second > first,
    "单调时钟结合原子递增保证连续取号严格单调递增"
  );
  assert!(third > second);

  // 抬升后取号不小于新起点。
  generator.set_starting_offset(1_000_000);
  assert!(generator.get_sequence_number() >= 1_000_000);
}

#[test]
fn concurrent_strict_monotonicity() {
  let generator = Arc::new(SequenceNumberGenerator::new(100));
  let thread_count = 8;
  let iterations_per_thread = 5000;

  let handles: Vec<_> = (0..thread_count)
    .map(|_| {
      let g = Arc::clone(&generator);
      thread::spawn(move || {
        let mut numbers = Vec::with_capacity(iterations_per_thread);
        for _ in 0..iterations_per_thread {
          numbers.push(g.get_sequence_number());
        }
        numbers
      })
    })
    .collect();

  let mut all_numbers = Vec::with_capacity(thread_count * iterations_per_thread);
  for h in handles {
    let nums = h.join().unwrap();
    // 单线程内严格递增
    for window in nums.windows(2) {
      assert!(
        window[1] > window[0],
        "单线程视角严格递增: {} <= {}",
        window[1],
        window[0]
      );
    }
    all_numbers.extend(nums);
  }

  // 全局互不相同（无冲突）
  let total_count = all_numbers.len();
  let unique_set: HashSet<i64> = all_numbers.into_iter().collect();
  assert_eq!(
    unique_set.len(),
    total_count,
    "并发全序严格唯一，无任何重复序列号"
  );
}

#[test]
fn display_and_boundary_offsets() {
  let generator = SequenceNumberGenerator::new(-100);
  let s = format!("{generator}");
  assert!(s.starts_with("-100,"));
  let num = generator.get_sequence_number();
  assert!(num >= -100);

  generator.set_starting_offset(i64::MAX - 1000);
  assert!(generator.get_sequence_number() >= i64::MAX - 1000);
}
