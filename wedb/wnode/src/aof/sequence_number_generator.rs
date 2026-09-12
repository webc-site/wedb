//! 序列号生成器（对标 libs/common/SequenceNumberGenerator.cs:
//! SequenceNumberGenerator）。
//!
//! 对标 C# Stopwatch 高精度单调时钟，结合 Atomic 单调推进保证纳秒级精度与
//! 全序严格单调递增，杜绝粗粒度时钟高并发下同毫秒取号重复破坏全序单调。
//! 仅多物理子日志（分片）模式构造——单物理日志 + 多回放用日志地址排序，无需序列号。

use std::{
  fmt,
  sync::atomic::{AtomicI64, Ordering},
  time::Instant,
};

/// 高精度单调时钟与原子推进驱动的序列号生成器。
#[derive(Debug)]
pub struct SequenceNumberGenerator {
  /// 取号基准（构造时刻高精度时钟）。
  base_timestamp: Instant,
  /// 恢复/故障转移后的抬升偏移（时间前进保证）。
  starting_offset: AtomicI64,
  /// 上次派发的全局最大序列号（CAS 单调推进，保障全序严格单调）。
  last_sequence_number: AtomicI64,
}

impl SequenceNumberGenerator {
  /// 以指定起始偏移构造。
  pub fn new(starting_offset: i64) -> Self {
    Self {
      base_timestamp: Instant::now(),
      starting_offset: AtomicI64::new(starting_offset),
      last_sequence_number: AtomicI64::new(starting_offset.saturating_sub(1)),
    }
  }

  /// libs/common/SequenceNumberGenerator.cs:GetSequenceNumber
  ///
  /// 高精度单调时钟差值 + 起始偏移；结合 Atomic CAS 单调推进保证纳秒级精度与全序严格单调递增。
  #[inline]
  pub fn get_sequence_number(&self) -> i64 {
    let elapsed_nanos = self.base_timestamp.elapsed().as_nanos();
    let elapsed = i64::try_from(elapsed_nanos).unwrap_or(i64::MAX);
    let offset = self.starting_offset.load(Ordering::Acquire);
    let candidate = elapsed.saturating_add(offset);

    let mut prev = self.last_sequence_number.load(Ordering::Acquire);
    loop {
      let next = if candidate > prev {
        candidate
      } else {
        prev.saturating_add(1)
      };
      match self.last_sequence_number.compare_exchange_weak(
        prev,
        next,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => return next,
        Err(actual) => prev = actual,
      }
    }
  }

  /// 抬升起始偏移（恢复/故障转移后保证时间前进）。
  ///
  /// C# 以新对象整体替换生成器（新 base + 新 offset）；此处抬升 offset
  /// 并同步推进原子位点下界，语义等价且免热路径引用更替。
  pub fn set_starting_offset(&self, starting_offset: i64) {
    self
      .starting_offset
      .store(starting_offset, Ordering::Release);
    self
      .last_sequence_number
      .fetch_max(starting_offset.saturating_sub(1), Ordering::Release);
  }
}

impl fmt::Display for SequenceNumberGenerator {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{},{},{}",
      self.starting_offset.load(Ordering::Relaxed),
      self.last_sequence_number.load(Ordering::Relaxed),
      self.base_timestamp.elapsed().as_nanos()
    )
  }
}

#[cfg(test)]
mod tests {
  use std::{sync::Arc, thread};

  use gxhash::HashSet;

  use super::SequenceNumberGenerator;

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
}
