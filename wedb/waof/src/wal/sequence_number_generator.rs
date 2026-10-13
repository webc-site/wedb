//! 序列号生成器（对标 libs/common/SequenceNumberGenerator.cs:
//! SequenceNumberGenerator）。
//!
//! 对标 C# Stopwatch 高精度单调时钟，结合 Atomic 单调推进保证纳秒级精度与
//! 全序严格单调递增，杜绝粗粒度时钟高并发下同毫秒取号重复破坏全序单调。
//! 仅多物理子日志（分片）模式构造——单物理日志 + 多回放用日志地址排序，无需序列号。
//!
//! 在 garnet 中的相对路径: libs/common/SequenceNumberGenerator.cs

use std::{
  fmt,
  sync::atomic::{AtomicI64, Ordering},
  time::Instant,
};

/// libs/common/SequenceNumberGenerator.cs:SequenceNumberGenerator
///
/// 高精度单调时钟与原子推进驱动的序列号生成器。
#[derive(Debug)]
pub struct SequenceNumberGenerator {
  /// 取号基准（构造时刻高精度时钟）。
  ///
  /// 刻意保留 std::time::Instant：序列号基准对标 C# Stopwatch 高精度单调时钟，
  /// 要求纳秒级精度与全序严格单调；coarsetime 粗粒度时钟（约 1-10ms 粒度）
  /// 会令毫秒内取号全部落入 CAS +1 兜底，破坏高精度语义，故不走
  /// wbase::time 统一出口（该出口面向超时判定/剩余时长换算）。
  base_timestamp: Instant,
  /// 恢复/故障转移后的抬升偏移（时间前进保证）。
  starting_offset: AtomicI64,
  /// 上次派发的全局最大序列号（CAS 单调推进，保障全序严格单调）。
  last_sequence_number: AtomicI64,
}

impl SequenceNumberGenerator {
  /// libs/common/SequenceNumberGenerator.cs:SequenceNumberGenerator
  ///
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
