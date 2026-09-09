//! 原生直接虚拟内存追踪器 (对标 C# Tsavorite `NativeMemoryTracker.cs`)
//!
//! 全局追踪通过原生直接虚拟内存分配器 (mmap/VirtualAlloc) 占用的物理/虚拟内存字节数，
//! 供 Redis `INFO memory` 遥测 (如 `used_memory_native`) 及容器内存上限监控使用。
//!
//! 采用条带化 (Striped) 128 字节缓存行对齐 (Cache-line Padded) 的无锁原子计数器，
//! 消除多核心高并发分配/释放时的伪共享 (False Sharing)。

use wbase::{striped::StripedCounter, thread::current_thread_id};

static COUNTER: StripedCounter<64> = StripedCounter::new();

/// 原生直接虚拟内存全局追踪器 (对标 C# `NativeMemoryTracker`)
#[derive(Debug, Clone, Copy)]
pub struct NativeMemoryTracker;

impl NativeMemoryTracker {
  /// 获取当前已分配的原生虚拟内存总字节数 (对标 C# `NativeMemoryTracker.Bytes`)
  #[must_use]
  #[inline]
  pub fn bytes() -> usize {
    COUNTER.get_positive()
  }

  /// 记录一次直接虚拟内存分配 (对标 C# `NativeMemoryTracker.Add`)
  #[inline]
  pub(crate) fn add(delta: usize) {
    COUNTER.add(current_thread_id() as usize, delta as i64);
  }

  /// 记录一次直接虚拟内存释放 (对标 C# `NativeMemoryTracker.Subtract`)
  #[inline]
  pub(crate) fn subtract(delta: usize) {
    COUNTER.sub(current_thread_id() as usize, delta as i64);
  }
}
