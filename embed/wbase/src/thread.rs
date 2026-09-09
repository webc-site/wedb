//! 高吞吐全局唯一线程标识原语
//!
//! 适配 compio thread-per-core 架构：
//! - 每个线程在首访时通过原子递增获取全局唯一 ID，之后经 TLS 纯寄存器读取（< 1ns、0 锁、0 原子操作）；
//! - 全局统一单计数器，消除多套模块各自维护计数器的锁总线争用；
//! - 线程 ID 永不复用（u64 空间保证），天然免疫 ABA 与过期槽位误判。

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);

/// 获取当前线程全局唯一且单调递增的非零线程 ID（1 起步）
#[inline]
pub fn current_thread_id() -> u64 {
  thread_local! {
    static TID: u64 = NEXT_THREAD_ID.fetch_add(1, Relaxed);
  }
  TID.with(|&id| id)
}
