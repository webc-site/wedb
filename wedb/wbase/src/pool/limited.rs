//! 网络定长缓冲区池化管理
//!
//! 1:1 对标微软 Garnet LimitedFixedBufferPool（libs/common/Memory/LimitedFixedBufferPool.cs）
//!
//! 统一管理收发网络缓冲区，避免频繁堆分配与内存抖动。默认块大小 64KB (1 << 16)；
//! 提供快速借出、RAII 自动归还复用、Purge 清理及统计指标导出。
//!
//! 队列选型：C# 侧每层池为 `ConcurrentQueue<PoolEntry>`（libs/common/Memory/PoolLevel.cs:16），
//! 借出/归还是每 I/O 两次的纯非阻塞 TryDequeue/Enqueue（LimitedFixedBufferPool.cs:158/:120），
//! 竞争激烈且无需异步等待 → crossfire::flavor::Array（有界 MPMC 环，`Queue` trait 的
//! push/pop 即 TryDequeue/Enqueue 的零 CAS 重试等价物），容量上限语义对应 C#
//! `Interlocked.Increment(size) <= maxEntriesPerLevel` 的入池裁断（LimitedFixedBufferPool.cs:117）。

use std::{
  fmt,
  ops::{Deref, DerefMut},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::Relaxed},
  },
};

use crossfire::flavor::{Array, Queue};

use crate::align::CachePadded;

/// 默认网络缓冲区大小：64KB
pub const DEFAULT_BUFFER_SIZE: usize = 1 << 16;
/// 池内最大常驻闲置缓冲数
pub const DEFAULT_MAX_POOL_SIZE: usize = 1024;

/// 池化借出的缓冲区句柄（RAII 自动归还）
pub struct PooledBuffer {
  buffer: Option<Vec<u8>>,
  pool: Arc<LimitedFixedBufferPool>,
}

impl PooledBuffer {
  /// 获取底层切片引用
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.buffer.as_deref().unwrap_or(&[])
  }

  /// 获取底层可变切片引用
  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    self.buffer.as_deref_mut().unwrap_or(&mut [])
  }

  /// 获取底层 Vec 引用
  #[inline]
  pub fn vec_ref(&self) -> &Vec<u8> {
    self.buffer.as_ref().expect("pooled buffer active")
  }

  /// 获取底层可变 Vec 引用
  #[inline]
  pub fn vec_mut(&mut self) -> &mut Vec<u8> {
    self.buffer.as_mut().expect("pooled buffer active")
  }

  /// 提取底层 Vec 并放弃自动归还池（析构正常递减借出计数并释放 pool Arc，杜绝 Arc 泄漏）
  pub fn take(mut self) -> Vec<u8> {
    self.buffer.take().unwrap_or_default()
  }

  /// 临时提取底层 Vec 用于所有权转移（如异步 I/O），后续须通过 [`Self::set_buffer`] 归还
  #[inline]
  pub fn take_buffer(&mut self) -> Option<Vec<u8>> {
    self.buffer.take()
  }

  /// 归还或更新底层 Vec
  #[inline]
  pub fn set_buffer(&mut self, buf: Vec<u8>) {
    self.buffer = Some(buf);
  }
}

impl Deref for PooledBuffer {
  type Target = Vec<u8>;
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.vec_ref()
  }
}

impl DerefMut for PooledBuffer {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.vec_mut()
  }
}

impl Drop for PooledBuffer {
  fn drop(&mut self) {
    if let Some(buf) = self.buffer.take() {
      self.pool.return_buffer(buf);
    } else {
      self.pool.borrowed_count.fetch_sub(1, Relaxed);
    }
  }
}

impl fmt::Debug for PooledBuffer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("PooledBuffer")
      .field("len", &self.as_slice().len())
      .field(
        "capacity",
        &self.buffer.as_ref().map(|v| v.capacity()).unwrap_or(0),
      )
      .finish()
  }
}

/// 基于借用的池化缓冲区句柄（零 Arc 开销，RAII 自动归还）
pub struct PooledRefBuffer<'a> {
  buffer: Option<Vec<u8>>,
  pool: &'a LimitedFixedBufferPool,
}

impl<'a> PooledRefBuffer<'a> {
  /// 获取底层切片引用
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.buffer.as_deref().unwrap_or(&[])
  }

  /// 获取底层可变切片引用
  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    self.buffer.as_deref_mut().unwrap_or(&mut [])
  }

  /// 获取底层 Vec 引用
  #[inline]
  pub fn vec_ref(&self) -> &Vec<u8> {
    self.buffer.as_ref().expect("pooled buffer active")
  }

  /// 获取底层可变 Vec 引用
  #[inline]
  pub fn vec_mut(&mut self) -> &mut Vec<u8> {
    self.buffer.as_mut().expect("pooled buffer active")
  }

  /// 提取底层 Vec 并放弃自动归还池
  pub fn take(mut self) -> Vec<u8> {
    self.buffer.take().unwrap_or_default()
  }

  /// 临时提取底层 Vec 用于所有权转移（如异步 I/O），后续须通过 [`Self::set_buffer`] 归还
  #[inline]
  pub fn take_buffer(&mut self) -> Option<Vec<u8>> {
    self.buffer.take()
  }

  /// 归还或更新底层 Vec
  #[inline]
  pub fn set_buffer(&mut self, buf: Vec<u8>) {
    self.buffer = Some(buf);
  }
}

impl<'a> Deref for PooledRefBuffer<'a> {
  type Target = Vec<u8>;
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.vec_ref()
  }
}

impl<'a> DerefMut for PooledRefBuffer<'a> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.vec_mut()
  }
}

impl<'a> Drop for PooledRefBuffer<'a> {
  fn drop(&mut self) {
    if let Some(buf) = self.buffer.take() {
      self.pool.return_buffer(buf);
    } else {
      self.pool.borrowed_count.fetch_sub(1, Relaxed);
    }
  }
}

impl<'a> fmt::Debug for PooledRefBuffer<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("PooledRefBuffer")
      .field("len", &self.as_slice().len())
      .field(
        "capacity",
        &self.buffer.as_ref().map(|v| v.capacity()).unwrap_or(0),
      )
      .finish()
  }
}

/// 固定大小网络缓冲池（基于 crossfire::flavor::Array 纯原子无锁架构）
pub struct LimitedFixedBufferPool {
  queue: Array<Vec<u8>>,
  buffer_size: usize,
  max_pool_size: usize,
  allocated_count: CachePadded<AtomicUsize>,
  pub(crate) borrowed_count: CachePadded<AtomicUsize>,
}

impl fmt::Debug for LimitedFixedBufferPool {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LimitedFixedBufferPool")
      .field("buffer_size", &self.buffer_size)
      .field("max_pool_size", &self.max_pool_size)
      .field("borrowed_count", &self.borrowed_count())
      .field("free_count", &self.free_count())
      .finish()
  }
}

impl LimitedFixedBufferPool {
  /// 创建网络缓冲池
  pub fn new(buffer_size: usize, max_pool_size: usize) -> Arc<Self> {
    let size = if buffer_size == 0 {
      DEFAULT_BUFFER_SIZE
    } else {
      buffer_size
    };
    let cap = if max_pool_size == 0 {
      DEFAULT_MAX_POOL_SIZE
    } else {
      max_pool_size
    };

    Arc::new(Self {
      queue: Array::new(cap),
      buffer_size: size,
      max_pool_size: cap,
      allocated_count: CachePadded::new(AtomicUsize::new(0)),
      borrowed_count: CachePadded::new(AtomicUsize::new(0)),
    })
  }

  #[inline]
  fn alloc_or_pop(&self, min_size: usize) -> Vec<u8> {
    self.borrowed_count.fetch_add(1, Relaxed);
    let target_size = min_size.max(self.buffer_size);

    let buf = if target_size == self.buffer_size {
      self.queue.pop()
    } else {
      None
    };

    match buf {
      Some(v) => v,
      None => {
        self.allocated_count.fetch_add(1, Relaxed);
        Vec::with_capacity(target_size)
      }
    }
  }

  /// 借出一个指定最小容量的缓冲区
  pub fn get(self: &Arc<Self>, min_size: usize) -> PooledBuffer {
    let buffer = self.alloc_or_pop(min_size);
    PooledBuffer {
      buffer: Some(buffer),
      pool: Arc::clone(self),
    }
  }

  /// 借出一个指定最小容量的缓冲区句柄（零 Arc 开销，借用底层缓冲池生命周期）
  #[inline]
  pub fn get_ref(&self, min_size: usize) -> PooledRefBuffer<'_> {
    let buffer = self.alloc_or_pop(min_size);
    PooledRefBuffer {
      buffer: Some(buffer),
      pool: self,
    }
  }

  /// 归还缓冲区到底层池
  pub fn return_buffer(&self, mut buf: Vec<u8>) {
    self.borrowed_count.fetch_sub(1, Relaxed);
    if buf.capacity() == self.buffer_size {
      buf.clear();
      let _ = self.queue.push(buf);
    }
  }

  /// 清空池内所有闲置缓冲区
  pub fn purge(&self) {
    while self.queue.pop().is_some() {}
  }

  /// 当前池中闲置可用缓冲区数量
  #[inline]
  pub fn free_count(&self) -> usize {
    self.queue.len()
  }

  /// 当前借出中的缓冲区数量
  #[inline]
  pub fn borrowed_count(&self) -> usize {
    self.borrowed_count.load(Relaxed)
  }

  /// 累计分配的新缓冲区总数
  #[inline]
  pub fn allocated_count(&self) -> usize {
    self.allocated_count.load(Relaxed)
  }

  /// 最大常驻闲置缓冲数
  #[inline]
  pub fn max_pool_size(&self) -> usize {
    self.max_pool_size
  }
}

#[cfg(test)]
mod tests {
  use std::{mem::align_of, thread};

  use super::*;

  #[test]
  fn test_buffer_pool_cache_line_alignment() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    assert_eq!(align_of::<CachePadded<AtomicUsize>>(), 128);
    let alloc_addr = &*pool.allocated_count as *const AtomicUsize as usize;
    let borrow_addr = &*pool.borrowed_count as *const AtomicUsize as usize;
    assert!(alloc_addr.abs_diff(borrow_addr) >= 128);
  }

  #[test]
  fn test_buffer_pool_reuse_and_purge() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.free_count(), 0);

    {
      let mut b1 = pool.get(512);
      assert_eq!(pool.allocated_count(), 1);
      assert_eq!(pool.borrowed_count(), 1);
      b1.extend_from_slice(b"hello world");
      assert_eq!(b1.as_slice(), b"hello world");
    }

    // Drop 后自动归还
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 1);

    // 再次借出应复用
    {
      let b2 = pool.get(1024);
      assert_eq!(pool.allocated_count(), 1);
      assert_eq!(pool.free_count(), 0);
      assert!(b2.is_empty());
    }

    assert_eq!(pool.free_count(), 1);
    pool.purge();
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_take_and_drop() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    assert_eq!(Arc::strong_count(&pool), 1);
    let b = pool.get(1024);
    assert_eq!(Arc::strong_count(&pool), 2);
    assert_eq!(pool.borrowed_count(), 1);

    // take 后主动放弃归还
    let raw = b.take();
    assert_eq!(raw.capacity(), 1024);
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
    assert_eq!(Arc::strong_count(&pool), 1);
  }

  #[test]
  fn test_buffer_pool_take_buffer_and_set_buffer() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    let mut b = pool.get(1024);
    assert_eq!(pool.borrowed_count(), 1);

    // 模拟所有权临时移交给底层异步 I/O (如 Read)
    let raw = b.take_buffer().expect("buffer should exist");
    assert!(b.buffer.is_none());

    // 异步完成归还
    b.set_buffer(raw);
    assert!(b.buffer.is_some());

    drop(b);
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 1);
  }

  #[test]
  fn test_buffer_pool_dropped_without_set_buffer() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    let mut b = pool.get(1024);
    assert_eq!(pool.borrowed_count(), 1);

    // 提取后若发生异常丢弃，未 set_buffer 归还
    let _raw = b.take_buffer();
    drop(b);

    // 依然正确扣减借出计数，避免计数永久泄漏
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_oversized_allocation() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    {
      let b = pool.get(2048);
      assert!(b.capacity() >= 2048);
      assert_eq!(pool.borrowed_count(), 1);
      assert_eq!(pool.allocated_count(), 1);
    }

    // 归还超规缓冲不会入池
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_capacity_limit() {
    let pool = LimitedFixedBufferPool::new(1024, 2);
    let b1 = pool.get(1024);
    let b2 = pool.get(1024);
    let b3 = pool.get(1024);

    drop(b1);
    drop(b2);
    assert_eq!(pool.free_count(), 2);

    // 验证池容量有界，超出 max_pool_size 的归还被丢弃
    drop(b3);
    assert_eq!(pool.free_count(), 2);
  }

  #[test]
  fn test_buffer_pool_multithread_contention() {
    let pool = LimitedFixedBufferPool::new(1024, 32);
    let mut handles = Vec::new();

    for _ in 0..8 {
      let p = Arc::clone(&pool);
      handles.push(thread::spawn(move || {
        for _ in 0..1000 {
          let mut b = p.get(1024);
          b.extend_from_slice(b"concurrent test");
          assert_eq!(b.as_slice(), b"concurrent test");
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    assert_eq!(pool.borrowed_count(), 0);
    assert!(pool.free_count() <= 32);
  }
}
