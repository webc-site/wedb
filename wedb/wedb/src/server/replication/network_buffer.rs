use std::{
  fmt,
  ops::Deref,
  sync::atomic::{AtomicI64, AtomicUsize, Ordering},
};

use crossfire::flavor::{Array, Queue};

/// 最大单次 AOF 分包大小：1MB（对标 Garnet maxChunkSize: 1 << 20）
pub const MAX_CHUNK_SIZE: usize = 1 << 20;

/// 默认单个发送缓冲区容量：1MB
pub const DEFAULT_SEND_BUFFER_SIZE: usize = 1 << 20;

/// 默认环形池容量槽位数
pub const DEFAULT_RING_BUFFER_SLOTS: usize = 4;

/// libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:ReplicationNetworkBufferSettings
///
/// 增量流复制定长网络发送缓冲区，支持零分配切片写入与水位跟踪
#[derive(Debug)]
pub struct ReplicationSendBuffer {
  buffer: Vec<u8>,
  capacity: usize,
}

impl ReplicationSendBuffer {
  /// 创建指定容量的预分配定长缓冲区（零 memset 初始开销）
  pub fn new(capacity: usize) -> Self {
    Self {
      buffer: Vec::with_capacity(capacity),
      capacity,
    }
  }

  /// 缓冲区容量（字节）
  #[inline]
  pub fn capacity(&self) -> usize {
    self.capacity
  }

  /// 当前已写入字节数
  #[inline]
  pub fn len(&self) -> usize {
    self.buffer.len()
  }

  /// 缓冲区是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.buffer.is_empty()
  }

  /// 剩余可用空间（字节）
  #[inline]
  pub fn remaining(&self) -> usize {
    self.capacity.saturating_sub(self.buffer.len())
  }

  /// 重置写指针以复用内存，保持容量且避免重新堆分配
  #[inline]
  pub fn reset(&mut self) {
    self.buffer.clear();
  }

  /// 尝试向缓冲区写入数据切片；若容量不足则返回实际写入字节数
  pub fn write(&mut self, data: &[u8]) -> usize {
    let to_copy = data.len().min(self.remaining());
    if to_copy > 0 {
      self.buffer.extend_from_slice(&data[..to_copy]);
    }
    to_copy
  }

  /// 获取当前已写入有效字节切片视图
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.buffer[..]
  }
}

impl Deref for ReplicationSendBuffer {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl AsRef<[u8]> for ReplicationSendBuffer {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

/// libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:ReplicationNetworkBufferSettings
///
/// 网络发送缓冲池与背压水位门控器（基于 crossfire::flavor::Array 纯原子无锁架构实现零锁复用）
pub struct ReplicationSendBufferPool {
  queue: Array<ReplicationSendBuffer>,
  buffer_capacity: usize,
  max_inflight_bytes: i64,
  current_inflight_bytes: AtomicI64,
  borrow_count: AtomicUsize,
}

impl fmt::Debug for ReplicationSendBufferPool {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ReplicationSendBufferPool")
      .field("buffer_capacity", &self.buffer_capacity)
      .field("max_inflight_bytes", &self.max_inflight_bytes)
      .field(
        "current_inflight_bytes",
        &self.current_inflight_bytes.load(Ordering::Relaxed),
      )
      .field("borrow_count", &self.borrow_count.load(Ordering::Relaxed))
      .field("queued_buffers", &self.queue.len())
      .finish()
  }
}

impl ReplicationSendBufferPool {
  /// 创建新的网络发送缓冲池，预置定长槽位
  pub fn new(slots: usize, buffer_capacity: usize, max_inflight_bytes: i64) -> Self {
    let max_pool_slots = slots.max(DEFAULT_RING_BUFFER_SLOTS) * 2;
    let queue = Array::new(max_pool_slots);
    for _ in 0..slots {
      let _ = queue.push(ReplicationSendBuffer::new(buffer_capacity));
    }
    Self {
      queue,
      buffer_capacity,
      max_inflight_bytes,
      current_inflight_bytes: AtomicI64::new(0),
      borrow_count: AtomicUsize::new(0),
    }
  }

  /// 借出一个可复用的定长发送缓冲（若池为空则按规约扩容创建）
  pub fn acquire(&self) -> ReplicationSendBuffer {
    self.borrow_count.fetch_add(1, Ordering::Relaxed);
    self
      .queue
      .pop()
      .unwrap_or_else(|| ReplicationSendBuffer::new(self.buffer_capacity))
  }

  /// 归还缓冲区以供后续流同步复用（零锁归还，非标容量或队列满时自动丢弃）
  pub fn release(&self, mut buf: ReplicationSendBuffer) {
    if buf.capacity() != self.buffer_capacity {
      return;
    }
    buf.reset();
    let _ = self.queue.push(buf);
  }

  /// 累计借出次数统计
  #[inline]
  pub fn borrow_count(&self) -> usize {
    self.borrow_count.load(Ordering::Relaxed)
  }

  /// 登记在途网络传输字节数并返回是否触及背压上限
  pub fn track_inflight_send(&self, bytes: usize) -> bool {
    if bytes == 0 {
      return self.is_throttled();
    }
    let bytes_i64 = i64::try_from(bytes).unwrap_or(i64::MAX);
    let new_val = self
      .current_inflight_bytes
      .fetch_add(bytes_i64, Ordering::AcqRel)
      + bytes_i64;
    new_val >= self.max_inflight_bytes
  }

  /// 对端位点确认时释放已确认在途字节数（对称无锁原子减）
  pub fn acknowledge_inflight(&self, bytes: usize) {
    if bytes == 0 {
      return;
    }
    let bytes_i64 = i64::try_from(bytes).unwrap_or(i64::MAX);
    self
      .current_inflight_bytes
      .fetch_sub(bytes_i64, Ordering::AcqRel);
  }

  /// 当前在途未确认字节数（规整下界非负）
  #[inline]
  pub fn current_inflight_bytes(&self) -> i64 {
    self.current_inflight_bytes.load(Ordering::Acquire).max(0)
  }

  /// 是否需要执行网络背压等待
  #[inline]
  pub fn is_throttled(&self) -> bool {
    self.current_inflight_bytes() >= self.max_inflight_bytes
  }
}

impl Default for ReplicationSendBufferPool {
  fn default() -> Self {
    Self::new(
      DEFAULT_RING_BUFFER_SLOTS,
      DEFAULT_SEND_BUFFER_SIZE,
      (DEFAULT_SEND_BUFFER_SIZE * 4) as i64,
    )
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;

  #[test]
  fn test_send_buffer_reuse_and_backpressure() {
    let pool = Arc::new(ReplicationSendBufferPool::new(2, 1024, 2048));
    let mut buf = pool.acquire();
    assert_eq!(buf.capacity(), 1024);
    assert_eq!(buf.remaining(), 1024);

    let data = b"Hello Aof Stream Replication";
    let written = buf.write(data);
    assert_eq!(written, data.len());
    assert_eq!(buf.as_slice(), data);

    // 测试在途背压水位
    assert!(!pool.track_inflight_send(1000));
    assert!(!pool.is_throttled());
    assert!(pool.track_inflight_send(1200)); // 累计 2200 >= 2048
    assert!(pool.is_throttled());

    // ACK 确认释放
    pool.acknowledge_inflight(1500);
    assert_eq!(pool.current_inflight_bytes(), 700);
    assert!(!pool.is_throttled());

    // 归还复用
    pool.release(buf);
    let buf2 = pool.acquire();
    assert_eq!(buf2.len(), 0);
    assert_eq!(buf2.remaining(), 1024);
  }

  #[test]
  fn test_pool_exhaustion_and_overflow() {
    let pool = ReplicationSendBufferPool::new(1, 512, 1024);
    assert_eq!(pool.borrow_count(), 0);

    // 槽位只有 1，取出第 1 个
    let buf1 = pool.acquire();
    // 池已空，降级分配第 2 个
    let buf2 = pool.acquire();
    assert_eq!(buf1.capacity(), 512);
    assert_eq!(buf2.capacity(), 512);
    assert_eq!(pool.borrow_count(), 2);

    // 尝试归还异构容量的 buffer，应被静默丢弃不污染池
    let foreign_buf = ReplicationSendBuffer::new(1024);
    pool.release(foreign_buf);
    let acquired = pool.acquire();
    assert_eq!(acquired.capacity(), 512);

    // 归还超过 max_pool_slots (1.max(4) * 2 = 8)
    for _ in 0..16 {
      pool.release(ReplicationSendBuffer::new(512));
    }
    // 正常取回
    let buf3 = pool.acquire();
    assert_eq!(buf3.capacity(), 512);
  }

  #[test]
  fn test_early_ack_does_not_drift() {
    let pool = ReplicationSendBufferPool::new(2, 512, 1024);
    // 高速网络下 ACK 先于主端记账到达
    pool.acknowledge_inflight(500);
    assert_eq!(pool.current_inflight_bytes(), 0);

    // 随后主端记账到达，净水位正确归零
    pool.track_inflight_send(500);
    assert_eq!(pool.current_inflight_bytes(), 0);
  }

  #[test]
  fn test_buffer_deref_and_as_ref() {
    let mut buf = ReplicationSendBuffer::new(64);
    buf.write(b"data");
    assert_eq!(&buf[..], b"data");
    assert_eq!(buf.as_ref(), b"data");
  }

  #[test]
  fn test_concurrent_acquire_release() {
    use std::thread;

    let pool = Arc::new(ReplicationSendBufferPool::new(4, 256, 4096));
    let mut handles = Vec::new();

    for _ in 0..8 {
      let pool_clone = Arc::clone(&pool);
      handles.push(thread::spawn(move || {
        for _ in 0..100 {
          let mut buf = pool_clone.acquire();
          assert_eq!(buf.capacity(), 256);
          buf.write(b"ping");
          pool_clone.release(buf);
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }
  }
}
