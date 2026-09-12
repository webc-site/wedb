use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use parking_lot::Mutex;

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

/// libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:ReplicationNetworkBufferSettings
///
/// 网络发送缓冲池与背压水位门控器
#[derive(Debug)]
pub struct ReplicationSendBufferPool {
  pool: Mutex<Vec<ReplicationSendBuffer>>,
  buffer_capacity: usize,
  max_pool_slots: usize,
  max_inflight_bytes: i64,
  current_inflight_bytes: AtomicI64,
  borrow_count: AtomicUsize,
}

impl ReplicationSendBufferPool {
  /// 创建新的网络发送缓冲池，预置定长槽位
  pub fn new(slots: usize, buffer_capacity: usize, max_inflight_bytes: i64) -> Self {
    let mut initial_buffers = Vec::with_capacity(slots);
    for _ in 0..slots {
      initial_buffers.push(ReplicationSendBuffer::new(buffer_capacity));
    }
    Self {
      pool: Mutex::new(initial_buffers),
      buffer_capacity,
      max_pool_slots: slots.max(DEFAULT_RING_BUFFER_SLOTS) * 2,
      max_inflight_bytes,
      current_inflight_bytes: AtomicI64::new(0),
      borrow_count: AtomicUsize::new(0),
    }
  }

  /// 借出一个可复用的定长发送缓冲（若池为空则按规约扩容创建）
  pub fn acquire(&self) -> ReplicationSendBuffer {
    self.borrow_count.fetch_add(1, Ordering::Relaxed);
    let mut guard = self.pool.lock();
    guard
      .pop()
      .unwrap_or_else(|| ReplicationSendBuffer::new(self.buffer_capacity))
  }

  /// 归还缓冲区以供后续流同步复用
  pub fn release(&self, mut buf: ReplicationSendBuffer) {
    buf.reset();
    let mut guard = self.pool.lock();
    if guard.len() < self.max_pool_slots {
      guard.push(buf);
    }
  }

  /// 登记在途网络传输字节数并返回是否触及背压上限
  pub fn track_inflight_send(&self, bytes: usize) -> bool {
    if bytes == 0 {
      return self.is_throttled();
    }
    let new_val = self
      .current_inflight_bytes
      .fetch_add(bytes as i64, Ordering::AcqRel)
      + bytes as i64;
    new_val >= self.max_inflight_bytes
  }

  /// 对端位点确认时释放已确认在途字节数（CAS 循环保护防止负数下溢）
  pub fn acknowledge_inflight(&self, bytes: usize) {
    if bytes == 0 {
      return;
    }
    let mut cur = self.current_inflight_bytes.load(Ordering::Acquire);
    loop {
      let next = (cur - bytes as i64).max(0);
      match self.current_inflight_bytes.compare_exchange_weak(
        cur,
        next,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => break,
        Err(actual) => cur = actual,
      }
    }
  }

  /// 当前在途未确认字节数
  #[inline]
  pub fn current_inflight_bytes(&self) -> i64 {
    self.current_inflight_bytes.load(Ordering::Acquire)
  }

  /// 是否需要执行网络背压等待
  #[inline]
  pub fn is_throttled(&self) -> bool {
    self.current_inflight_bytes.load(Ordering::Acquire) >= self.max_inflight_bytes
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
}
