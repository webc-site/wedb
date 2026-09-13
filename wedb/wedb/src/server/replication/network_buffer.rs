use std::{
  fmt,
  ops::Deref,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use wnode::{
  DEFAULT_MAX_RECEIVE_BUFFER_SIZE, LimitedFixedBufferPool, NetworkBufferSettings, PooledBuffer,
};

/// 最大单次 AOF 分包大小：1MB（对标 Garnet maxChunkSize: 1 << 20）
pub const MAX_CHUNK_SIZE: usize = 1 << 20;

/// 默认单个发送缓冲区容量：1MB
pub const DEFAULT_SEND_BUFFER_SIZE: usize = 1 << 20;

/// 默认环形池容量槽位数
pub const DEFAULT_RING_BUFFER_SLOTS: usize = 4;

/// 流复制网络配置构造器（1:1 对标 Garnet ReplicationNetworkBufferSettings.cs）
pub struct ReplicationNetworkBufferSettings;

impl ReplicationNetworkBufferSettings {
  pub const RSS_SEND_BUFFER_SIZE: usize = 1 << 20;
  pub const RSS_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 12;

  pub const IRS_SEND_BUFFER_SIZE: usize = 1 << 17;
  pub const IRS_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 17;

  pub const AOF_SYNC_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 17;

  /// 副本同步会话网络缓冲区设置（对标 C# GetRSSNetworkBufferSettings）
  #[inline]
  pub const fn rss_settings() -> NetworkBufferSettings {
    NetworkBufferSettings::new(
      Self::RSS_SEND_BUFFER_SIZE,
      Self::RSS_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }

  /// 发起副本同步网络缓冲区设置（对标 C# GetIRSNetworkBufferSettings）
  #[inline]
  pub const fn irs_settings() -> NetworkBufferSettings {
    NetworkBufferSettings::new(
      Self::IRS_SEND_BUFFER_SIZE,
      Self::IRS_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }

  /// AOF 同步任务网络缓冲区设置（对标 C# GetAofSyncNetworkBufferSettings）
  #[inline]
  pub const fn aof_sync_settings(aof_page_size_bits: u32) -> NetworkBufferSettings {
    let send_size = 2 << aof_page_size_bits;
    NetworkBufferSettings::new(
      send_size,
      Self::AOF_SYNC_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }
}

/// libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:ReplicationNetworkBufferSettings
///
/// 增量流复制定长网络发送缓冲区，直接包装 [`PooledBuffer`] 实现零分支零分配复用
#[derive(Debug)]
pub struct ReplicationSendBuffer {
  buffer: PooledBuffer,
  capacity: usize,
}

impl ReplicationSendBuffer {
  /// 创建指定容量的独立定长缓冲区（单槽轻量池保证 RAII 闭环）
  pub fn new(capacity: usize) -> Self {
    let pool = LimitedFixedBufferPool::new(capacity, 1);
    let buffer = pool.get(capacity);
    Self { buffer, capacity }
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
  #[inline]
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
    self.buffer.as_slice()
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
/// 网络发送缓冲池与背压水位门控器（统一复用 [`wnode::LimitedFixedBufferPool`]，彻底消除无锁队列 Array 重复实现）
pub struct ReplicationSendBufferPool {
  pool: Arc<LimitedFixedBufferPool>,
  buffer_capacity: usize,
  max_inflight_bytes: i64,
  current_inflight_bytes: AtomicI64,
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
      .field("borrow_count", &self.pool.borrowed_count())
      .field("free_count", &self.pool.free_count())
      .finish()
  }
}

impl ReplicationSendBufferPool {
  /// 创建新的网络发送缓冲池，统一对接底层 LimitedFixedBufferPool
  pub fn new(slots: usize, buffer_capacity: usize, max_inflight_bytes: i64) -> Self {
    let max_pool_slots = slots.max(DEFAULT_RING_BUFFER_SLOTS) * 2;
    let pool = LimitedFixedBufferPool::new(buffer_capacity, max_pool_slots);
    Self {
      pool,
      buffer_capacity,
      max_inflight_bytes,
      current_inflight_bytes: AtomicI64::new(0),
    }
  }

  /// 底层定长缓冲池引用（对标 Garnet ReplicationManager.GetNetworkPool）
  #[inline]
  pub fn pool(&self) -> &Arc<LimitedFixedBufferPool> {
    &self.pool
  }

  /// 借出一个可复用的定长发送缓冲（直接复用底层 LimitedFixedBufferPool）
  pub fn acquire(&self) -> ReplicationSendBuffer {
    let buffer = self.pool.get(self.buffer_capacity);
    ReplicationSendBuffer {
      buffer,
      capacity: self.buffer_capacity,
    }
  }

  /// 归还缓冲区以供后续流同步复用（drop 触发 PooledBuffer 自动归还，杜绝计数下溢）
  #[inline]
  pub fn release(&self, buf: ReplicationSendBuffer) {
    drop(buf);
  }

  /// 累计借出次数统计（直接透出底层池借出计数）
  #[inline]
  pub fn borrow_count(&self) -> usize {
    self.pool.borrowed_count()
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

    // 取出第 1 个
    let buf1 = pool.acquire();
    // 借出第 2 个
    let buf2 = pool.acquire();
    assert_eq!(buf1.capacity(), 512);
    assert_eq!(buf2.capacity(), 512);
    assert_eq!(pool.borrow_count(), 2);

    // 尝试归还异构容量的独立 buffer，归还到其自身的独立单槽池，不污染外层池
    let foreign_buf = ReplicationSendBuffer::new(1024);
    pool.release(foreign_buf);
    let acquired = pool.acquire();
    assert_eq!(acquired.capacity(), 512);
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

  #[test]
  fn test_replication_network_buffer_settings() {
    let rss = ReplicationNetworkBufferSettings::rss_settings();
    assert_eq!(rss.send_buffer_size, 1 << 20);
    assert_eq!(rss.initial_receive_buffer_size, 1 << 12);

    let irs = ReplicationNetworkBufferSettings::irs_settings();
    assert_eq!(irs.send_buffer_size, 1 << 17);

    let aof = ReplicationNetworkBufferSettings::aof_sync_settings(24);
    assert_eq!(aof.send_buffer_size, 2 << 24);

    let inclusive = NetworkBufferSettings::get_inclusive(&[rss, irs, aof]);
    assert_eq!(inclusive.send_buffer_size, 2 << 24);
    assert_eq!(inclusive.initial_receive_buffer_size, 1 << 12);

    let pool = inclusive.create_buffer_pool(16);
    assert!(pool.validate(&rss));
    assert!(pool.validate(&irs));
    assert!(pool.validate(&aof));
  }
}
