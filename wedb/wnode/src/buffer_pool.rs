//! 网络缓冲区池化管理
//!
//! 1:1 对标微软 Garnet LimitedFixedBufferPool 与 NetworkBufferSettings
//!
//! 统一管理收发网络缓冲区，避免频繁堆分配与内存抖动。默认块大小 64KB (1 << 16)；
//! 提供快速借出、RAII 自动归还复用、Purge 清理及统计指标导出。

use std::{
  array,
  ops::{Deref, DerefMut},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::Relaxed},
  },
};

use parking_lot::Mutex;

/// 默认网络缓冲区大小：64KB
pub const DEFAULT_BUFFER_SIZE: usize = 1 << 16;
/// 池内最大常驻闲置缓冲数
pub const DEFAULT_MAX_POOL_SIZE: usize = 1024;
/// 分片数，降低多线程争用
const SHARDS: usize = 16;

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

  /// 提取底层 Vec 并放弃自动归还池
  pub fn take(mut self) -> Vec<u8> {
    self.pool.borrowed_count.fetch_sub(1, Relaxed);
    self.buffer.take().unwrap_or_default()
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
    }
  }
}

/// 固定大小网络缓冲池（分片优化版本，彻底避免缓存行颠簸与锁争用）
pub struct LimitedFixedBufferPool {
  buffer_size: usize,
  max_pool_size: usize,
  shard_capacity: usize,
  shards: [Mutex<Vec<Vec<u8>>>; SHARDS],
  allocated_count: AtomicUsize,
  borrowed_count: AtomicUsize,
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
    let shard_cap = cap.div_ceil(SHARDS);
    let shards = array::from_fn(|_| Mutex::new(Vec::new()));

    Arc::new(Self {
      buffer_size: size,
      max_pool_size: cap,
      shard_capacity: shard_cap,
      shards,
      allocated_count: AtomicUsize::new(0),
      borrowed_count: AtomicUsize::new(0),
    })
  }

  /// 借出一个指定最小容量的缓冲区
  pub fn get(self: &Arc<Self>, min_size: usize) -> PooledBuffer {
    self.borrowed_count.fetch_add(1, Relaxed);
    let target_size = min_size.max(self.buffer_size);

    let buf = if target_size == self.buffer_size {
      let start_shard = fastrand::usize(..SHARDS);
      let mut found = None;
      for i in 0..SHARDS {
        let idx = (start_shard + i) % SHARDS;
        if let Some(mut guard) = self.shards[idx].try_lock()
          && let Some(b) = guard.pop()
        {
          found = Some(b);
          break;
        }
      }
      if found.is_none() {
        let mut guard = self.shards[start_shard].lock();
        guard.pop()
      } else {
        found
      }
    } else {
      None
    };

    let buffer = match buf {
      Some(mut v) => {
        v.clear();
        v
      }
      None => {
        self.allocated_count.fetch_add(1, Relaxed);
        Vec::with_capacity(target_size)
      }
    };

    PooledBuffer {
      buffer: Some(buffer),
      pool: Arc::clone(self),
    }
  }

  /// 归还缓冲区到底层池
  pub(crate) fn return_buffer(&self, mut buf: Vec<u8>) {
    self.borrowed_count.fetch_sub(1, Relaxed);
    if buf.capacity() == self.buffer_size {
      let start_shard = fastrand::usize(..SHARDS);
      buf.clear();
      for i in 0..SHARDS {
        let idx = (start_shard + i) % SHARDS;
        if let Some(mut guard) = self.shards[idx].try_lock()
          && guard.len() < self.shard_capacity
        {
          guard.push(buf);
          return;
        }
      }
      let mut guard = self.shards[start_shard].lock();
      if guard.len() < self.shard_capacity {
        guard.push(buf);
      }
    }
  }

  /// 清空池内所有闲置缓冲区
  pub fn purge(&self) {
    for shard in &self.shards {
      let mut guard = shard.lock();
      guard.clear();
      guard.shrink_to_fit();
    }
  }

  /// 当前池中闲置可用缓冲区数量
  #[inline]
  pub fn free_count(&self) -> usize {
    self.shards.iter().map(|s| s.lock().len()).sum()
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
  use super::*;

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
}
