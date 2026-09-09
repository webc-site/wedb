//! 对象堆大小追踪器（对标 libs/server/Storage/SizeTracker/CacheSizeTracker.cs）
//!
//! C# 侧以定时采样任务追踪对象存堆字节数并按 50MB 批次做周期检查点；wkv
//! 信封模型下对象随 hlog 统一落盘，无独立堆，本追踪器退化为进程内字节数
//! 估计器（原子累加），供 INFO memory 与检查点规模参考。

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};

/// 堆字节数估计器
pub struct CacheSizeTracker {
  /// 对象堆估计字节数
  heap_bytes: AtomicI64,
  /// 读缓存堆估计字节数
  read_cache_bytes: AtomicI64,
  /// 采样任务是否已停止
  stopped: AtomicBool,
}

impl CacheSizeTracker {
  /// 创建归零追踪器
  pub fn new() -> Self {
    Self {
      heap_bytes: AtomicI64::new(0),
      read_cache_bytes: AtomicI64::new(0),
      stopped: AtomicBool::new(false),
    }
  }

  /// 累加对象堆估计字节数（可为负，对应回收）
  ///
  /// libs/server/Storage/SizeTracker/CacheSizeTracker.cs:AddHeapSize
  pub fn add_heap_size(&self, bytes: i64) {
    self.heap_bytes.fetch_add(bytes, Relaxed);
  }

  /// 累加读缓存堆估计字节数（可为负）
  ///
  /// libs/server/Storage/SizeTracker/CacheSizeTracker.cs:AddReadCacheHeapSize
  pub fn add_read_cache_heap_size(&self, bytes: i64) {
    self.read_cache_bytes.fetch_add(bytes, Relaxed);
  }

  /// 当前堆估计（字节）
  pub fn heap_bytes(&self) -> i64 {
    self.heap_bytes.load(Relaxed)
  }

  /// 停止采样任务
  ///
  /// 缺口说明：C# 侧停止定时采样循环；Rust 侧无后台循环（按需读取原子量），
  /// 仅落停止标志拒绝后续写入。
  ///
  /// libs/server/Storage/SizeTracker/CacheSizeTracker.cs:Stop
  pub fn stop(&self) {
    self.stopped.store(true, Relaxed);
  }

  /// 是否已停止
  pub fn is_stopped(&self) -> bool {
    self.stopped.load(Relaxed)
  }
}

impl Default for CacheSizeTracker {
  fn default() -> Self {
    Self::new()
  }
}
