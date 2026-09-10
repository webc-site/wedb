//! 线程本地 L1 私有栈与生命周期管理 (对标 C# `ThreadShard` 与 `[ThreadStatic] t_shards`)
//!
//! 每个线程独占私有 `local` 数组，借取和归还纯指针操作，**0 锁、0 原子操作、< 1ns**；
//! 线程终止时通过 TLS RAII 确定性析构自动密封 inbox 并将遗留缓冲区回收至全局条带仓库。

use std::{
  cell::RefCell,
  sync::{Arc, Weak},
};

use super::{
  BufferPool, CachedBuf, NUM_CLASSES,
  inbox::{ChainIter, CrossThreadInbox, SEALED},
};

/// 线程私有单池缓存 (对标 C# ThreadShard)
pub(crate) struct TlsPoolEntry {
  pub(crate) pool_id: u64,
  /// 创建本条目的线程 ID (线程退出时按自身条带分流 Depot，对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ThreadStripe()，
  /// 避免所有退出线程挤占同一条带)；在 get_or_create 正常上下文中采集，规避 TLS 析构期访问其他 TLS
  pub(crate) tid: u64,
  pub(crate) pool_weak: Weak<BufferPool>,
  pub(crate) inbox: Arc<CrossThreadInbox>,
  pub(crate) local: [Vec<CachedBuf>; NUM_CLASSES],
}

impl TlsPoolEntry {
  fn new(pool: &Arc<BufferPool>) -> Self {
    Self {
      pool_id: pool.pool_id,
      tid: current_thread_id(),
      pool_weak: Arc::downgrade(pool),
      inbox: Arc::new(CrossThreadInbox::new()),
      local: [const { Vec::new() }; NUM_CLASSES],
    }
  }

  /// 统一清扫：密封收件箱并清空本地栈，逐 class 处置在途与缓存缓冲
  ///
  /// `retire = true`（线程退出）：池存活且未关闭时溢出转移至全局条带仓库，否则释放许可；
  /// `retire = false`（池关闭拆除）：全部就地释放许可 (内部清扫，对标 BufferPool SealAndDrainShard 语义)
  fn sweep(&mut self, pool: Option<&BufferPool>, retire: bool) {
    let tid = self.tid;
    for cls in 0..NUM_CLASSES {
      // 1. 封闭收件箱并拔出在途跨线程缓冲 (零堆分配就地遍历)
      let chain = self.inbox.seal_and_drain(cls);
      if !chain.is_null() && chain != SEALED {
        for node in ChainIter::new(chain) {
          Self::sweep_node(cls, node, pool, retire, tid);
        }
      }
      // 2. 处置本地未消耗的缓冲
      for buf in self.local[cls].drain(..) {
        Self::sweep_node(cls, buf, pool, retire, tid);
      }
    }
  }

  /// 单节点处置：优先溢出转移至 Depot (许可随节点转移)；转移失败或池已消亡则释放许可，
  /// node drop 自动释放内存；池已彻底释放时许可随之消亡，仅需释放内存
  #[inline]
  fn sweep_node(cls: usize, node: CachedBuf, pool: Option<&BufferPool>, retire: bool, tid: u64) {
    let cap = node.cap;
    if let Some(pool) = pool
      && retire
      && !pool.is_closed()
      && pool.depot.push(cls, node, tid)
    {
      return;
    }
    if let Some(pool) = pool {
      pool.budget_for(cls).release(cap as i64);
    }
  }

  /// 清空并释放当前 entry 中属于该池的所有本地及在途缓冲许可 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:SealAndDrainShard)
  pub(crate) fn drain_and_release(&mut self, pool: &BufferPool) {
    self.sweep(Some(pool), false);
  }
}

impl Drop for TlsPoolEntry {
  fn drop(&mut self) {
    let pool_opt = self.pool_weak.upgrade();
    self.sweep(pool_opt.as_deref(), true);
  }
}

/// 线程本地多池管理器
pub(crate) struct TlsPoolManager {
  pub(crate) fast: Option<TlsPoolEntry>,
  pub(crate) others: Vec<TlsPoolEntry>,
}

impl TlsPoolManager {
  const fn new() -> Self {
    Self {
      fast: None,
      others: Vec::new(),
    }
  }

  /// 按 pool_id 查找条目 (fast 优先，其次 others；同一 pool 的条目仅存在于二者之一)
  pub(crate) fn find(&self, pool_id: u64) -> Option<&TlsPoolEntry> {
    if let Some(entry) = self.fast.as_ref()
      && entry.pool_id == pool_id
    {
      return Some(entry);
    }
    self.others.iter().find(|e| e.pool_id == pool_id)
  }

  /// [`TlsPoolManager::find`] 的可变版本
  pub(crate) fn find_mut(&mut self, pool_id: u64) -> Option<&mut TlsPoolEntry> {
    if let Some(entry) = self.fast.as_mut()
      && entry.pool_id == pool_id
    {
      return Some(entry);
    }
    self.others.iter_mut().find(|e| e.pool_id == pool_id)
  }

  pub(crate) fn get_or_create<'a>(&'a mut self, pool: &Arc<BufferPool>) -> &'a mut TlsPoolEntry {
    // fast 命中探测用不可变借用即时终结：stable NLL 下「可变借用 + 早返回 + 'a
    // 标注」会把借用拉长到整个函数，与慢路径的 take/insert 冲突 (E0499)；
    // 判定为 bool 后重新可变借用，两段借用互不重叠
    if self
      .fast
      .as_ref()
      .is_some_and(|e| e.pool_id == pool.pool_id)
    {
      // SAFETY: 判定与取用之间无任何变动，fast 槽必为 Some 且 pool_id 匹配
      return unsafe { self.fast.as_mut().unwrap_unchecked() };
    }

    // fast 槽位校验：已关闭则清扫释放，池已消亡则直接丢弃，仍存活则降级入 others
    if let Some(mut entry) = self.fast.take() {
      match entry.pool_weak.upgrade() {
        Some(p) if p.is_closed() => entry.drain_and_release(&p),
        Some(_) => self.others.push(entry),
        None => {}
      }
    }

    if let Some(idx) = self.others.iter().position(|e| e.pool_id == pool.pool_id) {
      let entry = self.others.swap_remove(idx);
      return self.fast.insert(entry);
    }

    // 清理 others 中已关闭或已释放的陈旧池，防止长期运行线程无界累积
    self
      .others
      .retain_mut(|entry| match entry.pool_weak.upgrade() {
        Some(p) if !p.is_closed() => true,
        Some(p) => {
          entry.drain_and_release(&p);
          false
        }
        None => false,
      });

    self.fast.insert(TlsPoolEntry::new(pool))
  }
}

thread_local! {
  pub(crate) static TLS_POOLS: RefCell<TlsPoolManager> = const { RefCell::new(TlsPoolManager::new()) };
}

/// 获取当前线程全局唯一且单调递增的非零线程 ID（统一复用 wbase 原语）
pub use crate::thread::current_thread_id;
