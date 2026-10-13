use super::*;

impl<D: Device> Clone for StoreSwapSlot<D> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: Arc::clone(&self.inner),
    }
  }
}

impl<D: Device> Default for StoreSwapSlot<D> {
  #[inline]
  fn default() -> Self {
    Self {
      inner: Arc::new(RwLock::new(None)),
    }
  }
}

impl<D: Device> StoreSwapSlot<D> {
  /// 创建空槽
  pub fn new() -> Self {
    Self::default()
  }

  /// 当前引擎（未播种且未置换时 None）
  #[inline]
  pub fn get(&self) -> Option<SharedStore<D>> {
    self.inner.read().clone()
  }

  /// 换持新引擎并返回被换下的旧引擎（若有）；首次调用即为播种
  pub fn swap(&self, store: SharedStore<D>) -> Option<SharedStore<D>> {
    let mut w = self.inner.write();
    let old = w.clone();
    *w = Some(store);
    old
  }
}

/// wkv 扩容状态机 → wtxn 事务屏障端口的装配适配（wtxn 为存储下层，不认 wkv，
/// 故端口实现在此接线；对标 C# `TransactionManager.cs:185` 构造期
/// `this.stateMachineDriver = db.StateMachineDriver` 的**实例绑定**：
/// 注销恒打在注册时那台驱动上——引擎在线置换后亦不错减他实例的活跃事务计数，
/// 错减令新引擎计数下溢翻转、此后每次扩容排空永不自收敛）
struct ResizeBarrier(Arc<IndexResizeState>);

impl TxnBarrier for ResizeBarrier {
  #[inline]
  fn end_txn(&self) {
    self.0.release_txn();
  }
}

/// 引擎实例事务锁表装配（一处构造、随本引擎的会话句柄共享，全仓无进程级静态锁表）
///
/// 锁源注入「当前引擎索引装载闭包」：事务键锁走 windex 哈希桶内嵌闩（与 wkv TTL 读改写
///        同一把锁、同一份内存），每笔事务现取当前引擎的 HashIndex 版本——粒度随 split 在线扩容
///        细化，并经 store_swap 跟随引擎在线置换（对标 C# LockTable 逐次现取 store.state[version]）
///
/// 全事务屏障注册闭包与锁源同址承接（对标 C# TransactionManager 随 store 挂同一
///        StateMachineDriver 的 AcquireTransactionVersion/EndTransaction）：注册经当前引擎
///        的 IndexResizeState 活跃事务计数，PREPARE_GROW 期拦停新事务、切表前排空旧表桶锁；
///        注册成功返回的票据即该实例的注销句柄，由事务管理器持票至桶闩尽释
pub(super) fn build_txn_lock_table(
  store: SharedStore<SegmentedDevice>,
  store_swap: &StoreSwapSlot,
) -> TxnLockTable {
  let base_store = Arc::clone(&store);
  let loader_swap = store_swap.clone();
  let acquire_store = Arc::clone(&store);
  let acquire_swap = store_swap.clone();
  TxnLockTable::from_loader_gated(
    move || {
      loader_swap
        .get()
        .unwrap_or_else(|| Arc::clone(&base_store))
        .index
        .load_full()
    },
    move || {
      let store = acquire_swap
        .get()
        .unwrap_or_else(|| Arc::clone(&acquire_store));
      // 计数注册成功才产票；票据一次 Arc 轻注册，换得与驱动实例的强绑定
      store
        .resize
        .try_acquire_txn()
        .then(|| Arc::new(ResizeBarrier(Arc::clone(&store.resize))) as TxnBarrierTicket)
    },
  )
}
