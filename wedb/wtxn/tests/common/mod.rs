//! wtxn 集成测试共用夹具

use std::sync::Arc;

use wtxn::{TransactionManager, TxnLockTable, WatchVersionMap};

/// 构造无 AOF 的最小事务管理器（64 桶版本表 + 一张独立的引擎实例锁表）
pub fn manager() -> TransactionManager {
  TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    None,
  )
}
