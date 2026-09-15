//! 范围索引锁面（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs）
//!
//! 锁协议：数据操作（RI.SET/GET/DEL field）持共享锁——原生 BfTree 点操作
//! 自身线程安全；生命周期操作（DEL 键 / 驱逐 / 检查点快照 / 删除前独占）
//! 持独占锁——防止并发数据操作触及正在释放 / 快照 / 恢复中的树。锁按键
//! 哈希 128 条带分段（引擎 [`wbftree::RangeIndexManager`] 内置，读优化）。
//!
//! C# 的 ReadRangeIndex 复合读路径（共享锁 + 检查点屏障自旋 + Flushed 晋升
//! 重试 + TreeHandle==0 惰性恢复重试）在 Rust 由 wkv
//! `StoreSession::acquire_tree_read` 承接（引擎侧等价实现，见
//! wedb/wkv/src/range_index.rs）；本分片承接删除独占锁原语。

use parking_lot::RwLockWriteGuard;
use wbftree::RangeIndexManager as Engine;

/// 删除独占锁 RAII 句柄（drop 即释放；C# ExclusiveRangeIndexLock.Dispose）
pub type ExclusiveRangeIndexLock<'a> = RwLockWriteGuard<'a, ()>;

/// 锁面（C# partial RangeIndexManager 的 Locking 分片）
pub struct RangeIndexManagerLocking;

impl RangeIndexManagerLocking {
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:AcquireExclusiveForDelete
  ///
  /// 为键哈希获取条带独占锁（TryDeleteRangeIndex 释放 BfTree 期间阻断并发
  /// 数据操作）。RAII：句柄 drop 即释放。调用方以
  /// `Engine::key_hash_of(key)` 派生哈希（与 C# GetKeyHash 口径一致）
  pub fn acquire_exclusive_for_delete(
    engine: &Engine,
    key_hash: u64,
  ) -> ExclusiveRangeIndexLock<'_> {
    engine.locks().write(key_hash)
  }
}

/// 删除路径组合原语：键哈希 + 管理器 → 独占锁句柄
///
/// C# TryDeleteRangeIndex 直接内联调用 AcquireExclusiveForDelete；Rust 以
/// 自由函数绑定引擎引用与锁句柄生命周期，避免调用方持悬垂条带锁
impl RangeIndexManagerLocking {
  /// 以原始键获取删除独占锁（键哈希派生 + [`Self::acquire_exclusive_for_delete`] 组合）
  pub fn acquire_exclusive_for_key<'a>(
    engine: &'a Engine,
    key: &[u8],
  ) -> ExclusiveRangeIndexLock<'a> {
    Self::acquire_exclusive_for_delete(engine, Engine::key_hash_of(key))
  }
}
