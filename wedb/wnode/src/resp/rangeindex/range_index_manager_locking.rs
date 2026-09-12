//! 范围索引锁面（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs）
//!
//! 锁协议：数据操作（RI.SET/GET/DEL field）持共享锁——原生 BfTree 点操作
//! 自身线程安全；生命周期操作（DEL 键 / 驱逐 / 检查点快照 / 删除前独占）
//! 持独占锁——防止并发数据操作触及正在释放 / 快照 / 恢复中的树。锁按键
//! 哈希 128 条带分段（引擎 [`wkv::RangeIndexManager`] 内置，读优化）。
//!
//! C# 的 ReadRangeIndex 复合读路径（共享锁 + 检查点屏障自旋 + Flushed 晋升
//! 重试 + TreeHandle==0 惰性恢复重试）在 Rust 由 wkv
//! `StoreSession::acquire_tree_read` 承接（引擎侧等价实现，见
//! wedb/wkv/src/range_index.rs）；本分片承接删除独占锁原语。

use parking_lot::RwLockWriteGuard;
use wkv::RangeIndexManager as Engine;

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

#[cfg(test)]
mod tests {
  use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
  };

  use tempfile::tempdir;

  use super::*;

  fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempdir().unwrap();
    let e = Arc::new(Engine::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap());
    (dir, e)
  }

  #[test]
  fn exclusive_lock_serializes_cross_thread() {
    let (_dir, engine) = engine();
    let key_hash = Engine::key_hash_of(b"del-key");

    {
      let _guard = RangeIndexManagerLocking::acquire_exclusive_for_delete(&engine, key_hash);
      // 持锁期间，他线程无法取得同条带写锁（100ms 内未获锁即证明互斥）
      let engine2 = Arc::clone(&engine);
      let (tx, rx) = mpsc::channel();
      let h = thread::spawn(move || {
        let g = RangeIndexManagerLocking::acquire_exclusive_for_delete(&engine2, key_hash);
        tx.send(()).unwrap();
        drop(g);
      });
      assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
      // 释放本线程锁后他线程应能获取
      drop(_guard);
      rx.recv_timeout(Duration::from_secs(5)).unwrap();
      h.join().unwrap();
    }
  }

  #[test]
  fn acquire_exclusive_for_key_derives_same_stripe() {
    let (_dir, engine) = engine();
    // 同键两条入口先后取放锁：锁句柄均可正常获取与释放（RAII 语义）
    let g1 =
      RangeIndexManagerLocking::acquire_exclusive_for_delete(&engine, Engine::key_hash_of(b"k"));
    drop(g1);
    let g2 = RangeIndexManagerLocking::acquire_exclusive_for_key(&engine, b"k");
    drop(g2);
  }
}
