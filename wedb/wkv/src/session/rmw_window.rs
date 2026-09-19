//! 同键读改写（RMW）原子窗口（对标 Tsavorite 会话锁器 `ISessionLocker` 与
//! `InternalRMW` 的 ephemeral 桶闩跨读改写全程协议）
//!
//! C# 侧非事务会话经 `BasicSessionLocker.TryLockEphemeralExclusive` 在 RMW 的
//! 读—算—写全程持本键主桶排他闩（`libs/storage/Tsavorite/cs/src/core/Index/
//! Tsavorite/Implementation/InternalRMW.cs` 的
//! `FindOrCreateTagAndTryEphemeralXLock` + try/finally `UnlockEphemeralExclusive`），
//! 事务会话经 `TransactionalSessionLocker` 只断言不取闩——事务已在同一份锁内存上
//! 持该键桶的排他闩（`libs/server/Transaction/TxnKeyEntry.cs` 与
//! `Implementation/Locking/OverflowBucketLockTable.cs` 共用 `store.LockTable`，
//! 桶下标同为 `keyHash & size_mask`）。
//!
//! rust 侧锁源同为 windex `HashBucket` 内嵌闩，本键 `user_key` 在当前 `HashIndex`
//! 版本下的主桶（`fast_hash & size_mask`）三处共取：本窗口、`wtxn` 事务键锁
//!（`wtxn/src/txn_lock_table.rs` 转发 `HashBucket`）、`wkv` TTL 读改写窗口
//!（`wkv/src/ttl.rs` 的 `acquire_keys_lock_exclusive`），故三者互斥即同键全序串行，
//! 全仓无第二把同址锁、亦无条带折算。
//!
//! 会话该走哪种锁器：C# 由 api 视图类型在编译期定（`libs/server/Resp/
//! RespServerSession.cs:ProcessMessages` 依 `txnManager.state == TxnState.Running`
//! 在 `basicApi` 与 `transactionalApi` 间派发），rust 不对命令层做双份单态化，
//! 改由同一判据在命令分派单点写 [`StoreSession::set_session_locking`]，
//! 窗口据此决定「自取闩」或「让闩于事务」。
//!
//! 与引擎侧回溯保护闩的关系：`session/raw/write/inplace.rs` 的 ephemeral 闩按
//! 物理记录键（带 `prefix|KeyTag` 前缀后再哈希）寻桶，与本窗口的 `user_key` 桶
//! 是两个不同基（garnet 单份哈希表故同键同桶，wedb 同键的 TTL/字符串/对象记录
//! 各自成键故各落一桶），两桶偶合时窗口持闩期内层取闩必失败并按 RETRY_LATER
//! 退避重试——该面在 HEAD 已随 `wtxn` 事务键锁与 `wkv/src/ttl.rs` 的 EXPIRE
//! 窗口同形存在（持 user_key 桶闩期内读写记录桶），属两基并存的既有结构，
//! 不在本窗口内做二次折算。

use std::sync::Arc;

use wdev::Device;
use windex::HashIndex;

use super::StoreSession;

/// 会话锁器模式（对标 C# 两套 `ISessionLocker` 实现的按调用面选型）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:ISessionLocker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLocking {
  /// 非事务会话：读改写窗口自取本键桶排他闩
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:BasicSessionLocker
  Basic,
  /// 事务会话：本键桶排他闩已由本会话事务持有，窗口让闩（绝不自旋等自己）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:TransactionalSessionLocker
  Transactional,
}

impl SessionLocking {
  /// 是否事务锁模式
  #[inline(always)]
  pub const fn is_transactional(self) -> bool {
    matches!(self, Self::Transactional)
  }
}

/// 同键读改写原子窗口（键级 RAII：本键桶排他闩的持有证明）
///
/// 值写回入口（[`Self::try_rmw_sync`] / [`Self::upsert_rmw`]）挂在本类型上，
/// 无窗口即取不到写回面，杜绝「读—写两步式盲写」的第二条路径；
/// `held` 为 `None` 即事务锁模式（本会话事务已持该桶闩，窗口零操作）。
pub struct RmwWindow<'a, 'k, D: Device> {
  /// 归属会话（值写回内核的宿主）
  session: &'a StoreSession<D>,
  /// 窗口钉定的用户键（写回目标键与取闩键同源，编译面即不可错配）
  user_key: &'k [u8],
  /// 钉定的索引版本与本键主桶下标（跨 split 扩容不串锁；Drop 按同一版本放闩）
  held: Option<(Arc<HashIndex>, usize)>,
}

impl<'a, 'k, D: Device> RmwWindow<'a, 'k, D> {
  /// 本窗口钉定的用户键
  #[inline(always)]
  pub fn user_key(&self) -> &'k [u8] {
    self.user_key
  }

  /// 窗口归属会话（crate 内值写回内核取用，实现面见
  /// `session/raw/write/rmw.rs` 的 `impl RmwWindow`）
  #[inline(always)]
  pub(crate) const fn session(&self) -> &'a StoreSession<D> {
    self.session
  }
}

impl<'a, 'k, D: Device> Drop for RmwWindow<'a, 'k, D> {
  #[inline]
  fn drop(&mut self) {
    if let Some((index, bucket)) = &self.held {
      index.bucket(*bucket).unlock_exclusive();
    }
  }
}

impl<D: Device> StoreSession<D> {
  /// 当前会话锁器模式
  #[inline(always)]
  pub fn session_locking(&self) -> SessionLocking {
    if self.session_locking.load(std::sync::atomic::Ordering::Relaxed) {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }

  /// 置位会话锁器模式并回旧值（分派单点选型与 RAII 还原共用；C# 由 api 视图类型
  /// 承载，rust 为会话态一位，见模块头口径说明）
  #[inline(always)]
  pub fn set_session_locking(&self, locking: SessionLocking) -> SessionLocking {
    let prev = self
      .session_locking
      .swap(locking.is_transactional(), std::sync::atomic::Ordering::Relaxed);
    if prev {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }

  /// 取本键读改写原子窗口——非阻塞臂（同步快路径专用）
  ///
  /// 返回 `None` = 本键桶排他闩被占（同键并发 RMW / 他事务持锁），调用方按既有
  /// 降级通道转异步闭环（对标 C# ephemeral 取闩失败回 `RETRY_LATER`），
  /// 同步域绝不自旋等闩；事务锁模式下本键桶闩已在本会话事务手上，直接回让闩窗口
  #[inline]
  pub fn try_rmw_window<'k>(&self, user_key: &'k [u8]) -> Option<RmwWindow<'_, 'k, D>> {
    let held = if self.session_locking().is_transactional() {
      None
    } else {
      let index = self.store.index.load_full();
      let bucket = index.bucket_index_for_key(user_key);
      if !index.bucket(bucket).try_lock_exclusive() {
        return None;
      }
      Some((index, bucket))
    };
    Some(RmwWindow {
      session: self,
      user_key,
      held,
    })
  }

  /// 取本键读改写原子窗口——让核等待臂（异步域专用，对标 C# 锁冲突转 pending 重试）
  pub async fn rmw_window<'k>(&self, user_key: &'k [u8]) -> RmwWindow<'_, 'k, D> {
    loop {
      if let Some(window) = self.try_rmw_window(user_key) {
        return window;
      }
      wbase::future::yield_now().await;
    }
  }
}
