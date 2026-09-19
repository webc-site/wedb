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
//! 桶下标同为 `keyHash & size_mask`）。C# 的 `InternalUpsert.cs:67` 同一取闩口
//! 亦覆盖纯写回，故锁内读—算—写是该引擎的唯一形态，命令层从不裸读旧值再盲写。
//!
//! rust 侧锁源同为 windex `HashBucket` 内嵌闩，本键 `user_key` 在当前 `HashIndex`
//! 版本下的主桶（`fast_hash & size_mask`）三处共取：本窗口、`wtxn` 事务键锁
//!（`wtxn/src/txn_lock_table.rs` 转发 `HashBucket`，键哈希同为 `fast_hash`）、
//! `wkv` TTL 读改写窗口（`wkv/src/ttl.rs` 经 `HashIndex::try_lock_key_exclusive`
//! 持桶闩），故三者互斥即同键全序串行，全仓无第二把同址锁、亦无条带折算。
//! 本窗口取闩即该入口的同两步组合（`bucket_index_for_key` 定位主桶 +
//! `HashBucket::try_lock_exclusive` 单次尝试），只是闩的持有证明要跨 `&self`
//! 借用期交回命令层，故以窗口自身承载放闩，不自建第二张锁表、不另立锁语义。
//! 取闩/放闩形态与 `wtxn::TxnKeyEntries::acquire_plan`/`release_held` 同款：
//! 钉定 `Arc<HashIndex>` 版本 + 纯数据桶下标，跨 split 扩容不串锁，不新建守卫类型。
//!
//! 会话该走哪种锁器：C# 由 api 视图类型在编译期定（`libs/server/Resp/
//! RespServerSession.cs:ProcessMessages` 依 `txnManager.state == TxnState.Running`
//! 在 `basicApi` 与 `transactionalApi` 间派发），rust 不对命令层做双份单态化，
//! 改由同一判据在命令分派单点写 [`StoreSession::push_session_locking`]，
//! 窗口据此决定「自取闩」或「让闩于事务」。
//!
//! 与引擎侧回溯保护闩的关系：`session/raw/write/inplace.rs` 的 ephemeral 闩按
//! 物理记录键（带 `prefix|KeyTag` 前缀后再哈希）寻桶，与本窗口的 `user_key` 桶
//! 是两个不同基（garnet 单份哈希表故同键同桶，wedb 同键的 TTL/字符串/对象记录
//! 各自成键故各落一桶），两桶偶合时窗口持闩期内层取闩必失败并按 RETRY_LATER
//! 退避重试——该面在 HEAD 已随 `wtxn` 事务键锁与 `wkv/src/ttl.rs` 的 EXPIRE
//! 窗口同形存在（持 user_key 桶闩期内读写记录桶），属两基并存的既有结构，
//! 不在本窗口内做二次折算；同理，纯写回族（SET/DEL/MSET 折叠）仍只持记录桶
//! ephemeral 闩，其与本窗口的交错面即该两基结构，另票承接。

use std::{
  fmt,
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use wbase::future::yield_now;
use wdev::Device;
use windex::{Error as WindexError, HashIndex};

use super::{BatchStoreSession, StoreSession};
use crate::error::{Error, Result};

/// 同步快路径取闩重试预算（索引层单键闩入口 `HashIndex::try_lock_key_exclusive`
/// 为一次尝试、无自旋驱动、无超时判定，取闩失败的重试由本调用方承接——对标 C#
/// ephemeral 取闩失败回 `RETRY_LATER`）：持闩期只有读—算—写三段纯内存操作，
/// 微秒级即放闩，故 1024 轮单次尝试足够；预算耗尽仍不得闩即交调用方降级，
/// 同步域绝不无限自旋
const RMW_LATCH_SPIN_ATTEMPTS: usize = 1024;

/// 异步域让核重试预算（对标 C# 锁冲突转 pending 重试的外层循环）：预算耗尽即回
/// [`windex::Error::LockTimeout`]，杜绝同线程持闩者不再让核时的无界自旋
const RMW_LATCH_YIELD_BUDGET: usize = 1024;

/// 会话锁器模式（对标 C# 两套 `ISessionLocker` 实现的按调用面选型，
/// 见 `libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs`
/// 的 `BasicSessionLocker` 与 `TransactionalSessionLocker` 两实现；映射锚点
/// 已在仓内其他位点登记，此处不复挂以免重复定义）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLocking {
  /// 非事务会话：读改写窗口自取本键桶排他闩
  Basic,
  /// 事务会话：本键桶排他闩已由本会话事务持有，窗口让闩（绝不自旋等自己）
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
/// 本类型只由 [`BatchStoreSession::try_rmw_window`] /
/// [`BatchStoreSession::rmw_window`] 构造，故持窗口必然处于批处理纪元保护下
/// （其值写回内核走零 `enter()` 的同步段，纪约与
/// `StoreSession::try_upsert_raw_sync_unprotected` 同）。
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

impl<D: Device> Drop for RmwWindow<'_, '_, D> {
  #[inline]
  fn drop(&mut self) {
    if let Some((index, bucket)) = &self.held {
      index.bucket(*bucket).unlock_exclusive();
    }
  }
}

/// 会话锁器模式 RAII 还原守卫（对标 C# api 视图按调用段进出选型：事务重放遍与
/// 事务过程视图进入时置 `Transactional`，离开即还原，杜绝位残留；分派选型位点
/// 的映射锚点登记在 `wnode/src/resp/resp_server_session.rs`，此处不复挂）
pub struct SessionLockingGuard<'a, D: Device> {
  session: &'a StoreSession<D>,
  prev: SessionLocking,
}

impl<D: Device> fmt::Debug for SessionLockingGuard<'_, D> {
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SessionLockingGuard")
      .field("prev", &self.prev)
      .finish_non_exhaustive()
  }
}

impl<D: Device> Drop for SessionLockingGuard<'_, D> {
  #[inline]
  fn drop(&mut self) {
    self.session.set_session_locking(self.prev);
  }
}

/// 会话锁器模式位（`StoreSession::session_locking` 字段的读写单点，
/// 判据与取用面在 [`crate::session::rmw_window`] 模块头）
#[derive(Debug)]
pub(crate) struct SessionLockingState(AtomicBool);

impl SessionLockingState {
  #[inline(always)]
  pub(crate) const fn new() -> Self {
    Self(AtomicBool::new(false))
  }

  #[inline(always)]
  fn get(&self) -> SessionLocking {
    if self.0.load(Ordering::Relaxed) {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }

  #[inline(always)]
  fn swap(&self, locking: SessionLocking) -> SessionLocking {
    let prev = self.0.swap(locking.is_transactional(), Ordering::Relaxed);
    if prev {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }
}

impl<D: Device> StoreSession<D> {
  /// 当前会话锁器模式
  #[inline(always)]
  pub fn session_locking(&self) -> SessionLocking {
    self.session_locking.get()
  }

  /// 置位会话锁器模式并回旧值（分派单点选型与 RAII 还原共用；C# 由 api 视图类型
  /// 承载，rust 为会话态一位，见模块头口径说明）
  #[inline(always)]
  pub fn set_session_locking(&self, locking: SessionLocking) -> SessionLocking {
    self.session_locking.swap(locking)
  }

  /// 置位会话锁器模式并在离开作用域时还原旧值（命令分派单点与事务过程视图用）
  #[inline(always)]
  pub fn push_session_locking(&self, locking: SessionLocking) -> SessionLockingGuard<'_, D> {
    let prev = self.set_session_locking(locking);
    SessionLockingGuard {
      session: self,
      prev,
    }
  }
}

impl<'a, D: Device> BatchStoreSession<'a, D> {
  /// 取本键读改写原子窗口——非阻塞臂（同步快路径专用）
  ///
  /// 返回 `None` = 自旋预算内未取到本键桶排他闩（同键并发 RMW / 他事务持锁 /
  /// EXPIRE 窗口占闩），调用方按既有降级通道转异步闭环（对标 C# ephemeral 取闩
  /// 失败回 `RETRY_LATER`），同步域绝不无限自旋；事务锁模式下本键桶闩已在本会话
  /// 事务手上，直接回让闩窗口
  #[inline]
  pub fn try_rmw_window<'s, 'k>(&'s self, user_key: &'k [u8]) -> Option<RmwWindow<'s, 'k, D>> {
    let session: &'s StoreSession<D> = self;
    let held = if session.session_locking().is_transactional() {
      None
    } else {
      let index = session.store.index.load_full();
      let bucket = index.bucket_index_for_key(user_key);
      let bucket_ref = index.bucket(bucket);
      let mut taken = bucket_ref.try_lock_exclusive();
      for _ in 0..RMW_LATCH_SPIN_ATTEMPTS {
        if taken {
          break;
        }
        spin_loop();
        taken = bucket_ref.try_lock_exclusive();
      }
      if !taken {
        return None;
      }
      Some((index, bucket))
    };
    Some(RmwWindow {
      session,
      user_key,
      held,
    })
  }

  /// 取本键读改写原子窗口——让核等待臂（异步域专用，对标 C# 锁冲突转 pending 重试）
  ///
  /// 每轮失败让核一次（持闩者在同核让出后即放闩，异核自旋即成），预算耗尽回
  /// [`windex::Error::LockTimeout`] 交调用方按存储错误应答，绝不留无界等待
  pub async fn rmw_window<'s, 'k>(&'s self, user_key: &'k [u8]) -> Result<RmwWindow<'s, 'k, D>> {
    for _ in 0..RMW_LATCH_YIELD_BUDGET {
      if let Some(window) = self.try_rmw_window(user_key) {
        return Ok(window);
      }
      yield_now().await;
    }
    match self.try_rmw_window(user_key) {
      Some(window) => Ok(window),
      None => Err(Error::Index(WindexError::LockTimeout)),
    }
  }
}
