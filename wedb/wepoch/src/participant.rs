//! 参与者会话句柄与 RAII 保护守卫
//!
//! 对照 C# LightEpoch 的单一保护机制：Rust 拆分出 TLS 作用域（resume/suspend）
//! 与 `Participant` 显式句柄双轨机制，本模块承载后者——代表单个线程或客户端会话
//! 在 `LightEpoch` 中的长期登记（如批处理会话），独占一个 `EpochEntry` 槽位。
//!
//! ## 线程亲和契约（一个槽位同一时刻只有一个属主）
//!
//! C# 侧槽位索引 `Metadata.Entries` 声明为 `[ThreadStatic]`
//! （对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:52-85），
//! 语言层面即不可能有两个线程持有同一槽位，`Acquire`/`Release` 天然单线程配对。
//! rust 侧本句柄以显式索引承载同一语义，故使用纪律与 C# 等价：
//!
//! - 一个 `Participant` 同一时刻只允许一个线程进出（`enter`/`refresh`/`exit` 与
//!   [`crate::EpochSuspendGuard`] 全部在本属主线程上配对完成），`refresh`/`exit`
//!   在 Debug 构建以 `debug_assert` 现场核验槽位线程归属；
//! - **可 `Send`**：句柄所有权可跨线程转移（转移即换绑属主，`enter` 绑定当时线程，
//!   见 `entry.rs::enter_with_tid` 的独占占位序），转移点须有 happens-before
//!   （join / channel / Mutex 皆可），且转移前本层保护须已退出；
//! - **禁并发共享**：不得以 `&Participant` / `Arc<Participant>` 让两个线程同时进出
//!   同一槽位。`refresh` 会替其他在途线程抬升公布纪元（旧纪元页可能被提前回收），
//!   `thread_protected_entry` / `bump_and_wait` 的活锁防御按 `thread_id` 归属判定，
//!   只认首层占位线程。本类型以 `PhantomData<Cell<()>>` 收紧为 `!Sync`，
//!   编译期即拒绝跨线程共享引用形态（对标 C# `[ThreadStatic]` 的语言层禁令）；
//!   上游各执行域须各持会话（每工作线程一个 `StoreSession`）。

use std::{cell::Cell, fmt, marker::PhantomData, ops::Deref, sync::Arc};

use wbase::thread::current_thread_id;

use crate::LightEpoch;

/// 参与者会话句柄
///
/// 代表单个线程或客户端会话在 `LightEpoch` 中的登记。
/// 每个参与者独占一个 `EpochEntry` 槽位，不可被 Clone；槽位同一时刻只有一个属主
/// 线程（可转移所有权，不可跨线程并发共享，见模块头线程亲和契约）。
pub struct Participant {
  epoch: Arc<LightEpoch>,
  entry_idx: usize,
  /// 单属主约束标记：`Cell<()>` 为 `Send` 非 `Sync`，使本类型保持 `Send`
  /// （所有权可跨线程转移）而收紧为 `!Sync`（编译期拒绝共享引用）
  _not_sync: PhantomData<Cell<()>>,
}

impl Participant {
  pub(crate) fn new(epoch: Arc<LightEpoch>, entry_idx: usize) -> Self {
    Self {
      epoch,
      entry_idx,
      _not_sync: PhantomData,
    }
  }

  /// 进入受保护的纪元区，返回 RAII 守卫 `EpochGuard`
  ///
  /// 对照 C# LightEpoch 的 Acquire 语义（主映射归属 LightEpoch::resume；本函数为会话句柄层拆分路径）
  /// （占位 + 公布纪元 + drain_count > 0 时协助收割的 Participant 侧路径）
  /// 若发生重入调用，则递增重入计数并维持已有纪元保护；
  /// 否则现场原子读取全局当前纪元并初始化重入计数。
  #[inline]
  pub fn enter(&self) -> EpochGuard<'_> {
    let tid = current_thread_id();
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    let protected_epoch = entry.enter_with_tid(&self.epoch.current_epoch, tid);
    self.epoch.drain_if_pending();
    EpochGuard {
      participant: self,
      protected_epoch,
    }
  }

  /// 刷新当前参与者公布的纪元至最新值，并触发就绪的延迟动作（对齐 LightEpoch ProtectAndDrain 语义）
  ///
  /// 对照 C# LightEpoch 的 ProtectAndDrain 语义（主映射归属 LightEpoch::protect_and_drain；本函数为会话句柄层拆分路径）
  /// 刻意差异：C# 在未保护区刷新会污染空闲槽位（Debug 断言拦截）；此处先校验
  /// 保护态再刷新，非法调用退化为无操作
  #[inline]
  pub fn refresh(&self) {
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    if entry.is_protected() {
      // 单属主契约现场核验：替他人保护区刷新会抬升其公布纪元（旧页提前回收）
      debug_assert_eq!(
        entry.thread_id(),
        current_thread_id(),
        "Participant::refresh 仅允许槽位属主线程调用（见模块头线程亲和契约）"
      );
      let current = self.epoch.current_epoch();
      entry.refresh_epoch(current);
      self.epoch.drain_if_pending();
    }
  }

  /// 退出受保护的纪元区
  ///
  /// 对照 C# LightEpoch 的 Suspend 语义（主映射归属 LightEpoch::suspend；本函数为会话句柄层拆分路径）
  /// （Release + drain_count > 0 时 SuspendDrain 的 Participant 侧路径）
  /// 递减重入计数；当重入计数归零时清空受保护的纪元，并在无其他活跃保护者时协助排空就绪延迟动作。
  #[inline]
  pub fn exit(&self) {
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    // 单属主契约现场核验：非属主线程退出会提前解除他人在途保护（悬垂根源）；
    // 未受保护时空闲槽位的线程绑定为 0，多余退出仍为无操作，不触发核验
    debug_assert!(
      !entry.is_protected() || entry.thread_id() == current_thread_id(),
      "Participant::exit 仅允许槽位属主线程调用（见模块头线程亲和契约）"
    );
    if entry.exit() {
      self.epoch.after_release();
    }
  }

  /// 尝试挂起/退出保护区（若当前处于保护区返回 true，否则 false）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/IEpochAccessor.cs:TrySuspend
  #[inline]
  pub fn try_suspend(&self) -> bool {
    if self.is_protected() {
      self.exit();
      true
    } else {
      false
    }
  }

  /// 恢复进入纪元保护区
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/IEpochAccessor.cs:Resume
  #[inline]
  pub fn resume(&self) {
    let tid = current_thread_id();
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    entry.enter_with_tid(&self.epoch.current_epoch, tid);
    self.epoch.drain_if_pending();
  }

  /// 获取当前参与者分配到的条目槽位索引
  #[inline]
  pub fn entry_idx(&self) -> usize {
    self.entry_idx
  }

  /// 检查当前参与者是否正处于保护区
  #[inline]
  pub fn is_protected(&self) -> bool {
    unsafe {
      self
        .epoch
        .entries
        .get_unchecked(self.entry_idx)
        .is_protected()
    }
  }

  /// 获取当前重入计数
  #[inline]
  pub fn reentrant_count(&self) -> u32 {
    unsafe {
      self
        .epoch
        .entries
        .get_unchecked(self.entry_idx)
        .reentrant_count()
    }
  }

  /// 获取当前保护的纪元
  #[inline]
  pub fn protected_epoch(&self) -> u64 {
    unsafe {
      self
        .epoch
        .entries
        .get_unchecked(self.entry_idx)
        .protected_epoch()
    }
  }
}

impl Drop for Participant {
  fn drop(&mut self) {
    // 释放占用的 entry 槽位
    unsafe {
      self
        .epoch
        .entries
        .get_unchecked(self.entry_idx)
        .release_reserve()
    };
    self.epoch.after_release();
  }
}

impl fmt::Debug for Participant {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Participant")
      .field("entry_idx", &self.entry_idx)
      .field("is_protected", &self.is_protected())
      .field("protected_epoch", &self.protected_epoch())
      .field("reentrant_count", &self.reentrant_count())
      .finish()
  }
}

/// 纪元保护 RAII 守卫
///
/// 绑定当前受保护的纪元，离开作用域 Drop 时自动调用 `Participant::exit()`。
pub struct EpochGuard<'a> {
  participant: &'a Participant,
  protected_epoch: u64,
}

impl EpochGuard<'_> {
  /// 获取当前守卫保护的纪元号
  #[inline]
  pub fn protected_epoch(&self) -> u64 {
    self.protected_epoch
  }
}

impl Drop for EpochGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.participant.exit();
  }
}

impl Deref for EpochGuard<'_> {
  type Target = Participant;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.participant
  }
}

impl fmt::Debug for EpochGuard<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("EpochGuard")
      .field("entry_idx", &self.participant.entry_idx)
      .field("protected_epoch", &self.protected_epoch)
      .finish()
  }
}

/// 基于 RAII 作用域自动管理生命周期的保护守卫（对照 libs/storage/Tsavorite/cs/test/test.epoch/helpers/EpochProtection.cs:Scope）
///
/// 绑定当前线程的受保护作用域，离开作用域 Drop 时自动调用 `LightEpoch::suspend()`。
/// 由于底层槽位与线程 ID 绑定，此守卫严禁跨线程转移 (`!Send + !Sync`)。
pub struct ProtectedScope<'a> {
  epoch: &'a Arc<LightEpoch>,
  _marker: PhantomData<*const ()>,
}

impl<'a> ProtectedScope<'a> {
  /// 创建并进入保护区
  pub fn new(epoch: &'a Arc<LightEpoch>) -> Self {
    epoch.resume();
    Self {
      epoch,
      _marker: PhantomData,
    }
  }
}

impl Drop for ProtectedScope<'_> {
  #[inline]
  fn drop(&mut self) {
    self.epoch.suspend();
  }
}

impl Deref for ProtectedScope<'_> {
  type Target = LightEpoch;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.epoch
  }
}

impl fmt::Debug for ProtectedScope<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProtectedScope")
      .field("epoch_id", &self.epoch.id)
      .field("current_epoch", &self.epoch.current_epoch())
      .finish()
  }
}
