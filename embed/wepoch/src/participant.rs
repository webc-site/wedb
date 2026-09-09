//! 参与者会话句柄与 RAII 保护守卫
//!
//! 对照 C# LightEpoch 的单一保护机制：Rust 拆分出 TLS 作用域（resume/suspend）
//! 与 `Participant` 显式句柄双轨机制，本模块承载后者——代表单个线程或客户端会话
//! 在 `LightEpoch` 中的长期登记（如批处理会话），独占一个 `EpochEntry` 槽位。

use std::{
  fmt,
  marker::PhantomData,
  ops::Deref,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use wbase::thread::current_thread_id;

use crate::{Error, LightEpoch, MAX_USER_WORDS, Result};

/// 参与者会话句柄
///
/// 代表单个线程或客户端会话在 `LightEpoch` 中的登记。
/// 每个参与者独占一个 `EpochEntry` 槽位，不可被 Clone。
pub struct Participant {
  epoch: Arc<LightEpoch>,
  entry_idx: usize,
}

impl Participant {
  pub(crate) fn new(epoch: Arc<LightEpoch>, entry_idx: usize) -> Self {
    Self { epoch, entry_idx }
  }

  /// 进入受保护的纪元区，返回 RAII 守卫 `EpochGuard`
  ///
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

  /// 刷新当前参与者公布的纪元至最新值，并触发就绪的延迟动作（对照 libs/client/LightEpoch.cs:ProtectAndDrain）
  #[inline]
  pub fn refresh(&self) {
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    if entry.is_protected() {
      let current = self.epoch.current_epoch();
      entry.refresh_epoch(current);
      self.epoch.drain_if_pending();
    }
  }

  /// 退出受保护的纪元区
  ///
  /// 递减重入计数；当重入计数归零时清空受保护的纪元，并在无其他活跃保护者时协助排空就绪延迟动作。
  #[inline]
  pub fn exit(&self) {
    let entry = unsafe { self.epoch.entries.get_unchecked(self.entry_idx) };
    if entry.exit() {
      self.epoch.after_release();
    }
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

  /// 校验用户字索引并返回参与者槽位上该列的原子引用
  #[inline]
  fn user_word_ref(&self, word_index: usize) -> Result<&AtomicI64> {
    if word_index >= MAX_USER_WORDS {
      return Err(Error::InvalidUserWordIndex(word_index));
    }
    unsafe {
      Ok(
        self
          .epoch
          .entries
          .get_unchecked(self.entry_idx)
          .user_word_atomic_unchecked(word_index),
      )
    }
  }

  /// 获取参与者对应的用户字值
  #[inline]
  pub fn user_word(&self, word_index: usize) -> Result<i64> {
    Ok(self.user_word_ref(word_index)?.load(Ordering::Acquire))
  }

  /// 设置参与者对应的用户字值
  #[inline]
  pub fn set_user_word(&self, word_index: usize, val: i64) -> Result<()> {
    self
      .user_word_ref(word_index)?
      .store(val, Ordering::Release);
    Ok(())
  }

  /// 获取参与者对应用户字的原子引用
  #[inline]
  pub fn user_word_atomic(&self, word_index: usize) -> Result<&AtomicI64> {
    self.user_word_ref(word_index)
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
  epoch: &'a LightEpoch,
  _marker: PhantomData<*const ()>,
}

impl<'a> ProtectedScope<'a> {
  /// 创建并进入保护区
  pub fn new(epoch: &'a LightEpoch) -> Self {
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
