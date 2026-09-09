use std::{
  fmt,
  mem::{align_of, offset_of, size_of},
  sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
};

use log::trace;

/// 最大支持的每个条目独立用户字 (UserWord) 槽位数
///
/// 对照 C# `LightEpoch.MaxUserWords = 6`：C# Entry 布局为 8(epoch) + 4(int threadId) +
/// 48(6 字) = 64；Rust 的 thread_id 用全局唯一 u64（8 字节，杜绝 C# managed thread id
/// 复用导致的判定歧义）并增设 reentrant(4) + reserved(1) 双机制字段，缓存行余量降为
/// 40 字节 = 5 字，同样恰好填满 64 字节缓存行
pub const MAX_USER_WORDS: usize = 5;

/// 严格 64 字节 CPU Cacheline 对齐的纪元条目结构
///
/// 对照 Microsoft Garnet Tsavorite 的 Entry 设计，保证每个参与者条目独占独立缓存行，
/// 彻底杜绝多核并发访问时的伪共享 (False Sharing) 性能衰减。
/// 内置 5 个 64 位用户字 (UserWord) 槽位，完美填满 64 字节缓存行 (8 + 8 + 4 + 1 + 3 + 40 = 64)。
#[repr(C, align(64))]
pub struct EpochEntry {
  /// 当前受保护的纪元（0 表示未进入保护区/空闲）
  epoch: AtomicU64,
  /// 占用当前槽位的线程 ID（0 表示无线程占用）
  thread_id: AtomicU64,
  /// 重入保护计数
  reentrant: AtomicU32,
  /// 是否被 Participant 显式句柄占用
  reserved: AtomicBool,
  /// 显式对齐补齐 3 字节
  _pad: [u8; 3],
  /// 用户字槽位（每个 8 字节，供子系统如 TsavoriteLog 低开销共享缓存行）
  user_words: [AtomicI64; MAX_USER_WORDS],
}

// 编译期钉死条目内存布局与 64 字节独占对齐
const _: () = {
  assert!(size_of::<EpochEntry>() == 64);
  assert!(align_of::<EpochEntry>() == 64);
  assert!(offset_of!(EpochEntry, epoch) == 0);
  assert!(offset_of!(EpochEntry, thread_id) == 8);
  assert!(offset_of!(EpochEntry, reentrant) == 16);
  assert!(offset_of!(EpochEntry, reserved) == 20);
  assert!(offset_of!(EpochEntry, user_words) == 24);
};

impl EpochEntry {
  /// 创建一个空闲的未占用条目
  pub const fn new() -> Self {
    Self {
      epoch: AtomicU64::new(0),
      thread_id: AtomicU64::new(0),
      reentrant: AtomicU32::new(0),
      reserved: AtomicBool::new(false),
      _pad: [0; 3],
      user_words: [const { AtomicI64::new(0) }; MAX_USER_WORDS],
    }
  }

  /// 尝试原子占用当前条目槽位（供 Participant 显式句柄使用）
  ///
  /// 与 `try_claim` 跨 `reserved`/`epoch` 两变量构成 Dekker 式互斥：
  /// 双方均为「先检对方标志 → CAS 己方变量 → 复检对方标志」，store-load 配对
  /// 依赖全序，故本路径必须使用 SeqCst，不可降级。
  #[inline]
  pub fn try_reserve(&self) -> bool {
    if self.epoch.load(Ordering::SeqCst) != 0 {
      return false;
    }
    if self
      .reserved
      .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
      .is_err()
    {
      return false;
    }
    // 严格双检：防止并发 try_claim 在 CAS 间隙抢先占领槽位
    if self.epoch.load(Ordering::SeqCst) != 0 {
      self.reserved.store(false, Ordering::SeqCst);
      return false;
    }
    true
  }

  /// 释放对当前条目槽位的预留占用
  #[inline]
  pub fn release_reserve(&self) {
    trace!("释放 EpochEntry 条目槽位");
    self.reset();
    self.reserved.store(false, Ordering::SeqCst);
  }

  /// 强制清空槽位保护状态与线程绑定
  ///
  /// 统一的槽位清零序列：reentrant → thread_id → epoch(Release)。
  /// `epoch=0` 是槽位空闲的唯一对外发布点，必须最后落笔（对照 C# Release 不变量）；
  /// 供 `exit` 正常退出与 `release_reserve` / TLS 兜底回收等 Drop 路径复用。
  #[inline]
  pub fn reset(&self) {
    self.reentrant.store(0, Ordering::Relaxed);
    self.thread_id.store(0, Ordering::Relaxed);
    self.epoch.store(0, Ordering::Release);
  }

  /// 进入纪元保护区（现场原子读取全局纪元并记录线程 ID）
  ///
  /// 内存序安全论证：槽位一经 `try_reserve`（reserved=true 独占，`try_claim` 拒绝 reserved 槽）
  /// 或 `try_claim`（epoch CAS 0→e 独占发布）归属唯一属主线程，`reentrant`/`thread_id`
  /// 只有属主线程读写，不存在 CAS 状态机原本要防的跨线程竞争，Relaxed 读改写即安全；
  /// 对外发布仍只靠下方 `epoch` 单点 Release store——读者 Acquire 读到非零 epoch
  /// 即与本线程此前全部写建立 happens-before（对照 C# Tsavorite Resume 的普通写实现）。
  #[inline]
  pub fn enter_with_tid(&self, current_epoch: &AtomicU64, thread_id: u64) -> u64 {
    // 仅属主线程读写 reentrant，Relaxed 读旧值 + Relaxed 写新值即构成无竞争原子更新
    let prev = self.reentrant.load(Ordering::Relaxed);
    if prev > 0 {
      self.reentrant.store(prev + 1, Ordering::Relaxed);
      return self.epoch.load(Ordering::Acquire);
    }
    // 现场原子读取当前全局纪元，杜绝公布早已推进回收的过期纪元
    let epoch = current_epoch.load(Ordering::Acquire);
    self.thread_id.store(thread_id, Ordering::Relaxed);
    self.epoch.store(epoch, Ordering::Release);
    self.reentrant.store(1, Ordering::Relaxed);
    epoch
  }

  /// 退出纪元保护区
  ///
  /// 递减重入计数；计数降为 0 时清理受保护的纪元与线程 ID。
  /// 返回 true 表示槽位已完全退出保护区（计数降为 0），false 表示重入尚未清零或本就未受保护。
  ///
  /// 内存序安全论证：`reentrant` 为属主线程私有字，Relaxed 增减即可（见 `enter_with_tid`）。
  /// 槽位释放的对外发布仍只靠最后的 `epoch.store(0, Release)` 单点：
  /// - Release 语义保证此前的 `reentrant=0`/`thread_id=0` 先于发布点可见，且不会重排到其后；
  /// - TLS 抢占者 `try_claim` 经 SeqCst load/CAS 观察到 epoch==0（≥Acquire），即与本线程
  ///   全部前置 store 建立 happens-before，其后续 `reentrant=1` 绝不会被本线程仍在途的
  ///   `reentrant=0` 晚到覆盖（次序颠倒会让重入计数被清零，槽位从此无人能释放，纪元永久泄漏）。
  #[inline]
  pub fn exit(&self) -> bool {
    let prev = self.reentrant.load(Ordering::Relaxed);
    if prev == 0 {
      return false;
    }
    if prev > 1 {
      self.reentrant.store(prev - 1, Ordering::Relaxed);
      return false;
    }
    // 对照 C# Release 不变量：epoch=0 是槽位空闲的唯一发布点，必须最后落笔
    // （reset 的清零序列恰为 reentrant → thread_id → epoch(Release)，复用即保证次序）
    self.reset();
    true
  }

  /// 尝试为线程原子占用并进入保护区（对照 C# Tsavorite TryClaimEntry）
  ///
  /// 时序严格对齐 C#：先观察槽位空闲，再现场读取全局当前纪元，最后以 CAS
  /// 一次性完成槽位占用与纪元发布（CAS 同时充当内存发布屏障）。
  /// 双检次序参见 `try_reserve`：与对方标志构成 Dekker 式互斥，须 SeqCst。
  #[inline]
  pub fn try_claim(&self, thread_id: u64, current_epoch: &AtomicU64) -> bool {
    if self.epoch.load(Ordering::SeqCst) != 0 || self.reserved.load(Ordering::SeqCst) {
      return false;
    }
    // 现场读取全局纪元，杜绝公布过期纪元（对照 C# Volatile.Read(ref CurrentEpoch)）
    let epoch = current_epoch.load(Ordering::Acquire);
    if self
      .epoch
      .compare_exchange(0, epoch, Ordering::SeqCst, Ordering::SeqCst)
      .is_err()
    {
      return false;
    }
    // 严格双检：防止并发 try_reserve 抢占了此槽位
    if self.reserved.load(Ordering::SeqCst) {
      self.epoch.store(0, Ordering::SeqCst);
      return false;
    }
    self.thread_id.store(thread_id, Ordering::Release);
    self.reentrant.store(1, Ordering::Release);
    true
  }

  /// 刷新当前条目公布的纪元（对照 C# Tsavorite ProtectAndDrain 刷新路径）
  #[inline]
  pub fn refresh_epoch(&self, new_epoch: u64) {
    self.epoch.store(new_epoch, Ordering::Release);
  }

  /// 获取当前条目受保护的纪元号（0 表示不在保护区）
  #[inline]
  pub fn protected_epoch(&self) -> u64 {
    self.epoch.load(Ordering::Acquire)
  }

  /// 检查当前条目是否正处于保护区
  #[inline]
  pub fn is_protected(&self) -> bool {
    self.protected_epoch() != 0
  }

  /// 获取当前条目的重入深度
  ///
  /// 仅属主线程读写该私有字，无跨线程发布需求，Relaxed 即可
  #[inline]
  pub fn reentrant_count(&self) -> u32 {
    self.reentrant.load(Ordering::Relaxed)
  }

  /// 递增当前条目的重入计数（用于 resume 快路径重入）
  ///
  /// 调用前提是本线程已持槽保护态（epoch 非零期间槽位不可能被他人抢占），
  /// reentrant 为属主私有字，Relaxed 读-写即构成无竞争原子更新，无需 CAS 自旋。
  #[inline]
  pub fn inc_reentrant(&self) {
    let prev = self.reentrant.load(Ordering::Relaxed);
    self.reentrant.store(prev + 1, Ordering::Relaxed);
  }

  /// 获取占用当前条目的线程 ID（0 表示未占用）
  #[inline]
  pub fn thread_id(&self) -> u64 {
    self.thread_id.load(Ordering::Acquire)
  }

  /// 读取指定槽位的用户字（无边界检查快速路径）
  ///
  /// # Safety
  /// 调用方须保证 `idx < MAX_USER_WORDS`
  #[inline]
  pub unsafe fn user_word_unchecked(&self, idx: usize) -> i64 {
    debug_assert!(idx < MAX_USER_WORDS);
    // SAFETY: 调用方保证 idx < MAX_USER_WORDS
    unsafe { self.user_words.get_unchecked(idx) }.load(Ordering::Acquire)
  }

  /// 写入指定槽位的用户字（无边界检查快速路径）
  ///
  /// # Safety
  /// 调用方须保证 `idx < MAX_USER_WORDS`
  #[inline]
  pub unsafe fn set_user_word_unchecked(&self, idx: usize, val: i64) {
    debug_assert!(idx < MAX_USER_WORDS);
    // SAFETY: 调用方保证 idx < MAX_USER_WORDS
    unsafe { self.user_words.get_unchecked(idx) }.store(val, Ordering::Release);
  }

  /// 获取指定槽位用户字的原子引用（无边界检查快速路径）
  ///
  /// # Safety
  /// 调用方须保证 `idx < MAX_USER_WORDS`
  #[inline]
  pub unsafe fn user_word_atomic_unchecked(&self, idx: usize) -> &AtomicI64 {
    debug_assert!(idx < MAX_USER_WORDS);
    // SAFETY: 调用方保证 idx < MAX_USER_WORDS
    unsafe { self.user_words.get_unchecked(idx) }
  }
}

impl Default for EpochEntry {
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for EpochEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("EpochEntry")
      .field("epoch", &self.epoch.load(Ordering::Relaxed))
      .field("thread_id", &self.thread_id.load(Ordering::Relaxed))
      .field("reentrant", &self.reentrant.load(Ordering::Relaxed))
      .field("reserved", &self.reserved.load(Ordering::Relaxed))
      .finish()
  }
}
