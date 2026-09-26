use std::{
  fmt,
  mem::{align_of, offset_of, size_of},
  sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

use log::trace;

/// 严格 64 字节 CPU Cacheline 对齐的纪元条目结构
///
/// 对照 Microsoft Garnet Tsavorite 的 Entry 设计，保证每个参与者条目独占独立缓存行，
/// 彻底杜绝多核并发访问时的伪共享 (False Sharing) 性能衰减。
/// C# Entry 用缓存行余量挂 6 个用户字槽位（供 TsavoriteLog 在途水位复用）；Rust 的在途
/// 水位由 waof 的 inflight_slots 单点承接，条目不预留该空间，余量交由 align(64) 补齐。
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
}

// 编译期钉死条目内存布局与 64 字节独占对齐
const _: () = {
  assert!(size_of::<EpochEntry>() == 64);
  assert!(align_of::<EpochEntry>() == 64);
  assert!(offset_of!(EpochEntry, epoch) == 0);
  assert!(offset_of!(EpochEntry, thread_id) == 8);
  assert!(offset_of!(EpochEntry, reentrant) == 16);
  assert!(offset_of!(EpochEntry, reserved) == 20);
};

impl EpochEntry {
  /// 创建一个空闲的未占用条目
  pub const fn new() -> Self {
    Self {
      epoch: AtomicU64::new(0),
      thread_id: AtomicU64::new(0),
      reentrant: AtomicU32::new(0),
      reserved: AtomicBool::new(false),
    }
  }

  /// 尝试原子占用当前条目槽位（供 Participant 显式句柄使用）
  ///
  /// C# 无显式预留机制（单一保护机制经 Acquire 隐式占位）；Rust 双轨拆分新增，
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
  /// `epoch=0` 是槽位空闲的唯一对外发布点，必须最后落笔（对照 C# Release 不变量）。
  ///
  /// 调用前提：本线程为该槽位唯一属主且已无任何在途重入（`Participant::drop` 归还
  /// 预留、线程退出 TLS 兜底回收两条 Drop 路径专用）。因含无条件的 `reentrant=0` 覆写，
  /// 正常退出路径须走 [`Self::exit`] 的 CAS 递减，不可复用本函数。
  #[inline]
  pub fn reset(&self) {
    self.reentrant.store(0, Ordering::Relaxed);
    self.thread_id.store(0, Ordering::Relaxed);
    self.epoch.store(0, Ordering::Release);
  }

  /// 进入纪元保护区（现场原子读取全局纪元并记录线程 ID）
  ///
  /// 对照 C# LightEpoch 的 Acquire 语义（主映射归属 LightEpoch::resume；本函数为槽位层拆分实现）
  /// （ReserveEntryForThread 占位 + 公布纪元的条目侧实现，占位与公布合并为单条 CAS
  /// 严格对标 `LightEpoch.cs:607-620` 的 `TryClaimEntry`）
  ///
  /// 内存序安全论证：
  /// - 首层进入以 `epoch` 的 CAS 0→e 一次性完成「占用槽位」与「公布纪元」，
  ///   CAS 本身即发布屏障；独占成立后才写 `thread_id` 与 `reentrant`
  ///   （对标 C#「The slot is now exclusively ours, so threadId needs no interlocked write」），
  ///   故竞争者绝无可能把自己绑定的线程 ID 覆盖到他人名下；
  /// - `reentrant` 的递增一律走 CAS，杜绝两个并发进入者各自读到同一旧值、
  ///   把两层重入记为一层的写丢失；
  /// - 首层那句 `reentrant.store(1)` 是本字段唯一的非 RMW 写入点，其独占性由 preceding
  ///   `epoch` CAS 担保：此刻槽位纪元已非零，任何后进者只能观察到非零纪元并退入
  ///   重入递增路径（或在本句落定前空转），不可能并发再写 1。
  ///
  /// 为何计数走 CAS 而非裸读写：`Participant` 已收紧为 `!Sync`
  /// （`participant.rs`），跨线程共享进出在编译期即被拒绝，单属主前提下本字段
  /// 实际无并发写者；CAS 递增保留为纵深防御——槽位状态同时被回收线程
  /// `bump_and_wait` / `thread_protected_entry` 跨线程观测，原子 RMW 使计数在
  /// 任何误用形态下自证完整（不会因写丢失提前 `reset` 解除他人在途保护），
  /// 无竞争时单次 CAS 即落定，不构成运行时开销。
  #[inline]
  pub fn enter_with_tid(&self, current_epoch: &AtomicU64, thread_id: u64) -> u64 {
    let mut cur = self.reentrant.load(Ordering::Acquire);
    loop {
      if cur > 0 {
        // 重入层：单条 CAS 递增；失败则以最新观测值重试
        match self.reentrant.compare_exchange_weak(
          cur,
          cur + 1,
          Ordering::AcqRel,
          Ordering::Acquire,
        ) {
          // 重入层不改写已公布的保护纪元，返回槽位当前公布的纪元
          Ok(_) => return self.epoch.load(Ordering::Acquire),
          Err(next) => {
            cur = next;
            continue;
          }
        }
      }
      // 现场原子读取当前全局纪元，杜绝公布早已推进回收的过期纪元
      let epoch = current_epoch.load(Ordering::Acquire);
      // 单条 CAS 同时完成占位与纪元公布；失败说明有并发进入者已公布，转重入递增
      if self
        .epoch
        .compare_exchange(0, epoch, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
      {
        cur = self.reentrant.load(Ordering::Acquire);
        continue;
      }
      self.thread_id.store(thread_id, Ordering::Relaxed);
      self.reentrant.store(1, Ordering::Relaxed);
      return epoch;
    }
  }

  /// 退出纪元保护区
  ///
  /// 对照 C# LightEpoch 的 Release 语义（主映射归属 LightEpoch::suspend；本函数为槽位层拆分实现）
  /// 递减重入计数；计数降为 0 时清理受保护的纪元与线程 ID。
  /// 返回 true 表示槽位已完全退出保护区（计数降为 0），false 表示重入尚未清零或本就未受保护。
  ///
  /// 内存序安全论证：
  /// - `reentrant` 以 CAS 递减，唯有把计数从 1 原子翻转为 0 的这一次退出才取得
  ///   「释放者」身份，彻底排除两个并发退出各读到 1 而双双 `reset` 的丢失更新
  ///   （后者即提前释放槽位、在途指针悬垂的根因）；
  /// - 槽位释放的对外发布仍只靠末尾 `epoch.store(0, Release)` 单点，清零序与 C#
  ///   `LightEpoch.cs:542-549` 的 Release 不变量同（先清 threadId，后零 localCurrentEpoch）：
  ///   Release 保证此前的 `thread_id=0` 不会重排到发布点之后；
  /// - 后继抢占者（`try_claim` 或本函数首层 CAS）只有观察到 epoch==0 才能占位，
  ///   其自身独占的 `reentrant.store(1)` 必晚于本线程的计数 CAS，绝不会被本线程
  ///   仍在途的清零覆盖（次序颠倒会将重入计数抹为 0，槽位从此无人能释放，纪元永久泄漏）；
  /// - 正因如此，末层退出不得复用 `reset()`——它会无条件再写一次 `reentrant=0`。
  #[inline]
  pub fn exit(&self) -> bool {
    let mut cur = self.reentrant.load(Ordering::Acquire);
    loop {
      if cur == 0 {
        // 本就未受保护：不触发任何释放与收尾
        return false;
      }
      if self
        .reentrant
        .compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
      {
        cur = self.reentrant.load(Ordering::Acquire);
        continue;
      }
      if cur > 1 {
        // 仍有外层重入，槽位继续受保护
        return false;
      }
      // 末层退出：epoch=0 是槽位空闲的唯一发布点，必须最后落笔
      self.thread_id.store(0, Ordering::Relaxed);
      self.epoch.store(0, Ordering::Release);
      return true;
    }
  }

  /// 尝试为线程原子占用并进入保护区（对照 C# Tsavorite TryClaimEntry）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:TryClaimEntry
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

  /// 获取当前条目公布的纪元号（严格对标 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Entry.localCurrentEpoch）
  #[inline(always)]
  pub fn local_current_epoch(&self) -> u64 {
    self.protected_epoch()
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

/// 严格对标 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Entry.ToString
impl fmt::Display for EpochEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "lce = {}, tid = {}",
      self.epoch.load(Ordering::Relaxed),
      self.thread_id.load(Ordering::Relaxed)
    )
  }
}
