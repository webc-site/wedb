use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread::yield_now,
};

use compio::time::sleep;
use log::trace;
use wbase::backoff::{Backoff, BackoffStage, SLEEP_DURATION};
use wdev::Device;
use wepoch::{LightEpoch, current_thread_id};

use super::HybridLog;
use crate::error::{Error, Result};

/// 三级退避：wbase::backoff 阶段机的异步适配（自旋 → yield 让核 → compio 异步睡眠）
///
/// # 与 C# 的等价性（刻意差异）
/// C# 无异步模型，等待纯同步——AllocatorBase.cs:1547-1563 WaitToRetryNow 与
/// 1675-1686 ShiftBeginAddress 的等待协议均为「Thread.Yield 自旋 kFlushSpinCount(10) 轮
/// → epoch.Suspend + flushEvent.Wait(1ms 超时) 阻塞等待 → epoch.Resume」。本实现
/// compio 线程每核模型禁止阻塞 reactor：前两阶段与 C# 同为纯同步自旋/让核；第三
/// 阶段以 compio::time::sleep(50μs) 异步睡眠替代 flushEvent.Wait 的阻塞语义——挂起
/// 当前任务并把 reactor 执行权让渡给同线程其他任务（含本屏障依赖的 drain 收割），
/// 等待语义等价（周期性复查谓词），阻塞性相反（绝不挂起线程）。阶段阈值复用
/// wbase::backoff 单一真源（32 自旋 / 1024 让核 / 50μs 睡眠），与 wepoch drain
/// 等待（epoch.rs 的 Backoff::snooze 循环）同构，杜绝 whlog 内私有阈值分叉。
#[inline]
async fn backoff(bo: &mut Backoff) {
  match bo.stage() {
    BackoffStage::Spin => spin_loop(),
    BackoffStage::Yield => yield_now(),
    BackoffStage::Sleep => sleep(SLEEP_DURATION).await,
  }
  bo.advance();
}

/// RAII 守卫：确保无论正常退出、异步 Future 被 Drop（取消/超时）还是 panic 展开，
/// 都能按退出深度严格重入 TLS 纪元保护区，保障异步取消安全性（Cancellation Safety）。
struct TlsResumeGuard<'a> {
  epoch: &'a LightEpoch,
  count: usize,
}

impl Drop for TlsResumeGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    for _ in 0..self.count {
      self.epoch.resume();
    }
  }
}

impl<D: Device> HybridLog<D> {
  /// 推进 ReadOnlyAddress 并通过 Epoch 延迟更新 SafeReadOnlyAddress
  ///
  /// 对标 C# ShiftReadOnlyAddress：Unsafe 状态先行发布，Safe 状态经
  /// `BumpCurrentEpoch(Action)` 在纪元排空（所有旧纪元在途写入完成）后推进，
  /// 这正是刷盘/驱逐可以安全触达的严谨边界。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftReadOnlyAddress
  pub fn shift_read_only_address(&self, new_ro: u64) {
    let old_ro = self.addresses.shift_read_only_address(new_ro);
    if new_ro > old_ro {
      let addrs = Arc::clone(&self.addresses);
      self.epoch.bump_current_epoch_action(move || {
        addrs.shift_safe_read_only_address(new_ro);
        trace!("Epoch 安全推进 SafeReadOnlyAddress 至 {new_ro:#x}");
      });
      if !self.epoch.this_instance_protected() {
        self.epoch.bump_epoch();
      }
    }
  }

  /// 推进 HeadAddress 并通过 Epoch 延迟更新 SafeHeadAddress
  ///
  /// 对标 C# ShiftHeadAddress / OnPagesClosed：head 内联推进使页槽位可进入待驱逐
  /// 候选，SafeHeadAddress 待纪元排空后推进——确保旧页上所有 epoch 保护的读者
  /// （裸指针直读）退出后槽位才可清零复用。
  ///
  /// 设计边界说明（联动 windex 扩容）：页复用的 quiesce 完全依赖 SafeHead 的
  /// 纪元语义，与索引层"前台持旧表桶引用期间切表"所需的 epoch/quiesce 为同一
  /// 原语；hlog 自身不持有任何索引结构引用，切表时序由 store/session 层保证。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress
  pub fn shift_head_address(&self, new_head: u64) {
    // 对齐 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:ShiftHeadAddress：head 钳制到最后已刷盘地址，
    // 驱逐不越过持久化前缀（维持 head <= flushed_until 不变式）
    let new_head = new_head.min(self.addresses.flushed_until());
    let old_head = self.addresses.shift_head_address(new_head);
    if new_head > old_head {
      let addrs = Arc::clone(&self.addresses);
      self.epoch.bump_current_epoch_action(move || {
        addrs.shift_safe_head_address(new_head);
        trace!("Epoch 安全推进 SafeHeadAddress 至 {new_head:#x}");
      });
      if !self.epoch.this_instance_protected() {
        self.epoch.bump_epoch();
      }
    }
  }

  /// 推进 BeginAddress 并截断过期的历史存储段（对标 C# ShiftBeginAddress）
  ///
  /// 核心顺序对齐 C#（只读区封存 → 补刷 → head → 截断），begin 时序为刻意差异：
  /// C# 最先推进 begin（AllocatorBase.ShiftBeginAddress "First update the begin
  /// address"），此处刻意后置——先补刷 `[flushed_until, new_begin)` 确保驱逐不越过
  /// 持久化前缀（维持 `head <= flushed_until` 快照不变式），随后推进 head（Epoch
  /// 延迟推进 safe_head，绝不内联强推）与 begin，避免补刷窗口内 begin 先行造成
  /// `[head, new_begin)` 提前不可见，最后经纪元排空屏障后物理截断设备历史段。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftBeginAddress
  pub async fn shift_begin_address(&self, new_begin: u64) -> Result<()> {
    if new_begin <= self.addresses.begin() {
      return Ok(());
    }
    let tail = self.addresses.tail();
    if new_begin > tail {
      return Err(Error::AddressOutOfRange {
        addr: new_begin,
        begin: self.addresses.begin(),
        tail,
      });
    }

    // 对齐 C#（AllocatorBase.cs:1667 无条件 ShiftReadOnlyAddress(newBeginAddress)）：
    // 无论区间是否已预刷盘，先冻结 [ro, new_begin) 为只读——补刷与截断窗口内
    // 原位更新会撕裂设备上的待截断前缀，冻结后原位更新改走 RCU 追加；
    // ro 已达标时 fetch_max 空转，零额外成本
    self.shift_read_only_address(new_begin);

    let flushed = self.addresses.flushed_until();
    if flushed < new_begin {
      // 批量补刷 [flushed, new_begin)（区间页仍驻留环形缓冲：head 尚未推进，
      // 必然可读）；冻结只读已由入口处无条件 ro-shift 完成，补刷窗口内的
      // 原位更新会撕裂设备上的待截断前缀，冻结后原位更新改走 RCU 追加
      self.flush_addr_range(flushed, new_begin).await?;
    }
    if self.addresses.flushed_until() < new_begin {
      return Err(Error::InvalidState(
        "shift_begin_address 补刷后待截断区间仍未完整落盘（flushed_until < new_begin）".into(),
      ));
    }

    self.shift_head_address(new_begin);
    // begin 仅推进自身；head/safe_head 的连带推进已由 shift_head_address 以
    // 纪元安全方式完成（"连带内联强推 safe_head"会绕过纪元排空语义，故不采用）
    self.addresses.shift_begin_address(new_begin);

    // 截断前纪元排空屏障：等待 safe_head 与 safe_read_only 均达标且读者全部退出后才物理截断
    self.wait_safe_read_only_drained(new_begin).await;

    let dev = Arc::clone(&self.device);
    dev
      .truncate_until_address(new_begin)
      .await
      .map_err(Error::from)
  }

  /// 通用纪元等待屏障：解除本线程旧纪元自钉，循环等待直至谓词满足
  async fn wait_epoch_condition<F>(&self, mut condition: F)
  where
    F: FnMut() -> bool,
  {
    if condition() {
      return;
    }
    let exited = self.unpin_self();
    let _guard = TlsResumeGuard {
      epoch: &self.epoch,
      count: exited,
    };
    let mut backoff_state = Backoff::new();
    while !condition() {
      if backoff_state.step_count().is_multiple_of(64) {
        let new_epoch = self.epoch.bump_epoch();
        let tid = current_thread_id();
        for entry in self.epoch.entries.iter() {
          if entry.is_protected() && entry.thread_id() == tid {
            entry.refresh_epoch(new_epoch);
          }
        }
      } else {
        self.epoch.drain();
      }
      backoff(&mut backoff_state).await;
    }
  }

  /// 截断前纪元排空屏障：异步等待直至封口 action 排空且 `safe_head >= new_begin`
  ///
  /// # C# 对应关系与刻意差异
  /// 对标 C# AllocatorBase.cs:1699-1705——C# 把 `TruncateUntilAddress(newBeginAddress)`
  /// 放进 `epoch.BumpCurrentEpoch(() => { OnPagesClosed(...); TruncateUntilAddress(...); })`
  /// 的 drain 动作闭包内，动作只在纪元完全排空后执行，即"先排空、后截断"（checkpoint
  /// 状态机 StateMachineDriver.cs:247 同构：`BumpCurrentEpoch(() => MakeTransitionWorker)`
  /// 包裹易碎阶段转换）。Rust 截断为 compio 异步设备 I/O，无法塞入 `Send + 'static`
  /// 同步闭包，故等价改写为显式屏障：先注册封口 action 再等待其排空。
  ///
  /// # 修复的竞态（封口动作为何必须注册在 begin 挪线之后）
  /// 磁盘区在途读者（`read_disk_record` 免纪元 I/O）的 `is_on_disk` 采样与设备读非
  /// 原子：采样为真到 I/O 完成之间段文件可能被删。begin 挪线后新读者经 `is_on_disk`
  /// 永远采不到待截断区间；而**挪线前已入场**的读者（含持守卫横跨 I/O 的
  /// `session.read_record` 协议）由封口 action 钉在其入场纪元上——屏障等待封口
  /// action 排空，截断因此严格 happens-after 全部存量磁盘读者的 I/O 完成。
  /// 封口 action 同时推进 `SafeHeadAddress` 与 `SafeReadOnlyAddress`，确保排空完成后
  /// `safe_head >= new_begin` 且单调递增不变式 `begin <= safe_head` 严格保持。
  ///
  /// # 无死锁论证（封口 action 由已注册 drain action 推进的条件链）
  /// 1. 本屏障在 begin 挪线后注册封口 action A（触发纪元 = 注册时 bump 前的全局
  ///    纪元 E），A 执行体推进 safe_ro/safe_head 并置位 `drained` 使谓词满足；
  /// 2. A 就绪 ⟺ `safe_to_reclaim_epoch >= E` ⟺ 条目表所有公布纪元 > E；
  /// 3. 本线程若持 TLS 保护区（`ProtectedScope`/`resume` 路径），公布纪元 <= E 将永久
  ///    自钉 A——已先逐层 `suspend` 完全退出并计数，屏障经 RAII [`TlsResumeGuard`] 无论
  ///    正常完成还是 Future 被 Drop 均按原深度 `resume` 重入（取消安全，完全杜绝
  ///    守卫深度丢失引发的 UAF）；
  /// 4. 本线程若持 `Participant` 句柄守卫（守卫归调用方栈帧所有，本函数无法代为
  ///    exit/重入），仿 `LightEpoch::help_drain`（对照 C# ProtectAndDrain）把本线程
  ///    名下仍受保护条目刷新至全局最新纪元，解除对 <= E 旧纪元的自钉；
  /// 5. 其余线程的保护区均为有穷临界区（LightEpoch 契约），终将退出；最后一名退出者
  ///    经 suspend_drain/drain 执行 A，drained 变为 true，循环有穷。等待体每轮优先
  ///    调用 `drain` 收割就绪动作，周期性轻量 `bump_epoch`，配合三级退避（含
  ///    compio::time::sleep 异步让渡 reactor），不裸忙等、不烧核、不阻塞单线程执行器。
  async fn wait_safe_read_only_drained(&self, new_begin: u64) {
    let drained = Arc::new(AtomicBool::new(false));
    let drained_clone = Arc::clone(&drained);
    let addrs = Arc::clone(&self.addresses);
    self.epoch.bump_current_epoch_action(move || {
      addrs.shift_safe_read_only_address(new_begin);
      addrs.shift_safe_head_address(new_begin);
      drained_clone.store(true, Ordering::Release);
      trace!("Epoch 封口推进 SafeHead/SafeReadOnly 至 {new_begin:#x}");
    });
    self
      .wait_epoch_condition(|| {
        drained.load(Ordering::Acquire) && self.addresses.safe_head() >= new_begin
      })
      .await;
    trace!("截断屏障排空完成：safe_head 已越过 {new_begin:#x}");
  }

  /// 临时解除本线程对旧纪元的自钉，返回需重入的 TLS 保护深度
  ///
  /// - TLS 保护区：逐层 `suspend` 完全退出（`suspend` 幂等且按重入计数递减，
  ///   循环至 `this_instance_protected` 为假），屏障后按返回深度 `resume` 重入；
  /// - `Participant` 句柄守卫：仿 `LightEpoch::help_drain` 语义把本线程名下仍受
  ///   保护条目刷新至全局最新纪元。刷新契约与 help_drain/ProtectAndDrain 一致：
  ///   刷新窗口内调用方不得持有指向可复用页的裸指针——begin 已先行推进，截断线
  ///   以下地址此后不可能再经内存区合法访问（`probe_resident` 的 head 边界检查
  ///   逐次生效），契约自然闭合。
  fn unpin_self(&self) -> usize {
    let mut exited = 0usize;
    while self.epoch.this_instance_protected() {
      self.epoch.suspend();
      exited += 1;
    }
    let tid = current_thread_id();
    let current = self.epoch.current_epoch();
    for entry in self.epoch.entries.iter() {
      // 条目公布纪元恒 <= 全局当前纪元，刷新为单调不回退，写侧 Release 与
      // 排空侧 Acquire 配对（同 help_drain 的 refresh_epoch(current) 语义）
      if entry.is_protected() && entry.thread_id() == tid {
        entry.refresh_epoch(current);
      }
    }
    exited
  }

  /// 获取当前 TailAddress
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetTailAddress
  /// （C# 另有 UnstableGetTailAddress 快照变体，Rust 原子加载等价，单一入口即可）
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.addresses.tail()
  }

  /// 获取安全尾部逻辑地址（对标 C# TsavoriteLog.SafeTailAddress）
  #[inline]
  pub fn safe_tail_address(&self) -> u64 {
    self.addresses.safe_tail()
  }

  /// 将 ReadOnlyAddress 迅速推进至当前 TailAddress（对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftReadOnlyToTail，使当前所有数据瞬间变为只读不可变）
  #[inline]
  pub fn shift_read_only_to_tail(&self) -> u64 {
    let tail = self.addresses.tail();
    self.shift_read_only_address(tail);
    tail
  }

  /// 推进 ReadOnlyAddress 并可选等待刷盘完成（对标 C# ShiftReadOnlyAddressWithWait）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftReadOnlyAddressWithWait
  pub async fn shift_read_only_address_with_wait(&self, new_ro: u64, wait: bool) -> Result<()> {
    let tail = self.addresses.tail();
    let new_ro = new_ro.min(tail);
    self.shift_read_only_address(new_ro);
    if wait {
      let flushed = self.addresses.flushed_until();
      if flushed < new_ro {
        self.flush_addr_range(flushed, new_ro).await?;
      }
      if self.addresses.flushed_until() < new_ro {
        return Err(Error::InvalidState(
          "shift_read_only_address_with_wait 刷盘后目标区间仍未完整落盘".into(),
        ));
      }
    }
    Ok(())
  }

  /// 推进 ReadOnlyAddress 与 HeadAddress，并可选等待 HeadAddress 驱逐完成（SafeHead 达标）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftAddressesWithWait
  pub async fn shift_addresses_with_wait(
    &self,
    new_ro: u64,
    new_head: u64,
    wait_for_eviction: bool,
  ) -> Result<()> {
    debug_assert!(
      new_head <= new_ro,
      "new_head ({new_head:#x}) 不能超过 new_ro ({new_ro:#x})"
    );
    let tail = self.addresses.tail();
    let new_ro = new_ro.min(tail);
    let new_head = new_head.min(new_ro);
    self.shift_read_only_address_with_wait(new_ro, true).await?;
    self.shift_head_address(new_head);
    if wait_for_eviction {
      self.wait_safe_head_drained(new_head).await;
    }
    Ok(())
  }

  /// 等待 SafeHeadAddress 达到指定目标（对标 C# ShiftAddressesWithWait 中等待 ClosedUntilAddress / SafeHeadAddress）
  pub async fn wait_safe_head_drained(&self, target_safe_head: u64) {
    debug_assert!(
      target_safe_head <= self.addresses.head(),
      "target_safe_head ({target_safe_head:#x}) 不能超过 head ({:#x})",
      self.addresses.head()
    );
    self
      .wait_epoch_condition(|| self.addresses.safe_head() >= target_safe_head)
      .await;
  }

  /// 获取当前 ReadOnlyAddress
  #[inline]
  pub fn read_only_address(&self) -> u64 {
    self.addresses.read_only()
  }

  /// 获取当前 SafeReadOnlyAddress
  #[inline]
  pub fn safe_read_only_address(&self) -> u64 {
    self.addresses.safe_read_only()
  }

  /// 获取当前 HeadAddress
  #[inline]
  pub fn head_address(&self) -> u64 {
    self.addresses.head()
  }

  /// 获取当前 SafeHeadAddress
  #[inline]
  pub fn safe_head_address(&self) -> u64 {
    self.addresses.safe_head()
  }

  /// 获取当前 FlushedUntilAddress
  #[inline]
  pub fn flushed_until_address(&self) -> u64 {
    self.addresses.flushed_until()
  }

  /// 获取当前 BeginAddress
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.addresses.begin()
  }

  /// 判断地址是否在可变区
  #[inline]
  pub fn is_mutable(&self, addr: u64) -> bool {
    self.addresses.is_mutable(addr)
  }

  /// 判断地址是否在只读区
  #[inline]
  pub fn is_read_only(&self, addr: u64) -> bool {
    self.addresses.is_read_only(addr)
  }

  /// 判断地址是否在内存中
  #[inline]
  pub fn is_in_memory(&self, addr: u64) -> bool {
    self.addresses.is_in_memory(addr)
  }

  /// 判断地址是否在磁盘区
  #[inline]
  pub fn is_on_disk(&self, addr: u64) -> bool {
    self.addresses.is_on_disk(addr)
  }
}
