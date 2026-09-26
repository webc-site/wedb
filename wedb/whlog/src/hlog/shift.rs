use std::sync::{
  Arc,
  atomic::{
    AtomicBool,
    Ordering::{Acquire, Release, SeqCst},
  },
};

use compio::time::sleep;
use log::trace;
use wbase::{
  backoff::{Backoff, BackoffStage},
  thread::current_thread_id,
};
use wdev::Device;

use super::HybridLog;
use crate::error::{Error, Result};

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
        self.epoch.bump_current_epoch();
      }
    }
  }

  /// 先封后刷的「封」：把只读线推进至 `bound` 并等 SafeReadOnlyAddress 达标
  ///
  /// 全系统唯一的刷盘读侧上界屏障，由 [super::HybridLog::flush_pages_range] 在发起
  /// 任何设备写入之前调用（前台驱逐、组提交 FlushStep、begin 补刷三类驱动因此同享一份
  /// 口径，调用方无需自备门槛），本函数不发起刷盘，只负责把 `bound` 以下变为「所有线程
  /// 一致认可的定稿区」。
  ///
  /// # 与 C# 的对应与刻意差异
  /// C# 把刷盘放进 `epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(newRo))` 的排空
  /// 动作闭包内，动作只在纪元完全排空后执行，故「SafeReadOnlyAddress 推进」与「页刷盘」
  /// 天然同序（AllocatorBase.cs:1644-1652 与 :1744-1758）；rust 刷盘为 compio 异步设备
  /// I/O，无法塞入 `Send + 'static` 同步闭包，故等价改写为本显式屏障：登记封印动作 →
  /// 等其排空（谓词 safe_read_only >= bound）→ 由刷盘内核续接异步写入。等待体复用
  /// 纪元屏障内核，转调 [`wepoch::wait_condition_async`]，杜绝第二套等待循环。
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesMarkedReadOnly
  pub(crate) async fn seal_read_only_and_drain(&self, bound: u64) {
    self.shift_read_only_address(bound);
    if self.addresses.safe_read_only() >= bound {
      return;
    }
    self
      .wait_epoch_condition(|| self.addresses.safe_read_only() >= bound)
      .await;
    trace!("只读封印排空完成：SafeReadOnlyAddress 已越过 {bound:#x}");
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
    // 对齐 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress：head 钳制到最后已刷盘地址，
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
        self.epoch.bump_current_epoch();
      }
    }
  }

  /// 推进 BeginAddress 并按检查点窗钳制截断历史存储段（对标 C# ShiftBeginAddress）
  ///
  /// 核心顺序对齐 C#（只读区封存 → 补刷 → head → 截断），begin 时序为刻意差异：
  /// C# 最先推进 begin（AllocatorBase.ShiftBeginAddress "First update the begin
  /// address"），此处刻意后置——先补刷 `[flushed_until, new_begin)` 确保驱逐不越过
  /// 持久化前缀（维持 `head <= flushed_until` 快照不变式），随后推进 head（Epoch
  /// 延迟推进 safe_head，绝不内联强推）与 begin，避免补刷窗口内 begin 先行造成
  /// `[head, new_begin)` 提前不可见，最后经纪元排空屏障后物理截断设备历史段。
  ///
  /// 物理删段受检查点窗钳制（「拍检查点才真正删文件」，对标 C#
  /// LogAccessor.cs:133 "log will be truncated after the next checkpoint" 契约与
  /// Recovery/Checkpoint.cs:58 CleanupLogCheckpoint 的发布点截断）：删段目标 =
  /// `new_begin.min(删段地板)`，地板即最近已发布检查点重放窗下界
  /// `meta.hlog_meta.begin_address`（[`Self::release_history_until`] 发布时抬升）；
  /// 未发布检查点（地板 0）全禁删。逻辑 begin 照常全量推进——Shift 丢弃档、
  /// gc_dead 死亡账本摘除（须 begin 越过死亡条目 tail_address）与 FLUSH 语义
  /// 均不因物理删段延后而受阻。C# 同型靠默认零紧缩成立的恢复安全，此处以
  /// 显式钳制等价落地。
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
      // 原位更新会撕裂设备上的待截断前缀，冻结后原位更新改走 RCU 追加。
      // 排空语义由刷盘内核承担（C# 的 :1669-1693 等待 FlushedUntil 达标之所以成立，
      // 正因为 FlushedUntil 只由纪元排空后的 SafeReadOnly 驱动推进）：
      // flush_pages_range 入口先把待刷上界封印为安全只读并等排空，故此处补刷
      // 区间落盘时 [flushed, new_begin) 内绝无在途编码，零/半截记录不会被计入前缀
      self.flush_addr_range(flushed, new_begin).await?;
    }
    if self.addresses.flushed_until() < new_begin {
      return Err(Error::InvalidState(
        "shift_begin_address 补刷后待截断区间仍未完整落盘（flushed_until < new_begin）".into(),
      ));
    }

    self.shift_head_address(new_begin);
    // 「先推进 begin → 纪元排空 → 设备截断」次序（对标 AllocatorBase.cs:
    // ShiftBeginAddress）：begin 仅推进自身，head/safe_head 的连带推进已由
    // shift_head_address 以纪元安全方式完成（"连带内联强推 safe_head"会绕过
    // 纪元排空语义、令旧页上 epoch 保护的读者失去保护，故不采用）。纪元排空
    // 屏障严格落于 begin 推进之后、设备物理删段之前——begin 挪线后新读者经
    // is_on_disk 永远采不到待截断区间，而挪线前已入场的在途磁盘读者由屏障
    // 内的封口 action 钉在其入场纪元上，截断因此严格 happens-after 其 I/O 完成。
    // 逻辑推进与物理删段显式分离（Device::truncate_begin_until 内核把两者绑在
    // 同一 target 上，而检查点窗钳制只许延后删段、不许延后推进——见
    // wdev/src/device.rs 该内核文档的「钳制由调用方完成」契约）：
    self.addresses.begin_address.fetch_max(new_begin, SeqCst);
    self.wait_safe_read_only_drained(new_begin).await;
    let until = new_begin.min(self.addresses.delete_floor());
    self
      .device
      .truncate_until_address(until)
      .await
      .map_err(Error::from)
  }

  /// 抬升物理删段地板并补收窗下历史段（检查点发布点专用入口）
  ///
  /// 对标 C# CleanupLogCheckpoint（libs/storage/Tsavorite/cs/src/core/Index/
  /// Recovery/Checkpoint.cs:54-59）：检查点 meta 落盘发布后，本检查点重放窗
  /// `[begin, tail)` 自此冻结受保，调用方以 `floor = begin_address` 触发本方法，
  /// 此前被 [`Self::shift_begin_address`] 钳制延后的越窗段在此一并补收。纪元
  /// 排空屏障与移位链同源（发布流程已等 safe_ro >= tail >= floor，谓词预真
  /// 零挂起，仅兜底并发在途磁盘读者）。
  pub async fn release_history_until(&self, floor: u64) -> Result<()> {
    self.addresses.raise_delete_floor(floor);
    self.wait_safe_read_only_drained(floor).await;
    self
      .device
      .truncate_until_address(floor)
      .await
      .map_err(Error::from)
  }

  /// 物理截断历史存储段（分配器内核级 truncate，全系统删段动作 100% 收敛点）
  ///
  /// 对标 C# LogAccessor.Truncate（libs/storage/Tsavorite/cs/src/core/Index/
  /// Tsavorite/LogAccessor.cs:148 `ShiftBeginAddress(BeginAddress, truncateLog: true)`）：
  /// C# begin 未变时落入 AllocatorBase.ShiftBeginAddress 的
  /// `epoch.BumpCurrentEpoch(() => TruncateUntilAddress(newBeginAddress))` 分支
  /// （AllocatorBase.cs:1659-1663），物理删段被纪元排空动作包裹。Rust 删段为 compio
  /// 异步设备 I/O，无法塞入 `Send + 'static` 同步闭包，等价改写为
  /// [`Self::wait_safe_read_only_drained`] 显式屏障：挪线前已入场、正处于
  /// `is_on_disk` 采样到异步磁盘读取之间的在途存量读者完成 I/O 后，方可执行删段。
  ///
  /// 物理删段目标与 [`Self::shift_begin_address`] 同钳制于删段地板 `delete_floor`
  /// （「拍检查点才真正删文件」契约）：未发布检查点（地板 0）全禁删——上层
  /// `truncate()`（如 UNSAFETRUNCATELOG 慢路径）绝不允许拆毁活动检查点重放窗
  /// `[begin, tail)` 所依赖的历史段，杜绝崩溃重启后检查点回放损坏；地板发布后
  /// 补收窗下历史段。上层存储一律转调本方法，严禁再直接触碰设备执行破坏性删段。
  pub async fn truncate(&self) -> Result<()> {
    let begin = self.addresses.begin();
    self.wait_safe_read_only_drained(begin).await;
    let until = begin.min(self.addresses.delete_floor());
    self
      .device
      .truncate_until_address(until)
      .await
      .map_err(Error::from)
  }

  /// 抬升物理删段地板（恢复装配点专用，不触发补删）
  ///
  /// 启动恢复 / 副本在线导入按恢复检查点的 `hlog_meta.begin_address` 重设地板
  /// （对标 C# OnRecovery 后版本基线接管；窗下存量残留段待下一检查点发布时
  /// 经 [`Self::release_history_until`] 补收，C# 同型）
  #[inline]
  pub fn raise_delete_floor(&self, floor: u64) {
    self.addresses.raise_delete_floor(floor);
  }

  /// 通用纪元等待屏障：解除本线程旧纪元自钉，循环等待直至谓词满足
  async fn wait_epoch_condition<F>(&self, condition: F)
  where
    F: FnMut() -> bool,
  {
    wepoch::wait_condition_async(
      Some(&self.epoch),
      true,
      condition,
      |backoff_state| {
        if backoff_state.step_count().is_multiple_of(64) {
          let new_epoch = self.epoch.bump_current_epoch();
          let tid = current_thread_id();
          self.epoch.refresh_thread_protected_entries(tid, new_epoch);
        } else {
          self.epoch.drain();
        }
      },
      None,
      sleep,
    )
    .await;
  }

  /// 截断前纪元排空屏障（截断专用封口，区别于刷盘内核的
  /// [Self::seal_read_only_and_drain]：本屏障同时封口 SafeHead 与 SafeReadOnly，
  /// 谓词锚在 safe_head 达标，物理删段必须 happens-after 它）：
  /// 异步等待直至封口 action 排空且 `safe_head >= new_begin`
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
  ///    自钉 A——转调 `wepoch::wait_condition_async`（`allow_protected = true` 档），
  ///    已先逐层 `suspend` 完全退出并计数，内部 RAII 守卫无论正常完成还是 Future
  ///    被 Drop 均按原深度 `resume` 重入（取消安全，完全杜绝守卫深度丢失引发的 UAF）；
  /// 4. 本线程若持 `Participant` 句柄守卫（守卫归调用方栈帧所有，本函数无法代为
  ///    exit/重入），仿 `LightEpoch::help_drain`（对照 C# ProtectAndDrain）把本线程
  ///    名下仍受保护条目刷新至全局最新纪元，解除对 <= E 旧纪元的自钉；
  /// 5. 其余线程的保护区均为有穷临界区（LightEpoch 契约），终将退出；最后一名退出者
  ///    经 suspend_drain/drain 执行 A，drained 变为 true，循环有穷。等待体每轮优先
  ///    调用 `drain` 收割就绪动作，周期性轻量 `bump_current_epoch`，配合三级退避（含
  ///    compio::time::sleep 异步让渡 reactor），不裸忙等、不烧核、不阻塞单线程执行器。
  async fn wait_safe_read_only_drained(&self, new_begin: u64) {
    let drained = Arc::new(AtomicBool::new(false));
    let drained_action = Arc::clone(&drained);
    let addrs = Arc::clone(&self.addresses);
    self.epoch.bump_current_epoch_action(move || {
      addrs.shift_safe_read_only_address(new_begin);
      addrs.shift_safe_head_address(new_begin);
      drained_action.store(true, Release);
      trace!("Epoch 封口推进 SafeHead/SafeReadOnly 至 {new_begin:#x}");
    });
    self
      .wait_epoch_condition(|| drained.load(Acquire) && self.addresses.safe_head() >= new_begin)
      .await;
    trace!("截断屏障排空完成：safe_head 已越过 {new_begin:#x}");
  }

  /// 获取当前 TailAddress
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetTailAddress
  /// （C# 另有 UnstableGetTailAddress 快照变体，Rust 原子加载等价，单一入口即可）
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.addresses.tail()
  }

  /// 将 ReadOnlyAddress 迅速推进至当前 TailAddress（对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftReadOnlyToTail，使当前所有数据瞬间变为只读不可变）
  #[inline]
  pub fn shift_read_only_to_tail(&self) -> u64 {
    let tail = self.addresses.tail();
    self.shift_read_only_address(tail);
    tail
  }

  /// 推进 ReadOnlyAddress 并可选等待刷盘完成（对标 C# ShiftReadOnlyAddressWithWait）
  ///
  /// 等待语义与 C# 同序：C# 的 `ShiftReadOnlyAddress` 把「推进 SafeReadOnlyAddress +
  /// 发起刷盘」放进同一个纪元排空动作，调用方随后自旋 `ProtectAndDrain` 直到
  /// `FlushedUntilAddress >= newRO`；rust 侧刷盘是异步 I/O，故排空+落笔由
  /// [super::HybridLog::flush_pages_range] 内核承担（入口先 `seal_read_only_and_drain`
  /// 封印待刷上界再写设备），此处只保留对持久化前缀的等待，全系统仅此一套刷盘门槛。
  ///
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
        self.wait_flushed_until_address_async(new_ro).await;
      }
      if self.addresses.flushed_until() < new_ro {
        return Err(Error::InvalidState(
          "shift_read_only_address_with_wait 刷盘后目标区间仍未完整落盘".into(),
        ));
      }
    }
    Ok(())
  }

  /// 异步等待 FlushedUntilAddress 推进至 target 或更高（事件驱动零轮询，对标 Garnet flushEvent.Wait）
  ///
  /// Spin/Yield 两阶段动作转调 wbase 真源；Sleep 阶段刻意不改用定时睡眠，而是 park 到
  /// flush 事件上（对标 C# AllocatorBase.cs:1546-1570 WaitToRetryNow 达到 kFlushSpinCount
  /// 后的 `flushEvent.Wait` 阻塞等待）——刷盘完成即唤醒，零轮询零延迟放大。
  pub async fn wait_flushed_until_address_async(&self, target: u64) {
    let mut bo = Backoff::new();
    loop {
      if self.addresses.flushed_until() >= target {
        return;
      }
      match bo.stage() {
        BackoffStage::Sleep => {
          let listener = self.flush_event.listen();
          if self.addresses.flushed_until() >= target {
            return;
          }
          listener.await;
        }
        stage => stage.wait_async(sleep).await,
      }
      bo.advance();
    }
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
