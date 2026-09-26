//! 复活强同步暂停宿主契约（挂起计数段在 wreviv，纪元排空段收归本文件）
//! (1:1 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:PauseRevivification
//!  / ResumeRevivification；wreviv/src/pool.rs:pause 契约明示「强同步暂停需配合上层 epoch
//!  排空，对标 BumpCurrentEpoch + 事件等待协议，由调用方按需组合」，本文件即该上层一半的
//!  唯一落点；全仓纪元等待仍收口 wepoch::wait_condition_async 单原语，此处不落第二套等待)

use std::time::Duration;

use compio::time::sleep;
use wdev::Device;
use wepoch::wait_condition_async;

use super::WedbStore;

/// 强同步暂停完成守卫：存续期挂起计数处于暂停态，Drop 自动恢复
///
/// 对标 C# MigrationDriver.cs:BeginAsyncMigrationTaskAsync finally 块的
/// `ResumeRevivification()`（rust 以 RAII 绑定配对，杜绝早退/panic 漏恢复）。
pub struct RevivificationPauseGuard<'a, D: Device> {
  store: &'a WedbStore<D>,
}

impl<D: Device> Drop for RevivificationPauseGuard<'_, D> {
  fn drop(&mut self) {
    self.store.reviv_pool.resume();
    log::info!("强同步暂停窗口收口：恢复存储复活分配");
  }
}

impl<D: Device> WedbStore<D> {
  /// 强同步暂停复活：冻结新复活分配，并等待先前进入临界区的在途复活写者全数退出
  ///
  /// 三段逐一对标 Tsavorite.cs:121-143 `PauseRevivification(timeout, token)`：
  /// 1. `reviv_pool.pause()` —— 挂起计数递减（对标 `RevivificationManager.PauseRevivification`，
  ///    冻结池取与链内原位复活两臂的新分配）；
  /// 2. `bump_current_epoch() - 1` —— 推进纪元并取前置纪元，对标
  ///    `epoch.BumpCurrentEpoch(() => pauseRevivEvent.Set())` 挂载 Set 回调的前置纪元
  ///    （rust 无共享事件量，以同纪元的谓词形态表达，见第 3 段）；
  /// 3. 转调 [`wait_condition_async`]，达成条件 `is_safe_to_reclaim(target)`、步进回调
  ///    `epoch.drain()` 收割就绪延迟动作——达成 ⟺ 前置纪元全部在途写者（含已拿到复活
  ///    槽位正在执行原位覆记者）已退出临界区，与 C# `pauseRevivEvent.Wait(timeout, token)`
  ///    同一屏障语义；形态对齐 wcpr/src/manager/create.rs `wait_epoch_drain` 先例。
  ///
  /// 档位：`allow_protected = true`（对标 whlog/src/hlog/shift.rs `wait_epoch_condition`）——
  /// 本调用发生在 compio reactor 线程上，同线程其他任务的 TLS 保护区经内部 RAII 守卫
  /// 临时让渡并在 Future 完成/取消时按原深度重入；同线程 `Participant` 自钉经入口
  /// `refresh_thread_protected_entries` 解除，活性依赖「同步批处理闭包单次调用内闭环」
  /// 的全仓既有契约（见 wepoch/src/epoch.rs `help_drain` 注释），跨线程写者不受刷新
  /// 影响，其排空正是本屏障的等待对象。
  ///
  /// 超时与取消：超时仍返回守卫（对标 C# `Wait` 返回值被忽略、Tsavorite.cs:139 继续
  /// 执行而暂停态保持，排空未达降级为告警）；Future 在挂起点被 Drop（上层取消，对标
  /// C# token 触发 OCE）时，守卫已先于等待构造，栈展开即 Drop 恢复挂起计数，暂停-恢复
  /// 配对无泄漏。
  ///
  /// 与 C# `pauseRevivLock` 互斥锁的刻意差异：该锁保护的只是共享
  /// `ManualResetEventSlim` 的 Reset/Set 不被并发暂停交错；本实现每次暂停独立取得
  /// 前置纪元、等待无共享可变状态，并发/嵌套暂停天然成立，挂起计数口径仍由 wreviv
  /// 单点承载（可重入，等量恢复）。
  pub async fn pause_revivification(
    &self,
    timeout: Option<Duration>,
  ) -> RevivificationPauseGuard<'_, D> {
    self.reviv_pool.pause();
    // 守卫先构造后等待：超时/取消路径经栈展开即恢复计数，不遗留悬挂暂停
    let guard = RevivificationPauseGuard { store: self };
    let target = self.epoch.bump_current_epoch() - 1;
    let drained = wait_condition_async(
      Some(&self.epoch),
      true,
      || self.epoch.is_safe_to_reclaim(target),
      |_backoff| {
        self.epoch.drain();
      },
      timeout,
      sleep,
    )
    .await;
    if !drained {
      log::warn!(
        "复活强同步暂停排空等待超时（纪元 {target} 仍未安全回收），在途复活写者可能未全数退出"
      );
    }
    guard
  }
}
