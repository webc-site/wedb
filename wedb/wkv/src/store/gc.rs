use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::Device;

use super::WedbStore;
use crate::{
  config::GcConfig,
  gc::{self, GcHandle, GcStatsSnapshot},
};

impl<D: Device> WedbStore<D> {
  /// 按 CONFIG SET 语义原子调和内置 GC 扫描任务（对标
  /// libs/server/StoreWrapper.cs:ReconcilePrimaryTask 的 taskLifecycleLock
  /// 串行化：停任务 → 按新配置重启）
  ///
  /// 持句柄槽锁串行「写运行态配置 → 启停」两步，与 [`Self::start_gc`] /
  /// [`Self::stop_gc`] 互斥，杜绝并发调和下「配置启用但循环已停」的错位态。
  /// `scan_interval_ms` 传 `None` 保持现值（仅翻开关）。
  ///
  /// 返回 true 表示扫描循环处于运行中。
  pub fn reconcile_gc_scan(self: &Arc<Self>, enabled: bool, scan_interval_ms: Option<u64>) -> bool
  where
    D: Device + 'static,
  {
    let mut slot = self.gc.lock();
    {
      let mut cfg = self.gc_cfg.write();
      cfg.enabled = enabled;
      if let Some(ms) = scan_interval_ms {
        cfg.scan_interval_ms = ms;
      }
    }
    // 与循环内判定一致：禁用或零间隔 = 无后台定时循环
    if !enabled || self.gc_cfg.read().scan_interval_ms == 0 {
      if let Some(handle) = slot.as_ref() {
        handle.stop();
      }
      return false;
    }
    if Runtime::try_current().is_none() {
      return false;
    }
    if slot.as_ref().is_some_and(GcHandle::is_active) {
      return true;
    }
    // 在场句柄已停（stop_gc / 热更新禁用后循环退出）：替换重拉，旧句柄 Drop
    // 兜底强取消残留任务
    *slot = Some(gc::GcManager::spawn(Arc::clone(self)));
    true
  }

  /// 启动内置 GC 后台循环（幂等；运行态配置 `gc_cfg.enabled` 为 false 或
  /// `scan_interval_ms` 为 0 时不启动）
  ///
  /// 已在跑的循环直接复用；先前经 [`Self::stop_gc`] 或热更新禁用退出的循环
  /// 按当前配置重新拉起（对标 C# ReconcilePrimaryTask 停任务后按新间隔
  /// RegisterAndRun 的语义）。须在 compio 运行时上下文内调用（`D: 'static`
  /// 为 spawn 任务硬性要求）；引擎 Drop 时自动停止循环。
  pub fn start_gc(self: &Arc<Self>) -> bool
  where
    D: Device + 'static,
  {
    let (enabled, ms) = {
      let cfg = self.gc_cfg.read();
      (cfg.enabled, cfg.scan_interval_ms)
    };
    self.reconcile_gc_scan(enabled, Some(ms))
  }

  /// 请求内置 GC 后台循环退出（协作式：至多再运行一个扫描间隔；幂等）
  ///
  /// 对标 C# TaskManager.CancelAsync(TaskType.ExpiredKeyDeletionTask)：
  /// CONFIG SET 把 `expired-key-deletion-scan-freq` 调为禁用值时由持有方调用。
  /// 循环退出后句柄留驻槽内，[`Self::start_gc`] 复检时替换重拉。
  pub fn stop_gc(&self) {
    if let Some(handle) = self.gc.lock().as_ref() {
      handle.stop();
    }
  }

  /// 内置 GC 后台循环是否仍在运行（未请求退出且任务未收敛）
  #[inline]
  pub fn gc_running(&self) -> bool {
    self.gc.lock().as_ref().is_some_and(GcHandle::is_active)
  }

  /// 读取内置 GC 统计快照（循环未启动返回 None）
  pub fn gc_stats(&self) -> Option<GcStatsSnapshot> {
    self.gc.lock().as_ref().map(GcHandle::stats)
  }

  /// 热更新内置 GC 运行态配置（对标 Garnet CONFIG SET → RuntimeServerConfig）
  ///
  /// GC 驱动循环每轮重读本配置：扫描/紧缩间隔、批预算修改下一轮即生效，无需
  /// 重启引擎。GC 未启动时更新同样持久生效——[`Self::start_gc`] 以此处的
  /// `enabled` 判定是否拉起循环；运行中把 `enabled` 置回 false（或间隔清 0）
  /// 时循环在下一轮检点退出（禁用即停，对标 C# CancelAsync），重新置 true 后
  /// 经 [`Self::start_gc`] 重拉。
  pub fn update_gc_config(&self, f: impl FnOnce(&mut GcConfig)) {
    f(&mut self.gc_cfg.write());
  }

  /// 读取内置 GC 运行态配置快照
  pub fn gc_config(&self) -> GcConfig {
    self.gc_cfg.read().clone()
  }
}
