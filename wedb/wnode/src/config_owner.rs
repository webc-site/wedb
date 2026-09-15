//! CONFIG SET → 生产后台任务域的调停执行器（libs/server/StoreWrapper.cs 的
//! rust 形态）
//!
//! C# 以 `StoreWrapper` 具体类型承接 `RuntimeServerConfig` 的更新回调
//!（`ReconcilePrimaryTask` / `ApplyAofSyncMaxLagBytes`）；rust 侧
//! `RuntimeServerConfig::try_set` 产出 [`ConfigReconcile`] 调停消息，由持有
//! 存储引擎引用的 server 层（CONFIG SET 命令域）经本模块
//! [`apply_config_reconcile`] 就地 match 执行——封闭枚举静态分发，
//! 无 trait object 与运行时动态查表。

use std::sync::Arc;

use wconf::ConfigReconcile;
use wdev::Device;
use wkv::WedbStore;

/// 执行 CONFIG SET 产出的调停消息（对标 StoreWrapper.ReconcilePrimaryTask
/// 家族 + ApplyAofSyncMaxLagBytes 的动作分派）
pub fn apply_config_reconcile<D>(store: &Arc<WedbStore<D>>, msg: ConfigReconcile)
where
  D: Device + 'static,
{
  match msg {
    // aof-commit-freq 变更落点（对标 ReconcilePrimaryTask(CommitTask) →
    // TryStartCommitTask）。rust 提交模型为同步 group commit（wkv
    // `FlushPipeline` / WaofSublog 刷盘状态机），无 C# `CommitTaskAsync`
    // 周期任务域；提交间隔在 AOF 日志构造期固化（`single_log_aof`），0 值
    // 切换已被 `RuntimeServerConfig` 拒绝（CommitFreqZero /
    // CommitFreqAutoCommitStart），安全值域 {-1, >0} 间变更无周期任务可重启，
    // 槽位存值即终态。
    ConfigReconcile::CommitTask { .. } => {}

    // expired-object-collection-freq 变更落点（对标
    // ReconcilePrimaryTask(ObjectCollectTask)）。wkv 统一 GC 循环不单设对象
    // 收集任务（wkv/src/gc.rs：字段级候选与 key 级候选同源单遍扫描，频率由
    // `GcConfig::scan_interval_ms` 统一承担），该配置项的任务域动作已并入
    // ExpiredKeyDeletionScan 分支的映射，此处无独立动作。
    ConfigReconcile::ObjectCollect { .. } => {}

    // aof-sync-max-lag-bytes 变更落点（对标 ApplyAofSyncMaxLagBytes →
    // AofBackpressure.SetBudget）。rust 侧 `AofBackpressure` 随 `GarnetLog`
    // 构造（aof/garnet_log.rs），经 AOF 门面装配（`open_with_aof`）晚于本
    // 执行器可触达的装配点，桥内无日志句柄域可推送——该项保持槽位存值
    //（wconf `aof-sync-max-lag-bytes` 槽位语义不变）。
    ConfigReconcile::AofSyncMaxLag { .. } => {}

    // expired-key-deletion-scan-freq 变更落点（对标
    // ReconcilePrimaryTask(ExpiredKeyDeletionTask) →
    // TryStartExpiredKeyDeletionTask）。`scan_frequency_secs > 0`：按新间隔
    //（秒 → 毫秒）启用扫描并确保循环在跑（未启动/已停则拉起，对标
    // `RegisterAndRun`）；`<= 0`：禁用并停循环（对标 `taskLifecycleLock` 下
    // 的 `CancelAsync`）。间隔与开关经共享 `GcConfig` 由循环每轮重读，
    // 下一轮生效。
    ConfigReconcile::ExpiredKeyDeletionScan {
      scan_frequency_secs,
    } => {
      if scan_frequency_secs > 0 {
        store.reconcile_gc_scan(true, Some(scan_frequency_secs as u64 * 1000));
      } else {
        store.reconcile_gc_scan(false, None);
      }
    }
  }
}
