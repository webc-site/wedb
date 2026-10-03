//! 向量后台清理暂停守卫（全量同步三落地面共用的清理闸门，单一事实源）
//!
//! 全量同步期间向量后台清理若并发推进，会与在线设备半写 / 引擎置换 /
//! 内存镜像回建竞扰，三个落地面统一以「构造即暂停、离开作用域即恢复」
//! 的 RAII 守卫承接：
//! - 检查点网络接收：[`FileDataSink`](super::receive_checkpoint_handler::FileDataSink)
//!   开槽即闸、收尾刷盘前置解除（弃置路径 Drop 兜底）；
//! - 磁盘基导入：`try_replica_diskbased_recovery`（对标
//!   libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:
//!   TryReplicaDiskbasedRecovery 的恢复窗口）；
//! - 无盘恢复：`try_replica_diskless_recovery`（对标
//!   libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:
//!   TryReplicaDisklessRecovery 的恢复窗口）。
//!
//! 收敛前本机制在上述三处各持一份同形实现（两份逐字重复的局部守卫
//! 结构体 + 接收槽手写 pause/resume 配对），本模块收口为单一定义。

use std::sync::Arc;

use wnode::resp::vector::vector_manager::VectorManager;

/// 向量后台清理暂停守卫：构造即暂停 [`VectorManager`] 清理，离开作用域即恢复
///
/// 提前恢复臂二选一：成功收尾用 [`Self::resume_and_queue`]（恢复并重排队
/// 清理），收尾刷盘前置解除用 [`Self::resume`]（仅恢复）；未显式恢复的
/// 路径（含全部错误早退）由 [`Drop`] 兜底，杜绝任何路径漏恢复。
pub(super) struct CleanupPauseGuard(Option<Arc<VectorManager>>);

impl CleanupPauseGuard {
  /// 暂停 `vm` 的后台清理并登记守卫（`None` 即空守卫，全程零操作）
  pub(super) fn new(vm: Option<&Arc<VectorManager>>) -> Self {
    if let Some(vm) = vm {
      vm.pause_cleanup_async();
    }
    Self(vm.cloned())
  }

  /// 提前恢复并重排队清理（恢复成功收尾专用：恢复窗口内被闸的清理任务
  /// 重回队推进；仅成功路径调用，失败路径不得伪装成功态）
  pub(super) fn resume_and_queue(&mut self) {
    if let Some(vm) = self.0.take() {
      vm.resume_cleanup();
      vm.queue_cleanups();
    }
  }

  /// 提前恢复（不重排队；收尾刷盘前解除闸门用，Drop 兜底同款语义）
  pub(super) fn resume(&mut self) {
    if let Some(vm) = self.0.take() {
      vm.resume_cleanup();
    }
  }
}

impl Drop for CleanupPauseGuard {
  fn drop(&mut self) {
    // 已提前恢复（字段已 take）则为空操作，否则兜底恢复
    self.resume();
  }
}
