use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::RwLock;
use wbase::map::HashSet;

use crate::server::{
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  migration::{
    migrate_state::MigrateState, sketch::Sketch, sketch_status::SketchStatus,
    transfer_option::TransferOption,
  },
};

/// 迁移任务入参聚合（C# MigrateSession 构造散参收敛为单一 spec，
/// Manager→Store→Session 三层透传共用，免 too_many_arguments）；
/// 节点 id 按 transpile 规范为 u128 纯二进制（Copy 语义，跨慢路径
/// 与后台任务转移零堆分配）
#[derive(Clone)]
pub struct MigrateTaskSpec {
  pub source_node_id: u128,
  pub target_address: String,
  pub target_port: i32,
  pub target_node_id: u128,
  pub username: String,
  pub passwd: String,
  pub copy_option: bool,
  pub replace_option: bool,
  /// 停等超时毫秒（MIGRATE 第 5 参，C# _timeout 同源）三态：>0 限时、
  /// 0 立即超时、-1 免超时（对标 Timeout.InfiniteTimeSpan）；其余负值
  /// 命令解析期显式拒收，映射单点见 migrate_driver::keys::wait_dur
  pub timeout: i32,
  /// 传输形态（libs/cluster/Session/TransferOption.cs，C# MigrateSession
  /// transferOption 同源；命令臂按解析结果填充并消费分派）
  pub transfer_option: TransferOption,
}

/// libs/cluster/Server/Migration/MigrateSession.cs:MigrateSession
pub struct MigrateSession {
  pub cluster_provider: Arc<ClusterProvider>,
  pub spec: MigrateTaskSpec,
  pub target_node_id: u128,
  slots: HashSet<i32>,
  pub status: parking_lot::RwLock<MigrateState>,
  pub sketch: Sketch,
  /// 取消令牌（对标 C# `_cts` CancellationTokenSource 的取消位）
  cancelled: AtomicBool,
}

impl MigrateSession {
  /// libs/cluster/Server/Migration/MigrateSession.cs:MigrateSession
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    spec: MigrateTaskSpec,
    slots: HashSet<i32>,
    sketch: Sketch,
  ) -> Self {
    Self {
      cluster_provider,
      target_node_id: spec.target_node_id,
      spec,
      slots,
      status: RwLock::new(MigrateState::Pending),
      sketch,
      cancelled: AtomicBool::new(false),
    }
  }

  /// 获取迁移任务配置
  pub fn spec(&self) -> &MigrateTaskSpec {
    &self.spec
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:Dispose
  ///
  /// 触发取消令牌（对标 `_cts?.Cancel()`）：在途远端停等与驱动循环各检查点
  /// 即刻收敛失败，由驱动 recover/收尾路径断开已打开的目标端客户端会话
  /// （对标 `_cts` 中断底层读写 + `migrateOperation[i].Dispose` 释放 gcs——
  /// rust 侧客户端连接由驱动栈帧独占持有，断连在持有点收口）
  pub fn dispose(&self) {
    self.cancelled.store(true, Ordering::Release);
  }

  /// 取消令牌是否已触发（C# `_cts.Token.ThrowIfCancellationRequested` 对位）
  #[inline]
  pub fn is_cancelled(&self) -> bool {
    self.cancelled.load(Ordering::Acquire)
  }

  pub fn get_slots(&self) -> &HashSet<i32> {
    &self.slots
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:GetRanges
  pub fn get_ranges(&self) -> Vec<(i32, i32)> {
    if self.slots.is_empty() {
      return Vec::new();
    }
    if self.slots.len() == 1 {
      let slot = *self.slots.iter().next().unwrap();
      return vec![(slot, slot)];
    }
    let mut sorted: Vec<i32> = self.slots.iter().copied().collect();
    sorted.sort_unstable();
    let mut ranges = Vec::new();
    let mut start_idx = 0;
    while start_idx < sorted.len() {
      let mut end_idx = start_idx + 1;
      while end_idx < sorted.len() && sorted[end_idx - 1] + 1 == sorted[end_idx] {
        end_idx += 1;
      }
      ranges.push((sorted[start_idx], sorted[end_idx - 1]));
      start_idx = end_idx;
    }
    ranges
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:ResetLocalSlot
  pub fn reset_local_slot(&self) {
    if let Some(cm) = self.cluster_provider.cluster_manager() {
      let usize_slots: HashSet<usize> = self.slots.iter().map(|&s| s as usize).collect();
      cm.try_reset_slots_state(&usize_slots);
    }
  }

  /// 手工 KEYS 迁移前置容忍：槽位已全部置 MIGRATING（NOTMIGRATING 解析门
  /// 放行后的必然形态，C# MigrateKeysAsync 不再翻转本端槽位）时直接放行；
  /// 否则按 Stable → MIGRATING 常规翻转（防御直调驱动形态）
  pub fn ensure_local_prepared_for_migration(&self) -> bool {
    if let Some(cm) = self.cluster_provider.cluster_manager() {
      let config = cm.current_config();
      let all_migrating = self
        .slots
        .iter()
        .all(|&s| config.get_state(s as u16) == SlotState::Migrating);
      if all_migrating {
        return true;
      }
    }
    self.try_prepare_local_for_migration()
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:TryPrepareLocalForMigration
  pub fn try_prepare_local_for_migration(&self) -> bool {
    let Some(cm) = self.cluster_provider.cluster_manager() else {
      *self.status.write() = MigrateState::Fail;
      return false;
    };
    let usize_slots: HashSet<usize> = self.slots.iter().map(|&s| s as usize).collect();
    if cm
      .try_prepare_slots_for_migration(&usize_slots, self.target_node_id)
      .is_err()
    {
      *self.status.write() = MigrateState::Fail;
      return false;
    }
    true
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:RelinquishOwnership
  pub fn relinquish_ownership(&self) -> bool {
    let Some(cm) = self.cluster_provider.cluster_manager() else {
      return false;
    };
    let usize_slots: HashSet<usize> = self.slots.iter().map(|&s| s as usize).collect();
    cm.try_prepare_slots_for_ownership_change(&usize_slots, self.target_node_id)
      .is_ok()
  }

  /// libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:CanAccessKey
  ///
  /// MIGRATING 槽位键级可访问性判定（sketch 状态语义与 C# 逐臂对齐）：
  /// - 槽位不在本会话管辖 / 键未被 sketch 收录 → 放行
  /// - `Initializing` / `Migrated` → 放行（C# 「Both reads and write
  ///   commands can access key if it exists」——「if it exists」由调用方
  ///   等待结束后的 `exists` 判定承接：键已发走且源端删除 → ASK）
  /// - `Transmitting` → 仅读放行，写须等待（载荷在途，源端写会在驱动
  ///   删除时丢失）
  /// - `Deleting` → 读写全等待（删除完成后按 exists = false 走 ASK）
  ///
  /// NOTE: Caller responsible for spin-wait（C# 同注）——rust 侧自旋由
  /// `ClusterManager::wait_key_gate` 的挂起轮询承接，本判定为单次快照
  pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
    if !self.slots.contains(&slot) {
      return true;
    }
    let (found, status) = self.sketch.probe(key);
    if !found {
      return true;
    }
    match status {
      SketchStatus::Initializing | SketchStatus::Migrated => true,
      SketchStatus::Transmitting => read_only,
      SketchStatus::Deleting => false,
    }
  }
}
