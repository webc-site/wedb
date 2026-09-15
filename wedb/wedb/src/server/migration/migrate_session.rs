use std::sync::Arc;

use gxhash::HashSet;
use parking_lot::RwLock;

use crate::server::{
  cluster_provider::ClusterProvider,
  migration::{migrate_state::MigrateState, sketch::Sketch, sketch_status::SketchStatus},
};

/// 迁移任务入参聚合（C# MigrateSession 构造散参收敛为单一 spec，
/// Manager→Store→Session 三层透传共用，免 too_many_arguments）
#[derive(Clone, Copy)]
pub struct MigrateTaskSpec<'a> {
  pub source_node_id: &'a str,
  pub target_address: &'a str,
  pub target_port: i32,
  pub target_node_id: &'a str,
  pub username: &'a str,
  pub passwd: &'a str,
  pub copy_option: bool,
  pub replace_option: bool,
  pub timeout: i32,
}

/// libs/cluster/Server/Migration/MigrateSession.cs:MigrateSession
pub struct MigrateSession {
  pub cluster_provider: Arc<ClusterProvider>,
  pub target_node_id: String,
  slots: HashSet<i32>,
  pub status: parking_lot::RwLock<MigrateState>,
  pub sketch: Sketch,
}

impl MigrateSession {
  /// libs/cluster/Server/Migration/MigrateSession.cs:MigrateSession
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    spec: MigrateTaskSpec<'_>,
    slots: HashSet<i32>,
    sketch: Sketch,
  ) -> Self {
    Self {
      cluster_provider,
      target_node_id: spec.target_node_id.to_string(),
      slots,
      status: RwLock::new(MigrateState::Pending),
      sketch,
    }
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:Dispose
  pub fn dispose(&self) {}

  pub fn get_slots(&self) -> &HashSet<i32> {
    &self.slots
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:Overlap
  pub fn overlap(&self, session: &Self) -> bool {
    self.slots.iter().any(|s| session.slots.contains(s))
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

  /// libs/cluster/Server/Migration/MigrateSession.cs:TryPrepareLocalForMigration
  pub fn try_prepare_local_for_migration(&self) -> bool {
    let Some(cm) = self.cluster_provider.cluster_manager() else {
      *self.status.write() = MigrateState::Fail;
      return false;
    };
    let usize_slots: HashSet<usize> = self.slots.iter().map(|&s| s as usize).collect();
    if cm
      .try_prepare_slots_for_migration(&usize_slots, &self.target_node_id)
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
    cm.try_prepare_slots_for_ownership_change(&usize_slots, &self.target_node_id)
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
