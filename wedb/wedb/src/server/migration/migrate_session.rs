use std::sync::Arc;

use gxhash::HashSet;

use crate::server::{
  cluster_provider::ClusterProvider,
  migration::{migrate_state::MigrateState, sketch::Sketch, sketch_status::SketchStatus},
};

/// 迁移任务入参聚合（C# MigrateSession 构造散参收敛为单一 spec，
/// Manager→Store→Session 三层透传共用，免 too_many_arguments）
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
  pub status: MigrateState,
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
      status: MigrateState::Pending,
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
  pub fn try_prepare_local_for_migration(&mut self) -> bool {
    let Some(cm) = self.cluster_provider.cluster_manager() else {
      self.status = MigrateState::Fail;
      return false;
    };
    let usize_slots: HashSet<usize> = self.slots.iter().map(|&s| s as usize).collect();
    if cm
      .try_prepare_slots_for_migration(&usize_slots, &self.target_node_id)
      .is_err()
    {
      self.status = MigrateState::Fail;
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
