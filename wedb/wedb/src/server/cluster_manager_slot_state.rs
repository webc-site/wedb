use log::trace;
use wbase::{hex::hex_str_u128, map::HashSet};

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::LOCAL_WORKER_ID, cluster_manager::ClusterManager, hash_slot::SlotState,
    worker::NodeRole,
  },
};

/// libs/cluster/Server/ClusterManagerSlotState.cs:ClusterManager
impl ClusterManager {
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryAddSlots
  pub fn try_add_slots(&self, slots: &HashSet<usize>) -> Result<()> {
    {
      let mut current = self.current_config.write();
      if current.num_workers() == 0 {
        return Err(Error::NoWorkers);
      }
      current.try_add_slots(Some(slots), SlotState::Stable)?;
    }
    self.flush_config();
    trace!("AddSlots {:?}", slots);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryRemoveSlots
  pub fn try_remove_slots(&self, slots: &HashSet<usize>) -> Result<()> {
    {
      let mut current = self.current_config.write();
      if current.num_workers() == 0 {
        return Err(Error::NoWorkers);
      }
      current.try_remove_slots(Some(slots))?;
      current.bump_local_node_config_epoch();
    }
    self.flush_config();
    trace!("RemoveSlots {:?}", slots);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForMigration
  pub fn try_prepare_slot_for_migration(&self, slot: usize, node_id: u128) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let migrating_worker_id = current.get_worker_id_from_node_id(node_id);
      if migrating_worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.local_node_id() == Some(node_id) {
        return Err(Error::MigrateToMyself);
      }
      if current.get_node_role_from_node_id(node_id) != NodeRole::Primary {
        return Err(Error::TargetNotPrimary(hex_str_u128(node_id)));
      }
      // 声明偏离不对标：C# 此处按默认参调 IsLocal((ushort)slot)，即
      // enableReplicaReads=true（ClusterManagerSlotState.cs:107；默认值见
      // ClusterConfig.cs:174），副本对其主节点持有的槽也过属主门、可置
      // MIGRATING 写本地配置。而 C# 同段注释自述该配置变更「only true for
      // the primary that owns this slot」且不随 gossip 传播（:123），同文件
      // IMPORTING 门（:232）与 MigrateCommand 槽门（:198/:250）均显式传
      // false，判定 :107 的 true 属笔误。故此处统一传 false，
      // 副本侧直接 SlotNotOwned 拒绝。
      if !current.is_local(slot as u16, false) {
        return Err(Error::SlotNotOwned(slot));
      }
      if current.get_state(slot as u16) != SlotState::Stable {
        return Err(Error::SlotAlreadyScheduled(slot));
      }
      current.update_slot_state(slot, migrating_worker_id, SlotState::Migrating);
    }
    self.flush_config();
    trace!("SetSlot MIGRATING {} TO {}", slot, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotsForMigration
  pub fn try_prepare_slots_for_migration(
    &self,
    slots: &HashSet<usize>,
    node_id: u128,
  ) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let migrating_worker_id = current.get_worker_id_from_node_id(node_id);
      if migrating_worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.local_node_id() == Some(node_id) {
        return Err(Error::MigrateToMyself);
      }
      if current.get_node_role_from_node_id(node_id) != NodeRole::Primary {
        return Err(Error::TargetNotPrimary(hex_str_u128(node_id)));
      }
      for &slot in slots {
        // 同单槽门 try_prepare_slot_for_migration 的声明偏离：C# 批量臂亦按
        // 默认参 true 调 IsLocal（ClusterManagerSlotState.cs:178），同判笔误
        // 不对标（批量 IMPORTING :292 显式 false 同证），此处统一 false，
        // 副本侧直接 SlotNotOwned 拒绝。
        if !current.is_local(slot as u16, false) {
          return Err(Error::SlotNotOwned(slot));
        }
        if current.get_state(slot as u16) != SlotState::Stable {
          return Err(Error::SlotAlreadyScheduled(slot));
        }
      }
      current.update_multi_slot_state(slots, migrating_worker_id, SlotState::Migrating);
    }
    self.flush_config();
    trace!("SetSlots MIGRATING {:?} TO {}", slots, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForImport
  pub fn try_prepare_slot_for_import(&self, slot: usize, node_id: u128) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let importing_worker_id = current.get_worker_id_from_node_id(node_id);
      if importing_worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.local_node_role() != NodeRole::Primary {
        return Err(Error::TargetNotPrimary("local".to_string()));
      }
      if current.is_local(slot as u16, false) {
        return Err(Error::SlotNotFree(slot));
      }
      let source_node_id = current.get_node_id_from_slot(slot as u16);
      if source_node_id != Some(node_id) {
        return Err(Error::SlotNotOwned(slot));
      }
      if current.get_state(slot as u16) != SlotState::Stable {
        return Err(Error::SlotAlreadyScheduled(slot));
      }
      current.update_slot_state(slot, importing_worker_id, SlotState::Importing);
    }
    self.flush_config();
    trace!("SetSlot IMPORTING {} FROM {}", slot, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotsForImport
  pub fn try_prepare_slots_for_import(&self, slots: &HashSet<usize>, node_id: u128) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let importing_worker_id = current.get_worker_id_from_node_id(node_id);
      if importing_worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.local_node_role() != NodeRole::Primary {
        return Err(Error::TargetNotPrimary("local".to_string()));
      }
      for &slot in slots {
        if current.is_local(slot as u16, false) {
          return Err(Error::SlotNotFree(slot));
        }
        let source_node_id = current.get_node_id_from_slot(slot as u16);
        if source_node_id != Some(node_id) {
          return Err(Error::SlotNotOwned(slot));
        }
        if current.get_state(slot as u16) != SlotState::Stable {
          return Err(Error::SlotAlreadyScheduled(slot));
        }
      }
      current.update_multi_slot_state(slots, importing_worker_id, SlotState::Importing);
    }
    self.flush_config();
    trace!("SetSlots IMPORTING {:?} FROM {}", slots, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForOwnershipChange
  pub fn try_prepare_slot_for_ownership_change(&self, slot: usize, node_id: u128) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let worker_id = current.get_worker_id_from_node_id(node_id);
      if worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      match current.get_state(slot as u16) {
        SlotState::Migrating => {
          current.update_slot_state(slot, worker_id, SlotState::Stable);
        }
        SlotState::Importing => {
          if current.local_node_id() != Some(node_id) {
            return Err(Error::NodeNotFound(hex_str_u128(node_id)));
          }
          current
            .update_slot_state(slot, LOCAL_WORKER_ID as u16, SlotState::Stable)
            .bump_local_node_config_epoch();
        }
        _ => {
          current.update_slot_state(slot, worker_id, SlotState::Stable);
        }
      }
    }
    self.flush_config();
    trace!("SetSlot {} STABLE TO {}", slot, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotsForOwnershipChange
  pub fn try_prepare_slots_for_ownership_change(
    &self,
    slots: &HashSet<usize>,
    node_id: u128,
  ) -> Result<()> {
    {
      let mut current = self.current_config.write();
      let worker_id = current.get_worker_id_from_node_id(node_id);
      if worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      current.update_multi_slot_state(slots, worker_id, SlotState::Stable);
      if current.local_node_id() == Some(node_id) {
        current.bump_local_node_config_epoch();
      }
    }
    self.flush_config();
    trace!("SetSlots {:?} STABLE TO {}", slots, node_id);
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryResetSlotState
  pub fn try_reset_slot_state(&self, slot: usize) {
    let mut current = self.current_config.write();
    let slot_state = current.get_state(slot as u16);
    if slot_state == SlotState::Migrating || slot_state == SlotState::Importing {
      let worker_id = if slot_state == SlotState::Migrating {
        LOCAL_WORKER_ID as u16
      } else {
        current.get_worker_id_from_slot(slot as u16) as u16
      };
      current.update_slot_state(slot, worker_id, SlotState::Stable);
      drop(current);
      self.flush_config();
    }
  }

  /// 批量重置槽状态为 Stable（对标 C# TryResetSlotState(`HashSet<int>` slots) 重载）
  pub fn try_reset_slots_state(&self, slots: &HashSet<usize>) {
    {
      let mut current = self.current_config.write();
      current.reset_multi_slot_state(slots);
    }
    self.flush_config();
  }
}
