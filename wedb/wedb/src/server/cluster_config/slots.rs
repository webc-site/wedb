//! slot 状态读写面：槽位状态查询、属主/端点投影、槽位集合变更与 gossip
//! 槽位图合并。
//!
//! 对位 garnet/libs/cluster/Server/ClusterConfig.cs 的 `#region GetFromSlot`
//! 与 TryAddSlots / AssignSlots / TryRemoveSlots / UpdateSlotState /
//! UpdateMultiSlotState / ResetMultiSlotState / MergeSlotMap / GetShardRanges /
//! GetSlotList / GetLocalPrimarySlots / GetSlotCountForState。

use wbase::map::HashSet;

use super::*;
use crate::{
  error::{Error, Result},
  server::hash_slot::{SLOT_STATE_KINDS, SlotState},
};

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:OutOfRange
  #[inline]
  pub const fn out_of_range(slot: i64) -> bool {
    slot < 0 || slot >= CLUSTER_SLOT_COUNT as i64
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsImportingSlot
  #[inline]
  pub fn is_importing_slot(&self, slot: u16) -> bool {
    self
      .slot_map
      .get(slot as usize)
      .is_some_and(|s| s.state == SlotState::Importing)
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsMigratingSlot
  #[inline]
  pub fn is_migrating_slot(&self, slot: u16) -> bool {
    self
      .slot_map
      .get(slot as usize)
      .is_some_and(|s| s.state == SlotState::Migrating)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetState
  #[inline]
  pub fn get_state(&self, slot: u16) -> SlotState {
    self
      .slot_map
      .get(slot as usize)
      .map_or(SlotState::Invalid, |s| s.state)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerIdFromSlot
  #[inline]
  pub fn get_worker_id_from_slot(&self, slot: u16) -> usize {
    // 对齐 C#（ClusterConfig.cs:447 经 HashSlot.workerId 投影）返回 eff 属主：
    // Migrating 槽仍由源节点持有并对外负责（C# ClusterManagerSlotState.cs:122
    // "return this node as the current owner"），迁移目标仅作 ASK 重定向
    self
      .slot_map
      .get(slot as usize)
      .map_or(0, |s| s.eff_worker_id() as usize)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeIdFromSlot
  ///
  /// 经 eff 属主投影（C# ClusterConfig.cs:455）
  #[inline]
  pub fn get_node_id_from_slot(&self, slot: u16) -> Option<u128> {
    let wid = self.get_worker_id_from_slot(slot);
    self.workers.get(wid).and_then(|w| w.nodeid)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetOwnerIdFromSlot
  ///
  /// 对齐 C#（ClusterConfig.cs:463）取 `_workerId` raw：即便 Migrating 也报
  /// 迁移目标（与 [`Self::get_node_id_from_slot`] 的 eff 语义刻意区分）
  #[inline]
  pub fn get_owner_id_from_slot(&self, slot: u16) -> Option<u128> {
    let wid = self.slot_map[slot as usize].worker_id as usize;
    self.workers.get(wid).and_then(|w| w.nodeid)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointFromSlot
  ///
  /// eff 属主投影（C# ClusterConfig.cs:471 经 GetWorkerIdFromSlot）：
  /// MOVED 端点指向仍服务该槽的源节点
  #[inline]
  pub fn get_endpoint_from_slot(
    &self,
    slot: u16,
    pref_type: ClusterPreferredEndpointType,
  ) -> (String, i32) {
    self.endpoint_of_worker_id(self.get_worker_id_from_slot(slot), pref_type)
  }

  /// libs/cluster/Server/ClusterConfig.cs:AskEndpointFromSlot
  ///
  /// 对齐 C#（ClusterConfig.cs:484）取 `_workerId` raw：ASK 重定向指向
  /// 迁移目标（与 [`Self::get_endpoint_from_slot`] 的 eff 源节点语义配对）
  #[inline]
  pub fn ask_endpoint_from_slot(
    &self,
    slot: u16,
    pref_type: ClusterPreferredEndpointType,
  ) -> (String, i32) {
    self.endpoint_of_worker_id(self.slot_map[slot as usize].worker_id as usize, pref_type)
  }

  /// 槽端点投影公共尾：wid 越界回退 `("?", -1)`，命中则按偏好类型投影
  /// 端点并取该 worker 端口
  fn endpoint_of_worker_id(
    &self,
    wid: usize,
    pref_type: ClusterPreferredEndpointType,
  ) -> (String, i32) {
    if wid < self.workers.len() {
      (
        self.get_endpoint_by_preferred_type(wid, pref_type),
        self.workers[wid].port,
      )
    } else {
      ("?".to_string(), -1)
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointByPreferredType
  fn get_endpoint_by_preferred_type(
    &self,
    worker_id: usize,
    pref_type: ClusterPreferredEndpointType,
  ) -> String {
    match pref_type {
      ClusterPreferredEndpointType::Ip => self.workers[worker_id].address.to_string(),
      ClusterPreferredEndpointType::Hostname => {
        if let Some(ref h) = self.workers[worker_id].hostname
          && !h.is_empty()
        {
          return h.to_string();
        }
        "?".to_string()
      }
      ClusterPreferredEndpointType::Unknown => "?".to_string(),
    }
  }

  /// 单遍扫描统计全部槽位状态计数；CLUSTER INFO 需要 4 个状态计数时
  /// 复用本方法，避免 4 次全表遍历
  pub fn slot_state_counts(&self) -> [usize; SLOT_STATE_KINDS] {
    self
      .slot_map
      .iter()
      .fold([0usize; SLOT_STATE_KINDS], |mut counts, slot| {
        counts[slot.state as usize] += 1;
        counts
      })
  }

  /// libs/cluster/Server/ClusterConfig.cs:TryAddSlots
  ///
  /// 先整体校验再占位：与 C# 的"新配置上试错"等价的 all-or-nothing 语义，
  /// 但无需整份克隆
  pub fn try_add_slots(&mut self, slots: Option<&HashSet<usize>>, state: SlotState) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      // C#（ClusterConfig.cs:1351）按 eff 属主判定空闲（workerId == 0；
      // eff 仅将 Migrating 投影为 LOCAL(1)，eff==0 ⟺ raw==0，此处直读等价）
      if self.slot_map[slot].worker_id != 0 {
        return Err(Error::SlotNotFree(slot));
      }
    }
    for &slot in s {
      let e = &mut self.slot_map[slot];
      e.worker_id = LOCAL_WORKER_ID as u16;
      e.state = state;
    }
    Ok(())
  }

  /// libs/cluster/Server/ClusterConfig.cs:AssignSlots
  pub fn assign_slots(&mut self, slots: &[usize], worker_id: u16, state: SlotState) -> &mut Self {
    for &slot in slots {
      let e = &mut self.slot_map[slot];
      e.worker_id = worker_id;
      e.state = state;
    }
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:TryRemoveSlots
  pub fn try_remove_slots(&mut self, slots: Option<&HashSet<usize>>) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      // C#（ClusterConfig.cs:1402）按 eff 属主判定非本地（eff==0 ⟺ raw==0，
      // 同 try_add_slots 的直读等价性）
      if self.slot_map[slot].worker_id == 0 {
        return Err(Error::SlotNotLocal(slot));
      }
    }
    for &slot in s {
      let e = &mut self.slot_map[slot];
      e.worker_id = 0;
      e.state = SlotState::Offline;
    }
    Ok(())
  }

  /// libs/cluster/Server/ClusterConfig.cs:UpdateSlotState
  pub fn update_slot_state(&mut self, slot: usize, worker_id: u16, state: SlotState) -> &mut Self {
    let e = &mut self.slot_map[slot];
    e.worker_id = worker_id;
    e.state = state;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:UpdateMultiSlotState
  pub fn update_multi_slot_state(
    &mut self,
    slots: &HashSet<usize>,
    worker_id: u16,
    state: SlotState,
  ) -> &mut Self {
    for &slot in slots {
      let e = &mut self.slot_map[slot];
      e.worker_id = worker_id;
      e.state = state;
    }
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:ResetMultiSlotState
  pub fn reset_multi_slot_state(&mut self, slots: &HashSet<usize>) -> &mut Self {
    for &slot in slots {
      // Migrating 槽归本地源节点，其余按 eff 属主回稳
      let st = self.get_state(slot as u16);
      let wid = if st == SlotState::Migrating {
        LOCAL_WORKER_ID as u16
      } else {
        self.get_worker_id_from_slot(slot as u16) as u16
      };
      let e = &mut self.slot_map[slot];
      e.worker_id = wid;
      e.state = SlotState::Stable;
    }
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:MergeSlotMap
  ///
  /// 原地合并槽位图，返回是否有槽位变化。调用方需先做整体克隆
  /// （与 C# Copy 一次 slotMap 相同开销）
  pub fn merge_slot_map(&mut self, sender_config: &ClusterConfig) -> bool {
    let sender_slot_map = &sender_config.slot_map;
    let assign_to_worker_id = sender_config
      .local_node_id()
      .map_or(0, |id| self.get_worker_id_from_node_id(id));
    if assign_to_worker_id == 0 {
      return false;
    }
    let mut updated = false;

    if sender_config.is_primary() {
      let sender_epoch = sender_config.local_node_config_epoch();

      for (slot, sender_slot) in self.slot_map.iter_mut().zip(sender_slot_map) {
        if sender_slot.state != SlotState::Stable {
          continue;
        }

        // 对齐 C#（ClusterConfig.cs:1168 同经 HashSlot.workerId 投影取 eff）：
        // 本地 Migrating 槽的当前归属按 LOCAL(1) 判定——迁移目标节点 gossip
        // 认领时走 epoch 比较直接移交，而非误判为"目标已是属主"把槽重置为
        // Offline 造成短暂失主
        let current_owner_id = slot.eff_worker_id() as usize;

        // 发送方非本槽认领者且是主：若本地认为属主即发送方（epoch 碰撞后
        // 的错位状态），重置为 Offline 给真实属主重新认领的机会
        if sender_slot.worker_id as usize != LOCAL_WORKER_ID {
          if current_owner_id == assign_to_worker_id as usize {
            slot.worker_id = RESERVED_WORKER_ID as u16;
            slot.state = SlotState::Offline;
            updated = true;
          }
          continue;
        }

        // 发送方是本槽认领者且为主：仅当其 epoch 更高才可改写本槽
        if sender_epoch != 0
          && self
            .workers
            .get(current_owner_id)
            .is_some_and(|w| w.config_epoch >= sender_epoch)
        {
          continue;
        }

        // 仅当属主或状态变化才算更新：避免 sender epoch=0 时的消息风暴
        updated |= slot.worker_id != assign_to_worker_id || slot.state != SlotState::Stable;
        slot.worker_id = assign_to_worker_id;
        slot.state = SlotState::Stable;
      }
    } else {
      // 发送方为副本：主节点目标在循环前一次性解析，杜绝循环内重复线性扫描；
      // 若目标主节点未在本地登记，禁止移交，防止将槽位挂至 0 号保留节点导致状态机破损
      let handoff_worker_id = sender_config
        .local_node_primary_id()
        .map_or(0, |pid| self.get_worker_id_from_node_id(pid));
      if handoff_worker_id == 0 {
        return false;
      }

      for (slot, sender_slot) in self.slot_map.iter_mut().zip(sender_slot_map) {
        if sender_slot.state != SlotState::Stable {
          continue;
        }

        let current_owner_id = slot.eff_worker_id() as usize;

        // 副本场景：仅当本槽现属主即发送方（旧主）才允许移交其主节点，
        // 保证计划内 failover 下多副本乱序 gossip 只有接管者生效；
        // assign_to_worker_id > 0，故此处天然排除了 current_owner_id == RESERVED_WORKER_ID (0)
        if current_owner_id != assign_to_worker_id as usize {
          continue;
        }

        updated |= slot.worker_id != handoff_worker_id || slot.state != SlotState::Stable;
        slot.worker_id = handoff_worker_id;
        slot.state = SlotState::Stable;
      }
    }

    updated
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetShardRanges
  ///
  /// 单遍扫描输出连续槽区间；哨兵扫描自然闭合末区间。
  /// 对齐 C#（ClusterConfig.cs:641）按 eff 属主分段（Migrating 槽随源节点）
  pub fn get_shard_ranges(&self, worker_id: usize) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut start: Option<u16> = None;
    for (i, slot) in self.slot_map.iter().enumerate() {
      match (start, slot.eff_worker_id() as usize == worker_id) {
        (None, true) => start = Some(i as u16),
        (Some(s), false) => {
          ranges.push((s, i as u16 - 1));
          start = None;
        }
        _ => {}
      }
    }
    if let Some(s) = start {
      ranges.push((s, CLUSTER_SLOT_COUNT as u16 - 1));
    }
    ranges
  }

  /// 按 eff 属主生成指定 worker 负责的槽位迭代器（零分配）
  pub fn slot_list_iter(&self, worker_id: u16) -> impl Iterator<Item = usize> + '_ {
    self
      .slot_map
      .iter()
      .enumerate()
      .filter_map(move |(i, slot)| (slot.eff_worker_id() == worker_id).then_some(i))
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetSlotList
  ///
  /// 对齐 C#（ClusterConfig.cs:918）按 eff 属主收集：TryStopWrites 的
  /// `GetSlotList(1)` 须把 Migrating 槽一并移交接管者
  pub fn get_slot_list(&self, worker_id: u16) -> Vec<usize> {
    self.slot_list_iter(worker_id).collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalPrimarySlots
  ///
  /// 获取当前节点的本地主节点所负责的槽位列表
  pub fn get_local_primary_slots(&self) -> Vec<usize> {
    let Some(primary_id) = self.local_node_primary_id() else {
      return Vec::new();
    };
    let target_wid = self.get_worker_id_from_node_id(primary_id);
    if target_wid == 0 {
      return Vec::new();
    }
    self
      .slot_map
      .iter()
      .enumerate()
      .filter_map(|(i, slot)| (slot.worker_id == target_wid).then_some(i))
      .collect()
  }
}
