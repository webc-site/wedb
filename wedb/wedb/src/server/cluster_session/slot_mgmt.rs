//! 槽位管理命令实现（对标 libs/cluster/Session/RespClusterSlotManagementCommands.cs）

use std::sync::Arc;

use itoa::Buffer;
use wbase::{
  hash_slot::{CLUSTER_SLOT_COUNT, slot_of},
  hex::{hex_str_u128, hex_u128},
  map::HashSet as GxHashSet,
  num::strict_i64,
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{StorageSession, resp::slow_path::SlowWait};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_SLOW_PATH_STORAGE, abort_with_wrong_number_of_arguments,
    cluster::{
      ERR_GENERIC_SLOT_OUT_OFF_RANGE, ERR_GENERIC_SLOT_STATE, ERR_INVALID_SLOT,
      migration_slot_domain_error, write_slot_duplicate_error, write_slot_range_error,
    },
  },
  command::RespCommand,
  ext::{
    RespSliceExt, RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head,
    resp_frame_head_len,
  },
  resp_memory_writer::RespWriter,
};

use super::{
  ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, migration_slot_supported,
};
use crate::{
  error::Error,
  server::{cluster_config::ClusterConfig, hash_slot::SlotState},
};

/// SessionParseStateExtensions.cs:TryGetSlotState（ASCII 大小写不敏感）
fn parse_slot_state(arg: &[u8]) -> Option<SlotState> {
  match arg.len() {
    4 => {
      if arg.eq_ignore_ascii_case(b"FAIL") {
        Some(SlotState::Fail)
      } else if arg.eq_ignore_ascii_case(b"NODE") {
        Some(SlotState::Node)
      } else {
        None
      }
    }
    6 if arg.eq_ignore_ascii_case(b"STABLE") => Some(SlotState::Stable),
    7 => {
      if arg.eq_ignore_ascii_case(b"OFFLINE") {
        Some(SlotState::Offline)
      } else if arg.eq_ignore_ascii_case(b"INVALID") {
        Some(SlotState::Invalid)
      } else {
        None
      }
    }
    9 => {
      if arg.eq_ignore_ascii_case(b"MIGRATING") {
        Some(SlotState::Migrating)
      } else if arg.eq_ignore_ascii_case(b"IMPORTING") {
        Some(SlotState::Importing)
      } else {
        None
      }
    }
    _ => None,
  }
}

/// 槽位状态操作错误 → C# ClusterManagerSlotState 错误文案
fn slot_state_err_text(e: Error) -> String {
  use Error as E;
  match e {
    E::NodeNotFound(id) => format!("ERR I don't know about node {id}"),
    E::MigrateToMyself => "ERR Can't MIGRATE to myself".to_string(),
    E::TargetNotPrimary(id) => format!("ERR Target node {id} is not a master node."),
    E::SlotNotOwned(slot) => format!("ERR I'm not the owner of hash slot {slot}"),
    E::SlotAlreadyScheduled(slot) => {
      format!("ERR Slot {slot} already scheduled for migration or import")
    }
    E::NoWorkers => "ERR workers not initialized".to_string(),
    other => other.to_string(),
  }
}

/// 库级定槽聚合枚举（doc/zh/db.md 4.1）：枚举本节点活跃逻辑库
///（`wkv::VirtualDbManager::list_logic_dbs` 薄取用面）按 `(ns, db)` 定槽，
/// 命中槽位的库逐库 `set_context` 开只读会话交 `f` 聚合（键内容不参与定槽，
/// 单会话只覆盖单库，命令面聚合全部命中库）
async fn for_each_db_in_slot(
  store: &Arc<WedbStore<SegmentedDevice>>,
  slot: u16,
  mut f: impl AsyncFnMut(&StorageSession<'_, SegmentedDevice>) -> wkv::Result<()>,
) -> wkv::Result<()> {
  for (ns, db) in store.vdb.list_logic_dbs() {
    if slot_of(ns, db) != slot {
      continue;
    }
    let session = store.new_session()?;
    session.set_context(ns, db);
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    f(&storage).await?;
  }
  Ok(())
}

/// COUNTKEYSINSLOT 慢路径（库级聚合槽键计数）
async fn count_keys_in_slot_slow(store: Arc<WedbStore<SegmentedDevice>>, slot: u16) -> Vec<u8> {
  let mut out = Vec::new();
  let mut total = 0usize;
  match for_each_db_in_slot(&store, slot, async |storage| {
    total += storage.count_keys_in_slot(slot).await?;
    Ok(())
  })
  .await
  {
    Ok(()) => out.write_resp_int(total as i64),
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// GETKEYSINSLOT 慢路径（库级聚合槽键列举）
///
/// 流式编码 + 帧头预留-回填（全仓单点 `wresp::ext::{reserve,backfill}_resp_frame_head`）：
/// 扫描回调内直接把 bulk string 写进 out，出帧条数扫完才知，故帧头先按上界
/// `key_count`（written 恒不超过它）估宽预留、扫毕以实际 written 回填；
/// 上界与实际同位宽是常态，回填零移动，不再整块前插搬移 body。失败路径边扫边写
/// 已残留半截 body，`truncate(base)` 撤帧后统一回存储错误（不变量 2）
async fn get_keys_in_slot_slow(
  store: Arc<WedbStore<SegmentedDevice>>,
  slot: u16,
  key_count: usize,
) -> Vec<u8> {
  let write_head = |buf: &mut Vec<u8>, n| buf.write_resp_array_len(n);
  let reserved = resp_frame_head_len(key_count, write_head);
  let mut out = Vec::new();
  let base = reserve_resp_frame_head(&mut out, reserved);
  let mut written = 0usize;
  let res = for_each_db_in_slot(&store, slot, async |storage| {
    if written >= key_count {
      return Ok(());
    }
    storage
      .get_keys_in_slot_with(slot, key_count - written, |key| {
        out.write_resp_bulk_string(key);
        written += 1;
        written < key_count
      })
      .await
  })
  .await;
  match res {
    Ok(()) => backfill_resp_frame_head(&mut out, base, reserved, written, write_head),
    Err(_) => {
      out.truncate(base);
      out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    }
  }
  out
}

/// CLUSTER DELKEYSINSLOT(RANGE) 慢路径（库级聚合槽键删除，对标
/// libs/cluster/Server/ClusterManagerSlotState.cs:DeleteKeysInSlots）
async fn del_keys_in_slots_slow(
  store: Arc<WedbStore<SegmentedDevice>>,
  slots: Vec<u16>,
) -> Vec<u8> {
  let mut out = Vec::new();
  let mut ok = true;
  for slot in &slots {
    let res = for_each_db_in_slot(&store, *slot, async |storage| {
      storage.delete_slot_keys(&[*slot]).await?;
      Ok(())
    })
    .await;
    if res.is_err() {
      ok = false;
      break;
    }
  }
  if ok {
    out.write_resp_simple_string("OK");
  } else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
  }
  out
}

/// 槽位列表解析错误类型（libs/cluster/Session/ClusterCommands.cs:TryParseSlots）
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum SlotParseError {
  /// 非整数参数（C# `!TryGetInt` → CmdStrings.ERR_INVALID_SLOT）
  NotInteger,
  /// 区间形态奇数尾巴：残单参缺区间终点伴参（C# 第二个 TryGetInt 失败，
  /// 同报 CmdStrings.ERR_INVALID_SLOT）
  MissingPair,
  /// 槽位数值越界 [0, 16383]
  OutOfRange,
  /// 区间倒挂 (start > end)，带两端实参以还原 C# 动态文案
  InvalidRange(i64, i64),
  /// 槽位重复指定
  Duplicate(i64),
}

impl SlotParseError {
  /// 写出对应的 RESP 错误协议帧
  #[inline]
  pub fn write_to(self, output: &mut Vec<u8>) {
    match self {
      Self::NotInteger | Self::MissingPair => output.write_resp_error(ERR_INVALID_SLOT),
      Self::OutOfRange => output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE),
      Self::InvalidRange(start, end) => write_slot_range_error(output, start, end),
      Self::Duplicate(slot) => write_slot_duplicate_error(output, slot),
    }
  }
}

impl ClusterSession {
  /// libs/cluster/Session/ClusterCommands.cs:TryParseSlots
  ///
  /// 槽位（或区间对）参数解析；判定序与 C# 逐臂同序：非整数 → 区间倒挂 →
  /// 越界 → 重复（错误随首个违规槽位返回，调用方整批报错；非 range 臂以
  /// `slot_end = slot_start` 收敛，其倒挂态天然不可达）。range 臂奇数尾巴
  /// 报缺伴（C# 第二个 TryGetInt 失败），不静默截断
  pub(super) fn try_parse_slots(
    args: &[&[u8]],
    range: bool,
  ) -> Result<GxHashSet<usize>, SlotParseError> {
    let mut slots = GxHashSet::default();
    if range {
      // 切片成对消费，余尾即缺伴参（与 C# 逐对步进同序：先解析全部完整对，
      // 尾巴残单参才报缺伴）
      let (pairs, tail) = args.as_chunks::<2>();
      for chunk in pairs {
        let Some(start) = strict_i64(chunk[0]) else {
          return Err(SlotParseError::NotInteger);
        };
        let Some(end) = strict_i64(chunk[1]) else {
          return Err(SlotParseError::NotInteger);
        };
        if start > end {
          return Err(SlotParseError::InvalidRange(start, end));
        }
        if ClusterConfig::out_of_range(start) || ClusterConfig::out_of_range(end) {
          return Err(SlotParseError::OutOfRange);
        }
        for s in start..=end {
          if !slots.insert(s as usize) {
            return Err(SlotParseError::Duplicate(s));
          }
        }
      }
      if !tail.is_empty() {
        return Err(SlotParseError::MissingPair);
      }
    } else {
      for a in args {
        let Some(slot) = strict_i64(a) else {
          return Err(SlotParseError::NotInteger);
        };
        if ClusterConfig::out_of_range(slot) {
          return Err(SlotParseError::OutOfRange);
        }
        if !slots.insert(slot as usize) {
          return Err(SlotParseError::Duplicate(slot));
        }
      }
    }
    Ok(slots)
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterKeySlot
  ///
  /// 库级定槽形态（doc/zh/db.md 4.1）：槽位是 `(namespace, active_db)` 的
  /// 纯函数，与键内容无关，故本命令回声**调用会话的当前库槽位**（`slot`
  /// 由会话侧 `active_db_slot` 下传），参数键仅按 C# 形态校验个数。
  /// 同库任意键同槽，客户端据此确认所在库的属主节点
  pub(super) fn network_cluster_keyslot(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    slot: u16,
  ) -> bool {
    if args.len() != 1 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    output.write_resp_int(slot as i64);
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSlots
  pub(super) fn network_cluster_slots(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if !args.is_empty() {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    if let Some(m) = self.cluster_manager() {
      let info = m
        .current_config()
        .get_slots_info(self.preferred_endpoint_type());
      output.extend_from_slice(info.as_bytes());
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterAddSlots
  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterAddSlotsRange
  pub(super) fn network_cluster_add_slots(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    let range = cmd == RespCommand::ClusterAddslotsrange;
    // C# 形态校验：单槽 ≥1 且 < MAX_HASH_SLOT_VALUE 参（:25）；区间形态偶数参
    let valid = if range {
      !args.is_empty() && args.len().is_multiple_of(2)
    } else {
      !args.is_empty() && args.len() < CLUSTER_SLOT_COUNT
    };
    if !valid {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    match Self::try_parse_slots(args, range) {
      Err(err) => err.write_to(output),
      Ok(slots) => match self.cluster_manager().map(|m| m.try_add_slots(&slots)) {
        Some(Err(Error::SlotNotFree(slot))) => {
          let mut buf = Buffer::new();
          output.write_resp_error(&format!("ERR Slot {} is already busy", buf.format(slot)));
        }
        // C# slotIndex == -1 的非冲突失败路径同回 +OK
        Some(_) => output.write_resp_simple_string("OK"),
        None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
      },
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelSlots
  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelSlotsRange
  pub(super) fn network_cluster_del_slots(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    let range = cmd == RespCommand::ClusterDelslotsrange;
    // C# 形态校验：单槽 ≥1 且 < MAX_HASH_SLOT_VALUE 参（:193）；区间形态偶数参
    let valid = if range {
      !args.is_empty() && args.len().is_multiple_of(2)
    } else {
      !args.is_empty() && args.len() < CLUSTER_SLOT_COUNT
    };
    if !valid {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    match Self::try_parse_slots(args, range) {
      Err(err) => err.write_to(output),
      Ok(slots) => match self.cluster_manager().map(|m| m.try_remove_slots(&slots)) {
        Some(Err(Error::SlotNotLocal(slot))) => {
          let mut buf = Buffer::new();
          output.write_resp_error(&format!("ERR Slot {} is not assigned", buf.format(slot)));
        }
        Some(_) => output.write_resp_simple_string("OK"),
        None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
      },
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlot
  pub(super) fn network_cluster_set_slot(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() < 2 || args.len() > 3 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let Some(slot) = strict_i64(args[0]) else {
      output.write_resp_error(ERR_INVALID_SLOT);
      return true;
    };
    let slot_state = match parse_slot_state(args[1]) {
      Some(s) if !matches!(s, SlotState::Invalid | SlotState::Offline | SlotState::Fail) => s,
      _ => {
        output.write_resp_error(&format!(
          "ERR Slot state {} not supported.",
          args[1].as_str_safe()
        ));
        return true;
      }
    };
    // C# 语法约束：STABLE 不带 node-id，其余状态必须带。
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let node_id = match args.get(2) {
      Some(arg) => match hex_u128(arg) {
        Some(id) => Some(id),
        None => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        }
      },
      None => None,
    };
    if (slot_state == SlotState::Stable) == node_id.is_some() {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    }
    if ClusterConfig::out_of_range(slot) {
      output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
      return true;
    }
    // 迁移域门禁：非默认域槽位置 MIGRATING 后无驱动可承接（键扫描只覆盖默认域，
    // 空迁即交权），解析期显式拒绝，杜绝半迁移状态
    if slot_state == SlotState::Migrating && !migration_slot_supported(slot) {
      output.write_resp_error(&migration_slot_domain_error(slot));
      return true;
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let result = match slot_state {
      SlotState::Stable => {
        m.try_reset_slot_state(slot as usize);
        Ok(())
      }
      SlotState::Importing => m.try_prepare_slot_for_import(slot as usize, node_id.unwrap()),
      SlotState::Migrating => m.try_prepare_slot_for_migration(slot as usize, node_id.unwrap()),
      SlotState::Node => m.try_prepare_slot_for_ownership_change(slot as usize, node_id.unwrap()),
      _ => unreachable!("SETSLOT 已拒绝 Invalid/Offline/Fail"),
    };
    match result {
      Ok(()) => {
        // C# RespClusterSlotManagementCommands.cs:493
        // BlockingWait(UnsafeBumpAndWait...)——网络线程阻塞等全会话静止
        self.unsafe_bump_and_wait_for_epoch_transition();
        output.write_resp_simple_string("OK");
      }
      Err(e) => output.write_resp_error(&slot_state_err_text(e)),
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlotsRange
  pub(super) fn network_cluster_set_slots_range(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() < 3 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let slot_state = match parse_slot_state(args[0]) {
      Some(s) if !matches!(s, SlotState::Invalid | SlotState::Offline | SlotState::Fail) => s,
      _ => {
        output.write_resp_error(ERR_GENERIC_SLOT_STATE);
        return true;
      }
    };
    let stable = slot_state == SlotState::Stable;
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let node_id = if stable {
      None
    } else {
      match hex_u128(args[1]) {
        Some(id) => Some(id),
        None => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        }
      }
    };
    match Self::try_parse_slots(&args[if stable { 1 } else { 2 }..], true) {
      Err(err) => err.write_to(output),
      Ok(slots) => {
        // 迁移域门禁（同 SETSLOT 臂单点判据）：区间内含非默认域槽即整批拒绝，
        // 回显最小违规槽位（HashSet 无序，取序确定）
        if slot_state == SlotState::Migrating
          && let Some(slot) = slots
            .iter()
            .copied()
            .filter(|&s| !migration_slot_supported(s as i64))
            .min()
        {
          output.write_resp_error(&migration_slot_domain_error(slot as i64));
          return true;
        }
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        let result = if stable {
          m.try_reset_slots_state(&slots);
          Ok(())
        } else {
          match slot_state {
            SlotState::Importing => m.try_prepare_slots_for_import(&slots, node_id.unwrap()),
            SlotState::Migrating => m.try_prepare_slots_for_migration(&slots, node_id.unwrap()),
            SlotState::Node => m.try_prepare_slots_for_ownership_change(&slots, node_id.unwrap()),
            _ => unreachable!("SETSLOTSRANGE 已拒绝 Invalid/Offline/Fail"),
          }
        };
        match result {
          Ok(()) => {
            // C# RespClusterSlotManagementCommands.cs:593
            // BlockingWait(UnsafeBumpAndWait...)
            self.unsafe_bump_and_wait_for_epoch_transition();
            output.write_resp_simple_string("OK");
          }
          Err(e) => output.write_resp_error(&slot_state_err_text(e)),
        }
      }
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterCountKeysInSlot
  pub(super) fn network_cluster_count_keys_in_slot(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 1 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let Some(slot) = strict_i64(args[0]) else {
      output.write_resp_error(ERR_INVALID_SLOT);
      return true;
    };
    if ClusterConfig::out_of_range(slot) {
      output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
      return true;
    }
    // 声明偏离不对标：C# NetworkClusterCountKeysInSlot / NetworkClusterGetKeysInSlot
    // （RespClusterSlotManagementCommands.cs:160 / :373）按默认参调
    // IsLocal((ushort)slot)，即 enableReplicaReads=true（默认值见 ClusterConfig.cs:174），
    // 副本对其主节点持有的槽判真、直接本地计数/取键。依 cluster_manager_slot_state.rs
    // SETSLOT 面同裁定：C# 此处缺省 true 属笔误（同文件 IMPORTING 门与 MigrateCommand
    // 槽门均显式传 false），管理命令读门应仅主节点本地，故 rust 统一传 false——副本
    // 一律 MOVED 重定向。行为不变，仅留此偏离声明。
    let slot = slot as u16;
    let local = self
      .cluster_manager()
      .is_some_and(|m| m.current_config().is_local(slot, false));
    if !local {
      self.redirect_slot(slot, output);
      return true;
    }
    match self.cluster_provider.try_store() {
      Some(store) => {
        *self.pending_slow.lock() = Some(SlowWait::new(count_keys_in_slot_slow(store, slot)));
      }
      None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterGetKeysInSlot
  pub(super) fn network_cluster_get_keys_in_slot(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let Some(slot) = strict_i64(args[0]) else {
      output.write_resp_error(ERR_INVALID_SLOT);
      return true;
    };
    let Some(key_count) = strict_i64(args[1]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    if ClusterConfig::out_of_range(slot) {
      output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
      return true;
    }
    // 声明偏离不对标：C# NetworkClusterCountKeysInSlot / NetworkClusterGetKeysInSlot
    // （RespClusterSlotManagementCommands.cs:160 / :373）按默认参调
    // IsLocal((ushort)slot)，即 enableReplicaReads=true（默认值见 ClusterConfig.cs:174），
    // 副本对其主节点持有的槽判真、直接本地计数/取键。依 cluster_manager_slot_state.rs
    // SETSLOT 面同裁定：C# 此处缺省 true 属笔误（同文件 IMPORTING 门与 MigrateCommand
    // 槽门均显式传 false），管理命令读门应仅主节点本地，故 rust 统一传 false——副本
    // 一律 MOVED 重定向。行为不变，仅留此偏离声明。
    let slot = slot as u16;
    let local = self
      .cluster_manager()
      .is_some_and(|m| m.current_config().is_local(slot, false));
    if !local {
      self.redirect_slot(slot, output);
      return true;
    }
    match self.cluster_provider.try_store() {
      Some(store) => {
        let key_count = key_count.max(0) as usize;
        *self.pending_slow.lock() =
          Some(SlowWait::new(get_keys_in_slot_slow(store, slot, key_count)));
      }
      None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelKeysInSlot
  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelKeysInSlotRange
  pub(super) fn network_cluster_del_keys_in_slot(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    let range = cmd == RespCommand::ClusterDelkeysinslotrange;
    let valid = if range {
      !args.is_empty() && args.len().is_multiple_of(2)
    } else {
      args.len() == 1
    };
    if !valid {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let slots = if range {
      match Self::try_parse_slots(args, true) {
        Ok(slots) => slots.into_iter().map(|s| s as u16).collect(),
        Err(err) => {
          err.write_to(output);
          return true;
        }
      }
    } else {
      let Some(slot) = strict_i64(args[0]) else {
        output.write_resp_error(ERR_INVALID_SLOT);
        return true;
      };
      if ClusterConfig::out_of_range(slot) {
        output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
        return true;
      }
      vec![slot as u16]
    };
    match self.cluster_provider.try_store() {
      Some(store) => {
        *self.pending_slow.lock() = Some(SlowWait::new(del_keys_in_slots_slow(store, slots)));
      }
      None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSlotState
  pub(super) fn network_cluster_slot_state(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 1 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let Some(slot) = strict_i64(args[0]) else {
      output.write_resp_error(ERR_INVALID_SLOT);
      return true;
    };
    if ClusterConfig::out_of_range(slot) {
      output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
      return true;
    }
    let slot = slot as u16;
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // C# 状态符号投影：STABLE "=" IMPORTING "<" MIGRATING ">" OFFLINE "x" FAIL "*"
    let state_str = match m.current_config().get_state(slot) {
      SlotState::Stable => "=",
      SlotState::Importing => "<",
      SlotState::Migrating => ">",
      SlotState::Offline => "x",
      SlotState::Fail => "*",
      SlotState::Node | SlotState::Invalid => "x",
    };
    // RESP 渲染点：节点 id 转 32 字符小写 hex（0 号保留位渲染空串）
    let owner = m
      .current_config()
      .get_owner_id_from_slot(slot)
      .map_or_else(String::new, hex_str_u128);
    // C# TryWriteAsciiDirect($"+{slot} {stateStr} {nodeId}\r\n") 整帧直写单点
    let mut buf = Buffer::new();
    RespWriter::new_ref(output).write_ascii_direct(&format!(
      "+{} {} {}\r\n",
      buf.format(slot),
      state_str,
      owner
    ));
    true
  }

  /// libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterBanList
  pub(super) fn network_cluster_banlist(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if !args.is_empty() {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let banlist = self
      .cluster_manager()
      .map(|m| m.get_ban_list())
      .unwrap_or_default();
    output.write_resp_array_len(banlist.len());
    for item in &banlist {
      output.write_resp_bulk_string(item.as_bytes());
    }
    true
  }
}
