//! 集群会话实现（对标 libs/cluster/Session/ClusterSession.cs）
//!
//! 实现 wnode 的 [`ClusterSession`] 切面（IClusterSession 会话侧子集），
//! 向 `RespServerSession` 提供槽位验证、重定向、CLUSTER 命令族与 ROLE
//! 集群分支数据。

use std::{
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
  time::Duration,
};

use compio::runtime::spawn;
use gxhash::HashSet as GxHashSet;
use itoa::Buffer;
use parking_lot::{Mutex, RwLock};
use waof::AofAddress;
use wbase::{
  convert::{TICKS_PER_MILLISECOND, UNIX_EPOCH_TICKS},
  hash_slot::hash_slot as cluster_slot,
  num::{strict_i32, strict_i64},
};
use wkv::WedbStore;
use wnode::{
  ClusterSlotVerificationInput, RoleInfo, SlotVerifyGate, StorageSession,
  cluster_session::ClusterSessionFace, extract_keys_from_slice, resp::slow_path::SlowWait,
  session_parse_state_extensions::ManagerType,
};
use wresp::{
  RespCommand, RespSliceExt, RespVecExt,
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_SLOW_PATH_STORAGE, abort_with_wrong_number_of_arguments, write_error_raw,
  },
  try_get_resp_command_info,
};

use crate::{
  error::Error,
  server::{
    cluster::{ClusterPreferredEndpointType, IClusterProvider},
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig, LOCAL_WORKER_ID},
    cluster_manager::{ClusterManager, GateVerdict, SlotVerifyRequest, SlotWaitMemo},
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    failover::failover_option::FailoverOption,
    hash_slot::SlotState,
    migration::migrate_driver::{MIGRATION_RECORD_KIND_STRING, parse_migration_payload},
    replication::{
      checkpoint_entry::CheckpointEntry, cluster_replication_session::AppendLogOutcome,
      recovery_status::RecoveryStatus, sync_metadata::SyncMetadata,
    },
    slot_verify::{ClusterSlotVerificationState, SlotVerifySessionState},
    worker::NodeRole,
  },
};

/// libs/cluster/CmdStrings.cs 集群域 RESP 错误文案（通用文案与 wresp 单源，
/// 仅集群专属文案留存本表）
mod err {
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE
  pub const SLOT_OUT_OF_RANGE: &str = "ERR Slot out of range";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT
  pub const CONFIG_EPOCH_ASSIGNMENT: &str =
    "ERR The user can assign a config epoch only when the node does not know any other node";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF
  pub const CANNOT_FORGET_MYSELF: &str = "ERR I tried hard but I can't forget myself";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY
  pub const CANNOT_FORGET_MY_PRIMARY: &str = "ERR Can't forget my primary";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER
  pub const CANNOT_FAILOVER_FROM_NON_MASTER: &str = "ERR Cannot failover a non-master node";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_UNKNOWN_ENDPOINT
  pub const UNKNOWN_ENDPOINT: &str = "ERR Unknown endpoint";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_SLOT_STATE
  pub const SLOT_STATE: &str = "ERR Invalid slot state";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_INVALID_SLOT
  pub const INVALID_SLOT: &str = "ERR Invalid or out of range slot";
  /// libs/cluster/CmdStrings.cs:RESP_ERR_MULTI_LOG_DISABLED
  pub const MULTI_LOG_DISABLED: &str = "ERR Multi-log disabled";
}

/// 集群 RESP 会话实现
pub struct ClusterSession {
  cluster_provider: Arc<ClusterProvider>,
  /// gossip 对端节点 id（C# RemoteNodeId：连接一旦确立不随 gossip 变更）
  remote_node_id: RwLock<Option<String>>,
  /// 最近一次 gossip 应答的配置字节（C# lastSentConfig：配置变更判定基准）
  last_sent_config: Mutex<Option<Vec<u8>>>,
  read_only: AtomicBool,
  internal_write: AtomicBool,
  /// 批次级纪元快照（C# _localCurrentEpoch：0 = 批外空闲，非 0 = 批内
  /// 持有的 provider 纪元；消费批首尾取放，provider 静止等待的观测面）
  local_current_epoch: AtomicI64,
  /// CLUSTER RESET 等需异步闭环命令挂起的慢路径执行体
  ///（会话侧经 [`ClusterSessionFace::take_pending_slow`] 取走驱动）
  pending_slow: Mutex<Option<SlowWait>>,
  /// 致命断流登记（C# 集群命令 GarnetException clientResponse:false 上抛
  /// 等价：处理失败时登记文案，会话侧经
  /// [`ClusterSessionFace::take_fatal_disconnect`] 取走转致命哨兵断连）
  fatal_disconnect: Mutex<Option<String>>,
  /// 槽位校验等待交接记忆（超时旗标 + 异步存在性裁决缓存）：等待体与
  /// 下一次门评之间的确定性交接，防挂起-重评活锁
  slot_wait_memo: Mutex<Option<Arc<SlotWaitMemo>>>,
}

impl ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:ClusterSession（构造）
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      remote_node_id: RwLock::new(None),
      last_sent_config: Mutex::new(None),
      read_only: AtomicBool::new(false),
      internal_write: AtomicBool::new(false),
      local_current_epoch: AtomicI64::new(0),
      pending_slow: Mutex::new(None),
      fatal_disconnect: Mutex::new(None),
      slot_wait_memo: Mutex::new(None),
    }
  }

  fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_provider.cluster_manager()
  }

  /// 槽位验证会话态快照（READONLY / ASKING / 内部写三标志投影）
  fn slot_verify_session_state(&self, session_asking: bool) -> SlotVerifySessionState {
    SlotVerifySessionState {
      session_asking,
      read_only_session: self.read_only.load(Ordering::Relaxed),
      internal_write: self.internal_write.load(Ordering::Relaxed),
    }
  }

  /// libs/cluster/Session/ClusterSession.cs:UnsafeBumpAndWaitForEpochTransitionAsync
  ///
  /// 释放本会话批内快照 → provider 推进纪元并等全会话静止 → 重取快照。
  /// C# 命令侧以 `AsyncUtils.BlockingWait` 驱动的同步批内形态（网络线程
  /// 阻塞等待语义），本会话自身快照先行清零，不阻塞静止等待收敛
  pub fn unsafe_bump_and_wait_for_epoch_transition(&self) -> bool {
    self.release_current_epoch();
    let caught_up = self.cluster_provider.bump_and_wait_for_epoch_transition();
    self.acquire_current_epoch();
    caught_up
  }

  /// libs/cluster/Session/ClusterSession.cs:Redirect（槽位非本地属主 → MOVED）
  fn redirect_slot(&self, slot: u16, output: &mut Vec<u8>) {
    if let Some(m) = self.cluster_manager() {
      let config = m.current_config();
      let (endpoint, port) = config.get_endpoint_from_slot(slot, ClusterPreferredEndpointType::Ip);
      ClusterSlotVerificationState::Moved {
        slot,
        endpoint,
        port,
      }
      .write_resp_error(output);
    }
  }

  /// libs/cluster/Session/ClusterCommands.cs:TryParseSlots
  ///
  /// 槽位（或区间对）参数解析；重复/越界/区间倒挂报错（C# 同名语义：
  /// 错误随首个违规槽位返回，调用方整批报错）
  fn try_parse_slots(args: &[&[u8]], range: bool) -> Result<GxHashSet<usize>, (&'static str, i64)> {
    let mut slots = GxHashSet::default();
    let mut i = 0;
    while i < args.len() {
      // 区间形态成对取起止，单槽形态起止同值
      let Some(slot_start) = strict_i64(args[i]) else {
        return Err((err::INVALID_SLOT, 0));
      };
      let slot_end = if range {
        let Some(end) = args.get(i + 1).copied().and_then(strict_i64) else {
          return Err((err::INVALID_SLOT, 0));
        };
        i += 1;
        end
      } else {
        slot_start
      };
      i += 1;
      if slot_start > slot_end {
        return Err(("ERR Invalid range", slot_start));
      }
      if ClusterConfig::out_of_range(slot_start.max(0) as usize)
        || ClusterConfig::out_of_range(slot_end.max(0) as usize)
      {
        return Err((err::SLOT_OUT_OF_RANGE, slot_end));
      }
      for slot in slot_start..=slot_end {
        if !slots.insert(slot as usize) {
          return Err(("duplicate", slot));
        }
      }
    }
    Ok(slots)
  }

  /// 内部写标志位存取（trait 面 is_internal_write_session 处标注 C# 映射）
  pub fn internal_write(&self) -> bool {
    self.internal_write.load(Ordering::Relaxed)
  }

  /// libs/cluster/Session/ClusterSession.cs:SetInternalWriteSession
  pub fn set_internal_write(&self, val: bool) {
    self.internal_write.store(val, Ordering::Relaxed);
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip
  ///
  /// gossip 载荷合并 + 配置应答（lastSentConfig 变更判定）+ 复制健康检查
  fn network_cluster_gossip(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.is_empty() || args.len() > 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterGossip));
      return true;
    }
    let (with_meet, payload) = if args.len() > 1 {
      (args[0].eq_ignore_ascii_case(b"WITHMEET"), args[1])
    } else {
      (false, args[0])
    };
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.stats.update_gossip_bytes_recv(payload.len() as i64);
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if !payload.is_empty() {
      // 载荷版本预检（C# ClusterConfig.TryPeekVersion）
      let version_ok =
        ClusterConfig::try_peek_version(payload).is_some_and(|v| v == CLUSTER_CONFIG_VERSION);
      if !version_ok {
        log::warn!("Received gossip with incompatible config version");
      } else if let Ok(other) = ClusterConfig::from_byte_array(payload) {
        let known = m
          .current_config()
          .is_known(other.local_node_id().unwrap_or_default());
        if with_meet || known {
          m.try_merge(&other, true);
          if let Some(id) = other.local_node_id() {
            *self.remote_node_id.write() = Some(id.to_string());
          }
        } else {
          log::warn!(
            "Received gossip from unknown node: {}",
            other.local_node_id().unwrap_or_default()
          );
        }
      }
    }
    // 配置变更或 WITHMEET 显式要求 → 回当前配置字节；否则空载荷
    let current_bytes = m.current_config().to_byte_array();
    let changed = self
      .last_sent_config
      .lock()
      .as_ref()
      .is_none_or(|last| last != &current_bytes);
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.stats
        .update_gossip_bytes_send(current_bytes.len() as i64);
    }
    if changed || with_meet {
      output.write_resp_bulk_string(&current_bytes);
      *self.last_sent_config.lock() = Some(current_bytes);
    } else {
      output.write_resp_bulk_string(b"");
    }
    // gossip 后的复制健康检查（C# EnsureReplication）
    {
      let remote = self.remote_node_id.read();
      self.cluster_provider.ensure_replication(remote.as_deref());
    }
    true
  }

  /// libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailover
  fn network_cluster_failover(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() > 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterFailover));
      return true;
    }
    let mut option = FailoverOption::Default;
    let mut abort = false;
    let mut timeout_secs: i64 = 0;
    if let Some(arg0) = args.first() {
      if arg0.eq_ignore_ascii_case(b"ABORT") {
        abort = true;
      } else if arg0.eq_ignore_ascii_case(b"FORCE") {
        option = FailoverOption::Force;
      } else if arg0.eq_ignore_ascii_case(b"TAKEOVER") {
        option = FailoverOption::Takeover;
      } else {
        output.write_resp_error(&format!(
          "ERR Failover option ({}) not supported",
          String::from_utf8_lossy(arg0)
        ));
        return true;
      }
      if let Some(arg1) = args.get(1) {
        match strict_i64(arg1) {
          Some(v) => timeout_secs = v,
          None => {
            output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return true;
          }
        }
      }
    }
    let Some(fm) = self.cluster_provider.failover_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if abort {
      fm.try_abort_replica_failover();
      output.write_resp_simple_string("OK");
      return true;
    }
    // 副本身份校验（C# current.IsReplica && LocalNodePrimaryId != null）
    let (is_replica, primary_addr) = self
      .cluster_manager()
      .map(|m| {
        let config = m.current_config();
        (
          config.is_replica() && config.local_node_primary_id().is_some(),
          config.get_local_node_primary_address(),
        )
      })
      .unwrap_or((false, (None, 0)));
    if !is_replica {
      output.write_resp_error("ERR Node is not configured as a REPLICA");
      return true;
    }
    const DEFAULT_REPLICA_FAILOVER_TIMEOUT_SECS: u64 = 60;
    let timeout = if timeout_secs > 0 {
      Duration::from_secs(timeout_secs as u64)
    } else {
      Duration::from_secs(DEFAULT_REPLICA_FAILOVER_TIMEOUT_SECS)
    };
    if !fm.try_start_replica_failover(option, timeout) {
      let (addr, port) = primary_addr;
      output.write_resp_error(&format!(
        "ERR failed to start failover for primary({}:{port})",
        addr.unwrap_or_default(),
        port = port
      ));
      return true;
    }
    output.write_resp_simple_string("OK");
    true
  }

  /// libs/cluster/Session/ReplicaOfCommand.cs:NetworkTryREPLICAOF
  fn network_replicaof(&self, cmd: RespCommand, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() != 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    // REPLICAOF NO ONE：解除从属（保留数据），转为主节点
    if args[0].eq_ignore_ascii_case(b"NO") && args[1].eq_ignore_ascii_case(b"ONE") {
      let Some(rm) = self.cluster_provider.replication_manager() else {
        output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
        return true;
      };
      if !rm.begin_recovery(RecoveryStatus::ReplicaOfNoOne, false) {
        output.write_resp_error(ERR_RECOVERY_LOCK);
        return true;
      }
      if let Some(m) = self.cluster_manager() {
        m.try_reset_replica();
      }
      rm.try_update_for_failover();
      rm.reset_replica_replay_driver_store();
      // C# ReplicaOfCommand.cs:48 BlockingWait(UnsafeBumpAndWait...)
      self.unsafe_bump_and_wait_for_epoch_transition();
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
      output.write_resp_simple_string("OK");
      return true;
    }
    let Some(port) = strict_i64(args[1]) else {
      output.write_resp_error(&format!(
        "ERR REPLICAOF failed to parse port '{}'",
        String::from_utf8_lossy(args[1])
      ));
      return true;
    };
    let addr = String::from_utf8_lossy(args[0]);
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let Some(primary_id) = m
      .current_config()
      .get_worker_node_id_from_address(&addr, port as i32)
    else {
      output.write_resp_error(&format!("ERR I don't know about node {addr}:{port}."));
      return true;
    };
    // 数据同步发起面（C# TryReplicateDiskbasedSyncAsync）：配置翻转经
    // try_add_replica_async 闭环，AOF attach 由装配期重连钩子承接
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match m.try_add_replica_async(&primary_id, true, false).await {
        Ok(()) => out.write_resp_simple_string("OK"),
        Err(Error::CannotAcquireRecoveryLock) => out.write_resp_error(ERR_RECOVERY_LOCK),
        Err(e) => out.write_resp_error(&replicate_err_text(e)),
      }
      out
    }));
    true
  }

  /// libs/cluster/Session/FailoverCommand.cs:TryFAILOVER（顶层 FAILOVER）
  fn network_failover(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let mut replica_address: Option<&str> = None;
    let mut replica_port: i64 = 0;
    let mut timeout_ms: i64 = -1;
    let mut abort = false;
    let mut force = false;
    let mut option_takeover = false;
    let mut i = 0;
    while i < args.len() {
      let arg = args[i];
      i += 1;
      if arg.eq_ignore_ascii_case(b"TO") {
        let Some(addr) = args.get(i) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        let Ok(addr_str) = from_utf8(addr) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        replica_address = Some(addr_str);
        i += 1;
        let Some(port) = args.get(i).copied().and_then(strict_i64) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        replica_port = port;
        i += 1;
      } else if arg.eq_ignore_ascii_case(b"TIMEOUT") {
        let Some(t) = args.get(i).copied().and_then(strict_i64) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        timeout_ms = t;
        i += 1;
      } else if arg.eq_ignore_ascii_case(b"ABORT") {
        abort = true;
      } else if arg.eq_ignore_ascii_case(b"FORCE") {
        force = true;
      } else if arg.eq_ignore_ascii_case(b"TAKEOVER") {
        force = true;
        // TAKEOVER 额外豁免同步等待语义（C# FailoverOption.TAKEOVER）
        option_takeover = true;
      } else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return true;
      }
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let is_primary = m.current_config().local_node_role() == NodeRole::Primary;
    if !is_primary {
      output.write_resp_error(err::CANNOT_FAILOVER_FROM_NON_MASTER);
      return true;
    }
    // TO 目标校验：须为已知节点、副本角色、且隶属于本节点
    if let Some(replica_address) = replica_address {
      let config = m.current_config();
      let Some(replica_id) =
        config.get_worker_node_id_from_address(replica_address, replica_port as i32)
      else {
        output.write_resp_error(err::UNKNOWN_ENDPOINT);
        return true;
      };
      let Some(worker) = config.get_worker_from_node_id(&replica_id) else {
        output.write_resp_error(err::UNKNOWN_ENDPOINT);
        return true;
      };
      if worker.role != NodeRole::Replica {
        output.write_resp_error(&format!(
          "ERR Node @{replica_address}:{replica_port} is not a replica."
        ));
        return true;
      }
      if worker.replica_of_node_id.as_deref() != config.local_node_id() {
        output.write_resp_error(&format!(
          "ERR Node @{replica_address}:{replica_port} is not my replica."
        ));
        return true;
      }
    }
    let Some(fm) = self.cluster_provider.failover_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if abort {
      m.try_set_local_node_role(NodeRole::Primary);
      fm.try_abort_replica_failover();
    } else {
      let timeout = if timeout_ms <= 0 {
        Duration::MAX
      } else {
        Duration::from_millis(timeout_ms as u64)
      };
      let option = if option_takeover {
        FailoverOption::Takeover
      } else if force {
        FailoverOption::Force
      } else {
        FailoverOption::Default
      };
      fm.try_start_primary_failover(
        replica_address.unwrap_or_default(),
        replica_port as i32,
        option,
        timeout,
      );
    }
    output.write_resp_simple_string("OK");
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReserve
  ///
  /// 迁移预保留向量集上下文（仅节点间使用）：`args[0]` 须为
  /// VECTOR_SET_CONTEXTS，`args[1]` 为正整数上下文数；应答 `*n` +
  /// 逐上下文十进制简单串（C# TryWriteInt64AsSimpleString）
  fn network_cluster_reserve(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    // C# parseState.Count < 2 / 非法计数 → invalidParameters（元数错误）
    if args.len() < 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterReserve));
      return true;
    }
    if !args[0].eq_ignore_ascii_case(b"VECTOR_SET_CONTEXTS") {
      write_error_raw(output, "Unrecognized reservation type");
      return true;
    }
    let count = match strict_i64(args[1]) {
      Some(v) if v > 0 => v as usize,
      _ => {
        abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterReserve));
        return true;
      }
    };
    let Some(vm) = self.cluster_provider.try_vector_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    match vm.reserve_contexts_for_migration(count) {
      Some(contexts) => {
        output.write_resp_array_len(contexts.len());
        for ctx in contexts {
          let mut buf = Buffer::new();
          output.write_resp_simple_string(buf.format(ctx));
        }
      }
      // 上下文空间耗尽（C# 无对应失败分支；超 u32::MAX 编址上限显式拒绝）
      None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAdvanceTime
  ///
  /// 副本侧时间脉冲（2 参：子日志下标 + 序列号）；解析失败 / 越界 /
  /// 恢复中均无应答写出（C# 同口径静默）
  fn network_cluster_advance_time(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() != 2 {
      abort_with_wrong_number_of_arguments(
        output,
        cluster_sub_name(RespCommand::ClusterAdvanceTime),
      );
      return true;
    }
    let (Some(idx), Some(sequence_number)) = (strict_i32(args[0]), strict_i64(args[1])) else {
      // C# 解析失败仅记日志
      return true;
    };
    let Some(rm) = self.cluster_provider.replication_manager() else {
      return true;
    };
    // 子日志越界或恢复中（C# CannotStreamAOF）不可推进
    if idx < 0 || idx as usize >= rm.sublog_count() || rm.cannot_stream_aof() {
      return true;
    }
    if let Some(driver) = rm
      .replica_replay_driver_store
      .get_replay_driver(idx as usize)
    {
      driver.signal_time_advance(sequence_number);
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterMlogKeyTime
  ///
  /// 多日志键序列号查询（1-2 参：键 + 可选 FRONTIER）：主端回序列号生成器
  /// 最新值，副本回键的重放序列号。AOF 门控未点亮 / 单物理日志按 C#
  /// RESP_ERR_MULTI_LOG_DISABLED 同口径显式报错
  fn network_cluster_mlog_key_time(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.is_empty() || args.len() > 2 {
      abort_with_wrong_number_of_arguments(
        output,
        cluster_sub_name(RespCommand::ClusterMlogKeyTime),
      );
      return true;
    }
    let Some(aof) = self
      .cluster_provider
      .try_aof()
      .filter(|a| a.multi_log_enabled())
    else {
      output.write_resp_error(err::MULTI_LOG_DISABLED);
      return true;
    };
    let is_primary = self
      .cluster_manager()
      .is_some_and(|m| m.current_config().is_primary());
    let sequence_number = if is_primary {
      // 主端：序列号生成器最新值
      aof.get_sequence_number()
    } else {
      // 副本：键的重放序列号（C# GetBool(1) 解析失败按 false；无管理器 -1）
      let frontier = args
        .get(1)
        .and_then(|a| strict_i64(a))
        .is_some_and(|v| v != 0);
      aof
        .read_consistency_manager()
        .map_or(-1, |m| m.get_key_sequence_number(args[0], frontier))
    };
    output.write_resp_int(sequence_number);
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAppendLog
  ///
  /// 主端 AOF 记录帧接收（5-6 参：nodeId、子日志下标、三位地址、可选 AOF
  /// 页）：初始化帧回 +OK，记录帧无应答（C# 同口径）；处理失败（角色
  /// 不符 / divergent / 恢复中）不写应答行直接断流（C# 同场景
  /// `throw new GarnetException(..., clientResponse: false)` 上抛 →
  /// RespServerSession catch → DisposeNetworkSender，经
  /// [`ClusterSessionFace::take_fatal_disconnect`] 信号通道承接）
  fn network_cluster_appendlog(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() < 5 || args.len() > 6 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterAppendlog));
      return true;
    }
    let Some(session) = self.cluster_provider.try_replica_replication_session() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let (Some(idx), Some(previous), Some(current), Some(next)) = (
      strict_i64(args[1]),
      strict_i64(args[2]),
      strict_i64(args[3]),
      strict_i64(args[4]),
    ) else {
      // C# 解析失败仅记日志
      return true;
    };
    let node_id = String::from_utf8_lossy(args[0]);
    let payload = args.get(5).copied().unwrap_or(&[]);
    match session.process_append_log(
      &node_id,
      idx.max(0) as usize,
      previous,
      current,
      next,
      payload,
    ) {
      Ok(AppendLogOutcome::Initialized) => output.write_resp_simple_string("OK"),
      // C# 普通记录帧不回写
      Ok(AppendLogOutcome::Record) => {}
      // 致命断流：不给应答直接断（clientResponse:false 口径），主端经断链
      // 感知转入重同步，防 shipped_watermark 静默推进扩大主从分歧
      Err(e) => {
        let msg = e.to_string();
        log::error!("APPENDLOG 处理失败断流: {msg}");
        *self.fatal_disconnect.lock() = Some(msg);
      }
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterInitiateReplicaSync
  ///
  /// 主端同步发起（5 参：副本节点 id、指派主 id、检查点条目、副本 AOF
  /// 起止位点）：按上报元数据构造同步请求，策略协商 + 建连 + 补扫交
  /// 慢路径承载（C# BlockingWait 等价）；成功 +OK，失败回错误文案
  fn network_cluster_initiate_replica_sync(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() != 5 {
      abort_with_wrong_number_of_arguments(
        output,
        cluster_sub_name(RespCommand::ClusterInitiateReplicaSync),
      );
      return true;
    }
    let Some(assets) = self.cluster_provider.try_primary_replication() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let replica_node_id = String::from_utf8_lossy(args[0]).into_owned();
    let assigned_primary_id = String::from_utf8_lossy(args[1]).into_owned();
    let Some(checkpoint_entry) = CheckpointEntry::from_byte_array(args[2]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let replica_aof_begin = AofAddress::from_span(args[3]);
    let replica_aof_tail = AofAddress::from_span(args[4]);
    // 副本 endpoint 反查（C# TryBeginDiskbasedSyncAsync 内经配置定位副本）
    let Some(endpoint) = self.cluster_manager().and_then(|m| {
      m.current_config()
        .get_endpoint_from_node_id(&replica_node_id)
    }) else {
      output.write_resp_error(&format!("ERR I don't know about node {replica_node_id}."));
      return true;
    };
    let meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: replica_node_id,
      current_primary_repl_id: assigned_primary_id,
      current_store_version: 0,
      current_aof_begin_address: replica_aof_begin,
      current_aof_tail_address: replica_aof_tail,
      current_replication_offset: replica_aof_tail,
      checkpoint_entry: Some(checkpoint_entry),
    };
    let PrimaryReplicationAssets {
      wal,
      pump,
      sync_session,
    } = &*assets;
    // 本端节点 id（init 帧 node_id；副本侧据此校验 current primary）
    let local_node_id = self
      .cluster_manager()
      .and_then(|m| m.current_config().local_node_id().map(String::from))
      .unwrap_or_default();
    let endpoint = endpoint.to_string();
    let wal = Arc::clone(wal);
    let pump = Arc::clone(pump);
    let sync_session = Arc::clone(sync_session);
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match sync_session
        .initiate_replica_sync(&pump, &wal, &local_node_id, &endpoint, &meta, false)
        .await
      {
        Ok(_) => out.write_resp_simple_string("OK"),
        Err(msg) => out.write_resp_error(&msg),
      }
      out
    }));
    true
  }
}

impl ClusterSessionFace for ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:SetReadOnlySession
  fn set_read_only_session(&self) {
    self.read_only.store(true, Ordering::Relaxed);
  }

  /// libs/cluster/Session/ClusterSession.cs:SetReadWriteSession
  fn set_read_write_session(&self) {
    self.read_only.store(false, Ordering::Relaxed);
  }

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerify
  ///
  /// 依键规格提取键位做多键槽位校验；键规格未命中键（参数形态不含键）按
  /// C# default 结果放行。C# 的 CanOperateOnKey / WaitForSlotToStabalize 在
  /// 网络线程内联自旋，rust 侧不可同步判定时登记挂起等待体
  ///（[`SlotVerifyGate::Wait`]）交网络泵驱动，等待完成由消费循环重评本命令
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    let Some(cm) = self.cluster_manager() else {
      return SlotVerifyGate::Serve;
    };
    let extracted;
    let keys = if input.key_specs.is_empty() {
      args
    } else {
      extracted = extract_keys_from_slice(args, input.key_specs, input.is_sub_command);
      extracted.as_slice()
    };
    if keys.is_empty() {
      return SlotVerifyGate::Serve;
    }
    let session = self.slot_verify_session_state(input.session_asking > 0);
    // 等待体交接记忆：超时旗标 + 异步存在性裁决缓存（首评无记忆）
    let memo = self.slot_wait_memo.lock().take();
    match cm.evaluate_multi_key_gate(
      keys,
      input.read_only,
      session,
      input.wait_for_stable_slot,
      ClusterPreferredEndpointType::Ip,
      memo.as_deref(),
    ) {
      GateVerdict::Serve => SlotVerifyGate::Serve,
      GateVerdict::Redirect(state) => {
        // MOVED/ASK/CLUSTERDOWN/CROSSSLOT/TRYAGAIN 直写输出（C# WriteClusterSlotVerificationMessage）
        state.write_resp_error(output);
        SlotVerifyGate::Redirected
      }
      GateVerdict::Wait { .. } => {
        // 登记挂起等待体：轮询迁移推进与存活性裁决，超时置位记忆旗标；
        // 消费循环回退游标挂起，等待完成后重评本命令。等待体须 'static，
        // 持 ClusterManager 强引用自持
        let memo = memo.unwrap_or_else(|| Arc::new(SlotWaitMemo::new(keys.len())));
        let req = SlotVerifyRequest {
          keys: keys.iter().map(|k| k.to_vec()).collect(),
          read_only: input.read_only,
          session,
          wait_for_stable: input.wait_for_stable_slot,
          pref_type: ClusterPreferredEndpointType::Ip,
        };
        *self.slot_wait_memo.lock() = Some(Arc::clone(&memo));
        let waiter = Arc::clone(&cm);
        *self.pending_slow.lock() = Some(SlowWait::new(async move {
          waiter.wait_key_gate(req, memo).await;
          Vec::new()
        }));
        SlotVerifyGate::Wait
      }
    }
  }

  /// libs/cluster/Session/ClusterSession.cs:ProcessClusterCommands
  ///
  /// CLUSTER 子命令族（`cmd` 为解析器解析后的子命令枚举，对标 C# switch）
  fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    match cmd {
      RespCommand::ClusterNodes => {
        let info = self
          .cluster_manager()
          .map(|m| {
            m.current_config()
              .get_cluster_info(Some(&self.cluster_provider))
          })
          .unwrap_or_default();
        let mut buf = Buffer::new();
        output.push(b'$');
        output.extend_from_slice(buf.format(info.len()).as_bytes());
        output.extend_from_slice(b"\r\n");
        output.extend_from_slice(info.as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      RespCommand::ClusterKeyslot => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let slot = cluster_slot(args[0]);
        let mut buf = Buffer::new();
        output.push(b':');
        output.extend_from_slice(buf.format(slot).as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      RespCommand::ClusterMyid => {
        let mut buf = Buffer::new();
        if let Some(m) = self.cluster_manager() {
          let config = m.current_config();
          let myid = config.local_node_id().unwrap_or("");
          output.push(b'$');
          output.extend_from_slice(buf.format(myid.len()).as_bytes());
          output.extend_from_slice(b"\r\n");
          output.extend_from_slice(myid.as_bytes());
          output.extend_from_slice(b"\r\n");
        } else {
          output.extend_from_slice(b"$0\r\n\r\n");
        }
        true
      }
      RespCommand::ClusterSlots => {
        if let Some(m) = self.cluster_manager() {
          let info = m
            .current_config()
            .get_slots_info(ClusterPreferredEndpointType::Ip);
          output.extend_from_slice(info.as_bytes());
        }
        true
      }
      RespCommand::ClusterShards => {
        if let Some(m) = self.cluster_manager() {
          let info = m.current_config().get_shards_info(
            Some(&self.cluster_provider),
            ClusterPreferredEndpointType::Ip,
          );
          output.extend_from_slice(info.as_bytes());
        }
        true
      }
      RespCommand::ClusterInfo => {
        let info = self
          .cluster_manager()
          .map(|m| m.get_info())
          .unwrap_or_default();
        let mut buf = Buffer::new();
        output.push(b'$');
        output.extend_from_slice(buf.format(info.len()).as_bytes());
        output.extend_from_slice(b"\r\n");
        output.extend_from_slice(info.as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      RespCommand::ClusterBumpepoch => {
        if let Some(m) = self.cluster_manager() {
          if m.try_bump_cluster_epoch() {
            output.extend_from_slice(b"+BUMPED\r\n");
          } else {
            output.extend_from_slice(b"+STILL\r\n");
          }
        } else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterReset
      //
      // 同步段仅校验参数（0/1/2 参：SOFT|HARD + 可选过期秒数）；实际闭环
      // （HasKeysInSlots 槽键判定 → TryReset → HARD 清库）为异步域，挂
      // 慢路径执行体由网络泵驱动——对标 C# 网络线程内联 TryReset（含
      // ReleaseCurrentEpoch 纪元让渡）的整段语义
      RespCommand::ClusterReset => {
        if args.len() > 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        // C# soft = option.EqualsUpperCaseSpanIgnoringCase("SOFT")：仅显式
        // SOFT 为软重置，其余（含 HARD）均为硬重置
        let mut soft = true;
        if let Some(opt) = args.first()
          && !opt.eq_ignore_ascii_case(b"SOFT")
        {
          soft = false;
        }
        let mut expiry_secs: i64 = 60;
        if let Some(exp) = args.get(1) {
          match strict_i64(exp) {
            Some(v) => expiry_secs = v,
            None => {
              output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
              return true;
            }
          }
        }
        match (self.cluster_manager(), self.cluster_provider.try_store()) {
          (Some(m), Some(store)) => {
            let slots: Vec<u16> = m
              .current_config()
              .get_slot_list(LOCAL_WORKER_ID as u16)
              .into_iter()
              .map(|s| s as u16)
              .collect();
            *self.pending_slow.lock() = Some(SlowWait::new(async move {
              cluster_reset_slow(m, store, slots, soft, expiry_secs).await
            }));
          }
          // 集群管理器或存储未装配：明确报错，绝不静默吞命令
          _ => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
        }
        true
      }
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterAddSlots
      RespCommand::ClusterAddslots | RespCommand::ClusterAddslotsrange => {
        let range = cmd == RespCommand::ClusterAddslotsrange;
        // C# 形态校验：单槽 ≥1 参；区间形态偶数参
        let valid = if range {
          !args.is_empty() && args.len().is_multiple_of(2)
        } else {
          !args.is_empty()
        };
        if !valid {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        match Self::try_parse_slots(args, range) {
          Err((msg, slot)) => {
            if msg == "duplicate" {
              let mut buf = Buffer::new();
              output.write_resp_error(&format!(
                "ERR Slot {} specified multiple times",
                buf.format(slot)
              ));
            } else {
              output.write_resp_error(msg);
            }
          }
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
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelSlots
      RespCommand::ClusterDelslots | RespCommand::ClusterDelslotsrange => {
        let range = cmd == RespCommand::ClusterDelslotsrange;
        let valid = if range {
          !args.is_empty() && args.len().is_multiple_of(2)
        } else {
          !args.is_empty()
        };
        if !valid {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        match Self::try_parse_slots(args, range) {
          Err((msg, slot)) => {
            if msg == "duplicate" {
              let mut buf = Buffer::new();
              output.write_resp_error(&format!(
                "ERR Slot {} specified multiple times",
                buf.format(slot)
              ));
            } else {
              output.write_resp_error(msg);
            }
          }
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
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlot
      RespCommand::ClusterSetslot => {
        if args.len() < 2 || args.len() > 3 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(slot) = strict_i64(args[0]) else {
          output.write_resp_error(err::INVALID_SLOT);
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
        let node_id = args.get(2).map(|a| a.as_str_safe());
        // C# 语法约束：STABLE 不带 node-id，其余状态必须带
        if (slot_state == SlotState::Stable) == node_id.is_some() {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        }
        if ClusterConfig::out_of_range(slot.max(0) as usize) {
          output.write_resp_error(err::SLOT_OUT_OF_RANGE);
          return true;
        }
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        let node_id_str = node_id.unwrap_or_default();
        let result = match slot_state {
          SlotState::Stable => {
            m.try_reset_slot_state(slot as usize);
            Ok(())
          }
          SlotState::Importing => m.try_prepare_slot_for_import(slot as usize, node_id_str),
          SlotState::Migrating => m.try_prepare_slot_for_migration(slot as usize, node_id_str),
          SlotState::Node => m.try_prepare_slot_for_ownership_change(slot as usize, node_id_str),
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
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlotsRange
      RespCommand::ClusterSetslotsrange => {
        if args.len() < 3 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let slot_state = match parse_slot_state(args[0]) {
          Some(s) if !matches!(s, SlotState::Invalid | SlotState::Offline | SlotState::Fail) => s,
          _ => {
            output.write_resp_error(err::SLOT_STATE);
            return true;
          }
        };
        let stable = slot_state == SlotState::Stable;
        let node_id_str = if stable { "" } else { args[1].as_str_safe() };
        match Self::try_parse_slots(&args[if stable { 1 } else { 2 }..], true) {
          Err((msg, slot)) => {
            if msg == "duplicate" {
              let mut buf = Buffer::new();
              output.write_resp_error(&format!(
                "ERR Slot {} specified multiple times",
                buf.format(slot)
              ));
            } else {
              output.write_resp_error(msg);
            }
          }
          Ok(slots) => {
            let Some(m) = self.cluster_manager() else {
              output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
              return true;
            };
            let result = if stable {
              m.try_reset_slots_state(&slots);
              Ok(())
            } else {
              match slot_state {
                SlotState::Importing => m.try_prepare_slots_for_import(&slots, node_id_str),
                SlotState::Migrating => m.try_prepare_slots_for_migration(&slots, node_id_str),
                SlotState::Node => m.try_prepare_slots_for_ownership_change(&slots, node_id_str),
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
      // libs/cluster/Session/ClusterCommands.cs:NetworkClusterCountKeysInSlot
      RespCommand::ClusterCountkeysinslot => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(slot) = strict_i64(args[0]) else {
          output.write_resp_error(err::INVALID_SLOT);
          return true;
        };
        if ClusterConfig::out_of_range(slot.max(0) as usize) {
          output.write_resp_error(err::SLOT_OUT_OF_RANGE);
          return true;
        }
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
            *self.pending_slow.lock() = Some(SlowWait::new(async move {
              count_keys_in_slot_slow(store, slot).await
            }));
          }
          None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
        }
        true
      }
      // libs/cluster/Session/ClusterCommands.cs:NetworkClusterGetKeysInSlot
      RespCommand::ClusterGetkeysinslot => {
        if args.len() != 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(slot) = strict_i64(args[0]) else {
          output.write_resp_error(err::INVALID_SLOT);
          return true;
        };
        let Some(key_count) = strict_i64(args[1]) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        if ClusterConfig::out_of_range(slot.max(0) as usize) {
          output.write_resp_error(err::SLOT_OUT_OF_RANGE);
          return true;
        }
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
            *self.pending_slow.lock() = Some(SlowWait::new(async move {
              get_keys_in_slot_slow(store, slot, key_count).await
            }));
          }
          None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
        }
        true
      }
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterDelKeysInSlot
      RespCommand::ClusterDelkeysinslot | RespCommand::ClusterDelkeysinslotrange => {
        let range = cmd == RespCommand::ClusterDelkeysinslotrange;
        if range && (args.is_empty() || !args.len().is_multiple_of(2)) {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        if !range && args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let parsed = if range {
          Self::try_parse_slots(args, true)
        } else {
          match strict_i64(args[0]) {
            Some(slot) => Ok(GxHashSet::from_iter([slot.max(0) as usize])),
            None => Err((err::INVALID_SLOT, 0)),
          }
        };
        let Ok(slots) = parsed else {
          output.write_resp_error(err::INVALID_SLOT);
          return true;
        };
        let slots: Vec<u16> = slots.into_iter().map(|s| s as u16).collect();
        match self.cluster_provider.try_store() {
          Some(store) => {
            *self.pending_slow.lock() = Some(SlowWait::new(async move {
              del_keys_in_slots_slow(store, slots).await
            }));
          }
          None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
        }
        true
      }
      // libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSlotState
      RespCommand::ClusterSlotstate => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(slot) = strict_i64(args[0]) else {
          output.write_resp_error(err::INVALID_SLOT);
          return true;
        };
        if ClusterConfig::out_of_range(slot.max(0) as usize) {
          output.write_resp_error(err::SLOT_OUT_OF_RANGE);
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
        let owner = m
          .current_config()
          .get_owner_id_from_slot(slot)
          .unwrap_or_default();
        let mut buf = Buffer::new();
        output.push(b'+');
        output.extend_from_slice(buf.format(slot).as_bytes());
        output.push(b' ');
        output.extend_from_slice(state_str.as_bytes());
        output.push(b' ');
        output.extend_from_slice(owner.as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMeet
      RespCommand::ClusterMeet => {
        if args.len() != 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(port) = strict_i64(args[1]) else {
          output.write_resp_error(&format!(
            "ERR Invalid port '{}' specified. Please make sure the port is a valid number",
            String::from_utf8_lossy(args[1])
          ));
          return true;
        };
        let ip = String::from_utf8_lossy(args[0]).into_owned();
        if let Some(gm) = self.cluster_provider.gossip_manager() {
          spawn(async move {
            if let Err(e) = gm.try_meet_async(&ip, port as i32).await {
              log::warn!("CLUSTER MEET {ip}:{port} failed: {e}");
            }
          })
          .detach();
        }
        output.write_resp_simple_string("OK");
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterForget
      RespCommand::ClusterForget => {
        if args.is_empty() || args.len() > 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let mut expiry_seconds: i64 = 60;
        if let Some(exp) = args.get(1) {
          match strict_i64(exp) {
            Some(v) => expiry_seconds = v,
            None => {
              output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
              return true;
            }
          }
        }
        let node_id = String::from_utf8_lossy(args[0]);
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        match m.try_remove_worker(&node_id, expiry_seconds.max(0) as u64) {
          Ok(()) => {
            // C# 同步摘除该节点的在途迁移任务
            if let Some(mm) = self.cluster_provider.migration_manager() {
              mm.try_remove_migration_task_node(&node_id);
            }
            output.write_resp_simple_string("OK");
          }
          Err(Error::CannotForgetMyself) => output.write_resp_error(err::CANNOT_FORGET_MYSELF),
          Err(Error::CannotForgetPrimary) => output.write_resp_error(err::CANNOT_FORGET_MY_PRIMARY),
          Err(Error::NodeNotFound(id)) => {
            output.write_resp_error(&format!("ERR I don't know about node {id}."))
          }
          Err(e) => output.write_resp_error(&e.to_string()),
        }
        true
      }
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReplicas
      RespCommand::ClusterReplicas => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let node_id = String::from_utf8_lossy(args[0]);
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        // C# ListReplicas：对 nodeid 的每个副本输出 CLUSTER NODES 格式行
        let replicas: Vec<String> = {
          let config = m.current_config();
          config
            .get_replica_ids(&node_id)
            .iter()
            .map(|rid| {
              let worker_id = config.get_worker_id_from_node_id(rid);
              let info = m.get_connection_info(rid);
              config.get_node_info(worker_id.into(), &info)
            })
            .collect()
        };
        output.write_resp_array_len(replicas.len());
        for item in &replicas {
          output.write_resp_bulk_string(item.as_bytes());
        }
        true
      }
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReplicate
      RespCommand::ClusterReplicate => {
        if args.is_empty() || args.len() > 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        if let Some(flag) = args.get(1)
          && !flag.eq_ignore_ascii_case(b"SYNC")
          && !flag.eq_ignore_ascii_case(b"ASYNC")
        {
          output.write_resp_error(&format!(
            "ERR Invalid CLUSTER REPLICATE FLAG ({}) not valid",
            String::from_utf8_lossy(flag)
          ));
          return true;
        }
        // 同步发起面（C# TryReplicateDiskbasedSyncAsync 的配置前段）：配置翻转
        // 由 try_add_replica_async 承接（begin_recovery(ClusterReplicate) +
        // make_replica_of + flush）；数据面 attach 依赖装配期重连钩子
        let node_id = String::from_utf8_lossy(args[0]).into_owned();
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        *self.pending_slow.lock() = Some(SlowWait::new(async move {
          let mut out = Vec::new();
          match m.try_add_replica_async(&node_id, true, false).await {
            Ok(()) => out.write_resp_simple_string("OK"),
            Err(Error::CannotAcquireRecoveryLock) => {
              out.write_resp_error("ERR Recovery in progress, could not acquire recoverLock")
            }
            Err(e) => out.write_resp_error(&replicate_err_text(e)),
          }
          out
        }));
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterSetConfigEpoch
      RespCommand::ClusterSetconfigepoch => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let Some(config_epoch) = strict_i64(args[0]) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        let Some(m) = self.cluster_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        if m.current_config().num_workers() > 1 {
          output.write_resp_error(err::CONFIG_EPOCH_ASSIGNMENT);
          return true;
        }
        match m.try_set_local_config_epoch(config_epoch) {
          Ok(()) => output.write_resp_simple_string("OK"),
          // C# RESP_ERR_GENERIC_CONFIG_EPOCH_NOT_SET
          Err(_) => output
            .write_resp_error("ERR Node config epoch was not set due to invalid epoch specified"),
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterEndpoint
      RespCommand::ClusterEndpoint => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        const UNASSIGNED_ENDPOINT: &[u8] = b"unassigned:0";
        let node_id = String::from_utf8_lossy(args[0]);
        let endpoint = self
          .cluster_manager()
          .and_then(|m| m.current_config().get_endpoint_from_node_id(&node_id));
        // C# 未知节点回落占位 worker（'unassigned:0'）
        if let Some(endpoint) = endpoint {
          output.write_resp_bulk_string(endpoint.to_string().as_bytes());
        } else {
          output.write_resp_bulk_string(UNASSIGNED_ENDPOINT);
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterHelp
      RespCommand::ClusterHelp => {
        if !args.is_empty() {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        output.write_resp_array_len(CLUSTER_HELP.len());
        for line in CLUSTER_HELP {
          output.write_resp_simple_string(line);
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterBanList
      RespCommand::ClusterBanlist => {
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
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMyParentId
      RespCommand::ClusterMyparentid => {
        if !args.is_empty() {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let parent = self.cluster_manager().map(|m| {
          let config = m.current_config();
          if config.is_primary() {
            config.local_node_id().unwrap_or_default().to_string()
          } else {
            config
              .local_node_primary_id()
              .unwrap_or_default()
              .to_string()
          }
        });
        match parent {
          Some(id) => output.write_resp_bulk_string(id.as_bytes()),
          None => output.write_resp_bulk_string(b""),
        }
        true
      }
      // libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate
      RespCommand::ClusterMigrate => {
        if args.len() < 3 || args.len() > 4 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let replace_arg = args[1];
        let replace = replace_arg.eq_ignore_ascii_case(b"T") || replace_arg == b"1";
        let payload = if args.len() == 4 {
          args[3].to_vec()
        } else {
          args[2].to_vec()
        };

        let Some(store) = self.cluster_provider.try_store() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        let provider = self.cluster_provider.clone();
        *self.pending_slow.lock() = Some(SlowWait::new(async move {
          cluster_migrate_slow(provider, store, replace, payload).await
        }));
        true
      }
      // libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMTasks
      RespCommand::ClusterMtasks => {
        if !args.is_empty() {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let mtasks = self
          .cluster_provider
          .migration_manager()
          .map(|mm| mm.get_migration_task_count())
          .unwrap_or(0);
        let mut buf = Buffer::new();
        output.push(b':');
        output.extend_from_slice(buf.format(mtasks).as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      // libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailover
      RespCommand::ClusterFailover => self.network_cluster_failover(args, output),
      // libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailStopWrites
      RespCommand::ClusterFailstopwrites => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let node_id = String::from_utf8_lossy(args[0]);
        if let Some(m) = self.cluster_manager() {
          if !node_id.is_empty() {
            // 副本发起接管请求：主节点停写并向该副本让渡属主
            m.try_stop_writes(&node_id);
          } else {
            m.try_reset_replica();
          }
        }
        // C# RespClusterFailoverCommands.cs:128 BlockingWait(UnsafeBumpAndWait...)
        self.unsafe_bump_and_wait_for_epoch_transition();
        let offset = self
          .cluster_provider
          .replication_manager()
          .map(|rm| rm.get_current_replication_offset().to_aof_string())
          .unwrap_or_default();
        output.write_resp_bulk_string(offset.as_bytes());
        true
      }
      // libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailReplicationOffset
      RespCommand::ClusterFailreplicationoffset => {
        if args.len() != 1 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        let primary_offset = AofAddress::from_span(args[0]);
        let Some(rm) = self.cluster_provider.replication_manager() else {
          output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
          return true;
        };
        *self.pending_slow.lock() = Some(SlowWait::new(async move {
          let mut out = Vec::new();
          let _ = rm
            .wait_for_replication_offset_async(&primary_offset, Duration::from_secs(10))
            .await;
          let r_offset = rm.get_current_replication_offset();
          out.write_resp_bulk_string(r_offset.to_aof_string().as_bytes());
          out
        }));
        true
      }
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterFlushAll
      RespCommand::ClusterFlushall => {
        if !args.is_empty() {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        match self.cluster_provider.try_store() {
          Some(store) => {
            *self.pending_slow.lock() = Some(SlowWait::new(cluster_flush_all_slow(store)));
          }
          None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip
      RespCommand::ClusterGossip => self.network_cluster_gossip(args, output),
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterPublish
      RespCommand::ClusterPublish | RespCommand::ClusterSpublish => {
        if args.len() != 2 {
          abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
          return true;
        }
        if let Some(m) = self.cluster_manager() {
          let channel = args[0].to_vec();
          let message = args[1].to_vec();
          spawn(async move { m.try_cluster_publish_async(cmd, &channel, &message).await }).detach();
        }
        // C# 无应答写出
        true
      }
      // libs/cluster/Session/ReplicaOfCommand.cs:NetworkTryREPLICAOF（顶层 REPLICAOF/SECONDARYOF）
      RespCommand::Replicaof | RespCommand::Secondaryof => {
        self.network_replicaof(cmd, args, output)
      }
      // libs/cluster/Session/FailoverCommand.cs:TryFAILOVER（顶层 FAILOVER）
      RespCommand::Failover => self.network_failover(args, output),
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReserve
      RespCommand::ClusterReserve => self.network_cluster_reserve(args, output),
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAdvanceTime
      RespCommand::ClusterAdvanceTime => self.network_cluster_advance_time(args, output),
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterMlogKeyTime
      RespCommand::ClusterMlogKeyTime => self.network_cluster_mlog_key_time(args, output),
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAppendLog
      RespCommand::ClusterAppendlog => self.network_cluster_appendlog(args, output),
      // libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterInitiateReplicaSync
      RespCommand::ClusterInitiateReplicaSync => {
        self.network_cluster_initiate_replica_sync(args, output)
      }
      _ => {
        output.extend_from_slice(b"-ERR unknown subcommand or not implemented for 'CLUSTER'\r\n");
        true
      }
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:IsPrimary
  fn is_primary(&self) -> bool {
    IClusterProvider::is_primary(&*self.cluster_provider)
  }

  /// 副本判定（委派 ClusterProvider 实现）
  fn is_replica(&self) -> bool {
    IClusterProvider::is_replica(&*self.cluster_provider)
  }

  /// 内部写会话判定（AOF 回放 / 复制推流 / 迁移）
  ///
  /// libs/cluster/Session/ClusterSession.cs:IsInternalWriteSession
  fn is_internal_write_session(&self) -> bool {
    self.internal_write()
  }

  /// 批次级纪元快照读取（0 = 批外空闲）
  ///
  /// libs/cluster/Session/ClusterSession.cs:LocalCurrentEpoch
  fn local_current_epoch(&self) -> i64 {
    self.local_current_epoch.load(Ordering::Acquire)
  }

  /// 消费批首快照 provider 当前纪元
  ///
  /// libs/cluster/Session/ClusterSession.cs:AcquireCurrentEpoch
  fn acquire_current_epoch(&self) {
    self
      .local_current_epoch
      .store(self.cluster_provider.current_epoch(), Ordering::Release);
  }

  /// 消费批尾清零快照
  ///
  /// libs/cluster/Session/ClusterSession.cs:ReleaseCurrentEpoch
  fn release_current_epoch(&self) {
    self.local_current_epoch.store(0, Ordering::Release);
  }

  /// 主节点复制信息（委派 ClusterProvider 实现）
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    IClusterProvider::get_primary_info(&*self.cluster_provider)
  }

  /// 副本自身角色信息（委派 ClusterProvider 实现）
  fn get_replica_info(&self) -> RoleInfo {
    IClusterProvider::get_replica_info(&*self.cluster_provider)
  }

  /// ROLE 命令 usingShardedLog 判定输入（C# serverOptions.AofPhysicalSublogCount，
  /// 经 ReplicationManager sublog 计数）
  fn aof_sublog_count(&self) -> usize {
    self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.sublog_count())
      .unwrap_or(1)
  }

  /// libs/cluster/Session/ClusterSession.cs:Dispose
  ///
  /// 会话不持网络发送器与存储上下文（并行域各自管理生命周期），无清理动作
  fn dispose(&self) {}

  /// 取走 CLUSTER RESET 等挂起的慢路径执行体（会话主循环转挂网络泵驱动）
  fn take_pending_slow(&self) -> Option<SlowWait> {
    self.pending_slow.lock().take()
  }

  /// 取走致命断流登记（APPENDLOG 拒收等 clientResponse:false 场景），
  /// 会话侧转致命哨兵：不写错误应答行，发尽累积应答后断连
  fn take_fatal_disconnect(&self) -> Option<String> {
    self.fatal_disconnect.lock().take()
  }

  /// DEBUG PURGEBP 集群侧缓冲池清洗（转发 ClusterProvider 实现）
  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    IClusterProvider::purge_buffer_pool(&*self.cluster_provider, manager_type);
  }
}

/// CLUSTER RESET 慢路径执行段（对标 C# `TryReset` + `FlushDB(true)` 整链）
///
/// 1. HasKeysInSlots 槽键判定（C# TryReset 首段）：本节点持有槽上仍有键
///    即拒绝（保守口径：不过滤墓碑与到期键）；
/// 2. `try_reset`：SuspendConfigMerge → 复位恢复态 → 关闭全部集群连接 →
///    新配置（SOFT 保留 nodeId/epoch，HARD 换新 id 且 epoch 归零）；
/// 3. HARD 清库（C# `!soft → clusterProvider.FlushDB(true)`）：删除全部
///    用户键（对标 DatabaseManagerBase.ResetDatabase 清库段；本装配无
///    AOF 实例，无截断动作）。
///
/// 返回完整 RESP 应答字节
async fn cluster_reset_slow(
  manager: Arc<ClusterManager>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  slots: Vec<u16>,
  soft: bool,
  expiry_secs: i64,
) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  {
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    // 槽键判定（libs/cluster/Server/ClusterManagerWorkerState.cs:TryReset 首段）
    match storage.has_keys_in_slots(&slots).await {
      Ok(true) => {
        out.write_resp_error("ERR CLUSTER RESET can't be called with master nodes containing keys");
        return out;
      }
      Err(_) => {
        out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
        return out;
      }
      Ok(false) => {}
    }
    // HARD 清库（C# FlushDB(true)）：删除全部用户键（String/Meta 及其旁路
    // 子键随版本栅栏逻辑失效；与 FLUSHDB 慢路径同一清库入口
    // `StorageSession::delete_all_user_keys`——Meta 键走版本栅栏 + 树文件
    // 排空的完整异步删除，杜绝 try_delete_sync 对复合对象的静默降级丢失）
    if !soft && storage.delete_all_user_keys().await.is_err() {
      out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
      return out;
    }
  }
  match manager.try_reset(soft, expiry_secs.max(0) as u64) {
    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
    Err(_) => out.write_resp_error("ERR Cluster reset failed"),
  }
  out
}

/// 集群管理器或存储域未装配的统一拒绝文案
const ERR_CLUSTER_NOT_INITIALIZED: &str = "ERR Cluster not initialized";
/// 恢复锁占用（C# RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK）
const ERR_RECOVERY_LOCK: &str = "ERR Recovery in progress, could not acquire recoverLock";

/// libs/cluster/Session/ClusterCommandInfo.cs:GetClusterCommands
const CLUSTER_HELP: [&str; 64] = [
  "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
  "ADDSLOTS <slot> [<slot> ...]",
  "\tAssign slots to current node.",
  "ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tAssign slot ranges to current node.",
  "BUMPEPOCH",
  "\tAdvance the cluster config epoch.",
  "BANLIST",
  "\t Return banlist of nodeids",
  "COUNTKEYSINSLOT <slot>",
  "\tReturn the number of keys in <slot>.",
  "DELSLOTS <slot> [<slot> ...]",
  "\tDelete slots information from current node.",
  "DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tDelete slot ranges information from current node.",
  "DELKEYSINSLOT slot",
  "\tScan the DB and delete keys mapping to corresponding slot.",
  "DELKEYSINSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tScan the DB and delete keys mapping to corresponding slot ranges.",
  "FAILOVER [FORCE | TAKEOVER]",
  "\tSend only to replica, forces the replica to start a manual failover of its master instance",
  "FORGET <node-id> [ban node for seconds = default(60)]",
  "\tRemove a node from the cluster.",
  "GETKEYSINSLOT <slot> <count>",
  "\tGETKEYSINSLOT <slot> <count>",
  "INFO",
  "\tReturn information about the cluster.",
  "KEYSLOT",
  "\tReturn the SLOT a provided KEY is mapped to.",
  "MEET <ip> <port> [<bus-port>]",
  "\tConnect nodes into a working cluster.",
  "MTASKS",
  "\tReturn number of outstanding migration tasks.",
  "MYID",
  "\tReturn the node id.",
  "MYPARENTID",
  "\tReturn primary id or own id if instance not a replica",
  "ENDPOINT",
  "\tEndpoint <nodeid>",
  "\tReturn 'ip:port' for nodeid. 'unassigned:0' if nodeid is not known",
  "NODES",
  "\tReturn cluster configuration seen by node. Output format:",
  "\t<id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
  "REPLICATE <node-id>",
  "\tConfigure current node as replica to <node-id>.",
  "REPLICAS <node-id>",
  "\tReturn <node-id> replicas.",
  "RESET [HARD|SOFT]",
  "\tReset node configuration (default:SOFT). Default SOFT option resets configuration by forgetting slot mapping and nodes. HARD resets config epoch and generates new nodeid and flushes DB data.",
  "SET-CONFIG-EPOCH <epoch>",
  "\tSet config epoch of current node.",
  "SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE <node-id>)",
  "\tSet slot state.",
  "SETSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tSet state of slots in range.",
  "SLOTS",
  "\tReturn information about slots range mappings. Each range is made of:",
  "SLOTSTATE",
  "\tReturn information about slot state",
  "\tstart, end, master and replicas IP addresses, ports and ids",
  "SHARDS",
  "\tReturns details about the shards of the cluster. A shard is defined as a collection of nodes that serve the same set of slots and that replicate from each other",
  "HELP",
  "\tPrints this help.",
];

/// 默认集群命令族回退名称
pub const DEFAULT_CLUSTER_CMD_NAME: &str = "cluster";

/// 子命令 RESP 名（错误文案回显用，对标 C# `ClusterSession.cs:119-121` 与 `RespCommandsInfo.GetRespCommandName(command).ToLowerInvariant()`）
#[inline]
pub fn cluster_sub_name(cmd: RespCommand) -> &'static str {
  try_get_resp_command_info(cmd).map_or(DEFAULT_CLUSTER_CMD_NAME, |e| e.name)
}

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

/// 副本接入错误 → C# TryAddReplica 错误文案
fn replicate_err_text(e: Error) -> String {
  use Error as E;
  match e {
    E::MigrateToMyself => "ERR Can't replicate myself".to_string(),
    E::NodeNotFound(id) => format!("ERR I don't know about node {id}"),
    E::TargetNotPrimary(id) => format!("ERR Target node {id} is not a master node."),
    E::SlotAlreadyScheduled(_) => {
      "ERR Primary has been assigned slots and cannot be a replica".to_string()
    }
    other => other.to_string(),
  }
}

/// COUNTKEYSINSLOT 慢路径（C# CountKeysInSlot 槽键计数）
async fn count_keys_in_slot_slow(
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  slot: u16,
) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match storage.count_keys_in_slot(slot).await {
    Ok(n) => {
      let mut buf = Buffer::new();
      out.push(b':');
      out.extend_from_slice(buf.format(n).as_bytes());
      out.extend_from_slice(b"\r\n");
    }
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// GETKEYSINSLOT 慢路径（C# GetKeysInSlot 槽键列举）
async fn get_keys_in_slot_slow(
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  slot: u16,
  key_count: usize,
) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match storage.get_keys_in_slot(slot, key_count).await {
    Ok(keys) => {
      let rendered: Vec<String> = keys
        .into_iter()
        .map(|k| String::from_utf8_lossy(&k).into_owned())
        .collect();
      out.write_resp_array_len(rendered.len());
      for item in &rendered {
        out.write_resp_bulk_string(item.as_bytes());
      }
    }
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// CLUSTER DELKEYSINSLOT(RANGE) 慢路径（C# DeleteKeysInSlots）
async fn del_keys_in_slots_slow(
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  slots: Vec<u16>,
) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match storage.delete_slot_keys(&slots).await {
    Ok(_) => out.write_resp_simple_string("OK"),
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// CLUSTER FLUSHALL 慢路径（C# FlushAllDatabases 清库段，复用 CLUSTER RESET
/// 同一清库入口 `delete_all_user_keys`）
async fn cluster_flush_all_slow(store: Arc<WedbStore<wdev::SegmentedDevice>>) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match storage.delete_all_user_keys().await {
    Ok(_) => out.write_resp_simple_string("OK"),
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// CLUSTER MIGRATE 慢路径 (libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate)
///
/// 限制（显式裁剪，禁止静默丢键）：迁移帧仅支持 string 记录 (kind=1)，
/// 非法 kind 显式报错拒绝，绝不把未知记录类型静默当 string 写入。对象/
/// 大值 chunk 迁移属未实现，待 M 系列帧扩展立项补全。
///
/// REPLACE 语义（对标 C# `replaceOption || !Exists(keySlice)`，Exists 为
/// 双存储域通用存在性）：存在性探测用 `contains_key`（集合 Meta 元记录 +
/// String 域 + KeyTag::ObjectEnvelope 对象信封域 + TTL 惰性过期裁决）——
/// - replace=false 且目标键已存在为对象记录：跳过写入，目标端保留原对象
///   （源端随后删自己的键，数据不消失）；原实现仅探 String 域会误判对象键
///   「不存在」进而清退对象、string 覆写，违反 REPLACE 语义，已修正；
/// - replace=true：经 `upsert_string` 覆盖写，String 域写入自带对象信封
///   覆写清退（wkv `try_upsert_tag_sync_unprotected`），对象键被 string
///   安全覆盖，旧对象完整释放，语义可接受。
async fn cluster_migrate_slow(
  provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  replace: bool,
  payload: Vec<u8>,
) -> Vec<u8> {
  let mut out = Vec::new();
  let (record_count, records) = match parse_migration_payload(&payload) {
    Ok(res) => res,
    Err(e) => {
      out.write_resp_error(&format!("ERR Invalid migration payload: {e:?}"));
      return out;
    }
  };
  if record_count == 0 {
    out.write_resp_simple_string("OK");
    return out;
  }

  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  let cluster_mgr = provider.cluster_manager();
  if let Some(cm) = &cluster_mgr {
    let cfg = cm.current_config.read();
    for record in &records {
      let slot = cluster_slot(record.key);
      if !cfg.is_importing_slot(slot) {
        out.write_resp_error(&format!("ERR Slot {slot} is not in importing state"));
        return out;
      }
    }
  }

  for record in records {
    // 帧类型显式校验：本接收端仅实现 kind=1 (string 记录)，未知类型
    // 显式拒绝，杜绝未来帧扩展后旧接收端静默误读为 string
    if record.kind != MIGRATION_RECORD_KIND_STRING {
      out.write_resp_error(&format!(
        "ERR Unsupported migration record kind {}",
        record.kind
      ));
      return out;
    }

    // REPLACE 判定（对标 C# replaceOption || !Exists）：contains_key 双域
    // 存在性——对象记录键在 replace=false 下跳过写入（目标端保留原对象）；
    // 探测失败保守写入（与原 read_string 失败口径一致）
    let should_write = replace
      || match storage.batch.contains_key(record.key).await {
        Ok(false) => true,
        Ok(true) => false,
        Err(_) => true,
      };

    if should_write {
      if let Err(err) = storage.upsert_string(record.key, record.val).await {
        log::error!("CLUSTER MIGRATE upsert_string failed: {err:?}");
        out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
        return out;
      }
      if record.expire_unix_ms > 0 {
        let expire_ticks = record.expire_unix_ms * TICKS_PER_MILLISECOND + UNIX_EPOCH_TICKS;
        let _ = storage.expire_at_ticks(record.key, expire_ticks).await;
      }
    }
  }

  out.write_resp_simple_string("OK");
  out
}
