//! 集群复制命令实现（对标 libs/cluster/Session/RespClusterReplicationCommands.cs）

use std::{
  future::Future,
  sync::{Arc, atomic::Ordering},
};

use async_lock::Mutex as AsyncLockMutex;
use itoa::Buffer;
use parking_lot::Mutex;
use waof::{AofAddress, AofEntryType};
use wbase::{
  hash_slot::slot_of,
  hex::{hex_str_u128, hex_u128},
  num::{strict_i32, strict_i64, strict_u64},
};
use wconn::record::parse_migration_payload;
use wkv::WedbStore;
use wnode::{
  StorageSession, range_index::RangeIndexMigrationReceiveState, resp::slow_path::SlowWait,
};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_SLOW_PATH_STORAGE, abort_with_wrong_number_of_arguments,
    cluster::{
      ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ERR_GENERIC_VALUE_IS_NOT_BOOLEAN,
      ERR_MULTI_LOG_DISABLED, ERR_UNKNOWN_NODE_PREFIX,
    },
    write_error_raw,
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::{ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, reject_wrong_arity};
use crate::{
  error::Error,
  server::{
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    migration::{
      chunk_reassembler::ChunkReassembler,
      frame_import::{FrameImport, import_migration_frames},
    },
    replication::{
      assembly::try_replicate_sync_async,
      checkpoint_entry::{CheckpointEntry, CheckpointFileType},
      cluster_replication_session::AppendLogOutcome,
      receive_checkpoint_handler::CheckpointImportCtx,
      replica_diskbased_sync::{ReplicaRecoverRequest, try_replica_diskbased_recovery},
      replica_diskless_sync::{try_begin_diskless_sync_async, try_replica_diskless_recovery},
      replica_sync_task_store::ReplicaSyncSessionTaskStore,
      replicate_sync_options::ReplicateSyncOptions,
      replication_manager::ReplicationManager,
      sync_metadata::SyncMetadata,
    },
    worker::NodeRole,
  },
};

/// 检查点传输流 token 解析（C# `new Guid(fileTokenBytes)` 的 16B 裸字节
/// 形态；rust token 为 u128 LE）
pub(super) fn parse_checkpoint_token(arg: &[u8]) -> Option<u128> {
  let bytes: [u8; 16] = arg.try_into().ok()?;
  Some(u128::from_le_bytes(bytes))
}

/// 在册磁盘同步任务摘除守卫（C# PrimarySync.cs:104 后台体 finally
/// TryRemove 的 Drop 承接）：慢路径终局或会话断链 future 取消两路都摘
/// 登记；摘除失败按 C# 口径记错误日志
struct SyncTaskGuard<'a> {
  store: &'a ReplicaSyncSessionTaskStore,
  node: u128,
}

impl Drop for SyncTaskGuard<'_> {
  fn drop(&mut self) {
    if !self.store.try_remove(self.node) {
      log::error!(
        "Unable to remove replica sync session for remote node {}",
        hex_str_u128(self.node)
      );
    }
  }
}

/// 会话侧副本同步发起慢路径（对标 C# ReplicaOfCommand.cs:90-93 与
/// RespClusterReplicationCommands.cs:104-106 的
/// `BlockingWait(ReplicaDisklessSync ? TryReplicateDisklessSyncAsync :
/// TryReplicateDiskbasedSyncAsync)`：rust 网络线程不阻塞，同一发起经
/// `pending_slow` 慢路径承接并回写 OK / 错误应答。
///
/// 本处不读开关——选路交唯一选路口 [`try_replicate_sync_async`]：diskless 支
/// 向主端发 CLUSTER ATTACH_SYNC，diskbased 支向主端发
/// CLUSTER INITIATE_REPLICA_SYNC；两支都在登记副本后当场发起一次 attach，
/// 失败即回 -ERR 文案（C# 同口径，绝不先回 OK）
pub(super) fn queue_try_replicate_sync(
  m: Arc<ClusterManager>,
  opts: ReplicateSyncOptions,
) -> SlowWait {
  SlowWait::new(async move {
    let mut out = Vec::new();
    match try_replicate_sync_async(&m.cluster_provider, opts).await {
      Ok(()) => out.write_resp_simple_string("OK"),
      Err(msg) => out.write_resp_error(&msg),
    }
    out
  })
}

/// NetworkClusterFlushAll 的慢路径执行体（同一函数的异步续段）
///
/// （`storeWrapper.FlushAllDatabases(unsafeTruncateLog: false)` 清全部库）：
/// 经 wkv `flush_all_databases` 物理截断 O(1) 清空全部租户全部库并重置虚拟
/// 映射——绝不走默认会话的 delete_all_user_keys（那是 (ns=0, db=0) 单库
/// 语义，多租户多库全残留）；主库随后入队 FlushAll 广播条目，副本经回放
/// 条目承接清库（C# IsPrimary 门控同形），从库不再二次入队
async fn cluster_flush_all_slow(provider: Arc<ClusterProvider>) -> Vec<u8> {
  let mut out = Vec::new();
  if provider.is_device_contaminated() {
    out.write_resp_error(
      "ERR device is contaminated by a failed checkpoint receive, refusing flush",
    );
    return out;
  }
  let Some(store) = provider.try_store() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  match store.flush_all_databases().await {
    Ok(_) => {
      // 入队失败 = 清库已生效而 AOF 缺条目（主从发散面），按存储错误拒绝
      if let Some(aof) = provider.try_aof()
        && let Err(e) = aof.enqueue_safe_flush_aof_if_primary(
          provider.is_primary(),
          AofEntryType::FlushAll,
          false,
          0,
          0,
        )
      {
        log::warn!("CLUSTER FLUSHALL 广播条目入队失败: {e}");
        out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
        return out;
      }
      out.write_resp_simple_string("OK");
    }
    Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
  }
  out
}

/// CLUSTER SYNC 导入壳（无盘全量同步数据导入；帧导入实体收归
/// [`import_migration_frames`]，与 CLUSTER MIGRATE 链共核共态）。本壳负责
/// 协议面差异：载荷解析、写入会话构造、恒覆写口径（承接面无存在性跳过，
/// replace 恒 true）与向量帧登记槽（复制会话所属库槽）。接收态随连接
/// （对标 C# per-connection chunkedRecordReassembler /
/// rangeIndexMigrationState 字段，ClusterSession 会话字段两链共用）；
/// 无盘同步经单条持久连接逐命令下发，跨命令分块/RI 续传即由该字段承接。
///
/// 全 owned 形态：载荷与接收态句柄 move 进慢路径执行体，由网络泵 await
/// 驱动至闭环（C# NetworkClusterSync 网络线程同步阻塞收割的 compio 挂起
/// 承接；载荷 to_vec 一次脱离接收缓冲——泵 await 期间缓冲被复用，与
/// `SlowWait::for_command` 参数拷贝同一纪律）；应答整段返回交泵写回
async fn cluster_sync_slow(
  provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  chunk_reassembler: Arc<Mutex<ChunkReassembler>>,
  ri_receive_state: Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>>,
  payload: Vec<u8>,
) -> Vec<u8> {
  let mut out = Vec::new();
  let (record_count, frames) = match parse_migration_payload(&payload) {
    Ok(res) => res,
    Err(e) => {
      out.write_resp_error(&format!("ERR Invalid sync payload: {e:?}"));
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
  let storage = StorageSession::new(batch);
  let import = FrameImport {
    provider: &provider,
    session: &session,
    storage: &storage,
    chunks: &chunk_reassembler,
    ri: &ri_receive_state,
    // 承接面恒覆写（对标 C# NetworkClusterSync 无条件 basicGarnetApi.SET）
    replace: true,
    // 向量帧登记槽取复制会话所属库（库级定槽 doc/zh/db.md 4.1，键内容不参与定槽；
    // 跨域落域上下文帧到达后逐载荷换域重算）
    vector_slot: slot_of(session.namespace(), session.active_db()),
    // 无盘同步承接面放行跨域扩展帧（落域上下文 + DbMeta 映射——锚前域映射
    // 唯一通路，见 frame_import 模块文档）
    accept_domain_frames: true,
  };
  // 错误收场与 MIGRATE 链同口径：核心显式拒绝即复位两接收态后应答
  match import_migration_frames(frames, &import).await {
    Ok(()) => out.write_resp_simple_string("OK"),
    Err(err) => {
      import.reset_receive_states();
      out.write_resp_error(&err);
    }
  }
  out
}

impl ClusterSession {
  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReplicas
  pub(super) fn network_cluster_replicas(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() != 1, cmd, output);
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(node_id) = hex_u128(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let replicas = m.list_replicas(node_id);
    output.write_resp_array_len(replicas.len());
    for item in &replicas {
      output.write_resp_bulk_string(item.as_bytes());
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReplicate
  pub(super) fn network_cluster_replicate(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.is_empty() || args.len() > 2, cmd, output);
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
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(node_id) = hex_u128(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // AOF 门（对标 RespClusterReplicationCommands.cs:86-90）：未开 AOF 即拒绝
    // 翻转角色并回错（副本无法落盘重放，复制不可用）；读取源取装配期 aof
    // 在位性（boot.rs set_aof 同源），不新增配置项。REPLICAOF 命令面 C# 无此门
    if self.cluster_provider.try_aof().is_none() {
      output.write_resp_error(ERR_GENERIC_REPLICATION_AOF_TURNEDOFF);
      return true;
    }
    // 发起参数束（对标 RespClusterReplicationCommands.cs:96-102：SYNC 为前台、
    // 缺省与 ASYNC 为后台；Force:false TryAddReplica:true
    // AllowReplicaResetOnFailure:true UpgradeLock:false）
    let background = !matches!(args.get(1), Some(flag) if flag.eq_ignore_ascii_case(b"SYNC"));
    let opts = ReplicateSyncOptions::new(node_id, background, false, true, true, false);
    *self.pending_slow.lock() = Some(queue_try_replicate_sync(m, opts));
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterFlushAll
  pub(super) fn network_cluster_flushall(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    match self.cluster_provider.try_store() {
      Some(_) => {
        let provider = Arc::clone(&self.cluster_provider);
        *self.pending_slow.lock() = Some(SlowWait::new(cluster_flush_all_slow(provider)));
      }
      None => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// CLUSTER FLUSHALL_NS 总线换号帧接收端（doc/zh/db.md 4.5 全租户秒清广播）
  ///
  /// 本端口多租户扩展控制帧（C# 无 ns 维度，帧结构模板为
  /// RespClusterReplicationCommands.cs:643 NetworkClusterFlushAll 的
  /// 恰好 N 参数直落换号形态）。换号执行复用 RESP 主路径同一
  /// SingleDatabaseManager::flush_namespace 漏斗（换号 + FlushNs 广播条目 +
  /// reclaim，3d04ac4 收口），本函数绝不另写第二套换号；守卫全部同步段
  /// 完成（配置读锁不跨 await）：ns 0 属物理截断域不在此帧语义、origin
  /// 回声拒绝、本机非 Primary 拒绝（副本经 AOF FlushNs 回放收敛，与 C#
  /// 总线只连 Master 同口径）、帧 epoch 低于本机配置中 origin 的
  /// config_epoch 或 origin 不可知即陈旧帧拒绝。
  /// 调用方身份门不在本臂（本切面对话的会话 ns 不经切面下传），单点收口
  /// 在 CLUSTER 命令面唯一入层 wnode admin_commands 的
  /// network_process_cluster_command：非节点间连接且非 ns0 会话即回
  /// RESP_ERR_NOPERM，判据复用与依据见该处注释（doc/zh/db.md 3.5）
  pub(super) fn network_cluster_flushall_ns(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if self.cluster_provider.is_device_contaminated() {
      output.write_resp_error(
        "ERR device is contaminated by a failed checkpoint receive, refusing flush",
      );
      return true;
    }
    reject_wrong_arity!(args.len() != 3, RespCommand::ClusterFlushallNs, output);
    // ns 为本端口多租户 u64 线面字段（无 C# 对应），不走库号的 int32 档
    let Some(ns) = strict_u64(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let Some(origin) = hex_u128(args[1]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(epoch) = strict_i64(args[2]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    if ns == 0 {
      output.write_resp_error("ERR FLUSHALL_NS namespace 0 is out of scope");
      return true;
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let guard = {
      let config = m.current_config();
      let local_id = config.local_node_id();
      if local_id == Some(origin) {
        Err("ERR FLUSHALL_NS origin node is self")
      } else if !matches!(
        local_id.and_then(|id| config.get_worker_from_node_id(id)),
        Some(w) if w.role == NodeRole::Primary
      ) {
        Err("ERR This node is not a master node.")
      } else {
        match config.get_worker_from_node_id(origin) {
          Some(origin_worker) if epoch >= origin_worker.config_epoch => Ok(()),
          _ => Err("ERR FLUSHALL_NS stale or unknown origin epoch"),
        }
      }
    };
    if let Err(msg) = guard {
      output.write_resp_error(msg);
      return true;
    }
    let Some(dm) = self.cluster_provider.try_database_manager() else {
      output.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
      return true;
    };
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match dm.flush_namespace(ns, false).await {
        Ok(()) => out.write_resp_simple_string("OK"),
        Err(_) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
      }
      out
    }));
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReserve
  pub(super) fn network_cluster_reserve(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    // C# parseState.Count < 2 / 非法计数 → invalidParameters（元数错误）
    reject_wrong_arity!(args.len() < 2, RespCommand::ClusterReserve, output);
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
    // 预留段为真异步（登记写透 `.await` 闭环，无内联收割）：解析段同步承接
    // 后整体挂起 pending_slow，网络泵 await 闭环后写回应答（对标本文件
    // FLUSHALL_NS 臂的 SlowWait 挂起先例，不发明新机制）
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match vm.reserve_contexts_for_migration(count).await {
        Some(contexts) => {
          out.write_resp_array_len(contexts.len());
          for ctx in contexts {
            let mut buf = Buffer::new();
            out.write_resp_simple_string(buf.format(ctx));
          }
        }
        // 上下文空间耗尽（C# 无对应失败分支；超 u32::MAX 编址上限显式拒绝）
        None => out.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
      }
      out
    }));
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAdvanceTime
  ///
  /// 副本侧时间脉冲（2 参：子日志下标 + 序列号）；解析失败 / 越界 /
  /// 恢复中均无应答写出（C# 同口径静默）
  pub(super) fn network_cluster_advance_time(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(args.len() != 2, RespCommand::ClusterAdvanceTime, output);
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
    // 驱动在册于本连接自持代的驱动仓（对标 C# :693
    // `replicaReplayDriverStore?.GetReplayDriver(...)?.SignalTimeAdvance(...)`
    // ——会话私有代际引用双重判空：未捕获代际（无成功初始化的连接）或
    // 该子日志无驱动均静默直过）
    if let Some(store) = self.replica_replay_driver_store.lock().as_ref()
      && let Some(driver) = store.get_replay_driver(idx as usize)
    {
      driver.signal_time_advance(sequence_number);
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterMlogKeyTime
  ///
  /// 多日志键序列号查询（1-2 参：键 + 可选 FRONTIER）：主端回序列号生成器
  /// 最新值，副本回键的重放序列号。AOF 门控未点亮 / 单物理日志按 C#
  /// ERR_MULTI_LOG_DISABLED 同口径显式报错
  pub(super) fn network_cluster_mlog_key_time(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(
      args.is_empty() || args.len() > 2,
      RespCommand::ClusterMlogKeyTime,
      output
    );
    let Some(aof) = self
      .cluster_provider
      .try_aof()
      .filter(|a| a.multi_log_enabled())
    else {
      output.write_resp_error(ERR_MULTI_LOG_DISABLED);
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

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:LogPrimaryStream
  #[inline]
  pub(crate) fn log_primary_stream(
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
  ) {
    log::debug!(
      "LogPrimaryStream physicalSublogIdx: {physical_sublog_idx}, previousAddress: {previous_address}, currentAddress: {current_address}, nextAddress: {next_address}"
    );
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAppendLog
  ///
  /// 主端 AOF 记录帧接收（5-6 参：nodeId、子日志下标、三位地址、可选 AOF
  /// 页）：初始化帧回 +OK，记录帧无应答（C# 同口径）；处理失败（角色
  /// 不符 / divergent / 恢复中）不写应答行直接断流（C# 同场景
  /// `throw new GarnetException(..., clientResponse: false)` 上抛 →
  /// RespServerSession catch → DisposeNetworkSender，经
  /// [`ClusterSessionFace::take_fatal_disconnect`] 信号通道承接）
  pub(super) fn network_cluster_appendlog(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(
      args.len() < 5 || args.len() > 6,
      RespCommand::ClusterAppendlog,
      output
    );
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
    let Ok(physical_sublog_idx) = usize::try_from(idx) else {
      log::error!("APPENDLOG 收到非法负子日志索引: {idx}");
      return true;
    };
    Self::log_primary_stream(physical_sublog_idx, previous, current, next);
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(node_id) = hex_u128(args[0]) else {
      return true;
    };
    let payload = args.get(5).copied().unwrap_or(&[]);
    match session.process_append_log(
      node_id,
      physical_sublog_idx,
      previous,
      current,
      next,
      payload,
    ) {
      // 主 id 校验通过即钉死本连接为活跃复制流（对标 C#
      // RespClusterReplicationCommands.cs:218 校验通过处置 IsReplicating =
      // true：EnsureReplication 不因 AOF 流空闲误判断链；本面 dispose 据
      // 同标志清理会话自持代际的驱动仓，见 ClusterSessionFace::dispose）
      Ok(outcome @ (AppendLogOutcome::Initialized | AppendLogOutcome::Record)) => {
        self.is_replicating.store(true, Ordering::Release);
        match outcome {
          AppendLogOutcome::Initialized => {
            // 初始化帧注册成功即捕获驱动仓当时代际为会话私有引用（对标
            // C# RespClusterReplicationCommands.cs:221-224
            // `if (InitializeReplicaReplayDriver(...))
            //  replicaReplayDriverStore =
            //  rm.ReplicaReplayDriverStore;`）：断连仅终结本连接所属代际
            if let Some(rm) = self.cluster_provider.replication_manager() {
              *self.replica_replay_driver_store.lock() =
                Some(rm.current_replica_replay_driver_store());
            }
            output.write_resp_simple_string("OK");
          }
          // C# 普通记录帧不回写
          AppendLogOutcome::Record => {}
        }
      }
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
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:TryBeginDiskbasedSyncAsync
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:ReplicaSyncSessionBackgroundTaskAsync
  /// （`TryBeginDiskbasedSyncAsync`：头部 TryAddReplicaSyncSession 入口去重
  /// （同副本在册即回 RESP_ERR_CREATE_SYNC_SESSION_ERROR）→ 尾部局部函数体
  /// TryGetSession 取会话 → `SendCheckpointAsync` → finally TryRemove 摘除登记，
  /// 并以 errorMessage 回 RESP。rust 会话为装配期单例 `assets.sync_session`，去重
  /// 登记册为 [`ReplicationManager::replica_sync_task_store`]，本处即该头部去重
  /// 与后台体的位点：端点反查失败即回错、同步体经 `pending_slow` 慢路径承载
  /// （终局或取消经 Drop 守卫摘登记，承接 C# finally TryRemove）、结果收敛为
  /// +OK/错误文案，故该符号挂载于此，`SendCheckpointAsync` 本体见
  /// [`initiate_replica_sync`](crate::server::replication::replica_sync_session::ReplicaSyncSession::initiate_replica_sync)）
  ///
  /// 主端同步发起（5 参：副本节点 id、指派主 id、检查点条目、副本 AOF
  /// 起止位点）：按上报元数据构造同步请求，策略协商 + 建连 + 补扫交
  /// 慢路径承载（C# BlockingWait 等价）；成功 +OK，失败回错误文案
  pub(super) fn network_cluster_initiate_replica_sync(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(
      args.len() != 5,
      RespCommand::ClusterInitiateReplicaSync,
      output
    );
    let Some(assets) = self.cluster_provider.try_primary_replication() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(replica_node_id) = hex_u128(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
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
        .get_endpoint_from_node_id(replica_node_id)
    }) else {
      output.write_resp_error(&format!(
        "{ERR_UNKNOWN_NODE_PREFIX}{}.",
        hex_str_u128(replica_node_id)
      ));
      return true;
    };
    // 入口去重登记（C# TryBeginDiskbasedSyncAsync 头部 TryAddReplicaSyncSession，
    // ReplicaSyncSessionTaskStore.cs:82）：同一副本并发重复发起时次路拒绝，
    // 杜绝两路同步体互拆推流驱动与截断钉线；rm 停机 dispose 全摘
    let Some(rm) = self.cluster_provider.replication_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if !rm.replica_sync_task_store.try_add(replica_node_id) {
      // C# CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR 原文案（重复细节
      // 日志已在 try_add 内按 C# "already exists" 口径记录）
      output.write_resp_error("PRIMARY-ERR Failed creating replica sync session task");
      return true;
    }
    let meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: replica_node_id,
      current_primary_repl_id: assigned_primary_id,
      // 副本 store 版本单点取自上报的检查点条目（C# 磁盘入口
      // NetworkClusterInitiateReplicaSync 只交 replicaCheckpointEntry + AOF 起止
      // 位点，两侧 storeVersion 比对在 ValidateMetadata 内自 CheckpointEntry 取，
      // SyncMetadata.currentStoreVersion 在本链判定中不被消费）
      current_store_version: checkpoint_entry.metadata.store_version,
      current_aof_begin_address: replica_aof_begin,
      current_aof_tail_address: replica_aof_tail,
      current_replication_offset: replica_aof_tail,
      checkpoint_entry: Some(checkpoint_entry),
    };
    let sync_session = Arc::clone(&assets.sync_session);
    // 本端节点 id（init 帧 node_id；副本侧据此校验 current primary）
    let local_node_id = self
      .cluster_manager()
      .and_then(|m| m.current_config().local_node_id())
      .unwrap_or_default();
    let endpoint = endpoint.to_string();
    let provider = Arc::clone(&self.cluster_provider);
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      // 在册摘除守卫（C# 后台体 finally TryRemove 的 Drop 承接）：慢路径终局
      // 或会话断链 future 取消两路都摘登记；摘除失败按 C# 口径记错误日志
      let _unregister = SyncTaskGuard {
        store: &rm.replica_sync_task_store,
        node: replica_node_id,
      };
      match sync_session
        .initiate_replica_sync(&provider, &assets, local_node_id, &endpoint, &meta)
        .await
      {
        Ok(_) => out.write_resp_simple_string("OK"),
        Err(msg) => out.write_resp_error(&msg),
      }
      out
    }));
    true
  }

  /// 检查点接收命令执行闭环（上下文解析 -> pending_slow 挂起 -> 泵驱动应答）
  ///
  /// 执行体 owned 化（数据段 to_vec 脱离接收缓冲——挂起期间缓冲被复用，
  /// `SlowWait::for_command` 参数拷贝同一纪律）挂 [`ClusterSession::pending_slow`]，
  /// 会话侧转挂网络泵 await 闭环（同文件 begin_replica_recover 等五处先例
  /// 同构；C# 网络线程 BlockingWait 同步收割的 compio 挂起承接，连接内应答
  /// 顺序由泵保序，可观测行为一致）
  fn execute_checkpoint_recv<F, Fut>(&self, args: &[&[u8]], output: &mut Vec<u8>, f: F) -> bool
  where
    F: FnOnce(Arc<ReplicationManager>, CheckpointImportCtx, u128, CheckpointFileType) -> Fut
      + 'static,
    Fut: Future<Output = Result<(), String>> + 'static,
  {
    let Some(rm) = self.cluster_provider.replication_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let Some(token) = parse_checkpoint_token(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(file_type_int) = strict_i64(args[1]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let Some(file_type) = CheckpointFileType::from_protocol(file_type_int) else {
      output.write_resp_error(&format!("ERR invalid checkpoint filetype {file_type_int}"));
      return true;
    };
    let ctx = match self.cluster_provider.checkpoint_import_ctx() {
      Ok(ctx) => ctx,
      // 磁盘 IO 硬错（只读目录/ENOSPC/权限受限）：应答携带 path/errno 因由，
      // 主端与运维可辨「盘错」与「配置态未就绪」（对位 C# 类型化异常臂分列；
      // IOERR 大写前缀经 write_resp_error 原样成帧不叠 ERR）
      Err(Error::Io(e)) => {
        output.write_resp_error(&format!("IOERR create checkpoint dir: {e}"));
        return true;
      }
      // 目录/引擎未接线属集群配置态（等拓扑收敛可重试），维持原帧零漂移
      Err(_) => {
        output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
        return true;
      }
    };
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match f(rm, ctx, token, file_type).await {
        Ok(()) => out.write_resp_simple_string("OK"),
        Err(msg) => out.write_resp_error(&msg),
      }
      out
    }));
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSnapshotData
  ///
  /// 检查点数据统一接收帧（4 参：token、文件类型、段起始地址、数据）：
  /// startAddress = -1 单消息元数据载荷，空 data 流收尾哨兵，其余为段流；
  /// 处理失败回 -ERR（C# GarnetException clientResponse 默认形态），会话
  /// 不断连
  pub(super) fn network_cluster_snapshot_data(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(args.len() != 4, RespCommand::ClusterSnapshotData, output);
    let Some(start_address) = strict_i64(args[2]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let data = args[3].to_vec();
    self.execute_checkpoint_recv(args, output, move |rm, ctx, token, file_type| async move {
      rm.recv_checkpoint_handler
        .process_snapshot_data(&rm, &ctx, token, file_type, start_address, &data)
        .await
    })
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSendCheckpointMetadata
  ///
  /// 检查点元数据接收帧（3 参：token、文件类型、元数据字节，旧形态命令；
  /// 单消息整包写 + 收尾，C# ProcessMetadata 同构）
  pub(super) fn network_cluster_send_checkpoint_metadata(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(
      args.len() != 3,
      RespCommand::ClusterSendCkptMetadata,
      output
    );
    let data = args[2].to_vec();
    self.execute_checkpoint_recv(args, output, |rm, ctx, token, file_type| async move {
      rm.recv_checkpoint_handler
        .process_metadata(&rm, &ctx, token, file_type, &data)
        .await
    })
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSendCheckpointFileSegment
  ///
  /// 检查点文件段接收帧（5 参：token、文件类型、段起始地址、数据、段号；
  /// 旧形态命令——segmentId 为向下兼容校验位不消费，C# 同注释）
  pub(super) fn network_cluster_send_checkpoint_file_segment(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(
      args.len() != 5,
      RespCommand::ClusterSendCkptFileSegment,
      output
    );
    let Some(start_address) = strict_i64(args[2]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let data = args[3].to_vec();
    self.execute_checkpoint_recv(args, output, move |rm, ctx, token, file_type| async move {
      rm.recv_checkpoint_handler
        .process_file_segment(&rm, &ctx, token, file_type, start_address, &data)
        .await
    })
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterBeginReplicaRecover
  ///
  /// 检查点导入触发帧（6 参：recoverStoreFromToken、replayAOFMap、
  /// primaryReplicaId、检查点条目字节、begin/tail 位点 span）：副本从
  /// 接收文件集恢复引擎并置换在线引擎，应答复制位点 bulk string（C# 同
  /// 口径）；失败回错误文案。慢路径承载（C# 网络线程同步阻塞等价）。
  pub(super) fn network_cluster_begin_replica_recover(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(
      args.len() != 6,
      RespCommand::ClusterBeginReplicaRecover,
      output
    );
    let Some(rm) = self.cluster_provider.replication_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // C# TryReadBool 仅收 1/0（ParseUtils.cs:224-237）
    let recover_store_from_token = match args[0] {
      b"1" => true,
      b"0" => false,
      _ => {
        output.write_resp_error(ERR_GENERIC_VALUE_IS_NOT_BOOLEAN);
        return true;
      }
    };
    let Some(replay_aof_map) = strict_i64(args[1]).and_then(|v| u64::try_from(v).ok()) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let primary_repl_id = String::from_utf8_lossy(args[2]).into_owned();
    let Some(remote_entry) = CheckpointEntry::from_byte_array(args[3]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let begin_address = AofAddress::from_span(args[4]);
    let tail_address = AofAddress::from_span(args[5]);

    let provider = Arc::clone(&self.cluster_provider);
    let rm = Arc::clone(&rm);
    let request = ReplicaRecoverRequest {
      recover_store_from_token,
      replay_aof_map,
      primary_repl_id,
      remote_entry,
      begin_address,
      tail_address,
    };
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      match try_replica_diskbased_recovery(&provider, &rm, &request).await {
        Ok(offset) => out.write_resp_bulk_string(offset.to_aof_string().as_bytes()),
        Err(msg) => out.write_resp_error(&msg),
      }
      out
    }));
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterAttachSync
  pub(super) fn network_cluster_attach_sync(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(args.len() != 1, RespCommand::ClusterAttachSync, output);
    let Ok(sync_meta) = SyncMetadata::from_byte_array(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(rm) = self.cluster_provider.replication_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };

    let provider = Arc::clone(&self.cluster_provider);
    if sync_meta.origin_node_role == NodeRole::Replica {
      let Some(assets) = self.cluster_provider.try_primary_replication() else {
        output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
        return true;
      };
      let Some(endpoint) = self.cluster_manager().and_then(|m| {
        m.current_config()
          .get_endpoint_from_node_id(sync_meta.origin_node_id)
      }) else {
        output.write_resp_error(&format!(
          "{ERR_UNKNOWN_NODE_PREFIX}{}.",
          hex_str_u128(sync_meta.origin_node_id)
        ));
        return true;
      };
      let local_node_id = self
        .cluster_manager()
        .and_then(|m| m.current_config().local_node_id())
        .unwrap_or_default();
      let endpoint = endpoint.to_string();
      *self.pending_slow.lock() = Some(SlowWait::new(async move {
        let mut out = Vec::new();
        match try_begin_diskless_sync_async(
          &provider,
          &assets,
          local_node_id,
          &endpoint,
          &sync_meta,
        )
        .await
        {
          Ok(offset) => out.write_resp_bulk_string(offset.to_aof_string().as_bytes()),
          Err(msg) => out.write_resp_error(&msg),
        }
        out
      }));
    } else {
      match try_replica_diskless_recovery(&provider, &rm, &sync_meta) {
        Ok(offset) => output.write_resp_bulk_string(offset.to_aof_string().as_bytes()),
        Err(msg) => output.write_resp_error(&msg),
      }
    }
    true
  }

  /// libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSync
  ///
  /// 载荷与接收态全 owned 进慢路径执行体（[`cluster_sync_slow`]），挂
  /// [`ClusterSession::pending_slow`] 由网络泵 await 闭环——C# 网络线程同步
  /// 收割的 compio 挂起承接（同文件检查点接收命令族同构）；载荷 to_vec 一次
  /// 脱离接收缓冲（泵挂起期间缓冲被复用），应答整段返回交泵写回，连接内
  /// 应答顺序由泵保序（C# 同步收割形态的可观测等价）
  pub(super) fn network_cluster_sync(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(args.len() != 2, RespCommand::ClusterSync, output);
    let Some(store) = self.cluster_provider.try_store() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份（接收态已会话化，
    // 源节点 id 不再参与接收态路由，仅作语法校验与应答语义保留）
    if hex_u128(args[0]).is_none() {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let ri_receive_state = self.ensure_range_index_receive_state();
    *self.pending_slow.lock() = Some(SlowWait::new(cluster_sync_slow(
      Arc::clone(&self.cluster_provider),
      store,
      Arc::clone(&self.chunk_reassembler),
      ri_receive_state,
      args[1].to_vec(),
    )));
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_log_primary_stream() {
    ClusterSession::log_primary_stream(0, 100, 200, 300);
    assert_eq!(parse_checkpoint_token(&[0u8; 16]), Some(0u128));
  }
}
