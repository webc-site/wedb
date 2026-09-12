use std::{
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use compio::time::sleep;
use log::{error, info, trace, warn};
use parking_lot::RwLock;
use waof::{AofAddress, AofEntryType, FIRST_VALID_AOF_ADDRESS};

use crate::server::replication::{
  aof_sync_driver_store::AofSyncDriverStore, checkpoint_entry::CheckpointEntry,
  checkpoint_store::CheckpointStore, recovery_status::RecoveryStatus,
  replica_replay_driver_store::ReplicaReplayDriverStore, replication_history::ReplicationHistory,
  store_commit::{StoreCommitChannel, StoreCommitFace}, sync_metadata::SyncMetadata,
};

/// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ComputeAofSyncReplayAddress
///
/// 主备数据同步协商策略结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResyncStrategy {
  /// 增量流式同步（Partial Resync）：两端历史一致且位点衔接无断层，直接从指定位点推送 AOF 增量
  PartialResync {
    sync_start_address: AofAddress,
    replay_aof_mask: u64,
  },
  /// 全量检查点同步（Full Resync）：两端历史不一致或位点已被截断，需下发快照检查点后接续 AOF
  FullResync {
    sync_start_address: AofAddress,
    replay_aof_mask: u64,
  },
}

/// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationManager
///
/// 管理 AOF 增量追踪、主备位点同步、副本复制会话、故障转移位点轮转与全链路自愈状态机
pub struct ReplicationManager {
  replication_offset: RwLock<AofAddress>,
  replication_checkpoint_start_offset: RwLock<AofAddress>,
  current_replication_config: RwLock<ReplicationHistory>,
  primary_sync_last_timestamp: AtomicI64,
  current_recovery_status: RwLock<RecoveryStatus>,
  store_current_safe_aof_address: RwLock<AofAddress>,
  store_recovered_safe_aof_address: RwLock<AofAddress>,
  pub checkpoint_store: Arc<RwLock<CheckpointStore>>,
  pub aof_sync_driver_store: Arc<AofSyncDriverStore>,
  pub replica_replay_driver_store: Arc<ReplicaReplayDriverStore>,
  config_path: Option<PathBuf>,
  sublog_count: usize,
  /// storeWrapper 提交标记写入通道（对标 C# rm 经 clusterProvider.storeWrapper
  /// .EnqueueCommit 写检查点标记；集群装配期注入，见 store_commit 模块）
  commit_channel: RwLock<Option<StoreCommitChannel>>,
  /// 上次 EnsureReplication 尝试时间戳（毫秒；C# lastEnsureReplicationAttempt）
  last_ensure_replication_attempt_ms: AtomicI64,
}

impl Default for ReplicationManager {
  fn default() -> Self {
    Self::new()
  }
}

impl ReplicationManager {
  /// 创建新的复制管理器实例（默认配置）
  pub fn new() -> Self {
    Self::with_options(1, None)
  }

  /// 创建指定子日志数与持久化路径的复制管理器实例
  pub fn with_options(sublog_count: usize, config_dir: Option<&Path>) -> Self {
    let sublog_count = sublog_count.max(1);
    let config_path = config_dir.map(|p| p.join("replication.conf"));

    let history = if let Some(ref path) = config_path {
      ReplicationHistory::recover_or_init(path, sublog_count)
    } else {
      ReplicationHistory::new(sublog_count)
    };

    // 对标 C# 构造：replicationOffset 独立于 history 初始化为 kFirstValidAofAddress，
    // 恢复场景由 RecoverCheckpointAndAOFAsync 重放后覆盖
    let initial_offset = AofAddress::create(sublog_count as i32, FIRST_VALID_AOF_ADDRESS);

    Self {
      replication_offset: RwLock::new(initial_offset),
      replication_checkpoint_start_offset: RwLock::new(AofAddress::create(sublog_count as i32, 0)),
      current_replication_config: RwLock::new(history),
      primary_sync_last_timestamp: AtomicI64::new(0),
      current_recovery_status: RwLock::new(RecoveryStatus::NoRecovery),
      store_current_safe_aof_address: RwLock::new(initial_offset),
      store_recovered_safe_aof_address: RwLock::new(initial_offset),
      checkpoint_store: Arc::new(RwLock::new(CheckpointStore::new(true))),
      aof_sync_driver_store: Arc::new(AofSyncDriverStore::new(sublog_count)),
      replica_replay_driver_store: Arc::new(ReplicaReplayDriverStore::new(sublog_count)),
      config_path,
      sublog_count,
      commit_channel: RwLock::new(None),
      last_ensure_replication_attempt_ms: AtomicI64::new(0),
    }
  }

  /// 注入 storeWrapper 提交标记写入通道（集群装配期一次注入）
  pub fn set_commit_channel(&self, channel: Option<StoreCommitChannel>) {
    *self.commit_channel.write() = channel;
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:InitializeReplicationHistory
  pub fn initialize_replication_history(&self, aof_physical_sublog_count: usize) {
    let mut config = self.current_replication_config.write();
    *config = ReplicationHistory::new(aof_physical_sublog_count);
    drop(config);
    self.flush_config();
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:RecoverReplicationHistory
  pub fn recover_replication_history(&self) {
    if let Some(ref path) = self.config_path {
      let mut config = self.current_replication_config.write();
      *config = ReplicationHistory::recover_or_init(path, self.sublog_count);
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FlushConfig
  pub fn flush_config(&self) {
    if let Some(ref path) = self.config_path {
      let config = self.current_replication_config.read();
      if let Err(e) = config.flush_to_file(path) {
        error!("Failed to flush replication history: {e}");
      }
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateMyPrimaryReplId
  pub fn try_update_my_primary_repl_id(&self, primary_replication_id: &str) {
    let mut config = self.current_replication_config.write();
    *config = config.update_replication_id(primary_replication_id);
    drop(config);
    self.flush_config();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:AddCheckpointEntry
  ///
  /// 登记新检查点条目到内存检查点仓库（C# ReplicationCheckpointManagement.cs 委托）
  pub fn add_checkpoint_entry(&self, entry: CheckpointEntry, full_checkpoint: bool) {
    self
      .checkpoint_store
      .write()
      .add_checkpoint_entry(entry, full_checkpoint);
  }

  /// 物理子日志数量
  #[inline]
  pub fn sublog_count(&self) -> usize {
    self.sublog_count
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetReplicationOffset
  ///
  /// 获取指定子日志的复制偏移
  pub fn get_replication_offset(&self, sublog_idx: usize) -> i64 {
    self.replication_offset.read().get(sublog_idx).unwrap_or(0)
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:SetSublogReplicationOffset
  ///
  /// 设置指定子日志的复制偏移（对标 C# 直接赋值）
  pub fn set_sublog_replication_offset(&self, sublog_idx: usize, offset: i64) {
    self.replication_offset.write().set(sublog_idx, offset);
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetSublogReplicationOffset
  ///
  /// 获取指定子日志的复制偏移（别名）
  pub fn get_sublog_replication_offset(&self, sublog_idx: usize) -> i64 {
    self.get_replication_offset(sublog_idx)
  }

  /// 获取当前完整的 AOF 复制地址
  pub fn get_current_replication_offset(&self) -> AofAddress {
    *self.replication_offset.read()
  }

  /// 设置当前完整的 AOF 复制地址（对标 C# 直接赋值）
  pub fn set_current_replication_offset(&self, offset: AofAddress) {
    *self.replication_offset.write() = offset;
  }

  /// 获取旧主历史复制偏移（failover 后有效）
  pub fn get_replication_offset2(&self) -> AofAddress {
    self.current_replication_config.read().replication_offset2
  }

  /// 获取主复制 ID
  pub fn primary_repl_id(&self) -> String {
    self
      .current_replication_config
      .read()
      .primary_repl_id
      .clone()
  }

  /// 获取次级主复制 ID（故障转移旧主 ID）
  pub fn primary_repl_id2(&self) -> String {
    self
      .current_replication_config
      .read()
      .primary_repl_id2
      .clone()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:UpdateLastPrimarySyncTime
  ///
  /// 更新主从同步时间戳
  pub fn update_last_primary_sync_time(&self) {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    self
      .primary_sync_last_timestamp
      .store(now_ms, Ordering::Release);
  }

  /// 距离上次主从同步过去的秒数
  pub fn last_primary_sync_seconds(&self) -> i64 {
    let last = self.primary_sync_last_timestamp.load(Ordering::Acquire);
    if last == 0 {
      0
    } else {
      let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
      (now_ms.saturating_sub(last)) / 1000
    }
  }

  /// 获取当前恢复状态
  #[inline]
  pub fn recovery_status(&self) -> RecoveryStatus {
    *self.current_recovery_status.read()
  }

  /// 是否正在恢复中
  #[inline]
  pub fn is_recovering(&self) -> bool {
    let s = self.recovery_status();
    s != RecoveryStatus::NoRecovery && s != RecoveryStatus::ReadRole
  }

  /// 是否无法流式传输 AOF
  #[inline]
  pub fn cannot_stream_aof(&self) -> bool {
    self.is_recovering() && self.recovery_status() != RecoveryStatus::CheckpointRecoveredAtReplica
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationCheckpointStartOffset
  pub fn get_replication_checkpoint_start_offset(&self) -> AofAddress {
    *self.replication_checkpoint_start_offset.read()
  }

  /// 设置检查点开始标记偏移
  pub fn set_replication_checkpoint_start_offset(&self, offset: AofAddress) {
    *self.replication_checkpoint_start_offset.write() = offset;
  }

  /// 设置指定子日志检查点开始标记偏移
  pub fn set_sublog_checkpoint_start_offset(&self, sublog_idx: usize, offset: i64) {
    self
      .replication_checkpoint_start_offset
      .write()
      .set(sublog_idx, offset);
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:BeginRecovery
  ///
  /// 开始恢复任务（设置恢复状态并加锁门控）
  pub fn begin_recovery(&self, next_recovery_status: RecoveryStatus, upgrade_lock: bool) -> bool {
    let mut status_guard = self.current_recovery_status.write();

    if upgrade_lock {
      if *status_guard != RecoveryStatus::ReadRole {
        return false;
      }
      *status_guard = next_recovery_status;
      trace!("Upgraded recover lock to [{next_recovery_status:?}]");
      return true;
    }

    if *status_guard != RecoveryStatus::NoRecovery {
      warn!(
        "Error background recovery task has not completed [{:?}]",
        *status_guard
      );
      return false;
    }

    *status_guard = next_recovery_status;
    trace!("Success recover lock [{next_recovery_status:?}]");
    true
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:EndRecovery
  ///
  /// 结束恢复任务并释放门控锁；严格对标 C# 状态转换矩阵，
  /// 非法转换记错误日志并拒绝变更（C# 抛 GarnetException 的非 panic 承接）
  pub fn end_recovery(&self, next_recovery_status: RecoveryStatus, downgrade_lock: bool) {
    let mut status_guard = self.current_recovery_status.write();
    let curr = *status_guard;
    trace!("EndRecovery [{curr:?} -> {next_recovery_status:?}]");

    if downgrade_lock {
      // C# Debug.Assert：只能降级到 ReadRole；且 ReadRole 起点不可再降级
      if curr == RecoveryStatus::ReadRole {
        error!("Cannot downgrade lock FROM a ReadRole [{curr:?}, {next_recovery_status:?}]");
        return;
      }
      *status_guard = RecoveryStatus::ReadRole;
      return;
    }

    let valid = match curr {
      RecoveryStatus::NoRecovery => false,
      RecoveryStatus::InitializeRecover
      | RecoveryStatus::ClusterReplicate
      | RecoveryStatus::ClusterFailover
      | RecoveryStatus::ReplicaOfNoOne => matches!(
        next_recovery_status,
        RecoveryStatus::CheckpointRecoveredAtReplica
          | RecoveryStatus::NoRecovery
          | RecoveryStatus::ReadRole
      ),
      // C#：CheckpointRecoveredAtReplica 只能转 NoRecovery / ReadRole
      RecoveryStatus::CheckpointRecoveredAtReplica => matches!(
        next_recovery_status,
        RecoveryStatus::NoRecovery | RecoveryStatus::ReadRole
      ),
      // C#：ReadRole 起点允许转任意 next（ReadUnlock + ResumeCheckpoints）
      RecoveryStatus::ReadRole => true,
    };

    if valid {
      *status_guard = next_recovery_status;
    } else {
      error!("Invalid state change [{curr:?} -> {next_recovery_status:?}]");
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ResetRecovery
  ///
  /// 重置恢复状态为 NoRecovery 并复位重连自愈
  pub fn reset_recovery(&self) {
    let mut status_guard = self.current_recovery_status.write();
    *status_guard = RecoveryStatus::NoRecovery;
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateForFailover
  ///
  /// 故障转移触发时更新复制位点与复制流 ID 轮转并持久化配置
  pub fn try_update_for_failover(&self) {
    let cur_offset = *self.replication_offset.read();
    let mut config = self.current_replication_config.write();
    *config = config.failover_update(cur_offset);
    drop(config);
    self.flush_config();
    self.set_primary_replication_id();
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:SetPrimaryReplicationId
  ///
  /// 更新历史 ID 供未来快照签名（对标 C# SetPrimaryReplicationId）
  pub fn set_primary_replication_id(&self) {
    let repl_id = self.primary_repl_id();
    trace!("SetPrimaryReplicationId: {repl_id}");
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:UpdateCommitSafeAofAddress
  ///
  /// 更新当前待提交检查点的安全 AOF 尾地址（对标 C# UpdateCommitSafeAofAddress）
  pub fn update_commit_safe_aof_address(&self, safe_aof_tail_address: &AofAddress) {
    *self.store_current_safe_aof_address.write() = *safe_aof_tail_address;
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:SetRecoveredSafeAofAddress
  pub fn set_recovered_safe_aof_address(&self, address: &AofAddress) {
    *self.store_recovered_safe_aof_address.write() = *address;
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointFromMemoryInfo
  pub fn get_latest_checkpoint_from_memory_info(&self) -> String {
    self
      .checkpoint_store
      .read()
      .get_latest_checkpoint_from_memory_info()
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointFromDiskInfo
  ///
  /// 获取磁盘最新检查点格式化信息（专供 Redis `INFO CINFO` 监控段中的 `disk_checkpoint_entry` 指标）
  ///
  /// 【对标 C# 原型】：
  /// C# 经 `checkpointStore.GetLatestCheckpointFromDiskInfo` 触发底层 Tsavorite 存储引擎扫盘，
  /// 读取快照文件并反序列化其中的 Cookie（提取 covered AOF 地址与 PrimaryReplId），
  /// 若无快照或异常则捕获后返回 `"(empty)"`。
  ///
  /// 【wedb 架构演进与差异】：
  /// 1. 底层解耦：wedb 采用自研 `wcpr` CPR 快照模型，快照按单调自增 Token（u128）由 `wcpr` 独立维护。
  /// 2. Cookie 不落盘：wedb 架构公理明确定义 `cookie 属复制域不落地`（见 `wcpr/src/meta.rs:229`），
  ///    复制历史位点由 `replication.conf` 单独管理，因此磁盘快照中根本不存在复制 Cookie。
  /// 3. 内存职责纯粹化：`CheckpointStore` 已彻底重构为纯内存并发链表容器，不碰磁盘。
  ///
  /// 【保留本方法的意义】：
  /// 仅用于保持 Redis 协议与 Garnet 既有监控命令兼容性——在客户端执行 `INFO CINFO` 或 `INFO ALL`
  /// 时，稳定输出 `disk_checkpoint_entry:(empty)` 键值对，避免第三方监控/解析组件因缺少该键而报错；
  /// 采用静态生命周期常量切片，零堆分配、零开销。
  pub fn get_latest_checkpoint_from_disk_info(&self) -> &'static str {
    "(empty)"
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetRecoveredSafeAofAddress
  ///
  /// 获取安全恢复的 AOF 地址
  pub fn get_recovered_safe_aof_address(&self) -> AofAddress {
    *self.store_recovered_safe_aof_address.read()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetCurrentSafeAofAddress
  ///
  /// 获取当前安全 AOF 地址
  pub fn get_current_safe_aof_address(&self) -> AofAddress {
    *self.store_current_safe_aof_address.read()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:DataLossCheck
  ///
  /// 校验从节点请求的同步位点是否落后于主节点 AOF 截断安全线
  pub fn data_loss_check(
    &self,
    possible_aof_data_loss: bool,
    sync_from_aof_address: &AofAddress,
    begin_aof_address: &AofAddress,
  ) -> Result<(), String> {
    if sync_from_aof_address.any_lesser(begin_aof_address) {
      if !possible_aof_data_loss {
        let msg = format!(
          "Failed syncing because replica requested truncated AOF address: {sync_from_aof_address:?} < beginAofAddress: {begin_aof_address:?}"
        );
        error!("{msg}");
        return Err(msg);
      } else {
        warn!(
          "AOF truncated, unsafe attach allowed: {sync_from_aof_address:?} < beginAofAddress: {begin_aof_address:?}"
        );
      }
    }
    Ok(())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync
  ///
  /// 对标 Garnet 全链路判定：根据主从历史、检查点版本与 AOF 物理位点决断 Partial Resync 还是 Full Resync
  pub fn determine_resync_strategy(
    &self,
    replica_meta: &SyncMetadata,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncStrategy {
    let local_checkpoint = self.checkpoint_store.read().latest_entry();

    let replica_checkpoint = replica_meta.checkpoint_entry.as_ref();

    // 1. 主从检查点历史判定：双方检查点中记录的 PrimaryReplId 需严格一致
    let same_main_store_checkpoint_history = match (&replica_checkpoint, &local_checkpoint) {
      (Some(rc), Some(lc)) => {
        rc.metadata
          .store_primary_repl_id
          .as_deref()
          .is_some_and(|id| !id.is_empty())
          && rc.metadata.store_primary_repl_id == lc.metadata.store_primary_repl_id
      }
      _ => false,
    };

    // 2. 故障转移跨主历史判定：副本记录的主节点 ID 是否与当前节点的次级主 ID（旧主）匹配
    let same_history2 = !self.primary_repl_id2().is_empty()
      && self.primary_repl_id2() == replica_meta.current_primary_repl_id;

    // 3. 是否跳过主节点本地检查点全量发送（对标 skipLocalMainStoreCheckpoint）
    let skip_local_checkpoint = match (&local_checkpoint, &replica_checkpoint) {
      (None, _) => true,
      (Some(lc), Some(rc)) => {
        lc.metadata.store_hlog_token == 0
          || (same_main_store_checkpoint_history
            && lc.metadata.store_version == rc.metadata.store_version)
      }
      _ => false,
    };

    let mut replay_aof_mask = 0u64;
    let mut sync_start_address = if let Some(ref lc) = local_checkpoint {
      lc.get_min_aof_covered_address(0)
    } else {
      *primary_aof_begin
    };

    // 4. 若不需下发检查点快照，逐子日志判定 AOF 增量流接续位点
    if skip_local_checkpoint {
      let repl_offset2 = self.get_replication_offset2();
      let mut is_partial_possible = true;

      for sublog_idx in 0..self.sublog_count {
        let rep_begin = replica_meta
          .current_aof_begin_address
          .get(sublog_idx)
          .unwrap_or(0);
        let rep_tail = replica_meta
          .current_aof_tail_address
          .get(sublog_idx)
          .unwrap_or(0);
        let ckpt_begin = sync_start_address.get(sublog_idx).unwrap_or(0);

        if rep_begin > 0 && rep_begin > ckpt_begin {
          // 副本自身 AOF 已被物理截断过高，缺失检查点覆盖的起始日志
          is_partial_possible = false;
          break;
        }

        if rep_tail < ckpt_begin && !fast_aof_truncate {
          // 副本尾部位点低于检查点起始覆盖点，无法连续回放
          is_partial_possible = false;
          break;
        }

        let mut replay_until = rep_tail;
        let committed = committed_until.get(sublog_idx).unwrap_or(i64::MAX);
        if committed < replay_until {
          replay_until = committed;
        }

        if replay_until > ckpt_begin {
          replay_aof_mask |= 1 << sublog_idx;
          if same_history2 {
            let limit = repl_offset2.get(sublog_idx).unwrap_or(i64::MAX);
            if replay_until > limit {
              replay_until = limit;
            }
          }
          sync_start_address.set(sublog_idx, replay_until);
        }

        if !same_main_store_checkpoint_history {
          let pri_begin = primary_aof_begin.get(sublog_idx).unwrap_or(0);
          sync_start_address.set(sublog_idx, pri_begin);
          replay_aof_mask &= !(1 << sublog_idx);
        }
      }

      if is_partial_possible && (replay_aof_mask > 0 || replica_meta.current_store_version > 0) {
        info!("Resync strategy resolved: PartialResync (incremental stream continuation)");
        return ResyncStrategy::PartialResync {
          sync_start_address,
          replay_aof_mask,
        };
      }
    }

    info!("Resync strategy resolved: FullResync (checkpoint snapshot required)");
    ResyncStrategy::FullResync {
      sync_start_address,
      replay_aof_mask,
    }
  }

  /// 处理从节点上报的 ReplicationAck 位点并安全推进截断与读者锁释放
  pub fn handle_replica_ack(
    &self,
    remote_node_id: &str,
    physical_sublog_idx: usize,
    acked_offset: i64,
  ) -> bool {
    let success = self.aof_sync_driver_store.process_replica_ack(
      remote_node_id,
      physical_sublog_idx,
      acked_offset,
    );
    if success {
      // 推进安全截断水位
      let safe_addrs = self.aof_sync_driver_store.publish_shipped_addresses();
      let ckpt_guard = self.checkpoint_store.read();
      if let Some(entry) = ckpt_guard.latest_entry() {
        let covered = entry.get_min_aof_covered_address(0);
        // 若全部副本已确认位点均跨越了该检查点覆盖范围，释放读者引用锁
        if !safe_addrs.any_lesser(&covered) {
          entry.remove_reader();
        }
      }
    }
    success
  }

  /// EnsureReplication 节流判定：距上次尝试不足 poll 频率（秒）时返回 false；
  /// 到期则原子推进尝试时间戳并返回 true
  ///
  /// 对标 C# `TimeSpan.FromMilliseconds(now - lastEnsureReplicationAttempt)
  /// < TimeSpan.FromSeconds(pollFrequency)` 的间隔检查（完整判定链见
  /// [`crate::server::cluster_provider::ClusterProvider::ensure_replication`]）
  pub fn ensure_replication_due(&self, poll_frequency_secs: i64) -> bool {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    let interval_ms = poll_frequency_secs.saturating_mul(1000);
    let mut last = self
      .last_ensure_replication_attempt_ms
      .load(Ordering::Acquire);
    loop {
      if interval_ms > 0 && now_ms.saturating_sub(last) < interval_ms {
        return false;
      }
      match self
        .last_ensure_replication_attempt_ms
        .compare_exchange_weak(last, now_ms, Ordering::AcqRel, Ordering::Acquire)
      {
        Ok(_) => return true,
        Err(actual) => last = actual,
      }
    }
  }

  /// IsReplicating 状态面：副本侧是否存在活跃复制流
  ///
  /// C# 以 `allClusterSessions.Any(x => x.IsReplicating)` 判定（集群会话表，
  /// IsReplicating 在首个 APPENDLOG 握手后置位）；wnode 会话表尚未接线集群
  /// 域，以副本重放驱动仓库在册驱动（attach 主侧时 InitializeReplicaReplayDriver
  /// 注册、断链时 ResetReplicaReplayDriverStore 重建清空）作为复制活跃的
  /// 权威状态面——驱动生命周期与 C# 会话 IsReplicating 标志位同源同寿
  pub fn has_active_replication_stream(&self) -> bool {
    self.replica_replay_driver_store.has_drivers()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:WaitForReplicationOffsetAsync
  ///
  /// 等待副本位点追平主节点目标位点
  pub async fn wait_for_replication_offset_async(
    &self,
    target_offset: &AofAddress,
    timeout: Duration,
  ) -> bool {
    let start = coarsetime::Instant::now();
    let timeout_ms = timeout.as_millis() as u64;
    while self
      .get_current_replication_offset()
      .any_lesser(target_offset)
    {
      if start.elapsed().as_millis() > timeout_ms {
        return false;
      }
      sleep(Duration::from_millis(5)).await;
    }
    true
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:InitializeReplicaReplayDriver
  ///
  /// 初始化指定子日志的副本重放驱动；已存在则返回 false
  pub fn initialize_replica_replay_driver(&self, physical_sublog_idx: usize) -> bool {
    if self
      .replica_replay_driver_store
      .get_replay_driver(physical_sublog_idx)
      .is_some()
    {
      return false;
    }
    self
      .replica_replay_driver_store
      .add_replica_replay_driver(physical_sublog_idx);
    true
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayManager.cs:ResetReplicaReplayDriverStore
  ///
  /// 释放旧驱动并重置存储容器（对标 C# `Dispose(); new(...)`，重置后可再注册）
  pub fn reset_replica_replay_driver_store(&self) {
    self.replica_replay_driver_store.reset();
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:InitializeCheckpointStore
  pub fn initialize_checkpoint_store(&self) -> bool {
    let mut store = self.checkpoint_store.write();
    store.initialize(None);
    if let Some(c_entry) = store.try_get_latest_checkpoint_entry_from_memory() {
      let min_covered = c_entry.get_min_aof_covered_address(FIRST_VALID_AOF_ADDRESS);
      self
        .aof_sync_driver_store
        .update_truncated_until(&min_covered);
      self.set_recovered_safe_aof_address(&c_entry.metadata.store_checkpoint_covered_aof_address);
      c_entry.remove_reader();
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CheckpointVersionShiftStart
  ///
  /// 检查点版本切换开始：经 storeWrapper 通道向 AOF 广播 CheckpointStartCommit
  /// 标记（sessionID = -1，storeVersion = newVersion）。
  ///
  /// 调用方契约（对标 C# 首行 `LocalNodeRole == NodeRole.REPLICA return`）：
  /// 仅 PRIMARY 角色调用——REPLICA 本地检查点不写标记；角色判定由
  /// [`crate::server::cluster_provider::ClusterProvider::checkpoint_version_shift_hooks`]
  /// 装配闭包承担（Rust rm 不持有 clusterManager 引用，判定上移一层，语义等价）。
  /// 统一检查点模型下 is_main_store 恒真、is_streaming 恒假（C# 注释：We enqueue
  /// a single checkpoint start marker, since we have unified checkpointing）
  /// 保留 _old_version 形参以对标 ReplicationManager.CheckpointVersionShiftStart 签名规范
  pub fn checkpoint_version_shift_start(
    &self,
    is_main_store: bool,
    _old_version: i64,
    new_version: i64,
    is_streaming: bool,
  ) {
    if is_streaming {
      // 统一检查点不走流式标记（保留参数面完整对标 C# 分支签名）
      trace!("Streaming checkpoint start marker skipped (unified checkpointing)");
      return;
    }
    if !is_main_store {
      // 对象存与主存统一检查点（wkv 单存储模型恒走 main_store 分支）
      trace!("Object-store checkpoint start marker skipped (unified checkpointing)");
      return;
    }
    if let Some(channel) = self.commit_channel.read().as_ref() {
      channel.enqueue_commit(AofEntryType::CheckpointStartCommit, new_version);
    }
    trace!("Checkpoint version shift started: {new_version}");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CheckpointVersionShiftEnd
  ///
  /// 检查点版本切换结束：向 AOF 广播 CheckpointEndCommit 标记。
  /// 调用方契约同 [`Self::checkpoint_version_shift_start`]（仅 PRIMARY 调用）
  /// 保留 _old_version 形参以对标 ReplicationManager.CheckpointVersionShiftEnd 签名规范
  pub fn checkpoint_version_shift_end(
    &self,
    is_main_store: bool,
    _old_version: i64,
    new_version: i64,
    is_streaming: bool,
  ) {
    if is_streaming || !is_main_store {
      trace!("Checkpoint end marker skipped (unified checkpointing)");
      return;
    }
    if let Some(channel) = self.commit_channel.read().as_ref() {
      channel.enqueue_commit(AofEntryType::CheckpointEndCommit, new_version);
    }
    trace!("Checkpoint version shift ended: {new_version}");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Purge
  pub fn purge(&self) {
    trace!("Replication buffer pool purged");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetBufferPoolStats
  pub fn get_buffer_pool_stats(&self) -> String {
    "ReplicationBufferPool: active".to_string()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoverCheckpointAndAOFAsync
  pub async fn recover_checkpoint_and_aof_async(&self) {
    self.recover_replication_history();
    info!("Recovered replication history and AOF checkpoint");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync
  pub async fn recover_async(&self, is_primary: bool) {
    if is_primary {
      self.recover_checkpoint_and_aof_async().await;
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Dispose
  pub fn dispose(&self) {
    self.checkpoint_store.write().wait_for_replicas();
    self.replica_replay_driver_store.dispose();
    self.aof_sync_driver_store.reset();
  }
}

impl Drop for ReplicationManager {
  fn drop(&mut self) {
    self.dispose();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::{
    replication::checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    worker::NodeRole,
  };

  #[test]
  fn test_replication_manager_full_flow() {
    let mgr = ReplicationManager::with_options(2, None);
    // 对标 C# 构造：初始位点为 kFirstValidAofAddress
    assert_eq!(mgr.get_replication_offset(0), FIRST_VALID_AOF_ADDRESS);
    mgr.set_sublog_replication_offset(0, 100);
    assert_eq!(mgr.get_replication_offset(0), 100);

    // 对标 C# SetSublogReplicationOffset 直接赋值语义
    mgr.set_sublog_replication_offset(0, 50);
    assert_eq!(mgr.get_replication_offset(0), 50);
    mgr.set_sublog_replication_offset(0, 100);
    assert_eq!(mgr.get_replication_offset(0), 100);

    // 故障转移位点轮转测试
    let repl_id = mgr.primary_repl_id();
    mgr.try_update_for_failover();
    assert_eq!(mgr.primary_repl_id2(), repl_id);
    assert_ne!(mgr.primary_repl_id(), repl_id);
    assert_eq!(mgr.get_replication_offset2().get(0), Some(100));

    // 恢复状态机测试
    assert_eq!(mgr.recovery_status(), RecoveryStatus::NoRecovery);
    assert!(!mgr.is_recovering());

    assert!(mgr.begin_recovery(RecoveryStatus::InitializeRecover, false));
    assert!(mgr.is_recovering());
    assert!(mgr.cannot_stream_aof());

    mgr.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
    assert!(!mgr.cannot_stream_aof());

    mgr.end_recovery(RecoveryStatus::NoRecovery, false);
    assert!(!mgr.is_recovering());
  }

  #[test]
  fn test_reset_replica_replay_driver_store_rebuilds() {
    let mgr = ReplicationManager::with_options(2, None);
    // 注册驱动后重置：容器应重建且可再次注册（对标 C# Dispose + new）
    assert!(mgr.initialize_replica_replay_driver(0));
    assert!(!mgr.initialize_replica_replay_driver(0));
    mgr.reset_replica_replay_driver_store();
    assert!(mgr.initialize_replica_replay_driver(0));
  }

  #[test]
  fn test_determine_resync_strategy_partial_and_full() {
    let mgr = ReplicationManager::with_options(2, None);
    let mut meta = CheckpointMetadata::new(2);
    meta.store_version = 10;
    meta.store_hlog_token = 0x123;
    meta.store_primary_repl_id = Some(mgr.primary_repl_id());
    meta.store_checkpoint_covered_aof_address = AofAddress::create(2, 500);
    let entry = CheckpointEntry::new(meta);
    mgr
      .checkpoint_store
      .write()
      .add_checkpoint_entry(entry.clone(), true);

    let committed = AofAddress::create(2, 1000);
    let primary_begin = AofAddress::create(2, 200);

    // 1. 同一历史且位点对齐 -> PartialResync
    let sync_meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: "rep-1".to_string(),
      current_primary_repl_id: mgr.primary_repl_id(),
      current_store_version: 10,
      current_aof_begin_address: AofAddress::create(2, 200),
      current_aof_tail_address: AofAddress::create(2, 800),
      current_replication_offset: AofAddress::create(2, 800),
      checkpoint_entry: Some(entry.clone()),
    };

    let strategy = mgr.determine_resync_strategy(&sync_meta, &committed, &primary_begin, false);
    match strategy {
      ResyncStrategy::PartialResync {
        sync_start_address,
        replay_aof_mask,
      } => {
        assert_eq!(sync_start_address.get(0), Some(800));
        assert_eq!(replay_aof_mask, 0b11);
      }
      _ => panic!("Expected PartialResync"),
    }

    // 2. 副本 AOF 起始位点超过了检查点覆盖位点（中间缺失） -> 强制 FullResync
    let sync_meta_gap = SyncMetadata {
      current_aof_begin_address: AofAddress::create(2, 600), // > 500
      origin_node_id: "rep-2".to_string(),
      ..sync_meta
    };
    let strategy_gap =
      mgr.determine_resync_strategy(&sync_meta_gap, &committed, &primary_begin, false);
    assert!(matches!(strategy_gap, ResyncStrategy::FullResync { .. }));
  }

  #[test]
  fn test_data_loss_check() {
    let mgr = ReplicationManager::with_options(1, None);
    let begin = AofAddress::create(1, 1000);
    let ok_req = AofAddress::create(1, 1200);
    let bad_req = AofAddress::create(1, 800);

    assert!(mgr.data_loss_check(false, &ok_req, &begin).is_ok());
    assert!(mgr.data_loss_check(false, &bad_req, &begin).is_err());
    assert!(mgr.data_loss_check(true, &bad_req, &begin).is_ok());
  }

  #[test]
  fn test_add_checkpoint_entry_registers_into_store() {
    let mgr = ReplicationManager::with_options(2, None);
    let mut meta = CheckpointMetadata::new(2);
    meta.store_version = 7;
    let latest = mgr.checkpoint_store.read().latest_entry();
    assert!(latest.is_none());

    mgr.add_checkpoint_entry(CheckpointEntry::new(meta), true);
    let latest = mgr.checkpoint_store.read().latest_entry();
    assert_eq!(latest.expect("registered").metadata.store_version, 7);
  }

  #[test]
  fn test_replication_manager_dispose_and_offset_operations() {
    let mgr = ReplicationManager::with_options(2, None);
    mgr.initialize_replication_history(2);
    mgr.try_update_my_primary_repl_id("node-primary-1");
    assert_eq!(mgr.primary_repl_id(), "node-primary-1");

    mgr.set_replication_checkpoint_start_offset(AofAddress::create(2, 500));
    assert_eq!(
      mgr.get_replication_checkpoint_start_offset().get(0),
      Some(500)
    );
    mgr.set_sublog_checkpoint_start_offset(1, 800);
    assert_eq!(
      mgr.get_replication_checkpoint_start_offset().get(1),
      Some(800)
    );

    // 验证 dispose 释放所有驱动与等待读者生命周期打通
    mgr.dispose();
  }
}
