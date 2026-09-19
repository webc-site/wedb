use std::{
  fs::metadata,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
  },
  time::Duration,
};

use compio::time::timeout;
use crossfire::oneshot::{TxOneshot, oneshot};
use event_listener::{Event, EventListener};
use futures_util::future::{Either, select};
use log::{error, info, trace, warn};
use parking_lot::{Mutex, RwLock};
use waof::{AofAddress, AofEntryType};
use wbase::{
  pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE, LimitedFixedBufferPool},
  time,
};
use wcpr::latest_checkpoint_meta;
use wnode::{aof::GarnetLog, database::checkpoint_version};

use crate::server::replication::{
  aof_sync_driver::{AofSyncDriverStore, ReplicaRoleInfo},
  checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  checkpoint_store::CheckpointStore,
  diskless_replication::ReplicationSyncManager,
  receive_checkpoint_handler::ReceiveCheckpointHandler,
  recovery_status::RecoveryStatus,
  replica_replay_driver_store::ReplicaReplayDriverStore,
  replica_replay_task::ReplayAssets,
  replication_history::ReplicationHistory,
  store_commit::StoreCommitFn,
  sync_metadata::SyncMetadata,
};

/// INFO CINFO 检查点缺席形态（对标 C# "(empty)"，多处复用一处定义）
const EMPTY_CHECKPOINT_INFO: &str = "(empty)";

/// 异步等待副本位点追平的 oneshot 契约（对标 Garnet TaskCompletionSource）
struct OffsetWaiter {
  target: AofAddress,
  tx: Option<TxOneshot<()>>,
}

/// 主备数据同步协商策略结果（磁盘臂见 [`ReplicationManager::disk_resync_strategy`]，
/// 无盘臂见 [`ReplicationManager::diskless_resync_strategy`]）
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

/// 检查点下发动作与 AOF 接续位点协商中间态（两条策略臂共用的判定素材）
struct ResyncNegotiation {
  /// C# skipLocalMainStoreCheckpoint：本地检查点条目无需向副本下发
  skip_local_checkpoint: bool,
  /// 副本 AOF 位点与主端可服务区间之间存在断层，无法增量接续
  is_partial_possible: bool,
  replay_aof_mask: u64,
  sync_start_address: AofAddress,
}

/// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationManager
///
/// 管理 AOF 增量追踪、主备位点同步、副本复制会话、故障转移位点轮转与全链路自愈状态机
pub struct ReplicationManager {
  pub replication_offset: RwLock<AofAddress>,
  pub replication_checkpoint_start_offset: RwLock<AofAddress>,
  pub current_replication_config: RwLock<ReplicationHistory>,
  /// replication.conf 落盘互斥（C# ReplicationHistoryManager.cs:FlushConfig 的
  /// `lock (this)`）：序列化 + 写设备整段互斥，任一时刻设备上恒为某单一完整版本
  history_flush_lock: Mutex<()>,
  pub primary_sync_last_timestamp: AtomicI64,
  pub current_recovery_status: RwLock<RecoveryStatus>,
  pub store_current_safe_aof_address: RwLock<AofAddress>,
  pub store_recovered_safe_aof_address: RwLock<AofAddress>,
  pub checkpoint_store: Arc<RwLock<CheckpointStore>>,
  pub aof_sync_driver_store: Arc<AofSyncDriverStore>,
  pub replica_replay_driver_store: Arc<ReplicaReplayDriverStore>,
  /// 检查点网络接收处理器（C# ReplicationManager.recvCheckpointHandler
  /// 字段；activeSink 单文件状态机，同步尝试间经 [`Self::reset_recv_ckpt`] 置换）
  pub recv_checkpoint_handler: ReceiveCheckpointHandler,
  /// 复制网络缓冲池（对标 C# networkPool：NetworkBufferSettings.CreateBufferPool）
  pub network_pool: Arc<LimitedFixedBufferPool>,
  pub config_path: Option<PathBuf>,
  pub sublog_count: usize,
  /// 快照根目录（C# 构造期经 ClusterProvider 持 CheckpointDir 的承接；
  /// 磁盘检查点探测 [`Self::get_latest_checkpoint_from_disk_info`] 的扫盘源，
  /// 集群装配期注入，未注入 = 无磁盘视图）
  pub checkpoint_dir: RwLock<Option<PathBuf>>,
  /// storeWrapper 提交标记写入通道（对标 C# rm 经 clusterProvider.storeWrapper
  /// .EnqueueCommit 写检查点标记；集群装配期注入，见 store_commit 模块）
  pub commit_channel: RwLock<Option<StoreCommitFn>>,
  /// 上次 EnsureReplication 尝试时间戳（毫秒；C# lastEnsureReplicationAttempt）
  pub last_ensure_replication_attempt_ms: AtomicI64,
  /// 位点推进精确事件唤醒队列（零轮询消灭 sleep，基于 crossfire::oneshot）
  offset_waiters: Mutex<Vec<OffsetWaiter>>,
  /// 活跃等待者计数（快路径 0 锁快速检查）
  waiters_count: AtomicUsize,
  /// 停机取消面（对标 C# ReplicationManager.cs:27 ctsRepManager——
  /// CancellationTokenSource 的粘滞标志半边，dispose 置位；无超时位点
  /// 等待以「先注册取消 listener、再查本标志」次序消除丢唤醒窗口）
  cancelled: AtomicBool,
  /// 停机取消面唤醒事件（ctsRepManager.Cancel 的唤醒半边）：notify 打断
  /// 已注册的在途无超时位点等待，按 C# :572 口径以 -1 位点收口
  cancel_event: Event,
  /// 副本重放应用资产（对标 C# rm 构造期 recordToAof:false 的 aofProcessor
  /// 与 storeWrapper 可达面；rust 装配期差异同 set_commit_channel 先例——
  /// aof/store 晚于 rm 构造，wire_replication_data_plane 一次注入）
  pub replay_assets: RwLock<Option<Arc<ReplayAssets>>>,
  /// 主端角色实时谓词（对标 C# CurrentConfig.LocalNodeRole == PRIMARY；
  /// 装配期注入 provider.is_primary 闭包，主端下 ReplicationOffset 走
  /// 本地 AOF 日志尾动态读，副本下走 replication_offset 字段。日志尾句柄
  /// 不另存一份——复用 AofSyncDriverStore.log（C# 同一
  /// storeWrapper.appendOnlyFile.Log 反查面，set_aof 一次注入）
  primary_role: RwLock<Option<Arc<dyn Fn() -> bool + Send + Sync>>>,
  /// 无盘同步会话册子与 leader 编排（对标 C# ReplicationManager.
  /// replicationSyncManager 字段，PrimaryOps/DisklessReplication/
  /// ReplicationSyncManager.cs；构造期随 rm 装配，C# 同位）
  pub replication_sync_manager: Arc<ReplicationSyncManager>,
}

impl Default for ReplicationManager {
  fn default() -> Self {
    Self::new()
  }
}

impl ReplicationManager {
  /// 创建新的复制管理器实例（默认配置，无持久化目录）
  pub fn new() -> Self {
    Self::with_options(1, None, false)
  }

  /// 创建指定子日志数与持久化路径的复制管理器实例
  ///
  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationManager（构造门控段）：
  /// `Recover && replicationConfigDevice.GetFileSize(0) > 0` 才恢复复制历史，
  /// 否则初始化新历史（InitializeReplicationHistory = new + FlushConfig，
  /// 不 recover 时旧 replication.conf 被新历史覆盖）；构造尾段
  /// SetPrimaryReplicationId。recover_or_init 读损坏回退 new + flush 与
  /// C# RecoverReplicationHistory 的 catch 分支等价
  pub fn with_options(sublog_count: usize, config_dir: Option<&Path>, recover: bool) -> Self {
    let sublog_count = sublog_count.max(1);
    let config_path = config_dir.map(|p| p.join("replication.conf"));

    // 对标 C# 构造：replicationOffset 独立于 history 初始化为空日志起点（C#
    // kFirstValidAofAddress=64 源自 TsavoriteLog 设备头区；rust WalLog 无头区，
    // 空日志判据 begin == tail，起点即 0）。恢复场景由
    // RecoverCheckpointAndAOFAsync 重放后覆盖（宿主装配尾段回填，
    // 见 StorageSessionProvider::open_recovered_with_config_and_aof 消费方）
    let initial_offset = AofAddress::create(sublog_count as i32, 0);

    let slf = Self {
      replication_offset: RwLock::new(initial_offset),
      replication_checkpoint_start_offset: RwLock::new(AofAddress::create(sublog_count as i32, 0)),
      current_replication_config: RwLock::new(ReplicationHistory::new(sublog_count)),
      history_flush_lock: Mutex::new(()),
      primary_sync_last_timestamp: AtomicI64::new(0),
      current_recovery_status: RwLock::new(RecoveryStatus::NoRecovery),
      store_current_safe_aof_address: RwLock::new(initial_offset),
      store_recovered_safe_aof_address: RwLock::new(initial_offset),
      checkpoint_store: Arc::new(RwLock::new(CheckpointStore::new(true))),
      aof_sync_driver_store: Arc::new(AofSyncDriverStore::new(sublog_count)),
      replica_replay_driver_store: Arc::new(ReplicaReplayDriverStore::new(sublog_count)),
      recv_checkpoint_handler: ReceiveCheckpointHandler::new(),
      network_pool: LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE),
      config_path,
      sublog_count,
      checkpoint_dir: RwLock::new(None),
      commit_channel: RwLock::new(None),
      last_ensure_replication_attempt_ms: AtomicI64::new(0),
      offset_waiters: Mutex::new(Vec::new()),
      waiters_count: AtomicUsize::new(0),
      cancelled: AtomicBool::new(false),
      cancel_event: Event::new(),
      replay_assets: RwLock::new(None),
      primary_role: RwLock::new(None),
      replication_sync_manager: ReplicationSyncManager::new(),
    };

    // C# 构造门控：Recover 且 replication.conf 非空才恢复历史，否则初始化新历史
    let can_recover = recover
      && slf
        .config_path
        .as_deref()
        .is_some_and(|p| metadata(p).is_ok_and(|m| m.len() > 0));
    if can_recover {
      slf.recover_replication_history();
    } else {
      slf.initialize_replication_history(sublog_count);
    }
    slf.set_primary_replication_id();
    slf
  }

  /// 注入 storeWrapper 提交标记写入回调（集群装配期一次注入，对标 C# 委托字段装配）
  pub fn set_commit_channel(&self, commit: Option<StoreCommitFn>) {
    *self.commit_channel.write() = commit;
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:InitializeReplicationHistory
  pub fn initialize_replication_history(&self, aof_physical_sublog_count: usize) {
    let mut config = self.current_replication_config.write();
    *config = ReplicationHistory::new(aof_physical_sublog_count);
    drop(config);
    self.flush_config();
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:RecoverReplicationHistory
  ///
  /// 从 replication.conf 恢复复制历史（读损坏回退初始化新历史 + 落盘）；
  /// 仅构造期门控（[`Self::with_options`]）与测试调用，size 门控由构造方判定
  pub fn recover_replication_history(&self) {
    if let Some(ref path) = self.config_path {
      let mut config = self.current_replication_config.write();
      *config = ReplicationHistory::recover_or_init(path, self.sublog_count);
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FlushConfig
  ///
  /// 落盘口单一互斥（C# `lock (this)`）：先占 [`Self::history_flush_lock`] 再读当前
  /// 历史，序列化与写设备整段在锁内——后到者必待前者 rename 完成后才取值落盘，
  /// 故设备上恒为某单一完整版本、落盘序与内存版本序一致（读锁仅取值拷贝，
  /// 不跨 IO 持有，互斥由本锁承担）
  pub fn flush_config(&self) {
    let Some(ref path) = self.config_path else {
      return;
    };
    let _flush_guard = self.history_flush_lock.lock();
    let config = self.current_replication_config.read().copy();
    if let Err(e) = config.flush_to_file(path) {
      error!("Failed to flush replication history: {e}");
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateMyPrimaryReplId
  pub fn try_update_my_primary_repl_id(&self, primary_replication_id: &str) {
    let mut config = self.current_replication_config.write();
    *config = config.update_replication_id(primary_replication_id);
    drop(config);
    self.flush_config();
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:AddCheckpointEntry
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

  /// 注入主端角色实时谓词（对标 C# rm 读位点时反查
  /// clusterManager.CurrentConfig.LocalNodeRole == PRIMARY；rust 依赖方向反转，
  /// ClusterProvider::set_aof 装配期一次注入）。日志尾句柄不在此注入，
  /// 复用同一装配点写好的 AofSyncDriverStore.log，全 rm 只留一份本地 AOF 句柄。
  pub fn set_primary_role_source(&self, primary_role: Option<Arc<dyn Fn() -> bool + Send + Sync>>) {
    *self.primary_role.write() = primary_role;
  }

  /// 本地是否主端角色（对标 C# CurrentConfig.LocalNodeRole == PRIMARY；
  /// 未注入角色源时按主处理，对齐 ClusterProvider::is_primary 的
  /// unwrap_or(true)——无集群管理器的独立/未装配形态默认主）
  #[inline]
  fn is_primary_role(&self) -> bool {
    self.primary_role.read().as_ref().is_none_or(|f| f())
  }

  /// 本地 AOF 物理日志句柄（None = AOF 门控未点亮，对标 C# !EnableAOF）。
  /// 句柄由 ClusterProvider::set_aof 注入到 AofSyncDriverStore.log，即 C#
  /// storeWrapper.appendOnlyFile.Log 的唯一反查面——rm 内不再存第二份。
  #[inline]
  fn local_aof_log(&self) -> Option<Arc<GarnetLog>> {
    self.aof_sync_driver_store.log.read().clone()
  }

  /// 本地 AOF 日志尾（对标 C# storeWrapper.appendOnlyFile.Log.TailAddress；
  /// AOF 门控未点亮 = None，句柄同 C# 为一处注入）。私有读原语：只服务本
  /// 类型的角色分支 getter，INFO / gossip / failover 应答等消费方一律走
  /// [`Self::get_current_replication_offset`]，不得绕过角色分支直取日志尾。
  #[inline]
  fn replication_log_tail(&self) -> Option<AofAddress> {
    self.local_aof_log().map(|l| l.tail_address())
  }

  /// 本地指定子日志 AOF 尾（对标 C# appendOnlyFile.Log.GetTailAddress(sublogIdx)）
  /// 私有读原语，约束同上（唯一出口 [`Self::get_replication_offset`]）
  #[inline]
  fn replication_log_tail_at(&self, sublog_idx: usize) -> Option<i64> {
    self.local_aof_log().map(|l| l.get_tail_address(sublog_idx))
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetReplicationOffset
  ///
  /// 获取指定子日志的复制偏移。主端角色且 AOF 在场时动态读日志尾
  /// （对标 C# PRIMARY 分支 appendOnlyFile.Log.GetTailAddress(sublogIdx)），
  /// 副本或未装配动态源时读 replication_offset 字段（副本重放链权威回推）。
  pub fn get_replication_offset(&self, sublog_idx: usize) -> i64 {
    if self.is_primary_role()
      && let Some(tail) = self.replication_log_tail_at(sublog_idx)
    {
      return tail;
    }
    self.replication_offset.read().get(sublog_idx).unwrap_or(0)
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:SetSublogReplicationOffset
  ///
  /// 设置指定子日志的复制偏移（对标 C# 直接赋值）。推进源 = 副本重放链
  /// 应用记录进存储后的权威回推（applied 语义，见 replica_replay_task
  /// consume_chunk；退化形态——重放资产缺席时——由副本会话流式落盘面
  /// 直推 enqueued，登记见 task/done/replica-offset-semantics.md）
  pub fn set_sublog_replication_offset(&self, sublog_idx: usize, offset: i64) {
    self.replication_offset.write().set(sublog_idx, offset);
    self.wake_offset_waiters();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetSublogReplicationOffset
  ///
  /// 获取指定子日志的复制偏移（别名；C# 消费方 TryApplyPendingPulse 追平
  /// 守卫由 replica_replay_driver 消化，syncReplay 位点校验折叠进
  /// ThrottlePrimary 等待）
  pub fn get_sublog_replication_offset(&self, sublog_idx: usize) -> i64 {
    self.get_replication_offset(sublog_idx)
  }

  /// 注入副本重放应用资产（wire_replication_data_plane 装配期一次调用；
  /// None = 退化装配，副本位点保持 enqueued 形态）
  pub fn set_replay_assets(&self, assets: Option<Arc<ReplayAssets>>) {
    *self.replay_assets.write() = assets;
  }

  /// 副本重放应用资产句柄
  pub fn replay_assets(&self) -> Option<Arc<ReplayAssets>> {
    self.replay_assets.read().clone()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset
  ///
  /// 获取当前完整的 AOF 复制地址。主端角色且 AOF 在场时动态读日志尾
  /// （对标 C# PRIMARY 分支 appendOnlyFile.Log.TailAddress），副本或未装配
  /// 动态源时读 replication_offset 字段。INFO / CLUSTER NODES / gossip /
  /// failover 停写应答的位点全部单点走本方法，不再各写一份读法。
  pub fn get_current_replication_offset(&self) -> AofAddress {
    if self.is_primary_role()
      && let Some(tail) = self.replication_log_tail()
    {
      return tail;
    }
    *self.replication_offset.read()
  }

  /// 提交尾复制地址：[`Self::try_update_for_failover`] 的取值源分解——AOF 在场
  /// 动态读 storeWrapper.appendOnlyFile.Log.CommittedUntilAddress，AOF 缺席
  /// 回退 replication_offset 字段。C# 侧 TryUpdateForFailover 映射登记在该方法。
  fn get_committed_replication_offset(&self) -> AofAddress {
    if let Some(log) = self.local_aof_log() {
      return log.committed_until_address();
    }
    *self.replication_offset.read()
  }

  /// 设置当前完整的 AOF 复制地址（对标 C# 直接赋值）
  pub fn set_current_replication_offset(&self, offset: AofAddress) {
    *self.replication_offset.write() = offset;
    self.wake_offset_waiters();
  }

  /// 唤醒所有位点已追平的异步等待者（零锁争用，精确唤醒）
  fn wake_offset_waiters(&self) {
    // 快路径：无等待者单指令返回，消除高频复制流的热路径锁争用
    if self.waiters_count.load(Ordering::Acquire) == 0 {
      return;
    }
    let current = self.get_current_replication_offset();
    let mut waiters = self.offset_waiters.lock();
    waiters.retain_mut(|w| {
      if let Some(ref tx) = w.tx
        && tx.is_disconnected()
      {
        self.waiters_count.fetch_sub(1, Ordering::Release);
        return false;
      }
      if !current.any_lesser(&w.target) {
        if let Some(tx) = w.tx.take() {
          tx.send(());
        }
        self.waiters_count.fetch_sub(1, Ordering::Release);
        false
      } else {
        true
      }
    });
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset2
  ///
  /// 获取旧主历史复制偏移（failover 后有效）
  pub fn get_replication_offset2(&self) -> AofAddress {
    self.current_replication_config.read().replication_offset2
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:PrimaryReplId
  ///
  /// 获取主复制 ID
  pub fn primary_repl_id(&self) -> String {
    self
      .current_replication_config
      .read()
      .primary_repl_id
      .clone()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:PrimaryReplId2
  ///
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
    let now_ms = time::now_ms() as i64;
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
      let now_ms = time::now_ms() as i64;
      (now_ms.saturating_sub(last)) / 1000
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoveryStatus
  ///
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

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CannotStreamAOF
  ///
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
    // 对标 C# TryUpdateForFailover 取 storeWrapper.appendOnlyFile.Log
    // .CommittedUntilAddress（动态提交尾），不读冻结的 replication_offset 字段
    let cur_offset = self.get_committed_replication_offset();
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

  /// libs/server/GarnetCheckpointManager.cs:SetRecoveredSafeAofAddress
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

  /// 注入快照根目录（集群装配期一次注入，对标 C# rm 构造期持 CheckpointDir；
  /// 同步注入 checkpoint_store，淘汰与孤儿清理由此获得物理删除能力）
  pub fn set_checkpoint_dir(&self, dir: PathBuf) {
    *self.checkpoint_dir.write() = Some(dir.clone());
    self.checkpoint_store.write().set_checkpoint_dir(dir);
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointFromDiskInfo
  ///
  /// 获取磁盘最新检查点格式化信息（专供 Redis `INFO CINFO` 监控段中的
  /// `disk_checkpoint_entry` 指标）
  ///
  /// 【对标 C# 原型】：
  /// C# 经 `checkpointStore.GetLatestCheckpointFromDiskInfo` 触发底层 Tsavorite
  /// 存储引擎扫盘，读取快照文件并反序列化其中的 Cookie，输出
  /// `cEntry.ToString()`（CheckpointMetadata 键值串）；若无快照或异常则
  /// 捕获后返回 `"(empty)"`。
  ///
  /// 【wedb 架构演进与差异】：
  /// 1. 快照磁盘模型由 `wcpr` 承接：快照按单调自增 token（u128）命名，
  ///    元数据文件 `checkpoint_<token>.meta` 经 bitcode 封签落盘。
  /// 2. Cookie 不落盘：wedb 架构公理明确定义 `cookie 属复制域不落地`
  ///    （见 `wcpr/src/meta.rs`），storeVersion / storePrimaryReplId 仅存于
  ///    内存检查点仓库，磁盘快照中不存在——故输出以 token 充当
  ///    storeHlogToken / storeIndexToken，`checkpoint_aof_address` 充当
  ///    storeCheckpointCoveredAofAddress。
  /// 3. 检查点目录经 [`Self::set_checkpoint_dir`] 装配期注入（C# rm 构造期
  ///    即持 CheckpointDir；rust rm 构造早于目录装配，注入时序同
  ///    set_commit_channel 先例）。
  ///
  /// 目录未注入、目录无快照、读取或解码失败一律回退 `"(empty)"`（对标
  /// C# catch 分支）；token 十六进制形态与内存条目 Display 同族。
  pub fn get_latest_checkpoint_from_disk_info(&self) -> String {
    self
      .checkpoint_dir
      .read()
      .as_deref()
      .map(latest_checkpoint_meta_info)
      .unwrap_or_else(|| EMPTY_CHECKPOINT_INFO.to_string())
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

  /// AOF 接续位点与检查点下发协商（磁盘/无盘两条策略臂共用的判定素材）
  ///
  /// 在 garnet 中的相对路径: libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress
  ///
  /// C# 该函数按 AofPhysicalSublogCount 逐子日志算出 replayAOFMap 并推进
  /// checkpointAofBeginAddress，recoverFromRemote（= !skipLocalMainStoreCheckpoint）
  /// 由调用侧 DiskbasedReplication/ReplicaSyncSession.cs ValidateMetadata 给出；
  /// rust 把同一组入参下的两布尔与位点推进一次算齐，供两条策略臂各取所需，
  /// partial/full、needFullSync 的判定取向不落在此处。
  ///
  /// 与 C# 的形态差异：C# 副本起始位点越过检查点覆盖线时仅记日志（该子日志位
  /// 不进 replayAOFMap），副本尾位点低于覆盖线且非 FastAofTruncate 时抛异常
  /// 终止本次同步；rust 无异常通道，两形态统一收敛为 is_partial_possible = false。
  fn negotiate_resync(
    &self,
    replica_meta: &SyncMetadata,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncNegotiation {
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
    let mut is_partial_possible = true;
    let mut sync_start_address = if let Some(ref lc) = local_checkpoint {
      lc.get_min_aof_covered_address(0)
    } else {
      *primary_aof_begin
    };

    // 4. 若不需下发检查点快照，逐子日志判定 AOF 增量流接续位点
    if skip_local_checkpoint {
      let repl_offset2 = self.get_replication_offset2();

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
    }

    ResyncNegotiation {
      skip_local_checkpoint,
      is_partial_possible,
      replay_aof_mask,
      sync_start_address,
    }
  }

  /// 磁盘链路重同步策略判定：下发本地检查点快照，还是从协商位点直推 AOF 增量
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ValidateMetadata
  ///
  /// C# 磁盘臂的判据只有两条：ValidateMetadata 的 skipLocalMainStoreCheckpoint
  /// （本地无检查点条目 / storeHlogToken 为 0 / 同检查点历史且两侧 CheckpointEntry
  /// 的 storeVersion 相等）决定要不要下发快照，ComputeAofSyncReplayAddress 决定
  /// 增量流从哪一位点接续；skip 为真且位点可接续即 PartialResync，否则 FullResync。
  ///
  /// 副本 store 版本维度只经 CheckpointEntry.metadata.storeVersion 进入判定：
  /// 磁盘入口 NetworkClusterInitiateReplicaSync
  /// （libs/cluster/Session/RespClusterReplicationCommands.cs:259）与
  /// TryBeginDiskbasedSyncAsync 全程不构造、不读带 store 版本的 SyncMetadata，
  /// 故此臂不引用 SyncMetadata.current_store_version。
  pub fn disk_resync_strategy(
    &self,
    replica_meta: &SyncMetadata,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncStrategy {
    let nego = self.negotiate_resync(
      replica_meta,
      committed_until,
      primary_aof_begin,
      fast_aof_truncate,
    );
    if nego.skip_local_checkpoint && nego.is_partial_possible {
      info!("Disk resync strategy resolved: PartialResync (incremental stream continuation)");
      return ResyncStrategy::PartialResync {
        sync_start_address: nego.sync_start_address,
        replay_aof_mask: nego.replay_aof_mask,
      };
    }
    info!("Disk resync strategy resolved: FullResync (checkpoint snapshot required)");
    ResyncStrategy::FullResync {
      sync_start_address: nego.sync_start_address,
      replay_aof_mask: nego.replay_aof_mask,
    }
  }

  /// 无盘链路重同步策略判定：本会话免快照放行，还是纳入全量扇出
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:NeedToFullSync
  ///
  /// C# 四条件任一成立即全量（needToFullSync 为假的会话在 PrepareForSyncAsync
  /// 里直接 SetStatus(SUCCESS) 摘除，不参与快照扇出）：
  /// 1. 主从历史不一致（PrimaryReplId 与副本上报 currentPrimaryReplId 不等）；
  /// 2. 副本 store 版本 != 主端当下版本（:204 是不等判据，方向非存在性判据，
  ///    主端当下版本由 ReplicationSyncManager.cs:267 自 store.CurrentVersion 取）；
  /// 3. 副本 AOF 尾位点越出主端可服务区间 [Log.BeginAddress, Log.TailAddress]；
  /// 4. 待回放量超 ReplicaDisklessSyncFullSyncAofThreshold。
  ///
  /// 第 4 条在 rust 缺席：仓内无该门限的任何配置面（server_options /
  /// runtime_config 皆无对应项，C# 侧已登记
  /// js/check/ignore/server.yml:ReplicaDisklessSyncFullSyncAofThresholdValue），
  /// 按转写纪律不自造第二套门限常量与默认值，待门限配置单独立项时接上。
  pub fn diskless_resync_strategy(
    &self,
    replica_meta: &SyncMetadata,
    current_store_version: i64,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    primary_aof_tail: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncStrategy {
    let send_main_store = self.primary_repl_id() != replica_meta.current_primary_repl_id
      || replica_meta.current_store_version != current_store_version;
    let out_of_range_aof = replica_meta
      .current_aof_tail_address
      .is_out_of_range(primary_aof_begin, primary_aof_tail);
    let full_sync = send_main_store || out_of_range_aof;

    let nego = self.negotiate_resync(
      replica_meta,
      committed_until,
      primary_aof_begin,
      fast_aof_truncate,
    );
    if full_sync {
      info!("Diskless resync strategy resolved: FullResync (streaming snapshot fan-out required)");
      return ResyncStrategy::FullResync {
        sync_start_address: nego.sync_start_address,
        replay_aof_mask: nego.replay_aof_mask,
      };
    }
    info!("Diskless resync strategy resolved: PartialResync (aof replay from negotiated address)");
    ResyncStrategy::PartialResync {
      sync_start_address: nego.sync_start_address,
      replay_aof_mask: nego.replay_aof_mask,
    }
  }

  /// EnsureReplication 节流**到期判定**（纯读，不消费窗口）：距上次尝试不足
  /// poll 频率（秒）时返回 None；到期返回 Some(observed_last_ms)——本次读到的
  /// 尝试时间戳原值，作为真正发起处 [`Self::try_consume_ensure_replication_window`]
  /// 的 CAS 比对基准
  ///
  /// 对标 C# `ReplicationManager.cs:192` 的 `Volatile.Read`（只取
  /// oldLastEnsureReplicationAttempt）+ `:197-201` 间隔门：判定到期**不**推进
  /// 时间戳，被角色/复制流/failover 门挡回的到期帧不得白吃一个 poll 频率窗口
  /// （完整判定链见
  /// [`crate::server::cluster_provider::ClusterProvider::ensure_replication`]）
  pub fn ensure_replication_due(&self, poll_frequency_secs: i64) -> Option<i64> {
    let now_ms = time::now_ms() as i64;
    let interval_ms = poll_frequency_secs.saturating_mul(1000);
    let observed_last_ms = self
      .last_ensure_replication_attempt_ms
      .load(Ordering::Acquire);
    if interval_ms > 0 && now_ms.saturating_sub(observed_last_ms) < interval_ms {
      return None;
    }
    Some(observed_last_ms)
  }

  /// EnsureReplication 节流**窗口消费**：以到期判定读到的原值为基准 CAS 推进
  /// 尝试时间戳；比对不等（判定与消费之间另有一次尝试在途）返回 false
  ///
  /// 对标 C# `ReplicationManager.cs:251-256`：`Environment.TickCount64` 取新值 +
  /// `Interlocked.CompareExchange` 不等即 bail（another-attempt 语义）。调用点
  /// 必须位于 PreventRoleChange 与 TOCTOU 复检之后——真正发起才消费，false 时
  /// 调用方须 AllowRoleChange 后放弃本轮（C# finally 臂）
  pub fn try_consume_ensure_replication_window(&self, observed_last_ms: i64) -> bool {
    self
      .last_ensure_replication_attempt_ms
      .compare_exchange(
        observed_last_ms,
        time::now_ms() as i64,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
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
  /// 无内层超时的位点追平等待（对标 C#：调用侧 BlockingWait 不带超时，
  /// 唯一提前退出面是 ctsRepManager 停机取消）。追平返回当下位点
  /// （C# `return ReplicationOffset`），停机按 C# :572 返回 Create(
  /// AofPhysicalSublogCount, -1)。调用侧限时单点由 cluster_timeout 兜底
  /// （PrimaryFailoverSession.cs:22 WaitAsync(clusterTimeout)），本函数
  /// 不得叠加第二层内超时常量
  pub async fn wait_for_replication_offset_async(&self, target_offset: &AofAddress) -> AofAddress {
    // C# while 环首判对位：AnyLesser 假分支先于取消检查，追平即当下位点直返
    if !self
      .get_current_replication_offset()
      .any_lesser(target_offset)
    {
      return self.get_current_replication_offset();
    }
    // 先注册取消 listener（event_listener 5.4 listen() 创建即插入等待链），
    // 再读停机粘滞标志：观测到未置位时 dispose 的置位与 notify 必晚于插入，
    // 通知必达本 listener，无丢唤醒窗口；已置位即刻按 C# :572 返 -1 位点
    let mut cancel = self.cancel_event.listen();
    if self.cancelled.load(Ordering::SeqCst) {
      return self.cancelled_offset();
    }
    if self
      .wait_for_replication_offset_async_with_abort(target_offset, None, &mut cancel)
      .await
    {
      self.get_current_replication_offset()
    } else {
      self.cancelled_offset()
    }
  }

  /// 停机应答位点（对标 C# WaitForReplicationOffsetAsync :572 的
  /// AofAddress.Create(AofPhysicalSublogCount, -1)：各槽 -1 为明确的
  /// 未同步哨兵，调用方比对判定永不将其误认为追平）
  fn cancelled_offset(&self) -> AofAddress {
    AofAddress::create(self.sublog_count as i32, -1)
  }

  /// 等待副本位点追平目标位点（带中断面）
  ///
  /// duration 为 None 即无界等待，只与取消 listener 竞速（对标 C#
  /// WaitForReplicationOffsetAsync 轮询环本体）；Some 即有界等待（对标
  /// 轮询环外叠 WaitAsync(timeout, token) 的副本侧形态）。listener 须由
  /// 调用方预注册传入（listen() 创建即插入等待链），无界臂借此完成
  /// 「注册先于粘滞标志检查」的次序契约，杜绝 dispose 与挂起之间的丢唤醒。
  /// 取消触发或超时即以未追平收口并注销在途等待项。取消面的必要性对位：
  /// C# 轮询环天然观察超时、RPC 等待由 cts.Cancel 打断（FailoverManager.cs:
  /// TryAbortReplicaFailover → Dispose → cts.Cancel），rust 事件驱动 oneshot
  /// 等待必须由已注册 listener 精准唤醒，否则 abort 后在途位点等待挂满
  /// 剩余超时（failover abort 响应性缺陷，见 task/ing/failover-abort.md）
  pub async fn wait_for_replication_offset_async_with_abort(
    &self,
    target_offset: &AofAddress,
    duration: Option<Duration>,
    abort: &mut EventListener,
  ) -> bool {
    // 1. 快路径：位点已追平直接就绪，零分配
    if !self
      .get_current_replication_offset()
      .any_lesser(target_offset)
    {
      return true;
    }

    let (tx, rx) = oneshot();
    {
      let mut waiters = self.offset_waiters.lock();
      // 双检：登记锁间隙到来的位点推进
      if !self
        .get_current_replication_offset()
        .any_lesser(target_offset)
      {
        return true;
      }
      waiters.push(OffsetWaiter {
        target: *target_offset,
        tx: Some(tx),
      });
      self.waiters_count.fetch_add(1, Ordering::Release);
    }

    // 2. 挂起等待位点精确唤醒与中断 listener 竞速；结果先落定为本站变量，
    //    竞速 future 随语句终结释放（rx 断连）后清扫注销本等待项防泄漏
    //    ——若以 match 穿查 await，临时 future 活到 match 结束，清扫时
    //    is_disconnected 尚不成立，登记项将泄漏至下一次位点推进
    let caught = match duration {
      Some(d) => matches!(timeout(d, select(rx, abort)).await, Ok(Either::Left(_))),
      // 无界臂（C# 轮询环本体）：只与取消 listener 竞速，Left = 位点推进唤醒
      None => matches!(select(rx, abort).await, Either::Left(_)),
    };
    if !caught {
      let mut waiters = self.offset_waiters.lock();
      waiters.retain_mut(|w| {
        if let Some(ref tx) = w.tx
          && tx.is_disconnected()
        {
          self.waiters_count.fetch_sub(1, Ordering::Release);
          return false;
        }
        true
      });
    }
    caught
  }

  /// 是否存在在途位点等待项（诊断与测试观测面：中断/超时收口后应归零）
  pub fn has_offset_waiters(&self) -> bool {
    self.waiters_count.load(Ordering::Acquire) > 0
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayManager.cs:InitializeReplicaReplayDriver
  ///
  /// 初始化指定子日志的副本重放驱动（挂当前重放资产与本管理器弱引用）；
  /// 已存在或驱动仓库已关闭则返回 false
  pub fn initialize_replica_replay_driver(self: &Arc<Self>, physical_sublog_idx: usize) -> bool {
    if self
      .replica_replay_driver_store
      .get_replay_driver(physical_sublog_idx)
      .is_some()
    {
      return false;
    }
    self
      .replica_replay_driver_store
      .add_replica_replay_driver(
        physical_sublog_idx,
        self.replay_assets(),
        Arc::downgrade(self),
      )
      .is_some()
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayManager.cs:ResetReplicaReplayDriverStore
  ///
  /// 释放旧驱动并重置存储容器（对标 C# `Dispose(); new(...)`，重置后可再注册）
  pub fn reset_replica_replay_driver_store(&self) {
    self.replica_replay_driver_store.reset();
  }

  /// 检查点接收状态置换（对标 C# TryReplicateDiskbasedSyncAsync 每次 attach
  /// `recvCheckpointHandler = new(...)` / finally Dispose：弃置残留活跃槽）
  pub fn reset_recv_checkpoint_handler(&self) {
    self.recv_checkpoint_handler.reset();
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:InitializeCheckpointStore
  ///
  /// 扫盘 seed 最新磁盘检查点（C# Initialize 的 GetLatestCheckpointEntryFromDisk
  /// 对位）后初始化内存仓库，并触发除最新条目外的孤儿快照清理。
  /// 磁盘快照无复制域 cookie（`cookie 属复制域不落地`），条目组装同
  /// cluster_provider add_new_checkpoint_entry 口径：store_version 由 token
  /// 派生、hlog/index token 同 token、covered 地址取检查点元数据、repl id 取
  /// 当前主复制 ID（历史已由 replication.conf 恢复）
  pub fn initialize_checkpoint_store(&self) -> bool {
    let mut store = self.checkpoint_store.write();
    let disk_entry = self
      .checkpoint_dir
      .read()
      .as_deref()
      .and_then(|d| latest_checkpoint_meta(d))
      .map(|(token, meta)| {
        let mut metadata = CheckpointMetadata::new(self.sublog_count);
        metadata.store_version = checkpoint_version(token);
        metadata.store_hlog_token = token;
        metadata.store_index_token = token;
        metadata.store_checkpoint_covered_aof_address = AofAddress::create(
          self.sublog_count as i32,
          meta.checkpoint_aof_address.unwrap_or(0) as i64,
        );
        metadata.store_primary_repl_id = Some(self.primary_repl_id());
        CheckpointEntry::new(metadata)
      });
    store.initialize(disk_entry);
    if let Some(c_entry) = store.try_get_latest_checkpoint_entry_from_memory() {
      let min_covered = c_entry.get_min_aof_covered_address(0);
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
  /// 检查点版本切换开始：经 storeWrapper 提交通道向 AOF 广播 CheckpointStartCommit
  /// 标记（sessionID = -1，storeVersion = newVersion）。
  ///
  /// 调用方契约（对标 C# 首行 `LocalNodeRole == NodeRole.REPLICA return`）：
  /// 仅 PRIMARY 角色调用——REPLICA 本地检查点不写标记；角色判定由
  /// [`crate::server::cluster_provider::ClusterProvider`] 的 `wnode::ClusterProvider`
  /// 实现承担（Rust rm 不持 clusterManager 引用，判定上移一层，语义等价）。
  /// 统一检查点模型下不区分 main/object store、不走流式标记（C# 注释：We enqueue
  /// a single checkpoint start marker, since we have unified checkpointing），
  /// 故 is_main_store / is_streaming / oldVersion 形参随流式半接口一并移除
  pub fn checkpoint_version_shift_start(&self, new_version: i64) {
    if let Some(commit) = self.commit_channel.read().as_ref() {
      commit(AofEntryType::CheckpointStartCommit, new_version);
    }
    trace!("Checkpoint version shift started: {new_version}");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CheckpointVersionShiftEnd
  ///
  /// 检查点版本切换结束：向 AOF 广播 CheckpointEndCommit 标记。
  /// 调用方契约与形参收敛同 [`Self::checkpoint_version_shift_start`]（仅 PRIMARY 调用）
  pub fn checkpoint_version_shift_end(&self, new_version: i64) {
    if let Some(commit) = self.commit_channel.read().as_ref() {
      commit(AofEntryType::CheckpointEndCommit, new_version);
    }
    trace!("Checkpoint version shift ended: {new_version}");
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Purge
  ///
  /// 清空复制网络缓冲池内全部闲置缓冲区（C# networkPool.Purge()）
  pub fn purge(&self) {
    self.network_pool.purge();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetBufferPoolStats
  ///
  /// 复制网络缓冲池统计（C# networkPool.GetStats()）
  pub fn get_buffer_pool_stats(&self) -> String {
    format!(
      "max_pool_size={} free_buffers={} borrowed_buffers={} allocated_buffers={}",
      self.network_pool.max_pool_size(),
      self.network_pool.free_count(),
      self.network_pool.borrowed_count(),
      self.network_pool.allocated_count(),
    )
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync
  ///
  /// 复制域启动恢复。与 C# 的差异登记（依赖方向反转）：
  /// - 复制历史恢复在构造期完成（[`Self::with_options`] 门控，对标 C#
  ///   ReplicationManager 构造段 Recover && fileSize > 0），本方法不再重复。
  /// - C# PRIMARY / REPLICA+ClusterReplicaResumeWithData 分支经
  ///   `storeWrapper` 做 checkpoint+AOF 数据面恢复并 `ReplayAOF` 后
  ///   `replicationOffset.SetValue(replayedUntil)` 回填位点；rust rm 不持
  ///   storeWrapper 可达面，数据面恢复与位点回填由 wnode 宿主装配期承接
  ///   （`StorageSessionProvider::open_recovered_with_config_and_aof` + 装配尾段
  ///   set_current_replication_offset，时序同为端点 accept 之前——对齐
  ///   C# StoreWrapper.RecoverAsync 单机分支的结构），本方法仅承接 rm 自有
  ///   的检查点内存索引初始化。
  /// - `RecoverCheckpointAndAOFAsync` 的独立方法已删（原实现仅恢复
  ///   replication history，名实不符）；其 C# 职责按上述拆分归位。
  /// - REPLICA+ClusterReplicaResumeWithData 分支：该配置面未落地，副本
  ///   重启后等待与 primary 重新同步（C# 未配置该项时同语义）。
  pub async fn recover_async(&self, is_primary: bool) {
    if is_primary && !self.initialize_checkpoint_store() {
      warn!("Failed acquiring latest memory checkpoint metadata at RecoverAsync");
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/ReplicationPrimaryAofSync.cs:GetReplicaInfo
  pub fn get_replica_info(&self) -> Vec<ReplicaRoleInfo> {
    let offset = self.get_current_replication_offset();
    self.aof_sync_driver_store.get_replica_info(&offset)
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Dispose
  pub fn dispose(&self) {
    // 对标 C# Dispose:491-493 首两行 `_disposed = true; ctsRepManager.Cancel()`：
    // 先置粘滞标志再 notify（二者次序与等待方「注册先于读标志」构成
    // Dekker 配对，杜绝丢唤醒），在途无超时位点等待按 C# :572 收口
    self.cancelled.store(true, Ordering::SeqCst);
    self.cancel_event.notify(usize::MAX);
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

/// libs/cluster/Server/Replication/CheckpointStore.cs:GetLatestCheckpointFromDiskInfo
///
/// 扫盘读最新有效快照元数据并格式化（wcpr 磁盘模型承接 C# Tsavorite 扫盘，
/// 单点在 [`wcpr::latest_checkpoint_meta`]）；目录无快照、读取或解码失败统一
/// 回退 "(empty)"（对标 C# catch 分支）
fn latest_checkpoint_meta_info(dir: &Path) -> String {
  latest_checkpoint_meta(dir)
    .map(|(token, meta)| {
      format!(
        "storeHlogToken={:x},storeIndexToken={:x},storeCheckpointCoveredAofAddress={}",
        token,
        token,
        meta
          .checkpoint_aof_address
          .map_or_else(|| EMPTY_CHECKPOINT_INFO.to_string(), |a| a.to_string())
      )
    })
    .unwrap_or_else(|| EMPTY_CHECKPOINT_INFO.to_string())
}
