use std::{
  collections::HashMap,
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
use waof::{AofAddress, AofEntryType, WalLog};
use wbase::{
  pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool},
  time,
};
use wcpr::latest_checkpoint_meta;
use wdev::SegmentedDevice;
use wnode::{aof::GarnetLog, database::checkpoint_version};

use crate::server::replication::{
  aof_sync_driver::{AofSyncDriverStore, ReplicaRoleInfo},
  checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  checkpoint_store::CheckpointStore,
  diskless_replication::ReplicationSyncManager,
  error::ReplicationError,
  receive_checkpoint_handler::ReceiveCheckpointHandler,
  recovery_status::RecoveryStatus,
  replica_replay_driver_store::ReplicaReplayDriverStore,
  replica_replay_task::ReplayAssets,
  replica_sync_task_store::ReplicaSyncSessionTaskStore,
  replication_history::{REPLICATION_STATE_FILE, ReplicationHistory},
  store_commit::StoreCommitFn,
  sync_metadata::SyncMetadata,
};

mod checkpoint;
mod history;
mod offsets;
mod readers;
mod recovery;
mod resync;

/// INFO CINFO 检查点缺席形态（对标 C# "(empty)"，多处复用一处定义）
pub(crate) const EMPTY_CHECKPOINT_INFO: &str = "(empty)";

/// 异步等待副本位点追平的 oneshot 契约（对标 Garnet TaskCompletionSource）
struct OffsetWaiter {
  target: AofAddress,
  tx: Option<TxOneshot<()>>,
}

/// 主备数据同步协商策略结果（磁盘臂见 [`ReplicationManager::disk_resync_strategy`]，
/// 无盘臂见 [`ReplicationManager::diskless_resync_strategy`]）
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
  /// replication.toml 落盘互斥（C# ReplicationHistoryManager.cs:FlushConfig 的
  /// `lock (this)`）：序列化 + 写设备整段互斥，任一时刻设备上恒为某单一完整版本
  history_flush_lock: Mutex<()>,
  pub primary_sync_last_timestamp: AtomicI64,
  pub current_recovery_status: RwLock<RecoveryStatus>,
  pub store_current_safe_aof_address: RwLock<AofAddress>,
  pub store_recovered_safe_aof_address: RwLock<AofAddress>,
  pub checkpoint_store: Arc<RwLock<CheckpointStore>>,
  pub aof_sync_driver_store: Arc<AofSyncDriverStore>,
  /// 副本重放驱动仓的当前代实例（对标 C# ReplicationManager.
  /// ReplicaReplayDriverStore 公开字段的换代替换语义，ReplicaReplayManager.
  /// cs:14/34-38：reset 即「旧实例 Dispose + new 新实例」，容器非共享
  /// 单例；读取经 [`Self::replica_replay_driver_store`]，会话在注册成功时
  /// 捕获当时代际私有持有，断连只 dispose 自持代际）
  replica_replay_driver_store: RwLock<Arc<ReplicaReplayDriverStore>>,
  /// 检查点网络接收处理器（C# ReplicationManager.recvCheckpointHandler
  /// 字段；activeSink 单文件状态机，同步尝试间经 `reset_recv_ckpt` 置换）
  pub recv_checkpoint_handler: ReceiveCheckpointHandler,
  /// 复制网络缓冲池（对标 C# networkPool：NetworkBufferSettings.CreateBufferPool；
  /// 读取经 [`Self::network_pool`]，对标 C# GetNetworkPool property）
  network_pool: Arc<LimitedFixedBufferPool>,
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
  /// 磁盘同步在册任务仓（对标 C# ReplicationManager.
  /// replicaSyncSessionTaskStore 字段，PrimaryOps/
  /// ReplicaSyncSessionTaskStore.cs:25 构造期随 rm 装配）：磁盘链入口按
  /// 副本节点 id 去重登记，同一副本并发重复发起 INITIATE_REPLICA_SYNC 次路
  /// 拒绝；dispose 清空（C# Dispose:500 同位）
  pub replica_sync_task_store: ReplicaSyncSessionTaskStore,
  /// 快照下发在途读者在册表（store_hlog_token → (扇区对齐 begin, 同条目在途
  /// 会话数)）：C# CheckpointStore.cs:196-210「活日志截断钳制到最老活跃
  /// reader 引用条目」的聚合真源。注册/注销由 replica_sync_session 的条目
  /// 读者计数单点（try_add_reader / remove_reader 同位）驱动，集合每次变更
  /// 在锁内重算聚合 min begin 并经回调前向写入 whlog 的 reader_pin 水位
  /// （回调持锁执行——水位写序与在册变更序一致，并发会话互不覆盖对方聚合）；
  /// 空集 = u64::MAX（无在途读者）。按 token 计数：多会话读同一条目共享计数，
  /// 最后一名注销才摘除，单会话释放不会抬掉他会话的钉
  snapshot_reader_pins: Mutex<HashMap<u128, (u64, u32)>>,
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
  /// 不 recover 时旧 replication.toml 被新历史覆盖）；构造尾段
  /// SetPrimaryReplicationId。recover_or_init 读损坏回退 new + flush 与
  /// C# RecoverReplicationHistory 的 catch 分支等价
  pub fn with_options(sublog_count: usize, config_dir: Option<&Path>, recover: bool) -> Self {
    let sublog_count = sublog_count.max(1);
    let config_path = config_dir.map(|p| p.join(REPLICATION_STATE_FILE));

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
      replica_replay_driver_store: RwLock::new(Arc::new(ReplicaReplayDriverStore::new(
        sublog_count,
      ))),
      recv_checkpoint_handler: ReceiveCheckpointHandler::new(),
      network_pool: LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
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
      replica_sync_task_store: ReplicaSyncSessionTaskStore::new(),
      snapshot_reader_pins: Mutex::new(HashMap::new()),
    };

    // C# 构造门控：Recover 且 replication.toml 非空才恢复历史，否则初始化新历史
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
    // C# 构造尾 SetPrimaryReplicationId：rust 无构造期缓存写面（快照签名 ID
    // 于登记点现取 primary_repl_id，见 try_update_for_failover 内注释锚），
    // 仅留注释锚
    slf
  }

  /// 注入 storeWrapper 提交标记写入回调（集群装配期一次注入，对标 C# 委托字段装配）
  pub fn set_commit_channel(&self, commit: Option<StoreCommitFn>) {
    *self.commit_channel.write() = commit;
  }

  /// 物理子日志数量
  #[inline]
  pub fn sublog_count(&self) -> usize {
    self.sublog_count
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
    let now_ms = time::now_ms_i64();
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
  /// 对标 C# `ReplicationManager.cs:251-256`：`Environment.单调毫秒` 取新值 +
  /// `Interlocked.CompareExchange` 不等即 bail（another-attempt 语义）。调用点
  /// 必须位于 PreventRoleChange 与 TOCTOU 复检之后——真正发起才消费，false 时
  /// 调用方须 AllowRoleChange 后放弃本轮（C# finally 臂）
  pub fn try_consume_ensure_replication_window(&self, observed_last_ms: i64) -> bool {
    self
      .last_ensure_replication_attempt_ms
      .compare_exchange(
        observed_last_ms,
        time::now_ms_i64(),
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
    self.current_replica_replay_driver_store().has_drivers()
  }

  /// 当前代重放驱动仓句柄（对标 C# ReplicationManager.ReplicaReplayDriverStore
  /// 字段的读取：会话在 APPENDLOG 初始化帧注册成功时取当时代际引用私有持有，
  /// 见 RespClusterReplicationCommands.cs:221-224）
  pub fn current_replica_replay_driver_store(&self) -> Arc<ReplicaReplayDriverStore> {
    Arc::clone(&self.replica_replay_driver_store.read())
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayManager.cs:InitializeReplicaReplayDriver
  ///
  /// 初始化指定子日志的副本重放驱动（挂当前重放资产与本管理器弱引用）；
  /// 已存在或当时代驱动仓已关闭则返回 false
  pub fn initialize_replica_replay_driver(self: &Arc<Self>, physical_sublog_idx: usize) -> bool {
    let store = self.current_replica_replay_driver_store();
    if store.get_replay_driver(physical_sublog_idx).is_some() {
      return false;
    }
    store
      .add_replica_replay_driver(
        physical_sublog_idx,
        self.replay_assets(),
        Arc::downgrade(self),
      )
      .is_some()
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayManager.cs:ResetReplicaReplayDriverStore
  ///
  /// 换代：旧实例 dispose（终结其全部在册驱动与背景重放）+ new 全新代次
  /// 实例（对标 C# `ReplicaReplayDriverStore?.Dispose();
  /// ReplicaReplayDriverStore = new(...)`，ReplicaReplayDriverStore.cs:73-95
  /// 的 dispose 经内部标志幂等）。换代后旧连接迟到触发的 dispose 仅命中已
  /// 处置的旧实例幂等空转，绝不影响本管理器当前持有的新一代驱动仓
  pub fn reset_replica_replay_driver_store(&self) {
    let mut current = self.replica_replay_driver_store.write();
    current.dispose();
    *current = Arc::new(ReplicaReplayDriverStore::new(self.sublog_count));
  }

  /// 复制网络缓冲池读出面（对标 C# ReplicationManager.cs:networkPool 字段）：
  /// 副本同步（推流 wire / attach / 检查点下发）建连注入同池，跨连接复用
  /// （C# AofSyncTask.cs、ReplicaSyncSession.cs、ReplicaDiskbasedSync.cs、
  /// ReplicaDisklessSync.cs 同源传池）
  pub fn network_pool(&self) -> Arc<LimitedFixedBufferPool> {
    Arc::clone(&self.network_pool)
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
    self.network_pool.get_stats()
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
    // 对标 C# Dispose:501 `ReplicaReplayDriverStore?.Dispose()`：终结当前代
    self.current_replica_replay_driver_store().dispose();
    self.aof_sync_driver_store.reset();
    // 对标 C# Dispose:500 replicaSyncSessionTaskStore.Dispose()：在册磁盘
    // 同步任务随 rm 停机全摘（会话自有资源由各会话体退场路径收敛）
    self.replica_sync_task_store.clear();
  }
}

impl Drop for ReplicationManager {
  fn drop(&mut self) {
    self.dispose();
  }
}

/// 快照读钉在册表聚合值：全部在途读者 begin 的最小值（对标 C#
/// CheckpointStore.cs:196-210 钳制目标「最老活跃 reader」）；空集回 u64::MAX
/// （whlog reader_pin 的「无在途读者」哨兵，使地板不受限）
fn aggregate_reader_pin(pins: &HashMap<u128, (u64, u32)>) -> u64 {
  pins
    .values()
    .map(|(begin, _)| *begin)
    .min()
    .unwrap_or(u64::MAX)
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
        meta.checkpoint_aof_address.as_ref().map_or_else(
          || EMPTY_CHECKPOINT_INFO.to_string(),
          |addrs| {
            addrs
              .iter()
              .map(u64::to_string)
              .collect::<Vec<_>>()
              .join(",")
          },
        )
      )
    })
    .unwrap_or_else(|| EMPTY_CHECKPOINT_INFO.to_string())
}
