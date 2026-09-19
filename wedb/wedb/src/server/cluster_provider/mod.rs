use std::{
  fs::{create_dir_all, metadata, read},
  ops::ControlFlow,
  path::{Path, PathBuf},
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU8, AtomicU64, Ordering},
  },
  thread,
  time::Duration,
};

use coarsetime::Instant;
use compio::runtime::spawn;
use itoa::Buffer;
use parking_lot::RwLock;
use waof::{AofAddress, WalLog};
use wbase::{
  future::yield_now,
  hex::hex_str_u128,
  map::{ConcurrentMap, new_concurrent_map},
};
use wconf::{RuntimeServerConfig, node_options::DEFAULT_ON_DEMAND_CHECKPOINT};
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;
use wcpr::CheckpointMeta;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  ClusterProvider as WnodeClusterProvider, ClusterProviderHandle, PrimaryTasks, RoleInfo,
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_session::ClusterSessionFace,
  database::{SingleDatabaseManager, checkpoint_version},
  resp::{slow_path::SlowFuture, vector::vector_manager::VectorManager},
  service::StoreSwapSlot,
  session_parse_state_extensions::ManagerType,
};
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::{command::RespCommand, ext::RespVecExt, metrics::MetricsItem};

use crate::{
  args::{DEFAULT_CLUSTER_NODE_TIMEOUT_MS, DEFAULT_GOSSIP_DELAY_MS, DEFAULT_GOSSIP_SAMPLE_PERCENT},
  error,
  server::{
    cluster::{
      CheckpointCallbackFace, CheckpointMetadata, ClusterPreferredEndpointType, IClusterProvider,
    },
    cluster_config::ClusterConfig,
    cluster_manager::{ClusterManager, read_device},
    cluster_session::{ClusterSession, ERR_CLUSTER_NOT_INITIALIZED},
    connection_info::ConnectionInfo,
    failover::failover_manager::FailoverManager,
    gossip::gossip_manager::GossipManager,
    migration::migration_manager::{DEFAULT_MAX_SEND_BUFFER_CONTENT_SIZE, MigrationManager},
    replication::{
      aof_replication_pump::AofReplicationPump, assembly::try_replicate_sync_async,
      checkpoint_entry::CheckpointEntry, cluster_replication_session::ClusterReplicationSession,
      receive_checkpoint_handler::CheckpointImportCtx, recovery_status::RecoveryStatus,
      replica_sync_session::ReplicaSyncSession, replicate_sync_options::ReplicateSyncOptions,
      replication_manager::ReplicationManager, store_commit::StoreCommitFn,
    },
    worker::NodeRole,
  },
};

/// 主端 AOF 推流装配面（CLUSTER INITIATE_REPLICA_SYNC 发起侧依赖束：
/// 物理日志 + 推流泵 + 主端同步会话；AOF 门控点亮时经
/// [`ClusterProvider::set_primary_replication`] 一次注入）
pub struct PrimaryReplicationAssets {
  /// 主端物理日志（同步策略协商的位点基准 + 推流数据源）
  pub wal: Arc<WalLog<SegmentedDevice>>,
  /// 主端推流泵（attach 副本 sink 后同栈分发新写入）
  pub pump: Arc<AofReplicationPump>,
  /// 主端副本同步会话（策略协商 + 建连 + 补扫）
  pub sync_session: Arc<ReplicaSyncSession>,
}

/// WeDB 分布式集群提供者核心门面（对标 C# Garnet.cluster.ClusterProvider）
pub struct ClusterProvider {
  pub cluster_manager: RwLock<Option<Arc<ClusterManager>>>,
  pub replication_manager: RwLock<Option<Arc<ReplicationManager>>>,
  pub failover_manager: RwLock<Option<Arc<FailoverManager>>>,
  pub migration_manager: RwLock<Option<Arc<MigrationManager>>>,
  pub gossip_manager: RwLock<Option<Arc<GossipManager>>>,
  pub auth_container: RwLock<(Option<String>, Option<String>)>,
  replication_reestablishment_timeout_secs: AtomicI32,
  /// FastAofTruncate 选项（C# GarnetServerOptions.FastAofTruncate，默认
  /// false；副本接收面跳跃重对齐分支的开关，装配期自 RuntimeServerOptions 注入）
  fast_aof_truncate: AtomicBool,
  /// 集群节点超时毫秒数原始槽（C# RuntimeServerConfig ClusterNodeTimeout 的
  /// 毫秒形态；0 = 无限超时哨兵，对标 Timeout.InfiniteTimeSpan，
  /// libs/server/Config/RuntimeServerConfig.cs:320；全部消费点经
  /// cluster_node_timeout() 单点取值，不得直读本槽）
  cluster_node_timeout_ms: AtomicU64,
  /// gossip 周期毫秒数（C# GarnetServerOptions.GossipDelay 的毫秒形态，
  /// 默认 5000；gossip 主循环 sleep 与 gossip 发送超时源，装配期自
  /// ClusterArgs 注入）
  gossip_delay_ms: AtomicU64,
  /// gossip 抽样百分比（C# GarnetServerOptions.GossipSamplePercent，
  /// 默认 100 = 全量广播；装配期自 ClusterArgs 注入）
  gossip_sample_percent: AtomicI32,
  /// 集群重定向端点偏好（C# serverOptions.ClusterPreferredEndpointType；
  /// MOVED/ASK 重定向与 CLUSTER SLOTS/SHARDS 输出的地址形态源，装配期自
  /// ClusterArgs 注入）
  preferred_endpoint_type: AtomicU8,
  /// Garnet 当前纪元（对标 C# ClusterProvider.GarnetCurrentEpoch，初始为 1）
  garnet_current_epoch: AtomicI64,
  /// 副本重放最大滞后字节数（C# GarnetServerOptions.AofReplayMaxLagBytes，
  /// 默认 -1；INFO 复制段 aof_replay_max_lag_bytes 直读源，装配期自
  /// ClusterArgs 注入）
  aof_replay_max_lag_bytes: AtomicI32,
  /// 无盘同步 leader 攒批窗口秒数（C# GarnetServerOptions.
  /// ReplicaDisklessSyncDelay，默认 5；C# 消费点
  /// runtimeConfig.GetInt(REPL_DISKLESS_SYNC_DELAY) 的 rust 承接面，与其余
  /// provider 选项槽同形态装配期自 RuntimeServerOptions 注入；diskless 主端
  /// 会话驱动攒批等待的唯一读取源，全部消费点经
  /// replica_diskless_sync_delay() 单点取值，不得直读本槽）
  replica_diskless_sync_delay_secs: AtomicI32,
  /// 活跃集群会话弱引用表（C# GarnetServerBase.activeHandlers 承接的集群
  /// 会话枚举面：papaya 无锁并发表对标 ConcurrentDictionary<INetworkHandler,
  /// byte>，纪元静止等待的枚举不排阻会话注册注销；会话体归连接任务独占，
  /// 此处仅存弱引用，键为 Weak::as_ptr 地址，过期即会话已亡，枚举时自清扫
  /// 免注销钩子；BumpAndWaitForEpochTransition 的静止等待遍历源）
  cluster_sessions: ConcurrentMap<usize, Weak<ClusterSession>>,
  /// 弱引用自身（用于按需向上派生包含本对象的会话，无锁读取）
  self_weak: OnceLock<Weak<ClusterProvider>>,
  /// 当前在线引擎槽（对标 C# clusterProvider.storeWrapper 的存储可达面——
  /// C# 侧 store 为 StoreWrapper.cs:41 单计算属性，本层持宿主槽句柄即活视图，
  /// 全集群一份引擎状态、零引擎拷贝（槽为 Arc 薄句柄，共享状态零循环引用）：
  /// 装配期 set_store 播种、注入宿主置换槽后与宿主同源，CLUSTER RESET 的
  /// HasKeysInSlots 扫描与 HARD 清库经此下发。未播种时集群命令族仍可用，
  /// 仅 RESET 慢路径降级报错）
  store_slot: RwLock<StoreSwapSlot>,
  /// 向量集合管理器（CLUSTER RESERVE 迁移预保留面，对标 C#
  /// RespServerSession 会话持有的 vectorManager；装配期一次注入）
  vector_manager: RwLock<Option<Arc<VectorManager>>>,
  /// AOF 门面（MLOG_KEY_TIME 序列号读取面，对标 C#
  /// storeWrapper.appendOnlyFile；AOF 门控点亮时注入，未启用为 None）
  aof: RwLock<Option<Arc<GarnetAppendOnlyFile>>>,
  /// 运行时配置可达面（对标 C# ClusterProvider 构造期 serverOptions 字段
  /// （libs/cluster/Server/ClusterProvider.cs）——复制面每轮实时读热更槽值
  /// 的源，如 AofSyncTask 脉冲节流读 AofTailWitnessFreqMs；装配期自
  /// StorageSessionProvider.runtime_config 一次注入，全仓唯一实例，
  /// CONFIG SET 落槽即时可见，未注入即 None 无热更面）
  runtime_config: RwLock<Option<Arc<RuntimeServerConfig>>>,
  /// 本地物理日志句柄（副本发起 INITIATE_REPLICA_SYNC 的 begin/tail 位点源
  /// 与副本接收会话落盘目标；AOF 门控点亮时装配期注入）
  wal: RwLock<Option<Arc<WalLog<SegmentedDevice>>>>,
  /// 副本接收面会话（CLUSTER APPENDLOG 落盘重放；AOF 门控点亮时注入）
  replica_replication: RwLock<Option<Arc<ClusterReplicationSession<SegmentedDevice>>>>,
  /// 主端推流装配面（CLUSTER INITIATE_REPLICA_SYNC 发起面）
  primary_replication: RwLock<Option<Arc<PrimaryReplicationAssets>>>,
  /// 检查点目录（C# clusterProvider 经 storeWrapper 反查 CheckpointDir 的
  /// 依赖方向反转形态；快照发送源与副本接收落盘目标的公共根，装配期注入）
  checkpoint_dir: RwLock<Option<PathBuf>>,
  /// Primary 类后台任务生命周期域（C# storeWrapper 任务域可达面；降副本/
  /// REPLICAOF/全量同步前挂起，升主/接管恢复——对标 StoreWrapper 的
  /// SuspendPrimaryOnlyTasksAsync / StartPrimaryTasks 家族）
  primary_tasks: RwLock<Option<Arc<PrimaryTasks>>>,
  /// 发布订阅中枢（对标 C# clusterProvider.storeWrapper.subscribeBroker 可达面；
  /// CLUSTER PUBLISH 接收面本地投递源，装配期自 StorageSessionProvider 注入）
  pubsub: RwLock<Option<Arc<SubscribeBroker>>>,
  /// 逻辑数据库管理器（对标 C# StoreWrapper.databaseManager；按需检查点与快照管理）
  database_manager: RwLock<Option<Arc<SingleDatabaseManager<SegmentedDevice>>>>,
  /// 是否启用按需检查点（C# GarnetServerOptions.OnDemandCheckpoint，
  /// libs/server/Servers/GarnetServerOptions.cs:405 默认 true；装配期自
  /// RuntimeServerOptions 注入，--on-demand-checkpoint / nested_text 同源）
  on_demand_checkpoint: AtomicBool,
  /// 无盘同步开关（C# GarnetServerOptions.ReplicaDisklessSync，默认 false；
  /// 副本侧同步发起端 diskless / diskbased 选路的唯一读取面，装配期自
  /// NodeArgs.repl_diskless_sync 注入）
  replica_diskless_sync: AtomicBool,
  /// 重启恢复开关（C# GarnetServerOptions.Recover，默认 false；启动期主动
  /// attach 臂 ReplicationManager.Start 的分支判据，装配期自 NodeArgs.recover 注入）
  recover: AtomicBool,
  /// 集群出站 TLS 客户端配置（gossip / 复制 / 迁移 / failover 五类出站
  /// 连接的同一单源；对标 C# 各消费点直读
  /// clusterProvider.serverOptions.TlsOptions?.TlsClientOptions 的可达面，
  /// rust 收敛为装配期一次注入、单一访问口；None = 明文集群）
  #[cfg(feature = "tls")]
  cluster_tls_client: RwLock<Option<Arc<ClientTlsConfig>>>,
}

impl Default for ClusterProvider {
  fn default() -> Self {
    Self {
      cluster_manager: RwLock::new(None),
      replication_manager: RwLock::new(None),
      failover_manager: RwLock::new(None),
      migration_manager: RwLock::new(None),
      gossip_manager: RwLock::new(None),
      auth_container: RwLock::new((None, None)),
      replication_reestablishment_timeout_secs: AtomicI32::new(0),
      fast_aof_truncate: AtomicBool::new(false),
      cluster_node_timeout_ms: AtomicU64::new(DEFAULT_CLUSTER_NODE_TIMEOUT_MS),
      gossip_delay_ms: AtomicU64::new(DEFAULT_GOSSIP_DELAY_MS),
      gossip_sample_percent: AtomicI32::new(DEFAULT_GOSSIP_SAMPLE_PERCENT),
      preferred_endpoint_type: AtomicU8::new(ClusterPreferredEndpointType::Ip as u8),
      garnet_current_epoch: AtomicI64::new(1),
      aof_replay_max_lag_bytes: AtomicI32::new(-1),
      // C# GarnetServerOptions.cs:ReplicaDisklessSyncDelay 默认 5（秒）
      replica_diskless_sync_delay_secs: AtomicI32::new(5),
      cluster_sessions: new_concurrent_map(),
      self_weak: OnceLock::new(),
      store_slot: RwLock::new(StoreSwapSlot::new()),
      vector_manager: RwLock::new(None),
      aof: RwLock::new(None),
      runtime_config: RwLock::new(None),
      wal: RwLock::new(None),
      replica_replication: RwLock::new(None),
      primary_replication: RwLock::new(None),
      checkpoint_dir: RwLock::new(None),
      primary_tasks: RwLock::new(None),
      pubsub: RwLock::new(None),
      database_manager: RwLock::new(None),
      on_demand_checkpoint: AtomicBool::new(DEFAULT_ON_DEMAND_CHECKPOINT),
      replica_diskless_sync: AtomicBool::new(false),
      recover: AtomicBool::new(false),
      #[cfg(feature = "tls")]
      cluster_tls_client: RwLock::new(None),
    }
  }
}

impl ClusterProvider {
  /// 当前节点是否为主节点
  #[inline]
  pub fn is_primary(&self) -> bool {
    WnodeClusterProvider::is_primary(self)
  }

  /// 当前节点是否为从节点
  #[inline]
  pub fn is_replica(&self) -> bool {
    WnodeClusterProvider::is_replica(self)
  }

  /// 创建并装配全部集群管理器组件（对标 C# ClusterProvider 构造函数）
  pub fn new() -> Arc<Self> {
    let cp = Arc::new(Self::default());
    if cp.self_weak.set(Arc::downgrade(&cp)).is_err() {
      log::warn!("self_weak 初始化重复调用");
    }
    *cp.cluster_manager.write() = Some(Arc::new(ClusterManager::new(Arc::clone(&cp))));
    *cp.replication_manager.write() = Some(Arc::new(ReplicationManager::new()));
    *cp.failover_manager.write() = Some(Arc::new(FailoverManager::new(Arc::clone(&cp))));
    *cp.migration_manager.write() = Some(Arc::new(MigrationManager::new(Arc::clone(&cp))));
    *cp.gossip_manager.write() = Some(Arc::new(GossipManager::new(Arc::clone(&cp))));
    cp
  }

  /// 获取自身 Arc 句柄（基于构造时登记的弱引用）
  pub fn self_arc(&self) -> Option<Arc<Self>> {
    self.self_weak.get().and_then(|w| w.upgrade())
  }

  /// 获取只读查询与缓冲池集群提供者句柄
  pub fn provider_handle(&self) -> ClusterProviderHandle {
    (self.self_arc().unwrap_or_else(|| Arc::new(Self::default()))) as _
  }

  /// 初始化复制管理器（对标 C# ClusterProvider 构造函数初始化 ReplicationManager：
  /// 构造期即持 CheckpointDir，`Recover && fileSize > 0` 门控恢复复制历史，
  /// 详见 ReplicationManager::with_options）
  ///
  /// rust 结构差异：ClusterProvider::new() 时数据目录未知，先建无持久化
  /// 默认实例保运行期路径可达；宿主装配期（端点 accept 之前、set_aof /
  /// wire_replication_data_plane 等挂 rm 资产的注入之前）以真实目录无条件
  /// 重建一次。
  pub fn initialize_replication_manager(
    &self,
    sublog_count: usize,
    config_dir: Option<&Path>,
    recover: bool,
  ) {
    *self.replication_manager.write() = Some(Arc::new(ReplicationManager::with_options(
      sublog_count,
      config_dir,
      recover,
    )));
  }

  /// 集群拓扑持久化装配（对标 C# ClusterManager 构造段 64-143 行的设备建立、
  /// 恢复与 InitLocal、周期刷盘拉起）
  ///
  /// libs/cluster/Server/ClusterManager.cs:ClusterManager
  ///
  /// rust 结构差异：ClusterProvider::new() 时数据目录未知，ClusterManager 仅
  /// 建空配置；宿主装配期（端点 accept 之前）以真实路径与刷盘频率调用本方法
  /// 完成恢复。recoverConfig 门控对标 C#:79：刷盘频率 != -1、盘上文件非空、
  /// 未指定 clean-cluster-config。`announce_hostname` 为集群宣告主机名配置
  /// （C# serverOptions.ClusterAnnounceHostname），透传至 InitLocal 定本地位
  /// 主机名。周期刷盘任务须在 compio 运行时内拉起
  /// （gossip/spawn 同款前置）
  pub fn initialize_cluster_config(
    &self,
    address: &str,
    port: i32,
    config_path: &Path,
    flush_frequency_ms: i32,
    clean_config: bool,
    announce_hostname: &str,
  ) -> error::Result<()> {
    let Some(cm) = self.cluster_manager() else {
      return Ok(());
    };
    cm.set_persist_options(config_path.to_path_buf(), flush_frequency_ms);

    let recover_config =
      flush_frequency_ms != -1 && !clean_config && metadata(config_path).is_ok_and(|m| m.len() > 0);
    if clean_config {
      log::info!("Skipping recovery of local config due to clean-cluster-config flag set");
    } else {
      log::info!("Attempt to recover cluster config from: {config_path:?}");
    }
    if recover_config {
      let bytes = read_device(config_path)?;
      let recovered = ClusterConfig::from_byte_array(&bytes)?;
      log::debug!("Recover cluster config from disk");
      // endpoint 变更（容器漂移）仅记日志，本地位由 init_local 按恢复字段重建
      if address != recovered.local_node_ip() || port != recovered.local_node_port() {
        log::info!(
          "Updating local Endpoint: From {}:{} to {address}:{port}",
          recovered.local_node_ip(),
          recovered.local_node_port()
        );
      }
      *cm.current_config.write() = recovered;
    } else {
      log::debug!("Initialize new node instance config");
    }

    cm.init_local(address, port, recover_config, announce_hostname);
    if flush_frequency_ms > 0 {
      cm.start_flush_task(Duration::from_millis(flush_frequency_ms as u64));
    }
    Ok(())
  }

  /// 获取当前节点连接信息
  pub fn get_connection_info(&self, node_id: u128) -> ConnectionInfo {
    self
      .cluster_manager()
      .map(|cm| cm.get_connection_info(node_id))
      .unwrap_or_default()
  }

  /// 获取 ClusterManager 句柄
  #[inline]
  pub fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_manager.read().clone()
  }

  /// 获取 ReplicationManager 句柄
  #[inline]
  pub fn replication_manager(&self) -> Option<Arc<ReplicationManager>> {
    self.replication_manager.read().clone()
  }

  /// 获取 FailoverManager 句柄
  #[inline]
  pub fn failover_manager(&self) -> Option<Arc<FailoverManager>> {
    self.failover_manager.read().clone()
  }

  /// 获取 MigrationManager 句柄
  #[inline]
  pub fn migration_manager(&self) -> Option<Arc<MigrationManager>> {
    self.migration_manager.read().clone()
  }

  /// 迁移/无盘同步单块内容上限真源：migration_manager 已装配即取其
  /// `max_send_buffer_content_size()`，未装配回退同源派生缺省
  /// （对标 C# NetworkBufferSettings.MaxSendBufferContentSize 单点）
  #[inline]
  pub fn max_send_buffer_content_size(&self) -> usize {
    self
      .migration_manager()
      .map(|mm| mm.max_send_buffer_content_size())
      .unwrap_or(DEFAULT_MAX_SEND_BUFFER_CONTENT_SIZE)
  }

  /// 注入复制重连轮询频率（秒；0 = 禁用，服务器总装期自 RuntimeServerOptions 注入）
  pub fn set_replication_reestablishment_timeout(&self, secs: i32) {
    self
      .replication_reestablishment_timeout_secs
      .store(secs, Ordering::Release);
  }

  /// 注入 FastAofTruncate 选项（对标 C# clusterProvider.serverOptions.
  /// FastAofTruncate 的读取面；装配期自 RuntimeServerOptions 一次注入，
  /// 源 --fast-aof-truncate / nested_text 同一配置面）
  pub fn set_fast_aof_truncate(&self, enabled: bool) {
    self.fast_aof_truncate.store(enabled, Ordering::Release);
  }

  /// FastAofTruncate 选项（副本接收面跳跃重对齐分支的开关，兼
  /// [`Self::allow_data_loss`] 派生输入；C# GarnetServerOptions.cs:400 默认 false）
  #[inline]
  pub fn fast_aof_truncate(&self) -> bool {
    self.fast_aof_truncate.load(Ordering::Acquire)
  }

  /// 注入无盘同步 leader 攒批窗口秒数（对标 C# serverOptions.
  /// ReplicaDisklessSyncDelay；装配期自 RuntimeServerOptions 一次注入）
  pub fn set_replica_diskless_sync_delay(&self, secs: i32) {
    self
      .replica_diskless_sync_delay_secs
      .store(secs, Ordering::Release);
  }

  /// 无盘同步 leader 攒批窗口秒数（<= 0 = 关闭攒批；C#
  /// runtimeConfig.GetInt(REPL_DISKLESS_SYNC_DELAY) 消费面）
  #[inline]
  pub fn replica_diskless_sync_delay(&self) -> i32 {
    self
      .replica_diskless_sync_delay_secs
      .load(Ordering::Acquire)
  }

  /// 注入集群节点超时毫秒数（装配期一次调用；0 = 无限超时哨兵）
  pub fn set_cluster_node_timeout_ms(&self, ms: u64) {
    self.cluster_node_timeout_ms.store(ms, Ordering::Release);
  }

  /// 集群节点超时（未注入时取默认值）。
  ///
  /// 对标 C# RuntimeServerConfig.GetTimeSpan
  /// （libs/server/Config/RuntimeServerConfig.cs:318-321）：非正值解释为无限
  /// 超时，此处以 None 承载（同 wconf::RuntimeServerConfig::get_time_span 的
  /// 全仓约定）。`Duration::MAX` 不可用作哨兵——compio 定时器
  /// `Instant::now() + Duration::MAX` 溢出 panic，故无限分支不挂计时器
  #[inline]
  pub fn cluster_node_timeout(&self) -> Option<Duration> {
    match self.cluster_node_timeout_ms.load(Ordering::Acquire) {
      0 => None,
      ms => Some(Duration::from_millis(ms)),
    }
  }

  /// 注入 gossip 周期毫秒数（装配期一次调用；对标 GarnetServerOptions.GossipDelay
  /// 秒转毫秒，默认 5000）
  pub fn set_gossip_delay_ms(&self, ms: u64) {
    self.gossip_delay_ms.store(ms, Ordering::Release);
  }

  /// gossip 周期毫秒数（未注入时取默认值）
  pub fn gossip_delay_ms(&self) -> u64 {
    self.gossip_delay_ms.load(Ordering::Acquire)
  }

  /// 注入 gossip 抽样百分比（装配期一次调用；对标
  /// GarnetServerOptions.GossipSamplePercent，默认 100）
  pub fn set_gossip_sample_percent(&self, pct: i32) {
    self.gossip_sample_percent.store(pct, Ordering::Release);
  }

  /// gossip 抽样百分比（未注入时取默认值）
  pub fn gossip_sample_percent(&self) -> i32 {
    self.gossip_sample_percent.load(Ordering::Acquire)
  }

  /// 注入集群重定向端点偏好（装配期一次调用；对标 C#
  /// serverOptions.ClusterPreferredEndpointType，默认 Ip）
  pub fn set_preferred_endpoint_type(&self, pref_type: ClusterPreferredEndpointType) {
    self
      .preferred_endpoint_type
      .store(pref_type as u8, Ordering::Release);
  }

  /// 集群重定向端点偏好（未注入时取默认值 Ip）
  pub fn preferred_endpoint_type(&self) -> ClusterPreferredEndpointType {
    // 判别值仅经 set_preferred_endpoint_type / 默认值写入，恒落在枚举域内
    match self.preferred_endpoint_type.load(Ordering::Acquire) {
      1 => ClusterPreferredEndpointType::Hostname,
      2 => ClusterPreferredEndpointType::Unknown,
      _ => ClusterPreferredEndpointType::Ip,
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:EnsureReplication
  ///
  /// 入站 gossip 会话的复制健康检查（C# EnsureReplication 完整判定链；
  /// C# 在 rm 上实现并经 clusterProvider 直达各管理器，Rust 依赖方向反转后
  /// 判定链上收至本层，rm 保留节流判定与节流消费两枚原语供本链调用）：
  /// 1. 轮询频率 0 = 禁用；
  /// 2. 距上次尝试不足频率 → 返回（节流；到期只判定不消费，见
  ///    [`ReplicationManager::ensure_replication_due`]，对标 C# :192 的
  ///    `Volatile.Read`）；
  /// 3. 仅 REPLICA 且活跃会话来自其 primary 时动作；
  /// 4. 已有活跃复制流（IsReplicating 状态面）→ 无需动作；
  /// 5. failover 进行中抑制自动重连（防 ReadRole 锁阻塞 TakeOverAsPrimary）；
  /// 6. PreventRoleChange + TOCTOU 复检通过后**才** CAS 消费节流窗口（对标 C#
  ///    :251-256；CAS 不等 = 判定与消费之间另有尝试在途，AllowRoleChange 后
  ///    返回，不重复发起）——第 2~5 步任一挡回的到期帧都不吃掉窗口，下一帧
  ///    仍立即可判到期；
  /// 7. 后台重连发起（对标 C# ReplicationManager.cs:271-272
  ///    Task.Run 体内的 `ReplicaDisklessSync ? TryReplicateDisklessSyncAsync :
  ///    TryReplicateDiskbasedSyncAsync` 选路：本挂点只构造参数束，选路交唯一
  ///    选路口 [`try_replicate_sync_async`]（diskless 支走副本主动 ATTACH_SYNC
  ///    发起端，diskbased 支走
  ///    [`recover_replication`](crate::server::replication::assembly::recover_replication)
  ///    向 primary 发 INITIATE_REPLICA_SYNC）；失败记告警，按
  ///    ClusterReplicationReestablishmentTimeout 轮询节奏重试，finally
  ///    AllowRoleChange）。
  ///
  /// 启动阻塞臂口径：C# ReplicationManager.cs:604-605 的 `Start` 内
  /// `BlockingWait(ReplicaDisklessSync ? ... : ...)`（NodeId 传 null、
  /// TryAddReplica:false，Force 取开关本身）在 rust 无对应挂点，且本函数也不
  /// 能承接——本函数受第 1 步（轮询频率 0 即禁用，默认 0）与第 3 步（要求
  /// `active_remote_node_id` 即已存在来自主端的活跃会话）双门控，只覆盖
  /// 「会话断链后的重连」，不覆盖「重启后首次接入」；故副本重启后的首帧前
  /// 角色为 REPLICA 不代表已 attach。启动期主动发起挂点为独立待办
  /// （task/ing/replicaof-diskbased-sync-initiate.md），本轮不在此另立第二
  /// 发起路径。
  ///
  /// 心跳口径：本函数不刷新 last_primary_sync_time（对标 C#——EnsureReplication
  /// 本体无 UpdateLastPrimarySyncTime 调用，C# 刷新点全在同步建立面
  /// TryReplicaDiskbasedRecovery / ReceiveCheckpointHandler）；rust 挂副本
  /// APPENDLOG 初始化帧握手成功处，见
  /// [`crate::server::replication::cluster_replication_session`]。
  pub fn ensure_replication(self: &Arc<Self>, active_remote_node_id: Option<u128>) {
    use std::sync::atomic::Ordering;

    let poll_frequency = self
      .replication_reestablishment_timeout_secs
      .load(Ordering::Acquire) as i64;
    // 1. 禁用
    if poll_frequency == 0 {
      return;
    }
    let Some(rm) = self.replication_manager() else {
      return;
    };
    // 2. 节流到期判定（纯读；窗口消费推迟到第 6 步真正发起处）
    let Some(window_observed) = rm.ensure_replication_due(poll_frequency) else {
      return;
    };

    // 3. 角色判定：仅 REPLICA 且活跃会话来自其 primary
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    let primary_id = {
      let config = cm.current_config();
      if !config.is_replica() {
        return;
      }
      config.local_node_primary_id()
    };
    if primary_id != active_remote_node_id {
      return;
    }

    // 4. IsReplicating 状态面：活跃复制流在册则无需动作
    if rm.has_active_replication_stream() {
      return;
    }

    // 5. failover 进行中抑制自动重连
    if let Some(fm) = self.failover_manager()
      && fm.is_failover_in_progress()
    {
      log::debug!("Suppressing auto-resync during active failover");
      return;
    }

    // 6. 重连动作面：PreventRoleChange + 复检 + 窗口消费
    //（对标 C# EnsureReplication 尾段：prevent → 复检 → CAS 消费 → Task.Run →
    // finally allow）
    let Some(primary) = primary_id else {
      return;
    };
    if !self.prevent_role_change() {
      return;
    }
    // TOCTOU 复检（对标 C# PreventRoleChange 后的二次判定：复制状态在持锁
    // 间隙已变更 → 释放角色锁并放弃本轮重连）
    let still_replica_of_primary = self.cluster_manager().is_some_and(|cm| {
      let config = cm.current_config();
      config.is_replica() && config.local_node_primary_id() == Some(primary)
    });
    if !still_replica_of_primary {
      self.allow_role_change();
      log::info!("Skip resync: replication state changed after PreventRoleChange");
      return;
    }
    // 节流窗口消费（对标 C# ReplicationManager.cs:251-256：判定到期不消费，
    // 走到这里才算真正发起；CAS 不等 = 判定与消费之间另有尝试在途，
    // 释放角色锁后放弃本轮，不重复发起）
    if !rm.try_consume_ensure_replication_window(window_observed) {
      self.allow_role_change();
      log::info!("Skip resync: another ensure_replication attempt consumed the window");
      return;
    }
    let provider = Arc::clone(self);
    spawn(async move {
      log::info!(
        "Beginning resync to {} after replication session failed",
        hex_str_u128(primary)
      );
      // 断链重连发起（对标 C# ReplicationManager.cs:270-272：
      // Background:false Force:true TryAddReplica:true
      // AllowReplicaResetOnFailure:false UpgradeLock:true，按
      // ReplicaDisklessSync 开关在 diskless / diskbased 两支选路；
      // 失败仅告警，finally AllowRoleChange）
      let opts = ReplicateSyncOptions::new(primary, false, true, true, false, true);
      let resynced = try_replicate_sync_async(&provider, opts).await;
      provider.allow_role_change();
      match resynced {
        Ok(()) => log::info!("Resync to {} successfully started", hex_str_u128(primary)),
        Err(e) => log::warn!(
          "Failed to resync to {} after replication session failed: {e}",
          hex_str_u128(primary)
        ),
      }
    })
    .detach();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Start
  ///
  /// 重启后主动发起与 PRIMARY 的首次同步（C# ClusterProvider.cs:89-93
  /// `Start()` 内 `replicationManager.Start()` 段的对偶；同段的
  /// `clusterManager.Start()` 半段由 gossip 挂点承接，二者一并挂在装配尾段
  /// [`WnodeClusterProvider::start`]）。逐条对标 C# 三分支：
  /// - 本地角色 REPLICA 且 recover 且已记 primary → 当场经唯一选路口
  ///   [`try_replicate_sync_async`] 发起一次 attach（C# syncOpts:592-599
  ///   NodeId:null、Background:false、Force 取 ReplicaDisklessSync 开关本身、
  ///   TryAddReplica:false、AllowReplicaResetOnFailure:false、UpgradeLock:false；
  ///   rust node_id 取本端 primary 供日志可读，TryAddReplica:false 不消费），
  ///   失败仅记日志（C# LogError 同口径，绝不影响启动）；
  /// - PRIMARY 且无 primary → 空动作（重启为主，副本自行发起恢复）；
  /// - 其余 → 配置不一致告警（C# LogWarning 同口径）。
  ///
  /// 与 [`ClusterProvider::ensure_replication`] 断链重连臂的区别（C# 两处形态
  /// 不同：Start 阻塞一次性、EnsureReplication 后台轮询）：本臂无
  /// poll_frequency（默认 0 即禁用）与活跃会话双门控，专覆盖「重启首帧前
  /// 接入」；C# Start 以 BlockingWait 阻塞启动线程，rust 无网络线程阻塞约束，
  /// 以一次性 spawn 承接同一发起（错误口径同为「仅记日志」），不复用
  /// ensure_replication 的轮询体，杜绝第二条发起路径。
  pub fn start_replication_attach(&self) {
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    let (role, primary_id) = {
      let config = cm.current_config();
      (config.local_node_role(), config.local_node_primary_id())
    };
    if role == NodeRole::Replica && self.recover() {
      let Some(primary) = primary_id else {
        log::warn!(
          "Replication manager starting configuration inconsistent role:{role:?} replicaOfId:None"
        );
        return;
      };
      // C# Start:Background:false Force:ReplicaDisklessSync TryAddReplica:false
      // AllowReplicaResetOnFailure:false UpgradeLock:false
      let opts = ReplicateSyncOptions::new(
        primary,
        false,
        self.replica_diskless_sync(),
        false,
        false,
        false,
      );
      let Some(provider) = self.self_arc() else {
        return;
      };
      spawn(async move {
        if let Err(e) = try_replicate_sync_async(&provider, opts).await {
          log::error!("An error occurred at ReplicationManager.Start: {e}");
        }
      })
      .detach();
    } else if role == NodeRole::Primary && primary_id.is_none() {
      // 重启为主：无动作，副本自行发起恢复（C# :612-616 同口径）
    } else {
      log::warn!(
        "Replication manager starting configuration inconsistent role:{role:?} replicaOfId:{primary_id:?}"
      );
    }
  }

  /// 获取 GossipManager 句柄
  #[inline]
  pub fn gossip_manager(&self) -> Option<Arc<GossipManager>> {
    self.gossip_manager.read().clone()
  }

  /// 集群检查点装配：注入 storeWrapper 提交标记写入回调（一次注入）
  ///
  /// 对标 C# ReplicationManager 构造内经 clusterProvider.storeWrapper 反查
  /// AOF 写入面（Rust 依赖方向反转，由装配层正向注入 GarnetLog 适配闭包）
  pub fn set_commit_channel(&self, commit: Option<StoreCommitFn>) {
    if let Some(rm) = self.replication_manager() {
      rm.set_commit_channel(commit);
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterUsername
  pub fn cluster_username(&self) -> Option<String> {
    self.auth_container.read().0.clone()
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPassword
  pub fn cluster_password(&self) -> Option<String> {
    self.auth_container.read().1.clone()
  }

  /// 获取 Garnet 当前纪元（对标 C# GarnetCurrentEpoch）
  #[inline]
  pub fn current_epoch(&self) -> i64 {
    self.garnet_current_epoch.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpCurrentEpoch
  ///
  /// 推进 Garnet 集群纪元
  #[inline]
  pub fn bump_current_epoch(&self) -> i64 {
    self.garnet_current_epoch.fetch_add(1, Ordering::AcqRel) + 1
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
  ///
  /// 推进集群纪元并自旋等待全部活跃集群会话批内纪元快照追平（C# 遍历
  /// storeWrapper.Servers → ActiveClusterSessions 逐会话重试至
  /// LocalCurrentEpoch 追平，快照 0 = 批外空闲放行；rust 每轮以
  /// yield_now 让步执行器，对标 C# await Task.Yield()）。以
  /// cluster_node_timeout() 为上限，超时返 false；None（0 = 无限）不设限，
  /// 与 C# 无限自旋一致（调用方同款忽略返值放行，false 仅表达静止未达成）
  pub async fn bump_and_wait_for_epoch_transition_async(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let start = Instant::now();
    let limit = self.cluster_node_timeout();
    while !self.all_sessions_caught_up(current_epoch) {
      if limit.is_some_and(|d| start.elapsed() >= d.into()) {
        return false;
      }
      yield_now().await;
    }
    true
  }

  /// 纪元推进全会话静止的命令批内同步形态（C# 命令侧
  /// `AsyncUtils.BlockingWait(BumpAndWaitForEpochTransitionAsync())` 的
  /// 语义：网络线程阻塞等待，见 RespClusterSlotManagementCommands.cs:493）
  ///
  /// compio 单线程每核下，发起会话所在线程的其余会话必处批外（快照 0），
  /// 阻塞自旋仅等他核会话收尾，无死锁；上限与追平判定同异步形态
  pub fn bump_and_wait_for_epoch_transition(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let start = Instant::now();
    let limit = self.cluster_node_timeout();
    while !self.all_sessions_caught_up(current_epoch) {
      if limit.is_some_and(|d| start.elapsed() >= d.into()) {
        return false;
      }
      thread::yield_now();
    }
    true
  }

  /// 全部活跃集群会话纪元是否追平（ClusterProvider.cs:377
  /// ActiveClusterSessions 枚举的等价面；papaya 无锁快照枚举对标 C#
  /// ConcurrentDictionary 轻量迭代，静止等待不排阻会话注册注销；枚举时
  /// 顺带清扫过期弱引用）
  fn all_sessions_caught_up(&self, current_epoch: i64) -> bool {
    let pin = self.cluster_sessions.pin();
    pin
      .iter()
      .try_for_each(|(key, weak)| match weak.upgrade() {
        Some(s) => {
          let entry_epoch = s.local_current_epoch();
          // C# 判定取反：entryEpoch != 0 && entryEpoch < currentEpoch 才重试
          if entry_epoch == 0 || entry_epoch >= current_epoch {
            ControlFlow::Continue(())
          } else {
            ControlFlow::Break(())
          }
        }
        // 会话已亡：当场自清扫死弱引用，免注销钩子
        None => {
          pin.remove(key);
          ControlFlow::Continue(())
        }
      })
      .is_continue()
  }

  /// 播种当前在线引擎（集群装配期一次调用；对标 C# 构造期经 storeWrapper
  /// 建立的存储可达面。写入本层引擎槽——宿主槽注入后与之同源，全链一份引擎
  /// 状态，无第二份拷贝）
  pub fn set_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    self.store_slot.read().swap(store);
  }

  /// 当前在线引擎（唯一读取口，取自本层引擎槽；未播种时 None）
  pub fn try_store(&self) -> Option<Arc<WedbStore<SegmentedDevice>>> {
    self.store_slot.read().get()
  }

  /// 注入 Primary 类后台任务生命周期域（集群装配期一次调用；按当前角色
  /// 初始化挂起态——恢复态副本节点在此同步停 GC，对标 C# StoreWrapper.Start
  /// 按角色分派 StartPrimaryTasks / StartReplicaTasks）
  pub fn set_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    if self.is_replica() {
      tasks.suspend();
      if let Some(store) = self.try_store() {
        store.stop_gc();
      }
    }
    *self.primary_tasks.write() = Some(tasks);
  }

  /// Primary 类后台任务生命周期域（未注入时 None）
  pub fn primary_tasks(&self) -> Option<Arc<PrimaryTasks>> {
    self.primary_tasks.read().clone()
  }

  /// 挂起 Primary 类后台任务（角色位翻转停周期任务轮次 + 停内置 GC 扫描/
  /// 紧缩；对标 libs/server/StoreWrapper.cs:SuspendPrimaryOnlyTasksAsync——
  /// 降副本 TryAddReplicaAsync、REPLICAOF 指向主端、全量同步 attach 前
  /// 调用。副本读路径惰性过期与确定性 TtlPurge 重放不受影响）
  pub fn suspend_primary_tasks(&self) {
    if let Some(tasks) = self.primary_tasks.read().as_ref() {
      tasks.suspend();
    }
    if let Some(store) = self.try_store() {
      store.stop_gc();
    }
  }

  /// 恢复 Primary 类后台任务（角色位翻转复跑周期任务 + 重拉周期对象收集 +
  /// 重启内置 GC 扫描/紧缩；对标 libs/server/StoreWrapper.cs:StartPrimaryTasks
  /// ——REPLICAOF NO ONE、failover 接管调用）
  pub fn resume_primary_tasks(&self) {
    if let Some(tasks) = self.primary_tasks.read().as_ref() {
      if let Some(store) = self.try_store() {
        tasks.resume(&store);
      } else {
        tasks.set_replica(false);
      }
    }
    if let Some(store) = self.try_store() {
      store.start_gc();
    }
  }

  /// 注入向量集合管理器（集群装配期一次调用；对标 C# RespServerSession
  /// 会话持有的 vectorManager——CLUSTER RESERVE 迁移预保留面）
  pub fn set_vector_manager(&self, vector_manager: Arc<VectorManager>) {
    *self.vector_manager.write() = Some(vector_manager);
  }

  /// 向量集合管理器（未注入时 None）
  pub fn try_vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.read().clone()
  }

  /// 注入发布订阅中枢（集群装配期一次调用；对标 C# clusterProvider.storeWrapper.subscribeBroker）
  pub fn set_pubsub(&self, pubsub: Option<Arc<SubscribeBroker>>) {
    *self.pubsub.write() = pubsub;
  }

  /// 发布订阅中枢（未注入或 --disable-pubsub 时 None）
  pub fn subscribe_broker(&self) -> Option<Arc<SubscribeBroker>> {
    self.pubsub.read().clone()
  }

  /// 注入 AOF 门面（AOF 门控点亮时装配期一次调用；对标 C#
  /// storeWrapper.appendOnlyFile 可达面——MLOG_KEY_TIME 序列号读取）。
  /// 物理日志句柄同步注入复制域驱动仓库（C# AofSyncDriverStore 构造期
  /// 反查 appendOnlyFile.Log；Rust 装配期注入，见
  /// AofSyncDriverStore::attach_log——SafeTruncateAof 物理截断面，同时亦是
  /// 主端运行期位点的动态读日志尾源），并向复制管理器注入主端角色谓词
  /// （对标 C# ReplicationOffset getter 在 PRIMARY 角色动态读
  /// appendOnlyFile.Log.TailAddress——主端运行期位点靠该角色谓词 + 同一份
  /// 日志尾推进，INFO / gossip / failover 停写应答的位点单点收口于
  /// ReplicationManager::get_current_replication_offset）
  pub fn set_aof(&self, aof: Option<Arc<GarnetAppendOnlyFile>>) {
    if let Some(rm) = self.replication_manager() {
      rm.aof_sync_driver_store
        .attach_log(aof.as_ref().map(|a| Arc::clone(a.log())));
      // 主端角色实时谓词：捕获自身弱引用回查 is_primary（避免 rm -> provider
      // 强引用成环），无 provider 时按主处理，对齐 is_primary 的 unwrap_or(true)
      let weak = self.self_weak.get().cloned();
      let primary_role: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        weak
          .as_ref()
          .and_then(|w| w.upgrade())
          .is_none_or(|p| p.is_primary())
      });
      rm.set_primary_role_source(Some(primary_role));
    }
    *self.aof.write() = aof;
  }

  /// AOF 门面（AOF 门控未点亮时 None）
  pub fn try_aof(&self) -> Option<Arc<GarnetAppendOnlyFile>> {
    self.aof.read().clone()
  }

  /// 注入运行时配置（对标 C# ClusterProvider.cs 构造期传入的 serverOptions
  /// 可达面；装配期一次调用，全仓唯一
  /// [`RuntimeServerConfig`] 实例的薄克隆——热更槽值由 CONFIG SET 就地落
  /// 槽，消费方每轮实时读取即时生效，杜绝第二张配置表与装配期快照）
  pub fn set_runtime_config(&self, runtime_config: Arc<RuntimeServerConfig>) {
    *self.runtime_config.write() = Some(runtime_config);
  }

  /// 运行时配置可达面（装配期未注入时 None）
  pub fn try_runtime_config(&self) -> Option<Arc<RuntimeServerConfig>> {
    self.runtime_config.read().clone()
  }

  /// 注入集群出站 TLS 客户端配置（装配期自 wconf TLS 字段投影构造，
  /// 五类出站消费点的单一配置源；映射口径见 [`ClientTlsConfig::new`]）
  #[cfg(feature = "tls")]
  pub fn set_cluster_tls_client(&self, tls: Option<Arc<ClientTlsConfig>>) {
    *self.cluster_tls_client.write() = tls;
  }

  /// 集群出站 TLS 客户端配置（未配置即 None = 明文集群）
  #[cfg(feature = "tls")]
  pub fn try_cluster_tls_client(&self) -> Option<Arc<ClientTlsConfig>> {
    self.cluster_tls_client.read().clone()
  }

  /// 注入本地物理日志句柄（AOF 门控点亮时装配期一次调用；副本发起同步的
  /// begin/tail 位点源与副本接收会话落盘目标共用同一实例）
  pub fn set_wal(&self, wal: Arc<WalLog<SegmentedDevice>>) {
    *self.wal.write() = Some(wal);
  }

  /// 本地物理日志句柄（AOF 门控未点亮时 None）
  pub fn try_wal(&self) -> Option<Arc<WalLog<SegmentedDevice>>> {
    self.wal.read().clone()
  }

  /// 注入副本接收面会话（AOF 门控点亮时装配期一次调用；CLUSTER APPENDLOG
  /// 记录帧经此落盘重放，对标 C# 会话侧 replicaReplaySession 可达面）
  pub fn set_replica_replication_session(
    &self,
    session: Option<Arc<ClusterReplicationSession<SegmentedDevice>>>,
  ) {
    *self.replica_replication.write() = session;
  }

  /// 副本接收面会话（未注入时 None）
  pub fn try_replica_replication_session(
    &self,
  ) -> Option<Arc<ClusterReplicationSession<SegmentedDevice>>> {
    self.replica_replication.read().clone()
  }

  /// 注入主端推流装配面（AOF 门控点亮时装配期一次调用；CLUSTER
  /// INITIATE_REPLICA_SYNC 发起面）
  pub fn set_primary_replication(&self, assets: Option<Arc<PrimaryReplicationAssets>>) {
    *self.primary_replication.write() = assets;
  }

  /// 主端推流装配面（未注入时 None）
  pub fn try_primary_replication(&self) -> Option<Arc<PrimaryReplicationAssets>> {
    self.primary_replication.read().clone()
  }

  /// 注入检查点目录（对标 C# clusterProvider 经 storeWrapper 反查
  /// CheckpointDir；快照发送源与副本接收落盘目标的公共根）
  pub fn set_checkpoint_dir(&self, dir: PathBuf) {
    *self.checkpoint_dir.write() = Some(dir);
  }

  /// 检查点目录（未注入时 None）
  pub fn try_checkpoint_dir(&self) -> Option<PathBuf> {
    self.checkpoint_dir.read().clone()
  }

  /// 采纳宿主引擎置换槽（对标 C# ClusterProvider.storeWrapper 装配注入）：
  /// 此后本层与宿主共读共写同一槽，一次置换两侧同时见新引擎；已播种的装配期
  /// 引擎随采纳迁入宿主槽，故与 [`Self::set_store`] 的先后次序无关
  pub fn set_store_swap_slot(&self, slot: StoreSwapSlot) {
    let mut current = self.store_slot.write();
    if let Some(seed) = current.get() {
      slot.swap(seed);
    }
    *current = slot;
  }

  /// 注入逻辑数据库管理器（装配期注入，对标 C# StoreWrapper.databaseManager）
  pub fn set_database_manager(&self, dm: Arc<SingleDatabaseManager<SegmentedDevice>>) {
    *self.database_manager.write() = Some(dm);
  }

  /// 逻辑数据库管理器句柄
  pub fn try_database_manager(&self) -> Option<Arc<SingleDatabaseManager<SegmentedDevice>>> {
    self.database_manager.read().clone()
  }

  /// 是否启用按需检查点（libs/server/Servers/GarnetServerOptions.cs:OnDemandCheckpoint）
  ///
  /// 消费面两处（与 C# 同点）：主端副本 attach 前的按需重拍判据（对标 C#
  /// ReplicaSyncSession.cs:280）与 [`Self::allow_data_loss`] 派生输入（对标 C#
  /// ReplicaSyncSession.cs:190）
  pub fn on_demand_checkpoint(&self) -> bool {
    self.on_demand_checkpoint.load(Ordering::Acquire)
  }

  /// 注入是否启用按需检查点（装配期自 RuntimeServerOptions 一次注入，
  /// 源 --on-demand-checkpoint / nested_text 同一配置面）
  pub fn set_on_demand_checkpoint(&self, enabled: bool) {
    self.on_demand_checkpoint.store(enabled, Ordering::Release);
  }

  /// 是否允许数据丢失（libs/server/Servers/GarnetServerOptions.cs:AllowDataLoss，
  /// libs/cluster/Server/ClusterProvider.cs:AllowDataLoss 直转）
  ///
  /// 全仓唯一算式即 C# 派生式 `UseAofNullDevice || (FastAofTruncate &&
  /// !OnDemandCheckpoint)`（GarnetServerOptions.cs:653-654）：本仓未移植 null AOF
  /// 设备，故只剩后一项。C# 该布尔派生只读、无写口，rust 同样不设 setter，
  /// 消费点（按需重拍混尽放行、DataLossCheck）直读本处；C# 在
  /// AofSyncDriverStore.cs:365、:469 由库内反查 provider 取值，rust 该库不持
  /// provider 引用，故由调用方传入（该位点的放行接线归驱动注册链单
  /// aof-driver-register-pre-transfer）
  #[inline]
  pub fn allow_data_loss(&self) -> bool {
    self.fast_aof_truncate() && !self.on_demand_checkpoint()
  }

  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaDisklessSync
  ///
  /// 无盘同步开关（副本侧同步发起端 diskless / diskbased 选路的唯一读取面，
  /// 对标 C# 四处驱动点直读的 `clusterProvider.serverOptions.ReplicaDisklessSync`：
  /// ReplicationManager.cs:271-272 断链重连、:604-605 启动 Recover、
  /// ReplicaOfCommand.cs:90-93、RespClusterReplicationCommands.cs:104-106）
  pub fn replica_diskless_sync(&self) -> bool {
    self.replica_diskless_sync.load(Ordering::Acquire)
  }

  /// 注入无盘同步开关（服务器总装期自 NodeArgs.repl_diskless_sync 一次注入）
  pub fn set_replica_diskless_sync(&self, enabled: bool) {
    self.replica_diskless_sync.store(enabled, Ordering::Release);
  }

  /// libs/server/Servers/ServerOptions.cs:Recover
  ///
  /// 重启恢复开关（启动期主动 attach 臂 [`ClusterProvider::start_replication_attach`]
  /// 的分支判据，对标 C# ReplicationManager.Start 直读的
  /// `clusterProvider.serverOptions.Recover`——该字段声明于基类 ServerOptions，
  /// 经 GarnetServerOptions 继承而来，故挂载点取定义处 ServerOptions.cs）
  pub fn recover(&self) -> bool {
    self.recover.load(Ordering::Acquire)
  }

  /// 注入重启恢复开关（服务器总装期自 NodeArgs.recover 一次注入）
  pub fn set_recover(&self, enabled: bool) {
    self.recover.store(enabled, Ordering::Release);
  }

  /// 按需拍摄快照并注册检查点条目（对标 C# StoreWrapper.TakeOnDemandCheckpointAsync）
  pub async fn take_on_demand_checkpoint(&self) -> Result<bool, String> {
    let Some(dm) = self.try_database_manager() else {
      return Ok(false);
    };
    let taken = dm
      .take_checkpoint(false)
      .await
      .map_err(|e| format!("On-demand checkpoint failed: {e}"))?;
    if !taken {
      return Ok(false);
    }
    if let Some(checkpoint_dir) = self.try_checkpoint_dir()
      && let Ok(Some(token)) = wcpr::find_latest_checkpoint(&checkpoint_dir)
      && let Ok(meta_bytes) = read(checkpoint_dir.join(wcpr::meta_filename(token)))
      && let Ok(meta) = CheckpointMeta::decode(&meta_bytes)
    {
      let sublogs = self
        .replication_manager()
        .map(|rm| rm.sublog_count())
        .unwrap_or(1);
      let covered_u64 = meta.checkpoint_aof_address.unwrap_or(0);
      let covered_addr = AofAddress::create(sublogs as i32, covered_u64 as i64);
      self
        .add_new_checkpoint_entry(true, covered_addr, token, token)
        .await;
    }
    Ok(true)
  }

  /// 在线引擎置换（副本检查点导入闭环收口）：单次写本层引擎槽——宿主已采纳
  /// 同槽时新引擎即对后续新会话装配生效，无需第二处更新（对标 C# 全体调用方
  /// 经 StoreWrapper.cs:41 单计算属性自动转发恢复后的引擎；存量会话随批纪元
  /// 自然收敛）
  pub fn swap_online_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    self.store_slot.read().swap(store);
  }

  /// 检查点导入落盘依赖束（三 arm 接收面现取现用；目录缺失时惰性创建）
  pub fn checkpoint_import_ctx(&self) -> Result<CheckpointImportCtx, String> {
    use crate::server::replication::receive_checkpoint_handler::CheckpointImportCtx;
    let dir = self
      .try_checkpoint_dir()
      .ok_or_else(|| ERR_CLUSTER_NOT_INITIALIZED.to_string())?;
    create_dir_all(&dir).map_err(|e| format!("IOERR create checkpoint dir: {e}"))?;
    let device = self
      .try_store()
      .ok_or_else(|| ERR_CLUSTER_NOT_INITIALIZED.to_string())?
      .device
      .clone();
    Ok(CheckpointImportCtx {
      store_device: device,
      checkpoint_dir: dir,
    })
  }

  /// 执行序列号生成器复位（故障转移触发时调用；对标 C#
  /// ReplicaFailoverSession.cs:154 经 storeWrapper.appendOnlyFile 直达
  /// GarnetAppendOnlyFile.ResetSequenceNumberGenerator，AOF 门面未装配
  /// 时空转——单物理日志模式 C# 侧同样短路）
  pub fn reset_sequence_number_generator(&self) {
    if let Some(aof) = self.try_aof() {
      aof.reset_sequence_number_generator();
    }
  }

  /// 注入副本重放最大滞后字节数（C# serverOptions.AofReplayMaxLagBytes 的
  /// 装配期注入；INFO 复制段直读）
  pub fn set_aof_replay_max_lag_bytes(&self, value: i32) {
    self
      .aof_replay_max_lag_bytes
      .store(value, Ordering::Relaxed);
  }

  /// 副本重放最大滞后字节数（C# runtimeConfig.GetInt(AOF_REPLAY_MAX_LAG_
  /// BYTES) 读取面：-1 = 异步重放不节流，0 = 同步重放（每帧锁步），>0 =
  /// 异步重放滞后超限阻塞推流；副本会话 ThrottlePrimary 门限源）
  #[inline]
  pub fn aof_replay_max_lag_bytes(&self) -> i32 {
    self.aof_replay_max_lag_bytes.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/ClusterProvider.cs:Dispose
  pub fn dispose(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.dispose();
    }
    if let Some(rm) = self.replication_manager() {
      rm.dispose();
    }
    if let Some(fm) = self.failover_manager() {
      fm.dispose();
    }
    if let Some(mm) = self.migration_manager() {
      mm.dispose();
    }
  }
}

impl IClusterProvider for ClusterProvider {
  /// libs/cluster/Server/ClusterProvider.cs:CreateClusterSession
  ///
  /// 构造即登记活跃会话弱引用表（C# 侧会话经 activeHandlers 承载，此处为
  /// 等价枚举源）。返回注册进表的同一 `Arc`——调用方（会话消费者装配 /
  /// 测试）持强引用，弱引用与会话生命周期闭合；C# 返回 IClusterSession
  /// 接口形态，rust 经 `Into<wnode ClusterSession>` 达成同款擦除
  fn create_cluster_session(&self) -> Arc<ClusterSession> {
    let Some(cp) = self.self_weak.get().and_then(|w| w.upgrade()) else {
      return Arc::new(ClusterSession::new(Arc::new(Self::default())));
    };
    let session = Arc::new(ClusterSession::new(cp));
    // 键取 Arc 指针地址（Weak::as_ptr 同址）：无锁登记，清扫按此键移除
    self
      .cluster_sessions
      .pin()
      .insert(Arc::as_ptr(&session) as usize, Arc::downgrade(&session));
    session
  }

  fn allow_data_loss(&self) -> bool {
    ClusterProvider::allow_data_loss(self)
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPublishAsync
  async fn cluster_publish_async<'a>(
    &'a self,
    cmd: RespCommand,
    channel: &'a [u8],
    message: &'a [u8],
  ) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.try_cluster_publish_async(cmd, channel, message).await;
    }
  }

  async fn recover_async(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.start();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:SafeTruncateAOF
  /// （IClusterProvider.cs:SafeTruncateAOF 声明）
  ///
  /// PRIMARY：经 AofSyncDriverStore 按全副本最小已发位点安全截断（记账 + 走
  /// [`GarnetLog::truncate_until_async`] 唯一物理回收真身即时删段，见
  /// AofSyncDriverStore::safe_truncate_aof）；
  /// REPLICA：物理截断本地 AOF 至指定位点。C# 该分支按 FastAofTruncate 二择
  /// （真 `Log.UnsafeShiftBeginAddress(truncateUntil, truncateLog: true)` 即时删段 /
  /// 假 `Log.TruncateUntil(truncateUntil)` 逻辑截断）；rust 依方案 B 取单一口径恒走
  /// 物理真身——提交面不删段，逻辑截断永不落盘；副本无 Commit，刷盘由复制流驱动。
  async fn safe_truncate_aof(&self, truncate_until: &AofAddress) {
    let Some(rm) = self.replication_manager() else {
      return;
    };
    if self.is_primary() {
      rm.aof_sync_driver_store
        .safe_truncate_aof(truncate_until)
        .await;
    } else if let Some(aof) = self.try_aof() {
      aof.log().truncate_until_async(truncate_until).await;
    }
  }

  fn prevent_role_change(&self) -> bool {
    if let Some(rm) = self.replication_manager() {
      rm.begin_recovery(RecoveryStatus::ReadRole, false)
    } else {
      true
    }
  }

  fn allow_role_change(&self) {
    if let Some(rm) = self.replication_manager() {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }
  }
}

/// 检查点回调切面（对标 C# ClusterProvider 对 IClusterProvider 检查点方法族的实现）
impl CheckpointCallbackFace for ClusterProvider {
  /// libs/server/Cluster/IClusterProvider.cs:OnCheckpointInitiated
  ///
  /// REPLICA 取检查点开始标记偏移（ReplicationCheckpointStartOffset），PRIMARY 取当前复制位点
  ///
  /// 与 C# `EnableAOF && clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA`
  /// 保持一致：仅按配置角色（`local_node_role()`）判定，不看恢复态。
  /// 不可用 `is_replica()`——它叠加了 `replication_manager.is_recovering()`，
  /// 会让主节点恢复期误入副本分支，错取 ReplicationCheckpointStartOffset
  /// （那是副本检查点开始标记的截断位点，主节点语义完全不同）。
  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress) {
    if let Some(rm) = self.replication_manager() {
      // 对标 C# CurrentConfig.LocalNodeRole == NodeRole.REPLICA 的配置角色直判
      let replica_by_config = self
        .cluster_manager()
        .is_some_and(|mgr| mgr.current_config().local_node_role() == NodeRole::Replica);
      if replica_by_config {
        *checkpoint_covered_aof_address = rm.get_replication_checkpoint_start_offset();
      } else {
        *checkpoint_covered_aof_address = rm.get_current_replication_offset();
      }
      rm.update_commit_safe_aof_address(checkpoint_covered_aof_address);
    }
  }

  /// libs/server/Cluster/IClusterProvider.cs:AddNewCheckpointEntry
  ///
  /// 登记新检查点条目到内存仓库并安全截断 AOF（对标 C# 逐字段构造）
  /// 保留 _object_store_checkpoint_token 形参以对标 IClusterProvider.AddNewCheckpointEntry 接口签名
  async fn add_new_checkpoint_entry(
    &self,
    full: bool,
    checkpoint_covered_aof_address: AofAddress,
    store_checkpoint_token: u128,
    // 保留 _object_store_checkpoint_token 形参以对标 IClusterProvider.AddNewCheckpointEntry 接口签名
    _object_store_checkpoint_token: u128,
  ) {
    if let Some(rm) = self.replication_manager() {
      let mut metadata = CheckpointMetadata::new(rm.sublog_count());
      metadata.store_version = checkpoint_version(store_checkpoint_token);
      metadata.store_hlog_token = store_checkpoint_token;
      metadata.store_index_token = store_checkpoint_token;
      metadata.store_checkpoint_covered_aof_address = checkpoint_covered_aof_address;
      metadata.store_primary_repl_id = Some(rm.primary_repl_id());

      // 供副本跟踪检查点历史：attach 新主时据此清理旧检查点
      rm.add_checkpoint_entry(CheckpointEntry::new(metadata), full);
    }
    self
      .safe_truncate_aof(&checkpoint_covered_aof_address)
      .await;
  }
}

impl WnodeClusterProvider for ClusterProvider {
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    true
  }

  /// 槽位归属本地判定（SWAPDB 库级门禁消费面）：C# ClusterConfig.IsLocal 的
  /// 写面口径，read_write_session 恒 false（副本不因读放行而获得换库资格，
  /// 换库为写操作，主节点回放/迁移源端态由 is_local 本体承载）
  fn is_slot_local(&self, slot: u16) -> bool {
    self
      .cluster_manager()
      .is_some_and(|cm| cm.current_config().is_local(slot, false))
  }

  /// libs/cluster/Server/ClusterProvider.cs:Start
  ///
  /// 启动集群后台治理与复制：clusterManager.Start() 拉起 gossip 半段 +
  /// replicationManager.Start() 启动期主动 attach 半段（重启后 REPLICA 首帧前
  /// 接入，二者均已在 compio 运行时内，由 GarnetServer.Start → Provider.Start 调用）
  fn start(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.start();
    }
    self.start_replication_attach();
  }

  fn flush_config(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.flush_config();
    }
  }

  #[inline]
  fn dispose(&self) {
    self.dispose();
  }

  /// libs/cluster/Server/ClusterProvider.cs:UpdateClusterAuth
  fn update_cluster_auth(
    &self,
    cluster_username: Option<String>,
    cluster_password: Option<String>,
  ) {
    let mut auth = self.auth_container.write();
    let old_user = auth.0.clone();
    *auth = (cluster_username.or(old_user), cluster_password);
  }

  /// CONFIG SET cluster-node-timeout 调停投影落点：直写本 provider 原子槽
  ///（gossip / failover / 集群管理全部消费面经 cluster_node_timeout() 单点
  /// 即时读取，对标 Gossip.cs:25 / FailoverManager.cs:24 每轮 GetTimeSpan
  /// (CLUSTER_NODE_TIMEOUT) 现取）；0 = 无限超时哨兵
  fn set_cluster_node_timeout_ms(&self, ms: u64) {
    self.set_cluster_node_timeout_ms(ms);
  }

  fn is_primary(&self) -> bool {
    self
      .cluster_manager()
      .map(|mgr| mgr.current_config().is_primary())
      .unwrap_or(true)
  }

  fn is_replica(&self) -> bool {
    let role_is_replica = self
      .cluster_manager()
      .map(|mgr| mgr.current_config().is_replica())
      .unwrap_or(false);
    let recovering = self
      .replication_manager()
      .map(|rm| rm.is_recovering())
      .unwrap_or(false);
    role_is_replica || recovering
  }

  fn is_replica_node(&self, node_id: u128) -> bool {
    self
      .cluster_manager()
      .map(|mgr| {
        mgr
          .current_config()
          .workers
          .iter()
          .any(|w| w.nodeid == Some(node_id) && w.role == NodeRole::Replica)
      })
      .unwrap_or(false)
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetRunId
  fn get_run_id(&self) -> String {
    if let Some(rm) = self.replication_manager() {
      rm.primary_repl_id()
    } else {
      self
        .cluster_manager()
        .and_then(|mgr| mgr.current_config().local_node_id())
        .map_or_else(String::new, hex_str_u128)
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetPrimaryInfo
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    let Some(rm) = self.replication_manager() else {
      return (AofAddress::default(), Vec::new());
    };
    let offset = rm.get_current_replication_offset();
    let cm = self.cluster_manager();
    let replicas = rm
      .aof_sync_driver_store
      .get_replica_info(&offset)
      .into_iter()
      .map(|info| {
        let (address, port) = cm
          .as_ref()
          .map(|cm| {
            cm.current_config()
              .get_worker_address_from_node_id(info.node_id)
          })
          .unwrap_or((None, 0));
        RoleInfo {
          replication_offset: info.replication_offset.get(0).unwrap_or(0),
          replication_lag: info.replication_lag.get(0).unwrap_or(0),
          replication_state: if info.is_connected {
            "online"
          } else {
            "offline"
          }
          .into(),
          address: address.unwrap_or_default(),
          port,
          ..RoleInfo::default()
        }
      })
      .collect();
    (offset, replicas)
  }

  fn get_replica_info(&self) -> RoleInfo {
    let Some(rm) = self.replication_manager() else {
      return RoleInfo::default();
    };
    let Some(cm) = self.cluster_manager() else {
      return RoleInfo::default();
    };
    let config = cm.current_config();
    let (address, port) = config.get_local_node_primary_address();
    let connected = config
      .local_node_primary_id()
      .is_some_and(|id| cm.get_connection_info(id).connected);
    RoleInfo {
      replication_offset: rm.get_replication_offset(0),
      replication_state: if rm.is_recovering() {
        "sync"
      } else if connected {
        "connected"
      } else {
        "connect"
      }
      .into(),
      address: address.unwrap_or_default(),
      port,
      ..RoleInfo::default()
    }
  }

  fn get_replication_info(&self) -> Vec<MetricsItem> {
    let Some(rm) = self.replication_manager() else {
      return Vec::new();
    };
    let is_pri = self.is_primary();
    let role = if is_pri { "master" } else { "slave" };
    let failover_status = self
      .failover_manager()
      .map(|fm| fm.get_failover_status())
      .unwrap_or_else(|| "no-failover".to_string());
    let last_failover_status = self
      .failover_manager()
      .map(|fm| fm.get_last_failover_status())
      .unwrap_or_else(|| "no-failover".to_string());
    let cur_offset = rm.get_current_replication_offset().to_aof_string();
    let offset2 = rm.get_replication_offset2().to_aof_string();
    let rec_status: &'static str = rm.recovery_status().into();
    let connected_slaves = rm.aof_sync_driver_store.count_connected_replicas();
    let sync_driver_count = rm.aof_sync_driver_store.count();

    let mut num_buf = Buffer::new();
    let mut items = vec![
      MetricsItem::new("role", role),
      MetricsItem::new("connected_slaves", num_buf.format(connected_slaves)),
      MetricsItem::new("master_failover_state", failover_status),
      MetricsItem::new("master_replid", rm.primary_repl_id()),
      MetricsItem::new("master_replid2", rm.primary_repl_id2()),
      MetricsItem::new("master_repl_offset", cur_offset.clone()),
      MetricsItem::new("second_repl_offset", offset2),
      MetricsItem::new(
        "store_current_safe_aof_address",
        rm.get_current_safe_aof_address().to_aof_string(),
      ),
      MetricsItem::new(
        "store_recovered_safe_aof_address",
        rm.get_recovered_safe_aof_address().to_aof_string(),
      ),
      MetricsItem::new("recover_status", rec_status),
      MetricsItem::new("last_failover_state", last_failover_status),
      MetricsItem::new("sync_driver_count", num_buf.format(sync_driver_count)),
    ];
    if !is_pri && let Some(cm) = self.cluster_manager() {
      let config = cm.current_config();
      let (addr, port) = config.get_local_node_primary_address();
      if let Some(a) = addr {
        items.push(MetricsItem::new("master_host", a));
      }
      items.push(MetricsItem::new("master_port", num_buf.format(port)));
      let link_status = cm.get_primary_link_status(&config);
      items.push(link_status[0].clone());
      items.push(link_status[1].clone());
      items.push(MetricsItem::new(
        "master_sync_in_progress",
        rm.is_recovering().to_string(),
      ));
      items.push(MetricsItem::new("slave_read_repl_offset", cur_offset));
      items.push(MetricsItem::new("slave_priority", "100"));
      items.push(MetricsItem::new("slave_read_only", "1"));
      items.push(MetricsItem::new("replica_announced", "1"));
      items.push(MetricsItem::new(
        "master_sync_last_io_seconds_ago",
        num_buf.format(rm.last_primary_sync_seconds()),
      ));
      let (vec_lag, acc_lag, sublog_vector, drift_vector) = match self
        .try_aof()
        .map(|aof| (aof.log().tail_address(), aof.read_consistency_manager()))
      {
        Some((tail, rcm)) => {
          let offset = rm.get_current_replication_offset();
          let sublog_vector = rcm.as_ref().map_or_else(
            || "-1".to_string(),
            |m| m.get_physical_sublog_max_sequence_vector(),
          );
          let drift_vector = rcm.as_ref().map_or_else(
            || "-1".to_string(),
            |m| m.get_physical_sublog_max_drift_sequence_vector(),
          );
          (
            tail.diff(&offset).to_aof_string(),
            tail.aggregate_diff(&offset).to_string(),
            sublog_vector,
            drift_vector,
          )
        }
        None => (
          "0".to_string(),
          "0".to_string(),
          "-1".to_string(),
          "-1".to_string(),
        ),
      };
      items.push(MetricsItem::new("replication_offset_vector_lag", vec_lag));
      items.push(MetricsItem::new("replication_offset_acc_lag", acc_lag));
      items.push(MetricsItem::new(
        "aof_replay_max_lag_bytes",
        self
          .aof_replay_max_lag_bytes
          .load(Ordering::Relaxed)
          .to_string(),
      ));
      items.push(MetricsItem::new(
        "physical_sublog_max_sequence_vector",
        sublog_vector,
      ));
      items.push(MetricsItem::new(
        "physical_sublog_max_drift_sequence_vector",
        drift_vector,
      ));
    } else {
      // 主端 slaveN 行 = C# replicaInfo[i].ToString() 逐项入列
      //（libs/cluster/Server/ClusterProvider.cs:263-267），复用 get_primary_info
      // 的 RoleInfo 投影（端点反查单点），ReplicaRoleInfo 保持内部身份不再对外渲染
      for (i, replica) in self.get_primary_info().1.into_iter().enumerate() {
        items.push(MetricsItem::new(format!("slave{i}"), replica.to_string()));
      }
    }
    items
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetCheckpointInfo
  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    if let Some(rm) = self.replication_manager() {
      vec![
        MetricsItem::new(
          "memory_checkpoint_entry",
          rm.get_latest_checkpoint_from_memory_info(),
        ),
        MetricsItem::new(
          "disk_checkpoint_entry",
          rm.get_latest_checkpoint_from_disk_info(),
        ),
      ]
    } else {
      vec![
        MetricsItem::new("memory_checkpoint_entry", "(empty)"),
        MetricsItem::new("disk_checkpoint_entry", "(empty)"),
      ]
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetGossipStats
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    if let Some(gm) = self.gossip_manager() {
      let open_conns = gm.connection_store.count();
      gm.stats.to_metrics_items(metrics_disabled, open_conns)
    } else {
      Vec::new()
    }
  }

  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    vec![
      MetricsItem::new(
        "migration_manager",
        self
          .migration_manager()
          .map(|mm| mm.get_buffer_pool_stats())
          .unwrap_or_default(),
      ),
      MetricsItem::new(
        "replication_manager",
        self
          .replication_manager()
          .map(|rm| rm.get_buffer_pool_stats())
          .unwrap_or_default(),
      ),
    ]
  }

  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    match manager_type {
      ManagerType::MigrationManager => {
        if let Some(mm) = self.migration_manager() {
          mm.purge();
        }
      }
      ManagerType::ReplicationManager => {
        if let Some(rm) = self.replication_manager() {
          rm.purge();
        }
      }
      ManagerType::ServerListener => {
        log::error!("PURGEBP: ServerListener buffer pool purge is not supported");
      }
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ResetGossipStats
  fn reset_gossip_stats(&self) {
    if let Some(gm) = self.gossip_manager() {
      gm.stats.reset();
    }
  }

  #[inline]
  fn aof_sublog_count(&self) -> usize {
    self
      .replication_manager()
      .map(|rm| rm.sublog_count())
      .unwrap_or(1)
  }

  /// 全租户换号广播协调者注入（doc/zh/db.md 4.5；应答字节闭环：收齐全部
  /// Primary 的 +OK ack 才回 +OK，任一失败/超时回错误，入口据此应答）
  fn flushall_broadcast(&self, ns: u64) -> Option<SlowFuture> {
    let mgr = self.cluster_manager()?;
    Some(SlowFuture::new(async move {
      let mut out = Vec::new();
      match mgr.flushall_broadcast_async(ns).await {
        Ok(()) => out.write_resp_simple_string("OK"),
        Err(e) => out.write_resp_error(&format!("ERR FLUSHALL broadcast failed: {e}")),
      }
      out
    }))
  }

  /// 检查点版本切换开始（对标 C# ReplicationManager.CheckpointVersionShiftStart 委托）
  ///
  /// 检查点内核在版本推进（IN_PROGRESS）处经本句柄调用；REPLICA 角色直接返回
  /// （C# rm 首行判定，Rust rm 不持 clusterManager，判定上移本层），主库转发
  /// ReplicationManager 经提交通道写 CheckpointStartCommit 标记
  fn checkpoint_version_shift_start(&self, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_start(new_version);
    }
  }

  /// 检查点版本切换结束（对标 C# ReplicationManager.CheckpointVersionShiftEnd 委托）
  ///
  /// 快照成功、截断之前（WAIT_FLUSH）经本句柄调用；角色判定同
  /// [`Self::checkpoint_version_shift_start`]（失败路径不到此处）
  fn checkpoint_version_shift_end(&self, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_end(new_version);
    }
  }
}
