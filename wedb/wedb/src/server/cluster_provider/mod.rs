mod assets;
mod checkpoint;
mod traits;

use std::{
  fs::metadata,
  path::{Path, PathBuf},
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU8, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::runtime::spawn;
use parking_lot::RwLock;
use waof::WalLog;
use wbase::{
  hex::hex_str_u128,
  map::{ConcurrentMap, new_concurrent_map},
};
use wconf::{RuntimeServerConfig, node_options::DEFAULT_ON_DEMAND_CHECKPOINT};
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;
use wdev::SegmentedDevice;
use wnode::{
  ClusterProvider as WnodeClusterProvider, ClusterProviderHandle, PrimaryTasks,
  aof::garnet_append_only_file::GarnetAppendOnlyFile, database::SingleDatabaseManager,
  resp::vector::vector_manager::VectorManager, service::StoreSwapSlot,
};
use wpubsub::subscribe_broker::SubscribeBroker;

use crate::{
  args::{DEFAULT_CLUSTER_NODE_TIMEOUT_MS, DEFAULT_GOSSIP_DELAY_MS, DEFAULT_GOSSIP_SAMPLE_PERCENT},
  error,
  server::{
    cluster::{ClusterPreferredEndpointType, IClusterProvider},
    cluster_config::ClusterConfig,
    cluster_manager::{ClusterManager, read_device},
    cluster_session::ClusterSession,
    connection_info::ConnectionInfo,
    failover::failover_manager::FailoverManager,
    gossip::gossip_manager::GossipManager,
    migration::migration_manager::{DEFAULT_MAX_SEND_BUFFER_CONTENT_SIZE, MigrationManager},
    replication::{
      aof_replication_pump::AofReplicationPump, assembly::try_replicate_sync_async,
      cluster_replication_session::ClusterReplicationSession,
      replica_sync_session::ReplicaSyncSession, replicate_sync_options::ReplicateSyncOptions,
      replication_manager::ReplicationManager,
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
}
