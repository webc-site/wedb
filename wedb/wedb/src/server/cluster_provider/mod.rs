//! 集群提供者核心门面（对标 C# Garnet.cluster.ClusterProvider）
//!
//! 本件只留类型与字段块、默认值、角色与句柄、子管理器取口，对齐 C# 本体规模；
//! 其余职责按 C# 分件形态置于同目录：assets.rs 装配期注入槽族、
//! flags.rs 运行期旋钮与布尔标志对、replication.rs 管理器初始化与复制编排链、
//! checkpoint.rs 纪元机制与检查点/恢复面、traits.rs 三个契约 trait 实现。

mod assets;
mod checkpoint;
mod flags;
mod replication;
mod traits;

use std::{
  path::PathBuf,
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU8, AtomicU64, Ordering},
  },
};

use parking_lot::RwLock;
use waof::WalLog;
use wbase::map::{ConcurrentMap, new_concurrent_map};
use wconf::{RuntimeServerConfig, node_options::DEFAULT_ON_DEMAND_CHECKPOINT};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  ClusterProvider as WnodeClusterProvider, ClusterProviderHandle, PrimaryTasks,
  aof::garnet_append_only_file::GarnetAppendOnlyFile, database::SingleDatabaseManager,
  resp::vector::vector_manager::VectorManager, servers::ConsumerRegistry, service::StoreSwapSlot,
};
use wpubsub::subscribe_broker::SubscribeBroker;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  args::{DEFAULT_CLUSTER_NODE_TIMEOUT_MS, DEFAULT_GOSSIP_DELAY_MS, DEFAULT_GOSSIP_SAMPLE_PERCENT},
  server::{
    cluster::ClusterPreferredEndpointType,
    cluster_manager::ClusterManager,
    cluster_session::ClusterSession,
    failover::failover_manager::FailoverManager,
    gossip::gossip_manager::GossipManager,
    migration::migration_manager::MigrationManager,
    replication::{
      aof_replication_pump::AofReplicationPump,
      cluster_replication_session::ClusterReplicationSession,
      replica_sync_session::ReplicaSyncSession, replication_manager::ReplicationManager,
    },
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
  /// RuntimeServerOptions 注入，--on-demand-checkpoint / toml 同源）
  on_demand_checkpoint: AtomicBool,
  /// 无盘同步开关（C# GarnetServerOptions.ReplicaDisklessSync，默认 false；
  /// 副本侧同步发起端 diskless / diskbased 选路的唯一读取面，装配期自
  /// NodeArgs.repl_diskless_sync 注入）
  replica_diskless_sync: AtomicBool,
  /// 重启恢复开关（C# GarnetServerOptions.Recover，默认 false；启动期主动
  /// 重启恢复开关（C# GarnetServerOptions.Recover，默认 false；启动期主动
  /// attach 臂 ReplicationManager.Start 的分支判据，装配期自 NodeArgs.recover 注入）
  recover: AtomicBool,
  /// 存储设备污染标记（如副本接收检查点失败导致设备文件部分覆写坏态）：
  /// 跨会话的管理面屏障，只由一次成功的全量恢复收口清除（接收面的本轮
  /// 闸门在 ReceiveCheckpointHandler，会话初始化即净态）
  device_contaminated: AtomicBool,
  /// 集群出站 TLS 客户端配置（gossip / 复制 / 迁移 / failover 五类出站
  /// 连接的同一单源；对标 C# 各消费点直读
  /// clusterProvider.serverOptions.TlsOptions?.TlsClientOptions 的可达面，
  /// rust 收敛为装配期一次注入、单一访问口；None = 明文集群）
  #[cfg(feature = "tls")]
  cluster_tls_client: RwLock<Option<Arc<ClientTlsConfig>>>,
  /// 本实例活跃消费者注册表（装配期自宿主 StorageSessionProvider 注入，
  /// [`Self::set_consumer_registry`]；None = 未装配宿主的退化形态——单测
  /// 直驱置换面，清扫回退进程级 [`ConsumerRegistry::global`] 兜底）。副本
  /// 检查点导入的置换清扫射程单点：实例级槽保证多实例同进程形态（嵌入
  /// 双实例等）下换引擎只断本实例的客户端会话，不越槽误杀他实例连接
  ///（票 zcode-r37-lockfix 发现 A 残留，txnfix2 第二节 6 条跨实例爆炸半径）
  consumer_registry: RwLock<Option<Arc<ConsumerRegistry>>>,
  /// 引擎在线置换写面钩子束（宿主注入的换机重挂回调：WATCH 版本推进钩子 +
  /// AOF per-op 事件汇，装配期 [`assets`]/[`Self::set_engine_swap_hooks`]
  /// 单点注入；None = 无宿主钩子的退化形态——部分单测直驱置换面）。C# 锚点：
  /// 副本恢复为原位恢复（SingleDatabaseManager.RecoverCheckpointAsync 同一
  /// store 对象上重建），functionsState 钩子接线跨恢复全程存活；rust 实例
  /// 置换形态下由 [`checkpoint`]/[`Self::swap_online_store`] 在投槽前消费
  /// 本束补挂，杜绝换机即断线（钩子缺席时 WATCH 版本推进静默旁路、AOF
  /// per-op 镜像零条目）
  engine_swap_hooks: RwLock<Option<EngineSwapHooks>>,
}

/// 引擎置换钩子束类型（入参 = 换入引擎；宿主侧构造单点为 wnode
/// `StorageSessionProvider::engine_swap_hook_bundle`）
pub type EngineSwapHooks = Arc<dyn Fn(&Arc<WedbStore<SegmentedDevice>>) + Send + Sync>;

// SAFETY: ClusterProvider 的全部字段均为原子类型（Atomic*）、并发容器（ConcurrentMap）或带锁结构（RwLock/OnceLock），
// 内部不存在任何未经同步的内部可变性。显式实现 Send 与 Sync 用于打破与 ClusterReplicationSession
// 之间因相互持有 Arc 而触发的 auto trait 循环递归求值（#159228）。
unsafe impl Send for ClusterProvider {}
unsafe impl Sync for ClusterProvider {}

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
      device_contaminated: AtomicBool::new(false),
      #[cfg(feature = "tls")]
      cluster_tls_client: RwLock::new(None),
      consumer_registry: RwLock::new(None),
      engine_swap_hooks: RwLock::new(None),
    }
  }
}

impl ClusterProvider {
  /// 检查底层设备是否处于污染状态
  #[inline]
  pub fn is_device_contaminated(&self) -> bool {
    self.device_contaminated.load(Ordering::Acquire)
      || self
        .replication_manager()
        .is_some_and(|rm| rm.is_device_contaminated())
  }

  /// 标记底层设备已损坏/污染（管理面屏障：本地 flush / take_checkpoint 在
  /// 下一轮全量恢复收口前拒绝服务）
  ///
  /// 不代置 rm 接收闸门：接收闸门是本轮会话态，C# 每轮 attach 全新
  /// ReceiveCheckpointHandler（ReplicaDiskbasedSync.cs:161）即净态，代置
  /// 会把瞬时故障升级成跨会话永久闭锁；两面标记在
  /// [`Self::is_device_contaminated`] 已并校，需同时关闭的调用点各自显式
  /// 调 rm 侧接口
  #[inline]
  pub fn mark_device_contaminated(&self) {
    self.device_contaminated.store(true, Ordering::Release);
  }

  /// 新一轮全量同步会话初始化（C# `recvCheckpointHandler = new(...)`
  /// （ReplicaDiskbasedSync.cs:161）的 rust 对位，调用点
  /// [`recover_replication`](crate::server::replication::assembly::recover_replication)）：
  /// 接收面净态放行新一轮从头覆盖重推；
  /// 上轮留有未恢复的 StoreHlog 半写时，把设备损坏事实升级到本层管理面
  /// 屏障（C# 无此屏障，rust 直写在线设备布局自加，解除点唯一＝成功恢复
  /// 收口 [`Self::clear_device_contaminated`]）
  pub fn reset_recv_checkpoint_handler(&self) {
    if let Some(rm) = self.replication_manager()
      && rm.reset_recv_checkpoint_handler()
    {
      self.mark_device_contaminated();
    }
  }

  /// 解除管理面污染屏障（一次成功的全量恢复收口，与
  /// [`ReplicationManager::on_recovery_success`] 同位成对；两面一并净态）
  #[inline]
  pub fn clear_device_contaminated(&self) {
    self.device_contaminated.store(false, Ordering::Release);
    if let Some(rm) = self.replication_manager() {
      rm.clear_device_contaminated();
    }
  }

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

  /// 获取 GossipManager 句柄
  #[inline]
  pub fn gossip_manager(&self) -> Option<Arc<GossipManager>> {
    self.gossip_manager.read().clone()
  }
}
