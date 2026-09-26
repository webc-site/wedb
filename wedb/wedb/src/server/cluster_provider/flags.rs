//! 运行期旋钮与布尔标志对：每个旋钮的「字段—写口—读者」在本件内同屏核对
//! （迁移/无盘同步块上限、复制重连轮询频率、FastAofTruncate、无盘同步攒批
//! 窗口、集群节点超时、gossip 周期与抽样、重定向端点偏好、按需检查点、
//! 允许数据丢失、无盘同步开关、重启恢复开关）

use std::{sync::atomic::Ordering, time::Duration};

use wconf::ServerConfigType;

use crate::server::{
  cluster::ClusterPreferredEndpointType, cluster_provider::ClusterProvider,
  migration::migration_manager::DEFAULT_MAX_SEND_BUFFER_CONTENT_SIZE,
};

impl ClusterProvider {
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
  /// 源 --fast-aof-truncate / toml 同一配置面）
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

  /// 无盘同步 leader 攒批窗口秒数（<= 0 = 关闭攒批；优先从 runtime_config
  /// 实时读取，未注入回退原子槽；C# runtimeConfig.GetInt(REPL_DISKLESS_SYNC_DELAY) 消费面）
  #[inline]
  pub fn replica_diskless_sync_delay(&self) -> i32 {
    if let Some(cfg) = self.runtime_config.read().as_ref() {
      cfg.get_int(ServerConfigType::ReplDisklessSyncDelay)
    } else {
      self
        .replica_diskless_sync_delay_secs
        .load(Ordering::Acquire)
    }
  }

  /// 副本 attach 级握手/编排超时（优先从 runtime_config 动态读取，None 表示无限；未注入回退 Some(60s) 保嵌入形态）
  #[inline]
  pub fn repl_attach_timeout(&self) -> Option<Duration> {
    if let Some(cfg) = self.runtime_config.read().as_ref() {
      cfg.get_time_span(ServerConfigType::ReplAttachTimeout)
    } else {
      Some(Duration::from_secs(60))
    }
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

  /// 是否启用按需检查点（libs/server/Servers/GarnetServerOptions.cs:OnDemandCheckpoint）
  ///
  /// 消费面两处（与 C# 同点）：主端副本 attach 前的按需重拍判据（对标 C#
  /// ReplicaSyncSession.cs:280）与 [`Self::allow_data_loss`] 派生输入（对标 C#
  /// ReplicaSyncSession.cs:190）
  pub fn on_demand_checkpoint(&self) -> bool {
    self.on_demand_checkpoint.load(Ordering::Acquire)
  }

  /// 注入是否启用按需检查点（装配期自 RuntimeServerOptions 一次注入，
  /// 源 --on-demand-checkpoint / toml 同一配置面）
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
