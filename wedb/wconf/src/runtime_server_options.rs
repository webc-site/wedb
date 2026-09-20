use crate::{log_compaction_type::LogCompactionType, node_options::DEFAULT_SLOW_LOG_MAX_ENTRIES};

/// `RuntimeServerConfig` 消费的启动选项子集。
///
/// 对标 libs/server/Servers/GarnetServerOptions.cs:GarnetServerOptions 中被
/// RuntimeServerConfig 构造（Init 播种 + 只读回落格式化）读取的字段。
///
/// 字段默认值与 C# 字段初始化器逐项一致。
#[derive(Debug, Clone)]
pub struct RuntimeServerOptions {
  // —— Init 播种字段 ——
  /// libs/server/Servers/GarnetServerOptions.cs:ClusterTimeout（默认 60，秒）。
  pub cluster_timeout: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaSyncDelayMs（默认 5，毫秒）。
  pub replica_sync_delay_ms: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayMaxLagBytes（默认 -1）。
  pub aof_replay_max_lag_bytes: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:AofTailWitnessFreqMs（默认 10，毫秒；对标 defaults.conf:179）。
  pub aof_tail_witness_freq_ms: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:AofSyncMaxLagBytes（默认 -1）。
  pub aof_sync_max_lag_bytes: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaDisklessSyncDelay（默认 5，秒）。
  pub replica_diskless_sync_delay: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaAttachTimeout（C# 为 TimeSpan，此处以秒表达；
  /// CLI/CONFIG 面的秒数语义：<= 0 视为无限超时）。
  pub replica_attach_timeout_secs: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:ClusterReplicationReestablishmentTimeout（默认 0，秒）。
  pub cluster_replication_reestablishment_timeout: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:CompactionMaxSegments（默认 32）。
  pub compaction_max_segments: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:CompactionType（默认 None）。
  /// C# 的 CompactionForceDelete 不移植：wedb 紧缩/移位经设备截断无条件物理
  /// 回收历史段，forceDelete 次序无对位需求（见 server_config_type 说明）。
  pub compaction_type: LogCompactionType,
  /// libs/server/Servers/GarnetServerOptions.cs:SlowLogThreshold（默认 0，微秒）。
  pub slow_log_threshold: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:SlowLogMaxEntries（默认 128；StoreWrapper.cs:243
  /// 慢日志容器构造容量）。
  pub slow_log_max_entries: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ObjectScanCountLimit（默认 1000）。
  pub object_scan_count_limit: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:EnableScatterGatherGet（默认 true）。
  pub enable_scatter_gather_get: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:AofSizeLimitEnforceFrequencySecs（默认 5，秒）。
  pub aof_size_limit_enforce_frequency_secs: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:CommitFrequencyMs（默认 0：逐操作自动提交）。
  pub commit_frequency_ms: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ExpiredObjectCollectionFrequencySecs（默认 0：禁用）。
  pub expired_object_collection_frequency_secs: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ExpiredKeyDeletionScanFrequencySecs（默认 -1：禁用）。
  pub expired_key_deletion_scan_frequency_secs: i32,

  // —— 只读回落格式化字段（CONFIG GET 经选项直读，无运行时槽位）——
  /// libs/server/Servers/GarnetServerOptions.cs:EnableAOF（默认 false）。
  pub enable_aof: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:MaxDatabases（默认 16）。
  pub max_databases: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:CheckpointBaseDirectory（派生属性）。
  pub checkpoint_base_directory: String,
  /// libs/server/Servers/ServerOptions.cs:LogDir（可空）。
  pub log_dir: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:UnixSocketPath（可空）。
  pub unix_socket_path: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:EnableCluster（默认 false）。
  pub enable_cluster: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:AofMemorySize（默认 "128m"）。
  pub aof_memory_size: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofPageSize（默认 "32m"）。
  pub aof_page_size: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofSegmentSize（默认 "1g"）。
  pub aof_segment_size: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofPhysicalSublogCount（默认 1）。
  pub aof_physical_sublog_count: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayTaskCount（默认 1）。
  pub aof_replay_task_count: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:WaitForCommit（默认 false；参数源
  /// `--aof-commit-wait`，NodeArgs::aof_commit_wait 投影）。
  pub wait_for_commit: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:AofSizeLimit（默认 ""）。
  pub aof_size_limit: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayDriftThreshold（默认 -1）。
  pub replay_drift_threshold: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayDriftCheckFreq（默认 0）。
  pub replay_drift_check_freq: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaSyncTimeout（默认 5s）：副本一致读等待
  /// 回放推进与回放对齐栅栏的阻塞上界，超时上抛中止（ReadSessionState 读
  /// 超时来源；纯启动选项，C# RuntimeServerConfig 亦不暴露 CONFIG 面）
  pub replica_sync_timeout_secs: u64,
  /// libs/server/Servers/GarnetServerOptions.cs:FastAofTruncate（默认 false）。
  pub fast_aof_truncate: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:OnDemandCheckpoint（默认 true）。按需检查点开关，
  /// 装配期注进 ClusterProvider 后由其唯一消费面读取（C# 同为
  /// serverOptions.OnDemandCheckpoint 直读：
  /// ReplicaSyncSession.cs:190、:280），C# RuntimeServerConfig 未设该 CONFIG 名额，
  /// 故此字段不播种槽位、不注册只读格式器。
  pub on_demand_checkpoint: bool,
}

impl Default for RuntimeServerOptions {
  /// 逐字段对齐 GarnetServerOptions.cs 的字段初始化器。
  fn default() -> Self {
    Self {
      cluster_timeout: 60,
      replica_sync_delay_ms: 5,
      aof_replay_max_lag_bytes: -1,
      aof_tail_witness_freq_ms: 10,
      aof_sync_max_lag_bytes: -1,
      replica_diskless_sync_delay: 5,
      replica_attach_timeout_secs: 60,
      cluster_replication_reestablishment_timeout: 0,
      compaction_max_segments: 32,
      compaction_type: LogCompactionType::None,
      slow_log_threshold: 0,
      slow_log_max_entries: DEFAULT_SLOW_LOG_MAX_ENTRIES,
      object_scan_count_limit: 1000,
      enable_scatter_gather_get: true,
      aof_size_limit_enforce_frequency_secs: 5,
      commit_frequency_ms: 0,
      expired_object_collection_frequency_secs: 0,
      expired_key_deletion_scan_frequency_secs: -1,
      enable_aof: false,
      max_databases: 16,
      checkpoint_base_directory: String::new(),
      log_dir: None,
      unix_socket_path: None,
      enable_cluster: false,
      aof_memory_size: Some("128m".into()),
      aof_page_size: Some("32m".into()),
      aof_segment_size: Some("1g".into()),
      aof_physical_sublog_count: 1,
      aof_replay_task_count: 1,
      replay_drift_threshold: -1,
      replay_drift_check_freq: 0,
      replica_sync_timeout_secs: 5,
      wait_for_commit: false,
      aof_size_limit: Some(String::new()),
      fast_aof_truncate: false,
      on_demand_checkpoint: true,
    }
  }
}
