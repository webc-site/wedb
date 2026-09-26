use wbase::cfg::LogCompactionType;

use crate::node_options::{
  DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS, DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
  DEFAULT_AOF_TAIL_WITNESS_FREQ_MS, DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
  DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
  DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS, DEFAULT_HLOG_PAGE_SIZE, DEFAULT_MAX_DATABASES,
  DEFAULT_OBJECT_SCAN_COUNT_LIMIT, DEFAULT_ON_DEMAND_CHECKPOINT,
  DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS, DEFAULT_REPLICA_SYNC_DELAY_MS,
  DEFAULT_REPLICA_SYNC_TIMEOUT_SECS, DEFAULT_SLOW_LOG_MAX_ENTRIES, DEFAULT_SLOW_LOG_THRESHOLD,
  DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
};

/// `RuntimeServerConfig` 消费的启动选项子集。
///
/// 对标 libs/server/Servers/GarnetServerOptions.cs:GarnetServerOptions 中被
/// RuntimeServerConfig 构造（Init 播种 + 只读回落格式化）读取的字段。
pub const DEFAULT_CLUSTER_TIMEOUT: i32 = 60;

/// AOF 回放滞后节流预算缺省字节（对标 GarnetServerOptions.cs:387
/// AofReplayMaxLagBytes = -1；-1 = 无限滞后）
pub const DEFAULT_AOF_REPLAY_MAX_LAG_BYTES: i32 = -1;
/// 无盘同步宽限期缺省秒数（对标 GarnetServerOptions.cs:415
/// ReplicaDisklessSyncDelay = 5）
pub const DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS: i32 = 5;
/// 哈希索引紧缩前的段数上限缺省（对标 GarnetServerOptions.cs:236
/// CompactionMaxSegments = 32）
pub const DEFAULT_COMPACTION_MAX_SEGMENTS: i32 = 32;
/// AOF 提交节拍缺省毫秒数（对标 GarnetServerOptions.cs:157 CommitFrequencyMs = 0；
/// 0 = 逐操作自动提交，<0 = 手动提交档）
pub const DEFAULT_COMMIT_FREQUENCY_MS: i32 = 0;
/// AOF 物理子日志缺省条数（对标 GarnetServerOptions.cs:117
/// AofPhysicalSublogCount = 1）
pub const DEFAULT_AOF_PHYSICAL_SUBLOG_COUNT: i32 = 1;
/// AOF 回放任务缺省并发数（对标 GarnetServerOptions.cs:122
/// AofReplayTaskCount = 1）
pub const DEFAULT_AOF_REPLAY_TASK_COUNT: i32 = 1;
/// AOF 回放漂移阈值缺省（对标 GarnetServerOptions.cs:128
/// AofReplayDriftThreshold = -1；-1 = 关闭漂移体检）
pub const DEFAULT_AOF_REPLAY_DRIFT_THRESHOLD: i64 = -1;
/// AOF 回放漂移检查周期缺省（对标 GarnetServerOptions.cs:139
/// AofReplayDriftCheckFreq = 1；<= 0 = 禁用）
pub const DEFAULT_AOF_REPLAY_DRIFT_CHECK_FREQ: i64 = 1;
/// GET 散集合并缺省开关（对标 GarnetServerOptions.cs:376
/// EnableScatterGatherGet = true）
pub const DEFAULT_ENABLE_SCATTER_GATHER_GET: bool = true;

/// 运行时服务器选项。主要提供纯运行时旋钮，少数与启动选项互补或作为内部参数。
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
  /// 本仓兼职注：同为分层键后台降阶评估轮宿主任务启动门（doc/zh/deviations.md §120）。
  pub expired_object_collection_frequency_secs: i32,
  /// libs/server/Servers/GarnetServerOptions.cs:ExpiredKeyDeletionScanFrequencySecs（默认 -1：禁用）。
  pub expired_key_deletion_scan_frequency_secs: i32,

  // —— 启动播种、CONFIG 只读回显字段（启动期经选项装配物理层；CONFIG GET 经
  // 选项直读回显，无运行时槽位）——
  /// libs/server/Servers/ServerOptions.cs:46 PageSize（"16m" 字段初始化器）的
  /// rust 生效值形态：主存日志单页容量字节。缺省唯一真源
  /// [`crate::node_options::DEFAULT_HLOG_PAGE_SIZE`]；启动装配口把
  /// `StoreConfig::page_size`（`--hlog-page-size` 显式项或内存预算规划器推导值）
  /// 单点投影覆盖本字段，消费面仅 wnode `AofSettings::from_options` 校验三
  /// （C# GetAofSettings 读同对象 PageSizeBits() 同源）。纯装配期校验投影：
  /// 不播种 CONFIG 槽位、不注册只读格式器。
  pub hlog_page_size: usize,
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
  /// libs/server/Servers/GarnetServerOptions.cs:AofMemorySize（默认 "128m"；参数源
  /// `--aof-memory`，NodeArgs::aof_memory_size 投影，启动期经 wnode
  /// AofSettings::from_options 组合体检后播种物理窗口）。
  pub aof_memory_size: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofPageSize（默认 "32m"；参数源
  /// `--aof-page-size`，NodeArgs::aof_page_size 投影，启动期经 wnode
  /// AofSettings::from_options 组合体检后播种物理页容量）。
  pub aof_page_size: Option<String>,
  /// libs/server/Servers/GarnetServerOptions.cs:AofSegmentSize（默认 "1g"；参数源
  /// `--aof-segment-size`，NodeArgs::aof_segment_size 投影，启动期经 wnode
  /// AofSettings::from_options 组合体检后播种设备段容量）。
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
  /// 恒默认自陈：CLI 面缺席——NodeArgs 无 `--aof-replay-drift-*` 旋钮（对照
  /// C# Options.cs:230-237 注册），`runtime_server_options()` 投影无赋值，本
  /// 字段仅 Default 播种（-1 = 漂移屏障禁用），消费侧 garnet_append_only_file.rs
  /// → ReadConsistencyManager::new 只读，行为恒同 C# 默认部署形态；登记见
  /// doc/zh/deviations.md §158 b，勿判假旋钮、勿补 CLI 旋钮使其可动。
  pub replay_drift_threshold: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayDriftCheckFreq（默认 1, <= 0 禁用）。
  /// 恒默认、CLI 面缺席同上一条（§158 b）。
  pub replay_drift_check_freq: i64,
  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaSyncTimeout（默认 5s）：副本一致读等待
  /// 回放推进与回放对齐栅栏的阻塞上界，超时上抛中止（ReadSessionState 读
  /// 超时来源；纯启动选项，C# RuntimeServerConfig 亦不暴露 CONFIG 面）。
  /// CLI/CONFIG 面以秒表达，`<=0` 为无限超时哨兵（对标 Options.cs:995
  /// `<=0 ? InfiniteTimeSpan`），投影时折为 `u64::MAX` 秒经消费侧 `from_secs`
  /// 折算为永不触发的读超时。
  pub replica_sync_timeout_secs: u64,
  /// libs/server/Servers/GarnetServerOptions.cs:FastAofTruncate（默认 false）。
  pub fast_aof_truncate: bool,
  /// libs/server/Servers/GarnetServerOptions.cs:OnDemandCheckpoint（默认 true）。按需检查点开关，
  /// 装配期注进 ClusterProvider 后由其唯一消费面读取（C# 同为
  /// serverOptions.OnDemandCheckpoint 直读：
  /// ReplicaSyncSession.cs:190、:280），C# RuntimeServerConfig 未设该 CONFIG 名额，
  /// 故此字段不播种槽位、不注册只读格式器。
  pub on_demand_checkpoint: bool,
  /// 向量集合量化任务数（0 = 对齐 CPU 核数）
  pub vector_set_quantization_task_count: i32,
}

impl Default for RuntimeServerOptions {
  /// 逐字段对齐 GarnetServerOptions.cs 的字段初始化器。
  ///
  /// 取值一律引常量：与 `NodeArgs` 同名的旋钮直接引 node_options 的
  /// `DEFAULT_*` 族（生产装配口 `NodeArgs::runtime_server_options` 的
  /// clap/serde 缺省即同一常量），本 crate 独有的播种字段引本模块新增常量，
  /// 杜绝同值双真源（改默认值只动一处）。
  fn default() -> Self {
    Self {
      cluster_timeout: DEFAULT_CLUSTER_TIMEOUT,
      replica_sync_delay_ms: DEFAULT_REPLICA_SYNC_DELAY_MS,
      aof_replay_max_lag_bytes: DEFAULT_AOF_REPLAY_MAX_LAG_BYTES,
      aof_tail_witness_freq_ms: DEFAULT_AOF_TAIL_WITNESS_FREQ_MS,
      aof_sync_max_lag_bytes: DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
      replica_diskless_sync_delay: DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS,
      replica_attach_timeout_secs: DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS,
      cluster_replication_reestablishment_timeout:
        DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
      compaction_max_segments: DEFAULT_COMPACTION_MAX_SEGMENTS,
      compaction_type: LogCompactionType::None,
      slow_log_threshold: DEFAULT_SLOW_LOG_THRESHOLD,
      slow_log_max_entries: DEFAULT_SLOW_LOG_MAX_ENTRIES,
      object_scan_count_limit: DEFAULT_OBJECT_SCAN_COUNT_LIMIT,
      enable_scatter_gather_get: DEFAULT_ENABLE_SCATTER_GATHER_GET,
      // u64 常量落 i32 槽：缺省 5 远在 i32 界内，收窄无损（显式值的饱和收窄
      // 口径在投影口 NodeArgs::runtime_server_options，本处只播缺省）
      aof_size_limit_enforce_frequency_secs: DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS as i32,
      commit_frequency_ms: DEFAULT_COMMIT_FREQUENCY_MS,
      expired_object_collection_frequency_secs: DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS,
      expired_key_deletion_scan_frequency_secs: DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
      enable_aof: false,
      hlog_page_size: DEFAULT_HLOG_PAGE_SIZE,
      max_databases: DEFAULT_MAX_DATABASES,
      checkpoint_base_directory: String::new(),
      log_dir: None,
      unix_socket_path: None,
      enable_cluster: false,
      aof_memory_size: Some("128m".into()),
      aof_page_size: Some("32m".into()),
      aof_segment_size: Some("1g".into()),
      aof_physical_sublog_count: DEFAULT_AOF_PHYSICAL_SUBLOG_COUNT,
      aof_replay_task_count: DEFAULT_AOF_REPLAY_TASK_COUNT,
      replay_drift_threshold: DEFAULT_AOF_REPLAY_DRIFT_THRESHOLD,
      replay_drift_check_freq: DEFAULT_AOF_REPLAY_DRIFT_CHECK_FREQ,
      // i32 常量放大至 u64 哨兵槽（值 5 非负，无损；<=0 折 u64::MAX 的口径在投影侧）
      replica_sync_timeout_secs: DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64,
      wait_for_commit: false,
      aof_size_limit: Some(String::new()),
      fast_aof_truncate: false,
      on_demand_checkpoint: DEFAULT_ON_DEMAND_CHECKPOINT,
      vector_set_quantization_task_count: DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
    }
  }
}
