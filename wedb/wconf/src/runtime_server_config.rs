use std::{
  sync::{
    Arc, LazyLock,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use crate::{
  config_kind::ConfigKind,
  config_meta::{ConfigMeta, ConfigOwner, ConfigUpdateOwner, EnumMeta},
  config_time_unit::ConfigTimeUnit,
  error::ConfigError,
  log_compaction_type::LogCompactionType,
  runtime_server_options::RuntimeServerOptions,
  server_config_type::ServerConfigType,
};

/// 全部已声明 ServerConfigType 的槽位数
pub const TABLE_SIZE: usize = (ServerConfigType::AofNullDevice as u16 + 1) as usize;

/// 全部 CONFIG 类型的槽位表（对标 libs/server/Config/RuntimeServerConfig.cs:RuntimeServerConfig）。
///
/// 以 `ServerConfigType` 为下标的运行时可调配置中心表：启动时从
/// `RuntimeServerOptions`（GarnetServerOptions 子集）播种，运行期经 CONFIG SET
/// 更新，由 StoreWrapper 持有以便服务器与集群层实时读取。
///
/// 底层为单个 `AtomicI64` 数组（无分配、连续、O(1) 下标；load/store 即 C#
/// `Volatile.Read/Write` 的原子语义）。每个槽位是原始 8 字节单元，具体解释由
/// 每个选项的 `ConfigMeta` 给出——异构类型（int、long、bool、enum、秒数超时）
/// 无损编码进槽位。
pub struct RuntimeServerConfig {
  /// 槽位值数组，下标即 `ServerConfigType` 判别值。
  values: [AtomicI64; TABLE_SIZE],
  /// 启动选项副本：仅为只读参数的回落格式化保留（对齐 C# `serverOptions`）；
  /// 运行时可调值播种后一律经类型化访问器读取，保证 CONFIG SET 全局可见。
  options: RuntimeServerOptions,
  /// 持有方，更新动作经其触达后台任务生命周期。None 时仅落槽位、无生命周期副作用
  ///（对齐 C# 单测独立构造时的 null owner）。
  owner: Option<ConfigOwner>,
}

// —— 只读配置项格式化辅助函数（启动选项直读，避免运行时闭包分配）——

fn fmt_timeout(_: &RuntimeServerOptions) -> String {
  "0".into()
}
fn fmt_save(_: &RuntimeServerOptions) -> String {
  String::new()
}
fn fmt_append_only(o: &RuntimeServerOptions) -> String {
  if o.enable_aof { "yes" } else { "no" }.into()
}
fn fmt_databases(o: &RuntimeServerOptions) -> String {
  let mut buf = itoa::Buffer::new();
  buf.format(o.max_databases).to_string()
}
fn fmt_dir(o: &RuntimeServerOptions) -> String {
  o.checkpoint_base_directory.clone()
}
fn fmt_logdir(o: &RuntimeServerOptions) -> String {
  o.log_dir.clone().unwrap_or_default()
}
fn fmt_unix_socket(o: &RuntimeServerOptions) -> String {
  o.unix_socket_path.clone().unwrap_or_default()
}
fn fmt_cluster_enabled(o: &RuntimeServerOptions) -> String {
  if o.enable_cluster { "yes" } else { "no" }.into()
}
fn fmt_aof_memory(o: &RuntimeServerOptions) -> String {
  o.aof_memory_size.clone().unwrap_or_default()
}
fn fmt_aof_page_size(o: &RuntimeServerOptions) -> String {
  o.aof_page_size.clone().unwrap_or_default()
}
fn fmt_aof_segment_size(o: &RuntimeServerOptions) -> String {
  o.aof_segment_size.clone().unwrap_or_default()
}
fn fmt_aof_physical_sublog_count(o: &RuntimeServerOptions) -> String {
  let mut buf = itoa::Buffer::new();
  buf.format(o.aof_physical_sublog_count).to_string()
}
fn fmt_aof_replay_task_count(o: &RuntimeServerOptions) -> String {
  let mut buf = itoa::Buffer::new();
  buf.format(o.aof_replay_task_count).to_string()
}
fn fmt_aof_commit_wait(o: &RuntimeServerOptions) -> String {
  if o.wait_for_commit { "yes" } else { "no" }.into()
}
fn fmt_aof_size_limit(o: &RuntimeServerOptions) -> String {
  o.aof_size_limit.clone().unwrap_or_default()
}
fn fmt_fast_aof_truncate(o: &RuntimeServerOptions) -> String {
  if o.fast_aof_truncate { "yes" } else { "no" }.into()
}
fn fmt_aof_null_device(o: &RuntimeServerOptions) -> String {
  if o.use_aof_null_device { "yes" } else { "no" }.into()
}

/// 静态元数据表（下标 == `ServerConfigType` 判别值）。纯编译期常量，零运行时分配。
///
/// 对标 C# 静态构建方法：每个 `ConfigMeta::read_only` 条目即 C# `SetReadOnly`
/// 局部函数的调用点，每个 `ConfigMeta::runtime` 条目即 C# `Set` 局部函数的调用点。
///
/// libs/server/Config/RuntimeServerConfig.cs:BuildMeta
pub static META: [ConfigMeta; RuntimeServerConfig::TABLE_SIZE] = [
  // 0: None
  ConfigMeta::EMPTY,
  // 1: All
  ConfigMeta::EMPTY,
  // 2: Timeout
  ConfigMeta::read_only(
    "timeout",
    ConfigKind::INT32
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    ConfigTimeUnit::Seconds,
    fmt_timeout,
  ),
  // 3: Save
  ConfigMeta::read_only("save", ConfigKind::STRING, ConfigTimeUnit::None, fmt_save),
  // 4: AppendOnly
  ConfigMeta::read_only(
    "appendonly",
    ConfigKind::BOOL,
    ConfigTimeUnit::None,
    fmt_append_only,
  ),
  // 5: SlaveReadOnly (固定兼容配置项，由 CONFIG GET 处理器直接处理，不入表)
  ConfigMeta::EMPTY,
  // 6: Databases
  ConfigMeta::read_only(
    "databases",
    ConfigKind::INT32,
    ConfigTimeUnit::None,
    fmt_databases,
  ),
  // 7: ClusterNodeTimeout
  ConfigMeta::runtime(
    "cluster-node-timeout",
    ConfigKind::INT32
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Seconds,
    None,
  ),
  // 8: ReplicaSyncDelay
  ConfigMeta::runtime(
    "replica-sync-delay",
    ConfigKind::INT32
      .with(ConfigKind::MILLISECONDS)
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Milliseconds,
    None,
  ),
  // 9: AofReplayMaxLagBytes
  ConfigMeta::runtime(
    "aof-replay-max-lag-bytes",
    ConfigKind::INT32,
    -1,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 10: AofSyncMaxLagBytes
  ConfigMeta::runtime(
    "aof-sync-max-lag-bytes",
    ConfigKind::INT64,
    -1,
    i64::MAX,
    None,
    ConfigTimeUnit::None,
    Some(RuntimeServerConfig::apply_aof_sync_max_lag_update),
  ),
  // 11: AofTailWitnessFreq
  ConfigMeta::runtime(
    "aof-tail-witness-freq",
    ConfigKind::INT32
      .with(ConfigKind::MILLISECONDS)
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Milliseconds,
    None,
  ),
  // 12: ReplDisklessSyncDelay
  ConfigMeta::runtime(
    "repl-diskless-sync-delay",
    ConfigKind::INT32
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Seconds,
    None,
  ),
  // 13: ReplAttachTimeout
  ConfigMeta::runtime(
    "repl-attach-timeout",
    ConfigKind::INT32
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Seconds,
    None,
  ),
  // 14: ClusterReplicationReestablishmentTimeout
  ConfigMeta::runtime(
    "cluster-replication-reestablishment-timeout",
    ConfigKind::INT32
      .with(ConfigKind::SECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Seconds,
    None,
  ),
  // 15: CompactionMaxSegments
  ConfigMeta::runtime(
    "compaction-max-segments",
    ConfigKind::INT32,
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 16: CompactionForceDelete
  ConfigMeta::runtime(
    "compaction-force-delete",
    ConfigKind::BOOL,
    0,
    1,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 17: CompactionType
  ConfigMeta::runtime(
    "compaction-type",
    ConfigKind::ENUM,
    0,
    0,
    Some(EnumMeta::LogCompactionType),
    ConfigTimeUnit::None,
    None,
  ),
  // 18: SlowlogLogSlowerThan
  ConfigMeta::runtime(
    "slowlog-log-slower-than",
    ConfigKind::INT32
      .with(ConfigKind::MICROSECONDS)
      .with(ConfigKind::TIME_SPAN),
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::Microseconds,
    None,
  ),
  // 19: ObjectScanCountLimit
  ConfigMeta::runtime(
    "object-scan-count-limit",
    ConfigKind::INT32,
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 20: SgGet
  ConfigMeta::runtime(
    "sg-get",
    ConfigKind::BOOL,
    0,
    1,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 21: AofSizeLimitEnforceFrequency
  ConfigMeta::runtime(
    "aof-size-limit-enforce-frequency",
    ConfigKind::INT32,
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    None,
  ),
  // 22: AofCommitFreq
  ConfigMeta::runtime(
    "aof-commit-freq",
    ConfigKind::INT32,
    -1,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    Some(RuntimeServerConfig::apply_commit_frequency_update),
  ),
  // 23: ExpiredObjectCollectionFreq
  ConfigMeta::runtime(
    "expired-object-collection-freq",
    ConfigKind::INT32,
    0,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    Some(RuntimeServerConfig::apply_expired_object_collection_update),
  ),
  // 24: ExpiredKeyDeletionScanFreq
  ConfigMeta::runtime(
    "expired-key-deletion-scan-freq",
    ConfigKind::INT32,
    -1,
    i32::MAX as i64,
    None,
    ConfigTimeUnit::None,
    Some(RuntimeServerConfig::apply_expired_key_deletion_update),
  ),
  // 25: Dir
  ConfigMeta::read_only("dir", ConfigKind::STRING, ConfigTimeUnit::None, fmt_dir),
  // 26: Logdir
  ConfigMeta::read_only(
    "logdir",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_logdir,
  ),
  // 27: UnixSocket
  ConfigMeta::read_only(
    "unixsocket",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_unix_socket,
  ),
  // 28: ClusterEnabled
  ConfigMeta::read_only(
    "cluster-enabled",
    ConfigKind::BOOL,
    ConfigTimeUnit::None,
    fmt_cluster_enabled,
  ),
  // 29: AofMemory
  ConfigMeta::read_only(
    "aof-memory",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_aof_memory,
  ),
  // 30: AofPageSize
  ConfigMeta::read_only(
    "aof-page-size",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_aof_page_size,
  ),
  // 31: AofSegmentSize
  ConfigMeta::read_only(
    "aof-segment-size",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_aof_segment_size,
  ),
  // 32: AofPhysicalSublogCount
  ConfigMeta::read_only(
    "aof-physical-sublog-count",
    ConfigKind::INT32,
    ConfigTimeUnit::None,
    fmt_aof_physical_sublog_count,
  ),
  // 33: AofReplayTaskCount
  ConfigMeta::read_only(
    "aof-replay-task-count",
    ConfigKind::INT32,
    ConfigTimeUnit::None,
    fmt_aof_replay_task_count,
  ),
  // 34: AofCommitWait
  ConfigMeta::read_only(
    "aof-commit-wait",
    ConfigKind::BOOL,
    ConfigTimeUnit::None,
    fmt_aof_commit_wait,
  ),
  // 35: AofSizeLimit
  ConfigMeta::read_only(
    "aof-size-limit",
    ConfigKind::STRING,
    ConfigTimeUnit::None,
    fmt_aof_size_limit,
  ),
  // 36: FastAofTruncate
  ConfigMeta::read_only(
    "fast-aof-truncate",
    ConfigKind::BOOL,
    ConfigTimeUnit::None,
    fmt_fast_aof_truncate,
  ),
  // 37: AofNullDevice
  ConfigMeta::read_only(
    "aof-null-device",
    ConfigKind::BOOL,
    ConfigTimeUnit::None,
    fmt_aof_null_device,
  ),
];

/// 参数名（含别名）→ 类型的静态查找表。纯编译期常量，零运行时分配。
///
/// libs/server/Config/RuntimeServerConfig.cs:BuildNameLookup
pub static NAME_LOOKUP: [(&[u8], ServerConfigType); 36] = [
  (b"timeout", ServerConfigType::Timeout),
  (b"save", ServerConfigType::Save),
  (b"appendonly", ServerConfigType::AppendOnly),
  (b"databases", ServerConfigType::Databases),
  (
    b"cluster-node-timeout",
    ServerConfigType::ClusterNodeTimeout,
  ),
  (b"cluster-timeout", ServerConfigType::ClusterNodeTimeout), // Redis / CLI 兼容别名
  (b"replica-sync-delay", ServerConfigType::ReplicaSyncDelay),
  (
    b"aof-replay-max-lag-bytes",
    ServerConfigType::AofReplayMaxLagBytes,
  ),
  (
    b"aof-sync-max-lag-bytes",
    ServerConfigType::AofSyncMaxLagBytes,
  ),
  (
    b"aof-tail-witness-freq",
    ServerConfigType::AofTailWitnessFreq,
  ),
  (
    b"repl-diskless-sync-delay",
    ServerConfigType::ReplDisklessSyncDelay,
  ),
  (b"repl-attach-timeout", ServerConfigType::ReplAttachTimeout),
  (
    b"cluster-replication-reestablishment-timeout",
    ServerConfigType::ClusterReplicationReestablishmentTimeout,
  ),
  (
    b"compaction-max-segments",
    ServerConfigType::CompactionMaxSegments,
  ),
  (
    b"compaction-force-delete",
    ServerConfigType::CompactionForceDelete,
  ),
  (b"compaction-type", ServerConfigType::CompactionType),
  (
    b"slowlog-log-slower-than",
    ServerConfigType::SlowlogLogSlowerThan,
  ),
  (
    b"object-scan-count-limit",
    ServerConfigType::ObjectScanCountLimit,
  ),
  (b"sg-get", ServerConfigType::SgGet),
  (
    b"aof-size-limit-enforce-frequency",
    ServerConfigType::AofSizeLimitEnforceFrequency,
  ),
  (b"aof-commit-freq", ServerConfigType::AofCommitFreq),
  (
    b"expired-object-collection-freq",
    ServerConfigType::ExpiredObjectCollectionFreq,
  ),
  (
    b"expired-key-deletion-scan-freq",
    ServerConfigType::ExpiredKeyDeletionScanFreq,
  ),
  (b"dir", ServerConfigType::Dir),
  (b"logdir", ServerConfigType::Logdir),
  (b"unixsocket", ServerConfigType::UnixSocket),
  (b"cluster-enabled", ServerConfigType::ClusterEnabled),
  (b"aof-memory", ServerConfigType::AofMemory),
  (b"aof-page-size", ServerConfigType::AofPageSize),
  (b"aof-segment-size", ServerConfigType::AofSegmentSize),
  (
    b"aof-physical-sublog-count",
    ServerConfigType::AofPhysicalSublogCount,
  ),
  (
    b"aof-replay-task-count",
    ServerConfigType::AofReplayTaskCount,
  ),
  (b"aof-commit-wait", ServerConfigType::AofCommitWait),
  (b"aof-size-limit", ServerConfigType::AofSizeLimit),
  (b"fast-aof-truncate", ServerConfigType::FastAofTruncate),
  (b"aof-null-device", ServerConfigType::AofNullDevice),
];

/// 本表处理的全部类型（可设置 + 只读），供 CONFIG GET *。纯编译期常量，零运行时分配。
///
/// libs/server/Config/RuntimeServerConfig.cs:BuildRuntimeTypes
pub static RUNTIME_TYPES: [ServerConfigType; 35] = [
  ServerConfigType::Timeout,
  ServerConfigType::Save,
  ServerConfigType::AppendOnly,
  ServerConfigType::Databases,
  ServerConfigType::ClusterNodeTimeout,
  ServerConfigType::ReplicaSyncDelay,
  ServerConfigType::AofReplayMaxLagBytes,
  ServerConfigType::AofSyncMaxLagBytes,
  ServerConfigType::AofTailWitnessFreq,
  ServerConfigType::ReplDisklessSyncDelay,
  ServerConfigType::ReplAttachTimeout,
  ServerConfigType::ClusterReplicationReestablishmentTimeout,
  ServerConfigType::CompactionMaxSegments,
  ServerConfigType::CompactionForceDelete,
  ServerConfigType::CompactionType,
  ServerConfigType::SlowlogLogSlowerThan,
  ServerConfigType::ObjectScanCountLimit,
  ServerConfigType::SgGet,
  ServerConfigType::AofSizeLimitEnforceFrequency,
  ServerConfigType::AofCommitFreq,
  ServerConfigType::ExpiredObjectCollectionFreq,
  ServerConfigType::ExpiredKeyDeletionScanFreq,
  ServerConfigType::Dir,
  ServerConfigType::Logdir,
  ServerConfigType::UnixSocket,
  ServerConfigType::ClusterEnabled,
  ServerConfigType::AofMemory,
  ServerConfigType::AofPageSize,
  ServerConfigType::AofSegmentSize,
  ServerConfigType::AofPhysicalSublogCount,
  ServerConfigType::AofReplayTaskCount,
  ServerConfigType::AofCommitWait,
  ServerConfigType::AofSizeLimit,
  ServerConfigType::FastAofTruncate,
  ServerConfigType::AofNullDevice,
];

/// 进程级共享默认配置（无服务器注入时会话的回落源）。C# 侧恒由
/// storeWrapper 持有实例，不存在该形态；rust 会话可脱离服务器装配
/// 独立构造（单测/脚本域），故给默认回落。
static SHARED_DEFAULT: LazyLock<Arc<RuntimeServerConfig>> =
  LazyLock::new(|| Arc::new(RuntimeServerConfig::with_defaults()));

impl RuntimeServerConfig {
  /// 索引全部已声明 `ServerConfigType` 所需的槽位数
  ///（libs/server/Config/RuntimeServerConfig.cs:ComputeTableSize）。
  ///
  /// 取最大判别值 + 1，无需哨兵成员，枚举空洞亦可安全下标。
  #[inline]
  pub const fn compute_table_size() -> usize {
    (ServerConfigType::AofNullDevice as u16 + 1) as usize
  }

  pub const TABLE_SIZE: usize = Self::compute_table_size();

  /// libs/server/Config/RuntimeServerConfig.cs:RuntimeServerConfig（构造）。
  ///
  /// 创建以启动选项播种的运行时配置。`owner` 为持有方，更新动作经其执行
  /// 任务生命周期变更；独立构造（无持有服务器）传 None。
  pub fn new(options: RuntimeServerOptions, owner: Option<ConfigOwner>) -> Self {
    let config = Self {
      values: [const { AtomicI64::new(0) }; Self::TABLE_SIZE],
      options,
      owner,
    };
    config.init(&config.options);
    config
  }

  /// 以默认启动选项构造（对齐 `new(GarnetServerOptions)` 的字段默认值）。
  pub fn with_defaults() -> Self {
    Self::new(RuntimeServerOptions::default(), None)
  }

  /// 进程级共享默认配置实例（会话无服务器注入时的回落源）
  pub fn shared_default() -> Arc<Self> {
    Arc::clone(&SHARED_DEFAULT)
  }

  /// 本表处理的全部配置类型
  ///（libs/server/Config/RuntimeServerConfig.cs:RuntimeTypes）。
  #[inline]
  pub fn runtime_types() -> &'static [ServerConfigType] {
    &RUNTIME_TYPES
  }

  /// 播种全部运行时槽位（libs/server/Config/RuntimeServerConfig.cs:Init）。
  fn init(&self, o: &RuntimeServerOptions) {
    let seed = |t: ServerConfigType, v: i64| {
      self.values[t as usize].store(v, Ordering::Release);
    };
    seed(
      ServerConfigType::ClusterNodeTimeout,
      i64::from(o.cluster_timeout),
    );
    seed(
      ServerConfigType::ReplicaSyncDelay,
      i64::from(o.replica_sync_delay_ms),
    );
    seed(
      ServerConfigType::AofReplayMaxLagBytes,
      i64::from(o.aof_replay_max_lag_bytes),
    );
    seed(
      ServerConfigType::AofTailWitnessFreq,
      i64::from(o.aof_tail_witness_freq_ms),
    );
    seed(
      ServerConfigType::AofSyncMaxLagBytes,
      o.aof_sync_max_lag_bytes,
    );
    seed(
      ServerConfigType::ReplDisklessSyncDelay,
      i64::from(o.replica_diskless_sync_delay),
    );
    seed(
      ServerConfigType::ReplAttachTimeout,
      Self::seconds_from_time_span(o.replica_attach_timeout_secs),
    );
    seed(
      ServerConfigType::ClusterReplicationReestablishmentTimeout,
      i64::from(o.cluster_replication_reestablishment_timeout),
    );
    seed(
      ServerConfigType::CompactionMaxSegments,
      i64::from(o.compaction_max_segments),
    );
    seed(
      ServerConfigType::CompactionForceDelete,
      i64::from(o.compaction_force_delete),
    );
    seed(
      ServerConfigType::CompactionType,
      i64::from(o.compaction_type as u8),
    );
    seed(
      ServerConfigType::SlowlogLogSlowerThan,
      i64::from(o.slow_log_threshold),
    );
    seed(
      ServerConfigType::ObjectScanCountLimit,
      i64::from(o.object_scan_count_limit),
    );
    seed(
      ServerConfigType::SgGet,
      i64::from(o.enable_scatter_gather_get),
    );
    seed(
      ServerConfigType::AofSizeLimitEnforceFrequency,
      i64::from(o.aof_size_limit_enforce_frequency_secs),
    );
    seed(
      ServerConfigType::AofCommitFreq,
      i64::from(o.commit_frequency_ms),
    );
    seed(
      ServerConfigType::ExpiredObjectCollectionFreq,
      i64::from(o.expired_object_collection_frequency_secs),
    );
    seed(
      ServerConfigType::ExpiredKeyDeletionScanFreq,
      i64::from(o.expired_key_deletion_scan_frequency_secs),
    );
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetInt
  ///
  /// 以原生单位的 32 位整数读取当前值。
  #[inline]
  pub fn get_int(&self, type_: ServerConfigType) -> i32 {
    Self::assert_kind(type_, ConfigKind::INT32);
    self.values[type_ as usize].load(Ordering::Acquire) as i32
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetLong
  ///
  /// 以原生单位的 64 位整数读取当前值。
  #[inline]
  pub fn get_long(&self, type_: ServerConfigType) -> i64 {
    Self::assert_kind(type_, ConfigKind::INT64);
    self.values[type_ as usize].load(Ordering::Acquire)
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetBool
  ///
  /// 以布尔读取当前值。
  #[inline]
  pub fn get_bool(&self, type_: ServerConfigType) -> bool {
    Self::assert_kind(type_, ConfigKind::BOOL);
    self.values[type_ as usize].load(Ordering::Acquire) != 0
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetMicroseconds
  ///
  /// 以微秒读取当前值。
  #[inline]
  pub fn get_microseconds(&self, type_: ServerConfigType) -> i64 {
    self.convert_duration(
      type_,
      ConfigKind::MICROSECONDS,
      ConfigTimeUnit::Microseconds,
    )
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetMilliseconds
  ///
  /// 以毫秒读取当前值。
  #[inline]
  pub fn get_milliseconds(&self, type_: ServerConfigType) -> i64 {
    self.convert_duration(
      type_,
      ConfigKind::MILLISECONDS,
      ConfigTimeUnit::Milliseconds,
    )
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetSeconds
  ///
  /// 以秒读取当前值。
  #[inline]
  pub fn get_seconds(&self, type_: ServerConfigType) -> i64 {
    self.convert_duration(type_, ConfigKind::SECONDS, ConfigTimeUnit::Seconds)
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetTimeSpan
  ///
  /// 以时长读取当前值。非正存储值按“无限超时”解释并返回 `None`
  ///（对齐 `Timeout.InfiniteTimeSpan`，遵循全仓“非正即无限”的超时约定）。
  /// 以 0 表示“无延迟”（如 replica-sync-delay）而非“无限”的选项，
  /// 必须经 `get_milliseconds` / `get_seconds` 读取，不得走本方法。
  #[inline]
  pub fn get_time_span(&self, type_: ServerConfigType) -> Option<Duration> {
    Self::assert_kind(type_, ConfigKind::TIME_SPAN);

    let meta = &META[type_ as usize];
    let raw = self.values[type_ as usize].load(Ordering::Acquire);
    if raw <= 0 {
      return None;
    }

    Some(match meta.time_unit {
      ConfigTimeUnit::Microseconds => Duration::from_micros(raw as u64),
      ConfigTimeUnit::Milliseconds => Duration::from_millis(raw as u64),
      _ => Duration::from_secs(raw as u64),
    })
  }

  /// libs/server/Config/RuntimeServerConfig.cs:GetEnum
  ///
  /// 以声明的枚举成员读取当前值。表内唯一枚举选项为 compaction-type
  ///（LogCompactionType），故以具体类型承接 C# 的泛型 `GetEnum<TEnum>`。
  ///
  /// 槽位值越界或非已声明成员时返回 `Err`。实践中不可达：全部写入经
  /// `try_set`（拒绝未声明值）；与 C# 的仅调试断言不同，此检查保留于
  /// release，损坏槽位表现为错误而非越界枚举流入服务器。
  #[inline]
  pub fn get_enum(&self, type_: ServerConfigType) -> Result<LogCompactionType, ConfigError> {
    Self::assert_kind(type_, ConfigKind::ENUM);

    // 槽位保存的是加宽到 64 位的底层值。
    let raw = self.values[type_ as usize].load(Ordering::Acquire);

    // 校验边界与成员声明，并安全收窄。
    LogCompactionType::from_raw(raw).ok_or(ConfigError::EnumOutOfRange { raw })
  }

  /// libs/server/Config/RuntimeServerConfig.cs:TrySet
  ///
  /// 校验 `value`，合法则更新 `type_` 的槽位；拒绝时返回 `Err`（"ERR " 前缀
  /// 的拒绝原因），槽位保持不变。
  pub fn try_set(&self, type_: ServerConfigType, value: &str) -> Result<(), ConfigError> {
    let meta = &META[type_ as usize];
    if meta.read_only {
      return Err(ConfigError::ReadOnly {
        name: meta.name.into(),
      });
    }

    let parsed: i64 = match meta.kind & ConfigKind::STORAGE_MASK {
      ConfigKind::INT32 => {
        let Ok(i32_value) = value.parse::<i32>() else {
          return Err(ConfigError::InvalidInteger {
            name: meta.name.into(),
          });
        };
        let v = i64::from(i32_value);
        if v < meta.min || v > meta.max {
          return Err(ConfigError::OutOfRange {
            name: meta.name.into(),
            min: meta.min,
            max: meta.max,
          });
        }
        v
      }
      ConfigKind::INT64 => {
        let Ok(v) = value.parse::<i64>() else {
          return Err(ConfigError::InvalidInteger {
            name: meta.name.into(),
          });
        };
        if v < meta.min || v > meta.max {
          return Err(ConfigError::OutOfRange {
            name: meta.name.into(),
            min: meta.min,
            max: meta.max,
          });
        }
        v
      }
      ConfigKind::BOOL => {
        if value.eq_ignore_ascii_case("yes") || value.eq_ignore_ascii_case("true") || value == "1" {
          1
        } else if value.eq_ignore_ascii_case("no")
          || value.eq_ignore_ascii_case("false")
          || value == "0"
        {
          0
        } else {
          return Err(ConfigError::InvalidBool {
            name: meta.name.into(),
          });
        }
      }
      ConfigKind::ENUM => {
        let Some(v) = meta.enum_type.and_then(|e| e.try_parse_to_long(value)) else {
          return Err(ConfigError::InvalidEnum {
            name: meta.name.into(),
            value: value.into(),
          });
        };
        v
      }
      _ => {
        return Err(ConfigError::NotRuntimeAdjustable {
          name: meta.name.into(),
        });
      }
    };

    // 先发布新值，使更新动作重启的任务能观察到它，再执行动作；
    // 动作拒绝则回滚槽位，保持选项不变。
    let old_value = self.values[type_ as usize].load(Ordering::Acquire);
    self.values[type_ as usize].store(parsed, Ordering::Release);

    if let Some(Err(error)) = meta
      .update_action
      .map(|action| action(self, old_value, parsed))
    {
      self.values[type_ as usize].store(old_value, Ordering::Release);
      return Err(error);
    }

    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:ApplyCommitFrequencyUpdate
  ///
  /// 在周期 AOF 提交任务上落实 aof-commit-freq 变更。取值 0（逐操作自动提交）
  /// 在构造时固化进 AOF 日志，无法在活跃日志上切换，故改 0——或启动即为 0 时
  /// 的任何变更——均被拒绝。安全的 {-1, >0} 转换经 owner 重启（或停止）提交
  /// 任务以采纳新间隔。
  /// 满足 C# ApplyCommitFrequencyUpdate(long oldValue, long newValue) 回调签名，保留 _old_value
  fn apply_commit_frequency_update(
    &self,
    _old_value: i64,
    new_value: i64,
  ) -> Result<(), ConfigError> {
    if new_value == 0 {
      return Err(ConfigError::CommitFreqZero);
    }
    if self.options.commit_frequency_ms == 0 {
      return Err(ConfigError::CommitFreqAutoCommitStart);
    }
    if let Some(owner) = &self.owner {
      owner.reconcile_commit_task();
    }
    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:ApplyAofSyncMaxLagUpdate
  ///
  /// 将新的整日志预算推入每个数据库常驻构造的主侧 AofBackpressure。闸门读取
  /// 裸字段，故重调预算——或从禁用状态启用——无需重启即生效，不涉生命周期任务。
  /// 满足 C# ApplyAofSyncMaxLagUpdate(long oldValue, long newValue) 回调签名，保留 _old_value
  fn apply_aof_sync_max_lag_update(
    &self,
    _old_value: i64,
    new_value: i64,
  ) -> Result<(), ConfigError> {
    if let Some(owner) = &self.owner {
      owner.apply_aof_sync_max_lag_bytes(new_value);
    }
    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:ApplyExpiredObjectCollectionUpdate
  ///
  /// 重启 / 停止收集任务以采纳新间隔（禁用时停止），
  /// 落实 expired-object-collection-freq 变更。
  /// 满足 C# ApplyExpiredObjectCollectionUpdate(long oldValue, long newValue) 回调签名，保留参数
  fn apply_expired_object_collection_update(
    &self,
    _old_value: i64,
    _new_value: i64,
  ) -> Result<(), ConfigError> {
    if let Some(owner) = &self.owner {
      owner.reconcile_object_collect_task();
    }
    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:ApplyExpiredKeyDeletionUpdate
  ///
  /// 重启 / 停止扫描任务以采纳新间隔（禁用时停止并恢复按需 EXPDELSCAN），
  /// 落实 expired-key-deletion-scan-freq 变更。
  /// 满足 C# ApplyExpiredKeyDeletionUpdate(long oldValue, long newValue) 回调签名，保留参数
  fn apply_expired_key_deletion_update(
    &self,
    _old_value: i64,
    _new_value: i64,
  ) -> Result<(), ConfigError> {
    if let Some(owner) = &self.owner {
      owner.reconcile_expired_key_deletion_task();
    }
    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:Name
  ///
  /// `type_` 的规范线上参数名。
  #[inline]
  pub fn name(type_: ServerConfigType) -> &'static str {
    META[type_ as usize].name
  }

  /// libs/server/Config/RuntimeServerConfig.cs:TryGetType
  ///
  /// 将参数名（含别名、ASCII 大小写不敏感）解析为本表处理的配置类型。
  #[inline]
  pub fn try_get_type(name: &[u8]) -> Option<ServerConfigType> {
    NAME_LOOKUP
      .iter()
      .find(|(key, _)| name.eq_ignore_ascii_case(key))
      .map(|(_, t)| *t)
  }

  /// CLI/CONFIG 面以秒表达这些超时，<= 0 视为无限超时（存 0）。
  ///
  /// C# 入参为 `TimeSpan`（含 InfiniteTimeSpan 哨兵）；Rust 侧选项以秒整数
  /// 承接（见 `RuntimeServerOptions.replica_attach_timeout_secs`），
  /// 负值即 C# 的负 TimeSpan / 无限。
  ///
  /// libs/server/Config/RuntimeServerConfig.cs:SecondsFromTimeSpan
  ///
  /// TimeSpan 秒数折规范秒数：非正值或无限超时一律归 0（对标 C# TimeSpan 行为）。
  pub fn seconds_from_time_span(ts_secs: i64) -> i64 {
    if ts_secs <= 0 { 0 } else { ts_secs }
  }

  /// libs/server/Config/RuntimeServerConfig.cs:AssertKind
  ///
  /// 仅调试：请求的读取视图必须在选项声明内。
  #[inline]
  fn assert_kind(type_: ServerConfigType, requested_kind: ConfigKind) {
    debug_assert!(
      (META[type_ as usize].kind & requested_kind) != ConfigKind::NONE,
      "配置 {type_:?} 声明为 {:?}，不能按 {requested_kind:?} 读取",
      META[type_ as usize].kind
    );
  }

  /// libs/server/Config/RuntimeServerConfig.cs:ConvertDuration
  ///
  /// 读取时长类槽位并从存储单位换算到请求单位。向粗单位换算截断；
  /// 需要全精度时用 `get_time_span`。
  #[inline]
  fn convert_duration(
    &self,
    type_: ServerConfigType,
    requested_kind: ConfigKind,
    requested_unit: ConfigTimeUnit,
  ) -> i64 {
    Self::assert_kind(type_, requested_kind);

    let meta = &META[type_ as usize];
    let raw = self.values[type_ as usize].load(Ordering::Acquire);
    if meta.time_unit == requested_unit {
      return raw;
    }

    let stored_micros = match meta.time_unit {
      ConfigTimeUnit::Microseconds => raw,
      ConfigTimeUnit::Milliseconds => raw * 1000,
      _ => raw * 1_000_000,
    };

    match requested_unit {
      ConfigTimeUnit::Microseconds => stored_micros,
      ConfigTimeUnit::Milliseconds => stored_micros / 1000,
      _ => stored_micros / 1_000_000,
    }
  }

  /// libs/server/Config/RuntimeServerConfig.cs:EnsureValidKind
  ///
  /// 元数据静态校验：恰好一个 storage 类别；duration 视图与时间单位互相绑定。
  /// 建表处以 debug_assert 调用（全部条目静态可见，正确性由编译期 +
  /// 单元测试共同保证），错误语义与 C# 的 InvalidOperationException 对齐。
  pub fn ensure_valid_kind(
    kind: ConfigKind,
    time_unit: ConfigTimeUnit,
  ) -> Result<(), &'static str> {
    let storage_kind = kind & ConfigKind::STORAGE_MASK;
    let single = storage_kind.bits() != 0 && (storage_kind.bits() & (storage_kind.bits() - 1)) == 0;
    if !single {
      return Err("必须声明恰好一个 storage 类别");
    }

    if (kind & ConfigKind::DURATION_MASK) != ConfigKind::NONE && time_unit == ConfigTimeUnit::None {
      return Err("声明了 duration 视图但未声明时间单位");
    }

    if (kind & ConfigKind::DURATION_MASK) == ConfigKind::NONE && time_unit != ConfigTimeUnit::None {
      return Err("声明了时间单位但没有 duration 视图");
    }

    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:EnsureSupportedEnum
  ///
  /// 元数据静态校验：ENUM 选项必须声明受支持的枚举类别。C# 侧校验底层
  /// 整型可无损加宽进 64 位槽位；Rust 侧枚举一律整型判别值，仅需保证
  /// 元数据存在。
  pub fn ensure_supported_enum(enum_type: Option<EnumMeta>) -> Result<(), &'static str> {
    if enum_type.is_none() {
      return Err("运行时配置选项未声明枚举类别");
    }
    Ok(())
  }

  /// libs/server/Config/RuntimeServerConfig.cs:RespFormat
  ///
  /// 以 RESP 字符串表示读取当前值。
  /// 暴露全部静态元数据表供检验。
  #[inline]
  pub fn meta() -> &'static [ConfigMeta] {
    &META
  }

  pub fn resp_format(&self, type_: ServerConfigType) -> String {
    let meta = &META[type_ as usize];
    if meta.read_only {
      // 只读回落：取值直接来自启动选项。
      return meta
        .read_only_formatter
        .map_or_else(String::new, |f| f(&self.options));
    }

    let raw = self.values[type_ as usize].load(Ordering::Acquire);
    match meta.kind & ConfigKind::STORAGE_MASK {
      ConfigKind::INT32 => {
        let mut buf = itoa::Buffer::new();
        buf.format(raw as i32).to_string()
      }
      ConfigKind::INT64 => {
        let mut buf = itoa::Buffer::new();
        buf.format(raw).to_string()
      }
      ConfigKind::BOOL => if raw != 0 { "yes" } else { "no" }.into(),
      ConfigKind::ENUM => meta.enum_type.and_then(|e| e.name_of(raw)).map_or_else(
        || {
          let mut buf = itoa::Buffer::new();
          buf.format(raw).to_string()
        },
        str::to_owned,
      ),
      _ => {
        let mut buf = itoa::Buffer::new();
        buf.format(raw).to_string()
      }
    }
  }
}
