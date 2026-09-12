use std::{
  sync::{
    LazyLock,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use super::{
  config_kind::ConfigKind,
  config_meta::{ConfigMeta, ConfigOwner, ConfigUpdateAction, ConfigUpdateOwner, EnumMeta},
  config_name_comparer::ConfigNameComparer,
  config_time_unit::ConfigTimeUnit,
  error::ConfigError,
  log_compaction_type::LogCompactionType,
  runtime_server_options::RuntimeServerOptions,
  server_config_type::ServerConfigType,
};

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
  values: [AtomicI64; Self::TABLE_SIZE],
  /// 启动选项副本：仅为只读参数的回落格式化保留（对齐 C# `serverOptions`）；
  /// 运行时可调值播种后一律经类型化访问器读取，保证 CONFIG SET 全局可见。
  options: RuntimeServerOptions,
  /// 持有方，更新动作经其触达后台任务生命周期。None 时仅落槽位、无生命周期副作用
  ///（对齐 C# 单测独立构造时的 null owner）。
  owner: Option<ConfigOwner>,
}

/// 静态元数据表（下标 == `ServerConfigType` 判别值）。非 runtime 成员保持默认
///（IsRuntime == false），由 bespoke CONFIG 代码处理而非本表。
static META: LazyLock<Box<[ConfigMeta]>> = LazyLock::new(RuntimeServerConfig::build_meta);

/// 参数名（含别名）→ 类型的静态查找表。
static NAME_LOOKUP: LazyLock<Vec<(&'static [u8], ServerConfigType)>> =
  LazyLock::new(RuntimeServerConfig::build_name_lookup);

/// 本表处理的全部类型（可设置 + 只读），供 CONFIG GET *。
static RUNTIME_TYPES: LazyLock<Vec<ServerConfigType>> =
  LazyLock::new(RuntimeServerConfig::build_runtime_types);

impl RuntimeServerConfig {
  /// 索引全部已声明 `ServerConfigType` 所需的槽位数
  ///（libs/server/Config/RuntimeServerConfig.cs:ComputeTableSize）。
  ///
  /// 取最大判别值 + 1，无需哨兵成员，枚举空洞亦可安全下标。
  #[inline]
  pub const fn compute_table_size() -> usize {
    (ServerConfigType::AofNullDevice as u16 + 1) as usize
  }

  const TABLE_SIZE: usize = Self::compute_table_size();

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

  /// libs/server/Config/RuntimeServerConfig.cs:BuildMeta
  ///
  /// 为每个 `ServerConfigType` 建立元数据（下标 == 判别值）。
  fn build_meta() -> Box<[ConfigMeta]> {
    let mut m = vec![ConfigMeta::EMPTY; Self::TABLE_SIZE];

    // 运行时可调选项登记（对齐 C# 局部函数 Set）。
    let set = |m: &mut [ConfigMeta],
               t: ServerConfigType,
               name: &'static str,
               kind: ConfigKind,
               min: i64,
               max: i64,
               enum_type: Option<EnumMeta>,
               time_unit: ConfigTimeUnit,
               update_action: Option<ConfigUpdateAction>| {
      if (kind & ConfigKind::ENUM) != ConfigKind::NONE {
        debug_assert!(
          Self::ensure_supported_enum(enum_type).is_ok(),
          "运行时配置选项声明的枚举类别不受支持"
        );
      }
      debug_assert!(Self::ensure_valid_kind(kind, time_unit).is_ok());
      m[t as usize] = ConfigMeta {
        name,
        kind,
        min,
        max,
        enum_type,
        is_runtime: true,
        read_only: false,
        time_unit,
        read_only_formatter: None,
        update_action,
      };
    };

    // 只读参数：经本表暴露于 CONFIG GET（含 GET *），但 CONFIG SET 拒绝——
    // 常量或物理参数（需重启）。取值由逐选项 formatter 直接读启动选项
    // 注意：slave-read-only 刻意不在表内：它是固定兼容配置项，
    // 由 CONFIG GET 处理器直接处理。
    // libs/server/Config/RuntimeServerConfig.cs:SetReadOnly
    let set_read_only = |m: &mut [ConfigMeta],
                         t: ServerConfigType,
                         name: &'static str,
                         kind: ConfigKind,
                         formatter: fn(&RuntimeServerOptions) -> String,
                         time_unit: ConfigTimeUnit| {
      debug_assert!(Self::ensure_valid_kind(kind, time_unit).is_ok());
      m[t as usize] = ConfigMeta {
        name,
        kind,
        min: 0,
        max: 0,
        enum_type: None,
        is_runtime: true,
        read_only: true,
        time_unit,
        read_only_formatter: Some(formatter),
        update_action: None,
      };
    };

    set_read_only(
      &mut m,
      ServerConfigType::Timeout,
      "timeout",
      ConfigKind::INT32 | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      |_| "0".into(),
      ConfigTimeUnit::Seconds,
    );
    set_read_only(
      &mut m,
      ServerConfigType::Save,
      "save",
      ConfigKind::STRING,
      |_| String::new(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AppendOnly,
      "appendonly",
      ConfigKind::BOOL,
      |o| {
        if o.enable_aof {
          "yes".into()
        } else {
          "no".into()
        }
      },
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::Databases,
      "databases",
      ConfigKind::INT32,
      |o| o.max_databases.to_string(),
      ConfigTimeUnit::None,
    );

    // 直接从启动选项解析的只读非数值参数（文件路径、套接字、物理开关）。
    // 无运行时槽位，纯为 CONFIG GET 暴露。
    set_read_only(
      &mut m,
      ServerConfigType::Dir,
      "dir",
      ConfigKind::STRING,
      |o| o.checkpoint_base_directory.clone(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::Logdir,
      "logdir",
      ConfigKind::STRING,
      |o| o.log_dir.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::UnixSocket,
      "unixsocket",
      ConfigKind::STRING,
      |o| o.unix_socket_path.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::ClusterEnabled,
      "cluster-enabled",
      ConfigKind::BOOL,
      |o| {
        if o.enable_cluster {
          "yes".into()
        } else {
          "no".into()
        }
      },
      ConfigTimeUnit::None,
    );

    // 直接从启动选项解析的只读 AOF 参数。描述物理 AOF 布局或仅启动期开关，
    // 变更需重启，故 CONFIG SET 拒绝；无运行时槽位，纯为 CONFIG GET 暴露。
    set_read_only(
      &mut m,
      ServerConfigType::AofMemory,
      "aof-memory",
      ConfigKind::STRING,
      |o| o.aof_memory_size.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofPageSize,
      "aof-page-size",
      ConfigKind::STRING,
      |o| o.aof_page_size.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofSegmentSize,
      "aof-segment-size",
      ConfigKind::STRING,
      |o| o.aof_segment_size.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofPhysicalSublogCount,
      "aof-physical-sublog-count",
      ConfigKind::INT32,
      |o| o.aof_physical_sublog_count.to_string(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofReplayTaskCount,
      "aof-replay-task-count",
      ConfigKind::INT32,
      |o| o.aof_replay_task_count.to_string(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofCommitWait,
      "aof-commit-wait",
      ConfigKind::BOOL,
      |o| {
        if o.wait_for_commit {
          "yes".into()
        } else {
          "no".into()
        }
      },
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofSizeLimit,
      "aof-size-limit",
      ConfigKind::STRING,
      |o| o.aof_size_limit.clone().unwrap_or_default(),
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::FastAofTruncate,
      "fast-aof-truncate",
      ConfigKind::BOOL,
      |o| {
        if o.fast_aof_truncate {
          "yes".into()
        } else {
          "no".into()
        }
      },
      ConfigTimeUnit::None,
    );
    set_read_only(
      &mut m,
      ServerConfigType::AofNullDevice,
      "aof-null-device",
      ConfigKind::BOOL,
      |o| {
        if o.use_aof_null_device {
          "yes".into()
        } else {
          "no".into()
        }
      },
      ConfigTimeUnit::None,
    );

    set(
      &mut m,
      ServerConfigType::ClusterNodeTimeout,
      "cluster-node-timeout",
      ConfigKind::INT32 | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Seconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::ReplicaSyncDelay,
      "replica-sync-delay",
      ConfigKind::INT32 | ConfigKind::MILLISECONDS | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Milliseconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::AofReplayMaxLagBytes,
      "aof-replay-max-lag-bytes",
      ConfigKind::INT32,
      -1,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      None,
    );
    // 主侧复制背压预算（整日志字节数）。每个数据库常驻构造的 AofBackpressure
    // 读取裸字段，故 ApplyAofSyncMaxLagUpdate 将 CONFIG SET 直接推入活跃闸门
    // ——无重启、无生命周期任务。
    set(
      &mut m,
      ServerConfigType::AofSyncMaxLagBytes,
      "aof-sync-max-lag-bytes",
      ConfigKind::INT64,
      -1,
      i64::MAX,
      None,
      ConfigTimeUnit::None,
      Some(Self::apply_aof_sync_max_lag_update),
    );
    set(
      &mut m,
      ServerConfigType::AofTailWitnessFreq,
      "aof-tail-witness-freq",
      ConfigKind::INT32 | ConfigKind::MILLISECONDS | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Milliseconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::ReplDisklessSyncDelay,
      "repl-diskless-sync-delay",
      ConfigKind::INT32 | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Seconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::ReplAttachTimeout,
      "repl-attach-timeout",
      ConfigKind::INT32 | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Seconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::ClusterReplicationReestablishmentTimeout,
      "cluster-replication-reestablishment-timeout",
      ConfigKind::INT32 | ConfigKind::SECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Seconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::CompactionMaxSegments,
      "compaction-max-segments",
      ConfigKind::INT32,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      None,
    );
    set(
      &mut m,
      ServerConfigType::CompactionForceDelete,
      "compaction-force-delete",
      ConfigKind::BOOL,
      0,
      1,
      None,
      ConfigTimeUnit::None,
      None,
    );
    set(
      &mut m,
      ServerConfigType::CompactionType,
      "compaction-type",
      ConfigKind::ENUM,
      0,
      0,
      Some(EnumMeta::LogCompactionType),
      ConfigTimeUnit::None,
      None,
    );
    set(
      &mut m,
      ServerConfigType::SlowlogLogSlowerThan,
      "slowlog-log-slower-than",
      ConfigKind::INT32 | ConfigKind::MICROSECONDS | ConfigKind::TIME_SPAN,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::Microseconds,
      None,
    );
    set(
      &mut m,
      ServerConfigType::ObjectScanCountLimit,
      "object-scan-count-limit",
      ConfigKind::INT32,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      None,
    );
    set(
      &mut m,
      ServerConfigType::SgGet,
      "sg-get",
      ConfigKind::BOOL,
      0,
      1,
      None,
      ConfigTimeUnit::None,
      None,
    );

    // AOF 大小上限执行频率（秒）：后台 checkpoint 执行任务每轮重读，
    // 故 CONFIG SET 对运行中任务即时生效。
    set(
      &mut m,
      ServerConfigType::AofSizeLimitEnforceFrequency,
      "aof-size-limit-enforce-frequency",
      ConfigKind::INT32,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      None,
    );

    // 变更需经 UpdateAction 重启 / 停止所属任务的后台任务频率：任务在启动时
    // 捕获自身间隔，运行期变更需 kill+restart 而非重读。
    //
    // aof-commit-freq（ms）：-1 = 手动提交（无周期任务），> 0 = 周期提交间隔。
    // 取值 0（逐操作自动提交）在启动时固化进 AOF 日志，无法在活跃日志上切换；
    // ApplyCommitFrequencyUpdate 拒绝改为 0，也拒绝在启动即为 0 时的任何变更。
    set(
      &mut m,
      ServerConfigType::AofCommitFreq,
      "aof-commit-freq",
      ConfigKind::INT32,
      -1,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      Some(Self::apply_commit_frequency_update),
    );
    // expired-object-collection-freq（秒）：<= 0 = 禁用（无任务），> 0 = 收集间隔。
    set(
      &mut m,
      ServerConfigType::ExpiredObjectCollectionFreq,
      "expired-object-collection-freq",
      ConfigKind::INT32,
      0,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      Some(Self::apply_expired_object_collection_update),
    );
    // expired-key-deletion-scan-freq（秒）：<= 0 = 禁用（无任务，允许按需 EXPDELSCAN），
    // > 0 = 后台扫描间隔。
    set(
      &mut m,
      ServerConfigType::ExpiredKeyDeletionScanFreq,
      "expired-key-deletion-scan-freq",
      ConfigKind::INT32,
      -1,
      i64::from(i32::MAX),
      None,
      ConfigTimeUnit::None,
      Some(Self::apply_expired_key_deletion_update),
    );

    m.into()
  }

  /// libs/server/Config/RuntimeServerConfig.cs:BuildNameLookup
  ///
  /// 参数名（含别名）→ 类型的查找表。规模仅数十项，查找走线性扫描 +
  /// ASCII 大小写不敏感比较，零哈希、零分配。
  fn build_name_lookup() -> Vec<(&'static [u8], ServerConfigType)> {
    let mut d = Vec::with_capacity(Self::TABLE_SIZE + 1);
    for (i, meta) in META.iter().enumerate() {
      if meta.is_runtime {
        d.push((meta.name.as_bytes(), ServerConfigType::ALL_MEMBERS[i]));
      }
    }

    // cluster-node-timeout 的 Redis / CLI 兼容别名。
    d.push((b"cluster-timeout", ServerConfigType::ClusterNodeTimeout));
    d
  }

  /// libs/server/Config/RuntimeServerConfig.cs:BuildRuntimeTypes
  ///
  /// 本表处理的全部类型（settable + read-only），供 CONFIG GET *。
  fn build_runtime_types() -> Vec<ServerConfigType> {
    META
      .iter()
      .enumerate()
      .filter(|(_, meta)| meta.is_runtime)
      .map(|(i, _)| ServerConfigType::ALL_MEMBERS[i])
      .collect()
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
      .find(|(key, _)| ConfigNameComparer::equals(name, key))
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
      ConfigKind::INT32 => (raw as i32).to_string(),
      ConfigKind::INT64 => raw.to_string(),
      ConfigKind::BOOL => if raw != 0 { "yes" } else { "no" }.into(),
      ConfigKind::ENUM => meta
        .enum_type
        .and_then(|e| e.name_of(raw))
        .map_or_else(|| raw.to_string(), str::to_owned),
      _ => raw.to_string(),
    }
  }
}
