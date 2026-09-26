//! 默认值单源锚测试（zcode-r20-wconf 发现二）
//!
//! 锁死 `RuntimeServerOptions::default()` 的播种值与 `node_options` 的
//! `DEFAULT_*` 常量族逐项相等：改默认值只需动常量一处，本测试保证另一侧
//! （生产装配路径 NodeArgs 常量臂 vs 回落构造路径 `RuntimeServerOptions::default()`）
//! 不再退化为同值双真源。
//!
//! 对标 C# 侧 defaults.conf 单份默认值基线 + Options 字段初始化器，
//! 由 ServerSettingsManager.cs:GetArgumentNameToValue 反射单点读取。
//!
//! 自研依据: 默认值单一来源（本仓配置契约）

use wconf::{
  node_options::{
    DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS, DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
    DEFAULT_AOF_TAIL_WITNESS_FREQ_MS, DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
    DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
    DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS, DEFAULT_HLOG_PAGE_SIZE,
    DEFAULT_MAX_DATABASES, DEFAULT_OBJECT_SCAN_COUNT_LIMIT, DEFAULT_ON_DEMAND_CHECKPOINT,
    DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS, DEFAULT_REPLICA_SYNC_DELAY_MS,
    DEFAULT_REPLICA_SYNC_TIMEOUT_SECS, DEFAULT_SLOW_LOG_MAX_ENTRIES, DEFAULT_SLOW_LOG_THRESHOLD,
    DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT, NodeArgs, NodeOptionsError,
    SLOW_LOG_THRESHOLD_MIN_MICROS,
  },
  runtime_server_options::{
    DEFAULT_AOF_PHYSICAL_SUBLOG_COUNT, DEFAULT_AOF_REPLAY_DRIFT_CHECK_FREQ,
    DEFAULT_AOF_REPLAY_DRIFT_THRESHOLD, DEFAULT_AOF_REPLAY_MAX_LAG_BYTES,
    DEFAULT_AOF_REPLAY_TASK_COUNT, DEFAULT_CLUSTER_TIMEOUT, DEFAULT_COMMIT_FREQUENCY_MS,
    DEFAULT_COMPACTION_MAX_SEGMENTS, DEFAULT_ENABLE_SCATTER_GATHER_GET,
    DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS, RuntimeServerOptions,
  },
};

#[test]
fn default_impl_matches_constant_family() {
  let o = RuntimeServerOptions::default();
  // 与 NodeArgs 共享的常量臂
  assert_eq!(o.replica_sync_delay_ms, DEFAULT_REPLICA_SYNC_DELAY_MS);
  assert_eq!(o.aof_tail_witness_freq_ms, DEFAULT_AOF_TAIL_WITNESS_FREQ_MS);
  assert_eq!(o.aof_sync_max_lag_bytes, DEFAULT_AOF_SYNC_MAX_LAG_BYTES);
  assert_eq!(
    o.replica_attach_timeout_secs,
    DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS
  );
  assert_eq!(
    o.cluster_replication_reestablishment_timeout,
    DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT
  );
  assert_eq!(o.slow_log_threshold, DEFAULT_SLOW_LOG_THRESHOLD);
  assert_eq!(o.slow_log_max_entries, DEFAULT_SLOW_LOG_MAX_ENTRIES);
  assert_eq!(o.object_scan_count_limit, DEFAULT_OBJECT_SCAN_COUNT_LIMIT);
  assert_eq!(o.max_databases, DEFAULT_MAX_DATABASES);
  assert_eq!(o.hlog_page_size, DEFAULT_HLOG_PAGE_SIZE);
  assert_eq!(o.on_demand_checkpoint, DEFAULT_ON_DEMAND_CHECKPOINT);
  assert_eq!(
    o.vector_set_quantization_task_count,
    DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT
  );
  assert_eq!(
    o.expired_object_collection_frequency_secs,
    DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS
  );
  assert_eq!(
    o.expired_key_deletion_scan_frequency_secs,
    DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS
  );
  assert_eq!(
    o.aof_size_limit_enforce_frequency_secs,
    DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS as i32
  );
  assert_eq!(
    o.replica_sync_timeout_secs,
    DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64
  );
  // 本 crate 独有播种字段（就地新增常量）
  assert_eq!(o.cluster_timeout, DEFAULT_CLUSTER_TIMEOUT);
  assert_eq!(o.aof_replay_max_lag_bytes, DEFAULT_AOF_REPLAY_MAX_LAG_BYTES);
  assert_eq!(
    o.replica_diskless_sync_delay,
    DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS
  );
  assert_eq!(o.compaction_max_segments, DEFAULT_COMPACTION_MAX_SEGMENTS);
  assert_eq!(o.commit_frequency_ms, DEFAULT_COMMIT_FREQUENCY_MS);
  assert_eq!(
    o.enable_scatter_gather_get,
    DEFAULT_ENABLE_SCATTER_GATHER_GET
  );
  assert_eq!(
    o.aof_physical_sublog_count,
    DEFAULT_AOF_PHYSICAL_SUBLOG_COUNT
  );
  assert_eq!(o.aof_replay_task_count, DEFAULT_AOF_REPLAY_TASK_COUNT);
  assert_eq!(o.replay_drift_threshold, DEFAULT_AOF_REPLAY_DRIFT_THRESHOLD);
  assert_eq!(
    o.replay_drift_check_freq,
    DEFAULT_AOF_REPLAY_DRIFT_CHECK_FREQ
  );
}

/// 回落构造路径（`NodeArgs::default()` 经 `runtime_server_options()` 投影）与
/// 常量族同值：NodeArgs 未显式覆盖的字段全部落到常量缺省，杜绝两臂分叉。
#[test]
fn node_args_projection_falls_back_to_constants() {
  let o = NodeArgs::default().runtime_server_options();
  assert_eq!(o.replica_sync_delay_ms, DEFAULT_REPLICA_SYNC_DELAY_MS);
  assert_eq!(o.aof_tail_witness_freq_ms, DEFAULT_AOF_TAIL_WITNESS_FREQ_MS);
  assert_eq!(o.aof_sync_max_lag_bytes, DEFAULT_AOF_SYNC_MAX_LAG_BYTES);
  assert_eq!(
    o.replica_attach_timeout_secs,
    DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS
  );
  assert_eq!(
    o.cluster_replication_reestablishment_timeout,
    DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT
  );
  assert_eq!(o.slow_log_threshold, DEFAULT_SLOW_LOG_THRESHOLD);
  assert_eq!(o.slow_log_max_entries, DEFAULT_SLOW_LOG_MAX_ENTRIES);
  assert_eq!(o.object_scan_count_limit, DEFAULT_OBJECT_SCAN_COUNT_LIMIT);
  assert_eq!(o.max_databases, DEFAULT_MAX_DATABASES);
  assert_eq!(
    o.aof_size_limit_enforce_frequency_secs,
    DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS as i32
  );
  assert_eq!(
    o.replica_sync_timeout_secs,
    DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64
  );
}

/// 慢日志阈值下限单源锚（发现一）：判定与报错区间同引
/// [`SLOW_LOG_THRESHOLD_MIN_MICROS`]，0（禁用档）合法、非零低于下限拒启。
///
/// 对标 libs/host/Configuration/Options.cs:860-863
/// `SlowLogThreshold > 0 && SlowLogThreshold < 100` 抛
/// 「SlowLogThreshold must be at least 100 microseconds.」。
#[test]
fn slow_log_threshold_bound_is_single_sourced() {
  let mut args = NodeArgs {
    slow_log_threshold: SLOW_LOG_THRESHOLD_MIN_MICROS - 1,
    ..Default::default()
  };
  assert!(
    matches!(
      args.validate(),
      Err(NodeOptionsError::ValueOutOfRange(
        "slow-log-threshold",
        SLOW_LOG_THRESHOLD_MIN_MICROS,
        i32::MAX,
        _
      ))
    ),
    "actual: {:?}",
    args.validate()
  );
  // 0 = 禁用合法值、下限本身合法（对位 C# `> 0` 前置条件）
  for legal in [DEFAULT_SLOW_LOG_THRESHOLD, SLOW_LOG_THRESHOLD_MIN_MICROS] {
    args.slow_log_threshold = legal;
    assert!(args.validate().is_ok(), "{legal} 应合法");
  }
}
