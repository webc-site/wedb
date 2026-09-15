use std::path::{Path, PathBuf};

use itoa::Buffer;

use crate::log_compaction_type::LogCompactionType;

/// `RuntimeServerConfig` 消费的启动选项子集。
///
/// 对标 libs/server/Servers/GarnetServerOptions.cs:GarnetServerOptions 中被
/// RuntimeServerConfig 构造（Init 播种 + 只读回落格式化）读取的字段。
///
/// 字段默认值与 C# 字段初始化器逐项一致。
#[derive(Debug, Clone)]
pub struct RuntimeServerOptions {
  // —— Init 播种字段 ——
  /// GarnetServerOptions.cs:ClusterTimeout（默认 60，秒）。
  pub cluster_timeout: i32,
  /// GarnetServerOptions.cs:ReplicaSyncDelayMs（默认 5，毫秒）。
  pub replica_sync_delay_ms: i32,
  /// GarnetServerOptions.cs:AofReplayMaxLagBytes（默认 -1）。
  pub aof_replay_max_lag_bytes: i32,
  /// GarnetServerOptions.cs:AofTailWitnessFreqMs（默认 100，毫秒）。
  pub aof_tail_witness_freq_ms: i32,
  /// GarnetServerOptions.cs:AofSyncMaxLagBytes（默认 -1）。
  pub aof_sync_max_lag_bytes: i64,
  /// GarnetServerOptions.cs:ReplicaDisklessSyncDelay（默认 5，秒）。
  pub replica_diskless_sync_delay: i32,
  /// GarnetServerOptions.cs:ReplicaAttachTimeout（C# 为 TimeSpan，此处以秒表达；
  /// CLI/CONFIG 面的秒数语义：<= 0 视为无限超时）。
  pub replica_attach_timeout_secs: i64,
  /// GarnetServerOptions.cs:ClusterReplicationReestablishmentTimeout（默认 0，秒）。
  pub cluster_replication_reestablishment_timeout: i32,
  /// GarnetServerOptions.cs:CompactionMaxSegments（默认 32）。
  pub compaction_max_segments: i32,
  /// GarnetServerOptions.cs:CompactionForceDelete（默认 false）。
  pub compaction_force_delete: bool,
  /// GarnetServerOptions.cs:CompactionType（默认 None）。
  pub compaction_type: LogCompactionType,
  /// GarnetServerOptions.cs:SlowLogThreshold（默认 0，微秒）。
  pub slow_log_threshold: i32,
  /// GarnetServerOptions.cs:ObjectScanCountLimit（默认 1000）。
  pub object_scan_count_limit: i32,
  /// GarnetServerOptions.cs:EnableScatterGatherGet（默认 true）。
  pub enable_scatter_gather_get: bool,
  /// GarnetServerOptions.cs:AofSizeLimitEnforceFrequencySecs（默认 5，秒）。
  pub aof_size_limit_enforce_frequency_secs: i32,
  /// GarnetServerOptions.cs:CommitFrequencyMs（默认 0：逐操作自动提交）。
  pub commit_frequency_ms: i32,
  /// GarnetServerOptions.cs:ExpiredObjectCollectionFrequencySecs（默认 0：禁用）。
  pub expired_object_collection_frequency_secs: i32,
  /// GarnetServerOptions.cs:ExpiredKeyDeletionScanFrequencySecs（默认 -1：禁用）。
  pub expired_key_deletion_scan_frequency_secs: i32,

  // —— 只读回落格式化字段（CONFIG GET 经选项直读，无运行时槽位）——
  /// GarnetServerOptions.cs:EnableAOF（默认 false）。
  pub enable_aof: bool,
  /// GarnetServerOptions.cs:MaxDatabases（默认 16）。
  pub max_databases: i32,
  /// GarnetServerOptions.cs:CheckpointBaseDirectory（派生属性）。
  pub checkpoint_base_directory: String,
  /// GarnetServerOptions.cs:LogDir（可空）。
  pub log_dir: Option<String>,
  /// GarnetServerOptions.cs:UnixSocketPath（可空）。
  pub unix_socket_path: Option<String>,
  /// GarnetServerOptions.cs:EnableCluster（默认 false）。
  pub enable_cluster: bool,
  /// GarnetServerOptions.cs:AofMemorySize（默认 "128m"）。
  pub aof_memory_size: Option<String>,
  /// GarnetServerOptions.cs:AofPageSize（默认 "32m"）。
  pub aof_page_size: Option<String>,
  /// GarnetServerOptions.cs:AofSegmentSize（默认 "1g"）。
  pub aof_segment_size: Option<String>,
  /// GarnetServerOptions.cs:AofPhysicalSublogCount（默认 1）。
  pub aof_physical_sublog_count: i32,
  /// GarnetServerOptions.cs:AofReplayTaskCount（默认 1）。
  pub aof_replay_task_count: i32,
  /// GarnetServerOptions.cs:WaitForCommit（默认 false）。
  pub wait_for_commit: bool,
  /// GarnetServerOptions.cs:AofSizeLimit（默认 ""）。
  pub aof_size_limit: Option<String>,
  /// GarnetServerOptions.cs:FastAofTruncate（默认 false）。
  pub fast_aof_truncate: bool,
}

impl Default for RuntimeServerOptions {
  /// 逐字段对齐 GarnetServerOptions.cs 的字段初始化器。
  fn default() -> Self {
    Self {
      cluster_timeout: 60,
      replica_sync_delay_ms: 5,
      aof_replay_max_lag_bytes: -1,
      aof_tail_witness_freq_ms: 100,
      aof_sync_max_lag_bytes: -1,
      replica_diskless_sync_delay: 5,
      replica_attach_timeout_secs: 60,
      cluster_replication_reestablishment_timeout: 0,
      compaction_max_segments: 32,
      compaction_force_delete: false,
      compaction_type: LogCompactionType::None,
      slow_log_threshold: 0,
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
      wait_for_commit: false,
      aof_size_limit: Some(String::new()),
      fast_aof_truncate: false,
    }
  }
}

impl RuntimeServerOptions {
  /// 检查点基目录借用切片（零分配）。
  #[inline]
  pub fn checkpoint_base_directory_str(&self) -> &str {
    if !self.checkpoint_base_directory.is_empty() {
      &self.checkpoint_base_directory
    } else {
      self.log_dir.as_deref().unwrap_or_default()
    }
  }

  /// 存储检查点基目录（对标 CheckpointBaseDirectory/Store）。
  #[inline]
  pub fn store_checkpoint_base_directory(&self) -> PathBuf {
    Path::new(self.checkpoint_base_directory_str()).join("Store")
  }

  /// 数据库检查点目录名：`checkpoints` 或 `checkpoints_{dbId}`。
  pub fn get_checkpoint_directory_name(db_id: i32) -> String {
    if db_id == 0 {
      "checkpoints".to_string()
    } else {
      let mut buf = Buffer::new();
      let mut s = String::with_capacity(12 + 11);
      s.push_str("checkpoints_");
      s.push_str(buf.format(db_id));
      s
    }
  }

  /// 存储检查点目录。
  #[inline]
  pub fn get_store_checkpoint_directory(&self, db_id: i32) -> PathBuf {
    self
      .store_checkpoint_base_directory()
      .join(Self::get_checkpoint_directory_name(db_id))
  }

  /// 数据库 AOF 目录名：`AOF` 或 `AOF_{dbId}`。
  pub fn get_append_only_file_directory_name(db_id: i32) -> String {
    if db_id == 0 {
      "AOF".to_string()
    } else {
      let mut buf = Buffer::new();
      let mut s = String::with_capacity(4 + 11);
      s.push_str("AOF_");
      s.push_str(buf.format(db_id));
      s
    }
  }

  /// 数据库 AOF 目录。
  #[inline]
  pub fn get_append_only_file_directory(&self, db_id: i32) -> PathBuf {
    Path::new(self.checkpoint_base_directory_str())
      .join(Self::get_append_only_file_directory_name(db_id))
  }
}
