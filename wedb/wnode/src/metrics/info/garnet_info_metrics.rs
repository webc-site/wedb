use std::{env, num::NonZeroUsize, thread};

use crate::metrics::{
  format_info_section, garnet_session_metrics::GarnetSessionMetrics,
  info_metrics_type::InfoMetricsType, latency::garnet_latency_metrics::fmt_n2,
  metrics_item::MetricsItem, system_metrics::SystemMetrics,
};

/// INFO 的默认段集合：排除高成本段（对齐 C# DefaultInfo）。
/// KEYSPACE 需对每库做全日志扫描计数（Garnet 无 O(1) 键计数），
/// 从 default/ALL/EVERYTHING 集合排除，仅在显式 `INFO KEYSPACE` 时填充。
pub const DEFAULT_INFO: &[InfoMetricsType] = &[
  InfoMetricsType::Server,
  InfoMetricsType::Memory,
  InfoMetricsType::Cluster,
  InfoMetricsType::Replication,
  InfoMetricsType::Stats,
  InfoMetricsType::Store,
  InfoMetricsType::Persistence,
  InfoMetricsType::Clients,
  InfoMetricsType::Modules,
  InfoMetricsType::BpStats,
  InfoMetricsType::CInfo,
];

/// 除模块生成段外的全部信息段（对齐 C# AllInfoSet：DefaultInfo 去除 Modules）。
pub const ALL_INFO_SET: &[InfoMetricsType] = &[
  InfoMetricsType::Server,
  InfoMetricsType::Memory,
  InfoMetricsType::Cluster,
  InfoMetricsType::Replication,
  InfoMetricsType::Stats,
  InfoMetricsType::Store,
  InfoMetricsType::Persistence,
  InfoMetricsType::Clients,
  InfoMetricsType::BpStats,
  InfoMetricsType::CInfo,
];

/// 单库快照：STORE / MEMORY / PERSISTENCE / STOREHASHTABLE / STOREREVIV
/// 各段填充所需的库级事实。
///
/// C# 经 `storeWrapper.GetDatabasesSnapshot()` 直查 GarnetDatabase 对象；
/// 存储域尚未落地，此处以纯数据快照承接（StoreWrapper 落地后由其适配）。
#[derive(Debug, Default, Clone)]
pub struct DbSnapshot {
  /// 库 id。
  pub id: i32,
  // —— STORE 段 ——
  /// 当前版本。
  pub current_version: i64,
  /// 最近 checkpoint 版本。
  pub last_checkpointed_version: i64,
  /// 系统状态文本。
  pub system_state: String,
  /// 索引桶数。
  pub index_bucket_count: i64,
  /// 单索引桶字节数。
  pub index_bucket_size_bytes: i64,
  /// 索引内存字节数。
  pub index_memory_size_bytes: i64,
  /// 索引溢出桶数。
  pub index_overflow_bucket_count: i64,
  /// 索引溢出内存字节数。
  pub index_overflow_memory_size_bytes: i64,
  /// 索引总内存字节数。
  pub index_total_memory_size_bytes: i64,
  /// 日志页大小。
  pub log_page_size_bytes: i64,
  /// 日志最大分配页数。
  pub log_max_allocated_page_count: i64,
  /// 日志已分配页数。
  pub log_allocated_page_count: i64,
  /// 日志内存上限。
  pub log_max_memory_size_bytes: i64,
  /// 日志当前内存。
  pub log_memory_size_bytes: i64,
  /// 日志堆大小。
  pub log_heap_size_bytes: i64,
  /// 日志 begin 地址。
  pub log_begin_address: i64,
  /// 日志 head 地址。
  pub log_head_address: i64,
  /// 日志只读安全地址。
  pub log_safe_readonly_address: i64,
  /// 日志已刷盘地址。
  pub log_flushed_until_address: i64,
  /// 日志 tail 地址。
  pub log_tail_address: i64,
  /// 读缓存快照（未启用为 None）。
  pub read_cache: Option<ReadCacheSnapshot>,
  /// 主日志目标内存（SizeTracker 存在时非 None）。
  pub mainlog_target_size: Option<i64>,
  /// 读缓存目标内存。
  pub readcache_target_size: Option<i64>,
  /// AOF 日志内存字节数。
  pub aof_memory_size_bytes: i64,
  /// AOF 持久化快照（未启用为 None）。
  pub aof: Option<AofSnapshot>,
  /// 哈希分布转储文本（STOREHASHTABLE）。
  pub hash_distribution_dump: String,
  /// 复活统计转储文本（STOREREVIV）。
  pub revivification_dump: String,
}

/// 读缓存快照。
#[derive(Debug, Default, Clone)]
pub struct ReadCacheSnapshot {
  /// 页大小。
  pub page_size_bytes: i64,
  /// 最大分配页数。
  pub max_allocated_page_count: i64,
  /// 已分配页数。
  pub allocated_page_count: i64,
  /// 内存上限。
  pub max_memory_size_bytes: i64,
  /// 当前内存。
  pub memory_size_bytes: i64,
  /// 堆大小。
  pub heap_size_bytes: i64,
  /// begin 地址。
  pub begin_address: i64,
  /// head 地址。
  pub head_address: i64,
  /// tail 地址。
  pub tail_address: i64,
}

/// AOF 持久化快照。
#[derive(Debug, Default, Clone)]
pub struct AofSnapshot {
  /// 已提交 begin 地址。
  pub committed_begin_address: i64,
  /// 已提交 until 地址。
  pub committed_until_address: i64,
  /// 已刷盘地址。
  pub flushed_until_address: i64,
  /// begin 地址。
  pub begin_address: i64,
  /// tail 地址。
  pub tail_address: i64,
}

/// 全局指标快照（STATS / CLIENTS 段；C# 直读 storeWrapper.monitor.GlobalMetrics）。
#[derive(Debug, Default, Clone)]
pub struct GlobalMetricsSnapshot {
  /// 活跃连接数。
  pub total_connections_active: i64,
  /// 收到的连接数。
  pub total_connections_received: i64,
  /// 已释放的连接数。
  pub total_connections_disposed: i64,
  /// 瞬时命令吞吐。
  pub instantaneous_cmd_per_sec: f64,
  /// 瞬时网络入吞吐（KiB/s）。
  pub instantaneous_net_input_tpt: f64,
  /// 瞬时网络出吞吐（KiB/s）。
  pub instantaneous_net_output_tpt: f64,
  /// 全局会话指标。
  pub global_session_metrics: GarnetSessionMetrics,
}

/// 服务器级事实（SERVER / MEMORY / STATS 段；C# 直读 StoreWrapper 与
/// GarnetServerOptions 字段）。
#[derive(Debug, Clone)]
pub struct ServerFacts {
  /// Garnet 版本。
  pub version: String,
  /// 运行实例 id。
  pub run_id: String,
  /// RESP 协议版本。
  pub redis_protocol_version: String,
  /// 是否启用集群。
  pub enable_cluster: bool,
  /// 是否启用 AOF。
  pub enable_aof: bool,
  /// 指标采样频率（秒；0 = 未启用）。
  pub metrics_sampling_frequency: i32,
  /// 是否启用延迟监视。
  pub latency_monitor: bool,
  /// 是否启用命令统计监视。
  pub command_stats_monitor: bool,
  /// 进程启动时刻（Unix 秒，uptime 计算）。
  pub startup_timestamp_unix_secs: i64,
  /// 日志目录（只读回落）。
  pub log_dir: String,
}

/// INFO 数据源（对标 C# 侧 StoreWrapper / monitor / clusterProvider 的
/// 直读面；StoreWrapper 域落地后由其实现本 trait）。
pub trait InfoProvider {
  /// 服务器级事实。
  fn server_facts(&self) -> ServerFacts;

  /// 全部库的快照（按 id 升序）。
  fn databases(&self) -> Vec<DbSnapshot>;

  /// 最大库 id（对齐 StoreWrapper.MaxDatabaseId；无库返回 -1）。
  fn max_database_id(&self) -> i32;

  /// 全局指标快照（监视器未启用为 None）。
  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot>;

  /// 聚合命令统计：`(cmdstat 名(小写), calls, rejected_calls)`，
  /// 已过滤 calls/rejected 均为 0 与 "unknown"（对齐 C# PopulateCommandStatsInfo
  /// 的聚合 + RespCommandsInfo.GetRespCommandName 解析路径）。
  fn command_stats(&self) -> Vec<(String, u64, u64)>;

  /// 库的 (键数, 过期键数)。
  fn keyspace_stats(&self, db_id: i32) -> (u64, u64);

  /// 集群复制信息段；None = 无集群提供方（C# clusterProvider == null）。
  fn replication_info(&self) -> Option<Vec<MetricsItem>>;

  /// gossip 统计段。
  fn gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem>;

  /// 缓冲池统计：`(server_socket_i / 集群端口名, 统计文本)`。
  fn buffer_pool_stats(&self) -> Vec<(String, String)>;

  /// 集群 checkpoint 信息段。
  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>>;

  /// 每库混合日志内存分布转储：`(主存储转储, 对象存储转储)`。
  fn hlog_scan_dump(&self) -> Vec<(String, String)>;

  /// 主侧安全 AOF 地址（对齐 StoreWrapper.safeAofAddress）。
  fn safe_aof_address(&self) -> i64;
}

/// INFO 各段指标的填充与序列化
///（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:GarnetInfoMetrics）。
pub struct GarnetInfoMetrics {
  server_info: Option<Vec<MetricsItem>>,
  memory_info: Option<Vec<MetricsItem>>,
  cluster_info: Option<Vec<MetricsItem>>,
  replication_info: Option<Vec<MetricsItem>>,
  stats_info: Option<Vec<MetricsItem>>,
  store_info: Option<Vec<Vec<MetricsItem>>>,
  store_hash_distr_info: Option<Vec<Vec<MetricsItem>>>,
  store_reviv_info: Option<Vec<Vec<MetricsItem>>>,
  persistence_info: Option<Vec<Vec<MetricsItem>>>,
  clients_info: Option<Vec<MetricsItem>>,
  keyspace_info: Option<Vec<MetricsItem>>,
  buffer_pool_stats: Option<Vec<MetricsItem>>,
  checkpoint_stats: Option<Vec<MetricsItem>>,
  hlog_scan_stats: Option<Vec<Vec<MetricsItem>>>,
  command_stats_info: Option<Vec<MetricsItem>>,
}

impl Default for GarnetInfoMetrics {
  fn default() -> Self {
    Self::new()
  }
}

impl GarnetInfoMetrics {
  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GarnetInfoMetrics（构造）。
  pub fn new() -> Self {
    Self {
      server_info: None,
      memory_info: None,
      cluster_info: None,
      replication_info: None,
      stats_info: None,
      store_info: None,
      store_hash_distr_info: None,
      store_reviv_info: None,
      persistence_info: None,
      clients_info: None,
      keyspace_info: None,
      buffer_pool_stats: None,
      checkpoint_stats: None,
      hlog_scan_stats: None,
      command_stats_info: None,
    }
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateServerInfo
  fn populate_server_info(&mut self, provider: &impl InfoProvider) {
    let facts = provider.server_facts();
    let uptime_secs =
      coarsetime::Clock::now_since_epoch().as_secs() as i64 - facts.startup_timestamp_unix_secs;
    let uptime_secs = uptime_secs.max(0);
    self.server_info = Some(vec![
      MetricsItem::new("garnet_version", facts.version),
      MetricsItem::new("server_name", "garnet"),
      MetricsItem::new("os", env::consts::OS),
      MetricsItem::new(
        "processor_count",
        thread::available_parallelism()
          .map_or(1, NonZeroUsize::get)
          .to_string(),
      ),
      MetricsItem::new(
        "arch_bits",
        if cfg!(target_pointer_width = "64") {
          "64"
        } else {
          "32"
        },
      ),
      MetricsItem::new("uptime_in_seconds", uptime_secs.to_string()),
      MetricsItem::new("uptime_in_days", (uptime_secs / 86_400).to_string()),
      MetricsItem::new(
        "monitor_task",
        if facts.metrics_sampling_frequency > 0 {
          "enabled"
        } else {
          "disabled"
        },
      ),
      MetricsItem::new("monitor_freq", facts.metrics_sampling_frequency.to_string()),
      MetricsItem::new(
        "latency_monitor",
        if facts.latency_monitor {
          "enabled"
        } else {
          "disabled"
        },
      ),
      MetricsItem::new(
        "commandstats_monitor",
        if facts.command_stats_monitor {
          "enabled"
        } else {
          "disabled"
        },
      ),
      MetricsItem::new("run_id", facts.run_id),
      MetricsItem::new("redis_version", facts.redis_protocol_version),
      MetricsItem::new(
        "redis_mode",
        if facts.enable_cluster {
          "cluster"
        } else {
          "standalone"
        },
      ),
    ]);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateMemoryInfo
  fn populate_memory_info(&mut self, provider: &impl InfoProvider) {
    let facts = provider.server_facts();
    let mut store_index_size = 0i64;
    let mut store_mainlog_memory_target_size = 0i64;
    let mut store_mainlog_memory_size = 0i64;
    let mut store_readcache_memory_size = 0i64;
    let mut aof_log_memory_size = if facts.enable_aof { 0 } else { -1 };

    for db in provider.databases() {
      store_index_size += db.index_total_memory_size_bytes;
      aof_log_memory_size += db.aof_memory_size_bytes;

      match db.mainlog_target_size {
        None => store_mainlog_memory_size += db.log_memory_size_bytes,
        Some(target) => {
          store_mainlog_memory_target_size += target;
          store_mainlog_memory_size += db.log_memory_size_bytes;
        }
      }

      match db.readcache_target_size {
        None => {
          store_readcache_memory_size += db.read_cache.as_ref().map_or(0, |rc| rc.memory_size_bytes)
        }
        // C# 同时累加进 store_readcache_memory_target_size，但该累加值
        // 不出现在任何 INFO 输出（仅写不读），故此处不再携带。
        Some(_) => {
          store_readcache_memory_size +=
            db.read_cache.as_ref().map_or(0, |rc| rc.memory_size_bytes);
        }
      }
    }

    let total_store_size =
      store_index_size + store_mainlog_memory_size + store_readcache_memory_size;

    let m = |name: &str, value: i64| MetricsItem::new(name, value.to_string());
    self.memory_info = Some(vec![
      MetricsItem::new("system_page_size", page_size().to_string()),
      m("total_system_memory", SystemMetrics::get_total_memory(1)),
      m(
        "total_system_memory(MB)",
        SystemMetrics::get_total_memory(1 << 20),
      ),
      m(
        "available_system_memory",
        SystemMetrics::get_physical_available_memory(1),
      ),
      m(
        "available_system_memory(MB)",
        SystemMetrics::get_physical_available_memory(1 << 20),
      ),
      m(
        "proc_paged_memory_size",
        SystemMetrics::get_paged_memory_size(1),
      ),
      m(
        "proc_paged_memory_size(MB)",
        SystemMetrics::get_paged_memory_size(1 << 20),
      ),
      m(
        "proc_peak_paged_memory_size",
        SystemMetrics::get_peak_paged_memory_size(1),
      ),
      m(
        "proc_peak_paged_memory_size(MB)",
        SystemMetrics::get_peak_paged_memory_size(1 << 20),
      ),
      m(
        "proc_pageable_memory_size",
        SystemMetrics::get_paged_system_memory_size(1),
      ),
      m(
        "proc_pageable_memory_size(MB)",
        SystemMetrics::get_paged_system_memory_size(1 << 20),
      ),
      m(
        "proc_private_memory_size",
        SystemMetrics::get_private_memory_size64(1),
      ),
      m(
        "proc_private_memory_size(MB)",
        SystemMetrics::get_private_memory_size64(1 << 20),
      ),
      m(
        "proc_virtual_memory_size",
        SystemMetrics::get_virtual_memory_size64(1),
      ),
      m(
        "proc_virtual_memory_size(MB)",
        SystemMetrics::get_virtual_memory_size64(1 << 20),
      ),
      m(
        "proc_peak_virtual_memory_size",
        SystemMetrics::get_peak_virtual_memory_size64(1),
      ),
      m(
        "proc_peak_virtual_memory_size(MB)",
        SystemMetrics::get_peak_virtual_memory_size64(1 << 20),
      ),
      m(
        "proc_physical_memory_size",
        SystemMetrics::get_physical_memory_usage(1),
      ),
      m(
        "proc_physical_memory_size(MB)",
        SystemMetrics::get_physical_memory_usage(1 << 20),
      ),
      m(
        "proc_peak_physical_memory_size",
        SystemMetrics::get_peak_physical_memory_usage(1),
      ),
      m(
        "proc_peak_physical_memory_size(MB)",
        SystemMetrics::get_peak_physical_memory_usage(1 << 20),
      ),
      // C# 的 GC/NativeAllocator 计数为 .NET 运行时专属，Rust 侧以 0 占位语义。
      m("gc_committed_bytes", 0),
      m("gc_heap_bytes", 0),
      m("gc_managed_memory_bytes_excluding_heap", 0),
      m("gc_fragmented_bytes", 0),
      m("native_allocator_bytes", 0),
      m("store_index_size", store_index_size),
      m("store_mainlog_memory_size", store_mainlog_memory_size),
      m("store_readcache_memory_size", store_readcache_memory_size),
      m("total_main_store_size", total_store_size),
      m(
        "store_heap_memory_target_size",
        store_mainlog_memory_target_size,
      ),
      m("aof_memory_size", aof_log_memory_size),
    ]);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateClusterInfo
  fn populate_cluster_info(&mut self, provider: &impl InfoProvider) {
    let facts = provider.server_facts();
    self.cluster_info = Some(vec![MetricsItem::new(
      "cluster_enabled",
      if facts.enable_cluster { "1" } else { "0" },
    )]);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateReplicationInfo
  fn populate_replication_info(&mut self, provider: &impl InfoProvider) {
    self.replication_info = Some(provider.replication_info().unwrap_or_else(|| {
      vec![
        MetricsItem::new("role", "master"),
        MetricsItem::new("connected_slaves", "0"),
        MetricsItem::new("master_failover_state", "no-failover"),
        MetricsItem::new("master_replid", generate_default_hex_id()),
        MetricsItem::new("master_replid2", generate_default_hex_id()),
        MetricsItem::new("master_repl_offset", "N/A"),
        MetricsItem::new("second_repl_offset", "N/A"),
        MetricsItem::new("store_current_safe_aof_address", "N/A"),
        MetricsItem::new("store_recovered_safe_aof_address", "N/A"),
      ]
    }));
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStatsInfo
  fn populate_stats_info(&mut self, provider: &impl InfoProvider) {
    let facts = provider.server_facts();
    let Some(global) = provider.global_metrics() else {
      // 监视器未启用：全部计 0（对齐 metricsDisabled 分支）。
      self.stats_info = Some(vec![
        MetricsItem::new("total_connections_active", "0"),
        MetricsItem::new("total_connections_received", "0"),
        MetricsItem::new("total_connections_disposed", "0"),
        MetricsItem::new("total_commands_processed", "0"),
        MetricsItem::new("instantaneous_ops_per_sec", "0"),
        MetricsItem::new("total_net_input_bytes", "0"),
        MetricsItem::new("total_net_output_bytes", "0"),
        MetricsItem::new("instantaneous_net_input_KBps", "0"),
        MetricsItem::new("instantaneous_net_output_KBps", "0"),
        MetricsItem::new("total_pending", "0"),
        MetricsItem::new("total_found", "0"),
        MetricsItem::new("total_notfound", "0"),
        MetricsItem::new("garnet_hit_rate", fmt_n2(0.0)),
        MetricsItem::new("total_cluster_commands_processed", "0"),
        MetricsItem::new("total_write_commands_processed", "0"),
        MetricsItem::new("total_read_commands_processed", "0"),
        MetricsItem::new("total_number_resp_server_session_exceptions", "0"),
        MetricsItem::new("total_transaction_commands_received", "0"),
        MetricsItem::new("total_transaction_commands_execution_failed", "0"),
      ]);
      return;
    };

    let session = &global.global_session_metrics;
    let tt = session.get_total_found() + session.get_total_notfound();
    let garnet_hit_rate = if tt > 0 {
      session.get_total_found() as f64 / tt as f64
    } else {
      0.0
    } * 100.0;

    let mut items = vec![
      MetricsItem::new(
        "total_connections_active",
        global.total_connections_active.to_string(),
      ),
      MetricsItem::new(
        "total_connections_received",
        global.total_connections_received.to_string(),
      ),
      MetricsItem::new(
        "total_connections_disposed",
        global.total_connections_disposed.to_string(),
      ),
      MetricsItem::new(
        "total_commands_processed",
        session.get_total_commands_processed().to_string(),
      ),
      MetricsItem::new(
        "instantaneous_ops_per_sec",
        format!("{}", global.instantaneous_cmd_per_sec),
      ),
      MetricsItem::new(
        "total_net_input_bytes",
        session.get_total_net_input_bytes().to_string(),
      ),
      MetricsItem::new(
        "total_net_output_bytes",
        session.get_total_net_output_bytes().to_string(),
      ),
      MetricsItem::new(
        "instantaneous_net_input_KBps",
        format!("{}", global.instantaneous_net_input_tpt),
      ),
      MetricsItem::new(
        "instantaneous_net_output_KBps",
        format!("{}", global.instantaneous_net_output_tpt),
      ),
      MetricsItem::new("total_pending", session.get_total_pending().to_string()),
      MetricsItem::new("total_found", session.get_total_found().to_string()),
      MetricsItem::new("total_notfound", session.get_total_notfound().to_string()),
      MetricsItem::new("garnet_hit_rate", fmt_n2(garnet_hit_rate)),
      MetricsItem::new(
        "total_cluster_commands_processed",
        session.get_total_cluster_commands_processed().to_string(),
      ),
      MetricsItem::new(
        "total_write_commands_processed",
        session.get_total_write_commands_processed().to_string(),
      ),
      MetricsItem::new(
        "total_read_commands_processed",
        session.get_total_read_commands_processed().to_string(),
      ),
      MetricsItem::new(
        "total_number_resp_server_session_exceptions",
        session
          .get_total_number_resp_server_session_exceptions()
          .to_string(),
      ),
      MetricsItem::new(
        "total_transaction_commands_received",
        session
          .get_total_transaction_commands_received()
          .to_string(),
      ),
      MetricsItem::new(
        "total_transaction_commands_execution_failed",
        session
          .get_total_transaction_commands_execution_failed()
          .to_string(),
      ),
    ];

    if facts.enable_cluster {
      items.extend(provider.gossip_stats(false));
    }
    self.stats_info = Some(items);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCommandStatsInfo
  fn populate_command_stats_info(&mut self, provider: &impl InfoProvider) {
    if !provider.server_facts().command_stats_monitor {
      self.command_stats_info = Some(vec![MetricsItem::new(
        "",
        "Command stats monitoring is disabled. Enable with --commandstats-monitor flag.",
      )]);
      return;
    }

    let stats = provider.command_stats();
    self.command_stats_info = if stats.is_empty() {
      None
    } else {
      Some(
        stats
          .into_iter()
          .map(|(name, calls, rejected)| {
            MetricsItem::new(
              format!("cmdstat_{name}"),
              format!(
                "calls={calls},usec=0,usec_per_call=0.00,rejected_calls={rejected},failed_calls=0"
              ),
            )
          })
          .collect(),
      )
    };
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreStats
  fn populate_store_stats(&mut self, provider: &impl InfoProvider) {
    let mut store_info = vec![Vec::new(); (provider.max_database_id() + 1).max(0) as usize];
    for db in provider.databases() {
      let stats = Self::get_database_store_stats(provider, &db);
      store_info[db.id as usize] = stats;
    }
    self.store_info = Some(store_info);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetDatabaseStoreStats
  fn get_database_store_stats(provider: &impl InfoProvider, db: &DbSnapshot) -> Vec<MetricsItem> {
    let n = |v: i64| v.to_string();
    let mut items = vec![
      MetricsItem::new("CurrentVersion", n(db.current_version)),
      MetricsItem::new("LastCheckpointedVersion", n(db.last_checkpointed_version)),
      MetricsItem::new("SystemState", db.system_state.clone()),
      MetricsItem::new("IndexBucketCount", n(db.index_bucket_count)),
      MetricsItem::new("IndexBucketSizeBytes", n(db.index_bucket_size_bytes)),
      MetricsItem::new("IndexMemorySizeBytes", n(db.index_memory_size_bytes)),
      MetricsItem::new(
        "IndexOverflowBucketCount",
        n(db.index_overflow_bucket_count),
      ),
      MetricsItem::new(
        "IndexOverflowMemorySizeBytes",
        n(db.index_overflow_memory_size_bytes),
      ),
      MetricsItem::new(
        "IndexTotalMemorySizeBytes",
        n(db.index_total_memory_size_bytes),
      ),
      MetricsItem::new("LogDir", provider.server_facts().log_dir),
      MetricsItem::new("Log.PageSizeBytes", n(db.log_page_size_bytes)),
      MetricsItem::new("Log.MaxPageCount", n(db.log_max_allocated_page_count)),
      MetricsItem::new("Log.AllocatedPageCount", n(db.log_allocated_page_count)),
      MetricsItem::new("Log.MaxMemorySizeBytes", n(db.log_max_memory_size_bytes)),
      MetricsItem::new("Log.CurrentMemorySizeBytes", n(db.log_memory_size_bytes)),
      MetricsItem::new("Log.CurrentHeapSizeBytes", n(db.log_heap_size_bytes)),
      MetricsItem::new("Log.BeginAddress", n(db.log_begin_address)),
      MetricsItem::new("Log.HeadAddress", n(db.log_head_address)),
      MetricsItem::new("Log.SafeReadOnlyAddress", n(db.log_safe_readonly_address)),
      MetricsItem::new("Log.FlushedUntilAddress", n(db.log_flushed_until_address)),
      MetricsItem::new("Log.TailAddress", n(db.log_tail_address)),
    ];

    let read_cache = db.read_cache.as_ref();
    let na = |v: Option<i64>| v.map_or_else(|| "N/A".to_string(), |v| v.to_string());
    items.push(MetricsItem::new(
      "ReadCache.PageSizeBytes",
      na(read_cache.map(|rc| rc.page_size_bytes)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.MaxPageCount",
      na(read_cache.map(|rc| rc.max_allocated_page_count)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.AllocatedPageCount",
      na(read_cache.map(|rc| rc.allocated_page_count)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.MaxMemorySizeBytes",
      na(read_cache.map(|rc| rc.max_memory_size_bytes)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.CurrentMemorySizeBytes",
      na(read_cache.map(|rc| rc.memory_size_bytes)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.CurrentHeapSizeBytes",
      na(read_cache.map(|rc| rc.heap_size_bytes)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.BeginAddress",
      na(read_cache.map(|rc| rc.begin_address)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.HeadAddress",
      na(read_cache.map(|rc| rc.head_address)),
    ));
    items.push(MetricsItem::new(
      "ReadCache.TailAddress",
      na(read_cache.map(|rc| rc.tail_address)),
    ));
    items
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreHashDistribution
  fn populate_store_hash_distribution(&mut self, provider: &impl InfoProvider) {
    let mut info = vec![Vec::new(); (provider.max_database_id() + 1).max(0) as usize];
    for db in provider.databases() {
      info[db.id as usize] = vec![MetricsItem::new("", db.hash_distribution_dump.clone())];
    }
    self.store_hash_distr_info = Some(info);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreRevivInfo
  fn populate_store_reviv_info(&mut self, provider: &impl InfoProvider) {
    let mut info = vec![Vec::new(); (provider.max_database_id() + 1).max(0) as usize];
    for db in provider.databases() {
      info[db.id as usize] = vec![MetricsItem::new("", db.revivification_dump.clone())];
    }
    self.store_reviv_info = Some(info);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulatePersistenceInfo
  fn populate_persistence_info(&mut self, provider: &impl InfoProvider) {
    let mut info = vec![Vec::new(); (provider.max_database_id() + 1).max(0) as usize];
    for db in provider.databases() {
      info[db.id as usize] = Self::get_database_persistence_stats(provider, &db);
    }
    self.persistence_info = Some(info);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetDatabasePersistenceStats
  fn get_database_persistence_stats(
    provider: &impl InfoProvider,
    db: &DbSnapshot,
  ) -> Vec<MetricsItem> {
    let aof_enabled = provider.server_facts().enable_aof;
    let na = |v: Option<i64>| v.map_or_else(|| "N/A".to_string(), |v| v.to_string());
    let aof = db.aof.as_ref();
    vec![
      MetricsItem::new(
        "CommittedBeginAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          na(aof.map(|a| a.committed_begin_address))
        },
      ),
      MetricsItem::new(
        "CommittedUntilAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          na(aof.map(|a| a.committed_until_address))
        },
      ),
      MetricsItem::new(
        "FlushedUntilAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          na(aof.map(|a| a.flushed_until_address))
        },
      ),
      MetricsItem::new(
        "BeginAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          na(aof.map(|a| a.begin_address))
        },
      ),
      MetricsItem::new(
        "TailAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          na(aof.map(|a| a.tail_address))
        },
      ),
      MetricsItem::new(
        "SafeAofAddress",
        if !aof_enabled {
          "N/A".into()
        } else {
          provider.safe_aof_address().to_string()
        },
      ),
    ]
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateClientsInfo
  fn populate_clients_info(&mut self, provider: &impl InfoProvider) {
    let connected = provider.global_metrics().map_or(0, |g| {
      g.total_connections_received - g.total_connections_disposed
    });
    self.clients_info = Some(vec![MetricsItem::new(
      "connected_clients",
      if provider.global_metrics().is_some() {
        connected.to_string()
      } else {
        "0".to_string()
      },
    )]);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateKeyspaceInfo
  fn populate_keyspace_info(&mut self, provider: &impl InfoProvider) {
    let mut items = None;
    for db in provider.databases() {
      let (key_count, expire_count) = provider.keyspace_stats(db.id);

      // Redis 仅列出当前至少持有一个键的库。
      if key_count == 0 {
        continue;
      }
      items.get_or_insert_with(Vec::new).push(MetricsItem::new(
        format!("db{}", db.id),
        format!("keys={key_count},expires={expire_count},avg_ttl=0"),
      ));
    }
    self.keyspace_info = items;
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateClusterBufferPoolStats
  fn populate_cluster_buffer_pool_stats(&mut self, provider: &impl InfoProvider) {
    self.buffer_pool_stats = Some(
      provider
        .buffer_pool_stats()
        .into_iter()
        .map(|(name, stats)| MetricsItem::new(name, stats))
        .collect(),
    );
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCheckpointInfo
  fn populate_checkpoint_info(&mut self, provider: &impl InfoProvider) {
    self.checkpoint_stats = provider.checkpoint_info();
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateHlogScanInfo
  fn populate_hlog_scan_info(&mut self, provider: &impl InfoProvider) {
    let mut result = Vec::new();
    for (i, (main, object)) in provider.hlog_scan_dump().into_iter().enumerate() {
      // 空转储以 "Empty" 呈现（对齐 C#）。
      let main = if main.is_empty() {
        "Empty".to_string()
      } else {
        main
      };
      let object = if object.is_empty() {
        "Empty".to_string()
      } else {
        object
      };
      result.push(vec![
        MetricsItem::new(format!("MainStore_HLog_{i}"), main),
        MetricsItem::new(format!("ObjectStore_HLog_{i}"), object),
      ]);
    }
    self.hlog_scan_stats = Some(result);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetSectionHeader
  ///
  /// 段名内不得含词分隔符，否则部分客户端无法解析 INFO 输出。
  pub fn get_section_header(info_type: InfoMetricsType, db_id: i32) -> String {
    match info_type {
      InfoMetricsType::Server => "Server".into(),
      InfoMetricsType::Memory => "Memory".into(),
      InfoMetricsType::Cluster => "Cluster".into(),
      InfoMetricsType::Replication => "Replication".into(),
      InfoMetricsType::Stats => "Stats".into(),
      InfoMetricsType::Store => format!("Store_DB_{db_id}"),
      InfoMetricsType::StoreHashtable => format!("StoreHashTableDistribution_DB_{db_id}"),
      InfoMetricsType::StoreReviv => format!("StoreDeletedRecordRevivification_DB_{db_id}"),
      InfoMetricsType::Persistence => format!("Persistence_DB_{db_id}"),
      InfoMetricsType::Clients => "Clients".into(),
      InfoMetricsType::Keyspace => "Keyspace".into(),
      InfoMetricsType::Modules => "Modules".into(),
      InfoMetricsType::BpStats => "BufferPoolStats".into(),
      InfoMetricsType::CInfo => "CheckpointInfo".into(),
      InfoMetricsType::HlogScan => format!("MainStoreHLogScan_DB_{db_id}"),
      InfoMetricsType::CommandStats => "Commandstats".into(),
    }
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetSectionRespInfo
  ///
  /// 追加 `# <header>\r\n` 与全部指标行；直接复用 wnode::format_info_section。
  #[inline]
  fn get_section_resp_info(
    section_header: &str,
    info: Option<&[MetricsItem]>,
    sb_response: &mut String,
  ) {
    format_info_section(section_header, info, sb_response);
  }

  /// 对应 GetRespInfo 单段填充实现
  fn get_resp_info_single(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
    sb_response: &mut String,
  ) {
    let header = Self::get_section_header(section, db_id);

    match section {
      InfoMetricsType::Server => {
        self.populate_server_info(provider);
        Self::get_section_resp_info(&header, self.server_info.as_deref(), sb_response);
      }
      InfoMetricsType::Memory => {
        self.populate_memory_info(provider);
        Self::get_section_resp_info(&header, self.memory_info.as_deref(), sb_response);
      }
      InfoMetricsType::Cluster => {
        self.populate_cluster_info(provider);
        Self::get_section_resp_info(&header, self.cluster_info.as_deref(), sb_response);
      }
      InfoMetricsType::Replication => {
        self.populate_replication_info(provider);
        Self::get_section_resp_info(&header, self.replication_info.as_deref(), sb_response);
      }
      InfoMetricsType::Stats => {
        self.populate_stats_info(provider);
        Self::get_section_resp_info(&header, self.stats_info.as_deref(), sb_response);
      }
      InfoMetricsType::Store => {
        self.populate_store_stats(provider);
        Self::get_section_resp_info(
          &header,
          self
            .store_info
            .as_deref()
            .and_then(|v| v.get(db_id as usize))
            .map(|v| &v[..]),
          sb_response,
        );
      }
      InfoMetricsType::StoreHashtable => {
        self.populate_store_hash_distribution(provider);
        Self::get_section_resp_info(
          &header,
          self
            .store_hash_distr_info
            .as_deref()
            .and_then(|v| v.get(db_id as usize))
            .map(|v| &v[..]),
          sb_response,
        );
      }
      InfoMetricsType::StoreReviv => {
        self.populate_store_reviv_info(provider);
        Self::get_section_resp_info(
          &header,
          self
            .store_reviv_info
            .as_deref()
            .and_then(|v| v.get(db_id as usize))
            .map(|v| &v[..]),
          sb_response,
        );
      }
      InfoMetricsType::Persistence => {
        if !provider.server_facts().enable_aof {
          return;
        }
        self.populate_persistence_info(provider);
        Self::get_section_resp_info(
          &header,
          self
            .persistence_info
            .as_deref()
            .and_then(|v| v.get(db_id as usize))
            .map(|v| &v[..]),
          sb_response,
        );
      }
      InfoMetricsType::Clients => {
        self.populate_clients_info(provider);
        Self::get_section_resp_info(&header, self.clients_info.as_deref(), sb_response);
      }
      InfoMetricsType::Keyspace => {
        self.populate_keyspace_info(provider);
        Self::get_section_resp_info(&header, self.keyspace_info.as_deref(), sb_response);
      }
      InfoMetricsType::Modules => {
        Self::get_section_resp_info(&header, None, sb_response);
      }
      InfoMetricsType::BpStats => {
        self.populate_cluster_buffer_pool_stats(provider);
        Self::get_section_resp_info(&header, self.buffer_pool_stats.as_deref(), sb_response);
      }
      InfoMetricsType::CInfo => {
        self.populate_checkpoint_info(provider);
        Self::get_section_resp_info(&header, self.checkpoint_stats.as_deref(), sb_response);
      }
      InfoMetricsType::HlogScan => {
        self.populate_hlog_scan_info(provider);
        Self::get_section_resp_info(
          &header,
          self
            .hlog_scan_stats
            .as_deref()
            .and_then(|v| v.get(db_id as usize))
            .map(|v| &v[..]),
          sb_response,
        );
      }
      InfoMetricsType::CommandStats => {
        self.populate_command_stats_info(provider);
        Self::get_section_resp_info(&header, self.command_stats_info.as_deref(), sb_response);
      }
    }
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetRespInfo（多段）
  ///
  /// 按序填充并拼接各段；段间以 `\r\n` 分隔。
  pub fn get_resp_info(
    &mut self,
    sections: &[InfoMetricsType],
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> String {
    let mut sb_response = String::new();
    for (i, section) in sections.iter().enumerate() {
      self.get_resp_info_single(*section, db_id, provider, &mut sb_response);
      if i != sections.len() - 1 {
        sb_response.push_str("\r\n");
      }
    }
    sb_response
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetMetricInternal
  fn get_metric_internal(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<Vec<MetricsItem>> {
    match section {
      InfoMetricsType::Server => {
        self.populate_server_info(provider);
        self.server_info.clone()
      }
      InfoMetricsType::Memory => {
        self.populate_memory_info(provider);
        self.memory_info.clone()
      }
      InfoMetricsType::Cluster => {
        self.populate_cluster_info(provider);
        self.cluster_info.clone()
      }
      InfoMetricsType::Replication => {
        self.populate_replication_info(provider);
        self.replication_info.clone()
      }
      InfoMetricsType::Stats => {
        self.populate_stats_info(provider);
        self.stats_info.clone()
      }
      InfoMetricsType::Store => {
        self.populate_store_stats(provider);
        self
          .store_info
          .as_deref()
          .and_then(|v| v.get(db_id as usize))
          .cloned()
      }
      InfoMetricsType::StoreHashtable => {
        self.populate_store_hash_distribution(provider);
        self
          .store_hash_distr_info
          .as_deref()
          .and_then(|v| v.get(db_id as usize))
          .cloned()
      }
      InfoMetricsType::StoreReviv => {
        self.populate_store_reviv_info(provider);
        self
          .store_reviv_info
          .as_deref()
          .and_then(|v| v.get(db_id as usize))
          .cloned()
      }
      InfoMetricsType::Persistence => {
        if !provider.server_facts().enable_aof {
          return None;
        }
        self.populate_persistence_info(provider);
        self
          .persistence_info
          .as_deref()
          .and_then(|v| v.get(db_id as usize))
          .cloned()
      }
      InfoMetricsType::Clients => {
        self.populate_clients_info(provider);
        self.clients_info.clone()
      }
      InfoMetricsType::Keyspace => {
        self.populate_keyspace_info(provider);
        self.keyspace_info.clone()
      }
      InfoMetricsType::Modules => None,
      InfoMetricsType::CommandStats => {
        self.populate_command_stats_info(provider);
        self.command_stats_info.clone()
      }
      _ => None,
    }
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetMetric
  pub fn get_metric(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<Vec<MetricsItem>> {
    self.get_metric_internal(section, db_id, provider)
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetInfoMetrics
  ///
  /// 迭代产出非空段（对齐 C# yield return）。
  pub fn get_info_metrics(
    &mut self,
    sections: &[InfoMetricsType],
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Vec<(InfoMetricsType, Vec<MetricsItem>)> {
    sections
      .iter()
      .filter_map(|&section| {
        self
          .get_metric_internal(section, db_id, provider)
          .map(|items| (section, items))
      })
      .collect()
  }
}

/// 页大小（对齐 Environment.SystemPageSize 的用途）。
fn page_size() -> usize {
  // 各平台页大小下限 4096；macOS/Linux 的实际值不影响 INFO 语义。
  4096
}

/// 生成 40 位十六进制实例 id（对齐 Garnet.common Generator.DefaultHexId 的
/// 随机十六进制形态；以时间熵 + 迭代计数驱动的 xorshift 生成）。
pub fn generate_default_hex_id() -> String {
  use std::sync::atomic::{AtomicU64, Ordering};
  static STATE: AtomicU64 = AtomicU64::new(0);
  let mut state = STATE.fetch_add(1, Ordering::Relaxed);
  if state == 0 {
    state = coarsetime::Clock::now_since_epoch().as_nanos() | 1;
  }
  let mut out = String::with_capacity(40);
  while out.len() < 40 {
    // xorshift64*。
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    let x = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
    out.push_str(&format!("{x:016x}"));
  }
  out.truncate(40);
  out
}
