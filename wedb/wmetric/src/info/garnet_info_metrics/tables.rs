//! INFO 段集合常量与五张 const 行表（段头表 / 内存源成对表 / STATS 行表 /
//! 读缓存行表 / AOF 行表）。

use wresp::metrics::{InfoMetricsType, MetricsItem, fmt_n2};

use super::snapshots::{AofSnapshot, GlobalMetricsSnapshot, ReadCacheSnapshot};
use crate::system_metrics::SystemMetrics;

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

/// 缺省 40 位全零十六进制身份串（对标 C# libs/common/Generator.cs:31 DefaultHexId(40)；
/// 用于复制信息未就绪时的稳定占位）
pub const DEFAULT_HEX_ID: &str = "0000000000000000000000000000000000000000";

/// 段头与是否携带 `_DB_{db_id}` 后缀的编译期常量表（判别值升序）。
pub(super) const SECTION_HEADERS: [(&str, bool); 16] = [
  ("Server", false),
  ("Memory", false),
  ("Cluster", false),
  ("Replication", false),
  ("Stats", false),
  ("Store", true),
  ("StoreHashTableDistribution", true),
  ("StoreDeletedRecordRevivification", true),
  ("Persistence", true),
  ("Clients", false),
  ("Keyspace", false),
  ("Modules", false),
  ("BufferPoolStats", false),
  ("CheckpointInfo", false),
  ("MainStoreHLogScan", true),
  ("Commandstats", false),
];

/// 兆字节换算除数（`units` 形参按字节量级取 1 / 1 MiB 两档）
pub(super) const MB_UNITS: i64 = 1 << 20;

/// 内存源取值函数（形参为字节量级除数）
pub(super) type MemSourceFn = fn(i64) -> i64;

/// 内存源成对表：`(字节名, MB 名, 取值函数)`——字节/兆字节两行同源
///（对标 C# PopulateMemoryInfo 逐对的 `Get.../Get...(MB)` 双行口径）。
pub(super) const MEM_SOURCE_PAIRS: &[(&str, &str, MemSourceFn)] = &[
  (
    "total_system_memory",
    "total_system_memory(MB)",
    SystemMetrics::get_total_memory,
  ),
  (
    "available_system_memory",
    "available_system_memory(MB)",
    SystemMetrics::get_physical_available_memory,
  ),
  (
    "proc_paged_memory_size",
    "proc_paged_memory_size(MB)",
    SystemMetrics::get_paged_memory_size,
  ),
  (
    "proc_peak_paged_memory_size",
    "proc_peak_paged_memory_size(MB)",
    SystemMetrics::get_peak_paged_memory_size,
  ),
  (
    "proc_pageable_memory_size",
    "proc_pageable_memory_size(MB)",
    SystemMetrics::get_paged_system_memory_size,
  ),
  (
    "proc_private_memory_size",
    "proc_private_memory_size(MB)",
    SystemMetrics::get_private_memory_size64,
  ),
  (
    "proc_virtual_memory_size",
    "proc_virtual_memory_size(MB)",
    SystemMetrics::get_virtual_memory_size64,
  ),
  (
    "proc_peak_virtual_memory_size",
    "proc_peak_virtual_memory_size(MB)",
    SystemMetrics::get_peak_virtual_memory_size64,
  ),
  (
    "proc_physical_memory_size",
    "proc_physical_memory_size(MB)",
    SystemMetrics::get_physical_memory_usage,
  ),
  (
    "proc_peak_physical_memory_size",
    "proc_peak_physical_memory_size(MB)",
    SystemMetrics::get_peak_physical_memory_usage,
  ),
];

/// STATS 段行构造函数（入参为指标名与全局指标快照）
pub(super) type StatsRowFn = fn(&'static str, &GlobalMetricsSnapshot) -> MetricsItem;

/// Stats 段行表：`(行名, 构造函数)`——监视器未启用的零值兜底（C#
/// metricsDisabled 分支）与实测共用同源，根除双份逐行字面量的行名漂移
///（零值口径即 [`GlobalMetricsSnapshot::default`]，数值格式化零多余堆分配，
/// 各行 Display 出参与原 "0"/fmt_n2(0.0) 字面量逐字节一致）。
pub(super) const STATS_ROWS: &[(&str, StatsRowFn)] = &[
  ("total_connections_active", |name, g| {
    MetricsItem::from_i64(name, g.total_connections_active)
  }),
  ("total_connections_received", |name, g| {
    MetricsItem::from_i64(name, g.total_connections_received)
  }),
  ("total_connections_disposed", |name, g| {
    MetricsItem::from_i64(name, g.total_connections_disposed)
  }),
  ("rejected_connections", |name, g| {
    MetricsItem::from_i64(name, g.rejected_connections)
  }),
  ("total_commands_processed", |name, g| {
    MetricsItem::from_u64(
      name,
      g.global_session_metrics.get_total_commands_processed(),
    )
  }),
  ("instantaneous_ops_per_sec", |name, g| {
    MetricsItem::from_f64(name, g.instantaneous_cmd_per_sec)
  }),
  ("total_net_input_bytes", |name, g| {
    MetricsItem::from_u64(name, g.global_session_metrics.get_total_net_input_bytes())
  }),
  ("total_net_output_bytes", |name, g| {
    MetricsItem::from_u64(name, g.global_session_metrics.get_total_net_output_bytes())
  }),
  ("instantaneous_net_input_KBps", |name, g| {
    MetricsItem::from_f64(name, g.instantaneous_net_input_tpt)
  }),
  ("instantaneous_net_output_KBps", |name, g| {
    MetricsItem::from_f64(name, g.instantaneous_net_output_tpt)
  }),
  ("total_pending", |name, g| {
    MetricsItem::from_u64(name, g.global_session_metrics.get_total_pending())
  }),
  ("total_found", |name, g| {
    MetricsItem::from_u64(name, g.global_session_metrics.get_total_found())
  }),
  ("total_notfound", |name, g| {
    MetricsItem::from_u64(name, g.global_session_metrics.get_total_notfound())
  }),
  ("garnet_hit_rate", |name, g| {
    let s = &g.global_session_metrics;
    let tt = s.get_total_found() + s.get_total_notfound();
    let rate = if tt > 0 {
      s.get_total_found() as f64 / tt as f64
    } else {
      0.0
    } * 100.0;
    MetricsItem::new(name, fmt_n2(rate))
  }),
  ("total_cluster_commands_processed", |name, g| {
    MetricsItem::from_u64(
      name,
      g.global_session_metrics
        .get_total_cluster_commands_processed(),
    )
  }),
  ("total_write_commands_processed", |name, g| {
    MetricsItem::from_u64(
      name,
      g.global_session_metrics
        .get_total_write_commands_processed(),
    )
  }),
  ("total_read_commands_processed", |name, g| {
    MetricsItem::from_u64(
      name,
      g.global_session_metrics.get_total_read_commands_processed(),
    )
  }),
  ("total_number_resp_server_session_exceptions", |name, g| {
    MetricsItem::from_u64(
      name,
      g.global_session_metrics
        .get_total_number_resp_server_session_exceptions(),
    )
  }),
];

pub(super) type ReadCacheRow = (&'static str, fn(&ReadCacheSnapshot) -> i64);
pub(super) type AofRow = (&'static str, fn(&AofSnapshot) -> i64);

/// 读缓存九项行表：`(行名, 字段取器)`——快照 None 恒 N/A（STORE 段）。
pub(super) const READ_CACHE_ROWS: &[ReadCacheRow] = &[
  ("ReadCache.PageSizeBytes", |c| c.page_size_bytes),
  ("ReadCache.MaxPageCount", |c| c.max_allocated_page_count),
  ("ReadCache.AllocatedPageCount", |c| c.allocated_page_count),
  ("ReadCache.MaxMemorySizeBytes", |c| c.max_memory_size_bytes),
  ("ReadCache.CurrentMemorySizeBytes", |c| c.memory_size_bytes),
  ("ReadCache.CurrentHeapSizeBytes", |c| c.heap_size_bytes),
  ("ReadCache.BeginAddress", |c| c.begin_address),
  ("ReadCache.HeadAddress", |c| c.head_address),
  ("ReadCache.TailAddress", |c| c.tail_address),
];

/// AOF 地址族五行表：`(行名, 字段取器)`——PERSISTENCE 段 na 口径共用。
pub(super) const AOF_ROWS: &[AofRow] = &[
  ("CommittedBeginAddress", |a| a.committed_begin_address),
  ("CommittedUntilAddress", |a| a.committed_until_address),
  ("FlushedUntilAddress", |a| a.flushed_until_address),
  ("BeginAddress", |a| a.begin_address),
  ("TailAddress", |a| a.tail_address),
];
