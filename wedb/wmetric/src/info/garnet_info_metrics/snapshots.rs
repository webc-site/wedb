//! INFO 填充所需的纯数据快照（STORE / MEMORY / PERSISTENCE / STATS / SERVER
//! 各段消费的库级与进程级事实）。

use std::borrow::Cow;

use crate::garnet_session_metrics::GarnetSessionMetrics;

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
  /// 升阶树页缓存已预留字节数（在线活跃树环 + 在途 scratch 环容量和；rust
  /// 自研总闸观测面，C# 无对位，登记锚 deviations.md §166c）。
  pub tree_cache_reserved_bytes: i64,
  /// 升阶树页缓存总预算定额字节（0 = 不设限；rust 自研超集行，
  /// deviations.md §166c）。
  pub tree_cache_budget_bytes: i64,
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
  /// AOF 日志内存字节数。
  pub aof_memory_size_bytes: i64,
  /// AOF 持久化快照（未启用为 None）。
  pub aof: Option<AofSnapshot>,
  /// 哈希分布转储文本（STOREHASHTABLE）。
  pub hash_distribution_dump: String,
  /// 复活统计转储文本（STOREREVIV）。
  pub revivification_dump: String,
  /// wkv 虚库行标记（rust 形态特有，C# 每库皆物理库恒 false）：
  /// wkv 单物理存储多库前缀隔离，键空间扫描发现的无物理快照行的虚库号
  /// 以虚库行并入行表（仅贡献 KEYSACE 逐库枚举与行表长度，存储标量恒
  /// 零值），STORE / MEMORY / PERSISTENCE 三处物理事实消费点对虚库行
  /// 跳过填行，存储事实经 `GarnetInfoMetrics::store_row` 回落物理
  /// 首行呈现，与纯同步快照形态字节一致（最小扫描事实严禁外溢非扫描段）。
  pub virtual_db: bool,
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
  /// 刷盘失败累计（常驻提交驱动致命故障信号，PERSISTENCE 段
  /// `aof_flush_failures` 行尾出；对标 C# TsavoriteLog cannedException 的
  /// 运维可见面——非零即提交驱动发生过致命故障，运维可告警）
  pub flush_failures: u64,
}

/// 全局指标快照（STATS / CLIENTS 段；C# 直读 storeWrapper.monitor.GlobalMetrics）。
#[derive(Debug, Default, Clone, Copy)]
pub struct GlobalMetricsSnapshot {
  /// 活跃连接数。
  pub total_connections_active: i64,
  /// 收到的连接数。
  pub total_connections_received: i64,
  /// 已释放的连接数。
  pub total_connections_disposed: i64,
  /// 因连接上限（maxclients）被拒的连接数（PR #2157，C#
  /// TotalConnectionsRejected → INFO STATS rejected_connections 行）。
  pub rejected_connections: i64,
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
/// GarnetServerOptions 字段）。恒定串字段（版本 / 协议版本 / 进程级
/// run_id）为 `Cow<'static, str>`：装配侧零拷贝借静态字面，跨 `Clone`
/// 面（InfoSurface 快照转手）免堆分配。
#[derive(Debug, Clone)]
pub struct ServerFacts {
  /// Garnet 版本。
  pub version: Cow<'static, str>,
  /// 运行实例 id（集群态为提供方运行时串，单机态为进程级常量）。
  pub run_id: Cow<'static, str>,
  /// RESP 协议版本。
  pub redis_protocol_version: Cow<'static, str>,
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
  /// 进程启动时刻的单调刻度（100ns 计时域，源 `wbase::time::now_stopwatch_ticks`，
  /// 仅供同进程 uptime 区间作差，绝不当时间戳用；
  /// 对位 C# StoreWrapper 的 startupTimestamp 字段；起表锚留 wnode info_provider.rs
  /// 的 init_startup_ticks 一处）
  pub startup_stopwatch_ticks: u64,
  /// 日志目录（只读回落）。
  pub log_dir: String,
}
