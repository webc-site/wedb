use std::{env, fmt, num::NonZeroUsize, thread};

use wbase::time::{TICKS_PER_SECOND, now_stopwatch_ticks};
use wresp::metrics::{InfoMetricsType, MetricsItem, fmt_n2};

use crate::{
  GarnetServerMonitor, garnet_session_metrics::GarnetSessionMetrics, system_metrics::SystemMetrics,
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

/// 缺省 40 位全零十六进制身份串（对标 C# libs/common/Generator.cs:31 DefaultHexId(40)；
/// 用于复制信息未就绪时的稳定占位）
pub const DEFAULT_HEX_ID: &str = "0000000000000000000000000000000000000000";

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
  /// 自研总闸观测面，C# 无对位）。
  pub tree_cache_reserved_bytes: i64,
  /// 升阶树页缓存总预算定额字节（0 = 不设限）。
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
  /// wkv 虚库行标记（rust 形态特有，C# 每库皆物理库恒 false）：
  /// wkv 单物理存储多库前缀隔离，键空间扫描发现的无物理快照行的虚库号
  /// 以虚库行并入行表（仅贡献 KEYSACE 逐库枚举与行表长度，存储标量恒
  /// 零值），STORE / MEMORY / PERSISTENCE 三处物理事实消费点对虚库行
  /// 跳过填行，存储事实经 [`GarnetInfoMetrics::store_row`] 回落物理
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
  /// 进程启动时刻的单调刻度（100ns 计时域，源 `wbase::time::now_stopwatch_ticks`，
  /// 仅供同进程 uptime 区间作差，绝不当时间戳用；
  /// 在 garnet 中的相对路径:libs/server/StoreWrapper.cs:StoreWrapper.startupTimestamp）
  pub startup_stopwatch_ticks: u64,
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

  /// 全局指标快照（监视器未启用为 None）。
  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot>;

  /// 聚合命令统计：`(cmdstat 名(小写), calls, rejected_calls, failed_calls)`，
  /// 已过滤 calls/rejected 两栏判零（failed 恒透出不参与过滤，对齐 C#
  /// PopulateCommandStatsInfo 谓词两栏判零，GarnetInfoMetrics.cs:274）与
  /// "unknown"；两栏口径实码见 info_provider.rs 聚合臂，分叉登记 deviations §130。
  fn command_stats(&self) -> Vec<(String, u64, u64, u64)>;

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

  /// 主侧安全 AOF 地址：真复制位点投影（cluster 主侧 get_primary_info 首元素），
  /// 非主/无提供方/无集群回 0——C# 对位字段系仅声明 -1 的死字段（恒 "-1"），
  /// 哨兵方向反向分叉已登 deviations §130，严禁按死字段形回改。
  fn safe_aof_address(&self) -> i64;

  /// 后台任务健康快照：`(任务名/计数名, 展示值)`——存活任务 `name=alive`,
  /// 死亡任务 `name=dead(panic=N)`,登记计数 `name=N`（r30-bgthread 发现五：
  /// server 段 `bg_task_health` 一行暴露；宿主未接监督面为空，绝不虚报）。
  fn bg_task_health(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  /// 原生分配器记账字节数（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:143
  /// `native_allocator_bytes` ← Tsavorite NativeMemoryTracker.Bytes 全局总账）。
  /// 观测叠影字段，不参与 total_main_store_size 求和；无 windex 记账源的宿主
  /// 形态走本缺省 0 占位，绝不虚报。
  fn native_allocator_bytes(&self) -> i64 {
    0
  }

  /// server_socket 池统计行：`(server_socket_i, 统计文本)`（对标
  /// GarnetInfoMetrics.cs:413 逐 TCP server 出行的宿主侧供给臂）。无网络
  /// 监听面的宿主形态走本缺省空表，与 C# 无 server 态同。
  fn server_socket_buffer_pool_stats(&self) -> Vec<(String, String)> {
    Vec::new()
  }
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
    // uptime 是「过了多久」的区间语义，取单调刻度域作差（C#
    // `Stopwatch.GetElapsedTime(storeWrapper.startupTimestamp)` 对位）：单调域
    // 差值恒非负，saturating_sub 仅作防御；两次读数各含 +1 哨兵偏移，作差自消
    let uptime_secs = (now_stopwatch_ticks().saturating_sub(facts.startup_stopwatch_ticks)
      / TICKS_PER_SECOND) as i64;
    let mut server_items = vec![
      MetricsItem::new("garnet_version", facts.version),
      MetricsItem::new("server_name", "garnet"),
      MetricsItem::new("os", env::consts::OS),
      MetricsItem::from_usize(
        "processor_count",
        thread::available_parallelism().map_or(1, NonZeroUsize::get),
      ),
      MetricsItem::new(
        "arch_bits",
        if cfg!(target_pointer_width = "64") {
          "64"
        } else {
          "32"
        },
      ),
      MetricsItem::from_i64("uptime_in_seconds", uptime_secs),
      MetricsItem::from_i64("uptime_in_days", uptime_secs / 86_400),
      MetricsItem::new("monitor_task", onoff(facts.metrics_sampling_frequency > 0)),
      MetricsItem::from_i64("monitor_freq", facts.metrics_sampling_frequency as i64),
      MetricsItem::new("latency_monitor", onoff(facts.latency_monitor)),
      MetricsItem::new("commandstats_monitor", onoff(facts.command_stats_monitor)),
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
    ];
    // 后台任务健康一行（r30-bgthread 发现五：任务名/存活/panic 计数 + 登记计数，
    // 对标 C# TaskManager 注册表 IsRunning 观测面；宿主未接监督面为空即省略，
    // 绝不虚报）——C# 无对位项（.NET 无 panic 语义），rust 观测缺口补齐
    let bg = provider.bg_task_health();
    if !bg.is_empty() {
      let text = bg
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",");
      server_items.push(MetricsItem::new("bg_task_health", text));
    }
    self.server_info = Some(server_items);
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
      // wkv 虚库行零值且非物理事实持有者，跳之（语义与 [`Self::store_row`]
      // 物理首行回落同口径；C# 每库皆物理，无本分支）
      if db.virtual_db {
        continue;
      }
      store_index_size += db.index_total_memory_size_bytes;
      aof_log_memory_size += db.aof_memory_size_bytes;
      // 主日志目标内存仅存在时另记，实测内存两分支同值直累
      if let Some(target) = db.mainlog_target_size {
        store_mainlog_memory_target_size += target;
      }
      store_mainlog_memory_size += db.log_memory_size_bytes;
      // C# readcache 目标内存另累加进 store_readcache_memory_target_size，但该
      // 累加值不出现在任何 INFO 输出（仅写不读），故两臂同值合并、不再携带。
      store_readcache_memory_size += db.read_cache.as_ref().map_or(0, |rc| rc.memory_size_bytes);
    }

    let total_store_size =
      store_index_size + store_mainlog_memory_size + store_readcache_memory_size;

    let m = |name: &'static str, value: i64| MetricsItem::from_i64(name, value);
    let mut items = Vec::with_capacity(1 + MEM_SOURCE_PAIRS.len() * 2 + 11);
    items.push(MetricsItem::from_usize("system_page_size", page_size()));
    items.extend(MEM_SOURCE_PAIRS.iter().flat_map(|&(name, mb_name, f)| {
      [
        MetricsItem::from_i64(name, f(1)),
        MetricsItem::from_i64(mb_name, f(MB_UNITS)),
      ]
    }));
    items.extend([
      // PopulateMemoryInfo 读 GC.GetGCMemoryInfo，无运行时对位）。
      // native_allocator_bytes：provider 真值（C# :143 ← Tsavorite
      // NativeMemoryTracker.Bytes 全局总账；rust 对位 windex::ram::
      // NativeMemoryTracker，宿主侧经本 trait 单点接线，无记账源形态出 0）
      m("gc_committed_bytes", 0),
      m("gc_heap_bytes", 0),
      m("gc_managed_memory_bytes_excluding_heap", 0),
      m("gc_fragmented_bytes", 0),
      m("native_allocator_bytes", provider.native_allocator_bytes()),
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
    self.memory_info = Some(items);
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
      // 在 garnet 中的相对路径:libs/server/Metrics/Info/GarnetInfoMetrics.cs:167-168
      // 无集群提供方时兜底填 Generator.DefaultHexId() 全零常量，杜绝连续两次 INFO 产生 diff
      vec![
        MetricsItem::new("role", "master"),
        MetricsItem::new("connected_slaves", "0"),
        MetricsItem::new("master_failover_state", "no-failover"),
        MetricsItem::new("master_replid", DEFAULT_HEX_ID),
        MetricsItem::new("master_replid2", DEFAULT_HEX_ID),
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
    let global = provider.global_metrics();
    // 监视器未启用：全部计 0（对齐 metricsDisabled 分支）——零值快照与实测共用
    // STATS_ROWS 单源；C# clusterEnabled 即无条件并入 gossip 行，
    // metricsDisabled 仅逐行折零不折行（GarnetInfoMetrics.cs:212-219）
    let zero = GlobalMetricsSnapshot::default();
    let snap = global.as_ref().unwrap_or(&zero);
    let mut items: Vec<MetricsItem> = STATS_ROWS
      .iter()
      .map(|&(name, get)| MetricsItem::new(name, get(snap)))
      .collect();

    if facts.enable_cluster {
      // metrics_disabled 与 C# PopulateStatsInfo 局部同源
      //（storeWrapper.monitor == null），gossip 段并入不再另立「指标是否禁用」口径。
      let metrics_disabled = GarnetServerMonitor::global()
        .and_then(|m| m.snapshot())
        .is_none();
      items.extend(provider.gossip_stats(metrics_disabled));
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
          .map(|(name, calls, rejected, failed)| {
            MetricsItem::new(
              format!("cmdstat_{name}"),
              format!(
                "calls={calls},usec=0,usec_per_call=0.00,rejected_calls={rejected},failed_calls={failed}"
              ),
            )
          })
          .collect(),
      )
    };
  }

  /// STORE 族行表预分配（对标 C# `new MetricsItem[storeWrapper.MaxDatabaseId +
  /// 1][]`）：行数 = 快照最大库 id + 1（空快照 0 行）。未填行保留空 Vec
  ///（对应 C# 的 null 项），取行时由 [`Self::store_row`] 回落至物理首行。
  fn store_rows(databases: &[DbSnapshot]) -> Vec<Vec<MetricsItem>> {
    let rows = databases
      .iter()
      .map(|db| db.id)
      .max()
      .map_or(0, |max| max as usize + 1);
    vec![Vec::new(); rows]
  }

  /// 按库段取行收口（对标 C# `storeInfo[dbId]` 的单点行表访问）：
  /// 优先命中活跃库行；wedb 单物理存储多库前缀隔离，快照行表恒以唯一物理
  /// 行（db 0）承载全部库的存储事实，活跃库号越界或命中未填行时回落至该
  /// 物理首行，段头仍按活跃库号呈现。
  fn store_row(rows: Option<&[Vec<MetricsItem>]>, db_id: i32) -> Option<&[MetricsItem]> {
    let rows = rows?;
    rows
      .get(db_id as usize)
      .filter(|row| !row.is_empty())
      .or_else(|| rows.first().filter(|row| !row.is_empty()))
      .map(|row| &row[..])
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreStats
  fn populate_store_stats(&mut self, provider: &impl InfoProvider) {
    self.store_info = Some(Self::filled_rows(
      provider,
      true,
      Self::get_database_store_stats,
    ));
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
      MetricsItem::new("TreeCache.ReservedBytes", n(db.tree_cache_reserved_bytes)),
      MetricsItem::new("TreeCache.BudgetBytes", n(db.tree_cache_budget_bytes)),
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

    let rc = db.read_cache.as_ref();
    // 读缓存九项单次遍历出行（快照缺失恒 N/A，对齐 C# 逐项 null 合并口径）
    items.extend(
      READ_CACHE_ROWS
        .iter()
        .map(|&(name, get)| MetricsItem::new(name, na_opt(rc.map(get)))),
    );
    items
  }

  /// 逐库填行表共用骨架（对标 C# 行表预分配 + 逐库出行样板，STORE /
  /// STOREHASHTABLE / STOREREVIV / PERSISTENCE 四段同源）：`row` 出一条库快照
  /// 的该段行；`skip_virtual` = 虚库行不填——存储/持久化事实单源物理首行
  ///（store_row 回落呈现），杜绝扫描侧最小事实外溢；转储段虚库行照常填充。
  fn filled_rows<P: InfoProvider>(
    provider: &P,
    skip_virtual: bool,
    row: fn(&P, &DbSnapshot) -> Vec<MetricsItem>,
  ) -> Vec<Vec<MetricsItem>> {
    let databases = provider.databases();
    let mut info = Self::store_rows(&databases);
    for db in &databases {
      if skip_virtual && db.virtual_db {
        continue;
      }
      info[db.id as usize] = row(provider, db);
    }
    info
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreHashDistribution
  fn populate_store_hash_distribution(&mut self, provider: &impl InfoProvider) {
    // 转储段逐库单行 `MetricsItem["", dump]`（对标 C# 两段同形样板）
    self.store_hash_distr_info = Some(Self::filled_rows(provider, false, |_, db| {
      vec![MetricsItem::new("", db.hash_distribution_dump.clone())]
    }));
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateStoreRevivInfo
  fn populate_store_reviv_info(&mut self, provider: &impl InfoProvider) {
    self.store_reviv_info = Some(Self::filled_rows(provider, false, |_, db| {
      vec![MetricsItem::new("", db.revivification_dump.clone())]
    }));
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulatePersistenceInfo
  fn populate_persistence_info(&mut self, provider: &impl InfoProvider) {
    self.persistence_info = Some(Self::filled_rows(
      provider,
      true,
      Self::get_database_persistence_stats,
    ));
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetDatabasePersistenceStats
  fn get_database_persistence_stats(
    provider: &impl InfoProvider,
    db: &DbSnapshot,
  ) -> Vec<MetricsItem> {
    // AOF 未启用整段 N/A；启用后快照缺失（None）亦 N/A（对齐 C# 空合口径）
    let enabled = provider.server_facts().enable_aof;
    // 未启用视同快照缺失，地址族五行与逐行 N/A 口径单源
    let aof = db.aof.as_ref().filter(|_| enabled);
    let mut items: Vec<MetricsItem> = AOF_ROWS
      .iter()
      .map(|&(name, get)| MetricsItem::new(name, na_opt(aof.map(get))))
      .collect();
    items.push(MetricsItem::new(
      "SafeAofAddress",
      na_opt(enabled.then(|| provider.safe_aof_address())),
    ));
    // 刷盘失败累计行尾项（r30-bgthread 发现五：常驻提交驱动致命故障信号，
    // 非零即可告警；C# cannedException 的运维可见面对位）——快照缺失以 0 呈现
    items.push(MetricsItem::new(
      "aof_flush_failures",
      na_opt(enabled.then(|| db.aof.as_ref().map_or(0, |a| a.flush_failures))),
    ));
    items
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateClientsInfo
  ///
  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateClientsInfo
  fn populate_clients_info(&mut self, provider: &impl InfoProvider) {
    // 监视器未启用出 0（原对 global_metrics() 的双读取归并为单快照）
    let connected = provider.global_metrics().map_or(0, |g| {
      g.total_connections_received - g.total_connections_disposed
    });
    self.clients_info = Some(vec![MetricsItem::from_i64("connected_clients", connected)]);
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
  ///
  /// 拼装序对标 C# :408-416：先逐 TCP server 出 `server_socket_{i}` 行，
  /// clusterProvider 非空再追加集群端口行——一次迭代器链单次收集。
  fn populate_cluster_buffer_pool_stats(&mut self, provider: &impl InfoProvider) {
    self.buffer_pool_stats = Some(
      provider
        .server_socket_buffer_pool_stats()
        .into_iter()
        .chain(provider.buffer_pool_stats())
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
    // 空转储以 "Empty" 呈现（对齐 C#）。
    let dump = |s: String| if s.is_empty() { "Empty".to_string() } else { s };
    let mut result = Vec::new();
    for (i, (main, object)) in provider.hlog_scan_dump().into_iter().enumerate() {
      result.push(vec![
        MetricsItem::new(format!("MainStore_HLog_{i}"), dump(main)),
        MetricsItem::new(format!("ObjectStore_HLog_{i}"), dump(object)),
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
  /// 追加 `# <header>\r\n` 与全部指标行；单次预估容量，零额外 `format!` 堆分配。
  /// 无名首项（多行字符串指标）直接裸出值，避免行首游离冒号。
  #[inline]
  fn get_section_resp_info(
    section_header: &str,
    info: Option<&[MetricsItem]>,
    sb_response: &mut String,
  ) {
    let header_len = 2 + section_header.len() + 2;
    let items_len = match info {
      Some(items) => items
        .iter()
        .map(|it| it.name.len() + it.value.len() + 3)
        .sum::<usize>(),
      None => 0,
    };
    sb_response.reserve(header_len + items_len);

    sb_response.push_str("# ");
    sb_response.push_str(section_header);
    sb_response.push_str("\r\n");
    let Some(info) = info else {
      return;
    };
    if info.first().is_some_and(|item| item.name.is_empty()) {
      sb_response.push_str(&info[0].value);
      sb_response.push_str("\r\n");
      return;
    }
    for item in info {
      sb_response.push_str(&item.name);
      sb_response.push(':');
      sb_response.push_str(&item.value);
      sb_response.push_str("\r\n");
    }
  }

  /// 按段填充并取回该段指标（`GetRespInfo`/`GetMetric` 双面共用骨架）：
  /// 单段段直取自身，行表段按活跃库取行；PERSISTENCE 未启用 AOF 不填
  ///（对应 C# PopulatePersistenceInfo 前置短路），MODULES 恒空段。
  fn populate_section(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<&[MetricsItem]> {
    match section {
      InfoMetricsType::Server => {
        self.populate_server_info(provider);
        self.server_info.as_deref()
      }
      InfoMetricsType::Memory => {
        self.populate_memory_info(provider);
        self.memory_info.as_deref()
      }
      InfoMetricsType::Cluster => {
        self.populate_cluster_info(provider);
        self.cluster_info.as_deref()
      }
      InfoMetricsType::Replication => {
        self.populate_replication_info(provider);
        self.replication_info.as_deref()
      }
      InfoMetricsType::Stats => {
        self.populate_stats_info(provider);
        self.stats_info.as_deref()
      }
      InfoMetricsType::Store => {
        self.populate_store_stats(provider);
        Self::store_row(self.store_info.as_deref(), db_id)
      }
      InfoMetricsType::StoreHashtable => {
        self.populate_store_hash_distribution(provider);
        Self::store_row(self.store_hash_distr_info.as_deref(), db_id)
      }
      InfoMetricsType::StoreReviv => {
        self.populate_store_reviv_info(provider);
        Self::store_row(self.store_reviv_info.as_deref(), db_id)
      }
      InfoMetricsType::Persistence => {
        // 未启用 AOF 整段不填（C# 前置短路；渲染面在段头落笔前另行拦截）
        if !provider.server_facts().enable_aof {
          return None;
        }
        self.populate_persistence_info(provider);
        Self::store_row(self.persistence_info.as_deref(), db_id)
      }
      InfoMetricsType::Clients => {
        self.populate_clients_info(provider);
        self.clients_info.as_deref()
      }
      InfoMetricsType::Keyspace => {
        self.populate_keyspace_info(provider);
        self.keyspace_info.as_deref()
      }
      InfoMetricsType::Modules => None,
      InfoMetricsType::BpStats => {
        self.populate_cluster_buffer_pool_stats(provider);
        self.buffer_pool_stats.as_deref()
      }
      InfoMetricsType::CInfo => {
        self.populate_checkpoint_info(provider);
        self.checkpoint_stats.as_deref()
      }
      InfoMetricsType::HlogScan => {
        self.populate_hlog_scan_info(provider);
        Self::store_row(self.hlog_scan_stats.as_deref(), db_id)
      }
      InfoMetricsType::CommandStats => {
        self.populate_command_stats_info(provider);
        self.command_stats_info.as_deref()
      }
    }
  }

  /// 对应 GetRespInfo 单段填充实现
  fn get_resp_info_single(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
    sb_response: &mut String,
  ) {
    // PERSISTENCE 未启用 AOF 整段不出（连段头都不落笔，对齐 C# 短路位）
    if section == InfoMetricsType::Persistence && !provider.server_facts().enable_aof {
      return;
    }
    let header = Self::get_section_header(section, db_id);
    let info = self.populate_section(section, db_id, provider);
    Self::get_section_resp_info(&header, info, sb_response);
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

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetMetric（内部即 GetMetricInternal）
  ///
  /// BpStats/CInfo/HlogScan 三段仅 RESP 渲染面（C# 同走 `_ => null` 不覆盖），
  /// 此处前置过滤保持不填充。
  pub fn get_metric(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<Vec<MetricsItem>> {
    if matches!(
      section,
      InfoMetricsType::BpStats | InfoMetricsType::CInfo | InfoMetricsType::HlogScan
    ) {
      return None;
    }
    self
      .populate_section(section, db_id, provider)
      .map(<[MetricsItem]>::to_vec)
  }
}

/// 兆字节换算除数（`units` 形参按字节量级取 1 / 1 MiB 两档）
const MB_UNITS: i64 = 1 << 20;

/// 内存源取值函数（形参为字节量级除数）
type MemSourceFn = fn(i64) -> i64;

/// 内存源成对表：`(字节名, MB 名, 取值函数)`——字节/兆字节两行同源
///（对标 C# PopulateMemoryInfo 逐对的 `Get.../Get...(MB)` 双行口径）。
const MEM_SOURCE_PAIRS: &[(&str, &str, MemSourceFn)] = &[
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

/// 页大小（对齐 Environment.SystemPageSize 的用途）。
fn page_size() -> usize {
  // 各平台页大小下限 4096；macOS/Linux 的实际值不影响 INFO 语义。
  4096
}

/// bool 观测面 → enabled/disabled 二值文本（server 段三处共用）。
const fn onoff(enabled: bool) -> &'static str {
  if enabled { "enabled" } else { "disabled" }
}

/// 数值行缺值 N/A 空合口径（STORE 读缓存 / PERSISTENCE AOF 族共用）。
fn na_opt<T: fmt::Display>(value: Option<T>) -> String {
  value.map_or_else(|| "N/A".to_string(), |v| v.to_string())
}

/// STATS 段行取值函数（入参为全局指标快照）
type StatsFn = fn(&GlobalMetricsSnapshot) -> String;

/// Stats 段行表：`(行名, 取值函数)`——监视器未启用的零值兜底（C#
/// metricsDisabled 分支）与实测共用同源，根除双份逐行字面量的行名漂移
///（零值口径即 [`GlobalMetricsSnapshot::default`]，各行 Display 出参与原
/// "0"/fmt_n2(0.0) 字面量逐字节一致）。
const STATS_ROWS: &[(&str, StatsFn)] = &[
  ("total_connections_active", |g| {
    g.total_connections_active.to_string()
  }),
  ("total_connections_received", |g| {
    g.total_connections_received.to_string()
  }),
  ("total_connections_disposed", |g| {
    g.total_connections_disposed.to_string()
  }),
  ("total_commands_processed", |g| {
    g.global_session_metrics
      .get_total_commands_processed()
      .to_string()
  }),
  ("instantaneous_ops_per_sec", |g| {
    g.instantaneous_cmd_per_sec.to_string()
  }),
  ("total_net_input_bytes", |g| {
    g.global_session_metrics
      .get_total_net_input_bytes()
      .to_string()
  }),
  ("total_net_output_bytes", |g| {
    g.global_session_metrics
      .get_total_net_output_bytes()
      .to_string()
  }),
  ("instantaneous_net_input_KBps", |g| {
    g.instantaneous_net_input_tpt.to_string()
  }),
  ("instantaneous_net_output_KBps", |g| {
    g.instantaneous_net_output_tpt.to_string()
  }),
  ("total_pending", |g| {
    g.global_session_metrics.get_total_pending().to_string()
  }),
  ("total_found", |g| {
    g.global_session_metrics.get_total_found().to_string()
  }),
  ("total_notfound", |g| {
    g.global_session_metrics.get_total_notfound().to_string()
  }),
  ("garnet_hit_rate", |g| {
    let s = &g.global_session_metrics;
    let tt = s.get_total_found() + s.get_total_notfound();
    let rate = if tt > 0 {
      s.get_total_found() as f64 / tt as f64
    } else {
      0.0
    } * 100.0;
    fmt_n2(rate)
  }),
  ("total_cluster_commands_processed", |g| {
    g.global_session_metrics
      .get_total_cluster_commands_processed()
      .to_string()
  }),
  ("total_write_commands_processed", |g| {
    g.global_session_metrics
      .get_total_write_commands_processed()
      .to_string()
  }),
  ("total_read_commands_processed", |g| {
    g.global_session_metrics
      .get_total_read_commands_processed()
      .to_string()
  }),
  ("total_number_resp_server_session_exceptions", |g| {
    g.global_session_metrics
      .get_total_number_resp_server_session_exceptions()
      .to_string()
  }),
  ("total_transaction_commands_received", |g| {
    g.global_session_metrics
      .get_total_transaction_commands_received()
      .to_string()
  }),
  ("total_transaction_commands_execution_failed", |g| {
    g.global_session_metrics
      .get_total_transaction_commands_execution_failed()
      .to_string()
  }),
];

type ReadCacheRow = (&'static str, fn(&ReadCacheSnapshot) -> i64);
type AofRow = (&'static str, fn(&AofSnapshot) -> i64);

/// 读缓存九项行表：`(行名, 字段取器)`——快照 None 恒 N/A（STORE 段）。
const READ_CACHE_ROWS: &[ReadCacheRow] = &[
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
const AOF_ROWS: &[AofRow] = &[
  ("CommittedBeginAddress", |a| a.committed_begin_address),
  ("CommittedUntilAddress", |a| a.committed_until_address),
  ("FlushedUntilAddress", |a| a.flushed_until_address),
  ("BeginAddress", |a| a.begin_address),
  ("TailAddress", |a| a.tail_address),
];
