//! INFO 各段指标的 populate_* 填充函数族（对标 C# GarnetInfoMetrics 的
//! PopulateXxxInfo / GetDatabaseXxxStats 同名件；段缓存字段直写在
//! [`GarnetInfoMetrics`] 上，取行骨架与逐库填行表共用单源）。

use std::{env, num::NonZeroUsize, thread};

use wbase::time::{TICKS_PER_SECOND, now_stopwatch_ticks};
use wresp::metrics::{InfoMetricsType, MetricsItem};

use super::{
  provider::{GarnetInfoMetrics, InfoProvider},
  snapshots::{DbSnapshot, GlobalMetricsSnapshot},
  tables::{AOF_ROWS, DEFAULT_HEX_ID, MB_UNITS, MEM_SOURCE_PAIRS, READ_CACHE_ROWS, STATS_ROWS},
};
use crate::GarnetServerMonitor;

impl GarnetInfoMetrics {
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
      // gc_* 四行恒 0：C# :113-114 经 GC.GetGCMemoryInfo 出运行时真值，该四件套
      // 系 .NET 托管 GC 专有观测量、无 rust 运行时对位物，行保留作格式兼容位；
      // 严禁按下行的 InfoProvider 语义混接第二真值源（登记锚 deviations.md §165a）。
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
      .map(|&(name, get)| get(name, snap))
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
    let mut items = Vec::with_capacity(23 + READ_CACHE_ROWS.len());
    items.extend([
      MetricsItem::from_i64("CurrentVersion", db.current_version),
      MetricsItem::from_i64("LastCheckpointedVersion", db.last_checkpointed_version),
      MetricsItem::new("SystemState", db.system_state.clone()),
      MetricsItem::from_i64("IndexBucketCount", db.index_bucket_count),
      MetricsItem::from_i64("IndexBucketSizeBytes", db.index_bucket_size_bytes),
      MetricsItem::from_i64("IndexMemorySizeBytes", db.index_memory_size_bytes),
      MetricsItem::from_i64("IndexOverflowBucketCount", db.index_overflow_bucket_count),
      MetricsItem::from_i64(
        "IndexOverflowMemorySizeBytes",
        db.index_overflow_memory_size_bytes,
      ),
      MetricsItem::from_i64(
        "IndexTotalMemorySizeBytes",
        db.index_total_memory_size_bytes,
      ),
      // rust 自研超集行（wbftree 升阶树页缓存总闸观测面，C#
      // GetDatabaseStoreStats 无对位行），登记锚 deviations.md §166c
      MetricsItem::from_i64("TreeCache.ReservedBytes", db.tree_cache_reserved_bytes),
      MetricsItem::from_i64("TreeCache.BudgetBytes", db.tree_cache_budget_bytes),
      MetricsItem::new("LogDir", provider.server_facts().log_dir),
      MetricsItem::from_i64("Log.PageSizeBytes", db.log_page_size_bytes),
      MetricsItem::from_i64("Log.MaxPageCount", db.log_max_allocated_page_count),
      MetricsItem::from_i64("Log.AllocatedPageCount", db.log_allocated_page_count),
      MetricsItem::from_i64("Log.MaxMemorySizeBytes", db.log_max_memory_size_bytes),
      MetricsItem::from_i64("Log.CurrentMemorySizeBytes", db.log_memory_size_bytes),
      MetricsItem::from_i64("Log.CurrentHeapSizeBytes", db.log_heap_size_bytes),
      MetricsItem::from_i64("Log.BeginAddress", db.log_begin_address),
      MetricsItem::from_i64("Log.HeadAddress", db.log_head_address),
      MetricsItem::from_i64("Log.SafeReadOnlyAddress", db.log_safe_readonly_address),
      MetricsItem::from_i64("Log.FlushedUntilAddress", db.log_flushed_until_address),
      MetricsItem::from_i64("Log.TailAddress", db.log_tail_address),
    ]);

    let rc = db.read_cache.as_ref();
    // 读缓存九项单次遍历出行（快照缺失恒 N/A，对齐 C# 逐项 null 合并口径）
    items.extend(
      READ_CACHE_ROWS
        .iter()
        .map(|&(name, get)| na_i64(name, rc.map(get))),
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
    let mut items = Vec::with_capacity(AOF_ROWS.len() + 2);
    items.extend(
      AOF_ROWS
        .iter()
        .map(|&(name, get)| na_i64(name, aof.map(get))),
    );
    items.push(na_i64(
      "SafeAofAddress",
      enabled.then(|| provider.safe_aof_address()),
    ));
    // 刷盘失败累计行尾项（r30-bgthread 发现五：常驻提交驱动致命故障信号，
    // 非零即可告警；C# cannedException 的运维可见面对位）——快照缺失以 0 呈现
    items.push(na_u64(
      "aof_flush_failures",
      enabled.then(|| db.aof.as_ref().map_or(0, |a| a.flush_failures)),
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

  /// 按段填充并取回该段指标（`GetRespInfo`/`GetMetric` 双面共用骨架）：
  /// 单段段直取自身，行表段按活跃库取行；PERSISTENCE 未启用 AOF 不填
  ///（对应 C# PopulatePersistenceInfo 前置短路），MODULES 恒空段。
  pub(super) fn populate_section(
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
}

/// 页大小（对齐 Environment.SystemPageSize 的运行时真值口径；与
/// windex/src/ram/direct_vm.rs 的 page_size::get() 探针同源）。
fn page_size() -> usize {
  page_size::get()
}

/// bool 观测面 → enabled/disabled 二值文本（server 段三处共用）。
const fn onoff(enabled: bool) -> &'static str {
  if enabled { "enabled" } else { "disabled" }
}

/// 数值行缺值 N/A 空合（针对 i64，避免中间 to_string 堆分配）。
#[inline]
fn na_i64(name: &'static str, value: Option<i64>) -> MetricsItem {
  match value {
    Some(v) => MetricsItem::from_i64(name, v),
    None => MetricsItem::new(name, "N/A"),
  }
}

/// 数值行缺值 N/A 空合（针对 u64，避免中间 to_string 堆分配）。
#[inline]
fn na_u64(name: &'static str, value: Option<u64>) -> MetricsItem {
  match value {
    Some(v) => MetricsItem::from_u64(name, v),
    None => MetricsItem::new(name, "N/A"),
  }
}
