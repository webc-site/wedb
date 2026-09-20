//! INFO 命令数据源（wmetric [`InfoProvider`] 的会话侧实现）
//!
//! C# INFO 数据面直读 storeWrapper / monitor / clusterProvider；rust 会话
//! 可达面为运行时配置 + 集群会话切面（ROLE 投影）+ 进程级静态量。存储域
//! 标量段（STORE / PERSISTENCE / MEMORY 的 store_* 项）经会话存储执行域
//! 句柄的 [`GarnetApiFace::store_snapshots`] 单一入口填充（wkv
//! `WedbStore::store_snapshot` 聚合 + `project_db_snapshot` 全仓唯一投影，
//! 见 garnet_api 模块）；持久化段随 EnableAOF=false 跳过——wmetric 段
//! 填充器的缺省形态，绝不虚报计数。扫描族段（KEYSPACE 逐库计数 /
//! HLOGSCAN 混合日志分布 / STOREHASHTABLE 哈希分布 / STOREREVIV 复活
//! 统计）经慢路径扫描通道承接（显式 `INFO keyspace [hlogscan]
//! [storehashtable] [storereviv]` 请求降级 exec_slow，见 resp_server_session
//! 的 INFO 分派与 garnet_api 的 exec_slow Info 臂）。

use std::sync::OnceLock;

use wbase::time::now_ms;
use wconf::ServerConfigType;
use wmetric::{
  CommandStats, DbSnapshot, GarnetServerMonitor, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
  info::garnet_info_metrics::generate_default_hex_id,
};
use wresp::{command::RespCommand, metrics::MetricsItem};

use super::{
  resp_commands_info_data::resp_command_to_cs_name, resp_server_session::RespServerSession,
};
use crate::servers::consumer_registry::ConsumerRegistry;

/// 进程启动时刻（Unix 秒；C# StoreWrapper.ProcessStartTime 的进程生命周期代理）
fn startup_unix_secs() -> i64 {
  static START: OnceLock<i64> = OnceLock::new();
  *START.get_or_init(|| now_ms() as i64 / 1000)
}

/// 进程运行实例 id（C# runId：40 位十六进制，进程生命周期内恒定）
fn run_id() -> &'static str {
  static RUN_ID: OnceLock<String> = OnceLock::new();
  RUN_ID.get_or_init(generate_default_hex_id)
}

/// 会话侧 INFO 数据源（按会话构造，读取共享面）
pub(crate) struct SessionInfoSource<'a> {
  session: &'a RespServerSession,
}

impl<'a> SessionInfoSource<'a> {
  pub(crate) fn new(session: &'a RespServerSession) -> Self {
    Self { session }
  }

  /// 服务器级事实（C# StoreWrapper / GarnetServerOptions 直读的会话可达投影）
  fn facts(&self) -> ServerFacts {
    let rc = &self.session.runtime_config;
    ServerFacts {
      version: env!("CARGO_PKG_VERSION").to_string(),
      run_id: run_id().to_string(),
      redis_protocol_version: super::resp_server_session::REDIS_PROTOCOL_VERSION.to_string(),
      enable_cluster: rc.get_bool(ServerConfigType::ClusterEnabled),
      // AppendOnly 为只读投影项（无运行时槽位，get_bool 恒读空槽）：经 resp_format
      // 走只读格式化直读启动选项，与 CONFIG GET 同一机制同一真源（对应 C#
      // GarnetInfoMetrics.cs:84 直读 storeWrapper.serverOptions.EnableAOF）
      enable_aof: rc.resp_format(ServerConfigType::AppendOnly) == "yes",
      // 采样频率与 LogDir 均为配置真实值（C# GarnetInfoMetrics.cs:65-66/:309
      // 直读 serverOptions.MetricsSamplingFrequency / LogDir 的对位）：
      // monitor_freq 经进程级监视器取构造同源注入的频率秒数（监视器未装配
      // 的裸会话形态回 0，monitor_task 门保持 >0 判定）；LogDir 经
      // resp_format 只读回落直读启动选项（与 CONFIG GET logdir 同源）
      metrics_sampling_frequency: GarnetServerMonitor::global()
        .map(|m| m.sampling_frequency_secs().min(i32::MAX as u64) as i32)
        .unwrap_or(0),
      latency_monitor: self.session.latency_metrics.is_some(),
      command_stats_monitor: self.session.command_stats.is_some(),
      startup_timestamp_unix_secs: startup_unix_secs(),
      log_dir: rc.resp_format(ServerConfigType::Logdir),
    }
  }
}

impl InfoProvider for SessionInfoSource<'_> {
  fn server_facts(&self) -> ServerFacts {
    self.facts()
  }

  /// 库快照（C# storeWrapper.GetDatabasesSnapshot 的会话侧单一入口：
  /// 转调存储执行域句柄的 [`GarnetApiFace::store_snapshots`]；未注入执行域
  /// 的裸会话形态回空集，MEMORY/STORE 段按缺省形态呈现，绝不虚报计数）
  fn databases(&self) -> Vec<DbSnapshot> {
    self
      .session
      .garnet_api
      .as_ref()
      .map_or_else(Vec::new, |api| api.store_snapshots())
  }

  /// 全局指标快照（优先读取全局监视器快照；若未安装全局监视器，则回退读取会话级指标）
  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    GarnetServerMonitor::global()
      .and_then(|m| m.snapshot())
      .or_else(|| {
        self
          .session
          .session_metrics
          .as_ref()
          .map(|sm| GlobalMetricsSnapshot {
            global_session_metrics: sm.snapshot(),
            ..Default::default()
          })
      })
  }

  /// 聚合命令统计（C# GarnetInfoMetrics.cs:PopulateCommandStatsInfo 的聚合
  /// 面：周期采样开启时 globalCommandStats 已含 history + 活跃会话的上一轮
  /// 采样，直接取用；仅命令统计开启（无周期采样）时取 history 并遍历全部
  /// 服务器的活跃消费者逐会话补并（GarnetInfoMetrics.cs:248-257）；监视器
  /// 未装配时回落活跃会话——C# 中该组合态不存在（monitor == null 蕴含开关
  /// 全关），rust 会话级开关独立于装配面，开关开启即如实上报可达计数）
  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    let monitor = GarnetServerMonitor::global();
    let mut aggregate = monitor.as_ref().and_then(|m| m.command_stats_aggregate());
    let merge_active = match &monitor {
      // 周期采样开启：活跃会话镜像已并入 global，无需补并
      Some(m) if m.tracks_command_stats() => false,
      // 仅命令统计开启：补并活跃会话
      Some(_) => true,
      // 监视器未装配：回落活跃会话
      None => true,
    };
    if merge_active {
      match ConsumerRegistry::global() {
        Some(registry) => {
          // 遍历活跃消费者逐会话补并（条目挂接的即会话共享句柄，本会话
          // 亦在其中；C# GarnetInfoMetrics.cs:252-256 遍历 ActiveConsumers）
          for entry in registry.active_consumers() {
            if let Some(stats) = entry.command_stats_snapshot() {
              aggregate.get_or_insert_with(CommandStats::new).add(&stats);
            }
          }
        }
        // 注册表未装配（裸会话形态）：仅本会话可达
        None => {
          if let Some(stats) = &self.session.command_stats {
            aggregate
              .get_or_insert_with(CommandStats::new)
              .add(&stats.lock());
          }
        }
      }
    }
    let Some(aggregate) = aggregate else {
      return Vec::new();
    };

    // C# 逐命令输出路径：零计数跳过（calls 与 rejected 均零），
    // RespCommandsInfo.GetRespCommandName 小写化，"unknown" 跳过
    aggregate
      .entries
      .iter()
      .enumerate()
      .filter(|(_, e)| e.calls > 0 || e.rejected_calls > 0)
      .filter_map(|(idx, e)| {
        let cmd = RespCommand::from_repr(idx as u16)?;
        let name = resp_command_to_cs_name(cmd).to_lowercase();
        (name != "unknown").then_some((name, e.calls, e.rejected_calls, e.failed_calls))
      })
      .collect()
  }

  /// 键空间计数：仅显式 `INFO KEYSPACE` 触达，走慢路径扫描通道
  ///（exec_slow Info 分支逐库扫描，段文本由慢路径数据源填充），
  /// 同步面不触达本方法
  fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
    (0, 0)
  }

  /// 复制信息段（单一转调集群提供方唯一实现；None = 会话无集群提供方，
  /// 与 C# clusterProvider == null 同构，由 wmetric 出九字段占位分支）
  ///
  /// 本方法只是会话侧适配位：C# GarnetInfoMetrics.cs 的 PopulateReplicationInfo 一枚
  /// 1:1 挂载在 `wmetric::info::garnet_info_metrics::GarnetInfoMetrics::populate_replication_info`，
  /// 此处不复挂；下沉的集群提供方形态即下方锚点
  /// → libs/cluster/Server/ClusterProvider.cs:GetReplicationInfo
  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    let provider = self.session.cluster_provider.as_ref()?;
    Some(provider.get_replication_info())
  }

  /// gossip 统计段（转调集群提供方，透传 metrics_disabled 形参）
  ///
  /// 本方法只是会话侧适配位：C# ClusterProvider.cs 的 GetGossipStats 一枚 1:1 挂载在
  /// `wedb::server::cluster_provider::ClusterProvider::get_gossip_stats`，此处不复挂
  fn gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    self
      .session
      .cluster_provider
      .as_ref()
      .map_or_else(Vec::new, |p| p.get_gossip_stats(metrics_disabled))
  }

  /// 缓冲池统计（转调集群提供方，字段名知识只留集群层；此处做一次
  /// Vec<MetricsItem> 到 Vec<(String, String)> 投影，与 trait 契约一致）
  ///
  /// 在 garnet 中的相对路径:libs/cluster/Server/ClusterProvider.cs:GetBufferPoolStats
  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    self
      .session
      .cluster_provider
      .as_ref()
      .map_or_else(Vec::new, |p| {
        p.get_buffer_pool_stats()
          .into_iter()
          .map(|item| (item.name.into_owned(), item.value))
          .collect()
      })
  }

  /// 集群 checkpoint 信息段（转调集群提供方；None = 会话无集群提供方，
  /// 与 C# clusterProvider?.GetCheckpointInfo 同构）
  ///
  /// 本方法只是会话侧适配位：C# ClusterProvider.cs 的 GetCheckpointInfo 一枚 1:1 挂载在
  /// `wedb::server::cluster_provider::ClusterProvider::get_checkpoint_info`，此处不复挂
  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
    Some(
      self
        .session
        .cluster_provider
        .as_ref()?
        .get_checkpoint_info(),
    )
  }

  /// 混合日志内存分布转储（同步面不触达：HLOGSCAN 段需跨 await 的存储域
  /// 扫描，纯显式慢段请求经 INFO 慢路径数据源 [`InfoSlowScanSource`] 承接；
  /// 混合段请求按 wmetric 缺省形态呈现「Empty」，绝不虚报）
  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  /// 主侧安全 AOF 地址（集群切面主视角复制位点投影）
  fn safe_aof_address(&self) -> i64 {
    self
      .session
      .cluster_provider
      .as_ref()
      .filter(|p| p.is_primary())
      .map(|p| p.get_primary_info().0.get(0).unwrap_or(0))
      .unwrap_or(0)
  }

  /// 订阅邮箱溢出丢弃数聚合（rust 域扩展指标；活跃消费者条目镜像求和，
  /// 注册表未装配回 None——对齐 C# 非 Garnet 服务器「cannot be listed」
  /// 形态的省略口径。镜像为泵逐批刷新，最终一致）
  fn pubsub_dropped(&self) -> Option<u64> {
    Some(
      ConsumerRegistry::global()?
        .active_consumers()
        .iter()
        .map(|entry| entry.pubsub_dropped())
        .sum(),
    )
  }
}

/// 慢路径 INFO 扫描数据源（KEYSPACE 逐库键空间计数 + HLOGSCAN 混合日志
/// 分布转储 + STOREHASHTABLE 哈希分布转储 + STOREREVIV 复活统计转储；
/// C# PopulateKeyspaceInfo 的逐库 GetKeyspaceStats、PopulateHlogScanInfo
/// 的 storeWrapper.HybridLogDistributionScan、PopulateStoreHashDistribution
/// 的 db.Store.DumpDistribution 与 PopulateStoreRevivInfo 的
/// DumpRevivificationStats 扫描承接：显式 `INFO keyspace [hlogscan]
/// [storehashtable] [storereviv]` 慢路径消费，仅触达所请求段，其余成员为
/// trait 缺省形态，绝不虚报计数）
pub(crate) struct InfoSlowScanSource {
  /// 有键库的 (id, 活键数, 带 TTL 键数)；空库已在扫描侧按 C#
  /// 「仅列出至少持有一个键的库」口径剔除
  keyspace: Vec<(i32, u64, u64)>,
  /// 每库混合日志分布转储文本（下标 = 库 id；wedb 单物理日志，
  /// 统计由 db 0 形态呈现，其余库无独立物理日志不产生条目）
  hlog_dump: Vec<String>,
  /// 每库哈希索引分布转储文本（STOREHASHTABLE；wedb 单物理存储，db 0
  /// 形态呈现；db0 无转储时该段不产生条目）
  hash_dump: Vec<String>,
  /// 每库复活回收统计转储文本（STOREREVIV；db 0 形态呈现）
  reviv_dump: Vec<String>,
}

impl InfoSlowScanSource {
  pub(crate) fn new(
    keyspace: Vec<(i32, u64, u64)>,
    hlog_dump: Vec<String>,
    hash_dump: Vec<String>,
    reviv_dump: Vec<String>,
  ) -> Self {
    Self {
      keyspace,
      hlog_dump,
      hash_dump,
      reviv_dump,
    }
  }
}

impl InfoProvider for InfoSlowScanSource {
  fn server_facts(&self) -> ServerFacts {
    // 仅 KEYSPACE/HLOGSCAN 段消费本数据源，facts 不被任何段读取，最小值即可
    ServerFacts {
      version: env!("CARGO_PKG_VERSION").to_string(),
      run_id: run_id().to_string(),
      redis_protocol_version: super::resp_server_session::REDIS_PROTOCOL_VERSION.to_string(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 0,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_timestamp_unix_secs: startup_unix_secs(),
      log_dir: String::new(),
    }
  }

  /// 库快照：KEYSPACE 段仅消费库 id（DbSnapshot 存储标量字段属 STORE 段，
  /// 本数据源段集合不含 STORE，不填充——标量组装点全仓唯一在
  /// garnet_api 的 `project_db_snapshot`）；STOREHASHTABLE / STOREREVIV
  /// 段消费同名转储文本字段（db 0 形态）
  fn databases(&self) -> Vec<DbSnapshot> {
    // 行表上界 = 有键库最大 id 与 0 取大（纯转储段请求时 keyspace 为空，
    // 行表按 db 0 一行呈现）
    let max_db = self.keyspace.iter().map(|&(id, ..)| id).max().unwrap_or(0);
    (0..=max_db)
      .map(|id| {
        let idx = id as usize;
        DbSnapshot {
          id,
          hash_distribution_dump: self.hash_dump.get(idx).cloned().unwrap_or_default(),
          revivification_dump: self.reviv_dump.get(idx).cloned().unwrap_or_default(),
          ..DbSnapshot::default()
        }
      })
      .collect()
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    None
  }

  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    Vec::new()
  }

  /// 扫描快照查表（C# GetKeyspaceStats 返回值投影）
  fn keyspace_stats(&self, db_id: i32) -> (u64, u64) {
    self
      .keyspace
      .iter()
      .find(|&&(id, ..)| id == db_id)
      .map_or((0, 0), |&(_, keys, expires)| (keys, expires))
  }

  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    Vec::new()
  }

  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  /// 混合日志分布转储查表（C# HybridLogDistributionScan 返回值投影：
  /// main store dump，对象存储槽恒空——wedb 单物理日志无对象存储域）
  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    self
      .hlog_dump
      .iter()
      .map(|dump| (dump.clone(), String::new()))
      .collect()
  }

  fn safe_aof_address(&self) -> i64 {
    0
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::cluster_provider::ClusterProvider;

  /// 返回哨兵字段的集群提供方，用于验证 SessionInfoSource 四臂纯转调
  struct FwdProvider;

  impl ClusterProvider for FwdProvider {
    fn get_replication_info(&self) -> Vec<MetricsItem> {
      vec![MetricsItem::new("sentinel_repl", "RID")]
    }
    fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
      vec![MetricsItem::new(
        "sentinel_gossip",
        metrics_disabled.to_string(),
      )]
    }
    fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
      vec![MetricsItem::new("sentinel_bp", "BPOOL")]
    }
    fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
      vec![MetricsItem::new("sentinel_ckpt", "CKPT")]
    }
  }

  fn session_with_cluster() -> RespServerSession {
    let mut session = RespServerSession::default();
    session.attach_cluster_provider(Arc::new(FwdProvider));
    session
  }

  #[test]
  fn replication_forwards_to_provider() {
    let session = session_with_cluster();
    let src = SessionInfoSource::new(&session);
    let items = src.replication_info().expect("集群态应有复制段");
    assert!(
      items
        .iter()
        .any(|i| i.name.as_ref() == "sentinel_repl" && i.value == "RID")
    );
  }

  #[test]
  fn gossip_threads_metrics_disabled() {
    let session = session_with_cluster();
    let src = SessionInfoSource::new(&session);
    assert!(src.gossip_stats(true).iter().any(|i| i.value == "true"));
    assert!(src.gossip_stats(false).iter().any(|i| i.value == "false"));
  }

  #[test]
  fn buffer_pool_projects_metrics_item_to_tuples() {
    let session = session_with_cluster();
    let src = SessionInfoSource::new(&session);
    assert_eq!(
      src.buffer_pool_stats(),
      vec![("sentinel_bp".to_string(), "BPOOL".to_string())]
    );
  }

  #[test]
  fn checkpoint_forwards_to_provider() {
    let session = session_with_cluster();
    let src = SessionInfoSource::new(&session);
    let ck = src.checkpoint_info().expect("集群态应有 checkpoint 段");
    assert!(
      ck.iter()
        .any(|i| i.name.as_ref() == "sentinel_ckpt" && i.value == "CKPT")
    );
  }

  #[test]
  fn standalone_session_has_no_cluster_facet() {
    let session = RespServerSession::default();
    let src = SessionInfoSource::new(&session);
    assert!(src.replication_info().is_none());
    assert!(src.checkpoint_info().is_none());
    assert!(src.gossip_stats(false).is_empty());
    assert!(src.buffer_pool_stats().is_empty());
  }

  /// 会话侧回归纯转调后，集群复制字段名字面量一处定义只留在集群层
  #[test]
  fn wnode_production_has_no_handbuilt_replication_literals() {
    let src = include_str!("info_provider.rs");
    let production = src.split("#[cfg(test)]").next().unwrap_or(src);
    for name in [
      "master_replid",
      "second_repl_offset",
      "sync_driver_count",
      "master_failover_state",
    ] {
      assert!(
        !production.contains(name),
        "会话侧不应再手工拼装复制字段字面量 {name}"
      );
    }
    // 四臂转调句柄同名方法，生产调用点存在
    for fwd in [
      "get_replication_info",
      "get_gossip_stats",
      "get_buffer_pool_stats",
      "get_checkpoint_info",
    ] {
      assert!(production.contains(fwd), "会话侧应转调 {fwd}");
    }
  }
}
