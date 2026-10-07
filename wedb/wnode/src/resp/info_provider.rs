//! INFO 命令数据源（wmetric [`InfoProvider`] 的会话侧实现）
//!
//! C# INFO 数据面直读 storeWrapper / monitor / clusterProvider；rust 会话
//! 可达面为运行时配置 + 集群会话切面（ROLE 投影）+ 进程级静态量。存储域
//! 标量段（STORE / PERSISTENCE / MEMORY 的 store_* 项）经会话存储执行域
//! 句柄的 [`crate::resp::garnet_api::GarnetApiFace::store_snapshots`] 单一入口填充（wkv
//! `WedbStore::store_snapshot` 聚合 + `project_db_snapshot` 全仓唯一投影，
//! 见 garnet_api 模块）；持久化段随 EnableAOF=false 跳过——wmetric 段
//! 填充器的缺省形态，绝不虚报计数。扫描族段（KEYSPACE 逐库计数 /
//! HLOGSCAN 混合日志分布 / STOREHASHTABLE 哈希分布 / STOREREVIV 复活
//! 统计）数据面在存储域且须跨 await：凡渲染段集含扫描族段的 INFO 请求
//!（any 语义，混合段请求整请求降级，对标 C# 逐段实填），由会话分派门
//!（resp_server_session core.rs）放行存储漏斗统一挂起慢路径——非扫描面
//! 在漏斗调度点经 [`InfoSurface`] 同步快照（与快照尾参同渠道同口径），
//! 扫描行经 [`crate::resp::garnet_api::GarnetApiFace::exec_slow_info`] 异步产出，两路合成
//! [`InfoSlowSource`] 承接全段集渲染（慢臂无会话可达面）。

use std::{borrow::Cow, sync::OnceLock};

use wbase::{
  hex::generate_hex_id,
  supervise::{counter_snapshots, snapshots},
  time::now_stopwatch_ticks,
};
use wconf::ServerConfigType;
use windex::ram::NativeMemoryTracker;
use wmetric::{
  CommandStats, DbSnapshot, GarnetInfoMetrics, GarnetServerMonitor, GlobalMetricsSnapshot,
  InfoCommand, InfoProvider, ServerFacts,
};
use wresp::{
  catalog::commands_info,
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, write_error_raw},
  command::RespCommand,
  metrics::{InfoMetricsType, MetricsItem},
};

use super::resp_server_session::RespServerSession;
use crate::{
  cluster_provider::ClusterProviderHandle, servers::consumer_registry::ConsumerRegistry,
};

/// 扫描族慢段判别（单点定义，INFO 降级门与慢路径组合源共判据）：
/// KEYSPACE 全库扫描计数 / HLOGSCAN 混合日志分布扫描 / STOREHASHTABLE
/// 哈希分布诊断扫描 / STOREREVIV 复活统计转储——数据面在存储域且须跨
/// await，凡渲染段集含之即整请求降级（rust compio 异步存储域特有降级
/// 形态，C# 对位为段填充器内同步专用扫描会话）
pub(crate) const fn info_scan_section(t: InfoMetricsType) -> bool {
  matches!(
    t,
    InfoMetricsType::Keyspace
      | InfoMetricsType::HlogScan
      | InfoMetricsType::StoreHashtable
      | InfoMetricsType::StoreReviv
  )
}

/// 进程启动时刻的单调刻度（100ns 计时域，进程生命周期内 OnceLock 单点恒定；
/// 对位 C# StoreWrapper 的 startupTimestamp 字段，起表锚留 [`init_startup_ticks`] 一处）
///
/// [`doc(hidden)`] 测试专用读数面：INFO uptime 起点装配期预热的端到端直断言
///（tests/info_uptime_prewarm.rs，锚刻度 ≤ 装配完成刻度）；生产读取面为
/// [`SessionInfoSource::facts`] 的 `startup_stopwatch_ticks` 单点，非公共
/// API 契约
#[doc(hidden)]
pub fn startup_ticks() -> u64 {
  static START: OnceLock<u64> = OnceLock::new();
  *START.get_or_init(now_stopwatch_ticks)
}

/// 装配期预热起点（libs/server/StoreWrapper.cs:StoreWrapper 构造函数 :215 赋值
/// startupTimestamp 的对位：C# 起表在 InitializeServer 构造 StoreWrapper 时、
/// 先于 Start 的 RecoverAsync，uptime 天然计入检查点加载与 AOF 重放全程；
/// rust 首条 INFO 族命令才懒取会使恢复时长与启动静默期全部漏计，故由存储
/// 装配口在恢复前显式预热。OnceLock 幂等，重复调用无害）
pub(crate) fn init_startup_ticks() {
  let _ = startup_ticks();
}

/// 进程运行实例 id（C# runId：40 位十六进制，进程生命周期内恒定）
pub fn run_id() -> &'static str {
  static RUN_ID: OnceLock<String> = OnceLock::new();
  RUN_ID.get_or_init(generate_hex_id)
}

/// server_socket 行名（C# GarnetInfoMetrics.cs:413 `server_socket_{i}`；
/// rust 宿主全连接共享单池，恒 i = 0 一行）
const SERVER_SOCKET_ROW: &str = "server_socket_0";

/// 运行 ID 解析（对标 C# libs/server/StoreWrapper.cs:176 RunId => enableCluster ? clusterProvider.GetRunId() : runId）。
/// 集群态为提供方运行时串（Owned），单机态零拷贝借进程级常量（Borrowed）
#[inline]
pub(crate) fn resolve_run_id(
  enable_cluster: bool,
  cluster_provider: Option<&ClusterProviderHandle>,
) -> Cow<'static, str> {
  if enable_cluster && let Some(provider) = cluster_provider {
    let id = provider.get_run_id();
    if !id.is_empty() {
      return Cow::Owned(id);
    }
  }
  Cow::Borrowed(run_id())
}

/// 会话侧 INFO 数据源（按会话构造，读取共享面）
pub struct SessionInfoSource<'a> {
  session: &'a RespServerSession,
}

impl<'a> SessionInfoSource<'a> {
  pub fn new(session: &'a RespServerSession) -> Self {
    Self { session }
  }

  /// 服务器级事实（C# StoreWrapper / GarnetServerOptions 直读的会话可达投影）
  fn facts(&self) -> ServerFacts {
    let rc = &self.session.runtime_config;
    let enable_cluster = rc.resp_format(ServerConfigType::ClusterEnabled) == "yes"
      || rc.get_bool(ServerConfigType::ClusterEnabled);
    ServerFacts {
      // 恒定串三联零拷贝借静态（版本 / 协议版本常量 + 单机态进程级 run_id），
      // 集群态 run_id 为提供方运行时串（Owned 臂）
      version: Cow::Borrowed(env!("CARGO_PKG_VERSION")),
      run_id: resolve_run_id(enable_cluster, self.session.cluster_provider.as_ref()),
      redis_protocol_version: Cow::Borrowed(super::resp_server_session::REDIS_PROTOCOL_VERSION),
      enable_cluster,
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
      startup_stopwatch_ticks: startup_ticks(),
      log_dir: rc.resp_format(ServerConfigType::Logdir),
    }
  }
}

impl InfoProvider for SessionInfoSource<'_> {
  fn server_facts(&self) -> ServerFacts {
    self.facts()
  }

  /// 库快照（C# storeWrapper.GetDatabasesSnapshot 的会话侧单一入口：
  /// 转调存储执行域句柄的 [`crate::resp::garnet_api::GarnetApiFace::store_snapshots`]；未注入执行域
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
  /// 面，分支键为采样频率（GarnetInfoMetrics.cs:233 MetricsSamplingFrequency
  /// > 0）而非 commandstats 开关投影：周期采样在跑时 globalCommandStats 已含
  /// > history + 活跃会话的上一轮采样，直接取用；无周期采样（含缺省频率 0 的
  /// > commandstats 单开合法形态）时取 history 并遍历全部服务器的活跃消费者
  /// > 逐会话补并（GarnetInfoMetrics.cs:244-256）；监视器未装配时回落活跃
  /// > 会话——C# 中该组合态不存在（monitor == null 蕴含开关全关），rust 会话级
  /// > 开关独立于装配面，开关开启即如实上报可达计数）
  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    let monitor = GarnetServerMonitor::global();
    let mut aggregate = monitor.as_ref().and_then(|m| m.command_stats_aggregate());
    let merge_active = match &monitor {
      // 周期采样在跑：活跃会话镜像已并入 global 单源，无需补并
      Some(m) if m.sampling_frequency_secs() > 0 => false,
      // 无周期采样（commandstats 单开）：history + 活跃会话逐会话补并
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
        // 目录单源（对标 C# RespCommandsInfo.GetRespCommandName(cmd).ToLowerInvariant()，
        // 缺席/未知名 "unknown" 跳过）
        let name = commands_info::get_resp_command_name(cmd).to_ascii_lowercase();

        (name != "unknown").then_some((name, e.calls, e.rejected_calls, e.failed_calls))
      })
      .collect()
  }

  /// 键空间计数：分派门 any 语义下凡段集含扫描族段的 INFO 请求整请求降级
  /// 慢路径（resp_server_session core.rs 的 INFO 分派门），同步渲染面无
  /// 可达调用点（DEFAULT/ALL 段集合亦不含 KEYSPACE）；本臂按 trait 契约
  /// 保留形态恒回 (0, 0)（populate_keyspace_info 对零键库跳行不渲染，
  /// 绝不虚报计数），慢路径由组合源 [`InfoSlowSource`] 交扫描快照查表
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
  /// `Vec<MetricsItem>` 到 `Vec<(String, String)>` 投影，与 trait 契约一致）
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

  /// server_socket 池统计行（C# GarnetInfoMetrics.cs:413 逐 TCP server 出
  /// `server_socket_{i}` 行的对位）：rust 宿主全连接共享单池——net/handler/
  /// mod.rs:112-115 处理器构造期即持宿主 GarnetServer 的共享池，全连接同池
  /// 等价覆盖全部监听器，恒出一行 `server_socket_0` 覆盖 C# 逐 server 枚举
  /// 语义，不造按监听器假枚举；池不可得的裸会话走 trait 缺省空表，与 C#
  /// 无 server 态同
  fn server_socket_buffer_pool_stats(&self) -> Vec<(String, String)> {
    self
      .session
      .listener_buffer_pool
      .as_ref()
      .map(|pool| vec![(SERVER_SOCKET_ROW.to_string(), pool.get_stats())])
      .unwrap_or_default()
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

  /// 混合日志内存分布转储：与 [`Self::keyspace_stats`] 同款降级形态——
  /// 凡段集含 HLOGSCAN 的请求已由分派门整请求降级慢路径，同步渲染面无
  /// 可达调用点，本臂按契约保留恒回空表（绝不虚报），慢路径由组合源
  /// [`InfoSlowSource`] 交扫描快照承接
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

  /// 后台任务健康快照（r30-bgthread 发现五：wbase::supervise 监督名单 + 登记
  /// 计数，server 段 `bg_task_health` 一行出——对标 C# TaskManager 注册表
  /// IsRunning/IsCompleted 的运维三面，panic 死亡与空闲不可区分的观测缺口补齐）
  fn bg_task_health(&self) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for s in snapshots() {
      let value = if s.alive {
        "alive".to_string()
      } else {
        format!("dead(panic={})", s.panics)
      };
      rows.push((s.name.to_string(), value));
    }
    for c in counter_snapshots() {
      rows.push((c.name.to_string(), c.value.to_string()));
    }
    rows
  }

  /// 原生分配器记账字节数（C# GarnetInfoMetrics.cs:143 ← Tsavorite
  /// NativeMemoryTracker.Bytes 的单点接线：直读 windex::ram::
  /// NativeMemoryTracker 进程级全局总账，wmetric 不新增 windex 依赖，
  /// 依赖分层不动，无第二记账面）
  fn native_allocator_bytes(&self) -> i64 {
    NativeMemoryTracker::bytes() as i64
  }
}

/// 慢路径 INFO 扫描产物（KEYSPACE 逐库键空间计数 + HLOGSCAN 混合日志
/// 分布转储 + STOREHASHTABLE 哈希分布转储 + STOREREVIV 复活统计转储；
/// C# PopulateKeyspaceInfo 的逐库 GetKeyspaceStats、PopulateHlogScanInfo
/// 的 storeWrapper.HybridLogDistributionScan、PopulateStoreHashDistribution
/// 的 db.Store.DumpDistribution 与 PopulateStoreRevivInfo 的
/// DumpRevivificationStats 扫描承接：分派门降级（any 语义）的慢路径异步
/// 产出，仅所请求扫描段有行，其余字段为空，绝不虚报计数）
#[derive(Default, Clone)]
pub struct InfoScanResult {
  /// 有键库的 (id, 活键数, 带 TTL 键数)；空库已在扫描侧按 C#
  /// 「仅列出至少持有一个键的库」口径剔除
  pub keyspace: Vec<(i32, u64, u64)>,
  /// 每库混合日志分布转储文本（下标 = 库 id；wedb 单物理日志，
  /// 统计由 db 0 形态呈现，其余库无独立物理日志不产生条目）
  pub hlog_dump: Vec<String>,
  /// 每库哈希索引分布转储文本（STOREHASHTABLE；wedb 单物理存储，db 0
  /// 形态呈现；db0 无转储时该段不产生条目）
  pub hash_dump: Vec<String>,
  /// 每库复活回收统计转储文本（STOREREVIV；db 0 形态呈现）
  pub reviv_dump: Vec<String>,
  /// 存储域扫描失败标记（KEYSPACE/HLOGSCAN 扫描 Err 置位；渲染臂据此改出
  /// 慢路径存储错误帧，客户端可区分空日志与存储故障，不静默降级空转储）
  pub storage_failed: bool,
}

/// 慢路径 INFO 非扫描面调度点同步快照 ([`crate::resp::garnet_api::GarnetApiFace::exec_slow_info`]
/// 的类型化调度参数，与命令名 / resp_version / 快照尾参同渠道同口径：
/// 慢臂无会话可达面，会话可达事实须在调度点一次性取齐搬入 future）。
///
/// needs 映射按请求段集只取所触达臂（C# 逐段实填的 rust 异步对位降级
/// 形态）：未触达臂保持缺省空形态，对应段本次不渲染，绝不虚报；
/// `server_facts` 恒取（多段共读且为纯读取投影，零成本）
pub struct InfoSurface {
  /// 服务器级事实（调度点经 [`SessionInfoSource`] 投影，恒取）
  facts: ServerFacts,
  /// 库快照（ServerFacts 之外的物理事实行，needs：MEMORY / STORE /
  /// PERSISTENCE / STOREHASHTABLE / STOREREVIV）
  pub databases: Vec<DbSnapshot>,
  /// 全局指标快照（needs：STATS / CLIENTS）
  global_metrics: Option<GlobalMetricsSnapshot>,
  /// 聚合命令统计（needs：COMMANDSTATS）
  command_stats: Vec<(String, u64, u64, u64)>,
  /// 复制信息段（needs：REPLICATION）
  replication_info: Option<Vec<MetricsItem>>,
  /// gossip 统计段（needs：STATS 且 enable_cluster；metrics_disabled 入参
  /// 与 wmetric populate_stats_info 同源判定在调度点预解析后快照）
  gossip_stats: Vec<MetricsItem>,
  /// 集群缓冲池统计（needs：BPSTATS）
  buffer_pool_stats: Vec<(String, String)>,
  /// server_socket 池统计（needs：BPSTATS）
  server_socket_buffer_pool_stats: Vec<(String, String)>,
  /// 集群 checkpoint 信息段（needs：CINFO）
  checkpoint_info: Option<Vec<MetricsItem>>,
  /// 主侧安全 AOF 地址（needs：PERSISTENCE）
  safe_aof_address: i64,
  /// 后台任务健康快照（needs：SERVER）
  bg_task_health: Vec<(String, String)>,
  /// 原生分配器记账（needs：MEMORY）
  native_allocator_bytes: i64,
}

/// 段集命中则取该消费面快照（`f` 仅在命中时求值），否则落类型缺省空形态
fn grab<T: Default>(needed: bool, f: impl FnOnce() -> T) -> T {
  needed.then(f).unwrap_or_default()
}

impl InfoSurface {
  /// 调度点快照装配（段集已由分派门判为「含扫描族段」的非 RESET/HELP/
  /// 非法形态，段名解析单点复用 wmetric [`InfoCommand::parse_sections`]）
  pub fn capture(source: &SessionInfoSource<'_>, sections: &[InfoMetricsType]) -> Self {
    let has = |t: InfoMetricsType| sections.contains(&t);
    let has_any = |ts: &[InfoMetricsType]| ts.iter().any(|&t| has(t));
    let facts = source.server_facts();
    let needs_gossip = has(InfoMetricsType::Stats) && facts.enable_cluster;
    // 段集命中即取该消费面，未触达落类型缺省空形态（绝不虚报）
    Self {
      databases: grab(
        has_any(&[
          InfoMetricsType::Memory,
          InfoMetricsType::Store,
          InfoMetricsType::Persistence,
          InfoMetricsType::StoreHashtable,
          InfoMetricsType::StoreReviv,
        ]),
        || source.databases(),
      ),
      global_metrics: grab(
        has_any(&[InfoMetricsType::Stats, InfoMetricsType::Clients]),
        || source.global_metrics(),
      ),
      command_stats: grab(has(InfoMetricsType::CommandStats), || {
        source.command_stats()
      }),
      replication_info: grab(has(InfoMetricsType::Replication), || {
        source.replication_info()
      }),
      gossip_stats: grab(needs_gossip, || {
        // metrics_disabled 判定与 wmetric garnet_info_metrics.rs
        // populate_stats_info 的 C# 局部同源式逐字一致（storeWrapper.monitor
        // == null 投影）
        let metrics_disabled = GarnetServerMonitor::global()
          .and_then(|m| m.snapshot())
          .is_none();
        source.gossip_stats(metrics_disabled)
      }),
      buffer_pool_stats: grab(has(InfoMetricsType::BpStats), || source.buffer_pool_stats()),
      server_socket_buffer_pool_stats: grab(has(InfoMetricsType::BpStats), || {
        source.server_socket_buffer_pool_stats()
      }),
      checkpoint_info: grab(has(InfoMetricsType::CInfo), || source.checkpoint_info()),
      safe_aof_address: grab(has(InfoMetricsType::Persistence), || {
        source.safe_aof_address()
      }),
      bg_task_health: grab(has(InfoMetricsType::Server), || source.bg_task_health()),
      native_allocator_bytes: grab(has(InfoMetricsType::Memory), || {
        source.native_allocator_bytes()
      }),
      facts,
    }
  }
}

/// 慢路径 INFO 组合数据源（非扫描面 [`InfoSurface`] 调度点快照 + 扫描面
/// [`InfoScanResult`] 异步产物的合成，承接降级请求的全段集渲染，对标 C#
/// InfoCommand.cs:NetworkINFO → GarnetInfoMetrics 单次 GetRespInfo 逐段
/// 实填形态——rust 异步存储域拆两路取数、单点合成）
pub struct InfoSlowSource {
  surface: InfoSurface,
  scan: InfoScanResult,
}

impl InfoSlowSource {
  pub fn new(surface: InfoSurface, scan: InfoScanResult) -> Self {
    Self { surface, scan }
  }
}

impl InfoProvider for InfoSlowSource {
  fn server_facts(&self) -> ServerFacts {
    self.surface.facts.clone()
  }

  /// 行表合成（两分支单点）：
  /// - 面快照无物理行（本次请求的会话面未含库消费段，或执行域无库快照）：
  ///   纯扫描形态，与既有扫描臂字节一致——行表 0..=扫描最大库号（纯转储
  ///   段请求出 db 0 一行），哈希 / 复活转储取物理首行统一覆盖各行；
  /// - 面快照含物理行：合并形态——物理事实行（STORE / MEMORY / PERSISTENCE
  ///   消费）保留，哈希 / 复活转储字段按扫描产物覆盖，扫描发现而快照缺席
  ///   的虚库号以 [`DbSnapshot::virtual_db`] 标记行补齐（仅供 KEYSACE 枚举
  ///   与行表长度，wmetric 物理事实消费点对虚库行跳过，最小扫描事实
  ///   严禁外溢非扫描段），按库号升序单源收口（对标 C#
  ///   PopulateKeyspaceInfo 的 OrderBy(db.Id)）
  fn databases(&self) -> Vec<DbSnapshot> {
    let hash = self.scan.hash_dump.first().cloned().unwrap_or_default();
    let reviv = self.scan.reviv_dump.first().cloned().unwrap_or_default();
    let scan_max = self.scan.keyspace.iter().map(|&(id, ..)| id).max();
    if self.surface.databases.is_empty() {
      let max_db = scan_max.unwrap_or(0);
      return (0..=max_db)
        .map(|id| DbSnapshot {
          id,
          hash_distribution_dump: hash.clone(),
          revivification_dump: reviv.clone(),
          // 无物理事实持有者的纯扫描行同款标记（物理事实消费点跳之，
          // 与合并形态虚库行同一口径；转储段与 KEYSACE 消费不受影响）
          virtual_db: true,
          ..DbSnapshot::default()
        })
        .collect();
    }
    let mut rows = self.surface.databases.clone();
    for row in &mut rows {
      row.hash_distribution_dump = hash.clone();
      row.revivification_dump = reviv.clone();
    }
    let max_db = scan_max
      .into_iter()
      .chain(rows.iter().map(|d| d.id))
      .max()
      .unwrap_or(0);
    for id in 0..=max_db {
      if !rows.iter().any(|row| row.id == id) {
        rows.push(DbSnapshot {
          id,
          hash_distribution_dump: hash.clone(),
          revivification_dump: reviv.clone(),
          virtual_db: true,
          ..DbSnapshot::default()
        });
      }
    }
    rows.sort_by_key(|row| row.id);
    rows
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    self.surface.global_metrics
  }

  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    self.surface.command_stats.clone()
  }

  /// 扫描快照查表（C# GetKeyspaceStats 返回值投影）
  fn keyspace_stats(&self, db_id: i32) -> (u64, u64) {
    self
      .scan
      .keyspace
      .iter()
      .find(|&&(id, ..)| id == db_id)
      .map_or((0, 0), |&(_, keys, expires)| (keys, expires))
  }

  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    self.surface.replication_info.clone()
  }

  fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    self.surface.gossip_stats.clone()
  }

  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    self.surface.buffer_pool_stats.clone()
  }

  fn server_socket_buffer_pool_stats(&self) -> Vec<(String, String)> {
    self.surface.server_socket_buffer_pool_stats.clone()
  }

  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
    self.surface.checkpoint_info.clone()
  }

  /// 混合日志分布转储查表（C# HybridLogDistributionScan 返回值投影：
  /// main store dump，对象存储槽恒空——wedb 单物理日志无对象存储域）
  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    self
      .scan
      .hlog_dump
      .iter()
      .map(|dump| (dump.clone(), String::new()))
      .collect()
  }

  fn safe_aof_address(&self) -> i64 {
    self.surface.safe_aof_address
  }

  fn bg_task_health(&self) -> Vec<(String, String)> {
    self.surface.bg_task_health.clone()
  }

  fn native_allocator_bytes(&self) -> i64 {
    self.surface.native_allocator_bytes
  }
}

/// 慢路径 INFO 应答渲染单点（组合数据源装配 + wmetric 出帧单源
/// [`InfoCommand::write_info_reply`]——同步面与慢路径共用同一帧型与段序
/// 逻辑；扫描失败改出慢路径存储错误帧，与旧扫描臂同款不静默降级）
pub(crate) fn render_info_slow_reply(
  sections: &[InfoMetricsType],
  scan: &InfoScanResult,
  surface: InfoSurface,
  active_db: i32,
  resp_version: u8,
) -> Vec<u8> {
  let mut output = Vec::new();
  if scan.storage_failed {
    write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
    return output;
  }
  let provider = InfoSlowSource::new(surface, scan.clone());
  let mut info = GarnetInfoMetrics::new();
  InfoCommand::write_info_reply(
    sections,
    active_db,
    &provider,
    &mut info,
    resp_version,
    &mut output,
  );
  output
}
