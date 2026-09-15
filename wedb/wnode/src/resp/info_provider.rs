//! INFO 命令数据源（wmetric [`InfoProvider`] 的会话侧实现）
//!
//! C# INFO 数据面直读 storeWrapper / monitor / clusterProvider；rust 会话
//! 可达面为运行时配置 + 集群会话切面（ROLE 投影）+ 进程级静态量。存储域
//! 段（STORE / PERSISTENCE / KEYSPACE）需存储域快照通道，当前返回空集：
//! 键空间按「空库跳过」口径不出段、持久化段随 EnableAOF=false 跳过——
//! 均为 wmetric 段填充器的缺省形态，绝不虚报计数。

use std::sync::OnceLock;

use wbase::time::now_ms;
use wconf::ServerConfigType;
use wmetric::{
  DbSnapshot, GarnetServerMonitor, GlobalMetricsSnapshot, InfoProvider, MetricsItem, ServerFacts,
  info::garnet_info_metrics::generate_default_hex_id,
};

use super::resp_server_session::RespServerSession;

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
      enable_aof: rc.get_bool(ServerConfigType::AppendOnly),
      metrics_sampling_frequency: if self.session.session_metrics.is_some() {
        1
      } else {
        0
      },
      latency_monitor: self.session.get_latency_metrics().is_some(),
      command_stats_monitor: false,
      startup_timestamp_unix_secs: startup_unix_secs(),
      log_dir: String::new(),
    }
  }
}

impl InfoProvider for SessionInfoSource<'_> {
  fn server_facts(&self) -> ServerFacts {
    self.facts()
  }

  /// 库快照（需存储域快照通道，空集：MEMORY/STORE/KEYSPACE 段按缺省形态呈现）
  fn databases(&self) -> Vec<DbSnapshot> {
    Vec::new()
  }

  fn max_database_id(&self) -> i32 {
    self.session.max_databases - 1
  }

  /// 全局指标快照（优先读取全局监视器快照；若未安装全局监视器，则回退读取会话级指标）
  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    GarnetServerMonitor::global()
      .and_then(|m| m.snapshot())
      .or_else(|| {
        self
          .session
          .session_metrics
          .map(|sm| GlobalMetricsSnapshot {
            global_session_metrics: sm,
            ..Default::default()
          })
      })
  }

  /// 聚合命令统计（服务器级聚合容器未装配：COMMANDSTATS 段不出指标）
  fn command_stats(&self) -> Vec<(String, u64, u64)> {
    Vec::new()
  }

  /// 键空间计数（仅显式 INFO KEYSPACE 触达；空库快照下不被调用）
  fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
    (0, 0)
  }

  /// 复制信息段（集群切面 ROLE 投影；None = 单机形态走 C# clusterProvider
  /// == null 的占位分支）
  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    let cluster = self.session.cluster_session.as_ref()?;
    let mut items = Vec::new();
    if cluster.is_primary() {
      let (offset, replicas) = cluster.get_primary_info();
      items.push(MetricsItem::new("role", "master"));
      items.push(MetricsItem::new(
        "connected_slaves",
        replicas.len().to_string(),
      ));
      items.push(MetricsItem::new(
        "master_repl_offset",
        offset.get(0).unwrap_or(0).to_string(),
      ));
      for (i, r) in replicas.iter().enumerate() {
        items.push(MetricsItem::new(
          format!("slave{i}"),
          format!(
            "ip={},port={},state={},offset={}",
            r.address, r.port, r.replication_state, r.replication_offset
          ),
        ));
      }
    } else {
      let role = cluster.get_replica_info();
      items.push(MetricsItem::new("role", "slave"));
      items.push(MetricsItem::new("master_host", role.address.clone()));
      items.push(MetricsItem::new("master_port", role.port.to_string()));
      items.push(MetricsItem::new(
        "master_link_status",
        role.replication_state.clone(),
      ));
      items.push(MetricsItem::new(
        "slave_repl_offset",
        role.replication_offset.to_string(),
      ));
    }
    Some(items)
  }

  /// gossip 统计段（集群切面未暴露：段不出指标）
  fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    Vec::new()
  }

  /// 缓冲池统计（集群切面未暴露：段不出指标）
  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  /// 集群 checkpoint 信息段（集群切面未暴露：段跳过）
  fn checkpoint_info(&self) -> Option<Vec<wmetric::MetricsItem>> {
    None
  }

  /// 混合日志内存分布转储（存储域快照通道缺口：段不出指标）
  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  /// 主侧安全 AOF 地址（集群切面主视角复制位点投影）
  fn safe_aof_address(&self) -> i64 {
    self
      .session
      .cluster_session
      .as_ref()
      .filter(|c| c.is_primary())
      .map(|c| c.get_primary_info().0.get(0).unwrap_or(0))
      .unwrap_or(0)
  }
}
