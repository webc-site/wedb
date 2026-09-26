//! INFO BPSTATS 段 server_socket 行拼装回归（工单
//! wnode-bpstats-server-socket-lines-missing）
//!
//! 对标 C# GarnetInfoMetrics.cs:408-416 PopulateClusterBufferPoolStats：
//! 先逐 TCP server 出 `server_socket_{i}` 行（值＝networkPool.GetStats()
//! 统计文本），clusterProvider 非空再追加集群端口行。修复前
//! populate_cluster_buffer_pool_stats 仅转调 buffer_pool_stats，standalone
//! 默认面（BpStats 在 DEFAULT_INFO 段集）BPSTATS 段仅剩段头。本用例钉死
//! wmetric 层拼装契约：socket 行在前、集群行在后、段头恒在。socket 统计
//! 文本取真实 LimitedFixedBufferPool 真值（C# GetStats 对位）；集群行哨兵
//! 是 provider 投影输入面——本层只锁拼装序，wnode 会话接线真值归宿主层。

use wbase::pool::LimitedFixedBufferPool;
use wmetric::{DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts};
use wresp::metrics::{InfoMetricsType, MetricsItem};

/// 服务器级事实最小形态（BPSTATS 段不消费 facts）
fn bare_facts() -> ServerFacts {
  ServerFacts {
    version: "1.0.0".into(),
    run_id: "run".into(),
    redis_protocol_version: "7.0".into(),
    enable_cluster: false,
    enable_aof: false,
    metrics_sampling_frequency: 0,
    latency_monitor: false,
    command_stats_monitor: false,
    startup_stopwatch_ticks: 0,
    log_dir: String::new(),
  }
}

/// 拼装序夹具：socket 行与集群行双投影面
struct SocketFirstProvider {
  socket_rows: Vec<(String, String)>,
  cluster_rows: Vec<(String, String)>,
}

impl InfoProvider for SocketFirstProvider {
  fn server_facts(&self) -> ServerFacts {
    bare_facts()
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    Vec::new()
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    None
  }

  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    Vec::new()
  }

  fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
    (0, 0)
  }

  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    Vec::new()
  }

  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    self.cluster_rows.clone()
  }

  fn server_socket_buffer_pool_stats(&self) -> Vec<(String, String)> {
    self.socket_rows.clone()
  }

  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  fn safe_aof_address(&self) -> i64 {
    0
  }
}

/// 渲染 BPSTATS 段文本
fn render_bpstats(provider: &impl InfoProvider) -> String {
  let mut info = GarnetInfoMetrics::new();
  info.get_resp_info(&[InfoMetricsType::BpStats], 0, provider)
}

/// server_socket_0 行名（C# GarnetInfoMetrics.cs:413 `server_socket_{i}`）
const SERVER_SOCKET_ROW: &str = "server_socket_0";

/// 行名回归锁：段头恒在，server_socket_0 行等值透出真实池统计文本
/// （C# GarnetServerTcp.GetBufferPoolStats ← networkPool.GetStats() 对位）
#[test]
fn bpstats_renders_server_socket_row_with_real_pool_truth() {
  let pool = LimitedFixedBufferPool::new(4096, 4);
  // 借出一块：统计文本的 borrowed_buffers 计数取真实运行态快照，随后归还
  let borrowed = pool.get_ref(4096);
  let stats = pool.get_stats();
  drop(borrowed);

  let provider = SocketFirstProvider {
    socket_rows: vec![(SERVER_SOCKET_ROW.to_string(), stats.clone())],
    cluster_rows: Vec::new(),
  };
  let text = render_bpstats(&provider);

  assert!(
    text.starts_with("# BufferPoolStats\r\n"),
    "应有 BufferPoolStats 段头: {text}"
  );
  let row = text
    .lines()
    .find(|l| l.starts_with("server_socket_0:"))
    .unwrap_or_else(|| panic!("缺 server_socket_0 行: {text}"));
  assert_eq!(
    row,
    format!("{SERVER_SOCKET_ROW}:{stats}"),
    "应等值透出真实池统计文本: {text}"
  );
}

/// 集群形态拼装序：socket 行在前、集群行在后（C# :411-415 先逐 server
/// 数组填充再 `[.. socket, .. cluster]` 追加的次序）
#[test]
fn bpstats_socket_rows_precede_cluster_rows() {
  let pool = LimitedFixedBufferPool::new(4096, 4);
  let provider = SocketFirstProvider {
    socket_rows: vec![(SERVER_SOCKET_ROW.to_string(), pool.get_stats())],
    cluster_rows: vec![("sentinel_cluster_bp".to_string(), "CLUSTER_BP".to_string())],
  };
  let text = render_bpstats(&provider);

  let socket = text
    .find("server_socket_0:")
    .unwrap_or_else(|| panic!("缺 server_socket_0 行: {text}"));
  let cluster = text
    .find("sentinel_cluster_bp:")
    .unwrap_or_else(|| panic!("缺集群行: {text}"));
  assert!(
    socket < cluster,
    "socket 行须排在集群行前（C# 拼装序）: {text}"
  );
}
