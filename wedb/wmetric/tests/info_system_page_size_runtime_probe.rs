//! INFO MEMORY 段 system_page_size 运行时真值回归（工单
//! wmetric-info-observable-divergence-registry-trio a 项）
//!
//! 对标 C# GarnetInfoMetrics.cs:118 `system_page_size` 读
//! Environment.SystemPageSize 运行时真值：非 4K 页宿主（darwin arm64
//! 16KB、64K 页 aarch64/ppc64 Linux）上恒 4096 硬编码与 C# 逐字段分叉。
//! 本用例钉死 rust 侧经 page_size::get() 探针（与
//! windex/src/ram/direct_vm.rs 同源）出运行时真值，杜绝回潮硬编码。
//!
//! 自研依据: system_page_size 运行时探针（C# 对应 RespInfoTests.cs INFO 段）

use wmetric::{DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts};
use wresp::metrics::{InfoMetricsType, MetricsItem};

/// 服务器级事实最小形态（MEMORY 段仅消费 enable_aof 投影 aof_memory_size）
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

struct BareProvider;

impl InfoProvider for BareProvider {
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
    Vec::new()
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

/// 渲染 MEMORY 段文本
fn render_memory(provider: &impl InfoProvider) -> String {
  let mut info = GarnetInfoMetrics::new();
  info.get_resp_info(&[InfoMetricsType::Memory], 0, provider)
}

/// 从渲染文本解析整数字段
fn field(text: &str, name: &str) -> i64 {
  let prefix = format!("{name}:");
  text
    .lines()
    .find_map(|l| l.strip_prefix(&prefix))
    .unwrap_or_else(|| panic!("缺字段 {name}: {text}"))
    .parse()
    .unwrap_or_else(|_| panic!("字段 {name} 非整数: {text}"))
}

/// system_page_size 首行出运行时真值：与 page_size::get() 探针等值
///（darwin arm64 上即 16384、4K 页 Linux 上即 4096），恒为 ≥ 4096 的
/// 2 的幂——杜绝恒 4096 硬编码回潮（非 4K 页宿主必翻红）。
#[test]
fn system_page_size_emits_runtime_probe_truth() {
  let memory = render_memory(&BareProvider);
  let probe = page_size::get() as u64;
  assert_eq!(
    field(&memory, "system_page_size"),
    probe as i64,
    "system_page_size 须与 page_size::get() 探针等值: {memory}"
  );
  assert!(probe >= 4096, "各支持平台页大小下限 4096: {probe}");
  assert!(probe.is_power_of_two(), "页大小恒为 2 的幂: {probe}");
}
