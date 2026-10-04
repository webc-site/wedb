//! INFO memory 段 native_allocator_bytes 真值透出回归（工单
//! wmetric-native-allocator-bytes-unwired）
//!
//! 对标 C# GarnetInfoMetrics.cs:143 `native_allocator_bytes` ← Tsavorite
//! NativeMemoryTracker.Bytes 全局总账：观测叠影字段，不参与
//! total_main_store_size 求和，与 store_index_size 双列各表口径。本用例钉死
//! wmetric 层透传契约：provider 回报非零时等值透出、恒 ≥ store_index_size
//! （tracker 记页对齐 reserve 全局总账，恒 ≥ 主索引桶数组净字节）、无记账源
//! 形态回 0——杜绝回潮恒 0。wnode SessionInfoSource 对
//! windex::ram::NativeMemoryTracker::bytes 的委托接线属嵌入链，归宿主层验证。
//!
//! 自研依据: native_allocator_bytes 接线（C# 对应 RespInfoTests.cs INFO 段）

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

/// 带原生记账的存储形态：native = tracker 全局总账（页对齐 reserve 后跨度），
/// 恒 ≥ 主索引桶数组净字节（index_total_bytes 取小于 native 的真实量级）
struct TrackedProvider {
  native_bytes: i64,
  index_total_bytes: i64,
}

impl InfoProvider for TrackedProvider {
  fn server_facts(&self) -> ServerFacts {
    bare_facts()
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    vec![DbSnapshot {
      id: 0,
      index_total_memory_size_bytes: self.index_total_bytes,
      ..Default::default()
    }]
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

  fn native_allocator_bytes(&self) -> i64 {
    self.native_bytes
  }
}

/// 无记账源形态（裸宿主 / 删库归零基线）：native_allocator_bytes 走 trait 缺省 0
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

/// provider 回报非零：等值透出、非零、≥ store_index_size，且不掺入
/// total_main_store_size（观测叠影字段双列各表口径）
#[test]
fn native_allocator_bytes_passes_provider_truth() {
  let provider = TrackedProvider {
    native_bytes: 65536,
    index_total_bytes: 4288,
  };
  let memory = render_memory(&provider);

  assert!(
    memory.contains("# Memory\r\n"),
    "应有 Memory 段头: {memory}"
  );
  let native = field(&memory, "native_allocator_bytes");
  assert_eq!(native, 65536, "应等值透出 provider 真值: {memory}");
  assert!(native > 0, "记账在场应非零: {memory}");

  let index = field(&memory, "store_index_size");
  assert_eq!(index, 4288);
  assert!(native >= index, "全局总账恒 ≥ 桶数组净字节: {memory}");

  // 叠影字段不参与求和：total_main_store_size = index + mainlog + readcache
  let total = field(&memory, "total_main_store_size");
  assert_eq!(
    total, index,
    "native_allocator_bytes 不得掺入 total_main_store_size: {memory}"
  );
}

/// 无记账源形态：trait 缺省 0 占位出 0（删库归零基线的透传对位，非恒 0 回潮）
#[test]
fn untracked_form_reports_zero_baseline() {
  let memory = render_memory(&BareProvider);
  assert_eq!(field(&memory, "native_allocator_bytes"), 0, "{memory}");
  assert_eq!(field(&memory, "store_index_size"), 0, "{memory}");
}
