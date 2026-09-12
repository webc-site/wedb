//! 指标 API（对标 libs/server/Servers/MetricsApi.cs:MetricsApi）
//!
//! C# 持 GarnetProvider 直查 StoreWrapper.monitor；托管面以监视器句柄 +
//! [`InfoProvider`] 数据面入参承接（INFO 数据经存储域快照trait供给，
//! 延迟直方图与复位标志走监视器本体）。

use std::sync::Arc;

use parking_lot::Mutex;

use crate::metrics::{
  garnet_server_monitor::GarnetServerMonitor,
  info::garnet_info_metrics::{DEFAULT_INFO, GarnetInfoMetrics, InfoProvider},
  info_metrics_type::InfoMetricsType,
  latency::{
    garnet_latency_metrics::GarnetLatencyMetrics, latency_metrics_type::LatencyMetricsType,
  },
  metrics_item::MetricsItem,
};

/// 指标 API
pub struct MetricsApi {
  /// 服务器监视器（C# provider.StoreWrapper.monitor）
  monitor: Option<Arc<Mutex<GarnetServerMonitor>>>,
}

impl MetricsApi {
  /// 构造指标 API（监视器未启用传 None，复位族转为空操作）
  ///
  /// libs/server/Servers/MetricsApi.cs:MetricsApi
  pub fn new(monitor: Option<Arc<Mutex<GarnetServerMonitor>>>) -> Self {
    Self { monitor }
  }

  /// 取指定类别的 INFO 指标段
  ///
  /// libs/server/Servers/MetricsApi.cs:GetInfoMetrics（GetMetric 路径）
  pub fn get_info_metrics(
    &self,
    info_metrics_type: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<Vec<MetricsItem>> {
    GarnetInfoMetrics::new().get_metric(info_metrics_type, db_id, provider)
  }

  /// 取多个 INFO 指标段（None = 全部默认段；对应 GetInfoMetrics 多段重载）
  pub fn get_info_metrics_all(
    &self,
    info_metrics_types: Option<&[InfoMetricsType]>,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Vec<(InfoMetricsType, Vec<MetricsItem>)> {
    let sections = info_metrics_types.unwrap_or(DEFAULT_INFO);
    GarnetInfoMetrics::new().get_info_metrics(sections, db_id, provider)
  }

  /// 指定类别的 INFO 段头（静态）
  ///
  /// libs/server/Servers/MetricsApi.cs:GetHeader
  pub fn get_header(info_metrics_type: InfoMetricsType, db_id: i32) -> String {
    GarnetInfoMetrics::get_section_header(info_metrics_type, db_id)
  }

  /// 置位 INFO 段复位标志（监视器下轮采样生效）
  ///
  /// libs/server/Servers/MetricsApi.cs:ResetInfoMetrics
  pub fn reset_info_metrics(&self, info_metrics_type: InfoMetricsType) {
    if let Some(monitor) = &self.monitor {
      monitor.lock().reset_event_flags[info_metrics_type.idx()] = true;
    }
  }

  /// 置位多个 INFO 段复位标志（None = 全部默认段；对应 ResetInfoMetrics 多段重载）
  pub fn reset_info_metrics_all(&self, info_metrics_types: Option<&[InfoMetricsType]>) {
    let sections = info_metrics_types.unwrap_or(DEFAULT_INFO);
    for &section in sections {
      self.reset_info_metrics(section);
    }
  }

  /// 取指定类别的延迟直方图分位（未启用 / 无样本返回空表）
  ///
  /// libs/server/Servers/MetricsApi.cs:GetLatencyMetrics
  pub fn get_latency_metrics(&self, latency_metrics_type: LatencyMetricsType) -> Vec<MetricsItem> {
    let Some(monitor) = &self.monitor else {
      return Vec::new();
    };
    let Some(global_latency_metrics) = monitor.lock().global_latency_metrics() else {
      return Vec::new();
    };
    global_latency_metrics
      .lock()
      .get_latency_metrics(latency_metrics_type)
  }

  /// 取多个延迟类别分位（None = 默认类别集；对应 GetLatencyMetrics 多类别重载）
  pub fn get_latency_metrics_all(
    &self,
    latency_metrics_types: Option<&[LatencyMetricsType]>,
  ) -> Vec<(LatencyMetricsType, Vec<MetricsItem>)> {
    // C#：全局延迟指标缺席时整体返回空表
    let Some(monitor) = &self.monitor else {
      return Vec::new();
    };
    if monitor.lock().global_latency_metrics().is_none() {
      return Vec::new();
    }
    let types = latency_metrics_types.unwrap_or(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
    types
      .iter()
      .map(|&latency_metrics_type| {
        (
          latency_metrics_type,
          self.get_latency_metrics(latency_metrics_type),
        )
      })
      .collect()
  }

  /// 置位延迟类别复位标志
  ///
  /// libs/server/Servers/MetricsApi.cs:ResetLatencyMetrics
  pub fn reset_latency_metrics(&self, latency_metrics_type: LatencyMetricsType) {
    if let Some(monitor) = &self.monitor {
      monitor.lock().reset_latency_metrics[latency_metrics_type.idx()] = true;
    }
  }

  /// 置位多个延迟类别复位标志（None = 默认类别集；对应 ResetLatencyMetrics 多类别重载）
  pub fn reset_latency_metrics_all(&self, latency_metrics_types: Option<&[LatencyMetricsType]>) {
    let types = latency_metrics_types.unwrap_or(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
    for &latency_metrics_type in types {
      self.reset_latency_metrics(latency_metrics_type);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::metrics::info::garnet_info_metrics::{DbSnapshot, GlobalMetricsSnapshot, ServerFacts};

  /// 最小 provider 桩（对齐 info_command 测试的 MockProvider 形态）
  struct MockProvider;

  impl InfoProvider for MockProvider {
    fn server_facts(&self) -> ServerFacts {
      ServerFacts {
        version: "1.0.0".into(),
        run_id: "run123".into(),
        redis_protocol_version: "7.0".into(),
        enable_cluster: false,
        enable_aof: false,
        metrics_sampling_frequency: 10,
        latency_monitor: false,
        command_stats_monitor: false,
        startup_timestamp_unix_secs: 0,
        log_dir: "/tmp/log".into(),
      }
    }

    fn databases(&self) -> Vec<DbSnapshot> {
      vec![DbSnapshot {
        id: 0,
        current_version: 7,
        ..DbSnapshot::default()
      }]
    }

    fn max_database_id(&self) -> i32 {
      0
    }

    fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
      None
    }

    fn command_stats(&self) -> Vec<(String, u64, u64)> {
      Vec::new()
    }

    // 满足 InfoProvider trait 契约保留
    fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
      (0, 0)
    }

    fn replication_info(&self) -> Option<Vec<MetricsItem>> {
      None
    }

    // 满足 InfoProvider trait 契约保留
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

  #[test]
  fn header_and_info_metrics_flow_through_provider() {
    let api = MetricsApi::new(None);
    let header = MetricsApi::get_header(InfoMetricsType::Server, 0);
    assert!(!header.is_empty());

    let provider = MockProvider;
    let items = api.get_info_metrics(InfoMetricsType::Server, 0, &provider);
    assert!(items.is_some());
    let all = api.get_info_metrics_all(None, 0, &provider);
    assert!(!all.is_empty());
  }

  #[test]
  fn reset_flags_without_monitor_are_noop() {
    let api = MetricsApi::new(None);
    api.reset_info_metrics(InfoMetricsType::Stats);
    api.reset_info_metrics_all(None);
    api.reset_latency_metrics(LatencyMetricsType::NetRsLat);
    api.reset_latency_metrics_all(None);
    assert!(
      api
        .get_latency_metrics(LatencyMetricsType::NetRsLat)
        .is_empty()
    );
    assert!(api.get_latency_metrics_all(None).is_empty());
  }

  #[test]
  fn reset_flags_set_on_monitor() {
    let monitor = Arc::new(Mutex::new(GarnetServerMonitor::new(1, true, true, false)));
    let api = MetricsApi::new(Some(Arc::clone(&monitor)));
    api.reset_info_metrics(InfoMetricsType::Stats);
    api.reset_latency_metrics(LatencyMetricsType::NetRsLat);
    let monitor = monitor.lock();
    assert!(monitor.reset_event_flags[InfoMetricsType::Stats.idx()]);
    assert!(monitor.reset_latency_metrics[LatencyMetricsType::NetRsLat.idx()]);
  }
}
