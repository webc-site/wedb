//! 指标与延迟（对标 libs/server/Metrics/Info/InfoCommand.cs 与
//! libs/server/Metrics/Latency/RespLatencyCommands.cs 的会话侧投影：
//! 会话指标挂载、延迟表存取与批次起停，及 INFO 纯慢段降级判定）。

use std::sync::Arc;

use wbase::time::now_stopwatch_ticks;
use wconf::ServerConfigType;
use wmetric::{GarnetLatencyMetricsSession, LatencyMetricsType, SessionMetricsHandle};

use super::core::RespServerSession;

impl RespServerSession {
  /// 装配期注入会话指标共享句柄（本会话 `session_metrics` 字段的唯一写入点；
  /// 对标 C# RespServerSession.cs:264
  /// 构造期单点建 sessionMetrics 的判定在本仓搬到装配侧：句柄由 `service.rs`
  /// 按采样频率门控唯一创建，经
  /// [`RespSessionConsumer::attach_session_metrics`](crate::resp::resp_session_consumer::RespSessionConsumer::attach_session_metrics)
  /// 同名转发到本口，使会话与存储执行域共持同一 Arc；None = 采样关闭，
  /// 与 C# null 会话指标同形）
  pub fn attach_session_metrics(&mut self, metrics: Option<Arc<SessionMetricsHandle>>) {
    self.session_metrics = metrics;
  }

  /// libs/server/Resp/RespServerSession.cs:GetLatencyMetrics
  pub fn get_latency_metrics(&self) -> Option<Arc<GarnetLatencyMetricsSession>> {
    self.latency_metrics.clone()
  }

  /// libs/server/Resp/RespServerSession.cs:ResetLatencyMetrics
  pub fn reset_latency_metrics(&self, latency_event: LatencyMetricsType) {
    if let Some(metrics) = &self.latency_metrics {
      metrics.reset(latency_event);
    }
  }

  /// 批次消费入口的延迟/慢日志起始装配（C# TryConsumeMessages:481/:486-490）：
  /// 延迟监视开启即启动 NET_RS_LAT 计时；慢日志门开启时起始刻度与延迟
  /// 计时同源（LatencyMetrics.Get(NET_RS_LAT)），无延迟监视直取单调秒表
  /// （C# `Stopwatch.GetTimestamp()`）
  pub(super) fn latency_batch_start(&mut self) {
    let slow_log_enabled = self
      .runtime_config
      .get_microseconds(ServerConfigType::SlowlogLogSlowerThan)
      > 0;
    let Some(latency) = &self.latency_metrics else {
      if slow_log_enabled {
        self.slow_log_start_ticks = now_stopwatch_ticks();
      }
      return;
    };
    latency.start(LatencyMetricsType::NetRsLat, now_stopwatch_ticks());
    if slow_log_enabled {
      self.slow_log_start_ticks = latency.get(LatencyMetricsType::NetRsLat);
    }
  }

  /// 批次消费出口的延迟停表（C# TryConsumeMessages:586-598）：有成功消费
  /// 字节才记录——慢命令批次切 NET_RS_LAT_ADMIN 桶，随后把字节/命令数
  /// 记入吞吐直方图
  pub(super) fn latency_batch_stop(&mut self, consumed: usize, op_count: u64) {
    let Some(latency) = &self.latency_metrics else {
      return;
    };
    if consumed == 0 {
      return;
    }
    let now = now_stopwatch_ticks();
    if self.contains_slow_command {
      latency.stop_and_switch(
        LatencyMetricsType::NetRsLat,
        LatencyMetricsType::NetRsLatAdmin,
        now,
      );
      self.contains_slow_command = false;
    } else {
      latency.stop(LatencyMetricsType::NetRsLat, now);
    }
    latency.record_value(LatencyMetricsType::NetRsBytes, consumed as i64);
    latency.record_value(LatencyMetricsType::NetRsOps, op_count as i64);
  }

  /// INFO 纯慢段请求（KEYSPACE/HLOGSCAN/STOREHASHTABLE/STOREREVIV）的降级
  /// 判定（rust compio 异步存储域特有降级点，无 C# 对标函数——C#
  /// GetKeyspaceStats / HybridLogDistributionScan / DumpDistribution /
  /// DumpRevivificationStats 网络线程同步执行；rust 存储域扫描须跨
  /// await，与 [`Self::network_dbsize`] 同构降级 Ok(false) 挂 SlowWait
  /// 异步闭环）
  ///
  /// 唯一到达路径：[`Self::process_other_commands`] 放行的纯慢段请求
  /// （DEFAULT/ALL 段集合不含这四段，其余 INFO 请求在会话侧同步闭环）
  pub(crate) fn try_info_keyspace_slow_path(
    &mut self,
    _parse_state: &[&[u8]],
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Ok(false)
  }
}
