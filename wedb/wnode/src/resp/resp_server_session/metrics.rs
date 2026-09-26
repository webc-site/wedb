//! 指标与延迟（对标 libs/server/Metrics/Info/InfoCommand.cs 与
//! libs/server/Metrics/Latency/RespLatencyCommands.cs 的会话侧投影：
//! 会话指标挂载、延迟表存取与批次起停，及 INFO 扫描族段降级判定）。

use std::sync::Arc;

use wbase::{convert::stopwatch::TICKS_PER_MICROSECOND, time::now_stopwatch_ticks};
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
  ///
  /// 借用而非克隆：延迟表为本会话独占的拥有型实例，共享出口会把无锁直写
  /// 降级成写锁竞争
  pub fn get_latency_metrics(&self) -> Option<&GarnetLatencyMetricsSession> {
    self.latency_metrics.as_ref()
  }

  /// 批次消费入口的延迟/慢日志起始装配（C# TryConsumeMessages:481/:484-489）：
  /// 延迟监视开启即启动 NET_RS_LAT 计时；慢日志阈值与 C# 同源，仅在批入口
  /// 单次读运行时配置（µs × 刻度因子折算 tick）缓存到会话字段，
  /// CONFIG SET slowlog-log-slower-than 自下一批次起生效；门开启时起始刻度与
  /// 阈值刷新严格对齐（有延迟监视取 LatencyMetrics.Get(NET_RS_LAT)，
  /// 无则直取单调秒表，C# `Stopwatch.GetTimestamp()`），杜绝阈值零值
  /// 与批次起始戳脱节
  pub(super) fn latency_batch_start(&mut self) {
    // C# :486 `slowLogThreshold = config > 0 ? config * TimeStampToMicroseconds : 0`
    //（get_microseconds 读值即微秒原生单位，> 0 判后 as u64 无截断）
    let threshold_us = self
      .runtime_config
      .get_microseconds(ServerConfigType::SlowlogLogSlowerThan);
    self.slow_log_threshold = if threshold_us > 0 {
      threshold_us as u64 * TICKS_PER_MICROSECOND
    } else {
      0
    };
    let Some(latency) = &mut self.latency_metrics else {
      if self.slow_log_threshold > 0 {
        self.slow_log_start_ticks = now_stopwatch_ticks();
      }
      return;
    };
    latency.start(LatencyMetricsType::NetRsLat, now_stopwatch_ticks());
    if self.slow_log_threshold > 0 {
      self.slow_log_start_ticks = latency.get(LatencyMetricsType::NetRsLat);
    }
  }

  /// 批次消费出口的延迟停表（C# TryConsumeMessages:586-598）：有成功消费
  /// 字节才记录——慢命令批次切 NET_RS_LAT_ADMIN 桶，随后把字节/命令数
  /// 记入吞吐直方图
  pub(super) fn latency_batch_stop(&mut self, consumed: usize, op_count: u64) {
    let Some(latency) = &mut self.latency_metrics else {
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

  /// INFO 扫描族段请求（段集含 KEYSPACE/HLOGSCAN/STOREHASHTABLE/STOREREVIV
  /// 任一段，any 语义混合段整请求降级；rust compio 异步存储域特有降级点，
  /// 无 C# 对标函数——C# GetKeyspaceStats / HybridLogDistributionScan /
  /// DumpDistribution / DumpRevivificationStats 网络线程同步执行；rust
  /// 存储域扫描须跨 await，与 [`Self::network_dbsize`] 同构降级 Ok(false)
  /// 挂 SlowWait 异步闭环，扫描行与非扫描面两路经组合数据源渲染）
  ///
  /// 唯一到达路径：[`Self::process_other_session_commands`] INFO 分派门
  /// 放行的降级段集请求（RESET/HELP/非法段与纯非扫描段请求在会话侧同步
  /// 闭环；DEFAULT/ALL 段集合不含扫描族段）
  pub(crate) fn try_info_keyspace_slow_path(
    &mut self,
    _parse_state: &[&[u8]],
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Ok(false)
  }
}
