//! 监视器装配域：进程级安装与采样任务拉起
//!
//! 对标 C# libs/server/Metrics/GarnetServerMonitor.cs（采样循环启动位
//! `start_server_monitor` 留 mod.rs 公共路径锚）

use super::*;

/// 监视器进程级安装（构造 + install_global；对标 C# StoreWrapper.cs:226
/// monitor 随 StoreWrapper 构造：早于 GarnetServer.Start 的 servers[i].Start，
/// 任何连接建立时 RespServerSession 即可取得 monitor_iterations 时钟与
/// globalLatencyMetrics 出口——装配点若晚于网络监听，窗口期连接拿零值时钟
/// 与空延迟出口，LATENCY 样本静默丢失）。构造与全局槽安装不依赖 compio
/// 运行时；latency_monitor / commandstats_monitor 决定聚合成员是否就位
/// （C# GarnetServerMonitor.cs:64 构造参数）。
///
/// **单实例进程契约**：进程级监视器槽全局唯一（首装即赢）。若同一进程重复启动多个实例，
/// 后装实例将无法注册其监视器，必须记录 warn 日志留痕。
pub(super) fn install_server_monitor(
  frequency_secs: u64,
  latency_monitor: bool,
  commandstats_monitor: bool,
) -> Arc<GarnetServerMonitor> {
  let monitor = Arc::new(GarnetServerMonitor::new(
    frequency_secs,
    true,
    latency_monitor,
    commandstats_monitor,
  ));
  // 首装即赢（OnceLock 槽）：同进程第二实例安装被拒必须留痕禁静默——
  // 后装实例的会话 dispose 归并与 INFO/LATENCY 读路径将全部落到首装实例，
  // 指标面跨实例串数据（单实例进程契约声明见 GLOBAL_MONITOR 槽文档）
  if !monitor.install_global() {
    warn!("同进程第二监视器安装被拒（首装即赢，单实例进程契约）：指标面归并首装实例");
  }
  monitor
}
