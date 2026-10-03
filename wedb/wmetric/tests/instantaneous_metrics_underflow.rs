//! 监视器瞬时吞吐基线下 dip 饱和对标测试
//!（对标 libs/server/Metrics/GarnetServerMonitor.cs:UpdateInstantaneousMetrics；
//! C# 正常单调口径参照 test/standalone/Garnet.test/RespMetricsTest.cs 的
//! 瞬时吞吐采样断言，基线瞬时下 dip 在 C# 侧为 unchecked 回绕潜在缺陷、
//! 无既有用例，本文件按饱和语义钉死）
//!
//! 自研回归锁: 瞬时指标下溢防御

use std::{cell::Cell, future::ready};

use wbase::future::block_on;
use wmetric::{
  GarnetServerMonitor, GarnetSessionMetrics, MonitorIterationInputs, ServerSample, SessionSample,
};

/// 单会话采样快照（其余计数字段取默认零值）。
fn session(input: u64, output: u64, cmds: u64) -> SessionSample {
  SessionSample {
    metrics: GarnetSessionMetrics {
      total_net_input_bytes: input,
      total_net_output_bytes: output,
      total_commands_processed: cmds,
      ..GarnetSessionMetrics::default()
    },
    command_stats: None,
  }
}

/// 逐轮驱动采样循环（每轮一个会话三元组集，即时构造样本；轮数耗尽即取消
/// 退出，对齐 wnode/tests/server_monitor_tests.rs 的 run_iterations 驱动形态）。
fn run_rounds(monitor: &GarnetServerMonitor, rounds: &[Vec<(u64, u64, u64)>]) {
  let done = Cell::new(0usize);
  let cancelled = || done.get() >= rounds.len();
  let resolve = || {
    let idx = done.get().min(rounds.len() - 1);
    done.set(done.get() + 1);
    let sessions: Vec<SessionSample> = rounds[idx]
      .iter()
      .map(|&(i, o, c)| session(i, o, c))
      .collect();
    MonitorIterationInputs {
      servers: vec![ServerSample {
        total_connections_received: 1,
        total_connections_disposed: 0,
        total_connections_active: sessions.len() as i64,
        sessions,
      }],
      reset_active_sessions: || {},
      reset_active_command_stats: || {},
      reset_gossip_stats: || {},
      reset_revivification_stats: || {},
    }
  };
  block_on(monitor.main_monitor_task_async(|_d| ready(()), cancelled, resolve));
}

/// 正常单调轮：按采样间隔折算瞬时吞吐（C# byteUnit=1KiB、频率 1s 口径）。
#[test]
fn normal_round_reports_positive_throughput() {
  let monitor = GarnetServerMonitor::new(1, true, false, false);
  run_rounds(&monitor, &[vec![(20480, 10240, 100)]]);

  let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
  assert_eq!(snap.instantaneous_net_input_tpt, 20.0);
  assert_eq!(snap.instantaneous_net_output_tpt, 10.0);
  assert_eq!(snap.instantaneous_cmd_per_sec, 100.0);
}

/// 基线下 dip 轮：会话已移出活跃列表、dispose 指标尚未并入历史，
/// 本轮累计低于上轮基线——裸减法在 Debug 下 panic、Release 下回绕成
/// 天文数字；饱和语义应三值平滑截 0。
#[test]
fn baseline_dip_saturates_to_zero() {
  let monitor = GarnetServerMonitor::new(1, true, false, false);
  // 第一轮建立基线（入 20480 / 出 10240 / 命令 100）
  run_rounds(&monitor, &[vec![(20480, 10240, 100)]]);
  // 第二轮会话移出但历史未归并：curr 全 0，低于基线
  run_rounds(&monitor, &[vec![]]);

  let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
  assert_eq!(snap.instantaneous_net_input_tpt, 0.0);
  assert_eq!(snap.instantaneous_net_output_tpt, 0.0);
  assert_eq!(snap.instantaneous_cmd_per_sec, 0.0);

  // 基线随 dip 轮滚动到 0 后，下一轮真实增长按新基线全额折算（饱和不粘滞）
  run_rounds(&monitor, &[vec![(21504, 10240, 100)]]);
  let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
  assert_eq!(snap.instantaneous_net_input_tpt, 21.0);
  assert_eq!(snap.instantaneous_net_output_tpt, 10.0);
  assert_eq!(snap.instantaneous_cmd_per_sec, 100.0);
}
