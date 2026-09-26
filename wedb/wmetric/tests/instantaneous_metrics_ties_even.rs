//! 瞬时吞吐中点舍入趋偶锁测（对标 libs/server/Metrics/GarnetServerMonitor.cs:
//! UpdateInstantaneousMetrics :127-129 三枚 Math.Round——.NET 默认
//! MidpointRounding.ToEven；rust 侧 round2 与 cmd 臂以 round_ties_even 逐字
//! 对位，f64::round 半离零会在中点样本差一末位，本文件钉死趋偶语义防回退）
//!
//! 自研回归锁: 瞬时吞吐舍入中点趋偶（对齐 C# ToEven）

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

/// 单轮驱动采样循环（对齐 instantaneous_metrics_underflow.rs 的驱动形态）。
fn run_round(monitor: &GarnetServerMonitor, input: u64, output: u64, cmds: u64) {
  let done = Cell::new(0usize);
  let resolve = || {
    done.set(done.get() + 1);
    MonitorIterationInputs {
      servers: vec![ServerSample {
        total_connections_received: 1,
        total_connections_disposed: 0,
        total_connections_active: 1,
        sessions: vec![session(input, output, cmds)],
      }],
      reset_active_sessions: || {},
      reset_active_command_stats: || {},
      reset_gossip_stats: || {},
      reset_revivification_stats: || {},
    }
  };
  block_on(monitor.main_monitor_task_async(|_d| ready(()), || done.get() >= 1, resolve));
}

/// round2 中点趋偶：1 秒窗净增 128 字节 → 128/1024 = 0.125，v*100 = 12.5
/// 恰落中点，ToEven 舍向偶数 12 → 0.12（半离零口径为 0.13，即与本断言互斥）。
/// 另侧 384 字节 → 0.375 → 37.5 → 趋偶进位 38 → 0.38，钉死「趋偶非截断」。
#[test]
fn tpt_midpoint_rounds_ties_to_even() {
  let monitor = GarnetServerMonitor::new(1, true, false, false);
  run_round(&monitor, 128, 384, 4);

  let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
  assert_eq!(snap.instantaneous_net_input_tpt, 0.12);
  assert_eq!(snap.instantaneous_net_output_tpt, 0.38);
  assert_eq!(snap.instantaneous_cmd_per_sec, 4.0);
}

/// cmd 面中点趋偶：2 秒窗 5 条命令 → 5/2 = 2.5，Math.Round 单参默认 ToEven
/// 舍向偶数 2（半离零口径为 3）。字节侧取非中点值以免串扰。
#[test]
fn cmd_per_sec_midpoint_rounds_ties_to_even() {
  let monitor = GarnetServerMonitor::new(2, true, false, false);
  run_round(&monitor, 2048, 1024, 5);

  let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
  assert_eq!(snap.instantaneous_net_input_tpt, 1.0);
  assert_eq!(snap.instantaneous_net_output_tpt, 0.5);
  assert_eq!(snap.instantaneous_cmd_per_sec, 2.0);
}
