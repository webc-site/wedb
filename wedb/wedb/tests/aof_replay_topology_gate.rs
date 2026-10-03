//! 复制域 AOF 拓扑装配双门反证锁测（票
//! wnode-aof-multi-replay-single-physical-silent-zero-replay）
//!
//! 两门现产均不可达（aof-physical-sublog-count / aof-replay-task-count 系
//! read_only 槽、CLI / CONFIG SET 无用户通路、runtime_server_options 投影
//! 不设值缺省恒 1），纯防未来旋钮接通漂移。boot 闭包内联门不可从 CLI 驱
//! 动，判据收口 [`aof_boot_gate_violation`] 纯函数直驱取证（同
//! cluster_node_timeout_subsecond_gate.rs 的亚秒门先例形）：装配违规组合
//! 的 RuntimeServerOptions 配置面即断言拒启信息，回退补门则多回放臂断言
//! 落空转红。
//!
//! 回放任务数门的危害面（激活后）：multi_log_enabled = physical > 1 ||
//! replay > 1（对标 C# GarnetServerOptions.cs:1244，rust
//! garnet_append_only_file.rs:78 同形）→ 单物理时
//! recover_latest_sequence_number 恒 Some(-1)（addresses.rs，对标
//! GarnetLog.cs:182-198）→ record_gate::skip_replay 首条即跳 →
//! multi_log_recover 收 Ok(0) 静默零重放、检查点后写全丢零告警。

use wconf::RuntimeServerOptions;
use wedb::{ClusterArgs, server::boot::aof_boot_gate_violation};

/// 缺省拓扑（1+1）双门放行：投影缺省值必须恒过门，防误杀现产路径
#[test]
fn default_topology_passes_both_gates() {
  assert!(
    aof_boot_gate_violation(&RuntimeServerOptions::default()).is_none(),
    "缺省 1+1 拓扑不得触门"
  );
}

/// 反证锁测主案：物理子日志恒 1、回放任务数 4（复现票面半扇可达组合的
/// 配置面）必须拒启，且错误信息明确指向回放任务数门与静默零重放危害
#[test]
fn single_physical_multi_replay_rejected() {
  let opts = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: 4,
    ..RuntimeServerOptions::default()
  };
  match aof_boot_gate_violation(&opts) {
    Some(msg) => {
      assert!(
        msg.contains("aof-replay-task-count=1"),
        "错误信息应指明回放任务数门: {msg}"
      );
      assert!(
        msg.contains("silently replays zero records"),
        "错误信息应自陈静默零重放危害: {msg}"
      );
    }
    None => panic!("单物理 + 4 回放任务组合必须拒启，不得静默放行"),
  }
}

/// 既有单物理多子日志门同族复用形：回放数合法（1）、物理数 2 仍须落
/// 既有门信息（本票补齐不得改变既有门语义与信息）
#[test]
fn multi_sublog_single_replay_rejected_by_existing_door() {
  let opts = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  match aof_boot_gate_violation(&opts) {
    Some(msg) => assert!(
      msg.contains("aof-physical-sublog-count=1"),
      "错误信息应指明物理子日志门: {msg}"
    ),
    None => panic!("多物理子日志组合必须拒启"),
  }
}

/// 双违规并报序：物理门先行（既有门优先报错，回放门为补齐同族第二扇）
#[test]
fn both_violations_report_sublog_door_first() {
  let opts = RuntimeServerOptions {
    aof_physical_sublog_count: 4,
    aof_replay_task_count: 4,
    ..RuntimeServerOptions::default()
  };
  let msg = aof_boot_gate_violation(&opts).expect("双违规必须拒启");
  assert!(
    msg.contains("aof-physical-sublog-count=1"),
    "双违规应先报既有物理子日志门: {msg}"
  );
}

/// 非 1 即拒的判据域：0 / 负值系装配面非法输入，与既有物理门 != 1 同形
/// 一并拒启（消费侧 .max(1) 折算不改变装配门从严口径）
#[test]
fn non_positive_replay_count_rejected() {
  for replay in [0, -1, i32::MIN] {
    let opts = RuntimeServerOptions {
      aof_physical_sublog_count: 1,
      aof_replay_task_count: replay,
      ..RuntimeServerOptions::default()
    };
    assert!(
      aof_boot_gate_violation(&opts).is_some(),
      "回放任务数 {replay} 非法须拒启"
    );
  }
}

/// 门在 run_cluster_server 顶层投影面不误伤：缺省 CLI 装配的投影两字段
/// 恒 1，整链启动段不受本票补齐影响（现产不可达实证）
#[test]
fn cli_projection_stays_default_single() {
  use wconf::{ConfigFileArgs, ServerArgs};
  let args = ClusterArgs::from_args_iter(["wedb", "--port", "0"]).unwrap();
  let projected = args.node_args().runtime_server_options();
  assert_eq!(projected.aof_physical_sublog_count, 1);
  assert_eq!(projected.aof_replay_task_count, 1);
  assert!(aof_boot_gate_violation(&projected).is_none());
}
