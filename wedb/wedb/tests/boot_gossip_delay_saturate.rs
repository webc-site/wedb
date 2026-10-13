#![recursion_limit = "256"]
//! boot 播种 gossip 周期秒→毫秒折算饱和回归（真闭环）
//!
//! 对标 garnet C# 契约：GossipDelay 为 int 秒（Options.cs:295-296），消费前
//! 一次性 TimeSpan.FromSeconds(int)（ClusterManager.cs:112，double 域恒不
//! 溢出），全程无毫秒中间槽。rust 改采毫秒原子槽，boot 播种臂曾以
//! secs * 1000 裸乘折算：u64 秒域无界，>= 2^64/1000 值域 debug 构建
//! attempt to multiply with overflow panic（节点无法启动）、release 环绕
//! 成畸形周期（逼近 0 即忙轮询烧核）。收口 saturating_mul 饱和单点（同族
//! cluster-node-timeout 折算 runtime_server_config.rs 与 wbase convert.rs
//! 先例），大值钳 u64::MAX 毫秒 = 事实上永不到期上界，行为可预测。
//!
//! 续票纠偏注记（wedb-boot-cluster-time-knobs-upper-gate）：boot 上限闸
//! 落地后，契约带外（> i32::MAX 秒，C# IntRangeValidation(0, int.MaxValue)
//! 上界）启动期即拒，本册原「极值饱和放行」的启动烟测臂按新语义改写为
//! 拒启断言；带内值 saturating_mul 恒不触顶，播种臂降级为冗余保护层保留。

use std::{
  thread::{sleep, spawn},
  time::Duration,
};

use tempfile::tempdir;
use wconf::ConfigFileArgs;
use wedb::{
  ClusterArgs, Error as WedbError,
  server::{
    boot::{is_oversized_gossip_delay_secs, run_cluster_server},
    cluster_provider::ClusterProvider,
  },
};
use wnode::{Error as WnodeError, ShutdownCoordinator};

/// 溢出界：floor(2^64/1000) = 18446744073709551 为末个裸乘安全值，
/// 18446744073709552 起裸乘溢出（*1000 超 2^64）
const LAST_SAFE_SECS: u64 = 18_446_744_073_709_551;
const FIRST_OVERFLOW_SECS: u64 = LAST_SAFE_SECS + 1;

/// 单值启动冒烟：进群裸启动 + 150ms 存活断言 + 优雅停机。debug 构建
/// overflow-checks 默认开——裸乘折算的溢出 panic 会让启动线程提前崩溃，
/// is_finished 即红
fn boot_smoke_once(gossip_delay_secs: &str) {
  let dir = tempdir().expect("临时目录");
  let dir_str = dir.path().to_string_lossy().to_string();
  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--dir",
    &dir_str,
    "--gossip-delay-secs",
    gossip_delay_secs,
  ])
  .expect("参数解析");

  let coordinator = ShutdownCoordinator::new();
  let coord = coordinator.clone();
  let handle = spawn(move || run_cluster_server(args, Some(coord)));

  sleep(Duration::from_millis(150));
  assert!(
    !handle.is_finished(),
    "gossip-delay-secs={gossip_delay_secs} 启动线程不得因折算溢出 panic 提前崩溃"
  );

  coordinator.stop();
  let res = handle.join().expect("启动线程 join");
  assert!(
    res.is_ok(),
    "gossip-delay-secs={gossip_delay_secs} 应优雅退出: {res:?}"
  );
}

/// 契约带内边界启动回归不破：缺省 5s 与 C# 契约上界 i32::MAX 秒（boot
/// 上限闸落地后可达的最大档）——启动线程存活、优雅退出；带内值经播种臂
/// saturating_mul 恒不触顶（i32::MAX*1000 < u64::MAX）
#[test]
fn boot_in_contract_gossip_delay_secs_boots() {
  boot_smoke_once("5");
  boot_smoke_once(&i32::MAX.to_string());
}

/// 契约带外拒启（契约带上界拒收断言，非溢出 panic 断言）：clap 裸 u64
/// 解析（args.rs:76-77 无 value_parser）令 1e11 与 u64::MAX 字面哨兵档
/// 照常进槽（from_args_iter 不 Err 即值源可达实证），run_cluster_server
/// 顶层闸名单在任何装配副作用之前拒启——对齐 C# 入口
/// IntRangeValidation(0, int.MaxValue) 秒域上界
#[test]
fn boot_oversized_gossip_delay_secs_rejected_before_boot() {
  for secs in ["99999999999", "18446744073709551615"] {
    let dir = tempdir().expect("临时目录");
    let args = ClusterArgs::from_args_iter([
      "wedb",
      "--port",
      "0",
      "--dir",
      &dir.path().to_string_lossy(),
      "--gossip-delay-secs",
      secs,
    ])
    .expect("clap 裸 u64 解析须放行（值源可达）");
    match run_cluster_server(args, None) {
      Err(WedbError::Node(WnodeError::InvalidArgument(msg))) => {
        assert!(
          msg.contains("gossip-delay-secs"),
          "错误信息应指明 gossip-delay-secs: {msg}"
        );
      }
      other => panic!("{secs}s 应契约带上界拒启（InvalidArgument），实际: {other:?}"),
    }
  }
}

/// 上限门域边界（纯判据面）：缺省 5 与契约上界 i32::MAX 放行、
/// i32::MAX+1 与 u64::MAX 拒
#[test]
fn gossip_delay_upper_gate_boundary() {
  assert!(
    !is_oversized_gossip_delay_secs(5),
    "缺省 5s 回归不破（DEFAULT_GOSSIP_DELAY_SECS）"
  );
  assert!(
    !is_oversized_gossip_delay_secs(i32::MAX as u64),
    "契约上界本身应放行"
  );
  for secs in [i32::MAX as u64 + 1, 99_999_999_999, u64::MAX] {
    assert!(
      is_oversized_gossip_delay_secs(secs),
      "契约带外 {secs}s 应拒（带上界拒收）"
    );
  }
}

/// 播种落槽与消费链边界契约：溢出界上方全钳 u64::MAX 毫秒（可预期上界
/// Duration，非环绕畸形小值）；溢出界下方精确折算直落，常规值（C# 默认
/// 5 秒）零漂移
#[test]
fn gossip_delay_slot_chain_keeps_upper_bound() {
  let cp = ClusterProvider::new();
  for secs in [FIRST_OVERFLOW_SECS, u64::MAX] {
    cp.set_gossip_delay_ms(secs.saturating_mul(1000));
    assert_eq!(
      cp.gossip_delay_ms(),
      u64::MAX,
      "{secs}s 折算须钳 u64::MAX 毫秒"
    );
    // 消费面（gossip_manager::gossip_delay / 建连超时同式
    // Duration::from_millis）：毫秒域钳制值经 Duration 换算精确无环绕
    // （注意 from_millis(u64::MAX) 是毫秒域上界，非 Duration::MAX 本身）
    assert_eq!(
      Duration::from_millis(cp.gossip_delay_ms()).as_millis(),
      u64::MAX as u128,
      "{secs}s 换算 Duration 后毫秒域须精确无损耗"
    );
  }
  cp.set_gossip_delay_ms(LAST_SAFE_SECS.saturating_mul(1000));
  assert_eq!(cp.gossip_delay_ms(), LAST_SAFE_SECS * 1000);
  cp.set_gossip_delay_ms(5u64.saturating_mul(1000));
  assert_eq!(cp.gossip_delay_ms(), 5000, "C# 默认 5 秒折算零漂移");
}
