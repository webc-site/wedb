//! INFO uptime 起点装配期预热端到端（task/ing/zcode-r27-boottimeline 发现二）
//!
//! C# 对标 StoreWrapper 构造件的 :215 赋值 startupTimestamp，构造点
//! GarnetServer.cs:298 先于 Start :530 的 RecoverAsync：uptime 自存储装配
//! 起表，恢复时长与启动静默期天然计入。rust 修复前 `startup_ticks` OnceLock
//! 仅在 INFO 服务路径懒取——装配后静默期全部漏计，首条 INFO 报 uptime≈0。
//!
//! 本用例以真实节点装配 → 直断言起表锚刻度 ≤ 装配完成刻度，钉死「起表
//! 先于 accept」契约：锚不晚于装配完成 ⟺ 装配后任意静默期全程计入
//! uptime。懒取形态锚取于本断言时刻、恒晚于装配完成刻度，直接判死——
//! 无需真实静默间隔等待（原 2.2s 睡眠形态的等价收敛）。

use std::{thread::sleep, time::Duration};

use wbase::time::now_stopwatch_ticks;
use wnode::resp::info_provider::startup_ticks;
use wnode_test::start_node;

#[test]
fn info_uptime_counts_assembly_quiescence() {
  // 装配节点：起表点在 StorageSessionProvider 构造期（先于装配完成）
  let (_dir, _server, _) = start_node();
  // 装配完成时刻的单调刻度：uptime 起表锚须已在此前初始化
  let assembled = now_stopwatch_ticks();
  // 垫一枚跨 tick 延时（tick 粒度 100ns）：懒取形态下锚求值于延时之后，
  // 必然严格晚于 assembled 确定性判死；无延时时同刻度背靠背读数同 tick
  // 相等会漏检（判别力五五开）
  sleep(Duration::from_millis(2));
  let anchor = startup_ticks();
  assert!(
    anchor <= assembled,
    "uptime 起表锚须不晚于装配完成（锚 {anchor} > 装配完成刻度 {assembled}，装配后静默期将漏计）"
  );
}
