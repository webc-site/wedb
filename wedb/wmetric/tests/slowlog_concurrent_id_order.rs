//! 慢日志容器并发追加 id 单调性回归测试
//!（对标 libs/server/Metrics/Slowlog/SlowLogContainer.cs:Add 的全局递增
//! id 契约；真实场景为多个客户端连接并发触发慢查询、各自经
//! RespSlowlogCommands 路径调用 SlowLogContainer::add，如
//! wnode/src/resp/metrics_commands.rs 中按容器共享给全部会话）。
//! 修复前取号在锁外，线程调度错位可产生 [..., id=1, id=0] 的物理倒置，
//! 破坏 SLOWLOG GET 快照单调性与满容 FIFO 淘汰秩序，本文件钉死该竞态。）
//!
//! 自研依据: SLOWLOG 并发单调 ID（C# 对应 RespSlowLogTests.cs）

use std::{
  sync::{Arc, Barrier},
  thread,
};

use wmetric::{SlowLogContainer, SlowLogEntry};
use wresp::command::RespCommand;

/// 并发连接数与每连接慢查询条数。
const THREADS: usize = 8;
const PER_THREAD: usize = 64;
const TOTAL: usize = THREADS * PER_THREAD;

fn entry(seq: usize) -> SlowLogEntry {
  SlowLogEntry {
    id: 0,
    timestamp: 1000,
    duration: seq as i32 + 1,
    command: RespCommand::Get,
    arguments: None,
    client_ip_port: "127.0.0.1:1234".into(),
    client_name: "cli".into(),
  }
}

/// 多线程并发 add：栅栏对齐起跑放大竞态窗口，断言快照 id 从 0 起
/// 逐条 +1 严格连续单调（蕴含唯一、无倒置、无丢号）。
#[test]
fn concurrent_add_keeps_ids_strictly_monotonic() {
  let container = Arc::new(SlowLogContainer::new(TOTAL as i32));
  let start = Arc::new(Barrier::new(THREADS));
  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let container = Arc::clone(&container);
      let start = Arc::clone(&start);
      thread::spawn(move || {
        start.wait();
        for i in 0..PER_THREAD {
          container.add(entry(t * PER_THREAD + i));
        }
      })
    })
    .collect();
  for h in handles {
    h.join().unwrap();
  }

  let ids: Vec<i64> = container.get_entries(-1).iter().map(|e| e.id).collect();
  assert_eq!(ids.len(), TOTAL, "容量恰为总条数，零淘汰");
  assert_eq!(ids.first(), Some(&0));
  assert_eq!(ids.last(), Some(&(TOTAL as i64 - 1)));
  assert!(
    ids.windows(2).all(|w| w[1] == w[0] + 1),
    "物理顺序与 id 单调必须严格一致: {ids:?}"
  );
}

/// 满容环形裁剪下 FIFO 秩序：容量小于总条数时并发追加，
/// 保留的最新快照仍是连续递增 id 尾部区间（淘汰序 = 入队序 = 取号序）。
#[test]
fn concurrent_add_trim_keeps_monotonic_tail() {
  let cap = TOTAL / 2;
  let container = Arc::new(SlowLogContainer::new(cap as i32));
  let start = Arc::new(Barrier::new(THREADS));
  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let container = Arc::clone(&container);
      let start = Arc::clone(&start);
      thread::spawn(move || {
        start.wait();
        for i in 0..PER_THREAD {
          container.add(entry(t * PER_THREAD + i));
        }
      })
    })
    .collect();
  for h in handles {
    h.join().unwrap();
  }

  let ids: Vec<i64> = container.get_entries(-1).iter().map(|e| e.id).collect();
  assert_eq!(ids.len(), cap);
  assert_eq!(ids.first(), Some(&(TOTAL as i64 - cap as i64)));
  assert!(
    ids.windows(2).all(|w| w[1] == w[0] + 1),
    "裁剪后尾部快照必须连续递增: {ids:?}"
  );
}

/// 容量为 0：入口短路，条目零入库（且不消耗自增 id 序列）。
#[test]
fn zero_capacity_add_is_noop() {
  let container = SlowLogContainer::new(0);
  for i in 0..8 {
    container.add(entry(i));
  }
  assert_eq!(container.count(), 0);
  assert!(container.get_entries(-1).is_empty());
}
