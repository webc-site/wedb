//! AOF 复制推流泵（数据源 → AofSyncDriver 的最小闭环）
//!
//! 对标 C# 形态：ReplicaSyncSession 为每个副本建 AofSyncDriver，其后台任务以
//! TsavoriteLog 迭代器从 startAddress 逐记录驱动 AofSyncTask.Consume
//!（经 GarnetClientSession 逐记录转发）→ Throttle 刷新背压水位
//!（libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs
//! 与 libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume/Throttle）。
//!
//! Rust 架构差异（刻意）：网络传输层（GarnetClientSession 等价物）尚未接线，
//! Consume 的网络面由 AofSyncTask 内置的发送缓冲池记账承接；数据源侧以
//! `waof::WalLog` 推流端口承接——attach 后每条新写入
//! 同栈触发逐副本分发（推流序与 AOF 地址序原子一致，见 waof/src/log.rs
//! ReplicationSinkFn 文档），存量积压由 [`AofReplicationPump::sync_backlog`]
//! 补扫（对标 C# 迭代器从 startAddress 的历史扫描）。两者合起来构成
//! 「读 AOF 记录 → 按 replica 位点发送」的完整最小闭环。
//!
//! 常驻节流：C# 每副本一条 `AofSyncDriver.RunAsync` 后台任务，迭代泵
//! `BulkConsumeAllAsync(consumer, REPLICA_SYNC_DELAY)` 在流耗尽时以
//! `Task.Delay(REPLICA_SYNC_DELAY)` 空转等待，每轮 `TryBulkConsumeNext` 开头
//! 调 `consumer.Throttle()`——即迭代间隙的周期节流（idle 发布语义的驱动源）。
//! Rust 的实时路径是无常驻任务的同栈推流，故由
//! [`AofReplicationPump::start_throttle_loop`] 以独立周期循环承接同等节流。

use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use waof::{WalLog, WalScanIterator};
use wdev::Device;

use super::{
  aof_sync_driver::AofSyncDriver, aof_sync_driver_store::AofSyncDriverStore,
  driver_registry::DriverLifecycle,
};

/// AOF 复制推流泵：waof 记录流 → 副本驱动逐记录转发
pub struct AofReplicationPump {
  store: Arc<AofSyncDriverStore>,
  /// 常驻节流循环运行标志（与循环共享；Drop 置 false 令其自然退出）
  throttle_running: Arc<AtomicBool>,
}

impl AofReplicationPump {
  /// 创建推流泵（绑定主侧副本驱动仓库）
  pub fn new(store: Arc<AofSyncDriverStore>) -> Self {
    Self {
      store,
      throttle_running: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 驱动仓库句柄
  pub fn store(&self) -> &Arc<AofSyncDriverStore> {
    &self.store
  }

  /// 启动常驻节流循环（须在 compio 运行时上下文调用；重复调用幂等）
  ///
  /// 对标 C# `RunAofSyncTaskAsync → iter.BulkConsumeAllAsync(this,
  /// REPLICA_SYNC_DELAY)` 的空闲路径：流耗尽时每 `interval` 醒来对全部在册
  /// 副本调 [`AofSyncDriverStore::throttle_replica`]——承载 C# Throttle 的
  /// idle 发布语义（增量不足但追平空闲时也重报水位；否则单向棘轮会让追平的
  /// 副本永不重发水位，闸门内停滞的追加方死锁，见 AofSyncTask.cs:Throttle
  /// 注释）。`interval` 取 `ReplicaSyncDelayMs` 配置（默认 5ms）
  pub fn start_throttle_loop(&self, interval: Duration) {
    if self.throttle_running.swap(true, Ordering::AcqRel) {
      return;
    }
    let store = Arc::clone(&self.store);
    let running = Arc::clone(&self.throttle_running);
    spawn(async move {
      while running.load(Ordering::Acquire) {
        store.throttle_all();
        sleep(interval).await;
      }
    })
    .detach();
  }

  /// 注入 WalLog 推流端口（此后每条新写入同栈分发到衔接中的副本驱动）
  ///
  /// 对标 C# 后台 AofSyncTask 循环的实时增量路径：新记录入队即逐副本
  /// Consume。仅当地址与副本已发位点严格衔接时分发（`addr == previous_address`），
  /// 落后副本交由 [`Self::sync_backlog`] 补扫——避免对落后副本发错位帧
  ///（对标 C# Consume 的 `currentAddress < previousAddress` 断言拒绝语义）
  pub fn attach_sink<D: Device>(&self, wal: &WalLog<D>) -> bool {
    let store = Arc::clone(&self.store);
    let sink = move |addr: u64, frame: &[u8]| {
      let next = addr + frame.len() as u64;
      store.for_each_driver(|driver| {
        dispatch_frame(driver, addr, next, frame);
      });
    };
    wal.set_replication_sink(Arc::new(sink))
  }

  /// 补扫存量积压：各副本从已发位点扫至日志尾，逐记录转发 + 节流重报
  ///
  /// 对标 C# AofSyncTask 迭代器从 startAddress 的历史扫描（attach 时刻之前的
  /// 记录不经推流端口，须由迭代器补齐）。单物理子日志拓扑：WalLog 单日志
  /// 对接 driver 的 sublog 0。返回 (转发记录数, 跳过记录数)
  pub async fn sync_backlog<D: Device>(&self, wal: &WalLog<D>) -> io::Result<(u64, u64)> {
    let drivers = self.store.drivers();
    let mut total_forwarded = 0u64;
    let mut total_skipped = 0u64;

    for driver in drivers {
      let Some(task) = driver.task_ref(0) else {
        continue;
      };
      let mut forwarded = 0u64;
      let mut skipped = 0u64;

      loop {
        let from = task.previous_address().max(0) as u64;
        let until = wal.committed_until_address();
        if from >= until {
          break;
        }

        let mut iter: WalScanIterator<D> = wal.scan(from, until);
        let mut batch_forwarded = 0u64;
        while let Some(record) = iter.next().await.map_err(io::Error::other)? {
          // 帧重建：WalRecord 负载 + 记录头还原完整帧（Consume 转发完整记录帧，
          // 与推流端口的帧口径一致：8B 记录头 + 负载）
          let frame = record.reconstruct_frame();
          match task.consume(&frame, record.address as i64, record.next_address as i64) {
            Ok(()) => batch_forwarded += 1,
            Err(_) => {
              // 断连或位错：终止本副本补扫（C# 迭代器异常同款终止语义）
              skipped += 1;
              break;
            }
          }
        }

        forwarded += batch_forwarded;
        if batch_forwarded == 0 || skipped > 0 {
          break;
        }
      }

      if forwarded > 0 {
        // 对标 C# 迭代间隙的 Throttle：推送进展后刷新背压水位
        self.store.throttle_replica(driver.remote_node_id());
      }
      total_forwarded += forwarded;
      total_skipped += skipped;
    }

    Ok((total_forwarded, total_skipped))
  }
}

/// 单副本帧分发：地址衔接时转发（Consume 语义）
fn dispatch_frame(driver: &AofSyncDriver, addr: u64, next: u64, frame: &[u8]) {
  let Some(task) = driver.task_ref(0) else {
    return;
  };
  if !task.is_connected() {
    return;
  }
  if task.previous_address() == addr as i64 {
    // 推流端口要求无阻塞；consume 仅做缓冲池记账与位点推进，天然满足
    if let Err(e) = task.consume(frame, addr as i64, next as i64) {
      log::warn!(
        "dispatch_frame: replica {} consume failed at addr {}: {e}",
        driver.remote_node_id(),
        addr
      );
    }
  }
}

impl Drop for AofReplicationPump {
  fn drop(&mut self) {
    // 常驻节流循环先停（对标 C# cts 取消后台同步任务），再处置全部驱动
    //（对标 C# ReplicaSyncSession Dispose 链：会话关闭 → driver.Dispose →
    // 任务断连标记）
    self.throttle_running.store(false, Ordering::Release);
    self.store.for_each_driver(|driver| {
      driver.dispose();
    });
  }
}
