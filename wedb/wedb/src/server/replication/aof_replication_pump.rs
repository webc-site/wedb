//! AOF 复制推流泵（数据源 → AofSyncDriver 的最小闭环）
//!
//! 对标 C# 形态：ReplicaSyncSession 为每个副本建 AofSyncDriver，其后台任务以
//! TsavoriteLog 迭代器从 startAddress 逐记录驱动 AofSyncTask.Consume
//!（经 GarnetClientSession 逐记录转发）→ Throttle 刷新背压水位
//!（libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs
//! 与 libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume/Throttle）。
//!
//! Rust 架构差异（刻意）：网络传输层以 AofSyncWire 发送通道 trait 注入
//!（TCP / 内存双形态，见 replica_wire 模块）；数据源侧以 `waof::WalLog`
//! 推流唤醒信号承接——attach 后每条新写入触发容量 1 信号（满即折叠），
//! 本泵被唤醒后按各副本位点从环形缓冲拉取新帧（推流序与 AOF 地址序原子
//! 一致，数据面不流经信号），存量积压同样由 [`AofReplicationPump::sync_backlog`]
//! 按地址序拉取。两者合起来构成「读 AOF 记录 → 按 replica 位点发送」的
//! 完整最小闭环。
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

use compio::{runtime::spawn, time::timeout};
use crossfire::{
  AsyncRx, MAsyncTx,
  mpsc::{Array, bounded_async},
};
use parking_lot::Mutex;
use waof::{WalLog, WalScanIterator};
use wdev::Device;

use super::{aof_sync_driver_store::AofSyncDriverStore, driver_registry::DriverLifecycle};

/// AOF 复制推流泵：waof 记录流 → 副本驱动逐记录转发（基于 crossfire 事件通知驱动节流）
pub struct AofReplicationPump {
  store: Arc<AofSyncDriverStore>,
  /// 常驻节流循环运行标志（与循环共享；Drop 置 false 令其自然退出）
  throttle_running: Arc<AtomicBool>,
  /// 节流事件触发端（容量为 1，无锁折叠去重）
  throttle_tx: MAsyncTx<Array<()>>,
  /// 节流事件接收端（启动时单次取出移交常驻协程）
  throttle_rx: Mutex<Option<AsyncRx<Array<()>>>>,
}

impl AofReplicationPump {
  /// 创建推流泵（绑定主侧副本驱动仓库）
  pub fn new(store: Arc<AofSyncDriverStore>) -> Self {
    let (throttle_tx, throttle_rx) = bounded_async::<()>(1);
    Self {
      store,
      throttle_running: Arc::new(AtomicBool::new(false)),
      throttle_tx,
      throttle_rx: Mutex::new(Some(throttle_rx)),
    }
  }

  /// 驱动仓库句柄
  pub fn store(&self) -> &Arc<AofSyncDriverStore> {
    &self.store
  }

  /// 触发一次节流信号（无锁非阻塞，位点前移或新日志追加时被动调用）
  #[inline]
  pub fn notify(&self) {
    let _ = self.throttle_tx.try_send(());
  }

  /// 启动事件通知驱动的节流反应器（彻底消灭无脑 sleep 轮询）
  ///
  /// 对标 Garnet 被动节流机制：
  /// 1. 静默期：通过 rx.recv().await 深度被动休眠，0% CPU，0 唤醒；
  /// 2. 写入期：新日志分发立即唤醒，实时刷新水位至背压闸门；
  /// 3. 空闲收尾：idle_delay 超时单次触发，落地残存水位（对标 C# idle == true 语义），
  ///    解冻闸门内停滞的追加方，随后立刻切回深度休眠。
  pub fn start_throttle_loop(&self, idle_delay: Duration) {
    if self.throttle_running.swap(true, Ordering::AcqRel) {
      return;
    }
    let Some(rx) = self.throttle_rx.lock().take() else {
      return;
    };
    let store = Arc::clone(&self.store);
    let running = Arc::clone(&self.throttle_running);
    // 启动时主动打一次脉冲，同步启动前可能遗留的积压
    self.notify();

    spawn(async move {
      while running.load(Ordering::Acquire) {
        // 步骤 1：深度被动挂起，无写入时不产生任何唤醒或轮询开销
        let Ok(()) = rx.recv().await else {
          break; // 通道关闭退出
        };
        if !running.load(Ordering::Acquire) {
          break;
        }

        // 步骤 2：收到位点前移事件，执行首轮节流
        store.throttle_all();

        // 步骤 3：空闲收尾防抖窗口（在 idle_delay 内连续吸收高并发写入）
        loop {
          if !running.load(Ordering::Acquire) {
            return;
          }
          match timeout(idle_delay, rx.recv()).await {
            Ok(Ok(())) => {
              // 仍有新写入追加：立即节流并继续防抖
              store.throttle_all();
            }
            Ok(Err(_)) => return,
            Err(_) => {
              // 超过 idle_delay 无新追加：进入空闲期，单次执行收尾发布（对标 C# idle == true）
              store.throttle_all();
              // 终结空闲防抖，返回步骤 1 深度被动挂起
              break;
            }
          }
        }
      }
    })
    .detach();
  }

  /// 注册推流唤醒信号并启动增量拉取循环
  ///
  /// 推流形态：WalLog 入队完成后发容量 1 唤醒信号（满即折叠，多帧写入只留
  /// 一个），本循环被唤醒后按各副本已发位点从环形缓冲拉取新帧——推流序与
  /// AOF 地址序原子一致（同一环形缓冲的线性化序），并发写入下从侧应用序与
  /// 主侧重启重放序永不发散。数据面不流经信号：帧在 WalLog 环形缓冲内，
  /// 按 [`WalLog::safe_tail_address`] 安全可读面拉取。
  ///
  /// 对标 C# AofSyncTask 周期泵：实时增量路径由入队信号驱动（compio 任务
  /// 唤醒，零轮询），位点衔接判定与逐记录 Consume 语义不变。仅当地址与副本
  /// 已发位点严格衔接时分发，落后副本由同一次扫描天然补齐（拉取按地址序，
  /// 无需单独补扫路径）。返回 false 表示信号端已被注册（重复 attach 同一
  /// WalLog，既有循环覆盖全部驱动）。
  pub fn attach_wake<D: Device + 'static>(&self, wal: &Arc<WalLog<D>>) -> bool {
    let (tx, rx) = bounded_async::<()>(1);
    if !wal.set_replication_wake(tx) {
      return false;
    }
    let store = Arc::clone(&self.store);
    let trigger = self.throttle_tx.clone();
    let wal = Arc::new(wal.clone());
    spawn(async move {
      while rx.recv().await.is_ok() {
        let _ = pump_backlog(&store, &trigger, &wal).await;
      }
    })
    .detach();
    true
  }

  /// 补扫存量积压：各副本从已发位点扫至日志尾，逐记录转发 + 节流重报
  ///
  /// 对标 C# AofSyncTask 迭代器从 startAddress 的历史扫描（attach 时刻之前的
  /// 记录不经推流端口，须由迭代器补齐）。单物理子日志拓扑：WalLog 单日志
  /// 对接 driver 的 sublog 0。返回 (转发记录数, 跳过记录数)
  pub async fn sync_backlog<D: Device>(&self, wal: &WalLog<D>) -> io::Result<(u64, u64)> {
    pump_backlog(&self.store, &self.throttle_tx, wal).await
  }
}

/// 推流拉取主体（自由函数：信号唤醒循环与同步补扫共用，无 self 依赖）
///
/// 各副本从已发位点（previous_address）扫至 [`WalLog::safe_tail_address`]，
/// 逐记录转发；断连或位错终止本副本本轮（C# 迭代器异常同款终止语义）。
/// 有进展的副本刷新背压水位（对标 C# 迭代间隙的 Throttle）。
async fn pump_backlog<D: Device>(
  store: &AofSyncDriverStore,
  throttle_tx: &MAsyncTx<Array<()>>,
  wal: &WalLog<D>,
) -> io::Result<(u64, u64)> {
  {
    let drivers = store.drivers();
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
        let until = wal.safe_tail_address();
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
        store.throttle_replica(driver.remote_node_id());
        let _ = throttle_tx.try_send(());
      }
      total_forwarded += forwarded;
      total_skipped += skipped;
    }

    Ok((total_forwarded, total_skipped))
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
