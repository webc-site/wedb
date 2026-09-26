//! AOF 复制推流泵（数据源 → AofSyncDriver 的最小闭环）
//!
//! 对标 C# 形态（libs/cluster/Server/Replication/PrimaryOps/ReplicationPrimaryAofSync.cs:TryConnectToReplica
//! 发起推流任务 + libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:RunAsync
//! 的每副本常驻迭代泵）：ReplicationManager 为每个副本建 AofSyncDriver，
//! 其后台任务以 TsavoriteLog 迭代器从 startAddress 逐记录驱动
//! AofSyncTask.Consume（经 GarnetClientSession 逐记录转发）→ Throttle 刷新
//! 背压水位（libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume/Throttle）。
//! C# ReplicaSyncSession（Diskbased/Diskless 两形）只是该链的两个调用侧，
//! 其文件本体由本仓 replica_sync_session.rs /
//! diskless_replication/replica_sync_session.rs 分别承接，不经本模块锚定。
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
//! Rust 无每副本常驻任务，由单条常驻循环
//! [`AofReplicationPump::start_throttle_loop`] 承接同等节流：事件与
//! replica-sync-delay 周期竞速，事件臂即时节流，超时臂即 C# 常驻泵的空转轮
//! （idle 兜底发布 / stall 心跳脉冲 / 断连副本退场感知三责在该周期上永续
//! 运转，与有无写入无关）。

use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::{runtime::spawn, time::timeout};
use crossfire::{
  AsyncRx, MAsyncTx,
  mpsc::{Array, bounded_async},
};
use parking_lot::Mutex;
use waof::{WalLog, WalScanIterator};
use wbase::hex::hex_str_u128;
use wconf::RuntimeServerConfig;
use wdev::Device;

use super::{
  aof_sync_driver::AofSyncDriverStore,
  replica_replay_task::{REPLAY_CHUNK_BYTES, current_sync_delay},
};

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

  /// 启动常驻节流循环（事件与周期竞速，静默期深度被动休眠 0% CPU）
  ///
  /// 对标 C# 常驻节流机制（BulkConsumeAllAsync 每轮 TryBulkConsumeNext 开头
  /// 无条件 `consumer.Throttle()` 的永续周期形态）：
  /// 1. 事件臂：新日志分发或位点前移立即唤醒，实时刷新水位至背压闸门
  ///    （容量 1 通道折叠事件洪流）；
  /// 2. 周期臂：replica-sync-delay 超时空转轮承接 C# 常驻泵的空转轮——
  ///    idle 兜底发布（对标 C# idle == true 语义，解冻闸门内停滞的追加方）、
  ///    stall 心跳脉冲与断连副本退场感知（throttle_all 内出册）在该周期上
  ///    永续运转，与有无写入无关；周期每轮现取 replica-sync-delay 槽位
  ///    （CONFIG SET 即时生效），无配置句柄形态回落兜底常量。
  pub fn start_throttle_loop(&self, runtime_config: Option<Arc<RuntimeServerConfig>>) {
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
        // 事件与周期竞速：事件臂即时节流；超时臂为无事件空转轮，节流扫描
        // 照常执行（idle 兜底发布 + 断连副本退场感知）。周期每轮现取
        // replica-sync-delay 槽位，CONFIG SET 即时生效
        let delay = current_sync_delay(runtime_config.as_deref());
        match timeout(delay, rx.recv()).await {
          Ok(Ok(())) => {
            store.throttle_all();
          }
          Ok(Err(_)) => return, // 通道关闭退出
          Err(_) => {
            store.throttle_all();
          }
        }
      }
    })
    .detach();
  }

  /// 注册推流唤醒信号并启动增量拉取循环
  ///
  /// libs/cluster/Server/Replication/PrimaryOps/ReplicationPrimaryAofSync.cs:TryConnectToReplica
  ///
  /// C# 主端发起 AOF 增量推流的建流任务点（diskbased / diskless 两个
  /// ReplicaSyncSession 侧同位调用）：端点反查 + 请求位点对本地日志尾的
  /// 越界校验 + `Task.Run(aofSyncDriver.RunAsync)` 派生常驻推流。rust 侧
  /// 前两步上移至会话链（端点反查在 cluster_session 入口 get_endpoint_from_node_id、
  /// 位点校验并入策略协商 DataLossCheck），本处承接其核心的第三步——挂
  /// 推流唤醒并启动常驻增量循环，即 RunAsync 常驻任务的本仓形态：
  ///
  /// 推流形态：WalLog 入队完成后发容量 1 唤醒信号（满即折叠，多帧写入只留
  /// 一个），本循环被唤醒后按各副本已发位点从环形缓冲拉取新帧——推流序与
  /// AOF 地址序原子一致（同一环形缓冲的线性化序），并发写入下从侧应用序与
  /// 主侧重启重放序永不发散。数据面不流经信号：帧在 WalLog 环形缓冲内，
  /// 按 [`WalLog::safe_tail_address`] 安全可读面拉取。
  ///
  /// 对标 C# AofSyncTask 周期泵：实时增量路径由入队信号驱动（compio 任务
  /// 唤醒，零轮询），位点衔接判定与逐记录 Consume 语义不变。仅当地址与副本
  /// 已发位点严格衔接时分发，落后副本由同一信号下的连泵循环补齐（拉取按
  /// 地址序，无需单独补扫路径）。返回 false 表示信号端已被注册（重复
  /// attach 同一 WalLog，既有循环覆盖全部驱动）。
  pub fn attach_wake<D: Device + 'static>(&self, wal: &Arc<WalLog<D>>) -> bool {
    let (tx, rx) = bounded_async::<()>(1);
    if !wal.set_replication_wake(tx) {
      return false;
    }
    let store = Arc::clone(&self.store);
    let trigger = self.throttle_tx.clone();
    let wal = Arc::clone(wal);
    spawn(async move {
      while rx.recv().await.is_ok() {
        // 连泵至无进展：单趟窗口受限后剩余积压由信号续泵，连泵循环补齐
        //（全部副本追平或无积压即回信号挂起，无空转）。扫描错误已在泵内
        // 逐副本就地出册处置，泵体不再向驱动侧抛错——无死 Err 臂残留
        loop {
          let (forwarded, _skipped) = pump_backlog(&store, &trigger, &wal).await;
          if forwarded == 0 {
            break;
          }
        }
      }
    })
    .detach();
    true
  }

  /// 补扫存量积压：各副本从已发位点扫至日志尾，逐记录转发 + 节流重报
  ///
  /// 对标 C# AofSyncTask 迭代器从 startAddress 的历史扫描（attach 时刻之前的
  /// 记录不经推流端口，须由迭代器补齐）。单物理子日志拓扑由装配层强制保证：
  /// `boot.rs` 复制域装配段对 `aof_physical_sublog_count != 1` 直接报错退出，
  /// 故本泵取 driver 的 sublog 0 即全量，泵侧不设 N != 1 冗余断言。分片内核
  /// 已逐子日志就位（GarnetLog/ShardedLog，核实见 task/done/sublog-
  /// single-constraint.md），扇出接通后本函数按子日志内层循环 task_ref(i) +
  /// 对应 WalLog 扫描，复用既有单点、不建第二套泵。返回
  /// (转发记录数, 跳过记录数)。
  ///
  /// 签名保留 `io::Result` 外形态（attach 装配链契约不变，调用侧 `?` 兜底
  /// 语义不动）；泵体错误面已收敛为逐副本就地出册处置（见
  /// [`pump_backlog`] 扫描错误臂），本函数当前无错误产生路径
  pub async fn sync_backlog<D: Device>(&self, wal: &WalLog<D>) -> io::Result<(u64, u64)> {
    // 连泵至无进展：单趟窗口受限后一轮只推进各副本一个窗口，补扫语义
    //（按位点扫至日志尾）由连泵循环完成
    let mut total_forwarded = 0u64;
    let mut total_skipped = 0u64;
    loop {
      let (forwarded, skipped) = pump_backlog(&self.store, &self.throttle_tx, wal).await;
      total_forwarded += forwarded;
      total_skipped += skipped;
      if forwarded == 0 {
        return Ok((total_forwarded, total_skipped));
      }
    }
  }
}

/// 推流单飞闸守卫：持有期标记驱动在泵，Drop 释放（泵体错误面已收敛为
/// 逐副本就地出册，无早退路径，正常扫完即随作用域释放）
struct PumpGuard<'a>(&'a AtomicBool);

impl Drop for PumpGuard<'_> {
  fn drop(&mut self) {
    self.0.store(false, Ordering::Release);
  }
}

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:RunAofSyncTaskAsync
///
/// 推流拉取主体（自由函数：信号唤醒循环与同步补扫共用，无 self 依赖）。
/// C# RunAofSyncTaskAsync 为每子日志常驻任务的主体：建连（ConnectAsync）→
/// init 帧（ExecuteClusterAppendLogInit）→ BulkConsumeAllAsync 扫描推流；
/// rust 拓扑重排为事件驱动——建连与 init 帧段由 `TcpSessionWire::connect`
/// 承接（见其文档），本函数承接扫描推流主体（按副本位点扫至日志尾，
/// 逐记录 [`super::aof_sync_task::AofSyncTask::consume`] 转发），无常驻任务。
///
/// 各副本从推流游标（accepted_address，含溢流在途帧）扫至
/// [`WalLog::safe_tail_address`]，逐记录转发；扫描 I/O 错误（设备读故障 /
/// CRC 损坏）与消费断连或位错均就地出册退场并终止本副本本轮（对标 C#
/// AofSyncDriver.cs:RunAsync catch(Exception) + finally
/// aofSyncDriverStore.TryRemove(this) 的统一退场契约：推流链路任何致命错误
/// （含 AOF 扫描 I/O 错误）该副本驱动必然出册并断链，出册经
/// try_remove_current 内重报水位，截断线与闸门不再被该副本钳制——
/// 死副本驱动不再留册，AOF 截断线与背压闸门不被失联位点钉制）。有进展的
/// 副本刷新背压水位（对标 C# 迭代间隙的 Throttle）。游标
/// 刻意与已发送水位（previous_address）分离：溢流滞留帧未落通道不计水位，
/// 但也不可重拉重发（副本端要求帧地址严格衔接日志尾，重发同帧即 Divergent
/// 断流）。
///
/// 单飞闸：信号唤醒循环与 attach 期手动补扫可能并发进入本函数（attach 链
/// 先 attach_wake 挂循环、随即 sync_backlog 补扫，写负载下信号循环同刻被
/// 唤醒），原子位保证同一驱动同一时刻至多一条泵——双泵对同一
/// accepted_address 起扫时后到者 consume 复帧报 InvalidInput，健康驱动被
/// try_remove_current 出册 dispose，而 INITIATE_REPLICA_SYNC 已回 +OK、
/// 副本重放驱动使 ensure_replication 被抑制，主端无驱动、副本会话存活，
/// 数据面死锁至手工干预。在泵驱动直接跳过：另一条泵正覆盖同一地址区间，
/// 补扫语义（按位点扫至日志尾）由在泵者天然完成。对标 C# 每副本唯一常驻
/// 泵 RunAsync 的单飞形态（AofSyncDriver.cs）
///
/// 单趟窗口 + 轮转（对标 C# BulkConsumeAllAsync maxChunkSize 分块与每副本
/// 独立任务的并发推进）：内层扫描收敛为单窗口单趟（窗口上限复用副本重放
/// 同款 [`super::replica_replay_task::REPLAY_CHUNK_BYTES`]），一轮只推进各
/// 副本一个窗口即换下一个副本，剩余积压由调用方连泵续扫——追尾 loop 不设
/// 界时持续写入下首驱动 from < until 恒成立永不退出，快照序靠后的副本在本
/// 轮永不被触达（稳态饿死）；起始下标取 store 轮转游标模快照长度并推进，
/// 多副本按轮公平分时，不引入每副本第二条常驻任务（避免双机制）
async fn pump_backlog<D: Device>(
  store: &AofSyncDriverStore,
  throttle_tx: &MAsyncTx<Array<()>>,
  wal: &WalLog<D>,
) -> (u64, u64) {
  let drivers = store.drivers();
  if drivers.is_empty() {
    return (0, 0);
  }
  let start = store.next_pump_cursor() % drivers.len();
  let mut total_forwarded = 0u64;
  let mut total_skipped = 0u64;

  for offset in 0..drivers.len() {
    let driver = &drivers[(start + offset) % drivers.len()];
    let Some(task) = driver.task_ref(0) else {
      continue;
    };
    // 单飞闸：已被其他泵接管即在泵，跳过本副本（AcqRel 交换即获取）
    if driver.pumping.swap(true, Ordering::AcqRel) {
      continue;
    }
    let _pumping = PumpGuard(&driver.pumping);

    // 单窗口单趟：只扫 [from, from + 窗口) 一段，剩余积压由连泵循环续扫
    let from = task.accepted_address().max(0) as u64;
    let until = wal
      .safe_tail_address()
      .min(from + REPLAY_CHUNK_BYTES as u64);
    let mut forwarded = 0u64;
    let mut skipped = 0u64;
    let mut scan_failed = false;
    if from < until {
      let mut iter: WalScanIterator<D> = wal.scan(from, until);
      loop {
        let frame = match iter.next_frame().await {
          Ok(Some(frame)) => frame,
          Ok(None) => break,
          Err(err) => {
            // 扫描 I/O 错误（设备读故障 / 内存窗真实 CRC 损坏，waof iterator
            // 排除一切平滑终止分支后的真致命路径）：与同函数 consume 败臂同
            // 口径就地处置——warn（含副本节点、错误原文与扫描位点）→
            // try_remove_current 出册（内含 dispose 断链 + 闸门重报，副本端
            // 感知断连走 ensure_replication 重同步自愈）→ skipped 计数后换
            // 下一个驱动。绝不再以 `?` 抛离整轮泵：冻结在册位点会永久钉死
            // AOF 截断线与背压闸门，单副本读故障被放大为主端全量写面冻结。
            // 注：瞬时 I/O 错同样即出册断链，系与 C# RunAsync catch-all +
            // finally TryRemove 同形的刻意代价（出册→重挂→重同步为既有闭环）
            log::warn!(
              "AOF stream scan failed for replica {}: {} (scan from: {from})",
              hex_str_u128(driver.remote_node_id()),
              err
            );
            skipped += 1;
            store.try_remove_current(driver);
            scan_failed = true;
            break;
          }
        };
        // 帧直组转发：直接消费扫描面直组完整帧（8B 记录头 + 负载，一次分配零重拷），
        // 消除 reconstruct_frame 的二次分配重拷，推流端口帧口径严格对齐
        match task.consume(
          &frame.frame,
          frame.address as i64,
          frame.next_address as i64,
        ) {
          Ok(()) => forwarded += 1,
          Err(err) => {
            // 异常捕获与可观测性（对标 C# AofSyncTask.cs:196-207 LogError 字段：
            // 记录远端节点 ID、错误详情与当前 accepted 位点）
            log::warn!(
              "AOF stream consume failed for replica {}: {} (accepted: {})",
              hex_str_u128(driver.remote_node_id()),
              err,
              task.accepted_address()
            );
            // 断连或位错：驱动退场出册并终止本副本补扫（对标 C# Consume 异常
            // 上抛 → RunAsync finally aofSyncDriverStore.TryRemove(this) 的
            // 统一退场语义；实例匹配防误删并发重挂的同节点新驱动，死副本
            // 出册即解除截断线与背压闸门的位点钉制）
            skipped += 1;
            store.try_remove_current(driver);
            break;
          }
        }
      }
    }

    if scan_failed {
      // 肇事驱动已出册退场，本轮不再为其发布水位（重报已在出册内完成）
      total_skipped += skipped;
      continue;
    }

    if forwarded > 0 {
      // 对标 C# 迭代间隙的 Throttle：推送进展后刷新背压水位
      store.throttle_replica(driver.remote_node_id());
      let _ = throttle_tx.try_send(());
    }
    total_forwarded += forwarded;
    total_skipped += skipped;
  }

  (total_forwarded, total_skipped)
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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use parking_lot::Mutex;
  use waof::{AofAddress, WalConfig};
  use wdev::SegmentedDevice;

  use super::*;
  use crate::server::replication::{
    aof_sync_driver::AofSyncDriver,
    replica_wire::test_wire::{CallbackWire, FrameSink},
  };

  /// 单趟窗口受限 + 轮转公平（对标 C# BulkConsumeAllAsync maxChunkSize 分块
  /// 与每副本独立任务的并发推进）：积压超过单趟窗口时一轮 pump_backlog 只
  /// 推进各副本一个窗口即换驱动——首驱动不再追尾扫至日志尾无限占泵，
  /// 快照序靠后的副本同轮被触达
  #[test]
  fn pump_backlog_single_window_rotates_across_drivers() {
    Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(
        SegmentedDevice::single_file(dir.path().join("primary.wal")).expect("create wal device"),
      );
      let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"));

      wal.enqueue(&[0u8; 56]).unwrap();
      // 积压 2048 × 1024B ≈ 2MB > 单趟窗口 REPLAY_CHUNK_BYTES (1MB)
      for _ in 0..2048u32 {
        wal.enqueue(&[0u8; 1024]).unwrap();
      }
      wal.commit().await.unwrap();
      let tail = wal.tail_address() as i64;

      let store = Arc::new(AofSyncDriverStore::new(1));
      let driver_a = Arc::new(AofSyncDriver::new(
        1,
        0xA,
        1,
        &AofAddress::create(1, 64),
        None,
      ));
      let driver_b = Arc::new(AofSyncDriver::new(
        1,
        0xB,
        1,
        &AofAddress::create(1, 64),
        None,
      ));
      driver_a.attach_wire(Arc::new(CallbackWire::new(FrameSink::Buffer(Arc::new(
        Mutex::new(Vec::new()),
      )))));
      driver_b.attach_wire(Arc::new(CallbackWire::new(FrameSink::Buffer(Arc::new(
        Mutex::new(Vec::new()),
      )))));
      assert!(store.try_add_replication_driver(driver_a.clone(), false));
      assert!(store.try_add_replication_driver(driver_b.clone(), false));

      let pump = AofReplicationPump::new(Arc::clone(&store));
      let (forwarded, skipped) = pump_backlog(&store, &pump.throttle_tx, &wal).await;
      assert_eq!(skipped, 0);
      assert!(forwarded > 0);

      // 副本 A 未追平：单趟窗口封顶，不再追尾扫至日志尾
      let pos_a = driver_a.get_previous_address(0);
      assert!(
        pos_a < tail,
        "单趟窗口受限，首驱动不得在一轮内追平全量积压（{pos_a} vs {tail}）"
      );
      // 副本 B 同轮被触达：轮转后推进越过起点（追尾 loop 不设界时 B 在
      // 持续写入下永不被触达）
      let pos_b = driver_b.get_previous_address(0);
      assert!(pos_b > 64, "轮转游标应使后续副本同轮被触达（{pos_b}）");
    });
  }
}
