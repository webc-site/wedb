//! AOF 泵扫描错误臂出册回归（工单 wedb-aof-pump-scan-error-driver-no-eviction）
//!
//! 对标 C# 契约：libs/cluster/Server/Replication/PrimaryOps/AofOperations/
//! AofSyncDriver.cs:RunAsync 的 catch(Exception) + finally
//! aofSyncDriverStore.TryRemove(this)（:166-177）统一退场收口——推流链路任何
//! 致命错误（含 AOF 扫描 I/O 错误）该副本驱动必然出册并断链，出册经重报水位，
//! 截断线与背压闸门不再被该副本钳制。
//!
//! 注入形态仿 whlog/tests/hlog/flaky_device.rs：位点先 commit 刷出内存环形窗，
//! 迫使泵扫描走设备冷读路径，在冻结驱动扫描区指定位点以下持续返回读错误
//! （对标 waof iterator.rs fetch_window 设备 I/O 错误上抛形态 :334），之上
//! 正常下发——同册双健康驱动，断言三点：
//! 1. 扫描错误驱动即出册断链，另一驱动续转追平，泵体不再向驱动侧抛错；
//! 2. 出册重报后 safe_truncate_aof 截断线可越过原冻结位点；
//! 3. 预算耗尽下闸门等待方在出册重报后即获放行。

use std::{
  io,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Error as WdevError, Result as WdevResult, SegmentedDevice};
use wedb::server::replication::{
  aof_replication_pump::AofReplicationPump,
  aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
  replica_wire::test_wire::{CallbackWire, FrameSink},
};
use wnode::aof::aof_backpressure::AofBackpressure;

/// 内存环形写窗容量（64KB，扇区大小整数倍）
const RING_CAP: usize = 64 * 1024;
/// 首记录负载 56B + 8B 帧头 = 64B：其后数据记录自地址 64 起衔接
const PAD_PAYLOAD: usize = 56;
/// 数据记录条数
const RECORDS: usize = 512;
/// 单条数据记录负载字节
const PAYLOAD: usize = 1024;
/// 单条数据记录帧长（负载 + 8B 帧头）
const FRAME: i64 = (PAYLOAD as i64) + 8;
/// 首条数据记录位点（肇事驱动冻结位点）
const FROZEN_ADDR: i64 = 64;
/// 数据记录区末端位点
const DATA_END: i64 = FROZEN_ADDR + (RECORDS as i64) * FRAME;
/// 持续读故障注入区上界：低于该位点的设备读恒失败
const FAIL_BELOW: u64 = 300_000;
/// 健康副本 B 起始位点（冷读区、注入区之上）
const START_B: i64 = FROZEN_ADDR + 300 * FRAME;
/// 出册后截断线推进目标（原冻结位点与 B 位点之间）
const TRUNC_TARGET: i64 = 200_000;

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_FROZEN: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000A;
const REPLICA_HEALTHY: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000B;

/// 指定位点以下持续读故障注入设备（仿 whlog flaky_device 注入闸门形态）：
/// 命中注入区的 read_aligned/read_raw 持续上抛 [`WdevError::Io`]，令
/// WalScanIterator 冷读经 fetch_window 设备错误路径失败；其余面全量透传
/// 内层 SegmentedDevice
struct ScanFailDevice {
  inner: SegmentedDevice,
  /// 注入区上界：低于该逻辑位点的读恒失败
  fail_below: u64,
  /// 注入命中计数（断言注入实际生效，杜绝假 mock 空转）
  hits: AtomicU64,
}

impl ScanFailDevice {
  fn new(inner: SegmentedDevice, fail_below: u64) -> Self {
    Self {
      inner,
      fail_below,
      hits: AtomicU64::new(0),
    }
  }

  /// 读故障闸门：命中注入区计数并返回模拟介质读错误
  fn read_gate(&self, offset: u64) -> Option<WdevError> {
    if offset < self.fail_below {
      self.hits.fetch_add(1, Ordering::Relaxed);
      return Some(WdevError::Io(io::Error::other(format!(
        "simulated media read failure at offset {offset}"
      ))));
    }
    None
  }

  fn hits(&self) -> u64 {
    self.hits.load(Ordering::Relaxed)
  }
}

impl Device for ScanFailDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  #[inline]
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (WdevResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (WdevResult<usize>, AlignedBuf) {
    match self.read_gate(offset) {
      Some(e) => (Err(e), buf),
      None => self.inner.read_aligned(offset, buf).await,
    }
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (WdevResult<usize>, AlignedBuf) {
    match self.read_gate(offset) {
      Some(e) => (Err(e), buf),
      None => self.inner.read_raw(offset, buf).await,
    }
  }

  async fn sync(&self) -> WdevResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> WdevResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }

  #[inline]
  fn get_file_size(&self, segment_id: u32) -> WdevResult<u64> {
    self.inner.get_file_size(segment_id)
  }
}

/// 每批入队记录数：批字节 32×1032 + 扇区圆整 < RING_CAP，保证环内预占不越界
const BATCH: usize = 32;

/// 搭建冻结场景：双驱动同册，其中 REPLICA_FROZEN 起始位点落在刷出内存窗后
/// 的持续读故障区（每轮扫描必复现），REPLICA_HEALTHY 起始位点在故障区之上、
/// 仍处冷读区（设备读正常下发）
async fn setup(dir: &Path) -> (Arc<ScanFailDevice>, Arc<WalLog<ScanFailDevice>>) {
  let device = Arc::new(ScanFailDevice::new(
    SegmentedDevice::single_file(dir.join("primary.wal")).expect("create wal device"),
    FAIL_BELOW,
  ));
  // 小内存窗配置：位点先 flush 出内存窗，迫使泵扫描走设备冷读路径
  let wal =
    Arc::new(WalLog::new(Arc::clone(&device), WalConfig::new(RING_CAP)).expect("create wal"));
  wal.enqueue(&[0u8; PAD_PAYLOAD]).unwrap();
  // 分批入队 + 分批 commit_flush_only 腾窗（不写 commit 帧，地址面纯数据帧，
  // 环内预占恒不超容量），全部记录随批次落盘并驱逐出内存窗
  for _ in 0..RECORDS / BATCH {
    for _ in 0..BATCH {
      wal.enqueue(&[0u8; PAYLOAD]).unwrap();
    }
    wal.commit_flush_only().await.unwrap();
  }

  // 注入几何自检：肇事位点与健康位点都必须在内存窗外（冷读），且分别落于
  // 注入区内 / 注入区上界之上
  assert_eq!(wal.tail_address() as i64, DATA_END);
  let cold_floor = wal.tail_address() as i64 - RING_CAP as i64;
  assert!(FROZEN_ADDR < FAIL_BELOW as i64 && FROZEN_ADDR < cold_floor);
  assert!(
    START_B >= FAIL_BELOW as i64 && START_B < cold_floor,
    "健康驱动位点须已 flush 出内存窗且位于注入区之上"
  );
  (device, wal)
}

fn add_driver(
  store: &Arc<AofSyncDriverStore>,
  remote: u128,
  start: i64,
  sink: Arc<Mutex<Vec<Vec<u8>>>>,
) -> Arc<AofSyncDriver> {
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    remote,
    1,
    &AofAddress::create(1, start),
    None,
  ));
  driver.attach_wire(Arc::new(CallbackWire::new(FrameSink::Buffer(sink))));
  assert!(store.try_add_replication_driver(Arc::clone(&driver), false));
  driver
}

/// 断言一：扫描错误驱动就地出册断链，另一驱动同轮续转追平，泵体收 Ok 化
#[test]
fn scan_error_evicts_frozen_driver_and_sibling_keeps_pumping() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let (device, wal) = setup(dir.path()).await;

    let store = Arc::new(AofSyncDriverStore::new(1));
    let frozen = add_driver(&store, REPLICA_FROZEN, FROZEN_ADDR, Default::default());
    let healthy = add_driver(&store, REPLICA_HEALTHY, START_B, Default::default());

    let pump = AofReplicationPump::new(Arc::clone(&store));
    // 旧形态：扫描错误经 `?` 抛离泵体 → 本调用返回 Err、肇事驱动留册；
    // 新契约：Ok 返回，错误驱动出册、健康驱动同轮补齐
    let (forwarded, skipped) = pump
      .sync_backlog(&wal)
      .await
      .expect("泵体错误面已收敛为逐副本就地出册，不得再向驱动侧抛错");
    assert!(forwarded > 0, "健康驱动应转发积压");
    assert_eq!(skipped, 1, "肇事驱动应恰好出册一次");
    assert!(device.hits() > 0, "读故障注入必须实际命中");

    assert_eq!(store.count(), 1, "扫描错误驱动应已出册");
    assert_eq!(store.drivers()[0].remote_node_id(), REPLICA_HEALTHY);
    assert!(
      !frozen.is_connected(),
      "出册驱动应经 try_remove_current 内 dispose 断链"
    );
    assert!(
      healthy.get_previous_address(0) == DATA_END,
      "另一驱动应续转追平至数据记录末端（实际 {}，期望 {DATA_END}）",
      healthy.get_previous_address(0)
    );
  });
}

/// 断言二 + 三：出册重报后截断线越过原冻结位点、预算耗尽下闸门等待方即获放行
#[test]
fn scan_error_eviction_unpins_truncate_line_and_backpressure_gate() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let (_device, wal) = setup(dir.path()).await;

    let store = Arc::new(AofSyncDriverStore::new(1));
    // 每子日志预算 2048B：冻结位点钉死闸门后尾差远超预算
    let bp = Arc::new(AofBackpressure::new(1, 2048));
    store.attach_backpressure(Some(Arc::clone(&bp)));
    add_driver(&store, REPLICA_FROZEN, FROZEN_ADDR, Default::default());
    let healthy = add_driver(&store, REPLICA_HEALTHY, START_B, Default::default());
    assert_eq!(
      bp.get_shipped_watermark(0),
      FROZEN_ADDR,
      "冻结驱动应把闸门水位钳在位点 {FROZEN_ADDR}"
    );

    // 入队背压等待方：尾差超预算即挂起（对照 wnode 既有背压等待形态）
    let bp_wait = Arc::clone(&bp);
    let released = Arc::new(AtomicBool::new(false));
    let released_flag = Arc::clone(&released);
    spawn(async move {
      bp_wait.wait_async(0, DATA_END).await;
      released_flag.store(true, Ordering::Release);
    })
    .detach();
    sleep(Duration::from_millis(50)).await;
    assert!(
      !released.load(Ordering::Acquire),
      "冻结位点钉死闸门期间等待方必须挂起"
    );

    let pump = AofReplicationPump::new(Arc::clone(&store));
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.expect("泵体不得抛错");
    assert!(forwarded > 0);
    assert_eq!(skipped, 1);

    // 断言二：出册重报后截断线可越过原冻结位点（旧形态肇事驱动留册，
    // min_active 恒被钳在 FROZEN_ADDR，截断点越不过去）
    let limit = store
      .safe_truncate_aof(&AofAddress::create(1, TRUNC_TARGET))
      .await;
    assert_eq!(
      limit.get(0),
      Some(TRUNC_TARGET),
      "截断线应越过原冻结位点 {FROZEN_ADDR}，实际 {limit:?}"
    );

    // 断言三：健康驱动追平重报后，等待方在出册后即获放行
    assert!(healthy.get_previous_address(0) == DATA_END);
    for _ in 0..100 {
      if released.load(Ordering::Acquire) {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(
      released.load(Ordering::Acquire),
      "出册重报后闸门水位应解除尾差钳制放行等待方，实际水位 {}",
      bp.get_shipped_watermark(0)
    );
    assert!(bp.get_shipped_watermark(0) > FROZEN_ADDR);
  });
}
