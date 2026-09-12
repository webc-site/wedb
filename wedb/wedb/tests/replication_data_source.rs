//! AOF 复制数据源闭环集成测试：WalLog 记录流 → AofSyncDriver 逐副本转发
//!
//! 对标 C# ReplicaSyncSession + AofSyncTask 的数据面形态（迭代器补扫 + 实时
//! 推流），验证「读 AOF 记录 → 按 replica 位点发送」的最小闭环：
//! - attach_sink 前的存量积压由 sync_backlog 补扫转发
//! - attach_sink 后的新写入同栈逐副本分发（推流序与地址序原子一致）
//! - 位点推进、发送缓冲记账与背压水位联动

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::replication::{
  AofBackpressureFace, aof_replication_pump::AofReplicationPump, aof_sync_driver::AofSyncDriver,
  aof_sync_driver_store::AofSyncDriverStore,
};

/// 首条记录负载：8B 记录头 + 56B 负载 = 64B，对齐 Garnet AOF 的
/// kFirstValidAofAddress 头区语义（waof 无保留区的刻意差异下，以等效
/// 记录占位使副本起始位点 64 精确对齐首条业务记录）
const HEADER_PAD_PAYLOAD_LEN: usize = 56;

fn open_wal(dir: &tempfile::TempDir) -> Arc<WalLog<SegmentedDevice>> {
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join("primary.wal")).expect("create wal device"),
  );
  Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"))
}

#[test]
fn aof_record_stream_pumps_to_replica_drivers() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(&dir);

    // 1. 头区占位记录（[0, 64)，对齐 Garnet kFirstValidAofAddress 语义）
    wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
    // 2. attach 前写入 3 条积压记录
    for i in 0..3u8 {
      wal.enqueue(format!("backlog-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();
    let backlog_tail = wal.tail_address();

    // 3. 建驱动仓库与副本驱动（起始位点 64 = 首条业务记录地址）
    let store = Arc::new(AofSyncDriverStore::new(1));
    let driver = Arc::new(AofSyncDriver::new(
      "primary-1".to_string(),
      "replica-1".to_string(),
      &AofAddress::create(1, 64),
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));

    // 4. 接入推流泵（此后新写入同栈分发）
    let pump = AofReplicationPump::new(Arc::clone(&store));
    assert!(pump.attach_sink(&wal));

    // 5. 先补扫 attach 前积压（对标 C# 副本 attach 的迭代器历史扫描：
    // 衔接先前，实时分发才生效）
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
    assert_eq!((forwarded, skipped), (3, 0), "积压 3 条全部补扫转发");
    assert_eq!(
      driver.get_previous_address(0) as u64,
      backlog_tail,
      "补扫后已发位点应追平积压尾"
    );

    // 6. attach 后写入 2 条实时记录（推流端口路径：同栈逐副本分发）
    for i in 0..2u8 {
      wal.enqueue(format!("live-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();
    let final_tail = wal.tail_address();

    // 7. 位点闭环断言：副本已发位点追平主侧尾
    let task = driver.get_task(0).unwrap();
    assert_eq!(
      task.previous_address() as u64,
      final_tail,
      "实时分发 + 补扫后已发位点应等于日志尾"
    );
    assert_eq!(task.shipped_watermark_address() as u64, final_tail);
    assert_eq!(task.start_address(), 64);
    assert!(backlog_tail < final_tail);

    // 8. 发送缓冲记账：5 条业务帧（3 积压 + 2 实时）均进入在途字节统计
    //（对标 C# networkPool.GetStats 的观测面，验证帧确实进入了发送面）
    assert!(
      task.current_inflight_bytes() > 0,
      "转发的帧应计入发送缓冲在途字节"
    );

    // 9. ACK 推进副本确认位点（闭环的下游回执）
    assert!(driver.process_ack(0, final_tail as i64));
    assert_eq!(driver.get_acked_address(0) as u64, final_tail);
  });
}

/// 迟到副本（attach 晚于实时写入）由补扫路径从自身起始位点追平
#[test]
fn late_attach_replica_catches_up_via_backlog() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(&dir);

    wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
    for i in 0..4u8 {
      wal.enqueue(format!("record-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();

    // 泵先 attach（此时无副本，实时写入暂不分发）
    let store = Arc::new(AofSyncDriverStore::new(1));
    let pump = AofReplicationPump::new(Arc::clone(&store));
    assert!(pump.attach_sink(&wal));

    wal.enqueue(b"tail-record").unwrap();
    wal.commit().await.unwrap();
    let tail = wal.tail_address();

    // 迟到副本注册后一次补扫追平（含 sink 未分发的全部存量）
    let driver = Arc::new(AofSyncDriver::new(
      "primary-1".to_string(),
      "replica-late".to_string(),
      &AofAddress::create(1, 64),
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
    assert_eq!((forwarded, skipped), (5, 0), "5 条存量记录全部补扫转发");
    assert_eq!(driver.get_previous_address(0) as u64, tail);
  });
}

/// 断连副本补扫终止语义：Consume 拒绝后停止转发（对标 C# 迭代器异常终止）
#[test]
fn disconnected_replica_backlog_terminates() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(&dir);

    wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
    for i in 0..3u8 {
      wal.enqueue(format!("record-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();

    let store = Arc::new(AofSyncDriverStore::new(1));
    let driver = Arc::new(AofSyncDriver::new(
      "primary-1".to_string(),
      "replica-dead".to_string(),
      &AofAddress::create(1, 64),
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));
    driver.get_task(0).unwrap().set_connected(false);

    let pump = AofReplicationPump::new(store);
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
    assert_eq!((forwarded, skipped), (0, 1), "断连副本首条即拒绝并终止");
    assert_eq!(driver.get_previous_address(0), 64, "位点不推进");
  });
}

struct MockGate {
  tail: Arc<AtomicI64>,
  budget: i64,
  watermark: AtomicI64,
}

impl MockGate {
  fn new(budget: i64, tail: Arc<AtomicI64>) -> Self {
    Self {
      tail,
      budget,
      watermark: AtomicI64::new(0),
    }
  }

  fn any_stalled(&self) -> bool {
    let w = self.watermark.load(Ordering::Acquire);
    let t = self.tail.load(Ordering::Acquire);
    (t - w) > self.budget
  }
}

impl AofBackpressureFace for MockGate {
  fn publish_shipped_address(&self, _idx: usize, min_shipped: i64) {
    self.watermark.store(min_shipped, Ordering::Release);
  }

  fn publish_delta_bytes(&self) -> i64 {
    self.budget / 8
  }
}

/// 常驻节流循环的空闲发布语义（对标 C# BulkConsumeAllAsync 空闲路径：
/// 流耗尽时每 REPLICA_SYNC_DELAY 毫秒 Task.Delay 后下一轮
/// TryBulkConsumeNext 开头 consumer.Throttle()——idle 分支重报水位）
#[test]
fn throttle_loop_publishes_idle_watermark() {
  Runtime::new().unwrap().block_on(async {
    let tail = Arc::new(AtomicI64::new(9_000));
    let gate = Arc::new(MockGate::new(4096, Arc::clone(&tail)));

    let store = Arc::new(AofSyncDriverStore::new(1));
    store.attach_backpressure(Some(gate.clone()));
    let driver = Arc::new(AofSyncDriver::new(
      "primary-1".to_string(),
      "replica-1".to_string(),
      &AofAddress::create(1, 0),
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));
    assert!(gate.any_stalled(), "副本 attach 后水位收紧应停滞");

    let task = driver.get_task(0).unwrap();
    task.consume(b"bulk", 64, 9_000).unwrap();
    assert!(
      store.throttle_replica("replica-1"),
      "首推增量 9000 ≥ 512 触发发布"
    );
    assert!(!gate.any_stalled(), "水位 9000 追平尾 9000 应放行");

    // 副本小幅推进（增量 400 < 512）+ 主侧尾大幅推进（差 4300 > 预算）
    task.consume(b"tail", 9_000, 9_400).unwrap();
    tail.store(13_300, Ordering::Release);
    assert!(
      !store.throttle_replica("replica-1"),
      "增量不足且非空闲首测 → 不发布"
    );
    assert!(gate.any_stalled(), "水位仍 9000：尾差 4300 > 4096 停滞");

    // 常驻节流循环承接 C# 空闲路径：下一轮 idle 分支发布 9400 解除停滞
    let pump = AofReplicationPump::new(Arc::clone(&store));
    pump.start_throttle_loop(Duration::from_millis(5));
    sleep(Duration::from_millis(30)).await;
    assert!(
      !gate.any_stalled(),
      "常驻循环 idle 发布 9400 后尾差 3900 ≤ 4096 应放行"
    );
    // Drop 停循环（显式释放验证退出链不 panic）
    drop(pump);
  });
}
