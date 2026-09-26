//! AOF 复制数据源闭环集成测试：WalLog 记录流 → AofSyncDriver 逐副本转发
//!
//! 对标 C# ReplicaSyncSession + AofSyncTask 的数据面形态（迭代器补扫 + 实时
//! 推流），验证「读 AOF 记录 → 按 replica 位点发送」的最小闭环：
//! - attach 前的存量积压由 sync_backlog 补扫转发
//! - attach 后的新写入经入队信号唤醒增量拉取（推流序与地址序原子一致）
//! - 位点推进与背压水位联动

use std::{
  sync::{
    Arc, Barrier,
    atomic::{AtomicU64, Ordering},
  },
  thread::spawn,
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::replication::{
  aof_replication_pump::AofReplicationPump,
  aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
};
use wnode::aof::aof_backpressure::AofBackpressure;

/// 测试副本身份（内部 u128）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;
const REPLICA_2: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0003;
const REPLICA_DEAD: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000D;

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

#[compio::test]
async fn aof_record_stream_pumps_to_replica_drivers() {
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
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));

  // 4. 接入推流泵（此后新写入经信号唤醒增量拉取）
  let pump = AofReplicationPump::new(Arc::clone(&store));
  assert!(pump.attach_wake(&wal));

  // 5. 先补扫 attach 前积压（对标 C# 副本 attach 的迭代器历史扫描：
  // 衔接先前，实时分发才生效）
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  // +1 = commit 元数据帧：推流面保真传输全部帧，从侧恢复据此收敛提交上界
  assert_eq!(
    (forwarded, skipped),
    (3 + 1, 0),
    "积压 3 条与 commit 帧全部补扫转发"
  );
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
  let task = driver.task_ref(0).unwrap();
  assert_eq!(
    task.previous_address() as u64,
    final_tail,
    "实时分发 + 补扫后已发位点应等于日志尾"
  );
  assert_eq!(task.shipped_watermark_address() as u64, final_tail);
  assert_eq!(task.start_address(), 64);
  assert!(backlog_tail < final_tail);
}

/// 迟到副本（attach 晚于实时写入）由补扫路径从自身起始位点追平
#[compio::test]
async fn late_attach_replica_catches_up_via_backlog() {
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
  assert!(pump.attach_wake(&wal));

  wal.enqueue(b"tail-record").unwrap();
  wal.commit().await.unwrap();
  let tail = wal.tail_address();

  // 迟到副本注册后一次补扫追平（含 sink 未分发的全部存量）
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_2,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  // +2 = 两次 commit 各随批写出一个 commit 元数据帧（推流面保真传输全部帧）
  assert_eq!(
    (forwarded, skipped),
    (5 + 2, 0),
    "5 条存量记录与 commit 帧全部补扫转发"
  );
  assert_eq!(driver.get_previous_address(0) as u64, tail);
}

/// 断连副本补扫终止语义：Consume 拒绝后停止转发并驱动退场出册
///（对标 C# Consume 异常上抛 → RunAsync finally TryRemove(this)）
#[compio::test]
async fn disconnected_replica_backlog_terminates() {
  let dir = tempfile::tempdir().unwrap();
  let wal = open_wal(&dir);

  wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
  for i in 0..3u8 {
    wal.enqueue(format!("record-{i}").as_bytes()).unwrap();
  }
  wal.commit().await.unwrap();

  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_DEAD,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));
  driver.task_ref(0).unwrap().set_connected(false);

  let pump = AofReplicationPump::new(Arc::clone(&store));
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  assert_eq!((forwarded, skipped), (0, 1), "断连副本首条即拒绝并终止");
  assert_eq!(driver.get_previous_address(0), 64, "位点不推进");
  // 退场出册（对标 C# RunAsync finally TryRemove）：死副本驱动不再留册
  assert_eq!(store.count(), 0, "断连驱动应退场出册");
  assert!(!driver.is_connected(), "退场驱动应被 dispose 断连");
}

/// 死副本出册解除截断线钉制：断链驱动在册时 safe_truncate_aof 被死位点钳制，
/// 经 pump 补扫退场后截断线直达目标位点（对标 C# RunAsync finally 移除后
/// SafeTruncateAof 不再遍历失联驱动）
#[compio::test]
async fn dead_replica_removal_unpins_safe_truncate() {
  let dir = tempfile::tempdir().unwrap();
  let wal = open_wal(&dir);

  wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
  for i in 0..3u8 {
    wal.enqueue(format!("record-{i}").as_bytes()).unwrap();
  }
  wal.commit().await.unwrap();

  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_DEAD,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));
  // 模拟断链（对标 wire 断连后 is_connected 翻假、previous_address 永停）
  driver.dispose();

  // 死驱动仍在册：截断请求 9_000 被死位点 64 钳制
  let pump = AofReplicationPump::new(Arc::clone(&store));
  assert_eq!(
    store
      .safe_truncate_aof(&AofAddress::create(1, 9_000))
      .await
      .get(0),
    Some(64),
    "死副本在册时截断线被死位点钳制"
  );

  // 补扫退场：死驱动出册后截断线直达目标位点
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  assert_eq!((forwarded, skipped), (0, 1));
  assert_eq!(store.count(), 0, "死副本驱动应退场出册");
  assert_eq!(
    store
      .safe_truncate_aof(&AofAddress::create(1, 9_000))
      .await
      .get(0),
    Some(9_000),
    "死副本出册后截断线不再被钳制"
  );
}

/// 死副本退场后重挂续推：同键新驱动经重挂置换入库后 pump 正常推送增量
///（对标 C# 退场 TryRemove 后副本重连 TryAddReplicationDriver 重建推流）
#[compio::test]
async fn reattach_after_dead_replica_removal_resumes_pumping() {
  let dir = tempfile::tempdir().unwrap();
  let wal = open_wal(&dir);

  wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
  for i in 0..3u8 {
    wal.enqueue(format!("backlog-{i}").as_bytes()).unwrap();
  }
  wal.commit().await.unwrap();

  let store = Arc::new(AofSyncDriverStore::new(1));
  let dead = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(dead.clone(), false));
  dead.dispose();

  // 断链驱动经补扫退场出册
  let pump = AofReplicationPump::new(Arc::clone(&store));
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  assert_eq!((forwarded, skipped), (0, 1));
  assert_eq!(store.count(), 0);

  // 重挂：新驱动从当前日志尾续推（attach_replica_wire 置换形态）
  let resume = wal.tail_address() as i64;
  let fresh = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, resume),
    None,
  ));
  assert!(store.try_add_replication_driver(fresh.clone(), false));

  // 新写入正常推送（退场清理不阻碍重挂续推）
  wal.enqueue(b"post-reattach").unwrap();
  wal.commit().await.unwrap();
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  assert_eq!(skipped, 0, "重挂驱动不得被退场清理误伤");
  assert!(forwarded > 0, "重挂后应续推新增量");
  assert_eq!(
    fresh.get_previous_address(0) as u64,
    wal.tail_address(),
    "重挂驱动位点应追平新日志尾"
  );
}

/// 常驻节流循环的空闲发布语义（对标 C# BulkConsumeAllAsync 空闲路径：
/// 流耗尽时每 replica-sync-delay 毫秒 Task.Delay 后下一轮
/// TryBulkConsumeNext 开头 consumer.Throttle()——idle 分支重报水位）
#[compio::test]
async fn throttle_loop_publishes_idle_watermark() {
  let budget = 4096i64;
  let bp = Arc::new(AofBackpressure::new(1, budget));
  let tail = Arc::new(AtomicU64::new(9_000));
  bp.set_counter_log(Arc::clone(&tail));

  let store = Arc::new(AofSyncDriverStore::new(1));
  store.attach_backpressure(Some(Arc::clone(&bp)));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 0),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));
  assert!(bp.any_stalled(), "副本 attach 后水位收紧应停滞");

  let task = driver.task_ref(0).unwrap();
  task.consume(b"bulk", 64, 9_000).unwrap();
  assert!(
    store.throttle_replica(REPLICA_ID),
    "首推增量 9000 ≥ 512 触发发布"
  );
  assert!(!bp.any_stalled(), "水位 9000 追平尾 9000 应放行");

  // 副本小幅推进（增量 400 < 512）+ 主侧尾大幅推进（差 4300 > 预算）
  task.consume(b"tail", 9_000, 9_400).unwrap();
  tail.store(13_300, Ordering::Release);
  assert!(
    !store.throttle_replica(REPLICA_ID),
    "增量不足且非空闲首测 → 不发布"
  );
  assert!(bp.any_stalled(), "水位仍 9000：尾差 4300 > 4096 停滞");

  // 常驻节流循环承接 C# 空闲路径：下一轮 idle 分支发布 9400 解除停滞
  //（无配置句柄形态：空闲防抖窗口回落兜底常量 5ms）
  let pump = AofReplicationPump::new(Arc::clone(&store));
  pump.start_throttle_loop(None);
  // 有界轮询至常驻循环 idle 分支发布水位解除停滞（空闲防抖兜底 5ms，
  // 2s 上界）
  for _ in 0..400 {
    if !bp.any_stalled() {
      break;
    }
    sleep(Duration::from_millis(5)).await;
  }
  assert!(
    !bp.any_stalled(),
    "常驻循环 idle 发布 9400 后尾差 3900 ≤ 4096 应放行"
  );
  // Drop 停循环（显式释放验证退出链不 panic）
  drop(pump);
}

/// 双泵竞态回归：attach 链挂唤醒循环后手动 sync_backlog 与信号循环并发
/// 进入推流（写负载下两泵对同一 driver 从同一 accepted_address 起扫），
/// 单飞闸（driver.pumping 原子位）保证后到泵跳过在泵驱动——健康驱动不
/// 被 consume 复帧的 InvalidInput 走 try_remove_current 出册 dispose。
/// 断言 attach 后驱动仍在册、连接健康、副本有流（位点追平日志尾）且
/// 零 skip（无 InvalidInput 误判）。确定性碰撞：副本离线期写入突发构成
/// 宽积压区间，三泵（主线程 attach 补扫 + 两条跨核补扫线程）经 Barrier
/// 同时放行并扫同一区间。
#[test]
fn concurrent_pumps_keep_driver_registered() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(&dir);

    wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
    // 副本离线期写入突发：attach 前积压宽区间（4096 条）
    for i in 0..4096u32 {
      wal.enqueue(format!("backlog-{i}").as_bytes()).unwrap();
    }
    wal.commit().await.unwrap();

    let store = Arc::new(AofSyncDriverStore::new(1));
    let driver = Arc::new(AofSyncDriver::new(
      PRIMARY_ID,
      REPLICA_ID,
      1,
      &AofAddress::create(1, 64),
      None,
    ));
    assert!(store.try_add_replication_driver(driver.clone(), false));

    // attach 形态：挂信号唤醒循环
    let pump = Arc::new(AofReplicationPump::new(Arc::clone(&store)));
    assert!(pump.attach_wake(&wal));

    // 三泵同时放行：无闸时从同一 accepted_address 并扫同一积压区间，
    // 复帧 consume 必有一方 InvalidInput → try_remove_current 误删健康驱动
    let barrier = Arc::new(Barrier::new(3));
    let total_skipped = Arc::new(AtomicU64::new(0));
    let scanners: Vec<_> = (0..2)
      .map(|_| {
        let pump = Arc::clone(&pump);
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        let total_skipped = Arc::clone(&total_skipped);
        spawn(move || {
          let rt = Runtime::new().unwrap();
          barrier.wait();
          rt.block_on(async {
            let (_, skipped) = pump.sync_backlog(&wal).await.unwrap();
            total_skipped.fetch_add(skipped, Ordering::Relaxed);
          });
        })
      })
      .collect();
    barrier.wait();

    let _ = pump.sync_backlog(&wal).await.unwrap();
    for scanner in scanners {
      scanner.join().expect("补扫线程正常结束");
    }

    // 竞态后驱动仍在册且健康（对标 C# 每副本唯一常驻泵不被误删）
    assert_eq!(store.count(), 1, "双泵并发下健康驱动不得被误删出册");
    assert!(driver.is_connected(), "驱动连接健康面不得被 dispose");
    assert_eq!(
      total_skipped.load(Ordering::Relaxed),
      0,
      "无断连驱动不得产生 skip（InvalidInput 复帧误判面）"
    );

    // 副本有流：位点追平日志尾（存量 + 全部积压 + commit 帧）
    assert_eq!(
      driver.get_previous_address(0) as u64,
      wal.tail_address(),
      "副本已发位点应追平日志尾"
    );
  });
}

/// 溢流排空滞后的闸门周期臂解冻（对标 C# BulkConsumeAllAsync 流耗尽时
/// Task.Delay 后下一轮无条件 Throttle 的永续周期臂）：
/// 任务落网水位已推进而发布未跟进（溢流泵落网补账 ratchet_shipped 先行、
/// 节流事件缺席——与消费事件间隙同型的 ratchet 先行发布滞后状态）时，
/// backpressure_wait_key 先闸后排、挂起者不入队、replication_wake 永不
/// 发出，自持死锁链成立，闸门冻结写平面。常驻周期臂的超时空转轮在 delay
/// 周期内兜底发布，闸门追平日志尾、追加方放行
#[compio::test]
async fn frozen_gate_drains_via_periodic_throttle() {
  let budget = 4096i64;
  let bp = Arc::new(AofBackpressure::new(1, budget));
  let tail = Arc::new(AtomicU64::new(9_000));
  bp.set_counter_log(Arc::clone(&tail));

  let store = Arc::new(AofSyncDriverStore::new(1));
  store.attach_backpressure(Some(Arc::clone(&bp)));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 0),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));

  // 直发消费即时推进任务落网水位（对标 C# Consume 受理即计入），此后
  // 无任何节流事件：任务水位 9000 而闸门冻结在 attach 重报值 0
  let task = driver.task_ref(0).unwrap();
  task.consume(b"bulk", 64, 9_000).unwrap();
  assert_eq!(task.shipped_watermark_address(), 9_000);
  tail.store(9_000, Ordering::Release);
  assert_eq!(
    bp.get_shipped_watermark(0),
    0,
    "闸门应冻结在陈旧值（无事件无发布）"
  );
  assert!(bp.any_stalled(), "尾差 9000 > 4096 应停滞");

  // 常驻周期臂兜底：无事件深眠下超时空转轮 throttle_all 发布已推进的
  // 落网水位，闸门在 delay 周期内追平日志尾、追加方放行
  let pump = AofReplicationPump::new(Arc::clone(&store));
  pump.start_throttle_loop(None);
  // 有界轮询至周期臂把落网水位发布到闸门（delay 周期内兜底，2s 上界）
  for _ in 0..400 {
    if bp.get_shipped_watermark(0) == 9_000 {
      break;
    }
    sleep(Duration::from_millis(5)).await;
  }
  assert_eq!(
    bp.get_shipped_watermark(0),
    9_000,
    "周期臂应把已推进的落网水位发布到闸门"
  );
  assert!(!bp.any_stalled(), "闸门追平尾后追加方放行");
  drop(pump);
}

/// 死副本空闲退场臂（对标 C# Throttle :214-215 断连抛出 → RunAsync finally
/// TryRemove 的统一退场）：零写入期断连驱动被节流周期臂出册，AOF 截断线
/// 不再被失联位点钉死（consume 错误臂需泵在跑，空闲期不可达）
#[compio::test]
async fn dead_replica_exits_via_periodic_throttle_arm() {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_DEAD,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver.clone(), false));
  driver.dispose();

  // 死驱动在册：截断请求被失联位点 64 钳制
  assert_eq!(store.count(), 1);
  assert_eq!(
    store
      .safe_truncate_aof(&AofAddress::create(1, 9_000))
      .await
      .get(0),
    Some(64),
    "死副本在册时截断线被失联位点钳制"
  );

  // 常驻周期臂：无事件空转轮扫描出册断连驱动（delay 周期内退场）
  let pump = AofReplicationPump::new(Arc::clone(&store));
  pump.start_throttle_loop(None);
  // 有界轮询至周期臂扫描出册断连驱动（delay 周期内退场，2s 上界）
  for _ in 0..400 {
    if store.count() == 0 {
      break;
    }
    sleep(Duration::from_millis(5)).await;
  }
  assert_eq!(store.count(), 0, "断连驱动应在周期臂上出册");
  assert_eq!(
    store
      .safe_truncate_aof(&AofAddress::create(1, 9_000))
      .await
      .get(0),
    Some(9_000),
    "死副本出册后截断线解钉"
  );
  drop(pump);
}

/// 跨窗口积压双副本补灌回归（单趟窗口化 + 轮转游标收口）：积压超过单趟
/// 窗口（REPLAY_CHUNK_BYTES = 1MB）时补扫连泵排空，双副本位点都追平日志
/// 尾且零 skip——单趟窗口受限不丢量，轮转游标保证双副本同轮都被触达
#[compio::test]
async fn multi_window_backlog_drains_for_both_replicas() {
  let dir = tempfile::tempdir().unwrap();
  let wal = open_wal(&dir);

  wal.enqueue(&[0u8; HEADER_PAD_PAYLOAD_LEN]).unwrap();
  // 积压 2048 × 1024B ≈ 2MB > 单趟窗口 1MB
  for _ in 0..2048u32 {
    wal.enqueue(&[0u8; 1024]).unwrap();
  }
  wal.commit().await.unwrap();

  let store = Arc::new(AofSyncDriverStore::new(1));
  let driver_a = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_ID,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  let driver_b = Arc::new(AofSyncDriver::new(
    PRIMARY_ID,
    REPLICA_2,
    1,
    &AofAddress::create(1, 64),
    None,
  ));
  assert!(store.try_add_replication_driver(driver_a.clone(), false));
  assert!(store.try_add_replication_driver(driver_b.clone(), false));

  let pump = AofReplicationPump::new(Arc::clone(&store));
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.unwrap();
  assert_eq!(skipped, 0, "健康双副本零 skip");
  let tail = wal.tail_address() as i64;
  assert_eq!(driver_a.get_previous_address(0), tail, "副本 A 连泵追平");
  assert_eq!(driver_b.get_previous_address(0), tail, "副本 B 连泵追平");
  assert!(forwarded > 2048, "全量积压与 commit 帧转发");
}
