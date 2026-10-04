//! 泵扫描臂无 wire 钉线驱动休眠回归（工单
//! wedb-repl-aof-pump-wireless-pin-consume-defeats-truncation-pin）
//!
//! 对标 C# 契约：钉线驱动入库时不启动消费泵，传送窗口内保持休眠——
//! libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:AcquireCheckpointEntryAsync
//! `TryAddReplicationDriver(startAofAddress)` 入库的 AofSyncTask 构造即持
//! garnetClient 但迭代器 iter 为 null，唯一消费泵 RunAofSyncTaskAsync
//! （AofOperations/AofSyncTask.cs）仅由 startAofSync 段 TryConnectToReplica
//! 启动；SafeTruncateAof（AofSyncDriverStore.cs）以全部活跃驱动
//! previousAddress 取小钳制截断线，钉线窗口内安全截断无从越过覆盖位。
//!
//! rust 事件泵形态下 pump_backlog 统一扫册，钉线驱动（new(…, None) 无
//! wire 入库）若被放行扫描，consume 走记账分支空推三位点、钉线击穿。
//! 本测试断言泵体单点收口后的三点：
//! 1. 在册无 wire 钉线驱动经泵轮转后 previous/shipped/accepted 三位点不动；
//! 2. 同轮带 wire 真驱动与后置置换真驱动（attach_stream_driver 先入库后
//!    接线形态）照常推流；
//! 3. 钉线窗口内并发 safe_truncate_aof 的 truncated_until 不越过 pin_start。

use std::{path::Path, sync::Arc};

use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::replication::{
  aof_replication_pump::AofReplicationPump,
  aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
};
use wedb_test::replica_wire_test_wire::{FrameSink, callback_wire};

const LOCAL_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_00A1;
const PINNED_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_00B1;
const STREAM_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_00C1;
/// 首记录负载 56B + 8B 帧头 = 64B：其后数据记录自地址 64 起衔接（钉线位点）
const PIN_START: i64 = 64;
/// 数据记录条数（1024B 负载；提交流水另落 1 帧提交元数据）
const RECORDS: usize = 8;

/// 建主侧日志并写入 [0, 64) 首记录 + 64 起 8 条积压记录，返回日志句柄
async fn build_backlog(dir: &Path) -> Arc<WalLog<SegmentedDevice>> {
  let device =
    Arc::new(SegmentedDevice::single_file(dir.join("primary.wal")).expect("create wal device"));
  let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"));
  wal.enqueue(&[0u8; 56]).expect("enqueue pad record");
  for i in 0..RECORDS {
    wal
      .enqueue(&[i as u8; 1024])
      .expect("enqueue backlog record");
  }
  wal.commit().await.expect("commit backlog");
  wal
}

/// 钉线休眠 + 真驱动推流（票面验证点一/二）：同册双驱动——无 wire 钉线驱动
/// 经泵轮转后三位点恒钉 pin_start、零帧投递；带 wire 真驱动照常转发积压。
/// 随后按 attach_stream_driver 形态以真驱动原地置换钉线驱动，续泵恢复推流
#[compio::test]
async fn pump_backlog_keeps_wireless_pin_driver_dormant() {
  let dir = tempfile::tempdir().unwrap();
  let wal = build_backlog(dir.path()).await;

  let store = Arc::new(AofSyncDriverStore::new(1));
  // 无 wire 钉线驱动（diskbased 预锁 / diskless 扇出前批量入库形态）
  let pin_driver = Arc::new(AofSyncDriver::new(
    LOCAL_ID,
    PINNED_ID,
    1,
    &AofAddress::create(1, PIN_START),
    None,
  ));
  assert!(
    store.try_add_replication_driver(Arc::clone(&pin_driver), false),
    "钉线驱动入库"
  );
  // 带 wire 真驱动（另一副本）
  let frames = Arc::new(Mutex::new(Vec::new()));
  let stream_driver = Arc::new(AofSyncDriver::new(
    LOCAL_ID,
    STREAM_ID,
    1,
    &AofAddress::create(1, 0),
    None,
  ));
  stream_driver.attach_wire(callback_wire(FrameSink::Buffer(Arc::clone(&frames))));
  assert!(
    store.try_add_replication_driver(Arc::clone(&stream_driver), false),
    "真驱动入库"
  );

  let pump = AofReplicationPump::new(Arc::clone(&store));
  let (forwarded, skipped) = pump.sync_backlog(&wal).await.expect("sync backlog");

  // 钉线驱动休眠：consume 记账分支未被放行，三位点恒钉 pin_start
  let task = pin_driver.task_ref(0).unwrap();
  assert_eq!(task.previous_address(), PIN_START, "钉线 previous 不得空推");
  assert_eq!(
    task.shipped_watermark_address(),
    PIN_START,
    "钉线 shipped 不得空推"
  );
  assert_eq!(task.accepted_address(), PIN_START, "钉线 accepted 不得空推");

  // 真驱动照常推流：自 0 起扫全量积压（首记录 + 数据记录 + 提交元数据帧）
  let tail = wal.safe_tail_address() as i64;
  assert_eq!(skipped, 0, "无失败出册");
  assert_eq!(forwarded, (RECORDS + 2) as u64, "真驱动应转发全部积压");
  assert_eq!(
    frames.lock().len(),
    forwarded as usize,
    "转发帧应全部落通道"
  );
  assert_eq!(
    stream_driver.task_ref(0).unwrap().previous_address(),
    tail,
    "真驱动位点应追平日志尾"
  );

  // attach_stream_driver 置换形态：同 node_id 真驱动原地置换钉线驱动后续泵，
  // 自被置换驱动位点照常恢复推流
  let promoted_frames = Arc::new(Mutex::new(Vec::new()));
  let promoted = Arc::new(AofSyncDriver::new(
    LOCAL_ID,
    PINNED_ID,
    1,
    &AofAddress::create(1, PIN_START),
    None,
  ));
  promoted.attach_wire(callback_wire(FrameSink::Buffer(Arc::clone(
    &promoted_frames,
  ))));
  assert!(
    store.try_add_replication_driver(Arc::clone(&promoted), false),
    "真驱动原地置换钉线驱动"
  );
  let (forwarded, _) = pump.sync_backlog(&wal).await.expect("resync backlog");
  // 置换后自 pin_start 续推：除首记录外全部积压帧补发
  assert_eq!(
    forwarded,
    (RECORDS + 1) as u64,
    "置换后真驱动应推完剩余积压"
  );
  assert_eq!(promoted_frames.lock().len(), forwarded as usize);
  assert_eq!(promoted.task_ref(0).unwrap().previous_address(), tail);
}

/// 钉线窗口内并发截断（票面验证点三）：仅钉线驱动在册，泵轮转（含唤醒
/// 循环同刻扫册）后三位点不动，safe_truncate_aof 取小钳制生效——
/// truncated_until 不越过 pin_start，副本所需 [pin_start, 新覆盖位) 段保全
#[compio::test]
async fn pin_window_blocks_truncation_after_pump_rounds() {
  let dir = tempfile::tempdir().unwrap();
  let wal = build_backlog(dir.path()).await;
  let tail = wal.safe_tail_address() as i64;
  assert!(tail > PIN_START, "积压应越过钉线位点");

  let store = Arc::new(AofSyncDriverStore::new(1));
  let pin_driver = Arc::new(AofSyncDriver::new(
    LOCAL_ID,
    PINNED_ID,
    1,
    &AofAddress::create(1, PIN_START),
    None,
  ));
  assert!(
    store.try_add_replication_driver(Arc::clone(&pin_driver), false),
    "钉线驱动入库"
  );

  let pump = AofReplicationPump::new(Arc::clone(&store));
  // 泵轮转多轮（信号唤醒与补扫在本拓扑同走 pump_backlog 单点）：无真驱动
  // 时零转发——钉线驱动被判别位跳过，不得出现记账空推
  for _ in 0..3 {
    let (forwarded, skipped) = pump.sync_backlog(&wal).await.expect("sync backlog");
    assert_eq!(forwarded, 0, "钉线驱动不得被记账空推");
    assert_eq!(skipped, 0, "休眠非错误，不得计入跳过处置");
  }

  // 并发截断：传送窗内任一检查点完成推进截断线，仅被活跃驱动取小钳制
  let clamped = store.safe_truncate_aof(&AofAddress::create(1, tail)).await;
  assert_eq!(
    clamped.get(0),
    Some(PIN_START),
    "截断线不得越过无 wire 钉线位点"
  );
  assert_eq!(
    store.get_truncated_until().get(0),
    Some(PIN_START),
    "在册截断位点同样钳制在 pin_start"
  );
  let task = pin_driver.task_ref(0).unwrap();
  assert_eq!(task.previous_address(), PIN_START);
  assert_eq!(task.accepted_address(), PIN_START);
}
