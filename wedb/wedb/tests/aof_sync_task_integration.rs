use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wedb::server::replication::{
  aof_sync_task::{AofSyncTask, TimePulseSource},
  replica_wire::test_wire::CallbackWire,
};
use wnode::{
  GarnetLog,
  aof::{aof_backpressure::AofBackpressure, garnet_append_only_file::GarnetAppendOnlyFile},
};

/// 内存单物理子日志 AOF 门面（脉冲源构造）
fn pulse_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs("aof_sync_task", 1);
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ))
}

/// 时间脉冲链：构造期注入源 + 跨越频率窗口后 tail 移动触发真实发送，
/// 序列号取门面生成器；tail 无移动且无停顿时静默
#[test]
fn test_advance_time_pulse_sends_frame() {
  let received = Arc::new(Mutex::new(Vec::new()));
  let wire = Arc::new(CallbackWire::new(received.clone()));
  let task = AofSyncTask::new(
    0,
    0,
    0x0DE1,
    0x2E707E,
    Some(Arc::new(TimePulseSource {
      aof: pulse_aof(),
      backpressure: None,
      runtime_config: Arc::new(RuntimeServerConfig::new(RuntimeServerOptions::default())),
    })),
  );
  assert!(task.attach_wire(wire));

  // 推流游标先追平日志尾（真实段设备空日志尾 = 设备首地址 0；对标 C# iter
  // 消费到 tail 后 NextAddress == TailAddress 的可发脉冲前置态）
  task.consume(b"x", 0, 0).expect("consume ok");

  // 首次脉冲：tail 快照 -1 → tail(0) 视作移动 → 发送（对标 Array.Fill(-1) 初值）
  task.last_advance_time_pulse.store(0, Ordering::Release);
  task.send_advance_time_pulse();
  {
    let frames = received.lock();
    assert_eq!(frames.len(), 2, "应为 1 记录帧 + 1 脉冲帧");
    // 整帧锁定：尾元素 = 子日志下标 0 + 序列号（单物理日志模式序列号生成器
    // 缺席恒 0，取严格大于即 1），帧名/元素数/元素值任一漂移即红
    assert_eq!(
      frames[1].as_slice(),
      b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$1\r\n1\r\n",
      "第二帧应为 CLUSTER ADVANCE_TIME 脉冲整帧"
    );
  }

  // 快照已入账：tail 无移动且无停顿 → 静默（跨越频率窗口后仍不发）
  assert_eq!(
    task.pulse_tail_snapshot.load(Ordering::Acquire),
    0,
    "发送后快照应入账"
  );
  task.last_advance_time_pulse.store(0, Ordering::Release);
  task.send_advance_time_pulse();
  assert_eq!(received.lock().len(), 2, "tail 无移动且无停顿应静默");
}

#[test]
fn test_advance_time_pulse_when_stalled() {
  let received = Arc::new(Mutex::new(Vec::new()));
  let wire = Arc::new(CallbackWire::new(received.clone()));

  let aof = pulse_aof();
  let bp = Arc::new(AofBackpressure::new(1, 100));
  // Simulate stall: budget is 100, watermark is 0, tail is > 100
  bp.publish_shipped_address(0, 0);
  bp.set_counter_log(Arc::new(AtomicU64::new(150)));

  let task = AofSyncTask::new(
    0,
    0,
    0x0DE1,
    0x2E707E,
    Some(Arc::new(TimePulseSource {
      aof,
      backpressure: Some(bp),
      runtime_config: Arc::new(RuntimeServerConfig::new(RuntimeServerOptions::default())),
    })),
  );
  assert!(task.attach_wire(wire));

  // 推流游标先追平日志尾
  task.consume(b"x", 0, 0).expect("consume ok");

  // 首次脉冲：发送并入账快照
  task.last_advance_time_pulse.store(0, Ordering::Release);
  task.send_advance_time_pulse();
  assert_eq!(received.lock().len(), 2);

  // 再次脉冲：tail 无移动，但有停顿 (any_stalled == true)，应当继续发脉冲解冻
  task.last_advance_time_pulse.store(0, Ordering::Release);
  task.send_advance_time_pulse();
  assert_eq!(
    received.lock().len(),
    3,
    "即使 tail 无移动，因停顿应发脉冲解冻"
  );
}
