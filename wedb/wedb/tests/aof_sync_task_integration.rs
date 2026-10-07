#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::sync::{Arc, atomic::Ordering};

use parking_lot::Mutex;
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wedb::server::replication::aof_sync_task::{AofSyncTask, TimePulseSource};
use wedb_test::replica_wire_test_wire::{CallbackWire, FrameSink, callback_wire};
use wnode::{
  GarnetLog,
  aof::{aof_backpressure::AofBackpressure, garnet_append_only_file::GarnetAppendOnlyFile},
};
use wresp::frame::parse_resp_frame;

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
  let wire = callback_wire(received.clone());
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
  task.attach_wire(wire);

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
  let wire = callback_wire(received.clone());

  let aof = pulse_aof();
  // 真实日志替身：子日志尾推进越过预算 100（any_stalled 实时尾差判定）
  let (_dirs, backends) = wnode_test::test_sublogs("advance_time_stall", 1);
  let log = Arc::new(
    GarnetLog::new(&RuntimeServerOptions::default(), backends, None).expect("构造 GarnetLog"),
  );
  let bp = Arc::new(AofBackpressure::new(1, 100));
  // Simulate stall: budget is 100, watermark is 0, tail is > 100
  bp.publish_shipped_address(0, 0);
  bp.set_weak_log(Arc::downgrade(&log));
  let stalled_sub = log.get_sub_log(0);
  while stalled_sub.tail_address() <= 100 {
    stalled_sub.enqueue(b"x").expect("记录入环形缓冲");
  }

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
  task.attach_wire(wire);

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

#[test]
fn test_aof_sync_task_lifecycle_and_throttle() {
  let task = AofSyncTask::new(0, 100, 0x10CA1, 0x2E707E, None);
  assert_eq!(task.start_address(), 100);
  assert_eq!(task.previous_address(), 100);
  assert_eq!(task.accepted_address(), 100);
  assert_eq!(task.shipped_watermark_address(), 100);
  assert!(task.is_connected());

  let dummy_data = b"SET k v";
  task.consume(dummy_data, 100, 200).expect("consume ok");
  assert_eq!(task.previous_address(), 200);

  // 节流触发报备（delta 设置为 50，已产生 100 增量）
  let pub_addr = task.throttle(50);
  assert_eq!(pub_addr, Some(200));

  // 再次节流，未产生新增量，不重复触发
  assert_eq!(task.throttle(50), None);
}

/// 发送通道接线：consume 经 wire 投递真实帧字节；投递失败断连且位点不推进
#[test]
fn test_consume_forwards_frame_via_wire() {
  let received = Arc::new(Mutex::new(Vec::new()));
  let wire = callback_wire(received.clone());
  let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
  task.attach_wire(wire);

  task.consume(b"\x00payload", 64, 78).expect("consume ok");
  assert_eq!(task.previous_address(), 78);
  let frames = received.lock();
  assert_eq!(frames.len(), 1, "帧应经发送通道投递");
  // 整帧锁定（node_id 0x0DE1 渲染 32 字符定长小写 hex，元素依次为
  // CLUSTER APPENDLOG node_id 子日志下标 前址 现址 次址 记录负载）
  assert_eq!(
    frames[0].as_slice(),
    b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n00000000000000000000000000000de1\r\n$1\r\n0\r\n$2\r\n64\r\n$2\r\n64\r\n$2\r\n78\r\n$8\r\n\x00payload\r\n"
  );
}

/// 发送失败 → 断连 + 位点不推进（对标 C# Consume 异常上抛语义）
#[test]
fn test_consume_send_failure_disconnects() {
  let wire = callback_wire(FrameSink::Reject);
  let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
  task.attach_wire(wire);

  assert!(task.consume(b"rec", 64, 67).is_err());
  assert!(!task.is_connected());
  assert_eq!(task.previous_address(), 64, "失败路径位点不推进");
  // 断连后后续 consume 直接拒绝
  assert!(task.consume(b"rec", 64, 67).is_err());
}

/// 溢流滞留帧不得计入落网水位：Queued 分支协议流游标照常推进（对标
/// C# 无条件 previousAddress = nextAddress），落网水位由泵落网后经
/// ratchet_shipped 补账（主端 AOF 安全截断线取 previous_address（对标 C# SafeTruncateAof :88/:143），shipped_watermark_address 仅作背压闸门已发水位）
#[test]
fn test_consume_queued_frame_defers_watermark() {
  let queued = Arc::new(Mutex::new(Vec::new()));
  let wire = callback_wire(FrameSink::Queue(queued.clone()));
  let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
  task.attach_wire(wire);

  task.consume(b"rec1", 64, 80).expect("consume ok");
  assert_eq!(task.accepted_address(), 80, "推流游标应推进");
  assert_eq!(
    task.previous_address(),
    80,
    "协议流游标应无条件推进至 next_address"
  );
  assert_eq!(
    task.shipped_watermark_address(),
    64,
    "溢流滞留帧不得计入落网水位"
  );

  // 泵落通道补账：水位推进至已发面
  task.ratchet_shipped(80);
  assert_eq!(task.previous_address(), 80);
  assert_eq!(task.shipped_watermark_address(), 80);
  assert_eq!(queued.lock().len(), 1, "帧应滞留溢流队列表");
}

/// 溢流排队闭环：连续 consume 两帧均滞留溢流队列，第二帧帧头
/// previous_address 必须严格等于第一帧 next_address（对标 C# 契约
/// previousAddress = nextAddress；游标滞后会令从库
/// process_primary_stream 误判 FastAofTruncate 截断跳跃 →
/// wal.safe_initialize 重置日志地址空间损坏状态）
#[test]
fn test_consume_queued_frames_chain_protocol_cursor() {
  let queued = Arc::new(Mutex::new(Vec::new()));
  let wire = callback_wire(FrameSink::Queue(queued.clone()));
  let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
  task.attach_wire(wire);

  task.consume(b"rec1", 64, 80).expect("consume ok");
  task.consume(b"rec2", 80, 96).expect("consume ok");
  assert_eq!(task.previous_address(), 96, "协议流游标推进至第二帧次址");
  assert_eq!(
    task.shipped_watermark_address(),
    64,
    "两帧均未落网，落网水位保持起点"
  );
  assert_eq!(task.accepted_address(), 96);

  let frames = queued.lock();
  assert_eq!(frames.len(), 2, "两帧均应滞留溢流队列表");
  // 第二帧 8 元素数组：items[4] 即帧头 previous_address
  let (_, items) = parse_resp_frame(&frames[1])
    .expect("协议合法")
    .expect("complete");
  assert_eq!(items[4], b"80", "第二帧帧头前址必须衔接第一帧次址 80");
  assert_eq!(items[5], b"80", "第二帧现址");
  assert_eq!(items[6], b"96", "第二帧次址");
}

/// 脉冲源缺席（对标 C# timePulseEnabled=false）：脉冲链整体静默不发送
#[test]
fn test_advance_time_pulse_without_source_is_silent() {
  let received = Arc::new(Mutex::new(Vec::new()));
  let wire = callback_wire(received.clone());
  let task = AofSyncTask::new(0, 0, 0x0DE1, 0x2E707E, None);
  task.attach_wire(wire);
  task.send_advance_time_pulse();
  assert!(received.lock().is_empty(), "无脉冲源不得发送");
}

/// 脉冲帧字节面：advance_time 经内存通道投递 4 元素 CLUSTER ADVANCE_TIME 帧
#[test]
fn test_advance_time_frame_via_wire() {
  let received = Arc::new(Mutex::new(Vec::new()));
  let wire = CallbackWire::new(received.clone());
  wire.advance_time(0, 42).expect("pulse ok");
  let frames = received.lock();
  assert_eq!(frames.len(), 1);
  // 整帧锁定：advance_time(0, 42) → 子日志下标 0 + 序列号 42
  assert_eq!(
    frames[0].as_slice(),
    b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$2\r\n42\r\n"
  );
}

/// 成功消费刷新脉冲节流窗口（对标 C# Consume 尾段
/// lastAdvanceTimePulse = Environment.单调毫秒）：consume 后 100ms
/// 频率窗口重启，与 C# 节流口径一致
#[test]
fn test_consume_refreshes_pulse_throttle_window() {
  let task = AofSyncTask::new(0, 100, 0x10CA1, 0x2E707E, None);
  task.last_advance_time_pulse.store(0, Ordering::Release);
  task.consume(b"SET k v", 100, 200).expect("consume ok");
  assert!(
    task.last_advance_time_pulse.load(Ordering::Acquire) > 0,
    "成功消费应刷新节流窗口"
  );
}
