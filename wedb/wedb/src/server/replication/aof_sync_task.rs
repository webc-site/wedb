use std::{
  fmt::{self, Formatter},
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
};

use parking_lot::RwLock;

use crate::server::replication::{
  driver_registry::DriverLifecycle, network_buffer::ReplicationSendBufferPool,
  replica_wire::AofSyncWire,
};

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
///
/// 副本位点确认数据结构
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationAck {
  pub node_id: String,
  pub physical_sublog_idx: usize,
  pub acked_offset: i64,
  pub timestamp_ms: u64,
}

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
///
/// 单个物理子日志的 AOF 增量流式网络同步任务，集成定长缓冲复用、分包限制、背压门控与位点 ACK 机制
pub struct AofSyncTask {
  physical_sublog_idx: usize,
  start_address: i64,
  previous_address: AtomicI64,
  acked_address: AtomicI64,
  shipped_watermark_address: AtomicI64,
  last_throttle_shipped: AtomicI64,
  last_published_shipped_address: AtomicI64,
  last_advance_time_pulse: AtomicI64,
  last_ack_timestamp: AtomicI64,
  is_connected: AtomicBool,
  local_node_id: String,
  remote_node_id: String,
  send_buffer_pool: Arc<ReplicationSendBufferPool>,
  /// 副本发送通道（对标 C# garnetClient 字段；C# 构造期按端点建立，
  /// Rust 装配期经 attach_wire 注入——未注入时 consume 仅做位点记账，
  /// 见 attach_wire 文档）
  wire: RwLock<Option<Arc<dyn AofSyncWire>>>,
}

impl DriverLifecycle for AofSyncTask {
  #[inline]
  fn is_active(&self) -> bool {
    self.is_connected()
  }

  fn dispose(&self) {
    self.set_connected(false);
    if let Some(wire) = self.wire.write().take() {
      wire.disconnect();
    }
  }
}

/// 手写 Debug：发送通道为 dyn 端口（显示连接态即可）
impl fmt::Debug for AofSyncTask {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("AofSyncTask")
      .field("physical_sublog_idx", &self.physical_sublog_idx)
      .field("remote_node_id", &self.remote_node_id)
      .field("previous_address", &self.previous_address())
      .field("connected", &self.is_connected())
      .finish_non_exhaustive()
  }
}

impl AofSyncTask {
  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
  pub fn new(
    physical_sublog_idx: usize,
    start_address: i64,
    local_node_id: String,
    remote_node_id: String,
  ) -> Self {
    Self::with_buffer_pool(
      physical_sublog_idx,
      start_address,
      local_node_id,
      remote_node_id,
      Arc::new(ReplicationSendBufferPool::default()),
    )
  }

  /// 创建指定发送缓冲池的 AofSyncTask 实例
  pub fn with_buffer_pool(
    physical_sublog_idx: usize,
    start_address: i64,
    local_node_id: String,
    remote_node_id: String,
    send_buffer_pool: Arc<ReplicationSendBufferPool>,
  ) -> Self {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    Self {
      physical_sublog_idx,
      start_address,
      previous_address: AtomicI64::new(start_address),
      acked_address: AtomicI64::new(start_address),
      shipped_watermark_address: AtomicI64::new(start_address),
      last_throttle_shipped: AtomicI64::new(start_address),
      last_published_shipped_address: AtomicI64::new(start_address),
      last_advance_time_pulse: AtomicI64::new(0),
      last_ack_timestamp: AtomicI64::new(now_ms),
      is_connected: AtomicBool::new(true),
      local_node_id,
      remote_node_id,
      send_buffer_pool,
      wire: RwLock::new(None),
    }
  }

  /// 注入副本发送通道（对标 C# 构造器内 garnetClient 建立）
  ///
  /// C# AofSyncTask 构造即持有 GarnetClientSession；Rust 装配链在驱动
  /// 入库后按副本端点建连并注入（attach 顺序保证初始化帧先于记录帧，
  /// 见 TcpSessionWire::connect）。未注入 wire 的驱动仅提供位点记账面
  ///（单测/位面观测形态，生产装配恒注入）
  pub fn attach_wire(&self, wire: Arc<dyn AofSyncWire>) -> bool {
    *self.wire.write() = Some(wire);
    true
  }

  /// 卸载副本发送通道并主动断连
  pub fn detach_wire(&self) {
    if let Some(wire) = self.wire.write().take() {
      wire.disconnect();
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:StartAddress
  #[inline]
  pub fn start_address(&self) -> i64 {
    self.start_address
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:PreviousAddress
  #[inline]
  pub fn previous_address(&self) -> i64 {
    self.previous_address.load(Ordering::Acquire)
  }

  /// 副本最近确认确认位点
  #[inline]
  pub fn acked_address(&self) -> i64 {
    self.acked_address.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:ShippedWatermarkAddress
  #[inline]
  pub fn shipped_watermark_address(&self) -> i64 {
    self.shipped_watermark_address.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:IsConnected
  ///
  /// 本端标志与发送通道健康面双重判定（C# `garnetClient != null &&
  /// garnetClient.IsConnected`——对端断链由通道健康面即时感知，无需等待
  /// 下一次 Consume 失败）
  #[inline]
  pub fn is_connected(&self) -> bool {
    self.is_connected.load(Ordering::Acquire)
      && self.wire.read().as_ref().is_none_or(|w| w.is_connected())
  }

  /// 设置连接健康状态
  pub fn set_connected(&self, connected: bool) {
    self.is_connected.store(connected, Ordering::Release);
  }

  /// 子日志索引
  #[inline]
  pub fn physical_sublog_idx(&self) -> usize {
    self.physical_sublog_idx
  }

  /// 远程节点 ID
  #[inline]
  pub fn remote_node_id(&self) -> &str {
    &self.remote_node_id
  }

  /// 本地节点 ID
  #[inline]
  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }

  /// 最近一次收到 ACK 的时间戳（毫秒）
  #[inline]
  pub fn last_ack_timestamp(&self) -> i64 {
    self.last_ack_timestamp.load(Ordering::Acquire)
  }

  /// 检查 ACK 是否已超时
  pub fn is_ack_timed_out(&self, now_ms: i64, timeout_ms: i64) -> bool {
    if !self.is_connected() {
      return true;
    }
    let last = self.last_ack_timestamp.load(Ordering::Acquire);
    now_ms.saturating_sub(last) > timeout_ms
  }

  /// 处理副本上报的 ReplicationAck 位点确认（原子单调递增）
  pub fn process_ack(&self, acked_offset: i64) -> io::Result<()> {
    if !self.is_connected() {
      return Err(Error::new(
        ErrorKind::NotConnected,
        format!(
          "AOF stream client disconnected! [{}]",
          self.physical_sublog_idx
        ),
      ));
    }

    let prev = self.acked_address.fetch_max(acked_offset, Ordering::AcqRel);
    if acked_offset > prev {
      let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
      self.last_ack_timestamp.store(now_ms, Ordering::Release);
      self
        .shipped_watermark_address
        .fetch_max(acked_offset, Ordering::AcqRel);
      let delta = (acked_offset - prev) as usize;
      self.send_buffer_pool.acknowledge_inflight(delta);
    }

    Ok(())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume
  ///
  /// 消费并分包向从节点发送 AOF 批次记录，驱动 previous_address 单调前进：
  /// 经发送通道转发完整记录帧（对标 C# Consume 内 garnetClient.
  /// ExecuteClusterAppendLog）后推进位点；发送失败断连并上抛（对标 C#
  /// Consume 异常上抛 → RunAofSyncTaskAsync 终止 → 驱动出库）
  pub fn consume(&self, record: &[u8], current_address: i64, next_address: i64) -> io::Result<()> {
    if !self.is_connected.load(Ordering::Acquire) {
      return Err(Error::new(
        ErrorKind::NotConnected,
        format!(
          "AOF stream client disconnected! [{}]:({},{})",
          self.physical_sublog_idx,
          self.start_address,
          self.previous_address()
        ),
      ));
    }

    let wire_opt = self.wire.read().clone();
    if let Some(wire) = &wire_opt
      && !wire.is_connected()
    {
      self.set_connected(false);
      return Err(Error::new(
        ErrorKind::NotConnected,
        format!(
          "AOF stream client disconnected! [{}]:({},{})",
          self.physical_sublog_idx,
          self.start_address,
          self.previous_address()
        ),
      ));
    }

    let prev = self.previous_address.load(Ordering::Acquire);
    if current_address < prev {
      return Err(Error::new(
        ErrorKind::InvalidInput,
        format!("Current address {current_address} less than previous address {prev}"),
      ));
    }

    // 网络发送（对标 C# ExecuteClusterAppendLog：失败异常上抛，位点不推进）
    if let Some(wire) = wire_opt
      && let Err(e) = wire.append_log(
        &self.local_node_id,
        self.physical_sublog_idx,
        prev,
        current_address,
        next_address,
        record,
      )
    {
      self.set_connected(false);
      return Err(e);
    }

    // 网络发送缓冲区在途水位跟踪（对标逻辑位点增量，与 ACK 确认时的地址差严格一致）
    let delta = (next_address - current_address).max(0) as usize;
    self.send_buffer_pool.track_inflight_send(delta);

    self
      .previous_address
      .fetch_max(next_address, Ordering::AcqRel);
    self
      .shipped_watermark_address
      .fetch_max(next_address, Ordering::AcqRel);

    Ok(())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Throttle
  ///
  /// 节流探测与高水位报备检查，当产生足够增量或追平空闲时触发报备
  pub fn throttle(&self, publish_delta_bytes: i64) -> Option<i64> {
    if !self.is_connected() {
      return None;
    }

    let shipped = self.previous_address.load(Ordering::Acquire);
    let target_wm = self
      .shipped_watermark_address
      .fetch_max(shipped, Ordering::AcqRel)
      .max(shipped);

    let last_pub = self.last_published_shipped_address.load(Ordering::Acquire);
    let pending = target_wm - last_pub;
    let last_throttle = self.last_throttle_shipped.load(Ordering::Acquire);
    let idle = target_wm == last_throttle;

    self
      .last_throttle_shipped
      .store(target_wm, Ordering::Release);

    let res = if pending > 0 && (pending >= publish_delta_bytes || idle) {
      self
        .last_published_shipped_address
        .store(target_wm, Ordering::Release);
      Some(target_wm)
    } else {
      None
    };
    self.send_advance_time_pulse(target_wm);
    res
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:SendAdvanceTimePulse
  ///
  /// 节流空闲时发送时间脉冲推进心跳（原子最大值推进）
  pub fn send_advance_time_pulse(&self, sequence_number: i64) {
    self
      .last_advance_time_pulse
      .fetch_max(sequence_number, Ordering::AcqRel);
  }

  /// 发送缓冲池在途字节数（对标 C# networkPool.GetStats 的水位观测面）
  #[inline]
  pub fn current_inflight_bytes(&self) -> i64 {
    self.send_buffer_pool.current_inflight_bytes()
  }
}

#[cfg(test)]
mod tests {
  use parking_lot::Mutex;

  use super::*;
  use crate::server::replication::replica_wire::CallbackWire;

  #[test]
  fn test_aof_sync_task_lifecycle_and_ack() {
    let task = AofSyncTask::new(0, 100, "local".to_string(), "remote".to_string());
    assert_eq!(task.start_address(), 100);
    assert_eq!(task.previous_address(), 100);
    assert_eq!(task.acked_address(), 100);
    assert!(task.is_connected());

    let dummy_data = b"SET k v";
    task.consume(dummy_data, 100, 200).expect("consume ok");
    assert_eq!(task.previous_address(), 200);

    // 处理副本 ACK
    task.process_ack(180).expect("process ack ok");
    assert_eq!(task.acked_address(), 180);

    // 节流触发报备（delta 设置为 50，已产生 100 增量）
    let pub_addr = task.throttle(50);
    assert_eq!(pub_addr, Some(200));

    // 再次节流，未产生新增量，不重复触发
    assert_eq!(task.throttle(50), None);

    // 超时判定
    let now = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    assert!(!task.is_ack_timed_out(now, 5000));
    assert!(task.is_ack_timed_out(now + 10000, 5000));
  }

  /// 发送通道接线：consume 经 wire 投递真实帧字节；投递失败断连且位点不推进
  #[test]
  fn test_consume_forwards_frame_via_wire() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink_received = Arc::clone(&received);
    let wire = Arc::new(CallbackWire::new(move |frame: &[u8]| {
      sink_received.lock().push(frame.to_vec());
      true
    }));
    let task = AofSyncTask::new(0, 64, "primary-1".to_string(), "replica-1".to_string());
    assert!(task.attach_wire(wire));

    task.consume(b"\x00payload", 64, 78).expect("consume ok");
    assert_eq!(task.previous_address(), 78);
    let frames = received.lock();
    assert_eq!(frames.len(), 1, "帧应经发送通道投递");
    assert!(frames[0].starts_with(b"*8\r\n$7\r\nCLUSTER\r\n"));
  }

  /// 发送失败 → 断连 + 位点不推进（对标 C# Consume 异常上抛语义）
  #[test]
  fn test_consume_send_failure_disconnects() {
    let wire = Arc::new(CallbackWire::new(|_frame| false));
    let task = AofSyncTask::new(0, 64, "p".to_string(), "r".to_string());
    assert!(task.attach_wire(wire));

    assert!(task.consume(b"rec", 64, 67).is_err());
    assert!(!task.is_connected());
    assert_eq!(task.previous_address(), 64, "失败路径位点不推进");
    // 断连后后续 consume 直接拒绝
    assert!(task.consume(b"rec", 64, 67).is_err());
  }
}
