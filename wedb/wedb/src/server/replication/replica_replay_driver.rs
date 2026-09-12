use std::{
  io::{self, Error, ErrorKind},
  sync::atomic::{AtomicBool, AtomicI64, Ordering},
};

use crate::server::replication::{aof_sync_task::ReplicationAck, driver_registry::DriverLifecycle};

/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ReplicaReplayDriver
///
/// 副本节点单子日志 AOF 数据消费与重放驱动器
#[derive(Debug)]
pub struct ReplicaReplayDriver {
  pub physical_sublog_idx: usize,
  replayed_offset: AtomicI64,
  pending_pulse_sequence_number: AtomicI64,
  applied_pulse_sequence_number: AtomicI64,
  is_resumed: AtomicBool,
}

impl DriverLifecycle for ReplicaReplayDriver {
  #[inline]
  fn is_active(&self) -> bool {
    self.is_resumed.load(Ordering::Acquire)
  }

  fn dispose(&self) {
    self.suspend_replay();
  }
}

impl ReplicaReplayDriver {
  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ReplicaReplayDriver
  pub fn new(physical_sublog_idx: usize) -> Self {
    Self {
      physical_sublog_idx,
      replayed_offset: AtomicI64::new(0),
      pending_pulse_sequence_number: AtomicI64::new(0),
      applied_pulse_sequence_number: AtomicI64::new(0),
      is_resumed: AtomicBool::new(true),
    }
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ResumeReplay
  pub fn resume_replay(&self) -> bool {
    self
      .is_resumed
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:SuspendReplay
  pub fn suspend_replay(&self) {
    self.is_resumed.store(false, Ordering::Release);
  }

  /// 当前已重放位点
  #[inline]
  pub fn replayed_offset(&self) -> i64 {
    self.replayed_offset.load(Ordering::Acquire)
  }

  /// 原子更新已重放位点（单调递增推进）
  #[inline]
  pub fn set_replayed_offset(&self, offset: i64) {
    self.replayed_offset.fetch_max(offset, Ordering::AcqRel);
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ConsumeDirect
  ///
  /// 直接模式重放消费 AOF 数据块并驱动复制位点单调前进
  pub fn consume_direct(
    &self,
    record: &[u8],
    current_address: i64,
    next_address: i64,
    mut on_record: impl FnMut(&[u8], i64),
  ) -> io::Result<i64> {
    if current_address > next_address {
      return Err(Error::new(
        ErrorKind::InvalidInput,
        format!("Current address {current_address} exceeds next address {next_address}"),
      ));
    }

    // 若数据载荷非空，回调处理业务记录
    if !record.is_empty() {
      on_record(record, current_address);
    }

    self
      .replayed_offset
      .fetch_max(next_address, Ordering::AcqRel);
    Ok(next_address)
  }

  /// 构造位点确认 ACK 消息包，供副本向主节点汇报已重放位点
  pub fn create_replication_ack(&self, node_id: &str) -> ReplicationAck {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    ReplicationAck {
      node_id: node_id.to_string(),
      physical_sublog_idx: self.physical_sublog_idx,
      acked_offset: self.replayed_offset(),
      timestamp_ms: now_ms,
    }
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:SignalTimeAdvance
  ///
  /// 处理时间脉冲心跳推进（原子最大值推进）
  pub fn signal_time_advance(&self, sequence_number: i64) {
    self
      .pending_pulse_sequence_number
      .fetch_max(sequence_number, Ordering::AcqRel);
    self
      .applied_pulse_sequence_number
      .fetch_max(sequence_number, Ordering::AcqRel);
  }

  /// 获取最新处理的时间脉冲序列号
  pub fn applied_pulse_sequence_number(&self) -> i64 {
    self.applied_pulse_sequence_number.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ThrottlePrimary
  ///
  /// 根据副本落后落盘字节数门限判断是否需要节流
  pub fn should_throttle_primary(
    &self,
    max_lag_bytes: i64,
    tail_address: i64,
    current_replication_offset: i64,
  ) -> bool {
    if max_lag_bytes <= 0 {
      return false;
    }
    (tail_address - current_replication_offset) > max_lag_bytes
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_replica_replay_driver() {
    let driver = ReplicaReplayDriver::new(0);
    assert_eq!(driver.physical_sublog_idx, 0);

    let mut record_count = 0;
    let next = driver
      .consume_direct(b"RECORD_DATA", 1000, 1100, |rec, addr| {
        assert_eq!(rec, b"RECORD_DATA");
        assert_eq!(addr, 1000);
        record_count += 1;
      })
      .unwrap();

    assert_eq!(record_count, 1);
    assert_eq!(next, 1100);
    assert_eq!(driver.replayed_offset(), 1100);

    let ack = driver.create_replication_ack("replica-1");
    assert_eq!(ack.node_id, "replica-1");
    assert_eq!(ack.acked_offset, 1100);

    driver.signal_time_advance(42);
    assert_eq!(driver.applied_pulse_sequence_number(), 42);

    assert!(driver.should_throttle_primary(500, 2000, 1000));
    assert!(!driver.should_throttle_primary(500, 1400, 1000));

    // 验证挂起与恢复重放
    driver.suspend_replay();
    assert!(driver.resume_replay());
    assert!(!driver.resume_replay());
  }
}
