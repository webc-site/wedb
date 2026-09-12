use std::sync::Arc;

use waof::AofAddress;

use crate::server::replication::{
  aof_sync_task::AofSyncTask, driver_registry::DriverLifecycle,
  network_buffer::ReplicationSendBufferPool, replica_wire::AofSyncWire,
};

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:AofSyncDriver
///
/// 副本节点全量物理子日志 AOF 增量同步驱动器
#[derive(Debug)]
pub struct AofSyncDriver {
  local_node_id: String,
  remote_node_id: String,
  tasks: Vec<Arc<AofSyncTask>>,
}

impl DriverLifecycle for AofSyncDriver {
  #[inline]
  fn is_active(&self) -> bool {
    self.is_connected()
  }

  fn dispose(&self) {
    for task in &self.tasks {
      task.dispose();
    }
  }
}

impl AofSyncDriver {
  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:AofSyncDriver
  ///
  /// 起始位点原样进任务。刻意差异：C# 将 0 规整到 kFirstValidAofAddress(64)
  ///（`startAddress.SetValueIf(kFirstValidAofAddress, 0)`，TsavoriteLog 首
  /// 64B 为日志头部不可回放）；Rust WalLog 无保留头（见 waof log 模块
  /// 「此处无保留区」声明），begin=0 即首条记录地址，0 规整会永久跳过
  /// [0, 64) 区间的真实记录，故地址缺失兜底仍取 0 起始
  pub fn new(local_node_id: String, remote_node_id: String, start_address: &AofAddress) -> Self {
    let pool = Arc::new(ReplicationSendBufferPool::default());
    let sublog_count = start_address.length().max(1) as usize;
    let tasks = (0..sublog_count)
      .map(|i| {
        let start = start_address.get(i).unwrap_or(0);
        Arc::new(AofSyncTask::with_buffer_pool(
          i,
          start,
          local_node_id.clone(),
          remote_node_id.clone(),
          Arc::clone(&pool),
        ))
      })
      .collect();
    Self {
      local_node_id,
      remote_node_id,
      tasks,
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:IsConnected
  ///
  /// 检查所有子任务连接是否皆处于健康状态
  pub fn is_connected(&self) -> bool {
    self.tasks.iter().all(|t| t.is_connected())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:RemoteNodeId
  pub fn remote_node_id(&self) -> &str {
    &self.remote_node_id
  }

  /// 本地节点 ID
  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:StartAddress
  ///
  /// 获取所有子任务的起始地址集合
  pub fn start_address(&self) -> AofAddress {
    let mut addr = AofAddress::new(self.tasks.len() as i32);
    for (i, task) in self.tasks.iter().enumerate() {
      addr.set(i, task.start_address());
    }
    addr
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:PreviousAddress
  ///
  /// 获取所有子任务的当前已发送位点集合
  pub fn previous_address(&self) -> AofAddress {
    let mut addr = AofAddress::new(self.tasks.len() as i32);
    for (i, task) in self.tasks.iter().enumerate() {
      addr.set(i, task.previous_address());
    }
    addr
  }

  /// 获取所有子任务的最新副本 ACK 确认位点集合
  pub fn acked_address(&self) -> AofAddress {
    let mut addr = AofAddress::new(self.tasks.len() as i32);
    for (i, task) in self.tasks.iter().enumerate() {
      addr.set(i, task.acked_address());
    }
    addr
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetPreviousAddress
  ///
  /// 获取指定子日志的当前已发送位点
  pub fn get_previous_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .tasks
      .get(physical_sublog_idx)
      .map(|t| t.previous_address())
      .unwrap_or(0)
  }

  /// 获取指定子日志的最新 ACK 确认位点
  pub fn get_acked_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .tasks
      .get(physical_sublog_idx)
      .map(|t| t.acked_address())
      .unwrap_or(0)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetShippedWatermarkAddress
  ///
  /// 获取指定子日志已推送的高水位线
  pub fn get_shipped_watermark_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .tasks
      .get(physical_sublog_idx)
      .map(|t| t.shipped_watermark_address())
      .unwrap_or(0)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetStartAddress
  ///
  /// 获取指定子日志的起始位点
  pub fn get_start_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .tasks
      .get(physical_sublog_idx)
      .map(|t| t.start_address())
      .unwrap_or(0)
  }

  /// 处理指定物理子日志收到的副本 ReplicationAck
  pub fn process_ack(&self, physical_sublog_idx: usize, acked_offset: i64) -> bool {
    if let Some(task) = self.tasks.get(physical_sublog_idx) {
      task.process_ack(acked_offset).is_ok()
    } else {
      false
    }
  }

  /// 检查是否存在任意子日志 ACK 超时
  pub fn is_any_task_ack_timed_out(&self, now_ms: i64, timeout_ms: i64) -> bool {
    self
      .tasks
      .iter()
      .any(|t| t.is_ack_timed_out(now_ms, timeout_ms))
  }

  /// 获取指定子日志的同步任务句柄
  pub fn get_task(&self, physical_sublog_idx: usize) -> Option<Arc<AofSyncTask>> {
    self.tasks.get(physical_sublog_idx).cloned()
  }

  /// 获取指定子日志的同步任务借用（零拷贝快路径）
  #[inline]
  pub fn task_ref(&self, physical_sublog_idx: usize) -> Option<&AofSyncTask> {
    self.tasks.get(physical_sublog_idx).map(Arc::as_ref)
  }

  /// 获取全部任务列表
  pub fn tasks(&self) -> &[Arc<AofSyncTask>] {
    &self.tasks
  }

  /// 逐任务注入副本发送通道（对标 C# 构造器内逐子日志建 GarnetClientSession）
  ///
  /// 单物理子日志拓扑仅 task 0 接线即可承载数据面（C# AofPhysicalSublogCount=1
  /// 时仅 aofSyncTasks[0] 发送），但逐任务全量注入保持多子日志扩展下的
  /// 对称性；attach 顺序由装配链保证初始化帧先于记录帧
  pub fn attach_wire(&self, wire: Arc<dyn AofSyncWire>) {
    for task in &self.tasks {
      if !task.attach_wire(Arc::clone(&wire)) {
        log::warn!("向 AOF 同步任务挂载副本 Wire 失败");
      }
    }
  }

  /// 逐任务卸载副本发送通道
  pub fn detach_wire(&self) {
    for task in &self.tasks {
      task.detach_wire();
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_aof_sync_driver() {
    let start = AofAddress::create(2, 500);
    let driver = AofSyncDriver::new("local1".to_string(), "remote1".to_string(), &start);

    assert_eq!(driver.remote_node_id(), "remote1");
    assert!(driver.is_connected());
    assert_eq!(driver.get_start_address(0), 500);
    assert_eq!(driver.get_previous_address(1), 500);
    assert_eq!(driver.get_acked_address(0), 500);

    // 起始位点 0 保持原样（Rust WalLog 无保留头，0 即首记录地址）
    let zero_start = AofAddress::create(2, 0);
    let driver0 = AofSyncDriver::new("local".to_string(), "remote".to_string(), &zero_start);
    assert_eq!(driver0.get_start_address(0), 0);

    let task0 = driver.get_task(0).unwrap();
    task0.consume(b"abc", 500, 600).unwrap();
    assert_eq!(driver.get_previous_address(0), 600);
    assert_eq!(driver.previous_address().get(0), Some(600));
    assert_eq!(driver.previous_address().get(1), Some(500));

    // ACK 确认处理
    assert!(driver.process_ack(0, 580));
    assert_eq!(driver.get_acked_address(0), 580);
    assert_eq!(driver.acked_address().get(0), Some(580));
  }
}
