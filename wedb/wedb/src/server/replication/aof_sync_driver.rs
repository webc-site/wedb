use std::sync::Arc;

use log::{trace, warn};
use parking_lot::RwLock;
use waof::AofAddress;
use wbase::hex::hex_str_u128;
use wnode::aof::{aof_backpressure::AofBackpressure, garnet_log::GarnetLog};

use super::{
  aof_sync_task::{AofSyncTask, TimePulseSource},
  driver_registry::{DriverLifecycle, DriverRegistry},
  replica_wire::AofSyncWire,
};

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:AofSyncDriver
///
/// 副本节点全量物理子日志 AOF 增量同步驱动器
#[derive(Debug)]
pub struct AofSyncDriver {
  local_node_id: u128,
  remote_node_id: u128,
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
  /// 任务数由装配期注入的 sublog_count 单源决定（对标 C# 构造器 :111
  /// `new AofSyncTask[clusterProvider.serverOptions.AofPhysicalSublogCount]`，
  /// rust 唯一来源为 ReplicationManager 装配值 `rm.sublog_count()`；
  /// 生产装配段对物理子日志数有等值 1 的强制校验，见 boot.rs）；
  /// start_address 只提供各 index 的起始位点，短向量缺位按 0 兜底、
  /// 长向量的多余位点忽略（C# :113 按任务下标取 `startAddress[idx]` 的
  /// 定点语义）
  ///
  /// 起始位点原样进任务。刻意差异：C# 将 0 规整到 kFirstValidAofAddress(64)
  ///（`startAddress.SetValueIf(kFirstValidAofAddress, 0)`，TsavoriteLog 首
  /// 64B 为日志头部不可回放）；Rust WalLog 无保留头（见 waof log 模块
  /// 「此处无保留区」声明），begin=0 即首条记录地址，0 规整会永久跳过
  /// [0, 64) 区间的真实记录，故地址缺失兜底仍取 0 起始
  ///
  /// 脉冲源构造期逐任务注入（对标 C# 构造器内逐子日志
  /// `new AofSyncTask(clusterProvider, aofSyncDriverStore, …)` 的装配关系，
  /// 无入库后回注环节）
  pub fn new(
    local_node_id: u128,
    remote_node_id: u128,
    sublog_count: usize,
    start_address: &AofAddress,
    pulse_source: Option<Arc<TimePulseSource>>,
  ) -> Self {
    let tasks = (0..sublog_count)
      .map(|i| {
        let start = start_address.get(i).unwrap_or(0);
        Arc::new(AofSyncTask::new(
          i,
          start,
          local_node_id,
          remote_node_id,
          pulse_source.clone(),
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
  pub fn remote_node_id(&self) -> u128 {
    self.remote_node_id
  }

  /// 本地节点 ID
  pub fn local_node_id(&self) -> u128 {
    self.local_node_id
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
  /// 同步注入溢流水位回推端：溢流帧由 TCP 泵落通道后经 Weak 引用回推
  /// 任务已发送水位（Weak 破 task → wire → task 引用环）
  pub fn attach_wire(&self, wire: impl Into<AofSyncWire>) {
    let wire = wire.into();
    wire.set_ratchets(self.tasks.iter().map(Arc::downgrade).collect());
    for task in &self.tasks {
      if !task.attach_wire(wire.clone()) {
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

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:Dispose
  pub fn dispose(&self) {
    DriverLifecycle::dispose(self);
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:RunAsync
  pub async fn run_async(&self) {
    for task in &self.tasks {
      task.run_aof_sync_task_async().await;
    }
  }
}

/// libs/server/Cluster/RoleInfo.cs:RoleInfo
///
/// 副本同步会话角色统计信息（对标 C# RoleInfo）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaRoleInfo {
  pub node_id: u128,
  pub is_connected: bool,
  pub replication_offset: AofAddress,
  pub replication_lag: AofAddress,
}

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore
///
/// 主节点 AOF 增量复制驱动管理器，协调全副本同步进度、安全日志截断与背压门控水位
pub struct AofSyncDriverStore {
  /// 在册驱动容器（store 内部经 register/remove/for_each/snapshot 门面访问，
  /// 不对外暴露第二访问路径）
  registry: DriverRegistry<u128, AofSyncDriver>,
  pub truncated_until: RwLock<AofAddress>,
  pub sublog_count: usize,
  /// 主侧 AOF 背压闸门（C# 经 clusterProvider.storeWrapper.appendOnlyFile.backpressure 反查；
  /// Rust 依赖方向反转，集群装配期注入，见 attach_backpressure）
  pub backpressure: RwLock<Option<Arc<AofBackpressure>>>,
  /// 物理日志句柄（C# 经 clusterProvider.storeWrapper.appendOnlyFile?.Log 反查；
  /// Rust 依赖方向反转，集群装配期注入，见 attach_log）
  pub log: RwLock<Option<Arc<GarnetLog>>>,
}

impl AofSyncDriverStore {
  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore
  pub fn new(sublog_count: usize) -> Self {
    Self {
      registry: DriverRegistry::new(),
      truncated_until: RwLock::new(AofAddress::create(sublog_count as i32, 0)),
      sublog_count,
      backpressure: RwLock::new(None),
      log: RwLock::new(None),
    }
  }

  /// 注入物理日志句柄（集群装配期一次注入；对标 C# AofSyncDriverStore
  /// 构造期经 clusterProvider.storeWrapper.appendOnlyFile 可达 Log——
  /// SafeTruncateAof 尾部 Log.TruncateUntil + Commit 的物理截断面）
  pub fn attach_log(&self, log: Option<Arc<GarnetLog>>) {
    *self.log.write() = log;
  }

  /// 注入主侧 AOF 背压闸门（集群装配期一次注入；对标 C# AofSyncTask 构造内
  /// `backpressure = appendOnlyFile?.backpressure` 的装配关系）
  pub fn attach_backpressure(&self, bp: Option<Arc<AofBackpressure>>) {
    *self.backpressure.write() = bp;
  }

  /// 背压闸门快照
  pub fn backpressure(&self) -> Option<Arc<AofBackpressure>> {
    self.backpressure.read().clone()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverCount
  pub fn count(&self) -> usize {
    self.registry.count()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TruncatedUntil
  ///
  /// 获取当前逻辑截断位点
  pub fn get_truncated_until(&self) -> AofAddress {
    *self.truncated_until.read()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:UpdateTruncatedUntil
  pub fn update_truncated_until(&self, truncated: &AofAddress) {
    let mut guard = self.truncated_until.write();
    for i in 0..self.sublog_count {
      if let Some(val) = truncated.get(i) {
        let cur = guard.get(i).unwrap_or(0);
        if val > cur {
          guard.set(i, val);
        }
      }
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryAddReplicationDriver
  ///
  /// 添加或更新指定副本的复制驱动器；若起始位点早于已截断范围且不允许丢数据则拒绝添加。
  /// 驱动集合变更后向背压闸门重报全子日志水位（对标 C# 成功路径尾部
  /// PublishShippedAddresses：attach 副本会立即收紧各子日志最小已发水位）
  pub fn try_add_replication_driver(
    &self,
    driver: Arc<AofSyncDriver>,
    allow_data_loss: bool,
  ) -> bool {
    let truncated = *self.truncated_until.read();
    let start_address = driver.start_address();

    if start_address.any_lesser(&truncated) && !allow_data_loss {
      warn!(
        "AOF sync driver for {} with start address {:?} rejected, local AOF is truncated until {:?}",
        hex_str_u128(driver.remote_node_id()),
        start_address,
        truncated
      );
      return false;
    }

    trace!(
      "Added/updated AofSyncDriver for {}",
      hex_str_u128(driver.remote_node_id())
    );
    self.registry.register(driver.remote_node_id(), driver);
    self.publish_shipped_addresses_to_gate();
    true
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryAddReplicationDrivers
  ///
  /// 批量添加/注册副本驱动集合
  pub fn try_add_replication_drivers(
    &self,
    drivers: &[Arc<AofSyncDriver>],
    allow_data_loss: bool,
  ) -> bool {
    let truncated = *self.truncated_until.read();
    for driver in drivers {
      let start_address = driver.start_address();
      if start_address.any_lesser(&truncated) && !allow_data_loss {
        warn!(
          "AOF sync driver for {} with start address {:?} rejected, local AOF is truncated until {:?}",
          hex_str_u128(driver.remote_node_id()),
          start_address,
          truncated
        );
        return false;
      }
    }

    for driver in drivers {
      trace!(
        "Added/updated AofSyncDriver for {}",
        hex_str_u128(driver.remote_node_id())
      );
      self
        .registry
        .register(driver.remote_node_id(), Arc::clone(driver));
    }
    self.publish_shipped_addresses_to_gate();
    true
  }

  /// 全部在册驱动快照（推流泵逐副本分发用）
  pub fn drivers(&self) -> Vec<Arc<AofSyncDriver>> {
    self.registry.snapshot()
  }

  /// 遍历在册驱动执行只读借用闭包（零堆分配与零 Arc 克隆）
  pub fn for_each_driver(&self, f: impl FnMut(&AofSyncDriver)) {
    self.registry.for_each(f);
  }

  /// AofSyncDriverStore.cs 的 TryRemove 按节点 ID 变体（C# 仅提供引用匹配单一
  /// 重载，函数级映射声明归 [`Self::try_remove_current`]）；移除后向背压闸门重报
  /// 水位（对标 C# 成功路径尾部 PublishShippedAddresses：detach 副本可能放宽水位）
  pub fn try_remove(&self, remote_node_id: u128) -> bool {
    if let Some(driver) = self.registry.remove(&remote_node_id) {
      trace!("Removed AofSyncDriver for {}", hex_str_u128(remote_node_id));
      driver.dispose();
      self.publish_shipped_addresses_to_gate();
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryRemove
  ///
  /// 实例匹配移除：仅当在册驱动与传入驱动为同一实例时移除并退场处置
  ///（对标 C# TryRemove(AofSyncDriver) 的 `syncDriver == aofSyncDriver` 引用
  /// 匹配——推流退场与重挂置换并发时绝不误删同节点新驱动）；命中即 dispose
  /// 并向背压闸门重报水位（死副本出册解除截断线与闸门钉制）
  pub fn try_remove_current(&self, driver: &Arc<AofSyncDriver>) -> bool {
    if let Some(removed) = self
      .registry
      .remove_if_current(&driver.remote_node_id(), driver)
    {
      trace!(
        "Removed AofSyncDriver for {}",
        hex_str_u128(removed.remote_node_id())
      );
      removed.dispose();
      self.publish_shipped_addresses_to_gate();
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:CountConnectedReplicas
  ///
  /// 统计处于连接健康态的副本数量
  pub fn count_connected_replicas(&self) -> usize {
    self.registry.count_by(|d| d.is_connected())
  }

  /// 获取全部活跃同步任务中的最小 AOF 地址向量
  ///
  /// C# SafeTruncateAof 最小同步地址计算段（精确锚点见本文件 473 行）
  pub fn min_aof_address_from_active_sync_tasks(&self) -> AofAddress {
    let mut min_addr = AofAddress::create(self.sublog_count as i32, i64::MAX);
    self.registry.for_each(|d| {
      for i in 0..self.sublog_count {
        let prev = d.get_previous_address(i);
        if let Some(cur) = min_addr.get(i)
          && prev < cur
        {
          min_addr.set(i, prev);
        }
      }
    });
    min_addr
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:SafeTruncateAof
  ///
  /// 全子日志向量形态安全截断（单轮遍历完成截断计算与原子更新，零堆分配）。
  /// 截断走 [`GarnetLog::truncate_until_async`] 唯一物理回收真身（对标 C# 快形态
  /// `UnsafeShiftBeginAddress(truncateLog: true)` 的即时删段，取代 C# 的
  /// `TruncateUntil + Commit` 逻辑截断组合——rust 提交面不删段，逻辑截断永不落盘）。
  pub async fn safe_truncate_aof(&self, truncate_until: &AofAddress) -> AofAddress {
    let mut safe_limit = *truncate_until;
    let min_active = self.min_aof_address_from_active_sync_tasks();
    for i in 0..self.sublog_count {
      if let Some(min_prev) = min_active.get(i)
        && let Some(cur) = safe_limit.get(i)
        && min_prev < cur
      {
        safe_limit.set(i, min_prev);
      }
    }

    let watermark = {
      let mut guard = self.truncated_until.write();
      for i in 0..self.sublog_count {
        if let Some(val) = safe_limit.get(i)
          && val > guard.get(i).unwrap_or(0)
        {
          guard.set(i, val);
        }
      }
      *guard
    };

    // 先取出日志句柄再 await：parking_lot 读锁守卫非 Send，不得跨 await 持有
    let log = self.log.read().clone();
    if let Some(log) = log.as_ref() {
      log.truncate_until_async(&watermark).await;
      log.commit();
    }

    safe_limit
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:PublishShippedAddress
  ///
  /// 收集指定子日志跨所有副本已推送的最小高水位（无副本时返回 i64::MAX 解除门控）
  /// 并写入背压闸门
  pub fn publish_shipped_address(&self, physical_sublog_idx: usize) -> i64 {
    let mut min_shipped = i64::MAX;
    let mut has_drivers = false;
    self.registry.for_each(|d| {
      has_drivers = true;
      let addr = d.get_shipped_watermark_address(physical_sublog_idx);
      if addr < min_shipped {
        min_shipped = addr;
      }
    });
    if !has_drivers {
      min_shipped = i64::MAX;
    }
    if let Some(bp) = self.backpressure.read().as_ref() {
      bp.publish_shipped_address(physical_sublog_idx, min_shipped);
    }
    min_shipped
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:PublishShippedAddresses
  ///
  /// 收集全子日志跨所有副本已推送的最小高水位向量
  pub fn publish_shipped_addresses(&self) -> AofAddress {
    let mut result = AofAddress::create(self.sublog_count as i32, i64::MAX);
    self.registry.for_each(|d| {
      for i in 0..self.sublog_count {
        let addr = d.get_shipped_watermark_address(i);
        let cur = result.get(i).unwrap_or(i64::MAX);
        if addr < cur {
          result.set(i, addr);
        }
      }
    });
    self.publish_min_to_gate(&result);
    result
  }

  /// 向背压闸门写入全子日志最小已发水位向量（无闸门时直通）
  fn publish_min_to_gate(&self, min_addresses: &AofAddress) {
    if let Some(bp) = self.backpressure.read().as_ref() {
      for i in 0..self.sublog_count {
        bp.publish_shipped_address(i, min_addresses.get(i).unwrap_or(i64::MAX));
      }
    }
  }

  /// 驱动集合变更后的闸门重报
  fn publish_shipped_addresses_to_gate(&self) {
    self.publish_shipped_addresses();
  }

  /// 背压发布增量阈值（C# backpressure.PublishDeltaBytes，闸门缺席取 1）
  fn publish_delta(&self) -> i64 {
    self
      .backpressure
      .read()
      .as_ref()
      .map_or(1, |bp| bp.publish_delta_bytes())
  }

  /// 节流内核：单驱动遍历任务执行 task.throttle，收集有进展的子日志下标，
  /// 不发水位（发布统一由 [`Self::publish_dirty`] 锁外批量完成）
  fn throttle_driver(&self, driver: &AofSyncDriver, delta: i64, dirty: &mut Vec<usize>) {
    for (idx, task) in driver.tasks().iter().enumerate() {
      if task.throttle(delta).is_some() {
        dirty.push(idx);
      }
    }
  }

  /// 有进展子日志去重后逐个重报水位，返回是否发生发布
  fn publish_dirty(&self, dirty: &mut Vec<usize>) -> bool {
    if dirty.is_empty() {
      return false;
    }
    dirty.sort_unstable();
    dirty.dedup();
    for &idx in dirty.iter() {
      self.publish_shipped_address(idx);
    }
    true
  }

  /// 后台同步循环的副本节流点（对标 C# AofSyncTask.Throttle 的发布链，
  /// 发布口径与 [`Self::throttle_all`] 统一为去重后批量）
  pub fn throttle_replica(&self, remote_node_id: u128) -> bool {
    let Some(driver) = self.registry.get(&remote_node_id) else {
      return false;
    };
    let mut dirty = Vec::new();
    self.throttle_driver(&driver, self.publish_delta(), &mut dirty);
    self.publish_dirty(&mut dirty)
  }

  /// 对全部在册副本执行节流扫描，有进展子日志收集去重后批量重报水位
  ///（对标 C# AofSyncTask.Throttle 内 PublishShippedAddress，子日志数无上限）
  pub fn throttle_all(&self) -> bool {
    let delta = self.publish_delta();
    let mut dirty = Vec::new();
    self
      .registry
      .for_each(|driver| self.throttle_driver(driver, delta, &mut dirty));
    self.publish_dirty(&mut dirty)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:GetReplicaInfo
  ///
  /// 导出全部副本的位点同步与延迟统计数据
  pub fn get_replica_info(&self, primary_offset: &AofAddress) -> Vec<ReplicaRoleInfo> {
    let mut result = Vec::with_capacity(self.registry.count());
    self.registry.for_each(|d| {
      let prev = d.previous_address();
      let lag = primary_offset.diff(&prev);
      result.push(ReplicaRoleInfo {
        node_id: d.remote_node_id(),
        is_connected: d.is_connected(),
        replication_offset: prev,
        replication_lag: lag,
      });
    });
    result
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:Reset
  ///
  /// 清空并释放所有活跃副本复制驱动；无副本后向闸门写 MAX 水位
  pub fn reset(&self) {
    self.registry.reset();
    self.publish_shipped_addresses_to_gate();
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:Dispose
  pub fn dispose(&self) {
    self.reset();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_aof_sync_driver() {
    let start = AofAddress::create(2, 500);
    let driver = AofSyncDriver::new(0x10CA1, 0x12, 2, &start, None);

    assert_eq!(driver.remote_node_id(), 0x12);
    assert!(driver.is_connected());
    assert_eq!(driver.get_start_address(0), 500);
    assert_eq!(driver.get_previous_address(1), 500);
    assert_eq!(driver.get_shipped_watermark_address(0), 500);

    let zero_start = AofAddress::create(2, 0);
    let driver0 = AofSyncDriver::new(0x10CA1, 0x13, 2, &zero_start, None);
    assert_eq!(driver0.get_start_address(0), 0);

    let task0 = driver.get_task(0).unwrap();
    task0.consume(b"abc", 500, 600).unwrap();
    assert_eq!(driver.get_previous_address(0), 600);
    assert_eq!(driver.previous_address().get(0), Some(600));
    assert_eq!(driver.previous_address().get(1), Some(500));
    assert_eq!(driver.get_shipped_watermark_address(0), 600);
  }

  /// 任务数由装配注入的 sublog_count 决定，与上报位点向量长度脱钩
  ///（对标 C# :111 任务数组尺寸取 AofPhysicalSublogCount）：短向量缺位
  /// 按 0 兜底、多余位点忽略，杜绝「副本上报向量长度推任务数」的第二口径
  #[test]
  fn test_driver_task_count_follows_assembly_value_not_address_length() {
    let driver = AofSyncDriver::new(0x10CA1, 0x12, 2, &AofAddress::create(1, 500), None);
    assert_eq!(driver.tasks().len(), 2);
    assert_eq!(driver.get_start_address(0), 500);
    assert_eq!(driver.get_start_address(1), 0);

    let driver = AofSyncDriver::new(0x10CA1, 0x13, 1, &AofAddress::create(2, 500), None);
    assert_eq!(driver.tasks().len(), 1);
    assert_eq!(driver.get_start_address(0), 500);
    assert_eq!(driver.get_start_address(1), 0);
  }

  #[test]
  fn test_aof_sync_driver_store_basic() {
    let store = AofSyncDriverStore::new(2);
    assert_eq!(store.count(), 0);

    let d1 = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x12,
      2,
      &AofAddress::create(2, 100),
      None,
    ));
    assert!(store.try_add_replication_driver(d1, false));
    assert_eq!(store.count(), 1);
    assert_eq!(store.count_connected_replicas(), 1);

    let min_addr = store.min_aof_address_from_active_sync_tasks();
    assert_eq!(min_addr.get(0), Some(100));

    assert!(store.try_remove(0x12));
    assert_eq!(store.count(), 0);
  }

  /// throttle_all 对全部有进展子日志发布水位（对标 C# AofSyncTask.Throttle 内
  /// 逐任务 PublishShippedAddress；AofAddress 容量上限 MAX_SUBLOG_COUNT = 4，
  /// 旧 dirty_mask u64 的 64 封顶在当前架构不可达，动态收集消除该隐患）
  #[test]
  fn test_throttle_all_publishes_every_dirty_sublog() {
    let store = AofSyncDriverStore::new(4);
    let bp = Arc::new(AofBackpressure::new(4, 100));
    store.attach_backpressure(Some(Arc::clone(&bp)));

    let driver = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x12,
      4,
      &AofAddress::create(4, 100),
      None,
    ));
    assert!(store.try_add_replication_driver(driver, false));

    // 全部子日志推进已发送水位（未注入 wire 的记账形态）
    let d = store.registry.get(&0x12).unwrap();
    for idx in 0..4 {
      d.task_ref(idx).unwrap().consume(b"abc", 100, 200).unwrap();
    }

    assert!(store.throttle_all());
    for idx in 0..4 {
      assert_eq!(
        bp.get_shipped_watermark(idx),
        200,
        "子日志 {idx} 水位应发布到闸门"
      );
    }
  }

  /// 单副本节流与全量节流的内核等价性（节流单源化的直接回归）：
  /// 同一 driver 集合、同一推进状态，throttle_replica 与 throttle_all
  /// 产出同一发布结果与同一闸门水位序列
  #[test]
  fn test_throttle_replica_matches_throttle_all() {
    fn setup() -> (AofSyncDriverStore, Arc<AofBackpressure>) {
      let store = AofSyncDriverStore::new(3);
      let bp = Arc::new(AofBackpressure::new(3, 100));
      store.attach_backpressure(Some(Arc::clone(&bp)));
      let driver = Arc::new(AofSyncDriver::new(
        0x10CA1,
        0x12,
        3,
        &AofAddress::create(3, 100),
        None,
      ));
      assert!(store.try_add_replication_driver(driver, false));
      let d = store.registry.get(&0x12).unwrap();
      for idx in 0..3 {
        d.task_ref(idx).unwrap().consume(b"abc", 100, 200).unwrap();
      }
      (store, bp)
    }

    let (store_a, bp_a) = setup();
    let (store_b, bp_b) = setup();

    assert!(store_a.throttle_replica(0x12));
    assert!(store_b.throttle_all());
    for idx in 0..3 {
      assert_eq!(
        bp_a.get_shipped_watermark(idx),
        bp_b.get_shipped_watermark(idx),
        "子日志 {idx} 两条节流路径发布结果应一致"
      );
      assert_eq!(bp_a.get_shipped_watermark(idx), 200);
    }

    // 无新增量时两路径同样都不再发布
    assert!(!store_a.throttle_replica(0x12));
    assert!(!store_b.throttle_all());
  }

  /// 实例匹配退场移除（对标 C# TryRemove(AofSyncDriver) 的
  /// `syncDriver == aofSyncDriver` 引用匹配）：同键重挂新驱动不被旧驱动
  /// 退场误删，新实例自身退场命中移除
  #[test]
  fn test_try_remove_current_does_not_remove_reattached_driver() {
    let store = AofSyncDriverStore::new(1);
    let old = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x12,
      1,
      &AofAddress::create(1, 100),
      None,
    ));
    assert!(store.try_add_replication_driver(Arc::clone(&old), false));

    // 重挂置换：同键新驱动入库（attach_replica_wire 先 try_remove 后
    // register 完成后的 registry 形态：键对应新实例）
    let fresh = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x12,
      1,
      &AofAddress::create(1, 200),
      None,
    ));
    assert!(store.try_add_replication_driver(fresh.clone(), false));

    // 旧驱动退场移除：实例不匹配 → 新驱动保留
    assert!(
      !store.try_remove_current(&old),
      "同键新驱动不得被旧实例退场误删"
    );
    assert!(Arc::ptr_eq(&store.registry.get(&0x12).unwrap(), &fresh));
    assert!(old.is_connected(), "未命中实例不得被退场处置");

    // 新实例自身退场：命中移除
    assert!(store.try_remove_current(&fresh));
    assert_eq!(store.count(), 0);
    assert!(store.registry.get(&0x12).is_none());
  }
}
