use std::sync::Arc;

use log::{trace, warn};
use parking_lot::RwLock;
use waof::AofAddress;
use wnode::aof::aof_backpressure::AofBackpressure;

/// AOF 增量同步背压门控通知面
pub trait AofBackpressureFace: Send + Sync {
  /// 刷新指定物理子日志跨副本最小已推送水位
  fn publish_shipped_address(&self, physical_sublog_idx: usize, min_shipped: i64);

  /// 增量发布阈值（字节数）
  fn publish_delta_bytes(&self) -> i64 {
    1
  }
}

impl AofBackpressureFace for AofBackpressure {
  fn publish_shipped_address(&self, physical_sublog_idx: usize, min_shipped: i64) {
    self.publish_shipped_address(physical_sublog_idx, min_shipped);
  }

  fn publish_delta_bytes(&self) -> i64 {
    self.publish_delta_bytes()
  }
}

/// AOF 增量同步背压门控具体枚举（消除 dyn）
#[derive(Clone)]
pub enum AofBackpressureGate {
  /// 真实 AofBackpressure
  Production(Arc<AofBackpressure>),
  /// 水位原子更新（测试用）
  Watermark {
    watermark: Arc<std::sync::atomic::AtomicI64>,
    publish_delta: i64,
  },
  /// 日志记录（测试用）
  Log(Arc<RwLock<Vec<(usize, i64)>>>),
}

impl AofBackpressureFace for AofBackpressureGate {
  fn publish_shipped_address(&self, physical_sublog_idx: usize, min_shipped: i64) {
    match self {
      Self::Production(p) => p.publish_shipped_address(physical_sublog_idx, min_shipped),
      Self::Watermark { watermark, .. } => {
        watermark.store(min_shipped, std::sync::atomic::Ordering::Release)
      }
      Self::Log(log) => log.write().push((physical_sublog_idx, min_shipped)),
    }
  }

  fn publish_delta_bytes(&self) -> i64 {
    match self {
      Self::Production(p) => p.publish_delta_bytes(),
      Self::Watermark { publish_delta, .. } => *publish_delta,
      Self::Log(_) => 1,
    }
  }
}

impl From<Arc<AofBackpressure>> for AofBackpressureGate {
  fn from(p: Arc<AofBackpressure>) -> Self {
    Self::Production(p)
  }
}

use crate::server::replication::{aof_sync_driver::AofSyncDriver, driver_registry::DriverRegistry};

/// 副本同步会话角色统计信息（对标 C# RoleInfo）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaRoleInfo {
  pub node_id: String,
  pub is_connected: bool,
  pub replication_offset: AofAddress,
  pub replication_lag: AofAddress,
}

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore
///
/// 主节点 AOF 增量复制驱动管理器，协调全副本同步进度、安全日志截断、ACK 确认与背压门控水位
pub struct AofSyncDriverStore {
  registry: DriverRegistry<String, AofSyncDriver>,
  truncated_until: RwLock<AofAddress>,
  sublog_count: usize,
  /// 主侧 AOF 背压闸门（C# 经 clusterProvider.storeWrapper.appendOnlyFile.backpressure
  /// 反查；Rust 依赖方向反转，集群装配期注入，见 attach_backpressure）
  backpressure: RwLock<Option<AofBackpressureGate>>,
}

impl AofSyncDriverStore {
  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore
  pub fn new(sublog_count: usize) -> Self {
    Self {
      registry: DriverRegistry::new(),
      truncated_until: RwLock::new(AofAddress::create(sublog_count as i32, 0)),
      sublog_count,
      backpressure: RwLock::new(None),
    }
  }

  /// 注入主侧 AOF 背压闸门（集群装配期一次注入；对标 C# AofSyncTask 构造内
  /// `backpressure = appendOnlyFile?.backpressure` 的装配关系）
  pub fn attach_backpressure(&self, gate: Option<AofBackpressureGate>) {
    *self.backpressure.write() = gate;
  }

  /// 背压闸门快照
  pub fn backpressure(&self) -> Option<AofBackpressureGate> {
    self.backpressure.read().clone()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverCount
  pub fn count(&self) -> usize {
    self.registry.count()
  }

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
        driver.remote_node_id(),
        start_address,
        truncated
      );
      return false;
    }

    trace!(
      "Added/updated AofSyncDriver for {}",
      driver.remote_node_id()
    );
    self
      .registry
      .register(driver.remote_node_id().to_string(), driver);
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

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryRemove
  ///
  /// 移除指定副本节点的同步驱动；移除后向背压闸门重报水位
  ///（对标 C# 成功路径尾部 PublishShippedAddresses：detach 副本可能放宽水位）
  pub fn try_remove(&self, remote_node_id: &str) -> bool {
    let removed = self.registry.remove(remote_node_id);
    if removed.is_some() {
      trace!("Removed AofSyncDriver for {}", remote_node_id);
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

  /// 处理副本上报的位点确认 ACK
  pub fn process_replica_ack(
    &self,
    remote_node_id: &str,
    physical_sublog_idx: usize,
    acked_offset: i64,
  ) -> bool {
    if let Some(driver) = self.registry.get(remote_node_id) {
      driver.process_ack(physical_sublog_idx, acked_offset)
    } else {
      false
    }
  }

  /// 扫描并剔除 ACK 超时的副本连接（单次批量写锁 + 锁外清理）
  pub fn prune_timed_out_replicas(&self, timeout_ms: i64) -> Vec<String> {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    // 依驱动自身 ACK 状态判定，键为副本 ID 忽略
    self
      .registry
      .retain(|_replica_id, driver| !driver.is_any_task_ack_timed_out(now_ms, timeout_ms))
  }

  /// 单个物理子日志截断计算（单遍迭代求最小，零堆分配）
  ///
  /// `replication_upper_bound`：对标 C# "Bound truncation by replicationOffset"
  /// （防截断越过活跃重放位点；PRIMARY 调用方传 i64::MAX 即不限制）
  pub fn safe_truncate_sublog(
    &self,
    truncate_until: i64,
    physical_sublog_idx: usize,
    replication_upper_bound: i64,
  ) -> i64 {
    let mut min_prev = truncate_until;
    self.registry.for_each(|d| {
      let prev = d.get_previous_address(physical_sublog_idx);
      if prev < min_prev {
        min_prev = prev;
      }
    });
    let safe_limit = truncate_until.min(min_prev).min(replication_upper_bound);

    let mut guard = self.truncated_until.write();
    let cur = guard.get(physical_sublog_idx).unwrap_or(0);
    if safe_limit > cur {
      guard.set(physical_sublog_idx, safe_limit);
    }

    safe_limit
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:SafeTruncateAof
  ///
  /// 全子日志向量形态安全截断（单轮遍历完成截断计算与原子更新，零堆分配）
  pub fn safe_truncate_aof(&self, truncate_until: &AofAddress) -> AofAddress {
    let mut safe_limit = *truncate_until;
    self.registry.for_each(|d| {
      for i in 0..self.sublog_count {
        let addr = d.get_previous_address(i);
        if let Some(cur) = safe_limit.get(i)
          && addr < cur
        {
          safe_limit.set(i, addr);
        }
      }
    });

    let mut guard = self.truncated_until.write();
    for i in 0..self.sublog_count {
      if let Some(val) = safe_limit.get(i)
        && val > guard.get(i).unwrap_or(0)
      {
        guard.set(i, val);
      }
    }

    safe_limit
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AssertDoesNotExist
  ///
  /// 断言指定远端副本驱动在仓库中不存在（调试状态一致性校验）
  pub fn assert_does_not_exist(&self, remote_node_id: &str) {
    debug_assert!(
      self.registry.get(remote_node_id).is_none(),
      "syncDriver with {remote_node_id} should not exist!"
    );
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:PublishShippedAddress
  ///
  /// 收集指定子日志跨所有副本已推送的最小高水位（无副本时返回 i64::MAX 解除门控）
  /// 并写入背压闸门（对标 C# 内部同名方法：sync task 每次推送进展后刷新本子日志水位，
  /// 只重算有进展子日志的最小值——新鲜度提示而非正确性动作，陈旧值只会让追加方更早停滞）
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
    if let Some(gate) = self.backpressure.read().as_ref() {
      gate.publish_shipped_address(physical_sublog_idx, min_shipped);
    }
    min_shipped
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:PublishShippedAddresses
  ///
  /// 收集全子日志跨所有副本已推送的最小高水位向量（单次全局遍历，消除 N 次重复分配与读锁争用）
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

  /// 向背压闸门写入全子日志最小已发水位向量（无闸门时直通，零 Arc 克隆）
  ///
  /// 对标 C# PublishShippedAddresses 内部对 `backpressure.PublishShippedAddress`
  /// 的逐子日志写入；无副本（或 dispose 期）写 i64::MAX 释放门控
  fn publish_min_to_gate(&self, min_addresses: &AofAddress) {
    if let Some(gate) = self.backpressure.read().as_ref() {
      for i in 0..self.sublog_count {
        gate.publish_shipped_address(i, min_addresses.get(i).unwrap_or(i64::MAX));
      }
    }
  }

  /// 驱动集合变更后的闸门重报（对标 C# TryAdd/TryRemove/Reset 尾部 PublishShippedAddresses）
  fn publish_shipped_addresses_to_gate(&self) {
    self.publish_shipped_addresses();
  }

  /// 后台同步循环的副本节流点（对标 C# AofSyncTask.Throttle 的发布链）
  ///
  /// C# 每个 AofSyncTask 后台循环在迭代间隙调 Throttle()：内部按
  /// `pending >= backpressure.PublishDeltaBytes || idle` 判定后调
  /// `aofSyncDriverStore.PublishShippedAddress(physicalSublogIdx)` 刷新闸门；
  /// Rust 的 AofSyncTask 不持有 store 引用，由本方法把「逐 task 节流 → 有
  /// 进展子日志重报」的组合上收到 store 侧。publish_delta_bytes 缺省取闸门
  /// 配置（未挂闸门时退化为 C# backpressure == null 的直通路径）
  pub fn throttle_replica(&self, remote_node_id: &str) -> bool {
    let Some(driver) = self.registry.get(remote_node_id) else {
      return false;
    };
    let publish_delta = self
      .backpressure
      .read()
      .as_ref()
      .map_or(1, |gate| gate.publish_delta_bytes());
    let mut published = false;
    for idx in 0..driver.tasks().len() {
      // throttle 返回 Some(水位) 即该子日志到达发布条件（增量足够或空闲收尾）
      if driver.tasks()[idx].throttle(publish_delta).is_some() {
        self.publish_shipped_address(idx);
        published = true;
      }
    }
    published
  }

  /// 对全部在册副本执行节流扫描，并在锁外批量重报有进展的子日志水位（零堆分配，位掩码标记）
  ///
  /// 锁纪律保证：在 `registry` 读锁内仅执行无锁原子推进 `task.throttle(...)` 并收集脏位点，
  /// 读锁完全释放后再逐子日志调用 `publish_shipped_address`，彻底杜绝读锁嵌套导致的写优先级死锁。
  pub fn throttle_all(&self) -> bool {
    let publish_delta = self
      .backpressure
      .read()
      .as_ref()
      .map_or(1, |gate| gate.publish_delta_bytes());
    let mut dirty_mask = 0u64;

    self.registry.for_each(|driver| {
      for (idx, task) in driver.tasks().iter().enumerate() {
        if idx < 64 && task.throttle(publish_delta).is_some() {
          dirty_mask |= 1u64 << idx;
        }
      }
    });

    if dirty_mask != 0 {
      for idx in 0..self.sublog_count.min(64) {
        if (dirty_mask & (1u64 << idx)) != 0 {
          self.publish_shipped_address(idx);
        }
      }
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:GetReplicaInfo
  ///
  /// 导出全部副本的位点同步与延迟统计数据（零多余快照分配）
  pub fn get_replica_info(&self, primary_offset: &AofAddress) -> Vec<ReplicaRoleInfo> {
    let mut result = Vec::with_capacity(self.registry.count());
    self.registry.for_each(|d| {
      let prev = d.previous_address();
      let lag = primary_offset.diff(&prev);
      result.push(ReplicaRoleInfo {
        node_id: d.remote_node_id().to_string(),
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
  ///（对标 C# Reset 尾部注释：no drivers remain → max watermark → 门控全放行）
  pub fn reset(&self) {
    self.registry.reset();
    self.publish_shipped_addresses_to_gate();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  struct MockGate {
    published: RwLock<Vec<(usize, i64)>>,
  }

  impl MockGate {
    fn new() -> Self {
      Self {
        published: RwLock::new(Vec::new()),
      }
    }
    fn last_for_sublog(&self, sublog: usize) -> Option<i64> {
      self
        .published
        .read()
        .iter()
        .rev()
        .find(|(s, _)| *s == sublog)
        .map(|(_, v)| *v)
    }
  }

  impl AofBackpressureFace for MockGate {
    fn publish_shipped_address(&self, physical_sublog_idx: usize, min_shipped: i64) {
      self
        .published
        .write()
        .push((physical_sublog_idx, min_shipped));
    }
  }

  #[test]
  fn test_driver_store_lifecycle_and_safe_truncate() {
    let store = AofSyncDriverStore::new(2);
    assert_eq!(store.count(), 0);

    let d1 = Arc::new(AofSyncDriver::new(
      "local".to_string(),
      "r1".to_string(),
      &AofAddress::create(2, 100),
    ));
    assert!(store.try_add_replication_driver(d1.clone(), false));
    assert_eq!(store.count(), 1);
    assert_eq!(store.count_connected_replicas(), 1);

    // 尝试添加截断位点之前的副本驱动（非 allow_data_loss 拒绝）
    store.update_truncated_until(&AofAddress::create(2, 200));
    let d2 = Arc::new(AofSyncDriver::new(
      "local".to_string(),
      "r2".to_string(),
      &AofAddress::create(2, 50),
    ));
    assert!(!store.try_add_replication_driver(d2.clone(), false));
    assert!(store.try_add_replication_driver(d2.clone(), true)); // 允许丢数据添加成功

    // ACK 处理
    assert!(store.process_replica_ack("r1", 0, 150));
    assert_eq!(d1.get_acked_address(0), 150);

    // 安全截断
    let safe = store.safe_truncate_sublog(300, 0, i64::MAX);
    assert_eq!(safe, 50); // 受限于 r2 的 50

    // 移除 r2
    assert!(store.try_remove("r2"));
    assert_eq!(store.count(), 1);
    let safe2 = store.safe_truncate_sublog(300, 0, i64::MAX);
    assert_eq!(safe2, 100); // 变为受限于 r1 的 100

    // replicationOffset 上界收紧（对标 C# 截断不越过活跃重放位点）
    let safe3 = store.safe_truncate_sublog(300, 0, 80);
    assert_eq!(safe3, 80);
  }

  /// 背压门接线闭环（对标 C# PublishShippedAddress/PublishShippedAddresses 的
  /// 闸门写入语义：attach 收紧水位、推送刷新水位、detach/reset 释放门控）
  #[test]
  fn test_backpressure_gate_wiring() {
    let gate = Arc::new(MockGate::new());
    let store = AofSyncDriverStore::new(2);
    store.attach_backpressure(Some(gate.clone()));

    // 无副本：attach 后发布应把闸门水位写为 MAX（门控放行）
    store.publish_shipped_addresses();
    assert_eq!(
      gate.last_for_sublog(0),
      Some(i64::MAX),
      "无副本时门控应放行"
    );

    // attach 副本（start=64）：TryAdd 尾部重报应立即收紧水位到 0
    let d1 = Arc::new(AofSyncDriver::new(
      "local".to_string(),
      "r1".to_string(),
      &AofAddress::create(2, 0),
    ));
    assert!(store.try_add_replication_driver(d1.clone(), false));
    assert_eq!(gate.last_for_sublog(0), Some(0), "副本 attach 后水位收紧");

    // 推送进展：两个子日志各自 consume 到 9500
    d1.get_task(0)
      .unwrap()
      .consume(b"payload", 64, 9_500)
      .unwrap();
    d1.get_task(1)
      .unwrap()
      .consume(b"payload", 64, 9_500)
      .unwrap();
    assert!(store.throttle_replica("r1"), "增量足够应触发水位重报");
    assert_eq!(gate.last_for_sublog(0), Some(9_500), "水位推进到 9500");

    // detach：TryRemove 尾部重报应写 MAX 释放门控
    assert!(store.try_remove("r1"));
    assert_eq!(
      gate.last_for_sublog(0),
      Some(i64::MAX),
      "副本移除后门控应放行"
    );

    // reset：无驱动场景同样释放
    let d2 = Arc::new(AofSyncDriver::new(
      "local".to_string(),
      "r2".to_string(),
      &AofAddress::create(2, 0),
    ));
    assert!(store.try_add_replication_driver(d2, false));
    store.reset();
    assert_eq!(
      gate.last_for_sublog(0),
      Some(i64::MAX),
      "reset 后门控应放行"
    );
  }
}
