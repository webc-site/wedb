use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};

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
/// （C# RunAsync 常驻泵由 pump 模块的 AofReplicationPump 整体承接）
#[derive(Debug)]
pub struct AofSyncDriver {
  local_node_id: u128,
  remote_node_id: u128,
  tasks: Vec<Arc<AofSyncTask>>,
  /// 推流单飞闸：true = 某条 pump_backlog 正在驱动本副本（信号唤醒循环与
  /// attach 期手动补扫共用泵体，原子位互斥避免双泵对同一 accepted_address
  /// 起扫——后到者 consume 复帧报 InvalidInput 误删健康驱动）
  pub(crate) pumping: AtomicBool,
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
      pumping: AtomicBool::new(false),
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

  /// 逐任务地址向量采集（start_address/previous_address 两入口同形收口）
  fn collect_address(&self, mut get: impl FnMut(&AofSyncTask) -> i64) -> AofAddress {
    let mut addr = AofAddress::new(self.tasks.len() as i32);
    for (i, task) in self.tasks.iter().enumerate() {
      addr.set(i, get(task));
    }
    addr
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:StartAddress
  ///
  /// 获取所有子任务的起始地址集合
  pub fn start_address(&self) -> AofAddress {
    self.collect_address(AofSyncTask::start_address)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:PreviousAddress
  ///
  /// 获取所有子任务的当前已发送位点集合
  pub fn previous_address(&self) -> AofAddress {
    self.collect_address(AofSyncTask::previous_address)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetPreviousAddress
  ///
  /// 获取指定子日志的当前已发送位点
  pub fn get_previous_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .task_ref(physical_sublog_idx)
      .map_or(0, AofSyncTask::previous_address)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetShippedWatermarkAddress
  ///
  /// 获取指定子日志已推送的高水位线
  pub fn get_shipped_watermark_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .task_ref(physical_sublog_idx)
      .map_or(0, AofSyncTask::shipped_watermark_address)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:GetStartAddress
  ///
  /// 获取指定子日志的起始位点
  pub fn get_start_address(&self, physical_sublog_idx: usize) -> i64 {
    self
      .task_ref(physical_sublog_idx)
      .map_or(0, AofSyncTask::start_address)
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
  /// 推流轮转游标：每轮 pump_backlog 起始下标取游标模快照长度并推进
  /// （多副本按轮公平分时——单趟窗口受限后，持续写入下慢副本不再无限
  /// 占泵饿死快照序靠后的副本）
  pump_cursor: AtomicUsize,
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
      pump_cursor: AtomicUsize::new(0),
    }
  }

  /// 推流轮转游标推进（取走即加，调用方模快照长度取起始下标）
  #[inline]
  pub(crate) fn next_pump_cursor(&self) -> usize {
    self.pump_cursor.fetch_add(1, Ordering::Relaxed)
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
  ///
  /// 副本已截断位点向量的单调推进：取大判据单点在 [`AofAddress::monotonic_update`]
  /// （对位 C# AofAddress.MonotonicUpdate），本处只转调不内联复刻
  pub fn update_truncated_until(&self, truncated: &AofAddress) {
    self.truncated_until.write().monotonic_update(truncated);
  }

  /// 单驱动入库入口闸门（添加与批量添加共用）：起始位点早于已截断范围且
  /// 不允许丢数据则拒绝，告警文案与判据单源
  fn start_gate_ok(driver: &AofSyncDriver, truncated: &AofAddress, allow_data_loss: bool) -> bool {
    let start_address = driver.start_address();
    if start_address.any_lesser(truncated) && !allow_data_loss {
      warn!(
        "AOF sync driver for {} with start address {:?} rejected, local AOF is truncated until {:?}",
        hex_str_u128(driver.remote_node_id()),
        start_address,
        *truncated
      );
      return false;
    }
    true
  }

  /// 注册单驱动并回报被置换的旧驱动（同实例置换不算退场对象）
  fn register_one(&self, driver: &Arc<AofSyncDriver>) -> Option<Arc<AofSyncDriver>> {
    trace!(
      "Added/updated AofSyncDriver for {}",
      hex_str_u128(driver.remote_node_id())
    );
    self
      .registry
      .register(driver.remote_node_id(), Arc::clone(driver))
      .filter(|prev| !Arc::ptr_eq(prev, driver))
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryAddReplicationDriver
  ///
  /// 添加或原子原地置换指定副本的复制驱动器；若起始位点早于已截断范围且不允许丢数据则拒绝添加。
  /// 在 write 锁临界区内校验并原地覆盖已有同 node_id 驱动，被置换旧驱动退场 dispose，
  /// 与 safe_truncate_aof 严格互斥，杜绝先摘后挂窗口导致截断线越过授予位点删段。
  /// 驱动集合变更后向背压闸门重报全子日志水位（对标 C# 成功路径尾部
  /// PublishShippedAddresses：attach 副本会立即收紧各子日志最小已发水位）
  pub fn try_add_replication_driver(
    &self,
    driver: Arc<AofSyncDriver>,
    allow_data_loss: bool,
  ) -> bool {
    let prev = {
      let guard = self.truncated_until.write();
      if !Self::start_gate_ok(&driver, &guard, allow_data_loss) {
        return false;
      }
      self.register_one(&driver)
    };

    if let Some(prev) = prev {
      prev.dispose();
    }
    self.publish_shipped_addresses_to_gate();
    true
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryAddReplicationDriver
  ///
  /// 原子置换或添加指定副本的复制驱动器（与 [`Self::try_add_replication_driver`] 同构）；
  /// 在 parking_lot write 锁临界区内原子校验起始位点并原地更新/替换已有同 node_id 驱动，
  /// 消除先 try_remove 后 try_add 的非原子窗口
  #[inline]
  pub fn try_replace_replication_driver(
    &self,
    driver: Arc<AofSyncDriver>,
    allow_data_loss: bool,
  ) -> bool {
    self.try_add_replication_driver(driver, allow_data_loss)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:TryAddReplicationDrivers
  ///
  /// 批量添加/注册副本驱动集合
  pub fn try_add_replication_drivers(
    &self,
    drivers: &[Arc<AofSyncDriver>],
    allow_data_loss: bool,
  ) -> bool {
    let prevs = {
      let guard = self.truncated_until.write();
      for driver in drivers {
        if !Self::start_gate_ok(driver, &guard, allow_data_loss) {
          return false;
        }
      }
      let prevs: Vec<_> = drivers
        .iter()
        .filter_map(|d| self.register_one(d))
        .collect();
      prevs
    };

    for prev in prevs {
      prev.dispose();
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

  /// 全驱动逐子日志取小折叠（i64::MAX 为单位元初值）；`get` 选择位点来源，
  /// 已发送位点向量与已推送水位向量两路同形共用
  fn fold_min_addresses(&self, mut get: impl FnMut(&AofSyncDriver, usize) -> i64) -> AofAddress {
    let mut min_addr = AofAddress::create(self.sublog_count as i32, i64::MAX);
    self.registry.for_each(|d| {
      for i in 0..self.sublog_count {
        let prev = get(d, i);
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
  /// 获取全部活跃同步任务中的最小 AOF 地址向量（对标 C# AofSyncDriverStore.cs:85-91 与 :140-149）
  pub fn min_aof_address_from_active_sync_tasks(&self) -> AofAddress {
    self.fold_min_addresses(|d, i| d.get_previous_address(i))
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:SafeTruncateAof
  ///
  /// 全子日志向量形态安全截断（单轮遍历完成截断计算与原子更新，零堆分配）。
  /// 截断走 [`GarnetLog::truncate_until_async`] 唯一物理回收真身（对标 C# 快形态
  /// `UnsafeShiftBeginAddress(truncateLog: true)` 的即时删段，取代 C# 的
  /// `TruncateUntil + Commit` 逻辑截断组合——rust 提交面不删段，逻辑截断永不落盘）。
  pub async fn safe_truncate_aof(&self, truncate_until: &AofAddress) -> AofAddress {
    let mut safe_limit = *truncate_until;
    let (watermark, safe_limit) = {
      let mut guard = self.truncated_until.write();
      let min_active = self.min_aof_address_from_active_sync_tasks();
      // 取小方向单点（对位 C# AofAddress.MinExchange）：安全水位被活跃副本的
      // 已发位点下界钳制
      safe_limit.min_exchange(&min_active);

      // 取大方向单点（对位 C# AofAddress.MonotonicUpdate）：截断位点只升不降
      guard.monotonic_update(&safe_limit);
      (*guard, safe_limit)
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
    self.registry.for_each(|d| {
      let addr = d.get_shipped_watermark_address(physical_sublog_idx);
      if addr < min_shipped {
        min_shipped = addr;
      }
    });
    if let Some(bp) = self.backpressure.read().as_ref() {
      bp.publish_shipped_address(physical_sublog_idx, min_shipped);
    }
    min_shipped
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:PublishShippedAddresses
  ///
  /// 收集全子日志跨所有副本已推送的最小高水位向量（取小折叠复用
  /// [`Self::fold_min_addresses`]），并写入背压闸门
  pub fn publish_shipped_addresses(&self) -> AofAddress {
    let result = self.fold_min_addresses(|d, i| d.get_shipped_watermark_address(i));
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

  /// 对全部在册副本执行节流扫描，有进展子日志收集去重后批量重报水位；
  /// 断连驱动同轮出册退场（对标 C# Throttle :214-215 断连抛出 → RunAsync
  /// finally TryRemove(this) 的统一退场；实例匹配防误删并发重挂的同节点
  /// 新驱动）——零写入期死副本经此周期臂出册，AOF 截断线与背压闸门不被
  /// 失联位点钉死（consume 错误臂需泵在跑，空闲期不可达）
  pub fn throttle_all(&self) -> bool {
    let delta = self.publish_delta();
    let mut dirty = Vec::new();
    for driver in self.drivers() {
      if !driver.is_connected() {
        self.try_remove_current(&driver);
        continue;
      }
      self.throttle_driver(&driver, delta, &mut dirty);
    }
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

    let task0 = driver.task_ref(0).unwrap();
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

    // 重挂置换：同键新驱动入库（原地覆盖置换，被替换旧驱动退场 dispose）
    let fresh = Arc::new(AofSyncDriver::new(
      0x10CA1,
      0x12,
      1,
      &AofAddress::create(1, 200),
      None,
    ));
    assert!(store.try_add_replication_driver(fresh.clone(), false));
    assert!(!old.is_connected(), "被置换旧驱动已在置换时 dispose");
    assert!(fresh.is_connected(), "新驱动保持连接健康");

    // 旧驱动退场移除：实例不匹配 → 新驱动保留
    assert!(
      !store.try_remove_current(&old),
      "同键新驱动不得被旧实例退场误删"
    );
    assert!(Arc::ptr_eq(&store.registry.get(&0x12).unwrap(), &fresh));
    assert!(fresh.is_connected(), "新驱动不得被旧实例退场误处置");

    // 新实例自身退场：命中移除并 dispose
    assert!(store.try_remove_current(&fresh));
    assert_eq!(store.count(), 0);
    assert!(store.registry.get(&0x12).is_none());
    assert!(!fresh.is_connected(), "自身退场后驱动被 dispose");
  }
}
