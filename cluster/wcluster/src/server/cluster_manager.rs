use std::{
  fmt::Write as _,
  sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
  },
};

use log::trace;
use parking_lot::RwLock;

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, LOCAL_WORKER_ID},
    cluster_provider::ClusterProvider,
    hash_slot::SlotState,
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// garnet相对路径:Server:ClusterManager
pub struct ClusterManager {
  current_config: RwLock<ClusterConfig>,
  pub cluster_provider: Arc<ClusterProvider>,
  flush_count: AtomicI32,
  // Other fields omitted for simplicity in transpilation until full I/O is ready
}

impl ClusterManager {
  /// garnet相对路径:Server:ClusterManager:ClusterManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    let current_config = RwLock::new(ClusterConfig::new());
    // Init logic here
    Self {
      current_config,
      cluster_provider,
      flush_count: AtomicI32::new(0),
    }
  }

  /// NOTE: Unsafe! DO NOT USE, other than benchmarking
  /// garnet相对路径:Server:ClusterManager:UnsafeSetConfig
  pub fn unsafe_set_config(&self, cluster_config: ClusterConfig) {
    *self.current_config.write() = cluster_config;
  }

  /// garnet相对路径:Server:ClusterManager:InitLocal
  pub fn init_local(&self, address: &str, port: i32, recover_config: bool) {
    let mut config = self.current_config.write();
    if recover_config {
      // 先摘取本地字段再原地改写，避免 &mut 与读借用冲突
      let (node_id, config_epoch, role, primary_id) = {
        let c = &*config;
        (
          c.local_node_id().unwrap_or_default().to_string(),
          c.local_node_config_epoch(),
          c.local_node_role(),
          c.local_node_primary_id().map(String::from),
        )
      };
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: &node_id,
        address,
        port,
        config_epoch,
        role,
        replica_of_node_id: primary_id.as_deref(),
        hostname: None, // Format.GetHostName() equivalent
      });
    } else {
      let node_id = uuid::Uuid::new_v4().simple().to_string(); // equivalent to Generator.CreateHexId()
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: &node_id,
        address,
        port,
        config_epoch: 0,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
    }
  }

  /// garnet相对路径:Server:ClusterManager:FlushTaskAsync
  pub async fn flush_task_async(&self) {
    // mock flush task
  }

  /// garnet相对路径:Server:ClusterManager:DisposeBackgroundTasks
  pub fn dispose_background_tasks(&self) {
    // mock
  }

  /// garnet相对路径:Server:ClusterManager:Start
  pub fn start(&self) {
    // TryStartGossipTasks
  }

  /// garnet相对路径:Server:ClusterManager:TryStartGossipTasks
  pub fn try_start_gossip_tasks(&self) {
    // mock
  }

  /// garnet相对路径:Server:ClusterManager:FlushConfig
  pub fn flush_config(&self) {
    // mock
    self.flush_count.fetch_add(1, Ordering::SeqCst);
  }

  /// garnet相对路径:Server:ClusterManager:TryInitializeLocalWorker
  pub fn try_initialize_local_worker(&self, spec: LocalWorkerSpec<'_>) {
    let mut config = self.current_config.write();
    config.initialize_local_worker(spec);
  }

  /// garnet相对路径:Server:ClusterManager:GetInfo
  pub fn get_info(&self) -> String {
    // 持读锁直接统计，不克隆整份配置；单遍扫描取全部状态计数
    let current = self.current_config.read();
    let counts = current.slot_state_counts();
    let (stable, fail) = (
      counts[SlotState::Stable as usize],
      counts[SlotState::Fail as usize],
    );
    format!(
      "cluster_state:ok\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:{}\r\n\
             cluster_slots_fail:{}\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_received:0\r\n",
      stable,
      stable,
      fail,
      fail,
      current.num_workers(),
      current.get_primary_count(),
      current.get_max_config_epoch(),
      current.local_node_config_epoch(),
    )
  }

  /// garnet相对路径:Server:ClusterManager:GetRange
  ///
  /// 输入须升序；连续槽合并为 `start-end` 区间，其余逐个列出
  pub fn get_range(slots: &[usize]) -> String {
    let mut range = String::from("> ");
    // 哨兵值保证末区间在扫描内闭合，免去循环后重复收尾代码
    let mut prev = None;
    for s in slots.iter().copied().chain([usize::MAX]) {
      match prev {
        Some((start, end)) if s == end + 1 => prev = Some((start, s)),
        Some((start, end)) => {
          let _ = write!(range, "{start}-{end} ");
          prev = Some((s, s));
        }
        None => prev = Some((s, s)),
      }
    }
    range
  }

  /// garnet相对路径:Server:ClusterManager:TrySetLocalConfigEpoch
  ///
  /// 错误集中定义于 [`crate::error`]，不再用裸字节串
  pub fn try_set_local_config_epoch(&self, config_epoch: i64) -> Result<()> {
    {
      let mut current = self.current_config.write();
      if current.num_workers() == 0 {
        return Err(Error::NoWorkers);
      }
      if !current.set_local_worker_config_epoch(config_epoch) {
        return Err(Error::EpochNotSet);
      }
    }
    self.flush_config();
    trace!("SetConfigEpoch {}", config_epoch);
    Ok(())
  }

  /// garnet相对路径:Server:ClusterManager:TryBumpClusterEpoch
  pub fn try_bump_cluster_epoch(&self) -> bool {
    {
      let mut current = self.current_config.write();
      current.bump_local_node_config_epoch();
    }
    self.flush_config();
    true
  }

  /// garnet相对路径:Server:ClusterManager:TrySetLocalNodeRole
  pub fn try_set_local_node_role(&self, role: NodeRole) {
    {
      let mut current = self.current_config.write();
      current
        .set_local_worker_role(role)
        .bump_local_node_config_epoch();
    }
    self.flush_config();
  }

  /// garnet相对路径:Server:ClusterManager:TryResetReplica
  pub fn try_reset_replica(&self) {
    {
      let mut current = self.current_config.write();
      current
        .make_replica_of(None)
        .set_local_worker_role(NodeRole::Primary)
        .bump_local_node_config_epoch();
    }
    self.flush_config();
  }

  /// garnet相对路径:Server:ClusterManager:TryStopWrites
  pub fn try_stop_writes(&self, replica_id: &str) {
    {
      let mut current = self.current_config.write();
      let slots = current.get_slot_list(LOCAL_WORKER_ID as u16);
      let worker_id = current.get_worker_id_from_node_id(replica_id);
      current
        .make_replica_of(Some(replica_id))
        .assign_slots(&slots, worker_id, SlotState::Stable);
    }
    self.flush_config();
  }

  /// garnet相对路径:Server:ClusterManager:TryTakeOverForPrimary
  pub fn try_take_over_for_primary(&self) -> bool {
    {
      let mut current = self.current_config.write();
      if !current.is_replica() || current.local_node_primary_id().is_none() {
        return false;
      }
      current
        .take_over_from_primary()
        .bump_local_node_config_epoch();
    }
    self.flush_config();
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::worker::{LocalWorkerSpec, Worker};

  fn manager_with(primary_id: &str, epoch: i64) -> ClusterManager {
    let m = ClusterManager::new(Arc::new(ClusterProvider {}));
    {
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: primary_id,
        address: "127.0.0.1",
        port: 7000,
        config_epoch: epoch,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
    }
    m
  }

  #[test]
  fn init_local_generates_identity() {
    let m = ClusterManager::new(Arc::new(ClusterProvider {}));
    m.init_local("10.0.0.5", 7000, false);
    let config = m.current_config.read();
    let id = config.local_node_id().expect("生成 node id");
    assert_eq!(id.len(), 32, "uuid simple 形式 32 位 hex");
    assert_eq!(
      (config.local_node_ip(), config.local_node_port()),
      ("10.0.0.5", 7000)
    );
    assert!(config.is_primary());
  }

  #[test]
  fn set_config_epoch_gates_and_persists() {
    let m = manager_with("n1", 0);
    assert!(m.try_set_local_config_epoch(5).is_ok());
    // 非 0 epoch 拒绝覆写
    assert!(matches!(
      m.try_set_local_config_epoch(9),
      Err(Error::EpochNotSet)
    ));
    assert_eq!(
      m.current_config.read().local_node_config_epoch(),
      5,
      "拒绝路径不落盘"
    );
  }

  #[test]
  fn stop_writes_hands_slots_to_replica() {
    let m = manager_with("n1", 3);
    let n2 = {
      let mut config = m.current_config.write();
      config.workers.push(Worker {
        nodeid: Some("n2".to_string()),
        address: "10.0.0.2".to_string(),
        port: 7000,
        config_epoch: 0,
        role: NodeRole::Replica,
        replica_of_node_id: Some("n1".to_string()),
        replication_offset: 0,
        hostname: None,
      });
      (config.workers.len() - 1) as u16
    };
    {
      let mut config = m.current_config.write();
      config.assign_slots(&[1, 2, 3], LOCAL_WORKER_ID as u16, SlotState::Stable);
    }

    m.try_stop_writes("n2");

    let config = m.current_config.read();
    assert!(config.is_replica(), "让位后本地转副本");
    for slot in [1, 2, 3] {
      assert_eq!(config.get_worker_id_from_slot(slot), n2 as usize);
      assert_eq!(config.get_state(slot), SlotState::Stable);
    }
  }

  #[test]
  fn get_info_counts_single_pass() {
    let m = manager_with("n1", 3);
    {
      let mut config = m.current_config.write();
      config.assign_slots(&[0, 1], LOCAL_WORKER_ID as u16, SlotState::Stable);
    }
    let info = m.get_info();
    assert!(info.contains("cluster_slots_assigned:2"));
    assert!(info.contains("cluster_slots_ok:2"));
    assert!(info.contains("cluster_size:1"));
    assert!(info.contains("cluster_my_epoch:3"));
  }

  /// recover 模式恢复既有身份并仅更新端点；fresh 模式生成全新身份
  /// （flush 落盘前的内存回环：持久化面由 config 线格式测试覆盖）
  #[test]
  fn init_local_recover_keeps_identity() {
    let src = manager_with("n1", 4);
    {
      let mut config = src.current_config.write();
      config.assign_slots(&[1, 2], LOCAL_WORKER_ID as u16, SlotState::Stable);
    }

    // recover=true：node id/epoch/槽位回放，地址端口换新
    let m = ClusterManager::new(Arc::new(ClusterProvider {}));
    {
      let mut config = m.current_config.write();
      let old = src.current_config.read();
      // 模拟落盘恢复：把持久化身份灌入后再 init_local 重放端点
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: old.local_node_id().unwrap_or_default(),
        address: "0.0.0.0",
        port: 0,
        config_epoch: old.local_node_config_epoch(),
        role: old.local_node_role(),
        replica_of_node_id: None,
        hostname: None,
      });
      let slots = old.get_slot_list(LOCAL_WORKER_ID as u16);
      config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
    }
    m.init_local("10.9.9.9", 7200, true);
    {
      let config = m.current_config.read();
      assert_eq!(
        config.local_node_id(),
        src.current_config.read().local_node_id(),
        "恢复后节点 id 不变"
      );
      assert_eq!(config.local_node_config_epoch(), 4);
      assert_eq!(config.get_worker_id_from_slot(2), LOCAL_WORKER_ID);
      assert_eq!(
        (config.local_node_ip(), config.local_node_port()),
        ("10.9.9.9", 7200),
        "端点更新身份保留"
      );
    }

    // fresh：生成 32 位 hex 新 id
    let fresh = ClusterManager::new(Arc::new(ClusterProvider {}));
    fresh.init_local("10.0.0.1", 7000, false);
    assert_eq!(
      fresh
        .current_config
        .read()
        .local_node_id()
        .unwrap_or_default()
        .len(),
      32
    );
  }

  /// 接管门控：副本接管成功（槽位转移+epoch 自增），主节点拒绝
  #[test]
  fn take_over_gates_and_moves_slots() {
    // 主节点：无可接管对象
    let primary = manager_with("n1", 3);
    assert!(!primary.try_take_over_for_primary());

    // 副本 n2 of n1：主持槽 0..=1
    let replica = ClusterManager::new(Arc::new(ClusterProvider {}));
    {
      let mut config = replica.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: "n2",
        address: "10.0.0.2",
        port: 7002,
        config_epoch: 0,
        role: NodeRole::Replica,
        replica_of_node_id: Some("n1"),
        hostname: None,
      });
      let n1 = {
        config.workers.push(Worker {
          nodeid: Some("n1".to_string()),
          address: "10.0.0.1".to_string(),
          port: 7001,
          config_epoch: 3,
          role: NodeRole::Primary,
          replica_of_node_id: None,
          replication_offset: 0,
          hostname: None,
        });
        (config.workers.len() - 1) as u16
      };
      config.assign_slots(&[0, 1], n1, SlotState::Stable);
    }

    assert!(replica.try_take_over_for_primary());
    {
      let config = replica.current_config.read();
      assert!(config.is_primary());
      assert_eq!(config.local_node_primary_id(), None);
      for slot in [0, 1] {
        assert_eq!(config.get_worker_id_from_slot(slot), LOCAL_WORKER_ID);
        assert_eq!(config.get_state(slot), SlotState::Stable);
      }
      assert!(
        config.local_node_config_epoch() > 3,
        "接管后 epoch 越过旧主"
      );
    }
  }

  /// 副本复位与角色改写：epoch 取全员最大 +1 单调推进
  #[test]
  fn reset_replica_and_epoch_bump_manager_level() {
    let m = manager_with("n1", 1);
    {
      let mut config = m.current_config.write();
      config.make_replica_of(Some("n9"));
      config.workers.push(Worker {
        nodeid: Some("n9".to_string()),
        address: "10.0.0.9".to_string(),
        port: 7009,
        config_epoch: 10,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }

    // bump 对齐全员最大 epoch
    assert!(m.try_bump_cluster_epoch());
    assert_eq!(m.current_config.read().local_node_config_epoch(), 11);

    // 角色改写同样携带 bump
    m.try_set_local_node_role(NodeRole::Replica);
    {
      let config = m.current_config.read();
      assert!(config.is_replica());
      assert_eq!(config.local_node_config_epoch(), 12);
    }

    // 副本复位：解除复制关系转主
    m.try_reset_replica();
    {
      let config = m.current_config.read();
      assert!(config.is_primary());
      assert_eq!(config.local_node_primary_id(), None);
      assert_eq!(config.local_node_config_epoch(), 13);
    }
  }

  /// 槽区间合并：连续段折叠为 start-end，离散段以单点区间列出
  #[test]
  fn get_range_merges_contiguous_slots() {
    assert_eq!(
      ClusterManager::get_range(&[0, 1, 2, 5, 9, 10]),
      "> 0-2 5-5 9-10 "
    );
    assert_eq!(ClusterManager::get_range(&[7]), "> 7-7 ");
    assert_eq!(ClusterManager::get_range(&[]), "> ");
  }

  /// 纯内存模式：flush 只递增计数，不触碰文件系统
  #[test]
  fn flush_config_counts_persist_requests() {
    let m = manager_with("n1", 1);
    let before = m.flush_count.load(Ordering::SeqCst);
    m.try_bump_cluster_epoch();
    m.try_set_local_node_role(NodeRole::Primary);
    assert!(
      m.flush_count.load(Ordering::SeqCst) >= before + 2,
      "每次配置变更都发起一次持久化请求"
    );
  }
}
