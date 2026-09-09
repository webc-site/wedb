use std::sync::{
  Arc,
  atomic::{AtomicI32, Ordering},
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
  pub fn get_range(slots: &[usize]) -> String {
    if slots.is_empty() {
      return "> ".to_string();
    }
    let mut range = String::from("> ");
    let mut start = slots[0];
    let mut end = slots[0];
    for i in 1..=slots.len() {
      if i < slots.len() && slots[i] == end + 1 {
        end = slots[i];
      } else {
        range.push_str(&format!("{}-{} ", start, end));
        if i < slots.len() {
          start = slots[i];
          end = slots[i];
        }
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
