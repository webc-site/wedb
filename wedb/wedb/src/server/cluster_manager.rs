use std::{
  fmt::Write as _,
  sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
  },
};

use log::trace;
use parking_lot::RwLock;
use wbase::hash_slot::hash_slot as cluster_slot;
use wnode::MetricsItem;
use wresp::RespCommand;

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, ClusterPreferredEndpointType, LOCAL_WORKER_ID},
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    hash_slot::SlotState,
    slot_verify::{ClusterSlotVerificationState, multi_key_slot_verify, single_key_slot_verify},
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// 生成标准 40 字符十六进制节点 ID（对标 Garnet Generator.CreateHexId(40)）
pub fn create_hex_id() -> String {
  const HEX: &[u8; 16] = b"0123456789abcdef";
  let mut bytes = [0u8; 40];
  for b in &mut bytes {
    *b = HEX[fastrand::usize(..16)];
  }
  // SAFETY: 仅填充了合法 ASCII 十六进制小写字符
  unsafe { String::from_utf8_unchecked(bytes.to_vec()) }
}

/// 集群核心管理器（libs/cluster/Server/ClusterManager.cs）
pub struct ClusterManager {
  pub current_config: RwLock<ClusterConfig>,
  pub cluster_provider: Arc<ClusterProvider>,
  pub(crate) flush_count: AtomicI32,
  pub(crate) worker_ban_list: RwLock<gxhash::HashMap<String, i64>>,
  pub(crate) active_merge_lock: RwLock<()>,
}

impl ClusterManager {
  /// libs/cluster/Server/ClusterManager.cs:ClusterManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    let current_config = RwLock::new(ClusterConfig::new());
    Self {
      current_config,
      cluster_provider,
      flush_count: AtomicI32::new(0),
      worker_ban_list: RwLock::new(gxhash::HashMap::default()),
      active_merge_lock: RwLock::new(()),
    }
  }

  /// NOTE: Unsafe! DO NOT USE, other than benchmarking
  /// libs/cluster/Server/ClusterManager.cs:UnsafeSetConfig
  #[cfg(any(test, feature = "bench"))]
  pub fn unsafe_set_config(&self, cluster_config: ClusterConfig) {
    *self.current_config.write() = cluster_config;
  }

  /// 获取当前集群配置读句柄
  #[inline]
  pub fn current_config(&self) -> parking_lot::RwLockReadGuard<'_, ClusterConfig> {
    self.current_config.read()
  }

  /// 惰性更新本地复制偏移量（委托至当前集群配置）
  #[inline]
  pub fn lazy_update_local_replication_offset(&self, offset: i64) {
    self
      .current_config
      .write()
      .lazy_update_local_replication_offset(offset);
  }

  /// libs/cluster/Server/ClusterManager.cs:InitLocal
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
      let node_id = create_hex_id(); // equivalent to Generator.CreateHexId(40)
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

  /// libs/cluster/Server/ClusterManager.cs:Dispose
  pub fn dispose(&self) {
    self.dispose_background_tasks();
  }

  /// libs/cluster/Server/ClusterManager.cs:DisposeBackgroundTasks
  pub fn dispose_background_tasks(&self) {
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.dispose();
    }
  }

  /// libs/cluster/Server/ClusterManager.cs:Start
  pub fn start(&self) {
    self.try_start_gossip_tasks();
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryStartGossipTasks
  pub fn try_start_gossip_tasks(&self) {
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.start();
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetConnectionInfo
  pub fn get_connection_info(&self, node_id: &str) -> ConnectionInfo {
    self
      .cluster_provider
      .gossip_manager()
      .and_then(|gm| gm.connection_store.get_connection_info(node_id))
      .unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetPrimaryLinkStatus
  pub fn get_primary_link_status(&self, config: &ClusterConfig) -> [MetricsItem; 2] {
    let info = config
      .local_node_primary_id()
      .map(|primary_id| self.get_connection_info(primary_id))
      .unwrap_or_default();
    [
      MetricsItem::new(
        "master_link_status",
        if info.connected { "up" } else { "down" },
      ),
      MetricsItem::new("master_last_io_seconds_ago", info.last_io.to_string()),
    ]
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryClusterPublishAsync
  pub async fn try_cluster_publish_async(&self, cmd: RespCommand, channel: &[u8], message: &[u8]) {
    let Some(gm) = self.cluster_provider.gossip_manager() else {
      return;
    };
    let node_entries = {
      let conf = self.current_config();
      if cmd == RespCommand::Publish {
        conf.get_all_node_ids()
      } else {
        conf.get_node_ids_for_shard()
      }
    };

    let is_spublish = cmd != RespCommand::Publish;
    for (node_id, endpoint) in node_entries {
      let conn = match gm.connection_store.get_connection(&node_id) {
        Some(conn) => conn,
        None => {
          let ip_str = endpoint.ip().to_string();
          gm.connection_store
            .get_or_add(&node_id, &ip_str, endpoint.port() as i32)
        }
      };
      conn
        .try_cluster_publish_async(is_spublish, channel, message)
        .await;
    }
  }

  /// libs/cluster/Server/ClusterManager.cs:FlushConfig
  pub fn flush_config(&self) {
    self.flush_count.fetch_add(1, Ordering::SeqCst);
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryInitializeLocalWorker
  pub fn try_initialize_local_worker(&self, spec: LocalWorkerSpec<'_>) {
    let mut config = self.current_config.write();
    config.initialize_local_worker(spec);
  }

  /// libs/cluster/Server/ClusterManager.cs:GetInfo
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

  /// libs/cluster/Server/ClusterManager.cs:GetRange
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

  /// libs/cluster/Server/ClusterManager.cs:TrySetLocalConfigEpoch
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

  /// libs/cluster/Server/ClusterManager.cs:TryBumpClusterEpoch
  pub fn try_bump_cluster_epoch(&self) -> bool {
    {
      let mut current = self.current_config.write();
      current.bump_local_node_config_epoch();
    }
    self.flush_config();
    true
  }

  /// libs/cluster/Server/ClusterManager.cs:TrySetLocalNodeRole
  pub fn try_set_local_node_role(&self, role: NodeRole) {
    {
      let mut current = self.current_config.write();
      current
        .set_local_worker_role(role)
        .bump_local_node_config_epoch();
    }
    self.flush_config();
  }

  /// libs/cluster/Server/ClusterManager.cs:TryResetReplica
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

  /// libs/cluster/Server/ClusterManager.cs:TryStopWrites
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

  /// libs/cluster/Server/ClusterManager.cs:TryTakeOverForPrimary
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

  /// libs/cluster/Server/Gossip/Gossip.cs:SuspendConfigMerge
  ///
  /// 挂起配置合并（写锁，阻塞并发 merge）
  pub fn suspend_config_merge(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
    self.active_merge_lock.write()
  }

  /// 检查节点是否处于封禁期（按秒级时间戳判定）
  pub fn is_banned(&self, node_id: &str) -> bool {
    let now = coarsetime::Clock::now_since_epoch().as_secs() as i64;
    let ban_list = self.worker_ban_list.read();
    if let Some(&expiry) = ban_list.get(node_id) {
      expiry > now
    } else {
      false
    }
  }

  /// 封禁节点指定秒数
  pub fn ban_node(&self, node_id: &str, expiry_seconds: u64) {
    let expiry = (coarsetime::Clock::now_since_epoch().as_secs() + expiry_seconds) as i64;
    self
      .worker_ban_list
      .write()
      .insert(node_id.to_string(), expiry);
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GetBanList
  ///
  /// 获取当前封禁列表
  pub fn get_ban_list(&self) -> Vec<String> {
    let now = coarsetime::Clock::now_since_epoch().as_secs() as i64;
    let ban_list = self.worker_ban_list.read();
    ban_list
      .iter()
      .filter_map(|(id, &expiry)| {
        let diff = expiry - now;
        if diff > 0 {
          Some(format!("{id} : {diff}"))
        } else {
          None
        }
      })
      .collect()
  }

  /// 清理已过期的封禁条目
  pub fn cleanup_ban_list(&self) {
    let now = coarsetime::Clock::now_since_epoch().as_secs() as i64;
    // 仅需过期时间戳判定，键为被封禁节点 ID 忽略
    self
      .worker_ban_list
      .write()
      .retain(|_node_id, &mut expiry| expiry > now);
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMerge
  ///
  /// 合并远端 Gossip 配置，带纪元冲突仲裁与变更落盘
  pub fn try_merge(&self, sender_config: &ClusterConfig, acquire_lock: bool) -> bool {
    let _guard = if acquire_lock {
      Some(self.active_merge_lock.read())
    } else {
      None
    };

    if let Some(sender_id) = sender_config.local_node_id()
      && self.is_banned(sender_id)
    {
      trace!(
        "Cannot merge node <{}> because still in ban list",
        sender_id
      );
      return false;
    }

    let mut current = self.current_config.write();
    let ban_list = self.worker_ban_list.read();
    let merged_config = current.merge(sender_config, &ban_list);
    drop(ban_list);

    if let Some(mut next) = merged_config {
      next.handle_config_epoch_collision(sender_config);
      *current = next;
      drop(current);
      self.flush_config();
      true
    } else if current.handle_config_epoch_collision(sender_config) {
      drop(current);
      self.flush_config();
      true
    } else {
      false
    }
  }

  /// 验证单键槽位归属与可用性
  pub fn verify_key(
    &self,
    key: &[u8],
    read_only: bool,
    session_asking: bool,
    pref_type: ClusterPreferredEndpointType,
  ) -> ClusterSlotVerificationState {
    let slot = cluster_slot(key);
    self.verify_slot(slot, key, read_only, session_asking, pref_type)
  }

  /// 验证指定槽位与键的可用性
  pub fn verify_slot(
    &self,
    slot: u16,
    key: &[u8],
    read_only: bool,
    session_asking: bool,
    pref_type: ClusterPreferredEndpointType,
  ) -> ClusterSlotVerificationState {
    let config = self.current_config();
    let is_recovering = self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.is_recovering())
      .unwrap_or(false);
    let can_access_key = self
      .cluster_provider
      .migration_manager()
      .map(|mm| mm.can_access_key(key, slot as i32, read_only))
      .unwrap_or(true);
    single_key_slot_verify(
      &config,
      slot,
      read_only,
      session_asking,
      is_recovering,
      can_access_key,
      pref_type,
    )
  }

  /// 验证多键跨槽位与迁移态可用性
  pub fn verify_multi_key(
    &self,
    keys: &[&[u8]],
    read_only: bool,
    session_asking: bool,
    pref_type: ClusterPreferredEndpointType,
  ) -> ClusterSlotVerificationState {
    let config = self.current_config();
    let is_recovering = self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.is_recovering())
      .unwrap_or(false);
    let slots: Vec<u16> = keys.iter().map(|k| cluster_slot(k)).collect();
    let mm = self.cluster_provider.migration_manager();
    multi_key_slot_verify(
      &config,
      &slots,
      read_only,
      session_asking,
      is_recovering,
      pref_type,
      |idx| {
        mm.as_ref()
          .map(|m| m.can_access_key(keys[idx], slots[idx] as i32, read_only))
          .unwrap_or(true)
      },
    )
  }
}
