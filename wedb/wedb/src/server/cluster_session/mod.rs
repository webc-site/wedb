//! 集群会话实现（对标 libs/cluster/Session/ClusterSession.cs）
//!
//! 实现 wnode 的 [`ClusterSession`] 切面（IClusterSession 会话侧子集），
//! 向 `RespServerSession` 提供槽位验证、重定向、CLUSTER 命令族与 ROLE
//! 集群分支数据。

mod basic;
mod failover;
mod migrate;
mod replica_of;
mod replication;
mod slot_mgmt;
mod slot_verify;

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicI64, Ordering},
};

use async_lock::Mutex as AsyncLockMutex;
use parking_lot::{Mutex, RwLock};
use wbase::future::blocking_wait;
use wnode::{
  ClusterProvider as WnodeClusterProvider, ClusterSlotVerificationInput, SlotVerifyGate,
  cluster_session::ClusterSessionFace, rangeindex::RangeIndexMigrationReceiveState,
  resp::slow_path::SlowWait,
};
use wresp::{catalog::try_get_resp_command_info, command::RespCommand};

use crate::server::{
  cluster::ClusterPreferredEndpointType,
  cluster_manager::{ClusterManager, SlotWaitMemo},
  cluster_provider::ClusterProvider,
  migration::chunk_reassembler::ChunkReassembler,
  slot_verify::IterativeSlotVerifyCache,
};

/// 集群 RESP 会话实现
pub struct ClusterSession {
  pub(super) cluster_provider: Arc<ClusterProvider>,
  /// gossip 对端节点 id（C# RemoteNodeId：连接一旦确立不随 gossip 变更；
  /// Arc 供 cluster_gossip_slow 慢路径执行体 owned 捕获）
  pub(super) remote_node_id: Arc<RwLock<Option<u128>>>,
  /// 最近一次 gossip 应答的配置演化版本号（C# lastSentConfig 引用比较的
  /// 等价键；与发送侧 node_connection.last_sent_config_version 同形同键，
  /// -1 = 从未应答，唯一递增点 cluster_manager.flush_config；Arc 供
  /// cluster_gossip_slow 慢路径执行体 owned 捕获）
  pub(super) last_sent_config_version: Arc<AtomicI64>,
  pub(super) read_only: AtomicBool,
  /// 批次级纪元快照（C# _localCurrentEpoch：0 = 批外空闲，非 0 = 批内
  /// 持有的 provider 纪元；消费批首尾取放，provider 静止等待的观测面）
  pub(super) local_current_epoch: AtomicI64,
  /// CLUSTER RESET 等需异步闭环命令挂起的慢路径执行体
  ///（会话侧经 [`ClusterSessionFace::take_pending_slow`] 取走驱动）
  pub(super) pending_slow: Mutex<Option<SlowWait>>,
  /// 致命断流登记（C# 集群命令 GarnetException clientResponse:false 上抛
  /// 等价：处理失败时登记文案，会话侧经
  /// [`ClusterSessionFace::take_fatal_disconnect`] 取走转致命哨兵断连）
  pub(super) fatal_disconnect: Mutex<Option<String>>,
  /// 槽位校验等待交接记忆（超时旗标 + 异步存在性裁决缓存）：等待体与
  /// 下一次门评之间的确定性交接，防挂起-重评活锁
  pub(super) slot_wait_memo: Mutex<Option<Arc<SlotWaitMemo>>>,
  /// 迭代式槽位校验缓存态（C# RespClusterIterativeSlotVerify.cs 的
  /// cachedVerificationResult / configSnapshot / initialized 成员投影；
  /// 事务 Prepare 段逐键校验的会话级缓存）
  pub(super) iterative_slot_verify: Mutex<IterativeSlotVerifyCache>,
  /// 分块记录重组器（per-connection 实例字段，随会话实例析构回收；对标
  /// libs/cluster/Session/RespClusterMigrateCommands.cs:21
  /// chunkedRecordReassembler——C# 懒建 `??= new()`，rust 内联默认值等价；
  /// MIGRATE / SYNC 两链共用同一字段，与 C# 两类命令同宿主一致。
  /// Arc 供慢路径执行体 owned 捕获，锁内态与本字段同生命周期）
  pub(super) chunk_reassembler: Arc<Mutex<ChunkReassembler>>,
  /// RangeIndex 分块接收状态机（SerializedRangeIndexStream 续传态的
  /// per-connection 宿主，MIGRATE / SYNC 两链共用；构造需 store.range_index()
  /// 句柄，装配顺序不定故命令解析期经 [`ClusterSession::ensure_range_index_receive_state`]
  /// 懒建，store 未装配保持 None——慢路径碰 RI 帧报存储拒绝）
  pub(super) range_index_receive_state:
    Mutex<Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>>>,
}

impl ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:ClusterSession（构造）
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      remote_node_id: Arc::new(RwLock::new(None)),
      last_sent_config_version: Arc::new(AtomicI64::new(-1)),
      read_only: AtomicBool::new(false),
      local_current_epoch: AtomicI64::new(0),
      pending_slow: Mutex::new(None),
      fatal_disconnect: Mutex::new(None),
      slot_wait_memo: Mutex::new(None),
      iterative_slot_verify: Mutex::new(IterativeSlotVerifyCache::default()),
      chunk_reassembler: Arc::default(),
      range_index_receive_state: Mutex::new(None),
    }
  }

  #[inline]
  pub(super) fn preferred_endpoint_type(&self) -> ClusterPreferredEndpointType {
    self.cluster_provider.preferred_endpoint_type()
  }

  #[inline]
  pub(super) fn cluster_provider(&self) -> &ClusterProvider {
    &self.cluster_provider
  }

  #[inline]
  pub(super) fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_provider.cluster_manager()
  }

  /// RangeIndex 接收态懒建（C# 首帧 `??= new()` 等价：store 装配顺序不定，
  /// 命令解析期从 provider 取 range_index 句柄构造；store 未装配保持
  /// None——慢路径碰 RI 帧报存储拒绝，与原表实现 get_or_create 的 None
  /// 同型）。与分块重组器同属 MIGRATE / SYNC 两链共用的 per-connection
  /// 接收态（对标 C# RespServerSession 的 rangeIndexMigrationState /
  /// chunkedRecordReassembler 实例字段，两类命令同为该会话分部、共用同
  /// 一处）
  pub(super) fn ensure_range_index_receive_state(
    &self,
  ) -> Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>> {
    let mut slot = self.range_index_receive_state.lock();
    if slot.is_none()
      && let Some(store) = self.cluster_provider.try_store()
    {
      *slot = Some(Arc::new(AsyncLockMutex::new(
        RangeIndexMigrationReceiveState::new(store.range_index().clone()),
      )));
    }
    slot.clone()
  }

  /// libs/cluster/Session/ClusterSession.cs:UnsafeBumpAndWaitForEpochTransitionAsync
  ///
  /// 释放本会话批内快照 → provider 推进纪元并等全会话静止 → 重取快照。
  /// C# 命令侧以 `AsyncUtils.BlockingWait` 驱动的同步批内形态（网络线程
  /// 阻塞等待语义），本会话自身快照先行清零，不阻塞静止等待收敛
  pub fn unsafe_bump_and_wait_for_epoch_transition(&self) -> bool {
    self.release_current_epoch();
    let caught_up = self.cluster_provider.bump_and_wait_for_epoch_transition();
    self.acquire_current_epoch();
    caught_up
  }
}

impl ClusterSessionFace for ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:SetReadOnlySession
  fn set_read_only_session(&self) {
    self.read_only.store(true, Ordering::Relaxed);
  }

  /// libs/cluster/Session/ClusterSession.cs:SetReadWriteSession
  fn set_read_write_session(&self) {
    self.read_only.store(false, Ordering::Relaxed);
  }

  /// gossip 对端节点 id 读取口（会话固有字段 [`ClusterSession::remote_node_id`]
  /// 的 trait 转发；权威映射注释见字段定义处）
  fn remote_node_id(&self) -> Option<u128> {
    *self.remote_node_id.read()
  }

  /// 会话固有方法的重置入口转发（权威映射注释见会话固有定义处）
  fn reset_cached_slot_verification_result(&self) {
    self.reset_cached_slot_verification_result();
  }

  /// 会话固有迭代校验入口的 trait 转发（权威映射注释见会话固有定义处）
  fn network_iterative_slot_verify(
    &self,
    key: &[u8],
    read_only: bool,
    session_asking: bool,
    slot: u16,
  ) -> bool {
    self.network_iterative_slot_verify(key, read_only, session_asking, slot)
  }

  /// 会话固有缓存错误写出入口的 trait 转发（权威映射注释见会话固有定义处）
  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    self.write_cached_slot_verification_message(output);
  }

  /// 会话固有多键槽位校验入口的 trait 转发（权威映射注释见会话固有定义处）
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    self.network_multi_key_slot_verify(input, args, output)
  }

  /// libs/cluster/Session/ClusterSession.cs:ProcessClusterCommands
  fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    slot: u16,
  ) -> bool {
    match cmd {
      RespCommand::ClusterNodes => self.network_cluster_nodes(cmd, args, output),
      RespCommand::ClusterKeyslot => self.network_cluster_keyslot(cmd, args, output, slot),
      RespCommand::ClusterMyid => self.network_cluster_myid(cmd, args, output),
      RespCommand::ClusterSlots => self.network_cluster_slots(cmd, args, output),
      RespCommand::ClusterShards => self.network_cluster_shards(cmd, args, output),
      RespCommand::ClusterInfo => self.network_cluster_info(cmd, args, output),
      RespCommand::ClusterBumpepoch => self.network_cluster_bumpepoch(cmd, args, output),
      RespCommand::ClusterReset => self.network_cluster_reset(cmd, args, output),
      RespCommand::ClusterAddslots | RespCommand::ClusterAddslotsrange => {
        self.network_cluster_add_slots(cmd, args, output)
      }
      RespCommand::ClusterDelslots | RespCommand::ClusterDelslotsrange => {
        self.network_cluster_del_slots(cmd, args, output)
      }
      RespCommand::ClusterSetslot => self.network_cluster_set_slot(cmd, args, output),
      RespCommand::ClusterSetslotsrange => self.network_cluster_set_slots_range(cmd, args, output),
      RespCommand::ClusterCountkeysinslot => {
        self.network_cluster_count_keys_in_slot(cmd, args, output)
      }
      RespCommand::ClusterGetkeysinslot => self.network_cluster_get_keys_in_slot(cmd, args, output),
      RespCommand::ClusterDelkeysinslot | RespCommand::ClusterDelkeysinslotrange => {
        self.network_cluster_del_keys_in_slot(cmd, args, output)
      }
      RespCommand::ClusterSlotstate => self.network_cluster_slot_state(cmd, args, output),
      RespCommand::ClusterMeet => self.network_cluster_meet(cmd, args, output),
      RespCommand::ClusterForget => self.network_cluster_forget(cmd, args, output),
      RespCommand::ClusterReplicas => self.network_cluster_replicas(cmd, args, output),
      RespCommand::ClusterReplicate => self.network_cluster_replicate(cmd, args, output),
      RespCommand::ClusterSetconfigepoch => self.network_cluster_setconfigepoch(cmd, args, output),
      RespCommand::ClusterEndpoint => self.network_cluster_endpoint(cmd, args, output),
      RespCommand::ClusterHelp => self.network_cluster_help(cmd, args, output),
      RespCommand::ClusterBanlist => self.network_cluster_banlist(cmd, args, output),
      RespCommand::ClusterMyparentid => self.network_cluster_myparentid(cmd, args, output),
      RespCommand::ClusterMigrate => self.network_cluster_migrate(cmd, args, output),
      RespCommand::Migrate => self.network_try_migrate(args, output, slot),
      RespCommand::ClusterMtasks => self.network_cluster_mtasks(cmd, args, output),
      RespCommand::ClusterFailover => self.network_cluster_failover(args, output),
      RespCommand::ClusterFailstopwrites => {
        self.network_cluster_fail_stop_writes(cmd, args, output)
      }
      RespCommand::ClusterFailreplicationoffset => {
        self.network_cluster_fail_replication_offset(cmd, args, output)
      }
      RespCommand::ClusterFlushall => self.network_cluster_flushall(cmd, args, output),
      RespCommand::ClusterFlushallNs => self.network_cluster_flushall_ns(args, output),
      RespCommand::ClusterGossip => self.network_cluster_gossip(args, output),
      RespCommand::ClusterPublish | RespCommand::ClusterSpublish => {
        self.network_cluster_publish(cmd, args, output)
      }
      RespCommand::Replicaof | RespCommand::Secondaryof => {
        self.network_replicaof(cmd, args, output)
      }
      RespCommand::Failover => self.network_failover(args, output),
      RespCommand::ClusterReserve => self.network_cluster_reserve(args, output),
      RespCommand::ClusterAdvanceTime => self.network_cluster_advance_time(args, output),
      RespCommand::ClusterMlogKeyTime => self.network_cluster_mlog_key_time(args, output),
      RespCommand::ClusterAppendlog => self.network_cluster_appendlog(args, output),
      RespCommand::ClusterInitiateReplicaSync => {
        self.network_cluster_initiate_replica_sync(args, output)
      }
      RespCommand::ClusterSnapshotData => self.network_cluster_snapshot_data(args, output),
      RespCommand::ClusterSendCkptMetadata => {
        self.network_cluster_send_checkpoint_metadata(args, output)
      }
      RespCommand::ClusterSendCkptFileSegment => {
        self.network_cluster_send_checkpoint_file_segment(args, output)
      }
      RespCommand::ClusterBeginReplicaRecover => {
        self.network_cluster_begin_replica_recover(args, output)
      }
      RespCommand::ClusterAttachSync => self.network_cluster_attach_sync(args, output),
      RespCommand::ClusterSync => self.network_cluster_sync(args, output),
      _ => false,
    }
  }

  /// 批次级纪元快照读取（0 = 批外空闲）
  ///
  /// libs/cluster/Session/ClusterSession.cs:LocalCurrentEpoch
  fn local_current_epoch(&self) -> i64 {
    self.local_current_epoch.load(Ordering::Acquire)
  }

  /// 消费批首快照 provider 当前纪元
  ///
  /// libs/cluster/Session/ClusterSession.cs:AcquireCurrentEpoch
  fn acquire_current_epoch(&self) {
    self
      .local_current_epoch
      .store(self.cluster_provider.current_epoch(), Ordering::Release);
  }

  /// 消费批尾清零快照
  ///
  /// libs/cluster/Session/ClusterSession.cs:ReleaseCurrentEpoch
  fn release_current_epoch(&self) {
    self.local_current_epoch.store(0, Ordering::Release);
  }

  /// libs/cluster/Session/ClusterSession.cs:Dispose
  ///
  /// 会话不持网络发送器与存储上下文（并行域各自管理生命周期），无清理动作
  fn dispose(&self) {}

  /// 取走 CLUSTER RESET 等挂起的慢路径执行体（会话主循环转挂网络泵驱动）
  fn take_pending_slow(&self) -> Option<SlowWait> {
    self.pending_slow.lock().take()
  }

  /// 取走致命断流登记（APPENDLOG 拒收等 clientResponse:false 场景），
  /// 会话侧转致命哨兵：不写错误应答行，发尽累积应答后断连
  fn take_fatal_disconnect(&self) -> Option<String> {
    self.fatal_disconnect.lock().take()
  }

  /// 集群转发切面（rust 注入架构胶水面，无 C# 对应函数：C# NetworkPUBLISH
  /// 直连 clusterProvider.ClusterPublishAsync，rust 会话→集群域依赖倒置经
  /// 本切面中转，provider 层转发由 ClusterProvider::cluster_publish_async 承接）
  ///
  /// cluster_manager 在场（等价 EnableCluster）时以同步收割单点
  /// [`blocking_wait`] 内联驱动转发至闭环——C# 网络线程同步阻塞的 compio
  /// 单线程执行域等价物；无 manager 返回 false（等价 EnableCluster == false，
  /// PUBLISH 仅本地广播）
  fn cluster_publish(&self, cmd: RespCommand, channel: &[u8], message: &[u8]) -> bool {
    let Some(mgr) = self.cluster_manager() else {
      return false;
    };
    blocking_wait(mgr.try_cluster_publish_async(cmd, channel, message));
    true
  }

  /// 集群拓扑配置刷盘（CONFIG REWRITE 触发，转发 ClusterProvider 实现；
  /// 对标 C# ServerConfig.cs:105 直连 `clusterProvider.FlushConfig()`）
  fn flush_config(&self) {
    WnodeClusterProvider::flush_config(&*self.cluster_provider);
  }
}

/// 执行只读存储会话慢路径：封装 new_session -> enter_batch -> StorageSession::new_readonly -> 执行
macro_rules! run_readonly_storage_slow {
  ($store:expr, $storage:ident, $out:ident, $body:block) => {{
    let mut $out = Vec::new();
    let Ok(session) = $store.new_session() else {
      $out.write_resp_error(wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE);
      return $out;
    };
    let batch = session.enter_batch();
    let $storage = wnode::StorageSession::new_readonly(batch);
    $body;
    $out
  }};
}
pub(super) use run_readonly_storage_slow;

/// 集群管理器或存储域未装配的统一拒绝文案
pub(crate) const ERR_CLUSTER_NOT_INITIALIZED: &str = "ERR Cluster not initialized";

/// 默认集群命令族回退名称
pub const DEFAULT_CLUSTER_CMD_NAME: &str = "cluster";

/// 子命令 RESP 名（错误文案回显用，对标 C# `ClusterSession.cs:119-121` 与 `RespCommandsInfo.GetRespCommandName(command).ToLowerInvariant()`）
#[inline]
pub fn cluster_sub_name(cmd: RespCommand) -> &'static str {
  try_get_resp_command_info(cmd).map_or(DEFAULT_CLUSTER_CMD_NAME, |e| e.name)
}
