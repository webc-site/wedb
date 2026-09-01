use std::any::Any;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

use async_lock::Semaphore;
use compio::runtime::{Runtime, spawn};
use crossfire::oneshot::{TxOneshot, oneshot};
use futures_util::FutureExt;
use papaya::HashMap;
use webc_cmd::{DEFAULT_NODE_WEIGHT, MetaCache, NodeLocation, ShardTopology};
use zenoh::key_expr::KeyExpr;
use zenoh_raft::async_runtime::watch::{WatchReceiver, WatchSender, channel as watch_channel};
use zenoh_raft::async_runtime::{MpscReceiver, MpscSender, mpsc_channel};
use zenoh_raft::error::{ClientWriteError, RaftError};

use crate::conf::{ClusterMode, Conf, Endpoint, RaftConf};
use crate::error::{Error, Result};
use crate::util::{now_millis, sleep, yield_now};
use wedb_raft::FjallEngine;
use wedb_raft::network::{NetworkFactory, raft_forward_key};
use wedb_raft::store;
use wedb_raft::store::FjallStateMachine;
use wedb_raft::types::{
  AppliedState, BatchWriteReq, Cmd, ForwardResponse, GetKVReply, GetKVReq, GetMemberReply,
  GetMemberReq, JoinRequest, LeaveRequest, LogEntry, LogId, Node, NodeId, Raft, ReadPolicy,
  RequestPayload, ScanPrefixReply, ScanPrefixReq, TxnReply, TxnReq, UpsertKV,
};

/// 批量写入流水线任务
pub(crate) struct BatchWriteTask {
  pub entries: Vec<UpsertKV>,
  pub respond_to: TxOneshot<Result<()>>,
}

/// 最大并发提议批次数（流水线并发窗口）
const MAX_IN_FLIGHT_BATCHES: usize = 32;
/// 单个批次最大聚合任务数
const MAX_BATCH_TASKS: usize = 512;
/// 单个批次最大聚合 KV 操作数
const MAX_BATCH_ENTRIES: usize = 4096;
/// 触发协同让步的最小聚合任务阈值
const MIN_YIELD_BATCH_SIZE: usize = 32;

async fn run_batch_writer(
  raft: Arc<Raft>,
  mut rx: MpscReceiver<BatchWriteTask>,
  mut shutdown_rx: WatchReceiver<bool>,
) {
  let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT_BATCHES));
  let mut tasks = Vec::with_capacity(MAX_BATCH_TASKS);
  let mut combined_entries = Vec::with_capacity(MAX_BATCH_ENTRIES);

  loop {
    tasks.clear();
    combined_entries.clear();

    // 1. 等待首个写入任务或关闭信号
    let first_task = futures_util::select! {
        _ = shutdown_rx.wait_until(|s| *s).fuse() => break,
        task = rx.recv().fuse() => match task {
            Some(t) => t,
            None => break,
        }
    };

    let mut total_entries = first_task.entries.len();
    tasks.push(first_task);

    // 2. 第一轮非阻塞 try_recv 快速合并通道中已积压的并发请求
    while tasks.len() < MAX_BATCH_TASKS && total_entries < MAX_BATCH_ENTRIES {
      match rx.try_recv() {
        Ok(task) => {
          total_entries += task.entries.len();
          tasks.push(task);
        }
        Err(_) => break,
      }
    }

    // 3. 自适应微聚合：若合并任务数较少且通道活跃/未饱和，执行极轻量协作调度让步
    if tasks.len() < MIN_YIELD_BATCH_SIZE && total_entries < MAX_BATCH_ENTRIES {
      yield_now().await;
      while tasks.len() < MAX_BATCH_TASKS && total_entries < MAX_BATCH_ENTRIES {
        match rx.try_recv() {
          Ok(task) => {
            total_entries += task.entries.len();
            tasks.push(task);
          }
          Err(_) => break,
        }
      }
    }

    // 4. 预分配容量，零冗余合并所有任务条目
    combined_entries.reserve(total_entries);
    for task in &mut tasks {
      combined_entries.append(&mut task.entries);
    }

    // 5. 打包为单一 Raft 批量提议条目（附加系统当前毫秒级时间戳）
    let entry = LogEntry::new_with_time(
      Cmd::BatchUpsertKV {
        entries: mem::take(&mut combined_entries),
      },
      Some(now_millis()),
    );

    // 6. 并发流水线提交 Raft 多数派复制与日志持久化
    let raft_clone = raft.clone();
    let current_tasks = mem::take(&mut tasks);

    let permit = futures_util::select! {
        _ = shutdown_rx.wait_until(|s| *s).fuse() => {
            for task in current_tasks {
                task.respond_to.send(Err(Error::internal("Batch writer shutting down")));
            }
            break;
        }
        p = semaphore.acquire_arc().fuse() => p,
    };

    spawn(async move {
      let _permit = permit;
      let err = match raft_clone.client_write(entry).await {
        Ok(_) => None,
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
          Some((true, format!("Forward to leader: {fwd:?}")))
        }
        Err(e) => Some((false, format!("client write: {e}"))),
      };

      for task in current_tasks {
        let res = match &err {
          None => Ok(()),
          Some((true, msg)) => Err(Error::retryable(io::Error::other(msg.clone()))),
          Some((false, msg)) => Err(Error::internal(msg.clone())),
        };
        task.respond_to.send(res);
      }
    })
    .detach();
  }
}

pub struct RaftNode {
  pub(crate) engine: Arc<FjallEngine>,
  pub(crate) raft: Arc<Raft>,
  pub(crate) conf: Conf,
  pub(crate) state_machine: Arc<FjallStateMachine>,
  pub(crate) session: Arc<zenoh::Session>,
  pub(crate) shutdown_tx: WatchSender<bool>,
  pub(crate) service_handles: Mutex<Vec<JoinHandle<()>>>,
  pub(crate) _queryable: Mutex<Option<Box<dyn Any + Send + Sync>>>,
  pub(crate) meta_cache: Arc<MetaCache>,
  pub(crate) sharding: Arc<RwLock<ShardTopology>>,
  pub(crate) batch_write_tx: MpscSender<BatchWriteTask>,
  pub(crate) forward_key_cache: HashMap<NodeId, KeyExpr<'static>>,
}

/// 初始化 Zenoh 会话（配置最优化的 Unsecure QUIC 传输协议与端点）
async fn open_zenoh_session(conf: &RaftConf) -> Result<Arc<zenoh::Session>> {
  let mut zenoh_conf = zenoh::Config::default();
  // 禁用组播与 gossip 探活，减少额外后台开销与端口占用
  let _ = zenoh_conf.insert_json5("scouting/multicast/enabled", "false");
  let _ = zenoh_conf.insert_json5("scouting/gossip/enabled", "false");

  // 显式保证单播 QoS 开启，禁用 lowlatency（确保优先级多流调度生效）
  let _ = zenoh_conf.insert_json5("transport/unicast/qos/enabled", "true");
  let _ = zenoh_conf.insert_json5("transport/unicast/lowlatency", "false");
  let _ = zenoh_conf.insert_json5("transport/unicast/max_links", "1");

  let listen_ep = conf.endpoint.to_zenoh_endpoint();
  let _ = zenoh_conf.insert_json5("listen/endpoints", &format!("[\"{listen_ep}\"]"));

  let mut connect_eps = Vec::new();
  for peer in &conf.join {
    if let Err(e) = peer.parse::<SocketAddr>() {
      return Err(Error::conf(format!(
        "Invalid join address '{peer}': must be standard IP:PORT format ({e})"
      )));
    }
    let connect_ep = Endpoint::zenoh_endpoint(peer);
    connect_eps.push(format!("\"{connect_ep}\""));
  }

  if !connect_eps.is_empty() {
    let joined_eps = connect_eps.join(",");
    let ep_array = format!("[{joined_eps}]");
    let _ = zenoh_conf.insert_json5("connect/endpoints", &ep_array);
  }

  let session = zenoh::open(zenoh_conf)
    .await
    .map_err(|e| Error::internal(format!("Failed to open zenoh session: {e}")))?;

  Ok(Arc::new(session))
}

impl RaftNode {
  pub fn raft(&self) -> &Arc<Raft> {
    &self.raft
  }

  pub fn state_machine(&self) -> &Arc<FjallStateMachine> {
    &self.state_machine
  }

  pub fn session(&self) -> &Arc<zenoh::Session> {
    &self.session
  }

  pub fn engine(&self) -> &Arc<FjallEngine> {
    &self.engine
  }

  pub fn conf(&self) -> &Conf {
    &self.conf
  }

  #[inline]
  pub fn meta_cache(&self) -> &Arc<MetaCache> {
    &self.meta_cache
  }

  #[inline]
  pub fn sharding(&self) -> &Arc<RwLock<ShardTopology>> {
    &self.sharding
  }

  /// 检查当前节点是否为 Raft Leader
  #[inline]
  pub fn is_leader(&self) -> bool {
    match self.current_leader_id() {
      Some(id) => id == self.conf.node_id,
      None => false,
    }
  }

  /// 解析指定集群节点的 Redis 服务网络地址（支持显式配置端口或根据 Raft 端口偏移推算）
  #[inline]
  pub fn resolve_node_redis_addr(&self, node: &Node) -> String {
    if node.node_id == self.conf.node_id {
      return self.conf.redis.addr.clone();
    }

    let explicit_port = self.conf.topology.as_ref().and_then(|t| t.redis_port);
    let redis_port = explicit_port.unwrap_or_else(|| {
      let my_redis_port = self
        .conf
        .redis
        .addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<i32>().ok())
        .unwrap_or(6379);
      let my_raft_port = self.conf.raft.endpoint.port as i32;
      let offset = my_redis_port - my_raft_port;
      (node.endpoint.port as i32 + offset).max(1) as u16
    });

    let host = &node.endpoint.host;
    format!("{host}:{redis_port}")
  }

  /// 获取指定节点的 Redis 服务网络地址（用于 Redis Cluster -MOVED 重定向）
  pub async fn target_redis_addr(&self, target_node_id: u64) -> Option<String> {
    if target_node_id == self.conf.node_id {
      return Some(self.conf.redis.addr.clone());
    }

    if let Ok(topo) = self.sharding.read()
      && let Some(addr) = topo.nodes.get(&target_node_id)
    {
      return Some(addr.clone());
    }

    if let Ok(nodes) = self.state_machine.get_nodes()
      && let Some(node) = nodes.get(&target_node_id)
    {
      return Some(self.resolve_node_redis_addr(node));
    }

    None
  }

  /// 获取当前 Raft Leader 的 Redis 服务网络地址（用于 Redis Cluster -MOVED 重定向）
  pub async fn leader_redis_addr(&self) -> Option<String> {
    let leader_id = match self.current_leader_id() {
      Some(id) => id,
      None => self.get_leader().await.ok().flatten()?,
    };
    self.target_redis_addr(leader_id).await
  }

  pub async fn shutdown(&self) -> Result<()> {
    let _ = self.shutdown_tx.send(true);

    *self._queryable.lock().unwrap() = None;

    let _ = self.raft.shutdown().await;

    let handles = mem::take(&mut *self.service_handles.lock().unwrap());
    for h in handles {
      let _ = h.join();
    }

    Ok(())
  }

  pub(crate) async fn create(conf: &Conf) -> Result<Arc<Self>> {
    let engine = Arc::new(store::create_storage_engine(&conf.fjall.data_path)?);

    let data_dir = PathBuf::from(&conf.fjall.data_path);
    let (log_store, state_machine) = store::create_stores(&engine, data_dir).await?;

    let session = open_zenoh_session(&conf.raft).await?;

    let factory = NetworkFactory::new(session.clone());
    let raft_config = conf.raft.to_raft_config();

    let raft = Arc::new(
      zenoh_raft::Raft::new(
        conf.node_id,
        Arc::new(raft_config),
        factory.clone(),
        log_store,
        state_machine.clone(),
      )
      .await
      .map_err(|e| Error::internal(format!("Failed to create raft: {e}")))?,
    );

    let (shutdown_tx, shutdown_rx) = watch_channel(false);
    let (batch_write_tx, batch_write_rx) = mpsc_channel::<BatchWriteTask>(16384);
    let batch_raft = raft.clone();
    let batch_shutdown_rx = shutdown_rx.clone();
    let node_id = conf.node_id;

    let mut handles = Vec::with_capacity(2);
    let batch_handle = Builder::new()
      .name(format!("batch-writer-{node_id}"))
      .spawn(move || {
        if let Ok(rt) = Runtime::new() {
          rt.block_on(run_batch_writer(
            batch_raft,
            batch_write_rx,
            batch_shutdown_rx,
          ));
        }
      })
      .map_err(|e| Error::internal(format!("Failed to spawn batch writer thread: {e}")))?;
    handles.push(batch_handle);

    // 启动后台定时 TTL 物理清理协程（每 10 秒主动巡检清理过期键）
    let sweeper_sm = state_machine.clone();
    let mut sweeper_shutdown_rx = shutdown_rx.clone();
    let sweeper_handle = Builder::new()
      .name(format!("ttl-sweeper-{node_id}"))
      .spawn(move || {
        if let Ok(rt) = Runtime::new() {
          rt.block_on(async move {
            loop {
              futures_util::select! {
                  _ = sweeper_shutdown_rx.wait_until(|s| *s).fuse() => break,
                  _ = sleep(Duration::from_secs(10)).fuse() => {
                      if let Err(e) = sweeper_sm.sweep_expired_keys(1024) {
                          log::warn!("Background TTL sweeper error: {e}");
                      }
                  }
              }
            }
          });
        }
      })
      .map_err(|e| Error::internal(format!("Failed to spawn sweeper thread: {e}")))?;
    handles.push(sweeper_handle);

    let meta_cache = Arc::new(MetaCache::default());
    let mut initial_topo = ShardTopology::default();
    initial_topo.nodes.clear();
    initial_topo.weights.clear();
    initial_topo.racks.clear();
    initial_topo.locations.clear();
    let addr = conf.redis.addr.clone();
    let weight = conf
      .topology
      .as_ref()
      .and_then(|t| t.weight)
      .unwrap_or(DEFAULT_NODE_WEIGHT);
    let loc = conf
      .topology
      .as_ref()
      .map(|t| {
        NodeLocation::new(
          t.region.clone().unwrap_or_default(),
          t.zone.clone().unwrap_or_default(),
          t.rack.clone().unwrap_or_default(),
          t.host.clone().unwrap_or_default(),
        )
      })
      .unwrap_or_default();
    initial_topo.register_node_with_location(conf.node_id, addr, weight, loc);
    let sharding = Arc::new(RwLock::new(initial_topo));

    Ok(Arc::new(Self {
      engine,
      raft,
      conf: conf.clone(),
      state_machine: Arc::new(state_machine),
      session,
      shutdown_tx,
      service_handles: Mutex::new(handles),
      _queryable: Mutex::new(None),
      meta_cache,
      sharding,
      batch_write_tx,
      forward_key_cache: HashMap::new(),
    }))
  }

  /// 获取或预声明目标节点的转发 KeyExpr（紧凑数字 ExprId 缓存）
  pub(crate) async fn get_or_declare_forward_key(&self, leader_id: NodeId) -> KeyExpr<'static> {
    if let Some(key) = self.forward_key_cache.pin().get(&leader_id).cloned() {
      return key;
    }
    let key_str = raft_forward_key(leader_id);
    let declared = self
      .session
      .declare_keyexpr(key_str.clone())
      .await
      .unwrap_or_else(|_| KeyExpr::new(key_str).unwrap());
    self
      .forward_key_cache
      .pin()
      .insert(leader_id, declared.clone());
    declared
  }

  pub async fn batch_write_leader(&self, entries: Vec<UpsertKV>) -> Result<()> {
    if entries.is_empty() {
      return Ok(());
    }
    let (tx, rx) = oneshot::<Result<()>>();
    let task = BatchWriteTask {
      entries,
      respond_to: tx,
    };
    self
      .batch_write_tx
      .send(task)
      .await
      .map_err(|_| Error::internal("Batch write queue channel closed"))?;

    rx.recv_async()
      .await
      .map_err(|_| Error::internal("Batch write response channel closed"))?
  }

  pub async fn write_leader(&self, entry: LogEntry) -> Result<AppliedState> {
    let result = self.raft.client_write(entry).await?;
    Ok(result.data)
  }

  pub async fn txn_leader(&self, req: TxnReq) -> Result<TxnReply> {
    let entry = LogEntry::new(Cmd::Txn { req });
    let result = self.raft.client_write(entry).await?;
    match result.data {
      AppliedState::Txn(reply) => Ok(reply),
      _ => Err(Error::internal("Invalid applied state")),
    }
  }

  pub async fn exec(&self, payload: RequestPayload) -> Result<ForwardResponse> {
    self.exec_or_forward(payload).await
  }

  pub async fn write(&self, entry: LogEntry) -> Result<AppliedState> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.write_local_sharding(entry).await;
    }

    let payload = RequestPayload::Write(entry);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::Write(cmd) => Ok(cmd),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn batch_write(&self, req: BatchWriteReq) -> Result<()> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.batch_write_local_sharding(req).await;
    }

    let payload = RequestPayload::BatchWrite(req);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::BatchWrite(()) => Ok(()),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn txn(&self, req: TxnReq) -> Result<TxnReply> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.txn_local_sharding(req).await;
    }

    let payload = RequestPayload::Txn(req);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::Txn(reply) => Ok(reply),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn read(&self, req: GetKVReq) -> Result<GetKVReply> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.read_local_sharding(req).await;
    }
    let sm = &self.state_machine;
    let value = sm.get_kv(&req.key)?;
    Ok(value)
  }

  pub async fn read_linearizable(&self, req: GetKVReq) -> Result<GetKVReply> {
    self.read_with_policy(req, ReadPolicy::ReadIndex).await
  }

  pub async fn read_with_policy(&self, req: GetKVReq, policy: ReadPolicy) -> Result<GetKVReply> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.read_local_sharding(req).await;
    }

    self
      .raft
      .ensure_linearizable(policy)
      .await
      .map_err(|e| Error::internal(format!("Linearizable read check failed: {e}")))?;

    let sm = &self.state_machine;
    let value = sm.get_kv(&req.key)?;
    Ok(value)
  }

  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.scan_prefix_local_sharding(req).await;
    }
    let sm = &self.state_machine;
    let kvs = sm.scan_prefix(&req.prefix)?;
    Ok(kvs)
  }

  pub async fn scan_prefix_linearizable(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    self
      .scan_prefix_with_policy(req, ReadPolicy::ReadIndex)
      .await
  }

  pub async fn scan_prefix_with_policy(
    &self,
    req: ScanPrefixReq,
    policy: ReadPolicy,
  ) -> Result<ScanPrefixReply> {
    if self.conf.mode == ClusterMode::Sharding {
      return self.scan_prefix_local_sharding(req).await;
    }

    self
      .raft
      .ensure_linearizable(policy)
      .await
      .map_err(|e| Error::internal(format!("Linearizable scan check failed: {e}")))?;

    let sm = &self.state_machine;
    let kvs = sm.scan_prefix(&req.prefix)?;
    Ok(kvs)
  }

  pub async fn add_node(&self, req: JoinRequest) -> Result<()> {
    let payload = RequestPayload::Join(req);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::Join(()) => Ok(()),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn remove_node(&self, req: LeaveRequest) -> Result<()> {
    let payload = RequestPayload::Leave(req);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::Leave(()) => Ok(()),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn get_members(&self, req: GetMemberReq) -> Result<GetMemberReply> {
    let payload = RequestPayload::GetMembers(req);
    match self.exec_or_forward(payload).await? {
      ForwardResponse::GetMembers(reply) => Ok(reply),
      _ => Err(Error::internal("Invalid response type")),
    }
  }

  pub async fn get_applied_log_id(&self) -> Result<Option<LogId>> {
    let sm = &self.state_machine;
    sm.get_last_applied_log_id().map_err(Into::into)
  }

  pub async fn is_leader_async(&self) -> bool {
    self.is_leader()
  }

  pub async fn get_membership(&self) -> Result<GetMemberReply> {
    let sm = &self.state_machine;
    let stored = sm.get_last_membership()?;
    let current_leader = self.current_leader_id();

    let membership = stored
      .membership()
      .nodes()
      .map(|(id, node)| (*id, node.clone()))
      .collect();

    Ok(GetMemberReply {
      node_id: self.node_id(),
      current_leader,
      membership,
    })
  }

  pub fn node_id(&self) -> u64 {
    self.conf.node_id
  }

  pub fn leader_id(&self) -> Option<NodeId> {
    self.current_leader_id()
  }

  pub async fn trigger_snapshot(&self) -> Result<Option<LogId>> {
    self
      .raft
      .trigger()
      .snapshot()
      .await
      .map_err(|e| Error::internal(e.to_string()))?;
    Ok(self.snapshot_log_id())
  }

  pub fn snapshot_log_id(&self) -> Option<LogId> {
    self.state_machine.get_last_applied_log_id().ok().flatten()
  }

  pub async fn purge_log(&self, upto_index: u64) -> Result<()> {
    self
      .raft
      .trigger()
      .purge_log(upto_index)
      .await
      .map_err(|e| Error::internal(e.to_string()))?;
    Ok(())
  }

  pub async fn write_local_sharding(&self, entry: LogEntry) -> Result<AppliedState> {
    let sm = &self.state_machine;
    match entry.cmd {
      Cmd::UpsertKV(kv) => {
        sm.apply_batch_upsert_direct(&[kv])?;
        Ok(AppliedState::None)
      }
      Cmd::BatchUpsertKV { entries } => {
        sm.apply_batch_upsert_direct(&entries)?;
        Ok(AppliedState::None)
      }
      Cmd::Txn { req } => {
        let reply = sm.apply_txn_direct(req)?;
        Ok(AppliedState::Txn(reply))
      }
      Cmd::AddNode { .. } | Cmd::RemoveNode { .. } => Ok(AppliedState::None),
    }
  }

  pub async fn batch_write_local_sharding(&self, req: BatchWriteReq) -> Result<()> {
    let sm = &self.state_machine;
    sm.apply_batch_upsert_direct(&req.entries)
      .map_err(Into::into)
  }

  pub async fn txn_local_sharding(&self, req: TxnReq) -> Result<TxnReply> {
    let sm = &self.state_machine;
    let reply = sm.apply_txn_direct(req)?;
    Ok(reply)
  }

  pub async fn read_local_sharding(&self, req: GetKVReq) -> Result<GetKVReply> {
    let sm = &self.state_machine;
    let value = sm.get_kv(&req.key)?;
    Ok(value)
  }

  pub async fn scan_prefix_local_sharding(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    let sm = &self.state_machine;
    let kvs = sm.scan_prefix(&req.prefix)?;
    Ok(kvs)
  }
}
