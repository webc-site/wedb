use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicI64, Ordering},
};

use log::{error, trace, warn};
use wbase::time::now_ms;

use crate::{
  client::{GarnetClient, apply_tls},
  error::{Error, Result},
  server::{
    cluster_manager::ClusterManager, cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
  },
};

/// libs/cluster/Server/Gossip/GarnetServerNode.cs:GarnetServerNode
pub struct NodeConnection {
  pub node_id: u128,
  pub address: String,
  pub port: i32,
  pub client: Arc<GarnetClient>,
  pub last_send: AtomicI64,
  pub last_recv: AtomicI64,
  /// 上次全量发送时的配置演化版本号（对标 GarnetServerNode.lastConfig
  /// 引用比较：配置版本变化才发全量）
  pub last_sent_config_version: AtomicI64,
  pub has_sent_full: AtomicBool,
  /// gossip 探测飞行标志：false 空闲（含上轮已完成）可派发，true 飞行中。
  /// 对标 GarnetServerNode.gossipTask 三态轮询——C# 的 RanToCompletion 与
  /// null 在 TryGossip 视角下都允许立即重发，两态即对齐语义
  pub gossip_in_flight: AtomicBool,
  pub initialized: AtomicBool,
  pub disposed: AtomicBool,
}

impl NodeConnection {
  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:GarnetServerNode
  ///
  /// 客户端身份、超时与凭证对标 C# 构造臂直接从 clusterProvider 派生
  /// （:89-94）：clientName = `Gossip-{CurrentConfig.LocalNodeEndpoint}`；
  /// timeoutMilliseconds = cluster-node-timeout 毫秒形态（:69-70
  /// GetClientTimeoutMilliseconds 的 <=0 → 0 = 关闭口径，rust 侧
  /// cluster_node_timeout() 以 None 承载无限，换算回 0）；
  /// 认证凭证即 C# 的 ClusterUsername/ClusterPassword 读取面
  pub fn new(
    node_id: u128,
    address: String,
    port: i32,
    cluster_provider: &ClusterProvider,
  ) -> Self {
    let endpoint = format!("{}:{}", address, port);
    let client_name = cluster_provider
      .cluster_manager()
      .map(|cm| format!("Gossip-{}", cm.current_config().local_node_endpoint()));
    let timeout_ms = cluster_provider
      .cluster_node_timeout()
      .map_or(0, |d| d.as_millis() as u64);
    let client = GarnetClient::with_config(
      endpoint,
      cluster_provider.cluster_username(),
      cluster_provider.cluster_password(),
      timeout_ms,
      client_name,
    );
    // 出站 TLS 单源透传（对标 GarnetClusterConnectionStore.cs:195
    // `new GarnetServerNode(..., tlsOptions?.TlsClientOptions, ...)`）
    apply_tls!(client, cluster_provider);
    let client = Arc::new(client);
    Self {
      node_id,
      address,
      port,
      client,
      last_send: AtomicI64::new(0),
      last_recv: AtomicI64::new(0),
      last_sent_config_version: AtomicI64::new(-1),
      has_sent_full: AtomicBool::new(false),
      gossip_in_flight: AtomicBool::new(false),
      initialized: AtomicBool::new(false),
      disposed: AtomicBool::new(false),
    }
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:UpdateGossipSend
  #[inline]
  pub fn update_send_time(&self) {
    let now = now_ms() as i64;
    self.last_send.store(now, Ordering::Release);
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:UpdateGossipRecv
  #[inline]
  pub fn update_recv_time(&self) {
    let now = now_ms() as i64;
    self.last_recv.store(now, Ordering::Release);
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:GetConnectionInfo
  #[inline]
  pub fn get_connection_info(&self) -> ConnectionInfo {
    let ping = self.last_send.load(Ordering::Acquire);
    let pong = self.last_recv.load(Ordering::Acquire);
    let now = now_ms() as i64;
    let last_io = if pong == 0 {
      0
    } else {
      (now.saturating_sub(ping)) / 1000
    };
    ConnectionInfo {
      connected: self.client.is_connected(),
      ping,
      pong,
      last_io,
    }
  }

  /// 检查底层客户端是否已处于连接态
  #[inline]
  pub fn is_connected(&self) -> bool {
    self.client.is_connected()
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:InitializeAsync
  ///
  /// 确保仅初始化一次（对标 C# if (initialized != 0 || Interlocked.CompareExchange(ref initialized, 1, 0) != 0) return default;）。
  /// 双重检查避免重复握手以及对不可达节点产生级联 200ms 超时阻断。
  pub async fn initialize_async(&self) {
    if self.initialized.load(Ordering::Acquire) {
      return;
    }
    if self
      .initialized
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return;
    }
    self.client.connect_async().await;
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:TryMeetAsync
  pub async fn try_meet_async(&self, config_bytes: &[u8]) -> Result<Vec<u8>> {
    if self.disposed.load(Ordering::Acquire) {
      return Err(Error::Gossip("connection disposed".into()));
    }
    self.initialize_async().await;
    self.update_send_time();
    let resp = self.client.gossip_with_meet_async(config_bytes).await?;
    self.update_recv_time();
    Ok(resp)
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:GetMostRecentConfig
  ///
  /// 本轮派发要不要带配置、带什么：取当前配置快照与演化版本号，与本连接缓存的
  /// last_sent_config_version 比对（对标 C# per-node lastConfig 引用比较），
  /// 首轮或版本已变则序列化全量、否则返回空包 ping。取快照前按需惰性刷新本地
  /// 复制偏移（对标 C# 在 lastConfig 上 LazyUpdateLocalReplicationOffset，
  /// sublog-0 供 CLUSTER NODES 消费）。复制偏移源经 cluster_mgr 持有的
  /// cluster_provider 触达，与 C# 走 clusterProvider.replicationManager 同形，
  /// 不为下沉新增第二份配置缓存或额外存储句柄。last_sent_config_version 的
  /// 写入不在本方法（由 manager 在响应成功后推进），本方法只读比较，
  /// 保证「仅成功轮次记账」。返回 (配置字节, 版本号, 是否全量)。
  #[inline]
  pub fn get_most_recent_config(&self, cluster_mgr: &ClusterManager) -> (Vec<u8>, i64, bool) {
    if let Some(rm) = cluster_mgr.cluster_provider.replication_manager() {
      cluster_mgr.lazy_update_local_replication_offset(rm.get_replication_offset(0));
    }
    let config = cluster_mgr.current_config();
    let version = cluster_mgr.config_version();
    let first_send = !self.has_sent_full.swap(true, Ordering::SeqCst);
    if first_send || self.last_sent_config_version.load(Ordering::Relaxed) != version {
      (config.to_byte_array(), version, true)
    } else {
      (Vec::new(), version, false)
    }
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:GossipAsync
  pub async fn try_gossip_async(&self, config_bytes: &[u8]) -> Result<Vec<u8>> {
    if self.disposed.load(Ordering::Acquire) {
      return Err(Error::Gossip("connection disposed".into()));
    }
    self.initialize_async().await;
    self.update_send_time();
    let resp = self.client.gossip_async(config_bytes).await?;
    self.update_recv_time();
    Ok(resp)
  }

  /// TryFlushAllNsAsync（本端口多租户扩展的换号广播臂，形态仿
  /// TryMeetAsync：dispose 探测 + initialize + 收发计时）
  ///
  /// 定向发送 CLUSTER FLUSHALL_NS 帧并校验 +OK ack，非 OK 应答判败上抛
  pub async fn try_flushall_ns_async(&self, ns: u64, origin_hex: &str, epoch: i64) -> Result<()> {
    if self.disposed.load(Ordering::Acquire) {
      return Err(Error::Gossip("connection disposed".into()));
    }
    self.initialize_async().await;
    self.update_send_time();
    let resp = self
      .client
      .execute_cluster_flushall_ns_async(ns, origin_hex, epoch)
      .await?;
    self.update_recv_time();
    if resp != "OK" {
      return Err(Error::Gossip(format!(
        "node {} FLUSHALL_NS ack unexpected",
        self.node_id
      )));
    }
    Ok(())
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:TryClusterPublish
  ///
  /// 转发 CLUSTER PUBLISH / SPUBLISH 消息至指定远端节点。
  /// 对标 C# GarnetServerNode.TryClusterPublish:
  /// 1. 探测 dispose 锁（已释放则打 warn 快速返回）
  /// 2. 检查连接状态（未连接则打 error 快速返回，不产生同步重连阻塞）
  /// 3. 执行无响应异步写出
  pub async fn try_cluster_publish_async(&self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    if self.disposed.load(Ordering::Acquire) {
      warn!("Could not acquire readLock for publish forwarding");
      return;
    }

    self.initialize_async().await;

    if !self.client.is_connected() {
      error!("TryClusterPublish: client not connected; skipping publish forwarding");
      return;
    }

    self
      .client
      .cluster_publish_async(is_spublish, channel, message)
      .await;
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:Dispose
  pub fn dispose(&self) {
    if self
      .disposed
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      trace!("GarnetServerNode.Dispose called multiple times");
      return;
    }
    self.client.dispose();
  }
}
