use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicI64, Ordering},
};

use log::{error, trace, warn};
use wbase::time::now_ms;

use crate::{
  client::GarnetClient,
  error::{Error, Result},
  server::connection_info::ConnectionInfo,
};

/// libs/cluster/Server/Gossip/GarnetServerNode.cs:GarnetServerNode
pub struct NodeConnection {
  pub node_id: String,
  pub address: String,
  pub port: i32,
  pub client: Arc<GarnetClient>,
  pub last_send: AtomicI64,
  pub last_recv: AtomicI64,
  /// 上次全量发送时的配置演化版本号（对标 GarnetServerNode.lastConfig
  /// 引用比较：配置版本变化才发全量）
  pub last_sent_config_version: AtomicI64,
  pub has_sent_full: AtomicBool,
  pub initialized: AtomicBool,
  pub disposed: AtomicBool,
}

impl NodeConnection {
  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:GarnetServerNode
  pub fn new(
    node_id: String,
    address: String,
    port: i32,
    auth_username: Option<String>,
    auth_password: Option<String>,
  ) -> Self {
    let endpoint = format!("{}:{}", address, port);
    let client = if auth_username.is_some() || auth_password.is_some() {
      Arc::new(GarnetClient::with_auth(
        endpoint,
        auth_username,
        auth_password,
      ))
    } else {
      Arc::new(GarnetClient::with_endpoint(endpoint))
    };
    Self {
      node_id,
      address,
      port,
      client,
      last_send: AtomicI64::new(0),
      last_recv: AtomicI64::new(0),
      last_sent_config_version: AtomicI64::new(-1),
      has_sent_full: AtomicBool::new(false),
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
