use std::{
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::time::timeout;
use itoa::Buffer as IntBuf;
use parking_lot::RwLock;
use wconn::GarnetClient as ConnClient;

use crate::{
  error::{Error, Result},
  server::failover::failover_option::FailoverOption,
};

/// libs/client/GarnetClient.cs:GarnetClient
/// 节点通信客户端（与 wedb/wconn 对接，提供集群控制面协议调用）
pub struct GarnetClient {
  pub endpoint: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  connected: AtomicBool,
  inner: RwLock<Option<Arc<ConnClient>>>,
}

impl GarnetClient {
  pub fn new() -> Self {
    Self::with_endpoint("127.0.0.1:6379".to_string())
  }

  pub fn with_endpoint(endpoint: String) -> Self {
    Self {
      endpoint,
      auth_username: None,
      auth_password: None,
      connected: AtomicBool::new(false),
      inner: RwLock::new(None),
    }
  }

  pub fn with_auth(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
  ) -> Self {
    Self {
      endpoint,
      auth_username,
      auth_password,
      connected: AtomicBool::new(false),
      inner: RwLock::new(None),
    }
  }

  #[inline]
  pub fn is_connected(&self) -> bool {
    self.connected.load(Ordering::Acquire)
  }

  /// 建立底层 wconn 连接（集群控制面包装：构造 GarnetClient 委托会话并握手）
  pub async fn connect_async(&self) {
    if self.is_connected() {
      return;
    }
    let mut client = ConnClient::new(
      self.endpoint.clone(),
      self.auth_username.clone(),
      self.auth_password.clone(),
      Some("wedb".into()),
      32,
    );
    let connect_res = timeout(Duration::from_millis(200), client.connect_async()).await;
    if matches!(connect_res, Ok(Ok(()))) {
      *self.inner.write() = Some(Arc::new(client));
      self.connected.store(true, Ordering::Release);
    }
  }

  /// 重建底层连接（集群控制面包装：Dispose + Connect）
  pub async fn reconnect_async(&self) {
    self.dispose();
    self.connect_async().await;
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailReplicationOffsetAsync
  pub async fn execute_cluster_fail_replication_offset_async(&self, offset: &str) -> String {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      client
        .execute_for_string_result_async(&["CLUSTER", "FAILREPLICATIONOFFSET", offset])
        .await
        .unwrap_or_default()
    } else {
      String::new()
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailStopWritesAsync
  pub async fn execute_cluster_fail_stop_writes_async(&self, node_id: &[u8]) -> String {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      let node_id_str = from_utf8(node_id).unwrap_or("");
      client
        .execute_for_string_result_async(&["CLUSTER", "FAILSTOPWRITES", node_id_str])
        .await
        .unwrap_or_default()
    } else {
      String::new()
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailoverAsync
  pub async fn failover(&self, option: FailoverOption) -> bool {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      let cmd: &[&str] = match option {
        FailoverOption::Default => &["CLUSTER", "FAILOVER"],
        FailoverOption::Force => &["CLUSTER", "FAILOVER", "FORCE"],
        FailoverOption::Takeover => &["CLUSTER", "FAILOVER", "TAKEOVER"],
      };
      client
        .execute_for_string_result_async(cmd)
        .await
        .map(|resp| resp == "OK")
        .unwrap_or(false)
    } else {
      false
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipAsync
  pub async fn gossip_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      Ok(
        client
          .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", data])
          .await?,
      )
    } else {
      Err(Error::Gossip("Not connected".into()))
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipWithMeetAsync
  pub async fn gossip_with_meet_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      Ok(
        client
          .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", b"WITHMEET", data])
          .await?,
      )
    } else {
      Err(Error::Gossip("Not connected".into()))
    }
  }

  /// libs/client/GarnetClientAPI/GarnetClientServerCommands.cs:ReplicaOfAsync
  pub async fn replica_of(&self, ip: &str, port: i32) -> String {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      let mut port_buf = IntBuf::new();
      let port_str = port_buf.format(port);
      client
        .execute_for_string_result_async(&["REPLICAOF", ip, port_str])
        .await
        .unwrap_or_default()
    } else {
      String::new()
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterPublishNoResponse
  pub async fn cluster_publish_async(&self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    let client = self.inner.read().clone();
    if let Some(client) = client {
      let subcmd: &[u8] = if is_spublish { b"SPUBLISH" } else { b"PUBLISH" };
      if let Err(err) = client
        .execute_for_bytes_result_async(&[b"CLUSTER", subcmd, channel, message])
        .await
      {
        log::debug!("集群单向广播发送失败: {err}");
      }
    }
  }

  pub fn dispose(&self) {
    self.connected.store(false, Ordering::Release);
    *self.inner.write() = None;
  }
}

impl Default for GarnetClient {
  fn default() -> Self {
    Self::new()
  }
}
