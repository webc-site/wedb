use std::{str::from_utf8, sync::Arc, time::Duration};

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
      inner: RwLock::new(None),
    }
  }

  /// 连接健康面：wconn GarnetClient 同名探针的包装层转发（C# 口径见 wconn 处标注）
  ///
  /// 代理底层 wconn 会话真实连接态：网络泵退出（EOF/断链）即 false，
  /// 不再是 inner 在位即恒真的失真口径
  #[inline]
  pub fn is_connected(&self) -> bool {
    self.client().is_some_and(|c| c.is_connected())
  }

  #[inline]
  fn client(&self) -> Option<Arc<ConnClient>> {
    self.inner.read().clone()
  }

  /// 断连回收：命令失败且底层会话确已断连时摘除引用，驱动下次调用重连。
  /// ptr_eq 校验防止误伤并发重连放入的新连接
  fn reap_disconnected(&self, client: &Arc<ConnClient>) {
    if !client.is_connected() {
      let mut inner = self.inner.write();
      if inner.as_ref().is_some_and(|c| Arc::ptr_eq(c, client)) {
        *inner = None;
      }
    }
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
    }
  }

  /// 重建底层连接（集群控制面包装：Dispose + Connect）
  pub async fn reconnect_async(&self) {
    self.dispose();
    self.connect_async().await;
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailReplicationOffsetAsync
  pub async fn execute_cluster_fail_replication_offset_async(&self, offset: &str) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let resp = client
      .execute_for_string_result_async(&["CLUSTER", "FAILREPLICATIONOFFSET", offset])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailStopWritesAsync
  pub async fn execute_cluster_fail_stop_writes_async(&self, node_id: &[u8]) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let node_id_str = from_utf8(node_id).unwrap_or("");
    let resp = client
      .execute_for_string_result_async(&["CLUSTER", "FAILSTOPWRITES", node_id_str])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailoverAsync
  pub async fn failover(&self, option: FailoverOption) -> bool {
    let Some(client) = self.client() else {
      return false;
    };
    let cmd: &[&str] = match option {
      FailoverOption::Default => &["CLUSTER", "FAILOVER"],
      FailoverOption::Force => &["CLUSTER", "FAILOVER", "FORCE"],
      FailoverOption::Takeover => &["CLUSTER", "FAILOVER", "TAKEOVER"],
    };
    let resp = client.execute_for_string_result_async(cmd).await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.is_ok_and(|resp| resp == "OK")
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipAsync
  pub async fn gossip_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip("Not connected".into()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", data])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.map_err(Error::from)
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipWithMeetAsync
  pub async fn gossip_with_meet_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip("Not connected".into()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", b"WITHMEET", data])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.map_err(Error::from)
  }

  /// libs/client/GarnetClientAPI/GarnetClientServerCommands.cs:ReplicaOfAsync
  pub async fn replica_of(&self, ip: &str, port: i32) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let mut port_buf = IntBuf::new();
    let port_str = port_buf.format(port);
    let resp = client
      .execute_for_string_result_async(&["REPLICAOF", ip, port_str])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterPublishNoResponse
  pub async fn cluster_publish_async(&self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    if let Some(client) = self.client() {
      let subcmd: &[u8] = if is_spublish { b"SPUBLISH" } else { b"PUBLISH" };
      if let Err(err) = client
        .execute_for_bytes_result_async(&[b"CLUSTER", subcmd, channel, message])
        .await
      {
        self.reap_disconnected(&client);
        log::debug!("集群单向广播发送失败: {err}");
      }
    }
  }

  /// libs/cluster/Server/Migration/ClusterMigrateDriver.cs:SetSlotRange
  pub async fn set_slot_range_async(
    &self,
    state: &str,
    begin_slot: i32,
    end_slot: i32,
    node_id: Option<&str>,
  ) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let mut begin_buf = IntBuf::new();
    let mut end_buf = IntBuf::new();
    let begin_str = begin_buf.format(begin_slot);
    let end_str = end_buf.format(end_slot);
    let mut args = ["CLUSTER", "SETSLOTSRANGE", state, begin_str, end_str, ""];
    let slice = match node_id {
      Some(nid) => {
        args[5] = nid;
        &args[..6]
      }
      None => &args[..5],
    };
    let resp = client.execute_for_string_result_async(slice).await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    Ok(resp.unwrap_or_default())
  }

  /// libs/client/ClientSession/GarnetClientSessionMigrationExtensions.cs:SetClusterMigrateHeader
  pub async fn execute_cluster_migrate_async(
    &self,
    source_node_id: &str,
    replace: bool,
    payload: &[u8],
  ) -> Result<bool> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let replace_str: &[u8] = if replace { b"T" } else { b"F" };
    let vector_str: &[u8] = b"F";
    let res = client
      .execute_for_bytes_result_async(&[
        b"CLUSTER",
        b"MIGRATE",
        source_node_id.as_bytes(),
        replace_str,
        vector_str,
        payload,
      ])
      .await;
    if res.is_err() {
      self.reap_disconnected(&client);
    }
    let res = res.map_err(Error::from)?;
    Ok(res == b"OK" || res == b"+OK\r\n")
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterInitiateReplicaSync
  ///
  /// 副本向主端发起磁盘基同步（5 参：副本节点 id、指派主 repl id、检查点
  /// 条目序列化字节、副本 AOF begin/tail 位点 span）；+OK 返回 "OK"，
  /// -ERR 错误文案经 Err 透出（C# Task&lt;string&gt; 同口径）
  pub async fn initiate_replica_sync_async(
    &self,
    node_id: &str,
    primary_replid: &str,
    checkpoint_entry: &[u8],
    aof_begin: &[u8],
    aof_tail: &[u8],
  ) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[
        b"CLUSTER",
        // wresp 命令表子命令字面量（C# CmdStrings.initiate_replica_sync
        // "INITIATEREPLICASYNC" 在 rust 侧归一为下划线形态）
        b"INITIATE_REPLICA_SYNC",
        node_id.as_bytes(),
        primary_replid.as_bytes(),
        checkpoint_entry,
        aof_begin,
        aof_tail,
      ])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    let resp = resp.map_err(Error::from)?;
    let resp = from_utf8(&resp).unwrap_or_default().to_string();
    if resp == "OK" {
      Ok(resp)
    } else {
      Err(Error::InvalidArgument(resp))
    }
  }

  pub fn dispose(&self) {
    *self.inner.write() = None;
  }
}

impl Default for GarnetClient {
  fn default() -> Self {
    Self::new()
  }
}
