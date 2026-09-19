use std::{str::from_utf8, sync::Arc, time::Duration};

use compio::time::timeout;
use itoa::Buffer as IntBuf;
use parking_lot::RwLock;
use wconn::client::GarnetClient as ConnClient;
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;

use crate::{
  error::{Error, Result},
  server::failover::failover_option::FailoverOption,
};

const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;

/// 在途命令准入上限（2 的幂，wconn 构造期校验恒过；对标 C# maxOutstandingTasks）
const OUTSTANDING_TASKS_LIMIT: usize = 32;

/// 节点通信客户端 facade（集群控制面包装：`use wconn::client::GarnetClient` 为
/// [`ConnClient`]，协议调用全部委托底层会话，自身不实现客户端协议面）
///
/// C# `libs/client/GarnetClient.cs` 的 GarnetClient 主体 1:1 挂载在
/// [`wconn::client::GarnetClient::new`]，此处不复挂
pub struct GarnetClient {
  pub endpoint: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  /// 连接身份名（C# GarnetClient clientName，握手 SETNAME 用；
  /// None 即沿用本包装层缺省名，gossip 链注入 Gossip-{local endpoint}）
  client_name: Option<String>,
  /// 在途命令超时毫秒（对标 C# timeoutMilliseconds，0 = 关闭）
  timeout_ms: u64,
  /// 出站 TLS 配置（None = 明文；C# 构造重载 tlsOptions? 同位语义，
  /// 集群出站消费点经 [`ClusterProvider`](crate::server::cluster_provider::ClusterProvider)
  /// 单点取用）
  #[cfg(feature = "tls")]
  tls: Option<Arc<ClientTlsConfig>>,
  inner: RwLock<Option<Arc<ConnClient>>>,
}

impl GarnetClient {
  pub fn new() -> Self {
    Self::with_endpoint("127.0.0.1:6379".to_string())
  }

  pub fn with_endpoint(endpoint: String) -> Self {
    Self::with_config(endpoint, None, None, 0, None)
  }

  /// 出站 TLS 配置注入（None = 明文）
  ///
  /// 对标 C# 构造重载 `GarnetTlsOptions? tlsOptions` 第 2 形参位；
  /// rust 以注入器承载，建连期 [`Self::connect_async`] 单点消费
  #[cfg(feature = "tls")]
  pub fn set_tls(&mut self, tls: Option<Arc<ClientTlsConfig>>) {
    self.tls = tls;
  }

  pub fn with_auth(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
  ) -> Self {
    Self::with_config(endpoint, auth_username, auth_password, 0, None)
  }

  /// 全参构造（形参面对位 C# `libs/client/GarnetClient.cs` 的 GarnetClient 构造重载
  /// authUsername/authPassword/timeoutMilliseconds/clientName；facade 构造，
  /// 主体挂载在 [`wconn::client::GarnetClient::new`]，此处不复挂）
  pub fn with_config(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    timeout_ms: u64,
    client_name: Option<String>,
  ) -> Self {
    Self {
      endpoint,
      auth_username,
      auth_password,
      client_name,
      timeout_ms,
      #[cfg(feature = "tls")]
      tls: None,
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
      Some(self.client_name.clone().unwrap_or_else(|| "wedb".into())),
      OUTSTANDING_TASKS_LIMIT,
      // 在途命令超时与建连限时同源（C# timeoutMilliseconds 同一配置双用；
      // 0 = 在途超时关闭，对标 C# TimeoutChecker 不启用，建连保留缺省限时兜底）
      self.timeout_ms,
    )
    // 闸值为编译期常量 2 的幂，wconn 构造校验恒过（同 C# 构造期校验口径）
    .expect("outstanding tasks limit must be a power of two");
    // 出站 TLS 单点注入（None = 明文字节流，行为不变）
    #[cfg(feature = "tls")]
    client.set_tls(self.tls.clone());
    let connect_timeout_ms = if self.timeout_ms > 0 {
      self.timeout_ms
    } else {
      DEFAULT_CONNECT_TIMEOUT_MS
    };
    let timeout_dur = Duration::from_millis(connect_timeout_ms);
    let connect_res = timeout(timeout_dur, client.connect_async()).await;
    match connect_res {
      Ok(Ok(())) => {
        *self.inner.write() = Some(Arc::new(client));
      }
      Ok(Err(e)) => {
        log::warn!("GarnetClient 连接到 {} 失败: {e}", self.endpoint);
      }
      Err(_) => {
        log::warn!(
          "GarnetClient 连接到 {} 超时 ({}ms)",
          self.endpoint,
          timeout_dur.as_millis()
        );
      }
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

  /// rust 自有：总线定向换号广播帧（本端口多租户扩展，C# 无 ns 维度，无对应映射）：
  /// `CLUSTER FLUSHALL_NS <ns> <origin 32hex> <epoch>`，收端 ack 以字符串应答
  /// 承接，非 +OK / 断连以 Err 上抛供协调者判败
  pub async fn execute_cluster_flushall_ns_async(
    &self,
    ns: u64,
    origin_hex: &str,
    epoch: i64,
  ) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip("Not connected".into()));
    };
    let mut ns_buf = IntBuf::new();
    let ns_str = ns_buf.format(ns);
    let mut epoch_buf = IntBuf::new();
    let epoch_str = epoch_buf.format(epoch);
    let resp = client
      .execute_for_string_result_async(&["CLUSTER", "FLUSHALL_NS", ns_str, origin_hex, epoch_str])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.map_err(Error::from)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:IssuesFlushAllAsync
  ///
  /// 无盘全量同步清库复位帧：主端在快照流连接上向副本发 `CLUSTER FLUSHALL`
  /// （副本侧 cluster_flush_all_slow 物理截断全库后回 +OK），快照记录帧下发前
  /// 先行，保证副本残留键不与主端数据面发散
  pub async fn issues_flush_all_async(&self) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip("Not connected".into()));
    };
    let resp = client
      .execute_for_string_result_async(&["CLUSTER", "FLUSHALL"])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.map_err(Error::from)
  }

  /// libs/client/GarnetClientAPI/GarnetClientClusterCommands.cs:Failover
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

  /// 集群控制面 facade 下发口：委托 wconn GarnetClient 的 replica_of 单点
  /// 组装并下发 REPLICAOF（对标 C# IssueAttachReplicas 直调原生 client）。
  /// 本层仅负责连接复用与断连回收，不再二次手抄命令序列
  pub async fn replica_of(&self, ip: &str, port: i32) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let resp = client.replica_of(ip, port).await;
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
        .execute_no_response_async(&[b"CLUSTER", subcmd, channel, message])
        .await
      {
        self.reap_disconnected(&client);
        log::debug!("集群单向广播发送失败: {err}");
      }
    }
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrationDriver.cs:SetSlotRange
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
    let (args, len) = match node_id {
      Some(nid) => (
        ["CLUSTER", "SETSLOTSRANGE", state, nid, begin_str, end_str],
        6,
      ),
      None => (
        ["CLUSTER", "SETSLOTSRANGE", state, begin_str, end_str, ""],
        5,
      ),
    };
    let resp = client.execute_for_string_result_async(&args[..len]).await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    resp.map_err(Into::into)
  }

  /// libs/client/ClientSession/GarnetClientSessionMigrationExtensions.cs:SetClusterMigrateHeader
  ///
  /// 头格式（库级定槽 doc/zh/db.md 4.1 偏离声明）：`CLUSTER MIGRATE
  /// <sourceNodeId> <replace T/F> <slot-list> <payload>`，
  /// `slot-list` 为发送端会话槽集显式携带（C# 头无此参数，接收端逐键
  /// HashSlot 门禁随之废除，改头级一次性判槽）。C# 头的 isVectorSets 位
  /// 本仓废除：向量集帧自描述 kind=5/6 是唯一判据，头不承载该信息
  /// （见 ClusterSession::network_cluster_migrate）
  pub async fn execute_cluster_migrate_async(
    &self,
    source_node_id: &str,
    replace: bool,
    slot_list: &str,
    payload: &[u8],
  ) -> Result<bool> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let replace_str: &[u8] = if replace { b"T" } else { b"F" };
    let res = client
      .execute_for_bytes_result_async(&[
        b"CLUSTER",
        b"MIGRATE",
        source_node_id.as_bytes(),
        replace_str,
        slot_list.as_bytes(),
        payload,
      ])
      .await;
    if res.is_err() {
      self.reap_disconnected(&client);
    }
    let res = res.map_err(Error::from)?;
    Ok(res == b"OK" || res == b"+OK\r\n")
  }

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:ReserveDestinationVectorSetsAsync
  ///
  /// 目标端向量集上下文预留（CLUSTER RESERVE VECTOR_SET_CONTEXTS count），
  /// 应答为预留上下文 id 数组
  pub async fn reserve_vector_set_contexts_async(&self, count: usize) -> Result<Vec<u64>> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let mut count_buf = IntBuf::new();
    let count_str = count_buf.format(count);
    let resp = client
      .execute_for_string_array_result_async(&[
        "CLUSTER",
        "RESERVE",
        "VECTOR_SET_CONTEXTS",
        count_str,
      ])
      .await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    let resp = resp.map_err(Error::from)?;
    resp
      .iter()
      .map(|s| {
        s.parse::<u64>()
          .map_err(|_| Error::InvalidArgument(format!("向量集上下文预留应答非法: {s}")))
      })
      .collect()
  }

  /// CLUSTER ATTACH_SYNC 客户端门面转发（映射内部见注：帧构造与应答口径
  /// 的权威定义在 wconn 会话层 GarnetClientSession::execute_cluster_attach_sync，
  /// 即 wedb/wconn/src/session.rs；C# 侧 GarnetClient 经继承复用
  /// GarnetClientSessionReplicationExtensions 扩展，rust 以组合转发等价承载，
  /// 映射注释收敛于 wconn 会话层一处）。失败即 reap 断连客户端
  pub async fn execute_cluster_attach_sync(&self, sync_metadata: &[u8]) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let resp = client.execute_cluster_attach_sync(sync_metadata).await;
    if resp.is_err() {
      self.reap_disconnected(&client);
    }
    let resp = resp.map_err(Error::from)?;
    Ok(from_utf8(&resp).unwrap_or_default().to_string())
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:SetClusterSyncHeader
  pub async fn execute_cluster_sync(&self, source_node_id: &str, payload: &[u8]) -> Result<bool> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let res = client
      .execute_for_bytes_result_async(&[b"CLUSTER", b"SYNC", source_node_id.as_bytes(), payload])
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
        // wresp 命令表子命令字面量（与 C# CmdStrings.cs:475、
        // GarnetClientSessionReplicationExtensions.cs:19 同字面量，两侧无改名）
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

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterBeginReplicaRecover
  ///
  /// 主端通知副本从接收的检查点恢复（6 参：是否从 token 恢复、AOF 回放
  /// 掩码、主复制 ID、检查点条目字节、快照覆盖 begin/tail 位点 span）；
  /// 应答为副本复制位点 bulk string（C# Task&lt;string&gt; 同口径）
  pub async fn begin_replica_recover_async(
    &self,
    recover_store_from_token: bool,
    replay_aof_map: u64,
    primary_replid: &str,
    checkpoint_entry: &[u8],
    aof_begin: &[u8],
    aof_tail: &[u8],
  ) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let recover_str: &[u8] = if recover_store_from_token { b"T" } else { b"F" };
    let mask_str = replay_aof_map.to_string();
    let resp = client
      .execute_for_bytes_result_async(&[
        b"CLUSTER",
        b"BEGIN_REPLICA_RECOVER",
        recover_str,
        mask_str.as_bytes(),
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
    Ok(from_utf8(&resp).unwrap_or_default().to_string())
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterSnapshotData
  ///
  /// 检查点数据统一传输帧（4 参：token 字节、文件类型值、段起始地址、
  /// 数据；startAddress = -1 为单消息元数据载荷，空 data 为流收尾哨兵）；
  /// +OK 返回 "OK"，-ERR 错误文案经 Err 透出
  pub async fn snapshot_data_async(
    &self,
    file_token: &[u8],
    file_type: i64,
    start_address: i64,
    data: &[u8],
  ) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument("客户端未连接".to_string()));
    };
    let type_str = file_type.to_string();
    let addr_str = start_address.to_string();
    let resp = client
      .execute_for_bytes_result_async(&[
        b"CLUSTER",
        b"SNAPSHOT_DATA",
        file_token,
        type_str.as_bytes(),
        addr_str.as_bytes(),
        data,
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

/// 出站 TLS 单源注入宏：tls 形态经 [`GarnetClient::set_tls`] 就地改造
///（配置取 [`ClusterProvider::try_cluster_tls_client`]），明文形态零操作
///
/// 对标 C# 五类出站连接构造点的
/// `serverOptions.TlsOptions?.TlsClientOptions` 透传位（rust 收敛单宏）
macro_rules! apply_tls {
  ($client:ident, $provider:expr) => {
    #[cfg(feature = "tls")]
    let $client = {
      let mut c = $client;
      c.set_tls($provider.try_cluster_tls_client());
      c
    };
  };
}
pub(crate) use apply_tls;

impl Default for GarnetClient {
  fn default() -> Self {
    Self::new()
  }
}
