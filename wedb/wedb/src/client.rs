use std::{result, str::from_utf8, sync::Arc, time::Duration};

use compio::time::timeout;
use itoa::Buffer as IntBuf;
use parking_lot::RwLock;
use waof::AofAddress;
use wbase::pool::LimitedFixedBufferPool;
use wconn::{
  Error as WconnError,
  client::{DEFAULT_OUTSTANDING_TASKS as OUTSTANDING_TASKS_LIMIT, GarnetClient as ConnClient},
};
use wresp::cmd_strings::cluster;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  error::{Error, Result},
  server::failover::failover_option::FailoverOption,
};

/// 未连接拒答文案（控制面域 InvalidArgument 变体）
const ERR_NOT_CONNECTED: &str = "客户端未连接";

/// 未连接拒答文案（gossip 域 Gossip 变体，对标 C# Gossip.cs 的 Not connected）
const ERR_GOSSIP_NOT_CONNECTED: &str = "Not connected";

/// 集群控制面命令头（CLUSTER 子命令族首词元，字节帧形态）
const CLUSTER_CMD: &[u8] = b"CLUSTER";
/// 集群控制面命令头（字符串帧形态；与 [`CLUSTER_CMD`] 同词单源双形态）
const CLUSTER_CMD_STR: &str = "CLUSTER";
/// FAILOVER 子命令（failover 三臂同字）
const FAILOVER_CMD: &str = "FAILOVER";
/// GOSSIP 子命令（gossip 两臂同字）
const GOSSIP_CMD: &[u8] = b"GOSSIP";
/// SETSLOTSRANGE 子命令（定槽两臂同字）
const SETSLOTSRANGE_CMD: &str = "SETSLOTSRANGE";
/// SYNC 子命令（主从同步头帧）
const SYNC_CMD: &[u8] = b"SYNC";

/// OK-ack 判据全域单源（应答是否为裸 "OK"）：字节臂出自
/// `wconn::client::GarnetClient::execute_for_bytes_result_async`，字符串臂出自
/// `execute_for_string_result_async`；两臂读侧派发
///（`wconn/src/network/replies.rs` 的 `+OK` 快路径与行读臂）均已剥帧，产出恒为裸 "OK"。
/// server 域（gossip/replication/migration）应答判定一律经本谓词，禁手写字面量比较
///（C# 对应常量：libs/client/GarnetClient.cs:24 的 static readonly RESP_OK）
pub(crate) fn is_ok_ack(res: &[u8]) -> bool {
  res == b"OK"
}

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
  /// 在途命令超时毫秒（对标 C# timeoutMilliseconds，0 = 在途超时与建连限时同时关闭）
  timeout_ms: u64,
  /// 出站 TLS 配置（None = 明文；C# 构造重载 tlsOptions? 同位语义，
  /// 集群出站消费点经 [`ClusterProvider`](crate::server::cluster_provider::ClusterProvider)
  /// 单点取用）
  #[cfg(feature = "tls")]
  tls: Option<Arc<ClientTlsConfig>>,
  /// 网络缓冲池（None = 建连期底层会话自建，见 [`Self::set_network_pool`]）
  network_pool: Option<Arc<LimitedFixedBufferPool>>,
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
    client_name: Option<String>,
  ) -> Self {
    Self::with_config(endpoint, auth_username, auth_password, 0, client_name)
  }

  /// 客户端诊断名（C# clientName 对应访问器）
  pub fn client_name(&self) -> Option<&str> {
    self.client_name.as_deref()
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
      network_pool: None,
      inner: RwLock::new(None),
    }
  }

  /// 网络缓冲池注入（建连期透传底层 wconn 会话，读泵接收缓冲的取还单点）
  ///
  /// 对应 C# GarnetClientSession 构造 `networkPool` 形参：复制/迁移链建连
  /// 传 `ReplicationManager.GetNetworkPool` / `MigrationManager.GetNetworkPool`
  /// 同池跨连接复用（AofSyncTask.cs、MigrateSession.cs 同源）；None = 底层
  /// 会话自建（C# `?? CreateBufferPool` 同型回退）
  pub fn set_network_pool(&mut self, pool: Option<Arc<LimitedFixedBufferPool>>) {
    self.network_pool = pool;
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

  /// 断连回收判别门（命令 Err 臂收口单点）：失败即查底层真连接态并摘除，
  /// 各命令门面的 `if is_err { reap }` 样板由此单源
  #[inline]
  fn reap_on_err<T, E>(&self, client: &Arc<ConnClient>, res: &result::Result<T, E>) {
    if res.is_err() {
      self.reap_disconnected(client);
    }
  }

  /// 建立底层 wconn 连接（集群控制面包装：构造 GarnetClient 委托会话并握手）
  ///
  /// 错误收口本 crate 单源（wconn 域错误经 [`crate::error::Error::Conn`]
  /// 透明变体上抛；嵌入宿主单一 match 域，不直触 wconn::Result）
  pub async fn connect_async(&self) -> Result<()> {
    if self.is_connected() {
      return Ok(());
    }
    let mut client = ConnClient::new(
      self.endpoint.clone(),
      self.auth_username.clone(),
      self.auth_password.clone(),
      Some(self.client_name.clone().unwrap_or_else(|| "wedb".into())),
      OUTSTANDING_TASKS_LIMIT,
      // 在途命令超时与建连限时同源（C# timeoutMilliseconds 同一配置双用；
      // 0 = 在途超时与建连限时同时关闭，对标 C# 构造期 TimeoutChecker 不启用
      // 与建连的毫秒时限非正即不限时臂）
      self.timeout_ms,
    )
    // 闸值为编译期常量 2 的幂，wconn 构造校验恒过（同 C# 构造期校验口径）
    .expect("outstanding tasks limit must be a power of two");
    // 出站 TLS 单点注入（None = 明文字节流，行为不变）
    #[cfg(feature = "tls")]
    client.set_tls(self.tls.clone());
    // 网络缓冲池单点透传（None = 底层会话自建回退）
    client.set_network_pool(self.network_pool.clone());
    // 建连限时严格由 timeout_ms 单值驱动（对标 C# TryConnectSocketAsync 的
    // >0 门）：>0 才限时包裹，0 直接不限时建连，不设缺省兜底
    let connect_res = if self.timeout_ms > 0 {
      match timeout(
        Duration::from_millis(self.timeout_ms),
        client.connect_async(),
      )
      .await
      {
        Ok(res) => res,
        Err(_) => {
          // 建连限时放弃臂（对标 C# Gossip.cs:223-228 放弃即 Dispose）：局部
          // client 随本臂落下经 wconn `GarnetClient::Drop` 转发现拆连面，
          // 静默对端下挂起读即刻落定、泵收场、fd 与池缓冲确定性回收
          log::warn!(
            "GarnetClient 连接到 {} 超时 ({}ms)",
            self.endpoint,
            self.timeout_ms
          );
          return Err(WconnError::Timeout.into());
        }
      }
    } else {
      client.connect_async().await
    };
    match connect_res {
      Ok(()) => {
        *self.inner.write() = Some(Arc::new(client));
        Ok(())
      }
      Err(e) => {
        log::warn!("GarnetClient 连接到 {} 失败: {e}", self.endpoint);
        Err(e.into())
      }
    }
  }

  /// 重建底层连接（集群控制面包装：Dispose + Connect）
  pub async fn reconnect_async(&self) -> Result<()> {
    self.dispose();
    self.connect_async().await
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailReplicationOffsetAsync
  ///
  /// 请求载荷为 [`AofAddress::to_aof_binary`] 带 1 字节长度前缀二进制形
  /// （C# :61 ToByteArray 经 `Memory<byte>` 参数通道上线同形）；应答为逗号
  /// 串文本（C# 收端 ToString），借 bytes 通道上线后 from_utf8 收
  pub async fn execute_cluster_fail_replication_offset_async(&self, offset: &AofAddress) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let payload = offset.to_aof_binary();
    let resp = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        cluster::FAILREPLICATIONOFFSET.as_bytes(),
        &payload,
      ])
      .await;
    self.reap_on_err(&client, &resp);
    resp.map_or_else(
      |_| String::new(),
      |v| from_utf8(&v).unwrap_or_default().to_string(),
    )
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterFailStopWritesAsync
  pub async fn execute_cluster_fail_stop_writes_async(&self, node_id: &[u8]) -> String {
    let Some(client) = self.client() else {
      return String::new();
    };
    let node_id_str = from_utf8(node_id).unwrap_or("");
    let resp = client
      .execute_for_string_result_async(&[CLUSTER_CMD_STR, cluster::FAILSTOPWRITES, node_id_str])
      .await;
    self.reap_on_err(&client, &resp);
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
      return Err(Error::Gossip(ERR_GOSSIP_NOT_CONNECTED.into()));
    };
    let mut ns_buf = IntBuf::new();
    let ns_str = ns_buf.format(ns);
    let mut epoch_buf = IntBuf::new();
    let epoch_str = epoch_buf.format(epoch);
    let resp = client
      .execute_for_string_result_async(&[
        CLUSTER_CMD_STR,
        cluster::FLUSHALL_NS,
        ns_str,
        origin_hex,
        epoch_str,
      ])
      .await;
    self.reap_on_err(&client, &resp);
    resp.map_err(Error::from)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:IssuesFlushAllAsync
  ///
  /// 无盘全量同步清库复位帧：主端在快照流连接上向副本发 `CLUSTER FLUSHALL`
  /// （副本侧 cluster_flush_all_slow 物理截断全库后回 +OK），快照记录帧下发前
  /// 先行，保证副本残留键不与主端数据面发散
  pub async fn issues_flush_all_async(&self) -> Result<String> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip(ERR_GOSSIP_NOT_CONNECTED.into()));
    };
    let resp = client
      .execute_for_string_result_async(&[CLUSTER_CMD_STR, cluster::FLUSHALL])
      .await;
    self.reap_on_err(&client, &resp);
    resp.map_err(Error::from)
  }

  /// libs/client/GarnetClientAPI/GarnetClientClusterCommands.cs:Failover
  pub async fn failover(&self, option: FailoverOption) -> bool {
    let Some(client) = self.client() else {
      return false;
    };
    let cmd: &[&str] = match option {
      FailoverOption::Default => &[CLUSTER_CMD_STR, FAILOVER_CMD],
      FailoverOption::Force => &[CLUSTER_CMD_STR, FAILOVER_CMD, cluster::FORCE],
      FailoverOption::Takeover => &[CLUSTER_CMD_STR, FAILOVER_CMD, cluster::TAKEOVER],
    };
    let resp = client.execute_for_string_result_async(cmd).await;
    self.reap_on_err(&client, &resp);
    resp.is_ok_and(|resp| is_ok_ack(resp.as_bytes()))
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipAsync
  pub async fn gossip_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip(ERR_GOSSIP_NOT_CONNECTED.into()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[CLUSTER_CMD, GOSSIP_CMD, data])
      .await;
    self.reap_on_err(&client, &resp);
    resp.map_err(Error::from)
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipWithMeetAsync
  pub async fn gossip_with_meet_async(&self, data: &[u8]) -> Result<Vec<u8>> {
    let Some(client) = self.client() else {
      return Err(Error::Gossip(ERR_GOSSIP_NOT_CONNECTED.into()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        GOSSIP_CMD,
        cluster::WITHMEET.as_bytes(),
        data,
      ])
      .await;
    self.reap_on_err(&client, &resp);
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
    self.reap_on_err(&client, &resp);
    resp.unwrap_or_default()
  }

  /// libs/cluster/Server/Gossip/GarnetClientExtensions.cs:ExecuteClusterPublishNoResponse
  pub async fn cluster_publish_async(&self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    if let Some(client) = self.client() {
      let subcmd: &[u8] = if is_spublish {
        cluster::SPUBLISH.as_bytes()
      } else {
        cluster::PUBLISH.as_bytes()
      };
      let resp = client
        .execute_no_response_async(&[CLUSTER_CMD, subcmd, channel, message])
        .await;
      if let Err(err) = &resp {
        log::debug!("集群单向广播发送失败: {err}");
      }
      self.reap_on_err(&client, &resp);
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let mut begin_buf = IntBuf::new();
    let mut end_buf = IntBuf::new();
    let begin_str = begin_buf.format(begin_slot);
    let end_str = end_buf.format(end_slot);
    let (args, len) = match node_id {
      Some(nid) => (
        [
          CLUSTER_CMD_STR,
          SETSLOTSRANGE_CMD,
          state,
          nid,
          begin_str,
          end_str,
        ],
        6,
      ),
      None => (
        [
          CLUSTER_CMD_STR,
          SETSLOTSRANGE_CMD,
          state,
          begin_str,
          end_str,
          "",
        ],
        5,
      ),
    };
    let resp = client.execute_for_string_result_async(&args[..len]).await;
    self.reap_on_err(&client, &resp);
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let replace_str: &[u8] = if replace { b"T" } else { b"F" };
    let res = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        cluster::MIGRATE.as_bytes(),
        source_node_id.as_bytes(),
        replace_str,
        slot_list.as_bytes(),
        payload,
      ])
      .await;
    self.reap_on_err(&client, &res);
    let res = res.map_err(Error::from)?;
    Ok(is_ok_ack(&res))
  }

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:ReserveDestinationVectorSetsAsync
  ///
  /// 目标端向量集上下文预留（CLUSTER RESERVE VECTOR_SET_CONTEXTS count），
  /// 应答为预留上下文 id 数组
  pub async fn reserve_vector_set_contexts_async(&self, count: usize) -> Result<Vec<u64>> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let mut count_buf = IntBuf::new();
    let count_str = count_buf.format(count);
    let resp = client
      .execute_for_string_array_result_async(&[
        CLUSTER_CMD_STR,
        cluster::RESERVE,
        cluster::VECTOR_SET_CONTEXTS,
        count_str,
      ])
      .await;
    self.reap_on_err(&client, &resp);
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let resp = client.execute_cluster_attach_sync(sync_metadata).await;
    self.reap_on_err(&client, &resp);
    resp.map_err(Error::from)
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:SetClusterSyncHeader
  pub async fn execute_cluster_sync(&self, source_node_id: &str, payload: &[u8]) -> Result<bool> {
    let Some(client) = self.client() else {
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let res = client
      .execute_for_bytes_result_async(&[CLUSTER_CMD, SYNC_CMD, source_node_id.as_bytes(), payload])
      .await;
    self.reap_on_err(&client, &res);
    let res = res.map_err(Error::from)?;
    Ok(is_ok_ack(&res))
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let resp = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        // wresp 命令表子命令字面量（与 C# CmdStrings.cs:475、
        // GarnetClientSessionReplicationExtensions.cs:19 同字面量，两侧无改名）
        cluster::INITIATE_REPLICA_SYNC.as_bytes(),
        node_id.as_bytes(),
        primary_replid.as_bytes(),
        checkpoint_entry,
        aof_begin,
        aof_tail,
      ])
      .await;
    self.reap_on_err(&client, &resp);
    let resp = resp.map_err(Error::from)?;
    let resp = from_utf8(&resp).unwrap_or_default().to_string();
    if is_ok_ack(resp.as_bytes()) {
      Ok(resp)
    } else {
      Err(Error::Remote(resp))
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let recover_str: &[u8] = if recover_store_from_token { b"1" } else { b"0" };
    let mask_str = replay_aof_map.to_string();
    let resp = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        cluster::BEGIN_REPLICA_RECOVER.as_bytes(),
        recover_str,
        mask_str.as_bytes(),
        primary_replid.as_bytes(),
        checkpoint_entry,
        aof_begin,
        aof_tail,
      ])
      .await;
    self.reap_on_err(&client, &resp);
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
      return Err(Error::InvalidArgument(ERR_NOT_CONNECTED.to_string()));
    };
    let type_str = file_type.to_string();
    let addr_str = start_address.to_string();
    let resp = client
      .execute_for_bytes_result_async(&[
        CLUSTER_CMD,
        cluster::SNAPSHOT_DATA.as_bytes(),
        file_token,
        type_str.as_bytes(),
        addr_str.as_bytes(),
        data,
      ])
      .await;
    self.reap_on_err(&client, &resp);
    let resp = resp.map_err(Error::from)?;
    let resp = from_utf8(&resp).unwrap_or_default().to_string();
    if is_ok_ack(resp.as_bytes()) {
      Ok(resp)
    } else {
      Err(Error::Remote(resp))
    }
  }

  /// 拆连收口（C# GarnetClient Dispose(bool) :521-532 的 facade 对应面）：
  /// 摘除引用的同时转发底层 wconn 拆连面——双向 shutdown 即刻落定
  /// 常驻读，在途任务持 Arc 克隆不阻拆连；最后 Arc 落下经 wconn
  /// `GarnetClient::Drop` 转发同一面（幂等位守卫），静默对端下半开连接的
  /// 读泵 task、读半 fd 与池借出缓冲均确定性回收
  pub fn dispose(&self) {
    // 摘链与拆连分离：写锁只护内槽，拆连面在锁外触达（防他路持锁调用嵌套）
    let client = self.inner.write().take();
    if let Some(client) = client {
      client.dispose();
    }
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
