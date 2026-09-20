//! 主端无盘同步会话 (DisklessSyncSession)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs
//!
//! C# 的 ReplicaSyncSession 以 partial class 在 diskless / diskbased 两目录
//! 分部拼合；rust 无 partial，diskless 分部独立成 [`DisklessSyncSession`]，
//! 与既有 diskbased 会话
//! [`replica_sync_session`](super::super::replica_sync_session)（对标
//! DiskbasedReplication/ReplicaSyncSession.cs）分文件并存，不混写。
//!
//! 会话本体承接 C# diskless 分部的网络面与状态面：
//! - 网络方法（ConnectAsync / InitializeIterationBuffer / TryWriteRecordSpan /
//!   SendAndResetIterationBuffer）：C# 委托 AofSyncDriver 内 GarnetClient 的
//!   迭代缓冲；rust 快照流走独立 CLUSTER SYNC 停等链（副本承接面
//!   `cluster_sync_slow`），迭代缓冲收敛为扇出迭代器的共享攒批
//!   （见 [`replication_snapshot_iterator`](super::replication_snapshot_iterator)），
//!   会话仅持本副本的同步客户端句柄；推流驱动侧持有面见下条状态机。
//! - 状态机 SetStatus / WaitForSyncCompletionAsync：C# signalCompletion
//!   SemaphoreSlim 以 event_listener::Event 终态广播等价承载；FAILED 臂
//!   AofSyncDriverStore.TryRemove(AofSyncDriver) 的会话侧摘除随本会话持有的
//!   驱动实例（C# AddAofSyncTask 由 TryAddReplicationDrivers 预锁段回挂）走
//!   实例匹配退场，会话判败不留册。
//! - BeginAofSyncAsync：快照段收敛后以授予位点建 APPENDLOG 推流通道、
//!   经推流连接发 ATTACH_SYNC（primary 元数据）完成副本恢复握手，以副本
//!   回传位点构造 AofSyncDriver 入册并挂泵（原
//!   `replica_diskless_sync::try_begin_diskless_sync_async` 的顺序步骤
//!   迁入本方法，编排改由
//!   [`replication_sync_driver`](super::replication_sync_manager::ReplicationSyncManager::replication_sync_driver)
//!   承接）。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use event_listener::Event;
use parking_lot::Mutex;
use waof::AofAddress;
use wbase::hex::hex_str_u128;

use crate::{
  client::GarnetClient,
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    replication::{
      aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
      aof_sync_task::TimePulseSource,
      diskless_replication::sync_status::{SyncStatus, SyncStatusInfo},
      replica_wire::{REPL_ATTACH_TIMEOUT, TcpSessionWire},
      replication_manager::ReplicationManager,
      sync_metadata::SyncMetadata,
    },
    worker::NodeRole,
  },
};

/// 主端副本会话命令停等超时（C# serverOptions.ReplicaSyncTimeout 的 rust 既有
/// 承接值：清库帧与 ATTACH_SYNC 恢复握手共用，一处定义）
const REPLICA_SYNC_CMD_TIMEOUT: Duration = Duration::from_secs(30);

/// 无盘同步会话（C# diskless 分部 ReplicaSyncSession 本体）
pub struct DisklessSyncSession {
  /// 副本 attach 上报的同步元数据（C# replicaSyncMetadata）
  pub replica_meta: SyncMetadata,
  /// 副本推流端点（C# 侧经 GetEndpointFromNodeId 反查，rust 由 attach 帧
  /// 处理面解析后随会话携带）
  pub endpoint: String,
  /// 本批 leader 标志（C# GetSessionStore.IsFirst：批内首个入册会话）
  is_leader: bool,
  /// 状态机（C# ssInfo）+ 终态事件广播（C# signalCompletion）
  status: Mutex<SyncStatusInfo>,
  sync_done: Event,
  /// 快照扇出客户端（C# AofSyncDriver 内 garnet client 的快照流承载面；
  /// PrepareForSync 阶段建连，见 replication_sync_manager）
  client: Mutex<Option<Arc<GarnetClient>>>,
  /// 本会话是否需全量快照（C# NeedToFullSync 判定结果 fullSync；rust 策略
  /// 判定用本链自己的判据 diskless_resync_strategy，见 replication_sync_manager）
  is_full_sync: Mutex<bool>,
  /// 协商同步起始位点（C# aofSyncDriver.StartAddress 的 PartialResync 授予源）
  sync_start: Mutex<AofAddress>,
  /// 悲观快照覆盖位点（C# checkpointCoveredAofAddress，构造语义对标
  /// SnapshotIteratorManager.cs:56-61：批内共享一枚，扫描开始前取自 WAL 尾）
  checkpoint_covered_aof_address: Mutex<AofAddress>,
  /// 驱动册句柄（C# SetStatus 内 `clusterProvider.replicationManager
  /// .AofSyncDriverStore` 的可达面；C# 会话持 clusterProvider 反查，rust 依赖
  /// 方向收敛为直持驱动册，不持 ReplicationManager 免与其册子成引用环）
  driver_store: Arc<AofSyncDriverStore>,
  /// 本会话关联的 AOF 推流驱动（C# AofSyncDriver 属性 :23，由
  /// [`add_aof_sync_task`](Self::add_aof_sync_task) 于预锁入库时写入；
  /// C# SetStatus(FAILED) 即经该实例匹配摘除）
  aof_sync_driver: Mutex<Option<Arc<AofSyncDriver>>>,
}

impl DisklessSyncSession {
  /// 会话构造（C# 无独立构造器：ReplicationSyncManager.cs 的 AddReplicaSyncSession
  /// 在方法体内直接 `new ReplicaSyncSession(...)`，该符号的 1:1 锚点挂在入册编排
  /// `ReplicationSyncManager::add_replica_sync_session`，本构造点不复挂）
  pub(super) fn new(
    endpoint: String,
    replica_meta: SyncMetadata,
    is_leader: bool,
    sublog_count: usize,
    driver_store: Arc<AofSyncDriverStore>,
  ) -> Self {
    Self {
      replica_meta,
      endpoint,
      is_leader,
      status: Mutex::new(SyncStatusInfo::default()),
      sync_done: Event::new(),
      client: Mutex::new(None),
      is_full_sync: Mutex::new(false),
      sync_start: Mutex::new(AofAddress::create(sublog_count as i32, 0)),
      checkpoint_covered_aof_address: Mutex::new(AofAddress::create(sublog_count as i32, 0)),
      driver_store,
      aof_sync_driver: Mutex::new(None),
    }
  }

  /// 会话副本节点 id（C# replicaSyncMetadata.originNodeId）
  pub fn origin_node_id(&self) -> u128 {
    self.replica_meta.origin_node_id
  }

  /// 是否本批 leader（C# GetSessionStore.IsFirst）
  pub fn is_leader(&self) -> bool {
    self.is_leader
  }

  /// 关联本会话的 AOF 推流驱动实例
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:AddAofSyncTask
  ///
  /// C# 由 AofSyncDriverStore.TryAddReplicationDrivers 建驱动时逐会话回挂
  /// （AofSyncDriverStore.cs:447）；rust 驱动册不持会话，回挂点落在预锁段
  /// 调用方 `stream_sync`（见 replication_sync_manager）
  pub(super) fn add_aof_sync_task(&self, driver: Arc<AofSyncDriver>) {
    *self.aof_sync_driver.lock() = Some(driver);
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:SetStatus
  ///
  /// 首错保留（C# `ssInfo.error ??= error`）；FAILED 先按实例匹配摘除本会话
  /// 驱动（C# `AofSyncDriverStore.TryRemove(AofSyncDriver)`，摘除即断推流并
  /// 解除 AOF 截断线/背压钉制），再广播终态唤醒全部完成等待者
  /// （C# signalCompletion.Release）
  pub fn set_status(&self, status: SyncStatus, error: Option<String>) {
    {
      let mut info = self.status.lock();
      if info.error.is_none() {
        info.error = error;
      }
      // 状态后置于错误写入（C# 注释：set this after error to signal complete
      // state change），摘除与事件广播在锁外避免唤醒即读锁竞争
      info.sync_status = status;
    }
    match status {
      SyncStatus::Success => {
        self.sync_done.notify(usize::MAX);
      }
      SyncStatus::Failed => {
        // 摘除早于广播（C# :129-130 同序）：等待者被唤醒时本会话驱动已出册
        if let Some(driver) = self.aof_sync_driver.lock().clone() {
          self.driver_store.try_remove_current(&driver);
        }
        self.sync_done.notify(usize::MAX);
      }
      _ => {}
    }
  }

  /// 当前状态快照（C# GetSyncStatusInfo）
  pub fn status_info(&self) -> SyncStatusInfo {
    self.status.lock().clone()
  }

  /// C# Failed
  pub fn failed(&self) -> bool {
    self.status.lock().sync_status == SyncStatus::Failed
  }

  /// C# InProgress
  pub fn in_progress(&self) -> bool {
    self.status.lock().sync_status == SyncStatus::InProgress
  }

  /// 是否终态（SUCCESS / FAILED）
  pub fn is_terminal(&self) -> bool {
    matches!(
      self.status.lock().sync_status,
      SyncStatus::Success | SyncStatus::Failed
    )
  }

  /// 等待快照同步段收敛到终态
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:WaitForSyncCompletionAsync
  pub async fn wait_for_sync_completion(&self) {
    loop {
      // 先挂监听再判终态：终态广播与检查之间的竞态由监听器兜底捕获
      let listener = self.sync_done.listen();
      if self.is_terminal() {
        return;
      }
      listener.await;
    }
  }

  /// 快照扇出客户端句柄（未建连为 None，扇出侧判败摘除）
  pub fn client(&self) -> Option<Arc<GarnetClient>> {
    self.client.lock().clone()
  }

  pub(super) fn set_client(&self, client: Arc<GarnetClient>) {
    *self.client.lock() = Some(client);
  }

  /// C# fullSync 字段读
  pub fn is_full_sync(&self) -> bool {
    *self.is_full_sync.lock()
  }

  pub(super) fn set_full_sync(&self, full: bool) {
    *self.is_full_sync.lock() = full;
  }

  /// 协商同步起始位点读
  pub fn sync_start(&self) -> AofAddress {
    *self.sync_start.lock()
  }

  pub(super) fn set_sync_start(&self, addr: AofAddress) {
    *self.sync_start.lock() = addr;
  }

  /// 快照覆盖位点读（C# checkpointCoveredAofAddress）
  pub fn checkpoint_covered_aof_address(&self) -> AofAddress {
    *self.checkpoint_covered_aof_address.lock()
  }

  /// 快照覆盖位点写入（C# SnapshotIteratorManager.cs:60 构造期逐会话赋值）
  pub(super) fn set_checkpoint_covered_aof_address(&self, addr: AofAddress) {
    *self.checkpoint_covered_aof_address.lock() = addr;
  }

  /// 向副本下发 CLUSTER FLUSHALL 清库复位帧并等 +OK（仅全量支，见
  /// replication_sync_manager 的 stream_sync prepare 段）
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:IssueFlushAllAsync
  ///
  /// C# 为异步挂账形态（SetFlushTask 挂 flushTask + 超时包装，循环尾
  /// WaitForFlushAsync 等齐；resp != "OK" 即 SetStatus(FAILED)）；rust 快照流
  /// 为停等链，直接停等应答，非 OK / 超时 / 断连以 Err 上抛由调用方判败摘除
  pub(super) async fn issue_flush_all_async(
    &self,
    client: &Arc<GarnetClient>,
  ) -> Result<(), String> {
    let resp = timeout(REPLICA_SYNC_CMD_TIMEOUT, client.issues_flush_all_async())
      .await
      .map_err(|_| "cluster flushall timeout".to_string())?
      .map_err(|e| format!("cluster flushall failed: {e}"))?;
    if resp != "OK" {
      return Err(resp);
    }
    Ok(())
  }

  /// 快照段收敛后为本会话建立 AOF 增量推流
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:BeginAofSyncAsync
  ///
  /// 授予位点按策略分派（对标 ReplicaSyncSession.cs:229 fullSync ?
  /// checkpointCoveredAofAddress : aofSyncDriver.StartAddress）：FullResync
  /// 用批内共享快照覆盖锚（快照已含锚前效果，AOF 恰从锚续推无重放窗口），
  /// PartialResync 无快照窗口维持协商位点。
  ///
  /// rust 通道形态差异：C# 快照流与 AOF 推流复用同一 GarnetClient 连接；
  /// rust 快照流为 CLUSTER SYNC 停等链（会话 [`client`](Self::client)），
  /// AOF 推流独立 TcpSessionWire 连接，两链在副本端各自承接。驱动入册对标
  /// TryAddReplicationDriver + TryConnectToReplica 组合。
  ///
  /// 失败口径（对标 C# :259-263 `catch (Exception ex) → SetStatus(FAILED,
  /// ex.Message)`）：建连 / ATTACH_SYNC 恢复握手 / data_loss_check / 驱动入册
  /// 任一步失败都经 [`set_status`](Self::set_status) 落终态，判败即按实例摘除
  /// 本会话在册驱动——不残留旧驱动在泵上继续向已判败副本推流、也不残留钉线
  /// 拉死 AOF 截断线
  pub async fn begin_aof_sync(
    &self,
    provider: &Arc<ClusterProvider>,
    rm: &Arc<ReplicationManager>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
  ) -> Result<AofAddress, String> {
    self
      .try_begin_aof_sync(provider, rm, assets, local_node_id)
      .await
      .inspect_err(|e| {
        self.set_status(SyncStatus::Failed, Some(e.clone()));
      })
  }

  /// [`begin_aof_sync`](Self::begin_aof_sync) 的步骤体（rust 分解：C# 整段包在
  /// try/catch 内，rust 把「抛」收敛为 Err 上抛、catch 统一落在门面一处）
  async fn try_begin_aof_sync(
    &self,
    provider: &Arc<ClusterProvider>,
    rm: &Arc<ReplicationManager>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
  ) -> Result<AofAddress, String> {
    let PrimaryReplicationAssets { wal, pump, .. } = assets;
    let is_full = self.is_full_sync();
    let granted = if is_full {
      self.checkpoint_covered_aof_address()
    } else {
      self.sync_start()
    };
    let sublog_count = rm.sublog_count() as i32;

    // 建立 AOF 增量推流连接
    let auth_user = provider.cluster_username();
    let auth_pwd = provider.cluster_password();
    let wire = TcpSessionWire::connect(
      &self.endpoint,
      local_node_id,
      0,
      auth_user.as_deref(),
      auth_pwd.as_deref(),
      // 复制网络缓冲池同池注入（对标 C# AofSyncTask.cs:134
      // replicationManager.GetNetworkPool 形参）
      rm.network_pool(),
      // 出站 TLS 单源透传（配置源注释见 TcpSessionWire::connect）
      #[cfg(feature = "tls")]
      provider.try_cluster_tls_client().as_ref(),
    )
    .await
    .map_err(|e| format!("failed connecting to replica for AOF stream: {e}"))?;

    // diskless 恢复握手：经推流连接向副本发 ATTACH_SYNC（primary 元数据），
    // 副本 else 臂 try_replica_diskless_recovery 完成 WAL 地址空间对齐、
    // 复制位点收敛与主复制 ID 收敛后回传恢复位点；主端以该位点为推流起点，
    // 保证首个 APPENDLOG 记录帧与副本日志尾严格衔接（否则必命中 divergent
    // 断流）
    let recover_meta = SyncMetadata {
      full_sync: is_full,
      origin_node_role: NodeRole::Primary,
      origin_node_id: local_node_id,
      current_primary_repl_id: rm.primary_repl_id(),
      current_store_version: provider
        .try_store()
        .map(|store| store.current_version())
        .unwrap_or(0),
      current_aof_begin_address: granted,
      current_aof_tail_address: AofAddress::create(sublog_count, wal.tail_address() as i64),
      current_replication_offset: rm.get_current_replication_offset(),
      checkpoint_entry: None,
    };
    // 恢复帧限时取 attach 级 REPL_ATTACH_TIMEOUT(60s)：C# 主端恢复帧
    // ExecuteAttachSyncAsync（AofSyncDriver.cs）为裸 Task 无显式限时，
    // rust 加严设上界、不改语义，取值与 attach 级同旋钮单点对齐
    let sync_from = timeout(
      REPL_ATTACH_TIMEOUT,
      wire.attach_sync(&recover_meta.to_byte_array()),
    )
    .await
    .map_err(|_| "diskless attach sync timeout".to_string())?
    .map_err(|e| format!("failed issuing diskless attach sync to replica: {e}"))?;
    let sync_from = AofAddress::from_string(&sync_from)
      .ok_or_else(|| format!("invalid recovery offset returned from replica: {sync_from}"))?;

    // DataLossCheck 兜底（对标 C# SendCheckpointAsync 尾段
    // appendOnlyFile.DataLossCheck 同位语义，比对基准同为当下 Log.BeginAddress）：
    // 副本请求位点低于主端日志起点即快照流期间发生 AOF 截断，推流无法连续
    // 衔接——默认拒绝建流，allow_data_loss（派生式命中）放行仅告警
    rm.data_loss_check(
      provider.allow_data_loss(),
      &sync_from,
      &AofAddress::create(sublog_count, wal.begin_address() as i64),
    )?;

    // 驱动入库接线（推流起点 = 副本恢复位点，对标 C# BeginAofSyncAsync :254
    // TryAddReplicationDriver(ref syncFromAddress)）：此处为「二次更新」，
    // try_remove 先摘扇出前预锁的钉线驱动、再以恢复位点 try_add 置换（同 id
    // 覆盖，对标 C# TryAddReplicationDriver 对已存在 remoteNodeId 的原地替换）；
    // 新驱动不回挂会话（C# AofSyncDriver 属性 private set、仅 AddAofSyncTask
    // 一处写入，BeginAofSyncAsync 也不回挂，判败摘除的始终是预锁段回挂的实例，
    // 此后入册失败即未入册、无残留）；
    // 脉冲源随驱动构造一次注入，对标 C# AofSyncTask 构造期捕获 appendOnlyFile/
    // backpressure/clusterProvider——tail 位点、序列号读取源与每轮实时读的节流
    // 频率配置面；运行时配置未装配即无脉冲源，与 C# timePulseEnabled=false
    // 同形态静默）
    rm.aof_sync_driver_store
      .try_remove(self.replica_meta.origin_node_id);
    let pulse_source =
      provider
        .try_aof()
        .zip(provider.try_runtime_config())
        .map(|(aof, runtime_config)| {
          Arc::new(TimePulseSource {
            aof,
            backpressure: rm.aof_sync_driver_store.backpressure(),
            runtime_config,
          })
        });
    let driver = Arc::new(AofSyncDriver::new(
      local_node_id,
      self.replica_meta.origin_node_id,
      rm.sublog_count(),
      &sync_from,
      pulse_source,
    ));
    if !rm
      .aof_sync_driver_store
      .try_add_replication_driver(driver.clone(), false)
    {
      return Err("failed adding replication driver".to_string());
    }
    driver.attach_wire(wire);

    // 挂载推流泵
    pump.attach_wake(wal);
    let _ = pump.sync_backlog(wal).await;

    log::info!(
      "Diskless sync primary setup completed for replica {}, replica recovered at: {}, negotiated sync start: {}",
      hex_str_u128(self.replica_meta.origin_node_id),
      sync_from.to_aof_string(),
      self.sync_start().to_aof_string()
    );
    Ok(sync_from)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 会话最小装配（仅状态机与驱动持有面）
  fn session(node_id: u128, store: Arc<AofSyncDriverStore>) -> DisklessSyncSession {
    DisklessSyncSession::new(
      "127.0.0.1:0".to_string(),
      SyncMetadata {
        full_sync: true,
        origin_node_role: NodeRole::Replica,
        origin_node_id: node_id,
        current_primary_repl_id: "replid".to_string(),
        current_store_version: 0,
        current_aof_begin_address: AofAddress::create(2, 0),
        current_aof_tail_address: AofAddress::create(2, 100),
        current_replication_offset: AofAddress::create(2, 100),
        checkpoint_entry: None,
      },
      true,
      2,
      store,
    )
  }

  fn driver(remote_node_id: u128, start: i64) -> Arc<AofSyncDriver> {
    Arc::new(AofSyncDriver::new(
      0x10CA1,
      remote_node_id,
      2,
      &AofAddress::create(2, start),
      None,
    ))
  }

  /// 会话判败必连带摘除回挂的推流驱动（对标
  /// libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
  /// ReplicaSyncSession.cs:129 SetStatus(FAILED) 内
  /// AofSyncDriverStore.TryRemove(AofSyncDriver)）。旧行为 set_status 只写状态
  /// 与广播、不动驱动册：begin_aof_sync 于 data_loss_check 判败即早返，扇出前
  /// 预锁的钉线驱动留册，永久拉回 AOF 截断线致无界增长
  #[test]
  fn test_set_status_failed_detaches_session_driver() {
    let store = Arc::new(AofSyncDriverStore::new(2));
    let s = session(0x21, Arc::clone(&store));

    // prepare 段建连即败（尚未回挂驱动）：判败无册可摘，仅置终态
    s.set_status(SyncStatus::Failed, Some("connect failed".to_string()));
    assert_eq!(store.count(), 0);
    assert!(s.failed());

    // 预锁段回挂 + 入库后判败：驱动当场出册
    let d = driver(0x21, 100);
    assert!(store.try_add_replication_driver(Arc::clone(&d), false));
    assert_eq!(store.count(), 1);
    s.add_aof_sync_task(d);
    s.set_status(SyncStatus::Failed, Some("aof truncated".to_string()));
    assert_eq!(store.count(), 0, "判败必摘本会话驱动");
    // 首错保留（C# ssInfo.error ??= error）
    assert_eq!(s.status_info().error.as_deref(), Some("connect failed"));
  }

  /// SUCCESS 留册不摘（终态广播后 AOF 增量推流继续）；FAILED 只按实例匹配退场，
  /// 同节点已被二次 try_add 置换时不误删新驱动（C# TryRemove(AofSyncDriver) 的
  /// 引用匹配语义，rust 不另立按节点 id 的第二注销通道）
  #[test]
  fn test_set_status_detach_matches_driver_instance_only() {
    let store = Arc::new(AofSyncDriverStore::new(2));
    let s = session(0x22, Arc::clone(&store));
    let old = driver(0x22, 100);
    assert!(store.try_add_replication_driver(Arc::clone(&old), false));
    s.add_aof_sync_task(old);

    s.set_status(SyncStatus::Success, None);
    assert_eq!(store.count(), 1, "成功会话驱动留册");

    // 恢复位点二次 try_add 原地置换后再判败：会话仍持旧实例，摘除不得波及新驱动
    let fresh = driver(0x22, 200);
    assert!(store.try_add_replication_driver(Arc::clone(&fresh), false));
    s.set_status(SyncStatus::Failed, Some("late failure".to_string()));
    let kept = store.drivers();
    assert_eq!(kept.len(), 1);
    assert!(
      Arc::ptr_eq(&kept[0], &fresh),
      "只摘本会话实例，同键重挂的新驱动留册"
    );
  }
}
