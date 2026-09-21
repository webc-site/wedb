//! 主端无盘同步管理器与 leader 编排 (ReplicationSyncManager)
//!
//! 在 garnet 中的相对路径:
//! - libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs
//! - libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSessionTaskStore.cs
//!
//! 会话册子（C# [`ReplicaSyncSessionTaskStore`] 的 diskless 承接面
//! [`ReplicationSyncManager.GetSessionStore`]）持本批会话数组与 NumSessions，
//! 首入册会话为 leader（C# IsFirst）；syncInProgress 读写锁收敛为册子原子的
//! `sync_in_progress` 标志：批量开窗（begin_sync_batch）后新 attach 入册拒绝
//! （C# 读锁排队失败 → RESP_ERR_CREATE_SYNC_SESSION_ERROR 同位语义）。
//!
//! leader 编排链（C# ReplicationSyncDriverAsync → MainStreamingSnapshotDriver-
//! Async → TakeStreamingCheckpointAsync → BeginAofSyncAsync）在 rust 的口径
//! 差异：
//! - C# PrepareForSyncAsync 内 TryAddReplicationDrivers 预锁 AOF 地址防截断
//!   以及 IssueFlushAllAsync 复位副本库：rust 磁盘链既有口径不含此两步（截断
//!   竞态由 begin_aof_sync 的 DataLossCheck 兜底拒绝）；无盘链的清库复位帧
//!   已在 stream_sync prepare 段承接（C# chooseBetweenFullAndPartialSync 的
//!   NeedToFullSync → SetFlushTask(IssueFlushAllAsync) 同位），磁盘链地址
//!   预锁维持不动作；无盘活扫描的锚点窗口（锚后并发写双重应用）由扫描键门
//!   收口，见 [`super::scan_key_gate`] 与快照迭代器模块头；
//! - C# NeedToFullSync（replid 历史 + 库版本 + AOF 区间 + 阈值）：rust 用本链
//!   自己的判据 [`diskless_resync_strategy`](super::super::replication_manager::ReplicationManager::diskless_resync_strategy)
//!   逐会话在 prepare 段计算，不再借用磁盘链
//!   [`disk_resync_strategy`](super::super::replication_manager::ReplicationManager::disk_resync_strategy)
//!   （两链在 C# 本就是两套判定）；AOF 回放量门限（第 4 条件）在 rust 配置面
//!   缺席，登记口径见该函数文档；
//! - C# TryPauseCheckpoints 检查点暂停：rust 活扫描无封版检查点，不适用；
//! - C# WaitOrDieAsync 进度看门狗：rust 由扇出逐帧停等超时承接（见
//!   [`replication_snapshot_iterator`](super::replication_snapshot_iterator) 模块文档）。

use std::{sync::Arc, time::Duration};

use compio::time::sleep;
use parking_lot::{Mutex, RwLock};
use waof::AofAddress;
use wbase::future::yield_now;

use super::scan_key_gate::ScanKeyGate;
use crate::{
  client::GarnetClient,
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    replication::{
      aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
      diskless_replication::{
        replica_sync_session::DisklessSyncSession,
        replication_snapshot_iterator::run_snapshot_fanout,
        sync_status::{SyncStatus, SyncStatusInfo},
      },
      replication_manager::{ReplicationManager, ResyncStrategy},
      sync_metadata::SyncMetadata,
    },
  },
};

/// 会话册子内部状态（C# ReplicaSyncSessionTaskStore 数组 + Replication-
/// SyncManager syncInProgress / NumSessions / Sessions）
#[derive(Default)]
struct SyncManagerInner {
  sessions: Vec<Arc<DisklessSyncSession>>,
  sync_in_progress: bool,
}

/// 主端无盘同步管理器
pub struct ReplicationSyncManager {
  inner: Mutex<SyncManagerInner>,
  /// 扫描键门注册槽（单批量窗口至多一扇；见 [`super::scan_key_gate`] 模块头
  /// 的窗口语义——快照活扫描窗口写栅，锚前全阻、锚后按未读键集栅放）
  scan_gate: RwLock<Option<Arc<ScanKeyGate>>>,
}

impl ReplicationSyncManager {
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:ReplicationSyncManager
  pub fn new() -> Arc<Self> {
    Arc::new(Self {
      inner: Mutex::new(SyncManagerInner::default()),
      scan_gate: RwLock::new(None),
    })
  }

  /// 注册扫描键门（批量窗口单扇：已注册即拒绝，调用方上抛中止本轮扇出）
  pub(super) fn register_scan_gate(&self, gate: Arc<ScanKeyGate>) -> Result<(), String> {
    let mut slot = self.scan_gate.write();
    if slot.is_some() {
      return Err("diskless scan key gate already active".to_string());
    }
    *slot = Some(gate);
    Ok(())
  }

  /// 注销扫描键门（守卫 Drop 单点收口：挂起写命令经槽位门等待体重评放行）
  pub(super) fn clear_scan_gate(&self) {
    *self.scan_gate.write() = None;
  }

  /// 扫描键门裁决：本键写命令在扫描窗口内是否须挂起（读命令不入门；无键门
  /// 在场零开销直通。消费点：槽位门评 cluster_manager_slot_gate 三形态）
  #[inline]
  pub(crate) fn scan_gate_blocks_write(&self, key: &[u8], slot: u16) -> bool {
    self
      .scan_gate
      .read()
      .as_ref()
      .is_some_and(|gate| gate.blocks_write(key, slot))
  }

  /// 创建并入册副本同步会话（C# AddReplicaSyncSession +
  /// ReplicaSyncSessionTaskStore.TryAddReplicaSyncSession 组合：syncInProgress
  /// 读锁下入册、重复节点拒绝、首入册者为 leader）
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:AddReplicaSyncSession
  pub fn add_replica_sync_session(
    &self,
    endpoint: String,
    replica_meta: SyncMetadata,
    sublog_count: usize,
    driver_store: Arc<AofSyncDriverStore>,
  ) -> Result<Arc<DisklessSyncSession>, String> {
    let mut inner = self.inner.lock();
    if inner.sync_in_progress {
      return Err("sync session creation failed: replica sync is in progress".to_string());
    }
    if inner
      .sessions
      .iter()
      .any(|s| s.origin_node_id() == replica_meta.origin_node_id)
    {
      return Err(format!(
        "sync session for replica already exists: {}",
        replica_meta.origin_node_id
      ));
    }
    let is_leader = inner.sessions.is_empty();
    let session = Arc::new(DisklessSyncSession::new(
      endpoint,
      replica_meta,
      is_leader,
      sublog_count,
      driver_store,
    ));
    // C# SetStatus(INITIALIZING)——构造即初始态（SyncStatusInfo::default）
    inner.sessions.push(session.clone());
    Ok(session)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSessionTaskStore.cs:GetNumSessions
  ///
  /// 批量开窗并快照本批会话（C# MainStreamingSnapshotDriverAsync 的
  /// TryWriteLock + NumSessions/Sessions 取数：GetNumSessions 唯一生产
  /// 消费点即 ReplicationSyncManager.cs:185 开窗取数，rust 以会话快照
  /// 的 len 承接同读数；开窗后新 attach 入册拒绝）
  fn begin_sync_batch(&self) -> Result<Vec<Arc<DisklessSyncSession>>, String> {
    let mut inner = self.inner.lock();
    if inner.sync_in_progress {
      // C# 同位异常路径："Failed to acquire write syncInProgress lock!"——
      // 仅 leader 开窗且 leader 必为首入册者，正常不可达；防御性拒绝
      return Err("Failed to acquire write syncInProgress lock!".to_string());
    }
    inner.sync_in_progress = true;
    Ok(inner.sessions.clone())
  }

  /// 清册并关窗（C# GetSessionStore.Clear + syncInProgress.WriteUnlock）
  fn clear_sessions(&self) {
    let mut inner = self.inner.lock();
    inner.sessions.clear();
    inner.sync_in_progress = false;
  }

  /// 会话驱动主循环：leader 攒批等待 → 主快照扇出 → 本会话完成等待 →
  /// BeginAofSync 增量衔接
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:ReplicationSyncDriverAsync
  pub async fn replication_sync_driver(
    &self,
    session: &Arc<DisklessSyncSession>,
    provider: &Arc<ClusterProvider>,
    rm: &Arc<ReplicationManager>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
  ) -> Result<AofAddress, String> {
    // 攒批窗口：仅 leader 等待（C# :127-129，等待窗口让其他副本 attach 入册
    // 同批；窗口时长经 CONFIG REPL_DISKLESS_SYNC_DELAY 运行时可调）
    let diskless_sync_delay = provider.replica_diskless_sync_delay();
    if diskless_sync_delay > 0 && session.is_leader() {
      sleep(Duration::from_secs(diskless_sync_delay as u64)).await;
    }
    // 置 INPROGRESS（C# :132）——leader 的主驱动逐会话等待此状态
    session.set_status(SyncStatus::InProgress, None);

    // 仅 leader 发起主快照驱动（C# :136-142 Task.Run(MainStreamingSnapshot-
    // DriverAsync) + 等信号量；rust 无独立看门狗任务，同任务内联等待等价）
    if session.is_leader()
      && let Err(e) = self
        .main_streaming_snapshot_driver(provider, rm, assets, local_node_id)
        .await
    {
      log::error!("ReplicationSyncDriverAsync: main snapshot driver faulted: {e}");
      // 开窗本身失败（main 驱动未及收敛任何会话）：leader 自判败释放
      // 本任务等待链（内部错误路径已在 main 驱动全员判败，此处仅兜底）
      if !session.is_terminal() {
        session.set_status(SyncStatus::Failed, Some(e));
      }
    }

    // 等待主同步驱动对本会话收敛（C# WaitForSyncCompletionAsync :145）
    session.wait_for_sync_completion().await;
    let SyncStatusInfo {
      sync_status: status,
      error,
    } = session.status_info();
    // 判败早返（C# :148-154 `status.syncStatus:status.error` 文案形态）
    if status == SyncStatus::Failed {
      return Err(format!(
        "{status}:{}",
        error.unwrap_or_else(|| "unknown".to_string())
      ));
    }

    // 本会话 AOF 增量衔接（C# BeginAofSyncAsync :157），finally 释放快照客户端
    let result = session
      .begin_aof_sync(provider, rm, assets, local_node_id)
      .await;
    if let Some(client) = session.client() {
      client.dispose();
    }
    result
  }

  /// 主快照同步编排（C# MainStreamingSnapshotDriverAsync）
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:MainStreamingSnapshotDriverAsync
  async fn main_streaming_snapshot_driver(
    &self,
    provider: &Arc<ClusterProvider>,
    rm: &Arc<ReplicationManager>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
  ) -> Result<(), String> {
    let sessions = self.begin_sync_batch()?;
    let result = self
      .stream_sync(provider, rm, assets, local_node_id, &sessions)
      .await;
    // 终态收敛（C# :210-219：成功全员 SUCCESS、异常全员 FAILED——已有
    // 终态的会话（prepare 段判败 / PartialResync 免快照放行）不覆写）
    match &result {
      Ok(()) => {
        for s in &sessions {
          if s.in_progress() {
            s.set_status(SyncStatus::Success, None);
          }
        }
      }
      Err(e) => {
        log::error!("MainStreamingSnapshotDriverAsync faulted: {e}");
        for s in &sessions {
          if !s.is_terminal() {
            s.set_status(SyncStatus::Failed, Some(e.clone()));
          }
        }
      }
    }
    // 判败会话的钉线驱动摘除不在此处：C# SetStatus(FAILED) 内
    // AofSyncDriverStore.TryRemove(AofSyncDriver) 的 rust 承接已收敛到
    // [`set_status`](super::replica_sync_session::DisklessSyncSession::set_status)
    // 一处（按会话持有实例匹配退场），批内另立按节点 id 的第二注销通道会误删
    // 同键重挂的新驱动
    // finally 清册关窗（C# :220-234 GetSessionStore.Clear + 释放锁 + 信号）
    self.clear_sessions();
    result
  }

  /// 会话准备 + 全量扇出（快照扇出段 TakeStreamingCheckpointAsync 另锚于
  /// [`replication_snapshot_iterator`](super::replication_snapshot_iterator)）
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:PrepareForSyncAsync
  async fn stream_sync(
    &self,
    provider: &Arc<ClusterProvider>,
    rm: &Arc<ReplicationManager>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
    sessions: &[Arc<DisklessSyncSession>],
  ) -> Result<(), String> {
    // 等待全员进 INPROGRESS（C# :189-193 while(!InProgress) Task.Yield——
    // follower 任务在入册后即刻置态，自旋窗口为任务调度粒度）
    for s in sessions {
      while !(s.in_progress() || s.is_terminal()) {
        yield_now().await;
      }
    }

    let PrimaryReplicationAssets { wal, .. } = assets;
    let sublog_count = rm.sublog_count() as i32;
    let committed_until = AofAddress::create(sublog_count, wal.committed_until_address() as i64);
    let primary_aof_begin = AofAddress::create(sublog_count, wal.begin_address() as i64);
    let primary_aof_tail = AofAddress::create(sublog_count, wal.tail_address() as i64);
    // 主端当下 store 版本（对标 ReplicationSyncManager.cs:267 逐会话判定前
    // Sessions[i].currentStoreVersion = storeWrapper.store.CurrentVersion；
    // 与本链恢复帧 diskless_replication/replica_sync_session.rs 的
    // current_store_version 同源，不另立版本源）
    let current_store_version = provider
      .try_store()
      .map(|store| store.current_version())
      .unwrap_or(0);

    // prepare 段：逐会话策略协商 + 全量会话建连（C# PrepareForSyncAsync；
    // 免快照会话直接 SUCCESS 放行，C# SetStatus(SUCCESS)+Sessions[i]=null
    // 的摘除在 rust 以终态标记 + 扇出侧 retain 承接）
    let mut full_sessions: Vec<Arc<DisklessSyncSession>> = Vec::new();
    for s in sessions {
      if s.is_terminal() {
        continue;
      }
      let strategy = rm.diskless_resync_strategy(
        &s.replica_meta,
        current_store_version,
        &committed_until,
        &primary_aof_begin,
        &primary_aof_tail,
        provider.fast_aof_truncate(),
      );
      let is_full = matches!(strategy, ResyncStrategy::FullResync { .. });
      let sync_start = match &strategy {
        ResyncStrategy::PartialResync {
          sync_start_address, ..
        }
        | ResyncStrategy::FullResync {
          sync_start_address, ..
        } => *sync_start_address,
      };
      s.set_sync_start(sync_start);
      if !is_full {
        s.set_status(SyncStatus::Success, None);
        continue;
      }
      s.set_full_sync(true);
      let client = Arc::new(GarnetClient::with_auth(
        s.endpoint.clone(),
        provider.cluster_username(),
        provider.cluster_password(),
      ));
      client.connect_async().await;
      if !client.is_connected() {
        client.dispose();
        s.set_status(
          SyncStatus::Failed,
          Some("failed connecting to replica for stream sync".to_string()),
        );
        continue;
      }
      // 全量会话清库复位帧（C# chooseBetweenFullAndPartialSync :271-279：
      // NeedToFullSync 成立即 SetFlushTask(IssueFlushAllAsync)，循环尾
      // WaitForFlushAsync 等齐后快照记录帧才开闸）：副本 diskless 支不本地
      // Reset（ReplicaDisklessSync.cs 的 !disklessSync 门），残留键全清唯赖
      // 此帧，否则副本曾持有而主端已无的键永不删除，全量完成后数据面永久
      // 发散；仅全量支发（PartialResync 无快照窗口不重灌，C# 同门），失败
      // 即该会话判败摘除（C# SetFlushTask 非 OK → SetStatus(FAILED) 同位）
      if let Err(msg) = s.issue_flush_all_async(&client).await {
        client.dispose();
        s.set_status(SyncStatus::Failed, Some(msg));
        continue;
      }
      s.set_client(client);
      full_sessions.push(Arc::clone(s));
    }
    if full_sessions.is_empty() {
      return Ok(());
    }

    // 批内共享一枚快照覆盖锚（对标 C# SnapshotIteratorManager 构造期
    // CheckpointCoveredAddress = Log.TailAddress，ReplicationSnapshotIterator
    // .cs:56-61：逐会话赋同值），取锚时点已挪入快照扇出——活扫描 + 起点锚
    // 的收敛保证要求锚点后于扫描键门闭窗、先于任何读值（锚前记录效果已含
    // 于快照、AOF 推流恰从锚起续推（授予位点 = 锚），[协商位点, 锚] 窗口
    // 记录不再重放；锚点语义与门控窗口的协同见
    // [`replication_snapshot_iterator`](super::replication_snapshot_iterator)
    // 与 [`super::scan_key_gate`] 两处模块头）。取锚后逐会话赋同值，同批
    // 副本共享同锚即修掉旧逐副本各起一遍路径逐调用独立取锚造成授予位点
    // 互异的问题
    // 预锁截断线（对标 C# PrepareForSyncAsync #region pauseAofTruncation :240-254）：
    // 扇出前批量把本批全量副本驱动以当下 Log.BeginAddress 为 start 入库钉线，
    // 令流式快照传送窗口内 FastAofTruncate 无从越过起点；越线致 try_add 被拒即
    // 重取 minServiceable 位点退避重试（C# while 环等价）。建驱动时逐会话回挂
    // 实例（对标 C# TryAddReplicationDrivers 内 rss.AddAofSyncTask，
    // AofSyncDriverStore.cs:447），判败会话的钉线由 set_status 按该实例匹配退场
    // 防泄漏；成功会话的钉线由 begin_aof_sync 就地 try_remove + 二次 try_add
    // 更新置换
    let allow_data_loss = provider.allow_data_loss();
    loop {
      let min_serviceable = AofAddress::create(sublog_count, wal.begin_address() as i64);
      let pin_drivers: Vec<Arc<AofSyncDriver>> = full_sessions
        .iter()
        .map(|s| {
          let driver = Arc::new(AofSyncDriver::new(
            local_node_id,
            s.origin_node_id(),
            rm.sublog_count(),
            &min_serviceable,
            None,
          ));
          s.add_aof_sync_task(Arc::clone(&driver));
          driver
        })
        .collect();
      if rm
        .aof_sync_driver_store
        .try_add_replication_drivers(&pin_drivers, allow_data_loss)
      {
        break;
      }
      yield_now().await;
    }

    run_snapshot_fanout(provider, local_node_id, full_sessions, assets).await
  }
}

impl Default for ReplicationSyncManager {
  fn default() -> Self {
    Self {
      inner: Mutex::new(SyncManagerInner::default()),
      scan_gate: RwLock::new(None),
    }
  }
}
