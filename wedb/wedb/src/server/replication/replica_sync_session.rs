//! 主端副本同步会话（INITIATE_REPLICA_SYNC 服务面与副本 attach 链）
//!
//! 对标 C# ReplicaSyncSession（libs/cluster/Server/Replication/PrimaryOps/
//! DiskbasedReplication/ReplicaSyncSession.cs）——副本发起同步请求后，
//! 主端协商同步策略、建立副本发送通道（AofSyncDriver + wire）、挂推流
//! 泵并补扫存量积压。检查点快照下发段由 [`snapshot_transmission`] 承接
//! （SNAPSHOT_DATA 段流 + BEGIN_REPLICA_RECOVER 往返）。对标 C#
//! AcquireCheckpointEntryAsync 先于快照下发 TryAddReplicationDriver 钉住
//! 截断线（见 send_checkpoint_and_recover 预锁段），传送+恢复完成后由本
//! 会话承接 startAofSync 尾段：以授予位点二次 TryAddReplicationDriver 更新
//! + TryConnectToReplica（AofSyncDriver.RunAsync → connect + APPENDLOG_INIT
//! + 迭代泵）。

use std::{path::Path, sync::Arc};

use waof::AofAddress;
use wbase::{future::yield_now, hex::hex_str_u128};

use crate::{
  client::{GarnetClient, apply_tls},
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    replication::{
      aof_sync_driver::AofSyncDriver,
      aof_sync_task::TimePulseSource,
      checkpoint_entry::CheckpointEntry,
      replica_wire::{AofSyncWire, REPLICA_SYNC_TIMEOUT, TcpSessionWire},
      replication_manager::{ReplicationManager, ResyncStrategy},
      snapshot_transmission::{SnapshotTransmitSources, send_store_checkpoint},
      sync_metadata::SyncMetadata,
    },
    wait_async,
  },
};

/// 主端副本同步会话
pub struct ReplicaSyncSession {
  rm: Arc<ReplicationManager>,
}

/// 传输期读者守卫：持 tail 条目读者使淘汰链 TrySuspendReaders 失败而停手，
/// 快照文件不被 unlink（C# AcquireCheckpointEntryAsync 持读者 +
/// finally localEntry.RemoveReader 的 try/finally 三段形状）；Drop 兜底
/// 出错路径，transmit 返回前显式释放
#[derive(Default)]
struct SendReaderGuard(Option<Arc<CheckpointEntry>>);

impl SendReaderGuard {
  fn get(&self) -> Option<&CheckpointEntry> {
    self.0.as_deref()
  }

  /// 释放旧读者并持有新条目
  fn replace(&mut self, entry: Option<Arc<CheckpointEntry>>) {
    self.release();
    self.0 = entry;
  }

  fn release(&mut self) {
    if let Some(e) = self.0.take() {
      e.remove_reader();
    }
  }
}

impl Drop for SendReaderGuard {
  fn drop(&mut self) {
    self.release();
  }
}

impl ReplicaSyncSession {
  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ReplicaSyncSession
  ///
  /// 本端节点 id 由发起时刻动态传入（集群配置装配时序可能晚于本会话构造）
  pub fn new(rm: Arc<ReplicationManager>) -> Self {
    Self { rm }
  }

  /// 副本驱动入库并接线发送通道（内存通道 attach 形态）
  ///
  /// 对标 C# SendCheckpointAsync 尾段 TryAddReplicationDriver +
  /// TryConnectToReplica 的组合（wire 已由装配方建立）；重复 attach 先移除
  /// 旧驱动（对标 C# AcquireCheckpointEntryAsync 内 AssertDoesNotExist +
  /// 断链重连的驱动置换语义）；脉冲源随驱动构造一次注入
  ///（对标 C# AofSyncDriver 构造器逐子日志捕获 appendOnlyFile/backpressure）
  pub fn attach_replica_wire(
    &self,
    local_node_id: u128,
    remote_node_id: u128,
    wire: impl Into<AofSyncWire>,
    start_address: &AofAddress,
    pulse_source: Option<Arc<TimePulseSource>>,
  ) -> bool {
    self.rm.aof_sync_driver_store.try_remove(remote_node_id);
    let driver = Arc::new(AofSyncDriver::new(
      local_node_id,
      remote_node_id,
      self.rm.sublog_count(),
      start_address,
      pulse_source,
    ));
    if !self
      .rm
      .aof_sync_driver_store
      .try_add_replication_driver(driver.clone(), false)
    {
      return false;
    }
    driver.attach_wire(wire);
    true
  }

  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  /// ReplicaSyncSession.cs:SendCheckpointAsync
  ///
  /// INITIATE_REPLICA_SYNC 主端处理全链：协商策略 → FullResync 检查点下发
  /// （SNAPSHOT_DATA 段流 + BEGIN_REPLICA_RECOVER 往返）→ DataLossCheck →
  /// TCP 通道建连（含 init 帧握手）→ 驱动入库接线 → 推流泵挂载 → 存量补扫。
  /// 返回授予副本的同步起始位点（syncFromAofAddress 应答载荷）。
  ///
  /// skipLocalMainStoreCheckpoint 形态（本地无检查点 / PartialResync）：跳过
  /// 快照下发，从协商位点起 AOF 直推——rust 副本引擎不随 attach 重构，在线
  /// 引擎实例全程不换，换引擎只发生在 FullResync 的检查点导入链上（对位
  /// C# recoverFromRemote = !skipLocalMainStoreCheckpoint 一路透传给
  /// TryReplicaDiskbasedRecovery 的 recoverStoreFromToken）；判据原委见
  /// [`ReplicationManager::disk_resync_strategy`] 文档，该函数即 C#
  /// ValidateMetadata 的 rust 对位、锚点已在其上登记
  pub async fn initiate_replica_sync(
    &self,
    provider: &Arc<ClusterProvider>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
  ) -> Result<AofAddress, String> {
    let PrimaryReplicationAssets { wal, pump, .. } = assets;
    // 1. 同步策略协商（committed = 主端已提交位；primary_begin = 主端 AOF 起点）。
    //    子日志维度动态取 rm 装配值（C# Log.CommittedUntilAddress / Log.BeginAddress
    //    为 AofPhysicalSublogCount 维向量）；rust WalLog 为单物理日志，全部子日志
    //    槽位共享同一 u64 地址空间，create(N, v) 填满向量即该事实的正确投影——
    //    多子日志装配下高位槽位不再丢 0 / 误判为 MAX
    let sublog_count = self.rm.sublog_count() as i32;
    let committed_until = AofAddress::create(sublog_count, wal.committed_until_address() as i64);
    let primary_aof_begin = AofAddress::create(sublog_count, wal.begin_address() as i64);
    let strategy = self.rm.disk_resync_strategy(
      replica_meta,
      &committed_until,
      &primary_aof_begin,
      provider.fast_aof_truncate(),
    );
    let mut sync_start = match &strategy {
      ResyncStrategy::PartialResync {
        sync_start_address, ..
      }
      | ResyncStrategy::FullResync {
        sync_start_address, ..
      } => *sync_start_address,
    };

    // 2. FullResync 检查点下发（#region sendStoresSnapshotData +
    //    beginReplicaRecover 段）：检查点目录/本地检查点缺席时维持 AOF 直推
    if matches!(strategy, ResyncStrategy::FullResync { .. })
      && let Some(granted) = self
        .send_checkpoint_and_recover(provider, replica_endpoint, replica_meta, local_node_id)
        .await?
    {
      sync_start = granted;
    }

    // 3. 建立副本 TCP 发送通道（connect 即建连 + APPENDLOG init 握手等 +OK；
    //    副本会话对 init 帧注册重放驱动，握手成功即副本接收面就绪）
    let auth_user = provider.cluster_username();
    let auth_pwd = provider.cluster_password();
    let wire = TcpSessionWire::connect(
      replica_endpoint,
      local_node_id,
      0,
      auth_user.as_deref(),
      auth_pwd.as_deref(),
      // 出站 TLS 单源透传（配置源注释见 TcpSessionWire::connect）
      #[cfg(feature = "tls")]
      provider.try_cluster_tls_client().as_ref(),
    )
    .await
    .map_err(|e| format!("Failed connecting to replica for aofSync: {e}"))?;

    // 4. 时间脉冲源组装 + 驱动入库接线（TryAdd 失败即截断拒绝，对标 C#
    //    拒绝语义；脉冲源随驱动构造一次注入，对标 C# AofSyncTask 构造期
    //    捕获 appendOnlyFile / backpressure / clusterProvider——CLUSTER
    //    ADVANCE_TIME 心跳帧的 tail 位点、序列号读取源与每轮实时读的节流频率
    //    配置面；运行时配置未装配即无脉冲源，与 C# timePulseEnabled=false
    //    同形态整体静默）
    let pulse_source =
      provider
        .try_aof()
        .zip(provider.try_runtime_config())
        .map(|(aof, runtime_config)| {
          Arc::new(TimePulseSource {
            aof,
            backpressure: self.rm.aof_sync_driver_store.backpressure(),
            runtime_config,
          })
        });
    if !self.attach_replica_wire(
      local_node_id,
      replica_meta.origin_node_id,
      wire,
      &sync_start,
      pulse_source,
    ) {
      return Err("Failed trying to try update replication task".to_string());
    }

    // 5. 推流泵挂载（入队信号唤醒增量拉取）+ 存量补扫（attach 前的记录）
    pump.attach_wake(wal);
    let (forwarded, skipped) = pump
      .sync_backlog(wal)
      .await
      .map_err(|e| format!("AOF backlog sync failed: {e}"))?;
    log::info!(
      "Replica {} aof sync attached from {:?}: backlog forwarded {forwarded}, skipped {skipped}",
      hex_str_u128(replica_meta.origin_node_id),
      sync_start
    );

    Ok(sync_start)
  }

  /// 检查点下发 + BEGIN_REPLICA_RECOVER 往返（C# SendCheckpointAsync 的
  /// sendStoresSnapshotData / startAofSync 前段：AcquireCheckpointEntry 的
  /// 最新检查点获取、快照流、副本恢复位点回收与 DataLossCheck）
  ///
  /// 返回 None = skipLocalMainStoreCheckpoint（本地检查点缺席或目录未接线，
  /// 或按需重拍混尽后允许丢数据，维持 AOF 直推；C# 同名判定的对应形态）
  async fn send_checkpoint_and_recover(
    &self,
    provider: &Arc<ClusterProvider>,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
    local_node_id: u128,
  ) -> Result<Option<AofAddress>, String> {
    // AcquireCheckpointEntryAsync 的最新检查点获取与按需检查点拍摄（OnDemandCheckpoint）：
    // 覆盖起点落后截断线或元数据无效即触发 take_on_demand_checkpoint 重拍
    let mut num_odc_attempts = 0;
    const MAX_ODC_ATTEMPTS: usize = 2;
    let sublog_count = self.rm.sublog_count() as i32;

    let mut reader = SendReaderGuard::default();

    let entry = loop {
      // 原始条目（raw）供触发判定并持读者防淘汰；发送候选过滤无效元数据
      //（无效条目不发，混尽允许丢数据后落回 skip 直推）
      enum LatestOutcome {
        Empty,
        Found(Arc<CheckpointEntry>),
        Busy,
      }
      let outcome = {
        let store = self.rm.checkpoint_store.read();
        if store.entry_count() == 0 {
          LatestOutcome::Empty
        } else {
          match store.try_get_latest_checkpoint_entry_from_memory() {
            Some(e) => LatestOutcome::Found(e),
            None => LatestOutcome::Busy,
          }
        }
      };
      let raw = match outcome {
        LatestOutcome::Empty => None,
        LatestOutcome::Found(e) => Some(e),
        LatestOutcome::Busy => {
          log::warn!("Could not acquire lock for existing checkpoint, retrying.");
          yield_now().await;
          continue;
        }
      };
      let entry = raw
        .clone()
        .filter(|e| e.metadata.store_version != -1 && e.metadata.store_hlog_token != 0);
      reader.replace(raw);

      let truncated_until = self.rm.aof_sync_driver_store.get_truncated_until();
      // 触发面对标 C# startAofAddress.AnyLesser(TruncatedUntil) || !validMetadata：
      // 空库时 C# TryGetLatestCheckpointEntryFromMemory 造幻影条目且
      // skipLocalMainStoreCheckpoint=true 使 validMetadata 恒真，仅覆盖落后触发，
      // 幻影覆盖起点 = rust 首有效位点 0（rust WalLog 无头区，对标 C#
      // kFirstValidAofAddress=64 规整）；条目在册但 store_version==-1 或
      // store_hlog_token==0 即 !validMetadata 恒触发
      let needs_odc = match reader.get() {
        None => AofAddress::create(sublog_count, 0).any_lesser(&truncated_until),
        Some(e) => {
          e.get_min_aof_covered_address(0)
            .any_lesser(&truncated_until)
            || e.metadata.store_version == -1
            || e.metadata.store_hlog_token == 0
        }
      };

      if needs_odc && provider.on_demand_checkpoint() {
        if num_odc_attempts >= MAX_ODC_ATTEMPTS {
          // 混尽：对标 C# 保持读者落出重试圈，落后条目照发（AOF 直推兜底）
          if provider.allow_data_loss() {
            log::warn!(
              "Failed to acquire checkpoint after {num_odc_attempts} on-demand checkpoint attempts. Possible data loss."
            );
            break entry;
          } else {
            return Err(format!(
              "Failed to acquire checkpoint after {num_odc_attempts} on-demand checkpoint attempts: possible data loss"
            ));
          }
        }
        // 对标 C# RemoveReader 先于 TakeOnDemandCheckpoint：先释放读者，
        // 重拍登记触发的淘汰链才能推进
        reader.release();
        num_odc_attempts += 1;
        log::info!("Taking on-demand checkpoint, attempt {num_odc_attempts}.");
        match provider.take_on_demand_checkpoint().await {
          Ok(true) => yield_now().await,
          Ok(false) => log::warn!("On-demand checkpoint skipped (in progress or paused)"),
          Err(e) => log::warn!("On-demand checkpoint execution failed: {e}"),
        }
        yield_now().await;
        continue;
      } else {
        break entry;
      }
    };

    let Some(entry) = entry else {
      log::info!("Skip checkpoint send: no local checkpoint (skipLocalMainStoreCheckpoint)");
      return Ok(None);
    };
    let Some(checkpoint_dir) = provider.try_checkpoint_dir() else {
      log::warn!("Skip checkpoint send: checkpoint dir not wired");
      return Ok(None);
    };

    // 专用客户端向副本下发（用后即弃，对标 C# gcs 构造 / finally Dispose；
    // 出站 TLS 单源透传，对标 ReplicaDiskbasedSync.cs:112 构造 GarnetClient
    // 的 `tlsOptions: serverOptions.TlsOptions?.TlsClientOptions` 形参位）
    let client = GarnetClient::with_auth(
      replica_endpoint.to_string(),
      provider.cluster_username(),
      provider.cluster_password(),
    );
    apply_tls!(client, provider);
    client.connect_async().await;
    if !client.is_connected() {
      return Err("Failed connecting to replica for checkpoint send".to_string());
    }

    // 预锁截断线（对标 C# AcquireCheckpointEntryAsync :303 于快照下发前先
    // TryAddReplicationDriver 钉线）：以检查点覆盖下界注册驱动，令传送+恢复
    // 往返窗口内 FastAofTruncate 无从越过覆盖位；成功后保留在册，由
    // initiate_replica_sync 第 4 步 attach_replica_wire 以授予位点二次更新置换
    //（对标 C# startAofSync :199 二次 TryAddReplicationDriver）
    let pin_start = entry.get_min_aof_covered_address(0);
    let pin_driver = Arc::new(AofSyncDriver::new(
      local_node_id,
      replica_meta.origin_node_id,
      self.rm.sublog_count(),
      &pin_start,
      None,
    ));
    self
      .rm
      .aof_sync_driver_store
      .try_remove(replica_meta.origin_node_id);
    if !self
      .rm
      .aof_sync_driver_store
      .try_add_replication_driver(pin_driver, provider.allow_data_loss())
    {
      return Err("Failed to pin replication driver before checkpoint transfer".to_string());
    }

    let res = self
      .transmit_checkpoint(provider, &client, &checkpoint_dir, &entry, replica_meta)
      .await;
    client.dispose();
    // 对标 C# finally localEntry?.RemoveReader()：快照已被副本接收并恢复，
    // 释放读者后下一轮淘汰方可回收
    reader.release();
    // 传送/恢复失败即退钉（对标 C# catch :213 TryRemove(aofSyncDriver)）；
    // 成功则保留钉线，交由后续 attach 以 sync_start 更新置换，杜绝窗口空档
    if res.is_err() {
      self
        .rm
        .aof_sync_driver_store
        .try_remove(replica_meta.origin_node_id);
    }
    res
  }

  /// 快照流发送 + 副本恢复位点回收（传输与往返的错误统一收敛）
  async fn transmit_checkpoint(
    &self,
    provider: &Arc<ClusterProvider>,
    client: &GarnetClient,
    checkpoint_dir: &Path,
    entry: &CheckpointEntry,
    replica_meta: &SyncMetadata,
  ) -> Result<Option<AofAddress>, String> {
    // 快照逐帧应答限时取帧级 REPLICA_SYNC_TIMEOUT(5s)——C#
    // ReplicaSyncSession.cs:140 new SnapshotTransmissionDriver(gcs,
    // ReplicaSyncTimeout, logger)（FileTransmitSource.cs 逐块
    // WaitAsync(同值)）；cluster_node_timeout 专留节点失联判定，不挪用
    let timeout = Some(REPLICA_SYNC_TIMEOUT);
    let device = Arc::clone(
      &provider
        .try_store()
        .ok_or_else(|| "store not wired".to_string())?
        .device,
    );
    let sources = SnapshotTransmitSources {
      device,
      checkpoint_dir: Arc::from(checkpoint_dir),
    };
    send_store_checkpoint(client, &sources, entry, timeout).await?;

    // BEGIN_REPLICA_RECOVER：快照覆盖区间作副本 AOF 对齐基准（replayAOFMap
    // 恒 0——rust AOF 直推架构，见 replica_diskbased_sync 模块文档）
    let covered = entry.get_min_aof_covered_address(0).get(0).unwrap_or(0);
    let entry_bytes = entry.to_byte_array();
    let begin_span = covered.to_le_bytes().to_vec();
    // BEGIN_REPLICA_RECOVER 应答取同款帧级限时（C#
    // DiskbasedReplication/ReplicaSyncSession.cs:181-184
    // ExecuteClusterBeginReplicaRecover(...).WaitAsync(ReplicaSyncTimeout)）
    let resp = wait_async(
      Some(REPLICA_SYNC_TIMEOUT),
      client.begin_replica_recover_async(
        true,
        0,
        &self.rm.primary_repl_id(),
        &entry_bytes,
        &begin_span,
        &begin_span,
      ),
    )
    .await
    .ok_or_else(|| "begin replica recover timed out".to_string())?
    .map_err(|e| e.to_string())?;

    // 副本恢复位点（C# AofAddress.FromString(resp)）
    let sync_from = AofAddress::from_string(&resp)
      .ok_or_else(|| format!("invalid replication offset from replica: {resp}"))?;

    // DataLossCheck（C# SendCheckpointAsync 尾段：副本请求位点不得低于
    // 快照覆盖起点，防静默丢数据；比对维度与协商一致取装配子日志数）。
    // 可能丢数据标志对标 C# possibleAofDataLoss（SendCheckpointAsync 与
    // clusterProvider.AllowDataLoss 同值一处，均出自
    // GarnetServerOptions.cs:653-654 的唯一派生式）
    let covered_addr = AofAddress::create(self.rm.sublog_count() as i32, covered);
    self
      .rm
      .data_loss_check(provider.allow_data_loss(), &sync_from, &covered_addr)?;
    log::info!(
      "Replica {replica} recovered from checkpoint, sync from {}",
      sync_from.to_aof_string(),
      replica = hex_str_u128(replica_meta.origin_node_id),
    );
    Ok(Some(sync_from))
  }
}

#[cfg(test)]
mod tests {
  use std::{fs::create_dir_all, sync::Arc};

  use compio::runtime::Runtime;
  use wnode::database::{GarnetDatabase, SingleDatabaseManager};

  use super::*;
  use crate::server::{replication::checkpoint_entry::CheckpointMetadata, worker::NodeRole};

  /// 副本协商元数据（采集循环判定载荷；错误路径不触达传输段）
  fn replica_meta() -> SyncMetadata {
    SyncMetadata {
      full_sync: true,
      origin_node_role: NodeRole::Replica,
      origin_node_id: 0x0000_0000_0000_0000_0000_0000_0002_E70E,

      current_primary_repl_id: String::new(),
      current_store_version: -1,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 0),
      current_replication_offset: AofAddress::create(1, 0),
      checkpoint_entry: None,
    }
  }

  #[test]
  fn odc_empty_store_advance_truncated_rejects() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let provider = ClusterProvider::new();
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm.clone());

      // 空库 + 截断线前移：幻影覆盖起点 0 落后截断线触发重拍；
      // database_manager 缺席 → 重拍确定性失败，混尽且未允许丢数据拒绝 attach
      rm.aof_sync_driver_store
        .update_truncated_until(&AofAddress::create(1, 100));
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res
          .as_ref()
          .is_err_and(|e| e.contains("possible data loss")),
        "混尽且未允许丢数据应拒绝 attach: {res:?}"
      );
      Ok(())
    })
  }

  #[test]
  fn odc_disabled_skips_reshoot_entirely() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let provider = ClusterProvider::new();
      // 关掉按需检查点（--on-demand-checkpoint false 的装配形态）：判据命中也
      // 不重拍，直接落回 skip 直推（对标 C# ReplicaSyncSession.cs:280 的短路）
      provider.set_on_demand_checkpoint(false);
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm.clone());

      rm.aof_sync_driver_store
        .update_truncated_until(&AofAddress::create(1, 100));
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res.as_ref().is_ok_and(|o| o.is_none()),
        "关按需检查点即跳过重拍落回 skip 直推: {res:?}"
      );
      Ok(())
    })
  }

  #[test]
  fn odc_invalid_metadata_entry_triggers_reshoot() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let provider = ClusterProvider::new();
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm.clone());

      // 在册条目元数据无效（store_version==-1）：!validMetadata 恒触发重拍，
      // database_manager 缺席混尽后拒绝 attach
      rm.checkpoint_store
        .write()
        .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res
          .as_ref()
          .is_err_and(|e| e.contains("possible data loss")),
        "无效元数据应触发重拍并拒绝 attach: {res:?}"
      );
      Ok(())
    })
  }

  #[test]
  fn odc_exhausted_proceeds_when_data_loss_allowed() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let provider = ClusterProvider::new();
      // 允许丢数据形态 = C# 派生式命中（FastAofTruncate 且关按需检查点），
      // 无直配写口，经两输入装配（对标 GarnetServerOptions.cs:653-654）
      provider.set_fast_aof_truncate(true);
      provider.set_on_demand_checkpoint(false);
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm.clone());

      // 无效条目 + 截断线前移双触发面，混尽后允许丢数据 → 放行直推（skip 下发）
      rm.checkpoint_store
        .write()
        .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
      rm.aof_sync_driver_store
        .update_truncated_until(&AofAddress::create(1, 100));
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res.as_ref().is_ok_and(|o| o.is_none()),
        "允许丢数据应放行并落回 skip 直推: {res:?}"
      );
      Ok(())
    })
  }

  #[test]
  fn fresh_primary_empty_store_skips_checkpoint_send() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let provider = ClusterProvider::new();
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm);

      // 全新主端（空库 + 截断线 0）：幻影覆盖起点不落后，不触发重拍直推
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res.as_ref().is_ok_and(|o| o.is_none()),
        "全新主端不应触发重拍: {res:?}"
      );
      Ok(())
    })
  }

  #[test]
  fn odc_reshoot_success_flows_into_send_segment() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let (store_dir, store) = wtest_base::open_test_store("odc_reshoot")?;
      let cp_dir = store_dir.path().join("checkpoints");
      create_dir_all(&cp_dir).unwrap();
      let db = Arc::new(GarnetDatabase::new(
        0,
        Arc::clone(&store),
        Arc::clone(&store.device),
        cp_dir.clone(),
        None,
      ));
      let provider = ClusterProvider::new();
      provider.set_checkpoint_dir(cp_dir.clone());
      provider.set_database_manager(Arc::new(SingleDatabaseManager::new(cp_dir, db)));
      let rm = provider.replication_manager().unwrap();
      let session = ReplicaSyncSession::new(rm.clone());

      // 无效条目触发重拍：真身拍摄成功登记有效条目，采集循环放行进入发送段
      rm.checkpoint_store
        .write()
        .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
      // 副本端点不可达：错误来自发送段建连而非采集循环（重拍已放行）
      let res = session
        .send_checkpoint_and_recover(&provider, "127.0.0.1:1", &replica_meta(), 0)
        .await;
      assert!(
        res
          .as_ref()
          .is_err_and(|e| e.contains("Failed connecting to replica for checkpoint send")),
        "重拍成功后应推进到发送段建连: {res:?}"
      );
      let latest = rm
        .checkpoint_store
        .read()
        .latest_entry()
        .expect("重拍应登记检查点条目");
      assert_ne!(latest.metadata.store_hlog_token, 0);
      assert_ne!(latest.metadata.store_version, -1);
      Ok(())
    })
  }
}
