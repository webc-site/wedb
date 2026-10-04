//! 主端副本同步会话（INITIATE_REPLICA_SYNC 服务面与副本 attach 链）
//!
//! 对标 C# ReplicaSyncSession（libs/cluster/Server/Replication/PrimaryOps/
//! DiskbasedReplication/ReplicaSyncSession.cs）——副本发起同步请求后，
//! 主端协商同步策略、建立副本发送通道（AofSyncDriver + wire）、挂推流
//! 泵并补扫存量积压。检查点快照下发段由 [`super::snapshot_transmission`] 承接
//! （SNAPSHOT_DATA 段流 + BEGIN_REPLICA_RECOVER 往返）。对标 C#
//! AcquireCheckpointEntryAsync 先于快照下发 TryAddReplicationDriver 钉住
//! 截断线（见 send_checkpoint_and_recover 预锁段），传送+恢复完成后由本
//! 会话承接 startAofSync 尾段：以授予位点二次 TryAddReplicationDriver 更新
//! + TryConnectToReplica（AofSyncDriver.RunAsync → connect + APPENDLOG_INIT
//! + 迭代泵）。

use std::{path::Path, sync::Arc, time::Duration};

use waof::{AofAddress, WalLog};
use wbase::{future::yield_now, hex::hex_str_u128};
use wdev::SegmentedDevice;

use crate::{
  client::{GarnetClient, apply_tls},
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    replication::{
      aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
      aof_sync_task::TimePulseSource,
      checkpoint_entry::CheckpointEntry,
      checkpoint_store::read_meta_aligned_begin,
      error::{ConnectStage, ReplicationError},
      replica_wire::{AofSyncWire, TcpSessionWire},
      replication_manager::{ReplicationManager, ResyncStrategy},
      snapshot_transmission::{SnapshotTransmitSources, send_store_checkpoint},
      sync_metadata::SyncMetadata,
    },
    wait_async,
  },
};

/// 主端 → 副本推流建链单点（diskbased / diskless 两支共用）：TcpSessionWire
/// 建连（connect 即建连 + APPENDLOG init 握手等 +OK，副本会话对 init 帧注册
/// 重放驱动，握手成功即副本接收面就绪）；凭证与建连限时取 provider 活值
/// （限时源 RuntimeServerOptions.replica_sync_timeout_secs）、复制网络
/// 缓冲池同池注入（对标 C# AofSyncTask.cs:134 replicationManager.GetNetworkPool
/// 形参）、出站 TLS 单源透传统一收口。`stage` 承接两支既有错误文案前缀
///（有测试逐字断言，不可归一）
pub(crate) async fn connect_replica_stream_wire(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
  local_node_id: u128,
  replica_endpoint: &str,
  stage: ConnectStage,
) -> Result<Arc<TcpSessionWire>, ReplicationError> {
  TcpSessionWire::connect(
    replica_endpoint,
    local_node_id,
    0,
    (
      provider.cluster_username().as_deref(),
      provider.cluster_password().as_deref(),
    ),
    rm.network_pool(),
    provider.replica_sync_timeout(),
    // 出站 TLS 单源透传（配置源注释见 TcpSessionWire::connect）
    #[cfg(feature = "tls")]
    provider.try_cluster_tls_client().as_ref(),
  )
  .await
  .map_err(|e| ReplicationError::Connect {
    stage,
    detail: e.to_string(),
  })
}

/// 时间脉冲源组装单点（diskbased / diskless 两支共用）：CLUSTER ADVANCE_TIME
/// 心跳帧的 tail 位点、序列号读取源与每轮实时读的节流频率配置面；运行时配置
/// 未装配即 None，与 C# timePulseEnabled=false 同形态整体静默
pub(crate) fn replica_pulse_source(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
) -> Option<Arc<TimePulseSource>> {
  provider
    .try_aof()
    .zip(provider.try_runtime_config())
    .map(|(aof, runtime_config)| {
      Arc::new(TimePulseSource {
        aof,
        backpressure: rm.aof_sync_driver_store.backpressure(),
        runtime_config,
      })
    })
}

/// 驱动构造入册 + wire 接线（主端推流建链公共核，diskbased / diskless 两支
/// 共用；[`ReplicaSyncSession::attach_replica_wire`] 与
/// [`establish_replica_stream`] 均转发至此）：同 node_id 原子置换旧驱动
///（write 锁内原地覆盖并 dispose 旧实例，消除先摘后挂窗口），TryAdd 失败即
/// 截断拒绝；脉冲源随驱动构造一次注入
fn attach_stream_driver(
  rm: &Arc<ReplicationManager>,
  local_node_id: u128,
  remote_node_id: u128,
  wire: impl Into<AofSyncWire>,
  start_address: &AofAddress,
  pulse_source: Option<Arc<TimePulseSource>>,
  allow_data_loss: bool,
) -> bool {
  let driver = Arc::new(AofSyncDriver::new(
    local_node_id,
    remote_node_id,
    rm.sublog_count(),
    start_address,
    pulse_source,
  ));
  if !rm
    .aof_sync_driver_store
    .try_add_replication_driver(driver.clone(), allow_data_loss)
  {
    return false;
  }
  driver.attach_wire(wire);
  true
}

/// 主端推流建链公共尾段（diskbased / diskless 两支共用）：时间脉冲源组装 +
/// 驱动入册（write 锁内原地置换）+ wire 接线；带损放行语义经
/// `provider.allow_data_loss()` 单点承接。协议差异（diskless 的 ATTACH_SYNC
/// 恢复握手与 DataLossCheck）由调用支在建连后、入册前自行编排
pub(crate) fn establish_replica_stream(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
  local_node_id: u128,
  remote_node_id: u128,
  wire: impl Into<AofSyncWire>,
  sync_from: &AofAddress,
  add_err: &str,
) -> Result<(), ReplicationError> {
  if attach_stream_driver(
    rm,
    local_node_id,
    remote_node_id,
    wire,
    sync_from,
    replica_pulse_source(provider, rm),
    provider.allow_data_loss(),
  ) {
    Ok(())
  } else {
    Err(ReplicationError::Sync(add_err.to_string()))
  }
}

/// 主端 → 副本停等往返专用客户端构造 + 建连（用后即弃，对标 C# gcs 构造 /
/// finally Dispose）：凭证 + 复制网络池同池注入 + 出站 TLS 单源透传，与推流
/// 建链段同款装配；`why` 承接调用点错误文案（有测试逐字断言，不可归一）
async fn egress_client(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
  replica_endpoint: &str,
  stage: ConnectStage,
) -> Result<GarnetClient, ReplicationError> {
  let mut client = GarnetClient::with_auth(
    replica_endpoint.to_string(),
    provider.cluster_username(),
    provider.cluster_password(),
    Some("SendCheckpointAsync".to_string()),
  );
  client.set_network_pool(Some(rm.network_pool()));
  apply_tls!(client, provider);
  // 建连限时（对标 C# ConnectAsync(...).WaitAsync(ReplicaSyncTimeout)）：静默
  // 对端不得无限等待，超时按 Connect 阶段错误上收
  let connect_res = wait_async(provider.replica_sync_timeout(), client.connect_async()).await;
  match connect_res {
    Some(Ok(())) => {}
    Some(Err(e)) => {
      let err_msg = ReplicationError::Connect {
        stage,
        detail: e.to_string(),
      };
      log::warn!("{err_msg}");
      return Err(err_msg);
    }
    None => {
      let err_msg = ReplicationError::Connect {
        stage,
        detail: "connect timed out (replica_sync_timeout)".to_string(),
      };
      log::warn!("{err_msg}");
      return Err(err_msg);
    }
  }
  Ok(client)
}

struct RecoverRoundtripArgs<'a> {
  timeout: Option<Duration>,
  recover_store_from_token: bool,
  replay_aof_mask: u64,
  primary_repl_id: &'a str,
  checkpoint_entry: &'a [u8],
  aof_begin: &'a [u8],
  aof_tail: &'a [u8],
}

/// BEGIN_REPLICA_RECOVER 停等往返单点（`timeout` 由调用方取
/// RuntimeServerOptions.replica_sync_timeout_secs 活值下传，无限哨兵即 None
/// 不挂计时器，C#
/// ReplicaSyncSession.cs:181-184 ExecuteClusterBeginReplicaRecover
/// (...).WaitAsync(ReplicaSyncTimeout) 对位；客户端 Dispose 由调用方统一收口）
async fn recover_roundtrip(
  client: &GarnetClient,
  args: RecoverRoundtripArgs<'_>,
) -> Result<String, ReplicationError> {
  Ok(
    wait_async(
      args.timeout,
      client.begin_replica_recover_async(
        args.recover_store_from_token,
        args.replay_aof_mask,
        args.primary_repl_id,
        args.checkpoint_entry,
        args.aof_begin,
        args.aof_tail,
      ),
    )
    .await
    .ok_or_else(|| ReplicationError::Timeout("begin replica recover timed out"))??,
  )
}

/// 主端副本同步会话
pub struct ReplicaSyncSession {
  rm: Arc<ReplicationManager>,
}

/// PartialResync 钳制往返的授予参数（negotiate_resync 产出的回放掩码与
/// AOF 接续位点；参数束收敛免长形参列）
#[doc(hidden)]
pub struct ClampGrant {
  pub replay_aof_mask: u64,
  pub sync_start: AofAddress,
}

/// 传输期读者守卫（对标 C# CheckpointStore.cs:196-210 active-reader 钳制）：
/// 持 tail 条目读者使淘汰链 TrySuspendReaders 失败而停手，快照文件不被
/// unlink（C# AcquireCheckpointEntryAsync 持读者 + finally
/// localEntry.RemoveReader 的 try/finally 三段形状）；Drop 兜底出错路径，
/// transmit 返回前显式释放。
///
/// 同一守卫承担 C# 同一条 reader 计数的两面：条目读者计数挡 wcpr token 文件
/// 淘汰，reader_pin 聚合水位钳制主端活设备历史段截断（检查点发布 step 10
/// 的 release_history_until 抬地板经 effective_delete_floor 与本水位取 min）。
/// 注册 = 取到条目读者后读 wcpr meta 得扇区对齐 begin 并经
/// [`ReplicationManager::register_snapshot_reader`] 锁内重算聚合写水位；
/// 释放/Drop 注销并回抬水位。begin 走 meta 异步读，故 replace 为 async——
/// 注册必发生于入场 take 时点，不得推迟到 send_store_checkpoint 起念
///（其间地板已可能越过条目 begin，ms 级窗口即事故窗口）。
pub struct SendReaderGuard {
  provider: Arc<ClusterProvider>,
  rm: Arc<ReplicationManager>,
  entry: Option<Arc<CheckpointEntry>>,
  /// 在册读钉 (store_hlog_token, 扇区对齐 begin)；None = 未注册
  pin: Option<(u128, u64)>,
  /// 水位前向写通道（注册时从当期 store 派生并类型擦除：本层不点名 whlog
  /// 类型，且钉死"注销写在注册对象上"——会话中途 store 置换不误写新引擎）
  pin_writer: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl SendReaderGuard {
  pub fn new(provider: &Arc<ClusterProvider>, rm: &Arc<ReplicationManager>) -> Self {
    Self {
      provider: Arc::clone(provider),
      rm: Arc::clone(rm),
      entry: None,
      pin: None,
      pin_writer: None,
    }
  }

  pub fn get(&self) -> Option<&CheckpointEntry> {
    self.entry.as_deref()
  }

  /// 本会话在册读钉 begin（未注册 None）——供注册后地板复核（被超退圈）
  pub fn pinned_begin(&self) -> Option<u64> {
    self.pin.map(|(_, begin)| begin)
  }

  /// 释放旧读者（含读钉注销）并持有新条目；条目有效时在读者计数单点同位
  /// 注册快照读钉
  pub async fn replace(&mut self, entry: Option<Arc<CheckpointEntry>>) {
    self.release();
    let Some(entry) = entry else {
      return;
    };
    self.entry = Some(Arc::clone(&entry));
    // token==0 = 无 hlog 快照可发（发送侧同判据跳过），无需钉
    let token = entry.metadata.store_hlog_token;
    if token == 0 {
      return;
    }
    // 读钉 begin 与 send_store_checkpoint 的读取起址同源：wcpr meta 文件
    // decode 后扇区下对齐（不改 wire 格式、不进 CheckpointMetadata——条目
    // 按 bitcode 线格式上线（deviations.md §203，rust-internal 复制协议），
    // 字段增减即破本仓复制双端自洽契约，与 C# 线布局无关）
    let Some(store) = self.provider.try_store() else {
      return;
    };
    let Some(dir) = self.provider.try_checkpoint_dir() else {
      return;
    };
    let sector = store.device.sector_size() as u64;
    let Ok((_meta, begin)) = read_meta_aligned_begin(&dir, token, sector).await else {
      log::warn!("快照读钉注册跳过（meta 读取/解码失败）");
      return;
    };
    // 锁内重算聚合并经回调前向写水位（注册即含本会话，写序与在册变更序一致）
    let hlog = Arc::clone(store.hlog());
    let writer: Arc<dyn Fn(u64) + Send + Sync> = Arc::new(move |agg| hlog.set_reader_pin(agg));
    let w = Arc::clone(&writer);
    self
      .rm
      .register_snapshot_reader(token, begin, move |agg| w(agg));
    self.pin = Some((token, begin));
    self.pin_writer = Some(writer);
  }

  /// 同步释放：注销读钉（锁内重算聚合并前向写回水位）后退条目读者。
  /// 返回是否曾持有在册读钉（供调用方判定滞后补删）
  pub fn release(&mut self) -> bool {
    let mut was_pinned = false;
    if let (Some((token, _)), Some(writer)) = (self.pin.take(), self.pin_writer.take()) {
      was_pinned = true;
      self
        .rm
        .unregister_snapshot_reader(token, move |agg| writer(agg));
    }
    if let Some(entry) = self.entry.take() {
      entry.remove_reader();
    }
    was_pinned
  }
}

impl Drop for SendReaderGuard {
  fn drop(&mut self) {
    // Drop 臂只保证同步注销回抬水位（Drop 非 async 不发设备补删）；滞后
    // 补删由正常退场的显式 release + replay_history_release 承接，异常
    // 退场则交下一轮检查点发布/移位紧缩按新地板补收——与 C# 周期模型
    //（DeleteOutdatedCheckpoints 随下轮发布补收）同一收敛节奏
    self.release();
  }
}

/// 预锁截断线钉线守卫（钉线注册 → 在役驱动置换全程持有；对标 C#
/// AcquireCheckpointEntryAsync :303 钉线 + SendCheckpointAsync catch 块
/// :205-216 `TryRemove(aofSyncDriver)` 的取消臂承接）：磁盘链编排执行体
/// future 在快照传送窗 / 钳制往返窗 / 建连窗内被丢弃（副本断连 → 网络泵
/// 慢臂 RaceEnd::Disposed，future drop 即取消）时，Drop 臂按节点 id 退钉
/// 出册——与 initiate_replica_sync 尾段及 send_checkpoint_and_recover /
/// begin_replica_recover_clamp 失败臂的既有退钉同一单点通道。钉线驱动无
/// wire、is_connected 恒真，throttle_all 周期臂永不收割，不出册即
/// previous_address / shipped 水位恒钉预锁位点，safe_truncate_aof 与背压
/// 闸门被永久钳制。
///
/// 正常臂 disarm：成功时预锁已由 attach_replica_wire 以授予位点原地置换为
/// 在役推流驱动（守卫若触发即误杀在役流），失败时既有失败臂已先行退钉，
/// 守卫退场均为零操作。退钉（registry remove + dispose + 背压闸门重报）
/// 全为同步方法，Drop 内安全。
pub struct PinTruncationGuard<'a> {
  store: &'a AofSyncDriverStore,
  node: u128,
  disarmed: bool,
}

impl<'a> PinTruncationGuard<'a> {
  /// 持守卫（生产挂点为
  /// [`ReplicaSyncSession::initiate_replica_sync`](ReplicaSyncSession::initiate_replica_sync)
  /// 入口，本构造口供测试直构）
  pub fn new(store: &'a AofSyncDriverStore, node: u128) -> Self {
    Self {
      store,
      node,
      disarmed: false,
    }
  }

  /// 正常收尾解除守卫（退钉交还既有显式失败臂/置换链，防双重清理）
  pub fn disarm(&mut self) {
    self.disarmed = true;
  }
}

impl Drop for PinTruncationGuard<'_> {
  fn drop(&mut self) {
    if !self.disarmed {
      self.store.try_remove(self.node);
    }
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
  /// TryConnectToReplica 的组合（wire 已由装配方建立）；同 node_id 原地原子置换
  /// 旧驱动（对标 C# TryAddReplicationDriver 在 write 锁内原地覆盖并 dispose 旧驱动，
  /// 消除先摘后挂窗口）；带损放行语义由 allow_data_loss 透传承接；
  /// 脉冲源随驱动构造一次注入（对标 C# AofSyncDriver 构造器逐子日志捕获 appendOnlyFile/backpressure）
  pub fn attach_replica_wire(
    &self,
    local_node_id: u128,
    remote_node_id: u128,
    wire: impl Into<AofSyncWire>,
    start_address: &AofAddress,
    pulse_source: Option<Arc<TimePulseSource>>,
    allow_data_loss: bool,
  ) -> bool {
    attach_stream_driver(
      &self.rm,
      local_node_id,
      remote_node_id,
      wire,
      start_address,
      pulse_source,
      allow_data_loss,
    )
  }

  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync
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
  ) -> Result<AofAddress, ReplicationError> {
    // 预锁钉线守卫（入口 → 尾段退场全程持有，取消安全退钉见
    // PinTruncationGuard 文档）：钉线实际注册于下层 send_checkpoint_and_recover
    // / begin_replica_recover_clamp，正常臂（成功置换 / 失败臂退钉）退场前
    // 一律 disarm，行为零变化；仅 future 取消臂经 Drop 触发退钉。函数体收进
    // async 块使所有正常早退（含 `?`）统一经过 disarm 收敛点
    let mut pin_guard =
      PinTruncationGuard::new(&self.rm.aof_sync_driver_store, replica_meta.origin_node_id);
    let res = async {
      let PrimaryReplicationAssets { wal, .. } = assets;
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
      //    beginReplicaRecover 段）；PartialResync 轻量钳制往返——C#
      //    SendCheckpointAsync 在快照下发段之后无条件执行
      //    ExecuteClusterBeginReplicaRecover 往返，其部分重同步形态
      //   （skipLocalMainStoreCheckpoint=true）不发快照帧、以
      //    recoverStoreFromToken=false + replayAOFMap 掩码照常往返：副本把本地
      //    wal「应用位点~授予位点」残留段补应用进存储并对齐授予位点后回传位点，
      //    主端以回传位点挂推流驱动（C# 源注自陈 "start streaming from that
      //    address in order not to introduce duplicate insertions"），与无盘臂
      //    ATTACH_SYNC 恢复握手同形收口。快照缺席（本地检查点缺席/目录未接线）
      //    维持既有 AOF 直推形态。
      match &strategy {
        ResyncStrategy::FullResync { .. } => {
          if let Some(granted) = self
            .send_checkpoint_and_recover(
              provider,
              wal,
              replica_endpoint,
              replica_meta,
              local_node_id,
            )
            .await?
          {
            sync_start = granted;
          }
        }
        ResyncStrategy::PartialResync {
          replay_aof_mask, ..
        } => {
          sync_start = self
            .begin_replica_recover_clamp(
              provider,
              wal,
              replica_endpoint,
              replica_meta,
              local_node_id,
              ClampGrant {
                replay_aof_mask: *replay_aof_mask,
                sync_start,
              },
            )
            .await?;
        }
      }

      // 3~5. 建连 → 入库接线 → 推流补扫（#region startAofSync）。失败统一退钉
      //      出册（对标 C# catch 块 :205-216 的
      //      `if (aofSyncDriver != null) TryRemove(aofSyncDriver)` 异常清理契约）：
      //      钉线驱动在册期间建连失败 / 挂接拒绝 / 补扫失败若不出册，幽灵驱动
      //      零推流进度、previous_address 恒钉预锁位点，safe_truncate_aof 与
      //      背压闸门将被永久钳制（AOF 删段失效 + 写背压闭锁）
      let res = self
        .start_aof_sync(
          provider,
          assets,
          local_node_id,
          replica_endpoint,
          replica_meta,
          sync_start,
        )
        .await;
      if res.is_err() {
        self
          .rm
          .aof_sync_driver_store
          .try_remove(replica_meta.origin_node_id);
      }
      res
    }
    .await;
    pin_guard.disarm();
    res
  }

  /// TCP 通道建连 + 驱动入库接线 + 推流泵挂载与存量补扫（C# SendCheckpointAsync
  /// 的 #region startAofSync：以授予位点二次 TryAddReplicationDriver 置换钉线
  /// 驱动 + TryConnectToReplica 建连挂泵 + 存量补扫；错误统一上抛，退钉由
  /// 调用方 catch 对位段收敛）
  async fn start_aof_sync(
    &self,
    provider: &Arc<ClusterProvider>,
    assets: &PrimaryReplicationAssets,
    local_node_id: u128,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
    sync_start: AofAddress,
  ) -> Result<AofAddress, ReplicationError> {
    let PrimaryReplicationAssets { wal, pump, .. } = assets;

    // 3. 建立副本 TCP 发送通道（connect_replica_stream_wire 单点：凭证 +
    //    复制网络池 + 出站 TLS 统一收口；connect 即建连 + APPENDLOG init
    //    握手等 +OK，副本会话对 init 帧注册重放驱动，握手成功即副本接收面就绪）
    let wire = connect_replica_stream_wire(
      provider,
      &self.rm,
      local_node_id,
      replica_endpoint,
      ConnectStage::AofSync,
    )
    .await?;

    // 4. 时间脉冲源组装 + 驱动入库接线（TryAdd 失败即截断拒绝，对标 C#
    //    拒绝语义；脉冲源随驱动构造一次注入，对标 C# AofSyncTask 构造期
    //    捕获 appendOnlyFile / backpressure / clusterProvider——CLUSTER
    //    ADVANCE_TIME 心跳帧的 tail 位点、序列号读取源与每轮实时读的节流频率
    //    配置面；运行时配置未装配即无脉冲源，与 C# timePulseEnabled=false
    //    同形态整体静默）
    if !self.attach_replica_wire(
      local_node_id,
      replica_meta.origin_node_id,
      wire,
      &sync_start,
      replica_pulse_source(provider, &self.rm),
      provider.allow_data_loss(),
    ) {
      return Err(ReplicationError::Sync(
        "Failed trying to try update replication task".to_string(),
      ));
    }

    // 5. 推流泵挂载（入队信号唤醒增量拉取）+ 存量补扫（attach 前的记录）
    pump.attach_wake(wal);
    let (forwarded, skipped) = pump
      .sync_backlog(wal)
      .await
      .map_err(|e| ReplicationError::Sync(format!("AOF backlog sync failed: {e}")))?;
    log::info!(
      "Replica {} aof sync attached from {:?}: backlog forwarded {forwarded}, skipped {skipped}",
      hex_str_u128(replica_meta.origin_node_id),
      sync_start
    );

    Ok(sync_start)
  }

  /// PartialResync 的 BEGIN_REPLICA_RECOVER 轻量钳制往返（C# SendCheckpointAsync
  /// 的 startAofSync region 部分重同步形态：跳过快照下发，恢复往返照常执行）
  ///
  /// 对位件为上述 C# 下发件的 PartialResync 子段（同名整体对位锚留
  /// [`Self::initiate_replica_sync`] 一处）
  ///
  /// 以本地检查点覆盖 begin 为 begin、协商授予位点（replay_until =
  /// min(副本上报尾, 主端已提交) 经 repl_offset2 钳制，negotiate_resync 产出）
  /// 为 tail，发 BEGIN_REPLICA_RECOVER（recoverStoreFromToken=false +
  /// replayAOFMap 掩码）；副本侧 [`super::replica_diskbased_sync::
  /// try_replica_diskbased_recovery`] 真消费掩码——把本地 wal 残留段
  /// 「应用位点~授予位点」经既有记录应用链补应用进存储、safe_initialize
  /// 对齐授予位点（C# ReplayAOF + Log.Initialize），回传其复制位点。主端
  /// 以副本回传位点（非本地协商值）挂流，DataLossCheck 与全量臂同点同基线
  /// （当下活体日志起点）。
  #[doc(hidden)]
  pub async fn begin_replica_recover_clamp(
    &self,
    provider: &Arc<ClusterProvider>,
    wal: &WalLog<SegmentedDevice>,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
    local_node_id: u128,
    grant: ClampGrant,
  ) -> Result<AofAddress, ReplicationError> {
    let ClampGrant {
      replay_aof_mask,
      sync_start,
    } = grant;
    let sublog_count = self.rm.sublog_count() as i32;
    // 本地检查点覆盖 begin 与条目字节（C# beginAddress =
    // localEntry.GetMinAofCoveredAddress()、localEntry.ToByteArray()；
    // 无本地检查点的纯 AOF 历史形态取主端日志起点 + 空条目）
    let latest_entry = self.rm.checkpoint_store.read().latest_entry();
    let pin_start = latest_entry
      .as_ref()
      .map(|e| e.get_min_aof_covered_address(0))
      .unwrap_or_else(|| AofAddress::create(sublog_count, wal.begin_address() as i64));
    let begin_span = pin_start.get(0).unwrap_or(0).to_le_bytes();
    let entry_bytes = latest_entry
      .as_ref()
      .map(|e| e.to_byte_array())
      .unwrap_or_else(|| CheckpointEntry::with_sublogs(self.rm.sublog_count()).to_byte_array());
    let tail_span = sync_start.get(0).unwrap_or(0).to_le_bytes();

    // 预锁截断线（对标 C# AcquireCheckpointEntryAsync :303 于 SendCheckpointAsync
    // 钳制往返前先 TryAddReplicationDriver 钉线）：以检查点覆盖下界（无检查点
    // 时取 wal begin，与 begin_span 同源）注册驱动，令钳制往返全程受 safe_truncate_aof
    // 以该钉线取小钳制；预锁若被 start_gate_ok 拒绝（fast 豁免窗内 trunc_floor 已越覆盖位）
    // 即降级 FullResync 走 send_checkpoint_and_recover；成功后保留在册，由
    // initiate_replica_sync 尾段 attach_replica_wire 以授予位点二次更新置换
    //（同 node_id 原地置换，消除空档）
    if !self.pin_truncation_line(
      local_node_id,
      replica_meta.origin_node_id,
      &pin_start,
      provider.allow_data_loss(),
    ) {
      log::warn!(
        "Replica {} recover clamp pre-lock rejected, degrading to FullResync",
        hex_str_u128(replica_meta.origin_node_id)
      );
      let full_start = self
        .send_checkpoint_and_recover(provider, wal, replica_endpoint, replica_meta, local_node_id)
        .await?;
      return Ok(full_start.unwrap_or(sync_start));
    }

    // 专用客户端向副本下发（用后即弃，对标 C# gcs 构造 / finally Dispose；
    // egress_client 单点装配：凭证 + 复制网络缓冲池同池注入、出站 TLS 单源
    // 透传，与快照下发段同款）
    let roundtrip_res: Result<AofAddress, ReplicationError> = async {
      let client = egress_client(
        provider,
        &self.rm,
        replica_endpoint,
        ConnectStage::RecoverRoundtrip,
      )
      .await?;
      // BEGIN_REPLICA_RECOVER 往返（recover_roundtrip 单点：帧级限时；所有
      // 臂 Dispose 收口必达，对标 C# finally gcs.Dispose）
      let resp = recover_roundtrip(
        &client,
        RecoverRoundtripArgs {
          timeout: provider.replica_sync_timeout(),
          recover_store_from_token: false,
          replay_aof_mask,
          primary_repl_id: &self.rm.primary_repl_id(),
          checkpoint_entry: &entry_bytes,
          aof_begin: &begin_span,
          aof_tail: &tail_span,
        },
      )
      .await;
      client.dispose();
      let resp = resp?;

      // 副本恢复位点（C# AofAddress.FromString(resp)）
      let sync_from = AofAddress::from_string(&resp)
        .ok_or_else(|| ReplicationError::InvalidReplicaOffset(resp.clone()))?;

      // DataLossCheck 兜底（与全量臂同点同基线：当下活体 Log.BeginAddress）：
      // 副本回传位点低于主端活体日志起点即推流无法连续衔接——默认拒绝建流，
      // allow_data_loss（派生式命中）放行仅告警
      self
        .rm
        .data_loss_check_vs_live_begin(provider.allow_data_loss(), &sync_from, wal)?;
      Ok(sync_from)
    }
    .await;

    // 往返失败/超时臂退钉（对齐全量臂 send_checkpoint_and_recover 失败退钉
    // 与 initiate_replica_sync 失败退钉口径，对标 C# catch 块 TryRemove）；
    // 成功则保留预锁驱动在册，交由后续 attach 以授予位点更新置换
    if roundtrip_res.is_err() {
      self
        .rm
        .aof_sync_driver_store
        .try_remove(replica_meta.origin_node_id);
    }
    let sync_from = roundtrip_res?;

    log::info!(
      "Replica {replica} recover clamp completed, sync from {}",
      sync_from.to_aof_string(),
      replica = hex_str_u128(replica_meta.origin_node_id),
    );
    Ok(sync_from)
  }

  /// 预锁截断线钉线单点（对标 C# AcquireCheckpointEntryAsync :303 于快照下发 /
  /// 钳制往返前先 TryAddReplicationDriver 钉线）：以给定覆盖下界构造驱动注册
  /// 入册（同 node_id 原地置换旧驱动，消除先摘后挂空档），allow_data_loss
  /// 透传派生式放行语义；返回 false = 预锁被拒（fast 豁免窗内 trunc_floor 已
  /// 越覆盖位），处置归调用方——钳制臂降级 FullResync、快照臂返错
  fn pin_truncation_line(
    &self,
    local_node_id: u128,
    replica_node_id: u128,
    pin_start: &AofAddress,
    allow_data_loss: bool,
  ) -> bool {
    let pin_driver = Arc::new(AofSyncDriver::new(
      local_node_id,
      replica_node_id,
      self.rm.sublog_count(),
      pin_start,
      None,
    ));
    self
      .rm
      .aof_sync_driver_store
      .try_add_replication_driver(pin_driver, allow_data_loss)
  }

  /// 检查点下发 + BEGIN_REPLICA_RECOVER 往返（C# SendCheckpointAsync 的
  /// sendStoresSnapshotData / startAofSync 前段：AcquireCheckpointEntry 的
  /// 最新检查点获取、快照流、副本恢复位点回收与 DataLossCheck）
  ///
  /// 返回 None = skipLocalMainStoreCheckpoint（本地检查点缺席或目录未接线，
  /// 或按需重拍混尽后允许丢数据，维持 AOF 直推；C# 同名判定的对应形态）
  ///
  /// [`doc(hidden)`] 测试专用隐藏面：按需检查点重拍门集成测直驱口
  ///（wedb/tests/replica_sync_odc_reshoot_gate.rs），生产调用点为
  /// INITIATE_REPLICA_SYNC 会话链单点，非公共 API 契约
  #[doc(hidden)]
  pub async fn send_checkpoint_and_recover(
    &self,
    provider: &Arc<ClusterProvider>,
    wal: &WalLog<SegmentedDevice>,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
    local_node_id: u128,
  ) -> Result<Option<AofAddress>, ReplicationError> {
    // AcquireCheckpointEntryAsync 的最新检查点获取与按需检查点拍摄（OnDemandCheckpoint）：
    // 覆盖起点落后截断线或元数据无效即触发 take_on_demand_checkpoint 重拍
    let mut num_odc_attempts = 0;
    const MAX_ODC_ATTEMPTS: usize = 2;
    let sublog_count = self.rm.sublog_count() as i32;

    let mut reader = SendReaderGuard::new(provider, &self.rm);

    let entry = loop {
      // 对标 C# ReplicaSyncSession.cs:251 取 lastSaveTime 快照
      let last_save_time = provider.last_save_ms();
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
      // 读者计数与读钉注册同单点（async：begin 走 meta 异步读，注册即含本
      // 会话，晚于入场 take 的任何地板抬升都会见到本方钉或被本方复核捕获）
      reader.replace(raw).await;

      let truncated_until = self.rm.aof_sync_driver_store.get_truncated_until();
      // 注册后地板复核（对标 C# reader 钳制覆盖一切截断、rust 端口 meta 读
      // 与 store-pin 之间存在 ms 级窗口的补检）：地板已越过本条目 begin =
      // 注册前完成过一轮 raise→truncate，条目段可能已被 unlink，与 needs_odc
      // 同路退圈重拍。两侧同取扇区下对齐口径——发布链 raise 写入的原始
      // begin 可含段内零头（floor=64 vs 读者对齐位点 0），零头不算越过，
      // 否则未对齐库上判据恒真、重拍混尽误断同步
      let overtaken = reader.pinned_begin().is_some_and(|b| {
        provider.try_store().is_some_and(|s| {
          let sector = s.device.sector_size() as u64;
          s.hlog().delete_floor() / sector * sector > b
        })
      });
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
            || overtaken
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
            return Err(ReplicationError::CheckpointAcquire {
              attempts: num_odc_attempts,
            });
          }
        }
        // 对标 C# RemoveReader 先于 TakeOnDemandCheckpoint：先释放读者，
        // 重拍登记触发的淘汰链才能推进
        reader.release();
        num_odc_attempts += 1;
        log::info!("Taking on-demand checkpoint, attempt {num_odc_attempts}.");
        match provider.take_on_demand_checkpoint(last_save_time).await {
          Ok(true) => yield_now().await,
          Ok(false) => log::warn!("On-demand checkpoint skipped (in progress or already taken)"),
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
    // egress_client 单点装配：凭证 + 复制网络缓冲池同池注入，对标 C#
    // ReplicaSyncSession.cs:99 SendCheckpointAsync gcs 构造的
    // replicationManager.GetNetworkPool 形参 + 出站 TLS 单源透传）
    let client = egress_client(
      provider,
      &self.rm,
      replica_endpoint,
      ConnectStage::CheckpointSend,
    )
    .await?;
    if !client.is_connected() {
      return Err(ReplicationError::ConnectNotReady(
        ConnectStage::CheckpointSend,
      ));
    }

    // 预锁截断线（对标 C# AcquireCheckpointEntryAsync :303 于快照下发前先
    // TryAddReplicationDriver 钉线）：以检查点覆盖下界注册驱动，令传送+恢复
    // 往返窗口内 FastAofTruncate 无从越过覆盖位；同 node_id 原地覆盖置换旧驱动，
    // 消除先 try_remove 后 try_add 的非原子空档；成功后保留在册，由
    // initiate_replica_sync 第 4 步 attach_replica_wire 以授予位点二次更新置换
    //（对标 C# startAofSync :199 二次 TryAddReplicationDriver）
    let pin_start = entry.get_min_aof_covered_address(0);
    if !self.pin_truncation_line(
      local_node_id,
      replica_meta.origin_node_id,
      &pin_start,
      provider.allow_data_loss(),
    ) {
      return Err(ReplicationError::Sync(
        "Failed to pin replication driver before checkpoint transfer".to_string(),
      ));
    }

    let res = self
      .transmit_checkpoint(
        provider,
        wal,
        &client,
        &checkpoint_dir,
        &entry,
        replica_meta,
      )
      .await;
    client.dispose();
    // 对标 C# finally localEntry?.RemoveReader()：快照已被副本接收并恢复，
    // 释放读者后下一轮淘汰方可回收；快照读钉同点注销，被钉钳制的历史段
    // 由滞后补删补收
    let was_pinned = reader.release();
    // 滞后补删（读钉钳制的回收臂，对标 C# 周期模型里下轮
    // DeleteOutdatedCheckpoints 补收职责的 rust 主动补放）：注销后沿同一
    // release_history_until 单点删段通道补放被本会话封顶的地板——目标取
    // 最新在册 store 条目 begin（聚合水位若仍被其他在途会话钳制，raise
    // 内部以当前 pin 封顶，天然不越他人依赖段）；失败仅告警，下一轮检查点
    // 发布/移位紧缩按新地板补收，水位收敛不放弃
    if was_pinned {
      self.replay_history_release(provider).await;
    }
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

  /// 读钉注销后的滞后补删：以最新在册 store 条目的扇区对齐 begin 转调
  /// whlog `HybridLog::release_history_until`（全系统删段单一通道，不开第二
  /// 入口）。begin 读法与快照下发同源（wcpr meta 文件 decode + 扇区下对齐）
  async fn replay_history_release(&self, provider: &Arc<ClusterProvider>) {
    let Some(store) = provider.try_store() else {
      return;
    };
    let Some(dir) = provider.try_checkpoint_dir() else {
      return;
    };
    let Some(entry) = self.rm.checkpoint_store.read().latest_entry() else {
      return;
    };
    let token = entry.metadata.store_hlog_token;
    if token == 0 {
      return;
    }
    let sector = store.device.sector_size() as u64;
    let Ok((_meta, begin)) = read_meta_aligned_begin(&dir, token, sector).await else {
      log::warn!("滞后补删跳过（meta 读取/解码失败）");
      return;
    };
    if let Err(e) = store.hlog().release_history_until(begin).await {
      log::warn!("快照读钉注销后的滞后补删失败（下一轮发布/紧缩补收）: {e}");
    }
  }

  /// 快照流发送 + 副本恢复位点回收（传输与往返的错误统一收敛）
  async fn transmit_checkpoint(
    &self,
    provider: &Arc<ClusterProvider>,
    wal: &WalLog<SegmentedDevice>,
    client: &GarnetClient,
    checkpoint_dir: &Path,
    entry: &CheckpointEntry,
    replica_meta: &SyncMetadata,
  ) -> Result<Option<AofAddress>, ReplicationError> {
    // 快照逐帧应答限时取 RuntimeServerOptions.replica_sync_timeout_secs 活值
    // ——C# ReplicaSyncSession.cs:140 new SnapshotTransmissionDriver(gcs,
    // ReplicaSyncTimeout, logger)（FileTransmitSource.cs 逐块
    // WaitAsync(同值)）；无限哨兵折 None 不挂计时器；cluster_node_timeout
    // 专留节点失联判定，不挪用
    let timeout = provider.replica_sync_timeout();
    let device = Arc::clone(
      &provider
        .try_store()
        .ok_or_else(|| ReplicationError::NotWired("store"))?
        .device,
    );
    let sources = SnapshotTransmitSources {
      device,
      checkpoint_dir: Arc::from(checkpoint_dir),
    };
    send_store_checkpoint(client, &sources, entry, timeout).await?;

    // BEGIN_REPLICA_RECOVER：快照覆盖区间作副本 AOF 对齐基准（replayAOFMap
    // 恒 0——rust AOF 直推架构，见 replica_diskbased_sync 模块文档；
    // recover_roundtrip 单点：帧级限时，Dispose 由调用方传输完成后统一收口）
    let covered = entry.get_min_aof_covered_address(0).get(0).unwrap_or(0);
    let entry_bytes = entry.to_byte_array();
    let begin_span = covered.to_le_bytes();
    let resp = recover_roundtrip(
      client,
      RecoverRoundtripArgs {
        timeout: provider.replica_sync_timeout(),
        recover_store_from_token: true,
        replay_aof_mask: 0,
        primary_repl_id: &self.rm.primary_repl_id(),
        checkpoint_entry: &entry_bytes,
        aof_begin: &begin_span,
        aof_tail: &begin_span,
      },
    )
    .await?;

    // 副本恢复位点（C# AofAddress.FromString(resp)）
    let sync_from = AofAddress::from_string(&resp)
      .ok_or_else(|| ReplicationError::InvalidReplicaOffset(resp.clone()))?;

    // DataLossCheck 兜底（对标 C# SendCheckpointAsync 尾段 :187-191
    // appendOnlyFile.DataLossCheck 同位语义，比对基准为当下活体 Log.BeginAddress，
    // 与无盘臂同一基线单点；非快照覆盖起点覆盖下界）：
    // 副本请求位点低于主端活体日志起点即快照流期间发生 AOF 截断删段，推流无法连续
    // 衔接——默认拒绝建流，allow_data_loss（派生式命中）放行仅告警
    self
      .rm
      .data_loss_check_vs_live_begin(provider.allow_data_loss(), &sync_from, wal)?;
    log::info!(
      "Replica {replica} recovered from checkpoint, sync from {}",
      sync_from.to_aof_string(),
      replica = hex_str_u128(replica_meta.origin_node_id),
    );
    Ok(Some(sync_from))
  }
}
