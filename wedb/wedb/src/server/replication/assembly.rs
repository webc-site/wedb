//! 复制数据面生产装配（主端推流面 + 副本接收面 + 副本重连发起钩子）
//!
//! 对标 C# 装配形态：
//! - 主端推流面：C# ReplicationManager 构造期经 clusterProvider.storeWrapper
//!   反查 appendOnlyFile 建立 AofSyncDriverStore 与推流任务；Rust 依赖方向
//!   反转，由本模块 [`wire_replication_data_plane`] 在宿主装配期正向注入
//!   [`PrimaryReplicationAssets`]（wal + 推流泵 + 副本同步会话）。
//! - 副本接收面：C# 会话侧 replicaReplaySession 可达面；Rust 经
//!   `set_replica_replication_session` 注入 [`ClusterReplicationSession`]，
//!   CLUSTER APPENDLOG 记录帧经网络会话直达落盘重放。
//! - 副本同步发起：C# 四处驱动点各按 `ReplicaDisklessSync` 开关在
//!   TryReplicateDisklessSyncAsync / TryReplicateDiskbasedSyncAsync 之间三元
//!   选路；rust 收敛为唯一选路口 [`try_replicate_sync_async`]——diskbased 支
//!   即 C# ReplicationManager.RecoverReplication →
//!   TryReplicateDiskbasedSyncAsync（libs/cluster/Server/Replication/
//!   libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:ReplicaSyncAttachTaskAsync）：
//!   副本清空本地重放状态后向主端发 CLUSTER INITIATE_REPLICA_SYNC
//!   （5 参：节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail），
//!   主端 +OK 后回连副本建立 APPENDLOG 推流；本模块
//!   [`recover_replication`] 承接同一 attach 体（ensure_replication 断链时
//!   后台任务直调驱动、REPLICAOF 与 CLUSTER REPLICATE 命令臂当场发起并按
//!   失败回 -ERR；开关点亮时同一选路改由副本主动 ATTACH_SYNC 发起端
//!   [`super::replica_diskless_sync::try_replicate_diskless_sync_async`] 承接）。

use std::{future::Future, sync::Arc};

use compio::runtime::spawn;
use waof::WalLog;
use wbase::{hex::hex_str_u128, supervise::supervise_item};
use wdev::SegmentedDevice;
use wnode::aof::aof_processor::ReplicaCheckpointHook;

use super::{
  aof_replication_pump::AofReplicationPump,
  checkpoint_entry::CheckpointEntry,
  cluster_replication_session::ClusterReplicationSession,
  error::{EPOCH_DRAIN_UNSETTLED, ReplicationError},
  recovery_status::RecoveryStatus,
  replica_diskless_sync::try_replicate_diskless_sync_async,
  replica_replay_task,
  replica_sync_session::ReplicaSyncSession,
  replicate_sync_options::ReplicateSyncOptions,
};
use crate::{
  client::{GarnetClient, apply_tls},
  error::{RespScope, cluster_err_text},
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    wait_async,
  },
};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const REPLICA_SYNC_TASK: &str = "replica_sync";
/// 同上（attach 未来体逐次监督）
const REPLICA_ATTACH_TASK: &str = "replica_attach";

/// attach 未来体的监督运行（单点）：panic 转显式 Err 文案上送，后续
/// [`finish_replica_sync`] 的 finally 臂照常执行——对标 C# 无 panic 语义下
/// 异常必经 catch → finally 的收尾必达，杜绝恢复锁/角色复位臂被 panic 跳过
async fn supervised_attach(
  attach: impl Future<Output = Result<(), ReplicationError>>,
) -> Result<(), ReplicationError> {
  match supervise_item(REPLICA_ATTACH_TASK, attach).await {
    Ok(result) => result,
    Err(p) => Err(ReplicationError::Panic {
      stage: "attach",
      text: p.text().to_string(),
    }),
  }
}

/// attach 收尾的 finally 释放段（单点）：恢复锁由 try_add_replica_async
///（或启动臂的 InitializeRecover 前置）握到收尾，此处一次做全——
/// upgrade_lock 臂降回 ReadRole（外层驱动点统一 AllowRoleChange 收尾），
/// 其余臂释放到 NoRecovery。panic 臂经监督补跑同段，语义与 C#
/// ReplicaDisklessSync.cs:185-194 / ReplicaDiskbasedSync.cs:197-208 的
/// finally EndRecovery 对齐
pub(crate) fn release_attach_recovery(provider: &Arc<ClusterProvider>, upgrade_lock: bool) {
  if let Some(rm) = provider.replication_manager() {
    if upgrade_lock {
      rm.end_recovery(RecoveryStatus::ReadRole, true);
    } else {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }
  }
}

/// 复制数据面装配（宿主启动路径与集成测试共用的唯一装配体；AOF 门控
/// 点亮时调用一次）
///
/// 副本接收会话的 replay 端口传 None：记录帧重放推进走重放驱动仓库
///（init 帧握手注册驱动 + 背景重放任务应用回推），与会话断链处置
///（dispose 释放驱动仓库）同源同寿
pub fn wire_replication_data_plane(
  cluster: &Arc<ClusterProvider>,
  wal: Arc<WalLog<SegmentedDevice>>,
) {
  let Some(rm) = cluster.replication_manager() else {
    return;
  };
  // 副本重放应用资产注入（对标 C# rm 构造期 aofProcessor + storeWrapper
  // 反查装配）：aof + store 双在场才建（对标 C# EnableAOF 门控）；缺席为
  // 退化装配，副本位点保持会话落盘面 enqueued 形态。runtime_config 随资产
  // 注入（对标 C# storeWrapper.runtimeConfig 可达面：重放空转周期每轮现取）；
  // 检查点钩子经数据库管理器下达 take_checkpoint（对标 C# storeWrapper
  // .TakeCheckpointAsync 反查可达面，rust 依赖方向反转，装配期以类型擦除闭包
  // 注入 AofProcessor 的检查点结束臂），管理器现取现用（attach 期注入、
  // 缺位维持原语义）。take_checkpoint 为收口入口：占闸失败即 false（C#
  // AofProcessor.cs:307 `_ = BlockingWait(...)` 丢弃返回值同位，该轮跳过、
  // 后续结束标记再补拍）+ finally 还闸，与 SAVE/BGSAVE、AOF 限长任务、
  // 集群按需重拍同闸互斥
  match (cluster.try_aof(), cluster.try_store()) {
    (Some(aof), Some(store)) => {
      let cp = Arc::clone(cluster);
      let hook: Arc<ReplicaCheckpointHook> = Arc::new(move || {
        let cp = Arc::clone(&cp);
        Box::pin(async move {
          match cp.try_database_manager() {
            Some(dm) => dm.take_checkpoint(false).await.map(|_| ()),
            None => Ok(()),
          }
        })
      });
      rm.set_replay_assets(Some(Arc::new(replica_replay_task::ReplayAssets::new(
        aof,
        store,
        cluster.try_runtime_config(),
        Some(hook),
      ))));
    }
    _ => rm.set_replay_assets(None),
  };
  // 副本接收面：CLUSTER APPENDLOG → 保真落盘 + 背景重放应用位点回推
  cluster.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(cluster),
    Arc::clone(&wal),
    None,
  ))));
  // 主端推流面：策略协商 + 建连 + 补扫的发起资产。空闲防抖窗口周期在
  // 循环内每轮现取 replica-sync-delay 槽位（CONFIG SET 即时生效）
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(
    &rm.aof_sync_driver_store,
  )));
  pump.start_throttle_loop(cluster.try_runtime_config());
  cluster.set_primary_replication(Some(Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump,
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  })));
  // 本地日志句柄（副本重连发起的 begin/tail 位点源）
  cluster.set_wal(wal);
}

/// 单槽位点序列化为 span 字节（C# `AofAddress.Span` 形态：8B LE 裸字节，无长度头）
pub const fn aof_span_array(address: i64) -> [u8; 8] {
  address.to_le_bytes()
}

pub fn aof_span(address: i64) -> Vec<u8> {
  aof_span_array(address).to_vec()
}

/// 副本重连发起动作（diskbased attach 体；对标 C#
/// ReplicationManager.RecoverReplication → TryReplicateDiskbasedSyncAsync 的
/// ReplicaSyncAttachTaskAsync 发起段）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:ReplicaSyncAttachTaskAsync
///
/// 流程（对标 C# ReplicaSyncAttachTaskAsync 发起段）：
/// 1. 清空本地重放驱动仓库（重注册由主端 init 帧握手完成；此处预注册会令
///    IsReplicating 状态面误报「流活跃」而中断 ensure_replication 静默重试）；
/// 2. 构造 5 参（节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail；
///    检查点条目读盘取数——对标 C# :164 GetLatestCheckpointEntryFromDisk，
///    重启副本内存仓为空也可凭盘上快照上报（主端判同检查点历史走
///    PartialResync），无盘上快照时上报空条目（C# 空库语义））；
/// 3. 专用客户端向主端发起 CLUSTER INITIATE_REPLICA_SYNC，cluster_timeout
///    级超时（failover 治理同款，杜绝无限挂起）；
/// 4. 应答面：成功仅记日志（数据面由主端回连异步建立）；失败 / 超时回 Err
///    交驱动点处置——ensure_replication 重连臂记告警后按轮询节流重试（对标
///    C# RecoverReplication 失败后按轮询节奏重试），命令臂据此回错误应答
///    （C# 同一发起体的失败即 REPLICAOF / CLUSTER REPLICATE 的 -ERR 文案）
pub async fn recover_replication(
  provider: &Arc<ClusterProvider>,
  primary: u128,
) -> Result<(), ReplicationError> {
  let Some(rm) = provider.replication_manager() else {
    return Err(ReplicationError::NotInitialized("replication manager"));
  };
  // 1. 清空本地重放驱动仓库 + 检查点接收状态（对标 C# 每次 attach
  //    new ReceiveCheckpointHandler 的置换语义：本轮残留活跃槽与接收闸门
  //    一并净态，主端从头分块重推顺序覆盖上轮半写；未恢复的半写事实升级
  //    到 provider 管理面屏障，见 ClusterProvider::reset_recv_checkpoint_handler）
  //    + 清残留主端推流驱动
  //    （对标 C# 相邻序列 aofSyncDriverStore.Reset——"Remove aofSync tasks
  //    if this node was a primary"；断链重连时本节点可能刚被 gossip 翻回
  //    主角色残留驱动，shipped watermark 残留会钉死背压闸门）
  rm.reset_replica_replay_driver_store();
  provider.reset_recv_checkpoint_handler();
  rm.aof_sync_driver_store.reset();
  // C# 该序列后另有 replicationOffset.SetValue(0)
  // （ReplicaDiskbasedSync.cs:125），其前提是同函数 :146 storeWrapper.Reset()
  // 抹掉本地数据；rust attach 期无清库臂，且该字段已作 applied 位点权威源被两臂
  // 读取（replica_diskbased_sync.rs / replica_diskless_sync.rs），清零即令部分
  // 重同步自 0 重放整库 wal 或误判 granted < applied 而强制全量重灌，故有意不复位
  // （次序同理：C# 先连后复位 :116→:120/:123/:125，rust 先复位后建连，因 rust
  // 无 attach 期清库需要保护）。禁按「对齐 C#」名义回摆。

  // 2. 主端 endpoint 与本端节点 id（集群配置反查）
  let Some(cm) = provider.cluster_manager() else {
    return Err(ReplicationError::NotInitialized("cluster manager"));
  };
  let (address, port) = cm.current_config().get_local_node_primary_address();
  let Some(node_id) = cm.current_config().local_node_id().filter(|id| *id != 0) else {
    return Err(ReplicationError::Sync(format!(
      "replication recovery to {primary} skipped: local node id unknown"
    )));
  };
  let Some(address) = address else {
    return Err(ReplicationError::Sync(format!(
      "replication recovery to {primary} skipped: primary endpoint unknown"
    )));
  };

  // 3. 发起参数束（检查点条目读盘取数，对标 C# ReplicaDiskbasedSync.cs:164
  //    GetLatestCheckpointEntryFromDisk：重启副本内存仓为空但盘上快照在，
  //    凭盘上条目上报主端可判同检查点历史走 PartialResync；无盘上快照上报
  //    store_version=-1 空条目，C# ToByteArray 空库语义）
  let checkpoint_entry = rm
    .get_latest_checkpoint_entry_from_disk()
    .map(|entry| entry.to_byte_array())
    .unwrap_or_else(|| CheckpointEntry::with_sublogs(rm.sublog_count()).to_byte_array());
  let Some(wal) = provider.try_wal() else {
    return Err(ReplicationError::Sync(format!(
      "replication recovery to {primary} skipped: local wal not wired"
    )));
  };
  let aof_begin = aof_span_array(wal.begin_address() as i64);
  let aof_tail = aof_span_array(wal.tail_address() as i64);
  drop(wal);

  // 4. 专用客户端发起（用后即弃，对标 C# gcs 构造 / finally Dispose；
  //    复制网络缓冲池同池注入 + 出站 TLS 单源透传，对标 C#
  //    ReplicaDiskbasedSync.cs:111-112 gcs 构造的
  //    replicationManager.GetNetworkPool 与
  //    tlsOptions: serverOptions.TlsOptions?.TlsClientOptions 双形参）
  let mut client = GarnetClient::with_auth(
    format!("{address}:{port}"),
    provider.cluster_username(),
    provider.cluster_password(),
    Some("TryReplicateDiskbasedSyncAsync".to_string()),
  );
  client.set_network_pool(Some(rm.network_pool()));
  apply_tls!(client, provider);
  if let Err(e) = client.connect_async().await {
    log::warn!("Failed to connect to primary at {address}:{port}: {e}");
  }
  let initiated = client.is_connected();
  let res = if initiated {
    // 应答限时取 attach 级 repl_attach_timeout（优先读 runtime_config）——C#
    // ReplicaDiskbasedSync.cs:182-186 对 ExecuteClusterInitiateReplicaSync
    // 以 WaitAsync(GetTimeSpan(REPL_ATTACH_TIMEOUT)) 限时（回填源
    // ReplicaAttachTimeout，GarnetServerOptions.cs:425），与节点失联判定
    // 的 cluster_node_timeout 无涉
    wait_async(
      provider.repl_attach_timeout(),
      // 协议帧参数：节点 id 仅在命令面渲染 hex
      client.initiate_replica_sync_async(
        &hex_str_u128(node_id),
        &rm.primary_repl_id(),
        &checkpoint_entry,
        &aof_begin,
        &aof_tail,
      ),
    )
    .await
    .ok_or(ReplicationError::Timeout("timed out"))
  } else {
    Err(ReplicationError::Sync("not connected".to_string()))
  };
  client.dispose();

  // 5. 应答面：成功仅记日志（数据面由主端回连异步建立）；失败 / 超时回 Err
  //    交驱动点处置——轮询臂记告警后按节流重试、命令臂据此回 -ERR 文案
  //    两失败臂的差别只在尾缀原因（对端 -ERR 的 Error / 超时未连接的串），
  //    发起动作同名 INITIATE_REPLICA_SYNC，故归一原因后共用一处措辞、不留
  //    第二套模板（C# 同一 catch 把 ex.Message 原样作应答，
  //    libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:188-195）
  let reason = match res {
    Ok(Ok(_)) => {
      log::info!("Replica sync initiated to {primary}");
      return Ok(());
    }
    Ok(Err(e)) => e.to_string(),
    Err(msg) => msg.to_string(),
  };
  Err(ReplicationError::Sync(format!(
    "Failed to initiate replica sync to {primary}: {reason}"
  )))
}

/// 副本同步发起骨架（C# `TryReplicateDisklessSyncAsync` 与
/// `TryReplicateDiskbasedSyncAsync` 两支同形的公共前后段：登记副本 →
/// 一致性管理器换代 → 纪元等待 → attach 发起（Background 臂即发即忘）→
/// catch / finally 收尾；两支仅 attach 体不同，由驱动点作为未来对象传入。
/// 两支的 1:1 对标锚点在两个入口函数上，本函数不占锚位）
///
/// 任务体经 wbase [`supervise_item`] 顶层监督（单点一次成型）：前置臂
///（TryAddReplica / 换代 / 纪元等待）与 attach 发起间任何 panic 转 Err 并补跑
/// [`finish_replica_sync`]（catch 复位 + finally 释放段；未握锁起点 NoRecovery
/// 被 end_recovery 状态矩阵判非法拒绝，仅 error 留痕 no-op）——对标 C# 异常
/// 必经 catch → finally 的收尾必达
pub(crate) async fn replicate_sync_async<F>(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
  attach: F,
) -> Result<(), ReplicationError>
where
  // 仅 'static：本仓 compio 运行时为线程本地驱动器（spawn 无 Send 约束），
  // 而 attach 体内的 GarnetClient 建连未来非 Send，加 Send 界即断链
  F: Future<Output = Result<(), ReplicationError>> + 'static,
{
  let provider = Arc::clone(provider);
  let panic_arm_provider = Arc::clone(&provider);
  let outcome = supervise_item(REPLICA_SYNC_TASK, async move {
    // 1. TryAddReplica 臂（对标 C# TryAddReplicaAsync(options.NodeId,
    //    options.Force, options.UpgradeLock)；失败即原样上抛其错误文案，
    //    C# `return (false, error)` 同口径；本臂不消费时不取句柄，避免
    //    TryAddReplica:false 的调用形态被无关前置挡住）
    if opts.try_add_replica {
      let cm = provider
        .cluster_manager()
        .ok_or_else(|| ReplicationError::NotInitialized("cluster manager"))?;
      cm.try_add_replica_async(opts.node_id, opts.force, opts.upgrade_lock)
        .await
        // cluster_err_text 产出 RESP 应答文案（C# ex.Message 作应答同口径），
        // 本链错误统一承载，文案经 Display 原样透传
        .map_err(|e| ReplicationError::Sync(cluster_err_text(e, RespScope::WorkerReplica)))?;
    }

    // 2. 一致性管理器换代（C# storeWrapper.appendOnlyFile
    //    .CreateOrUpdateKeySequenceManager()，ReplicaDiskbasedSync.cs:50 /
    //    ReplicaDisklessSync.cs:44 挂接入口同位：TryAddReplica 臂后、纪元
    //    等待前）：改挂新主即整体换代，草图/前沿归零、旧管理器栅栏禁用，
    //    旧主代际高值不再经 fetch_max 残留污染副本读闸（假新鲜陈旧读 /
    //    持续假 ConsistentReadTimeout）。aof 句柄经 provider 单源取；
    //    MultiLogEnabled 内部门控使单日志形态恒 no-op
    if let Some(aof) = provider.try_aof() {
      aof.create_or_update_key_sequence_manager();
    }

    // 3. 纪元推进等待（C# session.UnsafeBumpAndWaitForEpochTransitionAsync；
    //    rust 无会话线程模型，统一经 provider 面推进）。返值必承判：C# 无限
    //    自旋恒真（ClusterProvider.cs:366-389 while(true)+goto retry 直至全部
    //    ActiveClusterSessions 追平），rust 已以 cluster_node_timeout() 有界化，
    //    false = 全会话批静止未达成（追平判定只枚举 cluster_sessions 弱引用表、
    //    批外快照 0 放行，见 cluster_provider/checkpoint.rs:54-66/:90-111）。
    //    未达成即照常 attach，则滞留会话 bump 前的在途写批与破坏性段赛跑——
    //    attach 期无 C# storeWrapper.Reset 清库臂（本文件 recover_replication
    //    :186-193 明文禁回摆、replicationOffset 有意不清零），实序为「检查点
    //    导入 + swap_online_store 引擎置换 + AOF 代际衔接」：置换只约束置换后
    //    写向、不回收置换前已 ACK 落旧引擎的写（ACK 写丢失），或落新主代际与
    //    主端全序分叉（副本漂移无自愈）。与本原语已收口的 failover.rs:171 判败
    //    帧、r25 迁移族同原语同害同判（承判先例以本码内 failover 判败帧为据，
    //    「有精确逆件判败 / 无逆件管理臂 warn 留痕」族分界已落册 deviations §182；
    //    禁另起第二套判败机制）。
    //    判败一律经 finish_replica_sync 构造返回——严禁裸 return Err：本臂跑在
    //    supervise_item 任务内，直返上抛会绕过 catch 臂（allow_replica_reset_on_failure
    //    时 try_reset_replica + resume_primary_tasks）与 finally 臂
    //    （release_attach_recovery 释放恢复锁）；三个前台驱动点与重连臂经
    //    try_add_replica_async 握 ClusterReplicate 恢复锁、启动臂经 begin_recovery
    //    (InitializeRecover) 握锁——裸 Err 即恢复锁永持、角色已翻 REPLICA 却不复位，
    //    后续一切恢复/重连被 ERR_RECOVERY_LOCK 拒死无自愈通路。此形制恰为 C#
    //    「该 await 抛异常 → catch(TryResetReplica) → finally(EndRecovery)」的 rust 同构收口。
    if !provider.bump_and_wait_for_epoch_transition_async().await {
      return finish_replica_sync(
        &provider,
        opts,
        Err(ReplicationError::Sync(format!(
          "Failed to initiate replica sync: {EPOCH_DRAIN_UNSETTLED}"
        ))),
      );
    }

    // 4. attach 发起：Background 臂即发即忘、错误在体内记日志（对标 C#
    //    `_ = TryBeginReplicaSyncAsync(forceAsync: true)` 后仍回 (true, default)）；
    //    前台臂回传结果供驱动点写 OK / -ERR 应答。attach 体一律经
    //    [`supervised_attach`] 监督运行，panic 转 Err 保证 finally 收尾必达
    if opts.background {
      let provider = Arc::clone(&provider);
      spawn(async move {
        let result = supervised_attach(attach).await;
        if let Err(e) = finish_replica_sync(&provider, opts, result) {
          log::warn!(
            "Background replica sync to {} failed: {e}",
            hex_str_u128(opts.node_id)
          );
        }
      })
      .detach();
      return Ok(());
    }
    let result = supervised_attach(attach).await;
    finish_replica_sync(&provider, opts, result)
  })
  .await;
  match outcome {
    Ok(result) => result,
    Err(p) => finish_replica_sync(
      &panic_arm_provider,
      opts,
      Err(ReplicationError::Panic {
        stage: "sync",
        text: p.text().to_string(),
      }),
    ),
  }
}

/// attach 收尾（C# attach 体内 catch / finally 两支的合并形态）
fn finish_replica_sync(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
  result: Result<(), ReplicationError>,
) -> Result<(), ReplicationError> {
  // catch 臂（C# catch：AllowReplicaResetOnFailure 时把本节点复位为主）
  if result.is_err()
    && opts.allow_replica_reset_on_failure
    && let Some(cm) = provider.cluster_manager()
  {
    cm.try_reset_replica();
    // 回滚自愈（C# 失败臂同病此处补齐）：try_add_replica_async 的挂起轴
    //（周期任务角色位 + store GC 扫描）随角色复位一并恢复——否则回滚后的
    // 主节点以 PRIMARY 继续接写而 GC/周期任务全停，无日志无指标暴露且
    // ensure_replication 重连仅在副本态触发，无任何自愈通路。resume 内
    // 角色守卫（仅配置角色主才恢复）为副本态回滚臂（不经此处）双保险
    provider.resume_primary_tasks();
  }

  // finally 臂（C# ReplicaDiskbasedSync.cs:197-208 / ReplicaDisklessSync.cs:
  // 185-194 的 finally 对偶）：锁释放统一收敛在 [`release_attach_recovery`]
  // 单点——upgrade_lock 臂降回 ReadRole（外层驱动点统一 AllowRoleChange
  // 收尾），其余臂释放到 NoRecovery；panic 臂经监督补跑同段。不变式：走到
  // 本收尾的调用必持锁（三个驱动点——重连臂 / REPLICAOF / CLUSTER REPLICATE
  // 经 try_add_replica_async 握 ClusterReplicate、启动臂经
  // start_replication_attach 前置握 InitializeRecover），故释放无需再按
  // 入口分支；若新增驱动点，必须在进入 attach 前先握锁，否则此处释放会落
  // 在 NoRecovery 起点被状态矩阵判非法
  release_attach_recovery(provider, opts.upgrade_lock);
  result
}

/// 副本磁盘基同步发起（登记副本 + 纪元等待后向主端发
/// CLUSTER INITIATE_REPLICA_SYNC，attach 体即 [`recover_replication`]）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicateDiskbasedSyncAsync
pub async fn try_replicate_diskbased_sync_async(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
) -> Result<(), ReplicationError> {
  let primary = opts.node_id;
  let attach_provider = Arc::clone(provider);
  replicate_sync_async(provider, opts, async move {
    recover_replication(&attach_provider, primary).await
  })
  .await
}

/// 副本同步发起的唯一选路口（对标 C# 四处驱动点各自的
/// `ReplicaDisklessSync ? TryReplicateDisklessSyncAsync :
/// TryReplicateDiskbasedSyncAsync` 三元：rust 全仓只在本函数读一次开关
/// [`ClusterProvider::replica_diskless_sync`]，驱动点一律经此发起，
/// 杜绝散读配置与第二套发起路径）
pub async fn try_replicate_sync_async(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
) -> Result<(), ReplicationError> {
  if provider.replica_diskless_sync() {
    try_replicate_diskless_sync_async(provider, opts).await
  } else {
    try_replicate_diskbased_sync_async(provider, opts).await
  }
}
