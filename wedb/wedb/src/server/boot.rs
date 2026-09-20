//! 集群服务端运行与生命周期组装

use std::{io, path::Path, sync::Arc};

use waof::AofEntryType;
use wconf::ServerArgs;
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;
use wnode::{
  ClusterProviderHandle, Error, RespSessionConsumer, Result, ServerBootstrap, ShutdownCoordinator,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};

use crate::{
  ClusterArgs,
  server::{
    announce,
    cluster::IClusterProvider,
    cluster_provider::ClusterProvider,
    replication::{StoreCommitFn, wire_replication_data_plane},
  },
};

/// 运行 WeDB 分布式集群节点服务
pub fn run_cluster_server(
  args: ClusterArgs,
  coordinator: Option<ShutdownCoordinator>,
) -> Result<()> {
  let node = args.node_args();
  let network_connection_limit = i64::from(node.network_connection_limit);

  // 采样节拍 / 延迟监视 / 逐命令统计 / TLS 由 ServerBootstrap::run_async 一处
  // 从 NodeArgs 投影（对标 C# StoreWrapper.cs:226-227 消费侧直读 options），
  // 本宿主零逐字段装配样板
  let mut bootstrap = ServerBootstrap::new(args)
    .with_cluster_provider(ClusterProvider::new())
    // 连接上限装配（C# GarnetServer.cs:294 opts.NetworkConnectionLimit 传入
    // GarnetServerTcp；-1 = 不限）
    .network_connection_limit(network_connection_limit)
    .banner("WeDB 分布式集群节点");

  if let Some(coord) = coordinator {
    bootstrap = bootstrap.with_shutdown_coordinator(coord);
  }

  bootstrap.run_async(|args, cluster| async move {
    let node = args.node_args();
    // 会话参数基线（NodeArgs → 会话选项映射收口 wnode 单点
    // `RespServerSessionOptions::from`，与单机同一份）
    let session_options = RespServerSessionOptions::from(node);
    let session_factory = {
      let cluster = Arc::clone(&cluster);
      move |network_sender_id, api: StoreGarnetApi<_>| {
        let provider_handle: ClusterProviderHandle = cluster.clone();
        Some(RespSessionConsumer::with_cluster(
          network_sender_id,
          session_options.clone(),
          cluster.create_cluster_session(),
          provider_handle,
          Arc::new(api),
        ))
      }
    };
    let data_path = node.data_path();
    // --recover 与 AOF 分派及运行时选项装配收口 wnode 基座（对标 C# Options.cs:139
    // Recover → StoreWrapper.RecoverAsync 集群分支；恢复在端点 accept 之前完成）
    let provider = StorageSessionProvider::open_from_args(node, data_path, session_factory).await?;
    // 运行时配置投影（复制域装配族共用一次投影：物理子日志数 / 重连轮询
    // 频率 / FastAofTruncate 同源读取）
    let runtime_options = node.runtime_server_options();
    // 复制域物理子日志数装配期强制校验（对标 C# AofSyncTask 构造期逐子日志
    // 自带扫描源 `physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx)`，
    // libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:120；
    // rust 推流泵数据源为单条 WalLog（aof_replication_pump::pump_backlog 签名），
    // 多子日志推流泵未实现——配置 >1 时驱动按 N 任务建、泵只喂 0 号任务，
    // 1..N 号子日志水位/截断线恒钉死、同步静默缺流；配置与装配事实不等即
    // 报错退出。核实（task/done/sublog-single-constraint.md）：本字段无 CLI /
    // CONFIG SET 用户配置通路（runtime_server_options 投影不设置、CONFIG 槽位
    // 只读），本门在用户配置面当前不可达，仅防未来接通投影时漂移；生产
    // 存储面装配口 single_log_aof 恒单后端，分片内核（GarnetLog sharded 分支）
    // 已有 N>1 集成测试但未经 server 面点亮——若放行 >1，hash%N 路由将在长度
    // 为 1 的后端集上越界，本门同样拦在此前。逐子日志扇出（N 设备装配、泵
    // 扇出、副本按子日志落盘、diskbased N 文件拓扑）另立棒，落点清单见上述
    // 核实文档）
    if runtime_options.aof_physical_sublog_count != 1 {
      return Err(Error::InvalidArgument(
        "the replication plane requires aof-physical-sublog-count=1 (multi-sublog replication pump not implemented)".into(),
      ));
    }
    // 复制域管理器生产装配（对标 C# ReplicationManager 构造期：以
    // CheckpointDir/cluster 持久化复制历史，Recover && fileSize > 0 门控
    // 恢复，否则初始化新历史；rust 装配期以真实目录重建默认实例，须先于
    // set_aof / wire_replication_data_plane 等挂 rm 资产的注入）
    cluster.initialize_replication_manager(
      runtime_options.aof_physical_sublog_count as usize,
      Some(&provider.checkpoint_dir.join("cluster")),
      node.recover,
    );
    // 存储注入集群提供者（对标 C# clusterProvider.storeWrapper 装配期建立；
    // CLUSTER RESET 的 HasKeysInSlots 扫描与 HARD 清库经此下达）。此处仅为
    // 装配期播种：集群侧引擎取用单源为 ClusterProvider 的引擎槽，下方
    // set_store_swap_slot 采纳宿主槽后与之同源，运行期置换（swap_online_store）
    // 单次写槽即两侧同时见新引擎，无第二通道
    cluster.set_store(provider.store());
    // 逻辑数据库管理器注入（按需检查点与快照管理，对标 C# StoreWrapper.databaseManager）
    cluster.set_database_manager(Arc::clone(&provider.database_manager));
    // 清库广播门注入（FLUSH 族 AOF 条目的主库判定源，对标 C# SafeFlushAOF
    // 的 clusterProvider.IsPrimary 门控；与本文件其他 set_* 注入同点时序）
    let flush_gate: ClusterProviderHandle = cluster.clone();
    provider.database_manager.attach_flush_gate(flush_gate);
    // 检查点目录注入（快照发送源与副本接收落盘目标公共根，对标 C#
    // clusterProvider 经 storeWrapper 反查 CheckpointDir）
    cluster.set_checkpoint_dir(provider.checkpoint_dir.clone());
    // 快照根目录注入复制管理器（对标 C# rm 构造期即持 CheckpointDir；
    // INFO CINFO 的 disk_checkpoint_entry 扫盘探测源，rm 构造早于目录
    // 装配，走本装配期注入）
    if let Some(rm) = cluster.replication_manager() {
      rm.set_checkpoint_dir(provider.checkpoint_dir.clone());
    }
    // 向量集合管理器注入（CLUSTER RESERVE 迁移预保留面，对标 C# 会话侧
    // vectorManager 可达面）
    cluster.set_vector_manager(Arc::clone(&provider.vector_manager));
    // 发布订阅中枢注入（对标 C# clusterProvider.storeWrapper.subscribeBroker 装配期建立；
    // CLUSTER PUBLISH 接收面本地投递源）
    cluster.set_pubsub(provider.pubsub.clone());
    // AOF 门控（C# StoreWrapper.EnableAOF）：provider 由上方 open_from_args 按
    // (recover, aof) 四路分派装配，aof 为 true 的两臂即
    // StorageSessionProvider::open_with_config_and_aof /
    // open_recovered_with_config_and_aof，其 aof() 在此注入 set_aof。
    // 检查点版本切换标记的提交通道与 AOF 同源装配（对标 C# ReplicationManager
    // 构造期无条件把 checkpointVersionShift 委托挂到检查点管理器、闭包反查
    // storeWrapper.EnqueueCommit）：闭包绑定 GarnetLog::enqueue_database_commit，
    // 无 AOF 则无标记落盘面，故与 set_aof 同块注入
    if let Some(aof) = provider.aof() {
      cluster.set_aof(Some(Arc::clone(aof)));
      let log = Arc::clone(aof.log());
      let commit: StoreCommitFn = Arc::new(move |op_type: AofEntryType, version: i64| {
        // 标记入 AOF 失败即主从发散面，但检查点内核不可回滚（对标 C# EnqueueCommit
        // 无返回码吞错），此处据实记录不静默
        if let Err(e) = log.enqueue_database_commit(op_type, version) {
          log::warn!("Failed to enqueue checkpoint marker {op_type:?}@{version}: {e}");
        }
      });
      cluster.set_commit_channel(Some(commit));
    }
    // 复制数据面生产装配（对标 C# ReplicationManager 构造期 storeWrapper
    // 反查装配）：主端推流资产（INITIATE_REPLICA_SYNC 服务面）+ 副本接收
    // 会话（CLUSTER APPENDLOG 落盘重放）+ 本地日志位点源，一次注入
    if let Some(wal) = provider.wal() {
      wire_replication_data_plane(&cluster, Arc::clone(wal));
    }
    // 副本重连轮询频率与 FastAofTruncate 注入（对标 C# EnsureReplication 读
    // runtimeConfig.GetInt(ServerConfigType.
    // CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT)：默认 0 = 禁用自动重连，
    // --config 经 RuntimeServerOptions 可设；FastAofTruncate 同源自
    // serverOptions——副本接收面跳跃重对齐分支的开关，与下一行按需检查点
    // 共同构成 allow_data_loss 的唯一派生输入）
    cluster.set_replication_reestablishment_timeout(
      runtime_options.cluster_replication_reestablishment_timeout,
    );
    cluster.set_fast_aof_truncate(runtime_options.fast_aof_truncate);
    // 按需检查点开关注入（C# serverOptions.OnDemandCheckpoint 直读同源，
    // --on-demand-checkpoint / nested_text 配置面，默认 true）：主端副本 attach
    // 前的按需重拍判据源，兼 allow_data_loss 派生输入
    cluster.set_on_demand_checkpoint(runtime_options.on_demand_checkpoint);
    // 无盘同步 leader 攒批窗口注入（C# serverOptions.ReplicaDisklessSyncDelay
    // → runtimeConfig.GetInt(REPL_DISKLESS_SYNC_DELAY)，默认 5 秒；diskless
    // 主端会话驱动开窗前等待同批副本 attach 的时长源）
    cluster.set_replica_diskless_sync_delay(runtime_options.replica_diskless_sync_delay);
    // 无盘同步开关注入（C# GarnetServerOptions.cs:410 ReplicaDisklessSync，
    // --repl-diskless-sync / nested_text 配置面，默认 false）：副本侧同步发起
    // 端 diskless / diskbased 选路的唯一读取源
    cluster.set_replica_diskless_sync(node.repl_diskless_sync);
    // 重启恢复开关注入（C# GarnetServerOptions.Recover 同源）：启动期主动 attach
    // 臂 ReplicationManager.Start 对偶的分支判据，随 Provider.Start 在总装尾段读取
    cluster.set_recover(node.recover);
    // 集群出站 TLS 客户端配置注入（对标 C# GarnetTlsOptions 构造期
    // enableCluster 时产 TlsClientOptions：gossip / 复制 / 迁移 / failover
    // 五类出站连接的单源配置；证书对与入站共用 tls-cert/tls-key（C#
    // LocalCertificateSelectionCallback 复用服务端证书选择器的 mTLS 对等
    // 形态），任一 TLS 字段在位才装配，全空即明文集群零变化）
    #[cfg(feature = "tls")]
    if node.tls_cert.is_some()
      || node.tls_key.is_some()
      || node.tls_client_target_host.is_some()
      || node.tls_issuer_cert.is_some()
    {
      let tls = ClientTlsConfig::new(
        node.tls_cert.as_deref(),
        node.tls_key.as_deref(),
        node.tls_client_target_host.as_deref().unwrap_or(""),
        node.tls_server_cert_required,
        node.tls_issuer_cert.as_deref(),
      )
      .map_err(|e| Error::Io(io::Error::other(e.to_string())))?;
      cluster.set_cluster_tls_client(Some(Arc::new(tls)));
    }
    // 副本重放最大滞后字节数注入（C# serverOptions.AofReplayMaxLagBytes
    // 同源：默认 -1 = 异步重放不节流，0 = 同步重放，>0 = 滞后超限阻塞
    // 推流；副本会话 ThrottlePrimary 门限源）
    cluster.set_aof_replay_max_lag_bytes(runtime_options.aof_replay_max_lag_bytes);
    // 运行时配置可达面注入（对标 C# ClusterProvider.cs:50 构造期传入的
    // serverOptions 字段——AofSyncTask 每轮实时读 AofTailWitnessFreqMs 的
    // CLUSTER ADVANCE_TIME 节流频率源；与 StorageSessionProvider 共享同一
    // Arc 实例，CONFIG SET 热更落槽即时生效，不建第二张配置表、不留装配
    // 期快照）
    cluster.set_runtime_config(Arc::clone(&provider.runtime_config));
    // 集群节点超时（C# GarnetServerOptions.ClusterTimeout 等价物）：槽位
    // 校验等待（CanOperateOnKey / WaitForSlotToStabalize 挂起重评）的超时
    // 上限源，超时后按 ASK/CLUSTERDOWN 终评，杜绝命令永久挂起
    cluster.set_cluster_node_timeout_ms(args.cluster_node_timeout_ms);
    // 集群重定向端点偏好（C# serverOptions.ClusterPreferredEndpointType）：
    // MOVED/ASK 重定向与 CLUSTER SLOTS/SHARDS 输出的地址形态源
    cluster.set_preferred_endpoint_type(args.cluster_preferred_endpoint_type);
    // gossip 参数注入（C# GarnetServerOptions.GossipDelay /
    // GossipSamplePercent → ClusterManager 构造读取；对标
    // ClusterProvider.cs:60 构造期百分比校验）
    if !(0..=100).contains(&args.gossip_sample_percent) {
      return Err(Error::InvalidArgument(
        "Gossip sample fraction should be in range [0,100]".into(),
      ));
    }
    cluster.set_gossip_delay_ms(args.gossip_delay_secs * 1000);
    cluster.set_gossip_sample_percent(args.gossip_sample_percent);
    // 集群拓扑持久化装配（对标 C# ClusterManager 构造段的设备建立、盘恢复、
    // InitLocal 与周期刷盘拉起）：端点经宣告解析（C# Options.cs:800-811 匹配
    // 校验 + GetClusterEndpoint 的 Any 绑定出口探测，任何路径不产 0.0.0.0，
    // 见 server::announce）；置于复制域恢复之前、端点 accept 与 gossip 启动
    // 之前（须在 compio 运行时内，本回调即运行时内执行）
    let (announce_addr, announce_port) = announce::resolve_cluster_announce(
      node,
      args.cluster_announce_ip.as_deref(),
      args.cluster_announce_port,
      announce::probe_outbound_ip,
    )
    .map_err(|e| Error::Io(io::Error::other(e.to_string())))?;
    cluster
      .initialize_cluster_config(
        &announce_addr,
        announce_port,
        Path::new(&args.cluster_config_path()),
        args.cluster_config_flush_frequency_ms,
        args.clean_cluster_config,
        &node.cluster_announce_hostname,
      )
      .map_err(|e| Error::Io(io::Error::other(e.to_string())))?;
    // 复制域启动恢复（对标 C# GarnetServer.Start → Provider.RecoverAsync →
    // rm.RecoverAsync：PRIMARY 侧检查点内存索引重建；复制历史恢复已在 rm
    // 构造门控完成，数据面 checkpoint/AOF 恢复由上方 open_from_args 的
    // recover 臂（open_recovered_with_config*）承接）
    if node.recover
      && let Some(rm) = cluster.replication_manager()
    {
      // 位点回填（对标 C# RecoverCheckpointAndAOFAsync 尾段
      // replicationOffset.SetValue(ref replayedUntil)：重放后 AOF 尾为
      // gossip 广播与 failover 判定基线，先于 InitializeCheckpointStore；
      // 无 AOF 形态 recovered_aof_tail 为 None 跳过，对标 C# EnableAOF 门控）
      if let Some(tail) = provider.recovered_aof_tail() {
        rm.set_current_replication_offset(tail);
      }
      rm.recover_async(cluster.is_primary()).await;
    }
    // 采纳宿主置换槽（对标 C# clusterProvider.storeWrapper 单源可达；槽为 Arc
    // 薄句柄，共享状态零循环持有）：此后集群侧 try_store 与宿主 store() 读同一
    // 槽，上方 set_store 的种子随采纳迁入本槽，运行期置换单次写即两侧生效
    cluster.set_store_swap_slot(provider.store_swap_slot());
    // 注入 Primary 类后台任务生命周期域（C# StoreWrapper.Start 按角色分派
    // StartPrimaryTasks 的装配期对译：恢复态副本在此同步挂起周期任务与 GC
    // 扫描，升主经 resume_primary_tasks 恢复；须在 set_store 之后——挂起面
    // 要停在线引擎的 GC 循环）
    cluster.set_primary_tasks(provider.primary_tasks());
    let provider = Arc::new(provider);
    Ok(provider)
  })
}
