//! 集群服务端运行与生命周期组装

use std::{io, path::Path, sync::Arc};

use itoa::Buffer;
use waof::AofEntryType;
use wconf::{
  RuntimeServerOptions, ServerArgs, ServerConfigType,
  runtime_server_options::DEFAULT_CLUSTER_NODE_TIMEOUT_MS,
};
#[cfg(feature = "tls")]
use wnode::SessionProviderFace;
use wnode::{
  ClusterProviderHandle, Error as NodeError, RespSessionConsumer, ServerBootstrap,
  ShutdownCoordinator,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  ClusterArgs,
  error::Result,
  server::{
    announce,
    cluster::IClusterProvider,
    cluster_provider::ClusterProvider,
    replication::{StoreCommitFn, wire_replication_data_plane},
  },
};

/// 运行 WeDB 分布式集群节点服务
///
const ERR_GOSSIP_SAMPLE_RANGE: &str = "Gossip sample fraction should be in range [0,100]";

/// 亚秒集群节点超时拒启信息：CONFIG 秒槽整秒粒度承载不了亚秒，整除截断落 0
/// 会与 provider 毫秒生效槽语义反转（回显 0 = 无限，实际亚秒超时）
const ERR_CLUSTER_NODE_TIMEOUT_SUBSECOND: &str = "cluster-node-timeout-ms must be 0 (infinite) or >= 1000 (whole-seconds config slot cannot carry subsecond values)";

/// 超大集群节点超时拒启信息：C# 契约带 --cluster-timeout 秒整型上界
/// int.MaxValue（Options.cs:298-300 IntRangeValidation(0, int.MaxValue)），
/// 毫秒域即 i32::MAX*1000；契约带上界拒收（非溢出断言）
const ERR_CLUSTER_NODE_TIMEOUT_TOO_LARGE: &str = "cluster-node-timeout-ms exceeds contract upper bound 2147483647000 (C# --cluster-timeout is IntRangeValidation(0, int.MaxValue) seconds; 0 = infinite sentinel stays exempt)";

/// 超大 gossip 周期拒启信息：C# 契约带 --gossip-delay 秒整型上界
/// int.MaxValue（Options.cs:294-296 IntRangeValidation(0, int.MaxValue)）；
/// 契约带上界拒收（非溢出断言）
const ERR_GOSSIP_DELAY_TOO_LARGE: &str = "gossip-delay-secs exceeds contract upper bound 2147483647 (C# --gossip-delay is IntRangeValidation(0, int.MaxValue) seconds)";

/// 亚秒集群节点超时判定：0 = 无限哨兵豁免（args.rs 明写 0 = 无限超时），
/// 1..=999 拒启。C# --cluster-timeout 为秒整型（Options.cs:298-300），槽值
/// 与生效值恒一致；rust 毫秒粒度自引入的回显面漏洞以拒亚秒收口
#[inline]
pub const fn is_subsecond_cluster_node_timeout(ms: u64) -> bool {
  ms != 0 && ms < 1000
}

/// 超大集群节点超时判定（高边闸，与亚秒低边门同名单不重叠）：C# 契约带
/// --cluster-timeout 秒整型上界 int.MaxValue（Options.cs:298-300），毫秒域
/// 上界即 i32::MAX*1000，界内值经 compio 定时器加法恒为有限大界等待，与
/// C# 同形无实差；越界系契约外笔误（如 u64::MAX 当永等），启动期拒。
/// 0 = 无限哨兵豁免臂与亚秒门同源（args.rs 明写 0 = 无限超时）
#[inline]
pub const fn is_oversized_cluster_node_timeout(ms: u64) -> bool {
  ms != 0 && ms > (i32::MAX as u64) * 1000
}

/// 超大 gossip 周期判定：C# 契约带 --gossip-delay 秒整型上界 int.MaxValue
/// （Options.cs:294-296），rust 秒域 u64 无界，越界即契约外笔误，启动期拒。
/// 本闸落地后 boot 播种臂 saturating_mul（前张 done 票收口面，保留作冗余
/// 保护层）恒不触顶，进槽毫秒恒 <= i32::MAX*1000
#[inline]
pub const fn is_oversized_gossip_delay_secs(secs: u64) -> bool {
  secs > i32::MAX as u64
}

/// CLI 毫秒 → CONFIG 秒槽播种值：整除截断（拒 ceil，保「整秒 ×1000」单换算
/// 机制不双轨）。合法域（0 或 >= 1000）内 0 哨兵落 0（无限回显）、亚秒以上
/// 落 >= 1，回显与 provider 生效槽恒同向；i32::MAX 饱和防 u64 as i32 环绕
/// 成负触发「非正即无限」反转
#[inline]
pub const fn cluster_node_timeout_seed_secs(ms: u64) -> i64 {
  let secs = ms / 1000;
  if secs > i32::MAX as u64 {
    i32::MAX as i64
  } else {
    secs as i64
  }
}

/// 多子日志推流泵未实现拒启信息（复制域物理子日志数门，判据见
/// [`aof_boot_gate_violation`]）
const ERR_AOF_MULTI_SUBLOG: &str = "the replication plane requires aof-physical-sublog-count=1 (multi-sublog replication pump not implemented)";

/// 单物理多回放静默零重放拒启信息（复制域回放任务数门，票
/// wnode-aof-multi-replay-single-physical-silent-zero-replay）
const ERR_AOF_MULTI_REPLAY: &str = "the replication plane requires aof-replay-task-count=1 (single-physical multi-replay AOF recovery silently replays zero records)";

/// 复制域 AOF 拓扑装配期双门判据（物理子日志数门 / 回放任务数门同族同形，
/// 违规返回拒启信息单源；两门现产均无用户配置通路，系防漂移防御门，
/// 反证锁测直驱本纯函数，见 tests/aof_replay_topology_gate.rs）
///
/// 一、物理子日志数门（对标 C# AofSyncTask 构造期逐子日志自带扫描源
/// `physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx)`，
/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:120；
/// rust 推流泵数据源为单条 WalLog（aof_replication_pump::pump_backlog 签名），
/// 多子日志推流泵未实现——配置 >1 时驱动按 N 任务建、泵只喂 0 号任务，
/// 1..N 号子日志水位/截断线恒钉死、同步静默缺流；配置与装配事实不等即
/// 报错退出。核实结论原记于 task/done/sublog-single-constraint.md（随票勘误：
/// 该核实文档现已不可寻，本注记保留其来历；结论仍经现行码复核成立）：
/// 本字段无 CLI / CONFIG SET 用户配置通路（runtime_server_options 投影不
/// 设置、CONFIG 槽位只读），本门在用户配置面当前不可达，仅防未来接通
/// 投影时漂移；生产存储面装配口 single_log_aof 恒单后端，分片内核
/// （GarnetLog sharded 分支）已有 N>1 集成测试但未经 server 面点亮——若
/// 放行 >1，hash%N 路由将在长度为 1 的后端集上越界，本门同样拦在此前。
/// 逐子日志扇出（N 设备装配、泵扇出、副本按子日志落盘、diskbased N 文件
/// 拓扑）另立棒。
///
/// 二、回放任务数门（票 wnode-aof-multi-replay-single-physical-silent-zero-
/// replay，审核裁定取 boot 门补齐、不动 recover 分派面）：multi_log_enabled
/// = physical_sublog_count > 1 || replay_task_count > 1（对标 C#
/// GarnetServerOptions.cs:1244 MultiLogEnabled，rust 同形
/// wnode garnet_append_only_file.rs:78）。单物理+多回放组合下
/// recover_latest_sequence_number（addresses.rs，对标 GarnetLog.cs:182-198
/// 单物理恒返 true/-1）恒 Some(-1) → record_gate::skip_replay 见 -1 首条即
/// 跳 → multi_log_recover（对标 AofRecover.cs:63 分派）收 Ok(0) 静默零重放，
/// 检查点后写入全丢且零告警。C# 侧 AofReplayTaskCount 有 CLI 投影
///（GarnetServerOptions.cs:122 → Options.cs:229），上游组合真实可达，rust
/// 逐字继承该缺陷面，以 boot 门防御收口；本字段现系 read_only 槽
///（runtime_server_config.rs:377-382 aof-replay-task-count），投影不设值、
/// 缺省恒 1，本门现产不可达，纯防未来旋钮接通漂移。落点取 boot 门而非
/// recover 分派面报错，保 AofRecover.cs:63 的 1:1 对标形态，且库级直调
/// single_log_recover 的 1+4 合法用法（tests/aof_recover_chunk_parallel.rs）
/// 不经 server 面，不受本门影响。
#[inline]
pub const fn aof_boot_gate_violation(options: &RuntimeServerOptions) -> Option<&'static str> {
  if options.aof_physical_sublog_count != 1 {
    return Some(ERR_AOF_MULTI_SUBLOG);
  }
  if options.aof_replay_task_count != 1 {
    return Some(ERR_AOF_MULTI_REPLAY);
  }
  None
}

/// 错误收口本 crate 单源（[`crate::error::Error::Node`] 透明变体承接节点域
/// 错误；C# 拒启与运行错误同一天然通道上抛的对位），嵌入宿主单一 match 域
pub fn run_cluster_server(
  args: ClusterArgs,
  coordinator: Option<ShutdownCoordinator>,
) -> Result<()> {
  if !(0..=100).contains(&args.gossip_sample_percent) {
    return Err(NodeError::InvalidArgument(ERR_GOSSIP_SAMPLE_RANGE.into()).into());
  }
  // 亚秒集群节点超时拒启（对齐上一行 gossip 抽样范围校验先例）：毫秒值经
  // 亚秒门后，秒槽播种值恒与 provider 生效槽同向（0 = 无限 / >= 1 = 有限），
  // CONFIG GET 回显不再与实际生效值语义反转
  if is_subsecond_cluster_node_timeout(args.cluster_node_timeout_ms) {
    return Err(NodeError::InvalidArgument(ERR_CLUSTER_NODE_TIMEOUT_SUBSECOND.into()).into());
  }
  // 超大集群节点超时拒启（高边闸，同名单同形制）：C# 入口 IntRangeValidation
  // (0, int.MaxValue) 秒域上界原样搬到毫秒域，契约带上界拒收；0 无限哨兵臂
  // 维持豁免，亚秒档由上一行既有闸拒，两闸不重叠
  if is_oversized_cluster_node_timeout(args.cluster_node_timeout_ms) {
    return Err(NodeError::InvalidArgument(ERR_CLUSTER_NODE_TIMEOUT_TOO_LARGE.into()).into());
  }
  // 超大 gossip 周期拒启（同名单同形制，对位 C# --gossip-delay
  // IntRangeValidation(0, int.MaxValue) 秒域上界）：u64 秒域经播种臂饱和乘
  // 只会收拢成 u64::MAX 毫秒独木桥，入口收干净后高边全闭、消费侧不钳
  if is_oversized_gossip_delay_secs(args.gossip_delay_secs) {
    return Err(NodeError::InvalidArgument(ERR_GOSSIP_DELAY_TOO_LARGE.into()).into());
  }
  // 采样节拍 / 延迟监视 / 逐命令统计 / 连接上限 / TLS 由
  // ServerBootstrap::run_async 一处从 NodeArgs 投影（对标 C#
  // StoreWrapper.cs:226-227 消费侧直读 options），本宿主零逐字段装配样板
  let mut bootstrap = ServerBootstrap::new(args)
    .with_cluster_provider(ClusterProvider::new())
    .banner("WeDB 分布式集群节点");

  if let Some(coord) = coordinator {
    bootstrap = bootstrap.with_shutdown_coordinator(coord);
  }

  // 尾段收口：节点域错误（含装配回调内部）经 [`crate::error::Error::Node`]
  // 透明变体 `?` 转入本 crate 单源（装配回调错误域为 wnode，系
  // ServerBootstrap::run_async 的泛型约束所定）
  bootstrap.run_async(|args, cluster| async move {
    let node = args.node_args();
    // 集群互信凭据启动接线（对标 C# ClusterProvider.cs:56-57 构造期以
    // serverOptions.ClusterUsername/ClusterPassword 注入 AuthContainer）：
    // 启动即建互信，gossip / 复制 / failover 五类出站握手首连即携凭据，杜绝
    // 冷启动被对端拒后须外部 CONFIG SET cluster-username/password 补救的脱节。
    // update_cluster_auth 与运行期 CONFIG SET 共用同一 AuthContainer 单点，
    // 未配置（None,None）即明文集群、行为不变
    cluster.update_cluster_auth(node.cluster_username.clone(), node.cluster_password.clone());
    // 会话参数基线（NodeArgs → 会话选项映射收口 wnode 单点
    // `RespServerSessionOptions::from`，与单机同一份）
    let session_options = RespServerSessionOptions::from(node);
    // 看门狗停机句柄先行克隆（下方工厂闭包 move 走 session_options 本体）
    let lua_timeout = session_options.lua_timeout_manager.clone();
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
    // 复制域 AOF 拓扑装配期强制校验（物理子日志数 / 回放任务数双门同族，
    // 判据与来历注记单源 [`aof_boot_gate_violation`]，反证锁测见
    // tests/aof_replay_topology_gate.rs）
    if let Some(err_msg) = aof_boot_gate_violation(&runtime_options) {
      return Err(NodeError::InvalidArgument(err_msg.into()));
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
    // 引擎置换写面钩子束注入（对标 C# 原位恢复的 functionsState 接线跨恢复
    // 全程存活；rust 实例置换形态下运行期 swap_online_store 在投槽前消费本束
    // 对换入引擎统一重挂 WATCH 版本推进钩子与 AOF per-op 事件汇，并断开
    // 存量会话保「锁面==写面」）
    cluster.set_engine_swap_hooks(provider.engine_swap_hook_bundle());
    // 本实例消费者注册表注入（置换清扫射程的实例级收口：多实例同进程形态
    // 下换引擎只断本实例客户端会话，不越槽误杀他实例连接；票
    // zcode-r37-lockfix 发现 A 残留——txnfix2 第二节 6 条跨实例爆炸半径）
    cluster.set_consumer_registry(Arc::clone(&provider.registry));
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
    // 复制同步超时注入（C# serverOptions.ReplicaSyncTimeout 同源，<=0 已折无限哨兵）：
    // 建连与快照/停等往返限时的值源
    cluster.set_replica_sync_timeout_secs(runtime_options.replica_sync_timeout_secs);
    // 按需检查点开关注入（C# serverOptions.OnDemandCheckpoint 直读同源，
    // --on-demand-checkpoint / toml 配置面，默认 true）：主端副本 attach
    // 前的按需重拍判据源，兼 allow_data_loss 派生输入
    cluster.set_on_demand_checkpoint(runtime_options.on_demand_checkpoint);
    // 无盘同步 leader 攒批窗口注入（C# serverOptions.ReplicaDisklessSyncDelay
    // → runtimeConfig.GetInt(REPL_DISKLESS_SYNC_DELAY)，默认 5 秒；diskless
    // 主端会话驱动开窗前等待同批副本 attach 的时长源）
    cluster.set_replica_diskless_sync_delay(runtime_options.replica_diskless_sync_delay);
    // 无盘同步开关注入（C# GarnetServerOptions.cs:410 ReplicaDisklessSync，
    // --repl-diskless-sync / toml 配置面，默认 false）：副本侧同步发起
    // 端 diskless / diskbased 选路的唯一读取源
    cluster.set_replica_diskless_sync(node.repl_diskless_sync);
    // 重启恢复开关注入（C# GarnetServerOptions.Recover 同源）：启动期主动 attach
    // 臂 ReplicationManager.Start 对偶的分支判据，随 Provider.Start 在总装尾段读取
    cluster.set_recover(node.recover);
    // 集群出站 TLS 客户端配置注入（对标 C# GarnetTlsOptions 构造期
    // enableCluster 时产 TlsClientOptions：gossip / 复制 / 迁移 / failover
    // 五类出站连接的单源配置；证书源自 provider.tls_config() 派生——与入站
    // ServerTlsConfig 同一 ArcSwap 单源句柄，CONFIG SET cert-file-name 热换装
    // 与周期刷新对两方向一次生效，C# 出站闭包动态读 serverCertificateSelector
    // （GarnetTlsOptions.cs:185-189）的对位；tls_config() 缺席（仅 issuer/
    // target-host 的纯校验向配置）回落不带客户端证书，禁拒启；任一 TLS 字段
    // 在位才装配，全空即明文集群零变化）
    #[cfg(feature = "tls")]
    if node.has_tls() {
      let certs = provider.tls_config().map(|tls| tls.cert_source());
      let tls = ClientTlsConfig::from_shared_source(
        certs,
        node.tls_client_target_host.as_deref().unwrap_or(""),
        node.tls_server_cert_required,
        node.tls_issuer_cert.as_deref(),
      )
      .map_err(|e| NodeError::Io(io::Error::other(e.to_string())))?;
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
    // 上限源，超时后按 ASK/CLUSTERDOWN 终评，杜绝命令永久挂起。亚秒值已被
    // 顶层亚秒门拒启，此处播种值恒 0（无限哨兵）或 >= 1 整秒
    cluster.set_cluster_node_timeout_ms(args.cluster_node_timeout_ms);
    if args.cluster_node_timeout_ms != DEFAULT_CLUSTER_NODE_TIMEOUT_MS {
      // 秒槽播种（整除截断 + i32::MAX 饱和防 u64 as i32 环绕）；调停消息
      // 弃收为有意——provider 槽上一行已直投精确毫秒，回放消息（整秒×1000）
      // 反而抹掉非整秒尾差（1500ms 回放成 1000ms）；仅 Err 据实记录不静默
      let secs = cluster_node_timeout_seed_secs(args.cluster_node_timeout_ms);
      let mut buf = Buffer::new();
      if let Err(e) = provider
        .runtime_config
        .try_set(ServerConfigType::ClusterNodeTimeout, buf.format(secs))
      {
        log::warn!("播种 cluster-node-timeout 秒槽 {secs} 失败: {e}");
      }
    }
    // 集群重定向端点偏好（C# serverOptions.ClusterPreferredEndpointType）：
    // MOVED/ASK 重定向与 CLUSTER SLOTS/SHARDS 输出的地址形态源
    cluster.set_preferred_endpoint_type(args.cluster_preferred_endpoint_type);
    // gossip 参数注入（C# GarnetServerOptions.GossipDelay /
    // GossipSamplePercent → ClusterManager 构造读取；抽样百分比范围校验由
    // run_cluster_server 入口 fail-fast 单点承接，此处直接播种）
    // gossip 周期播种：秒 → 毫秒折算走 saturating_mul 饱和单点（同族
    // cluster-node-timeout 折算 runtime_server_config.rs 与 wbase convert.rs
    // 先例；C# GossipDelay 为 int 秒经 TimeSpan.FromSeconds 恒不溢出，rust
    // u64 秒域无界，超大值裸乘 debug 溢出 panic、release 环绕成畸形周期）。
    // 大值钳 u64::MAX 毫秒即事实上永不到期上界，行为可预测
    cluster.set_gossip_delay_ms(args.gossip_delay_secs.saturating_mul(1000));
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
    .map_err(|e| NodeError::Io(io::Error::other(e.to_string())))?;
    cluster
      .initialize_cluster_config(
        &announce_addr,
        announce_port,
        Path::new(&args.cluster_config_path()),
        args.cluster_config_flush_frequency_ms,
        args.clean_cluster_config,
        &node.cluster_announce_hostname,
      )
      .map_err(|e| NodeError::Io(io::Error::other(e.to_string())))?;
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
    // Lua 超时看门狗停机句柄挂接（对标 C# StoreWrapper 构造期持有
    // luaTimeoutManager、Dispose 收口）：管理器创建单点在上方的会话选项
    // 投影（assemble_lua_timeout 构造即 start 专属线程），此处仅克隆同一
    // Arc 供停机链 dispose_lua_timeout 取用，不设第二创建点
    let provider = provider.with_lua_timeout(lua_timeout);
    let provider = Arc::new(provider);
    Ok(provider)
  })?;
  Ok(())
}
