//! WeDB 分布式集群服务端主程序入口
//!
//! 集群与单机共用同一套 wnode::RespServerSession 会话体系、存储执行域与
//! StorageSessionProvider 装配基座（功能一致：存储 + 经纪 + 向量三件套同源）：
//! 集群差异仅为 decorate 钩子构造 with_cluster_session 会话消费者（挂
//! ClusterSession 切面，maxDatabases = 2），槽位验证、MOVED/ASK 重定向、
//! CLUSTER 命令族、ROLE/HELLO 集群分支均由会话主循环经切面驱动。

use std::{env::args_os, sync::Arc};

use wconf::{ConfigFileArgs, ServerArgs};
use wedb::{
  ClusterArgs,
  server::{
    cluster::IClusterProvider, cluster_provider::ClusterProvider,
    replication::wire_replication_data_plane,
  },
};
use wlua::LuaOptions;
use wnode::{
  Error, LoggingBuilder, RespSessionConsumer, Result, ServerBootstrap,
  resp::resp_server_session::RespServerSessionOptions,
  service::{StorageSessionProvider, assemble_lua_timeout},
};

/// 集群数据文件名（node dir 内）
const DATA_FILE: &str = "wedb-cluster.db";

/// C# 构造：EnableCluster 时 maxDatabases = 2
const CLUSTER_MAX_DATABASES: i32 = 2;

/// 日志文件刷盘间隔（0 表示立即刷盘）
const DEFAULT_LOG_FLUSH_INTERVAL: i32 = 0;

fn main() -> Result<()> {
  // 三层配置合并解析：默认值 → --config nested_text 文件 → 命令行显式项
  // （对标 ServerSettingsManager.cs:TryParseCommandLineArguments）
  let args =
    ClusterArgs::from_args_iter(args_os()).map_err(|e| Error::InvalidArgument(e.to_string()))?;

  // C# GarnetServer 构造器日志装配段：控制台（DisableConsoleLogger 未设）
  // + 可选落文件（serverSettings.FileLogger）+ 最低级别（serverSettings.LogLevel）
  let node = args.node_args();
  let metrics_sampling_frequency_secs = node.metrics_sampling_frequency_secs;
  let latency_monitor = node.latency_monitor;
  let commandstats_monitor = node.commandstats_monitor;
  let mut logging = LoggingBuilder::new().with_minimum_level(node.minimum_log_level());
  if let Some(file) = &node.file_logger {
    logging = logging.add_file(file, DEFAULT_LOG_FLUSH_INTERVAL);
  }
  logging
    .install()
    .map_err(|e| Error::LogInstall(e.to_string()))?;

  ServerBootstrap::new(args)
    .with_cluster_provider(ClusterProvider::new())
    .metrics_sampling_frequency(metrics_sampling_frequency_secs)
    .latency_monitor(latency_monitor)
    .commandstats_monitor(commandstats_monitor)
    .banner("WeDB 分布式集群节点")
    .run_async(|args, cluster| async move {
      let node = args.node_args();
      // Lua 超时管理器装配（C# StoreWrapper 构造段：EnableLua 且
      // LuaOptions.Timeout != Infinite 时 new LuaTimeoutManager；
      // GarnetServer.cs:Start 对应的定时循环由 tick 任务承接）。
      // 标量先行拷出：会话工厂随 provider 存活，不得借用 args。
      let (enable_lua, lua_timeout_ms, lua_txn_mode, commandstats_monitor, latency_monitor) = (
        node.enable_lua,
        node.lua_script_timeout_ms,
        node.lua_transaction_mode,
        node.commandstats_monitor,
        node.latency_monitor,
      );
      let lua_timeout_manager = assemble_lua_timeout(enable_lua, lua_timeout_ms);
      let lua_options = wlua::LuaOptions {
        timeout_millis: lua_timeout_ms.max(0),
        ..LuaOptions::default()
      };
      let session_factory = {
        let cluster = Arc::clone(&cluster);
        let lua_timeout_manager = lua_timeout_manager.clone();
        move |network_sender_id, api| {
          let options = RespServerSessionOptions {
            max_databases: CLUSTER_MAX_DATABASES,
            latency_monitor,
            command_stats_monitor: commandstats_monitor,
            enable_lua,
            lua_options: lua_options.clone(),
            lua_txn_mode,
            lua_timeout_manager: lua_timeout_manager.clone(),
            ..RespServerSessionOptions::default()
          };
          Some(RespSessionConsumer::with_cluster_session(
            network_sender_id,
            options,
            cluster.create_cluster_session(),
            api,
          ))
        }
      };
      let data_path = node.dir.join(DATA_FILE);
      // --recover 分派（对标 C# Options.cs:139 Recover → StoreWrapper
      // .RecoverAsync 集群分支 → clusterProvider.RecoverAsync →
      // rm.RecoverAsync；数据面恢复经 wnode 宿主承接，时序同为端点
      // accept 之前——GarnetServer.cs:Start）
      let provider = match (node.recover, node.aof) {
        (true, true) => {
          StorageSessionProvider::open_recovered_with_aof(
            data_path,
            node.wal_dir.as_deref(),
            node.aof_commit_ms,
            session_factory,
          )
          .await?
        }
        (true, false) => StorageSessionProvider::open_recovered(data_path, session_factory).await?,
        (false, true) => StorageSessionProvider::open_with_aof(
          data_path,
          node.wal_dir.as_deref(),
          node.aof_commit_ms,
          session_factory,
        )?,
        (false, false) => StorageSessionProvider::open(data_path, session_factory)?,
      }
      .with_requirepass(node.requirepass.as_deref())
      // 发布订阅装配覆盖（C# 默认 DisablePubSub = false；--disable-pubsub
      // 关闭 / --pubsub-page-size 调页，端点 accept 之前生效）
      .with_pubsub_config(node.disable_pubsub, node.pubsub_page_size)
      // 运行时配置 + 慢日志装配覆盖（--slowlog-log-slower-than /
      // --slowlog-max-len / --object-scan-count-limit / --aof-commit-ms /
      // --max-databases 播种，端点 accept 之前生效）
      .with_runtime_server_options(node.runtime_server_options());
      // 存储注入集群提供者（对标 C# clusterProvider.storeWrapper 装配期建立；
      // CLUSTER RESET 的 HasKeysInSlots 扫描与 HARD 清库经此下达）
      cluster.set_store(Arc::clone(&provider.store));
      // 向量集合管理器注入（CLUSTER RESERVE 迁移预保留面，对标 C# 会话侧
      // vectorManager 可达面）
      cluster.set_vector_manager(Arc::clone(&provider.vector_manager));
      // AOF 门控（C# StoreWrapper.EnableAOF）：当 args.node_args().aof 为 true 时，
      // 经 StorageSessionProvider::open_with_aof 构造后在此注入 set_aof
      if let Some(aof) = provider.aof() {
        cluster.set_aof(Some(Arc::clone(aof)));
      }
      // 复制数据面生产装配（对标 C# ReplicationManager 构造期 storeWrapper
      // 反查装配）：主端推流资产（INITIATEREPLICASYNC 服务面）+ 副本接收
      // 会话（CLUSTER APPENDLOG 落盘重放）+ 本地日志位点源，一次注入
      if let Some(wal) = provider.wal() {
        wire_replication_data_plane(&cluster, Arc::clone(wal));
      }
      // 副本重连轮询频率与 FastAofTruncate 注入（对标 C# EnsureReplication 读
      // runtimeConfig.GetInt(ServerConfigType.
      // CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT)：默认 0 = 禁用自动重连，
      // --config 经 RuntimeServerOptions 可设；FastAofTruncate 同源自
      // serverOptions——副本接收面跳跃重对齐分支的开关）
      let runtime_options = node.runtime_server_options();
      cluster.set_replication_reestablishment_timeout(
        runtime_options.cluster_replication_reestablishment_timeout,
      );
      cluster.set_fast_aof_truncate(runtime_options.fast_aof_truncate);
      // 集群节点超时（C# GarnetServerOptions.ClusterTimeout 等价物）：槽位
      // 校验等待（CanOperateOnKey / WaitForSlotToStabalize 挂起重评）的超时
      // 上限源，超时后按 ASK/CLUSTERDOWN 终评，杜绝命令永久挂起
      cluster.set_cluster_node_timeout_ms(args.cluster_node_timeout_ms);
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
      // 复制域启动恢复（对标 C# GarnetServer.Start → Provider.RecoverAsync →
      // rm.RecoverAsync：replication history 恢复 + PRIMARY 侧检查点内存
      // 索引重建；数据面 checkpoint/AOF 恢复已由上方 open_recovered* 承接）
      if node.recover
        && let Some(rm) = cluster.replication_manager()
      {
        rm.recover_async(cluster.is_primary()).await;
      }
      Ok(Arc::new(provider))
    })
}
