//! WeDB 单机服务端主程序入口
//!
//! 存储执行域装配下沉 wnode 基座：StorageSessionProvider 固化
//! new_session → StoreGarnetApi → 会话消费者 → item_broker → runtime_config
//! 公共流程（单机/集群功能一致，存储 + 经纪 + 向量三件套同源），
//! 单机差异仅为 RespSessionConsumer::new 构造（无集群切面）。

use std::{env::args_os, sync::Arc};

use clap::{ArgMatches, Parser};
use wconf::{ConfigFileArgs, NodeArgs, NodeOptionsError, ServerArgs};
use wnode::{
  Error, LoggingBuilder, MemoryForwardLogger, RespSessionConsumer, ServerBootstrap,
  resp::resp_server_session::RespServerSessionOptions, service::StorageSessionProvider,
};

/// WeDB 单机参数配置
#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "WeDB Standalone 单机服务节点")]
pub struct StandaloneArgs {
  /// 节点通用参数（端口、工作目录、线程、WAL路径等）
  #[command(flatten)]
  pub node: NodeArgs,
}

impl ServerArgs for StandaloneArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    &self.node
  }
}

impl ConfigFileArgs for StandaloneArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    Ok(Self {
      node: NodeArgs::from_layered_matches(matches)?,
    })
  }
}

fn main() -> wnode::Result<()> {
  // 先行缓冲安装（对标 C# GarnetServer 构造器 :86-88 的 initLogger 段）：正式
  // 日志面要等参数解析完才谈得上装配，期间记录先进内存，装配时回灌
  let pre_parse = MemoryForwardLogger::install().map_err(|e| Error::LogInstall(e.to_string()))?;

  // 三层配置合并解析：默认值 → --config toml 文件 → 命令行显式项
  // （对标 ServerSettingsManager.cs:TryParseCommandLineArguments；
  // --help/--version 走 stdout 干净全文 + exit(0) 的用户交互路径）
  let args = StandaloneArgs::from_args_iter_or_exit(args_os())
    .map_err(|e| Error::InvalidArgument(e.to_string()))?;

  // C# GarnetServer 构造器日志装配段收口 wnode 单点 LoggingBuilder::from_node
  // + flush_into（挂上正式日志面并回灌先行缓冲存量）；文件日志打开失败即
  // LogInstall 拒启（C# FileLoggerProvider.cs:50 File.Open 抛错对位）
  let node = &args.node;
  LoggingBuilder::from_node(node)
    .flush_into(&pre_parse)
    .map_err(|e| Error::LogInstall(e.to_string()))?;

  // 采样节拍 / 延迟监视 / 逐命令统计 / 连接上限 / TLS 由
  // ServerBootstrap::run_async 一处从 NodeArgs 投影（对标 C#
  // StoreWrapper.cs:226-227 消费侧直读 options），与集群宿主同一份装配事实
  ServerBootstrap::new(args)
    .banner("WeDB Standalone 单机节点")
    .run_async(|args, _noop_cluster| async move {
      let node = args.node_args();
      // 会话参数基线（NodeArgs → 会话选项映射收口 wnode 单点
      // `RespServerSessionOptions::from`，与集群同一份）
      let session_options = RespServerSessionOptions::from(node);
      let session_factory = move |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          session_options.clone(),
          Arc::new(api),
        ))
      };
      let data_path = node.data_path();
      // --recover 与 AOF 分派及运行时选项装配收口 wnode 基座（对标 C# Options.cs:139
      // Recover → StoreWrapper.RecoverAsync 单机分支；恢复在端点 accept 之前完成）
      let provider =
        StorageSessionProvider::open_from_args(node, data_path, session_factory).await?;
      Ok(Arc::new(provider))
    })
}

#[cfg(test)]
mod tests {
  use std::{env::temp_dir, fs, path::Path};

  use wconf::ConfigFileArgs;

  use super::*;

  #[test]
  fn test_standalone_cli_aof_and_wal_dir() {
    let args =
      StandaloneArgs::try_parse_from(["wedb-standalone", "--aof", "--wal-dir", "/data/wal"])
        .unwrap();
    assert!(args.node.aof);
    assert_eq!(args.node.wal_dir.as_deref(), Some(Path::new("/data/wal")));

    let args_default = StandaloneArgs::try_parse_from(["wedb-standalone"]).unwrap();
    assert!(!args_default.node.aof);
    assert_eq!(args_default.node.wal_dir, None);
  }

  #[test]
  fn test_standalone_config_file_cli_override_projection() {
    // 端到端：toml 文件加载 → CLI 覆盖 → 运行时选项投影生效
    let file = temp_dir().join("wedb-standalone-e2e.toml");
    fs::write(
      &file,
      "port = 7020\nslow_log_threshold = 2500\nslow_log_max_entries = 64\nmax_databases = 8\nobject_scan_count_limit = 777\n",
    )
    .unwrap();
    let args = StandaloneArgs::from_args_iter([
      "wedb-standalone",
      "--config",
      file.to_str().unwrap(),
      "--port",
      "7021",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    // CLI 覆盖文件值
    assert_eq!(args.node.port, 7021);
    // 文件值生效
    assert_eq!(args.node.slow_log_threshold, 2500);
    assert_eq!(args.node.slow_log_max_entries, 64);
    assert_eq!(args.node.max_databases, 8);
    assert_eq!(args.node.object_scan_count_limit, 777);

    // 运行时选项投影（provider.with_runtime_server_options 播种源）
    let opts = args.node.runtime_server_options();
    assert_eq!(opts.slow_log_threshold, 2500);
    assert_eq!(opts.slow_log_max_entries, 64);
    assert_eq!(opts.max_databases, 8);
    assert_eq!(opts.object_scan_count_limit, 777);
  }
}
