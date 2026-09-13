//! WeDB 单机服务端主程序入口
//!
//! 存储执行域装配下沉 wnode 基座：StorageSessionProvider 固化
//! new_session → StoreGarnetApi → 会话消费者 → item_broker → runtime_config
//! 公共流程（单机/集群功能一致，存储 + 经纪 + 向量三件套同源），
//! 单机差异仅为 RespSessionConsumer::new 构造（无集群切面）。

use std::sync::Arc;

use clap::Parser;
use wnode::{
  LoggingBuilder, NodeArgs, RespSessionConsumer, ServerArgs, ServerBootstrap,
  resp::resp_server_session::RespServerSessionOptions, service::StorageSessionProvider,
};

/// 单机数据文件名（node dir 内）
const DATA_FILE: &str = "wedb-standalone.db";

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

fn main() -> wnode::Result<()> {
  let args = StandaloneArgs::parse();

  // C# GarnetServer 构造器日志装配段：控制台（DisableConsoleLogger 未设）
  // + 可选落文件（serverSettings.FileLogger）+ 最低级别（serverSettings.LogLevel）
  let mut logging = LoggingBuilder::new().with_minimum_level(args.node.minimum_log_level());
  if let Some(file) = &args.node.file_logger {
    logging = logging.add_file(file, 0);
  }
  logging
    .install()
    .map_err(|e| wnode::Error::LogInstall(e.to_string()))?;

  ServerBootstrap::new(args)
    .banner("WeDB Standalone 单机节点")
    .run(|args, _noop_cluster| {
      Ok(Arc::new(StorageSessionProvider::open(
        args.node_args().dir.join(DATA_FILE),
        |network_sender_id, api| {
          Some(RespSessionConsumer::new(
            network_sender_id,
            RespServerSessionOptions::default(),
            api,
          ))
        },
      )?))
    })
}
