//! WeDB 单机服务端主程序入口
//!
//! 存储执行域装配下沉 wnode 基座：StorageSessionProvider 固化
//! new_session → StoreGarnetApi → 会话消费者 → item_broker → runtime_config
//! 公共流程（单机/集群功能一致，存储 + 经纪 + 向量三件套同源），
//! 单机差异仅为 RespSessionConsumer::new 构造（无集群切面）。

use std::sync::Arc;

use clap::Parser;
use wconf::{NodeArgs, ServerArgs};
use wlua::LuaOptions;
use wnode::{
  Error, LoggingBuilder, RespSessionConsumer, ServerBootstrap,
  resp::resp_server_session::RespServerSessionOptions,
  service::{StorageSessionProvider, assemble_lua_timeout},
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
  let node = &args.node;
  let mut logging = LoggingBuilder::new().with_minimum_level(node.minimum_log_level());
  if let Some(file) = &node.file_logger {
    logging = logging.add_file(file, 0);
  }
  logging
    .install()
    .map_err(|e| Error::LogInstall(e.to_string()))?;

  ServerBootstrap::new(args)
    .banner("WeDB Standalone 单机节点")
    .run_async(|args, _noop_cluster| async move {
      let node = args.node_args();
      // Lua 超时管理器装配（同集群 main：C# StoreWrapper 构造段 +
      // GarnetServer.cs:Start 的 luaTimeoutManager.Start()）。
      // 标量先行拷出：会话工厂随 provider 存活，不得借用 args。
      let (enable_lua, lua_timeout_ms, lua_txn_mode) = (
        node.enable_lua,
        node.lua_script_timeout_ms,
        node.lua_transaction_mode,
      );
      let lua_timeout_manager = assemble_lua_timeout(enable_lua, lua_timeout_ms);
      let lua_options = wlua::LuaOptions {
        timeout_millis: lua_timeout_ms.max(0),
        ..LuaOptions::default()
      };
      let session_factory = move |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          RespServerSessionOptions {
            enable_lua,
            lua_options: lua_options.clone(),
            lua_txn_mode,
            lua_timeout_manager: lua_timeout_manager.clone(),
            ..RespServerSessionOptions::default()
          },
          api,
        ))
      };
      let data_path = node.dir.join(DATA_FILE);
      // --recover 分派（对标 C# Options.cs:139 Recover →
      // StoreWrapper.RecoverAsync 单机分支：RecoverCheckpointAsync +
      // RecoverAOFAsync + ReplayAOF；恢复在端点 accept 之前完成——
      // GarnetServer.cs:Start 时序）
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
      .with_pubsub_config(node.disable_pubsub, node.pubsub_page_size);
      Ok(Arc::new(provider))
    })
}

#[cfg(test)]
mod tests {
  use std::path::Path;

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
}
