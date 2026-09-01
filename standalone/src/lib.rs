pub mod handler;

pub use handler::{StandaloneHandler, handle_cmd, handle_cmd_with_ctx};
pub use webc_cmd::{Cmd, ConnectionContext, ExpireCondition, RedisCommand, RedisServer};
pub use wedb_embed::{Conf, WeDb};

use compio::signal::ctrl_c;
use std::sync::Arc;

/// 启动 WeDb 单机版 Redis 服务器
pub async fn run_server(addr: &str, data_dir: &str) -> aok::Result<()> {
  run_server_with_conf(addr, &wedb_embed::Conf::new(data_dir)).await
}

/// 基于自定义嵌入式配置启动 WeDb 单机版 Redis 服务器
pub async fn run_server_with_conf(addr: &str, conf: &wedb_embed::Conf) -> aok::Result<()> {
  let db = Arc::new(WeDb::open_with_conf(conf)?);
  let handler = Arc::new(StandaloneHandler::new(db));
  let server = RedisServer::start(handler, addr.to_string()).await?;
  let server_addr = server.addr();
  log::info!("WeDb Standalone Redis server listening on {server_addr}");

  // 监听 Ctrl-C 信号以实现优雅退出
  let _ = ctrl_c().await;
  server.shutdown().await?;
  Ok(())
}
