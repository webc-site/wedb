use std::sync::Arc;
use webc_cmd::{Cmd, CmdHandler, ConnectionContext, RedisServer as BaseRedisServer};
use wedb_resp::RespValue;

use super::handler::RedisHandler;
use crate::error::{Error, Result};
use crate::node::RaftNode;

pub struct ClusterCmdHandler {
  node: Arc<RaftNode>,
}

impl CmdHandler for ClusterCmdHandler {
  async fn handle(&self, ctx: &mut ConnectionContext, cmd: Cmd) -> RespValue {
    match RedisHandler::handle(&self.node, ctx, cmd).await {
      Ok(resp) => resp,
      Err(e) => RespValue::error(format!("ERR {e}")),
    }
  }
}

pub struct RedisServer {
  server: BaseRedisServer,
}

impl RedisServer {
  pub fn addr(&self) -> &str {
    self.server.addr()
  }

  pub async fn start(node: Arc<RaftNode>, addr: String) -> Result<Self> {
    let handler = Arc::new(ClusterCmdHandler { node });
    let server = BaseRedisServer::start(handler, addr)
      .await
      .map_err(|e| Error::internal(e.to_string()))?;
    Ok(Self { server })
  }

  pub async fn shutdown(self) -> Result<()> {
    self
      .server
      .shutdown()
      .await
      .map_err(|e| Error::internal(e.to_string()))
  }
}
