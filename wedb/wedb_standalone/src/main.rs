//! WeDB 单机服务端主程序入口

use std::{mem, sync::Arc};

use clap::Parser;
use parking_lot::Mutex;
use wnode::{
  MessageConsumerFace, NodeArgs, ServerArgs, ServerBootstrap, SessionProviderFace, WireFormat,
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
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

/// 单机会话消费者（驱动真正的 RespServerSession 解析与执行）
struct StandaloneConsumer {
  session: Mutex<RespServerSession>,
}

impl StandaloneConsumer {
  fn new(id: u64) -> Self {
    Self {
      session: Mutex::new(RespServerSession::new(
        id as i64,
        RespServerSessionOptions::default(),
      )),
    }
  }
}

impl MessageConsumerFace for StandaloneConsumer {
  fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
    if req_buffer.is_empty() {
      return (0, Vec::new());
    }
    let mut session = self.session.lock();
    match session.try_consume_messages(req_buffer) {
      Some(consumed) => {
        let resp = mem::take(&mut session.output);
        (consumed, resp)
      }
      None => (0, Vec::new()),
    }
  }

  fn dispose(&self) {}
}

/// 单机构造会话提供者
struct StandaloneProvider;

impl SessionProviderFace for StandaloneProvider {
  type Consumer = StandaloneConsumer;

  /// 满足 SessionProviderFace trait 规范，单机会话构造暂不依赖线协议格式
  fn get_session(
    &self,
    _wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<Arc<StandaloneConsumer>> {
    Some(Arc::new(StandaloneConsumer::new(network_sender_id)))
  }
}

fn main() -> wnode::Result<()> {
  let args = StandaloneArgs::parse();
  ServerBootstrap::new(args)
    .banner("WeDB Standalone 单机节点")
    .run(|_args, _noop_cluster| Ok(Arc::new(StandaloneProvider)))
}
