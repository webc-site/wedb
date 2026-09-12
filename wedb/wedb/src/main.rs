//! WeDB 分布式集群服务端主程序入口

use std::sync::Arc;

use clap::Parser;
use parking_lot::Mutex;
use wedb::{
  ClusterArgs, WedbClusterProvider,
  server::{
    cluster::{IClusterProvider, IClusterSession},
    cluster_provider::ClusterProvider,
  },
};
use wnode::{MessageConsumerFace, ServerBootstrap, SessionProviderFace, WireFormat};
use wresp::{
  cmd_strings::{RESP_EMPTYLIST, RESP_OK, RESP_PONG},
  parse_resp_frame,
};

/// 集群会话消费者（真实对接 IClusterSession 处理集群协议与槽位路由）
struct ClusterConsumer {
  cluster_session: Mutex<Box<dyn IClusterSession>>,
}

impl ClusterConsumer {
  // _id: 预留网络发送端 ID 供会话诊断跟踪
  fn new(_id: u64, cluster_session: Box<dyn IClusterSession>) -> Self {
    Self {
      cluster_session: Mutex::new(cluster_session),
    }
  }

  /// 处理真实 RESP 集群命令与槽位验证路由
  fn process_command(&self, args: &[&[u8]]) -> Vec<u8> {
    if args.is_empty() {
      return Vec::new();
    }
    let mut output = Vec::with_capacity(64);
    let session = self.cluster_session.lock();
    let cmd = args[0];

    if cmd.eq_ignore_ascii_case(b"PING") {
      output.extend_from_slice(RESP_PONG);
      return output;
    }

    if cmd.eq_ignore_ascii_case(b"COMMAND") {
      output.extend_from_slice(RESP_EMPTYLIST);
      return output;
    }

    if cmd.eq_ignore_ascii_case(b"READONLY") {
      session.set_read_write_session(false);
      output.extend_from_slice(RESP_OK);
      return output;
    }

    if cmd.eq_ignore_ascii_case(b"READWRITE") {
      session.set_read_write_session(true);
      output.extend_from_slice(RESP_OK);
      return output;
    }

    if cmd.eq_ignore_ascii_case(b"CLUSTER") {
      session.process_cluster_commands(&args[1..], &mut output);
      return output;
    }

    // 数据读写命令：槽位校验与 -MOVED / -ASK 重定向（对标 Garnet NetworkIterativeSlotVerify）
    if args.len() >= 2 {
      let key = args[1];
      let read_only = !session.is_read_write_session();
      if !session.network_iterative_slot_verify(key, read_only, false)
        && let Some(err) = session.take_cached_slot_error()
      {
        return err;
      }
    }

    output.extend_from_slice(RESP_OK);
    output
  }
}

impl MessageConsumerFace for ClusterConsumer {
  fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
    let Some((consumed, args)) = parse_resp_frame(req_buffer) else {
      return (0, Vec::new());
    };
    let response = self.process_command(&args);
    (consumed, response)
  }

  fn dispose(&self) {
    self.cluster_session.lock().dispose();
  }
}

/// 集群会话提供者
struct ClusterNodeSessionProvider {
  provider: Arc<ClusterProvider>,
}

impl SessionProviderFace for ClusterNodeSessionProvider {
  type Consumer = ClusterConsumer;

  /// 满足 SessionProviderFace trait 规范，按发送端 id 装配带有真实集群会话的消费者
  fn get_session(
    &self,
    _wire_format: WireFormat,
    network_sender: u64,
  ) -> Option<Arc<ClusterConsumer>> {
    let cluster_session = self.provider.create_cluster_session();
    Some(Arc::new(ClusterConsumer::new(
      network_sender,
      cluster_session,
    )))
  }
}

fn main() -> wnode::Result<()> {
  let args = ClusterArgs::parse();
  let cluster_provider = WedbClusterProvider::new();
  let provider_inner = Arc::clone(&cluster_provider.inner);

  ServerBootstrap::new(args)
    .with_cluster_provider(cluster_provider)
    .banner("WeDB 分布式集群节点")
    .run(move |_args, _cluster| {
      Ok(Arc::new(ClusterNodeSessionProvider {
        provider: provider_inner,
      }))
    })
}
