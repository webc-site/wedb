#![cfg(not(feature = "tls"))]

//! 非 TLS 构建下携带 TLS 参数拒绝启动的集成断言（zcode-r37-tlsfix 新面一
//! 防回归：守卫单点在 [`wnode::ServerBootstrap::run_async`]，覆盖单机/集群
//! 全宿主；boot.rs 曾自持一道逐字节同文门已删，本用例锁死「wnode 单点 +
//! 非 tls 构建即拒」的契约）
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/RespTlsTests.cs
//!（C# 无此门：TlsOptions 缺席即静默明文，rust 收紧向刻意分叉）

use std::{path::PathBuf, sync::Arc};

use wconf::NodeArgs;
use wnode::{
  Error, Result, ServerBootstrap,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

/// 哑会话提供者（装配回调不得被到达，仅满足泛型约束）
struct NeverProvider;

/// 哑消费者
struct NeverConsumer;

impl MessageConsumerFace for NeverConsumer {
  fn try_consume_messages_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
    None
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    Vec::new()
  }
  fn return_recv_scratch(&mut self, _buf: Vec<u8>) {}
  fn dispose(&mut self) {}
}

impl SessionProviderFace for NeverProvider {
  type Consumer = NeverConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<NeverConsumer> {
    None
  }
}

#[test]
fn tls_args_rejected_at_bootstrap_without_tls_feature() {
  let mut args = NodeArgs::default();
  args.tls_cert = Some(PathBuf::from("/tmp/never-reached.cert"));

  let err = ServerBootstrap::new(args)
    .run_async(|_, _| async {
      Err(Error::InvalidArgument("装配回调不得到达".into())) as Result<Arc<NeverProvider>>
    })
    .expect_err("非 tls 构建携带 TLS 参数必须拒绝启动");

  assert!(
    err.to_string().contains("静默降级为明文"),
    "错误须点名静默降级守卫，实际: {err}"
  );
}
