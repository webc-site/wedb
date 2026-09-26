//! wedb 对外错误单源锁：boot / client 两面收口 crate::Error 单域
//!
//! 嵌入宿主组合调用（起服 + 客户端）单一 match 域：C# 拒启与运行错误经
//! ArgumentException / GarnetException / SocketException 同一天然通道上抛
//! 的对位——boot 域经 Error::Node 透明变体、client 域经 Error::Conn 透明
//! 变体入源，宿主不再直触 wnode::Result / wconn::Result。
#![cfg(not(feature = "tls"))]

use std::error::Error;

use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, client::GarnetClient, error, run_cluster_server};

/// boot 拒启（TLS 门禁，guard_no_tls 在 wnode run_async 内）错误经
/// [`wedb::error::Error::Node`] 透明变体透传可 match（Node 变体 From 链
/// 编译期锁的运行时对偶）
#[test]
fn boot_tls_gate_error_routed_through_node_variant() {
  for flag in [
    "--tls-cert",
    "--tls-key",
    "--tls-issuer-cert",
    "--tls-client-target-host",
  ] {
    let args = ClusterArgs::from_args_iter(["wedb", flag, "test_val"]).unwrap();
    let err = run_cluster_server(args, None).expect_err("必须拒绝启动");
    assert!(
      matches!(err, error::Error::Node(_)),
      "TLS 门禁拒启错误须经 Node 变体透传: {err:?}"
    );
  }
}

/// client 建连失败落 [`wedb::error::Error::Conn`] 变体（facade 收口后宿主
/// 单一 match 域锁）
#[compio::test]
async fn client_connect_failure_lands_conn_variant() {
  let client = GarnetClient::with_config("127.0.0.1:1".to_string(), None, None, 100, None);
  let err = client.connect_async().await.expect_err("保留端口须连不上");
  assert!(
    matches!(err, error::Error::Conn(_)),
    "client 建连失败须落 Conn 变体: {err:?}"
  );
}

/// From 链编译期锁：client 域（wconn）与节点域（wnode）错误均可透明入源，
/// 缺变体即编译失败
#[test]
fn error_from_chain_compile_lock() {
  fn assert_transparent<E: Error + From<U>, U: Error>() {}
  assert_transparent::<error::Error, wconn::Error>();
  assert_transparent::<error::Error, wnode::Error>();
}
