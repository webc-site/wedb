//! wedb 对外错误单源锁：client 面收口 crate::Error 单域
//!
//! 嵌入宿主组合调用（起服 + 客户端）单一 match 域：C# 拒启与运行错误经
//! ArgumentException / GarnetException / SocketException 同一天然通道上抛
//! 的对位——boot 域经 Error::Node 透明变体、client 域经 Error::Conn 透明
//! 变体入源，宿主不再直触 wnode::Result / wconn::Result。
//!
//! 册头 `#![cfg(not(feature = "tls"))]` 为 wedb 特性门：
//! 默认特性形态可见，--all-features 下该册出局。boot 域端到端拒启面
//! （TLS 门禁经 Node 变体透传 + InvalidArgument「未启用 tls 特性」文案）
//! 单册承接于同 crate 册 `tests/run_cluster_server_tls_gate.rs`；输入面
//! （四枚 `--tls-*` 旗标经 `ClusterArgs::from_args_iter` 落进 `NodeArgs`
//! 并点亮 `has_tls()`）由不受特性门的同 crate 册
//! `tests/tls_flags_node_projection.rs` 承接。本册守 client Conn 变体与
//! From 链编译锁两面。
#![cfg(not(feature = "tls"))]

use std::error::Error;

use wedb::{client::GarnetClient, error};

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
