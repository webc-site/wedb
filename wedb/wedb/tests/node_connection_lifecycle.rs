//! gossip 节点连接生命周期集成测试（自 src/server/gossip/node_connection.rs
//! 内嵌测试 1:1 迁出：跨进程边界（对 127.0.0.1:7001 的真实连接尝试），
//! 按集成测试归位）
//!
//! 前提：本机 7001 端口无监听，initialize_async 走「连接失败但标记已
//! 初始化」路径，后续断言锁定未连接状态下的短路行为
//!（publish 短路、dispose 单次 CAS 幂等、时间戳/连接信息自洽）。

use std::sync::atomic::Ordering;

use compio::runtime::Runtime;
use wedb::server::{cluster_provider::ClusterProvider, gossip::node_connection::NodeConnection};

#[test]
fn test_node_connection_lifecycle() {
  Runtime::new().unwrap().block_on(async {
    let cp = ClusterProvider::new();
    let conn = NodeConnection::new(
      0x0000_0000_0000_0000_0000_0000_0000_0DE1,
      "127.0.0.1".into(),
      7001,
      &cp,
    );

    assert_eq!(conn.node_id, 0x0000_0000_0000_0000_0000_0000_0000_0DE1);
    assert_eq!(conn.address, "127.0.0.1");
    assert_eq!(conn.port, 7001);
    assert!(!conn.initialized.load(Ordering::Acquire));
    assert!(!conn.disposed.load(Ordering::Acquire));

    // 首次 initialize_async：标记 initialized，尝试连接
    conn.initialize_async().await;
    assert!(conn.initialized.load(Ordering::Acquire));

    // 再次调用：快速短路返回
    conn.initialize_async().await;
    assert!(conn.initialized.load(Ordering::Acquire));

    // 未连接状态下尝试 publish：不阻塞、记录日志并直接返回
    conn
      .try_cluster_publish_async(false, b"test-chan", b"test-msg")
      .await;

    // 时间戳更新与连接信息获取
    conn.update_send_time();
    conn.update_recv_time();
    let info = conn.get_connection_info();
    assert!(!info.connected);
    assert!(info.ping > 0);
    assert!(info.pong > 0);

    // 释放连接：单次 CAS 幂等
    conn.dispose();
    assert!(conn.disposed.load(Ordering::Acquire));
    conn.dispose();

    // 释放后再次 publish：直接短路
    conn
      .try_cluster_publish_async(true, b"test-chan", b"test-msg")
      .await;
  });
}
