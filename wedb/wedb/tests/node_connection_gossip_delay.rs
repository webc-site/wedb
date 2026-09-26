//! gossip 建连等待面 gossip_delay 界集成测试（zcode-r24-gossip）
//!
//! SYN 黑洞形态（主机静默宕机、网络分区、防火墙丢包——恰是故障检测最需要
//! 工作的场景）下，initialize_async 的建连等待必须以 gossip_delay 为上界
//! 到点返回（对标 C# GarnetServerNode.InitializeAsync 的
//! `ReconnectAsync().WaitAsync(gossipDelay)`），不得以 facade 内层
//! cluster_node_timeout 回落值（DEFAULT_CONNECT_TIMEOUT_MS 5 秒）钉死等待
//! 面；超时放弃不回退 initialized 单次语义，重入走快速短路返回。
//!
//! 目标地址 203.0.113.1:7000（TEST-NET-3 官方保留网段，路由黑洞形态）；
//! 环境对保留网段立即报不可达时退化为快速失败分支，两分支契约同为到点
//! 返回且未连接。

use std::{
  sync::atomic::Ordering,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wedb::server::{cluster_provider::ClusterProvider, gossip::node_connection::NodeConnection};

/// gossip_delay 测试值：远小于 facade 内层回落限时 5 秒，保证断言上界
/// （2 秒）对「界缺失」形态（等待逼近 5 秒）有区分度
const GOSSIP_DELAY: Duration = Duration::from_millis(200);

#[test]
fn initialize_async_bounded_by_gossip_delay() {
  let cp = ClusterProvider::new();
  let conn = NodeConnection::new(0xBEEF, "203.0.113.1".into(), 7000, &cp);

  Runtime::new().unwrap().block_on(async {
    let start = Instant::now();
    conn.initialize_async(GOSSIP_DELAY).await;
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(2),
      "建连等待应被 gossip_delay({GOSSIP_DELAY:?}) 界住，实测 {elapsed:?}"
    );
    assert!(!conn.is_connected(), "黑洞地址不得建连成功");
    assert!(
      conn.initialized.load(Ordering::Acquire),
      "超时放弃不回退 initialized 单次语义"
    );

    // 超时放弃后重入走 initialized 快速短路返回，不得再次建连等待
    let reentry = Instant::now();
    conn.initialize_async(GOSSIP_DELAY).await;
    assert!(
      reentry.elapsed() < GOSSIP_DELAY,
      "重入应经 initialized 快速短路返回，不得重复建连等待"
    );
  });
}
