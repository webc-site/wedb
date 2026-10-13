#![recursion_limit = "256"]
//! 集群节点超时换算契约（对标 garnet/test/cluster/GarnetServerNodeTimeoutTests.cs：
//! gossip 客户端超时此前误用 TimeSpan.Milliseconds 亚秒分量而非全量毫秒，
//! rust 侧换算单点为 boot.rs `cluster_node_timeout_seed_secs`（CLI 毫秒 →
//! CONFIG 秒槽，i32 饱和）与 `ClusterProvider::cluster_node_timeout`
//! （0 哨兵 → None = 关闭））

use std::time::Duration;

use wedb::server::{boot::cluster_node_timeout_seed_secs, cluster_provider::ClusterProvider};

/// C# `ClientTimeoutUsesTotalMilliseconds`：整秒配置全量换算，不丢亚秒分量
///（C# 入参为秒、断言毫秒；rust 入参为 CLI 毫秒、断言整秒槽，同一换算链的
/// 反向投影，60s 生产缺省在册）
#[test]
fn client_timeout_uses_total_milliseconds() {
  assert_eq!(cluster_node_timeout_seed_secs(1_000), 1);
  assert_eq!(cluster_node_timeout_seed_secs(5_000), 5);
  assert_eq!(cluster_node_timeout_seed_secs(60_000), 60);
  assert_eq!(cluster_node_timeout_seed_secs(3_600_000), 3_600);
}

/// C# `NonPositiveClusterTimeoutDisablesClientTimeout`：非正配置 = 关闭超时。
/// rust 侧 0 哨兵两段承接：种子槽保 0（无限回显），provider 读取口 0 → None
///（不挂计时器；u64 槽无负值形态，负值域由 CLI 解析层拒收）
#[test]
fn non_positive_cluster_timeout_disables_client_timeout() {
  assert_eq!(cluster_node_timeout_seed_secs(0), 0);

  let provider = ClusterProvider::new();
  provider.set_cluster_node_timeout_ms(0);
  assert_eq!(provider.cluster_node_timeout(), None);
}

/// C# `LargeClusterTimeoutClampsToIntMaxValue`：大值 ×1000 溢出 int 时钳
/// i32::MAX，绝不允许环绕成负触发「非正即无限」反转
#[test]
fn large_cluster_timeout_clamps_to_int_max_value() {
  // C# 2_147_484 s ×1000 溢出用例的毫秒域对位：i32::MAX+1 秒的毫秒数
  assert_eq!(
    cluster_node_timeout_seed_secs(i32::MAX as u64 * 1000),
    i32::MAX as i64
  );
  assert_eq!(
    cluster_node_timeout_seed_secs(i32::MAX as u64 * 1000 + 1000),
    i32::MAX as i64
  );
  // u64::MAX 档与 cluster_node_timeout_subsecond_gate.rs 的钳位断言互为两章程
  // （彼处锁拒启门+回显一致性，此处锁 C# 换算回归映射），逐字重复系有意双章
  assert_eq!(cluster_node_timeout_seed_secs(u64::MAX), i32::MAX as i64);
}

/// provider 读取口全量毫秒保真：注入毫秒槽原样成 Duration，消费方
/// `NodeConnection` 的 `as_millis` 投影往返无损（C# 换算回归的 rust 面孔）
#[test]
fn provider_timeout_roundtrips_full_milliseconds() {
  let provider = ClusterProvider::new();
  provider.set_cluster_node_timeout_ms(60_000);
  assert_eq!(
    provider.cluster_node_timeout(),
    Some(Duration::from_secs(60))
  );

  provider.set_cluster_node_timeout_ms(1_500);
  assert_eq!(
    provider
      .cluster_node_timeout()
      .map(|d| d.as_millis() as u64),
    Some(1_500),
    "亚秒分量不得丢失（C# TimeSpan.Milliseconds 回归核心）"
  );
}
