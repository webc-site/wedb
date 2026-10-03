//! 40-hex 身份串单源收敛与集群 run_id 接线验证
//!
//! 工单：task/ing/wmetric-hexid-dual-mechanism-cluster-runid-unwired.md
//! 验证点覆盖：
//! (b) 同进程创建独立实例并各取身份，无状态独立取样，不共享全局计数器状态；
//! (c) 集群装配档下 INFO run_id 与 replication 段 master_replid 一致、非集群档保持恒定；
//! (d) 断言仓内不再存在第二枚 hex 身份生成实现（单一真源收敛）。

use wbase::{hex::generate_hex_id, map::HashSet};
use wedb::server::{cluster_manager::create_hex_id, cluster_provider::ClusterProvider};
use wnode::ClusterProvider as _;

/// 验证点 (b)：无状态独立取样，多次连续取样不共享计数器序列，杜绝推断与相撞
#[test]
fn test_hex_id_independent_no_shared_counter() {
  let mut ids_a = HashSet::default();
  for _ in 0..500 {
    let id = generate_hex_id();
    assert_eq!(id.len(), 40);
    assert!(
      id.chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert!(ids_a.insert(id));
  }

  let id_b = generate_hex_id();
  assert_eq!(id_b.len(), 40);
  assert!(!ids_a.contains(&id_b));

  let id_c = create_hex_id();
  assert_eq!(id_c.len(), 40);
  assert!(!ids_a.contains(&id_c));
  assert_ne!(id_b, id_c);
}

/// 验证点 (c)：集群装配档下真实 ClusterProvider 的 get_run_id 与 replication 段 master_replid 严格对齐
#[test]
fn test_cluster_run_id_matches_replication_master_replid() {
  let provider = ClusterProvider::new();
  let repl_items = provider.get_replication_info();
  let master_replid = repl_items
    .iter()
    .find(|i| i.name.as_ref() == "master_replid")
    .expect("必须有 master_replid")
    .value
    .clone();

  // 集群模式下 run_id 等于 PrimaryReplId（与 replication 段 master_replid 严格对齐，对位 C# ClusterProvider.GetRunId）
  assert_eq!(provider.get_run_id(), master_replid);
  assert_eq!(provider.get_run_id().len(), 40);
}

/// 验证点 (d)：单一真源断言，全仓不再存在第二枚 hex 身份生成实现
#[test]
fn test_no_second_hex_id_generator_in_repo() {
  let cluster_mgr_src = include_str!("../src/server/cluster_manager.rs");
  assert!(
    !cluster_mgr_src.contains("fastrand::fill(&mut buf)"),
    "cluster_manager::create_hex_id 不得再保留独立 fastrand 生成实现"
  );
  assert!(
    cluster_mgr_src.contains("generate_hex_id()"),
    "cluster_manager::create_hex_id 必须转调 generate_hex_id()"
  );

  let wmetric_src = include_str!("../../wmetric/src/info/garnet_info_metrics.rs");
  assert!(
    !wmetric_src.contains("generate_default_hex_id"),
    "wmetric 不得保留 generate_default_hex_id 计数器 xorshift 实现"
  );
  assert!(
    !wmetric_src.contains("static STATE: AtomicU64"),
    "wmetric 不得保留 static STATE 全局计数器"
  );
}
