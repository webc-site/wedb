//! provider 集群管理器或取或建单源
//!
//! 收口 cluster_slot_keys_alloc_fail_smooth /
//! cluster_slot_verify_redirect_clusterdown / cluster_swapdb_slot_state 三册
//! 逐字同形的「cluster_manager 取用、缺则新建并写回 provider」装配。消费面经 `wedb_test::cluster_manager_init` 引用（原 common/ 直挂面
//! 已收口进本 crate）。
//!

use std::sync::Arc;

use wedb::server::{cluster_manager::ClusterManager, cluster_provider::ClusterProvider};

/// provider 的集群管理器：在册即取，缺则由 provider 自持 Arc 新建并写回
///（裸壳 provider 预置拓扑的统一入口；`ClusterProvider::new` 装配形态恒有
/// self_arc，`unwrap` 沿各册原 `unwrap_or_else` 臂语义）
pub fn or_init_cluster_manager(cp: &ClusterProvider) -> Arc<ClusterManager> {
  cp.cluster_manager().unwrap_or_else(|| {
    let m = Arc::new(ClusterManager::new(cp.self_arc().unwrap()));
    *cp.cluster_manager.write() = Some(Arc::clone(&m));
    m
  })
}
