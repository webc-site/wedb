//! 双主 provider 基线形单源（全量槽 + 单槽让位 + 100ms 栅栏超时）
//!
//! cluster_migration / cluster_migration_domain / migrate_fail_inject /
//! reviv_pause_migration_interleave 四册同名 `two_primary_provider` wrapper
//! 的同参收敛：DE11 本地主持全量槽、DE12 远端主@7001 接管 `remote_slot`
//! 单槽、100ms 栅栏超时。消费面经 `wedb_test::two_primary_provider_100ms`
//! 引用（原 common/ `#[path]` 直挂面已收口进本 crate）。

use std::sync::Arc;

use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::cluster_provider::ClusterProvider;

use crate::two_primary_provider::two_primary_provider;

/// 装配双主 provider 基线形（`remote_slot`：远端让位单槽，REMOTE_SLOT /
/// SLOT0^1 系各册原值保留）
pub fn two_primary_provider_100ms(remote_slot: u16) -> Arc<ClusterProvider> {
  two_primary_provider(
    Some(100),
    CLUSTER_SLOT_COUNT,
    CLUSTER_SLOT_COUNT,
    &[remote_slot],
    None,
  )
}
