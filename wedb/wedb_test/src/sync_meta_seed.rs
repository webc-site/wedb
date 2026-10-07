//! 全量同步请求 meta 单源（副本零位点 FullResync 判据形态）
//!
//! 收口 diskless 系逐字同形的 SyncMetadata 构造：full_sync=false、副本角色、
//! 主从历史不一致（主复制 id 取副本端 rm）、零位点起止、无检查点历史。
//! 消费面经 `wedb_test::sync_meta_seed` 引用（原 common/ 直挂面已收口进本 crate）。
//!

use std::sync::Arc;

use waof::AofAddress;
use wedb::server::{
  replication::{replication_manager::ReplicationManager, sync_metadata::SyncMetadata},
  worker::NodeRole,
};

/// 副本零位点全量同步请求 meta（两端历史不一致 + 副本零位点 → FullResync）
pub fn full_resync_meta(replica_id: u128, rm_r: &Arc<ReplicationManager>) -> SyncMetadata {
  SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: replica_id,
    current_primary_repl_id: rm_r.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: None,
  }
}
