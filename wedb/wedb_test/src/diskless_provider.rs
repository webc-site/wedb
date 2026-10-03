//! 无盘系角色 provider 单源（`provider_with_role` 攒批窗关窗直通形）
//!
//! 收口 diskless 系九册逐字同形的 `provider_with_role(..., Some(0))` 装配：
//! 端口与槽位图形态按册参数化，攒批窗恒 Some(0) 关窗。消费面经
//! `wedb_test::diskless_provider` 引用（原 common/ `#[path]` 直挂面已收口进
//! 本 crate）。

use std::sync::Arc;

use wedb::server::{cluster_provider::ClusterProvider, worker::NodeRole};

use crate::node_storage::{NodeStorage, provider_with_role};

/// 无盘系角色 provider（`provider_with_role` 直通：攒批窗恒关窗 Some(0)；
/// `primary_id` 为副本角色挂靠的主端 ID，主端角色原样透传）
pub fn diskless_provider(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
  primary_id: u128,
  stable_slots: bool,
) -> Arc<ClusterProvider> {
  provider_with_role(node, node_id, port, role, primary_id, stable_slots, Some(0))
}
