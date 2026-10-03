//! 双主 provider 装配单源（DE11 本地主 + DE12 远端主两 worker 槽位图）
//!
//! 收口 9 册逐字同形的 `two_primary_provider` 装配体。语义锚：
//! - `ClusterProvider::new` 恒装新 `ClusterManager`，`cluster_manager()`
//!   恒 `Some`（原两形态 `unwrap` / `unwrap_or_else` 兜底死分支同义）；
//! - worker 表 \[0\] 保留、\[1\] 为本地（DE11）、push 后 \[2\] 为远端（DE12），
//!   `workers.len()` 于 push 前取值恒 2，即原各副本的 `remote_worker_id`
//!   （含 cluster_iterative_slot_verify 硬编码的 `worker_id: 2`）；
//! - 槽位图 [0, local_end) 填本地 Stable，[remote_tail, CLUSTER_SLOT_COUNT)
//!   整段与 `extra_remote` 单点改挂远端（单槽覆盖 / 后半整段两形态并容）。
//!
//! 拓扑差异面（栅栏超时、本地/远端分界、单槽覆盖、hostname）全部显式参，
//! 由调用方薄 wrapper 传值。消费面经 `wedb_test::two_primary_provider` 引用；
//! 节点身份单源见 `wedb_test::{de11_node_id, de12_node_id}`。

use std::sync::Arc;

use hipstr::HipStr;
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

use crate::{de11_node_id::DE11_NODE_ID, de12_node_id::DE12_NODE_ID};

/// 装配双主 provider（DE11 本地主 :7000 + DE12 远端主 :7001）
///
/// - `fence_timeout_ms`：原副本 `set_cluster_node_timeout_ms` 实参（100 /
///   用例常量）；`None` = 不调该 setter，保留 provider 默认
/// - `local_end`：本地 Stable 填充上界（[0, local_end)），全量本地拓扑传
///   `CLUSTER_SLOT_COUNT`，diskless 半分拓扑传 8192
/// - `remote_tail`：远端整段填充下界（[remote_tail, CLUSTER_SLOT_COUNT)），
///   无整段形态传 `CLUSTER_SLOT_COUNT`（空段），diskless 半分拓扑传 8192
/// - `extra_remote`：改挂远端 worker 的单槽清单（REMOTE_SLOT / SLOT0^1 系）
/// - `hostnames`：Some((本地, 远端)) = 双 worker 注入显式 hostname（CLUSTER
///   迭代槽校验系）；None = 双 worker hostname 均空
pub fn two_primary_provider(
  fence_timeout_ms: Option<u64>,
  local_end: usize,
  remote_tail: usize,
  extra_remote: &[u16],
  hostnames: Option<(&str, &str)>,
) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: DE11_NODE_ID,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: hostnames.map(|(local, _)| local),
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(DE12_NODE_ID),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: hostnames.map(|(_, remote)| HipStr::from(remote)),
    });
    for slot in 0..local_end {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in remote_tail..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
    for &slot in extra_remote {
      config.slot_map[slot as usize] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
  }
  if let Some(ms) = fence_timeout_ms {
    cp.set_cluster_node_timeout_ms(ms);
  }
  cp
}
