//! CLUSTER INFO 输出回归测试：钉死槽位/节点/ epoch 字段经 slot_state_counts
//! 单点真实取值，且 cluster_state 与 messages 计数按 C# 原作字面量口径输出

use std::sync::Arc;

use wedb::server::{
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  worker::{LocalWorkerSpec, NodeRole},
};

/// 从 INFO 文本逐行取 `key:value` 的值
fn field<'a>(info: &'a str, key: &str) -> &'a str {
  let prefix = format!("{key}:");
  info
    .lines()
    .find_map(|line| line.strip_prefix(&prefix))
    .unwrap_or_else(|| panic!("INFO 缺少字段 {key}"))
}

/// 槽位演化必须逐字段反映到 INFO 输出，杜绝假 0 固定值回退
#[test]
fn cluster_info_tracks_real_slot_state() {
  let m = ClusterManager::new(Arc::new(ClusterProvider::default()));
  m.current_config
    .write()
    .initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 3,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });

  // 初始无主态：四个槽位计数全 0
  let info = m.get_info();
  assert_eq!(field(&info, "cluster_slots_assigned"), "0");
  assert_eq!(field(&info, "cluster_slots_ok"), "0");
  assert_eq!(field(&info, "cluster_slots_pfail"), "0");
  assert_eq!(field(&info, "cluster_slots_fail"), "0");

  // 3 槽有主后 assigned/ok 随单点计数上浮
  m.current_config
    .write()
    .assign_slots(&[0, 1, 2], 1, SlotState::Stable);
  let info = m.get_info();
  assert_eq!(field(&info, "cluster_slots_assigned"), "3");
  assert_eq!(field(&info, "cluster_slots_ok"), "3");

  // 1 槽转 fail：有主判定降为 2，pfail/fail 同步反映 fail 计数
  //（C# 原作 pfail 列复用 FAIL 计数，字面口径不"修正"）
  m.current_config
    .write()
    .assign_slots(&[2], 1, SlotState::Fail);
  let info = m.get_info();
  assert_eq!(field(&info, "cluster_slots_assigned"), "2");
  assert_eq!(field(&info, "cluster_slots_ok"), "2");
  assert_eq!(field(&info, "cluster_slots_pfail"), "1");
  assert_eq!(field(&info, "cluster_slots_fail"), "1");

  // 节点与 epoch 字段取配置真实值
  assert_eq!(field(&info, "cluster_known_nodes"), "1");
  assert_eq!(field(&info, "cluster_size"), "1");
  assert_eq!(field(&info, "cluster_current_epoch"), "3");
  assert_eq!(field(&info, "cluster_my_epoch"), "3");

  // C# 原作字面量口径：真实 gossip 计数经 GossipStats 单点在 INFO stats 段
  // 暴露（端到端见 info_resetstat_arms.rs），CLUSTER INFO 不重复挂接
  assert_eq!(field(&info, "cluster_state"), "ok");
  assert_eq!(field(&info, "cluster_stats_messages_sent"), "0");
  assert_eq!(field(&info, "cluster_stats_messages_received"), "0");
}
