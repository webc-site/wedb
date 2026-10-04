//! forward_cluster_provider! 宏清单完整性防回归（经 Handle 即 `Arc<dyn>` 路径逐项钉死）
//!
//! [`wnode::cluster_provider`] 的 `Arc<T>` 转发层以宏枚举展开，漏列不报编译
//! 错、静默落 trait 默认体。历史缺陷：`add_new_checkpoint_entry` 漏列令
//! `Arc<dyn ClusterProviderFace>` 层（产线接收器 `OnceLock<ClusterProviderHandle>`，
//! boot.rs 注入）调用恒 None，方法解析命中 `Arc<T>` 层即止、永不 deref 到
//! wedb 真实现，集群形态检查点登记链旁路。本文件以记录型桩实现 trait 全部
//! 26 项、经 Handle 逐方法断言转发到位：宏清单与 trait 声明一损俱损。

use std::sync::Arc;

use parking_lot::Mutex;
use waof::AofAddress;
use wnode::{
  ClusterProvider as ClusterProviderFace, ClusterProviderHandle, resp::slow_path::SlowFuture,
  role_info::RoleInfo, session_parse_state_extensions::ManagerType,
};
use wresp::metrics::MetricsItem;

/// 记录型桩：trait 每个方法都覆写，返回哨兵值并登记调用名
///
/// 哨兵一律取 trait 默认体的反面（或非空容器），转发层漏列即落默认体，
/// 对应断言当场炸断
struct RecordingProvider {
  calls: Mutex<Vec<&'static str>>,
}

impl RecordingProvider {
  fn new() -> Self {
    Self {
      calls: Mutex::new(Vec::new()),
    }
  }

  fn hit(&self, name: &'static str) {
    self.calls.lock().push(name);
  }
}

impl ClusterProviderFace for RecordingProvider {
  fn is_cluster_enabled(&self) -> bool {
    self.hit("is_cluster_enabled");
    true
  }

  fn is_slot_local_stable(&self, _slot: u16) -> bool {
    self.hit("is_slot_local_stable");
    true
  }

  fn start(&self) {
    self.hit("start");
  }

  fn flush_config(&self) {
    self.hit("flush_config");
  }

  fn dispose(&self) {
    self.hit("dispose");
  }

  fn update_cluster_auth(&self, _username: Option<String>, _password: Option<String>) {
    self.hit("update_cluster_auth");
  }

  fn set_cluster_node_timeout_ms(&self, _ms: u64) {
    self.hit("set_cluster_node_timeout_ms");
  }

  fn is_primary(&self) -> bool {
    self.hit("is_primary");
    false
  }

  fn is_replica(&self) -> bool {
    self.hit("is_replica");
    true
  }

  fn is_replica_node(&self, _node_id: u128) -> bool {
    self.hit("is_replica_node");
    true
  }

  fn get_run_id(&self) -> String {
    self.hit("get_run_id");
    "run-id".to_string()
  }

  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    self.hit("get_primary_info");
    (AofAddress::create(3, 7), Vec::new())
  }

  fn get_replica_info(&self) -> RoleInfo {
    self.hit("get_replica_info");
    RoleInfo::default()
  }

  fn get_replication_info(&self) -> Vec<MetricsItem> {
    self.hit("get_replication_info");
    vec![MetricsItem::new("k", "v")]
  }

  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    self.hit("get_checkpoint_info");
    vec![MetricsItem::new("k", "v")]
  }

  fn get_gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    self.hit("get_gossip_stats");
    vec![MetricsItem::new("k", "v")]
  }

  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    self.hit("get_buffer_pool_stats");
    vec![MetricsItem::new("k", "v")]
  }

  fn purge_buffer_pool(&self, _manager_type: ManagerType) {
    self.hit("purge_buffer_pool");
  }

  fn reset_gossip_stats(&self) {
    self.hit("reset_gossip_stats");
  }

  fn aof_sublog_count(&self) -> usize {
    self.hit("aof_sublog_count");
    42
  }

  fn flushall_broadcast(&self, _ns: u64) -> Option<SlowFuture> {
    self.hit("flushall_broadcast");
    Some(SlowFuture::new(async { Vec::new() }))
  }

  fn checkpoint_version_shift_start(&self, _new_version: i64) {
    self.hit("checkpoint_version_shift_start");
  }

  fn checkpoint_version_shift_end(&self, _new_version: i64) {
    self.hit("checkpoint_version_shift_end");
  }

  fn on_checkpoint_initiated(&self, covered: &mut AofAddress) {
    self.hit("on_checkpoint_initiated");
    *covered = AofAddress::create(1, 99);
  }

  fn add_new_checkpoint_entry(
    &self,
    _full: bool,
    _covered: AofAddress,
    _store_checkpoint_token: u128,
    _object_store_checkpoint_token: u128,
  ) -> Option<SlowFuture> {
    self.hit("add_new_checkpoint_entry");
    Some(SlowFuture::new(async { Vec::new() }))
  }

  fn is_device_contaminated(&self) -> bool {
    self.hit("is_device_contaminated");
    true
  }
}

#[test]
fn handle_path_forwards_every_trait_method() {
  let provider = Arc::new(RecordingProvider::new());
  // 产线接收器同型：OnceLock<ClusterProviderHandle> 即 Arc<dyn ClusterProviderFace>
  let handle: ClusterProviderHandle = provider.clone();

  // 值面：哨兵逐项对默认体反面断言（漏列即落默认体，此处即炸）
  assert!(handle.is_cluster_enabled());
  assert!(handle.is_slot_local_stable(7));
  assert!(!handle.is_primary());
  assert!(handle.is_replica());
  assert!(handle.is_replica_node(9));
  assert_eq!(handle.get_run_id(), "run-id");
  assert_eq!(handle.get_primary_info().0.get(0), Some(7));
  assert_eq!(handle.aof_sublog_count(), 42);
  assert_eq!(handle.get_replication_info().len(), 1);
  assert_eq!(handle.get_checkpoint_info().len(), 1);
  assert_eq!(handle.get_gossip_stats(false).len(), 1);
  assert_eq!(handle.get_buffer_pool_stats().len(), 1);
  assert!(handle.flushall_broadcast(1).is_some());
  assert!(handle.is_device_contaminated());

  // 出参面：on_checkpoint_initiated 经 Handle 写入 covered
  let mut covered = AofAddress::default();
  handle.on_checkpoint_initiated(&mut covered);
  assert_eq!(covered.get(0), Some(99));

  // P1 本体：add_new_checkpoint_entry 经 Handle 必 Some——宏清单漏列即恒
  // None 的历史缺陷钉死处（集群形态主库检查点后条目登记与安全截断的入口）
  let slow = handle.add_new_checkpoint_entry(true, covered, 1, 1);
  assert!(
    slow.is_some(),
    "forward_cluster_provider! 漏列 add_new_checkpoint_entry 即此处恒 None"
  );

  // 副作用面：void 方法逐个触达
  handle.start();
  handle.flush_config();
  handle.dispose();
  handle.update_cluster_auth(None, None);
  handle.set_cluster_node_timeout_ms(5);
  handle.purge_buffer_pool(ManagerType::ReplicationManager);
  handle.reset_gossip_stats();
  handle.checkpoint_version_shift_start(1);
  handle.checkpoint_version_shift_end(1);
  handle.get_replica_info();

  // 全量核对：26 项全部经 Handle 转发到内层真实现（新增 trait 方法而漏补
  // 宏清单时，上方哨兵断言与本次数核对双重兜底）
  let mut calls = provider.calls.lock().clone();
  calls.sort_unstable();
  let mut expected = [
    "is_cluster_enabled",
    "is_slot_local_stable",
    "start",
    "flush_config",
    "dispose",
    "update_cluster_auth",
    "set_cluster_node_timeout_ms",
    "is_primary",
    "is_replica",
    "is_replica_node",
    "get_run_id",
    "get_primary_info",
    "get_replica_info",
    "get_replication_info",
    "get_checkpoint_info",
    "get_gossip_stats",
    "get_buffer_pool_stats",
    "purge_buffer_pool",
    "reset_gossip_stats",
    "aof_sublog_count",
    "flushall_broadcast",
    "checkpoint_version_shift_start",
    "checkpoint_version_shift_end",
    "on_checkpoint_initiated",
    "add_new_checkpoint_entry",
    "is_device_contaminated",
  ]
  .to_vec();
  expected.sort_unstable();
  assert_eq!(calls, expected, "trait 全量方法必须逐项经 Handle 转发到位");
}
