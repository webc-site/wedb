//! 集群会话抽象面（对标 libs/server/Cluster/IClusterSession.cs）

use super::cluster_slot_verification_input::ClusterSlotVerificationInput;

/// 集群会话抽象接口（dyn 兼容）
pub trait IClusterSession: Send + Sync {
  /// 远程节点 ID
  fn remote_node_id(&self) -> Option<String>;
  /// 设置远程节点 ID
  fn set_remote_node_id(&self, id: Option<String>);

  /// 是否为读写会话（READWRITE 命令置位）
  fn is_read_write_session(&self) -> bool;
  /// 设置读写会话状态
  fn set_read_write_session(&self, rw: bool);

  /// 是否为复制会话
  fn is_replicating(&self) -> bool;
  /// 设置复制会话状态
  fn set_replicating(&self, rep: bool);

  /// 内部写标记
  fn internal_write(&self) -> bool;
  /// 设置内部写标记
  fn set_internal_write(&self, val: bool);

  /// 获取当前保护纪元（无锁或短锁）
  fn acquire_current_epoch(&self);
  /// 释放当前保护纪元
  fn release_current_epoch(&self);

  /// 单键槽位迭代校验与重定向判定
  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool, asking: bool) -> bool;

  /// 多键槽位校验与重定向判定
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput,
    args: &[&[u8]],
  ) -> bool;

  /// 获取缓存的槽位重定向错误响应（如 -MOVED / -ASK）
  fn take_cached_slot_error(&self) -> Option<Vec<u8>>;

  /// 处理 CLUSTER 子命令
  fn process_cluster_commands(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool;

  /// 析构清理
  fn dispose(&self);
}
