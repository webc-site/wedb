//! 集群槽位验证输入（libs/server/Cluster/ClusterSlotVerificationInput.cs:ClusterSlotVerificationInput）

use super::key_spec::SimpleRespKeySpec;

/// 集群槽位验证输入参数
#[derive(Debug, Clone, Default)]
pub struct ClusterSlotVerificationInput {
  /// 是否只读命令
  pub read_only: bool,
  /// 命令是否启用了 ASKING
  pub session_asking: u8,
  /// 提取键位置的简化键规格
  pub key_specs: Vec<SimpleRespKeySpec>,
  /// 是否子命令
  pub is_sub_command: bool,
  /// 是否要求槽位稳定（例如写型向量集命令）
  pub wait_for_stable_slot: bool,
}
