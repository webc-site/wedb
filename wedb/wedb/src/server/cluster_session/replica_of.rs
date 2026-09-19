//! REPLICAOF/SECONDARYOF 命令实现（对标 libs/cluster/Session/ReplicaOfCommand.cs）

use wbase::num::strict_i64;
use wresp::{
  cmd_strings::abort_with_wrong_number_of_arguments, command::RespCommand, ext::RespVecExt,
};

use super::{
  ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name,
  replication::queue_try_replicate_sync,
};
use crate::server::{
  cluster_manager_worker_state::ERR_RECOVERY_LOCK,
  replication::{recovery_status::RecoveryStatus, replicate_sync_options::ReplicateSyncOptions},
};

impl ClusterSession {
  /// libs/cluster/Session/ReplicaOfCommand.cs:NetworkTryREPLICAOF
  pub(super) fn network_replicaof(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    // REPLICAOF NO ONE：解除从属（保留数据），转为主节点
    if args[0].eq_ignore_ascii_case(b"NO") && args[1].eq_ignore_ascii_case(b"ONE") {
      let Some(rm) = self.cluster_provider.replication_manager() else {
        output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
        return true;
      };
      if !rm.begin_recovery(RecoveryStatus::ReplicaOfNoOne, false) {
        output.write_resp_error(ERR_RECOVERY_LOCK);
        return true;
      }
      if let Some(m) = self.cluster_manager() {
        m.try_reset_replica();
      }
      rm.try_update_for_failover();
      rm.reset_replica_replay_driver_store();
      // C# ReplicaOfCommand.cs:48 BlockingWait(UnsafeBumpAndWait...)
      self.unsafe_bump_and_wait_for_epoch_transition();
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
      // 升主恢复 Primary 类后台任务（C# ReplicaOfCommand.cs:50-51
      // SuspendReplicaOnlyTasksAsync + StartPrimaryTasks 对译；rust 无副本
      // 专属任务，仅恢复侧生效）
      self.cluster_provider.resume_primary_tasks();
      output.write_resp_simple_string("OK");
      return true;
    }
    let Some(port) = strict_i64(args[1]) else {
      output.write_resp_error(&format!(
        "ERR REPLICAOF failed to parse port '{}'",
        String::from_utf8_lossy(args[1])
      ));
      return true;
    };
    let addr = String::from_utf8_lossy(args[0]);
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let Some(primary_id) = m
      .current_config()
      .get_worker_node_id_from_address(&addr, port as i32)
    else {
      output.write_resp_error(&format!("ERR I don't know about node {addr}:{port}."));
      return true;
    };
    // 发起参数束（对标 ReplicaOfCommand.cs:79-85：网络线程前台发起，
    // Force:true TryAddReplica:true AllowReplicaResetOnFailure:true
    // UpgradeLock:false；选路见 queue_try_replicate_sync）
    let opts = ReplicateSyncOptions::new(primary_id, false, true, true, true, false);
    *self.pending_slow.lock() = Some(queue_try_replicate_sync(m, opts));
    true
  }
}
