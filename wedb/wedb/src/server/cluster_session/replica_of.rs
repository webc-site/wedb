//! REPLICAOF/SECONDARYOF 命令实现（对标 libs/cluster/Session/ReplicaOfCommand.cs）

use wbase::num::strict_i32;
use wresp::{
  cmd_strings::{abort_with_wrong_number_of_arguments, cluster::ERR_UNKNOWN_NODE_PREFIX},
  command::RespCommand,
  ext::RespVecExt,
};

use super::{
  ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, reject_wrong_arity,
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
    reject_wrong_arity!(args.len() != 2, cmd, output);
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
      // 升主恢复 Primary 类后台任务——恢复锁内恢复（C# ReplicaOfCommand.cs:
      // 50-51 StartPrimaryTasks 在 try 块内、EndRecovery 于 finally 最后；
      // 与 failover 接管路径 take_over_as_primary_async 次序归一）：先恢复
      // 后释放恢复锁，杜绝「已非恢复态但 GC/周期任务仍停」的撕裂窗口
      //（窗口内并发 SAVE 可在 GC 停摆引擎上落快照、并发角色变更可与未收尾
      // 的本次升主交错）
      self.cluster_provider.resume_primary_tasks();
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
      output.write_resp_simple_string("OK");
      return true;
    }
    // 端口 int32 值域解析（对标 C# NumUtils.TryParse(out int port)，
    // ReplicaOfCommand.cs:61——越界即非整数拒绝），杜绝 strict_i64 接收后
    // as i32 静默截断假成功（与 CLUSTER MEET / FAILOVER TO 同根因单点）
    let Some(port) = strict_i32(args[1]) else {
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
      .get_worker_node_id_from_address(&addr, port)
    else {
      output.write_resp_error(&format!("{ERR_UNKNOWN_NODE_PREFIX}{addr}:{port}."));
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
