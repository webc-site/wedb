//! 集群故障转移命令实现（对标 libs/cluster/Session/RespClusterFailoverCommands.cs 与 FailoverCommand.cs）

use std::{str::from_utf8, time::Duration};

use waof::AofAddress;
use wbase::{hex::hex_u128, num::strict_i64};
use wnode::resp::slow_path::SlowWait;
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    abort_with_wrong_number_of_arguments,
    cluster::{ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER, ERR_GENERIC_UNKNOWN_ENDPOINT},
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::{ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name};
use crate::server::{failover::failover_option::FailoverOption, worker::NodeRole};

impl ClusterSession {
  /// libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailover
  pub(super) fn network_cluster_failover(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.len() > 2 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(RespCommand::ClusterFailover));
      return true;
    }
    let mut option = FailoverOption::Default;
    let mut abort = false;
    let mut timeout_secs: i64 = 0;
    if let Some(arg0) = args.first() {
      if arg0.eq_ignore_ascii_case(b"ABORT") {
        abort = true;
      } else if arg0.eq_ignore_ascii_case(b"FORCE") {
        option = FailoverOption::Force;
      } else if arg0.eq_ignore_ascii_case(b"TAKEOVER") {
        option = FailoverOption::Takeover;
      } else {
        output.write_resp_error(&format!(
          "ERR Failover option ({}) not supported",
          String::from_utf8_lossy(arg0)
        ));
        return true;
      }
      if let Some(arg1) = args.get(1) {
        match strict_i64(arg1) {
          Some(v) => timeout_secs = v,
          None => {
            output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return true;
          }
        }
      }
    }
    let Some(fm) = self.cluster_provider.failover_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if abort {
      fm.try_abort_replica_failover();
      output.write_resp_simple_string("OK");
      return true;
    }
    // 副本身份校验（C# current.IsReplica && LocalNodePrimaryId != null）
    let (is_replica, primary_addr) = self
      .cluster_manager()
      .map(|m| {
        let config = m.current_config();
        (
          config.is_replica() && config.local_node_primary_id().is_some(),
          config.get_local_node_primary_address(),
        )
      })
      .unwrap_or((false, (None, 0)));
    if !is_replica {
      output.write_resp_error("ERR Node is not configured as a REPLICA");
      return true;
    }
    // 缺省/显式 0 一律传零时长（C# failoverTimeout=default(TimeSpan) 传播形态，
    // RespClusterFailoverCommands.cs:28-53），由 FailoverSession::new 单点归一
    // 为 600 秒（FailoverSession.cs:85）；负值 C# 为即刻失败语义，Duration
    // 不可负，按缺省归一处理
    let timeout = if timeout_secs > 0 {
      Duration::from_secs(timeout_secs as u64)
    } else {
      Duration::ZERO
    };
    if !fm.try_start_replica_failover(option, timeout) {
      let (addr, port) = primary_addr;
      output.write_resp_error(&format!(
        "ERR failed to start failover for primary({}:{port})",
        addr.unwrap_or_default(),
        port = port
      ));
      return true;
    }
    output.write_resp_simple_string("OK");
    true
  }

  /// libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailStopWrites
  pub(super) fn network_cluster_fail_stop_writes(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 1 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份；空参即复位
    let node_id = hex_u128(args[0]);
    if let Some(m) = self.cluster_manager() {
      if let Some(node_id) = node_id {
        // 副本发起接管请求：主节点停写并向该副本让渡属主
        m.try_stop_writes(node_id);
      } else {
        m.try_reset_replica();
      }
    }
    // C# RespClusterFailoverCommands.cs:128 BlockingWait(UnsafeBumpAndWait...)
    self.unsafe_bump_and_wait_for_epoch_transition();
    // 停写应答位点单点走 rm.get_current_replication_offset()（C#:129 读
    // ReplicationOffset 属性）。TryStopWrites 已置本端 Role = REPLICA
    // （ClusterConfig.cs:1291 MakeReplicaOf），getter 角色分支据此回退
    // replicationOffset 字段——与 C# 同值（副本侧追平循环
    // ReplicaFailoverSession.cs:94 即读该应答）。此处不得另起第二条
    // 绕过角色分支的位点读法，否则与主端 getter 面分叉成两套位点来源。
    let offset = self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.get_current_replication_offset().to_aof_string())
      .unwrap_or_default();
    output.write_resp_bulk_string(offset.as_bytes());
    true
  }

  /// libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailReplicationOffset
  ///
  /// 本函数无内层超时（对标 C# :154 BlockingWait 实参只有目标位点）：
  /// 失败由调用方 cluster_timeout 兜底（PrimaryFailoverSession.cs:22
  /// WaitAsync(clusterTimeout)，超时该副本不入选），不得叠加第二层
  /// 超时常量——否则 cluster_timeout 大于内层常量时慢同步合格副本被
  /// 错误淘汰；停机取消按 C# ReplicationManager.cs:572 应答 -1 位点
  pub(super) fn network_cluster_fail_replication_offset(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    if args.len() != 1 {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    }
    let primary_offset = AofAddress::from_span(args[0]);
    let Some(rm) = self.cluster_provider.replication_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      // 应答写回等待返回的位点本体（C# :154-155 写 rOffset，非二次读属性）：
      // 追平答当下位点，停机答 -1 位点哨兵，调用方 equals_all 比对判定
      let r_offset = rm.wait_for_replication_offset_async(&primary_offset).await;
      out.write_resp_bulk_string(r_offset.to_aof_string().as_bytes());
      out
    }));
    true
  }

  /// libs/cluster/Session/FailoverCommand.cs:TryFAILOVER（顶层 FAILOVER）
  pub(super) fn network_failover(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let mut replica_address: Option<&str> = None;
    let mut replica_port: i64 = 0;
    let mut timeout_ms: i64 = -1;
    let mut abort = false;
    let mut force = false;
    let mut option_takeover = false;
    let mut i = 0;
    while i < args.len() {
      let arg = args[i];
      i += 1;
      if arg.eq_ignore_ascii_case(b"TO") {
        let Some(addr) = args.get(i) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        let Ok(addr_str) = from_utf8(addr) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        replica_address = Some(addr_str);
        i += 1;
        let Some(port) = args.get(i).copied().and_then(strict_i64) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        replica_port = port;
        i += 1;
      } else if arg.eq_ignore_ascii_case(b"TIMEOUT") {
        let Some(t) = args.get(i).copied().and_then(strict_i64) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        timeout_ms = t;
        i += 1;
      } else if arg.eq_ignore_ascii_case(b"ABORT") {
        abort = true;
      } else if arg.eq_ignore_ascii_case(b"FORCE") {
        force = true;
      } else if arg.eq_ignore_ascii_case(b"TAKEOVER") {
        force = true;
        // TAKEOVER 额外豁免同步等待语义（C# FailoverOption.TAKEOVER）
        option_takeover = true;
      } else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return true;
      }
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let is_primary = m.current_config().local_node_role() == NodeRole::Primary;
    if !is_primary {
      output.write_resp_error(ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER);
      return true;
    }
    // TO 目标校验：须为已知节点、副本角色、且隶属于本节点
    if let Some(replica_address) = replica_address {
      let config = m.current_config();
      let Some(replica_id) =
        config.get_worker_node_id_from_address(replica_address, replica_port as i32)
      else {
        output.write_resp_error(ERR_GENERIC_UNKNOWN_ENDPOINT);
        return true;
      };
      let Some(worker) = config.get_worker_from_node_id(replica_id) else {
        output.write_resp_error(ERR_GENERIC_UNKNOWN_ENDPOINT);
        return true;
      };
      if worker.role != NodeRole::Replica {
        output.write_resp_error(&format!(
          "ERR Node @{replica_address}:{replica_port} is not a replica."
        ));
        return true;
      }
      if worker.replica_of_node_id != config.local_node_id() {
        output.write_resp_error(&format!(
          "ERR Node @{replica_address}:{replica_port} is not my replica."
        ));
        return true;
      }
    }
    let Some(fm) = self.cluster_provider.failover_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if abort {
      m.try_set_local_node_role(NodeRole::Primary);
      fm.try_abort_replica_failover();
    } else {
      let timeout = if timeout_ms <= 0 {
        Duration::MAX
      } else {
        Duration::from_millis(timeout_ms as u64)
      };
      let option = if option_takeover {
        FailoverOption::Takeover
      } else if force {
        FailoverOption::Force
      } else {
        FailoverOption::Default
      };
      fm.try_start_primary_failover(
        replica_address.unwrap_or_default(),
        replica_port as i32,
        option,
        timeout,
      );
    }
    output.write_resp_simple_string("OK");
    true
  }
}
