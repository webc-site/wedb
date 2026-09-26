//! 集群故障转移命令实现（对标 libs/cluster/Session/RespClusterFailoverCommands.cs 与 FailoverCommand.cs）

use std::{str::from_utf8, sync::Arc, time::Duration};

use waof::AofAddress;
use wbase::{
  hex::hex_u128,
  num::{strict_i32, strict_i64},
};
use wnode::{ClusterSessionFace, resp::slow_path::SlowWait};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    abort_with_wrong_number_of_arguments,
    cluster::{
      ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER, ERR_GENERIC_REPLICATION_AOF_TURNEDOFF,
      ERR_GENERIC_UNKNOWN_ENDPOINT,
    },
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::{ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, reject_wrong_arity};
use crate::server::{failover::failover_option::FailoverOption, worker::NodeRole};

impl ClusterSession {
  /// libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailover
  pub(super) fn network_cluster_failover(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(args.len() > 2, RespCommand::ClusterFailover, output);
    let mut option = FailoverOption::Default;
    let mut abort = false;
    let mut timeout_secs: i64 = 0;
    // 从端选项白名单（deviations §117a）：C# 从端入口仅拦 DEFAULT/INVALID、
    // 七枚举全转（SessionParseStateExtensions.cs:52-74 + FailoverManager 无
    // 选项门），副本上 TO/TIMEOUT 静默按 DEFAULT 发起真实故障转移——系顶层
    // 词法器误用于从端的入口缺陷；rust 三词白名单为修复向收紧，严禁按 C#
    // 词表放宽（顶层 TAKEOVER 拒绝面另见 §13h）
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
        // 超时秒数 strict_i64 值域档（deviations §117d）：C# TryGetInt 仅
        // int32 值域（RespClusterFailoverCommands.cs:47），rust 收 i64 全档、
        // 界外大值原样入会话预算；文法向（前导零）仍沿 §32 拒收
        match strict_i64(arg1) {
          Some(v) => timeout_secs = v,
          None => {
            output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return true;
          }
        }
      }
    }
    // AOF 门（对标 RespClusterFailoverCommands.cs:52-76）：未开 AOF 一律拒绝，
    // 覆盖 ABORT 与提升臂——副本须以 AOF 落盘重放流承接复制，无 AOF 节点一旦
    // 经故障转移提升为主，重启即失全部数据且无法为新副本供流。与
    // network_cluster_replicate 同源判定（装配期 aof 在位性，C# EnableAOF 的
    // 装配等价面），门序在 failover_manager 之前（C# else 臂不触 failoverManager）
    if self.cluster_provider.try_aof().is_none() {
      output.write_resp_error(ERR_GENERIC_REPLICATION_AOF_TURNEDOFF);
      return true;
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
    // 缺省/显式 0 传零时长（C# failoverTimeout=default(TimeSpan) 传播形态，
    // RespClusterFailoverCommands.cs:28-53），由 FailoverSession::new 单点归一
    // 为 600 秒（FailoverSession.cs:85）。负值即刻失败语义（C# FromSeconds(负)
    // 原样入会话，FailoverSession.cs:85 非 default 不归一、:86 deadline 落过去，
    // ReplicaFailoverSession.cs:89 WaitAsync(负时限) 同步抛 ArgumentOutOfRangeException
    // 落 catch，DEFAULT 臂秒回 FAILOVER_ABORTED）：传 1 纳秒必败时长——非零
    // 绕过 600 秒归一、deadline 落即刻，会话首站建连等待即超时放弃，角色与
    // 槽位零变化
    let timeout = if timeout_secs > 0 {
      Duration::from_secs(timeout_secs as u64)
    } else if timeout_secs == 0 {
      Duration::ZERO
    } else {
      Duration::from_nanos(1)
    };
    if !fm.try_start_replica_failover(option, timeout) {
      // 发起失败文案单源（deviations §117c）：C# 以 ValueTuple 整体插值
      // 渲染 primary((addr, port)) 双层括号+逗号空格畸形形
      // （RespClusterFailoverCommands.cs:71 + ClusterConfig.cs:268），rust
      // 解构后渲染 primary(addr:port) 冒号单层形（主端缺席形 primary(:-1)
      // 同格式），严禁按 C# 逐字嵌套括号对齐
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
    reject_wrong_arity!(args.len() != 1, cmd, output);
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份；空参即复位，非空非法 hex 返回语法错误
    let node_id = if args[0].is_empty() {
      None
    } else {
      let Some(id) = hex_u128(args[0]) else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return true;
      };
      Some(id)
    };
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if let Some(node_id) = node_id {
      // 副本发起接管请求：主节点停写并向该副本让渡属主
      m.try_stop_writes(node_id);
    } else {
      m.try_reset_replica();
    }
    self.release_current_epoch();
    let provider = Arc::clone(&self.cluster_provider);
    let cm = Arc::clone(&m);
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      let mut out = Vec::new();
      // C# RespClusterFailoverCommands.cs:128-129 BlockingWait(UnsafeBumpAndWait…)
      // 静止达成后才把位点作为应答回出（原语 C# 侧恒真），慢路径异步等待
      // 全会话静止避免同步忙等阻塞 compio reactor。rust 有界化后 false =
      // 静止未达成、返值必承判（deviations §95 判败措辞同口径，与 r25 迁移
      // 族同原语同害同判）：滞留批内在途写此后仍可提交、本端采样位点非终
      // 态水位，照回即令副本在缺口上直入接管、已 ACK 写随槽位让渡永久丢
      // 失。未达成即赎回让渡（try_restore_stop_writes 以真实让渡标志为唯一
      // 判据，空参复位臂天然幂等无操作）并回 -ERR，不回位点应答——副本端
      // pause 臂按「无有效应答即放弃」消费本判败信号
      if !provider.bump_and_wait_for_epoch_transition_async().await {
        cm.try_restore_stop_writes();
        out.write_resp_error("ERR epoch drain not settled within cluster-node-timeout");
        return out;
      }
      // 停写应答位点单点走 rm.get_current_replication_offset()（C#:129 读
      // ReplicationOffset 属性）。TryStopWrites 已置本端 Role = REPLICA
      // （ClusterConfig.cs:1291 MakeReplicaOf），getter 角色分支据此回退
      // replicationOffset 字段——与 C# 同值（副本侧追平循环
      // ReplicaFailoverSession.cs:94 即读该应答）。此处不得另起第二条
      // 绕过角色分支的位点读法，否则与主端 getter 面分叉成两套位点来源。
      let offset = provider
        .replication_manager()
        .map(|rm| rm.get_current_replication_offset().to_aof_string())
        .unwrap_or_default();
      out.write_resp_bulk_string(offset.as_bytes());
      out
    }));
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
    reject_wrong_arity!(args.len() != 1, cmd, output);
    // 请求载荷为带 1 字节长度前缀二进制（对标 C# :151 AofAddress.FromByteArray
    // 与发端 GarnetClientExtensions.cs:61 ToByteArray 双端同形）；前缀与实长
    // 不符回语法错误（C# BinaryReader 异常掐断连接的实现形态不落，偏差沿
    // 既有 syntax error 拒绝面口径）
    let Some(primary_offset) = AofAddress::from_aof_binary(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
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
    let mut replica_port: i32 = 0;
    let mut timeout_ms: i64 = -1;
    let mut abort = false;
    let mut force = false;
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
        // 端口 int32 值域解析（对标 C# TryGetInt：FailoverCommand.cs:42-47，
        // 越界即非整数拒绝），杜绝 strict_i64 接收后 as i32 静默截断假成功
        let Some(port) = args.get(i).copied().and_then(strict_i32) else {
          output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return true;
        };
        replica_port = port;
        i += 1;
      } else if arg.eq_ignore_ascii_case(b"TIMEOUT") {
        // 顶层超时毫秒 strict_i64 值域档（deviations §117d，与从端秒档同源；
        // C# TryGetInt int32 值域回 not-integer，rust 收界外大值进会话预算）
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
        // C# FailoverCommand.cs:24 TryGetFailoverOption 把 TAKEOVER 解析为
        // 合法枚举逃过 syntax error，switch（:35-65）无 TAKEOVER 分支落
        // default throw、ProcessClusterCommands 无 catch 掐断连接——顶层
        // FAILOVER 的 TAKEOVER 在 C# 是不可用输入（Force 跳过同步等待的
        // 豁免语义仅存在于从端 CLUSTER FAILOVER 臂）。对齐拒绝面，回语法
        // 错误帧（掐连接系 C# 异常通道实现形态，不逐字复刻，偏差登记
        // deviations.md 条目 13h）
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return true;
      } else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return true;
      }
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // 角色门前置：非 PRIMARY 一律回 ERR（对标 C# FailoverCommand.cs:68-73）；
    // 例外放行仅限主节点已进入让渡态（try_stop_writes 置位）期间由外部下发的 ABORT，
    // 触发主端会话失败路径自赎；常态副本（未让渡）任何命令包括 ABORT 一律拒绝
    let is_primary = m.current_config().local_node_role() == NodeRole::Primary;
    if !is_primary && !(abort && m.is_stop_writes_delegated()) {
      output.write_resp_error(ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER);
      return true;
    }
    // TO 目标校验：须为已知节点、副本角色、且隶属于本节点
    // （deviations §117b）：给了地址即无条件三闸——C# 门条件
    // `replicaPort != -1 &&`（FailoverCommand.cs:76）在 -1 端口整体跳闸、
    // TO 地址被丢弃且探测集错向 GetLocalNodePrimaryEndpoints（双缺陷），
    // rust 对 -1 查无此端回 unknown endpoint，严禁复刻跳验放行门形
    if let Some(replica_address) = replica_address {
      let config = m.current_config();
      let Some(replica_id) = config.get_worker_node_id_from_address(replica_address, replica_port)
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
      // C# :103-107 abort 臂动作 = TrySetLocalNodeRole(PRIMARY) +
      // TryAbortReplicaFailover；本端口仅保留 try_abort_replica_failover()
      // 触发会话自赎——让渡态（停写降级 + 槽位划归副本）的赎回由主端会话
      // 失败路径闭环（primary_failover_session 的 !success && stopped_writes
      // 分支，abort 经 race_abort/is_aborted 前置查必然落入），角色与槽位
      // 一并恢复。不做任何无会话上下文的配置改写：无判据的裸赎回/裸角色
      // 翻转是槽位空洞与窃主通道的根源（PR #1670 前形态）
      fm.try_abort_replica_failover();
    } else {
      // 缺省/0/负一律传零时长，与从节点入口同口径（见 network_cluster_failover
      // 上方注释），由 FailoverSession::new 单点归一为 600 秒缺省预算。
      // 落档声明（C# :110 混合语义不复刻）：C# 主端负 ms 经
      // Timeout.InfiniteTimeSpan 特判（-1 = 无限）与 TimeSpan.FromMilliseconds(负)
      // 直入（deadline 落过去）混合，Duration 不可负且无限语义无处安放——
      // 本端口守单点归一契约，负值与 0 同归 600 秒缺省预算。严禁
      // Duration::MAX：MAX 不为零、绕过归一，喂给 coarsetime Instant 的
      // 裸 u64 ticks 加法即溢出（debug panic / release 回绕开局误判超时），
      // 并摧毁底层定时器的时限预算
      let timeout = if timeout_ms <= 0 {
        Duration::ZERO
      } else {
        Duration::from_millis(timeout_ms as u64)
      };
      let option = if force {
        FailoverOption::Force
      } else {
        FailoverOption::Default
      };
      fm.try_start_primary_failover(
        replica_address.unwrap_or_default(),
        replica_port,
        option,
        timeout,
      );
    }
    output.write_resp_simple_string("OK");
    true
  }
}
