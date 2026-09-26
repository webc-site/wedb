//! 集群基础命令实现（对标 libs/cluster/Session/RespClusterBasicCommands.cs）

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread::Builder,
};

use compio::runtime::{Runtime, spawn};
use parking_lot::RwLock;
use wbase::{
  ascii_sanitize,
  hex::{hex_str_u128, hex_u128},
  num::{strict_i32, strict_i64},
};
use wkv::WedbStore;
use wnode::resp::slow_path::SlowWait;
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_SLOW_PATH_STORAGE, RESP_OK, abort_with_pubsub_command_disabled,
    abort_with_wrong_number_of_arguments,
    cluster::{
      ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY, ERR_GENERIC_CANNOT_FORGET_MYSELF,
      ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT, ERR_GENERIC_CONFIG_UPDATE, ERR_RESET_WITH_KEYS_ASSIGNED,
      ERR_UNKNOWN_NODE_PREFIX, GENERIC_ERR_INVALID_PORT,
    },
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::{
  ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, reject_wrong_arity,
  run_readonly_storage_slow,
};
use crate::{
  error::Error,
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
  },
};

/// libs/cluster/Session/ClusterCommandInfo.cs:GetClusterCommands
const CLUSTER_HELP: [&str; 64] = [
  "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
  "ADDSLOTS <slot> [<slot> ...]",
  "\tAssign slots to current node.",
  "ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tAssign slot ranges to current node.",
  "BUMPEPOCH",
  "\tAdvance the cluster config epoch.",
  "BANLIST",
  "\t Return banlist of nodeids",
  "COUNTKEYSINSLOT <slot>",
  "\tReturn the number of keys in <slot>.",
  "DELSLOTS <slot> [<slot> ...]",
  "\tDelete slots information from current node.",
  "DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tDelete slot ranges information from current node.",
  "DELKEYSINSLOT slot",
  "\tScan the DB and delete keys mapping to corresponding slot.",
  "DELKEYSINSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tScan the DB and delete keys mapping to corresponding slot ranges.",
  "FAILOVER [FORCE | TAKEOVER]",
  "\tSend only to replica, forces the replica to start a manual failover of its master instance",
  "FORGET <node-id> [ban node for seconds = default(60)]",
  "\tRemove a node from the cluster.",
  "GETKEYSINSLOT <slot> <count>",
  "\tGETKEYSINSLOT <slot> <count>",
  "INFO",
  "\tReturn information about the cluster.",
  "KEYSLOT",
  "\tReturn the SLOT a provided KEY is mapped to.",
  "MEET <ip> <port> [<bus-port>]",
  "\tConnect nodes into a working cluster.",
  "MTASKS",
  "\tReturn number of outstanding migration tasks.",
  "MYID",
  "\tReturn the node id.",
  "MYPARENTID",
  "\tReturn primary id or own id if instance not a replica",
  "ENDPOINT",
  "\tEndpoint <nodeid>",
  "\tReturn 'ip:port' for nodeid. 'unassigned:0' if nodeid is not known",
  "NODES",
  "\tReturn cluster configuration seen by node. Output format:",
  "\t<id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
  "REPLICATE <node-id>",
  "\tConfigure current node as replica to <node-id>.",
  "REPLICAS <node-id>",
  "\tReturn <node-id> replicas.",
  "RESET [HARD|SOFT]",
  "\tReset node configuration (default:SOFT). Default SOFT option resets configuration by forgetting slot mapping and nodes. HARD resets config epoch and generates new nodeid and flushes DB data.",
  "SET-CONFIG-EPOCH <epoch>",
  "\tSet config epoch of current node.",
  "SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE <node-id>)",
  "\tSet slot state.",
  "SETSLOTRANGE start-slot end-slot [start-slot end-slot ...]",
  "\tSet state of slots in range.",
  "SLOTS",
  "\tReturn information about slots range mappings. Each range is made of:",
  "SLOTSTATE",
  "\tReturn information about slot state",
  "\tstart, end, master and replicas IP addresses, ports and ids",
  "SHARDS",
  "\tReturns details about the shards of the cluster. A shard is defined as a collection of nodes that serve the same set of slots and that replicate from each other",
  "HELP",
  "\tPrints this help.",
];

/// NetworkClusterReset 的慢路径执行体（同一函数的异步续段）
///
/// 次序对标 C# RespClusterBasicCommands.cs:NetworkClusterReset——
/// 1. `try_reset` 先行（C# :481-488）：槽位表取用与 HasKeysInSlots 键
///    检查在复位临界区内同源于当轮配置（有键拒绝则不换配置），成功后
///    换新配置（SOFT 保留 nodeId/epoch，HARD 换新 id 且 epoch 归零）；
/// 2. HARD 清库（C# :491 `if (!soft) clusterProvider.FlushDB(true)`）：
///    位于 TryReset 之后且不问其成败（C# 事实：TryReset 被键检查拒绝时
///    HARD 仍清库），清库失败不改写应答（C# FlushDB 返回值不阻断 +OK/
///    错误臂，仅记日志归档差异面）；删除全部用户键（与 FLUSHDB 慢路径
///    同一清库入口 `StorageSession::delete_all_user_keys`——Meta 键走
///    版本栅栏 + 树文件排空的完整异步删除，杜绝 try_delete_sync 对复合
///    对象的静默降级丢失）；
/// 3. 最后写出 TryReset 的应答（C# :493-496 直写 resp）。
///
/// 返回完整 RESP 应答字节
async fn cluster_reset_slow(
  manager: Arc<ClusterManager>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  soft: bool,
  expiry_secs: i64,
) -> Vec<u8> {
  run_readonly_storage_slow!(store, storage, out, {
    let resp = manager
      .try_reset(soft, expiry_secs.max(0) as u64, &storage)
      .await;
    if !soft {
      for (ns, db) in storage.batch.store.vdb.list_logic_dbs() {
        if let Err(e) = storage.batch.store.flush_database(ns, db).await {
          log::warn!(
            "CLUSTER RESET HARD flush failed for domain ({}, {}): {}",
            ns,
            db,
            e
          );
        }
      }
    }
    match resp {
      Ok(()) => out.extend_from_slice(RESP_OK),
      Err(Error::ResetWithKeysAssigned) => out.write_resp_error(ERR_RESET_WITH_KEYS_ASSIGNED),
      Err(Error::Storage(_)) => out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE),
      Err(_) => out.write_resp_error("ERR Cluster reset failed"),
    }
  })
}

/// NetworkClusterGossip 的慢路径执行体（同一函数的异步续段）
///
/// 次序对标 C# RespClusterBasicCommands.cs:383-427——载荷合并（TryMerge
/// 需竞争 active_merge_lock 读锁）→ RemoteNodeId 记忆 → 配置变更应答 →
/// EnsureReplication 复制健康检查。读锁等待以 await 让出线程，由网络泵
/// 在批尾纪元快照清零之后驱动——等价 C# :401-410 先 ReleaseCurrentEpoch
/// 再 TryMerge 的死锁规避（MIGRATE config suspension 持挂起写锁期间网络
/// 泵线程不被冻结），未引入第二套挂起机制。
/// 刻意差异：C# :384 在合并前捕获 current 引用，应答 pre-merge 配置；
/// rust 在合并后读版本号与配置，应答体可能已含本帧刚合并的配置
/// （收敛更快，方向无害），勿当 bug 反复登记
async fn cluster_gossip_slow(
  provider: Arc<ClusterProvider>,
  manager: Arc<ClusterManager>,
  remote_node_id: Arc<RwLock<Option<u128>>>,
  last_sent_config_version: Arc<AtomicI64>,
  other: Option<ClusterConfig>,
  with_meet: bool,
) -> Vec<u8> {
  let mut out = Vec::new();
  if let Some(other) = other {
    // 同步读守卫先落 bool 再进挂起门（读守卫绝不跨 await 持有）；
    // WITHMEET 显式信任（C# gossipWithMeet || current.IsKnown）
    let known = manager
      .current_config()
      .is_known(other.local_node_id().unwrap_or_default());
    if with_meet || known {
      manager.try_merge(&other, true).await;
      if let Some(id) = other.local_node_id() {
        *remote_node_id.write() = Some(id);
      }
    } else {
      log::warn!(
        "Received gossip from unknown node: {}",
        other.local_node_id().map_or_else(String::new, hex_str_u128)
      );
    }
  }
  // 事件重拉安全点（案一接线，与 CLUSTER MEET 发起侧同族）：入站 gossip
  // 到达即主循环重拉触发源——panic 终局臂复位拆池后由此事件复活；存活
  // 轮次由 is_running 幂等 CAS 门即拦零开销。置于合并段之后，重拉首轮
  // MEET 即含本帧合并进配置的新节点；本函数由网络泵驱动恒在常驻 runtime
  // 内，无 meet 跨线程臂的 spawn 无门可挂分叉
  manager.try_start_gossip_tasks();
  // 配置变更或 WITHMEET 显式要求 → 回当前配置字节；否则空载荷。
  // C# :422 `lastSentConfig != current` 为引用比较 O(1)，rust 以
  // config_version 作等价键（唯一递增点 flush_config，与发送侧
  // gossip_manager 判定同形同键），变更才序列化一次
  let version = manager.config_version();
  let changed = last_sent_config_version.load(Ordering::Relaxed) != version;
  if changed || with_meet {
    let current_bytes = manager.current_config().to_byte_array();
    if let Some(gm) = provider.gossip_manager() {
      gm.stats
        .update_gossip_bytes_send(current_bytes.len() as i64);
    }
    out.write_resp_bulk_string(&current_bytes);
    last_sent_config_version.store(version, Ordering::Relaxed);
  } else {
    out.write_resp_bulk_string(b"");
  }
  // gossip 后的复制健康检查（C# EnsureReplication）
  let remote = *remote_node_id.read();
  provider.ensure_replication(remote);
  out
}

/// NetworkClusterForget 的慢路径执行体（同一函数的异步续段）
///
/// 次序对标 C# RespClusterBasicCommands.cs:80-100——TryRemoveWorker 需取
/// active_merge_lock 写锁（SuspendConfigMerge 挂起窗口），失败按错误码
/// 分流应答，成功摘除该节点的在途迁移任务后回 +OK。写锁等待以 await
/// 让出线程，由网络泵在批尾纪元快照清零之后驱动——等价 C# :80 NOTE
/// 先 ReleaseCurrentEpoch 再进 TryRemoveWorker 的死锁规避。
async fn cluster_forget_slow(
  provider: Arc<ClusterProvider>,
  manager: Arc<ClusterManager>,
  node_id: u128,
  expiry_seconds: u64,
) -> Vec<u8> {
  let mut out = Vec::new();
  match manager.try_remove_worker(node_id, expiry_seconds).await {
    Ok(()) => {
      // C# 同步摘除该节点的在途迁移任务
      if let Some(mm) = provider.migration_manager() {
        mm.try_remove_migration_task_node(node_id);
      }
      out.write_resp_simple_string("OK");
    }
    Err(Error::CannotForgetMyself) => out.write_resp_error(ERR_GENERIC_CANNOT_FORGET_MYSELF),
    Err(Error::CannotForgetPrimary) => out.write_resp_error(ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY),
    Err(Error::NodeNotFound(id)) => {
      out.write_resp_error(&format!("{ERR_UNKNOWN_NODE_PREFIX}{id}."))
    }
    Err(e) => out.write_resp_error(&e.to_string()),
  }
  out
}

impl ClusterSession {
  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip
  ///
  /// 同步段仅做参数校验、接收统计与载荷版本预检/反序列化（快路径、无锁
  /// 等待）；合并 + 配置应答 + 复制健康检查为异步域（合并需竞争
  /// active_merge_lock 读锁），挂慢路径执行体 [`cluster_gossip_slow`]
  /// 由网络泵驱动——对标 C# 网络线程内联整段的语义
  pub(super) fn network_cluster_gossip(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    reject_wrong_arity!(
      args.is_empty() || args.len() > 2,
      RespCommand::ClusterGossip,
      output
    );
    let (with_meet, payload) = if args.len() > 1 {
      (args[0].eq_ignore_ascii_case(b"WITHMEET"), args[1])
    } else {
      (false, args[0])
    };
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.stats.update_gossip_bytes_recv(payload.len() as i64);
    }
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // 载荷版本预检（C# ClusterConfig.TryPeekVersion）：不兼容仅告警并
    // 降级为空载荷 ping，不阻断应答段；反序列化在本段完成后传值，
    // 慢路径执行体不借用接收缓冲
    let other = if payload.is_empty() {
      None
    } else {
      let version_ok =
        ClusterConfig::try_peek_version(payload).is_some_and(|v| v == CLUSTER_CONFIG_VERSION);
      if !version_ok {
        log::warn!("Received gossip with incompatible config version");
        None
      } else {
        ClusterConfig::from_byte_array(payload).ok()
      }
    };
    // 合并段读锁等待以 await 让出网络泵线程；挂起后本批停止消费、批尾
    // 纪元快照清零，等价 C# :401-410 先 ReleaseCurrentEpoch 再 TryMerge
    // 的死锁规避（纪元让渡由慢路径驱动点天然承接）
    *self.pending_slow.lock() = Some(SlowWait::new(cluster_gossip_slow(
      Arc::clone(&self.cluster_provider),
      m,
      Arc::clone(&self.remote_node_id),
      Arc::clone(&self.last_sent_config_version),
      other,
      with_meet,
    )));
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterNodes
  pub(super) fn network_cluster_nodes(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    let info = self
      .cluster_manager()
      .map(|m| {
        m.current_config()
          .get_cluster_info(Some(&self.cluster_provider))
      })
      .unwrap_or_default();
    output.write_resp_bulk_string(info.as_bytes());
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMyId
  pub(super) fn network_cluster_myid(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    // RESP 渲染点：节点 id 转 32 字符小写 hex；未初始化集群回空 bulk（C# RESP_EMPTY）
    let myid = self
      .cluster_manager()
      .and_then(|m| m.current_config().local_node_id())
      .map_or_else(String::new, hex_str_u128);
    output.write_resp_bulk_string(myid.as_bytes());
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterShards
  pub(super) fn network_cluster_shards(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    if let Some(m) = self.cluster_manager() {
      let info = m
        .current_config()
        .get_shards_info(Some(&self.cluster_provider), self.preferred_endpoint_type());
      output.extend_from_slice(info.as_bytes());
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterInfo
  pub(super) fn network_cluster_info(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    let info = self
      .cluster_manager()
      .map(|m| m.get_info())
      .unwrap_or_default();
    output.write_resp_bulk_string(info.as_bytes());
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterBumpEpoch
  pub(super) fn network_cluster_bumpepoch(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if m.try_bump_cluster_epoch() {
      output.extend_from_slice(RESP_OK);
    } else {
      output.write_resp_error(ERR_GENERIC_CONFIG_UPDATE);
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterReset
  ///
  /// 同步段仅校验参数（0/1/2 参：SOFT|HARD + 可选过期秒数）；实际闭环
  /// （TryReset 含临界区内的槽位表取用与 HasKeysInSlots 键判定 → HARD
  /// 清库 → 写出应答）为异步域，挂慢路径执行体由网络泵驱动——对标 C#
  /// 网络线程内联 TryReset（含 ReleaseCurrentEpoch 纪元让渡）的整段语义
  pub(super) fn network_cluster_reset(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() > 2, cmd, output);
    // C# soft = option.EqualsUpperCaseSpanIgnoringCase("SOFT")：仅显式
    // SOFT 为软重置，其余（含 HARD）均为硬重置
    let soft = args
      .first()
      .is_none_or(|opt| opt.eq_ignore_ascii_case(b"SOFT"));
    let mut expiry_secs: i64 = 60;
    if let Some(exp) = args.get(1) {
      let Some(v) = strict_i64(exp) else {
        output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return true;
      };
      expiry_secs = v;
    }
    match (self.cluster_manager(), self.cluster_provider.try_store()) {
      (Some(m), Some(store)) => {
        *self.pending_slow.lock() = Some(SlowWait::new(cluster_reset_slow(
          m,
          store,
          soft,
          expiry_secs,
        )));
      }
      // 集群管理器或存储未装配：明确报错，绝不静默吞命令
      _ => output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED),
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMeet
  pub(super) fn network_cluster_meet(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() != 2, cmd, output);
    // 端口 int32 值域解析（对标 C# SessionParseState.TryGetInt：越界即视为
    // 非法端口拒绝），杜绝 strict_i64 接收后 as i32 静默截断假成功
    let Some(port) = strict_i32(args[1]) else {
      // 对标 C# string.Format(CmdStrings.GenericErrInvalidPort, parseState.GetString(1))
      // → Encoding.ASCII.GetBytes：{0} 回显经 ASCII 解码，≥0x80 逐字节折 '?'
      // （wbase::ascii_sanitize 同语义），非 ASCII 字面不做 lossy 原样回显
      output.write_resp_error(&GENERIC_ERR_INVALID_PORT.replace("{0}", &ascii_sanitize(args[1])));
      return true;
    };
    let ip = String::from_utf8_lossy(args[0]).into_owned();
    // 发起失败显式回 ERR（对齐 C# MEET BlockingWait 异常上抛形态，杜绝未执行
    // 仍回 +OK 的假成功）：非 runtime 臂的线程创建失败同步可判，即回 ERR；
    // 线程内运行时创建失败（compio Runtime 非 Send，只能在新线程内构造）无法
    // 同步回传，落 log::error 留痕——原 `if let Ok(rt)` 静默失败臂的零痕形态
    // 在此补齐
    let mut meet_start_err: Option<String> = None;
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      if Runtime::try_current().is_some() {
        // 事件重拉安全点（案一接线，本函数原只直调 try_meet_async 无重拉）：
        // CLUSTER MEET 到达即 gossip 主循环重拉触发源——panic 终局臂复位
        // is_running 并拆池后，此处在发起 meet 前经 try_start_gossip_tasks
        // 于当前常驻 runtime 重拉主循环；存活轮次由幂等 CAS 门即拦零开销。
        // 跨线程臂不接：一次性 Runtime 承载不了常驻循环任务（compio detach
        // 任务随驱动 runtime 析构，线程退出即循环终止，C# Task.Run 挂全局
        // 线程池无此形态），该臂仅完成 meet 握手本身，重拉由下一到达常驻
        // runtime 的事件承接
        if let Some(m) = self.cluster_manager() {
          m.try_start_gossip_tasks();
        }
        spawn(async move {
          if let Err(e) = gm.try_meet_async(&ip, port, true).await {
            log::warn!("CLUSTER MEET {ip}:{port} failed: {e}");
          }
        })
        .detach();
      } else {
        let spawned =
          Builder::new()
            .name("cluster-meet".into())
            .spawn(move || match Runtime::new() {
              Ok(rt) => {
                rt.block_on(async move {
                  if let Err(e) = gm.try_meet_async(&ip, port, true).await {
                    log::warn!("CLUSTER MEET {ip}:{port} failed: {e}");
                  }
                });
              }
              Err(e) => {
                log::error!("CLUSTER MEET {ip}:{port} 运行时创建失败，MEET 未发起: {e}");
              }
            });
        if let Err(e) = spawned {
          meet_start_err = Some(format!("cluster-meet 线程创建失败: {e}"));
        }
      }
    }
    if let Some(reason) = meet_start_err {
      output.write_resp_error(&format!("ERR CLUSTER MEET failed to start: {reason}"));
      return true;
    }
    output.write_resp_simple_string("OK");
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterForget
  ///
  /// 同步段仅做参数校验与节点 id 解析；摘除（TryRemoveWorker 需取
  /// active_merge_lock 写锁，SuspendConfigMerge 挂起窗口）为异步域，
  /// 挂慢路径执行体 [`cluster_forget_slow`] 由网络泵驱动——对标 C#
  /// 网络线程内联整段的语义
  pub(super) fn network_cluster_forget(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.is_empty() || args.len() > 2, cmd, output);
    let mut expiry_seconds: i64 = 60;
    if let Some(exp) = args.get(1) {
      let Some(v) = strict_i64(exp) else {
        output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return true;
      };
      expiry_seconds = v;
    }
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(node_id) = hex_u128(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    // 写锁等待以 await 让出网络泵线程；挂起后本批停止消费、批尾纪元
    // 快照清零，等价 C# :80 NOTE 先 ReleaseCurrentEpoch 再进
    // TryRemoveWorker 的死锁规避（纪元让渡由慢路径驱动点天然承接）
    *self.pending_slow.lock() = Some(SlowWait::new(cluster_forget_slow(
      Arc::clone(&self.cluster_provider),
      m,
      node_id,
      expiry_seconds.max(0) as u64,
    )));
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterSetConfigEpoch
  pub(super) fn network_cluster_setconfigepoch(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() != 1, cmd, output);
    let Some(config_epoch) = strict_i64(args[0]) else {
      output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };
    let Some(m) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    if m.current_config().num_workers() > 1 {
      output.write_resp_error(ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT);
      return true;
    }
    match m.try_set_local_config_epoch(config_epoch) {
      Ok(()) => output.write_resp_simple_string("OK"),
      // C# 双错误分流：workers 未初始化 → RESP_ERR_GENERIC_WORKERS_NOT_INITIALIZED，
      // 文案单一来源在 error.rs，write_resp_error 自动补 ERR 前缀
      Err(e @ Error::NoWorkers) => output.write_resp_error(&e.to_string()),
      // C# RESP_ERR_GENERIC_CONFIG_EPOCH_NOT_SET
      Err(_) => {
        output.write_resp_error("ERR Node config epoch was not set due to invalid epoch specified")
      }
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterEndpoint
  pub(super) fn network_cluster_endpoint(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() != 1, cmd, output);
    const UNASSIGNED_ENDPOINT: &[u8] = b"unassigned:0";
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份
    let Some(node_id) = hex_u128(args[0]) else {
      output.write_resp_bulk_string(UNASSIGNED_ENDPOINT);
      return true;
    };
    let endpoint = self
      .cluster_manager()
      .and_then(|m| m.current_config().get_endpoint_from_node_id(node_id));
    // C# 未知节点回落占位 worker（'unassigned:0'）
    if let Some(endpoint) = endpoint {
      output.write_resp_bulk_string(endpoint.to_string().as_bytes());
    } else {
      output.write_resp_bulk_string(UNASSIGNED_ENDPOINT);
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterHelp
  pub(super) fn network_cluster_help(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    output.write_resp_array_len(CLUSTER_HELP.len());
    for line in CLUSTER_HELP {
      output.write_resp_simple_string(line);
    }
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterMyParentId
  pub(super) fn network_cluster_myparentid(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    // RESP 渲染点：节点 id 转 32 字符小写 hex
    let parent = self.cluster_manager().map(|m| {
      let config = m.current_config();
      if config.is_primary() {
        config.local_node_id()
      } else {
        config.local_node_primary_id()
      }
      .map_or_else(String::new, hex_str_u128)
    });
    // 未初始化集群同上回空 bulk（C# RESP_EMPTY）
    output.write_resp_bulk_string(parent.unwrap_or_default().as_bytes());
    true
  }

  /// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterPublish
  ///
  /// C# 收端不区分 PUBLISH/SPUBLISH（一律 Publish 入 aof，由常驻 StartAsync
  /// 消费循环分发）——其订阅图仅一张，SSUBSCRIBE 复用普通频道图故无害；
  /// rust 无 TsavoriteLog 介质亦无常驻消费任务，统一收敛为同步直投
  /// （对标 C# PublishNow，见 doc/zh/deviations.md §65），且三表分离（shard_subscriptions 独立）下
  /// SPUBLISH 须直投分片域（publish_shard_now），否则落地普通图：分片订阅者
  /// 收不到、同名频道普通/模式订阅者串台。分派只在接收端此一处，发送侧
  /// 分片定向见 ClusterManager::try_cluster_publish_async
  pub(super) fn network_cluster_publish(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(args.len() != 2, cmd, output);
    let Some(broker) = self.cluster_provider().subscribe_broker() else {
      abort_with_pubsub_command_disabled(output, "PUBLISH");
      return true;
    };
    if cmd == RespCommand::ClusterSpublish {
      broker.publish_shard_now(args[0], args[1]);
    } else {
      broker.publish_now(args[0], args[1]);
    }
    // C# 无应答写出
    true
  }
}
