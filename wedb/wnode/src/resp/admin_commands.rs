use std::mem;

use wbase::{ascii_sanitize, num::strict_i32};
use wcol::{
  ObjectOutput,
  hash::hash_object::{HashObject, HashOperation},
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wconf::ServerConfigType;
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_db_index_arg},
  cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_GENERIC, abort_with_error_message,
    abort_with_unknown_subcommand, abort_with_unknown_subcommand_or_wrong_num_args,
    write_error_raw, write_raw,
  },
  command::{RespCommand, is_cluster_sub_command},
  ext::RespVecExt,
};
use wval::GarnetObjectType;

use super::resp_server_session::{RespServerSession, collect_arg_views};
use crate::{
  resp::{
    acl_commands::AclGateVerdict,
    objects::{
      hash_commands::hash_load_sync,
      object_store_utils::{GarnetObjectPayload, ObjLoad, obj_save_or_gc},
      rmw_helpers::obj_writeback_recheck_sync,
      sorted_set_commands::{zset_load_sync, zset_save_or_gc},
    },
    slow_path::SlowWait,
  },
  session_parse_state_extensions::{ManagerType, manager_type_from_token},
};

/// GC 代数非法文案（本域两处复用）。
const ERR_INVALID_GC_GENERATION: &str = "ERR Invalid GC generation.";

/// 自定义（扩展）命令（C# CheckACLPermissions 的 CustomCommand 分叉判定；动态注册层
/// 删除后仅剩编译期静态对象命令 Customobjcmd，其名字与其余三者同走按名鉴权轨）
#[inline]
fn is_custom_command(cmd: RespCommand) -> bool {
  cmd == RespCommand::Customobjcmd
}

macro_rules! run_collect_keys {
  ($self:ident, $parse_state:ident, $store:ident, $output:ident, $cmd_name:literal, $load:expr, $operate:expr, $save:expr) => {{
    check_arg_count!($parse_state, 1.., $output, $cmd_name);

    if $parse_state[0] == b"*" {
      return Ok(false);
    }

    let mut wrong_type = false;
    let mut scratch = Vec::new();
    let mut sink_buf = Vec::new();
    for key in $parse_state {
      scratch.clear();
      let Some(_window) = $store.try_rmw_window(key) else {
        return Ok(false);
      };
      let mut obj = match $load($store, key, &mut scratch) {
        ObjLoad::Degrade => return Ok(false),
        ObjLoad::WrongType => {
          wrong_type = true;
          continue;
        }
        ObjLoad::Missing => continue,
        ObjLoad::Present(o) => o,
      };

      sink_buf.clear();
      let mut out_sink = ObjectOutput::mount(&mut sink_buf);
      $operate(&mut obj, $self.resp_protocol_version, &mut out_sink);
      if !obj_writeback_recheck_sync($store, key, true) {
        return Ok(false);
      }
      if $save($store, key, &obj) {
        continue;
      }
      return Ok(false);
    }

    if wrong_type {
      write_error_raw($output, cs::RESP_ERR_WRONG_TYPE);
    } else {
      write_raw($output, cs::RESP_OK);
    }
    Ok(true)
  }};
}

impl RespServerSession {
  /// libs/server/Resp/AdminCommands.cs:ProcessAdminCommands
  ///
  /// C# ProcessOtherCommands / ProcessAdminCommands 的 admin 会话级 arm 集
  ///（实现为本文件各 network_* 处理器）
  ///
  /// MONITOR / DEBUG / SAVE / BGSAVE / LASTSAVE /
  /// COMMITAOF / EXPDELSCAN（C# ProcessAdminCommands switch 的无存储面子集）；
  /// `None` 表示命令不属于本族，调用方继续后续分派
  pub fn process_admin_session_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    if !matches!(
      cmd,
      RespCommand::Monitor
        | RespCommand::Debug
        | RespCommand::Save
        | RespCommand::Bgsave
        | RespCommand::Lastsave
        | RespCommand::Commitaof
        | RespCommand::Expdelscan
    ) {
      return None;
    }
    let store = self.collect_args_store();
    let args = store.views();
    let mut output = mem::take(&mut self.output);
    let handled = match cmd {
      RespCommand::Monitor => self.network_monitor(&args, &mut output),
      RespCommand::Debug => self.network_debug(&args, &mut output),
      RespCommand::Save => self.network_save(&args, &mut output),
      RespCommand::Bgsave => self.network_bgsave(&args, &mut output),
      RespCommand::Lastsave => self.network_lastsave(&args, &mut output),
      RespCommand::Commitaof => self.network_commitaof(&args, &mut output),
      RespCommand::Expdelscan => self.network_expdelscan(&args, &mut output),
      _ => unreachable!(),
    };
    self.output = output;
    Some(handled.unwrap_or(true))
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissions
  ///
  /// C#: 自定义命令分叉 CheckACLPermissionsForCustomCommand；否则
  /// (!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝。无 ACL 认证器
  /// （NoAuth / Password 档）等价 default 用户 +@all → 恒放行。
  ///
  /// 三态裁决（[`AclGateVerdict`]）：挂载陈旧须点查刷新时同步段不就地裁决
  ///（存储点查严禁同步收割），登记刷新停车交泵异步闭环后回驱本命令重评
  pub fn check_acl_permissions(&mut self, cmd: RespCommand) -> AclGateVerdict {
    // 跨连接改权收敛预门（唯一失效判据：引擎 ACL 代数；每命令一次，位点在
    // 纪元守卫外，代相等即零存储读直过）：陈旧即停车，泵经执行域刷新臂
    // await 点查后回驱本命令重评
    if self.acl_refresh_park_needed() {
      self.pending_acl_refresh = true;
      return AclGateVerdict::Parked;
    }
    // 自定义（扩展）命令按名鉴权（C# 分叉，内建命令热路保持无分支；
    // 分叉裁决为终审，不落位图门）
    if is_custom_command(cmd) {
      return if self.check_acl_permissions_for_custom_command(cmd) {
        AclGateVerdict::Permitted
      } else {
        AclGateVerdict::Denied
      };
    }
    if !self.acl_permits(cmd) {
      // C# OnACLOrNoScriptFailure：清理被拒自定义命令的会话引用
      self.on_acl_or_no_script_failure(cmd);
      return AclGateVerdict::Denied;
    }
    AclGateVerdict::Permitted
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissionsForCustomCommand
  ///
  /// 自定义命令按名鉴权（无 NoAuth 豁免）：未认证拒绝；分派器已填当前命令名
  /// 走 CanAccessCustomCommand（拒绝优先），未填回落泛型位图（C# 同款回落，
  /// +@custom 规则仍放行）。无 ACL 认证器 → default 用户 +@all 恒放行。
  pub fn check_acl_permissions_for_custom_command(&mut self, cmd: RespCommand) -> bool {
    let permitted = self.acl_authenticator.is_none()
      || match &self.acl_user_handle {
        Some(handle) => match &self.current_custom_command {
          // 分派器已填当前自定义命令 → 按名鉴权
          Some((custom_cmd, custom)) if *custom_cmd == cmd => {
            handle.user().can_access_custom_command(cmd, custom.name)
          }
          // 未填（正常流不应发生）→ 回落泛型位图
          _ => handle.user().can_access_command(cmd),
        },
        // 未认证
        None => false,
      };
    if !permitted {
      self.on_acl_or_no_script_failure(cmd);
      return false;
    }
    true
  }
  /// libs/server/Resp/AdminCommands.cs:OnACLOrNoScriptFailure
  ///
  /// C# 清理在途自定义命令的会话引用（currentCustom*Command = null；仅当
  /// 被拒命令为自定义命令族时清理，余者无在途状态可清）
  pub fn on_acl_or_no_script_failure(&mut self, cmd: RespCommand) {
    if is_custom_command(cmd) {
      self.current_custom_command = None;
    }
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkMonitor
  pub fn network_monitor(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "MONITOR");

    // C# MONITOR 未实现，字面回 "ERR unknown command"
    write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF
  pub fn network_commitaof(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# BlockingWait(CommitAofAsync(dbId))：转挂存储执行域慢路径闭环 AOF
    // 物理提交（与 SAVE/BGSAVE 同通道），应答文案在慢路径执行段写出
    self.route_checkpoint_slow(RespCommand::Commitaof, "COMMITAOF", parse_state, output)
  }
  /// SAVE / LASTSAVE / COMMITAOF 共同体：arity ..=1 门 → 可选 DBID 校验 →
  /// checkpoint 通道转挂（缺省提交全部活跃库；带 DBID 时先走库号校验）
  fn route_checkpoint_slow(
    &mut self,
    cmd: RespCommand,
    cmd_name: &str,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=1, output, cmd_name);
    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state[0], output) {
      return Ok(true);
    }
    self.route_slow_command(cmd, parse_state, output);
    Ok(true)
  }
  /// 慢路径转挂公共骨架（SAVE / BGSAVE / LASTSAVE / COMMITAOF / EXPDELSCAN / DEBUG）
  ///
  /// 参数校验在会话侧闭环后，闭包转挂存储执行域慢路径（checkpoint 通道 /
  /// lastsave 时间戳随 StoreGarnetApi 注入，过期键扫描经 store 原语）；
  /// C# 网络线程 BlockingWait 的 compio 等价物。存储执行域未挂载 = 装配
  /// 缺口，按失败惯例降级
  fn route_slow_command(&mut self, cmd: RespCommand, parse_state: &[&[u8]], output: &mut Vec<u8>) {
    if let Some(api) = &self.garnet_api {
      let args: Vec<Vec<u8>> = parse_state.iter().map(|a| a.to_vec()).collect();
      self.pending_slow = Some(SlowWait::for_command(
        api,
        cmd,
        args,
        self.resp_protocol_version,
      ));
    } else {
      output.write_resp_error(RESP_ERR_GENERIC);
    }
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT
  ///
  /// 显式键清单逐键 RMW HCOLLECT（清除过期字段并回写，任一 WRONGTYPE
  /// 记为最终错误）；首键 `*` 为全库对象扫描（C# ObjectCollect 批扫），
  /// 同步快路径不可达，降级异步闭环（与 KEYS/SCAN 同口径）
  pub fn network_hcollect<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    run_collect_keys!(
      self,
      parse_state,
      store,
      output,
      "HCOLLECT",
      hash_load_sync,
      |obj: &mut HashObject, ver, out| {
        obj.operate(HashOperation::Hcollect as u8, &[], 0, 0, out, ver);
      },
      |store, key, obj: &HashObject| {
        obj_save_or_gc(
          store,
          key,
          GarnetObjectType::Hash,
          obj,
          obj.is_empty(),
          |o| o.to_blob(),
        )
        .unwrap_or(false)
      }
    )
  }

  /// libs/server/Resp/AdminCommands.cs:NetworkZCOLLECT
  ///
  /// 同 NetworkHCOLLECT 的有序集合形态（ZCOLLECT 逐键 RMW；`*` 降级异步）
  pub fn network_zcollect<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    run_collect_keys!(
      self,
      parse_state,
      store,
      output,
      "ZCOLLECT",
      zset_load_sync,
      |obj: &mut SortedSetObject, ver, out| {
        obj.operate(SortedSetOperation::Zcollect as u8, &[], 0, 0, out, ver);
      },
      |store, key, obj: &SortedSetObject| zset_save_or_gc(store, key, obj).unwrap_or(false)
    )
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkProcessClusterCommand
  pub fn network_process_cluster_command(
    &mut self,
    cmd: RespCommand,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# clusterSession == null 即集群未启用，报禁用错误；集群形态经切面
    // 路由至 ProcessClusterCommands
    let Some(cluster) = self.cluster_session.clone() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return Ok(true);
    };
    // CLUSTER FLUSHALL_NS 收令帧调用方身份门。本命令属 rust 自增多租户
    // 管理面（C# 无 ns 维度、无对应收令臂，非移植产物），安全边界依据
    // doc/zh/db.md 3.5：收令帧为跨主节点全集群清库扇出，仅节点间连接或
    // ns0 超管身份可发起。两判据全部复用既有单点，不落第三套身份判定：
    // 节点间连接 = ClusterSessionFace::remote_node_id（CLUSTER GOSSIP 建链
    // 确立的对端节点 id 唯一读取口，CLIENT 类型门同源）；ns0 超管 =
    // 会话 namespace（<ns>#user 认证绑定唯一真值，doc/zh/db.md 三）。
    // 本入口是 CLUSTER 命令面唯一入层（process_cluster_commands 全仓唯一
    // 调用点），门落此处即臂入口一处收口；被拒会话回 wresp::cmd_strings
    // 既有权限文案单源 RESP_ERR_NOPERM，不新增第二处文本源
    if cmd == RespCommand::ClusterFlushallNs
      && self.namespace != 0
      && cluster.remote_node_id().is_none()
    {
      abort_with_error_message(output, cs::RESP_ERR_NOPERM);
      return Ok(true);
    }
    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    cluster.process_cluster_commands(cmd, &args, output, self.active_db_slot());
    // CLUSTER 子命令处理计数（对标 libs/cluster/Session/ClusterCommands.cs:170
    // 内层 switch 出口单次累加：switch 正常返回即入账，参数错误应答不豁免）。
    // 内层分派体在 wedb 侧、本切面不持会话指标句柄，故累加点按 C# 入层的同一
    // 判定 is_cluster_sub_command 投在调用侧：顶层 MIGRATE / FAILOVER /
    // REPLICAOF 走外层三臂不经内层出口，与 C# 一致地不计数
    if is_cluster_sub_command(cmd)
      && let Some(metrics) = &self.session_metrics
    {
      metrics.incr_total_cluster_commands_processed(1);
    }
    // 集群切面挂起的慢路径（CLUSTER RESET 的 HasKeysInSlots 扫描等）
    // 转挂会话慢路径槽，网络泵 await 闭环
    if let Some(slow) = cluster.take_pending_slow() {
      self.pending_slow = Some(slow);
    }
    // 切面致命断流（C# 集群命令 GarnetException clientResponse:false 上抛）
    // 转投影为会话致命哨兵：不写错误应答行，发尽累积应答后断连
    if let Some(msg) = cluster.take_fatal_disconnect() {
      log::error!("集群命令致命断流: {msg}");
      self.fatal_disconnect = true;
    }
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkDebug
  pub fn network_debug(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "DEBUG");

    if !self.can_run_debug() {
      abort_with_error_message(output, RESP_ERR_DEBUG_DISALLOWED);
      return Ok(true);
    }

    let command = parse_state[0];
    macro_rules! check_debug_len {
      ($len:pat) => {
        if !matches!(parse_state.len(), $len) {
          return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
            &ascii_sanitize(command),
            "DEBUG",
            output,
          );
        }
      };
    }

    if command.eq_ignore_ascii_case(b"PANIC") {
      // C# 刻意抛异常崩溃进程；rust 禁 panic 约束下按存储失败惯例降级
      output.write_resp_error(RESP_ERR_GENERIC);
    } else if command.eq_ignore_ascii_case(b"ERROR") {
      check_debug_len!(2);
      // DEBUG ERROR <string>：按 RESP 错误帧原样回显（客户端测试钩子）
      write_error_raw(output, &ascii_sanitize(parse_state[1]));
    } else if command.eq_ignore_ascii_case(b"LOG") {
      check_debug_len!(2);
      // C# 写服务器日志（logger.LogInformation）；rust 日志接线归派发域，
      log::info!("{}", ascii_sanitize(parse_state[1]));
      write_raw(output, cs::RESP_OK);
    } else if command.eq_ignore_ascii_case(b"FLUSHANDEVICT") {
      check_debug_len!(1);
      // C# mainStore.Log.FlushAndEvict(wait: true)：转挂存储执行域慢路径
      self.route_slow_command(RespCommand::Debug, parse_state, output);
    } else if command.eq_ignore_ascii_case(b"FORCEGC") {
      check_debug_len!(1..=2);
      if parse_state.len() == 2 {
        let Some(generation) = strict_i32(parse_state[1]) else {
          abort_with_error_message(output, ERR_INVALID_GC_GENERATION);
          return Ok(true);
        };
        // C# 上界为 GC.MaxGeneration（.NET 恒为 2）；rust 无分代 GC，
        // 按同值域拒绝非法代数
        if !(0..=2).contains(&generation) {
          abort_with_error_message(output, ERR_INVALID_GC_GENERATION);
          return Ok(true);
        }
      }
      // rust 无 GC.Collect 等价物；C# 回 "GC completed"
      output.write_resp_simple_string("GC completed");
    } else if command.eq_ignore_ascii_case(b"PURGEBP") {
      check_debug_len!(2);
      // C# TryGetManagerType：解析失败回语法错误
      let Some(manager_type) = manager_type_from_token(parse_state[1]) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      };
      // C# NetworkPurgeBP：ServerListener 遍历 storeWrapper.Servers 直清各
      // 监听器网络缓冲池，属节点网络层本地维护，绝不依赖集群激活态——本
      // 会话注入的池即泵装配期共享的全监听器池（GarnetServerTcp.Purge）
      if manager_type == ManagerType::ServerListener {
        if let Some(pool) = &self.listener_buffer_pool {
          pool.purge();
        }
        output.write_resp_simple_string(manager_type.gc_completed_text());
        return Ok(true);
      }
      // 迁移/复制管理器分支对标 C# ClusterPurgeBufferPool：clusterSession
      // == null → CLUSTER_DISABLED，否则转发 clusterProvider.PurgeBufferPool
      let Some(provider) = self.cluster_provider.as_ref() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
        return Ok(true);
      };
      provider.purge_buffer_pool(manager_type);
      // C# 成功路径 GC.Collect 后回 "GC completed for <type>"
      output.write_resp_simple_string(manager_type.gc_completed_text());
    } else if command.eq_ignore_ascii_case(b"HELP") {
      const DEBUG_HELP: [&str; 18] = [
        "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "ERROR <string>",
        "\tReturn a Redis protocol error with <string> as message. Useful for clients",
        "\tunit tests to simulate Redis errors.",
        "LOG <message>",
        "\tWrite <message> to the server log.",
        "FLUSHANDEVICT",
        "\tFlush the main store's in-memory log to disk and evict it (shifts HeadAddress to",
        "\tTailAddress) so subsequent reads are served from disk.",
        "FORCEGC [generation]",
        "\tForce a blocking garbage collection of the given generation (default: max).",
        "PURGEBP <manager-type>",
        "\tPurge the network buffer pool for the given manager (MigrationManager,",
        "\tReplicationManager, or ServerListener) and force a blocking GC.",
        "PANIC",
        "\tCrash the server simulating a panic.",
        "HELP",
        "\tPrints this help",
      ];
      output.write_resp_array_len(DEBUG_HELP.len());
      for line in DEBUG_HELP {
        output.write_resp_simple_string(line);
      }
    } else {
      abort_with_unknown_subcommand(output, &ascii_sanitize(parse_state[0]), "DEBUG");
    }
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkROLE
  pub fn network_role(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "ROLE");

    // C# 集群分支依赖 clusterProvider；standalone 路径 = *3 master :0 *0
    let Some(provider) = self.cluster_provider.as_ref() else {
      output.write_resp_array_len(3);
      output.write_resp_bulk_string(b"master");
      output.write_resp_int(0);
      output.write_resp_array_len(0);
      return Ok(true);
    };

    // usingShardedLog = AofPhysicalSublogCount > 1（ROLE 应答数组形态随其扩展）
    let using_sharded_log = provider.aof_sublog_count() > 1;
    if provider.is_primary() {
      let (replication_offset, replica_info) = provider.get_primary_info();
      output.write_resp_array_len(if using_sharded_log { 4 } else { 3 });
      output.write_resp_bulk_string(b"master");
      output.write_resp_int(replication_offset.get(0).unwrap_or(0));
      output.write_resp_array_len(replica_info.len());
      for replica in &replica_info {
        output.write_resp_array_len(if using_sharded_log { 4 } else { 3 });
        output.write_resp_bulk_string(replica.address.as_bytes());
        output.write_resp_int(i64::from(replica.port));
        output.write_resp_int(replica.replication_offset);
        if using_sharded_log {
          output.write_resp_bulk_string(replica.replication_offset_vector.as_bytes());
        }
      }
      if using_sharded_log {
        output.write_resp_bulk_string(replication_offset.to_aof_string().as_bytes());
      }
    } else {
      let role = provider.get_replica_info();
      output.write_resp_array_len(if using_sharded_log { 6 } else { 5 });
      output.write_resp_bulk_string(b"slave");
      output.write_resp_bulk_string(role.address.as_bytes());
      output.write_resp_int(i64::from(role.port));
      output.write_resp_bulk_string(role.replication_state.as_bytes());
      output.write_resp_int(role.replication_offset);
      if using_sharded_log {
        output.write_resp_bulk_string(role.replication_offset_vector.as_bytes());
      }
    }
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkSAVE
  pub fn network_save(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 阻塞等待 TakeCheckpointAsync(false)：闭包转挂存储执行域
    self.route_checkpoint_slow(RespCommand::Save, "SAVE", parse_state, output)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkEXPDELSCAN
  ///
  /// 不变式：与后台过期键删除扫描互斥。C# 判据序 arity → 频率门 → DBID：
  /// `EXPIRED_KEY_DELETION_SCAN_FREQ > 0` 即
  /// AbortWithErrorMessage(RESP_ERR_EXPDELSCAN_INVALID)——后台任务
  /// （StoreWrapper.cs:TryStartExpiredKeyDeletionTask / ReconcilePrimaryTask）
  /// 与本门同读一槽，杜绝双扫描器并发
  pub fn network_expdelscan(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=1, output, "EXPDELSCAN");

    // 频率门（C# AdminCommands.cs:NetworkEXPDELSCAN）：后台扫描启用时
    // 拒绝手动扫描，判据以槽位为准
    if self
      .runtime_config
      .get_int(ServerConfigType::ExpiredKeyDeletionScanFreq)
      > 0
    {
      abort_with_error_message(output, cs::RESP_ERR_EXPDELSCAN_INVALID);
      return Ok(true);
    }

    if !parse_state.is_empty() && !self.try_parse_database_id(parse_state[0], output) {
      return Ok(true);
    }

    // C# 调 storeWrapper.ExpiredKeyDeletionScan（可变区过期键删除扫描）：
    // 转挂存储执行域慢路径（exec_slow 的 Expdelscan 臂），应答 *2 计数对
    self.route_slow_command(RespCommand::Expdelscan, parse_state, output);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkLASTSAVE
  pub fn network_lastsave(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 回数据库 LastSaveTime：经存储执行域 checkpoint 通道读取
    self.route_checkpoint_slow(RespCommand::Lastsave, "LASTSAVE", parse_state, output)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkBGSAVE
  pub fn network_bgsave(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=2, output, "BGSAVE");

    // BGSAVE [SCHEDULE] [DBID]
    let mut token_idx = 0usize;
    if !parse_state.is_empty() && parse_state[0].eq_ignore_ascii_case(b"SCHEDULE") {
      token_idx = 1;
    }
    if parse_state.len() > token_idx && !self.try_parse_database_id(parse_state[token_idx], output)
    {
      return Ok(true);
    }

    // C# 阻塞等待 TakeCheckpointAsync(true)：闭包同 SAVE 转挂存储执行域
    self.route_slow_command(RespCommand::Bgsave, parse_state, output);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:TryParseDatabaseId
  ///
  /// 校验 DBID 令牌（溢出走非整数档对标 C# TryGetInt i32；前导零拒收系 rust 严格收口，C# TryGetInt 因死参放行 007，见 doc/zh/deviations.md §32）；
  /// 失败时已写出错误应答并返回 false。
  /// 判定序：数字解析 → 范围门（dbId >= MaxDatabases）；
  /// C# 集群模式拒 dbId>0 的门禁刻意不收（doc/zh/db.md §1.3 已删除集群切库限制）
  pub fn try_parse_database_id(&mut self, raw: &[u8], output: &mut Vec<u8>) -> bool {
    let Some(db_id) = parse_db_index_arg(raw, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, output)
    else {
      return false;
    };

    if db_id >= self.max_databases {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return false;
    }

    true
  }
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithWrongNumberOfArgumentsOrUnknownSubcommand
  ///
  /// `ERR unknown subcommand or wrong number of arguments for '{0}'. Try {1} HELP`
  fn abort_with_wrong_number_of_arguments_or_unknown_subcommand(
    &mut self,
    sub_command: &str,
    cmd_name: &str,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    abort_with_unknown_subcommand_or_wrong_num_args(output, sub_command, cmd_name);
    Ok(true)
  }
}
