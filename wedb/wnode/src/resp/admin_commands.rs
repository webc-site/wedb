use std::mem;

use itoa::Buffer;
use wbase::num::{DbIndexError, parse_db_index, strict_i32};
use wcol::{
  ObjectOutput, hash::hash_object::HashOperation, zset::sorted_set_object::SortedSetOperation,
};
use wconf::ServerConfigType;
use wresp::{
  check_args::check_arg_count,
  cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_GENERIC, abort_with_error_message,
    abort_with_unknown_subcommand, abort_with_unknown_subcommand_or_wrong_num_args,
    write_error_raw, write_raw,
  },
  command::RespCommand,
  ext::{RespSliceExt, RespVecExt},
};
use wval::GarnetObjectType;

use super::resp_server_session::RespServerSession;
use crate::{
  resp::{
    objects::{
      hash_commands::{HashLoad, hash_load_sync},
      object_store_utils::{GarnetObjectPayload, obj_save_or_gc},
      sorted_set_commands::{ZsetLoad, zset_load_sync, zset_save_or_gc},
    },
    slow_path::SlowWait,
  },
  session_parse_state_extensions::manager_type_from_token,
};

/// GC 代数非法文案（本域两处复用）。
const ERR_INVALID_GC_GENERATION: &str = "ERR Invalid GC generation.";

/// 自定义（扩展）命令族（C# CheckACLPermissions 的 CustomCommand 分叉判定）
#[inline]
fn is_custom_command(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::Customrawstringcmd
      | RespCommand::Customobjcmd
      | RespCommand::Customtxn
      | RespCommand::Customprocedure
  )
}

impl RespServerSession {
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
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
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
  pub fn check_acl_permissions(&mut self, cmd: RespCommand) -> bool {
    // 自定义（扩展）命令按名鉴权（C# 分叉，内建命令热路保持无分支）
    if is_custom_command(cmd) {
      return self.check_acl_permissions_for_custom_command(cmd);
    }
    if !self.acl_permits(cmd) {
      // C# OnACLOrNoScriptFailure：清理被拒自定义命令的会话引用
      self.on_acl_or_no_script_failure(cmd);
      return false;
    }
    true
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
            handle.load().can_access_custom_command(cmd, custom.name)
          }
          // 未填（正常流不应发生）→ 回落泛型位图
          _ => handle.load().can_access_command(cmd),
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
    check_arg_count!(parse_state, ..=1, output, "COMMITAOF");

    // 缺省提交全部活跃库；带 DBID 时先走库号校验
    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state[0], output) {
      return Ok(true);
    }

    // C# BlockingWait(CommitAofAsync(dbId))：转挂存储执行域慢路径闭环 AOF
    // 物理提交（与 SAVE/BGSAVE 同通道），应答文案在慢路径执行段写出
    self.route_slow_command(RespCommand::Commitaof, parse_state, output);
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
  pub fn network_hcollect<D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "HCOLLECT");

    // C# ObjectCollect("*")：DbScan 批扫 + 收集锁（进行中回 NOTFOUND →
    // already in progress）；rust 同步执行域无全库扫描通道，降级异步
    if parse_state[0] == b"*" {
      return Ok(false);
    }

    let mut wrong_type = false;
    let mut scratch = Vec::new();
    for key in parse_state {
      let mut obj = match hash_load_sync(store, key, &mut scratch) {
        // 磁盘候选：降级异步（与对象命令同步执行域同口径）
        HashLoad::Degrade => return Ok(false),
        HashLoad::WrongType => {
          wrong_type = true;
          continue;
        }
        // C# RMW NOTFOUND → 跳过
        HashLoad::Missing => continue,
        HashLoad::Present(o) => o,
      };

      // HCOLLECT 无 RESP 输出面（C# 特殊通道无应答负载）：仅取副作用，
      // 挂本地 sink 不触会话输出
      obj.operate(
        HashOperation::Hcollect as u8,
        &[] as &[&[u8]],
        0,
        0,
        &mut ObjectOutput::mount(&mut Vec::new()),
        self.resp_protocol_version,
      );
      // HCOLLECT 清出全部过期字段后可能为空：空对象整键回收
      if obj_save_or_gc(
        store,
        key,
        GarnetObjectType::Hash,
        &obj,
        obj.is_empty(),
        |o| o.to_blob(),
      )
      .unwrap_or(false)
      {
        continue;
      }
      return Ok(false);
    }

    if wrong_type {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
    } else {
      write_raw(output, cs::RESP_OK);
    }
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkZCOLLECT
  ///
  /// 同 NetworkHCOLLECT 的有序集合形态（ZCOLLECT 逐键 RMW；`*` 降级异步）
  pub fn network_zcollect<D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "ZCOLLECT");

    if parse_state[0] == b"*" {
      return Ok(false);
    }

    let mut wrong_type = false;
    let mut scratch = Vec::new();
    for key in parse_state {
      let mut obj = match zset_load_sync(store, key, &mut scratch) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::WrongType => {
          wrong_type = true;
          continue;
        }
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };

      // ZCOLLECT 无 RESP 输出面（C# 特殊通道无应答负载）：仅取副作用，
      // 挂本地 sink 不触会话输出
      obj.operate(
        SortedSetOperation::Zcollect as u8,
        &[] as &[&[u8]],
        0,
        0,
        &mut ObjectOutput::mount(&mut Vec::new()),
        self.resp_protocol_version,
      );
      if zset_save_or_gc(store, key, &obj).unwrap_or(false) {
        continue;
      }
      return Ok(false);
    }

    if wrong_type {
      write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
    } else {
      write_raw(output, cs::RESP_OK);
    }
    Ok(true)
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
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    cluster.process_cluster_commands(cmd, &args, output, self.active_db_slot());
    // 集群命令处理计数（对标 libs/cluster/Session/ClusterCommands.cs:170 分发
    // 出口单次累加：switch 正常返回即入账，参数错误应答不豁免）
    if let Some(metrics) = &self.session_metrics {
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
    if command.eq_ignore_ascii_case(b"PANIC") {
      // C# 刻意抛异常崩溃进程；rust 禁 panic 约束下按存储失败惯例降级
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"ERROR") {
      if parse_state.len() != 2 {
        return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
          command.as_str_safe(),
          "DEBUG",
          output,
        );
      }
      // DEBUG ERROR <string>：按 RESP 错误帧原样回显（客户端测试钩子）
      write_error_raw(output, parse_state[1].as_str_safe());
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"LOG") {
      if parse_state.len() != 2 {
        return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
          command.as_str_safe(),
          "DEBUG",
          output,
        );
      }
      // C# 写服务器日志（logger.LogInformation）；rust 日志接线归派发域，
      log::info!("{}", parse_state[1].as_str_safe());
      write_raw(output, cs::RESP_OK);
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"FLUSHANDEVICT") {
      if parse_state.len() != 1 {
        return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
          command.as_str_safe(),
          "DEBUG",
          output,
        );
      }
      // C# mainStore.Log.FlushAndEvict(wait: true)：转挂存储执行域慢路径
      self.route_slow_command(RespCommand::Debug, parse_state, output);
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"FORCEGC") {
      if parse_state.len() > 2 {
        return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
          command.as_str_safe(),
          "DEBUG",
          output,
        );
      }
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
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"PURGEBP") {
      if parse_state.len() != 2 {
        return self.abort_with_wrong_number_of_arguments_or_unknown_subcommand(
          command.as_str_safe(),
          "DEBUG",
          output,
        );
      }
      // C# TryGetManagerType：解析失败回语法错误
      let Some(manager_type) = manager_type_from_token(parse_state[1]) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      };
      // C# ClusterPurgeBufferPool：clusterProvider == null → CLUSTER_DISABLED
      let Some(provider) = self.cluster_provider.clone() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
        return Ok(true);
      };
      // 集群提供者转发 clusterProvider.PurgeBufferPool（MigrationManager /
      // ReplicationManager 双分支；ServerListener 对标 C# GarnetException 拒绝）
      provider.purge_buffer_pool(manager_type);
      // C# 成功路径 GC.Collect 后回 "GC completed for <type>"
      output.write_resp_simple_string(manager_type.gc_completed_text());
      return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"HELP") {
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
      return Ok(true);
    }

    abort_with_unknown_subcommand(output, parse_state[0].as_str_safe(), "DEBUG");
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
    let Some(provider) = self.cluster_provider.clone() else {
      output.write_resp_array_len(3);
      output.write_resp_bulk_string(b"master");
      output.write_resp_int(0);
      output.extend_from_slice(b"*0\r\n");
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
          output.write_resp_bulk_string(replication_offset.to_aof_string().as_bytes());
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
        // C# 写 role.replication_offset 全地址字符串（多子日志形态）；
        // RoleInfo 副本侧偏移为单值投影，按数值字符串输出
        let mut b = Buffer::new();
        output.write_resp_bulk_string(b.format(role.replication_offset).as_bytes());
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
    check_arg_count!(parse_state, ..=1, output, "SAVE");

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state[0], output) {
      return Ok(true);
    }

    // C# 阻塞等待 TakeCheckpointAsync(false)：闭包转挂存储执行域
    self.route_slow_command(RespCommand::Save, parse_state, output);
    Ok(true)
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
    check_arg_count!(parse_state, ..=1, output, "LASTSAVE");

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state[0], output) {
      return Ok(true);
    }

    // C# 回数据库 LastSaveTime：经存储执行域 checkpoint 通道读取
    self.route_slow_command(RespCommand::Lastsave, parse_state, output);
    Ok(true)
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
  /// 校验 DBID 令牌（C# TryGetInt i32 严格口径，超 int32 值域即非整数档）；
  /// 失败时已写出错误应答并返回 false。
  /// 判定序：数字解析 → 范围门（dbId >= MaxDatabases）；
  /// C# 集群模式拒 dbId>0 的门禁刻意不收（doc/zh/db.md §1.3 已删除集群切库限制）
  pub fn try_parse_database_id(&mut self, raw: &[u8], output: &mut Vec<u8>) -> bool {
    let db_id = match parse_db_index(raw) {
      // 线面 i32 档，内部库 ID 仍 u64（Ok 档非负，as u64 升位无损）
      Ok(idx) => idx as u64,
      Err(DbIndexError::NotInteger) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return false;
      }
      Err(DbIndexError::OutOfRange) => {
        abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
        return false;
      }
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
