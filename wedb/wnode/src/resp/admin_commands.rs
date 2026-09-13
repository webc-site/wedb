use std::mem;

use wobject::{
  hash::hash_object::HashOperation, sortedset::sorted_set_object::SortedSetOperation,
  types::object_output::ObjectOutput,
};
use wresp::{
  RespCommand, RespSliceExt, RespVecExt, check_arg_count, cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_GENERIC, RESP_ERR_MODULE_DISALLOWED,
    RESP_ERR_REGISTERCS_DISALLOWED, abort_with_error_message, abort_with_unknown_subcommand,
    abort_with_unknown_subcommand_or_wrong_num_args, write_error_raw, write_raw,
  },
  strict_i32,
};

use super::resp_server_session::RespServerSession;
use crate::resp::objects::{
  hash_commands::{HashLoad, hash_load_sync},
  object_store_utils::{hash_to_blob, make_object_input, obj_save_or_gc},
  sorted_set_commands::{ZsetLoad, zset_load_sync, zset_save_or_gc},
};

/// GC 代数非法文案（本域两处复用）。
const ERR_INVALID_GC_GENERATION: &str = "ERR Invalid GC generation.";

/// libs/server/Servers/GarnetServerOptions.cs:MaxDatabases
///
/// C# 默认 16；rust 会话层未接服务器选项，按默认值校验 DBID
const MAX_DATABASES: i64 = 16;

/// 集群启用配置（单机模式默认为 false；C# 为 serverOptions.EnableCluster）
const CLUSTER_ENABLED: bool = false;

impl RespServerSession {
  /// C# ProcessOtherCommands / ProcessAdminCommands 的 admin 会话级 arm 集
  ///（实现为本文件各 network_* 处理器）
  ///
  /// MONITOR / DEBUG / REGISTERCS / MODULE LOADCS / SAVE / BGSAVE / LASTSAVE /
  /// COMMITAOF / EXPDELSCAN（C# ProcessAdminCommands switch 的无存储面子集）；
  /// `None` 表示命令不属于本族，调用方继续后续分派
  pub fn process_admin_session_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    let args = self.get_arg_slices();
    let mut output = mem::take(&mut self.output);
    let handled = match cmd {
      RespCommand::Monitor => self.network_monitor(&args, &mut output),
      RespCommand::Debug => self.network_debug(&args, &mut output),
      RespCommand::Registercs => self.network_register_cs(&args, &mut output),
      RespCommand::ModuleLoadcs => self.network_module_load(&args, &mut output),
      RespCommand::Save => self.network_save(&args, &mut output),
      RespCommand::Bgsave => self.network_bgsave(&args, &mut output),
      RespCommand::Lastsave => self.network_lastsave(&args, &mut output),
      RespCommand::Commitaof => self.network_commitaof(&args, &mut output),
      RespCommand::Expdelscan => self.network_expdelscan(&args, &mut output),
      _ => {
        self.output = output;
        return None;
      }
    };
    self.output = output;
    Some(handled.unwrap_or(true))
  }
  /// libs/server/Resp/AdminCommands.cs:CheckScriptPermissions
  ///
  /// 脚本 no-script 位图检查（SCRIPT LOAD 域）；rust 脚本域未维护 no-script
  /// 位图，等价 C# 位图为空的放行路径：恒允许
  pub fn check_script_permissions(&mut self, _cmd: RespCommand) -> bool {
    true
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissions
  ///
  /// C#: (!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝。默认
  /// NoAuth 认证器 IsAuthenticated = true → 恒放行；rust ACL（wacl）接线后
  /// 在此分叉
  pub fn check_acl_permissions(&mut self, _cmd: RespCommand) -> bool {
    true
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissionsForCustomCommand
  ///
  /// 自定义命令按名鉴权；同上按默认放行路径处理
  pub fn check_acl_permissions_for_custom_command(&mut self, _cmd: RespCommand) -> bool {
    true
  }
  /// libs/server/Resp/AdminCommands.cs:OnACLOrNoScriptFailure
  ///
  /// C# 清理在途自定义命令的会话引用（currentCustom*Command = null）
  pub fn on_acl_or_no_script_failure(&mut self, _cmd: RespCommand) {
    self.current_custom_command = None;
  }
  /// libs/server/Resp/AdminCommands.cs:CommitAOFAsync
  ///
  /// C# 委托 storeWrapper.CommitAOFAsync（异步 AOF 落盘闭环）
  pub async fn commit_aof_async(&mut self) -> wresp::Result<bool> {
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkMonitor
  pub fn network_monitor(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, empty, output, "MONITOR");

    // C# MONITOR 未实现，字面回 "ERR unknown command"
    write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:TryImportCommandsData
  ///
  /// C# 命令信息 JSON 文件导入管道（文件存在性 → 允许路径校验 → 反序列化），
  /// 服务于 REGISTERCS 的 INFO/DOCS 附件；rust 自定义命令域无该导入管道，
  /// 恒按"不可访问命令信息文件"失败
  pub fn try_import_commands_data(&mut self) -> bool {
    false
  }
  /// libs/server/Resp/AdminCommands.cs:TryRegisterCustomCommands
  ///
  /// C# 经 .NET 反射加载装配件并实例化自定义命令类；rust 无装配域，恒按
  /// "无法实例化类"失败（C# 语义内的注册失败路径）
  pub fn try_register_custom_commands(&mut self) -> bool {
    false
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkRegisterCs
  pub fn network_register_cs(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 6, output, "REGISTERCS");

    if !self.can_run_module() {
      // 对标 C# AbortWithErrorMessage(GenericErrCommandDisallowedWithOption,
      // REGISTERCS, "enable-module-command")
      abort_with_error_message(output, RESP_ERR_REGISTERCS_DISALLOWED);
      return Ok(true);
    }

    // 选项解析之后的注册依赖 .NET 装配域（见 TryRegisterCustomCommands），
    // rust 侧按注册失败路径降级
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INSTANTIATING_CLASS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkModuleLoad
  pub fn network_module_load(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, !empty, output, "MODULE|LOADCS");

    if !self.can_run_module() {
      abort_with_error_message(output, RESP_ERR_MODULE_DISALLOWED);
      return Ok(true);
    }

    // .NET 装配件加载在 rust 无对应物（ModuleUtils 域未移植），按加载失败降级
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INSTANTIATING_CLASS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF
  pub fn network_commitaof(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, <= 1, output, "COMMITAOF");

    // 缺省提交全部活跃库；带 DBID 时先走库号校验
    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, output)? {
      return Ok(true);
    }

    // C# 阻塞等待 storeWrapper.CommitAOFAsync；rust 会话层无该通道
    // （store_wrapper.rs:CommitAofAsync 未建成），按本域存储失败惯例降级
    output.write_resp_error(RESP_ERR_GENERIC);
    Ok(true)
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
    check_arg_count!(parse_state, !empty, output, "HCOLLECT");

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
        HashLoad::Error => {
          wrong_type = true;
          continue;
        }
        // C# RMW NOTFOUND → 跳过
        HashLoad::Missing => continue,
        HashLoad::Present(o) => o,
      };

      obj.operate(
        &make_object_input(
          wval::GarnetObjectType::Hash,
          HashOperation::Hcollect as u8,
          &[] as &[&[u8]],
          0,
          0,
        ),
        &mut ObjectOutput::new(),
        self.resp_protocol_version,
      );
      // HCOLLECT 清出全部过期字段后可能为空：空对象整键回收
      if obj_save_or_gc(
        store,
        key,
        wval::GarnetObjectType::Hash as u8,
        &obj,
        obj.is_empty(),
        hash_to_blob,
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
    check_arg_count!(parse_state, !empty, output, "ZCOLLECT");

    if parse_state[0] == b"*" {
      return Ok(false);
    }

    let mut wrong_type = false;
    let mut scratch = Vec::new();
    for key in parse_state {
      let mut obj = match zset_load_sync(store, key, &mut scratch) {
        ZsetLoad::Degrade => return Ok(false),
        ZsetLoad::Error => {
          wrong_type = true;
          continue;
        }
        ZsetLoad::Missing => continue,
        ZsetLoad::Present(o) => o,
      };

      obj.operate(
        &make_object_input(
          wval::GarnetObjectType::SortedSet,
          SortedSetOperation::Zcollect as u8,
          &[] as &[&[u8]],
          0,
          0,
        ),
        &mut ObjectOutput::new(),
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
    cmd: wresp::RespCommand,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# clusterSession == null 即集群未启用，报禁用错误；集群形态经切面
    // 路由至 ProcessClusterCommands
    let Some(cluster) = self.cluster_session.clone() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return Ok(true);
    };
    let args = self.get_arg_slices();
    cluster.process_cluster_commands(cmd, &args, output);
    // 集群切面挂起的慢路径（CLUSTER RESET 的 HasKeysInSlots 扫描等）
    // 转挂会话慢路径槽，网络泵 await 闭环
    if let Some(slow) = cluster.take_pending_slow() {
      self.pending_slow = Some(slow);
    }
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkDebug
  pub fn network_debug(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, !empty, output, "DEBUG");

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
      // C# 刷并驱逐主存储混合日志（mainStore.Log.FlushAndEvict）；rust wkv
      // 无会话可达的同义入口，按存储失败惯例降级
      output.write_resp_error(RESP_ERR_GENERIC);
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
      // PurgeBPCommand 域未建成（purge_bp_command.rs），按失败惯例降级
      output.write_resp_error(RESP_ERR_GENERIC);
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
    check_arg_count!(parse_state, empty, output, "ROLE");

    // C# 集群分支依赖 clusterProvider；standalone 路径 = *3 master :0 *0
    let Some(cluster) = self.cluster_session.clone() else {
      output.write_resp_array_len(3);
      output.write_resp_bulk_string(b"master");
      output.write_resp_int(0);
      output.extend_from_slice(b"*0\r\n");
      return Ok(true);
    };

    // usingShardedLog = AofPhysicalSublogCount > 1（ROLE 应答数组形态随其扩展）
    let using_sharded_log = cluster.aof_sublog_count() > 1;
    if cluster.is_primary() {
      let (replication_offset, replica_info) = cluster.get_primary_info();
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
      let role = cluster.get_replica_info();
      output.write_resp_array_len(if using_sharded_log { 6 } else { 5 });
      output.write_resp_bulk_string(b"slave");
      output.write_resp_bulk_string(role.address.as_bytes());
      output.write_resp_int(i64::from(role.port));
      output.write_resp_bulk_string(role.replication_state.as_bytes());
      output.write_resp_int(role.replication_offset);
      if using_sharded_log {
        // C# 写 role.replication_offset 全地址字符串（多子日志形态）；
        // RoleInfo 副本侧偏移为单值投影，按数值字符串输出
        let mut b = itoa::Buffer::new();
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
    check_arg_count!(parse_state, <= 1, output, "SAVE");

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, output)? {
      return Ok(true);
    }

    // C# 阻塞等待 TakeCheckpointAsync(false)；rust 检查点通道未接线
    // （store_wrapper.rs:TakeCheckpointAsync 未建成），按失败惯例降级并区分
    // 既有 C# 错误语义（checkpoint already in progress 不可达）
    output.write_resp_error(RESP_ERR_GENERIC);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkEXPDELSCAN
  pub fn network_expdelscan(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, <= 1, output, "EXPDELSCAN");

    // C# 先查 EXPIRED_KEY_DELETION_SCAN_FREQ 运行时配置；rust 运行时配置域
    // 未建成（后台扫描亦未启用），该拦截不可达

    let mut db_args: [&[u8]; 1] = [&[]];
    if !parse_state.is_empty() {
      db_args[0] = parse_state[0];
      if !self.try_parse_database_id(&db_args, output)? {
        return Ok(true);
      }
    }

    // C# 调 storeWrapper.ExpiredKeyDeletionScan（可变区过期键删除扫描）；
    // rust wkv 无会话可达入口，按失败惯例降级（绝不虚报扫描计数）
    output.write_resp_error(RESP_ERR_GENERIC);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkLASTSAVE
  pub fn network_lastsave(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, <= 1, output, "LASTSAVE");

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, output)? {
      return Ok(true);
    }

    // C# 回数据库 LastSaveTime；rust 检查点域未接线无时间戳来源，按失败
    // 惯例降级（不虚报时间戳）
    output.write_resp_error(RESP_ERR_GENERIC);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkBGSAVE
  pub fn network_bgsave(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, <= 2, output, "BGSAVE");

    // BGSAVE [SCHEDULE] [DBID]
    let mut token_idx = 0usize;
    if !parse_state.is_empty() && parse_state[0].eq_ignore_ascii_case(b"SCHEDULE") {
      token_idx = 1;
    }
    if parse_state.len() > token_idx {
      let db_args: [&[u8]; 1] = [parse_state[token_idx]];
      if !self.try_parse_database_id(&db_args, output)? {
        return Ok(true);
      }
    }

    // C# 阻塞等待 TakeCheckpointAsync(true)；检查点通道缺口同 NetworkSAVE
    output.write_resp_error(RESP_ERR_GENERIC);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:TryParseDatabaseId
  ///
  /// 校验 DBID 令牌（C# TryGetInt i32 严格口径）；失败时已写出错误应答并返回 false
  pub fn try_parse_database_id(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(db_id) = strict_i32(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(false);
    };
    let db_id = i64::from(db_id);

    // 集群模式禁非零 DBID；rust 集群会话域未挂载（等效集群未启用），拦截不可达
    if CLUSTER_ENABLED && db_id > 0 {
      abort_with_error_message(output, cs::RESP_ERR_DB_ID_CLUSTER_MODE);
      return Ok(false);
    }

    if !(0..MAX_DATABASES).contains(&db_id) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(false);
    }

    Ok(true)
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
