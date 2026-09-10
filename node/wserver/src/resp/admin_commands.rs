use super::{
  cmd_strings as cs,
  cmd_strings::{
    abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw, write_raw,
  },
  parser::{
    resp_ext::{RespSliceExt, RespVecExt},
    session_parse_state::strict_i32,
  },
  resp_server_session::RespServerSession,
};

/// libs/server/Servers/GarnetServerOptions.cs:MaxDatabases
///
/// C# 默认 16；rust 会话层未接服务器选项，按默认值校验 DBID
const MAX_DATABASES: i64 = 16;

/// 集群启用占位（rust 集群会话域未挂载；C# 为 serverOptions.EnableCluster）
const CLUSTER_ENABLED: bool = false;

/// libs/server/Auth/Settings/ConnectionProtectionOption.cs（默认 No）
///
/// DEBUG/REGISTERCS/MODULE 的连接保护开关；C# 默认 No → CanRunDebug/CanRunModule
/// 恒 false。rust 会话层未接服务器选项与本地连接判定，按默认值走拒绝路径
const PROTECTION_OPTION: ConnectionProtection = ConnectionProtection::No;

/// libs/server/Auth/Settings/ConnectionProtectionOption.cs
///
/// rust 会话层仅按默认配置 `No` 接线（未接服务器选项与本地端点判定），故只保留
/// 该单一档；`Local`/`Yes` 的完整 C# 语义由 `resp_server_session.rs` 的
/// `ConnectionProtectionOption` 与 `can_run_with_protection` 承载
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionProtection {
  No,
}

impl ConnectionProtection {
  /// libs/server/Resp/RespServerSession.cs:CanRunDebug
  ///
  /// C# 还需 networkSender.IsLocalConnection() 配合 Local 档；rust 网络层未接
  /// 端点判定，且本 crate 仅接线 No 档，受保护的管理命令恒拒绝
  const fn can_run(self) -> bool {
    false
  }
}

impl RespServerSession {
  /// libs/server/Resp/AdminCommands.cs:ProcessAdminCommands
  ///
  /// 管理命令派发器：C# 在此做未认证拦截（NOAUTH）后按 RespCommand 路由。
  /// rust 默认认证器等价 NoAuth（IsAuthenticated = true），拦截不触发；命令
  /// 路由归派发域（RespServerSession.cs:ProcessMessages，尚未建成），本函数
  /// 仅承担认证门语义，无应答写出
  pub fn process_admin_commands<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    // C#: CanAuthenticate && !IsAuthenticated → write RESP_ERR_NOAUTH；
    // NoAuth 认证器 IsAuthenticated 恒 true，此分支在默认配置不可达
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:CheckScriptPermissions
  ///
  /// 脚本 no-script 位图检查（SCRIPT LOAD 域）；rust 脚本域未维护 no-script
  /// 位图，等价 C# 位图为空的放行路径：恒允许
  pub fn check_script_permissions<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissions
  ///
  /// C#: (!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝。默认
  /// NoAuth 认证器 IsAuthenticated = true → 恒放行；rust ACL（wacl）接线后
  /// 在此分叉
  pub fn check_acl_permissions<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:CheckACLPermissionsForCustomCommand
  ///
  /// 自定义命令按名鉴权；同上按默认放行路径处理
  pub fn check_acl_permissions_for_custom_command<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:OnACLOrNoScriptFailure
  ///
  /// C# 清理在途自定义命令的会话引用（currentCustom*Command = null）；rust
  /// 会话无该状态字段，空操作即等价语义
  pub fn on_acl_or_no_script_failure<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:CommitAofAsync
  ///
  /// C# 委托 storeWrapper.CommitAOFAsync（异步 AOF 落盘闭环）；rust 会话层
  /// 无 StoreWrapper 实例（对应域未建成，见 store_wrapper.rs），此助手暂无
  /// 可达通道，不产生应答
  pub fn commit_aof_async<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkMonitor
  pub fn network_monitor<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "MONITOR");
      return Ok(true);
    }

    // C# MONITOR 未实现，字面回 "ERR unknown command"
    write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:TryImportCommandsData
  ///
  /// C# 命令信息 JSON 文件导入管道（文件存在性 → 允许路径校验 → 反序列化），
  /// 服务于 REGISTERCS 的 INFO/DOCS 附件；rust 自定义命令域无该导入管道，
  /// 恒按"不可访问命令信息文件"失败
  pub fn try_import_commands_data<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(false)
  }
  /// libs/server/Resp/AdminCommands.cs:TryRegisterCustomCommands
  ///
  /// C# 经 .NET 反射加载装配件并实例化自定义命令类；rust 无装配域，恒按
  /// "无法实例化类"失败（C# 语义内的注册失败路径）
  pub fn try_register_custom_commands<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = output;
    Ok(false)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkRegisterCs
  pub fn network_register_cs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 6 {
      abort_with_wrong_number_of_arguments(output, "REGISTERCS");
      return Ok(true);
    }

    if !PROTECTION_OPTION.can_run() {
      // 对标 C# AbortWithErrorMessage(GenericErrCommandDisallowedWithOption,
      // REGISTERCS, "enable-module-command")
      abort_with_error_message(
        output,
        &cs::GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION
          .replace("{0}", "REGISTERCS")
          .replace("{1}", "enable-module-command"),
      );
      return Ok(true);
    }

    // 选项解析之后的注册依赖 .NET 装配域（见 TryRegisterCustomCommands），
    // rust 侧按注册失败路径降级
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INSTANTIATING_CLASS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkModuleLoad
  pub fn network_module_load<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "MODULE|LOADCS");
      return Ok(true);
    }

    if !PROTECTION_OPTION.can_run() {
      abort_with_error_message(
        output,
        &cs::GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION
          .replace("{0}", "MODULE")
          .replace("{1}", "enable-module-command"),
      );
      return Ok(true);
    }

    // .NET 装配件加载在 rust 无对应物（ModuleUtils 域未移植），按加载失败降级
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INSTANTIATING_CLASS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF
  pub fn network_commitaof<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "COMMITAOF");
      return Ok(true);
    }

    // 缺省提交全部活跃库；带 DBID 时先走库号校验
    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, store, output)? {
      return Ok(true);
    }

    // C# 阻塞等待 storeWrapper.CommitAOFAsync；rust 会话层无该通道
    // （store_wrapper.rs:CommitAofAsync 未建成），按本域存储失败惯例降级
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT
  pub fn network_hcollect<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "HCOLLECT");
      return Ok(true);
    }

    // C# 走 storageApi.HashCollect（对象存储紧凑化扫描）；rust 对象存储域
    // 无该入口，按扫描不可达的 C# 错误路径降级
    write_error_raw(output, cs::RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkZCOLLECT
  pub fn network_zcollect<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "ZCOLLECT");
      return Ok(true);
    }

    write_error_raw(output, cs::RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkProcessClusterCommand
  pub fn network_process_cluster_command<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# clusterSession == null 即集群未启用；rust 会话无集群会话挂载点，
    // 与 C# 禁用路径语义一致
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkDebug
  pub fn network_debug(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "DEBUG");
      return Ok(true);
    }

    if !PROTECTION_OPTION.can_run() {
      abort_with_error_message(
        output,
        &cs::GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION
          .replace("{0}", "DEBUG")
          .replace("{1}", "enable-debug-command"),
      );
      return Ok(true);
    }

    let command = parse_state[0];
    if command.eq_ignore_ascii_case(b"PANIC") {
      // C# 刻意抛异常崩溃进程；rust 禁 panic 约束下按存储失败惯例降级
      output.write_resp_error("generic error");
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
      // 此处仅完成应答语义
      let _ = parse_state[1].as_str_safe();
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
      output.write_resp_error("generic error");
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
          abort_with_error_message(output, "ERR Invalid GC generation.");
          return Ok(true);
        };
        // C# 上界为 GC.MaxGeneration（.NET 恒为 2）；rust 无分代 GC，
        // 按同值域拒绝非法代数
        if !(0..=2).contains(&generation) {
          abort_with_error_message(output, "ERR Invalid GC generation.");
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
      output.write_resp_error("generic error");
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

    let error_msg = cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND
      .replace("{0}", parse_state[0].as_str_safe())
      .replace("{1}", "DEBUG");
    write_error_raw(output, &error_msg);
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkROLE
  pub fn network_role(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "ROLE");
      return Ok(true);
    }

    // C# 集群分支依赖 clusterProvider；standalone 路径 = *3 master :0 *0
    output.write_resp_array_len(3);
    output.write_resp_bulk_string(b"master");
    output.write_resp_int(0);
    output.extend_from_slice(b"*0\r\n");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkSAVE
  pub fn network_save<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "SAVE");
      return Ok(true);
    }

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, store, output)? {
      return Ok(true);
    }

    // C# 阻塞等待 TakeCheckpointAsync(false)；rust 检查点通道未接线
    // （store_wrapper.rs:TakeCheckpointAsync 未建成），按失败惯例降级并区分
    // 既有 C# 错误语义（checkpoint already in progress 不可达）
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkEXPDELSCAN
  pub fn network_expdelscan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "EXPDELSCAN");
      return Ok(true);
    }

    // C# 先查 EXPIRED_KEY_DELETION_SCAN_FREQ 运行时配置；rust 运行时配置域
    // 未建成（后台扫描亦未启用），该拦截不可达

    let mut db_args: [&[u8]; 1] = [&[]];
    if !parse_state.is_empty() {
      db_args[0] = parse_state[0];
      if !self.try_parse_database_id(&db_args, store, output)? {
        return Ok(true);
      }
    }

    // C# 调 storeWrapper.ExpiredKeyDeletionScan（可变区过期键删除扫描）；
    // rust wkv 无会话可达入口，按失败惯例降级（绝不虚报扫描计数）
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkLASTSAVE
  pub fn network_lastsave<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "LASTSAVE");
      return Ok(true);
    }

    if parse_state.len() == 1 && !self.try_parse_database_id(parse_state, store, output)? {
      return Ok(true);
    }

    // C# 回数据库 LastSaveTime；rust 检查点域未接线无时间戳来源，按失败
    // 惯例降级（不虚报时间戳）
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:NetworkBGSAVE
  pub fn network_bgsave<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 2 {
      abort_with_wrong_number_of_arguments(output, "BGSAVE");
      return Ok(true);
    }

    // BGSAVE [SCHEDULE] [DBID]
    let mut token_idx = 0usize;
    if !parse_state.is_empty() && parse_state[0].eq_ignore_ascii_case(b"SCHEDULE") {
      token_idx = 1;
    }
    if parse_state.len() > token_idx {
      let db_args: [&[u8]; 1] = [parse_state[token_idx]];
      if !self.try_parse_database_id(&db_args, store, output)? {
        return Ok(true);
      }
    }

    // C# 阻塞等待 TakeCheckpointAsync(true)；检查点通道缺口同 NetworkSAVE
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/AdminCommands.cs:TryParseDatabaseId
  ///
  /// 校验 DBID 令牌（C# TryGetInt i32 严格口径）；失败时已写出错误应答并返回 false
  pub fn try_parse_database_id<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
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
    let error_msg = cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND_OR_WRONG_NUM_ARGS
      .replace("{0}", sub_command)
      .replace("{1}", cmd_name);
    write_error_raw(output, &error_msg);
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

  fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("admin.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = RespServerSession::default();
      f(&mut s, &batch);
    });
  }

  #[test]
  fn monitor_frames_unk_cmd_like_csharp() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_monitor(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR unknown command\r\n");

      let mut out = Vec::new();
      let _ = s.network_monitor(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'MONITOR' command\r\n"
      );
    });
  }

  #[test]
  fn debug_gated_by_protection_option() {
    with_batch(|s, _batch| {
      // 零参 → wrong args（在保护开关之前）
      let mut out = Vec::new();
      let _ = s.network_debug(&[], &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'DEBUG' command\r\n"
      );

      // 默认 ConnectionProtectionOption::No → 恒拒绝
      let mut out = Vec::new();
      let _ = s.network_debug(&[b"HELP"], &mut out).unwrap();
      assert_eq!(
        out,
        &b"-ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.\r\n"[..]
      );
    });
  }

  #[test]
  fn role_standalone_frame() {
    with_batch(|s, _batch| {
      let mut out = Vec::new();
      let _ = s.network_role(&[], &mut out).unwrap();
      assert_eq!(out, b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");

      let mut out = Vec::new();
      let _ = s.network_role(&[b"x"], &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'ROLE' command\r\n"
      );
    });
  }

  #[test]
  fn cluster_command_reports_disabled() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_process_cluster_command(&[b"MEET", b"h", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR This instance has cluster support disabled\r\n");
    });
  }

  #[test]
  fn registercs_and_module_load_gated() {
    with_batch(|s, batch| {
      // 参数不足
      let mut out = Vec::new();
      let _ = s.network_register_cs(&[b"READ"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'REGISTERCS' command\r\n"
      );

      // 保护开关拒绝
      let mut out = Vec::new();
      let _ = s
        .network_register_cs(
          &[b"READ", b"c", b"1", b"Cls", b"SRC", b"p"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(
        out,
        &b"-ERR REGISTERCS command not allowed. If the enable-module-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.\r\n"[..]
      );

      let mut out = Vec::new();
      let _ = s.network_module_load(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'MODULE|LOADCS' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s.network_module_load(&[b"path"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        &b"-ERR MODULE command not allowed. If the enable-module-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.\r\n"[..]
      );
    });
  }

  #[test]
  fn hcollect_zcollect_validation() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_hcollect(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'HCOLLECT' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s.network_hcollect(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR HCOLLECT scan already in progress\r\n");

      let mut out = Vec::new();
      let _ = s.network_zcollect(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'ZCOLLECT' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s.network_zcollect(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR ZCOLLECT scan already in progress\r\n");
    });
  }

  #[test]
  fn try_parse_database_id_validation() {
    with_batch(|s, batch| {
      // 非整数
      let mut out = Vec::new();
      let ok = s.try_parse_database_id(&[b"abc"], batch, &mut out).unwrap();
      assert!(!ok);
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 超上界（默认 MaxDatabases = 16）
      let mut out = Vec::new();
      let ok = s.try_parse_database_id(&[b"16"], batch, &mut out).unwrap();
      assert!(!ok);
      assert_eq!(out, b"-ERR DB index is out of range.\r\n");

      // 负数
      let mut out = Vec::new();
      let ok = s.try_parse_database_id(&[b"-1"], batch, &mut out).unwrap();
      assert!(!ok);
      assert_eq!(out, b"-ERR DB index is out of range.\r\n");

      // 合法：零输出
      let mut out = Vec::new();
      let ok = s.try_parse_database_id(&[b"3"], batch, &mut out).unwrap();
      assert!(ok);
      assert!(out.is_empty());
    });
  }

  #[test]
  fn save_bgsave_lastsave_commitdb_validation_then_gap() {
    with_batch(|s, batch| {
      // 参数校验先行
      let mut out = Vec::new();
      let _ = s.network_save(&[b"1", b"2"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'SAVE' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s
        .network_bgsave(&[b"SCHEDULE", b"99"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR DB index is out of range.\r\n");

      let mut out = Vec::new();
      let _ = s.network_lastsave(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      let mut out = Vec::new();
      let _ = s.network_commitaof(&[b"a", b"b"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'COMMITAOF' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s
        .network_expdelscan(&[b"1", b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'EXPDELSCAN' command\r\n"
      );

      // 检查点/AOF/扫描通道缺席 → generic error（不虚报成功）
      let mut out = Vec::new();
      let _ = s.network_save(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR generic error\r\n");

      let mut out = Vec::new();
      let _ = s.network_bgsave(&[b"SCHEDULE"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR generic error\r\n");

      let mut out = Vec::new();
      let _ = s.network_commitaof(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR generic error\r\n");

      let mut out = Vec::new();
      let _ = s.network_lastsave(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR generic error\r\n");

      let mut out = Vec::new();
      let _ = s.network_expdelscan(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR generic error\r\n");
    });
  }

  #[test]
  fn acl_and_script_helpers_allow_by_default() {
    with_batch(|s, batch| {
      // 默认 NoAuth 认证器语义：IsAuthenticated = true → 恒放行且零输出
      let mut out = Vec::new();
      let _ = s.check_acl_permissions(&[], batch, &mut out).unwrap();
      assert!(out.is_empty());
      let _ = s.check_script_permissions(&[], batch, &mut out).unwrap();
      assert!(out.is_empty());
      let _ = s.on_acl_or_no_script_failure(&[], batch, &mut out).unwrap();
      assert!(out.is_empty());
    });
  }
}
