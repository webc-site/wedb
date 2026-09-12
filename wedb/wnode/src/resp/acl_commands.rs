//! RESP ACL 命令（对标 libs/server/Resp/ACLCommands.cs）
//!
//! 会话域尚未在 [`RespServerSession`] 挂载认证器与服务器选项，各处理
//! 函数经 [`AclCtx`] 显式注入 ACL 认证器 / 认证设置 / 自定义命令注册
//! 查询面（对标 C# 会话内 `_authenticator` + `storeWrapper` 的同一取数）。

use std::{fmt::Display, sync::Arc};

use wacl::{
  AccessControlList, AclError, AclParser, GarnetAclAuthenticator, User, UserHandle,
  auth::settings::acl_authentication_settings::AclAuthenticationSettings,
};
use wresp::{
  RespSliceExt, RespVecExt, cmd_strings as cs,
  cmd_strings::{
    abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw,
    write_map_len_resp2, write_raw,
  },
};

use super::resp_server_session::RespServerSession;

/// ACL 异常文案（C# "ERR {exception}" 模板；format! 需字面量，收敛为本函数）。
fn acl_exception_message(exception: impl Display) -> String {
  format!("ERR {exception}")
}

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ACL_AUTH_DISABLED
const RESP_ERR_ACL_AUTH_DISABLED: &str = "ERR ACL Authenticator is disabled.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ACL_AUTH_FILE_DISABLED
const RESP_ERR_ACL_AUTH_FILE_DISABLED: &str = "ERR This Garnet instance is not configured to use an ACL file. Please restart server with --acl-file option.";
/// GENPASS 位数参数的合法范围文案（对标 ACLCommands.cs 内联 u8 常量）
const RESP_ERR_ACL_GENPASS_BITS_RANGE: &str = "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096";

/// GENPASS 默认口令长度（64 个十六进制字符）
const GENPASS_DEFAULT_LENGTH: usize = 64;
/// GENPASS 位数上界
const GENPASS_MAX_BITS: i64 = 4096;

/// ACL 命令执行上下文（会话域接线前的显式挂点）
pub struct AclCtx<'a> {
  /// ACL 认证器（None 或非 ACL 档 → 命令拒绝路径）
  pub authenticator: Option<&'a GarnetAclAuthenticator>,
  /// ACL 认证设置（ACL LOAD / SAVE 需要配置文件位置）
  pub acl_settings: Option<&'a AclAuthenticationSettings>,
  /// 自定义命令注册查询（None 对标 ccm == null 跳过校验）
  pub is_custom_command_registered: Option<fn(&str) -> bool>,
}

impl RespServerSession {
  /// 校验会话认证器为 ACL 档（失败已写出错误应答）
  ///
  /// libs/server/Resp/ACLCommands.cs:ValidateACLAuthenticator
  fn validate_acl_authenticator(ctx: &AclCtx, output: &mut Vec<u8>) -> bool {
    if ctx.authenticator.is_none() {
      write_error_raw(output, RESP_ERR_ACL_AUTH_DISABLED);
      return false;
    }
    true
  }

  /// 校验认证设置携带 ACL 配置文件（失败已写出错误应答）
  ///
  /// libs/server/Resp/ACLCommands.cs:ValidateACLFileUse
  fn validate_acl_file_use(ctx: &AclCtx, output: &mut Vec<u8>) -> bool {
    let Some(settings) = ctx.acl_settings else {
      write_error_raw(output, RESP_ERR_ACL_AUTH_DISABLED);
      return false;
    };
    if settings.acl_configuration_file.is_none() {
      write_error_raw(output, RESP_ERR_ACL_AUTH_FILE_DISABLED);
      return false;
    }
    true
  }

  /// 处理 ACL LIST 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclList
  pub fn network_acl_list(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|list");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let user_handles = ctx
      .authenticator
      .map(|a| a.get_access_control_list().get_user_handles())
      .unwrap_or_default();
    output.write_resp_array_len(user_handles.len());
    for (_, user_handle) in user_handles {
      let described = user_handle.user().describe_user();
      output.write_resp_bulk_string(described.as_bytes());
    }
    Ok(true)
  }

  /// 处理 ACL USERS 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclUsers
  pub fn network_acl_users(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|users");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let user_handles = ctx
      .authenticator
      .map(|a| a.get_access_control_list().get_user_handles())
      .unwrap_or_default();
    output.write_resp_array_len(user_handles.len());
    for (username, _) in user_handles {
      output.write_resp_bulk_string(username.as_bytes());
    }
    Ok(true)
  }

  /// 处理 ACL CAT 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclCat
  pub fn network_acl_cat(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数（对标 C# AbortWithErrorMessage 字面量）
    if !parse_state.is_empty() {
      abort_with_error_message(
        output,
        "ERR Unknown subcommand or wrong number of arguments for ACL CAT.",
      );
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let categories = AclParser::list_categories();
    output.write_resp_array_len(categories.len());
    for category in categories {
      output.write_resp_bulk_string(category.as_bytes());
    }
    Ok(true)
  }

  /// 处理 ACL SETUSER 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclSetUser
  pub fn network_acl_set_user(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|setuser");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let Some(acl) = ctx.authenticator.map(|a| a.get_access_control_list()) else {
      return Ok(true);
    };
    // 必选：用户名
    let username = parse_state[0].as_str_safe();

    let result = Self::apply_set_user(acl, username, &parse_state[1..], ctx);
    match result {
      Ok(()) => {
        write_raw(output, cs::RESP_OK);
      }
      Err(exception) => {
        // 终止命令执行
        write_error_raw(output, &acl_exception_message(exception));
      }
    }
    Ok(true)
  }

  /// SETUSER 主体：取 / 建用户句柄 → 复制改写 → 校验新增自定义命令 → CAS 换新
  fn apply_set_user(
    acl: &Arc<AccessControlList>,
    username: &str,
    ops: &[&[u8]],
    ctx: &AclCtx<'_>,
  ) -> Result<(), AclError> {
    // 修改或创建同名用户
    let mut user_handle = acl.get_user_handle(username);
    if user_handle.is_none() {
      let handle = Arc::new(UserHandle::new(Arc::new(User::new(username.to_string()))));
      match acl.add_user_handle(Arc::clone(&handle)) {
        Ok(()) => user_handle = Some(handle),
        // 添加失败：取并发创建的同名用户
        Err(AclError::UserAlreadyExists(_)) => user_handle = acl.get_user_handle(username),
        Err(e) => return Err(e),
      }
    }
    let user_handle =
      user_handle.ok_or_else(|| AclError::Acl("user handle missing after add".into()))?;

    loop {
      // 对用户权限的修改必须针对生效用户
      let current_user = user_handle.user();
      let new_user = User::from_user(&current_user);

      // 记录操作前自定义命令集，只校验"新增"名（ACL 文件先于模块加载载入的
      // 既有名不重复校验，避免无关规则误报）
      let pre_allowed = current_user.custom_commands_allowed();
      let pre_denied = current_user.custom_commands_denied();

      // 其余参数全部为 ACL 操作
      for op in ops {
        AclParser::apply_acl_op_to_user(&new_user, op.as_str_safe())?;
      }

      // SETUSER 时模块已加载：新增的按名自定义权限必须能在
      // CustomCommandManager 解析，未知名大概率是笔误——失败关闭
      if let Some(is_registered) = ctx.is_custom_command_registered {
        for name in new_user.custom_commands_allowed() {
          // 允许 / 拒绝集之间切换的松载入名不误报
          if !pre_allowed.contains(&name) && !pre_denied.contains(&name) && !is_registered(&name) {
            return Err(AclError::Acl(format!(
              "Unknown custom command '{name}' (not registered with any loaded module)"
            )));
          }
        }
        for name in new_user.custom_commands_denied() {
          if !pre_allowed.contains(&name) && !pre_denied.contains(&name) && !is_registered(&name) {
            return Err(AclError::Acl(format!(
              "Unknown custom command '{name}' (not registered with any loaded module)"
            )));
          }
        }
      }

      if user_handle.try_set_user(Arc::new(new_user), &current_user) {
        break;
      }
    }
    Ok(())
  }

  /// 处理 ACL DELUSER 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclDelUser
  pub fn network_acl_del_user(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|deluser");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let Some(acl) = ctx.authenticator.map(|a| a.get_access_control_list()) else {
      return Ok(true);
    };

    // 逐个删除给定用户名的用户
    let mut successful_deletes: i64 = 0;
    let mut error: Option<AclError> = None;
    for username in parse_state {
      match acl.delete_user_handle(username.as_str_safe()) {
        Ok(true) => successful_deletes += 1,
        Ok(false) => {}
        Err(e) => {
          error = Some(e);
          break;
        }
      }
    }
    if let Some(exception) = error {
      log::debug!("ACLException: {exception}");
      // 终止命令执行
      write_error_raw(output, &acl_exception_message(exception));
      return Ok(true);
    }

    // 返回成功删除数
    output.write_resp_int(successful_deletes);
    Ok(true)
  }

  /// 处理 ACL WHOAMI 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclWhoAmI
  pub fn network_acl_who_am_i(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|whoami");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 返回当前已认证用户的用户名
    let name = ctx
      .authenticator
      .and_then(|a| a.get_user_handle())
      .map(|handle| handle.user().name.clone())
      .unwrap_or_default();
    output.write_resp_bulk_string(name.as_bytes());
    Ok(true)
  }

  /// 处理 ACL LOAD 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclLoad
  pub fn network_acl_load(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|load");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) || !Self::validate_acl_file_use(ctx, output) {
      return Ok(true);
    }

    let Some(settings) = ctx.acl_settings else {
      return Ok(true);
    };
    let Some(acl) = ctx.authenticator.map(|a| a.get_access_control_list()) else {
      return Ok(true);
    };

    // 重载配置的 ACL 配置文件
    let file = settings.acl_configuration_file.clone().unwrap_or_default();
    log::info!("Reading updated ACL configuration file '{file}'");
    match acl.load(&settings.default_password, &file) {
      Ok(()) => {
        write_raw(output, cs::RESP_OK);
      }
      Err(exception) => {
        write_error_raw(output, &acl_exception_message(exception));
      }
    }
    Ok(true)
  }

  /// 处理 ACL SAVE 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclSave
  pub fn network_acl_save(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "acl|save");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) || !Self::validate_acl_file_use(ctx, output) {
      return Ok(true);
    }

    let Some(settings) = ctx.acl_settings else {
      return Ok(true);
    };
    let Some(acl) = ctx.authenticator.map(|a| a.get_access_control_list()) else {
      return Ok(true);
    };

    let file = settings.acl_configuration_file.clone().unwrap_or_default();
    match acl.save(&file) {
      Ok(()) => {
        log::info!("ACL configuration file '{file}' saved!");
        write_raw(output, cs::RESP_OK);
      }
      Err(exception) => {
        log::error!("ACL SAVE faulted: {exception}");
        write_error_raw(output, &acl_exception_message(exception));
      }
    }
    Ok(true)
  }

  /// 处理 ACL GENPASS 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclGenPass
  pub fn network_acl_gen_pass(
    &mut self,
    _ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "acl|genpass");
      return Ok(true);
    }

    // 默认长度
    let mut length = GENPASS_DEFAULT_LENGTH;
    if let Some(&bits_arg) = parse_state.first() {
      let Some(bits) = bits_arg.try_parse_i64() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };

      if bits <= 0 || bits > GENPASS_MAX_BITS {
        abort_with_error_message(output, RESP_ERR_ACL_GENPASS_BITS_RANGE);
        return Ok(true);
      }

      // 位数向上取整到 4 的倍数再折算字节数得口令长度
      length = (bits / 4 + (bits % 4 != 0) as i64) as usize;
    }

    // 随机小写十六进制口令（C# RandomNumberGenerator.GetHexString 的
    // 项目内承接——fastrand 已是本仓随机数选型）
    let mut password = Vec::with_capacity(length);
    for _ in 0..length {
      let nibble = fastrand::u8(0..16);
      password.push(if nibble < 10 {
        b'0' + nibble
      } else {
        b'a' + nibble - 10
      });
    }
    output.write_resp_bulk_string(&password);
    Ok(true)
  }

  /// 处理 ACL GETUSER 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclGetUser
  pub fn network_acl_get_user(
    &mut self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须提供用户名
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "acl|getuser");
      return Ok(true);
    }
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let user = ctx
      .authenticator
      .map(|a| a.get_access_control_list())
      .and_then(|acl| acl.get_user_handle(parse_state[0].as_str_safe()))
      .map(|handle| User::from_user(&handle.user()));

    match user {
      None => {
        output.write_resp_null();
      }
      Some(user) => {
        write_map_len_resp2(output, 3);

        output.write_resp_bulk_string(b"flags");
        // RESP2 集合退化为数组
        output.write_resp_array_len(1);
        output.write_resp_bulk_string(if user.is_enabled() { b"on" } else { b"off" });

        output.write_resp_bulk_string(b"passwords");
        let passwords = user.copy_password_hashes();
        output.write_resp_array_len(passwords.len());
        for password in passwords {
          let hash = format!("#{password}");
          output.write_resp_bulk_string(hash.as_bytes());
        }

        output.write_resp_bulk_string(b"commands");
        let commands = user.get_enabled_commands_description();
        output.write_resp_bulk_string(commands.as_bytes());
      }
    }
    Ok(true)
  }
}
