//! RESP ACL 命令（对标 libs/server/Resp/ACLCommands.cs）
//!
//! ACL 用户规则以底层存储为唯一真实数据源（无全局用户字典，ACL 无内存用户
//! 表，LIST/USERS 扫存储、GETUSER/AUTH 点查存储）：
//! `SETUSER` 读改写落盘、`DELUSER` 墓碑删除、`GETUSER` 点查反序列化、
//! `LIST`/`USERS` 单遍扫描当前命名空间的 `KeyTag::Acl` 记录并就地收成小快照
//! （扫描可见即本轮快照，数组头与元素数同源、无从背离；ACL 用户量级小，快照仅
//! 是整份应答的提前驻留——会话 output 本就全量缓冲整框）。会话域经
//! [`AclCtx`] 显式注入 ACL 认证器 / 自定义命令注册查询面，
//! 存储访问经 [`AclStore`] 显式注入（对标 C# 会话内 `_authenticator` +
//! `storeWrapper` 的同一取数）。

use std::{fmt::Display, mem, str::from_utf8, sync::Arc};

use wacl::{
  AclError, AclParser, AclPassword, GarnetAclAuthenticator, User, UserHandle,
  access_control_list::DEFAULT_USER_NAME, user::parse_user_namespace_with_default,
};
use wdev::Device;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, abort_with_error_message, write_error_raw, write_raw},
  command::RespCommand,
  ext::{RespSliceExt, RespVecExt},
};

use super::{acl_store::AclStore, custom_objects, resp_server_session::RespServerSession};

/// ACL 异常文案（C# "ERR {exception}" 模板；format! 需字面量，收敛为本函数）。
fn acl_exception_message(exception: impl Display) -> String {
  format!("ERR {exception}")
}

/// 跨命名空间管理权限拒绝文案
const RESP_ERR_ACL_FOREIGN_NAMESPACE: &str =
  "ERR permission denied: only namespace 0 can manage foreign namespaces";
/// GENPASS 位数参数的合法范围文案（对标 ACLCommands.cs 内联 u8 常量）
const RESP_ERR_ACL_GENPASS_BITS_RANGE: &str = "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096";

/// GENPASS 默认口令长度（64 个十六进制字符）
const GENPASS_DEFAULT_LENGTH: usize = 64;
/// GENPASS 位数上界
const GENPASS_MAX_BITS: i64 = 4096;

/// ACL 命令执行上下文（会话域接线前的显式挂点）
pub struct AclCtx<'a> {
  /// ACL 认证器（None = 免认证形态 → 命令拒绝路径）
  pub authenticator: Option<&'a GarnetAclAuthenticator>,
  /// 自定义命令注册查询（None 对标 ccm == null 跳过校验）
  pub is_custom_command_registered: Option<fn(&str) -> bool>,
  /// 调用者会话所属的命名空间
  pub caller_namespace: u64,
}

impl RespServerSession {
  /// 校验会话认证器为 ACL 档（失败已写出错误应答）
  ///
  /// libs/server/Resp/ACLCommands.cs:ValidateACLAuthenticator
  fn validate_acl_authenticator(ctx: &AclCtx, output: &mut Vec<u8>) -> bool {
    if ctx.authenticator.is_none() {
      write_error_raw(output, cs::RESP_ERR_ACL_AUTH_DISABLED);
      return false;
    }
    true
  }

  /// 校验 ACL 文件配置面（失败已写出错误应答）
  ///
  /// libs/server/Resp/ACLCommands.cs:ValidateACLFileUse
  ///
  /// C# 两级判据：`AuthSettings is not AclAuthenticationSettings` 回
  /// AUTH_DISABLED（由 [`Self::validate_acl_authenticator`] 承接）、
  /// `AclConfigurationFile == null` 回 FILE_DISABLED。本仓按「彻底废弃文件
  /// 配置、ACL 以 `KeyTag::Acl` 落存储」装配，无 acl-file 配置项，第二级恒不
  /// 成立，故 LOAD/SAVE 一律回「不适用」错误帧，绝不为空动作回 +OK。
  fn validate_acl_file_use(output: &mut Vec<u8>) -> bool {
    write_error_raw(output, cs::RESP_ERR_ACL_AUTH_FILE_DISABLED);
    false
  }

  /// 引导态 default 用户句柄（内存单例，仅 ns 0 装配；非用户大字典）
  ///
  /// 存储尚无同名记录时作为列表/点查的兜底来源，对齐 C#
  /// `AccessControlList.GetDefaultUserHandle` 语义；非 0 命名空间的 default
  /// 由 ACL SETUSER 落存储记录承接，不走内存兜底
  fn in_memory_default_user(ctx: &AclCtx, ns: u64) -> Option<Arc<UserHandle>> {
    if ns != 0 {
      return None;
    }
    ctx
      .authenticator
      .map(|a| a.get_access_control_list().get_default_user_handle())
  }

  /// 处理 ACL LIST 子命令（单遍流式扫描当前命名空间的 Acl 记录成快照后整框直出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclList —— C# 侧 `GetUserHandles()`
  /// 取一次句柄表再写 Count、遍历同一句柄表（:66-73），rust 以「单遍扫描收集快
  /// 照 → 写数组头 → 由同一快照写正文」承接同一形态：数组头与元素数同源强一致
  pub fn network_acl_list<D: Device>(
    &self,
    ctx: &AclCtx,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    check_arg_count!(parse_state, ..=0, output, "acl|list");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 单遍扫描：解码即渲染，正文就地收成快照（顺带探 default 兜底位）。数组长度
    // 必须先于正文写出，应答一旦起头就回退不了，故不可解码的记录在本遍失败关闭。
    // 存储值为 bitcode 二进制编码，逐条 decode 后即弃，快照只驻留已渲染正文串
    let mut described: Vec<String> = Vec::new();
    let mut has_default = false;
    let mut decode_failed = false;
    if store
      .for_each_user_blocking(ctx.caller_namespace, |name, rule| {
        if name == DEFAULT_USER_NAME.as_bytes() {
          has_default = true;
        }
        match User::from_bytes(rule) {
          Ok(user) => described.push(user.describe_user()),
          Err(e) => {
            log::debug!("ACL record decode failure: {e}");
            decode_failed = true;
            return false;
          }
        }
        true
      })
      .is_err()
      || decode_failed
    {
      write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      return Ok(true);
    }
    // 引导态 default 用户驻留内存（requirepass / nopass 单例，非用户大字典）：
    // 存储尚无同名记录时补入列表首位，对齐 C# GetUserHandles 含 default 的语义
    let in_memory_default = if has_default {
      None
    } else {
      Self::in_memory_default_user(ctx, ctx.caller_namespace)
    };
    output.write_resp_array_len(described.len() + usize::from(in_memory_default.is_some()));
    if let Some(handle) = &in_memory_default {
      output.write_resp_bulk_string(handle.user().describe_user().as_bytes());
    }

    // 正文取自上面的同一快照：条数与符头恒等，扫描之后不再触存储，并发的
    // SETUSER/DELUSER 也无从使二者背离（快照前可见即在内、后可见亦不补出）
    for line in &described {
      output.write_resp_bulk_string(line.as_bytes());
    }
    Ok(true)
  }

  /// 处理 ACL USERS 子命令（单遍流式扫描当前命名空间的用户名成快照后整框直出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclUsers —— C# 侧同样取一次
  /// `GetUserHandles()` 写 Count、遍历同一句柄表（:95-101），口径与
  /// [`Self::network_acl_list`] 一致
  pub fn network_acl_users<D: Device>(
    &self,
    ctx: &AclCtx,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "acl|users");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 单遍扫描：用户名收成快照（顺带探 default 兜底位），用户名取自存储键、
    // 不触碰规则正文，故本遍只驻留名字
    let mut names: Vec<Vec<u8>> = Vec::new();
    let mut has_default = false;
    if store
      .for_each_user_blocking(ctx.caller_namespace, |name, _rule| {
        if name == DEFAULT_USER_NAME.as_bytes() {
          has_default = true;
        }
        names.push(name.to_vec());
        true
      })
      .is_err()
    {
      write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      return Ok(true);
    }
    // 引导态 default 用户（内存单例）兜底入列首位，对齐 C# 语义
    let in_memory_default = if has_default {
      None
    } else {
      Self::in_memory_default_user(ctx, ctx.caller_namespace)
    };
    output.write_resp_array_len(names.len() + usize::from(in_memory_default.is_some()));
    if let Some(handle) = &in_memory_default {
      output.write_resp_bulk_string(handle.user().name.as_bytes());
    }
    // 正文取自同一快照，数组头与元素数恒等（口径同 network_acl_list）
    for name in &names {
      output.write_resp_bulk_string(name);
    }
    Ok(true)
  }

  /// 处理 ACL CAT 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclCat
  pub fn network_acl_cat(
    &self,
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

  /// 处理 ACL SETUSER 子命令（读存储既有规则 → 施加操作 → 整体回写存储）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclSetUser
  pub fn network_acl_set_user<D: Device>(
    &self,
    ctx: &AclCtx,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    check_arg_count!(parse_state, 1.., output, "acl|setuser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 必选：用户名，支持 <ns>#用户名
    let raw_name = parse_state[0].as_str_safe();
    let (clean_user, target_ns) =
      match parse_user_namespace_with_default(raw_name, ctx.caller_namespace) {
        Ok(res) => res,
        Err(e) => {
          write_error_raw(output, &acl_exception_message(e));
          return Ok(true);
        }
      };
    if ctx.caller_namespace != 0 && (target_ns != ctx.caller_namespace || raw_name.contains('#')) {
      write_error_raw(output, RESP_ERR_ACL_FOREIGN_NAMESPACE);
      return Ok(true);
    }

    match Self::apply_set_user(store, clean_user, target_ns, &parse_state[1..], ctx) {
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

  /// SETUSER 主体：点查存储既有规则 → 复制改写 → 校验新增自定义命令 → 回写存储
  fn apply_set_user<D: Device>(
    store: &AclStore<'_, D>,
    clean_username: &str,
    target_ns: u64,
    ops: &[&[u8]],
    ctx: &AclCtx<'_>,
  ) -> Result<(), AclError> {
    let existing = store
      .read(target_ns, clean_username.as_bytes())
      .map_err(|e| AclError::Acl(e.to_string()))?;

    // 存储无记录即新建用户；有记录即以其为当前生效版本（存储为唯一真源）。
    // 引导态 default 用户驻留内存单例（requirepass / nopass 配置装配），存储
    // 尚无同名记录时以内存单例为改写基线，避免 SETUSER 丢失引导权限
    //（对标 C# 启动期 default 句柄已在 ACL 表内、SETUSER 就地改写）
    let current_user = match &existing {
      Some(bytes) => User::from_rule_bytes(clean_username, bytes)?,
      None => match Self::in_memory_default_user(ctx, target_ns)
        .filter(|handle| handle.user().name == clean_username)
      {
        Some(handle) => Arc::new(User::from_user(handle.user())),
        None => Arc::new(User::new(clean_username.to_string())),
      },
    };

    // 改权在独占可变的副本上逐条落定，全部应用完才整体写穿存储（连接本地
    // 值语义，无 C# 共享句柄的就地 CAS）
    let mut new_user = User::from_user(&current_user);

    // 记录操作前自定义命令集，只校验"新增"名（存储既有名不重复校验，
    // 避免无关规则误报）
    let pre_allowed = current_user.custom_commands_allowed();
    let pre_denied = current_user.custom_commands_denied();

    // 其余参数全部为 ACL 操作
    for op in ops {
      AclParser::apply_acl_op_to_user(&mut new_user, op.as_str_safe())?;
    }

    // SETUSER 时模块已加载：新增的按名自定义权限必须能在
    // CustomCommandManager 解析，未知名大概率是笔误——失败关闭
    if let Some(is_registered) = ctx.is_custom_command_registered {
      for name in new_user.custom_commands_allowed() {
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

    store
      .write(target_ns, clean_username.as_bytes(), &new_user.to_bytes())
      .map_err(|e| AclError::Acl(e.to_string()))
  }

  /// 处理 ACL DELUSER 子命令（向底层存储写入墓碑删除）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclDelUser
  pub fn network_acl_del_user<D: Device>(
    &self,
    ctx: &AclCtx,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    check_arg_count!(parse_state, 1.., output, "acl|deluser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 逐个删除给定用户名的用户
    let mut successful_deletes: i64 = 0;
    let mut error: Option<AclError> = None;
    for username in parse_state {
      let raw_name = username.as_str_safe();
      let (clean_user, target_ns) =
        match parse_user_namespace_with_default(raw_name, ctx.caller_namespace) {
          Ok(res) => res,
          Err(e) => {
            error = Some(e);
            break;
          }
        };
      if ctx.caller_namespace != 0 && (target_ns != ctx.caller_namespace || raw_name.contains('#'))
      {
        write_error_raw(output, RESP_ERR_ACL_FOREIGN_NAMESPACE);
        return Ok(true);
      }
      // default 为特殊用户，不可删除（对标 C# DeleteUserHandle）
      if clean_user == DEFAULT_USER_NAME {
        error = Some(AclError::Acl(
          "The special 'default' user cannot be removed from the system".into(),
        ));
        break;
      }
      match store.delete(target_ns, clean_user.as_bytes()) {
        Ok(true) => successful_deletes += 1,
        Ok(false) => {}
        Err(e) => {
          error = Some(AclError::Acl(e.to_string()));
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
    &self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    check_arg_count!(parse_state, ..=0, output, "acl|whoami");
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

  /// 处理 ACL LOAD 子命令（ACL 已落存储，无外部文件可重载 → 不适用）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclLoad
  pub fn network_acl_load(
    &self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "acl|load");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }
    if !Self::validate_acl_file_use(output) {
      return Ok(true);
    }
    Ok(true)
  }

  /// 处理 ACL SAVE 子命令（SETUSER/DELUSER 已同步写穿，无落盘动作可做 → 不适用）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclSave
  pub fn network_acl_save(
    &self,
    ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "acl|save");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }
    if !Self::validate_acl_file_use(output) {
      return Ok(true);
    }
    Ok(true)
  }

  /// 处理 ACL GENPASS 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclGenPass
  pub fn network_acl_gen_pass(
    &self,
    _ctx: &AclCtx,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=1, output, "acl|genpass");

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

  /// 处理 ACL GETUSER 子命令（点查底层存储并反序列化输出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclGetUser
  pub fn network_acl_get_user<D: Device>(
    &self,
    ctx: &AclCtx,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须提供用户名
    check_arg_count!(parse_state, 1, output, "acl|getuser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let raw_name = parse_state[0].as_str_safe();
    let (clean_user, target_ns) =
      match parse_user_namespace_with_default(raw_name, ctx.caller_namespace) {
        Ok(res) => res,
        Err(e) => {
          write_error_raw(output, &acl_exception_message(e));
          return Ok(true);
        }
      };
    if ctx.caller_namespace != 0 && (target_ns != ctx.caller_namespace || raw_name.contains('#')) {
      write_error_raw(output, RESP_ERR_ACL_FOREIGN_NAMESPACE);
      return Ok(true);
    }

    let record = match store.read(target_ns, clean_user.as_bytes()) {
      Ok(record) => record,
      Err(e) => {
        write_error_raw(output, &acl_exception_message(AclError::Acl(e.to_string())));
        return Ok(true);
      }
    };
    let user = match record {
      // 存储无记录：引导态 default 用户回落内存单例（非用户大字典）
      None => Self::in_memory_default_user(ctx, target_ns)
        .filter(|handle| handle.user().name == clean_user)
        .map(|handle| Arc::new(User::from_user(handle.user()))),
      Some(bytes) => match User::from_rule_bytes(clean_user, &bytes) {
        Ok(user) => Some(user),
        Err(e) => {
          write_error_raw(output, &acl_exception_message(e));
          return Ok(true);
        }
      },
    };

    match user {
      None => {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      Some(user) => {
        cs::write_map_len(output, 3, self.resp_protocol_version);

        output.write_resp_bulk_string(b"flags");
        // RESP2 集合退化为数组
        cs::write_set_len(output, 1, self.resp_protocol_version);
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

/// ACL 命令存储点查认证结果
pub enum AclAuthOutcome {
  /// 认证成功（携带用户句柄与目标命名空间）
  Success(Arc<UserHandle>, u64),
  /// 用户名或口令不匹配
  Denied,
  /// 存储访问失败
  StorageError,
}

impl RespServerSession {
  /// AUTH 存储点查认证（对标 doc/zh/db.md §3.3）
  ///
  /// 按 `(ns, 0, KeyTag::Acl, user)` 点查底层存储并反序列化用户规则；
  /// 校验账号启用与口令哈希。成功方由调用方挂载连接本地
  /// `Arc<UserHandle>`（句柄随连接析构释放，无全局用户字典）。
  pub fn authenticate_user_via_store<D: Device>(
    &self,
    store: &AclStore<'_, D>,
    username: &[u8],
    password: &[u8],
  ) -> AclAuthOutcome {
    // 调用方仅在命名用户（username 非空）时进入
    let uname = String::from_utf8_lossy(username);
    let (clean_user, target_ns) = match parse_user_namespace_with_default(&uname, self.namespace) {
      Ok(res) => res,
      Err(_) => return AclAuthOutcome::Denied,
    };
    let record = match store.read(target_ns, clean_user.as_bytes()) {
      Ok(record) => record,
      Err(_) => return AclAuthOutcome::StorageError,
    };
    // 存储无记录：非本认证面用户，交调用方回落内存认证器（default /
    // requirepass 引导形态）
    let Some(bytes) = record else {
      return AclAuthOutcome::Denied;
    };
    let Ok(user) = User::from_rule_bytes(clean_user, &bytes) else {
      return AclAuthOutcome::Denied;
    };
    if !user.is_enabled() {
      return AclAuthOutcome::Denied;
    }
    let Ok(password) = from_utf8(password) else {
      return AclAuthOutcome::Denied;
    };
    if !user.validate_password(&AclPassword::from_string(password)) {
      return AclAuthOutcome::Denied;
    }
    AclAuthOutcome::Success(Arc::new(UserHandle::new(user)), target_ns)
  }
}

impl RespServerSession {
  /// C# ProcessOtherCommands 的 ACL arm 集（实现为本文件各 network_acl_* 函数）
  ///
  /// ACL LIST/USERS/CAT/SETUSER/DELUSER/WHOAMI/LOAD/SAVE/GENPASS/GETUSER；
  /// `None` 表示命令不属于本族，调用方继续后续分派。
  /// 上下文就地构建（认证器 guard 局部保活，ctx 持其解引用）
  pub fn process_acl_commands<D: Device>(
    &mut self,
    cmd: RespCommand,
    store: &AclStore<'_, D>,
  ) -> Option<bool> {
    if !matches!(
      cmd,
      RespCommand::AclList
        | RespCommand::AclUsers
        | RespCommand::AclCat
        | RespCommand::AclSetuser
        | RespCommand::AclDeluser
        | RespCommand::AclWhoami
        | RespCommand::AclLoad
        | RespCommand::AclSave
        | RespCommand::AclGenpass
        | RespCommand::AclGetuser
    ) {
      return None;
    }
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    let mut output = mem::take(&mut self.output);
    // SETUSER 自改目标预判：目标即会话当前已认证用户时，命令后须重读存储
    // 刷新连接本地句柄（对标 C# 共享 UserHandle 的 CAS 换新即时生效语义；
    // 连接本地持有模型下句柄不共享，故显式刷新）
    let refresh_target: Option<(String, u64)> = if cmd == RespCommand::AclSetuser {
      args.first().and_then(|raw| {
        let raw = raw.as_str_safe();
        let (name, ns) = parse_user_namespace_with_default(raw, self.namespace).ok()?;
        let handle = self.acl_user_handle.as_ref()?;
        (handle.user().name == name && self.namespace == ns).then(|| (name.to_string(), ns))
      })
    } else {
      None
    };
    let auth_guard = self.acl_authenticator.as_ref().map(|acl| acl.lock());
    let ctx = AclCtx {
      authenticator: auth_guard.as_deref(),
      // SETUSER 按名规则须在扩展命令静态清单内可解析，未知名失败关闭
      //（对标 C# ccm 非 null 校验路径；扩展编译期在场恒校验）——判定即
      // 清单的按名解析单点，与解析/慢路径重放同表同语义，无第二处名单
      #[cfg(any(feature = "roaring", feature = "json"))]
      is_custom_command_registered: Some(custom_objects::is_custom_object_command),
      #[cfg(not(any(feature = "roaring", feature = "json")))]
      is_custom_command_registered: None,
      caller_namespace: self.namespace,
    };
    let handled = match cmd {
      RespCommand::AclList => self.network_acl_list(&ctx, store, &args, &mut output),
      RespCommand::AclUsers => self.network_acl_users(&ctx, store, &args, &mut output),
      RespCommand::AclCat => self.network_acl_cat(&ctx, &args, &mut output),
      RespCommand::AclSetuser => self.network_acl_set_user(&ctx, store, &args, &mut output),
      RespCommand::AclDeluser => self.network_acl_del_user(&ctx, store, &args, &mut output),
      RespCommand::AclWhoami => self.network_acl_who_am_i(&ctx, &args, &mut output),
      RespCommand::AclLoad => self.network_acl_load(&ctx, &args, &mut output),
      RespCommand::AclSave => self.network_acl_save(&ctx, &args, &mut output),
      RespCommand::AclGenpass => self.network_acl_gen_pass(&ctx, &args, &mut output),
      RespCommand::AclGetuser => self.network_acl_get_user(&ctx, store, &args, &mut output),
      _ => {
        self.output = output;
        return None;
      }
    };
    self.output = output;
    // ctx 持认证器 guard 解引用（最后一次使用已结束）；释放 guard 方可变更
    // 会话句柄
    drop(auth_guard);
    // 自改即时生效：重读存储换新连接本地句柄（存储无记录即视为删除，回落
    // 未认证态由后续命令门控自然拒绝）
    if let Some((name, ns)) = refresh_target
      && let Ok(Some(bytes)) = store.read(ns, name.as_bytes())
      && let Ok(user) = User::from_rule_bytes(&name, &bytes)
    {
      let handle = Arc::new(UserHandle::new(user));
      if let Some(acl) = &self.acl_authenticator {
        let mut acl = acl.lock();
        acl.user_handle = Some(Arc::clone(&handle));
        acl.namespace = ns;
      }
      self.set_user_handle(handle);
    }
    Some(handled.unwrap_or(true))
  }
}
