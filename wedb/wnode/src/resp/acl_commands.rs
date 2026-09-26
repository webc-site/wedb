//! RESP ACL 命令（对标 libs/server/Resp/ACLCommands.cs）
//!
//! ACL 用户规则以底层存储为唯一真实数据源（无全局用户字典，ACL 无内存用户
//! 表，LIST/USERS 扫存储、GETUSER/AUTH 点查存储）：
//! `SETUSER` 读改写落盘、`DELUSER` 墓碑删除、`GETUSER` 点查反序列化、
//! `LIST`/`USERS` 单遍扫描当前命名空间的 `KeyTag::Acl` 记录并就地收成小快照
//! （扫描可见即本轮快照，数组头与元素数同源、无从背离；ACL 用户量级小，快照仅
//! 是整份应答的提前驻留——会话 output 本就全量缓冲整框）。会话域经
//! [`AclCtx`] 显式注入引导 ACL 配置面 / 自定义命令注册查询面，
//! 已认证用户句柄唯一真源为会话 `acl_user_handle`（WHOAMI 直读，认证器
//! 不驻镜像），存储访问经 [`AclStore`] 显式注入（对标 C# 会话内
//! `_authenticator` + `storeWrapper` 的同一取数）。

use std::{borrow::Cow, fmt::Display, mem, sync::Arc};

use wacl::{
  AccessControlList, AclError, AclParser, AclPassword, User, UserHandle,
  access_control_list::DEFAULT_USER_NAME, ascii_sanitize, user::parse_user_namespace_with_default,
};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_i64_arg},
  cmd_strings::{self as cs, abort_with_error_message, write_error_raw, write_raw},
  command::RespCommand,
  ext::RespVecExt,
};

use super::{acl_store::AclStore, custom_objects, resp_server_session::RespServerSession};

/// ACL 异常文案（C# "ERR {exception}" 模板；format! 需字面量，收敛为本函数）。
fn acl_exception_message(exception: impl Display) -> String {
  format!("ERR {exception}")
}

/// 跨命名空间权限拒绝文案（ACL 管理臂与认证臂共用，全仓唯一）
pub(crate) const RESP_ERR_ACL_FOREIGN_NAMESPACE: &str =
  "ERR permission denied: only namespace 0 can manage foreign namespaces";

/// ACL 存储扫描失败文案（rust 自有存储底座失败口径，C# 内存句柄表无此分支；
/// 对标 NetworkAclList 取 GetUserHandles 直写形态，失败即关框）
pub const RESP_ERR_ACL_STORE_SCAN_FAILED: &str = "ERR ACL store scan failed";

/// 非 ns0 连接禁跨租户/显式 `#` 前缀的统一门禁判据（ACL 管理臂与认证臂
/// 同口径，唯一判据；见 doc/zh/db.md §2.2：显式 `<ns>#` 前缀仅 ns0 可用）
///
/// `caller_ns` 为会话当前绑定命名空间（管理臂取 `AclCtx::caller_namespace`，
/// 认证臂取 `RespServerSession::namespace`），`target_ns` 为
/// [`parse_user_namespace_with_default`] 的解析结果，`raw_name` 为原始用户名。
/// 裸名解析恒落 `caller_ns`，故 `target_ns != caller_ns` 蕴含显式 `#`；
/// `raw_name.contains('#')` 额外拦同租显式前缀（如 ns1 会话写 `1#bob`）
pub(crate) fn foreign_namespace_denied(caller_ns: u64, raw_name: &str, target_ns: u64) -> bool {
  caller_ns != 0 && (target_ns != caller_ns || raw_name.contains('#'))
}
/// GENPASS 位数参数的合法范围文案（对标 ACLCommands.cs 内联 u8 常量）
const RESP_ERR_ACL_GENPASS_BITS_RANGE: &str = "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096";
/// GENPASS OS 熵源失败文案（rust 自有底座失败口径——C# 侧 RandomNumberGenerator
/// 直接抛异常无此分支，失败即关框）
const RESP_ERR_ACL_GENPASS_ENTROPY_FAILED: &str =
  "ERR ACL GENPASS failed to read OS entropy source";

/// GENPASS 默认口令长度（64 个十六进制字符）
const GENPASS_DEFAULT_LENGTH: usize = 64;
/// GENPASS 位数上界
const GENPASS_MAX_BITS: i64 = 4096;

/// ACL 命令执行上下文（会话域接线前的显式挂点）
pub struct AclCtx<'a> {
  /// 引导期访问控制列表（None = 免认证形态 → 命令拒绝路径）；不可变配置面，
  /// 认证器只读共享免锁（已认证用户句柄唯一真源在会话 `acl_user_handle`）
  pub acl: Option<&'a AccessControlList>,
  /// 自定义命令注册查询（None 对标 ccm == null 跳过校验）
  pub is_custom_command_registered: Option<fn(&str) -> bool>,
  /// 调用者会话所属的命名空间
  pub caller_namespace: u64,
}

/// 鉴权预门三态裁决（[`RespServerSession::check_acl_permissions`] 的返回面）
///
/// C# 网络线程内联 BlockingWait，CheckACLPermissions 恒同步收敛；rust 点查
/// 存储须在泵侧 async 域闭环，故挂载陈旧须点查刷新时门不就地裁决而停车
///（[`AclGateVerdict::Parked`]），由泵刷新挂载后回驱原命令重评——同一命令
/// 的最终裁决（放行/拒绝）语义与 C# 逐位对齐，只是裁决时机后移一次重驱
pub enum AclGateVerdict {
  /// 挂载最新（或免认证形态）：命令按位图裁决放行，继续后续门链
  Permitted,
  /// 位图拒绝（NOPERM/NOAUTH 或 NOSCRIPT 前置失败已由调用方写应答）
  Denied,
  /// 挂载陈旧须异步点查刷新：命令未评未计，泵刷新后原命令重驱重评
  Parked,
}

impl RespServerSession {
  /// 校验会话为 ACL 档（失败已写出错误应答）
  ///
  /// libs/server/Resp/ACLCommands.cs:ValidateACLAuthenticator
  fn validate_acl_authenticator(ctx: &AclCtx, output: &mut Vec<u8>) -> bool {
    if ctx.acl.is_none() {
      write_error_raw(output, cs::RESP_ERR_ACL_AUTH_DISABLED);
      return false;
    }
    true
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
    ctx.acl.map(|acl| acl.get_default_user_handle())
  }

  /// ACL 管理臂用户名解析单点（SETUSER/DELUSER/GETUSER 共用）：ASCII 折叠 →
  /// `<ns>#` 解析 → 跨租门禁。Ok=(清理用户名, 目标命名空间)；Err=待直写回帧
  /// 的完整错误文案——解析失败走 `ERR {exception}` 模板，跨租拒绝复用
  /// [`RESP_ERR_ACL_FOREIGN_NAMESPACE`]（与原各臂就地内联形态逐字节同文）
  fn resolve_acl_username(caller_ns: u64, raw: &[u8]) -> Result<(String, u64), Cow<'static, str>> {
    let raw_name = ascii_sanitize(raw);
    let (clean_user, target_ns) = match parse_user_namespace_with_default(&raw_name, caller_ns) {
      Ok(res) => res,
      Err(e) => return Err(Cow::Owned(acl_exception_message(e))),
    };
    if foreign_namespace_denied(caller_ns, &raw_name, target_ns) {
      return Err(Cow::Borrowed(RESP_ERR_ACL_FOREIGN_NAMESPACE));
    }
    Ok((clean_user.to_owned(), target_ns))
  }

  /// ACL LIST/USERS 同形主体：arity/ACL 档门 → 单遍流式扫描当前命名空间的 Acl
  /// 记录就地收成快照（渲染后正文明细 + default 兜底位）→ 数组头与元素数取自
  /// 同一快照整框直出（扫描之后不再触存储，并发的 SETUSER/DELUSER 无从使二者
  /// 背离——快照前可见即在内、后可见亦不补出；ACL 用户量级小，快照仅是整份
  /// 应答的提前驻留——会话 output 本就全量缓冲整框）
  ///
  /// `render_rule` 将单条记录 `(用户名, 规则字节)` 渲染为正文（返回 None =
  /// 记录不可解码，本遍失败关闭；数组长度必须先于正文写出，应答一旦起头就
  /// 回退不了）；`render_default` 渲染存储尚无同名记录时补入首位的引导态
  /// default 元素（内存单例，对齐 C# GetUserHandles 含 default 语义）
  async fn network_acl_snapshot<D: Device, F, G>(
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
    tag: &str,
    mut render_rule: F,
    render_default: G,
  ) -> wresp::Result<bool>
  where
    F: FnMut(&[u8], &[u8]) -> Option<Vec<u8>>,
    G: Fn(&User) -> Vec<u8>,
  {
    // 不允许附加参数
    check_arg_count!(parse_state, ..=0, output, tag);
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    let mut described: Vec<Vec<u8>> = Vec::new();
    let mut has_default = false;
    let mut decode_failed = false;
    if store
      .for_each_user(ctx.caller_namespace, |name, rule| {
        if name == DEFAULT_USER_NAME.as_bytes() {
          has_default = true;
        }
        match render_rule(name, rule) {
          Some(item) => {
            described.push(item);
            true
          }
          None => {
            decode_failed = true;
            false
          }
        }
      })
      .await
      .is_err()
      || decode_failed
    {
      write_error_raw(output, RESP_ERR_ACL_STORE_SCAN_FAILED);
      return Ok(true);
    }
    // 引导态 default 用户驻留内存（requirepass / nopass 单例，非用户大字典）：
    // 存储尚无同名记录时补入列表首位
    let in_memory_default = if has_default {
      None
    } else {
      Self::in_memory_default_user(ctx, ctx.caller_namespace)
    };
    output.write_resp_array_len(described.len() + usize::from(in_memory_default.is_some()));
    if let Some(handle) = &in_memory_default {
      output.write_resp_bulk_string(&render_default(handle.user()));
    }
    for item in &described {
      output.write_resp_bulk_string(item);
    }
    Ok(true)
  }

  /// 处理 ACL LIST 子命令（单遍流式扫描当前命名空间的 Acl 记录成快照后整框直出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclList —— C# 侧 `GetUserHandles()`
  /// 取一次句柄表再写 Count、遍历同一句柄表（:66-73），rust 主体见
  /// [`Self::network_acl_snapshot`]：正文明细为逐条 bitcode 解码后的用户描述串
  pub async fn network_acl_list<D: Device>(
    &self,
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Self::network_acl_snapshot(
      ctx,
      store,
      parse_state,
      output,
      "acl|list",
      |_name, rule| match User::from_bytes(rule) {
        Ok(user) => Some(user.describe_user().into_bytes()),
        Err(e) => {
          log::debug!("ACL record decode failure: {e}");
          None
        }
      },
      |user| user.describe_user().into_bytes(),
    )
    .await
  }

  /// 处理 ACL USERS 子命令（单遍流式扫描当前命名空间的用户名成快照后整框直出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclUsers —— 口径与
  /// [`Self::network_acl_list`] 一致（主体见 [`Self::network_acl_snapshot`]）；
  /// 用户名取自存储键、不触碰规则正文，本遍只驻留名字
  pub async fn network_acl_users<D: Device>(
    &self,
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Self::network_acl_snapshot(
      ctx,
      store,
      parse_state,
      output,
      "acl|users",
      |name, _rule| Some(name.to_vec()),
      |user| user.name.as_bytes().to_vec(),
    )
    .await
  }

  /// 处理 ACL CAT 子命令
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclCat
  pub fn network_acl_cat(
    &self,
    ctx: &AclCtx<'_>,
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
  pub async fn network_acl_set_user<D: Device>(
    &self,
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    check_arg_count!(parse_state, 1.., output, "acl|setuser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 必选：用户名（解析/跨租门禁单点，见 resolve_acl_username；字节口径对标
    // C# parseState.GetString 的 ASCII 折叠，全 ACL 域同一单点 ascii_sanitize）
    let (clean_user, target_ns) =
      match Self::resolve_acl_username(ctx.caller_namespace, parse_state[0]) {
        Ok(res) => res,
        Err(frame) => {
          write_error_raw(output, &frame);
          return Ok(true);
        }
      };

    match Self::apply_set_user(store, &clean_user, target_ns, &parse_state[1..], ctx).await {
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
  async fn apply_set_user<D: Device>(
    store: &AclStore<'_, D>,
    clean_username: &str,
    target_ns: u64,
    ops: &[&[u8]],
    ctx: &AclCtx<'_>,
  ) -> Result<(), AclError> {
    let existing = store
      .read(target_ns, clean_username.as_bytes())
      .await
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

    // 改权在独占可变的副本上逐条落定，全部应用完才整体写穿存储（存储为唯一
    // 真源；在途连接由写口推进的引擎代数引回本记录、重读整体替换挂载句柄，见
    // RespServerSession::refresh_acl_mount_if_stale）；op 失败早退零残留，不随 C# 先建后验之失败残留（deviations §109，严禁回改）
    let mut new_user = User::from_user(&current_user);

    // 记录操作前自定义命令集，只校验"新增"名（存储既有名不重复校验，
    // 避免无关规则误报）
    let pre_allowed = current_user.custom_commands_allowed();
    let pre_denied = current_user.custom_commands_denied();

    // 其余参数全部为 ACL 操作（字节口径对标 C# parseState.GetString 的 ASCII
    // 折叠：含无效 UTF-8 字节的 >pass/<pass/#hash token 折 '?' 落账而非静默丢弃）
    for op in ops {
      AclParser::apply_acl_op_to_user(&mut new_user, &ascii_sanitize(op))?;
    }

    // SETUSER 时模块已加载：新增的按名自定义权限必须能在
    // CustomCommandManager 解析，未知名大概率是笔误——失败关闭。
    // 授权/收权双向同形：并链单环，只校验"新增"名（先前先收，序不变）
    if let Some(is_registered) = ctx.is_custom_command_registered {
      for name in new_user
        .custom_commands_allowed()
        .iter()
        .chain(new_user.custom_commands_denied().iter())
      {
        if !pre_allowed.contains(name) && !pre_denied.contains(name) && !is_registered(name) {
          return Err(AclError::Acl(format!(
            "Unknown custom command '{name}' (not registered with any loaded module)"
          )));
        }
      }
    }

    store
      .write(target_ns, clean_username.as_bytes(), &new_user.to_bytes())
      .await
      .map_err(|e| AclError::Acl(e.to_string()))
  }

  /// 处理 ACL DELUSER 子命令（向底层存储写入墓碑删除）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclDelUser
  pub async fn network_acl_del_user<D: Device>(
    &self,
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须至少有用户名
    check_arg_count!(parse_state, 1.., output, "acl|deluser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 逐个删除给定用户名的用户（用户名解析/跨租门禁单点，见 resolve_acl_username）
    let mut successful_deletes: i64 = 0;
    let mut error: Option<AclError> = None;
    for username in parse_state {
      let (clean_user, target_ns) = match Self::resolve_acl_username(ctx.caller_namespace, username)
      {
        Ok(res) => res,
        Err(frame) => {
          log::debug!("ACLException: {frame}");
          write_error_raw(output, &frame);
          return Ok(true);
        }
      };
      // ns 0 的 default 为引导态内存单例特殊用户，不可删除（对标 C#
      // DeleteUserHandle 的无条件拦截——C# 无命名空间，default 全局唯一）；
      // 非 0 命名空间下 default 是租户 SETUSER 落盘的普通存储记录（无内存
      // 兜底，见 in_memory_default_user），随租户生命周期可删
      if clean_user == DEFAULT_USER_NAME && target_ns == 0 {
        error = Some(AclError::Acl(
          "The special 'default' user cannot be removed from the system".into(),
        ));
        break;
      }
      match store.delete(target_ns, clean_user.as_bytes()).await {
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
    ctx: &AclCtx<'_>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 不允许附加参数
    check_arg_count!(parse_state, ..=0, output, "acl|whoami");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 返回当前已认证用户的用户名（唯一真源：会话挂载句柄 acl_user_handle，
    // 认证器不驻留镜像；对标 C# 会话直读 userHandle.Name）
    let name = self.user_name().map(str::to_string).unwrap_or_default();
    output.write_resp_bulk_string(name.as_bytes());
    Ok(true)
  }

  /// ACL LOAD/SAVE 同形主体：数据即时持久化，门检通过即回 +OK（对标 doc/zh/db.md §3.4）
  fn acl_noop_persist(
    ctx: &AclCtx<'_>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
    tag: &str,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, tag);
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// 处理 ACL LOAD 子命令（由于数据即时持久化，直接返回 +OK，对标 doc/zh/db.md §3.4）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclLoad
  pub fn network_acl_load(
    &self,
    ctx: &AclCtx<'_>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Self::acl_noop_persist(ctx, parse_state, output, "acl|load")
  }

  /// 处理 ACL SAVE 子命令（由于数据即时持久化，直接返回 +OK，对标 doc/zh/db.md §3.4）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclSave
  pub fn network_acl_save(
    &self,
    ctx: &AclCtx<'_>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Self::acl_noop_persist(ctx, parse_state, output, "acl|save")
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
      let Some(bits) = parse_i64_arg(bits_arg, output) else {
        return Ok(true);
      };

      if bits <= 0 || bits > GENPASS_MAX_BITS {
        abort_with_error_message(output, RESP_ERR_ACL_GENPASS_BITS_RANGE);
        return Ok(true);
      }

      // 位数向上取整到 4 的倍数再折算字节数得口令长度
      length = (bits / 4 + (bits % 4 != 0) as i64) as usize;
    }

    // 凭据面熵源＝OS CSPRNG（getrandom 承接 .NET RandomNumberGenerator 同一档
    // 取随机；仓内 fastrand 的性能选型不覆盖凭据面）。输出形状不变：
    // length 个小写 hex 字符，一字符折一个 nibble
    let mut entropy = vec![0u8; length.div_ceil(2)];
    if getrandom::fill(&mut entropy).is_err() {
      abort_with_error_message(output, RESP_ERR_ACL_GENPASS_ENTROPY_FAILED);
      return Ok(true);
    }
    let mut password = Vec::with_capacity(length);
    for byte in &entropy {
      for nibble in [byte >> 4, byte & 0x0f] {
        password.push(if nibble < 10 {
          b'0' + nibble
        } else {
          b'a' + nibble - 10
        });
      }
    }
    password.truncate(length);
    output.write_resp_bulk_string(&password);
    Ok(true)
  }

  /// 处理 ACL GETUSER 子命令（点查底层存储并反序列化输出）
  ///
  /// libs/server/Resp/ACLCommands.cs:NetworkAclGetUser
  pub async fn network_acl_get_user<D: Device>(
    &self,
    ctx: &AclCtx<'_>,
    store: &AclStore<'_, D>,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 必须提供用户名
    check_arg_count!(parse_state, 1, output, "acl|getuser");
    if !Self::validate_acl_authenticator(ctx, output) {
      return Ok(true);
    }

    // 用户名解析/跨租门禁单点（见 resolve_acl_username）
    let (clean_user, target_ns) =
      match Self::resolve_acl_username(ctx.caller_namespace, parse_state[0]) {
        Ok(res) => res,
        Err(frame) => {
          write_error_raw(output, &frame);
          return Ok(true);
        }
      };

    let record = match store.read(target_ns, clean_user.as_bytes()).await {
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
      Some(bytes) => match User::from_rule_bytes(&clean_user, &bytes) {
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
  /// 认证成功（携带用户句柄、目标命名空间与读前采样引擎 ACL 代数；代数于
  /// 点查记录【之前】采得，由调用方随句柄挂载，杜绝挂载现场读后采样吞掉
  /// 窗口内并发改权）
  Success(Arc<UserHandle>, u64, Option<u64>),
  /// 用户名或口令不匹配（含记录在场但账号停用、规则不可解析、用户名非法）。
  /// 记录在场即存储为唯一真源，本态【绝不】回落引导期内存认证器——回落即
  /// SETUSER default 换密/停用后旧 requirepass 经回落臂复活的认证面失效缝
  /// （对标 C# `GarnetACLAuthenticator.Authenticate` 查得句柄即定、无任何
  /// 回落臂形态，garnet/libs/server/Auth/GarnetACLAuthenticator.cs:58-79）
  Denied,
  /// 存储无该用户记录（非本存储面用户）：仅 ns0 会话由调用方回落引导期内存
  /// 认证器（requirepass / nopass 的 default 单例，装配期不落盘）；非 0 租户
  /// 会话同 Denied 直接 WRONGPASS（租户逃逸门禁）
  NoRecord,
  /// 存储访问失败
  StorageError,
  /// 非 ns0 会话携显式 `#` 前缀/跨租户目标被门禁拦截（与 ACL 管理臂同口径）
  ForeignNamespace,
}

impl RespServerSession {
  /// AUTH 存储点查认证（对标 doc/zh/db.md §3.3）
  ///
  /// 按 `(ns, 0, KeyTag::Acl, user)` 点查底层存储并反序列化用户规则；
  /// 校验账号启用与口令哈希。成功方由调用方挂载连接本地
  /// `Arc<UserHandle>`（句柄随连接析构释放，无全局用户字典）。
  ///
  /// 引擎 ACL 代数在点查记录【之前】读前采样、随 Success 返回（与预门臂
  /// refresh_acl_mount_if_stale、SETUSER 自改快路径同一采样口径），由调用方
  /// 经 set_user_handle 与句柄配对挂载——点查至挂载窗口内的并发
  /// SETUSER/DELUSER bump 必然使配对代数落后于最新，下一命令鉴权预门即触发
  /// 刷新收敛，杜绝旧权限滞留会话生命周期
  pub async fn authenticate_user_via_store<D: Device>(
    &self,
    store: &AclStore<'_, D>,
    username: &[u8],
    password: &[u8],
  ) -> AclAuthOutcome {
    // 调用方仅在命名用户（username 非空）时进入。用户名与口令的字节口径对标
    // C# ParseUtils.ReadString = Encoding.ASCII.GetString（ASCII 折叠单点，
    // 与 SETUSER 落账、引导认证臂同一口径）
    let uname = ascii_sanitize(username);
    let (clean_user, target_ns) = match parse_user_namespace_with_default(&uname, self.namespace) {
      Ok(res) => res,
      Err(_) => return AclAuthOutcome::Denied,
    };
    // 非 ns0 会话禁显式 `#` 前缀换租（与 ACL 管理臂同口径，判据/文案同一）；
    // 拦在回落内存认证器之前——`0#default` 引导单例成功亦属跨租重绑定
    if foreign_namespace_denied(self.namespace, &uname, target_ns) {
      return AclAuthOutcome::ForeignNamespace;
    }
    // 读前采样：必须先于 store.read 点查（Acquire 与 bump 的 Release 配对，
    // 保证随后的点查必见该代数下已落盘的记录）
    let generation = self
      .garnet_api
      .as_ref()
      .and_then(|api| api.acl_generation());
    let record = match store.read(target_ns, clean_user.as_bytes()).await {
      Ok(record) => record,
      Err(_) => return AclAuthOutcome::StorageError,
    };
    // 存储无记录：非本认证面用户，回 NoRecord 交调用方处置——仅 ns0 会话
    // 回落引导内存认证器（default / requirepass 形态）；非 0 租户会话同
    // Denied 即 WRONGPASS，禁回落（租户逃逸门禁，见 network_auth_session）。
    // 记录在场（含停用/口令不符/规则损坏）即存储为唯一真源，一律 Denied，
    // 绝不回落——引导单例旧口令在记录在场后永不可再入场（deviations §98 收口）
    let Some(bytes) = record else {
      return AclAuthOutcome::NoRecord;
    };
    let Ok(user) = User::from_rule_bytes(clean_user, &bytes) else {
      return AclAuthOutcome::Denied;
    };
    if !user.is_enabled() {
      return AclAuthOutcome::Denied;
    }
    // 口令 ASCII 折叠后取哈希：>0x7F 字节逐字节折 '?'，与 SETUSER 落账哈希互认
    //（原 from_utf8 严格拒口径废除——无效 UTF-8 口令直接 WRONGPASS 的分叉，
    // C# 折叠后仍可命中）
    if !user.validate_password(&AclPassword::from_string(&ascii_sanitize(password))) {
      return AclAuthOutcome::Denied;
    }
    AclAuthOutcome::Success(Arc::new(UserHandle::new(user)), target_ns, generation)
  }
}

impl RespServerSession {
  /// C# ProcessOtherCommands 的 ACL arm 集（实现为本文件各 network_acl_* 函数）
  ///
  /// ACL LIST/USERS/CAT/SETUSER/DELUSER/WHOAMI/LOAD/SAVE/GENPASS/GETUSER；
  /// `None` 表示命令不属于本族，调用方继续后续分派。
  /// 上下文就地构建（引导 ACL 以克隆的 Arc 解引用挂入 ctx）
  /// `args` 由调用方显式传入（停车快照的参数视图）：AUTH/HELLO/ACL 族经
  /// 漏斗停车闭环时本命令批已收口、接收缓冲已复位，parse_state 视图失效，
  /// 参数唯一可靠来源是停车快照
  pub async fn process_acl_commands<D: Device>(
    &mut self,
    cmd: RespCommand,
    args: &[&[u8]],
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
    let mut output = mem::take(&mut self.output);
    // SETUSER 读改写与 DELUSER 墓碑删除全程持 ACL 管理串行锁：跨 worker 并发
    // 改同一用户时后到者排队、以最新落盘记录为基线重放，杜绝「读旧 → 写覆盖」
    // 丢更新与收权被并发授权从陈旧基线复活（对标 C# NetworkAclSetUser 的
    // do/while CAS 重试环 + UserHandle.TrySetUser 的 Interlocked.CompareExchange
    // ——rust 存储为唯一真源、无共享句柄可 CAS，串行锁是「两命令操作全生效」
    // 的对位承接）。分派段为 async compio 任务内联段，`.await` 挂起任务态收割：
    // 争用零自旋（临界区含冷记录落盘回读窗口，async 挂起不 park 线程，无同核
    // 死锁，见 wkv SerialLock 类型文档）。其余 ACL 命令只读，不触锁
    let _acl_gate = if matches!(cmd, RespCommand::AclSetuser | RespCommand::AclDeluser) {
      Some(store.lock_acl().await)
    } else {
      None
    };
    // SETUSER 自改目标预判：目标即会话当前已认证用户时，命令后重读记录整体
    // 替换挂载句柄（同连接快路径捷径；跨连接由引擎代数在下一命令预门收敛）
    let refresh_target: Option<(String, u64)> = if cmd == RespCommand::AclSetuser {
      args.first().and_then(|raw| {
        let raw = ascii_sanitize(raw);
        let (name, ns) = parse_user_namespace_with_default(&raw, self.namespace).ok()?;
        let handle = self.acl_user_handle.as_ref()?;
        (handle.user().name == name && self.namespace == ns).then(|| (name.to_string(), ns))
      })
    } else {
      None
    };
    // 引导 ACL 只取不可变配置面：认证器只读共享免锁，直接克隆内层 Arc——
    // 已认证句柄真源在会话，ctx 不持认证器本体借用
    let bootstrap_acl = self
      .acl_authenticator
      .as_ref()
      .map(|acl| Arc::clone(&acl.acl));
    let ctx = AclCtx {
      acl: bootstrap_acl.as_deref(),
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
      RespCommand::AclList => self.network_acl_list(&ctx, store, args, &mut output).await,
      RespCommand::AclUsers => self.network_acl_users(&ctx, store, args, &mut output).await,
      RespCommand::AclCat => self.network_acl_cat(&ctx, args, &mut output),
      RespCommand::AclSetuser => {
        self
          .network_acl_set_user(&ctx, store, args, &mut output)
          .await
      }
      RespCommand::AclDeluser => {
        self
          .network_acl_del_user(&ctx, store, args, &mut output)
          .await
      }
      RespCommand::AclWhoami => self.network_acl_who_am_i(&ctx, args, &mut output),
      RespCommand::AclLoad => self.network_acl_load(&ctx, args, &mut output),
      RespCommand::AclSave => self.network_acl_save(&ctx, args, &mut output),
      RespCommand::AclGenpass => self.network_acl_gen_pass(&ctx, args, &mut output),
      RespCommand::AclGetuser => {
        self
          .network_acl_get_user(&ctx, store, args, &mut output)
          .await
      }
      _ => {
        self.output = output;
        return None;
      }
    };
    self.output = output;
    // 自改即时生效：跨连接改权经引擎代数在下一命令预门收敛（见
    // RespServerSession::refresh_acl_mount_if_stale），本臂只是同连接的快路径
    // 捷径——记录读【前】采代数、读后整体替换句柄，与预门臂共用同一 adopt
    // 出口、同一失效判据，无第二套口径
    if let Some((name, ns)) = refresh_target {
      let generation = self
        .garnet_api
        .as_ref()
        .and_then(|api| api.acl_generation());
      if let Ok(Some(bytes)) = store.read(ns, name.as_bytes()).await
        && let Ok(user) = User::from_rule_bytes(&name, &bytes)
      {
        self.adopt_acl_user(user, generation);
      }
    }
    Some(handled.unwrap_or(true))
  }
}
