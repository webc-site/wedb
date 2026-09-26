//! Garnet 用户与其访问权（对标 libs/server/ACL/User.cs）
//!
//! 用户为连接本地值语义对象（非 C# 的全连接共享可变句柄）：构造后即不可变，
//! 改权一律在独占可变的构造 / 复制体上一次性落定（[`AclParser`] 规则累加、
//! [`User::from_user`] 复制后改写），装配完成后以 `Arc<User>` 只读共享。
//! 因此本类型自身不持任何并发原语——热路径 `can_access_command` 即纯位图读，
//! 无原子换代、无引用计数往返；唯一的换代点在 [`super::UserHandle`] 整体
//! 替换（重读存储构造新句柄，旧句柄随旧连接语义消亡）。
//!
//! 在 garnet 中的相对路径: libs/server/ACL/GarnetUser.cs? 对标 C# ACL 用户（doc/zh/db.md ACL 按需点查零全局内存）

use std::{iter::once, sync::Arc};

use bitcode::{Decode, Encode};
use wbase::map::{HashSet, HashSetExt};
use wresp::{
  catalog::{
    CmdEntry, RespAclCategories, children_of, commands_for_category, try_get_resp_command_info,
  },
  command::RespCommand,
};

use super::{
  AclPassword, acl_exception::AclError, acl_parser::AclParser, acl_password::NUM_HASH_BYTES,
  command_permission_set::CommandPermissionSet,
};

/// 校验基础用户名（禁止包含 '#'，禁止为空）
#[inline]
pub fn validate_username(name: &str) -> Result<(), AclError> {
  if name.is_empty() {
    return Err(AclError::Acl("Username cannot be empty".into()));
  }
  if name.contains('#') {
    return Err(AclError::Acl(format!(
      "Username '{name}' cannot contain '#'"
    )));
  }
  Ok(())
}

/// 拆分命名空间与用户名：`<ns>#<username>` 或 `<username>`（零堆分配）
///
/// - "0#alice" -> Ok(("alice", 0))
/// - "1#bob" -> Ok(("bob", 1))
/// - "alice" -> Ok(("alice", 0))
/// - "default" -> Ok(("default", 0))
/// - 基础用户名自身禁止包含 '#'
/// - 若带有 '#' 但 '#' 前缀无法解析为合法整数，或 '#' 后面用户名为空，报错拒绝
pub fn parse_user_namespace(raw: &str) -> Result<(&str, u64), AclError> {
  parse_user_namespace_with_default(raw, 0)
}

/// 解析用户名及其目标命名空间（支持指定缺省命名空间）：
/// - 带 '#' 显式指定：如 "1#bob" -> Ok(("bob", 1))
/// - 不带 '#' 隐式归属：使用 default_ns
pub fn parse_user_namespace_with_default(
  raw: &str,
  default_ns: u64,
) -> Result<(&str, u64), AclError> {
  if let Some((ns_str, username)) = raw.split_once('#') {
    if username.is_empty() {
      return Err(AclError::Acl("Username cannot be empty after '#'".into()));
    }
    if username.contains('#') {
      return Err(AclError::Acl(format!(
        "Username '{username}' cannot contain '#'"
      )));
    }
    let ns = ns_str
      .parse::<u64>()
      .map_err(|_| AclError::Acl(format!("Invalid namespace prefix '{ns_str}'")))?;
    Ok((username, ns))
  } else {
    validate_username(raw)?;
    Ok((raw, default_ns))
  }
}

/// Garnet 用户（构造后不可变的连接本地值）
pub struct User {
  /// 用户名
  pub name: String,
  /// 账号当前是否启用（对标 IsEnabled 属性）
  is_enabled: bool,
  /// 是否免密登录（对标 IsPasswordless 属性）
  is_passwordless: bool,
  /// 启用的命令（对标 _enabledCommands，改权在独占可变体上就地落定）
  enabled_commands: CommandPermissionSet,
  /// 全部允许的口令哈希（对标 _passwordHashes 的 HashSet 语义：小量、插入去重、
  /// 删除按值剔除）
  password_hashes: Vec<AclPassword>,
}

/// ACL 用户记录的 bitcode 编码结构（存储唯一格式，见 [`User::to_bytes`] /
/// [`User::from_bytes`]）
///
/// 对标 doc/zh/db.md §3「值内容: 紧凑编码的用户规则（密码哈希、命令分类与
/// 白名单位图）」：`name` + 标志位 + 口令哈希集 + 命令权限位图（`CommandPermissionSet`
/// 已是位图结构）+ 自定义命令按名允许 / 拒绝集 + 协议输出面描述串。存储读写据此
/// 直构 [`User`]，绕开 [`AclParser`]；描述串仅作 ACL LIST / GETUSER 应答文本
/// 生成源（[`User::describe_user`]），不参与命令权限判定。单格式无文本旁轨。
///
/// 演进不变量约束：结构体禁含枚举变体，字段只许尾部追加；若破坏结构须增加版本域并拒旧。
#[derive(Encode, Decode)]
struct UserRecord {
  /// 用户名
  name: String,
  /// 账号是否启用
  is_enabled: bool,
  /// 是否免密登录
  is_passwordless: bool,
  /// 口令哈希集（每条 [`NUM_HASH_BYTES`] 字节）
  password_hashes: Vec<[u8; NUM_HASH_BYTES]>,
  /// 命令权限档位（`WIRE_MODE_*`）
  perms_mode: u8,
  /// 命令权限位图（Set 档为 [`CommandPermissionSet::get_command_list_length`] 个
  /// u64；哨兵档为空，解码按档位重建）
  perms_bitmap: Vec<u64>,
  /// 自定义命令允许集（规范大写名）
  custom_allowed: Vec<String>,
  /// 自定义命令拒绝集（拒绝优先）
  custom_denied: Vec<String>,
  /// 命令权限描述串（协议输出面，逐字节复现迁移前 ACL LIST 文本口径）
  description: String,
}

/// ACL 信息获取失败的样板错误文本（分类校验 / 命令信息查找单源）
const ACL_INFO_ERR: &str = "Unable to obtain ACL information, this shouldn't be possible";

impl User {
  /// 以给定名字创建新用户（默认禁用、无口令、无命令权限）
  pub fn new(name: String) -> Self {
    Self {
      name,
      is_enabled: false,
      is_passwordless: false,
      enabled_commands: CommandPermissionSet::none(),
      password_hashes: Vec::new(),
    }
  }

  /// 序列化为存储载荷（bitcode 紧凑编码，存储唯一格式）
  #[inline]
  pub fn to_bytes(&self) -> Vec<u8> {
    bitcode::encode(&self.to_record())
  }

  /// 导出存储记录快照（读侧汇聚各字段当前值）
  fn to_record(&self) -> UserRecord {
    let perms = &self.enabled_commands;
    UserRecord {
      name: self.name.clone(),
      is_enabled: self.is_enabled,
      is_passwordless: self.is_passwordless,
      password_hashes: self
        .password_hashes
        .iter()
        .map(|h| h.password_hash)
        .collect(),
      perms_mode: perms.wire_mode(),
      perms_bitmap: perms.wire_bitmap(),
      custom_allowed: perms.custom_allowed().iter().cloned().collect(),
      custom_denied: perms.custom_denied().iter().cloned().collect(),
      description: perms.description.clone(),
    }
  }

  /// 从存储载荷还原用户（bitcode 结构化直构，绕开文本解析器）
  pub fn from_bytes(bytes: &[u8]) -> Result<Arc<Self>, AclError> {
    Self::from_record(bitcode::decode(bytes)?)
  }

  /// 从 bitcode 记录结构化直构用户（字段一次落定，无 setter、无原子换代）
  fn from_record(record: UserRecord) -> Result<Arc<Self>, AclError> {
    let custom_allowed: HashSet<String> = record.custom_allowed.into_iter().collect();
    let custom_denied: HashSet<String> = record.custom_denied.into_iter().collect();
    let mut perms = CommandPermissionSet::from_wire(
      record.perms_mode,
      &record.perms_bitmap,
      custom_allowed,
      custom_denied,
    )?;
    // 描述串回填协议输出面（ACL LIST / GETUSER 逐字节复现迁移前文本口径）
    perms.set_description(record.description);
    Ok(Arc::new(User {
      name: record.name,
      is_enabled: record.is_enabled,
      is_passwordless: record.is_passwordless,
      password_hashes: record
        .password_hashes
        .into_iter()
        .map(|hash| AclPassword {
          password_hash: hash,
        })
        .collect(),
      enabled_commands: perms,
    }))
  }

  /// 从存储载荷还原用户（附带存储键用户名合法性校验，与 [`User::from_bytes`] 同解码面）
  pub fn from_rule_bytes(name: &str, bytes: &[u8]) -> Result<Arc<Self>, AclError> {
    validate_username(name)?;
    Self::from_bytes(bytes)
  }

  /// 拷贝构造（对标 C# User(User) 复制构造）：值语义深拷贝，副本与原体不共享
  /// 任何权限状态——改权只作用于副本，正是连接本地模型替代 C# 共享句柄 CAS 换新的出口
  pub fn from_user(user: &Self) -> Self {
    Self {
      name: user.name.clone(),
      is_enabled: user.is_enabled,
      is_passwordless: user.is_passwordless,
      enabled_commands: user.enabled_commands.copy(),
      password_hashes: user.password_hashes.clone(),
    }
  }

  /// 账号是否启用
  #[inline]
  pub fn is_enabled(&self) -> bool {
    self.is_enabled
  }

  /// 设置账号启用状态
  #[inline]
  pub fn set_enabled(&mut self, enabled: bool) {
    self.is_enabled = enabled;
  }

  /// 是否免密
  #[inline]
  pub fn is_passwordless(&self) -> bool {
    self.is_passwordless
  }

  /// 设置免密标记
  #[inline]
  pub fn set_passwordless(&mut self, passwordless: bool) {
    self.is_passwordless = passwordless;
  }

  /// 用户能否执行给定命令（纯位图读，无原子换代、无引用计数往返）
  ///
  /// libs/server/ACL/User.cs:CanAccessCommand
  #[inline]
  pub fn can_access_command(&self, command: RespCommand) -> bool {
    self.enabled_commands.can_run_command(command)
  }

  /// 用户能否按名执行给定自定义（扩展）命令
  ///
  /// libs/server/ACL/User.cs:CanAccessCustomCommand
  #[inline]
  pub fn can_access_custom_command(&self, generic_cmd: RespCommand, custom_name: &str) -> bool {
    self
      .enabled_commands
      .can_run_custom_command(generic_cmd, custom_name)
  }

  /// 加入给定分类（独占可变体上就地落定，无整体位图拷贝、无重试环）
  ///
  /// libs/server/ACL/User.cs:AddCategory
  pub fn add_category(&mut self, category: RespAclCategories) -> Result<(), AclError> {
    self.apply_category(category, true)
  }

  /// 移除给定分类（独占可变体上就地落定）
  ///
  /// libs/server/ACL/User.cs:RemoveCategory
  pub fn remove_category(&mut self, category: RespAclCategories) -> Result<(), AclError> {
    self.apply_category(category, false)
  }

  /// 分类加减同形骨架：`add` 选方向；空分类拒收、极端档位命中即无操作、
  /// ALL 即整体换为全允许 / 全拒绝档（对标 C# 的 CommandPermissionSet(all: true) 分支）
  fn apply_category(&mut self, category: RespAclCategories, add: bool) -> Result<(), AclError> {
    if category != RespAclCategories::ALL && category.is_empty() {
      return Err(AclError::Acl(ACL_INFO_ERR.into()));
    }
    // 全允许档加入 / 全拒绝档移除：无操作
    if (add && self.enabled_commands.is_all()) || (!add && self.enabled_commands.is_none()) {
      return Ok(());
    }
    if category == RespAclCategories::ALL {
      self.enabled_commands = if add {
        CommandPermissionSet::all()
      } else {
        CommandPermissionSet::none()
      };
      return Ok(());
    }
    let cmds = Self::determine_command_details(commands_for_category(category));
    let desc_update = format!(
      "{}@{}",
      if add { '+' } else { '-' },
      AclParser::get_name_by_acl_category(category)
    );
    self.apply_commands(&cmds, &desc_update, add);
    Ok(())
  }

  /// 加入给定命令（含其全部子命令）
  ///
  /// libs/server/ACL/User.cs:AddCommand
  pub fn add_command(&mut self, command: RespCommand) -> Result<(), AclError> {
    self.apply_command(command, true)
  }

  /// 移除给定命令（含其全部子命令）
  ///
  /// libs/server/ACL/User.cs:RemoveCommand
  pub fn remove_command(&mut self, command: RespCommand) -> Result<(), AclError> {
    self.apply_command(command, false)
  }

  /// 命令加减同形骨架：按名取信息、连带子命令展开后走统一应用路径
  fn apply_command(&mut self, command: RespCommand, add: bool) -> Result<(), AclError> {
    let info =
      try_get_resp_command_info(command).ok_or_else(|| AclError::Acl(ACL_INFO_ERR.into()))?;
    let cmds = Self::determine_command_details(once(info));
    let desc_update = format!("{}{}", if add { '+' } else { '-' }, info.name);
    self.apply_commands(&cmds, &desc_update, add);
    Ok(())
  }

  /// 命令集批量增删并更新 ACL 描述串（分类 / 单命令加减共用骨架）：
  /// 无操作快路命中则权限与描述均不写；存在重叠置位时触发深度整理
  fn apply_commands(&mut self, cmds: &[RespCommand], desc_update: &str, add: bool) {
    // 无操作快路：加时已全能跑 / 减时一个都跑不了
    let noop = if add {
      cmds
        .iter()
        .all(|&cmd| self.enabled_commands.can_run_command(cmd))
    } else {
      !cmds
        .iter()
        .any(|&cmd| self.enabled_commands.can_run_command(cmd))
    };
    if noop {
      return;
    }
    let mut deep = false;
    for &cmd in cmds {
      // 存在命令重叠时需要深度合理化
      deep = deep || self.enabled_commands.can_run_command(cmd);
      if add {
        self.enabled_commands.add_command(cmd);
      } else {
        self.enabled_commands.remove_command(cmd);
      }
    }
    let merged = format!("{} {desc_update}", self.enabled_commands.description);
    self.enabled_commands.description =
      Self::rationalize_acl_description(&self.enabled_commands, &merged, deep);
  }

  /// 按名允许自定义命令（名称规范化为大写；拒绝侧同步摘除）
  ///
  /// libs/server/ACL/User.cs:AddCustomCommand
  pub fn add_custom_command(&mut self, custom_name: &str) -> Result<(), AclError> {
    self.apply_custom_command(custom_name, true)
  }

  /// 按名拒绝自定义命令（显式拒绝覆盖任何 +@category 的放行）
  ///
  /// libs/server/ACL/User.cs:RemoveCustomCommand
  pub fn remove_custom_command(&mut self, custom_name: &str) -> Result<(), AclError> {
    self.apply_custom_command(custom_name, false)
  }

  /// 自定义命令加减同形骨架：`add` 选方向；名称规范化为大写后按允许 / 拒绝集
  /// 落定，拒绝优先语义与原双臂一致
  fn apply_custom_command(&mut self, custom_name: &str, add: bool) -> Result<(), AclError> {
    // 拒绝解析器也会拒绝的名字，防止绕过解析器的调用方毒化可持久化描述
    if !AclParser::is_valid_custom_command_name(custom_name) {
      return Err(AclError::Acl(format!(
        "Invalid custom command name '{custom_name}'"
      )));
    }
    let normalized = custom_name.to_ascii_uppercase();
    let perms = &self.enabled_commands;
    let allowed = perms.custom_allowed().contains(&normalized);
    let denied = perms.custom_denied().contains(&normalized);
    // 无操作快路：加时全允许档或已允许（未被拒）；减时非全允许档且已拒（未被允许）
    let noop = if add {
      perms.is_all() || (allowed && !denied)
    } else {
      !perms.is_all() && denied && !allowed
    };
    if noop {
      return Ok(());
    }
    let sign = if add { '+' } else { '-' };
    let lower = normalized.to_ascii_lowercase();
    if add {
      self.enabled_commands.add_custom_command(&normalized);
    } else {
      self.enabled_commands.remove_custom_command(&normalized);
    }
    let merged = format!("{} {sign}{lower}", self.enabled_commands.description);
    self.enabled_commands.description =
      Self::rationalize_acl_description(&self.enabled_commands, &merged, false);
    Ok(())
  }

  /// 新增一个允许口令哈希（集合语义：重复口令不产生第二条目）
  ///
  /// libs/server/ACL/User.cs:AddPasswordHash
  pub fn add_password_hash(&mut self, password: AclPassword) {
    if !self.password_hashes.contains(&password) {
      self.password_hashes.push(password);
    }
  }

  /// 移除一个允许口令哈希
  ///
  /// libs/server/ACL/User.cs:RemovePasswordHash
  pub fn remove_password_hash(&mut self, password: AclPassword) {
    self.password_hashes.retain(|h| *h != password);
  }

  /// 清除全部口令
  ///
  /// libs/server/ACL/User.cs:ClearPasswords
  pub fn clear_passwords(&mut self) {
    self.password_hashes.clear();
  }

  /// 移除全部已配置能力并禁用用户
  ///
  /// libs/server/ACL/User.cs:Reset
  pub fn reset(&mut self) {
    self.clear_passwords();
    self.enabled_commands = CommandPermissionSet::none();
    self.set_enabled(false);
  }

  /// 给定口令哈希对本用户是否有效
  ///
  /// libs/server/ACL/User.cs:ValidatePassword
  pub fn validate_password(&self, password: &AclPassword) -> bool {
    // 免密用户接受任意口令；其余逐一比对注册哈希（常量时间全遍历，避免提前退出侧信道）
    if self.is_passwordless {
      return true;
    }
    self
      .password_hashes
      .iter()
      .fold(false, |matched, hash| matched | (password == hash))
  }

  /// 以 ACL 规则格式导出用户设置的可读表示
  ///
  /// libs/server/ACL/User.cs:DescribeUser
  pub fn describe_user(&self) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(64);
    out.push_str("user ");
    out.push_str(&self.name);
    // 标志
    out.push_str(if self.is_enabled { " on" } else { " off" });
    if self.is_passwordless {
      out.push_str(" nopass");
    }
    // 口令
    for hash in &self.password_hashes {
      let _ = write!(out, " #{hash}");
    }
    // ACL
    let perms_str = self.enabled_commands.description.trim();
    if !perms_str.is_empty() {
      out.push(' ');
      out.push_str(perms_str);
    }
    out
  }

  /// 已启用命令的描述
  ///
  /// libs/server/ACL/User.cs:GetEnabledCommandsDescription
  pub fn get_enabled_commands_description(&self) -> String {
    self.enabled_commands.description.clone()
  }

  /// 自定义命令允许集快照
  ///
  /// libs/server/ACL/User.cs:CustomCommandsAllowed
  pub fn custom_commands_allowed(&self) -> HashSet<String> {
    self.enabled_commands.custom_allowed().clone()
  }

  /// 自定义命令拒绝集快照
  ///
  /// libs/server/ACL/User.cs:CustomCommandsDenied
  pub fn custom_commands_denied(&self) -> HashSet<String> {
    self.enabled_commands.custom_denied().clone()
  }

  /// 由命令信息条目推导其命令 / 子命令对
  ///
  /// libs/server/ACL/User.cs:DetermineCommandDetails
  pub(crate) fn determine_command_details<'a>(
    infos: impl IntoIterator<Item = &'a CmdEntry>,
  ) -> Vec<RespCommand> {
    let mut cmds = Vec::new();
    for info in infos {
      cmds.push(info.cmd);
      // 根命令连带全部子命令；子命令条目仅其自身
      if info.parent.is_none() {
        cmds.extend(children_of(info.cmd).map(|sub| sub.cmd));
      }
    }
    cmds
  }

  /// 检查描述中是否有 token 可在不改变有效权限的情况下剔除
  ///
  /// 昂贵但 ACL 修改低频；`use_deep_rationalization` 为 true 时执行
  /// 深度剔除循环（对标 C# 同名方法）
  ///
  /// libs/server/ACL/User.cs:RationalizeACLDescription
  fn rationalize_acl_description(
    set: &CommandPermissionSet,
    description: &str,
    use_deep_rationalization: bool,
  ) -> String {
    let mut parts: Vec<&str> = description.split_whitespace().collect();
    // 深度剔除：循环直至一轮无收缩（对标 C# while(useDeepRationalization)，
    // 该标志在循环内不变，等价于单层 if 门 + 收缩重试）
    if use_deep_rationalization {
      let mut shrunk = true;
      while shrunk {
        shrunk = false;
        let mut i = 0;
        while i < parts.len() {
          // 对标 C# `parts.Take(i).Skip(1)`：重建规则永远不含 parts[0]
          let mut without_rule = String::with_capacity(18 + description.len());
          without_rule.push_str("user test on >xxx");
          // i <= 1 时取空（LINQ Take/Skip 链优雅出空）
          let kept: &[&str] = if i > 1 { &parts[1..i] } else { &[] };
          for part in kept {
            without_rule.push(' ');
            without_rule.push_str(part);
          }
          if let Ok(without_user) = AclParser::parse_acl_rule(&without_rule)
            && without_user
              .copy_command_permission_set()
              .is_equivalent_to(set)
          {
            parts.remove(i);
            shrunk = true;
            continue; // 不递增 i（对标 C# i-- 后继续）
          }
          i += 1;
        }
      }
    }
    parts.join(" ")
  }

  /// 当前命令权限集的拷贝
  ///
  /// libs/server/ACL/User.cs:CopyCommandPermissionSet
  pub fn copy_command_permission_set(&self) -> CommandPermissionSet {
    self.enabled_commands.copy()
  }

  /// 口令哈希集的拷贝
  ///
  /// libs/server/ACL/User.cs:CopyPasswordHashes
  pub fn copy_password_hashes(&self) -> HashSet<AclPassword> {
    let mut set = HashSet::with_capacity(self.password_hashes.len());
    set.extend(self.password_hashes.iter().cloned());
    set
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::UserHandle;

  /// DescribeUser 输出格式（对标 BasicTests.BasicListTest 期望帧语义）
  #[test]
  fn describe_user_format() {
    // default 用户：on nopass +@all
    let mut default_user = User::new("default".into());
    default_user.add_category(RespAclCategories::ALL).unwrap();
    default_user.set_enabled(true);
    default_user.set_passwordless(true);
    assert_eq!(default_user.describe_user(), "user default on nopass +@all");

    // 新用户：off
    let mut u = User::new("x".into());
    assert_eq!(u.describe_user(), "user x off");

    // 带口令哈希
    u.set_enabled(true);
    u.add_password_hash(AclPassword::from_string("passw0rd"));
    let described = u.describe_user();
    assert_eq!(
      described,
      "user x on #8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9".to_string()
    );
  }

  /// 命令 / 分类 / 自定义命令访问决策
  #[test]
  fn access_decisions() {
    let mut u = User::new("x".into());
    assert!(!u.can_access_command(RespCommand::Get));

    u.add_command(RespCommand::Get).unwrap();
    assert!(u.can_access_command(RespCommand::Get));
    assert!(!u.can_access_command(RespCommand::Set));

    // 分类增删
    u.remove_command(RespCommand::Get).unwrap();
    u.add_category(RespAclCategories::KEYSPACE).unwrap();
    // DEL 属 keyspace
    assert!(u.can_access_command(RespCommand::Del));
    u.remove_category(RespAclCategories::KEYSPACE).unwrap();
    assert!(!u.can_access_command(RespCommand::Del));

    // 自定义命令按名 allow / deny
    u.add_custom_command("json.set").unwrap();
    assert!(u.can_access_custom_command(RespCommand::Customobjcmd, "JSON.SET"));
    u.remove_custom_command("json.set").unwrap();
    assert!(!u.can_access_custom_command(RespCommand::Customobjcmd, "json.set"));

    // 非法自定义名报错
    assert!(u.add_custom_command("bad name").is_err());
  }

  /// rust 自增命令 SUNSUBSCRIBE（位 370，C# 枚举尾值为 QUIT）的授权闭环：
  /// 入命令目录后，非 all 档用户经 +@pubsub 类别或显式 +sunsubscribe 均可
  /// 置位，显式 -sunsubscribe 可从类别展开中撤销（入册前位图含位 370 却无
  /// 任何置位入口）
  #[test]
  fn sunsubscribe_acl_grant_paths() {
    let by_category = AclParser::parse_acl_rule("user sunsub-cat on nopass -@all +@pubsub")
      .expect("+@pubsub 规则须可解析");
    assert!(
      by_category.can_access_command(RespCommand::Sunsubscribe),
      "+@pubsub 应经目录展开覆盖 SUNSUBSCRIBE"
    );

    let by_name = AclParser::parse_acl_rule("user sunsub-name on nopass -@all +sunsubscribe")
      .expect("+sunsubscribe 规则须可解析");
    assert!(by_name.can_access_command(RespCommand::Sunsubscribe));
    // 逐命令授权不牵连同族命令
    assert!(!by_name.can_access_command(RespCommand::Ssubscribe));

    let revoked =
      AclParser::parse_acl_rule("user sunsub-rev on nopass -@all +@pubsub -sunsubscribe")
        .expect("-sunsubscribe 规则须可解析");
    assert!(!revoked.can_access_command(RespCommand::Sunsubscribe));
  }

  /// NoAuth 命令移除被忽略（AUTH/HELLO/QUIT 不可拒绝）
  #[test]
  fn no_auth_commands_always_accessible_via_all() {
    let mut u = User::new("x".into());
    u.add_category(RespAclCategories::ALL).unwrap();
    u.remove_command(RespCommand::Auth).unwrap();
    for cmd in [RespCommand::Auth, RespCommand::Hello, RespCommand::Quit] {
      assert!(u.can_access_command(cmd));
    }
  }

  /// 拷贝构造隔离原用户后续修改
  #[test]
  fn copy_constructor_isolates() {
    let mut src = User::new("a".into());
    src.set_enabled(true);
    src.add_password_hash(AclPassword::from_string("p"));
    src.add_command(RespCommand::Get).unwrap();

    let snapshot = User::from_user(&src);
    src.add_command(RespCommand::Set).unwrap();
    src.add_password_hash(AclPassword::from_string("q"));
    src.set_enabled(false);

    assert!(snapshot.is_enabled());
    assert!(snapshot.can_access_command(RespCommand::Get));
    assert!(!snapshot.can_access_command(RespCommand::Set));
    assert_eq!(snapshot.copy_password_hashes().len(), 1);
  }

  /// 连接本地值语义回归：改权只在独占可变的副本上发生，换代 = 重读存储后
  /// 整体替换句柄（[`UserHandle`] 构造即定格）；已在多处只读共享的
  /// `Arc<User>` 永不因他处改权而漂移（C# 全局共享 User 形态下，`&self` CAS
  /// 换代会让所有持有者同时看到新权限——本断言即证伪该形态在本仓复现）
  #[test]
  fn shared_user_snapshot_does_not_drift() {
    let base = Arc::new(User::new("bob".into()));
    // 读侧：另一持有者看到的快照
    let reader = Arc::clone(&base);
    assert!(!reader.can_access_command(RespCommand::Set));

    // 写侧：复制 → 独占改权 → 新句柄（生产形态：ACL SETUSER 后重读存储
    // 构造新句柄替换，旧句柄随旧连接语义消亡）
    let mut updated = User::from_user(&base);
    updated.add_command(RespCommand::Set).unwrap();
    let handle = UserHandle::new(Arc::new(updated));

    // 原共享体不受影响，新权限只经新句柄可见
    assert!(!base.can_access_command(RespCommand::Set));
    assert!(!reader.can_access_command(RespCommand::Set));
    assert!(handle.user().can_access_command(RespCommand::Set));
  }

  /// 口令校验：免密优先 / 多口令 / 常量时间比对（错误哈希不通过）
  #[test]
  fn validate_password_semantics() {
    let mut u = User::new("x".into());
    u.add_password_hash(AclPassword::from_string("a"));
    u.add_password_hash(AclPassword::from_string("b"));
    assert!(u.validate_password(&AclPassword::from_string("a")));
    assert!(u.validate_password(&AclPassword::from_string("b")));
    assert!(!u.validate_password(&AclPassword::from_string("c")));

    u.set_passwordless(true);
    assert!(u.validate_password(&AclPassword::from_string("anything")));

    // 先关闭免密再验证移除语义（免密档任意口令恒通过）
    u.set_passwordless(false);
    u.remove_password_hash(AclPassword::from_string("a"));
    assert!(!u.validate_password(&AclPassword::from_string("a")));
    assert!(u.validate_password(&AclPassword::from_string("b")));
  }

  /// 用户名校验与命名空间拆分测试
  #[test]
  fn test_parse_user_namespace() {
    // 合法拆分
    assert_eq!(parse_user_namespace("0#alice").unwrap(), ("alice", 0));
    assert_eq!(parse_user_namespace("1#bob").unwrap(), ("bob", 1));
    assert_eq!(parse_user_namespace("42#carol").unwrap(), ("carol", 42));
    assert_eq!(parse_user_namespace("alice").unwrap(), ("alice", 0));
    assert_eq!(parse_user_namespace("default").unwrap(), ("default", 0));

    // 非法情况
    // 基础用户名自身含 '#'
    assert!(validate_username("alice#1").is_err());
    assert!(validate_username("").is_err());
    assert!(validate_username("a#b#c").is_err());
    assert!(validate_username("alice").is_ok());

    // 格式错误
    assert!(parse_user_namespace("").is_err());
    assert!(parse_user_namespace("#alice").is_err());
    assert!(parse_user_namespace("1#").is_err());
    assert!(parse_user_namespace("1#bob#2").is_err());
    assert!(parse_user_namespace("-1#bob").is_err());
    assert!(parse_user_namespace("abc#bob").is_err());
    assert!(parse_user_namespace("alice#1").is_err()); // 前缀非整数
  }

  /// 用户记录 bitcode 序列化与结构化反序列化测试
  #[test]
  fn test_user_serialization() {
    let mut u = User::new("alice".into());
    u.set_enabled(true);
    u.set_passwordless(true);
    u.add_category(RespAclCategories::ALL).unwrap();

    let bytes = u.to_bytes();
    // 编码确定性：重复编码字节恒等（存储唯一格式，无文本旁轨）
    assert_eq!(bytes, u.to_bytes());
    // 存储值为二进制编码，非旧文本行（单格式无文本旁轨）
    assert!(!bytes.starts_with(b"user "));

    let restored = User::from_bytes(&bytes).unwrap();
    assert_eq!(restored.name, "alice");
    assert!(restored.is_enabled());
    assert!(restored.is_passwordless());
    assert!(restored.can_access_command(RespCommand::Get));
    // 协议输出面文本经 decode → describe_user 就地复现
    assert_eq!(restored.describe_user(), u.describe_user());

    // from_rule_bytes：以文本入参面构造规则，落二进制记录后按键名点查还原
    let seeded = AclParser::parse_acl_rule("user bob on nopass +get").unwrap();
    let u2 = User::from_rule_bytes("bob", &seeded.to_bytes()).unwrap();
    assert_eq!(u2.name, "bob");
    assert!(u2.is_enabled());
    assert!(u2.is_passwordless());
    assert!(u2.can_access_command(RespCommand::Get));
    assert!(!u2.can_access_command(RespCommand::Set));
  }

  /// bitcode 记录承载口令哈希集与自定义命令允许 / 拒绝集（多字段往返）
  #[test]
  fn test_user_serialization_passwords_and_custom() {
    let mut u = User::new("carol".into());
    u.set_enabled(true);
    u.add_password_hash(AclPassword::from_string("passw0rd"));
    u.add_password_hash(AclPassword::from_string("another"));
    u.add_custom_command("json.set").unwrap();
    u.add_custom_command("rfm.scan").unwrap();
    u.remove_custom_command("bad.cmd").unwrap();

    let restored = User::from_bytes(&u.to_bytes()).unwrap();
    assert_eq!(restored.name, "carol");
    assert!(restored.validate_password(&AclPassword::from_string("passw0rd")));
    assert!(restored.validate_password(&AclPassword::from_string("another")));
    assert!(!restored.validate_password(&AclPassword::from_string("nope")));
    assert!(restored.custom_commands_allowed().contains("JSON.SET"));
    assert!(restored.custom_commands_allowed().contains("RFM.SCAN"));
    assert!(restored.custom_commands_denied().contains("BAD.CMD"));
    assert!(restored.can_access_custom_command(RespCommand::Customobjcmd, "json.set"));
  }
}
