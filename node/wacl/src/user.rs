//! Garnet 用户与其访问权（对标 libs/server/ACL/User.cs）
//!
//! 用户可被多会话共享：命令权限集经写锁原子换新（对标 C# 对
//! `_enabledCommands` 的 Interlocked CAS），口令哈希集与开关位各自
//! 独立同步。

use std::sync::atomic::{AtomicBool, Ordering};

use gxhash::{HashSet, HashSetExt};
use parking_lot::{Mutex, RwLock};
use wresp::RespCommand;

use super::{
  AclPassword, RespAclCategories,
  acl_exception::AclError,
  acl_parser::AclParser,
  command_catalog::{
    CmdEntry, children_of, try_get_commands_for_acl_category, try_get_resp_command_info,
  },
  command_permission_set::CommandPermissionSet,
};

/// Garnet 用户
pub struct User {
  /// 用户名
  pub name: String,
  /// 账号当前是否启用（对标 IsEnabled 属性）
  is_enabled: AtomicBool,
  /// 是否免密登录（对标 IsPasswordless 属性）
  is_passwordless: AtomicBool,
  /// 启用的命令（对标 _enabledCommands，写侧整体换新）
  enabled_commands: RwLock<CommandPermissionSet>,
  /// 全部允许的口令哈希（对标 _passwordHashes + lock）
  password_hashes: Mutex<HashSet<AclPassword>>,
}

impl User {
  /// 以给定名字创建新用户（默认禁用、无口令、无命令权限）
  pub fn new(name: String) -> Self {
    Self {
      name,
      is_enabled: AtomicBool::new(false),
      is_passwordless: AtomicBool::new(false),
      enabled_commands: RwLock::new(CommandPermissionSet::none()),
      password_hashes: Mutex::new(HashSet::new()),
    }
  }

  /// 拷贝构造（对标 C# User(User) 复制构造）
  pub fn from_user(user: &Self) -> Self {
    Self {
      name: user.name.clone(),
      is_enabled: AtomicBool::new(user.is_enabled()),
      is_passwordless: AtomicBool::new(user.is_passwordless()),
      enabled_commands: RwLock::new(user.copy_command_permission_set()),
      password_hashes: Mutex::new(user.copy_password_hashes()),
    }
  }

  /// 账号是否启用
  #[inline]
  pub fn is_enabled(&self) -> bool {
    self.is_enabled.load(Ordering::Relaxed)
  }

  /// 设置账号启用状态
  #[inline]
  pub fn set_enabled(&self, enabled: bool) {
    self.is_enabled.store(enabled, Ordering::Relaxed);
  }

  /// 是否免密
  #[inline]
  pub fn is_passwordless(&self) -> bool {
    self.is_passwordless.load(Ordering::Relaxed)
  }

  /// 设置免密标记
  #[inline]
  pub fn set_passwordless(&self, passwordless: bool) {
    self.is_passwordless.store(passwordless, Ordering::Relaxed);
  }

  /// 用户能否执行给定命令
  ///
  /// libs/server/ACL/User.cs:CanAccessCommand
  #[inline]
  pub fn can_access_command(&self, command: RespCommand) -> bool {
    self.enabled_commands.read().can_run_command(command)
  }

  /// 用户能否按名执行给定自定义（扩展）命令
  ///
  /// libs/server/ACL/User.cs:CanAccessCustomCommand
  #[inline]
  pub fn can_access_custom_command(&self, generic_cmd: RespCommand, custom_name: &str) -> bool {
    self
      .enabled_commands
      .read()
      .can_run_custom_command(generic_cmd, custom_name)
  }

  /// 加入给定分类
  ///
  /// libs/server/ACL/User.cs:AddCategory
  pub fn add_category(&self, category: RespAclCategories) -> Result<(), AclError> {
    let mut perms = self.enabled_commands.write();
    // 全允许档为无操作
    if perms.is_all() {
      return Ok(());
    }

    if category != RespAclCategories::ALL {
      let command_infos = try_get_commands_for_acl_category(category).ok_or_else(|| {
        AclError::Acl("Unable to obtain ACL information, this shouldn't be possible".into())
      })?;
      let cmds = Self::determine_command_details(&command_infos);

      // 已能跑全部成员即无操作
      if cmds.iter().all(|&cmd| perms.can_run_command(cmd)) {
        return Ok(());
      }
      let desc_update = format!("+@{}", AclParser::get_name_by_acl_category(category));

      let mut updated = perms.copy();
      let mut deep = false;
      for cmd in cmds {
        // 存在命令重叠时需要深度合理化
        deep = deep || updated.can_run_command(cmd);
        updated.add_command(cmd);
      }
      updated.description = Self::rationalize_acl_description(
        &updated,
        &format!("{} {desc_update}", updated.description),
        deep,
      );
      *perms = updated;
    } else {
      *perms = CommandPermissionSet::all();
    }
    Ok(())
  }

  /// 加入给定命令（含其全部子命令）
  ///
  /// libs/server/ACL/User.cs:AddCommand
  pub fn add_command(&self, command: RespCommand) -> Result<(), AclError> {
    let info = try_get_resp_command_info(command).ok_or_else(|| {
      AclError::Acl("Unable to obtain ACL information, this shouldn't be possible".into())
    })?;
    let to_add = Self::determine_command_details(&[info]);

    let mut perms = self.enabled_commands.write();
    // 已能跑即无操作，跳过整理工作
    if to_add.iter().all(|&cmd| perms.can_run_command(cmd)) {
      return Ok(());
    }
    let desc_update = format!("+{}", info.name);

    let mut updated = perms.copy();
    let mut deep = false;
    for cmd in to_add {
      deep = deep || updated.can_run_command(cmd);
      updated.add_command(cmd);
    }
    updated.description = Self::rationalize_acl_description(
      &updated,
      &format!("{} {desc_update}", updated.description),
      deep,
    );
    *perms = updated;
    Ok(())
  }

  /// 移除给定分类
  ///
  /// libs/server/ACL/User.cs:RemoveCategory
  pub fn remove_category(&self, category: RespAclCategories) -> Result<(), AclError> {
    let mut perms = self.enabled_commands.write();
    // 从全拒绝档移除是无操作
    if perms.is_none() {
      return Ok(());
    }

    if category != RespAclCategories::ALL {
      let command_infos = try_get_commands_for_acl_category(category).ok_or_else(|| {
        AclError::Acl("Unable to obtain ACL information, this shouldn't be possible".into())
      })?;
      let cmds = Self::determine_command_details(&command_infos);

      // 一个成员都跑不了即无操作
      if !cmds.iter().any(|&cmd| perms.can_run_command(cmd)) {
        return Ok(());
      }
      let desc_update = format!("-@{}", AclParser::get_name_by_acl_category(category));

      let mut updated = perms.copy();
      let mut deep = false;
      for cmd in cmds {
        deep = deep || updated.can_run_command(cmd);
        updated.remove_command(cmd);
      }
      updated.description = Self::rationalize_acl_description(
        &updated,
        &format!("{} {desc_update}", updated.description),
        deep,
      );
      *perms = updated;
    } else {
      *perms = CommandPermissionSet::none();
    }
    Ok(())
  }

  /// 移除给定命令（含其全部子命令）
  ///
  /// libs/server/ACL/User.cs:RemoveCommand
  pub fn remove_command(&self, command: RespCommand) -> Result<(), AclError> {
    let info = try_get_resp_command_info(command).ok_or_else(|| {
      AclError::Acl("Unable to obtain ACL information, this shouldn't be possible".into())
    })?;
    let to_remove = Self::determine_command_details(&[info]);

    let mut perms = self.enabled_commands.write();
    // 全都跑不了即无操作
    if to_remove.iter().all(|&cmd| !perms.can_run_command(cmd)) {
      return Ok(());
    }
    let desc_update = format!("-{}", info.name);

    let mut updated = perms.copy();
    let mut deep = false;
    for cmd in to_remove {
      deep = deep || updated.can_run_command(cmd);
      updated.remove_command(cmd);
    }
    updated.description = Self::rationalize_acl_description(
      &updated,
      &format!("{} {desc_update}", updated.description),
      deep,
    );
    *perms = updated;
    Ok(())
  }

  /// 按名允许自定义命令（名称规范化为大写；拒绝侧同步摘除）
  ///
  /// libs/server/ACL/User.cs:AddCustomCommand
  pub fn add_custom_command(&self, custom_name: &str) -> Result<(), AclError> {
    // 拒绝解析器也会拒绝的名字，防止绕过解析器的调用方毒化可持久化描述
    if !AclParser::is_valid_custom_command_name(custom_name) {
      return Err(AclError::Acl(format!(
        "Invalid custom command name '{custom_name}'"
      )));
    }
    let normalized = custom_name.to_ascii_uppercase();
    let desc_update = normalized.to_ascii_lowercase();

    let mut perms = self.enabled_commands.write();
    // 无操作快路：已允许
    if perms.is_all()
      || (perms.custom_allowed().contains(&normalized)
        && !perms.custom_denied().contains(&normalized))
    {
      return Ok(());
    }
    let mut updated = perms.copy();
    updated.add_custom_command(&normalized);
    updated.description = Self::rationalize_acl_description(
      &updated,
      &format!("{} +{desc_update}", updated.description),
      false,
    );
    *perms = updated;
    Ok(())
  }

  /// 按名拒绝自定义命令（显式拒绝覆盖任何 +@category 的放行）
  ///
  /// libs/server/ACL/User.cs:RemoveCustomCommand
  pub fn remove_custom_command(&self, custom_name: &str) -> Result<(), AclError> {
    if !AclParser::is_valid_custom_command_name(custom_name) {
      return Err(AclError::Acl(format!(
        "Invalid custom command name '{custom_name}'"
      )));
    }
    let normalized = custom_name.to_ascii_uppercase();
    let desc_update = normalized.to_ascii_lowercase();

    let mut perms = self.enabled_commands.write();
    // 无操作快路：已显式拒绝且不在任何允许集
    if !perms.is_all()
      && perms.custom_denied().contains(&normalized)
      && !perms.custom_allowed().contains(&normalized)
    {
      return Ok(());
    }
    let mut updated = perms.copy();
    updated.remove_custom_command(&normalized);
    updated.description = Self::rationalize_acl_description(
      &updated,
      &format!("{} -{desc_update}", updated.description),
      false,
    );
    *perms = updated;
    Ok(())
  }

  /// 新增一个允许口令哈希
  ///
  /// libs/server/ACL/User.cs:AddPasswordHash
  pub fn add_password_hash(&self, password: AclPassword) {
    self.password_hashes.lock().insert(password);
  }

  /// 移除一个允许口令哈希
  ///
  /// libs/server/ACL/User.cs:RemovePasswordHash
  pub fn remove_password_hash(&self, password: AclPassword) {
    self.password_hashes.lock().remove(&password);
  }

  /// 清除全部口令
  ///
  /// libs/server/ACL/User.cs:ClearPasswords
  pub fn clear_passwords(&self) {
    self.password_hashes.lock().clear();
  }

  /// 移除全部已配置能力并禁用用户
  ///
  /// libs/server/ACL/User.cs:Reset
  pub fn reset(&self) {
    self.clear_passwords();
    *self.enabled_commands.write() = CommandPermissionSet::none();
    self.set_enabled(false);
  }

  /// 给定口令哈希对本用户是否有效
  ///
  /// libs/server/ACL/User.cs:ValidatePassword
  pub fn validate_password(&self, password: &AclPassword) -> bool {
    // 免密用户接受任意口令；其余逐一比对注册哈希（常量时间相等）
    if self.is_passwordless() {
      return true;
    }
    self.password_hashes.lock().contains(password)
  }

  /// 以 ACL 规则格式导出用户设置的可读表示
  ///
  /// libs/server/ACL/User.cs:DescribeUser
  pub fn describe_user(&self) -> String {
    let mut out = format!("user {}", self.name);
    // 标志
    out.push_str(if self.is_enabled() { " on" } else { " off" });
    if self.is_passwordless() {
      out.push_str(" nopass");
    }
    // 口令
    for hash in self.password_hashes.lock().iter() {
      out.push_str(&format!(" #{hash}"));
    }
    // ACL
    let perms_str = self.enabled_commands.read().description.clone();
    if !perms_str.trim().is_empty() {
      out.push(' ');
      out.push_str(&perms_str);
    }
    out
  }

  /// 已启用命令的描述
  ///
  /// libs/server/ACL/User.cs:GetEnabledCommandsDescription
  pub fn get_enabled_commands_description(&self) -> String {
    self.enabled_commands.read().description.clone()
  }

  /// 自定义命令允许集快照
  pub fn custom_commands_allowed(&self) -> HashSet<String> {
    self.enabled_commands.read().custom_allowed().clone()
  }

  /// 自定义命令拒绝集快照
  pub fn custom_commands_denied(&self) -> HashSet<String> {
    self.enabled_commands.read().custom_denied().clone()
  }

  /// 由命令信息条目推导其命令 / 子命令对
  ///
  /// libs/server/ACL/User.cs:DetermineCommandDetails
  pub(crate) fn determine_command_details(infos: &[&CmdEntry]) -> Vec<RespCommand> {
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
    let mut parts: Vec<&str> = description
      .split(' ')
      .filter(|p| !p.trim().is_empty())
      .collect();
    // 深度剔除：循环直至一轮无收缩（对标 C# while(useDeepRationalization)，
    // 该标志在循环内不变，等价于单层 if 门 + 收缩重试）
    if use_deep_rationalization {
      let mut shrunk = true;
      while shrunk {
        shrunk = false;
        let mut i = 0;
        while i < parts.len() {
          // 对标 C# `parts.Take(i).Skip(1)`：重建规则永远不含 parts[0]
          let mut without_rule = String::from("user test on >xxx");
          // i <= 1 时取空（LINQ Take/Skip 链优雅出空）
          let kept: &[&str] = if i > 1 { &parts[1..i] } else { &[] };
          for part in kept {
            without_rule.push(' ');
            without_rule.push_str(part);
          }
          if let Ok(without_user) = AclParser::parse_acl_rule(&without_rule, None)
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
    self.enabled_commands.read().copy()
  }

  /// 口令哈希集的拷贝
  ///
  /// libs/server/ACL/User.cs:CopyPasswordHashes
  pub fn copy_password_hashes(&self) -> HashSet<AclPassword> {
    self.password_hashes.lock().clone()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// DescribeUser 输出格式（对标 BasicTests.BasicListTest 期望帧语义）
  #[test]
  fn describe_user_format() {
    // default 用户：on nopass +@all
    let default_user = User::new("default".into());
    default_user.add_category(RespAclCategories::ALL).unwrap();
    default_user.set_enabled(true);
    default_user.set_passwordless(true);
    assert_eq!(default_user.describe_user(), "user default on nopass +@all");

    // 新用户：off
    let u = User::new("x".into());
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
    let u = User::new("x".into());
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
    assert!(u.can_access_custom_command(RespCommand::Customrawstringcmd, "JSON.SET"));
    u.remove_custom_command("json.set").unwrap();
    assert!(!u.can_access_custom_command(RespCommand::Customrawstringcmd, "json.set"));

    // 非法自定义名报错
    assert!(u.add_custom_command("bad name").is_err());
  }

  /// NoAuth 命令移除被忽略（AUTH/HELLO/QUIT 不可拒绝）
  #[test]
  fn no_auth_commands_always_accessible_via_all() {
    let u = User::new("x".into());
    u.add_category(RespAclCategories::ALL).unwrap();
    u.remove_command(RespCommand::Auth).unwrap();
    for cmd in [RespCommand::Auth, RespCommand::Hello, RespCommand::Quit] {
      assert!(u.can_access_command(cmd));
    }
  }

  /// 拷贝构造隔离原用户后续修改
  #[test]
  fn copy_constructor_isolates() {
    let src = User::new("a".into());
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

  /// 口令校验：免密优先 / 多口令 / 常量时间比对（错误哈希不通过）
  #[test]
  fn validate_password_semantics() {
    let u = User::new("x".into());
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
}
