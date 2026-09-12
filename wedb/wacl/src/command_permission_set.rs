//! 命令权限集（对标 libs/server/ACL/CommandPermissionSet.cs）
//!
//! 每个 RespCommand + 子命令占一个位；`All` / `None` 为特殊档位（对标
//! C# 静态单例的身份判定）。自定义（扩展）命令名落在位图之外，走独立
//! 的按名允许 / 拒绝集合，拒绝优先。

use std::sync::Arc;

use gxhash::{HashSet, HashSetExt};
use wresp::RespCommand;

use super::command_catalog::{LAST_VALID_COMMAND, expand_for_acls, is_no_auth, normalize_for_acls};

/// 特殊档位（对标 C# `CommandPermissionSet.All` / `None` 单例身份）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Special {
  /// 全允许哨兵
  All,
  /// 全拒绝哨兵
  None,
  /// 具体位图档
  Set,
}

/// 权限集位图长度（u64 个数；位数为 LastValidCommand + 1 向上取整）
pub const COMMAND_LIST_LEN: usize = ((LAST_VALID_COMMAND as u16 as usize) + 1).div_ceil(64);

/// 命令权限集
pub struct CommandPermissionSet {
  /// 哨兵档位
  special: Special,
  /// 位图：每个 bit 对应一个 RespCommand / 子命令（哨兵档恒为零）
  command_list: [u64; COMMAND_LIST_LEN],
  /// 自定义命令允许集（名称统一大写，忽略大小写匹配）
  custom_allowed: Arc<HashSet<String>>,
  /// 自定义命令拒绝集（拒绝优先）
  custom_denied: Arc<HashSet<String>>,
  /// 描述（可被 ACL 解析器还原为等价权限集的规则串）
  pub description: String,
}

impl CommandPermissionSet {
  /// 全允许哨兵（对标 C# `CommandPermissionSet.All`）
  pub fn all() -> Self {
    Self {
      special: Special::All,
      command_list: [0; COMMAND_LIST_LEN],
      custom_allowed: Self::empty_custom(),
      custom_denied: Self::empty_custom(),
      description: "+@all".to_string(),
    }
  }

  /// 全拒绝哨兵（对标 C# `CommandPermissionSet.None`）
  pub fn none() -> Self {
    Self {
      special: Special::None,
      command_list: [0; COMMAND_LIST_LEN],
      custom_allowed: Self::empty_custom(),
      custom_denied: Self::empty_custom(),
      description: String::new(),
    }
  }
}

impl Default for CommandPermissionSet {
  fn default() -> Self {
    Self::none()
  }
}

impl CommandPermissionSet {
  /// 是否全允许哨兵（对标 C# `== CommandPermissionSet.All`）
  #[inline]
  pub fn is_all(&self) -> bool {
    matches!(self.special, Special::All)
  }

  /// 是否全拒绝哨兵（对标 C# `== CommandPermissionSet.None`）
  #[inline]
  pub fn is_none(&self) -> bool {
    matches!(self.special, Special::None)
  }

  /// 权限集位图长度（u64 个数；位数为 LastValidCommand + 1 向上取整）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:GetCommandListLength
  pub const fn get_command_list_length() -> usize {
    COMMAND_LIST_LEN
  }

  #[inline]
  fn empty_custom() -> Arc<HashSet<String>> {
    Arc::new(HashSet::new())
  }

  /// 指定位是否可跑
  #[inline]
  fn bit_on(&self, cmd: RespCommand) -> bool {
    let index = cmd as u16 as usize;
    if index >= COMMAND_LIST_LEN * 64 {
      return false;
    }
    (self.command_list[index / 64] >> (index % 64)) & 1 == 1
  }

  /// 命令 + 子命令对是否可执行
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CanRunCommand
  #[inline]
  pub fn can_run_command(&self, command: RespCommand) -> bool {
    // "一切皆允许" 走快路；全拒绝档不特判（位图本身即全零）
    if self.is_all() {
      return true;
    }
    self.bit_on(command)
  }

  /// 自定义（扩展）命令是否可执行（拒绝优先，按名匹配大小写不敏感）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CanRunCustomCommand
  #[inline]
  pub fn can_run_custom_command(&self, generic_cmd: RespCommand, custom_name: &str) -> bool {
    if self.is_all() {
      return true;
    }
    if !self.custom_denied.is_empty() && contains_ignore(&self.custom_denied, custom_name) {
      return false;
    }
    if !self.custom_allowed.is_empty() && contains_ignore(&self.custom_allowed, custom_name) {
      return true;
    }
    // 回落到泛型命令位（由 +@custom / +@all / +CustomRawStringCmd 置位）
    self.bit_on(generic_cmd)
  }

  /// 拷贝（All 档物化为全一位图；哨兵身份不随拷贝传递）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:Copy
  pub fn copy(&self) -> Self {
    let mut command_list = self.command_list;
    if self.is_all() {
      command_list.fill(u64::MAX);
    }
    Self {
      special: Special::Set,
      command_list,
      custom_allowed: Arc::clone(&self.custom_allowed),
      custom_denied: Arc::clone(&self.custom_denied),
      description: self.description.clone(),
    }
  }

  /// 自定义命令名加入允许集（同步从拒绝集摘除，后写胜出；调用方需持写侧串行）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:AddCustomCommand
  pub fn add_custom_command(&mut self, normalized_name: &str) {
    self.custom_allowed = insert_custom(&self.custom_allowed, normalized_name);
    self.custom_denied = remove_custom(&self.custom_denied, normalized_name);
  }

  /// 自定义命令名加入拒绝集（同步从允许集摘除，后写胜出）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:RemoveCustomCommand
  pub fn remove_custom_command(&mut self, normalized_name: &str) {
    self.custom_denied = insert_custom(&self.custom_denied, normalized_name);
    self.custom_allowed = remove_custom(&self.custom_allowed, normalized_name);
  }

  /// 自定义命令允许集快照
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CustomAllowed
  pub fn custom_allowed(&self) -> &HashSet<String> {
    &self.custom_allowed
  }

  /// 自定义命令拒绝集快照
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CustomDenied
  pub fn custom_denied(&self) -> &HashSet<String> {
    &self.custom_denied
  }

  /// 置位命令 + 其等价展开（对标 C# AddCommand；非线程安全，调用方串行）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:AddCommand
  pub fn add_command(&mut self, command: RespCommand) {
    debug_assert!(
      normalize_for_acls(command) == command,
      "Cannot control access to this command, it's an implementation detail"
    );
    let index = command as u16 as usize;
    self.command_list[index / 64] |= 1u64 << (index % 64);
    for extra in expand_for_acls(command) {
      let i = *extra as u16 as usize;
      self.command_list[i / 64] |= 1u64 << (i % 64);
    }
  }

  /// 清位命令 + 其等价展开（NoAuth 命令不可移除；非线程安全，调用方串行）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:RemoveCommand
  pub fn remove_command(&mut self, command: RespCommand) {
    debug_assert!(
      normalize_for_acls(command) == command,
      "Cannot control access to this command, it's an implementation detail"
    );
    // 这些命令的访问权不可移除
    if is_no_auth(command) {
      return;
    }
    let index = command as u16 as usize;
    self.command_list[index / 64] &= !(1u64 << (index % 64));
    for extra in expand_for_acls(command) {
      let i = *extra as u16 as usize;
      self.command_list[i / 64] &= !(1u64 << (i % 64));
    }
  }

  /// 与另一权限集是否等价（覆盖同一可执行命令集合；All 仅与 All 等价）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:IsEquivalentTo
  pub fn is_equivalent_to(&self, other: &Self) -> bool {
    if self.is_all() {
      other.is_all()
    } else {
      self.command_list == other.command_list
        && (Arc::ptr_eq(&self.custom_allowed, &other.custom_allowed)
          || set_eq_ignore(&self.custom_allowed, &other.custom_allowed))
        && (Arc::ptr_eq(&self.custom_denied, &other.custom_denied)
          || set_eq_ignore(&self.custom_denied, &other.custom_denied))
    }
  }
}

/// 大小写不敏感包含（集合内存放的即规范名，集合极小，线性零分配）
#[inline]
fn contains_ignore(set: &HashSet<String>, name: &str) -> bool {
  set.iter().any(|s| s.eq_ignore_ascii_case(name))
}

/// 集合大小写不敏感相等（名称已规范化，元素个数先比再逐个比对）
#[inline]
fn set_eq_ignore(a: &HashSet<String>, b: &HashSet<String>) -> bool {
  a.len() == b.len() && a.iter().all(|x| contains_ignore(b, x))
}

/// 复制集合并插入（名称已规范化；集合极小，整体重建）
#[inline]
fn insert_custom(set: &HashSet<String>, normalized_name: &str) -> Arc<HashSet<String>> {
  let mut next = HashSet::clone(set);
  next.insert(normalized_name.to_string());
  Arc::new(next)
}

/// 复制集合并移除
#[inline]
fn remove_custom(set: &HashSet<String>, normalized_name: &str) -> Arc<HashSet<String>> {
  let mut next = HashSet::new();
  next.extend(
    set
      .iter()
      .filter(|s| !s.eq_ignore_ascii_case(normalized_name))
      .cloned(),
  );
  Arc::new(next)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::RespAclCategories;

  #[test]
  fn sentinels_and_copy() {
    let none = CommandPermissionSet::none();
    assert!(none.is_none());
    assert!(!none.can_run_command(RespCommand::Get));
    assert_eq!(none.description, "");

    let all = CommandPermissionSet::all();
    assert!(all.is_all());
    assert!(all.can_run_command(RespCommand::Get));

    // All 物化拷贝：全一，不再是哨兵，但与 All 等价性按身份判定
    let materialized = all.copy();
    assert!(!materialized.is_all());
    assert!(materialized.can_run_command(RespCommand::Get));
    assert!(!materialized.is_equivalent_to(&all));
    assert!(all.is_equivalent_to(&CommandPermissionSet::all()));

    // None 拷贝后与空集等价（对标 C# 位图比较路径）
    let empty = none.copy();
    assert!(empty.is_equivalent_to(&CommandPermissionSet::none()));
    // C# 怪癖：All 实例底座位图全零，空位图与之比较经位图路径返回 true
    assert!(empty.is_equivalent_to(&all));
  }

  #[test]
  fn add_remove_command_with_expansion() {
    let mut set = CommandPermissionSet::none().copy();
    set.add_command(RespCommand::Set);
    // SET 置位同时展开 SETEXNX / SETEXXX / SETKEEPTTL / SETKEEPTTLXX
    for cmd in [
      RespCommand::Set,
      RespCommand::Setexnx,
      RespCommand::Setexxx,
      RespCommand::Setkeepttl,
      RespCommand::Setkeepttlxx,
    ] {
      assert!(set.can_run_command(cmd));
    }
    set.remove_command(RespCommand::Set);
    assert!(!set.can_run_command(RespCommand::Set));
    assert!(!set.can_run_command(RespCommand::Setkeepttl));
  }

  #[test]
  fn no_auth_commands_cannot_be_removed() {
    let mut set = CommandPermissionSet::all().copy();
    set.remove_command(RespCommand::Auth);
    set.remove_command(RespCommand::Hello);
    set.remove_command(RespCommand::Quit);
    for cmd in [RespCommand::Auth, RespCommand::Hello, RespCommand::Quit] {
      assert!(set.can_run_command(cmd));
    }
  }

  #[test]
  fn custom_command_deny_precedence() {
    let mut set = CommandPermissionSet::none().copy();
    assert!(!set.can_run_custom_command(RespCommand::Customrawstringcmd, "json.set"));

    set.add_custom_command("JSON.SET");
    assert!(set.can_run_custom_command(RespCommand::Customrawstringcmd, "json.set"));
    assert!(set.can_run_custom_command(RespCommand::Customrawstringcmd, "JSON.SET"));

    // 后写胜出：再拒绝后，拒绝优先
    set.remove_custom_command("json.set");
    assert!(!set.can_run_custom_command(RespCommand::Customrawstringcmd, "json.set"));

    // 泛型位兜底（+CustomRawStringCmd 置位后按名未列也放行）
    set.add_command(RespCommand::Customrawstringcmd);
    assert!(set.can_run_custom_command(RespCommand::Customrawstringcmd, "other.cmd"));
  }

  #[test]
  fn command_list_length_covers_all_commands() {
    // Reset = 369 为最大有效命令 → 需 370 位 → 6 个 u64
    let len = CommandPermissionSet::get_command_list_length();
    assert_eq!(len, 6);
    assert!(len * 64 > RespCommand::Reset as u16 as usize);
  }

  #[test]
  fn category_bits_sanity() {
    assert_eq!(RespAclCategories::ALL.bits(), (1 << 24) - 1);
    assert!(RespAclCategories::ALL.contains(RespAclCategories::ADMIN));
    assert!(RespAclCategories::ALL.contains(RespAclCategories::VECTOR));
    assert!(!RespAclCategories::ADMIN.contains(RespAclCategories::BITMAP));
  }
}
