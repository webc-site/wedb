//! 命令权限集（对标 libs/server/ACL/CommandPermissionSet.cs）
//!
//! 每个 RespCommand + 子命令占一个位；`All` / `None` 为特殊档位（对标
//! C# 静态单例的身份判定）。自定义（扩展）命令名落在位图之外，走独立
//! 的按名允许 / 拒绝集合，拒绝优先。
//!
//! 在 garnet 中的相对路径: libs/server/ACL/CommandPermissionSet.cs(对标 C# 命令权限集)

use std::sync::{Arc, OnceLock};

use wbase::map::{HashSet, HashSetExt};
use wresp::{
  catalog::{expand_for_acls, is_no_auth, normalize_for_acls},
  command::{LAST_VALID_COMMAND, RespCommand},
};

use super::acl_exception::AclError;

/// 特殊档位（对标 C# `CommandPermissionSet.All` / `None` 单例身份）
#[derive(Clone, Copy, PartialEq, Eq)]
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

/// 全允许位图（编译期常量，用于 All 档物化与快速填充）
pub const ALL_COMMANDS_BITS: [u64; COMMAND_LIST_LEN] = [u64::MAX; COMMAND_LIST_LEN];

/// 全拒绝位图（编译期常量，用于哨兵与默认空集合）
pub const EMPTY_COMMANDS_BITS: [u64; COMMAND_LIST_LEN] = [0; COMMAND_LIST_LEN];

/// bitcode 存储记录权限档位编码字节（对标 [`Special`]，避免序列化私有枚举变体）
pub(crate) const WIRE_MODE_SET: u8 = 0;
/// 全允许哨兵档
pub(crate) const WIRE_MODE_ALL: u8 = 1;
/// 全拒绝哨兵档
pub(crate) const WIRE_MODE_NONE: u8 = 2;

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
      command_list: EMPTY_COMMANDS_BITS,
      custom_allowed: Self::empty_custom(),
      custom_denied: Self::empty_custom(),
      description: "+@all".to_string(),
    }
  }

  /// 全拒绝哨兵（对标 C# `CommandPermissionSet.None`）
  pub fn none() -> Self {
    Self {
      special: Special::None,
      command_list: EMPTY_COMMANDS_BITS,
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

  /// 记录权限档位编码字节（供 [`crate::User`] 的 bitcode 存储编码）
  #[inline]
  pub(crate) fn wire_mode(&self) -> u8 {
    match self.special {
      Special::Set => WIRE_MODE_SET,
      Special::All => WIRE_MODE_ALL,
      Special::None => WIRE_MODE_NONE,
    }
  }

  /// 位图字序列（仅 Set 档有效；哨兵档恒空，解码按档位重建，不重复落零位图）
  #[inline]
  pub(crate) fn wire_bitmap(&self) -> Vec<u64> {
    if matches!(self.special, Special::Set) {
      self.command_list.to_vec()
    } else {
      Vec::new()
    }
  }

  /// 由 bitcode 记录字段结构化重建权限集（绕开文本解析器）
  ///
  /// 哨兵档（All / None）按身份重建以保 `is_all` / `is_none` 快路与等价性判定；
  /// Set 档按位图 + 自定义名集重建，描述串（协议输出面）由调用方回填。
  pub(crate) fn from_wire(
    mode: u8,
    bitmap: &[u64],
    custom_allowed: HashSet<String>,
    custom_denied: HashSet<String>,
  ) -> Result<Self, AclError> {
    match mode {
      WIRE_MODE_ALL => Ok(Self::all()),
      WIRE_MODE_NONE => Ok(Self::none()),
      WIRE_MODE_SET => {
        let command_list: [u64; COMMAND_LIST_LEN] = bitmap.try_into().map_err(|_| {
          AclError::Acl(format!(
            "ACL record bitmap length {} != expected {COMMAND_LIST_LEN}",
            bitmap.len()
          ))
        })?;
        Ok(Self {
          special: Special::Set,
          command_list,
          custom_allowed: Arc::new(custom_allowed),
          custom_denied: Arc::new(custom_denied),
          description: String::new(),
        })
      }
      other => Err(AclError::Acl(format!(
        "unknown ACL permission mode byte {other}"
      ))),
    }
  }

  /// 回填协议输出面描述串（仅 Set 档生效；由 [`crate::User`] 解码后就地
  /// 重建，保证 ACL LIST / GETUSER 输出与迁移前文本口径逐字节一致）。
  /// 哨兵档忽略：`is_all` / `is_none` 档描述恒为规范串（"+@all" / 空串，
  /// [`Self::all`] / [`Self::none`] 构造已落），回填任意串即展示面掩蔽
  /// 真实档位（损坏 / 前向版本记录的防护门）
  #[inline]
  pub(crate) fn set_description(&mut self, description: String) {
    if matches!(self.special, Special::Set) {
      self.description = description;
    }
  }

  /// 共享空集合单例（避免高频分配与加速 Arc::ptr_eq 判等）
  #[inline]
  fn empty_custom() -> Arc<HashSet<String>> {
    static EMPTY: OnceLock<Arc<HashSet<String>>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(HashSet::new())))
  }

  /// 指定位是否置位（单周期位测试，无除法与取模运算）
  #[inline]
  fn bit_on(&self, cmd: RespCommand) -> bool {
    let index = cmd as u16 as usize;
    let word = index >> 6;
    if word < COMMAND_LIST_LEN {
      (self.command_list[word] & (1u64 << (index & 63))) != 0
    } else {
      false
    }
  }

  /// 设置或清除单个命令位
  #[inline]
  fn set_bit(&mut self, cmd: RespCommand, on: bool) {
    let index = cmd as u16 as usize;
    let word = index >> 6;
    if word < COMMAND_LIST_LEN {
      let mask = 1u64 << (index & 63);
      if on {
        self.command_list[word] |= mask;
      } else {
        self.command_list[word] &= !mask;
      }
    }
  }

  /// 批量设置或清除命令与其等价展开集合
  #[inline]
  fn apply_cmd_bit(&mut self, cmd: RespCommand, on: bool) {
    self.set_bit(cmd, on);
    for &extra in expand_for_acls(cmd) {
      self.set_bit(extra, on);
    }
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
    // 回落到泛型命令位（由 +@custom / +@all / +CustomObjCmd 置位）
    self.bit_on(generic_cmd)
  }

  /// 拷贝（All 档物化为全一位图；哨兵身份不随拷贝传递）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:Copy
  pub fn copy(&self) -> Self {
    let command_list = if self.is_all() {
      ALL_COMMANDS_BITS
    } else {
      self.command_list
    };
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
    if self.is_none() {
      self.special = Special::Set;
    }
    self.custom_allowed = insert_custom(&self.custom_allowed, normalized_name);
    self.custom_denied = remove_custom(&self.custom_denied, normalized_name);
  }

  /// 自定义命令名加入拒绝集（同步从允许集摘除，后写胜出）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:RemoveCustomCommand
  pub fn remove_custom_command(&mut self, normalized_name: &str) {
    if self.is_all() {
      self.command_list = ALL_COMMANDS_BITS;
      self.special = Special::Set;
    }
    self.custom_denied = insert_custom(&self.custom_denied, normalized_name);
    self.custom_allowed = remove_custom(&self.custom_allowed, normalized_name);
  }

  /// 自定义命令允许集快照
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CustomAllowed
  #[inline]
  pub fn custom_allowed(&self) -> &HashSet<String> {
    &self.custom_allowed
  }

  /// 自定义命令拒绝集快照
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:CustomDenied
  #[inline]
  pub fn custom_denied(&self) -> &HashSet<String> {
    &self.custom_denied
  }

  /// 置位命令 + 其等价展开（对标 C# AddCommand；非线程安全，调用方串行）
  ///
  /// libs/server/ACL/CommandPermissionSet.cs:AddCommand
  pub fn add_command(&mut self, command: RespCommand) {
    if self.is_all() {
      return;
    }
    if self.is_none() {
      self.special = Special::Set;
    }
    debug_assert!(
      normalize_for_acls(command) == command,
      "Cannot control access to this command, it's an implementation detail"
    );
    self.apply_cmd_bit(command, true);
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
    if is_no_auth(command) || self.is_none() {
      return;
    }
    if self.is_all() {
      self.command_list = ALL_COMMANDS_BITS;
      self.special = Special::Set;
    }
    self.apply_cmd_bit(command, false);
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
