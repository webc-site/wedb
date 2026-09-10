//! 独立 ACL 模型（对标 libs/server/ACL：ACLPassword / User / UserHandle /
//! CommandPermissionSet / ACLParser / AccessControlList / SecretsUtility）
//!
//! 与 wserver 内 1:1 全量转写的 `wserver::acl` 相比，本 crate 是面向嵌入
//! 场景的自包含最小模型：口令仍按 C# 口径以 SHA-256 哈希存储、常量时间
//! 比较；命令权限以"全允许位 + 按名允许/拒绝集"承接（对应 C# 自定义命令
//! 的 per-name 模型，拒绝优先）；`+@all`/`-@all` 映射全允许位，其余分类
//! 需要命令目录，由 wserver::acl 承接（此处报 CategoryDoesNotExist）。

use std::{
  fmt,
  hash::{Hash, Hasher},
  mem,
};

use gxhash::{GxBuildHasher, HashMap, HashSet};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// default 用户名（对标 C# DefaultUserName）
const DEFAULT_USER_NAME: &str = "default";

/// 口令哈希字节数（SHA-256，对标 C# NumHashBytes）
const NUM_HASH_BYTES: usize = 32;

/// libs/server/ACL/SecretsUtility.cs:ConstantEquals
///
/// 常量时间字节比较（不因提前退出泄露前缀信息；长度不等直接 false）
#[inline]
pub fn constant_equals(a: &[u8], b: &[u8]) -> bool {
  if a.len() != b.len() {
    return false;
  }
  let mut diff = 0u8;
  for (x, y) in a.iter().zip(b.iter()) {
    diff |= x ^ y;
  }
  diff == 0
}

/// libs/server/ACL/ACLPassword.cs:ACLPassword
#[derive(Debug, Clone)]
pub struct AclPassword {
  /// 口令哈希（SHA-256）
  pub hash: [u8; NUM_HASH_BYTES],
}

impl AclPassword {
  /// libs/server/ACL/ACLPassword.cs:ACLPasswordFromString
  pub fn from_string(password: &str) -> Self {
    Self {
      hash: Sha256::digest(password.as_bytes()).into(),
    }
  }

  /// libs/server/ACL/ACLPassword.cs:ACLPasswordFromHash
  pub fn from_hash(hash_string: &str) -> Result<Self> {
    let bytes = hash_string.as_bytes();
    if bytes.len() != NUM_HASH_BYTES * 2 {
      return Err(Error::Password);
    }
    let mut hash = [0u8; NUM_HASH_BYTES];
    for (slot, pair) in hash.iter_mut().zip(bytes.as_chunks::<2>().0) {
      let hi = hex_val(pair[0]).ok_or(Error::Password)?;
      let lo = hex_val(pair[1]).ok_or(Error::Password)?;
      *slot = hi << 4 | lo;
    }
    Ok(Self { hash })
  }
}

/// 单个十六进制字符折值（大小写均可）
#[inline]
const fn hex_val(c: u8) -> Option<u8> {
  match c {
    b'0'..=b'9' => Some(c - b'0'),
    b'a'..=b'f' => Some(c - b'a' + 10),
    b'A'..=b'F' => Some(c - b'A' + 10),
    _ => None,
  }
}

/// libs/server/ACL/ACLPassword.cs:Equals（常量时间比较）
impl PartialEq for AclPassword {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    constant_equals(&self.hash, &other.hash)
  }
}

impl Eq for AclPassword {}

/// libs/server/ACL/ACLPassword.cs:GetHashCode（取首字节加速索引）
impl Hash for AclPassword {
  #[inline]
  fn hash<H: Hasher>(&self, state: &mut H) {
    state.write_u8(self.hash[0]);
  }
}

/// libs/server/ACL/ACLPassword.cs:ToString（小写十六进制）
impl fmt::Display for AclPassword {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for b in self.hash {
      write!(f, "{b:02x}")?;
    }
    Ok(())
  }
}

/// libs/server/ACL/CommandPermissionSet.cs:CommandPermissionSet
///
/// 全允许位 + 按名允许/拒绝集（拒绝优先，对应 C# 自定义命令 per-name 模型）
#[derive(Debug, Clone, Default)]
pub struct CommandPermissionSet {
  /// 全允许（对标 +@all / C# All 哨兵）
  pub allow_all: bool,
  /// 按名允许集
  pub allowed_commands: HashSet<String>,
  /// 按名拒绝集（优先级高于允许集）
  pub denied_commands: HashSet<String>,
}

impl CommandPermissionSet {
  /// libs/server/ACL/CommandPermissionSet.cs:CanRunCommand（拒绝优先）
  ///
  /// 拒绝集优先于全允许位：对标 C# "+@all 后 -cmd" 物化为位图清位的语义
  pub fn check_permission(&self, cmd: &str) -> bool {
    if self.denied_commands.contains(cmd) {
      return false;
    }
    self.allow_all || self.allowed_commands.contains(cmd)
  }

  /// 按名放行（同步从拒绝集摘除，后写胜出）
  pub fn allow(&mut self, cmd: &str) {
    self.allowed_commands.insert(cmd.to_string());
    self.denied_commands.remove(cmd);
  }

  /// 按名拒绝（同步从允许集摘除，后写胜出）
  pub fn deny(&mut self, cmd: &str) {
    self.denied_commands.insert(cmd.to_string());
    self.allowed_commands.remove(cmd);
  }
}

/// libs/server/ACL/User.cs:User
#[derive(Debug, Clone)]
pub struct User {
  pub name: String,
  /// 账号是否启用（对标 IsEnabled；新用户默认禁用，同 C# 构造）
  pub is_enabled: bool,
  /// 免密标记（对标 IsPasswordless）
  pub is_passwordless: bool,
  /// 全部允许口令哈希
  pub passwords: Vec<AclPassword>,
  /// 启用的命令权限
  pub permissions: CommandPermissionSet,
}

impl User {
  /// libs/server/ACL/User.cs:User(string)（默认禁用、无口令、无权限）
  pub fn new(name: String) -> Self {
    Self {
      name,
      is_enabled: false,
      is_passwordless: false,
      passwords: Vec::new(),
      permissions: CommandPermissionSet::default(),
    }
  }

  /// libs/server/ACL/User.cs:AddPasswordHash
  pub fn add_password_hash(&mut self, password: AclPassword) {
    if !self.passwords.contains(&password) {
      self.passwords.push(password);
    }
  }

  /// libs/server/ACL/User.cs:RemovePasswordHash
  pub fn remove_password_hash(&mut self, password: &AclPassword) {
    self.passwords.retain(|p| p != password);
  }

  /// libs/server/ACL/User.cs:ClearPasswords
  pub fn clear_passwords(&mut self) {
    self.passwords.clear();
  }

  /// libs/server/ACL/User.cs:ValidatePassword
  pub fn validate_password(&self, password: &AclPassword) -> bool {
    // 免密用户接受任意口令
    if self.is_passwordless {
      return true;
    }
    self.passwords.contains(password)
  }

  /// libs/server/ACL/User.cs:Reset（清口令 + 清权限 + 禁用）
  pub fn reset(&mut self) {
    self.clear_passwords();
    self.permissions = CommandPermissionSet::default();
    self.is_enabled = false;
    self.is_passwordless = false;
  }

  /// libs/server/ACL/User.cs:DescribeUser
  pub fn describe_user(&self) -> String {
    let mut out = format!("user {}", self.name);
    out.push_str(if self.is_enabled { " on" } else { " off" });
    if self.is_passwordless {
      out.push_str(" nopass");
    }
    for hash in &self.passwords {
      out.push_str(&format!(" #{hash}"));
    }
    // 权限规则（保持可再解析；集合迭代序不定，排序输出）
    if self.permissions.allow_all {
      out.push_str(" +@all");
    }
    let mut allowed: Vec<&String> = self.permissions.allowed_commands.iter().collect();
    allowed.sort();
    for cmd in allowed {
      out.push_str(&format!(" +{cmd}"));
    }
    let mut denied: Vec<&String> = self.permissions.denied_commands.iter().collect();
    denied.sort();
    for cmd in denied {
      out.push_str(&format!(" -{cmd}"));
    }
    out
  }
}

/// libs/server/ACL/UserHandle.cs:UserHandle
///
/// 用户句柄；C# 以 Interlocked.CompareExchange 换新（TrySetUser），本模型为
/// 单持有者独占访问，等价于就地整体换新
#[derive(Debug, Clone, Default)]
pub struct UserHandle {
  /// 当前指向的用户
  user: Option<Box<User>>,
}

impl UserHandle {
  /// libs/server/ACL/UserHandle.cs:UserHandle(User)
  pub fn new(user: User) -> Self {
    Self {
      user: Some(Box::new(user)),
    }
  }

  /// libs/server/ACL/UserHandle.cs:User 属性
  pub fn user(&self) -> Option<&User> {
    self.user.as_deref()
  }

  /// 可变访问（就地修改用户）
  pub fn user_mut(&mut self) -> Option<&mut User> {
    self.user.as_deref_mut()
  }

  /// libs/server/ACL/UserHandle.cs:TrySetUser
  pub fn try_set_user(&mut self, new_user: User) -> bool {
    if self.user.is_some() {
      self.user = Some(Box::new(new_user));
      true
    } else {
      false
    }
  }
}

/// libs/server/ACL/AccessControlList.cs:AccessControlList
pub struct AccessControlList {
  /// 用户名 → 句柄
  users: HashMap<String, UserHandle>,
}

impl Default for AccessControlList {
  fn default() -> Self {
    Self::new()
  }
}

impl AccessControlList {
  /// 构造：仅创建 default 用户（对标 C# 无配置文件构造 + CreateDefaultUserHandle：
  /// default 全权、启用、免密）
  pub fn new() -> Self {
    let mut acl = Self {
      users: HashMap::with_hasher(GxBuildHasher::default()),
    };
    let mut default_user = User::new(DEFAULT_USER_NAME.to_string());
    default_user.permissions.allow_all = true;
    default_user.is_enabled = true;
    default_user.is_passwordless = true;
    acl.add_user(default_user).ok();
    acl
  }

  /// libs/server/ACL/AccessControlList.cs:GetUserHandle
  pub fn get_user(&self, username: &str) -> Option<&UserHandle> {
    self.users.get(username)
  }

  /// 可变访问
  pub fn get_user_mut(&mut self, username: &str) -> Option<&mut UserHandle> {
    self.users.get_mut(username)
  }

  /// libs/server/ACL/AccessControlList.cs:AddUserHandle（同名已存在即报错）
  pub fn add_user(&mut self, user: User) -> Result<()> {
    let mut user = user;
    // 键即用户名：移出 name 而非克隆
    let name = mem::take(&mut user.name);
    if self.users.contains_key(&name) {
      return Err(Error::UserAlreadyExists(name));
    }
    self.users.insert(name, UserHandle::new(user));
    Ok(())
  }

  /// libs/server/ACL/AccessControlList.cs:DeleteUserHandle（default 不可删）
  pub fn delete_user(&mut self, username: &str) -> Result<bool> {
    if username == DEFAULT_USER_NAME {
      return Err(Error::Acl(
        "The special 'default' user cannot be removed from the system".into(),
      ));
    }
    Ok(self.users.remove(username).is_some())
  }
}

/// libs/server/ACL/ACLParser.cs:ACLParser
pub struct AclParser;

impl AclParser {
  /// libs/server/ACL/ACLParser.cs:ParseACLRule（单用户多操作形式）
  ///
  /// 支持：on / off / nopass / reset / resetpass / ><明文> / <<明文> /
  /// #<哈希> / !<哈希> / +<命令> / -<命令> / +@all / -@all；
  /// `~*`、`allkeys`、`resetkeys` 为无操作（仅支持全通配键模式，同 C#）
  pub fn parse_rules(user: &mut User, rules: &[&str]) -> Result<()> {
    for rule in rules {
      Self::apply_op(user, rule)?;
    }
    Ok(())
  }

  /// libs/server/ACL/ACLParser.cs:ApplyACLOpToUser
  pub fn apply_op(user: &mut User, rule: &str) -> Result<()> {
    if rule.is_empty() {
      return Ok(());
    }
    let eq_ic = |a: &str| rule.eq_ignore_ascii_case(a);
    if eq_ic("on") {
      user.is_enabled = true;
    } else if eq_ic("off") {
      user.is_enabled = false;
    } else if eq_ic("nopass") {
      user.clear_passwords();
      user.is_passwordless = true;
    } else if eq_ic("reset") {
      user.reset();
    } else if eq_ic("resetpass") {
      user.clear_passwords();
      user.is_passwordless = false;
    } else if let Some(pwd) = rule.strip_prefix('>') {
      user.add_password_hash(AclPassword::from_string(pwd));
    } else if let Some(pwd) = rule.strip_prefix('<') {
      user.remove_password_hash(&AclPassword::from_string(pwd));
    } else if let Some(hash) = rule.strip_prefix('#') {
      user.add_password_hash(AclPassword::from_hash(hash)?);
    } else if let Some(hash) = rule.strip_prefix('!') {
      user.remove_password_hash(&AclPassword::from_hash(hash)?);
    } else if let Some(cat) = rule.strip_prefix("+@") {
      // 完整分类目录需命令元数据，由 wserver::acl 承接；此处仅识别 @all
      if !cat.eq_ignore_ascii_case("all") {
        return Err(Error::CategoryDoesNotExist(cat.to_string()));
      }
      user.permissions.allow_all = true;
    } else if let Some(cat) = rule.strip_prefix("-@") {
      if !cat.eq_ignore_ascii_case("all") {
        return Err(Error::CategoryDoesNotExist(cat.to_string()));
      }
      user.permissions.allow_all = false;
    } else if let Some(cmd) = rule.strip_prefix('+') {
      user.permissions.allow(cmd);
    } else if let Some(cmd) = rule.strip_prefix('-') {
      user.permissions.deny(cmd);
    } else if rule == "~*" || eq_ic("allkeys") || eq_ic("resetkeys") {
      // 无操作：仅支持全通配键模式（同 C# 注释）
    } else {
      return Err(Error::UnknownOperation(rule.to_string()));
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const DUMMY_HASH: &str = "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";

  #[test]
  fn password_hashing_matches_garnet_vectors() {
    // SHA-256("passw0rd") 对标 garnet 测试黄金向量
    let p = AclPassword::from_string("passw0rd");
    assert_eq!(p.to_string(), DUMMY_HASH);
    assert_eq!(AclPassword::from_hash(DUMMY_HASH).unwrap(), p);
    // 大写十六进制同样接受
    assert_eq!(
      AclPassword::from_hash(&DUMMY_HASH.to_uppercase()).unwrap(),
      p
    );
    assert!(AclPassword::from_hash("abcd").is_err());
    assert!(AclPassword::from_hash(&"z".repeat(64)).is_err());
  }

  #[test]
  fn constant_equals_semantics() {
    assert!(constant_equals(b"abc", b"abc"));
    assert!(!constant_equals(b"abc", b"abd"));
    assert!(!constant_equals(b"abc", b"ab"));
  }

  #[test]
  fn default_user_shape() {
    let acl = AccessControlList::new();
    let default = acl.get_user("default").unwrap().user().unwrap();
    assert!(default.is_enabled);
    assert!(default.is_passwordless);
    assert!(default.permissions.check_permission("GET"));
    // default 不可删
    let mut acl2 = AccessControlList::new();
    assert!(acl2.delete_user("default").is_err());
  }

  #[test]
  fn parse_rules_semantics() {
    let mut u = User::new("alice".into());
    AclParser::parse_rules(&mut u, &["on", ">passw0rd", &format!("#{DUMMY_HASH}")]).unwrap();
    // 明文与同哈希去重为一个
    assert_eq!(u.passwords.len(), 1);
    assert!(u.validate_password(&AclPassword::from_string("passw0rd")));

    // < 明文删除 / ! 哈希删除
    AclParser::parse_rules(&mut u, &["<passw0rd"]).unwrap();
    assert!(!u.validate_password(&AclPassword::from_string("passw0rd")));

    // 命令允许 / 拒绝（拒绝优先）
    AclParser::parse_rules(&mut u, &["+get", "+set", "-set"]).unwrap();
    assert!(u.permissions.check_permission("get"));
    assert!(!u.permissions.check_permission("set"));

    // +@all / -@all；+@all 后 -cmd 拒绝优先（位图物化语义）
    AclParser::parse_rules(&mut u, &["+@all", "-set"]).unwrap();
    assert!(u.permissions.check_permission("anything"));
    assert!(!u.permissions.check_permission("set"));
    AclParser::parse_rules(&mut u, &["-@all", "+set"]).unwrap();
    assert!(!u.permissions.check_permission("anything"));
    assert!(u.permissions.check_permission("set"));

    // nopass / resetpass / reset
    AclParser::parse_rules(&mut u, &["nopass"]).unwrap();
    assert!(u.is_passwordless);
    AclParser::parse_rules(&mut u, &["resetpass"]).unwrap();
    assert!(!u.is_passwordless);
    AclParser::parse_rules(&mut u, &["reset"]).unwrap();
    assert!(!u.is_enabled);
    assert!(!u.validate_password(&AclPassword::from_string("x")));

    // 未知操作 / 未知分类（非 all）
    assert!(matches!(
      AclParser::apply_op(&mut u, "whatsthis"),
      Err(Error::UnknownOperation(_))
    ));
    assert!(matches!(
      AclParser::apply_op(&mut u, "+@nosuch"),
      Err(Error::CategoryDoesNotExist(_))
    ));
    // 键模式无操作
    AclParser::parse_rules(&mut u, &["~*", "allkeys", "resetkeys"]).unwrap();
  }

  #[test]
  fn describe_user_shape() {
    let mut u = User::new("bob".into());
    AclParser::parse_rules(&mut u, &["on", ">secret", "+get", "+set", "-set"]).unwrap();
    let secret_hash = AclPassword::from_string("secret").to_string();
    // 权限段可再解析：拒绝集后写胜出后 set 仅存在于拒绝侧
    assert_eq!(
      u.describe_user(),
      format!("user bob on #{secret_hash} +get -set")
    );

    let mut all = User::new("dave".into());
    AclParser::parse_rules(&mut all, &["on", "+@all", "-set"]).unwrap();
    assert_eq!(all.describe_user(), "user dave on +@all -set");

    let mut nopass = User::new("carl".into());
    AclParser::parse_rules(&mut nopass, &["nopass"]).unwrap();
    assert_eq!(nopass.describe_user(), "user carl off nopass");
  }

  #[test]
  fn duplicate_user_rejected() {
    let mut acl = AccessControlList::new();
    acl.add_user(User::new("bob".into())).unwrap();
    assert!(matches!(
      acl.add_user(User::new("bob".into())),
      Err(Error::UserAlreadyExists(_))
    ));
    // 原用户未被破坏
    assert!(acl.get_user("bob").unwrap().user().is_some());
    // 删除存在的用户
    assert!(acl.delete_user("bob").unwrap());
    assert!(!acl.delete_user("bob").unwrap());
  }

  #[test]
  fn user_handle_swap() {
    let mut u = User::new("a".into());
    u.is_enabled = true;
    let mut h = UserHandle::new(u);
    assert!(h.user().unwrap().is_enabled);

    let mut replacement = User::new("a".into());
    replacement.permissions.allow("get");
    assert!(h.try_set_user(replacement));
    assert!(h.user().unwrap().permissions.check_permission("get"));

    // 可变访问就地修改
    h.user_mut().unwrap().is_enabled = true;
    assert!(h.user().unwrap().is_enabled);
  }
}
