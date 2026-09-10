//! 访问控制列表（对标 libs/server/ACL/AccessControlList.cs）
//!
//! 用户表为 papaya 并发字典（gxhash 构建器）；整体换表（Load）经
//! `RwLock<Arc<..>>` 原子快照承接，读侧无锁；Save 以互斥锁串行化。

use std::{
  fs::File,
  io::{BufRead, BufReader, BufWriter, Write},
  path::Path,
  sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use whasher::{GxPapayaMap, new_papaya_map};

use super::{
  AclPassword, RespAclCategories, acl_exception::AclError, acl_parser::AclParser, user::User,
  user_handle::UserHandle,
};

/// default 用户名（对标 C# DefaultUserName）
pub const DEFAULT_USER_NAME: &str = "default";

/// 用户表类型（用户名 -> 用户句柄）
type UsersMap = GxPapayaMap<String, Arc<UserHandle>>;

/// 访问控制列表
pub struct AccessControlList {
  /// 全部已定义用户（整体换表原子性见 [`Self::load`]）
  users: RwLock<Arc<UsersMap>>,
  /// 当前默认用户句柄（快速默认查找）
  default_user: RwLock<Option<Arc<UserHandle>>>,
  /// Save 串行锁（对标 C# lock(this)）
  save_lock: Mutex<()>,
}

impl AccessControlList {
  /// 由可选 ACL 配置文件创建访问控制列表；未提供时仅创建默认用户
  ///
  /// 对标 C# 构造函数（defaultPassword / aclConfigurationFile）
  pub fn new(
    default_password: &str,
    acl_configuration_file: Option<&Path>,
  ) -> Result<Self, AclError> {
    let acl = Self::scratch();
    if let Some(file) = acl_configuration_file {
      // 尝试加载 ACL 配置文件
      acl.load(default_password, &file.display().to_string())?;
    } else {
      // 未定义 ACL 文件时仅创建默认用户
      let handle = acl.create_default_user_handle(default_password)?;
      *acl.default_user.write() = Some(handle);
    }
    Ok(acl)
  }

  /// 空表草稿（无默认用户；Load 的导入草稿与构造底座）
  fn scratch() -> Self {
    Self {
      users: RwLock::new(Arc::new(new_papaya_map())),
      default_user: RwLock::new(None),
      save_lock: Mutex::new(()),
    }
  }

  /// 按名取用户句柄
  ///
  /// libs/server/ACL/AccessControlList.cs:GetUserHandle
  pub fn get_user_handle(&self, username: &str) -> Option<Arc<UserHandle>> {
    let users = Arc::clone(&self.users.read());
    users.pin().get(username).map(Arc::clone)
  }

  /// 当前默认用户句柄
  ///
  /// libs/server/ACL/AccessControlList.cs:GetDefaultUserHandle
  pub fn get_default_user_handle(&self) -> Option<Arc<UserHandle>> {
    self.default_user.read().clone()
  }

  /// 加入用户（同名用户已存在即报错）
  ///
  /// libs/server/ACL/AccessControlList.cs:AddUserHandle
  pub fn add_user_handle(&self, user_handle: Arc<UserHandle>) -> Result<(), AclError> {
    let username = user_handle.user().name.clone();
    // 已有同名用户则不可加入
    if Arc::clone(&self.users.read())
      .pin()
      .try_insert(username.clone(), user_handle)
      .is_err()
    {
      return Err(AclError::UserAlreadyExists(username));
    }
    Ok(())
  }

  /// 按名删除用户（default 特殊用户不可删除）
  ///
  /// libs/server/ACL/AccessControlList.cs:DeleteUserHandle
  pub fn delete_user_handle(&self, username: &str) -> Result<bool, AclError> {
    if username == DEFAULT_USER_NAME {
      return Err(AclError::Acl(
        "The special 'default' user cannot be removed from the system".into(),
      ));
    }
    Ok(
      Arc::clone(&self.users.read())
        .pin()
        .remove(username)
        .is_some(),
    )
  }

  /// 清空全部用户
  ///
  /// libs/server/ACL/AccessControlList.cs:ClearUsers
  pub fn clear_users(&self) {
    *self.users.write() = Arc::new(new_papaya_map());
  }

  /// 全部用户名 / 句柄对快照
  ///
  /// libs/server/ACL/AccessControlList.cs:GetUserHandles
  pub fn get_user_handles(&self) -> Vec<(String, Arc<UserHandle>)> {
    let users = Arc::clone(&self.users.read());
    users
      .pin()
      .iter()
      .map(|(name, handle)| (name.clone(), Arc::clone(handle)))
      .collect()
  }

  /// 用户数
  pub fn len(&self) -> usize {
    Arc::clone(&self.users.read()).pin().len()
  }

  /// 是否无用户
  pub fn is_empty(&self) -> bool {
    Arc::clone(&self.users.read()).pin().is_empty()
  }

  /// 创建默认用户（已存在则直接返回既有句柄）
  ///
  /// libs/server/ACL/AccessControlList.cs:CreateDefaultUserHandle
  pub fn create_default_user_handle(
    &self,
    default_password: &str,
  ) -> Result<Arc<UserHandle>, AclError> {
    loop {
      if let Some(handle) = self.get_user_handle(DEFAULT_USER_NAME) {
        return Ok(handle);
      }
      // 默认用户始终全权
      let default_user = User::new(DEFAULT_USER_NAME.to_string());
      default_user.add_category(RespAclCategories::ALL)?;
      // 自动创建的默认用户始终启用
      default_user.set_enabled(true);
      // 按需设置口令，否则免密
      if !default_password.is_empty() {
        default_user.add_password_hash(AclPassword::from_string(default_password));
      } else {
        default_user.set_passwordless(true);
      }
      let default_user_handle = Arc::new(UserHandle::new(Arc::new(default_user)));
      // 加入用户表；并发竞争（同名已存在）时重取并发创建的用户
      match self.add_user_handle(Arc::clone(&default_user_handle)) {
        Ok(()) => return Ok(default_user_handle),
        Err(AclError::UserAlreadyExists(_)) => continue,
        Err(e) => return Err(e),
      }
    }
  }

  /// 加载 ACL 配置文件并整体替换当前规则；文件含错时旧规则保持不变
  ///
  /// libs/server/ACL/AccessControlList.cs:Load
  pub fn load(&self, default_password: &str, acl_configuration_file: &str) -> Result<(), AclError> {
    // 尝试加载 ACL 配置文件
    if !Path::new(acl_configuration_file).exists() {
      return Err(AclError::Acl(format!(
        "Cannot find ACL configuration file '{acl_configuration_file}'"
      )));
    }

    // 先导入临时访问控制列表保证原子性
    let scratch = Self::scratch();
    let reader = File::open(acl_configuration_file)
      .map(BufReader::new)
      .map_err(|_| {
        AclError::Acl(format!(
          "Unable to open ACL configuration file '{acl_configuration_file}'"
        ))
      })?;

    // 移除默认用户后导入（草稿表本就为空），逐行解析
    if let Err(exception) = scratch.import(reader, acl_configuration_file) {
      let AclError::Parsing {
        message,
        filename,
        line,
      } = exception
      else {
        return Err(exception);
      };
      return Err(AclError::parsing_wrap(&message, &filename, line));
    }

    // 补回默认用户并更新缓存的默认句柄
    let default_handle = scratch.create_default_user_handle(default_password)?;

    // 原子换表 + 换默认句柄
    *self.default_user.write() = Some(default_handle);
    *self.users.write() = scratch.users.into_inner();
    Ok(())
  }

  /// 保存当前全部用户规则到 ACL 配置文件
  ///
  /// libs/server/ACL/AccessControlList.cs:Save
  pub fn save(&self, acl_configuration_file: &str) -> Result<(), AclError> {
    if acl_configuration_file.is_empty() {
      return Err(AclError::Acl("ACL configuration file not set.".into()));
    }

    // 串行化：一次一落盘
    let _guard = self.save_lock.lock();
    let file = File::create(acl_configuration_file).map_err(|e| AclError::Acl(e.to_string()))?;
    let mut writer = BufWriter::with_capacity(1 << 16, file);
    for (_, user_handle) in self.get_user_handles() {
      writeln!(writer, "{}", user_handle.user().describe_user())
        .map_err(|e| AclError::Acl(e.to_string()))?;
    }
    writer.flush().map_err(|e| AclError::Acl(e.to_string()))?;
    Ok(())
  }

  /// 从输入逐行导入 ACL 规则
  ///
  /// libs/server/ACL/AccessControlList.cs:Import
  fn import<R: BufRead>(&self, input: R, configuration_file: &str) -> Result<(), AclError> {
    // 逐行读取并解析
    for (cur_line, line) in input.lines().enumerate() {
      let line = line.map_err(|e| AclError::Acl(e.to_string()))?;
      let line = line.trim();

      // 跳过空行与注释
      if line.is_empty() || line.starts_with('#') {
        continue;
      }

      // 解析该行 ACL 规则；异常携带文件与行号
      if let Err(exception) = AclParser::parse_acl_rule(line, Some(self)) {
        return Err(AclError::Parsing {
          message: exception.to_string(),
          filename: configuration_file.to_string(),
          line: cur_line as i32 + 1,
        });
      }
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::{fs, thread};

  use wresp::RespCommand;

  use super::*;

  /// 对标 garnet AclConfigurationFileTests.EmptyInput：空文件 → 仅 default 用户
  #[test]
  fn empty_input_file_yields_only_default() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    fs::write(&file, "").unwrap();

    let acl = AccessControlList::new("", Some(&file)).unwrap();
    let names: Vec<String> = acl.get_user_handles().into_iter().map(|(n, _)| n).collect();
    assert_eq!(names, vec!["default".to_string()]);
  }

  /// 对标 garnet AclConfigurationFileTests.NoDefaultRule：文件未定义 default 时自动补
  #[test]
  fn no_default_rule_creates_default() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    fs::write(
      &file,
      "user testA on >password123 +@admin\r\nuser testB on >passw0rd >password +@admin ",
    )
    .unwrap();

    let acl = AccessControlList::new("", Some(&file)).unwrap();
    assert_eq!(acl.len(), 3);
    let names: Vec<String> = acl.get_user_handles().into_iter().map(|(n, _)| n).collect();
    for expected in ["default", "testA", "testB"] {
      assert!(names.iter().any(|n| n == expected), "missing {expected}");
    }
    // 自动创建的 default：免密 + 全权 + 启用
    let default = acl.get_default_user_handle().unwrap();
    assert!(default.user().is_passwordless());
    assert!(default.user().can_access_command(RespCommand::Get));
  }

  /// 对标 garnet AclConfigurationFileTests.WithDefaultRule：文件定义的 default 优先，
  /// defaultPassword 兜底被忽略
  #[test]
  fn with_default_rule_takes_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    fs::write(
      &file,
      "user testA on >password123 +@admin +@slow\r\nuser testB on >passw0rd >password +@admin\r\nuser default on nopass +@admin +@slow",
    )
    .unwrap();

    let acl = AccessControlList::new("ignored-password", Some(&file)).unwrap();
    assert_eq!(acl.len(), 3);
    // 文件里的 default 免密，兜底口令未生效
    let described = acl
      .get_default_user_handle()
      .unwrap()
      .user()
      .describe_user();
    assert!(
      !described.contains('#'),
      "no password expected: {described}"
    );
    assert!(described.contains("nopass"));
  }

  /// 对标 garnet AclConfigurationFileTests.AclLoad：LOAD 整体替换表内容
  #[test]
  fn load_replaces_users_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    fs::write(
      &file,
      "user testA on >password123 +@admin +@slow\r\nuser testB on >passw0rd >password +@admin +@slow\r\nuser testC on >passw0rd\r\nuser default on nopass +@admin +@slow",
    )
    .unwrap();
    let acl = AccessControlList::new("", Some(&file)).unwrap();
    assert_eq!(acl.len(), 4);

    // 改写文件：删两用户、加一用户、default 保留
    fs::write(
      &file,
      "user testD on >password123\r\nuser testB on >passw0rd +@admin +@slow\r\nuser default on nopass +@admin",
    )
    .unwrap();
    acl.load("", &file.display().to_string()).unwrap();

    assert_eq!(acl.len(), 3);
    assert!(acl.get_user_handle("testA").is_none());
    assert!(acl.get_user_handle("testC").is_none());
    assert!(acl.get_user_handle("testD").is_some());
    assert!(acl.get_user_handle("testB").is_some());
    assert!(acl.get_default_user_handle().is_some());
  }

  /// 文件缺失 / 解析失败错误语义
  #[test]
  fn load_errors() {
    // 文件不存在
    let acl = AccessControlList::new("", None).unwrap();
    let err = acl.load("", "/nonexistent/users.acl").unwrap_err();
    assert!(
      err
        .to_string()
        .contains("Cannot find ACL configuration file")
    );

    // 解析失败：消息带文件与行号（注意 C# 冒号后两个空格）
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");
    fs::write(&file, "# 注释行\nuser alice on +@nosuch\n").unwrap();
    let err = match AccessControlList::new("", Some(&file)) {
      Err(e) => e,
      Ok(_) => panic!("expected parse failure"),
    };
    let msg = err.to_string();
    assert!(
      msg.starts_with("Unable to parse ACL rule") && msg.contains(":2:"),
      "{msg}"
    );
  }

  /// Save → Load 往返：DescribeUser 可再解析
  #[test]
  fn save_load_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("users.acl");

    let acl = AccessControlList::new("passw0rd", None).unwrap();
    AclParser::parse_acl_rule("user alice on >secret +@keyspace +set", Some(&acl)).unwrap();
    acl.save(&file.display().to_string()).unwrap();

    let restored = AccessControlList::new("", Some(&file)).unwrap();
    assert_eq!(restored.len(), 2);
    let alice = restored.get_user_handle("alice").unwrap().user();
    assert!(alice.is_enabled());
    assert!(alice.validate_password(&AclPassword::from_string("secret")));
    assert!(alice.can_access_command(RespCommand::Set));
    // default 有口令（构造入参）非免密
    assert!(
      restored
        .get_default_user_handle()
        .unwrap()
        .user()
        .validate_password(&AclPassword::from_string("passw0rd"))
    );

    // 空路径保存报错
    let err = acl.save("").unwrap_err();
    assert!(err.to_string().contains("ACL configuration file not set"));
  }

  /// 增删用户与 default 保护
  #[test]
  fn user_handle_crud() {
    let acl = AccessControlList::new("", None).unwrap();
    let handle = Arc::new(UserHandle::new(Arc::new(User::new("bob".into()))));
    acl.add_user_handle(Arc::clone(&handle)).unwrap();

    // 重名不可加入
    let dup = Arc::new(UserHandle::new(Arc::new(User::new("bob".into()))));
    assert!(matches!(
      acl.add_user_handle(dup),
      Err(AclError::UserAlreadyExists(u)) if u == "bob"
    ));

    // 删除存在的用户
    assert!(acl.delete_user_handle("bob").unwrap());
    assert!(!acl.delete_user_handle("bob").unwrap());

    // default 不可删
    let err = acl.delete_user_handle("default").unwrap_err();
    assert!(err.to_string().contains("cannot be removed"));

    // 清空
    acl.clear_users();
    assert!(acl.is_empty());
  }

  /// 并发创建 default 用户：竞争下仍收敛到单实例
  #[test]
  fn create_default_user_handle_converges() {
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    let handles: Vec<_> = (0..8)
      .map(|_| {
        let acl = Arc::clone(&acl);
        thread::spawn(move || acl.create_default_user_handle("").unwrap())
      })
      .collect();
    let joined: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let first = &joined[0];
    for h in &joined {
      assert!(Arc::ptr_eq(first, h));
    }
  }
}
