#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ACL 用户面单测（自 src/user.rs 内联测试迁出）：DescribeUser 输出格式、
//! 命令 / 分类 / 自定义命令访问决策、口令校验、bitcode 序列化往返与
//! 命名空间拆分
use std::sync::Arc;

use wacl::{AclParser, AclPassword, User, UserHandle, parse_user_namespace, validate_username};
use wresp::{catalog::RespAclCategories, command::RespCommand};

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

  let revoked = AclParser::parse_acl_rule("user sunsub-rev on nopass -@all +@pubsub -sunsubscribe")
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
