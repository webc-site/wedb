//! 在 garnet 中的相对路径: test/standalone/Garnet.test.acl/Resp/ACL/AclParserTests.cs + SetUserTests.cs（ACL 规则解析）
use std::sync::Arc;

use wacl::{AccessControlList, AclPassword, access_control_list::DEFAULT_USER_NAME};
use wresp::command::RespCommand;

/// 引导 default 用户：nopass 装配恒 +@all 且启用（空 defaultPassword 分支）
#[test]
fn default_user_nopass_full_access() {
  let acl = AccessControlList::new("").unwrap();
  let default = acl.get_default_user_handle();
  let user = default.user();
  assert_eq!(user.name, DEFAULT_USER_NAME);
  assert!(user.is_enabled());
  assert!(user.is_passwordless());
  // +@all：任一数据面命令放行，免密下任意口令可过
  assert!(user.can_access_command(RespCommand::Get));
  assert!(user.can_access_command(RespCommand::Set));
  assert!(user.validate_password(&AclPassword::from_string("anything")));
  assert!(user.describe_user().contains("nopass"));
}

/// 引导 default 用户：requirepass 口令哈希装配（带 defaultPassword 分支）
#[test]
fn default_user_with_requirepass_hash() {
  let acl = AccessControlList::new("passw0rd").unwrap();
  let default = acl.get_default_user_handle();
  let user = default.user();
  assert_eq!(user.name, DEFAULT_USER_NAME);
  assert!(user.is_enabled());
  assert!(!user.is_passwordless());
  assert!(user.validate_password(&AclPassword::from_string("passw0rd")));
  assert!(!user.validate_password(&AclPassword::from_string("wrong")));
  assert!(user.can_access_command(RespCommand::Get));
}

/// 免认证形态共享的 nopass default 句柄单例（NoAuth 档 GetDefaultUserHandle
/// 兜底的等价承接）：规格与 CreateDefaultUserHandle 同款（+@all / 启用 /
/// 免密），且两次取用为同一实例（全会话共享同一 Arc，对标 C#
/// storeWrapper 单一 ACL 实例）
#[test]
fn nopass_default_handle_singleton_full_access() {
  let first = AccessControlList::nopass_default_handle();
  let second = AccessControlList::nopass_default_handle();
  assert!(Arc::ptr_eq(&first, &second), "进程级单例须为同一实例");
  let user = first.user();
  assert_eq!(user.name, DEFAULT_USER_NAME);
  assert!(user.is_enabled());
  assert!(user.is_passwordless());
  assert!(user.can_access_command(RespCommand::Get));
  assert!(user.can_access_command(RespCommand::Set));
}
