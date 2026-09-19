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
