use std::{fs, sync::Arc};

use wacl::{
  AccessControlList, AclParser, GarnetAclAuthenticator, GarnetAclWithPasswordAuthenticator,
  IGarnetAuthenticator,
  auth::settings::acl_authentication_settings::AclAuthenticationSettings,
};
use wnode::resp::{acl_commands::AclCtx, resp_server_session::RespServerSession};
use wresp::{RespCommand, cmd_strings};

fn acl_settings(acl_file: Option<String>) -> AclAuthenticationSettings {
  AclAuthenticationSettings::new(acl_file, String::new())
}

fn ctx_for<'a>(
  auth: &'a GarnetAclWithPasswordAuthenticator,
  settings: &'a AclAuthenticationSettings,
  registered: Option<fn(&str) -> bool>,
) -> AclCtx<'a> {
  AclCtx {
    authenticator: Some(&auth.base),
    acl_settings: Some(settings),
    is_custom_command_registered: registered,
  }
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicWhoamiTest
#[test]
fn basic_whoami_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let session = RespServerSession::default();

  assert!(auth.authenticate(b"x", b""));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let mut out = Vec::new();
  session.network_acl_who_am_i(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"$7\r\ndefault\r\n");

  AclParser::parse_acl_rule("user testuser on nopass", Some(&acl)).unwrap();
  assert!(auth.authenticate(b"x", b"testuser"));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let mut out = Vec::new();
  session.network_acl_who_am_i(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"$8\r\ntestuser\r\n");

  // wrong number of arguments
  let mut out = Vec::new();
  session
    .network_acl_who_am_i(&ctx, &[b"x"], &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'acl|whoami' command\r\n"
  );
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicListTest
#[test]
fn basic_list_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  // default user
  let mut out = Vec::new();
  session.network_acl_list(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));

  // Add testuser
  let mut out = Vec::new();
  session
    .network_acl_set_user(&ctx, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session.network_acl_list(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*2\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
  assert!(frame.contains("user testuser off"));

  // Delete testuser
  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");

  let mut out = Vec::new();
  session.network_acl_list(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicUsersTest
#[test]
fn basic_users_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_users(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$7\r\ndefault\r\n"));

  let mut out = Vec::new();
  session
    .network_acl_set_user(&ctx, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session.network_acl_users(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$8\r\ntestuser\r\n"));
  assert!(frame.contains("$7\r\ndefault\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicGenPassTest
#[test]
fn basic_gen_pass_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  // default 64 hex chars
  let mut out = Vec::new();
  session.network_acl_gen_pass(&ctx, &[], &mut out).unwrap();
  let frame = String::from_utf8(out).unwrap();
  let (_, tail) = frame.split_once("\r\n").unwrap();
  let hex: &str = &tail[..64];
  assert!(
    hex
      .chars()
      .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
  );

  // 5 bits -> 2 chars
  let mut out = Vec::new();
  session
    .network_acl_gen_pass(&ctx, &[b"5"], &mut out)
    .unwrap();
  assert!(out.starts_with(b"$2\r\n"));

  // non-integer
  let mut out = Vec::new();
  session
    .network_acl_gen_pass(&ctx, &[b"abcd"], &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

  // out of range (0 / 4097)
  for bad in [b"0".as_slice(), b"4097".as_slice()] {
    let mut out = Vec::new();
    session
      .network_acl_gen_pass(&ctx, &[bad], &mut out)
      .unwrap();
    assert_eq!(
      out,
      &b"-ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096\r\n"[..]
    );
  }
}

/// test/standalone/Garnet.test.acl/Resp/ACL/GetUserTests.cs:GetUserTest
#[test]
fn get_user_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user alice on >passw0rd +@admin", Some(&acl)).unwrap();
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &[b"alice"], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n"));
  assert!(frame.contains("$9\r\npasswords\r\n*1\r\n$65\r\n#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9\r\n"));
  assert!(frame.contains("$8\r\ncommands\r\n$7\r\n+@admin\r\n"));

  // default user
  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &[b"default"], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/GetUserTests.cs:GetUserNotFoundTest
#[test]
fn get_user_not_found_test() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &[b"missing"], &mut out)
    .unwrap();
  assert_eq!(out, b"$-1\r\n");
}

/// test/standalone/Garnet.test.acl/Resp/ACL/DeleteUserTests.cs:DeleteSingleUser
#[test]
fn delete_single_user() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(None);
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session
    .network_acl_set_user(&ctx, &[b"testuser" as &[u8], b">passwd" as &[u8]], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");
  assert!(acl.get_user_handle("testuser").is_none());

  // delete unknown user returns 0
  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &[b"ghost"], &mut out)
    .unwrap();
  assert_eq!(out, b":0\r\n");
}

/// test/standalone/Garnet.test.acl/Resp/ACL/AclConfigurationFileTests.cs:AclLoad
#[test]
fn acl_load() {
  let dir = tempfile::tempdir().unwrap();
  let file = dir.path().join("users.acl");
  fs::write(&file, "user default on nopass +@all\n").unwrap();

  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(Some(file.display().to_string()));
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_load(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");
}

/// test/standalone/Garnet.test.acl/Resp/ACL/AclConfigurationFileTests.cs:AclSave
#[test]
fn acl_save() {
  let dir = tempfile::tempdir().unwrap();
  let file = dir.path().join("users.acl");
  fs::write(&file, "user default on nopass +@all\n").unwrap();

  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let settings = acl_settings(Some(file.display().to_string()));
  let ctx = ctx_for(&auth, &settings, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_load(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  AclParser::parse_acl_rule("user saved on >pw +get", Some(&acl)).unwrap();
  let mut out = Vec::new();
  session.network_acl_save(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let content = fs::read_to_string(&file).unwrap();
  assert!(content.contains("user saved on"));
  assert!(content.contains("user default on nopass +@all"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:DeniedCommandReturnsNoPermAsync
#[test]
fn denied_command_returns_no_perm() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user testuser on nopass +@all -type", Some(&acl)).unwrap();
  let handle = acl.get_user_handle("testuser").unwrap();
  let user = handle.read().clone();

  // 验证权限集合规则
  assert!(user.can_access_command(RespCommand::Get));
  assert!(!user.can_access_command(RespCommand::Type));

  // 验证会话在已认证但无命令权限时写出 NOPERM
  let mut session = RespServerSession::default();
  session.write_acl_permission_error(true);
  assert_eq!(
    session.output,
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  // 验证未认证时写出 NOAUTH
  let mut session_unauth = RespServerSession::default();
  session_unauth.write_acl_permission_error(false);
  assert_eq!(
    session_unauth.output,
    b"-NOAUTH Authentication required.\r\n"
  );

  // 验证 cmd_strings 集中定义的常量
  let mut raw_out = Vec::new();
  cmd_strings::write_error_raw(&mut raw_out, cmd_strings::RESP_ERR_NOPERM);
  assert_eq!(
    raw_out,
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:PermittedCommandStillWorksAsync
#[test]
fn permitted_command_still_works() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user testuser on nopass +@all -type", Some(&acl)).unwrap();
  let handle = acl.get_user_handle("testuser").unwrap();
  let user = handle.read().clone();

  assert!(user.can_access_command(RespCommand::Set));
  assert!(user.can_access_command(RespCommand::Get));
  assert!(!user.can_access_command(RespCommand::Type));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:ClientSetInfoDeniedReturnsNoPermAsync
#[test]
fn client_set_info_denied_returns_no_perm() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user testuser on nopass +@all -client|setinfo", Some(&acl)).unwrap();
  let handle = acl.get_user_handle("testuser").unwrap();
  let user = handle.read().clone();

  assert!(user.can_access_command(RespCommand::Get));
  assert!(!user.can_access_command(RespCommand::ClientSetinfo));

  let mut session = RespServerSession::default();
  session.write_acl_permission_error(true);
  assert_eq!(
    session.output,
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

/// 会话级 ACL 门控全链路（对标 test/standalone/Garnet.test.acl/Resp/ACL/
/// SetUserTests.cs:ProtectedDefaultUserErrorHandlingTest 与 Resp/
/// GarnetAuthenticatorTests.cs 的 PING → NOAUTH → AUTH → PING 序列）
#[test]
fn session_auth_gates_commands_until_authenticated() {
  // ACL 档：default 用户带口令（C# useAcl + defaultPassword 装配）
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user default on >pwd +@all", Some(&acl)).unwrap();
  let mut session = RespServerSession::default();
  session.attach_acl(
    Some(Arc::new(parking_lot::Mutex::new(GarnetAclAuthenticator::new(Arc::clone(
      &acl,
    ))))),
    None,
  );

  // 未认证 ACL LIST → NOAUTH（ProtectedDefaultUserErrorHandlingTest）
  assert_eq!(
    session.try_consume_messages(b"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n"),
    Some(23)
  );
  assert_eq!(session.take_output(), b"-NOAUTH Authentication required.\r\n");

  // 未认证 PING → NOAUTH
  assert_eq!(
    session.try_consume_messages(b"*1\r\n$4\r\nPING\r\n"),
    Some(14)
  );
  assert_eq!(session.take_output(), b"-NOAUTH Authentication required.\r\n");

  // AUTH default pwd → +OK
  assert_eq!(
    session.try_consume_messages(b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$3\r\npwd\r\n"),
    Some(36)
  );
  assert_eq!(session.take_output(), b"+OK\r\n");

  // 认证后 PING → +PONG
  assert_eq!(
    session.try_consume_messages(b"*1\r\n$4\r\nPING\r\n"),
    Some(14)
  );
  assert_eq!(session.take_output(), b"+PONG\r\n");

  // default 仍持 nopass 标志（C# ACLParser：>pwd 只加哈希不清免密）→ 任意口令通过
  assert_eq!(
    session.try_consume_messages(b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$1\r\nx\r\n"),
    Some(34)
  );
  assert_eq!(session.take_output(), b"+OK\r\n");

  // 未知用户 → WRONGPASS 用户名口令组合变体（BasicCommands.NetworkAUTH）
  assert_eq!(
    session.try_consume_messages(b"*3\r\n$4\r\nAUTH\r\n$6\r\nnobody\r\n$1\r\nx\r\n"),
    Some(33)
  );
  assert_eq!(
    session.take_output(),
    b"-WRONGPASS Invalid username/password combination\r\n"
  );
}

/// SETUSER 经会话主循环改权后即时生效（句柄 CAS 换新，会话无感）
#[test]
fn session_setuser_revokes_permission_live() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user default on nopass +@all", Some(&acl)).unwrap();
  let mut session = RespServerSession::default();
  session.attach_acl(
    Some(Arc::new(parking_lot::Mutex::new(GarnetAclAuthenticator::new(Arc::clone(
      &acl,
    ))))),
    None,
  );

  // 认证（nopass 空口令）
  assert_eq!(
    session.try_consume_messages(b"*2\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n"),
    Some(27)
  );
  assert_eq!(session.take_output(), b"+OK\r\n");
  assert_eq!(
    session.try_consume_messages(b"*1\r\n$4\r\nPING\r\n"),
    Some(14)
  );
  assert_eq!(session.take_output(), b"+PONG\r\n");

  // ACL SETUSER default -ping → +OK（bulk 串 "-ping" 需 $5 长度前缀）
  assert_eq!(
    session.try_consume_messages(
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-ping\r\n"
    ),
    Some(50)
  );
  assert_eq!(session.take_output(), b"+OK\r\n");

  // 权限撤销即时生效（同一会话持旧句柄实例，读到的已是换新后的用户）
  assert_eq!(
    session.try_consume_messages(b"*1\r\n$4\r\nPING\r\n"),
    Some(14)
  );
  assert_eq!(
    session.take_output(),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}
