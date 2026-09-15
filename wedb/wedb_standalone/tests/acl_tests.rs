use std::{fs, sync::Arc};

use parking_lot::Mutex;
use wacl::{
  AccessControlList, AclParser, GarnetAclAuthenticator, GarnetAclWithPasswordAuthenticator,
  IGarnetAuthenticator, auth::settings::acl_authentication_settings::AclAuthenticationSettings,
};
use wnode::resp::{
  acl_commands::AclCtx,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::{RespCommand, cmd_strings};

fn acl_settings(acl_file: Option<String>) -> AclAuthenticationSettings {
  AclAuthenticationSettings::new(acl_file, String::new())
}

/// 构造挂载 ACL 认证器的会话（对标 C# 构造函数 ACL 档装配）
fn acl_session(acl: &Arc<AccessControlList>) -> RespServerSession {
  let mut session = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  session.attach_acl(
    Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
      Arc::clone(acl),
    )))),
    None,
  );
  session
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
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

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
  let user = handle.user();

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
  let user = handle.user();

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
  let user = handle.user();

  assert!(user.can_access_command(RespCommand::Get));
  assert!(!user.can_access_command(RespCommand::ClientSetinfo));

  let mut session = RespServerSession::default();
  session.write_acl_permission_error(true);
  assert_eq!(
    session.output,
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

/// 会话级 ACL 门控端到端（对标 C# RespServerSession.cs:653 的
/// CheckACLPermissions 主循环门）：
/// PING → NOAUTH → AUTH badpass → WRONGPASS → AUTH ok → PONG；
/// ACL SETUSER default -ping 后同一会话句柄经 CAS 换新，PING → NOPERM
#[test]
fn session_level_acl_gating_end_to_end() {
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  // default 用户改为需口令（resetpass 关免密），构造期认证失败 → 会话未认证
  AclParser::parse_acl_rule("user default resetpass >pw +@all", Some(&acl)).unwrap();
  let mut session = acl_session(&acl);
  assert!(session.user_handle.is_none(), "构造期口令不符未认证");

  // 1. 未认证 PING → NOAUTH
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(
    session.take_output(),
    b"-NOAUTH Authentication required.\r\n"
  );

  // 2. AUTH badpass → WRONGPASS
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$7\r\nbadpass\r\n").is_some());
  assert_eq!(
    session.take_output(),
    format!("-{}\r\n", cmd_strings::RESP_WRONGPASS_INVALID_PASSWORD).as_bytes()
  );
  assert!(session.user_handle.is_none(), "认证失败不记录句柄");

  // 3. AUTH pw → +OK，句柄落位 default 用户
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n").is_some());
  assert_eq!(session.take_output(), b"+OK\r\n");
  assert_eq!(session.user_handle.as_deref(), Some("default"));

  // 4. 认证后 PING → +PONG（+@all 放行）
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(session.take_output(), b"+PONG\r\n");

  // 5. ACL SETUSER default -ping → +OK；会话持同一 UserHandle，
  //    SETUSER 经 TrySetUser CAS 换新即时生效
  assert!(
    feed(
      &mut session,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-ping\r\n"
    )
    .is_some()
  );
  assert_eq!(session.take_output(), b"+OK\r\n");

  // 6. 同会话 PING → NOPERM
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(
    session.take_output(),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

/// 未挂载认证器（NoAuth 档）的会话门控恒放行（对标 C# GetDefaultUserHandle
/// +@all 默认用户兜底）
#[test]
fn no_auth_session_allows_all_commands() {
  let mut session = RespServerSession::new(2, RespServerSessionOptions::default());
  // NoAuth 档构造即落到默认用户（C# GetDefaultUserHandle 兜底语义）
  assert_eq!(session.user_handle.as_deref(), Some("default"));
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(session.take_output(), b"+PONG\r\n");
  assert!(feed(&mut session, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").is_some());
  // 存储执行域未挂载时明确报错，证明命令已通过 ACL 门进入分派
  assert_eq!(
    session.take_output(),
    b"-ERR store execution domain not attached\r\n"
  );
}
