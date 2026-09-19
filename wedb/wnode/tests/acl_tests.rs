use std::{sync::Arc, thread};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wacl::{
  AccessControlList, AclParser, GarnetAclAuthenticator, GarnetAclWithPasswordAuthenticator,
  IGarnetAuthenticator, User,
};
use wdev::SegmentedDevice;
use wkv::{StoreSession, WedbStore};
use wnode::resp::{
  acl_commands::AclCtx,
  acl_store::AclStore,
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wresp::{cmd_strings, command::RespCommand};
use wtest_base::open_test_store;

/// 存储会话 + ACL 存储访问句柄（临时目录随结构体存活，Drop 清理）
struct TestAclStore {
  _dir: tempfile::TempDir,
  session: StoreSession<SegmentedDevice>,
}

impl TestAclStore {
  fn open(tag: &str) -> Self {
    let (dir, store): (_, Arc<WedbStore<SegmentedDevice>>) = open_test_store(tag).unwrap();
    let session = store.new_session().unwrap();
    Self { _dir: dir, session }
  }

  /// 绑定当前存储会话的 ACL 访问句柄（ACL 记录恒定落 db 0）
  fn acl(&self) -> AclStore<'_, SegmentedDevice> {
    AclStore::new(&self.session)
  }
}

/// 构造挂载 ACL 认证器 + 存储执行域的会话（对标 C# 构造函数 ACL 档装配）
fn acl_session(
  acl: &Arc<AccessControlList>,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespServerSession {
  let mut session = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  session.attach_acl(Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
    Arc::clone(acl),
  )))));
  session.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  session
}

fn ctx_for(
  auth: &GarnetAclWithPasswordAuthenticator,
  registered: Option<fn(&str) -> bool>,
) -> AclCtx<'_> {
  AclCtx {
    authenticator: Some(&auth.base),
    is_custom_command_registered: registered,
    caller_namespace: 0,
  }
}

/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicWhoamiTest
/// 命名用户规则落存储记录后经存储点查 AUTH（KeyTag::Acl 为唯一真源），WHOAMI 随认证切换
#[test]
fn basic_whoami_test() {
  let (_dir, store) = open_test_store("acl-whoami.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut session = acl_session(&acl, &store);

  // 初始以引导 default 免密认证
  assert_eq!(session.user_handle.as_deref(), Some("default"));
  assert!(feed(&mut session, b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"$7\r\ndefault\r\n");

  // Add testuser（SETUSER 落存储记录；C# 原版附 +@admin +@slow——WHOAMI 属
  // @slow 类普通门控命令（RespCommandsInfo.json ACL|WHOAMI AclCategories=Slow，
  // 非 IsNoAuth 豁免面），未授权用户跑 WHOAMI 即 NOPERM）
  assert!(
    feed(
      &mut session,
      b"*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\ntestuser\r\n$2\r\non\r\n$6\r\nnopass\r\n$7\r\n+@admin\r\n$6\r\n+@slow\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut session), b"+OK\r\n");

  // AUTH testuser x → 存储点查成功（nopass 任意口令可过）
  assert!(
    feed(
      &mut session,
      b"*3\r\n$4\r\nAUTH\r\n$8\r\ntestuser\r\n$1\r\nx\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut session), b"+OK\r\n");
  assert_eq!(session.user_handle.as_deref(), Some("testuser"));

  assert!(feed(&mut session, b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"$8\r\ntestuser\r\n");

  // Change users back to default：存储无 default 记录 → 回落引导句柄（仍免密）
  assert!(
    feed(
      &mut session,
      b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$1\r\nx\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut session), b"+OK\r\n");
  assert_eq!(session.user_handle.as_deref(), Some("default"));

  // wrong number of arguments
  assert!(
    feed(
      &mut session,
      b"*3\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n$1\r\nx\r\n"
    )
    .is_some()
  );
  assert_eq!(
    drain_output(&mut session),
    b"-ERR wrong number of arguments for 'acl|whoami' command\r\n"
  );
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicListTest
#[test]
fn basic_list_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list");
  let store = storage.acl();

  // default user（存储无记录 → 内存单例兜底）
  let mut out = Vec::new();
  session
    .network_acl_list(&ctx, &store, &[], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));

  // Add testuser
  let mut out = Vec::new();
  session
    .network_acl_set_user(&ctx, &store, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session
    .network_acl_list(&ctx, &store, &[], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*2\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
  assert!(frame.contains("user testuser off"));

  // Delete testuser
  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &store, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");

  let mut out = Vec::new();
  session
    .network_acl_list(&ctx, &store, &[], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicUsersTest
#[test]
fn basic_users_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-users");
  let store = storage.acl();

  let mut out = Vec::new();
  session
    .network_acl_users(&ctx, &store, &[], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$7\r\ndefault\r\n"));

  let mut out = Vec::new();
  session
    .network_acl_set_user(&ctx, &store, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session
    .network_acl_users(&ctx, &store, &[], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$8\r\ntestuser\r\n"));
  assert!(frame.contains("$7\r\ndefault\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicGenPassTest
#[test]
fn basic_gen_pass_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
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
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let mut session = RespServerSession::default();
  let storage = TestAclStore::open("acl-getuser");
  let store = storage.acl();

  // 规则经 SETUSER 落存储（存储为唯一真源），GETUSER 点查反序列化
  let mut out = Vec::new();
  session
    .network_acl_set_user(
      &ctx,
      &store,
      &[b"alice", b"on", b">passw0rd", b"+@admin"],
      &mut out,
    )
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &store, &[b"alice"], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n"));
  assert!(frame.contains("$9\r\npasswords\r\n*1\r\n$65\r\n#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9\r\n"));
  assert!(frame.contains("$8\r\ncommands\r\n$7\r\n+@admin\r\n"));

  // default user（存储无记录 → 内存单例兜底）
  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &store, &[b"default"], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n"));

  // HELLO 3：map 头 %3、flags 集合头 ~1（C# ACLCommands.cs NetworkAclGetUser →
  // RespServerSessionOutput.cs WriteMapLength / WriteSetLength 的 RESP3 臂），
  // passwords 数组与 commands bulk 版本无关不变
  session.resp_protocol_version = 3;
  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &store, &[b"alice"], &mut out)
    .unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("%3\r\n$5\r\nflags\r\n~1\r\n$2\r\non\r\n"));
  assert!(frame.contains("$9\r\npasswords\r\n*1\r\n$65\r\n#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9\r\n"));
  assert!(frame.contains("$8\r\ncommands\r\n$7\r\n+@admin\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/GetUserTests.cs:GetUserNotFoundTest
#[test]
fn get_user_not_found_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-getuser-missing");
  let store = storage.acl();

  let mut out = Vec::new();
  session
    .network_acl_get_user(&ctx, &store, &[b"missing"], &mut out)
    .unwrap();
  assert_eq!(out, b"$-1\r\n");
}

/// test/standalone/Garnet.test.acl/Resp/ACL/DeleteUserTests.cs:DeleteSingleUser
#[test]
fn delete_single_user() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-deluser");
  let store = storage.acl();

  let mut out = Vec::new();
  session
    .network_acl_set_user(
      &ctx,
      &store,
      &[b"testuser" as &[u8], b">passwd" as &[u8]],
      &mut out,
    )
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &store, &[b"testuser"], &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");
  // 墓碑删除后点查落空（存储为唯一真源）
  assert!(store.read(0, b"testuser").unwrap().is_none());

  // delete unknown user returns 0
  let mut out = Vec::new();
  session
    .network_acl_del_user(&ctx, &store, &[b"ghost"], &mut out)
    .unwrap();
  assert_eq!(out, b":0\r\n");
}

/// ACL LOAD 无外部文件可重载：回「不适用」错误帧，绝不回伪 +OK
#[test]
fn acl_load_is_not_applicable() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_load(&ctx, &[], &mut out).unwrap();
  assert_eq!(
    out,
    format!("-{}\r\n", cmd_strings::RESP_ERR_ACL_AUTH_FILE_DISABLED).as_bytes()
  );

  // 多余参数仍先行拒绝（早于文件面门）
  let mut out = Vec::new();
  session
    .network_acl_load(&ctx, &[b"extra"], &mut out)
    .unwrap();
  assert!(out.starts_with(b"-ERR wrong number of arguments"));
}

/// ACL SAVE 无事可做（写穿已由存储层承接）：回「不适用」错误帧，绝不回伪 +OK
#[test]
fn acl_save_is_not_applicable() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-save");
  let store = storage.acl();

  let mut out = Vec::new();
  session.network_acl_save(&ctx, &[], &mut out).unwrap();
  assert_eq!(
    out,
    format!("-{}\r\n", cmd_strings::RESP_ERR_ACL_AUTH_FILE_DISABLED).as_bytes()
  );

  // 持久化真实出口是存储写入面：SAVE 拒绝不影响 SETUSER 写穿落盘，
  // 点查可结构化还原规则（绕开文本解析器）
  let seeded = AclParser::parse_acl_rule("user saved on >pw +get").unwrap();
  store.write(0, b"saved", &seeded.to_bytes()).unwrap();
  let bytes = store.read(0, b"saved").unwrap().unwrap();
  assert!(!bytes.starts_with(b"user "));
  let saved = User::from_rule_bytes("saved", &bytes).unwrap();
  assert!(saved.can_access_command(RespCommand::Get));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:DeniedCommandReturnsNoPermAsync
#[test]
fn denied_command_returns_no_perm() {
  // 纯规则解析（写入面由存储承接），验证权限集合规则
  let user = AclParser::parse_acl_rule("user testuser on nopass +@all -type").unwrap();
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
  let user = AclParser::parse_acl_rule("user testuser on nopass +@all -type").unwrap();

  assert!(user.can_access_command(RespCommand::Set));
  assert!(user.can_access_command(RespCommand::Get));
  assert!(!user.can_access_command(RespCommand::Type));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:ClientSetInfoDeniedReturnsNoPermAsync
#[test]
fn client_set_info_denied_returns_no_perm() {
  let user = AclParser::parse_acl_rule("user testuser on nopass +@all -client|setinfo").unwrap();

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
  let (_dir, store) = open_test_store("acl-gating.db").unwrap();
  // default 用户构造期 requirepass 装配（免密关闭），构造期认证失败 → 会话未认证
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let mut session = acl_session(&acl, &store);
  assert!(session.user_handle.is_none(), "构造期口令不符未认证");

  // 1. 未认证 PING → NOAUTH
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(
    drain_output(&mut session),
    b"-NOAUTH Authentication required.\r\n"
  );

  // 2. AUTH badpass → WRONGPASS
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$7\r\nbadpass\r\n").is_some());
  assert_eq!(
    drain_output(&mut session),
    format!("-{}\r\n", cmd_strings::RESP_WRONGPASS_INVALID_PASSWORD).as_bytes()
  );
  assert!(session.user_handle.is_none(), "认证失败不记录句柄");

  // 3. AUTH pw → +OK，句柄落位 default 用户
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+OK\r\n");
  assert_eq!(session.user_handle.as_deref(), Some("default"));

  // 4. 认证后 PING → +PONG（+@all 放行）
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+PONG\r\n");

  // 5. ACL SETUSER default -ping → +OK；会话持同一 UserHandle，
  //    SETUSER 经 TrySetUser CAS 换新即时生效
  assert!(
    feed(
      &mut session,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-ping\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut session), b"+OK\r\n");

  // 6. 同会话 PING → NOPERM
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(
    drain_output(&mut session),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

/// 未挂载认证器（免认证形态）的会话门控恒放行（对标 C# GetDefaultUserHandle
/// +@all 默认用户兜底）
#[test]
fn no_auth_session_allows_all_commands() {
  let mut session = RespServerSession::new(2, RespServerSessionOptions::default());
  // 免认证形态构造即落到默认用户（C# GetDefaultUserHandle 兜底语义）
  assert_eq!(session.user_handle.as_deref(), Some("default"));
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+PONG\r\n");
  assert!(feed(&mut session, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").is_some());
  // 存储执行域未挂载时明确报错，证明命令已通过 ACL 门进入分派
  assert_eq!(
    drain_output(&mut session),
    b"-ERR store execution domain not attached\r\n"
  );
}

/// test/standalone/Garnet.test.acl/Resp/ACL/RespCommandTests.cs:AclCatACLsAsync
/// ACL CAT：返回全部分类名（25 类，与 C# ACLParser.categoryNames 一一对应）
#[test]
fn acl_cat_lists_all_categories() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  assert!(auth.authenticate(b"x", b""));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_cat(&ctx, &[], &mut out).unwrap();
  assert!(
    out.starts_with(b"*25\r\n"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
  // 数据面分类齐备（含 C# 用例关注的基本类型族）
  for category in [
    &b"admin"[..],
    b"bitmap",
    b"connection",
    b"hash",
    b"list",
    b"pubsub",
    b"read",
    b"scripting",
    b"set",
    b"sortedset",
    b"stream",
    b"string",
    b"transaction",
    b"write",
    b"all",
  ] {
    let framed = format!(
      "${}\r\n{}\r\n",
      category.len(),
      String::from_utf8_lossy(category)
    );
    assert!(
      out.windows(framed.len()).any(|w| w == framed.as_bytes()),
      "缺分类 {category:?}：got {}",
      String::from_utf8_lossy(&out)
    );
  }
}

/// C# ACLCommands.cs:NetworkAclCat 首段：ACL CAT 携带附加参数 → 字面错误文案
///（Garnet 不支持 ACL CAT &lt;category&gt;，分类命令清单未实现）
#[test]
fn acl_cat_with_argument_rejected() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  assert!(auth.authenticate(b"x", b""));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session
    .network_acl_cat(&ctx, &[b"string"], &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Unknown subcommand or wrong number of arguments for ACL CAT.\r\n"
  );
}

/// 默认用户（nopass default 构造即认证）经会话分派全链路 ACL CAT：
/// 默认用户 +@all → 放行并回分类数组（C# AclCatACLsAsync 的 default 管理员连接形态）
#[test]
fn acl_cat_via_session_dispatch_for_default_user() {
  let (_dir, store) = open_test_store("acl-cat-dispatch.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut session = RespServerSession::new(3, RespServerSessionOptions::default());
  session.attach_acl(Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
    Arc::clone(&acl),
  )))));
  session.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  assert!(feed(&mut session, b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n").is_some());
  let out = drain_output(&mut session);
  assert!(
    out.starts_with(b"*25\r\n"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
  let framed = b"$6\r\nstring\r\n";
  assert!(
    out.windows(framed.len()).any(|w| w == framed),
    "缺 string 分类：got {}",
    String::from_utf8_lossy(&out)
  );
}

/// 快照框径探针：解析 RESP2 bulk-string 数组框，返回（声明数组长度,
/// 元素正文清单）；框体与声明脱拍即断言失败（元素数不符 / 框头残缺即 panic）
fn parse_bulk_array(frame: &[u8]) -> (usize, Vec<Vec<u8>>) {
  /// 读取一行至 CRLF，返回行正文与下一行起点
  fn read_line(frame: &[u8], pos: usize) -> (&[u8], usize) {
    let end = frame[pos..]
      .windows(2)
      .position(|w| w == b"\r\n")
      .expect("行尾 CRLF 缺失")
      + pos;
    (&frame[pos..end], end + 2)
  }

  assert_eq!(frame.first(), Some(&b'*'), "非数组框");
  let (head, mut pos) = read_line(frame, 1);
  let declared: usize = String::from_utf8_lossy(head).parse().expect("数组长度非法");
  let mut items = Vec::new();
  while pos < frame.len() {
    assert_eq!(frame.get(pos..pos + 1), Some(&b"$"[..]), "元素框头非法");
    let (len_str, body) = read_line(frame, pos + 1);
    let len: usize = String::from_utf8_lossy(len_str)
      .parse()
      .expect("元素长度非法");
    assert_eq!(
      frame.get(body + len..body + len + 2),
      Some(&b"\r\n"[..]),
      "元素正文尾 CRLF 缺失"
    );
    items.push(frame[body..body + len].to_vec());
    pos = body + len + 2;
  }
  (declared, items)
}

/// ACL LIST / ACL USERS 单遍快照整框直出（对标 ACLCommands.cs:NetworkAclList /
/// NetworkAclUsers）：声明的数组长度与实际写出元素数一致（多位数符合成路径），
/// default 兜底仍居首，多用户正文逐条直出
#[test]
fn acl_list_and_users_stream_consistent_frames() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list-streaming");
  let store = storage.acl();

  // 两位数组长度：符头与正文同取自一份快照，二者之间不容纳占位偏差
  let users: Vec<String> = (0..12).map(|i| format!("user-{i}")).collect();
  for user in &users {
    let mut out = Vec::new();
    session
      .network_acl_set_user(&ctx, &store, &[user.as_bytes()], &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  }

  // LIST：12 条存储记录 + 引导态 default 兜底（存储无 default 记录）
  let mut out = Vec::new();
  session
    .network_acl_list(&ctx, &store, &[], &mut out)
    .unwrap();
  let (declared, items) = parse_bulk_array(&out);
  assert_eq!(declared, items.len(), "LIST 数组长度与写出元素数不一致");
  assert_eq!(declared, 13, "got {}", String::from_utf8_lossy(&out));
  assert!(
    String::from_utf8_lossy(&items[0]).starts_with("user default "),
    "default 兜底位次回归: {:?}",
    String::from_utf8_lossy(&items[0])
  );
  for user in &users {
    let head = format!("user {user} ");
    assert!(
      items[1..].iter().any(|i| i.starts_with(head.as_bytes())),
      "缺 {user} 规则正文"
    );
  }

  // USERS：同框径口径，元素为用户名
  let mut out = Vec::new();
  session
    .network_acl_users(&ctx, &store, &[], &mut out)
    .unwrap();
  let (declared, items) = parse_bulk_array(&out);
  assert_eq!(declared, items.len(), "USERS 数组长度与写出元素数不一致");
  assert_eq!(declared, 13, "got {}", String::from_utf8_lossy(&out));
  assert_eq!(items[0], b"default", "default 兜底位次回归");
  for user in &users {
    assert!(
      items[1..].iter().any(|i| i == user.as_bytes()),
      "缺用户名 {user}"
    );
  }
}

/// 不可解码记录在首遍即失败关闭：应答只回一条错误，不留半截数组框；
/// USERS 遍不触碰规则正文，同存储态下仍完整出框
#[test]
fn acl_list_undecodable_record_fails_before_opening_frame() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list-corrupt-record");
  let store = storage.acl();
  store
    .write(0, b"broken", b"not-a-bitcode-user-record")
    .unwrap();

  let mut out = Vec::new();
  session
    .network_acl_list(&ctx, &store, &[], &mut out)
    .unwrap();
  assert_eq!(
    out,
    format!("-{}\r\n", cmd_strings::RESP_ERR_GENERIC_UNK_CMD).into_bytes()
  );

  let mut out = Vec::new();
  session
    .network_acl_users(&ctx, &store, &[], &mut out)
    .unwrap();
  let (declared, items) = parse_bulk_array(&out);
  assert_eq!(declared, items.len(), "USERS 数组长度与写出元素数不一致");
  assert_eq!(declared, 2, "got {}", String::from_utf8_lossy(&out));
  assert_eq!(items[0], b"default");
  assert_eq!(items[1], b"broken");
}

/// 并发增删下快照口径回归：LIST / USERS 的数组符头与实际写出元素数恒一致
///
/// 旧实现「首遍计数定符头、次遍重扫写正文」把背离窗口摊成**两次全日志扫描**
/// （日志越大窗口越宽），其间本命名空间的 SETUSER/DELUSER 即令客户端按符头取数
/// 少读或多读；新实现单遍扫描收快照、符头与正文同源，窗口归零。
#[test]
fn acl_list_and_users_snapshot_frame_consistent_under_concurrent_mutation() {
  let (_dir, store): (_, Arc<WedbStore<SegmentedDevice>>) =
    open_test_store("acl-list-snapshot-race").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclWithPasswordAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let reader_session = store.new_session().unwrap();
  let reader = AclStore::new(&reader_session);

  // 写侧：另一会话线程不断增删同批命名用户，令读侧两半之间始终处于可背离态
  let writer_store = Arc::clone(&store);
  let writer = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
      let writer_session = writer_store.new_session().unwrap();
      let store = AclStore::new(&writer_session);
      for round in 0..240u32 {
        let name = format!("racer-{}", round % 6);
        let user = User::new(name.clone());
        let _ = store.write(0, name.as_bytes(), &user.to_bytes());
        let _ = store.delete(0, name.as_bytes());
      }
    });
  });

  // 读侧：每一框都须自洽（符头 == 元素数，且逐元素框体完整），任一次脱拍即断言
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for _ in 0..160 {
      let mut out = Vec::new();
      session
        .network_acl_list(&ctx, &reader, &[], &mut out)
        .unwrap();
      let (declared, items) = parse_bulk_array(&out);
      assert_eq!(
        declared,
        items.len(),
        "LIST 符头与元素数背离（按符头取数即截断/残留）: {}",
        String::from_utf8_lossy(&out)
      );
      assert!(
        items[0].starts_with(b"user default "),
        "default 兜底位次回归: {}",
        String::from_utf8_lossy(&out)
      );

      let mut out = Vec::new();
      session
        .network_acl_users(&ctx, &reader, &[], &mut out)
        .unwrap();
      let (declared, items) = parse_bulk_array(&out);
      assert_eq!(
        declared,
        items.len(),
        "USERS 符头与元素数背离（按符头取数即截断/残留）: {}",
        String::from_utf8_lossy(&out)
      );
      assert_eq!(items[0], b"default", "default 兜底位次回归");
    }
  });

  writer.join().unwrap();
}
