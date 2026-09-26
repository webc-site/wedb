use std::{
  future::Future,
  str::from_utf8,
  sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use wacl::{
  AccessControlList, AclError, AclParser, AclPassword, GarnetAclAuthenticator, User, UserHandle,
  acl_password_check,
};
use wdev::SegmentedDevice;
use wkv::{StoreSession, WedbStore};
use wnode::resp::{
  acl_commands::{AclAuthOutcome, AclCtx, AclGateVerdict, RESP_ERR_ACL_STORE_SCAN_FAILED},
  acl_store::AclStore,
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{drain_output, err_frame};
use wresp::{
  catalog::{
    RespAclCategories, commands_for_category, expand_for_acls, is_no_auth,
    try_get_resp_command_info,
  },
  cmd_strings,
  command::RespCommand,
};
use wtest_base::{open_test_store, resp_frame};
use wval::{KeyTag, NamespaceDbCodec};

/// 同步测试壳内闭环 async ACL 存储访问链（全链 async 化的测试对位）
fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

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
  session.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
  session.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  session
}

fn ctx_for(auth: &GarnetAclAuthenticator, registered: Option<fn(&str) -> bool>) -> AclCtx<'_> {
  AclCtx {
    acl: Some(auth.get_access_control_list()),
    is_custom_command_registered: registered,
    caller_namespace: 0,
  }
}

/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
///
/// 网络泵替身（与 drive_loop 逐位同构）：消费轮应答冲出 → 内联 await 驱动
/// 停车臂（ACL 挂载刷新重驱重评 / AUTH·HELLO·ACL 族产应答闭环 / 慢路径
/// 挂起并回）→ 续消费流水线余量；应答字节最后并回会话输出缓冲供断言
///（对位泵池化响应块随本轮实写）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let mut resp_buf = Vec::new();
  let mut remaining = s.try_consume_messages();
  s.take_output_into(&mut resp_buf);
  Runtime::new().unwrap().block_on(async {
    loop {
      // 重驱型刷新臂：点查后重入消费，门链以新挂载重评
      if s.take_pending_acl_refresh() {
        if let Some(api) = s.garnet_api.clone() {
          api.exec_acl_refresh(s).await;
        }
        remaining = s.try_consume_messages().or(remaining);
        s.take_output_into(&mut resp_buf);
        continue;
      }
      // 产应答型异步臂：认证/规则读写回写会话本地态，应答冲出后续消费
      if let Some((cmd, args, parked_output_len)) = s.take_pending_auth_acl() {
        let views: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        if let Some(api) = s.garnet_api.clone() {
          let _ = api.exec_auth_acl(s, cmd, &views).await;
        }
        s.account_parked_auth_acl_failure(cmd, parked_output_len);
        s.take_output_into(&mut resp_buf);
        remaining = s.try_consume_messages().or(remaining);
        s.take_output_into(&mut resp_buf);
        continue;
      }
      // 慢路径挂起（如 AUTH 冷上下文装载）：await 闭环，应答按流水线并回
      if let Some(slow) = s.take_slow_wait() {
        let reply = slow.resolve().await;
        s.resolve_slow_wait_into(&reply, &mut resp_buf);
        continue;
      }
      break;
    }
  });
  s.output.extend_from_slice(&resp_buf);
  remaining
}
/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicWhoamiTest
/// 命名用户规则落存储记录后经存储点查 AUTH（KeyTag::Acl 为唯一真源），WHOAMI 随认证切换
#[test]
fn basic_whoami_test() {
  let (_dir, store) = open_test_store("acl-whoami.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut session = acl_session(&acl, &store);

  // 初始以引导 default 免密认证
  assert_eq!(session.user_name(), Some("default"));
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
  assert_eq!(session.user_name(), Some("testuser"));

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
  assert_eq!(session.user_name(), Some("default"));

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
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list");
  let store = storage.acl();

  // default user（存储无记录 → 内存单例兜底）
  let mut out = Vec::new();
  block_on(session.network_acl_list(&ctx, &store, &[], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));

  // Add testuser
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(&ctx, &store, &[b"testuser"], &mut out)).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_list(&ctx, &store, &[], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*2\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
  assert!(frame.contains("user testuser off"));

  // Delete testuser
  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx, &store, &[b"testuser"], &mut out)).unwrap();
  assert_eq!(out, b":1\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_list(&ctx, &store, &[], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*1\r\n"));
  assert!(frame.contains("user default on nopass +@all"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicUsersTest
#[test]
fn basic_users_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-users");
  let store = storage.acl();

  let mut out = Vec::new();
  block_on(session.network_acl_users(&ctx, &store, &[], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$7\r\ndefault\r\n"));

  let mut out = Vec::new();
  block_on(session.network_acl_set_user(&ctx, &store, &[b"testuser"], &mut out)).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_users(&ctx, &store, &[], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("$8\r\ntestuser\r\n"));
  assert!(frame.contains("$7\r\ndefault\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:BasicGenPassTest
#[test]
fn basic_gen_pass_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
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

/// 从 `$<len>\r\n<body>\r\n` bulk string 框中取出 body（并校验框形态与声明长度一致）
fn bulk_body(out: &[u8]) -> Vec<u8> {
  let text = from_utf8(out).unwrap();
  let (header, rest) = text.split_once("\r\n").unwrap();
  let declared: usize = header
    .strip_prefix('$')
    .unwrap_or_else(|| panic!("非 bulk string 应答: {out:?}"))
    .parse()
    .unwrap();
  assert_eq!(
    rest.len(),
    declared + 2,
    "bulk string 尾部框架不符: {out:?}"
  );
  assert!(rest[declared..].starts_with("\r\n"));
  rest.as_bytes()[..declared].to_vec()
}

/// ACL GENPASS 熵源必须是 OS CSPRNG，不得取自本仓共享 fastrand 流
/// （对标 C# `ACLCommands.cs:429` 的 `RandomNumberGenerator.GetHexString(length, true)`）
#[test]
fn gen_pass_uses_os_entropy_not_fastrand() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();

  let gen_pass = |args: &[&[u8]]| {
    let mut out = Vec::new();
    session.network_acl_gen_pass(&ctx, args, &mut out).unwrap();
    bulk_body(&out)
  };

  // 1) 两次调用输出不同
  let first = gen_pass(&[]);
  let second = gen_pass(&[]);
  assert_ne!(first, second, "GENPASS 两次输出相同");

  // 2) 反向注入哨兵：固定全局 fastrand 种子两次重放——若口令仍逐位取自
  //    fastrand 流（修复前形态），两次输出必然逐字节相同；OS CSPRNG 下必不同
  let replay = || {
    fastrand::seed(0x5EED_BEEF);
    let a = gen_pass(&[]);
    fastrand::seed(0x5EED_BEEF);
    let b = gen_pass(&[]);
    (a, b)
  };
  let (a, b) = replay();
  assert_ne!(
    a, b,
    "重放 fastrand 全局流即可复现口令 ⇒ 熵源退化回伪随机共享流"
  );

  // 3) 输出形状：默认 64 字符、全小写 hex
  assert_eq!(first.len(), 64);
  for pwd in [&first, &second, &a, &b] {
    assert!(
      pwd
        .iter()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c)),
      "口令含非小写 hex 字符: {pwd:?}"
    );
  }

  // 4) bits 取整到 4 的倍数后的长度语义（对标 C# :425-426）
  for (bits, expected) in [
    (b"1".as_slice(), 1usize),
    (b"3", 1),
    (b"4", 1),
    (b"5", 2),
    (b"8", 2),
    (b"4096", 1024),
  ] {
    let pwd = gen_pass(&[bits]);
    assert_eq!(pwd.len(), expected, "bits={:?} 长度不符", bits);
    assert!(
      pwd
        .iter()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c)),
      "bits={:?} 字符集不符",
      bits
    );
  }
}

/// test/standalone/Garnet.test.acl/Resp/ACL/GetUserTests.cs:GetUserTest
#[test]
fn get_user_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let mut session = RespServerSession::default();
  let storage = TestAclStore::open("acl-getuser");
  let store = storage.acl();

  // 规则经 SETUSER 落存储（存储为唯一真源），GETUSER 点查反序列化
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx,
    &store,
    &[b"alice", b"on", b">passw0rd", b"+@admin"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"alice"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n"));
  assert!(frame.contains("$9\r\npasswords\r\n*1\r\n$65\r\n#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9\r\n"));
  assert!(frame.contains("$8\r\ncommands\r\n$7\r\n+@admin\r\n"));

  // default user（存储无记录 → 内存单例兜底）
  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"default"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.contains("*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n"));

  // HELLO 3：map 头 %3、flags 集合头 ~1（C# ACLCommands.cs NetworkAclGetUser →
  // RespServerSessionOutput.cs WriteMapLength / WriteSetLength 的 RESP3 臂），
  // passwords 数组与 commands bulk 版本无关不变
  session.resp_protocol_version = 3;
  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"alice"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(frame.starts_with("%3\r\n$5\r\nflags\r\n~1\r\n$2\r\non\r\n"));
  assert!(frame.contains("$9\r\npasswords\r\n*1\r\n$65\r\n#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9\r\n"));
  assert!(frame.contains("$8\r\ncommands\r\n$7\r\n+@admin\r\n"));
}

/// test/standalone/Garnet.test.acl/Resp/ACL/GetUserTests.cs:GetUserNotFoundTest
#[test]
fn get_user_not_found_test() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-getuser-missing");
  let store = storage.acl();

  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"missing"], &mut out)).unwrap();
  assert_eq!(out, b"$-1\r\n");
}

/// test/standalone/Garnet.test.acl/Resp/ACL/DeleteUserTests.cs:DeleteSingleUser
#[test]
fn delete_single_user() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-deluser");
  let store = storage.acl();

  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx,
    &store,
    &[b"testuser" as &[u8], b">passwd" as &[u8]],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx, &store, &[b"testuser"], &mut out)).unwrap();
  assert_eq!(out, b":1\r\n");
  // 墓碑删除后点查落空（存储为唯一真源）
  assert!(block_on(store.read(0, b"testuser")).unwrap().is_none());

  // delete unknown user returns 0
  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx, &store, &[b"ghost"], &mut out)).unwrap();
  assert_eq!(out, b":0\r\n");
}

/// 指定命名空间的会话域（ns 0 超管形态见 [`ctx_for`]）
fn ctx_in_ns(auth: &GarnetAclAuthenticator, ns: u64) -> AclCtx<'_> {
  AclCtx {
    acl: Some(auth.get_access_control_list()),
    is_custom_command_registered: None,
    caller_namespace: ns,
  }
}

/// default 删除拦截按命名空间分档：ns 0 的 default 是 requirepass / nopass
/// 引导态内存单例（对标 C# DeleteUserHandle 无条件拦截——C# 无命名空间，
/// default 全局唯一）；非 0 命名空间的 default 为租户 SETUSER 落盘的普通
/// 存储记录（in_memory_default_user 恒 None），随租户生命周期可删
#[test]
fn delete_default_user_namespaced() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-deluser-default");
  let store = storage.acl();

  // ns 0：default 引导态内存单例，删除拒绝（对标 C# 语义）
  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx_in_ns(&auth, 0), &store, &[b"default"], &mut out))
    .unwrap();
  assert_eq!(
    out,
    b"-ERR The special 'default' user cannot be removed from the system\r\n"
  );

  // 租户 ns 1：SETUSER default 落普通存储记录
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx_in_ns(&auth, 1),
    &store,
    &[b"default" as &[u8], b"on" as &[u8], b">passwd" as &[u8]],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert!(block_on(store.read(1, b"default")).unwrap().is_some());

  // 租户可删自身 default：确有删除 :1，墓碑后点查落空
  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx_in_ns(&auth, 1), &store, &[b"default"], &mut out))
    .unwrap();
  assert_eq!(out, b":1\r\n");
  assert!(
    block_on(store.read(1, b"default")).unwrap().is_none(),
    "租户 default 删除后点查落空"
  );

  // 重复删除无记录可删 → :0
  let mut out = Vec::new();
  block_on(session.network_acl_del_user(&ctx_in_ns(&auth, 1), &store, &[b"default"], &mut out))
    .unwrap();
  assert_eq!(out, b":0\r\n");
}

/// ACL LOAD 由于数据即时持久化，直接返回 +OK（对标 doc/zh/db.md §3.4）
#[test]
fn acl_load_returns_ok() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();

  let mut out = Vec::new();
  session.network_acl_load(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  // 多余参数仍先行拒绝
  let mut out = Vec::new();
  session
    .network_acl_load(&ctx, &[b"extra"], &mut out)
    .unwrap();
  assert!(out.starts_with(b"-ERR wrong number of arguments"));
}

/// ACL SAVE 由于数据即时持久化，直接返回 +OK（对标 doc/zh/db.md §3.4）
#[test]
fn acl_save_returns_ok() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-save");
  let store = storage.acl();

  let mut out = Vec::new();
  session.network_acl_save(&ctx, &[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  // 持久化真实出口是存储写入面：SETUSER 写穿落盘，
  // 点查可结构化还原规则（绕开文本解析器）
  let seeded = AclParser::parse_acl_rule("user saved on >pw +get").unwrap();
  block_on(store.write(0, b"saved", &seeded.to_bytes())).unwrap();
  let bytes = block_on(store.read(0, b"saved")).unwrap().unwrap();
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
  assert!(session.user_name().is_none(), "构造期口令不符未认证");

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
  assert!(session.user_name().is_none(), "认证失败不记录句柄");

  // 3. AUTH pw → +OK，句柄落位 default 用户
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+OK\r\n");
  assert_eq!(session.user_name(), Some("default"));

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

/// AUTH 失败应答的 WRONGPASS 变体按「用户名是否为空」择定（对标 C#
/// BasicCommands.cs:NetworkAUTH 失败臂的 `username.IsEmpty` 分叉）：单参数与
/// 双参显式空用户名同回 Invalid password，非空用户名（含规范化目标
/// `default`）回组合文案
#[test]
fn auth_wrongpass_variant_follows_empty_username() {
  let (_dir, store) = open_test_store("acl-wrongpass-variant.db").unwrap();
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let mut session = acl_session(&acl, &store);

  // 1. 单参数 AUTH <password>：C# 侧 username 为 default 空 span
  assert!(feed(&mut session, b"*2\r\n$4\r\nAUTH\r\n$7\r\nbadpass\r\n").is_some());
  assert_eq!(
    drain_output(&mut session),
    err_frame(cmd_strings::RESP_WRONGPASS_INVALID_PASSWORD),
    "单参数错密码须回 Invalid password"
  );

  // 2. 双参显式空用户名 AUTH "" <password>：与单参数同臂（本票修复点）
  assert!(
    feed(
      &mut session,
      b"*3\r\n$4\r\nAUTH\r\n$0\r\n\r\n$7\r\nbadpass\r\n"
    )
    .is_some()
  );
  assert_eq!(
    drain_output(&mut session),
    err_frame(cmd_strings::RESP_WRONGPASS_INVALID_PASSWORD),
    "双参空用户名错密码须回 Invalid password"
  );

  // 3. 非空用户名 AUTH default <password>：default 系规范化目标而非空串，
  //    须回组合文案
  assert!(
    feed(
      &mut session,
      b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$7\r\nbadpass\r\n"
    )
    .is_some()
  );
  assert_eq!(
    drain_output(&mut session),
    err_frame(cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD),
    "命名用户 default 错密码须回组合文案"
  );

  // 4. 认证成功对照：空用户名臂仍按 default 走门禁
  assert!(feed(&mut session, b"*3\r\n$4\r\nAUTH\r\n$0\r\n\r\n$2\r\npw\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+OK\r\n");
  assert_eq!(session.user_name(), Some("default"));
}

/// 跨连接改权即时生效（验证 NetworkAclSetUser 对全局共享 UserHandle 的 CAS 换新语义）：受害连接认证命名用户后，管理连接的
/// SETUSER 撤权 / 改密 / DELUSER 均在受害连接的下一条命令收敛
#[test]
fn acl_setuser_propagates_to_live_connections() {
  let (_dir, store) = open_test_store("acl-live-prop.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  // 两条独立连接：各自持会话级认证器，命名用户句柄连接本地持有（无共享）
  let mut admin = acl_session(&acl, &store);
  let mut victim = acl_session(&acl, &store);

  // 管理连接建 u：on + 口令 + 读类
  assert!(
    feed(
      &mut admin,
      b"*6\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$1\r\nu\r\n$2\r\non\r\n$3\r\n>pw\r\n$6\r\n+@read\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut admin), b"+OK\r\n");

  // 受害连接认证 u 并读键（键不存在 → $-1，即已过 ACL 门）
  assert!(feed(&mut victim, b"*3\r\n$4\r\nAUTH\r\n$1\r\nu\r\n$2\r\npw\r\n").is_some());
  assert_eq!(drain_output(&mut victim), b"+OK\r\n");
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(drain_output(&mut victim), b"$-1\r\n");

  // 跨连接撤权：SETUSER u -@all 推进一代 ACL 代数
  let generation = store.acl_generation();
  assert!(
    feed(
      &mut admin,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$1\r\nu\r\n$5\r\n-@all\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut admin), b"+OK\r\n");
  assert_eq!(
    store.acl_generation(),
    generation + 1,
    "SETUSER 写口须推进代数"
  );

  // 受害连接不重连、不重认证，下一条命令即 NOPERM
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  // 改密同例：resetpass >newpw +@read —— 记录仍在故挂载存活，新权限即生效
  assert!(
    feed(
      &mut admin,
      b"*6\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$1\r\nu\r\n$9\r\nresetpass\r\n$6\r\n>newpw\r\n$6\r\n+@read\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut admin), b"+OK\r\n");
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(drain_output(&mut victim), b"$-1\r\n");

  // 旧口令即刻失效、新口令可用（同一记录真源）
  assert!(feed(&mut victim, b"*3\r\n$4\r\nAUTH\r\n$1\r\nu\r\n$2\r\npw\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    format!(
      "-{}\r\n",
      cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD
    )
    .as_bytes()
  );
  assert!(
    feed(
      &mut victim,
      b"*3\r\n$4\r\nAUTH\r\n$1\r\nu\r\n$5\r\nnewpw\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut victim), b"+OK\r\n");

  // DELUSER：删除即推进代数，受害连接按未认证处理（句柄与认证器镜像同撤）
  let generation = store.acl_generation();
  assert!(
    feed(
      &mut admin,
      b"*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$1\r\nu\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut admin), b":1\r\n");
  assert_eq!(
    store.acl_generation(),
    generation + 1,
    "DELUSER 删口须推进代数"
  );

  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOAUTH Authentication required.\r\n"
  );
  assert!(victim.user_name().is_none(), "记录已删按未认证处理");
  assert!(victim.acl_user_handle.is_none(), "已撤挂载");
}

/// 代数相等快路径零存储读（失效判据唯一为代数，非记录内容）：绕过写口直改
/// ACL 记录（不推进代数）→ 决策必不变；经写口同改一次（推进代数）→ 决策即翻
#[test]
fn acl_generation_gate_skips_store_read_on_equal_generation() {
  let (_dir, store) = open_test_store("acl-gen-fastpath.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let writer_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&writer_session);
  let denied = AclParser::parse_acl_rule("user fast on >pw +@all -get").unwrap();
  let allowed = AclParser::parse_acl_rule("user fast on >pw +@all").unwrap();
  block_on(acl_store.write(0, b"fast", &denied.to_bytes())).unwrap();

  let mut victim = acl_session(&acl, &store);
  assert!(
    feed(
      &mut victim,
      b"*3\r\n$4\r\nAUTH\r\n$4\r\nfast\r\n$2\r\npw\r\n"
    )
    .is_some()
  );
  assert_eq!(drain_output(&mut victim), b"+OK\r\n");
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  // 直写物理键（不经 AclStore::write 出口，代数不动）：快路径不得回源存储
  let generation = store.acl_generation();
  let phys = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Acl, b"fast");
  let raw_bytes = allowed.to_bytes();
  let rt = Runtime::new().unwrap();
  rt.block_on(writer_session.upsert_raw(&phys, &raw_bytes))
    .unwrap();
  assert_eq!(store.acl_generation(), generation, "直写不得推进代数");
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOPERM this user has no permissions to run the command\r\n",
    "代数相等即零存储读，记录已改也不得回源"
  );

  // 同内容经写口落一遍 → 推进代数 → 下一命令收敛
  let out =
    block_on(AclStore::new(&store.new_session().unwrap()).write(0, b"fast", &raw_bytes)).is_ok();
  assert!(out, "写口落定");
  assert_eq!(store.acl_generation(), generation + 1);
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(drain_output(&mut victim), b"$-1\r\n");
}

/// 认证臂读前采样时序回归：AUTH 存储点查与句柄挂载之间发生 SETUSER 收权 /
/// DELUSER 落盘并推进代数，Success 携回的点查前代数必落后于最新代数，配对旧
/// 句柄在下一命令鉴权预门即时触发刷新——收权即 NOPERM、删除即 NOAUTH，新权
/// 限即时生效无旧权限滞留（修复前挂载现场读后采样会配对成「旧句柄 + 新代数」
/// 令预门恒判相等，旧权限滞留整个会话生命周期）
#[test]
fn acl_auth_store_generation_presample_no_stale_privilege() {
  let (_dir, store) = open_test_store("acl-auth-presample.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let writer_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&writer_session);
  let granted = AclParser::parse_acl_rule("user racy on >pw +@all").unwrap();
  block_on(acl_store.write(0, b"racy", &granted.to_bytes())).unwrap();

  let mut victim = acl_session(&acl, &store);

  // 1. 存储点查认证：Success 携回读前采样代数（此刻无并发写，等于当前代数）
  let AclAuthOutcome::Success(handle, target_ns, generation) =
    block_on(victim.authenticate_user_via_store(&acl_store, b"racy", b"pw"))
  else {
    panic!("racy 认证必须成功");
  };
  assert_eq!(target_ns, 0);
  assert_eq!(generation, Some(store.acl_generation()));

  // 2. 模拟窗口内并发改权：点查已返回、挂载未落位之间 SETUSER racy -@all 落盘
  let revoked = AclParser::parse_acl_rule("user racy on >pw -@all").unwrap();
  block_on(acl_store.write(0, b"racy", &revoked.to_bytes())).unwrap();
  let stale = generation.expect("存储执行域会话代数必为 Some");
  assert!(
    store.acl_generation() > stale,
    "写口须推进代数，令挂载配对的读前代数落后"
  );

  // 3. 挂载：旧句柄配对读前采得的旧代数（AUTH Success 臂经
  //    apply_authenticated_handle 转传同一值）
  victim.set_user_handle(handle, generation, true);

  // 4. 下一命令鉴权预门：代数落后即回源真源记录，收权即时收敛
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOPERM this user has no permissions to run the command\r\n",
    "窗口内收权必在预门即时生效，不得滞留旧权限"
  );

  // 5. DELUSER 同例：重认证取回句柄与读前代数，删除落盘后挂载，预门即撤挂载
  let AclAuthOutcome::Success(handle, _, generation) =
    block_on(victim.authenticate_user_via_store(&acl_store, b"racy", b"pw"))
  else {
    panic!("racy 重认证必须成功");
  };
  assert!(
    block_on(acl_store.delete(0, b"racy")).unwrap(),
    "删除须生效并推进代数"
  );
  assert!(
    store.acl_generation() > generation.expect("代数必为 Some"),
    "删口须推进代数"
  );
  victim.set_user_handle(handle, generation, true);
  assert!(feed(&mut victim, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut victim),
    b"-NOAUTH Authentication required.\r\n",
    "窗口内删除用户不得以旧权限继续执行命令"
  );
  assert!(victim.user_name().is_none(), "记录已删按未认证处理");
}

/// 引导期 default 挂载（内存单例，存储恒无同名记录）在无关 SETUSER 推进代数后
/// 必不误撤（否则任一改权即把全部在途免密连接打成 NOAUTH）
#[test]
fn acl_bootstrap_default_survives_unrelated_setuser() {
  let (_dir, store) = open_test_store("acl-bootstrap-keep.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut session = acl_session(&acl, &store);
  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+PONG\r\n");

  let writer_session = store.new_session().unwrap();
  let user = AclParser::parse_acl_rule("user other on >pw +@read").unwrap();
  block_on(AclStore::new(&writer_session).write(0, b"other", &user.to_bytes())).unwrap();
  assert!(store.acl_generation() > 0, "写口须推进代数");

  assert!(feed(&mut session, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(drain_output(&mut session), b"+PONG\r\n");
  assert_eq!(session.user_name(), Some("default"));
}

/// 未挂载认证器（免认证形态）的会话门控恒放行（对标 C# GetDefaultUserHandle
/// +@all 默认用户兜底）
#[test]
fn no_auth_session_allows_all_commands() {
  let mut session = RespServerSession::new(2, RespServerSessionOptions::default());
  // 免认证形态构造即落到默认用户（C# GetDefaultUserHandle 兜底语义）
  assert_eq!(session.user_name(), Some("default"));
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
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  assert!(auth.authenticate(b"", b"x", acl_password_check).is_some());
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
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  assert!(auth.authenticate(b"", b"x", acl_password_check).is_some());
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
  session.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(
    &acl,
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
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list-streaming");
  let store = storage.acl();

  // 两位数组长度：符头与正文同取自一份快照，二者之间不容纳占位偏差
  let users: Vec<String> = (0..12).map(|i| format!("user-{i}")).collect();
  for user in &users {
    let mut out = Vec::new();
    block_on(session.network_acl_set_user(&ctx, &store, &[user.as_bytes()], &mut out)).unwrap();
    assert_eq!(out, b"+OK\r\n");
  }

  // LIST：12 条存储记录 + 引导态 default 兜底（存储无 default 记录）
  let mut out = Vec::new();
  block_on(session.network_acl_list(&ctx, &store, &[], &mut out)).unwrap();
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
  block_on(session.network_acl_users(&ctx, &store, &[], &mut out)).unwrap();
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
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-list-corrupt-record");
  let store = storage.acl();
  block_on(store.write(0, b"broken", b"not-a-bitcode-user-record")).unwrap();

  let mut out = Vec::new();
  block_on(session.network_acl_list(&ctx, &store, &[], &mut out)).unwrap();
  assert_eq!(
    out,
    format!("-{RESP_ERR_ACL_STORE_SCAN_FAILED}\r\n").into_bytes()
  );

  let mut out = Vec::new();
  block_on(session.network_acl_users(&ctx, &store, &[], &mut out)).unwrap();
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
/// 压力形态：200 位常驻用户把单遍扫描摊厚（旧口径下即把两遍之间的窗口拉宽），
/// 写侧线程在读侧每一框之间持续增删命名用户。
#[test]
fn acl_list_and_users_snapshot_frame_consistent_under_concurrent_mutation() {
  const RESIDENT_USERS: u32 = 200;
  let (_dir, store): (_, Arc<WedbStore<SegmentedDevice>>) =
    open_test_store("acl-list-snapshot-race").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let reader_session = store.new_session().unwrap();
  let reader = AclStore::new(&reader_session);

  // 常驻用户垫量：扫描内核走全日志区间，记录越多单遍越慢（旧口径的窗口宽度）
  for i in 0..RESIDENT_USERS {
    let name = format!("resident-{i}");
    let user = User::new(name.clone());
    block_on(reader.write(0, name.as_bytes(), &user.to_bytes())).expect("常驻用户落盘");
  }

  // 写侧：另一会话线程持续增删命名用户，直至读侧收工
  let writer_store = Arc::clone(&store);
  let stop = Arc::new(AtomicBool::new(false));
  let writer_stop = Arc::clone(&stop);
  let writer = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
      let writer_session = writer_store.new_session().unwrap();
      let store = AclStore::new(&writer_session);
      let mut round = 0u32;
      while !writer_stop.load(Ordering::Relaxed) && round < 600 {
        // 净增长形态：每轮新增一名、每四名回收一名，令可见用户数逐轮抖动
        // （写后即删的等量抖动两遍采到同值，旧口径下也测不出背离）
        let name = format!("racer-{round}");
        let user = User::new(name.clone());
        // 压力写删成败均可（ Broker 抖动语义本就容忍单轮落空）
        let _ = store.write(0, name.as_bytes(), &user.to_bytes()).await;
        if round % 4 == 3 {
          let old = format!("racer-{}", round - 3);
          let _ = store.delete(0, old.as_bytes()).await;
        }
        round += 1;
      }
      round
    })
  });

  // 读侧：每一框都须自洽（符头 == 元素数，且逐元素框体完整），任一次脱拍即断言
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for _ in 0..12 {
      let mut out = Vec::new();
      block_on(session.network_acl_list(&ctx, &reader, &[], &mut out)).unwrap();
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
      for item in &items[1..] {
        assert!(
          item.starts_with(b"user resident-") || item.starts_with(b"user racer-"),
          "非本命名空间用户入框: {}",
          String::from_utf8_lossy(item)
        );
      }

      let mut out = Vec::new();
      block_on(session.network_acl_users(&ctx, &reader, &[], &mut out)).unwrap();
      let (declared, items) = parse_bulk_array(&out);
      assert_eq!(
        declared,
        items.len(),
        "USERS 符头与元素数背离（按符头取数即截断/残留）: {}",
        String::from_utf8_lossy(&out)
      );
      assert_eq!(items[0], b"default", "default 兜底位次回归");
    }
    stop.store(true, Ordering::Relaxed);
  });
  let writer_rounds = writer.join().unwrap();
  assert!(
    writer_rounds > 1,
    "写侧未与读侧交叠（rounds={writer_rounds}），本框压力形态失效"
  );
}

/// 全目录命令 ACL 矩阵：断言全目录命令均可被 +@all 放行、-@all 拒绝
#[test]
fn acl_matrix_all_catalog_commands_permitted_by_all_and_denied_by_none() {
  let user_all = AclParser::parse_acl_rule("user u_all on nopass +@all").unwrap();
  let user_none = AclParser::parse_acl_rule("user u_none on nopass -@all").unwrap();

  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut session_all = RespServerSession::default();
  session_all.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(
    &acl,
  )))));
  session_all.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&user_all))),
    None,
    false,
  );

  let mut session_none = RespServerSession::default();
  session_none.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(
    &acl,
  )))));
  session_none.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&user_none))),
    None,
    false,
  );

  let mut count = 0usize;
  for entry in commands_for_category(RespAclCategories::ALL) {
    count += 1;
    let cmd = entry.cmd;
    let name = entry.name;

    // 1. 用户层位图判定
    assert!(
      user_all.can_access_command(cmd),
      "+@all 用户应能访问命令: {name}"
    );
    assert!(
      !user_none.can_access_command(cmd),
      "-@all 用户不得访问命令: {name}"
    );

    // 2. 会话层门禁判定
    assert!(session_all.acl_permits(cmd), "+@all 会话应放行命令: {name}");

    if is_no_auth(cmd) {
      assert!(
        session_none.acl_permits(cmd),
        "免认证命令 (NoAuth) 无论权限如何均放行: {name}"
      );
    } else {
      assert!(
        !session_none.acl_permits(cmd),
        "-@all 会话非免密命令应被门禁拦截: {name}"
      );
    }
  }
  assert_eq!(count, 353, "必须覆盖全部 353 条目录命令");
}

/// ACL 类别展开与命令授权断言（对标 C# RespCommandTests 矩阵测试）
#[test]
fn acl_matrix_category_expansion_and_command_authorization() {
  const POPULATED_CATEGORIES: [RespAclCategories; 23] = [
    RespAclCategories::ADMIN,
    RespAclCategories::BITMAP,
    RespAclCategories::BLOCKING,
    RespAclCategories::CONNECTION,
    RespAclCategories::DANGEROUS,
    RespAclCategories::GEO,
    RespAclCategories::HASH,
    RespAclCategories::HYPERLOGLOG,
    RespAclCategories::FAST,
    RespAclCategories::KEYSPACE,
    RespAclCategories::LIST,
    RespAclCategories::PUBSUB,
    RespAclCategories::READ,
    RespAclCategories::SCRIPTING,
    RespAclCategories::SET,
    RespAclCategories::SORTEDSET,
    RespAclCategories::SLOW,
    RespAclCategories::STRING,
    RespAclCategories::TRANSACTION,
    RespAclCategories::VECTOR,
    RespAclCategories::WRITE,
    RespAclCategories::GARNET,
    RespAclCategories::CUSTOM,
  ];

  for cat in POPULATED_CATEGORIES {
    let cat_name = AclParser::get_name_by_acl_category(cat);
    let rule_plus = format!("user u on nopass -@all +@{cat_name}");
    let user_plus = AclParser::parse_acl_rule(&rule_plus).unwrap();
    let rule_minus = format!("user u on nopass +@all -@{cat_name}");
    let user_minus = AclParser::parse_acl_rule(&rule_minus).unwrap();

    let mut count = 0usize;
    // 单遍流式断言：零堆分配，同时验证 +@cat 放行与 +@all -@cat 撤销
    for e in commands_for_category(cat) {
      count += 1;
      assert!(
        user_plus.can_access_command(e.cmd),
        "命令 {} 应在 +@{} 中被放行",
        e.name,
        cat_name
      );
      if is_no_auth(e.cmd) {
        assert!(
          user_minus.can_access_command(e.cmd),
          "NoAuth 命令 {} 不得被 -@{} 撤销",
          e.name,
          cat_name
        );
      } else {
        assert!(
          !user_minus.can_access_command(e.cmd),
          "命令 {} 应在 -@{} 中被撤销",
          e.name,
          cat_name
        );
      }
    }
    assert!(count > 0, "分类 @{cat_name} 展开命令清单不得为空");
  }

  // STREAM 分类自守断言：当前未挂载命令，未来若挂载命令则提醒补充入 POPULATED_CATEGORIES
  assert_eq!(
    commands_for_category(RespAclCategories::STREAM).count(),
    0,
    "STREAM 分类挂载命令后需纳入测试矩阵"
  );

  // 3. 命令别名与展开集等价性校验
  // SET 展开集（Setexnx, Setexxx, Setkeepttl, Setkeepttlxx）
  let user_set = AclParser::parse_acl_rule("user u on nopass -@all +set").unwrap();
  assert!(user_set.can_access_command(RespCommand::Set));
  for &expanded in expand_for_acls(RespCommand::Set) {
    assert!(
      user_set.can_access_command(expanded),
      "SET 展开命令 {:?} 应随 +set 自动放行",
      expanded
    );
  }

  // BITOP 展开集（BitopAnd, BitopNot, BitopOr, BitopXor, BitopDiff）
  let user_bitop = AclParser::parse_acl_rule("user u on nopass -@all +bitop").unwrap();
  assert!(user_bitop.can_access_command(RespCommand::Bitop));
  for &expanded in expand_for_acls(RespCommand::Bitop) {
    assert!(
      user_bitop.can_access_command(expanded),
      "BITOP 展开命令 {:?} 应随 +bitop 自动放行",
      expanded
    );
  }

  // 4. 根命令级联展开子命令
  let user_client = AclParser::parse_acl_rule("user u on nopass -@all +client").unwrap();
  assert!(user_client.can_access_command(RespCommand::Client));
  assert!(user_client.can_access_command(RespCommand::ClientId));
  assert!(user_client.can_access_command(RespCommand::ClientInfo));
  assert!(user_client.can_access_command(RespCommand::ClientList));

  // 单子命令精确放行
  let user_sub = AclParser::parse_acl_rule("user u on nopass -@all +client|id").unwrap();
  assert!(user_sub.can_access_command(RespCommand::ClientId));
  assert!(!user_sub.can_access_command(RespCommand::ClientInfo));
  assert!(!user_sub.can_access_command(RespCommand::ClientList));

  // 5. 复合过滤规则验证
  let user_combo = AclParser::parse_acl_rule("user u on nopass -@all +@string -get").unwrap();
  assert!(user_combo.can_access_command(RespCommand::Set));
  assert!(!user_combo.can_access_command(RespCommand::Get));
  assert!(!user_combo.can_access_command(RespCommand::Hget));
}

/// 验证三条自增命令的类别与权限行为（SUNSUBSCRIBE、RI.COUNT 与 CLUSTER FLUSHALL_NS）
#[test]
fn acl_matrix_self_added_commands_authorization() {
  let (_dir, store) = open_test_store("acl-matrix-self-added.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // ==========================================
  // 1. SUNSUBSCRIBE (RespCommand::Sunsubscribe)
  // ==========================================
  let sunsub =
    try_get_resp_command_info(RespCommand::Sunsubscribe).expect("SUNSUBSCRIBE 须存在于命令目录");
  assert_eq!(sunsub.cs, "SUNSUBSCRIBE");
  assert_eq!(sunsub.name, "sunsubscribe");
  assert_eq!(sunsub.parent, None);
  assert_eq!(
    sunsub.cats,
    RespAclCategories::PUBSUB | RespAclCategories::READ | RespAclCategories::SLOW
  );

  // 类别放行验证：pubsub / read / slow
  for cat_rule in ["+@pubsub", "+@read", "+@slow"] {
    let rule = format!("user u on nopass -@all {cat_rule}");
    let user = AclParser::parse_acl_rule(&rule).unwrap();
    assert!(
      user.can_access_command(RespCommand::Sunsubscribe),
      "SUNSUBSCRIBE 应被 {cat_rule} 放行"
    );
  }

  // 无关分类不放行
  let u_unrelated = AclParser::parse_acl_rule("user u on nopass -@all +@string").unwrap();
  assert!(!u_unrelated.can_access_command(RespCommand::Sunsubscribe));

  // 逐命令放行与撤销
  let u_explicit = AclParser::parse_acl_rule("user u on nopass -@all +sunsubscribe").unwrap();
  assert!(u_explicit.can_access_command(RespCommand::Sunsubscribe));
  assert!(!u_explicit.can_access_command(RespCommand::Ssubscribe));

  let u_revoked = AclParser::parse_acl_rule("user u on nopass +@all -sunsubscribe").unwrap();
  assert!(!u_revoked.can_access_command(RespCommand::Sunsubscribe));

  let u_cat_revoked =
    AclParser::parse_acl_rule("user u on nopass -@all +@pubsub -sunsubscribe").unwrap();
  assert!(!u_cat_revoked.can_access_command(RespCommand::Sunsubscribe));

  // 会话端到端门禁
  let mut s_sunsub_denied = acl_session(&acl, &store);
  s_sunsub_denied.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_revoked))),
    Some(store.acl_generation()),
    false,
  );
  assert!(!s_sunsub_denied.acl_permits(RespCommand::Sunsubscribe));
  assert!(feed(&mut s_sunsub_denied, b"*1\r\n$12\r\nSUNSUBSCRIBE\r\n").is_some());
  assert_eq!(
    drain_output(&mut s_sunsub_denied),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  let mut s_sunsub_allowed = acl_session(&acl, &store);
  s_sunsub_allowed.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_explicit))),
    Some(store.acl_generation()),
    false,
  );
  assert!(s_sunsub_allowed.acl_permits(RespCommand::Sunsubscribe));
  assert!(feed(&mut s_sunsub_allowed, b"*1\r\n$12\r\nSUNSUBSCRIBE\r\n").is_some());
  let out = drain_output(&mut s_sunsub_allowed);
  assert_eq!(
    out,
    format!("-{}\r\n", cmd_strings::RESP_ERR_GENERIC_CLUSTER_DISABLED).into_bytes(),
    "SUNSUBSCRIBE 授权通过后应通过 ACL 门进入发布订阅分派（集群门禁报错）: got {}",
    String::from_utf8_lossy(&out)
  );

  // ==========================================
  // 2. RI.COUNT (RespCommand::Ricount)
  // ==========================================
  let ricount =
    try_get_resp_command_info(RespCommand::Ricount).expect("RI.COUNT (Ricount) 须存在于命令目录");
  assert_eq!(ricount.cs, "RICOUNT");
  assert_eq!(ricount.name, "ri.count");
  assert_eq!(ricount.parent, None);
  assert_eq!(
    ricount.cats,
    RespAclCategories::FAST | RespAclCategories::READ | RespAclCategories::GARNET
  );

  // 类别放行验证：fast / read / garnet
  for cat_rule in ["+@fast", "+@read", "+@garnet"] {
    let rule = format!("user u on nopass -@all {cat_rule}");
    let user = AclParser::parse_acl_rule(&rule).unwrap();
    assert!(
      user.can_access_command(RespCommand::Ricount),
      "RI.COUNT 应被 {cat_rule} 放行"
    );
  }

  // 无关分类不放行
  let u_unrelated_ri = AclParser::parse_acl_rule("user u on nopass -@all +@write").unwrap();
  assert!(!u_unrelated_ri.can_access_command(RespCommand::Ricount));

  // 点名放行（含 canonical 名 ri.count 与去点名 ricount）与撤销
  let u_ri_dot = AclParser::parse_acl_rule("user u on nopass -@all +ri.count").unwrap();
  assert!(u_ri_dot.can_access_command(RespCommand::Ricount));

  let u_ri_dotless = AclParser::parse_acl_rule("user u on nopass -@all +ricount").unwrap();
  assert!(u_ri_dotless.can_access_command(RespCommand::Ricount));

  let u_ri_revoked = AclParser::parse_acl_rule("user u on nopass +@all -ri.count").unwrap();
  assert!(!u_ri_revoked.can_access_command(RespCommand::Ricount));

  let u_ri_revoked2 = AclParser::parse_acl_rule("user u on nopass +@all -ricount").unwrap();
  assert!(!u_ri_revoked2.can_access_command(RespCommand::Ricount));

  let u_ri_cat_revoked =
    AclParser::parse_acl_rule("user u on nopass -@all +@fast -ri.count").unwrap();
  assert!(!u_ri_cat_revoked.can_access_command(RespCommand::Ricount));

  // 会话端到端门禁
  let mut s_ri_denied = acl_session(&acl, &store);
  s_ri_denied.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_ri_revoked))),
    Some(store.acl_generation()),
    false,
  );
  assert!(!s_ri_denied.acl_permits(RespCommand::Ricount));
  assert!(feed(&mut s_ri_denied, b"*2\r\n$8\r\nRI.COUNT\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    drain_output(&mut s_ri_denied),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  let mut s_ri_allowed = acl_session(&acl, &store);
  s_ri_allowed.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_ri_dot))),
    Some(store.acl_generation()),
    false,
  );
  assert!(s_ri_allowed.acl_permits(RespCommand::Ricount));
  assert!(matches!(
    s_ri_allowed.check_acl_permissions(RespCommand::Ricount),
    AclGateVerdict::Permitted
  ));
  assert!(feed(&mut s_ri_allowed, b"*2\r\n$8\r\nRI.COUNT\r\n$1\r\nk\r\n").is_some());
  let out = drain_output(&mut s_ri_allowed);
  assert!(
    !out.starts_with(b"-NOPERM") && !out.starts_with(b"-NOAUTH"),
    "RI.COUNT 授权通过后必须越过 ACL 门（不产生权限拦截错误）"
  );

  // ====================================================
  // 3. CLUSTER FLUSHALL_NS (RespCommand::ClusterFlushallNs)
  // ====================================================
  let cluster_flush = try_get_resp_command_info(RespCommand::ClusterFlushallNs)
    .expect("CLUSTER|FLUSHALL_NS 须存在于命令目录");
  assert_eq!(cluster_flush.cs, "CLUSTER_FLUSHALL_NS");
  assert_eq!(cluster_flush.name, "cluster|flushall_ns");
  assert_eq!(cluster_flush.parent, Some(RespCommand::Cluster));
  assert_eq!(
    cluster_flush.cats,
    RespAclCategories::ADMIN
      | RespAclCategories::DANGEROUS
      | RespAclCategories::SLOW
      | RespAclCategories::GARNET
  );

  // 类别放行验证：admin / dangerous / slow / garnet
  for cat_rule in ["+@admin", "+@dangerous", "+@slow", "+@garnet"] {
    let rule = format!("user u on nopass -@all {cat_rule}");
    let user = AclParser::parse_acl_rule(&rule).unwrap();
    assert!(
      user.can_access_command(RespCommand::ClusterFlushallNs),
      "CLUSTER FLUSHALL_NS 应被 {cat_rule} 放行"
    );
  }

  // 无关分类不放行
  let u_unrelated_cluster = AclParser::parse_acl_rule("user u on nopass -@all +@fast").unwrap();
  assert!(!u_unrelated_cluster.can_access_command(RespCommand::ClusterFlushallNs));

  // 根命令展开与单子命令放行/撤销
  let u_cluster_all = AclParser::parse_acl_rule("user u on nopass -@all +cluster").unwrap();
  assert!(u_cluster_all.can_access_command(RespCommand::ClusterFlushallNs));
  assert!(u_cluster_all.can_access_command(RespCommand::ClusterNodes));

  let u_cluster_sub =
    AclParser::parse_acl_rule("user u on nopass -@all +cluster|flushall_ns").unwrap();
  assert!(u_cluster_sub.can_access_command(RespCommand::ClusterFlushallNs));
  assert!(!u_cluster_sub.can_access_command(RespCommand::ClusterNodes));

  let u_cluster_rev =
    AclParser::parse_acl_rule("user u on nopass +@all -cluster|flushall_ns").unwrap();
  assert!(!u_cluster_rev.can_access_command(RespCommand::ClusterFlushallNs));
  assert!(u_cluster_rev.can_access_command(RespCommand::ClusterNodes));

  let u_cluster_root_rev =
    AclParser::parse_acl_rule("user u on nopass -@all +cluster -cluster|flushall_ns").unwrap();
  assert!(!u_cluster_root_rev.can_access_command(RespCommand::ClusterFlushallNs));
  assert!(u_cluster_root_rev.can_access_command(RespCommand::ClusterNodes));

  // 会话端到端门禁
  let flush_frame = b"*5\r\n$7\r\nCLUSTER\r\n$11\r\nFLUSHALL_NS\r\n$1\r\n1\r\n$32\r\n0123456789abcdef0123456789abcdef\r\n$1\r\n5\r\n";

  let mut s_cluster_denied = acl_session(&acl, &store);
  s_cluster_denied.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_cluster_rev))),
    Some(store.acl_generation()),
    false,
  );
  assert!(!s_cluster_denied.acl_permits(RespCommand::ClusterFlushallNs));
  assert!(feed(&mut s_cluster_denied, flush_frame).is_some());
  assert_eq!(
    drain_output(&mut s_cluster_denied),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  let mut s_cluster_allowed = acl_session(&acl, &store);
  s_cluster_allowed.set_user_handle(
    Arc::new(UserHandle::new(Arc::clone(&u_cluster_sub))),
    Some(store.acl_generation()),
    false,
  );
  assert!(s_cluster_allowed.acl_permits(RespCommand::ClusterFlushallNs));
  assert!(feed(&mut s_cluster_allowed, flush_frame).is_some());
  let out = drain_output(&mut s_cluster_allowed);
  assert_eq!(
    out,
    format!("-{}\r\n", cmd_strings::RESP_ERR_GENERIC_CLUSTER_DISABLED).into_bytes(),
    "CLUSTER FLUSHALL_NS 授权通过后应通过 ACL 门进入集群禁用报错: got {}",
    String::from_utf8_lossy(&out)
  );
}

/// r16-acl 审查票发现二回归（a）：SETUSER 携含无效 UTF-8 字节（0xFF）的口令
/// ——字节口径对标 C# ParseUtils.ReadString = Encoding.ASCII.GetString，逐字节
/// 折叠为 "?" 后 SHA256 落账（GETUSER 断言哈希等于折叠串哈希预置常量），同字节
/// 口令 AUTH 命中（原 as_str_safe 严格 UTF-8 口径整 token 变空串静默丢弃仍回
/// +OK 的假成功 + AUTH from_utf8 严格拒的认证可达面分叉，双回归）
#[test]
fn setuser_invalid_utf8_password_folds_and_auths() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-fold-invalid-utf8");
  let store = storage.acl();

  // >\xFF → 折叠 "?" 落账（哈希 = SHA256(b"?")）
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(&ctx, &store, &[b"u", b"on", b">\xFF"], &mut out)).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"u"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(
    frame.contains("$65\r\n#8a8de823d5ed3e12746a62ef169bcf372be0ca44f0a1236abc35df05d96928e1\r\n"),
    "口令哈希应为 ASCII 折叠串 SHA256(b\"?\"): {frame}"
  );

  // 同字节口令 AUTH 命中（存储点查臂同一折叠口径）
  let outcome = block_on(session.authenticate_user_via_store(&store, b"u", b"\xFF"));
  assert!(matches!(outcome, AclAuthOutcome::Success(..)));
}

/// r16-acl 审查票发现二回归（b）：非 ASCII 合法 UTF-8 口令（é = 0xC3 0xA9）
/// ——逐字节折叠 "??" 后哈希落账，等于 C# 口径 SHA256(ASCII 折叠串) 预置常量
/// （原口径存 SHA256(原始 UTF-8 字节)，与 C# 及本仓引导 default 的折叠哈希
/// 自相矛盾）；同明文 AUTH 命中，两侧哈希恒等
#[test]
fn setuser_non_ascii_utf8_password_folds_and_auths() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-fold-non-ascii");
  let store = storage.acl();

  // >é（0xC3 0xA9）→ 折叠 "??"（哈希 = SHA256(b"??")，C# 口径预置常量）
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(&ctx, &store, &[b"u", b"on", b">\xC3\xA9"], &mut out))
    .unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &store, &[b"u"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(
    frame.contains("$65\r\n#e270aeb347f2165574c3a5c5bf11d038bcd3acd5abfdb5ae8a1b52d91cb842f0\r\n"),
    "口令哈希应为 ASCII 折叠串 SHA256(b\"??\")（C# 口径）: {frame}"
  );

  // 同明文 AUTH 命中（存储落账与认证同口径，同明文哈希恒等）
  let outcome = block_on(session.authenticate_user_via_store(&store, b"u", b"\xC3\xA9"));
  assert!(matches!(outcome, AclAuthOutcome::Success(..)));
}

/// r16-acl 审查票发现一回归：SETUSER 读改写全程持 ACL 管理串行锁——两连接
/// 并发对同一用户施加互补操作，终态两条操作均在位。对标 C# NetworkAclSetUser
/// 的 do/while CAS 重试环（libs/server/Resp/ACLCommands.cs:181-226 +
/// UserHandle.cs:TrySetUser 的 Interlocked.CompareExchange）「两命令操作全生效」
/// 语义：裸读改写形态下并发双方读得同一基线后互相覆盖，必静默丢一方
#[test]
fn setuser_concurrent_complementary_ops_both_survive() {
  const ROUNDS: usize = 64;
  let (_dir, store): (_, Arc<WedbStore<SegmentedDevice>>) =
    open_test_store("acl-setuser-serialize-race").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // 串行建户（on nopass），两写线程只对权限面做互补增补
  {
    let mut session = acl_session(&acl, &store);
    assert!(
      feed(
        &mut session,
        b"*5\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nracer\r\n$2\r\non\r\n$6\r\nnopass\r\n"
      )
      .is_some()
    );
    assert_eq!(drain_output(&mut session), b"+OK\r\n");
  }

  // 每轮 Barrier 对齐两连接同时发射：裸读改写下双方极易读得同一基线，后写
  // 覆盖前写即丢一方；持锁后后到者以最新落盘记录为基线重放，两操作恒均在位
  let barrier = Arc::new(Barrier::new(2));
  let spawn_writer = |store: Arc<WedbStore<SegmentedDevice>>,
                      acl: Arc<AccessControlList>,
                      barrier: Arc<Barrier>,
                      frame: &'static [u8]| {
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(async move {
        let mut session = acl_session(&acl, &store);
        for _ in 0..ROUNDS {
          barrier.wait();
          assert!(feed(&mut session, frame).is_some());
          assert_eq!(drain_output(&mut session), b"+OK\r\n");
        }
      });
    })
  };
  let add_get = spawn_writer(
    Arc::clone(&store),
    Arc::clone(&acl),
    Arc::clone(&barrier),
    b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nracer\r\n$4\r\n+get\r\n",
  );
  let add_set = spawn_writer(
    Arc::clone(&store),
    Arc::clone(&acl),
    barrier,
    b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nracer\r\n$4\r\n+set\r\n",
  );
  add_get.join().unwrap();
  add_set.join().unwrap();

  // 终态：两条互补操作均在位（GETUSER commands 描述含 +get 与 +set）
  let reader_session = store.new_session().unwrap();
  let reader = AclStore::new(&reader_session);
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, None);
  let session = RespServerSession::default();
  let mut out = Vec::new();
  block_on(session.network_acl_get_user(&ctx, &reader, &[b"racer"], &mut out)).unwrap();
  let frame = String::from_utf8(out).unwrap();
  assert!(
    frame.contains("+get"),
    "并发增补后 +get 缺失（丢更新）: {frame}"
  );
  assert!(
    frame.contains("+set"),
    "并发增补后 +set 缺失（丢更新）: {frame}"
  );
}

// ============================================================================
// r308：garnet C# 行为面移植——SETUSER 口令操作族 / 受保护 default 用户 /
// GETUSER 多用户（锚逐例注于文档注释，garnet/test/standalone/
// Garnet.test.acl/Resp/ACL/ 下）
// ============================================================================

/// 对标 C# AclTest.cs:17-32 常量组（DummyPassword / 其 SHA-256 哈希 / DummyPasswordB）
const DUMMY_PASSWORD: &str = "passw0rd";
const DUMMY_PASSWORD_HASH: &str =
  "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";
const DUMMY_PASSWORD_B: &str = "paSSw0rd";

/// 会话帧收发闭环：resp_frame 单源组帧 + feed 泵替身闭环，返回线面应答字节
fn call(s: &mut RespServerSession, args: &[&[u8]]) -> Vec<u8> {
  assert!(
    feed(s, &resp_frame(args)).is_some(),
    "帧应被完整消费: {args:?}"
  );
  drain_output(s)
}

/// SetUserTests.cs:166/196 — add 明文 `>` / 哈希 `#` 口令：ACL LIST 归一哈希
/// 回显 + 两种形落账后 AUTH 明文命中行为面
#[test]
fn setuser_add_password_from_cleartext_and_hash() {
  let (_dir, store) = open_test_store("acl-pw-add.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  let hash_op = format!("#{DUMMY_PASSWORD_HASH}");
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"on", b">passw0rd"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(
      &mut s,
      &[b"ACL", b"SETUSER", b"userB", b"on", hash_op.as_bytes()]
    ),
    b"+OK\r\n"
  );

  // C# :178-189 / :208-219 — 两行均含归一哈希，default 行不受染指
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains(&format!("user userA on #{DUMMY_PASSWORD_HASH}")),
    "明文加密须以哈希回显: {list}"
  );
  assert!(
    list.contains(&format!("user userB on #{DUMMY_PASSWORD_HASH}")),
    "哈希加密原样落账: {list}"
  );
  assert!(list.contains("user default on nopass +@all"));

  // 行为面补缺：两种形登记后按明文认证均成功（改后 AUTH 结果臂）
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(s.user_name(), Some("userA"));
  assert_eq!(call(&mut s, &[b"AUTH", b"default", b"x"]), b"+OK\r\n");
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userB", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(s.user_name(), Some("userB"));
}

/// SetUserTests.cs:226/258 — `<明文` / `!哈希` 删密：LIST 哈希抹除 +
/// 删后 AUTH 立即 WRONGPASS（口令集空且非免密）
#[test]
fn setuser_remove_password_from_cleartext_and_hash() {
  let (_dir, store) = open_test_store("acl-pw-rm.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  let hash_add = format!("#{DUMMY_PASSWORD_HASH}");
  let hash_rm = format!("!{DUMMY_PASSWORD_HASH}");
  // userA：明文加、明文删（C# :236-241）
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"on", b">passw0rd"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"<passw0rd"]),
    b"+OK\r\n"
  );
  // userB：哈希加、哈希删（C# :268-273）
  assert_eq!(
    call(
      &mut s,
      &[b"ACL", b"SETUSER", b"userB", b"on", hash_add.as_bytes()]
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userB", hash_rm.as_bytes()]),
    b"+OK\r\n"
  );

  // C# :244-251 / :276-283 — 两用户行均不再含哈希（全场无 # 条目）
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(!list.contains('#'), "删密后 LIST 不应残留哈希: {list}");
  // 行为面补缺：删密后原明文认证必被拒（命名用户形组合文案）
  let wrongpass = err_frame(cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    wrongpass
  );
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userB", DUMMY_PASSWORD.as_bytes()]),
    wrongpass
  );
}

/// SetUserTests.cs:290 — 同口令重复 add（口令集合流语义）：LIST 仅一条哈希
#[test]
fn setuser_duplicate_password_single_entry() {
  let (_dir, store) = open_test_store("acl-pw-dup.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"on", b">passw0rd"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b">passw0rd"]),
    b"+OK\r\n"
  );

  // C# :308-315 — 重复 add 后用户行 `#` 计数恒 1（全场即 userA 一条）
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert_eq!(
    list.matches('#').count(),
    1,
    "重复口令须合流为单条目: {list}"
  );
  assert_eq!(list.matches(DUMMY_PASSWORD_HASH).count(), 1);
}

/// SetUserTests.cs:323 — `on >pw nopass`：nopass 清空已设口令转免密，
/// 任意错口令 AUTH 均放行
#[test]
fn setuser_passwordless_user_any_password_auth() {
  let (_dir, store) = open_test_store("acl-pw-nopass.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  assert_eq!(
    call(
      &mut s,
      &[b"ACL", b"SETUSER", b"userA", b"on", b">passw0rd", b"nopass"]
    ),
    b"+OK\r\n"
  );
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains("user userA on nopass") && !list.contains('#'),
    "nopass 须清掉已设 >pw 口令: {list}"
  );

  // C# :337-338 — 错口令 DummyPasswordB 登录免密用户成功
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD_B.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(s.user_name(), Some("userA"));
}

/// SetUserTests.cs:346 — resetpass：多口令与免密态全清、LIST 无哈希、
/// 清后 AUTH 任意口令 WRONGPASS
#[test]
fn setuser_resetpass_clears_all_passwords() {
  let (_dir, store) = open_test_store("acl-pw-resetpass.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  assert_eq!(
    call(
      &mut s,
      &[
        b"ACL",
        b"SETUSER",
        b"userA",
        b"on",
        b">passw0rd",
        b">paSSw0rd"
      ]
    ),
    b"+OK\r\n"
  );
  // C# :360-367 — 两条口令哈希并存
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert_eq!(list.matches('#').count(), 2, "应登记两条口令哈希: {list}");

  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"resetpass"]),
    b"+OK\r\n"
  );
  // C# :374-381 — resetpass 后哈希计数归零
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(!list.contains('#'), "resetpass 须清空全部口令: {list}");

  // 行为面补缺：清密后原口令认证立即被拒（非免密且口令集空）
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    err_frame(cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD)
  );
}

/// SetUserTests.cs:111 — 启用/停用臂：建户缺省 off 即 WRONGPASS；显式 on
/// 放行登录；再 off 即时撤登（C# :121-159 全链）
#[test]
fn enable_disable_user_auth() {
  let (_dir, store) = open_test_store("acl-enable-disable.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  let wrongpass = err_frame(cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
  // C# :121-133 — 未显式 on 的新用户 AUTH 必拒（停用与错口令同 WRONGPASS 臂）
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b">passw0rd"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    wrongpass
  );

  // C# :136-141 — 显式 on 后可登录
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"on"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(s.user_name(), Some("userA"));

  // C# :144-159 — 切回 default 停用该户，停用后 AUTH 再次 WRONGPASS
  assert_eq!(call(&mut s, &[b"AUTH", b"default"]), b"+OK\r\n");
  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"off"]),
    b"+OK\r\n"
  );
  assert_eq!(
    call(&mut s, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    wrongpass
  );
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(list.contains("user userA off"), "停用行须回显 off: {list}");
}

/// SetUserTests.cs:446 — reset：清口令、撤权、停用三效一体，LIST 行回归
/// 裸 `user <name> off`（C# :482 AreEqual 全等等价）
#[test]
fn setuser_reset_user_to_bare_off() {
  let (_dir, store) = open_test_store("acl-reset.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  assert_eq!(
    call(
      &mut s,
      &[
        b"ACL",
        b"SETUSER",
        b"userA",
        b"on",
        b">passw0rd",
        b"+@admin"
      ]
    ),
    b"+OK\r\n"
  );
  // C# :460-469 — 三要素均已在位
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains(&format!("user userA on #{DUMMY_PASSWORD_HASH}")) && list.contains("+@admin"),
    "reset 前应含 on/哈希/+@admin: {list}"
  );

  assert_eq!(
    call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"reset"]),
    b"+OK\r\n"
  );
  // C# :482 — 行内容恰为 `user userA off`：bulk 头长即全长，等形唯一
  let line = "user userA off";
  let bare = format!("${}\r\n{line}\r\n", line.len());
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains(&bare),
    "reset 后 LIST 行应恰好为 `{line}`: {list}"
  );
}

/// SetUserTests.cs:41/66/88 — 受保护 default 用户三臂：口令化 default 下
/// 新连接未认证、ACL 命令面 NOAUTH；单参 AUTH 隐式登录 default；双参
/// AUTH 显式命名 default 登录
#[test]
fn protected_default_user_login_and_error_arms() {
  let (_dir, store) = open_test_store("acl-protected-default.db").unwrap();
  let acl = Arc::new(AccessControlList::new(DUMMY_PASSWORD).unwrap());
  let mut s = acl_session(&acl, &store);

  // :50-59 — 未认证会话执行 ACL LIST 必 NOAUTH（ACL 族无免认证豁免）
  assert_eq!(
    call(&mut s, &[b"ACL", b"LIST"]),
    b"-NOAUTH Authentication required.\r\n"
  );

  // :75-81 — 仅口令单参 AUTH：隐式落 default 登录，WHOAMI 回显 default
  assert_eq!(
    call(&mut s, &[b"AUTH", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(call(&mut s, &[b"ACL", b"WHOAMI"]), b"$7\r\ndefault\r\n");

  // :97-103 — 新连接双参 AUTH default <口令>：显式命名登录
  let mut s2 = acl_session(&acl, &store);
  assert_eq!(
    call(&mut s2, &[b"AUTH", b"default", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
  assert_eq!(call(&mut s2, &[b"ACL", b"WHOAMI"]), b"$7\r\ndefault\r\n");
}

/// GetUserTests.cs:113 — GETUSER 多用户：三户并存（default 兜底 + 两命名
/// 户不同口令/权限）点查各归其主，commands 回显为加序合流全串
#[test]
fn get_user_multi_user() {
  let (_dir, store) = open_test_store("acl-getuser-multi.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = acl_session(&acl, &store);

  // C# :22 MultiCommandList + :115-117 三户定义以 SETUSER 逐户落存储
  const MULTI_CMDS: &str =
    "+get +set +setex +decr +decrby +incr +incrby +del +unlink +flushdb +latency";
  let pw_a = format!(">{DUMMY_PASSWORD}");
  let pw_b = format!(">{DUMMY_PASSWORD_B}");
  let mut args_a: Vec<&[u8]> = vec![b"ACL", b"SETUSER", b"userA", b"on", pw_a.as_bytes()];
  args_a.extend(MULTI_CMDS.split(' ').map(str::as_bytes));
  assert_eq!(call(&mut s, &args_a), b"+OK\r\n");
  assert_eq!(
    call(
      &mut s,
      &[
        b"ACL",
        b"SETUSER",
        b"userB",
        b"on",
        pw_b.as_bytes(),
        b"+set"
      ]
    ),
    b"+OK\r\n"
  );

  // B 侧哈希经 AclPassword 单源复算（口令不同必异于 A 侧在册哈希）
  let hash_b = AclPassword::from_string(DUMMY_PASSWORD_B).to_string();
  assert_ne!(hash_b, DUMMY_PASSWORD_HASH);

  // C# :130-136 — GETUSER userA：on 旗标 + A 口令哈希 + 合流命令全串
  let frame = String::from_utf8(call(&mut s, &[b"ACL", b"GETUSER", b"userA"])).unwrap();
  assert!(
    frame.starts_with("*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n"),
    "flags 臂形态不符: {frame}"
  );
  assert!(
    frame.contains(&format!("$65\r\n#{DUMMY_PASSWORD_HASH}\r\n")),
    "passwords 须回显 A 侧哈希: {frame}"
  );
  assert!(
    frame.contains(&format!("${}\r\n{MULTI_CMDS}\r\n", MULTI_CMDS.len())),
    "commands 须为加序合流全串: {frame}"
  );
  assert!(
    !frame.contains(&hash_b),
    "A 侧回显不得混入 B 侧口令哈希: {frame}"
  );

  // 多用户点查择主：GETUSER userB 仅含自身 +set 权限与 B 侧哈希
  let frame = String::from_utf8(call(&mut s, &[b"ACL", b"GETUSER", b"userB"])).unwrap();
  assert!(
    frame.starts_with("*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n"),
    "flags 臂形态不符: {frame}"
  );
  assert!(
    frame.contains(&format!("$65\r\n#{hash_b}\r\n")),
    "passwords 须回显 B 侧哈希: {frame}"
  );
  assert!(
    frame.contains("$8\r\ncommands\r\n$4\r\n+set\r\n"),
    "B 侧权限面只应有 +set: {frame}"
  );
  assert!(
    !frame.contains(DUMMY_PASSWORD_HASH),
    "B 侧回显不得混入 A 侧口令哈希: {frame}"
  );
}

// ============================================================================
// r310：garnet C# 行为面移植②——并发 AUTH/口令哈希（ParallelTests.cs）+
// 自定义命令按名 ACL（CustomCommandACLTests.cs，34 案语义分级：会话/RESP
// 行为面全移；纯 parse 级仅补差集；C# 动态注册 / 反射注入 / 启动配置面臂
// 不移，逐臂见各例注释与移交申报）
// ============================================================================

/// AclTest.cs:34 ParallelAuthTest（C# TestCase(128, 2048)，本侧等总量降规为
/// 8 线程 × 8 批 × 16 对 = 2048 次成功 + 2048 次失败 AUTH）：多会话并行交
/// 替「对口令 / 错口令」AUTH，成功臂恒 +OK、失败臂恒 WRONGPASS，风暴后服务
/// 端态无损（LIST 哈希在位、新会话 AUTH 仍过）
#[test]
fn parallel_auth_mixed_success_failure_keeps_state_intact() {
  const THREADS: usize = 8;
  const BATCHES: usize = 8;
  const PAIRS: usize = 16;
  let (_dir, store) = open_test_store("acl-parallel-auth.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // C# :43-44 — 风暴前串行建户 ACL SETUSER userA on >DummyPassword
  {
    let mut s = acl_session(&acl, &store);
    assert_eq!(
      call(&mut s, &[b"ACL", b"SETUSER", b"userA", b"on", b">passw0rd"]),
      b"+OK\r\n"
    );
  }

  // C# :57-59 — 每会话交替提交成功/失败两 AUTH：组一批流水帧与期望并回串
  let wrongpass = err_frame(cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
  let mut req = Vec::new();
  let mut expect = Vec::new();
  for _ in 0..PAIRS {
    req.extend(resp_frame(&[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]));
    expect.extend_from_slice(b"+OK\r\n");
    req.extend(resp_frame(&[
      b"AUTH",
      b"userA",
      DUMMY_PASSWORD_B.as_bytes(),
    ]));
    expect.extend_from_slice(&wrongpass);
  }

  // C# :48 — Parallel.ForAsync 对齐起跑（Barrier 替线程池会合点）
  let barrier = Arc::new(Barrier::new(THREADS));
  let handles: Vec<_> = (0..THREADS)
    .map(|_| {
      let store = Arc::clone(&store);
      let acl = Arc::clone(&acl);
      let barrier = Arc::clone(&barrier);
      let req = req.clone();
      let expect = expect.clone();
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          let mut s = acl_session(&acl, &store);
          barrier.wait();
          for _ in 0..BATCHES {
            assert!(feed(&mut s, &req).is_some(), "批帧应被完整消费");
            // C# :67-76 — 成功臂全 +OK、失败臂全 WRONGPASS（逐帧序比对）
            assert_eq!(drain_output(&mut s), expect);
          }
        });
      })
    })
    .collect();
  for h in handles {
    h.join().unwrap();
  }

  // 「不腐化服务端态」终验：用户行哈希原样在位，全新会话 AUTH 仍放行
  let mut s = acl_session(&acl, &store);
  let list = String::from_utf8(call(&mut s, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains(&format!("user userA on #{DUMMY_PASSWORD_HASH}")),
    "并发 AUTH 后用户行应无损: {list}"
  );
  let mut fresh = acl_session(&acl, &store);
  assert_eq!(
    call(&mut fresh, &[b"AUTH", b"userA", DUMMY_PASSWORD.as_bytes()]),
    b"+OK\r\n"
  );
}

/// ParallelTests.cs:89 ParallelPasswordHashTest（C# 128×2048，本侧降规
/// 8×512）：多线程并发口令哈希，全部结果须与串行基准逐字节一致（共享哈希
/// 态无漂移即 C# 例的全部断言面）
#[test]
fn parallel_password_hash_stable_across_threads() {
  const THREADS: usize = 8;
  const ITERS: usize = 512;
  // 串行基准：userA 口令哈希即 AclTest.cs 在册常量；B 侧口令哈希现算比对值
  let baseline_b = AclPassword::from_string(DUMMY_PASSWORD_B).to_string();
  let barrier = Arc::new(Barrier::new(THREADS));
  let handles: Vec<_> = (0..THREADS)
    .map(|_| {
      let barrier = Arc::clone(&barrier);
      let baseline_b = baseline_b.clone();
      thread::spawn(move || {
        barrier.wait();
        for _ in 0..ITERS {
          assert_eq!(
            AclPassword::from_string(DUMMY_PASSWORD).to_string(),
            DUMMY_PASSWORD_HASH
          );
          assert_eq!(
            AclPassword::from_string(DUMMY_PASSWORD_B).to_string(),
            baseline_b
          );
        }
      })
    })
    .collect();
  for h in handles {
    h.join().unwrap();
  }
}

/// CustomCommandACLTests.cs:93 — 语法合法未知名落自定义命令允许侧
/// （而非 CommandDoesNotExist 抛出）
#[test]
fn parser_unknown_valid_name_lands_custom_allow() {
  let user = AclParser::parse_acl_rule("user alice on +json.get").unwrap();
  assert!(user.custom_commands_allowed().contains("JSON.GET"));
  assert!(!user.custom_commands_denied().contains("JSON.GET"));
}

/// CustomCommandACLTests.cs:105/114/121 — 畸形自定义名（内嵌制表符 /
/// 非 ASCII / 非字母数字首字符）一律落「命令不存在」：自定义名回落门拒收
/// 标点与空白形（C# AclCommandDoesNotExistException 对位）
#[test]
fn parser_rejects_malformed_custom_names() {
  let mut user = User::new("alice".into());
  for op in [
    "+bad\tname",
    "+js\u{F6}n.get",
    "+-foo",
    "+.foo",
    "+_foo",
    "+|foo",
  ] {
    assert!(
      matches!(
        AclParser::apply_acl_op_to_user(&mut user, op),
        Err(AclError::CommandDoesNotExist(_))
      ),
      "畸形自定义名 {op:?} 应被拒收"
    );
  }
}

/// CustomCommandACLTests.cs:134/147 — 同名允许 / 拒绝的序收敛双向：
/// 后写胜出（+x -x 只落拒绝侧；-x +x 只落允许侧）
#[test]
fn parser_same_name_allow_deny_last_write_wins() {
  // C# :134 — +@custom +json.set -json.set：JSON.SET 仅在拒绝侧
  let mut u = User::new("alice".into());
  AclParser::apply_acl_op_to_user(&mut u, "+@custom").unwrap();
  AclParser::apply_acl_op_to_user(&mut u, "+json.set").unwrap();
  AclParser::apply_acl_op_to_user(&mut u, "-json.set").unwrap();
  assert!(!u.custom_commands_allowed().contains("JSON.SET"));
  assert!(u.custom_commands_denied().contains("JSON.SET"));

  // C# :147 — -json.set +json.set：JSON.SET 仅在允许侧
  let mut v = User::new("bob".into());
  AclParser::apply_acl_op_to_user(&mut v, "-json.set").unwrap();
  AclParser::apply_acl_op_to_user(&mut v, "+json.set").unwrap();
  assert!(v.custom_commands_allowed().contains("JSON.SET"));
  assert!(!v.custom_commands_denied().contains("JSON.SET"));
}

/// CustomCommandACLTests.cs:159 — 三种大小写形规范化到同一大写名落允许
/// 侧（派发侧按 NameStr 匹配的前提）
#[test]
fn parser_custom_name_case_insensitive_normalization() {
  for (name, op) in [
    ("alice", "+JSON.GET"),
    ("bob", "+json.get"),
    ("carol", "+Json.Get"),
  ] {
    let mut u = User::new(name.into());
    AclParser::apply_acl_op_to_user(&mut u, op).unwrap();
    assert!(
      u.custom_commands_allowed().contains("JSON.GET"),
      "{op} 应规范化为 JSON.GET 入允许侧"
    );
  }
}

/// CustomCommandACLTests.cs:177/224 — describe_user 往返保留自定义按名
/// 允许 / 拒绝令牌：写出行重解析后权限集等价（ACL 持久化语义防线，整理
/// 掉具名令牌即静默改变重载语义）
#[test]
fn describe_user_roundtrip_preserves_custom_sets() {
  let mut u = User::new("alice".into());
  u.set_enabled(true);
  u.add_category(RespAclCategories::CUSTOM).unwrap();
  u.add_custom_command("MYDICTGET").unwrap();
  u.remove_custom_command("SETWPIFPGT").unwrap();
  u.add_custom_command("foo.bar").unwrap();
  u.remove_custom_command("json.get").unwrap();

  let desc = u.describe_user();
  let back = AclParser::parse_acl_rule(&desc).unwrap();
  assert!(
    u.copy_command_permission_set()
      .is_equivalent_to(&back.copy_command_permission_set()),
    "往返后权限集应等价: {desc}"
  );
  // C# :193-194 / :241-244 — 四条具名令牌均在位（允许侧大写规范化）
  assert!(back.custom_commands_allowed().contains("MYDICTGET"));
  assert!(back.custom_commands_allowed().contains("FOO.BAR"));
  assert!(!back.custom_commands_allowed().contains("JSON.GET"));
  assert!(back.custom_commands_denied().contains("SETWPIFPGT"));
  assert!(back.custom_commands_denied().contains("JSON.GET"));
}

/// CustomCommandACLTests.cs:207 — User API 直调拒收可毒化持久化描述的名
/// （内嵌空白 / 混入规则令牌 / 空串 / 首字符标点），合法名放行；C# :198
/// null 入参臂在 rust 类型系统不可表达，不移（差集申报）
#[test]
fn user_api_rejects_poisoning_custom_names() {
  let mut u = User::new("alice".into());
  for bad in ["foo bar", "foo +@all", "", ".leading_dot"] {
    assert!(u.add_custom_command(bad).is_err(), "add 应拒收 {bad:?}");
    if !bad.is_empty() {
      assert!(
        u.remove_custom_command(bad).is_err(),
        "remove 应拒收 {bad:?}"
      );
    }
  }
  // C# :220 — 合法名不受牵连
  assert!(u.add_custom_command("json.set").is_ok());
}

/// CustomCommandACLTests.cs:272/285/297/307/320 — 按名门裁决矩阵：具名拒
/// 绝压过 +@custom 分类位、具名允许补位图缺位、+@all 哨兵恒放行、哨兵侧撤
/// 同名经物化生效、新用户失败关闭。C# 的 CustomRawStringCmd / CustomProcedure
/// 枚举臂随动态注册层删除不移，本块钉 Customobjcmd 形
#[test]
fn user_custom_gate_decision_matrix() {
  // C# :272 — +@custom 后 -具名：具名拒绝赢过分类位，同族余名仍走分类位
  let mut u = User::new("alice".into());
  u.add_category(RespAclCategories::CUSTOM).unwrap();
  u.remove_custom_command("SETWPIFPGT").unwrap();
  assert!(!u.can_access_custom_command(RespCommand::Customobjcmd, "SETWPIFPGT"));
  assert!(u.can_access_custom_command(RespCommand::Customobjcmd, "MYDICTGET"));

  // C# :285 — -@all 后 +具名：位图缺位仍由具名允许放行
  let mut v = User::new("bob".into());
  v.remove_category(RespAclCategories::ALL).unwrap();
  v.add_custom_command("SETWPIFPGT").unwrap();
  assert!(v.can_access_custom_command(RespCommand::Customobjcmd, "SETWPIFPGT"));
  assert!(!v.can_access_custom_command(RespCommand::Customobjcmd, "MYDICTGET"));

  // C# :297/:320 — +@all 哨兵对任意按名恒放行；裸新用户失败关闭
  let mut w = User::new("carol".into());
  w.add_category(RespAclCategories::ALL).unwrap();
  assert!(w.can_access_custom_command(RespCommand::Customobjcmd, "ANYTHING.GOES"));
  assert!(
    !User::new("dave".into()).can_access_custom_command(RespCommand::Customobjcmd, "ANYTHING")
  );

  // C# :307 — +@all 后 -具名：哨兵物化后该名撤回、余者仍全放行
  w.remove_custom_command("SETWPIFPGT").unwrap();
  assert!(!w.can_access_custom_command(RespCommand::Customobjcmd, "SETWPIFPGT"));
  assert!(w.can_access_custom_command(RespCommand::Customobjcmd, "OTHER"));
}

/// CustomCommandACLTests.cs:328 — 等价性判定纳入按名集：位图同、仅差一条
/// 具名拒绝即不等价（描述整理丢拒绝令牌形态的显式检举）
#[test]
fn is_equivalent_to_considers_custom_sets() {
  let mut u1 = User::new("a".into());
  u1.add_category(RespAclCategories::CUSTOM).unwrap();
  let mut u2 = User::new("b".into());
  u2.add_category(RespAclCategories::CUSTOM).unwrap();
  u2.remove_custom_command("SETWPIFPGT").unwrap();
  assert!(
    !u1
      .copy_command_permission_set()
      .is_equivalent_to(&u2.copy_command_permission_set()),
    "按名拒绝集差异须打破等价判定"
  );
}

/// 注册面探针（对标 C# server.Register.NewCommand 的会话侧注入）：仅
/// JSON.SET 视为已登记
fn probe_registered(name: &str) -> bool {
  name.eq_ignore_ascii_case("JSON.SET")
}

/// ACL 派发门拒绝线面帧（AdminCommands.cs:CheckACLPermissions 拒绝臂）
const NOPERM_FRAME: &[u8] = b"-NOPERM this user has no permissions to run the command\r\n";

/// CustomCommandACLTests.cs:345/359/367 — SETUSER 注册门三臂：新增未知名
/// 失败关闭（-ERR Unknown custom command）、已登记名放行、语法非法名先于
/// 注册门被解析器拒收（本侧 ASCII 折叠形 '??' 与 C# :372 同臂拒）
#[test]
fn setuser_strict_registration_gate_arms() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let ctx = ctx_for(&auth, Some(probe_registered));
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-setuser-strict-gate");
  let store = storage.acl();

  // C# :349-355 — 未登记新增名 → 报错帧，零落账
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx,
    &store,
    &[b"alice", b"on", b">pw", b"+definitely_not_registered"],
    &mut out,
  ))
  .unwrap();
  assert!(
    String::from_utf8_lossy(&out).starts_with("-ERR Unknown custom command"),
    "未知名应失败关闭: {}",
    String::from_utf8_lossy(&out)
  );

  // C# :362-363 — 已登记名 → +OK
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx,
    &store,
    &[b"bob", b"on", b">pw", b"+json.set"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");

  // C# :372-376 — 非 ASCII 非法名先于注册门被解析器拒（CommandDoesNotExist）
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &ctx,
    &store,
    &[b"carol", b"on", b"+js\xC3\xB6n.get"],
    &mut out,
  ))
  .unwrap();
  assert!(
    String::from_utf8_lossy(&out).contains("does not exist"),
    "非法名应落「命令不存在」臂: {}",
    String::from_utf8_lossy(&out)
  );

  // 行为面：bob 记录 JSON.SET 落允许侧（失败臂零残留由 alice 无记录佐证）
  let bytes = block_on(store.read(0, b"bob")).unwrap().unwrap();
  let bob = User::from_bytes(&bytes).unwrap();
  assert!(bob.custom_commands_allowed().contains("JSON.SET"));
  assert!(block_on(store.read(0, b"alice")).unwrap().is_none());
}

/// CustomCommandACLTests.cs:412/446 — 宽松装载名换向不重验注册门：宽容侧
/// 种入允许 / 拒绝名（对标 ACL 文件先于模块装载的松载入形态，C# 借反射种
/// 入，本侧 ctx 门位 None 即文件装载同侧），严门再切回向均 OK；全新未登记
/// 名仍拒
#[test]
fn setuser_allows_swap_of_loose_loaded_names() {
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let auth = GarnetAclAuthenticator::new(Arc::clone(&acl));
  let lenient = ctx_for(&auth, None);
  let strict = ctx_for(&auth, Some(probe_registered));
  let session = RespServerSession::default();
  let storage = TestAclStore::open("acl-loose-swap");
  let store = storage.acl();

  // 宽容种入：alice 允许侧、bob 拒绝侧各挂一枚未登记名
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &lenient,
    &store,
    &[b"alice", b"on", b"+loose_loaded_probe"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &lenient,
    &store,
    &[b"bob", b"on", b"-loose_loaded_probe2"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");

  // C# :436-442 — alice 允许→拒绝经严门切回向 OK，集迁移
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &strict,
    &store,
    &[b"alice", b"-loose_loaded_probe"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let bytes = block_on(store.read(0, b"alice")).unwrap().unwrap();
  let alice = User::from_bytes(&bytes).unwrap();
  assert!(
    alice
      .custom_commands_denied()
      .contains("LOOSE_LOADED_PROBE")
  );
  assert!(
    !alice
      .custom_commands_allowed()
      .contains("LOOSE_LOADED_PROBE")
  );

  // C# :461-467 — bob 拒绝→允许同形
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &strict,
    &store,
    &[b"bob", b"+loose_loaded_probe2"],
    &mut out,
  ))
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let bytes = block_on(store.read(0, b"bob")).unwrap().unwrap();
  let bob = User::from_bytes(&bytes).unwrap();
  assert!(
    bob
      .custom_commands_allowed()
      .contains("LOOSE_LOADED_PROBE2")
  );
  assert!(!bob.custom_commands_denied().contains("LOOSE_LOADED_PROBE2"));

  // 对照臂：既非既存也非登记的全新名在严门下仍失败关闭
  let mut out = Vec::new();
  block_on(session.network_acl_set_user(
    &strict,
    &store,
    &[b"alice", b"+brand_new_probe"],
    &mut out,
  ))
  .unwrap();
  assert!(
    String::from_utf8_lossy(&out).starts_with("-ERR Unknown custom command"),
    "全新未登记名仍须拒: {}",
    String::from_utf8_lossy(&out)
  );
}

/// CustomCommandACLTests.cs:473/533 — 派发侧具名拒绝赢过 +@custom 分类位：
/// JSON.SET 回 NOPERM、同族自定义命令仍走分类位不被 ACL 拒；ACL LIST 行回
/// 显 -json.set 具名令牌（SETUSER 经线面走本仓静态清单严门，具名令牌用真
/// 在位的扩展命令名）
#[test]
fn dispatch_custom_deny_beats_category_and_lists_token() {
  let (_dir, store) = open_test_store("acl-dispatch-deny-cat.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut admin = acl_session(&acl, &store);

  // C# :477 — alice：+@custom -json.set
  assert_eq!(
    call(
      &mut admin,
      &[
        b"ACL",
        b"SETUSER",
        b"alice",
        b"on",
        b">pw",
        b"+@custom",
        b"-json.set"
      ]
    ),
    b"+OK\r\n"
  );
  // C# :539-543 — LIST 的 alice 行含 -json.set 具名令牌
  let list = String::from_utf8(call(&mut admin, &[b"ACL", b"LIST"])).unwrap();
  assert!(
    list.contains("user alice on") && list.contains("-json.set"),
    "alice 行应回显 -json.set: {list}"
  );

  // C# :479 — alice 独立会话登录
  let mut a = acl_session(&acl, &store);
  assert_eq!(call(&mut a, &[b"AUTH", b"alice", b"pw"]), b"+OK\r\n");

  // C# :482-487 — SETWPIFPGT→本侧 JSON.SET 被具名拒绝压过分类位
  assert_eq!(call(&mut a, &[b"JSON.SET", b"k", b"$", b"1"]), NOPERM_FRAME);

  // C# :490-492 — 同族 MYDICTGET→JSON.GET 未被 ACL 拒（分类位放行；
  // 应答随数据面形态，钉「非 NOPERM」即对标 C# 的未拒断言）
  let out = call(&mut a, &[b"JSON.GET", b"missing"]);
  assert!(
    !out.starts_with(b"-NOPERM"),
    "JSON.GET 不应被 ACL 拒: {}",
    String::from_utf8_lossy(&out)
  );
}

/// CustomCommandACLTests.cs:496 — -@all 后仅 +具名允许单点放行：JSON.SET
/// 放行执行回 +OK，同族自定义命令无分类位回 NOPERM（SETWPIFPGT→JSON.SET、
/// MYDICTGET→JSON.GET 对位）
#[test]
fn dispatch_custom_allow_from_minus_all_grants_only_named() {
  let (_dir, store) = open_test_store("acl-dispatch-allow-none.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut admin = acl_session(&acl, &store);

  assert_eq!(
    call(
      &mut admin,
      &[
        b"ACL",
        b"SETUSER",
        b"alice",
        b"on",
        b">pw",
        b"-@all",
        b"+ping",
        b"+auth",
        b"+json.set"
      ]
    ),
    b"+OK\r\n"
  );
  let mut a = acl_session(&acl, &store);
  assert_eq!(call(&mut a, &[b"AUTH", b"alice", b"pw"]), b"+OK\r\n");

  // C# :506-507 — 具名允许的命令经派发链真实执行
  assert_eq!(
    call(&mut a, &[b"JSON.SET", b"bk", b"$", b"{\"x\":1}"]),
    b"+OK\r\n"
  );
  // C# :510-514 — 同族命令无 +mydictget / +@custom 不可达
  assert_eq!(call(&mut a, &[b"JSON.GET", b"bk"]), NOPERM_FRAME);
}

/// CustomCommandACLTests.cs:518 — SETUSER 令牌混大小写 + 线面大写命令名
/// 的派发匹配（规范化与匹配两侧均大小写不敏感）
#[test]
fn dispatch_custom_name_case_insensitive_on_wire() {
  let (_dir, store) = open_test_store("acl-dispatch-case.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut admin = acl_session(&acl, &store);

  assert_eq!(
    call(
      &mut admin,
      &[
        b"ACL",
        b"SETUSER",
        b"carol",
        b"on",
        b">pw",
        b"-@all",
        b"+ping",
        b"+auth",
        b"+JsOn.SeT"
      ]
    ),
    b"+OK\r\n"
  );
  let mut c = acl_session(&acl, &store);
  assert_eq!(call(&mut c, &[b"AUTH", b"carol", b"pw"]), b"+OK\r\n");
  assert_eq!(
    call(&mut c, &[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );
}

// r310 分级弃移申报（CustomCommandACLTests.cs 34 案，不移 9 案）：
// - :198 null 入参臂——rust &str 类型系统不可表达 null；
// - :248 集合直 Contains 的大小写不敏感 comparer 形——本侧集存规范大写名、
//   不敏感匹配收口于 can_run_custom_command 单源，已由 :134/:272 各臂覆盖；
// - :380 commandInfo 双字典伪影臂——本侧静态清单单点判定，无第二字典；
// - :412/:446 的反射注入口——语义面已移（setuser_allows_swap_of_loose_loaded_names），
//   C# 内部反射管道本身不移；
// - :547/:565/:577/:593 CustomTxn/CustomProcedure 派发臂——动态注册层随
//   wnode/src/resp/custom_objects.rs 表头注记删除，枚举占位三条同删在册
//   （wresp catalog 注释），无对应物；
// - :607/:632 启动期 aclFile + aclStrictCustomCommands 校验臂——本侧无
//   aclFile / aclStrictCustomCommands 启动配置面（配置目录零命中），无落点。
