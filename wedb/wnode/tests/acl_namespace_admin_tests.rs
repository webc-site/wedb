use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wacl::{
  AccessControlList,
  user::{parse_user_namespace, validate_username},
};
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

fn send_cmd(consumer: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut cmd_frame = Vec::new();
  cmd_frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
  for arg in args {
    cmd_frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
    cmd_frame.extend_from_slice(arg);
    cmd_frame.extend_from_slice(b"\r\n");
  }
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&cmd_frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  consumer.try_consume_messages_into(&mut resp);
  resp
}

/// 验证只有 namespace 0 有超管权限，非 0 命名空间越权管理必定被拒
#[test]
fn test_acl_admin_super_privilege_namespace_isolation() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let acl = Arc::new(AccessControlList::new("")?);
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("acl_admin.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      },
    )?
    .with_acl(Arc::clone(&acl));

    // 1. 获取一个默认处于 namespace 0 的会话（超管会话）
    let mut consumer_ns0 = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    assert_eq!(
      consumer_ns0.session().namespace,
      0,
      "新连接初始位于命名空间 0"
    );

    // 超管可以在命名空间 1 下创建用户 bob
    let out = send_cmd(
      &mut consumer_ns0,
      &[b"ACL", b"SETUSER", b"1#bob", b"on", b">bobpw", b"+@all"],
    );
    assert_eq!(out, b"+OK\r\n", "超管可以跨空间创建 1#bob 用户");

    // 超管可以在命名空间 2 下创建用户 charlie
    let out = send_cmd(
      &mut consumer_ns0,
      &[
        b"ACL",
        b"SETUSER",
        b"2#charlie",
        b"on",
        b">charliepw",
        b"+@all",
      ],
    );
    assert_eq!(out, b"+OK\r\n", "超管可以跨空间创建 2#charlie 用户");

    // 超管可以查看 1#bob
    let out = send_cmd(&mut consumer_ns0, &[b"ACL", b"GETUSER", b"1#bob"]);
    let out_str = String::from_utf8_lossy(&out);
    assert!(out_str.contains("flags"), "超管可以 GETUSER 1#bob");
    assert!(out_str.contains("on"), "用户已启用");

    // 2. 获取另一个会话，使用 1#bob 登录，该会话自动切换绑定为 namespace 1
    let mut consumer_ns1 = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    assert_eq!(consumer_ns1.session().namespace, 0);

    let auth_res = send_cmd(&mut consumer_ns1, &[b"AUTH", b"1#bob", b"bobpw"]);
    assert_eq!(auth_res, b"+OK\r\n", "1#bob 认证成功");
    assert_eq!(
      consumer_ns1.session().namespace,
      1,
      "认证后会话命名空间切换为 1"
    );

    // 3. 验证非 0 命名空间用户的超管越权防御
    // 3.1 尝试跨空间创建 0#hacker
    let out = send_cmd(
      &mut consumer_ns1,
      &[
        b"ACL",
        b"SETUSER",
        b"0#hacker",
        b"on",
        b">hackerpw",
        b"+@all",
      ],
    );
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止创建 0 空间用户"
    );

    // 3.2 尝试跨空间创建 2#hacker
    let out = send_cmd(
      &mut consumer_ns1,
      &[
        b"ACL",
        b"SETUSER",
        b"2#hacker",
        b"on",
        b">hackerpw",
        b"+@all",
      ],
    );
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止创建其他空间用户"
    );

    // 3.3 尝试显式携带本命名空间前缀 1#david
    let out = send_cmd(
      &mut consumer_ns1,
      &[b"ACL", b"SETUSER", b"1#david", b"on", b">davidpw", b"+@all"],
    );
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止在命令中使用带有 # 的用户名"
    );

    // 3.4 尝试越权查询 0#default
    let out = send_cmd(&mut consumer_ns1, &[b"ACL", b"GETUSER", b"0#default"]);
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止查看 0 空间用户"
    );

    // 3.5 尝试越权删除 0#default
    let out = send_cmd(&mut consumer_ns1, &[b"ACL", b"DELUSER", b"0#default"]);
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止删除 0 空间用户"
    );

    // 3.6 尝试越权删除 2#charlie
    let out = send_cmd(&mut consumer_ns1, &[b"ACL", b"DELUSER", b"2#charlie"]);
    assert_eq!(
      out, b"-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n",
      "非 0 用户禁止删除其他空间用户"
    );

    // 4. 验证非 0 命名空间管理本地用户（不带 #）
    let out = send_cmd(
      &mut consumer_ns1,
      &[b"ACL", b"SETUSER", b"subuser", b"on", b">subpw", b"+@all"],
    );
    assert_eq!(out, b"+OK\r\n", "非 0 用户可以管理本命名空间本地用户");

    let out = send_cmd(&mut consumer_ns1, &[b"ACL", b"GETUSER", b"subuser"]);
    assert!(
      String::from_utf8_lossy(&out).contains("flags"),
      "可以成功查询本地用户"
    );

    let out = send_cmd(&mut consumer_ns1, &[b"ACL", b"DELUSER", b"subuser"]);
    assert_eq!(out, b":1\r\n", "可以成功删除本地用户");

    // 5. 超管可以在 ns 0 删除 1#bob
    let out = send_cmd(&mut consumer_ns0, &[b"ACL", b"DELUSER", b"1#bob"]);
    assert_eq!(out, b":1\r\n", "超管可以删除 1#bob");

    aok::OK
  })
}

/// 验证登录成功后命名空间绑定与底层存储数据完全物理隔离
#[test]
fn test_acl_login_namespace_storage_isolation() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let acl = Arc::new(AccessControlList::new("")?);
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("acl_storage_iso.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      },
    )?
    .with_acl(Arc::clone(&acl));

    // ns 0 会话创建 10#tenant_user
    let mut consumer_ns0 = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    let out = send_cmd(
      &mut consumer_ns0,
      &[
        b"ACL",
        b"SETUSER",
        b"10#tenant_user",
        b"on",
        b">secret",
        b"+@all",
      ],
    );
    assert_eq!(out, b"+OK\r\n");

    // ns 0 写入 key: shared_key = "data_ns_0"
    let out = send_cmd(&mut consumer_ns0, &[b"SET", b"shared_key", b"data_ns_0"]);
    assert_eq!(out, b"+OK\r\n");

    // 会话 2 登录 10#tenant_user
    let mut consumer_ns10 = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    let out = send_cmd(&mut consumer_ns10, &[b"AUTH", b"10#tenant_user", b"secret"]);
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(consumer_ns10.session().namespace, 10);

    // 会话 2 查 shared_key：不可见（nil）
    let out = send_cmd(&mut consumer_ns10, &[b"GET", b"shared_key"]);
    assert_eq!(out, b"$-1\r\n", "命名空间 10 无法读到命名空间 0 的数据");

    // 会话 2 写入 key: shared_key = "data_ns_10"
    let out = send_cmd(&mut consumer_ns10, &[b"SET", b"shared_key", b"data_ns_10"]);
    assert_eq!(out, b"+OK\r\n");

    // 会话 2 读回自己的数据
    let out = send_cmd(&mut consumer_ns10, &[b"GET", b"shared_key"]);
    assert_eq!(out, b"$10\r\ndata_ns_10\r\n");

    // 会话 1 读自己的数据依然是 data_ns_0
    let out = send_cmd(&mut consumer_ns0, &[b"GET", b"shared_key"]);
    assert_eq!(out, b"$9\r\ndata_ns_0\r\n");

    aok::OK
  })
}

/// 验证用户名禁止包含 '#' 的约束（包括底层解析与 ACL 命令链路）
#[test]
fn test_acl_username_cannot_contain_hash() -> aok::Result<()> {
  // 1. 底层函数级校验
  assert!(validate_username("alice").is_ok());
  assert!(validate_username("alice#bob").is_err());
  assert!(parse_user_namespace("0#alice").is_ok());
  assert!(parse_user_namespace("0#alice#bob").is_err());
  assert!(parse_user_namespace("abc#alice").is_err());

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let acl = Arc::new(AccessControlList::new("")?);
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("acl_hash_check.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      },
    )?
    .with_acl(Arc::clone(&acl));

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // 2. 超管在 ns 0 下尝试创建基础用户名包含 '#' 的用户：0#alice#bob
    let out = send_cmd(
      &mut consumer,
      &[b"ACL", b"SETUSER", b"0#alice#bob", b"on", b">pw", b"+@all"],
    );
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("cannot contain '#'"),
      "基础用户名带 '#' 必须报错: {out_str}"
    );

    // 3. 跨空间基础用户名包含 '#'：1#bob#charlie
    let out = send_cmd(
      &mut consumer,
      &[
        b"ACL",
        b"SETUSER",
        b"1#bob#charlie",
        b"on",
        b">pw",
        b"+@all",
      ],
    );
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("cannot contain '#'"),
      "跨空间基础用户名带 '#' 必须报错: {out_str}"
    );

    // 4. 不以合法整数开头的带 '#' 用户名：foo#bar
    let out = send_cmd(
      &mut consumer,
      &[b"ACL", b"SETUSER", b"foo#bar", b"on", b">pw", b"+@all"],
    );
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("Invalid namespace prefix"),
      "非数字前缀带 '#' 必须报错: {out_str}"
    );

    // 5. GETUSER 包含非法 '#'
    let out = send_cmd(&mut consumer, &[b"ACL", b"GETUSER", b"0#alice#bob"]);
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("cannot contain '#'"),
      "GETUSER 基础用户名带 '#' 必须报错: {out_str}"
    );

    // 6. DELUSER 包含非法 '#'
    let out = send_cmd(&mut consumer, &[b"ACL", b"DELUSER", b"0#alice#bob"]);
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("cannot contain '#'"),
      "DELUSER 基础用户名带 '#' 必须报错: {out_str}"
    );

    aok::OK
  })
}

async fn send_slow_cmd(consumer: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = send_cmd(consumer, args);
  if let Some(slow) = consumer.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// 验证多租户下 FLUSHALL 与 FLUSHDB 的命名空间隔离
#[test]
fn test_flushall_multitenant_namespace_isolation() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let acl = Arc::new(AccessControlList::new("")?);
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("multitenant_flush.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      },
    )?
    .with_acl(Arc::clone(&acl));

    // 1. ns 0 创建租户账号
    let mut consumer_ns0 = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    let out = send_cmd(
      &mut consumer_ns0,
      &[b"ACL", b"SETUSER", b"1#t1", b"on", b">p1", b"+@all"],
    );
    assert_eq!(out, b"+OK\r\n");
    let out = send_cmd(
      &mut consumer_ns0,
      &[b"ACL", b"SETUSER", b"2#t2", b"on", b">p2", b"+@all"],
    );
    assert_eq!(out, b"+OK\r\n");

    // ns 0 写入数据到 db 0 和 db 1
    send_cmd(&mut consumer_ns0, &[b"SET", b"k0", b"v0"]);
    send_cmd(&mut consumer_ns0, &[b"SELECT", b"1"]);
    send_cmd(&mut consumer_ns0, &[b"SET", b"k0_db1", b"v0_db1"]);

    // 2. 租户 1 登录并写入 db 0 和 db 1
    let mut consumer_t1 = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    send_cmd(&mut consumer_t1, &[b"AUTH", b"1#t1", b"p1"]);
    send_cmd(&mut consumer_t1, &[b"SET", b"k1", b"v1"]);
    send_cmd(&mut consumer_t1, &[b"SELECT", b"1"]);
    send_cmd(&mut consumer_t1, &[b"SET", b"k1_db1", b"v1_db1"]);

    // 3. 租户 2 登录并写入 db 0 和 db 1
    let mut consumer_t2 = provider
      .get_session(WireFormat::Ascii, 3)
      .expect("会话创建成功");
    send_cmd(&mut consumer_t2, &[b"AUTH", b"2#t2", b"p2"]);
    send_cmd(&mut consumer_t2, &[b"SET", b"k2", b"v2"]);
    send_cmd(&mut consumer_t2, &[b"SELECT", b"1"]);
    send_cmd(&mut consumer_t2, &[b"SET", b"k2_db1", b"v2_db1"]);

    // 4. 租户 1 执行 FLUSHALL（必须只清空租户 1 的全部数据库，不影响租户 2 和 ns 0）
    let out = send_slow_cmd(&mut consumer_t1, &[b"FLUSHALL"]).await;
    assert_eq!(out, b"+OK\r\n", "租户 1 FLUSHALL 成功返回 +OK");

    // 验证租户 1 的 db 1 和 db 0 均已清空
    let out = send_cmd(&mut consumer_t1, &[b"GET", b"k1_db1"]);
    assert_eq!(out, b"$-1\r\n", "租户 1 的 db 1 数据被清空");
    send_cmd(&mut consumer_t1, &[b"SELECT", b"0"]);
    let out = send_cmd(&mut consumer_t1, &[b"GET", b"k1"]);
    assert_eq!(out, b"$-1\r\n", "租户 1 的 db 0 数据被清空");

    // 验证租户 2 的数据完好无损
    let out = send_cmd(&mut consumer_t2, &[b"GET", b"k2_db1"]);
    assert_eq!(out, b"$6\r\nv2_db1\r\n", "租户 2 的 db 1 数据不受影响");
    send_cmd(&mut consumer_t2, &[b"SELECT", b"0"]);
    let out = send_cmd(&mut consumer_t2, &[b"GET", b"k2"]);
    assert_eq!(out, b"$2\r\nv2\r\n", "租户 2 的 db 0 数据不受影响");

    // 验证 ns 0 的数据完好无损
    let out = send_cmd(&mut consumer_ns0, &[b"GET", b"k0_db1"]);
    assert_eq!(out, b"$6\r\nv0_db1\r\n", "ns 0 的 db 1 数据不受影响");
    send_cmd(&mut consumer_ns0, &[b"SELECT", b"0"]);
    let out = send_cmd(&mut consumer_ns0, &[b"GET", b"k0"]);
    assert_eq!(out, b"$2\r\nv0\r\n", "ns 0 的 db 0 数据不受影响");

    // 5. 租户 2 在 db 0 执行 FLUSHDB（只清空租户 2 的 db 0）
    let out = send_slow_cmd(&mut consumer_t2, &[b"FLUSHDB"]).await;
    assert_eq!(out, b"+OK\r\n");
    let out = send_cmd(&mut consumer_t2, &[b"GET", b"k2"]);
    assert_eq!(out, b"$-1\r\n", "租户 2 的 db 0 已清空");
    send_cmd(&mut consumer_t2, &[b"SELECT", b"1"]);
    let out = send_cmd(&mut consumer_t2, &[b"GET", b"k2_db1"]);
    assert_eq!(out, b"$6\r\nv2_db1\r\n", "租户 2 的 db 1 依然完好");

    // 6. ns 0 超管执行 FLUSHALL，全域物理截断
    let out = send_slow_cmd(&mut consumer_ns0, &[b"FLUSHALL"]).await;
    assert_eq!(out, b"+OK\r\n");
    let out = send_cmd(&mut consumer_ns0, &[b"GET", b"k0"]);
    assert_eq!(out, b"$-1\r\n", "ns 0 的数据在超管 FLUSHALL 后被清空");
    let out = send_cmd(&mut consumer_t2, &[b"GET", b"k2_db1"]);
    assert_eq!(out, b"$-1\r\n", "租户 2 的数据在超管 FLUSHALL 后被清空");

    // 7. 非 0 租户尝试 UNSAFETRUNCATELOG 必须被门禁拦截
    let out = send_slow_cmd(&mut consumer_t1, &[b"FLUSHALL", b"UNSAFETRUNCATELOG"]).await;
    assert!(
      String::from_utf8_lossy(&out).contains("only namespace 0 can truncate log"),
      "非 0 租户尝试物理截断必须报错拒绝"
    );

    aok::OK
  })
}
