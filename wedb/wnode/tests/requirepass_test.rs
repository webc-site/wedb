//! requirepass 认证链路集成测试
//!
//! 验证 StorageSessionProvider 与 RespSessionConsumer 的 requirepass 配置与 ACL 拦截行为：
//! 1. 未配置 requirepass 时：默认全放行，无密码阻拦。
//! 2. 配置了 requirepass 时：未认证拦截为 -NOAUTH、密码错误拒绝为 -WRONGPASS、正确密码通过并放行后续命令。
//! 3. 覆盖单参数 AUTH <password> 与双参数 AUTH default <password> 两种形式。
//! 4. RespSessionConsumer::attach_acl 显式注入链路。

use std::sync::Arc;

use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wedb_test::test_store_config;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};

const DB_NAME: &str = "test_requirepass.db";

/// 未配置 requirepass 保持默认全放行
#[test]
fn session_without_requirepass_allows_all() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    assert!(provider.acl.is_none());

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // 未认证 PING 直接放行
    let req = b"*1\r\n$4\r\nPING\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+PONG\r\n");

    // SET 命令直接放行
    resp.clear();
    let req = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+OK\r\n");

    aok::OK
  })
}

/// 配置空字符串密码等价于未配置
#[test]
fn session_with_empty_requirepass_allows_all() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?
    .with_requirepass(Some(""));

    assert!(provider.acl.is_none());

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    let req = b"*1\r\n$4\r\nPING\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+PONG\r\n");

    aok::OK
  })
}

/// 配置了 requirepass 时的完整认证拦截闭环：
/// 未 AUTH 拦截 -> 密码错误拒绝 -> 正确密码通过 -> 放行后续操作
/// 涵盖单参数 AUTH <password> 与双参数 AUTH <user> <password>
#[test]
fn session_with_requirepass_lifecycle() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?
    .with_requirepass(Some("my_secret_password"));

    assert!(provider.acl.is_some());

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    let mut resp = Vec::new();

    // 1. 未认证时执行 PING 拦截为 -NOAUTH
    let req = b"*1\r\n$4\r\nPING\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 2. 未认证时执行 SET 同样拦截为 -NOAUTH
    resp.clear();
    let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 3. AUTH 错误密码被拒绝（单参数形式）
    resp.clear();
    let req = b"*2\r\n$4\r\nAUTH\r\n$9\r\nwrongpass\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"-WRONGPASS Invalid password\r\n");

    // 4. AUTH 错误用户名或密码被拒绝（双参数形式）
    resp.clear();
    let req = b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\nwrongpass\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );

    // 5. 再次执行 PING 仍然被 -NOAUTH 拦截
    resp.clear();
    let req = b"*1\r\n$4\r\nPING\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 6. AUTH 正确密码成功通过（双参数 AUTH default <pass>）
    resp.clear();
    let req = b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$18\r\nmy_secret_password\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+OK\r\n");

    // 7. 认证成功后执行 PING 正常返回 +PONG
    resp.clear();
    let req = b"*1\r\n$4\r\nPING\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+PONG\r\n");

    // 8. 认证成功后执行 SET/GET 读写正常放行
    resp.clear();
    let req = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+OK\r\n");

    resp.clear();
    let req = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"$3\r\nbar\r\n");

    aok::OK
  })
}

/// 显式调用 RespSessionConsumer::attach_acl 委托测试
#[test]
fn consumer_attach_acl_delegation() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    let acl = Arc::new(AccessControlList::new("manual_pass", None)?);
    let auth = Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(acl))));
    consumer.attach_acl(auth, None);

    let mut resp = Vec::new();

    // 未认证 PING
    let req = b"*1\r\n$4\r\nPING\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // AUTH 成功
    resp.clear();
    let req = b"*2\r\n$4\r\nAUTH\r\n$11\r\nmanual_pass\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+OK\r\n");

    // 认证后 PING 放行
    resp.clear();
    let req = b"*1\r\n$4\r\nPING\r\n";
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());
    assert_eq!(resp, b"+PONG\r\n");

    aok::OK
  })
}
