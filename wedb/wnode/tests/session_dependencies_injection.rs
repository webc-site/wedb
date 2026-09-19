//! 会话依赖注入装配测试（SessionDependencies 生产链路与命令流验证）

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

const DB_NAME: &str = "session_deps_test.db";

#[test]
fn session_dependencies_full_injection_and_auth_flow() -> aok::Result<()> {
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
          Arc::new(api),
        ))
      },
    )?
    .with_requirepass(Some("topsecret"));

    let deps = provider.session_dependencies();
    assert!(
      deps.acl_authenticator.is_some(),
      "requirepass 模式下正确装配 ACL 认证器"
    );
    // 版本表单点实例透传：会话依赖与引擎宿主持同一张表（WATCH 栅栏唯一真源）
    assert!(
      Arc::ptr_eq(&deps.watch_version_map, &provider.watch_version_map),
      "WatchVersionMap 正常传递"
    );

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话应成功创建并注入依赖");

    // 1. 未鉴权时执行 PING，由于配置了 requirepass，必须返回 NOAUTH 错误
    let mut resp = Vec::new();
    let mut scratch = consumer.take_recv_scratch();
    scratch.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    consumer.return_recv_scratch(scratch);
    assert_eq!(consumer.try_consume_messages_into(&mut resp), Some(0));
    assert!(
      resp.starts_with(b"-NOAUTH"),
      "未认证前拒绝执行命令并返回 NOAUTH"
    );

    // 2. 发送正确的 AUTH 命令进行认证
    resp.clear();
    let mut scratch = consumer.take_recv_scratch();
    scratch.extend_from_slice(b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\ntopsecret\r\n");
    consumer.return_recv_scratch(scratch);
    assert_eq!(consumer.try_consume_messages_into(&mut resp), Some(0));
    assert_eq!(resp, b"+OK\r\n", "AUTH 成功返回 +OK");

    // 3. 认证通过后再次执行 PING，应返回 +PONG
    resp.clear();
    let mut scratch = consumer.take_recv_scratch();
    scratch.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    consumer.return_recv_scratch(scratch);
    assert_eq!(consumer.try_consume_messages_into(&mut resp), Some(0));
    assert_eq!(resp, b"+PONG\r\n", "认证后 PING 响应 +PONG");

    Ok(())
  })
}
