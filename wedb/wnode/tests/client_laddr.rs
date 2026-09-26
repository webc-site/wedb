#![cfg(feature = "tls")]

//! CLIENT INFO / CLIENT LIST laddr —— TLS 臂端到端确定性用例
//!
//! 在 garnet 中的相对路径： test/standalone/Garnet.test/RespTlsTests.cs
//!
//! C# 对标 libs/common/Networking/TcpNetworkHandlerBase.cs 构造期捕获
//! socket.LocalEndPoint（localEndpointName）→ LIST/INFO 两命令共读
//! networkSender.LocalEndpointName，laddr 恒有值。本用例以真实监听端点
//! 逐值断言（非前缀、非假 mock）。TLS 臂断言：底层 TCP 本地端点由 accept
//! 侧握手前捕获（server.rs run_tcp_accept_loop 的 `tcp_local_endpoint`，
//! 对偶 C# 构造期捕获先于 handler.Start 握手）、随 `NetworkHandler` 构造
//! 注入 handler 域单源。CLIENT LIST 走 `ConsumerRegistry::global()` 进程级
//! 单例（对齐 C# `Server is GarnetServerBase` 单服务器语义），nextest 每用例
//! 独立进程，跨用例单例互扰天然隔离；明文 TCP/UDS 臂见
//! wnode/tests/client_info_laddr_tests.rs。

use std::{io, num::NonZeroUsize, sync::Arc};

use compio::{BufResult, io::AsyncWriteExt, net::TcpStream, runtime::Runtime};
use wnode::{
  GarnetServer, RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions,
  service::StorageSessionProvider,
};
use wnode_tls_test::{read_reply, test_connector, test_server_tls};
use wtest_base::test_store_config;

/// 写出命令载荷（RESP 数组帧）
///
/// 写后即 flush：TLS 下 rustls 把写出攒在连接发送缓冲，flush 才落线缆
async fn send_cmd<S: AsyncWriteExt>(stream: &mut S, args: &[&[u8]]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(wtest_base::resp_frame(args)).await;
  res?;
  stream.flush().await
}

/// bulk 帧载荷文本（CLIENT INFO/LIST 均 bulk 应答）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = String::from_utf8_lossy(&frame[1..nl - 1])
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 行内字段提取（`key=<value>` 至下一空格；空值返回 None）
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
  line
    .split(' ')
    .find_map(|f| f.strip_prefix(key))
    .filter(|v| !v.is_empty())
}

#[test]
fn client_info_laddr_tls_match_listen_endpoint() -> aok::Result<()> {
  let dir = tempfile::tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join("node.db"),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?);
  // TLS 服务端：rust 的 tls_config 为 server 级。夹具为 wnode_tls_test 进程内单份
  // 自签证书，客户端信任锚同证书
  let tls_server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    1 << 16,
    8,
    Arc::clone(&provider),
  )?
  .with_tls_config(test_server_tls()?);
  tls_server.start(NonZeroUsize::new(1))?;
  let tls_addr = tls_server.local_addr()?;
  let connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // ── TLS 臂：握手前捕获的本地端点随流透传，laddr 与 TLS 监听端点逐值
    // 一致（修复点：透传落地前该字段为空串）──
    let tcp = TcpStream::connect(tls_addr).await?;
    let mut tls = connector.connect("localhost", tcp).await?;
    send_cmd(&mut tls, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut tls).await, b"+PONG\r\n");

    send_cmd(&mut tls, &[b"CLIENT", b"INFO"]).await?;
    let tls_info = bulk_body(&read_reply(&mut tls).await);
    let tls_laddr = field(&tls_info, "laddr=").expect("TLS CLIENT INFO laddr 非空");
    assert_eq!(
      tls_laddr,
      tls_addr.to_string(),
      "TLS CLIENT INFO laddr 须等于监听端点: {tls_info}"
    );

    // ── CLIENT LIST：TLS 连接注册后的登记行 ──
    send_cmd(&mut tls, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut tls).await);
    let tls_line = list
      .lines()
      .find(|l| field(l, "laddr=") == Some(tls_addr.to_string().as_str()))
      .expect("CLIENT LIST 含 laddr=TLS 监听地址 的行");
    assert!(
      field(tls_line, "addr=").is_some_and(|a| a.starts_with("127.0.0.1:")),
      "TLS 行 addr 为客户端临时端点: {tls_line}"
    );
    Ok::<(), io::Error>(())
  })?;
  tls_server.dispose();
  Ok(())
}
