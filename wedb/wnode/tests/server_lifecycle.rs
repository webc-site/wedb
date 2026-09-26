#![cfg(feature = "tls")]

//! GarnetServer TLS 生命周期端到端：握手收发、多请求并发、退场关闭序、
//! 握手期连接的预注册治理面
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/RespTlsTests.cs

use std::{
  io::{Error, ErrorKind},
  num::NonZeroUsize,
  sync::Arc,
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::{Runtime, spawn},
  time::timeout,
};
use wnode::service::StorageSessionProvider;
use wnode_test::{SessionFactory, session_factory};
use wnode_tls_test::{
  EchoProvider, QuitProvider, ping, read_reply, start_tls_server, test_connector, test_server_tls,
  wait_entries,
};
use wtest_base::test_store_config;

#[test]
fn test_garnet_server_tls_lifecycle() -> aok::Result<()> {
  // 1. 使用 rcgen 自签证书（纯内存、纯 Rust、零外部依赖）装配服务端
  let (server, addr) = start_tls_server(
    Arc::new(EchoProvider),
    test_server_tls()?,
    NonZeroUsize::new(1),
  )?;

  // 2. 构造客户端 TLS 配置（信任同一张自签证书）
  let connector = test_connector()?;

  // 3. 客户端连接并执行 TLS 握手与消息收发（带 5 秒超时保护）
  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

      ping(&mut tls_stream).await?;
      assert_eq!(read_reply(&mut tls_stream).await, b"+PONG\r\n");
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "TLS 测试超时"))?
  })?;

  server.stop();
  Ok(())
}

#[test]
fn test_garnet_server_tls_multi_requests_and_concurrency() -> aok::Result<()> {
  let (server, addr) = start_tls_server(
    Arc::new(EchoProvider),
    test_server_tls()?,
    NonZeroUsize::new(2),
  )?;
  let connector = Arc::new(test_connector()?);

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      // 验证 1：单连接多轮连续请求应答
      {
        let tcp_stream = TcpStream::connect(addr).await?;
        let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

        for _ in 0..5 {
          ping(&mut tls_stream).await?;
          assert_eq!(read_reply(&mut tls_stream).await, b"+PONG\r\n");
        }
      }

      // 验证 2：多连接并发 TLS 握手与通信
      let mut handles = Vec::new();
      for _ in 0..4 {
        let conn = Arc::clone(&connector);
        handles.push(spawn(async move {
          let tcp_stream = TcpStream::connect(addr).await?;
          let mut tls_stream = conn.connect("localhost", tcp_stream).await?;
          ping(&mut tls_stream).await?;
          assert_eq!(read_reply(&mut tls_stream).await, b"+PONG\r\n");
          Ok::<(), Error>(())
        }));
      }
      for h in handles {
        h.await??;
      }

      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "TLS 并发测试超时"))?
  })?;

  server.stop();
  Ok(())
}

/// TLS 连接退出的关闭序：服务端泵收场前先发 close_notify，客户端 TLS 栈读到干净 EOF
///
/// 判据取自客户端 rustls 自身（对端进程外可观测，不涉本进程内部状态）：收到
/// close_notify 后读侧归零，仅收到 TCP FIN 而以 EOF 截断收场时以
/// `ErrorKind::UnexpectedEof` 表达。故末次读必须走 futures 侧接口——compio 的
/// `AsyncRead::read` 把 UnexpectedEof 一并归零（服务端读臂即据此口径），据此判不
/// 出截断。缺关闭序时本用例即红。
#[test]
fn test_garnet_server_tls_exit_sends_close_notify() -> aok::Result<()> {
  let (server, addr) = start_tls_server(
    Arc::new(QuitProvider),
    test_server_tls()?,
    NonZeroUsize::new(1),
  )?;
  let connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

      tls_stream.write_all(b"QUIT\r\n".to_vec()).await.0?;
      tls_stream.flush().await?;

      // 应答先发尽
      assert_eq!(
        read_reply(&mut tls_stream).await,
        b"+OK\r\n",
        "QUIT 应答发尽后才断连"
      );

      // 关闭序判据：收到 close_notify 时 read 返回 Ok(0)
      let BufResult(res, _) = tls_stream.read(vec![0u8; 16]).await;
      match res {
        Ok(0) => {}
        Ok(n) => panic!("应答之后仍有未预期的明文字节: {n}"),
        Err(e) => panic!("服务端未发 close_notify，客户端 TLS 栈以截断收场: {e}"),
      }
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "TLS 关闭序测试超时"))?
  })?;

  server.stop();
  Ok(())
}

/// TLS 握手期连接的预注册治理面：握手未完成的连接已入注册表（CLIENT
/// LIST 可见）且可被 CLIENT KILL 秒断——对标 C# GarnetServerTcp.cs:256
/// `activeHandlers.TryAdd` 即刻注册（先于 :290 handler.Start 内的 TLS
/// 握手；rust 预注册于 accept 分支，握手 future 挂同一终止令牌）
#[test]
fn tls_handshaking_connection_is_listed_and_killable() -> aok::Result<()> {
  let dir = tempfile::tempdir()?;
  let factory: SessionFactory = session_factory;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join("tls-kill.db"),
    factory,
  )?);
  let (server, addr) =
    start_tls_server(provider.clone(), test_server_tls()?, NonZeroUsize::new(1))?;

  // 客户端信任锚（治理连接的正常握手）
  let connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 握手期连接：裸 TCP 连接后不发 ClientHello，服务端 accept 后挂起等待
    let mut stalled = TcpStream::connect(addr).await?;
    let stalled_addr = stalled.local_addr()?.to_string();

    // 预注册条目入表（accept 为异步路径，轮询收敛）
    wait_entries(&provider.registry, 1).await?;

    // 治理连接：正常 TLS 握手 + 会话建立
    let mut gov = connector
      .connect("localhost", TcpStream::connect(addr).await?)
      .await?;
    let _ = gov.write_all(b"*1\r\n$4\r\nPING\r\n".to_vec()).await;
    gov.flush().await?;
    assert_eq!(read_reply(&mut gov).await, b"+PONG\r\n");

    // CLIENT LIST 含握手期连接行（无会话默认投影；laddr 为 accept 侧握手
    // 前捕获的底层 TCP 本地端点）
    let _ = gov
      .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n".to_vec())
      .await;
    gov.flush().await?;
    let list = String::from_utf8_lossy(&read_reply(&mut gov).await).to_string();
    assert!(
      list.contains(&stalled_addr),
      "握手期连接须入 CLIENT LIST: {list}"
    );
    assert!(
      list.contains(" laddr=127.0.0.1:"),
      "TLS 行 laddr 为握手前捕获值: {list}"
    );

    // KILL 握手期连接：握手 future 挂终止令牌，秒断
    let kill = format!(
      "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nADDR\r\n${}\r\n{}\r\n",
      stalled_addr.len(),
      stalled_addr
    );
    let _ = gov.write_all(kill.into_bytes()).await;
    gov.flush().await?;
    assert_eq!(read_reply(&mut gov).await, b":1\r\n");

    // 被杀的握手期连接断开（握手 future 取消，流随 future 关闭）
    assert!(
      read_reply(&mut stalled).await.is_empty(),
      "握手期连接被杀须断开"
    );

    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}
