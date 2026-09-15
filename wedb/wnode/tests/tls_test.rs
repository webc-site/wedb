//! TLS 端到端集成测试（纯 Rust 方案）

#![cfg(feature = "tls")]

use std::{
  io::{Error, ErrorKind},
  time::Duration,
};

use compio::{runtime::spawn, time::timeout};

#[test]
fn test_garnet_server_tls_lifecycle() -> aok::Result<()> {
  use std::{num::NonZeroUsize, sync::Arc};

  use compio::{
    BufResult,
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    runtime::Runtime,
  };
  use compio_tls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::PrivateKeyDer},
  };
  use rcgen::generate_simple_self_signed;
  use wnode::{
    GarnetServer, MessageConsumerFace, ServerTlsConfig, SessionProviderFace, WireFormat,
  };

  struct EchoConsumer;
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
      if req_buffer.starts_with(b"PING\r\n") {
        resp_buf.extend_from_slice(b"+PONG\r\n");
        6
      } else {
        0
      }
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer)
    }
  }

  // 1. 使用 rcgen 动态生成自签名证书与私钥（纯内存、纯 Rust、零外部依赖）
  let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
  let certified_key = generate_simple_self_signed(subject_alt_names)?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )
  .with_tls_config(server_tls);

  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  // 2. 构造客户端 TLS 配置（信任刚刚生成的自签名证书）
  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;
  let client_config = ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_no_client_auth();
  let connector = TlsConnector::from(Arc::new(client_config));

  // 3. 客户端连接并执行 TLS 握手与消息收发（带 5 秒超时保护）
  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

      let _ = tls_stream.write_all(b"PING\r\n".to_vec()).await;
      tls_stream.flush().await?;
      let buf = vec![0u8; 128];
      let BufResult(res, read_buf) = tls_stream.read(buf).await;
      let n = res?;
      assert_eq!(&read_buf[..n], b"+PONG\r\n");
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
  use std::{num::NonZeroUsize, sync::Arc};

  use compio::{
    BufResult,
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    runtime::Runtime,
  };
  use compio_tls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::PrivateKeyDer},
  };
  use rcgen::generate_simple_self_signed;
  use wnode::{
    GarnetServer, MessageConsumerFace, ServerTlsConfig, SessionProviderFace, WireFormat,
  };

  struct EchoConsumer;
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
      let mut consumed = 0;
      while req_buffer[consumed..].starts_with(b"PING\r\n") {
        resp_buf.extend_from_slice(b"+PONG\r\n");
        consumed += 6;
      }
      consumed
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer)
    }
  }

  let certified_key = generate_simple_self_signed(vec!["localhost".into()])?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )
  .with_tls_config(server_tls);

  server.start(NonZeroUsize::new(2))?;
  let addr = server.local_addr()?;

  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;
  let client_config = ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_no_client_auth();
  let connector = Arc::new(TlsConnector::from(Arc::new(client_config)));

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      // 验证 1：单连接多轮连续请求应答
      {
        let tcp_stream = TcpStream::connect(addr).await?;
        let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

        for _ in 0..5 {
          let _ = tls_stream.write_all(b"PING\r\n".to_vec()).await;
          tls_stream.flush().await?;
          let buf = vec![0u8; 128];
          let BufResult(res, read_buf) = tls_stream.read(buf).await;
          let n = res?;
          assert_eq!(&read_buf[..n], b"+PONG\r\n");
        }
      }

      // 验证 2：多连接并发 TLS 握手与通信
      let mut handles = Vec::new();
      for _ in 0..4 {
        let conn = Arc::clone(&connector);
        handles.push(spawn(async move {
          let tcp_stream = TcpStream::connect(addr).await?;
          let mut tls_stream = conn.connect("localhost", tcp_stream).await?;
          let _ = tls_stream.write_all(b"PING\r\n".to_vec()).await;
          tls_stream.flush().await?;
          let buf = vec![0u8; 128];
          let BufResult(res, read_buf) = tls_stream.read(buf).await;
          let n = res?;
          assert_eq!(&read_buf[..n], b"+PONG\r\n");
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
