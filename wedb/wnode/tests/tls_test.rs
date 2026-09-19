//! TLS 端到端集成测试（纯 Rust 方案）

#![cfg(feature = "tls")]

use std::{
  io::{Error, ErrorKind},
  mem::take,
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

  struct EchoConsumer {
    buf: Vec<u8>,
    head: usize,
  }
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
      while self.buf[self.head..].starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      }
      if self.head >= self.buf.len() {
        self.buf.clear();
        self.head = 0;
        return Some(0);
      }
      Some(self.buf.len() - self.head)
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.buf)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.buf = buf;
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer {
        buf: Vec::new(),
        head: 0,
      })
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
    false,
    None,
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )?
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

  struct EchoConsumer {
    buf: Vec<u8>,
    head: usize,
  }
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
      while self.buf[self.head..].starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      }
      if self.head >= self.buf.len() {
        self.buf.clear();
        self.head = 0;
        return Some(0);
      }
      Some(self.buf.len() - self.head)
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.buf)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.buf = buf;
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer {
        buf: Vec::new(),
        head: 0,
      })
    }
  }

  let certified_key = generate_simple_self_signed(vec!["localhost".into()])?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
    false,
    None,
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )?
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

/// TLS 连接退出的关闭序：服务端泵收场前先发 close_notify，客户端 TLS 栈读到干净 EOF
///
/// 判据取自客户端 rustls 自身（对端进程外可观测，不涉本进程内部状态）：收到
/// close_notify 后读侧归零，仅收到 TCP FIN 而以 EOF 截断收场时以
/// `ErrorKind::UnexpectedEof` 表达。故末次读必须走 futures 侧接口——compio 的
/// `AsyncRead::read` 把 UnexpectedEof 一并归零（服务端读臂即据此口径），据此判不
/// 出截断。缺关闭序时本用例即红。
#[test]
fn test_garnet_server_tls_exit_sends_close_notify() -> aok::Result<()> {
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

  // QUIT 桩：+OK 应答后置待释放哨兵，泵发尽应答即退出（服务端主动收场一侧）
  struct QuitConsumer {
    buf: Vec<u8>,
    head: usize,
    to_dispose: bool,
  }
  impl MessageConsumerFace for QuitConsumer {
    fn take_dispose_request(&mut self) -> bool {
      self.to_dispose
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.buf)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.buf = buf;
    }
    fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"QUIT\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+OK\r\n");
        self.to_dispose = true;
      } else if !b"QUIT\r\n".starts_with(rest) {
        return None;
      }
      if self.head >= self.buf.len() {
        self.buf.clear();
        self.head = 0;
        return Some(0);
      }
      Some(self.buf.len() - self.head)
    }
    fn dispose(&mut self) {}
  }

  struct QuitProvider;
  impl SessionProviderFace for QuitProvider {
    type Consumer = QuitConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<QuitConsumer> {
      Some(QuitConsumer {
        buf: Vec::new(),
        head: 0,
        to_dispose: false,
      })
    }
  }

  let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
  let certified_key = generate_simple_self_signed(subject_alt_names)?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
    false,
    None,
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(QuitProvider),
  )?
  .with_tls_config(server_tls);

  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;
  let client_config = ClientConfig::builder()
    .with_root_certificates(root_store)
    .with_no_client_auth();
  let connector = TlsConnector::from(Arc::new(client_config));

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(5), async {
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = connector.connect("localhost", tcp_stream).await?;

      tls_stream.write_all(b"QUIT\r\n".to_vec()).await.0?;
      tls_stream.flush().await?;

      // 应答先发尽
      let buf = vec![0u8; 128];
      let BufResult(res, read_buf) = tls_stream.read(buf).await;
      let n = res?;
      assert_eq!(&read_buf[..n], b"+OK\r\n", "QUIT 应答发尽后才断连");

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

/// mTLS 钉根三连：issuer CA 签发的客户端证书握手成功；无证书与错 CA 证书
/// 握手失败（对标 GarnetTlsOptions.cs:ValidateClientCertificateCallback 的
/// ClientCertificateRequired=true + IssuerCertificatePath 臂）
#[test]
fn test_garnet_server_tls_mtls_client_cert_required() -> aok::Result<()> {
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

  struct EchoConsumer {
    buf: Vec<u8>,
    head: usize,
  }
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
      while self.buf[self.head..].starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      }
      if self.head >= self.buf.len() {
        self.buf.clear();
        self.head = 0;
        return Some(0);
      }
      Some(self.buf.len() - self.head)
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.buf)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.buf = buf;
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer {
        buf: Vec::new(),
        head: 0,
      })
    }
  }

  // 签发 CA 与竞争 CA（各自独立自签）
  let issue_ca = mk_ca()?;
  let rogue_ca = mk_ca()?;
  // 服务端证书与两个 CA 无关（自签）
  let certified_key =
    generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
    true,
    Some(vec![issue_ca.cert.der().to_vec().into()]),
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )?
  .with_tls_config(server_tls);

  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  // 客户端信任服务端自签证书
  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;

  let ok_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store.clone())
      .with_client_auth_cert(
        vec![issue_ca.client_cert.der().to_vec().into()],
        PrivateKeyDer::Pkcs8(issue_ca.client_key.serialized_der().to_vec().into()),
      )?,
  ));
  let no_cert_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store.clone())
      .with_no_client_auth(),
  ));
  let rogue_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_client_auth_cert(
        vec![rogue_ca.client_cert.der().to_vec().into()],
        PrivateKeyDer::Pkcs8(rogue_ca.client_key.serialized_der().to_vec().into()),
      )?,
  ));

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(10), async {
      // 合法签发：握手 + PING/PONG 全通
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = ok_connector.connect("localhost", tcp_stream).await?;
      let _ = tls_stream.write_all(b"PING\r\n".to_vec()).await;
      tls_stream.flush().await?;
      let buf = vec![0u8; 128];
      let BufResult(res, read_buf) = tls_stream.read(buf).await;
      assert_eq!(&read_buf[..res?], b"+PONG\r\n", "合法客户端证书应握手成功");

      // 无证书：必选臂拒绝（TLS1.3 下失败面可能在首读，两处都判；对端关闭表现为 Err 或 Ok(0) EOF）
      let tcp_stream = TcpStream::connect(addr).await?;
      let refused = match no_cert_connector.connect("localhost", tcp_stream).await {
        Err(_) => true,
        Ok(mut stream) => {
          let _ = stream.write_all(b"PING\r\n".to_vec()).await;
          let _ = stream.flush().await;
          let BufResult(res, _) = stream.read(vec![0u8; 16]).await;
          res.is_err() || matches!(res, Ok(0))
        }
      };
      assert!(refused, "无客户端证书应握手失败");

      // 错 CA 签发：链校验拒绝
      let tcp_stream = TcpStream::connect(addr).await?;
      let refused = match rogue_connector.connect("localhost", tcp_stream).await {
        Err(_) => true,
        Ok(mut stream) => {
          let _ = stream.write_all(b"PING\r\n".to_vec()).await;
          let _ = stream.flush().await;
          let BufResult(res, _) = stream.read(vec![0u8; 16]).await;
          res.is_err() || matches!(res, Ok(0))
        }
      };
      assert!(refused, "非签发 CA 的客户端证书应握手失败");
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "mTLS 测试超时"))?
  })?;

  server.stop();
  Ok(())
}

/// 宽松模式：required=true 而 issuer 缺席——任意自签证书放行（C#
/// GarnetTlsOptions.cs:273 语义），无证书仍拒绝
#[test]
fn test_garnet_server_tls_mtls_permissive_without_issuer() -> aok::Result<()> {
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

  struct EchoConsumer {
    buf: Vec<u8>,
    head: usize,
  }
  impl MessageConsumerFace for EchoConsumer {
    fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
      while self.buf[self.head..].starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      }
      if self.head >= self.buf.len() {
        self.buf.clear();
        self.head = 0;
        return Some(0);
      }
      Some(self.buf.len() - self.head)
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.buf)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.buf = buf;
    }
    fn dispose(&mut self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
      Some(EchoConsumer {
        buf: Vec::new(),
        head: 0,
      })
    }
  }

  // 服务端证书（自签）
  let certified_key =
    generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
  let cert_der = certified_key.cert.der().to_vec();
  let key_der = certified_key.key_pair.serialized_der().to_vec();

  // required=true 且 issuer 缺席 → 宽松校验器
  let server_tls = ServerTlsConfig::from_der(
    vec![cert_der.clone().into()],
    PrivateKeyDer::Pkcs8(key_der.into()),
    true,
    None,
  )?;

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  )?
  .with_tls_config(server_tls);

  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  // 客户端证书：与服务端无任何链关系的独立自签证书
  let client_key = generate_simple_self_signed(vec!["standalone-client".to_string()])?;

  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;

  let any_cert_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store.clone())
      .with_client_auth_cert(
        vec![client_key.cert.der().to_vec().into()],
        PrivateKeyDer::Pkcs8(client_key.key_pair.serialized_der().to_vec().into()),
      )?,
  ));
  let no_cert_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_no_client_auth(),
  ));

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(10), async {
      // 任意证书：链不校验，握手 + PING/PONG 全通
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = any_cert_connector.connect("localhost", tcp_stream).await?;
      let _ = tls_stream.write_all(b"PING\r\n".to_vec()).await;
      tls_stream.flush().await?;
      let buf = vec![0u8; 128];
      let BufResult(res, read_buf) = tls_stream.read(buf).await;
      assert_eq!(
        &read_buf[..res?],
        b"+PONG\r\n",
        "宽松模式任意证书应握手成功"
      );

      // 无证书：mandatory 臂仍拒绝
      let tcp_stream = TcpStream::connect(addr).await?;
      let refused = match no_cert_connector.connect("localhost", tcp_stream).await {
        Err(_) => true,
        Ok(mut stream) => {
          let _ = stream.write_all(b"PING\r\n".to_vec()).await;
          let _ = stream.flush().await;
          let BufResult(res, _) = stream.read(vec![0u8; 16]).await;
          res.is_err() || matches!(res, Ok(0))
        }
      };
      assert!(refused, "宽松模式无证书仍应握手失败");
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "宽松 mTLS 测试超时"))?
  })?;

  server.stop();
  Ok(())
}

/// 测试用自签 CA 与其签发的客户端证书（rcgen 单一签发链）
struct CaFixture {
  cert: rcgen::Certificate,
  client_cert: rcgen::Certificate,
  client_key: rcgen::KeyPair,
}

fn mk_ca() -> aok::Result<CaFixture> {
  use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};

  let ca_key = KeyPair::generate()?;
  let mut ca_params = CertificateParams::new(vec!["wedb-test-ca".to_string()])?;
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
  let cert = ca_params.self_signed(&ca_key)?;

  let client_key = KeyPair::generate()?;
  let client_params = CertificateParams::new(vec!["wedb-test-client".to_string()])?;
  let client_cert = client_params.signed_by(&client_key, &cert, &ca_key)?;
  Ok(CaFixture {
    cert,
    client_cert,
    client_key,
  })
}
