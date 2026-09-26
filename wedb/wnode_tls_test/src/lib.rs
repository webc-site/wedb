//! TLS 集成测试支撑库（`wnode_tls_test`）
//!
//! 自签证书夹具、回声/QUIT 桩、握手期治理面观测 mock、服务端 TLS 配置/会话
//! 工厂装配单源与 RESP 套接字读写 helper。证书进程内单例（LazyLock）：nextest
//! 每用例独立进程，同一进程内多用例共享一份 rcgen 证书，替代旧 tls_test.rs
//! 每用例重复生成 1-3 份。
//!
//! 依赖 wnode/wtls，仅限 TLS 装配域集成测试消费（wnode/tests，cfg(feature = "tls") 门控）。

use std::{
  io,
  mem::take,
  net::SocketAddr,
  num::NonZeroUsize,
  sync::{Arc, LazyLock},
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  time::sleep,
};
use compio_tls::{
  TlsConnector,
  rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
  },
};
use wnode::{GarnetServer, SessionProviderFace, servers::ConsumerRegistry};
use wtls::ServerTlsConfig;

/// 进程内单份自签证书（localhost + 127.0.0.1 双 SAN，服务端与信任锚共用）
static TEST_CERT: LazyLock<rcgen::CertifiedKey<rcgen::KeyPair>> = LazyLock::new(|| {
  rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
    .expect("rcgen 自签证书生成")
});

/// 自签证书 DER（每次调用克隆，避免跨用例共享可变 Vec）
pub fn test_cert_der() -> Vec<u8> {
  TEST_CERT.cert.der().to_vec()
}

/// 自签私钥 DER（PKCS8）
pub fn test_key_der() -> Vec<u8> {
  TEST_CERT.signing_key.serialized_der().to_vec()
}

/// 构造信任单张自签证书的客户端连接器（换装面新旧 CA 各一）
pub fn trust_connector(cert_der: Vec<u8>) -> io::Result<TlsConnector> {
  let mut root_store = RootCertStore::empty();
  root_store
    .add(cert_der.into())
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
  Ok(TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_no_client_auth(),
  )))
}

/// 信任进程内单份自签证书的连接器（最常用形态）
pub fn test_connector() -> io::Result<TlsConnector> {
  trust_connector(test_cert_der())
}

/// 标准测试服务端 TLS 配置：自签证书、不要求客户端证书、无刷新
pub fn test_server_tls() -> aok::Result<ServerTlsConfig> {
  server_tls_config(false, None)
}

/// 服务端 TLS 配置装配单源：DER 证书链/私钥取自进程内自签夹具，
/// `client_cert_required`/`issuer_ca` 透传 [`ServerTlsConfig::from_der`]
pub fn server_tls_config(
  client_cert_required: bool,
  issuer_ca: Option<Vec<CertificateDer<'static>>>,
) -> aok::Result<ServerTlsConfig> {
  Ok(ServerTlsConfig::from_der(
    vec![test_cert_der().into()],
    PrivateKeyDer::Pkcs8(test_key_der().into()),
    client_cert_required,
    issuer_ca,
    0,
  )?)
}

/// TLS 会话工厂装配单源：`GarnetServer::new(127.0.0.1:0, 4096, 8)` +
/// `with_tls_config` + `start` + `local_addr`，返回服务器句柄与监听端点
pub fn start_tls_server<P: SessionProviderFace + 'static>(
  provider: Arc<P>,
  server_tls: ServerTlsConfig,
  worker_threads: Option<NonZeroUsize>,
) -> aok::Result<(GarnetServer<P>, SocketAddr)> {
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?.with_tls_config(server_tls);
  server.start(worker_threads)?;
  let addr = server.local_addr()?;
  Ok((server, addr))
}

/// 握手拒绝判定：TLS 握手失败即拒绝；TLS1.3 下失败面落在首读时，
/// 写 PING 后读——报错或 EOF 均判拒绝（与原各测试内联判据一致）
pub async fn handshake_refused(connector: &TlsConnector, tcp_stream: TcpStream) -> bool {
  match connector.connect("localhost", tcp_stream).await {
    Err(_) => true,
    Ok(mut stream) => {
      let _ = stream.write_all(b"PING\r\n".to_vec()).await;
      let _ = stream.flush().await;
      let BufResult(res, _) = stream.read(vec![0u8; 16]).await;
      res.is_err() || matches!(res, Ok(0))
    }
  }
}

/// 测试用自签 CA 与其签发的客户端证书（rcgen 单一签发链）
pub struct CaFixture {
  pub cert: rcgen::Certificate,
  pub client_cert: rcgen::Certificate,
  pub client_key: rcgen::KeyPair,
}

/// 生成自签 CA 与其签发的客户端证书
pub fn mk_ca() -> aok::Result<CaFixture> {
  use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};

  let ca_key = KeyPair::generate()?;
  let mut ca_params = CertificateParams::new(vec!["wedb-test-ca".to_string()])?;
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
  let cert = ca_params.self_signed(&ca_key)?;
  let ca_issuer = Issuer::from_params(&ca_params, &ca_key);

  let client_key = KeyPair::generate()?;
  let client_params = CertificateParams::new(vec!["wedb-test-client".to_string()])?;
  let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;
  Ok(CaFixture {
    cert,
    client_cert,
    client_key,
  })
}

/// 最小回声消费者：PING → +PONG
pub struct EchoConsumer {
  buf: Vec<u8>,
  head: usize,
}

impl wnode::MessageConsumerFace for EchoConsumer {
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

/// 回声提供者：每会话发 [`EchoConsumer`]
pub struct EchoProvider;

impl wnode::SessionProviderFace for EchoProvider {
  type Consumer = EchoConsumer;
  fn get_session(&self, _wf: wnode::WireFormat, _id: u64) -> Option<EchoConsumer> {
    Some(EchoConsumer {
      buf: Vec::new(),
      head: 0,
    })
  }
}

/// QUIT 桩消费者：+OK 应答后置待释放哨兵，泵发尽应答即退出（服务端主动收场一侧）
pub struct QuitConsumer {
  buf: Vec<u8>,
  head: usize,
  to_dispose: bool,
}

impl wnode::MessageConsumerFace for QuitConsumer {
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

/// QUIT 桩提供者：每会话发 [`QuitConsumer`]
pub struct QuitProvider;

impl wnode::SessionProviderFace for QuitProvider {
  type Consumer = QuitConsumer;
  fn get_session(&self, _wf: wnode::WireFormat, _id: u64) -> Option<QuitConsumer> {
    Some(QuitConsumer {
      buf: Vec::new(),
      head: 0,
      to_dispose: false,
    })
  }
}

/// 带注册表的回声提供者：entries 即握手期/在途连接的治理面观测源
pub struct RegistryProvider {
  pub registry: Arc<ConsumerRegistry>,
}

impl wnode::SessionProviderFace for RegistryProvider {
  type Consumer = EchoConsumer;
  fn get_session(&self, _wf: wnode::WireFormat, _id: u64) -> Option<EchoConsumer> {
    Some(EchoConsumer {
      buf: Vec::new(),
      head: 0,
    })
  }
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}

/// 状态轮询节拍
pub const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// 轮询预注册条目数至期望值（5 秒内不收敛即硬失败）
pub async fn wait_entries(registry: &ConsumerRegistry, want: usize) -> io::Result<()> {
  for _ in 0..250 {
    if registry.active_consumers().len() == want {
      return Ok(());
    }
    sleep(POLL_INTERVAL).await;
  }
  Err(io::Error::other(format!(
    "活跃条目未在时限内收敛至 {want}，实际 {}",
    registry.active_consumers().len()
  )))
}

/// 对端关闭判定：半开套接字被服务端超时熔断后客户端读侧见 EOF（FIN 后
/// 读 0 字节；携 RST 收尾同判）
pub async fn assert_peer_closed(stream: &mut TcpStream, tag: &str) -> io::Result<()> {
  let BufResult(res, _) = stream.read(vec![0u8; 16]).await;
  let n = res.unwrap_or(0);
  if n != 0 {
    return Err(io::Error::other(format!(
      "{tag} 应被服务端关闭，却读到 {n} 字节"
    )));
  }
  Ok(())
}

/// 读一条完整 RESP 应答（帧完整性判定复用 wnode_test::complete_len）
pub async fn read_reply<S: AsyncRead>(stream: &mut S) -> Vec<u8> {
  let mut acc = Vec::new();
  loop {
    let buf = vec![0u8; 512];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&returned[..n]);
        if wnode_test::complete_len(&acc).is_some() {
          return acc;
        }
      }
    }
  }
}

/// TLS + PING 快捷握手（载荷逐字节断言的公共前置步）
pub async fn ping<S: AsyncWrite>(stream: &mut S) -> io::Result<()> {
  let _ = stream.write_all(b"PING\r\n".to_vec()).await;
  stream.flush().await?;
  Ok(())
}

/// 信任锚为旧自签根的客户端遇到轮换后的新自签证书：握手拒绝须收窄至
/// rustls InvalidCertificate(BadSignature) 具体变体（同 subject 以旧根公钥
/// 验新叶签名即落 BadSignature 臂；任何错误都算绿测不到换证语义）
pub fn is_bad_signature(e: &io::Error) -> bool {
  e.to_string()
    .contains("invalid peer certificate: BadSignature")
}
