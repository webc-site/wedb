//! 出站证书握手期现取端到端：捕获型 ClientCertVerifier 断言握手呈出的
//! end_entity 随 CONFIG SET 执行端 update_cert_file 热换装现取而变——
//! 出站 ClientTlsConfig 长驻实例零重建（boot.rs 同源装配形制）
//!
//! 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions
//!（:185-189 LocalCertificateSelectionCallback 闭包动态读 selector，
//! UpdateCertFile 换 selector 即传播出站；test/standalone/Garnet.test/
//! RespTlsTests.cs 换证语义对位）

use std::{
  fs::write,
  sync::{Arc, LazyLock},
  time::Duration,
};

use compio::{
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
  time::sleep,
};
use compio_tls::{
  TlsAcceptor,
  rustls::{
    self as rustls_ns, DigitallySignedStruct, ServerConfig, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    crypto::{self, WebPkiSupportedAlgorithms, ring},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime},
    server::{
      ClientHello, ResolvesServerCert,
      danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
  },
};
use parking_lot::Mutex;
use wtls::{ClientTlsConfig, ServerTlsConfig};

/// 捕获型客户端证书校验器：记录每次握手呈出的 end_entity 实体（对端
/// 视角身份观测面），恒过校验以隔离换证语义于身份呈现本身
#[derive(Debug)]
struct CaptureVerifier {
  seen: Mutex<Vec<Vec<u8>>>,
}

/// 签名校验算法表（ring provider 单例，与 src 同形制）
static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

impl ClientCertVerifier for CaptureVerifier {
  fn client_auth_mandatory(&self) -> bool {
    true
  }

  fn root_hint_subjects(&self) -> &[rustls_ns::DistinguishedName] {
    &[]
  }

  fn verify_client_cert(
    &self,
    end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _now: UnixTime,
  ) -> Result<ClientCertVerified, rustls_ns::Error> {
    self.seen.lock().push(end_entity.to_vec());
    Ok(ClientCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls_ns::Error> {
    crypto::verify_tls12_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls_ns::Error> {
    crypto::verify_tls13_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    SIGNATURE_ALGS.supported_schemes()
  }
}

/// 固定服务端证书解析器（服务端证书仅为夹具，换证语义观测面在出站身份）
#[derive(Debug)]
struct FixedResolver(Arc<CertifiedKey>);

impl ResolvesServerCert for FixedResolver {
  fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
    Some(Arc::clone(&self.0))
  }
}

/// rcgen 自签装配为 rustls CertifiedKey
fn self_signed_ck(dns: &str) -> CertifiedKey {
  let ck = rcgen::generate_simple_self_signed(vec![dns.to_string()]).expect("自签证书生成");
  let key = crypto::ring::sign::any_supported_type(&PrivateKeyDer::Pkcs8(
    PrivatePkcs8KeyDer::from(ck.signing_key.serialized_der().to_vec()),
  ))
  .expect("ring 装载自签私钥");
  CertifiedKey::new(vec![CertificateDer::from(ck.cert.der().to_vec())], key)
}

/// 有界轮询捕获数至期望值（TLS1.3 下客户端 connect 先行完成，服务端
/// 证书校验落在其后首读处理中，以可观测捕获为据不赌单拍）
async fn await_captures(verifier: &CaptureVerifier, want: usize) {
  for _ in 0..250 {
    if verifier.seen.lock().len() >= want {
      return;
    }
    sleep(Duration::from_millis(20)).await;
  }
  panic!(
    "捕获数未在时限内收敛至 {want}，实际 {}",
    verifier.seen.lock().len()
  );
}

/// 出站长驻实例握手呈证随入站热换装现取而变：
/// 第一次握手呈证书 A → update_cert_file 换装证书 B（真实重读磁盘装载
/// 路径）→ 同一 ClientTlsConfig 实例第二次握手呈证书 B（对端捕获视角
/// end_entity 字节即新证 DER）
#[test]
fn outbound_presents_rotated_cert_at_handshake() {
  let cert_a = rcgen::generate_simple_self_signed(vec!["cluster-a".to_string()]).expect("自签 A");
  let cert_b = rcgen::generate_simple_self_signed(vec!["cluster-b".to_string()]).expect("自签 B");
  let dir = tempfile::tempdir().expect("临时目录");
  let cert_path = dir.path().join("cert.pem");
  let key_path = dir.path().join("key.pem");
  write(&cert_path, cert_a.cert.pem()).expect("写 A 证书");
  write(&key_path, cert_a.signing_key.serialize_pem()).expect("写 A 私钥");

  Runtime::new().expect("运行时").block_on(async {
    // 入站配置 + 出站同源派生（boot.rs 装配链形制：出站自 cert_source()
    // 句柄构造，ClientTlsConfig 实例此后永不重建）
    let inbound =
      ServerTlsConfig::from_pem_files(&cert_path, &key_path, false, None, 0).expect("入站装配");
    let outbound =
      ClientTlsConfig::from_shared_source(Some(inbound.cert_source()), "", false, None)
        .expect("出站装配");

    // 对端服务：捕获校验器 + 固定自签服务端证书，连收两笔握手
    let verifier = Arc::new(CaptureVerifier {
      seen: Mutex::new(Vec::new()),
    });
    let server_config = ServerConfig::builder()
      .with_client_cert_verifier(Arc::clone(&verifier) as Arc<dyn ClientCertVerifier>)
      .with_cert_resolver(Arc::new(FixedResolver(Arc::new(self_signed_ck("peer")))));
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("监听");
    let addr = listener.local_addr().expect("端点");
    let endpoint = addr.to_string();
    spawn(async move {
      for _ in 0..2 {
        let (stream, _) = listener.accept().await.expect("accept");
        // 握手处理即捕获面；被捕获校验器恒过，失败仅为链路噪声
        let _ = acceptor.accept(stream).await;
      }
    })
    .detach();

    // 第一笔：呈证书 A
    let conn = outbound
      .connect(TcpStream::connect(addr).await.expect("TCP 连接"), &endpoint)
      .await
      .expect("首轮握手");
    drop(conn);
    await_captures(&verifier, 1).await;
    assert_eq!(
      verifier.seen.lock()[0],
      cert_a.cert.der().as_ref(),
      "首轮握手呈出的 end_entity 必须为装配期证书 A"
    );

    // 热换装至证书 B（CONFIG SET cert-file-name 执行端同径：真实重读磁盘
    // 装载并经 ArcSwap 原子换装活跃证书）
    write(&cert_path, cert_b.cert.pem()).expect("覆写 B 证书");
    write(&key_path, cert_b.signing_key.serialize_pem()).expect("覆写 B 私钥");
    inbound
      .update_cert_file(Some(&cert_path), Some(&key_path))
      .expect("换装 B");

    // 第二笔：同一长驻出站实例，握手必须现取新证书 B
    let conn = outbound
      .connect(TcpStream::connect(addr).await.expect("TCP 连接"), &endpoint)
      .await
      .expect("轮换后握手");
    drop(conn);
    await_captures(&verifier, 2).await;
    assert_eq!(
      verifier.seen.lock()[1],
      cert_b.cert.der().as_ref(),
      "轮换后同一长驻出站实例握手呈出的 end_entity 必须现取为新证书 B"
    );
  });
}
