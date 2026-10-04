//! mTLS 客户端 CA 轮换热生效端到端锁测：update_cert_file 不只换服务端
//! 证书，还须按存档构造态重读 issuer 文件整体重建客户端校验器（acceptor
//! 换装），新连接钉根随 CA 轮换同步换新。
//!
//! 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile
//!（:100-120，:118 `TlsServerOptions = GetSslServerAuthenticationOptions()`
//! 整体重建）、GetSslServerAuthenticationOptions（:162 回调重挂）、
//! ValidateClientCertificateCallback（:234-247）、GetCertificateIssuer
//!（:242 每次重建从 issuer 文件重新读盘）；消费点
//! libs/server/Servers/GarnetServerTcp.cs:290（每连接 accept 回调读
//! TlsServerOptions 属性当前值——rust 对位：每连接 acceptor() 现取）
//!
//! 四案锁（真实进程内握手，无桩校验器）：
//! a 初始钉根：CA1 签发客户端证书 + issuer=CA1 启动，握手过
//! b 轮换后新连接随换：issuer 文件换 CA2 + update_cert_file → CA2 过、
//!   CA1 拒（陈旧信任锚摘除）
//! c 旧连接存量：换装前建立的连接数据面不受扰（双向读写仍在）
//! d 回退 fail-fast：issuer 文件损坏 → update_cert_file Err 且旧钉根
//!   仍生效；防反向放宽：只换服务端证书不动 issuer → 新 CA 客户端证书仍拒

use std::{
  cell::RefCell,
  fs::write,
  io,
  net::SocketAddr,
  rc::Rc,
  sync::{Arc, LazyLock},
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
  time::sleep,
};
use compio_tls::{
  TlsConnector, TlsStream,
  rustls::{
    self as rustls_ns, ClientConfig, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{self, WebPkiSupportedAlgorithms, ring},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
  },
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use wtls::ServerTlsConfig;

/// 签名校验算法表（ring provider 单例；src 侧单点在 wtls::cert 为 pub(crate)，
/// 集成测试域不可达，故此处自带同形副本）
static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

/// 客户端侧远端证书恒真校验器：观测面收敛于服务端客户端证书钉根，
/// 服务端自签证书不占测试语义（签名核验仍全量委托 ring provider）
#[derive(Debug)]
struct TrustNoServerCert;

impl ServerCertVerifier for TrustNoServerCert {
  fn verify_server_cert(
    &self,
    _end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _server_name: &ServerName<'_>,
    _ocsp_response: &[u8],
    _now: UnixTime,
  ) -> Result<ServerCertVerified, rustls_ns::Error> {
    Ok(ServerCertVerified::assertion())
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

/// 自签测试 CA（客户端证书签发根；issuing key 存活供多张叶证签发）
struct TestCa {
  issuer: Issuer<'static, KeyPair>,
  cert_pem: String,
}

/// 生成自签 CA（无约束 BasicConstraints + KeyCertSign，可作 webpki 信任根）
fn mk_ca(cn: &str) -> TestCa {
  let ca_key = KeyPair::generate().expect("CA 密钥生成");
  let mut ca_params = CertificateParams::new(vec![cn.to_string()]).expect("CA 参数");
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
  let cert_pem = ca_params.self_signed(&ca_key).expect("CA 自签").pem();
  TestCa {
    issuer: Issuer::new(ca_params, ca_key),
    cert_pem,
  }
}

/// 客户端证书对（DER 叶证 + PKCS8 私钥 DER）
struct ClientPair {
  cert_der: Vec<u8>,
  key_der: Vec<u8>,
}

/// 由指定 CA 签发客户端证书（叶证无 EKU/密钥用法约束，webpki 按"缺省即
/// 全用途"放行，链构建直达信任根）
fn issue_client(ca: &TestCa, cn: &str) -> ClientPair {
  let key = KeyPair::generate().expect("客户端密钥生成");
  let params = CertificateParams::new(vec![cn.to_string()]).expect("客户端证书参数");
  let cert = params.signed_by(&key, &ca.issuer).expect("客户端证书签发");
  ClientPair {
    cert_der: cert.der().to_vec(),
    key_der: key.serialized_der().to_vec(),
  }
}

/// 自签服务端证书 PEM 对（换装素材，与服务端证书面语义无关）
fn self_signed_server(dns: &str) -> (String, String) {
  let ck = rcgen::generate_simple_self_signed(vec![dns.to_string()]).expect("服务端自签");
  (ck.cert.pem(), ck.signing_key.serialize_pem())
}

/// 服务端握手观测账本：每连接一条（成手留流供存量数据面探针）
enum Outcome {
  Accepted(Box<TlsStream<TcpStream>>),
  Rejected(String),
}

struct Journal {
  outcomes: RefCell<Vec<Outcome>>,
}

/// 有界轮询账本条目数至期望值（判定面在服务端：TLS1.3 下客户端 connect
/// 先行完成，客户端证书校验结果落在服务端 accept，以账本为据不赌单拍）
async fn await_outcomes(journal: &Journal, want: usize) {
  for _ in 0..250 {
    if journal.outcomes.borrow().len() >= want {
      return;
    }
    sleep(Duration::from_millis(20)).await;
  }
  panic!(
    "服务端握手账本未在时限内收敛至 {want}，实际 {}",
    journal.outcomes.borrow().len()
  );
}

/// 以指定客户端证书发起 mTLS 连接（远端证书校验恒真，钉根观测在服务端）
async fn connect_with_client_cert(
  addr: SocketAddr,
  client: &ClientPair,
) -> io::Result<TlsStream<TcpStream>> {
  let config = ClientConfig::builder()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(TrustNoServerCert))
    .with_client_auth_cert(
      vec![CertificateDer::from(client.cert_der.clone())],
      PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client.key_der.clone())),
    )
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
  let connector = TlsConnector::from(Arc::new(config));
  connector
    .connect("localhost", TcpStream::connect(addr).await?)
    .await
}

/// 服务端账本条目断言谓词：Accepted / Rejected
fn is_accepted(outcome: &Outcome) -> bool {
  matches!(outcome, Outcome::Accepted(_))
}

/// 四案锁主测：CA 轮换经 update_cert_file 整体重建 acceptor 钉根，
/// 每连接现取（wnode/server.rs acceptor() 同形制），全链真实握手
#[test]
fn mtls_client_ca_rotation_hot_swaps_verifier_pin() {
  let ca1 = mk_ca("rotation-ca-1");
  let ca2 = mk_ca("rotation-ca-2");
  let client1 = issue_client(&ca1, "client-of-ca1");
  let client2 = issue_client(&ca2, "client-of-ca2");

  let dir = tempfile::tempdir().expect("临时目录");
  let issuer_path = dir.path().join("issuer.pem");
  let cert_path = dir.path().join("cert.pem");
  let key_path = dir.path().join("key.pem");
  let (server_a_pem, server_a_key) = self_signed_server("server-a");
  let (server_b_pem, server_b_key) = self_signed_server("server-b");
  let (server_c_pem, server_c_key) = self_signed_server("server-c");
  write(&issuer_path, &ca1.cert_pem).expect("写 issuer CA1");
  write(&cert_path, &server_a_pem).expect("写服务端证书 A");
  write(&key_path, &server_a_key).expect("写服务端私钥 A");

  Runtime::new().expect("运行时").block_on(async {
    let config =
      ServerTlsConfig::from_pem_files(&cert_path, &key_path, true, Some(&issuer_path), 0)
        .expect("mTLS 装配（钉根 CA1）");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("监听");
    let addr = listener.local_addr().expect("端点");
    let journal = Rc::new(Journal {
      outcomes: RefCell::new(Vec::new()),
    });
    let server_cfg = config.clone();
    let server_journal = Rc::clone(&journal);
    spawn(async move {
      loop {
        let (stream, _) = listener.accept().await.expect("accept");
        let config = server_cfg.clone();
        let journal = Rc::clone(&server_journal);
        // 每连接现取当前换装 acceptor（GarnetServerTcp.cs:290 同形制）
        spawn(async move {
          let acceptor = config.acceptor();
          let outcome = match acceptor.accept(stream).await {
            Ok(tls) => Outcome::Accepted(Box::new(tls)),
            Err(e) => Outcome::Rejected(e.to_string()),
          };
          journal.outcomes.borrow_mut().push(outcome);
        })
        .detach();
      }
    })
    .detach();

    // 案 a（初始钉根）：CA1 签发客户端证书握手过，且留作存量连接
    let mut established = connect_with_client_cert(addr, &client1)
      .await
      .expect("案 a 初始握手");
    await_outcomes(&journal, 1).await;
    assert!(
      is_accepted(&journal.outcomes.borrow()[0]),
      "案 a：初始钉根 CA1 下 CA1 签发客户端证书必须握手通过"
    );

    // 防反向放宽：只换服务端证书不动 issuer 文件 → CA2 签发客户端证书仍拒
    write(&cert_path, &server_b_pem).expect("换服务端证书 B");
    write(&key_path, &server_b_key).expect("换服务端私钥 B");
    config
      .update_cert_file(Some(&cert_path), Some(&key_path))
      .expect("只换服务端证书");
    let _ = connect_with_client_cert(addr, &client2).await;
    await_outcomes(&journal, 2).await;
    assert!(
      !is_accepted(&journal.outcomes.borrow()[1]),
      "防反向放宽：issuer 未换时新 CA 签发客户端证书必须仍拒"
    );
    // 钉根未漂移：CA1 旧客户端证书照常过
    let _ = connect_with_client_cert(addr, &client1).await;
    await_outcomes(&journal, 3).await;
    assert!(
      is_accepted(&journal.outcomes.borrow()[2]),
      "防反向放宽：issuer 未换时旧 CA 钉根必须保持"
    );

    // 案 b（轮换后新连接随换）：issuer 文件换 CA2 + 换证重建 → CA2 过、CA1 拒
    write(&issuer_path, &ca2.cert_pem).expect("issuer 换 CA2");
    write(&cert_path, &server_a_pem).expect("换回服务端证书 A");
    write(&key_path, &server_a_key).expect("换回服务端私钥 A");
    config
      .update_cert_file(Some(&cert_path), Some(&key_path))
      .expect("issuer 轮换整体重建");
    let _ = connect_with_client_cert(addr, &client2).await;
    await_outcomes(&journal, 4).await;
    assert!(
      is_accepted(&journal.outcomes.borrow()[3]),
      "案 b：issuer 换 CA2 后新 CA 签发客户端证书必须握手通过（钉根随换）"
    );
    let _ = connect_with_client_cert(addr, &client1).await;
    await_outcomes(&journal, 5).await;
    assert!(
      !is_accepted(&journal.outcomes.borrow()[4]),
      "案 b：issuer 换 CA2 后旧 CA1 信任锚必须摘除（陈旧钉根残留即失败）"
    );

    // 案 c（旧连接存量）：换装前建立的连接数据面不受扰（双向读写仍在）
    established
      .write_all(b"still-alive".to_vec())
      .await
      .expect("存量连接客户端写");
    established.flush().await.expect("存量连接客户端 flush");
    let mut server_side = match journal.outcomes.borrow_mut().remove(0) {
      Outcome::Accepted(stream) => *stream,
      Outcome::Rejected(e) => panic!("案 a 连接必须成手: {e}"),
    };
    let probe = vec![0u8; b"still-alive".len()];
    let BufResult(res, returned) = server_side.read(probe).await;
    let n = res.expect("存量连接服务端读");
    assert_eq!(&returned[..n], b"still-alive", "存量连接服务端读到原文");
    server_side
      .write_all(returned[..n].to_vec())
      .await
      .expect("存量连接服务端回写");
    server_side.flush().await.expect("存量连接服务端 flush");
    let BufResult(res, echoed) = established.read(vec![0u8; b"still-alive".len()]).await;
    let n = res.expect("存量连接客户端读");
    assert_eq!(
      &echoed[..n],
      b"still-alive",
      "存量连接换装后数据面必须双向存活"
    );

    // 案 d（回退 fail-fast）：issuer 文件损坏 → update_cert_file Err 且旧
    // 钉根（CA2）仍生效（客户端证书照常过）
    write(&issuer_path, b"corrupted issuer\n").expect("写坏 issuer");
    write(&cert_path, &server_c_pem).expect("换服务端证书 C");
    write(&key_path, &server_c_key).expect("换服务端私钥 C");
    let err = config
      .update_cert_file(Some(&cert_path), Some(&key_path))
      .expect_err("坏 issuer 文件必须整体失败");
    assert_eq!(
      err.kind(),
      io::ErrorKind::NotFound,
      "坏 issuer 装载失败形态: {err}"
    );
    let _ = connect_with_client_cert(addr, &client2).await;
    await_outcomes(&journal, 5).await;
    assert!(
      is_accepted(&journal.outcomes.borrow()[4]),
      "案 d：失败保旧后旧钉根 CA2 必须仍生效"
    );
  });
}
