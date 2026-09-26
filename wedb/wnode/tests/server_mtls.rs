#![cfg(feature = "tls")]

//! mTLS 客户端证书校验端到端：钉根必选臂与 issuer 缺席宽松臂
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/RespTlsTests.cs

use std::{
  io::{Error, ErrorKind},
  num::NonZeroUsize,
  sync::Arc,
  time::Duration,
};

use compio::{net::TcpStream, runtime::Runtime, time::timeout};
use compio_tls::{
  TlsConnector,
  rustls::{ClientConfig, RootCertStore, pki_types::PrivateKeyDer},
};
use rcgen::{CertificateParams, KeyPair, generate_simple_self_signed};
use wnode_tls_test::{
  EchoProvider, handshake_refused, mk_ca, ping, read_reply, server_tls_config, start_tls_server,
  test_cert_der, test_connector,
};

/// mTLS 钉根三连：issuer CA 签发的客户端证书握手成功；无证书与错 CA 证书
/// 握手失败（对标 GarnetTlsOptions.cs:ValidateClientCertificateCallback 的
/// ClientCertificateRequired=true + IssuerCertificatePath 臂）
#[test]
fn test_garnet_server_tls_mtls_client_cert_required() -> aok::Result<()> {
  // 签发 CA 与竞争 CA（各自独立自签）
  let issue_ca = mk_ca()?;
  let rogue_ca = mk_ca()?;
  // 服务端证书与两个 CA 无关（共享自签）
  let cert_der = test_cert_der();

  let (server, addr) = start_tls_server(
    Arc::new(EchoProvider),
    server_tls_config(true, Some(vec![issue_ca.cert.der().to_vec().into()]))?,
    NonZeroUsize::new(1),
  )?;

  // 客户端信任服务端自签证书；客户端侧分别为签发证书 / 无证书 / 错 CA 证书
  let mk_connector =
    |auth: Option<(&rcgen::Certificate, &rcgen::KeyPair)>| -> aok::Result<TlsConnector> {
      let mut root_store = RootCertStore::empty();
      root_store.add(cert_der.clone().into())?;
      let builder = ClientConfig::builder().with_root_certificates(root_store);
      let config = match auth {
        Some((cert, key)) => builder.with_client_auth_cert(
          vec![cert.der().to_vec().into()],
          PrivateKeyDer::Pkcs8(key.serialized_der().to_vec().into()),
        )?,
        None => builder.with_no_client_auth(),
      };
      Ok(TlsConnector::from(Arc::new(config)))
    };

  let ok_connector = mk_connector(Some((&issue_ca.client_cert, &issue_ca.client_key)))?;
  let no_cert_connector = mk_connector(None)?;
  let rogue_connector = mk_connector(Some((&rogue_ca.client_cert, &rogue_ca.client_key)))?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(10), async {
      // 合法签发：握手 + PING/PONG 全通
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = ok_connector.connect("localhost", tcp_stream).await?;
      ping(&mut tls_stream).await?;
      assert_eq!(
        read_reply(&mut tls_stream).await,
        b"+PONG\r\n",
        "合法客户端证书应握手成功"
      );

      // 无证书：必选臂拒绝（TLS1.3 下失败面可能在首读，两处都判；对端关闭表现为 Err 或 Ok(0) EOF）
      let refused = handshake_refused(&no_cert_connector, TcpStream::connect(addr).await?).await;
      assert!(refused, "无客户端证书应握手失败");

      // 错 CA 签发：链校验拒绝
      let refused = handshake_refused(&rogue_connector, TcpStream::connect(addr).await?).await;
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
  // 服务端证书（共享自签）
  let cert_der = test_cert_der();

  // required=true 且 issuer 缺席 → 宽松校验器
  let (server, addr) = start_tls_server(
    Arc::new(EchoProvider),
    server_tls_config(true, None)?,
    NonZeroUsize::new(1),
  )?;

  // 客户端证书：与服务端无任何链关系的独立自签证书（换证语义所需的不同证书）
  let client_key = generate_simple_self_signed(vec!["standalone-client".to_string()])?;

  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;

  let any_cert_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_client_auth_cert(
        vec![client_key.cert.der().to_vec().into()],
        PrivateKeyDer::Pkcs8(client_key.signing_key.serialized_der().to_vec().into()),
      )?,
  ));
  let no_cert_connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(10), async {
      // 任意证书：链不校验，握手 + PING/PONG 全通
      let tcp_stream = TcpStream::connect(addr).await?;
      let mut tls_stream = any_cert_connector.connect("localhost", tcp_stream).await?;
      ping(&mut tls_stream).await?;
      assert_eq!(
        read_reply(&mut tls_stream).await,
        b"+PONG\r\n",
        "宽松模式任意证书应握手成功"
      );

      // 无证书：mandatory 臂仍拒绝
      let refused = handshake_refused(&no_cert_connector, TcpStream::connect(addr).await?).await;
      assert!(refused, "宽松模式无证书仍应握手失败");
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "宽松 mTLS 测试超时"))?
  })?;

  server.stop();
  Ok(())
}

/// 宽松臂有效期实校验：issuer 缺席 + 过期客户端证书必须握手失败
///
/// 对标 GarnetTlsOptions.cs:ValidateCertificateIssuer 的 X509Chain.Build
/// NotTimeValid 拒绝面（`wtls::AnyClientCert::verify_client_cert`）：链信任
/// 构建豁免，时间窗不豁免。修复前本用例红（无条件 `assertion()` 放行）
#[test]
fn test_garnet_server_tls_mtls_permissive_rejects_expired_client_cert() -> aok::Result<()> {
  let cert_der = test_cert_der();

  // required=true 且 issuer 缺席 → 宽松校验器
  let (server, addr) = start_tls_server(
    Arc::new(EchoProvider),
    server_tls_config(true, None)?,
    NonZeroUsize::new(1),
  )?;

  // 过期客户端证书（窗口 2001-2004，与任何 CA 无链关系的独立自签）
  let mut params = CertificateParams::new(vec!["expired-client.test".to_string()])?;
  params.not_before = time::OffsetDateTime::from_unix_timestamp(1_000_000_000)?;
  params.not_after = time::OffsetDateTime::from_unix_timestamp(1_100_000_000)?;
  let expired_key = KeyPair::generate()?;
  let expired = params.self_signed(&expired_key)?;

  let mut root_store = RootCertStore::empty();
  root_store.add(cert_der.into())?;
  let expired_connector = TlsConnector::from(Arc::new(
    ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_client_auth_cert(
        vec![expired.der().to_vec().into()],
        PrivateKeyDer::Pkcs8(expired_key.serialized_der().to_vec().into()),
      )?,
  ));

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(Duration::from_secs(10), async {
      // 过期证书：握手期或首读期拒绝（TLS1.3 失败面两处都判）
      let refused = handshake_refused(&expired_connector, TcpStream::connect(addr).await?).await;
      assert!(refused, "过期客户端证书必须被宽松臂拒绝");
      Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "过期证书 mTLS 测试超时"))?
  })?;

  server.stop();
  Ok(())
}
