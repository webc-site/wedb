#![cfg(feature = "tls")]

//! 集群出站 mTLS 证书热换装端到端：对端钉 CA-B 根、本节点出站装 CA-A
//! 签发证书被拒；经真实 RESP 会话 CONFIG SET cert-file-name 轮换至 CA-B
//! 签发新证后，长驻出站实例（装配后永不重建）重连即呈新证由拒转受——
//! 出站与入站同一 ArcSwap 证书单源，热换装两方向一次生效
//!
//! 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile
//!（:100-117 换 selector 经 :185-189 出站闭包动态读传播出站；对位
//! test/standalone/Garnet.test/RespTlsTests.cs 轮换族）

use std::{fs::write, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  io::{AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::timeout,
};
use wnode::{GarnetServer, SessionProviderFace, service::StorageSessionProvider};
use wnode_test::session_factory;
use wnode_tls_test::{EchoProvider, mk_ca, ping, read_reply, server_tls_config, start_tls_server};
use wtest_base::{resp_frame_str, test_store_config};
use wtls::{ClientTlsConfig, ServerTlsConfig};

/// 轮换前被钉根对端拒绝判定：TLS1.3 拒绝面或落 connect 报错、或落首读
/// 失败/非 PONG（对端 alert 可在客户端 connect 完成后才到达）
async fn outbound_rejected(out: &ClientTlsConfig, addr: SocketAddr) -> bool {
  let endpoint = addr.to_string();
  let Ok(tcp) = TcpStream::connect(addr).await else {
    return true;
  };
  let mut stream = match timeout(Duration::from_secs(5), out.connect(tcp, &endpoint)).await {
    Err(_) => return true,
    Ok(Err(_)) => return true,
    Ok(Ok(stream)) => stream,
  };
  let _ = stream.write_all(b"PING\r\n".to_vec()).await;
  match timeout(Duration::from_secs(5), read_reply(&mut stream)).await {
    Err(_) => true,
    Ok(bytes) => !bytes.starts_with(b"+PONG"),
  }
}

/// 出站长驻实例随 CONFIG SET cert-file-name 轮换呈新证：
/// ① 装 CA-A 证出站握手被钉 CA-B 根的对端拒；② CONFIG SET 换装 CA-B 证
///（真实 RESP 会话 + 真实磁盘装载路径）；③ 同一 ClientTlsConfig 实例重连
/// 呈新证握手通过并 PING/PONG 达应用层（钉根臂只认 CA-B 链，接受即对端
/// 视角新 DN/序列号）；④ 换装前建立的在长连接不受影响（C# 不断已有连接）
#[test]
fn outbound_cluster_cert_follows_config_set_rotation() -> aok::Result<()> {
  let ca_a = mk_ca()?;
  let ca_b = mk_ca()?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("node-cert.pem");
  let key_path = dir.path().join("node-key.pem");
  write(&cert_path, ca_a.client_cert.pem())?;
  write(&key_path, ca_a.client_key.serialize_pem())?;

  // 本节点入站配置 + provider + server（tls_config_from_node 同源
  // from_pem_files 路径；CONFIG SET 会话侧与出站侧共享此实例）
  let node_tls = ServerTlsConfig::from_pem_files(&cert_path, &key_path, false, None, 0)?;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("node.db"),
      session_factory,
    )?
    .with_tls_config(node_tls.clone()),
  );
  let node = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider))?
    .with_tls_config(node_tls);
  node.start(NonZeroUsize::new(1))?;
  let node_addr = node.local_addr()?;

  // 对端：钉 CA-B 根的 mTLS 回声服务（tls-client-cert-required +
  // issuer 钉根消费面对位）
  let peer_tls = server_tls_config(true, Some(vec![ca_b.cert.der().to_vec().into()]))?;
  let (peer, peer_addr) = start_tls_server(Arc::new(EchoProvider), peer_tls, NonZeroUsize::new(1))?;

  // 出站装配单点（boot.rs 同形：自 provider.tls_config() 派生共享证书
  // 源句柄）；本实例长驻，轮换全程不重建
  let certs = provider.tls_config().map(|tls| tls.cert_source());
  let outbound = ClientTlsConfig::from_shared_source(certs, "", false, None)?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // ① 轮换前：CA-A 旧证被钉 CA-B 根的对端拒
    assert!(
      outbound_rejected(&outbound, peer_addr).await,
      "轮换前出站呈 CA-A 旧证，钉 CA-B 根的对端必须拒绝"
    );

    // ② CA-B 签发新证对落盘，真实 RESP 会话 CONFIG SET 轮换（管理通道
    // 经同一出站实例连本节点 TLS 端口）
    write(&cert_path, ca_b.client_cert.pem())?;
    write(&key_path, ca_b.client_key.serialize_pem())?;
    let mut admin = outbound
      .connect(TcpStream::connect(node_addr).await?, &node_addr.to_string())
      .await?;
    let _ = admin
      .write_all(resp_frame_str(&[
        "CONFIG",
        "SET",
        "cert-file-name",
        cert_path.to_str().expect("utf8 path"),
        "cert-password",
        key_path.to_str().expect("utf8 path"),
      ]))
      .await;
    admin.flush().await?;
    assert_eq!(read_reply(&mut admin).await, b"+OK\r\n");

    // ③ 轮换后：同一长驻出站实例重连呈新证，由拒转受且 PING/PONG 达
    // 应用层
    let mut fresh = outbound
      .connect(TcpStream::connect(peer_addr).await?, &peer_addr.to_string())
      .await?;
    ping(&mut fresh).await?;
    assert_eq!(
      read_reply(&mut fresh).await,
      b"+PONG\r\n",
      "轮换后长驻出站实例重连必须呈新证被对端接受"
    );

    // ④ 换装前建立的在长连接不受影响（C# 不断已有连接语义）
    ping(&mut admin).await?;
    assert_eq!(read_reply(&mut admin).await, b"+PONG\r\n");

    Ok::<(), wnode::Error>(())
  })?;

  peer.stop();
  node.stop();
  Ok(())
}
