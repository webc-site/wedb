#![cfg(feature = "tls")]

//! 证书在线换装端到端：CONFIG SET 重载、定时刷新轮换、跨运行时重挂、
//! 室外降级可观测（换证语义需多份不同证书，现场 rcgen 生成）
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/RespTlsTests.cs

use std::{fs::write, net::SocketAddr, num::NonZeroUsize, sync::Arc, thread, time::Duration};

use compio::{
  io::{AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use rcgen::generate_simple_self_signed;
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::session_factory;
use wnode_tls_test::{is_bad_signature, ping, read_reply, trust_connector};
use wtest_base::{resp_frame_str, test_store_config};
use wtls::ServerTlsConfig;

/// 轮询参数：100ms 间隔、10s 上界、单次探测限时 2s（周期刷新生效态的等待锚：
/// 以可观测生效态为据，不赌单拍）
const RELOAD_POLL_ATTEMPTS: usize = 100;
const RELOAD_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RELOAD_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// 有界轮询至刷新循环感知磁盘新证书：新 CA 握手 + PING 首次成功即收敛
///（对标 C# RespTlsTests.cs 重连重试等证书生效；固定睡赌单拍在刷新任务
/// 被并行测试进程调度延迟、错过一拍时误红）
async fn await_cert_served(cert_der: Vec<u8>, addr: SocketAddr) -> bool {
  for _ in 0..RELOAD_POLL_ATTEMPTS {
    let served = timeout(RELOAD_PROBE_TIMEOUT, async {
      let connector = trust_connector(cert_der.clone()).ok()?;
      let stream = TcpStream::connect(addr).await.ok()?;
      let mut tls = connector.connect("localhost", stream).await.ok()?;
      ping(&mut tls).await.ok()?;
      (read_reply(&mut tls).await == b"+PONG\r\n").then_some(())
    })
    .await;
    if matches!(served, Ok(Some(()))) {
      return true;
    }
    sleep(RELOAD_POLL_INTERVAL).await;
  }
  false
}

/// CONFIG SET cert-file-name / cert-password 在线重载：+OK 后新握手用新
/// 证书、旧 CA 新握手被拒、已建立连接读写不受影响
///
/// 在 garnet 中的相对路径: libs/server/ServerConfig.cs:NetworkCONFIG_SET
///（certFileName/certPassword 臂 → GarnetTlsOptions.cs:UpdateCertFile 实时
/// 重构证书选择器；rust 对位 DynamicCertResolver ArcSwap 原子换装）
#[test]
fn config_set_cert_file_reloads_online() -> aok::Result<()> {
  // 换装前后两套独立自签名证书落盘（C# CertFileName/CertPassword PEM 形态：
  // password 即独立私钥文件路径）
  let old = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let new = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("cert.pem");
  let key_path = dir.path().join("key.pem");
  write(&cert_path, old.cert.pem())?;
  write(&key_path, old.signing_key.serialize_pem())?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // PEM 装配在运行时内（from_pem_files 契约）
    let tls = ServerTlsConfig::from_pem_files(&cert_path, &key_path, false, None, 0)?;
    let provider = Arc::new(
      StorageSessionProvider::open_with_config(
        test_store_config(),
        dir.path().join("hot-reload.db"),
        session_factory,
      )?
      // 同一实例分持两端：网络端点（run_async 经 SessionProviderFace::
      // tls_config 取用）与 CONFIG SET 会话域共享同一活跃证书
      .with_tls_config(tls.clone()),
    );
    let server =
      GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?.with_tls_config(tls);
    server.start(NonZeroUsize::new(1))?;
    let addr = server.local_addr()?;

    let old_connector = trust_connector(old.cert.der().to_vec())?;
    let new_connector = trust_connector(new.cert.der().to_vec())?;

    // 初次握手：旧证书
    let mut legacy = old_connector
      .connect("localhost", TcpStream::connect(addr).await?)
      .await?;
    ping(&mut legacy).await?;
    assert_eq!(read_reply(&mut legacy).await, b"+PONG\r\n");

    // 新证书对就地落盘至同一路径（换装臂指向同一文件，ACME 轮换形态；
    // 证书与私钥必须成对换新，禁只换证书造成链-钥错配）
    write(&cert_path, new.cert.pem())?;
    write(&key_path, new.signing_key.serialize_pem())?;

    // CONFIG SET 在线重载至新证书
    let _ = legacy
      .write_all(resp_frame_str(&[
        "CONFIG",
        "SET",
        "cert-file-name",
        cert_path.to_str().expect("utf8 path"),
        "cert-password",
        key_path.to_str().expect("utf8 path"),
      ]))
      .await;
    legacy.flush().await?;
    assert_eq!(read_reply(&mut legacy).await, b"+OK\r\n");

    // 新握手用新证书（新 CA 信任锚成功）
    let mut fresh = new_connector
      .connect("localhost", TcpStream::connect(addr).await?)
      .await?;
    ping(&mut fresh).await?;
    assert_eq!(read_reply(&mut fresh).await, b"+PONG\r\n");

    // 旧 CA 新握手被拒：rustls 客户端对服务端新证书链校验失败
    //（timeout 外层 Err = 超时挂起、内层 Err = 握手被拒；断言取内层握手
    // 失败形态。用全新 connector 发起：复用 legacy 的 ClientConfig 会话
    // 缓存会走 TLS1.3 resumption 跳过服务端证书校验，测不到换证语义）
    let old_ca_fresh = trust_connector(old.cert.der().to_vec())?;
    let rejected = timeout(
      Duration::from_secs(5),
      old_ca_fresh.connect("localhost", TcpStream::connect(addr).await?),
    )
    .await;
    assert!(
      matches!(&rejected, Ok(Err(e)) if is_bad_signature(e)),
      "服务端已换新证书，旧 CA 客户端新握手须拒于 InvalidCertificate(BadSignature): {:?}",
      rejected.map(|r| r.map(|_| ()))
    );

    // 已建立连接不受换装影响（C# 不断已有连接语义）
    ping(&mut legacy).await?;
    assert_eq!(read_reply(&mut legacy).await, b"+PONG\r\n");

    server.stop();
    Ok::<(), wnode::Error>(())
  })?;

  Ok(())
}

/// 无 TLS 明文节点 CONFIG SET cert-file-name 准确回 ERR TLS is disabled.
///（C# storeWrapper.serverOptions.TlsOptions == null 臂）
#[test]
fn config_set_cert_rejected_without_tls() -> aok::Result<()> {
  let dir = tempfile::tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join("no-tls.db"),
    session_factory,
  )?);
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?;
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await?;
    let _ = stream
      .write_all(resp_frame_str(&[
        "CONFIG",
        "SET",
        "cert-file-name",
        "whatever.pem",
      ]))
      .await;
    stream.flush().await?;
    assert_eq!(read_reply(&mut stream).await, b"-ERR TLS is disabled.\r\n");

    Ok::<(), wnode::Error>(())
  })?;

  server.stop();
  Ok(())
}

/// --cert-refresh-freq 定时热轮换：就覆盖磁盘证书文件后，刷新周期内新握手
/// 自动感知新证书（对标 C# ServerCertificateSelector 构造 Timer 周期
/// GetServerCertificate 原子换装 sslServerCertificate）
#[test]
fn cert_refresh_timer_serves_rotated_certificate() -> aok::Result<()> {
  let first = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let rotated = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("rotating.pem");
  let key_path = dir.path().join("rotating.key");
  write(&cert_path, first.cert.pem())?;
  write(&key_path, first.signing_key.serialize_pem())?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 刷新周期 1 秒（后台循环拉起须在运行时内）
    let tls = ServerTlsConfig::from_pem_files(&cert_path, &key_path, false, None, 1)?;
    let provider = Arc::new(
      StorageSessionProvider::open_with_config(
        test_store_config(),
        dir.path().join("refresh.db"),
        session_factory,
      )?
      .with_tls_config(tls.clone()),
    );
    let server =
      GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?.with_tls_config(tls);
    server.start(NonZeroUsize::new(1))?;
    let addr = server.local_addr()?;

    // 初次握手：第一版证书
    let first_connector = trust_connector(first.cert.der().to_vec())?;
    let mut legacy = first_connector
      .connect("localhost", TcpStream::connect(addr).await?)
      .await?;
    ping(&mut legacy).await?;
    assert_eq!(read_reply(&mut legacy).await, b"+PONG\r\n");

    // 就地覆盖磁盘证书文件：证书与私钥成对轮换（ACME 自动轮换的
    // 运行中进程不可见写盘形态，cert.pem 与 key.pem 同步换新）
    write(&cert_path, rotated.cert.pem())?;
    write(&key_path, rotated.signing_key.serialize_pem())?;

    // 有界轮询至刷新循环感知磁盘轮换：新握手自动取到新证书
    //（1s 周期，10s 上界）
    assert!(
      await_cert_served(rotated.cert.der().to_vec(), addr).await,
      "刷新循环未在时限内服务轮换证书"
    );

    server.stop();
    Ok::<(), wnode::Error>(())
  })?;

  Ok(())
}

/// 修复链真实覆盖（审查发现①）：纯线程（无 compio 运行时域）装配 freq>0
/// 配置——「构造即挂表」臂为无操作，仅记代际意图——再由运行室内
/// GarnetServer::start 的 ensure_refresh_loop 承接挂表，轮换证书被真实
/// 服务（新 cert 握手成功、旧 cert 拒于 BadSignature）。此前用例均在
/// 运行时内当场构造，本链零覆盖
///
/// 在 garnet 中的相对路径: libs/server/TLS/ServerCertificateSelector.cs:62
/// (C# 构造即挂 Timer；rust 室外构造由启动臂承接为唯一合法对位链)
#[test]
fn cert_refresh_serves_rotation_assembled_outside_runtime() -> aok::Result<()> {
  let first = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let rotated = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("chain.pem");
  let key_path = dir.path().join("chain.key");
  write(&cert_path, first.cert.pem())?;
  write(&key_path, first.signing_key.serialize_pem())?;

  // 室外构造：纯线程无运行时域，挂表意图留待 start 承接
  let (cp, kp) = (cert_path.clone(), key_path.clone());
  let tls = thread::spawn(move || ServerTlsConfig::from_pem_files(cp, kp, false, None, 1))
    .join()
    .expect("构造线程不应 panic")?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let provider = Arc::new(
      StorageSessionProvider::open_with_config(
        test_store_config(),
        dir.path().join("chain.db"),
        session_factory,
      )?
      .with_tls_config(tls.clone()),
    );
    let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?
      .with_tls_config(tls.clone());
    server.start(NonZeroUsize::new(1))?;
    let addr = server.local_addr()?;

    // 初次握手：第一版证书
    let mut legacy = trust_connector(first.cert.der().to_vec())?
      .connect("localhost", TcpStream::connect(addr).await?)
      .await?;
    ping(&mut legacy).await?;
    assert_eq!(read_reply(&mut legacy).await, b"+PONG\r\n");

    // 就地成对轮换；有界轮询至室内 start 承接挂表真实生效（新 CA 握手
    // 取到轮换证书，10s 上界）
    write(&cert_path, rotated.cert.pem())?;
    write(&key_path, rotated.signing_key.serialize_pem())?;
    assert!(
      await_cert_served(rotated.cert.der().to_vec(), addr).await,
      "室外构造→室内 start 链未在时限内服务轮换证书"
    );

    // 旧 CA 握手被拒于具体变体（全新 connector 发起，避 TLS1.3 resumption 旁路）
    let rejected = timeout(
      Duration::from_secs(5),
      trust_connector(first.cert.der().to_vec())?
        .connect("localhost", TcpStream::connect(addr).await?),
    )
    .await;
    assert!(
      matches!(&rejected, Ok(Err(e)) if is_bad_signature(e)),
      "室外构造→室内 start 链的轮换未以 BadSignature 拒绝旧 CA: {:?}",
      rejected.map(|r| r.map(|_| ()))
    );

    server.stop();
    Ok::<(), wnode::Error>(())
  })?;

  Ok(())
}

/// 去重票据回收（审查发现②）：宿主运行时析构后刷新循环已死，但
/// running_epoch 旧形制不复位 ⇒ 同 config 二次 start 永久漏挂。两幕断言：
/// rt1 首次 start 挂表真实生效（轮换 v2 被服务）；rt1 析构后 rt2 二次
/// start 重挂（再轮换 v3 被服务、v2 拒于 BadSignature）——修复前第二幕红
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:150-156
///（C# 定时器恒可重挂；rust 以票据复位承接跨运行时回收）
#[test]
fn cert_refresh_loop_remounts_after_host_runtime_exit() -> aok::Result<()> {
  let v1 = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let v2 = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let v3 = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("remount.pem");
  let key_path = dir.path().join("remount.key");
  write(&cert_path, v1.cert.pem())?;
  write(&key_path, v1.signing_key.serialize_pem())?;

  let (cp, kp) = (cert_path.clone(), key_path.clone());
  let tls = thread::spawn(move || ServerTlsConfig::from_pem_files(cp, kp, false, None, 1))
    .join()
    .expect("构造线程不应 panic")?;

  // 幕一：rt1 内 start 挂表，轮换 v1→v2 被服务（首挂基线，排除②误判为①）
  let rt1 = Runtime::new()?;
  rt1.block_on(async {
    let provider = Arc::new(
      StorageSessionProvider::open_with_config(
        test_store_config(),
        dir.path().join("remount1.db"),
        session_factory,
      )?
      .with_tls_config(tls.clone()),
    );
    let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?
      .with_tls_config(tls.clone());
    server.start(NonZeroUsize::new(1))?;
    let addr = server.local_addr()?;

    write(&cert_path, v2.cert.pem())?;
    write(&key_path, v2.signing_key.serialize_pem())?;
    // 有界轮询至首挂刷新循环服务 v2（10s 上界）
    assert!(
      await_cert_served(v2.cert.der().to_vec(), addr).await,
      "首挂刷新未在时限内服务轮换证书 v2"
    );

    server.stop();
    Ok::<(), wnode::Error>(())
  })?;
  // 宿主运行时析构：刷新任务随 executor 清理（修复前票据卡死当代际）
  drop(rt1);

  // 幕二：rt2 二次 start 必须重挂，再轮换 v2→v3 被服务
  let rt2 = Runtime::new()?;
  rt2.block_on(async {
    let provider = Arc::new(
      StorageSessionProvider::open_with_config(
        test_store_config(),
        dir.path().join("remount2.db"),
        session_factory,
      )?
      .with_tls_config(tls.clone()),
    );
    let server =
      GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?.with_tls_config(tls);
    server.start(NonZeroUsize::new(1))?;
    let addr = server.local_addr()?;

    write(&cert_path, v3.cert.pem())?;
    write(&key_path, v3.signing_key.serialize_pem())?;
    // 有界轮询至二次 start 重挂的刷新循环服务 v3（10s 上界）
    assert!(
      await_cert_served(v3.cert.der().to_vec(), addr).await,
      "二次 start 漏挂刷新：v3 未在时限内被服务（rt1 票据未复位则循环未重挂）"
    );

    // v2 此刻须已被 v3 换下（证明确发生了新一轮重载而非残留）
    let stale = timeout(
      Duration::from_secs(5),
      trust_connector(v2.cert.der().to_vec())?
        .connect("localhost", TcpStream::connect(addr).await?),
    )
    .await;
    assert!(
      matches!(&stale, Ok(Err(e)) if is_bad_signature(e)),
      "重挂循环应已换装 v3，v2 信任锚须拒于 BadSignature: {:?}",
      stale.map(|r| r.map(|_| ()))
    );

    server.stop();
    Ok::<(), wnode::Error>(())
  })?;

  Ok(())
}

/// 室外静默降级可观测性（审查发现③）：freq>0 且运行室外 server.start，
/// 刷新循环无 executor 可挂——修复前该臂无操作且零留痕（红相证据：
/// /tmp/tls-review-red.log 中 grep「证书刷新循环未挂载」0 命中），修复后
/// ensure_refresh_loop 返 false 供判定、server.rs 失败侧落 warn 行
/// （绿相证据：--nocapture 实采 /tmp/tls-review-green.log）
#[test]
fn start_outside_runtime_reports_missing_mount() -> aok::Result<()> {
  let cert = generate_simple_self_signed(vec!["localhost".to_string()])?;
  let dir = tempfile::tempdir()?;
  let cert_path = dir.path().join("outside.pem");
  let key_path = dir.path().join("outside.key");
  write(&cert_path, cert.cert.pem())?;
  write(&key_path, cert.signing_key.serialize_pem())?;

  let (cp, kp) = (cert_path.clone(), key_path.clone());
  let tls = thread::spawn(move || ServerTlsConfig::from_pem_files(cp, kp, false, None, 1))
    .join()
    .expect("构造线程不应 panic")?;

  // 室外降级观测面：freq>0 且无运行时域必须报 false（修复前返回单元、零留痕）
  assert!(
    !tls.ensure_refresh_loop(),
    "运行室外 ensure_refresh_loop 须报未挂载（false）"
  );
  // freq=0 是 C# 不挂 Timer 的禁用常态，不得混入降级臂
  let (cp0, kp0) = (cert_path.clone(), key_path.clone());
  let disabled = thread::spawn(move || ServerTlsConfig::from_pem_files(cp0, kp0, false, None, 0))
    .join()
    .expect("构造线程不应 panic")?;
  assert!(
    disabled.ensure_refresh_loop(),
    "freq=0 室外亦须报 true（禁用非降级）"
  );

  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("outside.db"),
      session_factory,
    )?
    .with_tls_config(tls.clone()),
  );
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)?
    .with_tls_config(tls.clone());
  // 室外 start（wnode_test::start_server 同形制）：失败侧 warn 留痕
  server.start(NonZeroUsize::new(1))?;
  server.stop();

  // 室内承接：报 true 且真实挂表；宿主析构票据复位后二次室内进入仍可挂
  let rt = Runtime::new()?;
  rt.block_on(async {
    assert!(
      tls.ensure_refresh_loop(),
      "运行时内 ensure_refresh_loop 须报挂载成功（true）"
    );
    Ok::<(), wnode::Error>(())
  })?;
  drop(rt);
  let rt2 = Runtime::new()?;
  rt2.block_on(async {
    assert!(
      tls.ensure_refresh_loop(),
      "宿主运行时析构后二次进入须可重挂（true）"
    );
    Ok::<(), wnode::Error>(())
  })?;

  Ok(())
}
