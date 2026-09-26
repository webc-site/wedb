use std::{
  fs::metadata,
  io::Error,
  mem::take,
  net::SocketAddr,
  num::NonZeroUsize,
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  runtime::Runtime,
};
use wconf::{RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wnode::{
  GarnetServer, MessageConsumerFace, SessionProviderFace, ShutdownCoordinator, WireFormat,
  bind_reuseport, config_owner::apply_config_reconcile, servers::ConsumerRegistry,
  service::open_node_with_config,
};
use wtest_base::test_store_config;

/// 回声会话桩：`PING\r\n` 整帧回 `+PONG\r\n`，残余半包驻留缓冲等待续读
#[derive(Default)]
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

/// 回声工厂：无注册表形态（纯收发用例）
struct EchoProvider;

impl SessionProviderFace for EchoProvider {
  type Consumer = EchoConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
    Some(EchoConsumer::default())
  }
}

/// 回声工厂 + 活跃消费者注册表（排空/上限用例的在途连接观测源）
struct RegistryProvider {
  registry: Arc<ConsumerRegistry>,
}

impl SessionProviderFace for RegistryProvider {
  type Consumer = EchoConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
    Some(EchoConsumer::default())
  }
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}

/// PING → +PONG 单帧往返：回声服务端到端用例的公共收发断言
async fn assert_pong<S: AsyncRead + AsyncWriteExt>(stream: &mut S) -> Result<(), Error> {
  let _ = stream.write_all(b"PING\r\n".to_vec()).await;
  let BufResult(res, read_buf) = stream.read(vec![0u8; 128]).await;
  let n = res?;
  assert_eq!(&read_buf[..n], b"+PONG\r\n");
  Ok(())
}

/// 单 worker 起一台随机端口 TCP 回声服务器（监听地址经 local_addr 取）
fn start_echo_server<P: SessionProviderFace + 'static>(
  provider: Arc<P>,
) -> aok::Result<GarnetServer<P>> {
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider).unwrap();
  server.start(NonZeroUsize::new(1))?;
  Ok(server)
}

#[test]
fn test_shutdown_coordinator() -> aok::Result<()> {
  let sc = ShutdownCoordinator::new();
  assert!(!sc.is_stopped());

  let listener = sc.listen();
  sc.stop();
  assert!(sc.is_stopped());

  let rt = Runtime::new()?;
  rt.block_on(async {
    listener.await;
  });
  Ok(())
}

#[compio::test]
async fn test_bind_reuseport() -> aok::Result<()> {
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let listener = bind_reuseport(addr).await?;
  let local = listener.local_addr()?;
  assert_ne!(local.port(), 0);

  // 测试 SO_REUSEPORT 允许再次绑定相同地址
  let listener2 = bind_reuseport(local).await?;
  assert_eq!(listener2.local_addr()?, local);
  Ok(())
}

#[cfg(unix)]
#[test]
fn test_uds_guard() -> aok::Result<()> {
  use std::os::unix::fs::PermissionsExt;

  use tempfile::tempdir;
  use wnode::UdsGuard;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let sock_path = dir.path().join("test.sock");

    {
      let (_listener, guard) = UdsGuard::bind(&sock_path, None).await?;
      assert!(sock_path.exists());
      assert_eq!(guard.path(), sock_path.as_path());
    }
    // guard drop 后，文件应已被清理
    assert!(!sock_path.exists());

    // unixsocketperm 指定即 bind 后收紧（C# GarnetServerTcp.cs:151
    // File.SetUnixFileMode 臂）；未指定档不断言具体位（避免 umask 依赖）
    let perm_path = dir.path().join("perm.sock");
    {
      let (_listener, _guard) = UdsGuard::bind(&perm_path, Some(0o600)).await?;
      let mode = metadata(&perm_path)?.permissions().mode();
      assert_eq!(mode & 0o777, 0o600);
    }
    Ok(())
  })
}

#[cfg(unix)]
#[test]
fn test_uds_guard_error_propagation() -> aok::Result<()> {
  use std::{fs, io::ErrorKind, os::unix::fs::PermissionsExt};

  use tempfile::tempdir;
  use wnode::UdsGuard;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 1. 空白路径显式拒绝（对标 C# ArgumentException.ThrowIfNullOrWhiteSpace）
    let err_empty = match UdsGuard::bind("", None).await {
      Err(e) => e,
      Ok(_) => panic!("期望空白路径失败"),
    };
    assert_eq!(err_empty.kind(), ErrorKind::InvalidInput);
    assert!(err_empty.to_string().contains("Unix 域套接字路径不能为空"));

    let err_spaces = match UdsGuard::bind("   ", None).await {
      Err(e) => e,
      Ok(_) => panic!("期望纯空格路径失败"),
    };
    assert_eq!(err_spaces.kind(), ErrorKind::InvalidInput);
    assert!(err_spaces.to_string().contains("Unix 域套接字路径不能为空"));

    let dir = tempdir()?;

    // 2. 路径为目录：remove_file 失败错误透传（点名路径和真实 errno）
    let dir_as_sock = dir.path().join("as_directory");
    fs::create_dir(&dir_as_sock)?;
    let err_dir = match UdsGuard::bind(&dir_as_sock, None).await {
      Err(e) => e,
      Ok(_) => panic!("期望路径为目录时失败"),
    };
    let err_str = err_dir.to_string();
    assert!(
      err_str.contains("清理旧 Unix 套接字文件失败") && err_str.contains("as_directory"),
      "文案须点名路径并透传真实失败原因: {err_str}"
    );

    // 3. 父目录不可写：绑定失败透传并点名路径
    let ro_parent = dir.path().join("ro_parent");
    fs::create_dir(&ro_parent)?;
    fs::set_permissions(&ro_parent, fs::Permissions::from_mode(0o555))?;
    let sock_in_ro = ro_parent.join("ro.sock");
    let err_ro = UdsGuard::bind(&sock_in_ro, None).await;
    // 恢复写权限以允许 tempdir 正常析构清理
    let _ = fs::set_permissions(&ro_parent, fs::Permissions::from_mode(0o755));
    let err_ro = match err_ro {
      Err(e) => e,
      Ok(_) => panic!("期望父目录只读时失败"),
    };
    assert_eq!(err_ro.kind(), ErrorKind::PermissionDenied);
    let err_ro_str = err_ro.to_string();
    assert!(
      err_ro_str.contains("ro.sock"),
      "文案须点名套接字路径: {err_ro_str}"
    );

    // 4. 父目录已存在且为文件（非目录无法自动创建）：错误透传点名路径
    let file_parent = dir.path().join("file_parent");
    fs::write(&file_parent, b"")?;
    let sock_in_file = file_parent.join("child.sock");
    let err_file = match UdsGuard::bind(&sock_in_file, None).await {
      Err(e) => e,
      Ok(_) => panic!("期望父目录为文件时失败"),
    };
    let err_file_str = err_file.to_string();
    assert!(
      err_file_str.contains("创建 Unix 套接字父目录失败") && err_file_str.contains("file_parent"),
      "文案须点名父目录路径: {err_file_str}"
    );

    Ok(())
  })
}

#[test]
fn test_garnet_server_lifecycle() -> aok::Result<()> {
  use compio::net::TcpStream;

  let server = start_echo_server(Arc::new(EchoProvider))?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await?;
    assert_pong(&mut stream).await?;
    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

/// 停机排空端到端：空闲长连接在 stop() 内收到终止并注销（C#
/// InternalDispose Phase 2 语义），join 返回时注册表已归零、客户端
/// 侧观测到连接被服务端关闭；远端半关闭连接同样被排空强收
#[test]
fn test_garnet_server_stop_drains_connections() -> aok::Result<()> {
  use compio::{BufResult, net::TcpStream};

  let registry = Arc::new(ConsumerRegistry::new());
  let provider = Arc::new(RegistryProvider {
    registry: Arc::clone(&registry),
  });
  let server = start_echo_server(provider)?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 会话建立并应答（应答先于 stop ⇒ 条目已注册、泵挂在空闲读上）后
    // 保持空闲——被动等待永不归零，必须经下杀令打断挂起读才能排空
    let mut full = TcpStream::connect(addr).await?;
    assert_pong(&mut full).await?;
    assert_eq!(registry.connection_totals(), (1, 0, 1));

    // stop 同步排空（远小于 5 秒超时护栏），返回即注册表归零
    let t0 = Instant::now();
    server.stop();
    assert!(
      t0.elapsed() < Duration::from_secs(4),
      "排空耗时 {:?} 疑似落到超时强收",
      t0.elapsed()
    );
    assert_eq!(registry.connection_totals(), (1, 1, 0));

    // 空闲连接被服务端终止：客户端观测到 EOF
    let BufResult(res, _) = full.read(vec![0u8; 1]).await;
    assert_eq!(res?, 0);
    Ok::<(), Error>(())
  })?;
  Ok(())
}

#[cfg(unix)]
#[test]
fn test_garnet_server_uds_lifecycle() -> aok::Result<()> {
  use compio::net::UnixStream;
  use tempfile::tempdir;

  let dir = tempdir()?;
  let sock_path = dir.path().join("uds_echo.sock");
  let ep = format!("unix:{}", sock_path.display());

  let server = GarnetServer::new(&[ep], 4096, 8, Arc::new(EchoProvider)).unwrap();
  server.start(NonZeroUsize::new(1))?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut stream = UnixStream::connect(&sock_path).await?;
    assert_pong(&mut stream).await?;
    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

#[test]
fn test_open_node_gc_config() -> aok::Result<()> {
  const TEST_GC_CONFIG_DB: &str = "test_gc_config.db";

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join(TEST_GC_CONFIG_DB);
    // 小预算测试配置注入（生产缺省经 open_from_args 走 store_config() 的
    // StoreConfig::auto，大机上规划出 GB 级索引）。默认装配 GC 禁用（对标 C#
    // ExpiredKeyDeletionScanFrequencySecs = -1）：槽位即唯一启停真值源，
    // 装配不越权开后台任务
    let (store, _broker, _vector_manager) = open_node_with_config(test_store_config(), &db_path)?;
    let gc_cfg = store.gc_config();
    assert!(!gc_cfg.enabled);
    assert_eq!(gc_cfg.scan_interval_ms, 0);
    assert!(!store.gc_running());

    // CONFIG SET 正值 → 调停拉起（try_set 落槽产出消息 →
    // apply_config_reconcile → 扫描循环在跑，与 config_owner_bridge 同判）
    let runtime_config = RuntimeServerConfig::new(RuntimeServerOptions::default());
    if let Some(msg) = runtime_config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, "5")? {
      apply_config_reconcile(None, &store, None, None, msg);
    }
    assert_eq!(store.gc_config().scan_interval_ms, 5_000);
    assert!(store.gc_running());

    // CONFIG SET 回 -1 → 禁用 + 循环退出
    if let Some(msg) = runtime_config.try_set(ServerConfigType::ExpiredKeyDeletionScanFreq, "-1")? {
      apply_config_reconcile(None, &store, None, None, msg);
    }
    assert!(!store.gc_config().enabled);
    assert!(!store.gc_running());
    Ok(())
  })
}

/// 连接上限 accept 守卫端到端（C# GarnetServerTcp.cs:236-241/302-307）：
/// limit=2 时前两条正常应答、第三条被即刻关闭（客户端见 EOF，非错误
/// 应答）；释放一条后在途归零、新连接可再进（计数不漂）
#[test]
fn test_network_connection_limit_rejects_excess() -> aok::Result<()> {
  use compio::{BufResult, net::TcpStream, time::sleep};

  let registry = Arc::new(ConsumerRegistry::new());
  let provider = Arc::new(RegistryProvider {
    registry: Arc::clone(&registry),
  });
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider)
    .unwrap()
    .with_network_connection_limit(2);
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 前两条占住在途额度：PING → PONG 正常
    let mut c1 = TcpStream::connect(addr).await?;
    assert_pong(&mut c1).await?;
    let mut c2 = TcpStream::connect(addr).await?;
    assert_pong(&mut c2).await?;

    // 第三条：connect 后不做任何写，读侧见立即 EOF（超限臂只关不发，
    // 客户端无任何 RESP 应答可读）
    let mut c3 = TcpStream::connect(addr).await?;
    let BufResult(res, _) = c3.read(vec![0u8; 16]).await;
    assert_eq!(res?, 0, "超限连接须被即刻关闭（EOF）");

    // 释放一条（客户端关闭 → 泵注销 + 守卫归零），轮询等在途归位后
    // 新连接可再进：证明计数随生命周期配对回收、无泄漏漂移
    drop(c1);
    let mut admitted = None;
    for _ in 0..200 {
      if let Ok(mut stream) = TcpStream::connect(addr).await {
        let _ = stream.write_all(b"PING\r\n".to_vec()).await;
        let BufResult(res, read_buf) = stream.read(vec![0u8; 128]).await;
        if res.is_ok_and(|n| &read_buf[..n] == b"+PONG\r\n") {
          admitted = Some(stream);
          break;
        }
      }
      sleep(Duration::from_millis(50)).await;
    }
    assert!(admitted.is_some(), "释放后新连接须可再进（在途计数归零）");

    server.stop();
    Ok::<(), Error>(())
  })?;
  Ok(())
}
