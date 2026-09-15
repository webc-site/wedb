use std::net::SocketAddr;

use compio::runtime::Runtime;
use wedb_test::test_store_config;
use wkv::{DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_SCAN_INTERVAL_MS};
use wnode::{ShutdownCoordinator, bind_reuseport, service::open_node_with_config};

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

#[test]
fn test_bind_reuseport() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_reuseport(addr).await?;
    let local = listener.local_addr()?;
    assert_ne!(local.port(), 0);

    // 测试 SO_REUSEPORT 允许再次绑定相同地址
    let listener2 = bind_reuseport(local).await?;
    assert_eq!(listener2.local_addr()?, local);
    Ok(())
  })
}

#[cfg(unix)]
#[test]
fn test_uds_guard() -> aok::Result<()> {
  use tempfile::tempdir;
  use wnode::UdsGuard;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let sock_path = dir.path().join("test.sock");

    {
      let (_listener, guard) = UdsGuard::bind(&sock_path).await?;
      assert!(sock_path.exists());
      assert_eq!(guard.path(), sock_path.as_path());
    }
    // guard drop 后，文件应已被清理
    assert!(!sock_path.exists());
    Ok(())
  })
}

#[test]
fn test_garnet_server_lifecycle() -> aok::Result<()> {
  use std::{io::Error, num::NonZeroUsize, sync::Arc};

  use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::TcpStream,
  };
  use wnode::{GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat};

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

  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(EchoProvider),
  );
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await?;
    let _ = stream.write_all(b"PING\r\n".to_vec()).await;
    let buf = vec![0u8; 128];
    let BufResult(res, read_buf) = stream.read(buf).await;
    let n = res?;
    assert_eq!(&read_buf[..n], b"+PONG\r\n");
    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

#[cfg(unix)]
#[test]
fn test_garnet_server_uds_lifecycle() -> aok::Result<()> {
  use std::{io::Error, num::NonZeroUsize, sync::Arc};

  use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::UnixStream,
  };
  use tempfile::tempdir;
  use wnode::{GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat};

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

  let dir = tempdir()?;
  let sock_path = dir.path().join("uds_echo.sock");
  let ep = format!("unix:{}", sock_path.display());

  let server = GarnetServer::new(&[ep], 4096, 8, Arc::new(EchoProvider));
  server.start(NonZeroUsize::new(1))?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut stream = UnixStream::connect(&sock_path).await?;
    let _ = stream.write_all(b"PING\r\n".to_vec()).await;
    let buf = vec![0u8; 128];
    let BufResult(res, read_buf) = stream.read(buf).await;
    let n = res?;
    assert_eq!(&read_buf[..n], b"+PONG\r\n");
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
    // 小预算测试配置注入（生产缺省 open_node 走 StoreConfig::auto，大机
    // 上规划出 GB 级索引）；test_store_config 的 GC 装配与生产缺省一致
    //（enabled + DEFAULT 扫描/紧缩周期），断言面不变
    let (store, _broker, _vector_manager) = open_node_with_config(test_store_config(), &db_path)?;
    let gc_cfg = store.gc_config();

    assert!(gc_cfg.enabled);
    assert_eq!(gc_cfg.scan_interval_ms, DEFAULT_GC_SCAN_INTERVAL_MS);
    assert_eq!(
      gc_cfg.compaction_interval_ms,
      DEFAULT_GC_COMPACTION_INTERVAL_MS
    );
    assert!(store.gc_running());
    Ok(())
  })
}
