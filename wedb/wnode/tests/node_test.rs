use std::net::SocketAddr;

use compio::runtime::Runtime;
use wnode::{ShutdownCoordinator, bind_reuseport};

#[test]
fn test_shutdown_coordinator() -> aok::Result<()> {
  let sc = ShutdownCoordinator::new();
  assert!(!sc.is_stopped());

  let rx = sc.new_cancel_channel();
  sc.stop();
  assert!(sc.is_stopped());

  assert!(rx.try_recv().is_ok());
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
    fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
      if req_buffer.starts_with(b"PING\r\n") {
        (6, b"+PONG\r\n".to_vec())
      } else {
        (0, Vec::new())
      }
    }
    fn dispose(&self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<Arc<EchoConsumer>> {
      Some(Arc::new(EchoConsumer))
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
    fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
      if req_buffer.starts_with(b"PING\r\n") {
        (6, b"+PONG\r\n".to_vec())
      } else {
        (0, Vec::new())
      }
    }
    fn dispose(&self) {}
  }

  struct EchoProvider;
  impl SessionProviderFace for EchoProvider {
    type Consumer = EchoConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<Arc<EchoConsumer>> {
      Some(Arc::new(EchoConsumer))
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
