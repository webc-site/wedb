//! 接入侧套接字选项内核落位测试（归位自 wnode::net::socket_opt 内联测试，
//! 仅依赖 pub API，集成级起真实 Runtime + bind + connect + accept）

use std::{io, net::SocketAddr};

use compio::{net::TcpStream, runtime::Runtime};
use socket2::SockRef;
use wnode::net::{bind_reuseport, configure_socket};

/// 生产唯一装配路径实测：接入侧套接字装上 nodelay 与 SO_KEEPALIVE
///
/// keepidle / keepintvl / keepcnt 的数值读回要 socket2 的 "all" 特性，
/// 三参数本身由 socket_opt 的常量单点定义，此处只断言开关确实落到内核
#[test]
fn test_configure_socket_sets_nodelay_and_keepalive() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_reuseport(addr).await?;
    // 握手在内核 backlog 中自行完成，accept 后发先至不影响取回连接
    let _client = TcpStream::connect(listener.local_addr()?).await?;
    let (accepted, _peer) = listener.accept().await?;

    configure_socket(&accepted)?;

    let sock = SockRef::from(&accepted);
    assert!(sock.tcp_nodelay()?, "TCP_NODELAY 未生效");
    assert!(sock.keepalive()?, "SO_KEEPALIVE 未开启");
    Ok::<(), io::Error>(())
  })?;
  Ok(())
}
