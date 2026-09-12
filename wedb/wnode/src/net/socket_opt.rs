//! 套接字选项与端口复用绑定

use std::{io, net::SocketAddr};

use compio::net::{TcpListener, TcpSocket};

/// TCP 监听套接字默认等待队列容量
pub const TCP_LISTEN_BACKLOG: i32 = 1024;

/// 创建支持多核并发监听的套接字 (SO_REUSEPORT / SO_REUSEADDR)
pub async fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
  let socket = if addr.is_ipv6() {
    TcpSocket::new_v6().await
  } else {
    TcpSocket::new_v4().await
  }?;

  socket.set_reuseaddr(true)?;
  #[cfg(all(
    unix,
    not(target_os = "solaris"),
    not(target_os = "illumos"),
    not(target_os = "cygwin")
  ))]
  socket.set_reuseport(true)?;

  socket.bind(addr).await?;
  socket.listen(TCP_LISTEN_BACKLOG).await
}
