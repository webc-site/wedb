//! 套接字选项与端口复用绑定
//!
//! 对标 C# GarnetServerTcp accept 内联 socket 装配（C#:249 处仅设 NoDelay，
//! keepalive 为 rust 自有面）

use std::{io, net::SocketAddr, time::Duration};

use compio::net::{TcpListener, TcpSocket, TcpStream};
use socket2::{SockRef, TcpKeepalive};

/// TCP 监听套接字默认等待队列容量
pub(crate) const TCP_LISTEN_BACKLOG: i32 = 512;

/// 保活空闲探测时间（300 秒，对标 Linux 常见默认），生产唯一取值
pub const DEFAULT_TCP_KEEPIDLE: Duration = Duration::from_secs(300);
/// 保活探测间隔时间（10 秒），生产唯一取值
pub const DEFAULT_TCP_KEEPINTVL: Duration = Duration::from_secs(10);
/// 保活探测最大重试次数（3 次），生产唯一取值
pub const DEFAULT_TCP_KEEPCNT: u32 = 3;

/// 配置 TCP 传输套接字选项（对标 C# GarnetServerTcp accept 内联装配）
///
/// 1. 禁用 Nagle 算法（TCP_NODELAY = true，C# 同处唯一实设项）；
/// 2. 以 DEFAULT_TCP_KEEP* 三常量开启 TCP 保活探测（SO_KEEPALIVE, keepidle,
///    keepintvl, keepcnt），防止跨节点长连接被静默切断。C# 无此项，三参数亦不经
///    配置面暴露（wconf/NodeArgs 零字段）；将来需按部署环境调参时，连同配置旋钮
///    一并重开，不先留一条只有单一取值的空形参。
pub fn configure_socket(stream: &TcpStream) -> io::Result<()> {
  let _ = stream.set_nodelay(true);

  let mut ka = TcpKeepalive::new().with_time(DEFAULT_TCP_KEEPIDLE);
  #[cfg(not(any(
    target_os = "openbsd",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos"
  )))]
  {
    ka = ka.with_interval(DEFAULT_TCP_KEEPINTVL);
  }
  #[cfg(not(any(
    target_os = "openbsd",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "windows"
  )))]
  {
    ka = ka.with_retries(DEFAULT_TCP_KEEPCNT);
  }
  SockRef::from(stream).set_tcp_keepalive(&ka)?;

  Ok(())
}

/// 创建支持多核并发监听的套接字 (SO_REUSEPORT / SO_REUSEADDR)
///
/// 与 C# `GarnetServerTcp.cs:111-114` 意图对照、裁决相反（有意设计偏差，登记于
/// `doc/zh/deviations.md` 第 11 条）：
/// - C# 单 Listener 集中 Accept + 线程池分发，全局仅一个 listenSocket，无需端口
///   复用，显式关 SO_REUSEPORT 防误启第二实例分抢端口；
/// - Rust 为 compio thread-per-core 架构，每 worker 线程独立 bind 同端
///   （`server.rs` 的 `start_tcp_workers`），SO_REUSEPORT 是内核按连接四元组哈希
///   分流的唯一机制，属架构底层刚性前提——若关闭，除 worker 0 外其余 worker 启动
///   即报 EADDRINUSE 崩溃。**严禁误改或关闭本选项。**
///
/// 安全边界：Linux ≥ 3.9 内核强制复用同端口套接字须同 effective UID，异 UID 进程
/// 无法复用抢入；同机同 UID 误启第二实例的分流为可观测的有意选择，防并发双写职责
/// 由存储目录排他锁（flock）承担，明确拒绝网络层双活探测（TOCTOU 竞态与假阳性，
/// 属过度设计）。
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

#[cfg(test)]
mod tests {
  use compio::runtime::Runtime;

  use super::*;

  /// 生产唯一装配路径实测：接入侧套接字装上 nodelay 与 SO_KEEPALIVE
  ///
  /// keepidle / keepintvl / keepcnt 的数值读回要 socket2 的 "all" 特性，
  /// 三参数本身由上面的常量单点定义，此处只断言开关确实落到内核
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
}
