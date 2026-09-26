//! 统一网络连接流抽象：封装 TCP、Unix 域套接字以及可选 TLS 流
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs`
//! - `libs/server/Servers/ServerTcpNetworkHandler.cs`

use std::io;

#[cfg(unix)]
use compio::net::UnixStream;
use compio::{
  BufResult,
  buf::IoBuf,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
};
use wbase::primed::PrimedRecv;
#[cfg(feature = "tls")]
use wtls::stream::{
  TlsHandles, TlsStream, tls_append_read, tls_flush, tls_shutdown, tls_write_flush,
};

/// 客户端网络流载体类型
///
/// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:networkSender / Stream / Socket
pub enum ConnectionStream {
  /// 标准 TCP 传输流
  Tcp(TcpStream),
  /// 本地 Unix 域套接字传输流
  #[cfg(unix)]
  Unix(UnixStream),
  /// 纯 Rust TLS 安全传输流 (基于 rustls)，读写句柄对共享同一连接状态机
  #[cfg(feature = "tls")]
  Tls(TlsHandles),
}

/// 明文 TCP 流本地端点文本（读取失败回退空串）
///
/// 在 garnet 中的相对路径: libs/common/Networking/TcpNetworkHandlerBase.cs:41
/// （构造期捕获取值式 `socket.LocalEndPoint?.ToString() ?? string.Empty`）。
/// TLS 装配下该取值必须发生在握手前——accept 得到的裸 `TcpStream` 是唯一可
/// 同步读取 local_addr 的时机（握手后 `TlsStream` 型擦除不透出内层）；捕获值
/// 经 `NetworkHandler` 构造注入（handler 域单源，对标 C# localEndpointName
/// 为 handler 构造器字段）
#[inline]
pub(crate) fn tcp_local_endpoint(stream: &TcpStream) -> String {
  stream
    .local_addr()
    .map_or(String::new(), |addr| addr.to_string())
}

/// 读写臂派发宏：本文件三臂分发的唯一书写处
///
/// 入参取流的共享视图 `&ConnectionStream`：独占形态入口（`&mut self`）在调用处写
/// `&*self` 交出共享视图，共享形态入口（`&self`）原样交出 —— 一对借用形态由此共用
/// 同一函数体。compio 对 `&mut S` 与 `&S` 的读写实现都转发到同一个 `&S`，故这层归一
/// 不改变任何传输语义（`&mut` 入口的独占性仍由方法签名在调用点保证）。
/// 明文臂（Tcp / Unix）对具体流同形调用，臂内按原名重绑一次可变局部，以取到
/// `&mut &S` 接收者（与改动前 `let mut r: &TcpStream = s` 同一写法）；TLS 臂落到
/// 函数级唯一的四个内核 `tls_append_read` / `tls_write_flush` / `tls_flush` /
/// `tls_shutdown`，shared 与非 shared 变体共用之。
///
/// 只能在 `impl ConnectionStream` 内使用（臂模式写作 `Self::`）。函数级映射声明仍只
/// 挂在六个入口方法上，本宏不复述任何映射键；形参写作 `接收者 => 明文臂, tls 臂`
/// 这一非表达式形态，rustfmt 不重排，三臂在调用点各占一行内。
macro_rules! stream_io {
  ($selfv:expr => |$stream:ident| $plain:expr, tls |$handles:ident| $tls:expr) => {
    match $selfv {
      Self::Tcp($stream) => {
        let mut $stream = $stream;
        $plain
      }
      #[cfg(unix)]
      Self::Unix($stream) => {
        let mut $stream = $stream;
        $plain
      }
      #[cfg(feature = "tls")]
      Self::Tls($handles) => $tls,
    }
  };
}

impl ConnectionStream {
  /// 装配 TLS 安全传输流：握手就绪的连接拆成读写互斥句柄对
  ///
  /// 函数级映射声明归 handler/mod.rs 的 NetworkHandler 装配点；本函数对应 C# 的
  /// TLS 分支 libs/common/Networking/NetworkHandler.cs:126
  /// `sslStream = new SslStream(...)` —— 握手就绪后 TLS 会话的网络发送器即本处理器
  /// （本地端点在握手前经 [`tcp_local_endpoint`] 捕获、随 `NetworkHandler` 构造
  /// 注入 handler 域——C# 对位是 TLS 握手阻塞在 `handler.Start`，NetworkHandler.cs:147，
  /// 而 localEndpointName 在其之前的 TcpNetworkHandlerBase 构造器已捕获，:41）
  #[cfg(feature = "tls")]
  pub fn tls(stream: TlsStream<TcpStream>) -> Self {
    Self::Tls(TlsHandles::new(stream))
  }

  /// 异步读取字节到缓冲区（TLS 臂经 [`PrimedRecv`] 契约取空闲段已初始化
  /// 视图，明文臂 compio 覆盖写口径）
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:Read
  #[inline]
  pub async fn read<B: PrimedRecv>(&mut self, buf: B) -> BufResult<usize, B> {
    stream_io!(&*self => |s| s.read(buf).await, tls |h| tls_append_read(&h.read, buf).await)
  }

  /// 异步写入完整字节切片/缓冲区
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/INetworkSender.cs:SendResponse / libs/common/Networking/NetworkHandler.cs:SendResponse
  #[inline]
  pub async fn write_all<B: IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
    stream_io!(&*self => |s| s.write_all(buf).await, tls |h| tls_write_flush(&h.write, buf).await)
  }

  /// 刷盘/刷新底层流缓冲
  ///
  /// rust 自有：流层 flush 面（C# sender 无 flush 层，写回即 SendResponse 直发）
  #[inline]
  pub async fn flush(&mut self) -> io::Result<()> {
    stream_io!(&*self => |s| s.flush().await, tls |h| tls_flush(&h.write).await)
  }

  /// 关闭流写侧：明文臂发出 FIN，TLS 臂先发 close_notify 再发 FIN
  ///
  /// 只关写侧即够：接收半边由「读取段已随泵循环结束」天然满足，且 compio 的
  /// `AsyncWrite::shutdown` 本就是 `shutdown(fd, SHUT_WR)`；等对端 FIN 反而会在
  /// 协议违规、对端仍在发送的场景反向挂住。
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/TcpNetworkHandlerBase.cs:Dispose
  /// （C# 一步 `socket.Shutdown(Both)` + 一步 `socket.Close()`；其 TLS 关闭通知由
  /// NetworkHandler 的 DisposeImpl 里 `sslStream?.Dispose()` 补发，rust 把两件事
  /// 并在本函数的 TLS 臂上——rustls 的 close 即 send_close_notify + flush + 写半关闭）
  #[inline]
  pub async fn shutdown(&mut self) -> io::Result<()> {
    stream_io!(&*self => |s| s.shutdown().await, tls |h| tls_shutdown(&h.write).await)
  }

  /// 共享借用形态读：future 存活期间允许并发 [`Self::write_all_shared`]
  ///
  /// 对标 libs/common/Networking/NetworkHandler.cs 的 Read（rust 所有权拆分的
  /// 共享借用变体，函数级映射声明归 [`Self::read`]）
  ///
  /// TCP/Unix 全双工流读句柄内部共享；TLS 流经读侧句柄取 rustls 连接的独占
  /// poll 权，读挂起即释锁 —— 两种传输在此面同构，订阅推送双路等待不分形态
  pub async fn read_shared<B: PrimedRecv>(&self, buf: B) -> BufResult<usize, B> {
    stream_io!(self => |s| s.read(buf).await, tls |h| tls_append_read(&h.read, buf).await)
  }

  /// 共享借用形态写出完整字节（与 [`Self::read_shared`] 并发安全）
  ///
  /// SendResponse 的共享借用变体
  ///
  /// 对标 libs/common/Networking/NetworkHandler.cs:GetNetworkSender —— SSL 会话
  /// 的网络发送器即本处理器自身，推送帧与命令应答共用同一写出内核
  pub async fn write_all_shared<B: IoBuf>(&self, buf: B) -> BufResult<(), B> {
    stream_io!(self => |s| s.write_all(buf).await, tls |h| tls_write_flush(&h.write, buf).await)
  }
}
