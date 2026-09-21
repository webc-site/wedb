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
  buf::{IoBuf, IoBufMut},
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
};
#[cfg(feature = "tls")]
use compio_tls::TlsStream;
#[cfg(feature = "tls")]
use futures_util::lock::BiLock;
#[cfg(feature = "tls")]
use wbase::tls::stream::{tls_append_read, tls_flush, tls_shutdown, tls_write_flush};

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

/// TLS 流的读写互斥句柄对（各持一枚 [`BiLock`] 句柄，指向同一 rustls 连接）
///
/// rustls 一条连接是一个状态机，读写入口都要 `&mut`，故拆成读写各一枚句柄共享
/// 它。锁粒度为单次 poll：poll 一结束（含 [`Poll::Pending`]）即释锁，在途读 op
/// 驻留在流内部的异步读槽而非调用方 future，故读挂起期间写句柄照样推进 ——
/// 与 TCP 全双工同一拓扑，订阅推送在两种传输上走同一条直写路径。
///
/// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:Enter
/// （C# TLS 会话的写侧互斥即发送器 Enter/Exit 信号量，:584-603，与之并发的
/// 是挂起中的 sslStream.ReadAsync，:368）
#[cfg(feature = "tls")]
pub struct TlsHandles {
  /// 读侧句柄
  read: BiLock<TlsStream<TcpStream>>,
  /// 写侧句柄
  write: BiLock<TlsStream<TcpStream>>,
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
  #[cfg(feature = "tls")]
  pub fn tls(stream: TlsStream<TcpStream>) -> Self {
    let (read, write) = BiLock::new(stream);
    Self::Tls(TlsHandles { read, write })
  }

  /// 异步读取字节到缓冲区
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:Read
  #[inline]
  pub async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
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

  /// 获取本地端点文本表示
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/INetworkSender.cs:LocalEndpointName（UDS/TLS 无端口语义时返回空串）
  pub fn local_endpoint(&self) -> String {
    match self {
      Self::Tcp(s) => s
        .local_addr()
        .map_or(String::new(), |addr| addr.to_string()),
      #[cfg(unix)]
      Self::Unix(_) => String::new(),
      #[cfg(feature = "tls")]
      Self::Tls(_) => String::new(),
    }
  }

  /// 共享借用形态读：future 存活期间允许并发 [`Self::write_all_shared`]
  ///
  /// 对标 libs/common/Networking/NetworkHandler.cs 的 Read（rust 所有权拆分的
  /// 共享借用变体，函数级映射声明归 [`Self::read`]）
  ///
  /// TCP/Unix 全双工流读句柄内部共享；TLS 流经读侧句柄取 rustls 连接的独占
  /// poll 权，读挂起即释锁 —— 两种传输在此面同构，订阅推送双路等待不分形态
  pub async fn read_shared<B: IoBufMut>(&self, buf: B) -> BufResult<usize, B> {
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
