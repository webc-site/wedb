//! 出站连接流抽象：明文 TCP、Unix 域套接字与可选 TLS 流
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/GarnetClient.cs:ConnectAsync`（按 `EndPoint` 形态分派 TCP 与
//!   Unix 域套接字建连，TLS 时 SslStream 包装 socket，rust 以 [`OutStream::Tls`]
//!   等价承载）
//!
//! TLS 臂内核形态对齐 wnode/src/net/stream.rs（入站 ConnectionStream 的
//! BiLock 互斥句柄对）：rustls 一条连接是一个状态机，读写入口都要 `&mut`，
//! 故拆读写各一枚 [`BiLock`] 句柄共享它；锁粒度为单次 poll，poll 一结束
//!（含 Pending）即释锁，读挂起期间写句柄照样推进，与 TCP 全双工同一拓扑。

use std::io;

#[cfg(unix)]
use compio::net::UnixStream;
use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
};
#[cfg(feature = "tls")]
use compio_tls::TlsStream;
#[cfg(feature = "tls")]
use futures_util::lock::BiLock;
use wbase::endpoint::uds_path;
#[cfg(feature = "tls")]
use wbase::tls::stream::{tls_append_read as tls_read, tls_shutdown, tls_write_flush};

use crate::Result;
#[cfg(feature = "tls")]
use crate::tls::ClientTlsConfig;

/// 出站连接流载体
pub enum OutStream {
  /// 明文 TCP 传输流
  Tcp(TcpStream),
  /// 本地 Unix 域套接字传输流
  #[cfg(unix)]
  Unix(UnixStream),
  /// TLS 安全传输流（握手就绪）
  #[cfg(feature = "tls")]
  Tls(Box<TlsStream<TcpStream>>),
}

impl OutStream {
  /// 出站建连：按端点形态分派 TCP 与 Unix 域套接字
  ///
  /// 形态判定单源在 `wbase::endpoint::uds_path`，与入站监听端点共读同一条规则，
  /// 本函数内不另立第二套前缀/后缀口径。TCP 臂设 nodelay，Unix 域套接字臂不设
  ///（C# 出站建 socket 的 `EndPoint is not UnixDomainSocketEndPoint` 门同语义）；
  /// 非 unix 平台无 Unix 域套接字臂，命中该形态端点即明确报错不回退 TCP
  pub(crate) async fn connect(endpoint: &str) -> Result<Self> {
    match uds_path(endpoint) {
      #[cfg(unix)]
      Some(path) => Ok(Self::Unix(UnixStream::connect(path).await?)),
      #[cfg(not(unix))]
      Some(_) => Err(
        io::Error::new(
          io::ErrorKind::Unsupported,
          "Unix domain socket not supported",
        )
        .into(),
      ),
      None => {
        let sock = TcpStream::connect(endpoint).await?;
        sock.set_nodelay(true).ok();
        Ok(Self::Tcp(sock))
      }
    }
  }

  /// 明文流升级为 TLS 流：TLS 配置在位即在 TCP 之上完成握手包裹
  ///（C# ConnectAsync 内 SslStream.AuthenticateAsClientAsync 分支的等价物），
  /// 无配置原样交回明文字节流
  ///
  /// Unix 域套接字臂不接 TLS：本仓出站 TLS 只在 TCP 上接线，命中该组合即明确
  /// 报错，绝不静默降级为明文
  #[cfg(feature = "tls")]
  pub(crate) async fn with_tls(
    self,
    tls: Option<&ClientTlsConfig>,
    endpoint: &str,
  ) -> Result<Self> {
    let Some(tls) = tls else {
      return Ok(self);
    };
    match self {
      Self::Tcp(sock) => Ok(Self::Tls(Box::new(tls.connect(sock, endpoint).await?))),
      #[cfg(unix)]
      Self::Unix(_) => Err(
        io::Error::new(
          io::ErrorKind::Unsupported,
          "Unix domain socket over TLS not supported",
        )
        .into(),
      ),
      // 调用方仅在 [`Self::connect`] 之后进入本函数，TLS 臂不重入
      Self::Tls(s) => Ok(Self::Tls(s)),
    }
  }

  /// 拆分读写两半：TCP 走 [`TcpStream::into_split`]、Unix 域套接字走
  /// [`UnixStream::into_split`] 零开销拆分，TLS 拆 BiLock 互斥句柄对
  pub fn split(self) -> (ReadHalf, WriteHalf) {
    match self {
      Self::Tcp(s) => {
        let (read, write) = s.into_split();
        (ReadHalf::Tcp(read), WriteHalf::Tcp(write))
      }
      #[cfg(unix)]
      Self::Unix(s) => {
        let (read, write) = s.into_split();
        (ReadHalf::Unix(read), WriteHalf::Unix(write))
      }
      #[cfg(feature = "tls")]
      Self::Tls(s) => {
        let (read, write) = BiLock::new(*s);
        (ReadHalf::Tls(read), WriteHalf::Tls(write))
      }
    }
  }
}

/// 读半句柄
pub enum ReadHalf {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
  #[cfg(feature = "tls")]
  Tls(BiLock<TlsStream<TcpStream>>),
}

impl ReadHalf {
  /// 异步读取（compio 裸 Vec 口径：读目标为整段容量、完成后长度截断为本次
  /// 读取数；对端未经 close_notify 断开时 UnexpectedEof 归一为 EOF，
  /// compio-tls read_futures 同口径）
  ///
  /// 流与缓冲**所有权进出**：读 future 自持两半（输出无借用、`'static`），
  /// 故读泵可把它跨等待轮次常驻存活——挂起期间不析构即不丢缓冲，完成后原样
  /// 取回再投下一读。C# 侧同一 `SocketAsyncEventArgs` 携同一接收缓冲跨收包
  /// 复用的所有权等价物
  ///（`libs/common/Networking/TcpNetworkHandlerBase.cs:HandleReceiveWithoutTLS`）。
  pub(crate) async fn read_owned(mut self, chunk: Vec<u8>) -> (Self, BufResult<usize, Vec<u8>>) {
    let res = match &mut self {
      Self::Tcp(s) => s.read(chunk).await,
      #[cfg(unix)]
      Self::Unix(s) => s.read(chunk).await,
      #[cfg(feature = "tls")]
      Self::Tls(h) => tls_read(h, chunk).await,
    };
    (self, res)
  }
}

/// 写半句柄
pub enum WriteHalf {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
  #[cfg(feature = "tls")]
  Tls(BiLock<TlsStream<TcpStream>>),
}

impl WriteHalf {
  /// 异步写尽整段缓冲后 flush
  pub(crate) async fn write_all(&mut self, buf: Vec<u8>) -> BufResult<(), Vec<u8>> {
    match self {
      Self::Tcp(s) => s.write_all(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.write_all(buf).await,
      #[cfg(feature = "tls")]
      Self::Tls(h) => tls_write_flush(h, buf).await,
    }
  }

  /// 关闭写半驱动读泵收场：明文流为 socket shutdown，TLS 为 close_notify
  pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
    match self {
      Self::Tcp(s) => s.shutdown().await,
      #[cfg(unix)]
      Self::Unix(s) => s.shutdown().await,
      #[cfg(feature = "tls")]
      Self::Tls(h) => tls_shutdown(h).await,
    }
  }
}
