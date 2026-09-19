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
#[cfg(feature = "tls")]
use std::{future::poll_fn, mem::MaybeUninit, slice::from_raw_parts_mut, task::Poll};

#[cfg(feature = "tls")]
use compio::buf::{IoBufMut, SetLen};
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
use futures_util::{AsyncRead as TlsAsyncRead, AsyncWrite as TlsAsyncWrite, lock::BiLock};
use wbase::endpoint::uds_path;

use crate::Result;
#[cfg(feature = "tls")]
use crate::tls::ClientTlsConfig;

/// 出站连接流载体
pub(crate) enum OutStream {
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
  pub(crate) fn split(self) -> (ReadHalf, WriteHalf) {
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
pub(crate) enum ReadHalf {
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
pub(crate) enum WriteHalf {
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

/// TLS 读内核：poll_lock 独占连接后按追加口径读
///
/// 缓冲所有权驻留本 future；Pending 交还重入。读目标为缓冲空闲段，
/// 首次触及整段零初始化（rustls 的 poll_read 只接受已初始化切片）
#[cfg(feature = "tls")]
async fn tls_read(
  handle: &BiLock<TlsStream<TcpStream>>,
  chunk: Vec<u8>,
) -> BufResult<usize, Vec<u8>> {
  let mut buf = Some(chunk);
  // 空闲段是否已置备（一次读 op 内缓冲不重排，整段零初始化只需做一次）
  let mut primed = false;
  poll_fn(move |cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    let mut stream = guard.as_pin_mut();
    let mut b = buf.take().expect("TLS 读缓冲存活至读完成");
    let target = append_target(&mut b, &mut primed);
    match stream.as_mut().poll_read(cx, target) {
      Poll::Pending => {
        buf = Some(b);
        Poll::Pending
      }
      Poll::Ready(Ok(len)) => {
        // SAFETY: rustls 已写入空闲段前 len 字节，总长推进 len 后全为初始化字节
        unsafe { b.advance_to(len) };
        Poll::Ready(BufResult(Ok(len), b))
      }
      // 对端未经 close_notify 直接断开：rustls 以 UnexpectedEof 表达 EOF
      Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
        Poll::Ready(BufResult(Ok(0), b))
      }
      Poll::Ready(Err(e)) => Poll::Ready(BufResult(Err(e), b)),
    }
  })
  .await
}

/// 追加读目标：缓冲空闲段，首次触及整段零初始化
#[cfg(feature = "tls")]
fn append_target<'a>(buf: &'a mut Vec<u8>, primed: &mut bool) -> &'a mut [u8] {
  let spare = buf.as_uninit();
  if !*primed {
    spare.fill(MaybeUninit::new(0));
    *primed = true;
  }
  // SAFETY: 空闲段已整段零初始化，指针与长度均取自 as_uninit 本身
  unsafe { from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), spare.len()) }
}

/// TLS 写内核：写尽 payload 后 flush 收尾
///
/// payload 所有权驻留本 future；rustls 写出为逐次拷进连接发送缓冲、
/// 不保留调用方切片引用，Pending 后带同一偏移重入即可
#[cfg(feature = "tls")]
async fn tls_write_flush(
  handle: &BiLock<TlsStream<TcpStream>>,
  buf: Vec<u8>,
) -> BufResult<(), Vec<u8>> {
  let mut buf = Some(buf);
  // 已写出的 payload 字节数
  let mut written = 0usize;
  let res = poll_fn(|cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    let mut stream = guard.as_pin_mut();
    let b = buf.take().expect("TLS 写缓冲存活至写出完成");
    let total = b.len();
    let res = loop {
      if written < total {
        // payload 借用止于本条语句，写出结果落定后即交还缓冲所有权
        let step = stream.as_mut().poll_write(cx, &b[written..]);
        match step {
          Poll::Ready(Ok(0)) => break Err(io::Error::from(io::ErrorKind::WriteZero)),
          Poll::Ready(Ok(n)) => written += n,
          Poll::Ready(Err(e)) => break Err(e),
          Poll::Pending => {
            buf = Some(b);
            return Poll::Pending;
          }
        }
      } else {
        match stream.as_mut().poll_flush(cx) {
          Poll::Ready(res) => break res,
          Poll::Pending => {
            buf = Some(b);
            return Poll::Pending;
          }
        }
      }
    };
    buf = Some(b);
    Poll::Ready(res)
  })
  .await;
  BufResult(res, buf.expect("TLS 写缓冲存活至写出完成"))
}

/// TLS 关闭：写侧句柄独占连接的 close_notify 发送
#[cfg(feature = "tls")]
async fn tls_shutdown(handle: &BiLock<TlsStream<TcpStream>>) -> io::Result<()> {
  poll_fn(|cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    guard.as_pin_mut().poll_close(cx)
  })
  .await
}
