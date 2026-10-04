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

#[cfg(unix)]
use std::os::fd::{AsFd, OwnedFd};
#[cfg(windows)]
use std::os::windows::io::OwnedSocket;
use std::{io, net::Shutdown};

#[cfg(unix)]
use compio::net::UnixStream;
use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
};
use socket2::SockRef;
use wbase::{endpoint::uds_path, primed::PrimedRecv};
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;
#[cfg(feature = "tls")]
use wtls::stream::{
  BiLock, TlsHandles, TlsStream, tls_append_read as tls_read, tls_shutdown, tls_write_flush,
};

use crate::Result;

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
  /// 出站建连：按端点形态分派 TCP 与 Unix 域套接字，并同点捕获拆连句柄
  ///（[`DisposeHandle`]，承接 C# GarnetClient 的 Dispose(bool) 无条件拆 fd 面
  ///（:521-532 `socket?.Dispose()`），契约锚单点见
  /// [`GarnetClient::dispose`](crate::client::GarnetClient::dispose)；TLS 臂在 `with_tls` 包裹**前**
  /// 捕获底层 fd 自持副本——compio-tls 不露内部流 accessor，而 dup fd 与
  /// 原句柄同指一条 socket，shutdown 作用于 socket 本体，包裹前后拆连同效）
  ///
  /// 形态判定单源在 `wbase::endpoint::uds_path`，与入站监听端点共读同一条规则，
  /// 本函数内不另立第二套前缀/后缀口径。TCP 臂设 nodelay，Unix 域套接字臂不设
  ///（C# 出站建 socket 的 `EndPoint is not UnixDomainSocketEndPoint` 门同语义）；
  /// 非 unix 平台无 Unix 域套接字臂，命中该形态端点即明确报错不回退 TCP
  pub(crate) async fn connect(endpoint: &str) -> Result<(Self, DisposeHandle)> {
    match uds_path(endpoint) {
      #[cfg(unix)]
      Some(path) => {
        let sock = UnixStream::connect(path).await?;
        let handle = DisposeHandle::dup(&sock)?;
        Ok((Self::Unix(sock), handle))
      }
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
        let handle = DisposeHandle::dup(&sock)?;
        Ok((Self::Tcp(sock), handle))
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
        let TlsHandles { read, write } = TlsHandles::new(*s);
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
  ///
  /// 缓冲载体为 [`PrimedRecv`]：明文臂走 compio 覆盖写口径，TLS 臂经契约
  /// 取空闲段已初始化视图（同段内存至多清零一次）
  pub(crate) async fn read_owned<B: PrimedRecv>(mut self, chunk: B) -> (Self, BufResult<usize, B>) {
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

/// dispose 拆连句柄：底层 socket fd 的**自持 dup 副本**（unix [`OwnedFd`]、
/// windows [`OwnedSocket`]，均 `Send + Sync`），承载
/// `libs/client/GarnetClient.cs:Dispose(bool)` :521-532 的
/// `socket?.Dispose()` 无条件拆 fd 面
///
/// 形态取舍（立案核查结论）：compio 流本体（`TcpStream`/`UnixStream`）经
/// `Socket → Attacher → SharedFd → synchrony::sync::Shared` 持线程内 Rc 计数，
/// **非 `Send` 非 `Sync`**——若以流克隆作句柄字段，[`GarnetClient`](crate::GarnetClient)
/// 连同门面 `Arc` 一同失格，wedb 复制层跨线程共享直接编译失败（本仓有案：
/// `StoreInner` 要求 `Arc<NodeConnection>: Send`）。dup 副本与原句柄同指
/// 一条 socket 本体，`shutdown(2)` 作用于 socket 而非 fd 号，拆连语义与持
/// 流克隆完全等价；副本自持 fd，drop 即 close 自己那一路，不竞逐泵侧句柄
/// 的存活，读写全句柄退尽后 socket 方真正回收
///
/// compio shutdown 形制核实结论：compio-net 异步 shutdown 恒为
/// `Shutdown::Write` 单向（compio-net-0.12.5 `src/socket/mod.rs:214`
/// `ShutdownSocket::new(.., std::net::Shutdown::Write)`，无 Both 形对外暴露），
/// 写半 FIN 依赖对端配合回拆，静默对端下挂起读永不落定。双向收口经
/// [`socket2::SockRef`] 借自持 fd 同步下发 `shutdown(2)`（SHUT_RDWR）——该
/// syscall 即刻返回不阻塞，挂起读被内核即刻以 EOF/err 落定（Linux 返 0，
/// kqueue 形态以可读/EOF 唤醒），compio thread-per-core 纪律无损；借用
/// 形态与 wnode `src/net/socket_opt.rs` keepalive 同例
pub struct DisposeHandle {
  /// 平台自持 socket fd 副本
  #[cfg(unix)]
  sock: OwnedFd,
  /// 平台自持 socket 句柄副本
  #[cfg(windows)]
  sock: OwnedSocket,
}

impl DisposeHandle {
  /// 从承载流捕获自持 dup 副本：unix 走 `AsFd`（impl_raw_fd! 生成），
  /// windows 走 `AsSocket`；dup 失败（EBADF/EMFILE 族）原样上抛，
  /// 建连随即失败——宁不返回半可用连接，不做静默降级
  #[cfg(unix)]
  fn dup<T: AsFd>(s: &T) -> io::Result<Self> {
    Ok(Self {
      sock: s.as_fd().try_clone_to_owned()?,
    })
  }

  /// windows 形态：socket 句柄按进程内可复制形态自持
  #[cfg(windows)]
  fn dup<T: std::os::windows::io::AsSocket>(s: &T) -> io::Result<Self> {
    Ok(Self {
      sock: s.as_socket().try_clone_to_owned()?,
    })
  }

  /// 双向拆连：收发两向一步关闭，挂起常驻读即刻落定、读泵沿既有读收场支
  /// 退出（pump `Wake::Read` 臂），池借出缓冲随函数返回 RAII 归池
  ///
  /// C# `socket?.Dispose()` 行的 rust 等价面；Dispose(bool) 契约锚单点留在
  /// [`GarnetClient::dispose`](crate::client::GarnetClient::dispose)
  pub(crate) fn shutdown_both(&self) {
    let res = SockRef::from(&self.sock).shutdown(Shutdown::Both);
    if let Err(e) = res {
      // 泵已先行收场（句柄所指 socket 已断/已关）时 ENOTCONN 族为幂等空操作，
      // 与 C# SafeHandle 二次 Dispose 无害同态
      log::debug!("dispose 双向拆连提示（连接已收场）: {e}");
    }
  }
}
