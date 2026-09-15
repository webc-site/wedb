//! 统一网络连接流抽象：封装 TCP、Unix 域套接字以及可选 TLS 流

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

/// 客户端网络流载体类型
pub enum ConnectionStream {
  /// 标准 TCP 传输流
  Tcp(TcpStream),
  /// 本地 Unix 域套接字传输流
  #[cfg(unix)]
  Unix(UnixStream),
  /// 纯 Rust TLS 安全传输流 (基于 rustls)
  #[cfg(feature = "tls")]
  Tls(Box<TlsStream<TcpStream>>),
}

impl ConnectionStream {
  /// 异步读取字节到缓冲区
  #[inline]
  pub async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    match self {
      Self::Tcp(s) => s.read(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.read(buf).await,
      #[cfg(feature = "tls")]
      Self::Tls(s) => s.read(buf).await,
    }
  }

  /// 异步写入完整字节切片/缓冲区
  #[inline]
  pub async fn write_all<B: IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
    match self {
      Self::Tcp(s) => s.write_all(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.write_all(buf).await,
      #[cfg(feature = "tls")]
      Self::Tls(s) => {
        let BufResult(res, buf) = s.write_all(buf).await;
        if res.is_ok()
          && let Err(e) = s.flush().await
        {
          return BufResult(Err(e), buf);
        }
        BufResult(res, buf)
      }
    }
  }

  /// 刷盘/刷新底层流缓冲
  #[inline]
  pub async fn flush(&mut self) -> io::Result<()> {
    match self {
      Self::Tcp(s) => s.flush().await,
      #[cfg(unix)]
      Self::Unix(s) => s.flush().await,
      #[cfg(feature = "tls")]
      Self::Tls(s) => s.flush().await,
    }
  }

  /// 本地端点文本（C# networkSender.LocalEndpointName；UDS 无端口语义，恒空串）
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

  /// 是否支持共享借用读写（TCP/Unix 全双工流读句柄内部共享，读挂起期间
  /// 可并发发起写——订阅推送直写网络的必要条件；TLS 流读写互斥（rustls
  /// 单状态机），不支持）
  pub fn supports_shared_rw(&self) -> bool {
    #[cfg(feature = "tls")]
    {
      !matches!(self, Self::Tls(_))
    }
    #[cfg(not(feature = "tls"))]
    {
      true
    }
  }

  /// 共享借用形态读：future 存活期间允许并发 [`Self::write_all_shared`]
  /// （TCP/Unix 全双工；仅 [`Self::supports_shared_rw`] 为真时可用）
  pub async fn read_shared<B: IoBufMut>(&self, buf: B) -> BufResult<usize, B> {
    match self {
      Self::Tcp(s) => {
        let mut r: &TcpStream = s;
        AsyncRead::read(&mut r, buf).await
      }
      #[cfg(unix)]
      Self::Unix(s) => {
        let mut r: &UnixStream = s;
        AsyncRead::read(&mut r, buf).await
      }
      #[cfg(feature = "tls")]
      Self::Tls(_) => unreachable!("TLS 流不支持共享借用读写"),
    }
  }

  /// 共享借用形态写出完整字节（与 [`Self::read_shared`] 并发安全；
  /// 仅 [`Self::supports_shared_rw`] 为真时可用）
  pub async fn write_all_shared<B: IoBuf>(&self, buf: B) -> BufResult<(), B> {
    match self {
      Self::Tcp(s) => {
        let mut w: &TcpStream = s;
        AsyncWriteExt::write_all(&mut w, buf).await
      }
      #[cfg(unix)]
      Self::Unix(s) => {
        let mut w: &UnixStream = s;
        AsyncWriteExt::write_all(&mut w, buf).await
      }
      #[cfg(feature = "tls")]
      Self::Tls(_) => unreachable!("TLS 流不支持共享借用读写"),
    }
  }
}

/// 会话底层独占网络写发送器
pub enum DirectWriter {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
  #[cfg(feature = "tls")]
  Tls(Box<TlsStream<TcpStream>>),
}

impl DirectWriter {
  /// 写入全部字节
  #[inline]
  pub async fn write_all<B: IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
    match self {
      Self::Tcp(s) => s.write_all(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.write_all(buf).await,
      #[cfg(feature = "tls")]
      Self::Tls(s) => {
        let BufResult(res, buf) = s.write_all(buf).await;
        if res.is_ok()
          && let Err(e) = s.flush().await
        {
          return BufResult(Err(e), buf);
        }
        BufResult(res, buf)
      }
    }
  }
}

/// 会话网络读取器
pub enum SessionReader {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
  #[cfg(feature = "tls")]
  Tls(Box<TlsStream<TcpStream>>),
}

impl SessionReader {
  /// 读取数据到可变缓冲区
  #[inline]
  pub async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    match self {
      Self::Tcp(s) => s.read(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.read(buf).await,
      #[cfg(feature = "tls")]
      Self::Tls(s) => s.read(buf).await,
    }
  }
}
