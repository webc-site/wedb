//! 统一网络连接流抽象：封装 TCP 与 Unix 域套接字

#[cfg(unix)]
use compio::net::UnixStream;
use compio::{
  BufResult,
  buf::{IoBuf, IoBufMut},
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
};

/// 客户端网络流载体类型
pub enum ConnectionStream {
  /// 标准 TCP 传输流
  Tcp(TcpStream),
  /// 本地 Unix 域套接字传输流
  #[cfg(unix)]
  Unix(UnixStream),
}

impl ConnectionStream {
  /// 异步读取字节到缓冲区
  #[inline]
  pub async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    match self {
      Self::Tcp(s) => s.read(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.read(buf).await,
    }
  }

  /// 异步写入完整字节切片/缓冲区
  #[inline]
  pub async fn write_all<B: IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
    match self {
      Self::Tcp(s) => s.write_all(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.write_all(buf).await,
    }
  }
}

/// 会话底层独占网络写发送器
pub enum DirectWriter {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
}

impl DirectWriter {
  /// 写入全部字节
  #[inline]
  pub async fn write_all<B: IoBuf>(&mut self, buf: B) -> BufResult<(), B> {
    match self {
      Self::Tcp(s) => s.write_all(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.write_all(buf).await,
    }
  }
}

/// 会话网络读取器
pub enum SessionReader {
  Tcp(TcpStream),
  #[cfg(unix)]
  Unix(UnixStream),
}

impl SessionReader {
  /// 读取数据到可变缓冲区
  #[inline]
  pub async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    match self {
      Self::Tcp(s) => s.read(buf).await,
      #[cfg(unix)]
      Self::Unix(s) => s.read(buf).await,
    }
  }
}
