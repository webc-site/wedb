//! TLS 流读写共享内核（BiLock 互斥句柄对与追加读/写尽/刷盘/关闭四原语）
//!
//! 在 garnet 中的相对路径:
//! - libs/common/Networking/NetworkHandler.cs（TLS 会话读写与 Enter/Exit 互斥）
//! - libs/common/Networking/TcpNetworkHandlerBase.cs（接收缓冲所有权复用）

use std::{future::poll_fn, io, task::Poll};

use compio::{
  buf::{BufResult, IoBuf},
  net::TcpStream,
};
/// rustls 连接流（握手就绪）；经本模块重导出，出入站流枚举无需直连 compio-tls
pub use compio_tls::TlsStream;
/// 共享同一 rustls 连接的互斥句柄（读写半各自持有；经本模块重导出，
/// 出入站流拆半枚举无需再直连 futures_util）
pub use futures_util::lock::BiLock;
use futures_util::{AsyncRead as TlsAsyncRead, AsyncWrite as TlsAsyncWrite};
use wbase::primed::PrimedRecv;

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
pub struct TlsHandles {
  /// 读侧句柄
  pub read: BiLock<TlsStream<TcpStream>>,
  /// 写侧句柄
  pub write: BiLock<TlsStream<TcpStream>>,
}

impl TlsHandles {
  /// 握手就绪的连接拆成读写互斥句柄对
  pub fn new(stream: TlsStream<TcpStream>) -> Self {
    let (read, write) = BiLock::new(stream);
    Self { read, write }
  }
}

/// TLS 追加读内核：poll_lock 独占连接后按追加口径读
///
/// 缓冲所有权驻留本 future；Pending 交还重入。读目标为缓冲空闲段的已
/// 初始化视图（[`PrimedRecv`] 契约：同一段空闲内存至多清零一次，memset
/// 随缓冲承载——对标 C# transportReceiveBuffer 分配期清零一次的复用形态，
/// rustls 的 poll_read 只接受已初始化切片）
pub async fn tls_append_read<B: PrimedRecv>(
  handle: &BiLock<TlsStream<TcpStream>>,
  buf: B,
) -> BufResult<usize, B> {
  let mut buf = Some(buf);

  poll_fn(move |cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    let mut stream = guard.as_pin_mut();
    let mut b = buf.take().expect("TLS 读缓冲存活至读完成");
    let target = b.primed_spare();

    match stream.as_mut().poll_read(cx, target) {
      Poll::Pending => {
        buf = Some(b);
        Poll::Pending
      }
      Poll::Ready(Ok(len)) => {
        // SAFETY: rustls 已写入空闲段前 len 字节，总长推进 len 后区间全为初始化字节
        unsafe { b.advance_to(len) };
        Poll::Ready(BufResult(Ok(len), b))
      }
      // 对端未经 close_notify 直接断开时 rustls 以 UnexpectedEof 表达 EOF
      Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
        Poll::Ready(BufResult(Ok(0), b))
      }
      Poll::Ready(Err(e)) => Poll::Ready(BufResult(Err(e), b)),
    }
  })
  .await
}

/// TLS 写出内核：写尽 payload 后 flush 收尾
///
/// payload 所有权驻留本 future；rustls 写出为逐次拷进连接发送缓冲、不保留调用方
/// 切片引用，Pending 后带同一偏移重入即可
pub async fn tls_write_flush<B: IoBuf>(
  handle: &BiLock<TlsStream<TcpStream>>,
  buf: B,
) -> BufResult<(), B> {
  let mut buf = Some(buf);
  let mut written = 0usize;

  let res = poll_fn(|cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    let mut stream = guard.as_pin_mut();
    let b = buf.take().expect("TLS 写缓冲存活至写出完成");
    let total = b.as_init().len();

    let res = loop {
      if written < total {
        let step = stream.as_mut().poll_write(cx, &b.as_init()[written..]);
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

/// TLS 刷盘（写侧句柄独占连接的 flush 调用）
pub async fn tls_flush(handle: &BiLock<TlsStream<TcpStream>>) -> io::Result<()> {
  poll_fn(|cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    guard.as_pin_mut().poll_flush(cx)
  })
  .await
}

/// TLS 关闭通知内核（写侧句柄独占连接的 close 调用）
///
/// 先 `send_close_notify` 入队，随队列写出并 flush 底层，最后关底层写半（FIN）
pub async fn tls_shutdown(handle: &BiLock<TlsStream<TcpStream>>) -> io::Result<()> {
  poll_fn(|cx| {
    let Poll::Ready(mut guard) = handle.poll_lock(cx) else {
      return Poll::Pending;
    };
    guard.as_pin_mut().poll_close(cx)
  })
  .await
}
