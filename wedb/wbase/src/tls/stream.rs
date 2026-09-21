//! TLS 读写共享基础实现（BiLock 包装与追加读逻辑）
//!
//! 提供基于 `futures_util::lock::BiLock` 包装的读写操作核心逻辑，避免 `wnode` 和 `wconn` 重复实现。

use std::{future::poll_fn, io, mem::MaybeUninit, slice::from_raw_parts_mut, task::Poll};

use compio::{
  buf::{BufResult, IoBuf, IoBufMut},
  net::TcpStream,
};
use compio_tls::TlsStream;
use futures_util::{AsyncRead as TlsAsyncRead, AsyncWrite as TlsAsyncWrite, lock::BiLock};

/// 追加读目标：缓冲空闲段，首次触及整段零初始化
#[inline]
pub fn append_target<'a, B: IoBufMut>(buf: &'a mut B, primed: &mut bool) -> &'a mut [u8] {
  let spare = buf.as_uninit();
  if !*primed {
    spare.fill(MaybeUninit::new(0));
    *primed = true;
  }
  // SAFETY: 空闲段已整段零初始化，指针与长度均取自 as_uninit 本身
  unsafe { from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), spare.len()) }
}

/// TLS 追加读内核：poll_lock 独占连接后按追加口径读
///
/// 缓冲所有权驻留本 future；Pending 交还重入。读目标为缓冲空闲段，
/// 首次触及整段零初始化（rustls 的 poll_read 只接受已初始化切片）
pub async fn tls_append_read<B: IoBufMut>(
  handle: &BiLock<TlsStream<TcpStream>>,
  buf: B,
) -> BufResult<usize, B> {
  let mut buf = Some(buf);
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
