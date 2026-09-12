//! 网络连接处理器与协议帧切片泵
//!
//! 1:1 对标微软 Garnet NetworkHandler 与 ServerTcpNetworkHandler
//!
//! 统一承接 TCP 与 Unix 域套接字的单连接生命周期：
//! 1. 握手检测：读取首包判定 WireFormat（默认 WireFormat::Ascii）；
//! 2. 关联 SessionProvider 创建会话消费者；
//! 3. 驱动协议帧切片循环（半包 shift、超大包 reserve）；
//! 4. 慢客户端控制（Throttle 背压写回）；
//! 5. 优雅关停与资源回收（Dispose）。

use std::{io, mem::take, sync::Arc};

use compio::buf::BufResult;
use parking_lot::Mutex;

use crate::{
  buffer_pool::LimitedFixedBufferPool,
  net::stream::ConnectionStream,
  throttle::NetworkSenderThrottle,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

/// 连接网络处理器
pub struct NetworkHandler<C: MessageConsumerFace> {
  pub handler_id: u64,
  pub remote_endpoint: String,
  buffer_pool: Arc<LimitedFixedBufferPool>,
  throttle: NetworkSenderThrottle,
  session: Mutex<Option<Arc<C>>>,
}

impl<C: MessageConsumerFace> NetworkHandler<C> {
  /// 构造处理器
  pub fn new(
    handler_id: u64,
    remote_endpoint: String,
    buffer_pool: Arc<LimitedFixedBufferPool>,
    throttle_max: usize,
  ) -> Self {
    Self {
      handler_id,
      remote_endpoint,
      buffer_pool,
      throttle: NetworkSenderThrottle::new(throttle_max),
      session: Mutex::new(None),
    }
  }

  /// 关联会话实例
  pub fn set_session(&self, session: Arc<C>) {
    *self.session.lock() = Some(session);
  }

  /// 释放网络处理器
  pub fn dispose(&self) {
    self.throttle.close();
    if let Some(session) = self.session.lock().take() {
      session.dispose();
    }
  }

  /// 驱动统一连接流（TCP / Unix）异步读写与协议切片泵
  pub async fn process_stream<P: SessionProviderFace<Consumer = C>>(
    &self,
    mut stream: ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    let res = self
      .drive_loop(&mut stream, session_provider, sender_id)
      .await;
    self.dispose();
    res
  }

  /// 连接泵主循环
  async fn drive_loop<P: SessionProviderFace<Consumer = C>>(
    &self,
    stream: &mut ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 池化接收缓冲
    let mut buf = self.buffer_pool.get(0);
    let mut read_pos = 0usize;
    let mut active_session: Option<Arc<C>> = None;

    // 读暂存缓冲，全程复用避免堆分配
    let mut chunk = vec![0u8; 16384];

    // 应答聚合缓冲
    let mut resp_buf = Vec::new();

    loop {
      if read_pos > 0 && buf.capacity().saturating_sub(buf.len()) < chunk.len() {
        buf.copy_within(read_pos.., 0);
        let keep = buf.len() - read_pos;
        buf.truncate(keep);
        read_pos = 0;
      }

      let BufResult(res, returned) = stream.read(chunk).await;
      chunk = returned;
      let n = match res {
        Ok(0) => break, // 对端正常关闭
        Ok(n) => n,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
        Err(e) => return Err(e),
      };

      buf.extend_from_slice(&chunk[..n]);

      // 握手阶段：识别 WireFormat::Ascii
      if active_session.is_none() {
        if buf.len() - read_pos < 4 {
          continue;
        }
        let session = session_provider
          .get_session(WireFormat::Ascii, sender_id)
          .ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "会话提供者拒绝建立会话")
          })?;
        self.set_session(Arc::clone(&session));
        active_session = Some(session);
      }

      let Some(session) = active_session.as_deref() else {
        break;
      };

      resp_buf.clear();
      while read_pos < buf.len() {
        let (consumed, resp) = session.try_consume_messages(&buf[read_pos..]);
        if consumed == 0 {
          break;
        }
        read_pos += consumed;
        if !resp.is_empty() {
          resp_buf.extend_from_slice(&resp);
        }
      }

      if !resp_buf.is_empty() {
        if self.throttle.enter_send().await.is_err() {
          break;
        }
        let payload = take(&mut resp_buf);
        let BufResult(write_res, reclaimed) = stream.write_all(payload).await;
        resp_buf = reclaimed;
        self.throttle.exit_send();

        if let Err(e) = write_res {
          if e.kind() == io::ErrorKind::BrokenPipe
            || e.kind() == io::ErrorKind::ConnectionReset
            || e.kind() == io::ErrorKind::UnexpectedEof
          {
            break;
          }
          return Err(e);
        }
      }

      if read_pos == buf.len() {
        buf.clear();
        read_pos = 0;
      }
    }

    Ok(())
  }
}
