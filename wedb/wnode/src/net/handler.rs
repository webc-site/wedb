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

use std::{io, sync::Arc};

use compio::buf::BufResult;
use wbase::pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool};

use crate::{
  net::stream::ConnectionStream,
  throttle::NetworkSenderThrottle,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

/// 读取前缓冲预留最小空闲容量阈值（不足时向前平移或扩容）
const MIN_READ_SPACE: usize = 4096;

/// 连接网络处理器
pub struct NetworkHandler<C: MessageConsumerFace> {
  pub handler_id: u64,
  pub remote_endpoint: String,
  buffer_pool: Arc<LimitedFixedBufferPool>,
  throttle: NetworkSenderThrottle,
  session: Option<C>,
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
      session: None,
    }
  }

  /// 关联会话实例
  pub fn set_session(&mut self, session: C) {
    self.session = Some(session);
  }

  /// 释放网络处理器
  pub fn dispose(&mut self) {
    self.throttle.close();
    if let Some(mut session) = self.session.take() {
      session.dispose();
    }
  }

  /// 驱动统一连接流（TCP / Unix）异步读写与协议切片泵
  pub async fn process_stream<P: SessionProviderFace<Consumer = C>>(
    &mut self,
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
    &mut self,
    stream: &mut ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 池化接收缓冲（容量 64KB，RAII 自动归还句柄）
    let mut pooled = self.buffer_pool.get(0);
    // 池化发送缓冲（容量 64KB，连接生命周期内复用，RAII 自动归还句柄）
    let mut resp_pooled = self.buffer_pool.get(DEFAULT_BUFFER_SIZE);
    let mut read_pos = 0usize;

    loop {
      let mut raw_buf = pooled.take_buffer().expect("pooled buffer active");
      // 在读取前，如果空闲空间不足预留阈值：
      if raw_buf.capacity().saturating_sub(raw_buf.len()) < MIN_READ_SPACE {
        // 若前面已有消费，进行向前平移对齐以腾出尾部空间
        if read_pos > 0 {
          raw_buf.copy_within(read_pos.., 0);
          let keep = raw_buf.len() - read_pos;
          raw_buf.truncate(keep);
          read_pos = 0;
        }
        // 若平移后空闲空间仍小于阈值（例如遇到单条超大报文），则扩容
        if raw_buf.capacity().saturating_sub(raw_buf.len()) < MIN_READ_SPACE {
          raw_buf.reserve(DEFAULT_BUFFER_SIZE);
        }
      }

      // 零拷贝直接读取网卡数据进入池化缓冲
      let BufResult(res, returned) = stream.read(raw_buf).await;
      pooled.set_buffer(returned);

      match res {
        Ok(0) => break, // 对端正常关闭
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
        Err(e) => return Err(e),
      }

      let raw_buf = pooled.vec_mut();

      // 握手阶段：识别 WireFormat::Ascii
      if self.session.is_none() {
        if raw_buf.len() - read_pos < 4 {
          continue;
        }
        let session = session_provider
          .get_session(WireFormat::Ascii, sender_id)
          .ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "会话提供者拒绝建立会话")
          })?;
        self.set_session(session);
      }

      let Some(session) = self.session.as_mut() else {
        break;
      };

      resp_pooled.clear();
      loop {
        let raw_buf = pooled.vec_ref();
        if read_pos < raw_buf.len() {
          let consumed =
            session.try_consume_messages_into(&raw_buf[read_pos..], resp_pooled.vec_mut());
          if consumed == 0 {
            break;
          }
          read_pos += consumed;
        }

        // 阻塞命令挂起：await 驱动至完成（compio 挂起不占线程；C# 为专线
        // 网络线程 BlockingWait），写出应答后继续消费流水线余量
        if let Some(blocked) = session.take_blocked_wait() {
          let (cmd, result) = blocked.resolve().await;
          session.resolve_blocked_wait_into(cmd, result, resp_pooled.vec_mut());
        }

        // 慢路径命令挂起：await 驱动至完成（对照 C# 网络线程同步执行
        // SCAN/KEYS/DBSIZE/CLUSTER RESET 等慢命令），应答按流水线顺序
        // 追加后继续消费流水线余量
        if let Some(slow) = session.take_slow_wait() {
          let reply = slow.resolve().await;
          if !reply.is_empty() {
            resp_pooled.vec_mut().extend_from_slice(&reply);
          }
        }

        if read_pos >= pooled.vec_ref().len() {
          break;
        }
      }

      if !resp_pooled.is_empty() {
        if self.throttle.enter_send().await.is_err() {
          break;
        }
        let payload = resp_pooled
          .take_buffer()
          .expect("pooled send buffer active");
        let BufResult(write_res, mut reclaimed) = stream.write_all(payload).await;
        reclaimed.clear();
        resp_pooled.set_buffer(reclaimed);
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

        if resp_pooled.capacity() > DEFAULT_BUFFER_SIZE {
          resp_pooled = self.buffer_pool.get(DEFAULT_BUFFER_SIZE);
        }
      }

      let raw_buf = pooled.vec_mut();
      if read_pos == raw_buf.len() {
        raw_buf.clear();
        read_pos = 0;
        if raw_buf.capacity() > DEFAULT_BUFFER_SIZE {
          pooled = self.buffer_pool.get(0);
        }
      }
    }

    Ok(())
  }
}

impl<C: MessageConsumerFace> Drop for NetworkHandler<C> {
  fn drop(&mut self) {
    self.dispose();
  }
}
