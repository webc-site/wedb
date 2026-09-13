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

use compio::{
  buf::BufResult,
  runtime::{CancelToken, Cancelled, FutureExt, spawn},
};
use wbase::{
  pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool},
  throttle::NetworkSenderThrottle,
};

use crate::{
  net::stream::ConnectionStream,
  servers::consumer_registry::{ConsumerEntry, ConsumerRegistry},
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
  /// 活跃消费者注册表（C# GarnetServerBase 域；释放时注销）
  consumers: Option<Arc<ConsumerRegistry>>,
  /// 本连接的注册条目（字节镜像 / KILL 触发位载体）
  consumer_entry: Option<Arc<ConsumerEntry>>,
  /// 挂起读打断令牌（哨兵任务在终止态取消之；compio CancelToken 线程亲和，
  /// 与泵/哨兵同运行时创建驱动）
  kill_token: Option<CancelToken>,
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
      consumers: None,
      consumer_entry: None,
      kill_token: None,
    }
  }

  /// 关联会话实例
  pub fn set_session(&mut self, session: C) {
    self.session = Some(session);
  }

  /// 释放网络处理器
  pub fn dispose(&mut self) {
    self.throttle.close();
    // 注销先于会话释放（C# GarnetServerTcp.DisposeMessageConsumer：TryRemove
    // → Session.Dispose 顺序），监视器不会同轮双计条目字节镜像与 dispose 归并
    if let (Some(registry), Some(entry)) = (self.consumers.take(), self.consumer_entry.take()) {
      registry.unregister(entry.id);
    }
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

      // 零拷贝直接读取网卡数据进入池化缓冲；有注册条目时挂取消令牌
      //（CLIENT KILL / 注销经哨兵打断挂起读，C# 直关套接字的等价物）
      let read_res = match self.kill_token.clone() {
        Some(token) => stream.read(raw_buf).with_cancel(token).fail_fast().await,
        None => Ok(stream.read(raw_buf).await),
      };
      let BufResult(res, returned) = match read_res {
        Ok(pair) => pair,
        Err(Cancelled) => break,
      };
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

        // 注册活跃消费者（C# GarnetServerTcp.HandleNewConnection 的
        // activeHandlers.TryAdd；会话 id 即网络发送器 id）并挂终止哨兵
        if let Some(registry) = session_provider.consumer_registry() {
          let entry = registry.register(
            sender_id as i64,
            self.remote_endpoint.clone(),
            stream.local_endpoint(),
          );
          self.consumers = Some(registry);
          self.consumer_entry = Some(Arc::clone(&entry));
          let kill_token = CancelToken::new();
          self.kill_token = Some(kill_token.clone());
          spawn_kill_watcher(entry, kill_token);
        }
      }

      let Some(session) = self.session.as_mut() else {
        break;
      };

      resp_pooled.clear();
      let batch_start = read_pos;
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

      // 镜像累加（监视器瞬时吞吐/ops/s 源；会话 dispose 时随条目注销换轨到
      // 历史归并，二者不双计）
      if let Some(entry) = &self.consumer_entry {
        entry.add_net_bytes((read_pos - batch_start) as u64, resp_pooled.len() as u64);
        if let Some(session) = self.session.as_mut() {
          session.mirror_session_counters(entry);
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

/// KILL/注销哨兵：监听终止广播并打断挂起中的读
///
/// C# CLIENT KILL 经 `networkSender.TryClose()` 直关套接字；rust 连接任务
/// 独占套接字，等价物为「注册条目 kill 位 + CancelToken」——被杀连接的
/// 挂起 `read` 由哨兵秒级打断，泵随之走 dispose（注销 + 会话释放）。
/// 令牌与泵同运行时驱动（compio CancelToken 线程亲和；KILL 方仅置原子位
/// 与广播事件，跨线程安全）
fn spawn_kill_watcher(entry: Arc<ConsumerEntry>, kill_token: CancelToken) {
  spawn(async move {
    loop {
      // 双重检查防错过唤醒（对齐 ShutdownCoordinator::wait 防竞态模式）
      if entry.is_terminating() {
        break;
      }
      let listener = entry.listen_terminate();
      if entry.is_terminating() {
        break;
      }
      listener.await;
    }
    kill_token.cancel();
  })
  .detach();
}
