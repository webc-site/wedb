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

use std::{
  future::{Future, poll_fn},
  io,
  mem::MaybeUninit,
  pin::Pin,
  sync::Arc,
  task::Poll,
};

use compio::{
  BufResult,
  buf::{IoBuf, IoBufMut, ReserveError, SetLen},
  runtime::{CancelToken, Cancelled, FutureExt, spawn},
};
use event_listener::EventListener;
use wbase::{
  pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool},
  throttle::NetworkSenderThrottle,
};
use wpubsub::PubSubMailbox;

use crate::{
  net::stream::ConnectionStream,
  servers::consumer_registry::{ConsumerEntry, ConsumerRegistry},
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

/// 读取前缓冲预留最小空闲容量阈值（不足时向前平移或扩容）
const MIN_READ_SPACE: usize = 4096;

/// 追加式接收读缓冲（所有权进出读操作，compio 驱动要求 'static）
///
/// compio 对裸 `Vec` 的读约定为覆盖语义（读目标为整段容量、完成后长度
/// 截断为本次读取数），半包残余字节会被下一批读取破坏；此包装把读目标
/// 改为空闲容量段、完成后按追加推进长度，网络字节得以在缓冲内跨批次
/// 累积 —— C# NetworkHandler 的 bytesRead 累积读取模型等价物
struct RecvAppend(Vec<u8>);

impl IoBuf for RecvAppend {
  fn as_init(&self) -> &[u8] {
    &self.0
  }
}

impl IoBufMut for RecvAppend {
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    // 读目标：空闲容量段（驱动自段首写入本次读取字节）
    self.0.spare_capacity_mut()
  }

  fn reserve(&mut self, len: usize) -> Result<(), ReserveError> {
    self
      .0
      .try_reserve(len)
      .map_err(|e| ReserveError::ReserveFailed(Box::new(e)))
  }
}

impl SetLen for RecvAppend {
  unsafe fn set_len(&mut self, len: usize) {
    // 契约：len 为绝对总长，[buf_len(), len) 已由驱动写入初始化字节，
    // 且 len <= buf_len() + 空闲容量，Vec::set_len 合法
    unsafe { self.0.set_len(len) };
  }

  unsafe fn advance_to(&mut self, len: usize) {
    // 驱动读完成路径（BufResultExt::map_advanced）以本次写入空闲容量段
    // 的字节数调用；追加语义下总长推进 len
    let current = self.0.len();
    unsafe { self.0.set_len(current + len) };
  }
}

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
  ///
  /// 缓冲管理单形态（C# NetworkHandler 的 bytesRead/readHead 模型）：会话
  /// 经 [`MessageConsumerFace::take_recv_scratch`] 暴露接收缓冲，网络字节
  /// 零拷贝直入会话缓冲，消费游标驻留会话跨批次持久；MIN_READ_SPACE 平移
  /// 仅在整段消费完时由会话执行（游标清零复位，offset/解析指针同时失效
  /// 安全），半包残余只扩不平。
  ///
  /// 读取挂取消令牌：有注册条目时（CLIENT KILL / 注销）哨兵打断挂起读，
  /// C# 直关套接字的等价物。
  async fn drive_loop<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    stream: &mut ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 池化发送缓冲（容量 64KB，连接生命周期内复用，RAII 自动归还句柄）
    let mut resp_pooled = self.buffer_pool.get(DEFAULT_BUFFER_SIZE);
    // 握手批净入字节（并入消费段首轮镜像，监视器字节口径与批次数对齐）
    let mut handshake_net_in = 0usize;

    // ── 握手段：收首批识别 WireFormat 并装配会话（C# Process →
    // serverHook.TryCreateMessageConsumer 装配点）。握手期会话未建，批次
    // 字节先落池化缓冲；会话创建点把未消费字节一次性迁入会话自有接收
    // 缓冲（此迁移点会话游标必为零，字节流无缝衔接、无重复并入），旧池
    // 缓冲随即 RAII 归还，此后网络字节零拷贝直入会话缓冲
    {
      let mut pooled = self.buffer_pool.get(0);
      loop {
        let mut raw_buf = pooled.take_buffer().expect("pooled buffer active");
        // 空闲空间不足预留阈值：扩容（握手期无消费，无需平移）
        if raw_buf.capacity() - raw_buf.len() < MIN_READ_SPACE {
          raw_buf.reserve(DEFAULT_BUFFER_SIZE);
        }
        let before = raw_buf.len();
        let BufResult(read_res, wrapped) = stream.read(RecvAppend(raw_buf)).await;
        raw_buf = wrapped.0;
        handshake_net_in += raw_buf.len() - before;
        pooled.set_buffer(raw_buf);

        match read_res {
          Ok(0) => return Ok(()), // 对端在会话建立前关闭
          Ok(_) => {}
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
          Err(e) => return Err(e),
        }

        if pooled.vec_ref().len() < 4 {
          continue; // 首批不足 4 字节，继续读
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

        // 未消费批次字节迁入会话自有接收缓冲（握手期无消费，整段迁移）
        if let Some(session) = self.session.as_mut() {
          let mut scratch = session.take_recv_scratch();
          scratch.extend_from_slice(pooled.vec_ref());
          session.return_recv_scratch(scratch);
        }
        break;
      }
    }

    loop {
      // ── 网络读取段 ──
      // 本轮网络进字节（统计网卡读取增量）
      let net_in;
      {
        let Some(session) = self.session.as_mut() else {
          break;
        };
        let mut scratch = session.take_recv_scratch();
        // 空闲空间不足预留阈值：整段消费完的缓冲已由会话清零复位（无需平移）；
        // 半包残余不可平移（会话游标驻留），仅扩容
        if scratch.capacity().saturating_sub(scratch.len()) < MIN_READ_SPACE {
          scratch.reserve(DEFAULT_BUFFER_SIZE);
        }
        // 订阅推送双路等待面（C# 广播线程直写订阅会话网络发送器的等价
        // 承接：读挂起期间邮箱到达事件唤醒本连接任务直写推送帧）。
        // TLS 流读写互斥（rustls 单状态机）不参与双路等待，推送随
        // 下一输入帧在消费段投递
        let push_mailbox = session
          .pubsub_mailbox()
          .filter(|_| stream.supports_shared_rw());
        let before = scratch.len();

        // 读完成形态：字节结果 / 取消（取消时读缓冲随 future 已失，
        // compio 读取消语义，断连收尾——与 KILL 主路径一致）
        enum ReadEnd {
          Bytes(BufResult<usize, RecvAppend>),
          Cancelled,
        }
        let read_end = match push_mailbox {
          Some(mailbox) => {
            // 读 future 存活于推送处理全程（绝不 drop 重建——半包字节
            // 会随 future 丢失）；无令牌形态以永不触发的哑令牌统一类型
            let token = self.kill_token.clone().unwrap_or_default();
            let mut read_fut = Box::pin(
              stream
                .read_shared(RecvAppend(scratch))
                .with_cancel(token)
                .fail_fast(),
            );
            loop {
              match wait_read_or_push(&mut read_fut, &mailbox).await {
                PushOutcome::Read(Ok(bytes)) => break ReadEnd::Bytes(bytes),
                PushOutcome::Read(Err(Cancelled)) => break ReadEnd::Cancelled,
                PushOutcome::Push => {
                  // 读仍在途：排空邮箱推送帧直写网络（TCP 全双工，
                  // 读 op 挂起中写合法且安全）
                  let Some(session) = self.session.as_mut() else {
                    break ReadEnd::Cancelled;
                  };
                  resp_pooled.clear();
                  session.drain_pubsub_into(resp_pooled.vec_mut());
                  if resp_pooled.is_empty() {
                    continue; // 唤醒竞态：邮箱已被他方排空
                  }
                  if self.throttle.enter_send().await.is_err() {
                    break ReadEnd::Cancelled;
                  }
                  let payload = resp_pooled
                    .take_buffer()
                    .expect("pooled send buffer active");
                  let BufResult(write_res, mut reclaimed) = stream.write_all_shared(payload).await;
                  reclaimed.clear();
                  resp_pooled.set_buffer(reclaimed);
                  self.throttle.exit_send();
                  // 推送写失败 = 连接异常，中断等待断连收尾
                  if write_res.is_err() {
                    break ReadEnd::Cancelled;
                  }
                }
              }
            }
          }
          None => {
            // 纯读阻塞形态：零拷贝网络字节追加直入会话自有接收缓冲
            //（半包残余驻留缓冲头部）；有注册条目时挂取消令牌
            //（KILL/注销打断挂起读）
            match self.kill_token.clone() {
              Some(token) => {
                match stream
                  .read(RecvAppend(scratch))
                  .with_cancel(token)
                  .fail_fast()
                  .await
                {
                  Ok(pair) => ReadEnd::Bytes(pair),
                  Err(Cancelled) => ReadEnd::Cancelled,
                }
              }
              None => ReadEnd::Bytes(stream.read(RecvAppend(scratch)).await),
            }
          }
        };

        let BufResult(read_res, wrapped) = match read_end {
          ReadEnd::Cancelled => break,
          ReadEnd::Bytes(bytes) => bytes,
        };
        scratch = wrapped.0;
        net_in = scratch.len() - before;
        // 取回缓冲，归还先于一切退出路径（半包残余字节必须驻留会话）
        if let Some(session) = self.session.as_mut() {
          session.return_recv_scratch(scratch);
        }
        match read_res {
          Ok(0) => break, // 对端正常关闭
          Ok(_) => {}
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
          Err(e) => return Err(e),
        }
      }

      // ── 消费段 ──
      let Some(session) = self.session.as_mut() else {
        break;
      };
      resp_pooled.clear();
      // 订阅推送顺带排空（有输入的订阅会话：推送帧随本批应答写出；
      // 空闲订阅会话的即时投递由读段双路等待承担）
      session.drain_pubsub_into(resp_pooled.vec_mut());
      // 协议违规哨兵（C# RespParsingException → 发尽应答后断连）
      let mut parse_violation = false;
      loop {
        // 消费返回 Some(_)：含收尾 0（整段消费完毕、缓冲已复位）与流水线
        // 余量两种形态，挂起检查必须先于跳出——慢/阻塞命令恰好是缓冲
        // 收尾帧时消费返回 0 但 pending_slow 已设置，直接 break 令挂起
        // 命令无人驱动，连接永久无应答（无下一批网络字节可期）；
        // 返回 None 为协议违规：游标原样，发尽本轮应答后断连
        if session
          .try_consume_messages_into(resp_pooled.vec_mut())
          .is_none()
        {
          parse_violation = true;
          break;
        }

        // 阻塞/慢路径挂起：await 驱动至完成后继续消费流水线余量
        let mut resumed = false;
        if let Some(blocked) = session.take_blocked_wait() {
          let (cmd, result) = blocked.resolve().await;
          session.resolve_blocked_wait_into(cmd, result, resp_pooled.vec_mut());
          resumed = true;
        }

        if let Some(slow) = session.take_slow_wait() {
          let reply = slow.resolve().await;
          if !reply.is_empty() {
            resp_pooled.vec_mut().extend_from_slice(&reply);
          }
          resumed = true;
        }

        // 半包残余等更多网络字节（含收尾 0 无挂起的等下一批）
        if !resumed {
          break;
        }
      }

      // 镜像累加（监视器瞬时吞吐/ops/s 源；会话 dispose 时随条目注销换轨到
      // 历史归并，二者不双计）
      if let Some(entry) = &self.consumer_entry {
        entry.add_net_bytes((net_in + handshake_net_in) as u64, resp_pooled.len() as u64);
        handshake_net_in = 0;
        if let Some(session) = self.session.as_mut() {
          session.mirror_session_counters(entry);
        }
      }

      // ── 写出段（Throttle 背压 + 缓冲复用）──
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

      // 会话待释放哨兵（QUIT → toDispose）：应答已发尽，主动断连
      //（C# Process 尾部 if (toDispose) DisposeNetworkSender(true) 语义；
      // dispose 请求取走即复位，命中即退出泵循环走 dispose 收尾）
      if let Some(session) = self.session.as_mut()
        && session.take_dispose_request()
      {
        break;
      }

      // 协议违规：应答已发尽，断连（C# DisposeNetworkSender 语义）
      if parse_violation {
        break;
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

/// 订阅推送双路等待结果
enum PushOutcome<R> {
  /// 读完成（网络字节就绪，含取消失败外的全部读出口）
  Read(R),
  /// 邮箱到达推送（读仍在途，future 存活于调用方，处理后带同一 future
  /// 重入等待）
  Push,
}

/// 订阅态读等待：读 future（存活于调用方，绝不 drop——compio 读取消丢
/// 半包字节）+ 邮箱到达事件双路竞速
///
/// 读挂起期间被邮箱事件唤醒即返回 [`PushOutcome::Push`]，调用方排空邮箱
/// 写推送帧后带着同一读 future 重入（TCP 全双工，读 op 挂起中写合法）。
/// 唤醒零丢失：listener 注册后双检 + 读 future 复查两道防线（对齐
/// EventWorkQueue::wait_to_read 的防竞态模式）
async fn wait_read_or_push<F, R>(
  read_fut: &mut Pin<Box<F>>,
  mailbox: &PubSubMailbox,
) -> PushOutcome<R>
where
  F: Future<Output = R>,
{
  let mut listener: Option<EventListener> = None;
  poll_fn(|cx| {
    loop {
      if let Poll::Ready(res) = read_fut.as_mut().poll(cx) {
        return Poll::Ready(PushOutcome::Read(res));
      }
      if mailbox.has_messages() {
        return Poll::Ready(PushOutcome::Push);
      }
      let mut lis = match listener.take() {
        Some(l) => l,
        None => {
          let lis = mailbox.listen();
          // 注册后双检：事件可能已发（唤醒丢失防线）
          if mailbox.has_messages() {
            return Poll::Ready(PushOutcome::Push);
          }
          lis
        }
      }; // 读复查（listener 注册期间读可能已就绪，driver waker 已在册）
      if let Poll::Ready(res) = read_fut.as_mut().poll(cx) {
        return Poll::Ready(PushOutcome::Read(res));
      }
      match Pin::new(&mut lis).poll(cx) {
        Poll::Ready(()) => continue, // 到达事件 → 回环（drain 或重检）
        Poll::Pending => {
          listener = Some(lis);
          return Poll::Pending;
        }
      }
    }
  })
  .await
}
