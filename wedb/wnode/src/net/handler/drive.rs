//! KILL/注销终止哨兵与连接泵主循环（读泵/写回域）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs:Start`

use std::{any::Any, io, panic::AssertUnwindSafe, sync::Arc};

use compio::{
  BufResult,
  runtime::{CancelToken, Cancelled, FutureExt},
};
use futures_util::FutureExt as _;
use log::{debug, error};
use wbase::pool::DEFAULT_BUFFER_SIZE;

use super::{
  NetworkHandler,
  buffer::{MIN_HANDSHAKE_BYTES, MIN_READ_SPACE, RecvAppend},
  kill::spawn_kill_watcher,
  push::{PushOutcome, wait_read_or_push},
};
use crate::{
  net::stream::ConnectionStream,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

impl<C: MessageConsumerFace> NetworkHandler<C> {
  /// 驱动统一连接流（TCP / Unix / TLS）异步读写与协议切片泵
  ///
  /// 泵循环无论从何路径收场，本函数都先走一遍流的关闭序再释放会话（见函数体内
  /// 顺序说明），故它是连接退出的唯一收场点。
  ///
  /// 在 garnet 中的相对路径:
  /// - `libs/common/Networking/NetworkHandler.cs:Start`
  pub async fn process_stream<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    mut stream: ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 会话隔离（C# RespServerSession.TryConsumeMessages 最外层 catch(Exception)
    // → Dispose 对位）：泵循环内 panic 只断本连接，不穿出连接任务。断连走
    // 下方与四条退出路径同一条收场尾巴（shutdown → dispose），accept 泵与
    // 其他连接任务不受影响。AssertUnwindSafe：panic 后不复用 drive_loop 的
    // 任何中间状态，直接收场，无跨 await 观察半更新状态的面。
    let res = match AssertUnwindSafe(self.drive_loop(&mut stream, session_provider, sender_id))
      .catch_unwind()
      .await
    {
      Ok(res) => res,
      Err(payload) => {
        error!(
          "连接 {sender_id}({}) 泵 panic，隔离断连: {}",
          self.remote_endpoint,
          panic_payload_text(&payload)
        );
        Err(io::Error::other("会话泵 panic"))
      }
    };
    // 关闭序（C# TcpNetworkHandlerBase.Dispose 的 Shutdown → Close → DisposeImpl
    // 序）：先 FIN / close_notify，后 dispose。顺序不可颠倒——dispose 释放会话与
    // 订阅推送通道，其后推送侧再写已关闭的流没有意义，与泵内「缓冲归还先于一切
    // 退出路径」同一顺序口径。退出路径（取消、对端 EOF、意外 EOF、错误上抛、
    // 泵 panic）共用此一条尾巴。对端已断时错误弃用（与 wconn 客户端泵同口径），
    // 绝不覆盖 drive_loop 的原返回值。
    let _ = stream.shutdown().await;
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
    // 握手批净入字节（并入消费段首轮镜像，监视器字节口径与批次数对齐）
    let mut handshake_net_in = 0usize;
    let mut kill_token: Option<CancelToken> = None;

    // ── 握手段：收首批识别 WireFormat 并装配会话（C# Process →
    // serverHook.TryCreateMessageConsumer 装配点）。握手期会话未建，批次
    // 字节先落池化缓冲；会话创建点把未消费字节一次性迁入会话自有接收
    // 缓冲（此迁移点会话游标必为零，字节流无缝衔接、无重复并入），旧池
    // 缓冲随即 RAII 归还，此后网络字节零拷贝直入会话缓冲
    {
      let mut pooled = self.buffer_pool.get_ref(0);
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

        if pooled.vec_ref().len() < MIN_HANDSHAKE_BYTES {
          continue; // 首批不足识别字节数，继续读
        }

        let mut session = session_provider
          .get_session(WireFormat::Ascii, sender_id)
          .ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "会话提供者拒绝建立会话")
          })?;

        // 未消费批次字节迁入会话自有接收缓冲（握手期无消费，整段迁移）
        let mut scratch = session.take_recv_scratch();
        scratch.extend_from_slice(pooled.vec_ref());
        session.return_recv_scratch(scratch);
        drop(pooled);

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
          let token = CancelToken::new();
          spawn_kill_watcher(entry, token.clone());
          kill_token = Some(token);
        }
        break;
      }
    }

    // 消费驱动序（C# NetworkHandler.Read → Process 循环序的等价重排）：
    // 先消费缓冲中现有完整帧（含握手批迁移字节），再读取下一批网络字节
    let mut net_in = 0usize;
    // 池化发送缓冲（容量 64KB，连接生命周期内复用，RAII 自动归还句柄；零 Arc 开销借用）
    let mut resp_pooled = self.buffer_pool.get_ref(DEFAULT_BUFFER_SIZE);
    'drive: while let Some(session) = self.session.as_mut() {
      // ── 消费驱动段 ──
      // 按轮循环（C# Process 满刷循环的泵投影：会话累计应答达水位在命令
      // 边界让渡时，本轮应答实写后立即重入消费，不等下一批网络字节——
      // 对标 C# SendAndReset → Send 后重取缓冲续写）；未触水位的普通批
      // 与单轮形态逐一相同（整批消费 → 单次写出）
      let mut written = 0usize;
      // 协议违规哨兵（C# RespParsingException → 发尽应答后断连）
      let mut parse_violation = false;
      loop {
        resp_pooled.clear();
        // 订阅推送顺带排空（有输入的订阅会话：推送帧随本轮应答写出；
        // 空闲订阅会话的即时投递由读段双路等待承担）
        session.drain_pubsub_into(resp_pooled.vec_mut());
        // 本轮让渡哨兵（置位 = 本轮应答实写后立即续消费）
        let mut watermarked = false;
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

          // 批内输出水位让渡：接收缓冲尚有完整帧，本轮应答实写后立即续消费
          if session.take_output_watermark_yield() {
            watermarked = true;
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
            session.resolve_slow_wait_into(&reply, resp_pooled.vec_mut());
            resumed = true;
          }

          // 半包残余等更多网络字节（含收尾 0 无挂起的等下一批）
          if !resumed {
            break;
          }
        }

        // ── 本轮写出段（Throttle 背压 + 缓冲复用；C# Send 的泵单点）──
        if !resp_pooled.is_empty() {
          // WAIT-FOR-COMMIT 持久性档出网前置等待（C# RespServerSession.cs:1453
          // `Send` 内 `if (waitForAofBlocking)` → storeWrapper.WaitForCommitAsync
          // 读点：rust 会话不持网络发送器，出网单点即本泵写出段，故读点随
          // 写出段逐轮就地取标记——水位让渡的多轮形态下每轮实写前重读，
          // 与 C# 每次 Send 直读字段同口径；compio 挂起不占线程，为 C#
          // 网络线程 BlockingWait 的异步等价。等待结果如 C# 一律弃用——
          // 跳过（无 AOF / 库读锁未取到）与成功两态都照常发出应答）
          if session.wait_for_aof_blocking() {
            session_provider.wait_for_commit_async().await;
          }
          // 镜像按发出字节口径累计（含违规批终局应答；发出即计）
          written += resp_pooled.len();
          if self.throttle.enter_send().await.is_err() {
            break 'drive;
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
              break 'drive;
            }
            return Err(e);
          }

          if resp_pooled.capacity() > DEFAULT_BUFFER_SIZE {
            resp_pooled = self.buffer_pool.get_ref(DEFAULT_BUFFER_SIZE);
          }
        }

        // 未触水位让渡：本批消费驱动收尾（挂起已 resolve 续消费的形态
        // 由内层循环承担，此处只收整批）
        if !watermarked {
          break;
        }
      }

      // 镜像累加（监视器瞬时吞吐/ops/s 源；会话 dispose 时随条目注销换轨到
      // 历史归并，二者不双计）
      if let Some(entry) = &self.consumer_entry {
        entry.add_net_bytes((net_in + handshake_net_in) as u64, written as u64);
        handshake_net_in = 0;
        if let Some(session) = self.session.as_mut() {
          session.mirror_session_counters(entry);
        }
      }

      // 会话待释放哨兵（QUIT → toDispose）：应答已发尽，主动断连
      //（C# Process 尾部 if (toDispose) DisposeNetworkSender(true) 语义；
      // dispose 请求取走即复位，命中即退出泵循环走 dispose 收尾）
      if let Some(session) = self.session.as_mut()
        && session.take_dispose_request()
      {
        break 'drive;
      }

      // 协议违规：应答已发尽，断连（C# DisposeNetworkSender 语义）
      if parse_violation {
        break 'drive;
      }

      // 致命断流信号（不可恢复错误 / APPENDLOG 拒收等）：应答已发尽，主动断连
      //（对标 C# clientResponse: false 上抛后断连语义；命中即退出泵循环走 dispose 收尾）
      if let Some(session) = self.session.as_mut()
        && session.take_fatal_disconnect()
      {
        debug!(
          "连接 {sender_id}({}) 触发致命断连信号，退出泵循环",
          self.remote_endpoint
        );
        break 'drive;
      }

      // ── 网络读取段（下一批；读完回到循环头消费）──
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
        // 承接：读挂起期间邮箱到达事件唤醒本连接任务直写推送帧）。传输形态
        // 不分流：TCP/Unix 全双工共享读句柄，TLS 走读写互斥句柄对（rustls 单
        // 连接状态机，逐轮 poll 取锁、挂起即释锁），两形态共用同一双路等待
        // 与同一条写出路径
        let push_mailbox = session.pubsub_mailbox();
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
            let token = kill_token.clone().unwrap_or_default();
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
                  // 读仍在途：排空邮箱推送帧直写网络（TCP/Unix 全双工读 op
                  // 挂起中写合法；TLS 读句柄挂起即释锁，写句柄据此推进同一
                  // 连接）
                  let Some(session) = self.session.as_mut() else {
                    break ReadEnd::Cancelled;
                  };
                  resp_pooled.clear();
                  session.drain_pubsub_into(resp_pooled.vec_mut());
                  if resp_pooled.is_empty() {
                    continue; // 唤醒竞态：邮箱已被他方排空
                  }
                  // 推送帧与命令应答同一出网规则（C# Publish 经会话 Send
                  // 写出，waitForAofBlocking 置位即先等 AOF 提交落盘）
                  if session.wait_for_aof_blocking() {
                    session_provider.wait_for_commit_async().await;
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
            match &kill_token {
              Some(token) => {
                match stream
                  .read(RecvAppend(scratch))
                  .with_cancel(token.clone())
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
    }

    Ok(())
  }
}

/// panic 载荷转可读文本（&str / String 直取，其余退回类型描述）
pub(crate) fn panic_payload_text(payload: &(dyn Any + Send)) -> String {
  if let Some(s) = payload.downcast_ref::<&str>() {
    (*s).into()
  } else if let Some(s) = payload.downcast_ref::<String>() {
    s.clone()
  } else {
    "非文本 panic 载荷".into()
  }
}
