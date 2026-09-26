//! KILL/注销终止哨兵与连接泵主循环（读泵/写回域）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs:Start`

use std::{
  any::Any,
  future::{Future, pending},
  io,
  mem::take,
  panic::AssertUnwindSafe,
  pin::pin,
  sync::Arc,
  task::{Context, Poll, Waker},
};

use compio::{
  BufResult,
  runtime::{CancelToken, Cancelled, FutureExt},
};
use futures_util::{
  FutureExt as _,
  future::{Either, select},
};
use log::{debug, error};

use super::{
  NetworkHandler,
  buffer::{MIN_HANDSHAKE_BYTES, MIN_READ_SPACE, RecvAppend},
  push::{PushOutcome, wait_read_or_push},
};
use crate::{
  net::stream::ConnectionStream,
  servers::consumer_registry::ConsumerEntry,
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
  /// 零拷贝直入会话缓冲，消费游标驻留会话跨批次持久；每批消费收尾由会话
  /// 执行 C# ShiftTransportReceiveBuffer 对偶收口——整段消费完清零复位，
  /// 半包残余平移至首部并截断长度（仅事务在途批驻留原偏移），泵取出的
  /// scratch 长度恒为未消费残余实际长度，64KB 常驻容量水位不因已消费
  /// 前缀驻留而扩容。
  ///
  /// 读写统一挂 KILL/注销终止令牌（C# 直关套接字令一切在途请求失败的对偶）：
  /// 读侧挂起（握手/主读取）与写出段四枚在途 await（命令臂 write_all、推送臂
  /// write_all_shared、两臂 wait_for_commit_async）经 [`killable`] 挂同一
  /// 令牌秒断；在途阻塞/慢挂起执行体同与终止广播 select 竞速（消费段），
  /// 终止胜出即刻丢弃执行体退出泵循环。
  async fn drive_loop<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    stream: &mut ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 握手批净入字节（并入消费段首轮镜像，监视器字节口径与批次数对齐）
    let mut handshake_net_in = 0usize;
    // KILL/注销终止令牌（accept 侧预注册装配，C# handler.Start 前注册的
    // 对偶——握手期挂起读同样可被 CLIENT KILL / 停机排空秒断；哑桩形态
    // None 不挂取消）
    let kill_token = self.kill_token.clone();
    // 池基准规格（随 network_buffer_size 配置经 buffer_size 访问器单点取得）：
    // 握手段低水位收敛与响应缓冲借出/复位共用的唯一借还锚点
    let buffer_size = self.buffer_pool.buffer_size();

    // ── 握手段：收首批识别 WireFormat 并装配会话（C# Process →
    // serverHook.TryCreateMessageConsumer 装配点）。握手期会话未建，批次
    // 字节先落池化缓冲；会话创建点把未消费字节一次性迁入会话自有接收
    // 缓冲（此迁移点会话游标必为零，字节流无缝衔接、无重复并入），旧池
    // 缓冲随即 RAII 归还，此后网络字节零拷贝直入会话缓冲。
    // 注册表条目由 accept 循环预注册（C# GarnetServerTcp.cs:256 TryAdd
    // 先于 handler.Start），本函数不再注册
    {
      let mut pooled = self.buffer_pool.get_ref(0);
      loop {
        let mut raw_buf = pooled.take_buffer().expect("pooled buffer active");
        // 空闲空间不足预留阈值：补足一个读取阈值量（握手期无消费，无需平移）。
        // 预留量随阈值走、不随硬编码规格——池块扩容经 amortized 倍增保持
        // 2 的幂，恒落池层级域内，归还端按容量精准配平回池；曾以
        // DEFAULT_BUFFER_SIZE(65536) 硬编码预留，非默认 network_buffer_size
        // 下借出块被扩至越界容量，归还判失配就地丢弃，池失一块常驻配额
        if raw_buf.capacity() - raw_buf.len() < MIN_READ_SPACE {
          raw_buf.reserve(MIN_READ_SPACE);
        }
        let before = raw_buf.len();
        // KILL/停机打断握手期挂起读（预注册条目治理面）：取消时读缓冲随
        // future 失（compio 读取消语义，与消费段同口径），走收场尾巴
        //（shutdown → dispose 注销）
        let BufResult(read_res, wrapped) =
          match killable(stream.read(RecvAppend::new(raw_buf)), &kill_token).await {
            Err(Cancelled) => return Ok(()),
            Ok(pair) => pair,
          };
        raw_buf = wrapped.into_parts().1;
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
        // 握手批净入字节就地镜像（C# RespServerSession.cs:600 TryConsumeMessages
        // 尾部对全部消费字节 incr_total_net_input_bytes 的条目侧承接：迁移字节
        // 驻留会话缓冲，但会话侧消费口径被条目镜像覆盖屏蔽（consumer_registry
        // monitor_sample 以 net_input_bytes 覆盖快照），收尾检查点前置至此——
        // 字节到达即入账，首轮消费段任意提前退出路径不再漏计）
        if let Some(entry) = &self.consumer_entry {
          entry.add_net_bytes(handshake_net_in as u64, 0);
        }
        // 握手块低水位收敛：分片到站触发阈值预留扩容的块，清空后缩回池基准
        // 规格再归还，恒回基础层级——扩容块若原样析构将漂移高级层级，逐连接
        // 蚕食基础层级闲置配额致穿透堆分配（对标 C# NetworkHandler.cs:529
        // ShrinkNetworkReceiveBuffer 低水位收缩，与响应缓冲写出复位同一收敛单点）
        pooled.vec_mut().clear();
        shrink_to_base(pooled.vec_mut(), buffer_size);
        drop(pooled);

        // 会话装配（端点对取 handler 构造期捕获单源——remote/local 同为
        // accept 侧已知值，TLS 臂流层擦除不可见故前置捕获，见
        // NetworkHandler::set_session）
        self.set_session(session);
        break;
      }
    }

    // 消费驱动序（C# NetworkHandler.Read → Process 循环序的等价重排）：
    // 先消费缓冲中现有完整帧（含握手批迁移字节），再读取下一批网络字节
    // 池化发送缓冲（容量为池基准规格，连接生命周期内复用，RAII 自动归还
    // 句柄；零 Arc 开销借用）
    let mut resp_pooled = self.buffer_pool.get_ref(buffer_size);
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

          // ACL 链停车臂（AUTH / HELLO / ACL 族与挂载陈旧刷新：命令链的
          // 存储点查严禁同步收割，消费段停车登记，此处内联 await 闭环）。
          // 两臂同与终止广播 select 竞速（C# RespServerSession.Dispose 的
          // asyncWaiterCancel 语义——KILL/停机胜出即丢弃执行体退出泵循环，
          // 不被在途认证/刷新拖住）
          let mut resumed = false;
          if session.take_pending_acl_refresh() {
            let terminated = {
              let terminate_fut = pin!(wait_terminate(self.consumer_entry.as_ref()));
              let refresh_fut = pin!(session.pending_acl_refresh_fut());
              matches!(select(terminate_fut, refresh_fut).await, Either::Left(_))
            };
            if terminated {
              break 'drive;
            }
            // 重驱型（不产应答）：游标已回退本命令起点，重入消费即重解析，
            // 门链以刷新后的挂载重评
            resumed = true;
          }
          if let Some((cmd, args, parked_output_len)) = session.take_pending_auth_acl() {
            // 参数视图出借停车快照（执行臂只读借用；快照生命周期覆盖 await）
            let views: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            let terminated = {
              let terminate_fut = pin!(wait_terminate(self.consumer_entry.as_ref()));
              let auth_fut = pin!(session.pending_auth_acl_fut(cmd, &views));
              matches!(select(terminate_fut, auth_fut).await, Either::Left(_))
            };
            if terminated {
              break 'drive;
            }
            // 停车臂失败应答补计（CommandStats 门泵侧收口：同步段扫描时
            // 应答尚未组装、判据恒假，此处按同判据补 failed_calls）
            session.account_parked_auth_acl_failure(cmd, parked_output_len);
            // 产应答型（命令游标已推进）：会话输出缓冲按流水线顺序冲出后
            // 续消费流水线余量
            session.flush_output_into(resp_pooled.vec_mut());
            resumed = true;
          }

          // 脚本内挂起（EVAL 内 BLPOP 等命中阻塞/慢路径）：泵层 async 续跑
          // 脚本协程至完成（协程化承接，VM 同步绑定零内联收割；C# 侧脚本内
          // 命令在回调栈上同步收割的协程对位），应答随续跑窗口并入会话输出，
          // 重入消费时按流水线顺序冲出。与终止广播 select 竞速（ACL 两臂同款：
          // KILL/停机广播胜出即丢弃执行体、退出泵循环，不被在途慢/阻塞等待拖住）
          if session.has_script_suspend() {
            let terminated = {
              let terminate_fut = pin!(wait_terminate(self.consumer_entry.as_ref()));
              let resume_fut = pin!(session.resume_suspended_script_fut(resp_pooled.vec_mut()));
              matches!(select(terminate_fut, resume_fut).await, Either::Left(_))
            };
            if terminated {
              break 'drive;
            }
            resumed = true;
          }

          // 阻塞/慢路径挂起：await 驱动至完成后继续消费流水线余量。两分支
          // 同与终止广播 select 竞速（C# RespServerSession.Dispose 的
          // asyncWaiterCancel?.Cancel() + asyncWaiter?.Signal() 语义——KILL/
          // 注销时立即撤销在途等待并放弃应答写回）：终止胜出即丢弃执行体、
          // 退出泵循环走 shutdown → dispose 收口注销，服务端停机排空与
          // CLIENT KILL 均不被在途慢/阻塞命令拖住
          // 阻塞挂起三路竞速：终止广播 × 阻塞结果 × 对端活性探测读（竞速样板收口
          // 见 [`probe_race`]）。C# 网络线程 BlockingWait 挂死期间连接生命周期由内核
          // 异步事件守护（TcpNetworkHandlerBase.cs:214 bytesTransferred == 0 或
          // SocketError != Success 即 Dispose → RespServerSession.Dispose :408
          // itemBroker?.HandleSessionDisposed 注销观察者）；compio 单协程顺序驱动无
          // 内核守护面，等待期间不读套接字即客户端断连盲区——FIN/RST 无人在场，观察
          // 者滞留经纪等待队列成僵尸，新写入元素被误弹出后写回 BrokenPipe 丢弃。对端
          // 脱机/终止胜出即 abort 注销观察者并退出泵循环，阻塞结果胜出写回应答续消费
          if let Some(mut blocked) = session.take_blocked_wait() {
            let end = probe_race(
              stream,
              self.consumer_entry.as_ref(),
              session,
              blocked.resolve(),
            )
            .await?;
            match end {
              RaceEnd::Disposed => {
                blocked.abort();
                break 'drive;
              }
              RaceEnd::Resolved((cmd, result)) => {
                session.resolve_blocked_wait_into(cmd, result, resp_pooled.vec_mut());
                resumed = true;
              }
            }
          }

          if let Some(slow) = session.take_slow_wait() {
            if let Some(ref entry) = self.consumer_entry {
              entry.set_in_slow_wait(true);
            }
            // 慢路径挂起三路竞速：终止广播 × 慢执行体 × 对端活性探测读（竞速样板收口
            // 见 [`probe_race`]）。慢执行体内联阻塞等待面（阻塞族冷键降级经
            // BlockWaitFace 登记观察者，timeout=0 无限等待）与 C# 同场景同构——挂起
            // 期连接生命周期由内核异步事件守护（TcpNetworkHandlerBase.cs:214）；compio
            // 单协程顺序驱动无内核守护面，不探测读即对端断连盲区。对端脱机胜出即丢弃
            // 执行体断连：执行体内阻塞观察者经 ObserverDropGuard 注销经纪
            let end = probe_race(
              stream,
              self.consumer_entry.as_ref(),
              session,
              slow.resolve(),
            )
            .await?;
            if let Some(ref entry) = self.consumer_entry {
              entry.set_in_slow_wait(false);
            }
            match end {
              // 执行体随竞速败侧丢弃（future drop 即取消），应答弃写；慢执行体内阻塞
              // 观察者经 ObserverDropGuard 注销，退出泵循环走收口注销
              RaceEnd::Disposed => break 'drive,
              RaceEnd::Resolved(reply) => {
                session.resolve_slow_wait_into(&reply, resp_pooled.vec_mut());
                resumed = true;
              }
            }
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
          // 网络线程 BlockingWait 的异步等价。提交失败即断连——C#
          // BlockingWait 抛 CommitFailureException 后应答不发出，
          // RespServerSession.cs:566 catch (Exception) Dispose 断连；
          // 返回值（false = 无 AOF 跳过）如 C# 弃用，成功照常发出应答）
          // 在途提交等待同样挂取消钩（C# TryClose 直关套接字使一切 outstanding
          // requests 失败的对位）：取消即弃应答写回走 break 收场尾巴
          if session.wait_for_aof_blocking() {
            match killable(session_provider.wait_for_commit_async(), &kill_token).await {
              Err(Cancelled) => break 'drive,
              Ok(Err(e)) => {
                error!(
                  "连接 {sender_id}({}) AOF 提交落盘等待失败，断连: {e}",
                  self.remote_endpoint
                );
                break 'drive;
              }
              Ok(Ok(_)) => {}
            }
          }
          // 镜像按发出字节口径累计（含违规批终局应答；发出即计）
          written += resp_pooled.len();
          if self.throttle.enter_send().await.is_err() {
            break 'drive;
          }
          let payload = resp_pooled
            .take_buffer()
            .expect("pooled send buffer active");
          // 在途写出挂取消钩（KILL/停机终止域补全）：取消撤销在途写 op，
          // payload 缓冲随 future 失（compio 取消语义，与读取消同口径），
          // 归还节流额度后走 break 收场尾巴
          let write_outcome = killable(stream.write_all(payload), &kill_token).await;
          self.throttle.exit_send();
          let BufResult(write_res, mut reclaimed) = match write_outcome {
            Err(Cancelled) => break 'drive,
            Ok(pair) => pair,
          };
          reclaimed.clear();
          // 应答曾超池基准规格：就地收敛复位回常驻水位（对标 C# SendAndReset
          // 分片复位——响应缓冲恒为配置规格不因大应答扩容驻留）。不重新借出：
          // 重借会从池队列弹出闲置块、旧扩容块析构迁移至高级层级，突发大应答
          // 逐轮蚕食基准层级闲置配额致池空退化为堆分配抖动
          shrink_to_base(&mut reclaimed, buffer_size);
          resp_pooled.set_buffer(reclaimed);

          if let Err(e) = write_res {
            if is_dead_conn(&e) {
              break 'drive;
            }
            return Err(e);
          }
        }

        // 未触水位让渡：本批消费驱动收尾（挂起已 resolve 续消费的形态
        // 由内层循环承担，此处只收整批）
        if !watermarked {
          break;
        }
      }

      // 出向镜像累加（监视器瞬时吞吐/ops/s 源；入向已在各字节到达点就地
      // 记账——握手迁移点、读取段净增、阻塞探测保全臂，检查点不重临的提前
      // 退出路径不再漏计入向；会话 dispose 时随条目注销换轨到历史归并，
      // 二者不双计）
      if let Some(entry) = &self.consumer_entry {
        entry.add_net_bytes(0, written as u64);
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
        // 空闲段初始化代际随缓冲进出（TLS 读清零每缓冲付一次的跨读记忆，
        // wbase::primed 契约；键驻会话、与借出的缓冲同生命周期）
        let prime_key = session.recv_prime_key();
        let mut scratch = session.take_recv_scratch();
        // 空闲空间不足预留阈值：会话层消费收尾已平移半包残余至首部并截断
        // 长度（C# ShiftTransportReceiveBuffer 对偶，事务在途批除外），取出
        // 的缓冲长度恒为残余实际长度（通常仅数字节至单个大参数长度），
        // 补足一个读取阈值量即可（常驻水位由会话层
        // DEFAULT_RECV_BUFFER_CAPACITY 单点回归），与握手段同一预留口径，
        // 不再硬编码 65536 抬高单批内存驻留
        if scratch.capacity().saturating_sub(scratch.len()) < MIN_READ_SPACE {
          scratch.reserve(MIN_READ_SPACE);
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
                .read_shared(RecvAppend::from_primed(scratch, prime_key))
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
                  // 推送实发字节数（与命令臂 written 同一「发出即计」口径，
                  // 取缓冲前先量）
                  let push_bytes = resp_pooled.len();
                  // 推送帧与命令应答同一出网规则（C# Publish 经会话 Send
                  // 写出，waitForAofBlocking 置位即先等 AOF 提交落盘）；
                  // 提交失败同命令臂：帧不发出，断连收尾；在途提交等待与
                  // 在途写出同样挂取消钩（KILL/停机终止域补全）
                  if session.wait_for_aof_blocking() {
                    match killable(session_provider.wait_for_commit_async(), &kill_token).await {
                      Err(Cancelled) => break ReadEnd::Cancelled,
                      Ok(Err(e)) => {
                        error!(
                          "连接 {sender_id}({}) 推送帧 AOF 提交落盘等待失败，断连: {e}",
                          self.remote_endpoint
                        );
                        break ReadEnd::Cancelled;
                      }
                      Ok(Ok(_)) => {}
                    }
                  }
                  if self.throttle.enter_send().await.is_err() {
                    break ReadEnd::Cancelled;
                  }
                  let payload = resp_pooled
                    .take_buffer()
                    .expect("pooled send buffer active");
                  // 取消撤销在途写 op，payload 缓冲随 future 失（与命令臂
                  // 同一收场口径）：归还节流额度后断连收尾
                  let write_outcome = killable(stream.write_all_shared(payload), &kill_token).await;
                  self.throttle.exit_send();
                  let BufResult(write_res, mut reclaimed) = match write_outcome {
                    Err(Cancelled) => break ReadEnd::Cancelled,
                    Ok(pair) => pair,
                  };
                  reclaimed.clear();
                  // 推送臂与命令臂同一低水位收敛：突发大推送后长连接不驻留
                  // 峰值容量（订阅会话常驻等待推送，缺失收缩即永久膨胀）
                  shrink_to_base(&mut reclaimed, buffer_size);
                  resp_pooled.set_buffer(reclaimed);
                  // 推送写失败 = 连接异常，中断等待断连收尾
                  if write_res.is_err() {
                    break ReadEnd::Cancelled;
                  }
                  // pubsub 推送字节镜像累加（C# RespServerSession.cs:1462 Send
                  // 唯一出向记账点的空闲等待期承接：订阅会话常驻读挂起，消费段
                  // 的 written 与收尾 add_net_bytes 永不重临，推送实发字节就地
                  // 计入条目镜像，否则出向流量在监视器/瞬时吞吐/会话采样口径
                  // 全盲；会话计数同步随行，杜绝复位与动态字段投影盲区）
                  if let Some(entry) = &self.consumer_entry {
                    entry.add_net_bytes(0, push_bytes as u64);
                    session.mirror_session_counters(entry);
                  }
                }
              }
            }
          }
          None => {
            // 纯读阻塞形态：零拷贝网络字节追加直入会话自有接收缓冲
            //（半包残余驻留缓冲头部）；有注册条目时挂取消令牌
            //（KILL/注销打断挂起读）
            match killable(
              stream.read(RecvAppend::from_primed(scratch, prime_key)),
              &kill_token,
            )
            .await
            {
              Err(Cancelled) => ReadEnd::Cancelled,
              Ok(bytes) => ReadEnd::Bytes(bytes),
            }
          }
        };

        let BufResult(read_res, wrapped) = match read_end {
          ReadEnd::Cancelled => break,
          ReadEnd::Bytes(bytes) => bytes,
        };
        let (prime_key, scratch) = wrapped.into_parts();
        let net_in = scratch.len() - before;
        // 取回缓冲，归还先于一切退出路径（半包残余字节必须驻留会话；
        // 代际先于缓冲回写会话，跨读记忆闭环）
        if let Some(session) = self.session.as_mut() {
          session.set_recv_prime_key(prime_key);
          session.return_recv_scratch(scratch);
        }
        // 入向净增字节就地镜像（字节到达即入账的记账前置点：检查点位于消费
        // 段之后，消费段任意提前退出路径——阻塞挂起 Disposed、慢路径终止、
        // AOF 失败、throttle、写错误——与读取段 Ok(0)/EOF/Err 收场都不再重临
        // 检查点，末批入向字节就地入账才不漏计；C# 对位为 RespServerSession
        // .cs:600 TryConsumeMessages 尾部对全部消费字节的 incr_total_net_input
        // _bytes——消费口径真值单源仍在会话侧，此处只补条目镜像）
        if net_in > 0
          && let Some(entry) = &self.consumer_entry
        {
          entry.add_net_bytes(net_in as u64, 0);
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

/// 挂 KILL/注销终止令牌执行一个在途 await 点（读/写出/提交等待统一机制）
///
/// 对位 C# GarnetTcpNetworkSender.TryClose（:285）的跨线程 `socket.Close()`
///（其注释自认 "should cause all outstanding requests to fail"，:291）：C#
/// 直关套接字令任何在途 send/提交等待即刻失败，handler 随即走 Dispose 链摘除
/// 注册表条目；rust 连接任务独占套接字无从外关，等价承接为一切在途 await 点
///（握手读、主读取段、命令臂 write_all 与推送臂 write_all_shared、两臂
/// wait_for_commit_async）统一挂本令牌——取消撤销在途 compio op，缓冲随
/// future 失（compio 取消语义），调用方与既有终止胜出臂同走 break 收场尾巴
///（shutdown → dispose 注销）。无令牌（未装配注册表的哑桩宿主）直落不挂取消
async fn killable<F: Future>(fut: F, token: &Option<CancelToken>) -> Result<F::Output, Cancelled> {
  match token {
    Some(token) => fut.with_cancel(token.clone()).fail_fast().await,
    None => Ok(fut.await),
  }
}

/// 终止竞速等待（注册条目 KILL/注销广播命中即完成；无条目永不完成）
///
/// 对位 C# RespServerSession.Dispose 的注销语义（`asyncWaiterCancel?.Cancel()`
///   + `asyncWaiter?.Signal()`——会话注销关闭时立即撤销在途异步等待并放弃
///     应答写回，网络执行循环迅速退出注销；法定锚点由
///     `RespServerSession::dispose`（resp::resp_server_session::core）单点持有）
///
/// 阻塞/慢挂起执行体与本等待 select 竞速：终止胜出即丢弃执行体、退出泵
/// 循环走 shutdown → unregister 收口。无注册条目（未装配注册表的宿主）永不
/// 终止，挂起只由执行体完成驱动
async fn wait_terminate(entry: Option<&Arc<ConsumerEntry>>) {
  if let Some(entry) = entry {
    entry.wait_terminate().await;
  } else {
    pending::<()>().await;
  }
}

/// 挂起执行体三路竞速的收场形态（阻塞臂/慢臂共用）
enum RaceEnd<T> {
  /// 执行体完成：携带其输出，写回应答续消费
  Resolved(T),
  /// KILL/停机广播或对端脱机：丢弃执行体（观察者随守卫注销），退出泵循环
  Disposed,
}

/// 竞速单点：终止广播 × 挂起执行体 × 对端活性探测读三路 select（阻塞臂与慢臂共
/// 用，消除两段同形轮询/保全样板）。终止与执行体各建一次（超时计时与广播注册不随
/// 探测重建而重置），探测读每轮重建（上轮完成必须重新提交）。执行体胜出即挤干探测
/// 读（noop waker 直查完成表）保全已到位字节；探测读到字节即平移入会话接收缓冲续等
/// 执行体，读到 EOF 或连接死亡判定 Disposed，其余错误上抛。
///
/// 探测读用独立缓冲：绝不占会话接收缓冲——执行体胜出弃探测读时挂起 op 连缓冲一并
/// 取消，会话缓冲无恙（compio 取消语义，与读取消同口径）。保全字节就地镜像入向净增：
/// 这批字节虽 extend 进会话缓冲，但下一批读取段的 before 基线已含之，读取段净增口径
/// 永不覆盖，检查点即使重临也无法补记，故到达即入账。
async fn probe_race<F, C>(
  stream: &mut ConnectionStream,
  entry: Option<&Arc<ConsumerEntry>>,
  session: &mut C,
  resolve: F,
) -> io::Result<RaceEnd<F::Output>>
where
  F: Future,
  C: MessageConsumerFace,
{
  let mut probe_buf = Vec::with_capacity(MIN_READ_SPACE);
  let mut terminate_fut = pin!(wait_terminate(entry));
  let mut resolve_fut = pin!(resolve);
  loop {
    let mut probe_fut = pin!(stream.read(RecvAppend::new(take(&mut probe_buf))));
    break match select(
      terminate_fut.as_mut(),
      select(resolve_fut.as_mut(), probe_fut.as_mut()),
    )
    .await
    {
      // KILL/停机广播胜出（应答弃写，执行体随竞速败侧丢弃）
      Either::Left(_) => Ok(RaceEnd::Disposed),
      // 执行体胜出：挤干探测读——已完成未取的结果取回保全；挂起中随 drop 取消无字节
      Either::Right((Either::Left((value, _)), _)) => {
        let mut cx = Context::from_waker(Waker::noop());
        if let Poll::Ready(BufResult(res, wrapped)) = probe_fut.as_mut().poll(&mut cx) {
          probe_buf = wrapped.into_parts().1;
          if let Ok(n) = res
            && n > 0
          {
            preserve_probe(session, entry, &probe_buf);
          }
        }
        Ok(RaceEnd::Resolved(value))
      }
      // 探测读胜出：对端脱机判定或活连接字节保全
      Either::Right((Either::Right((BufResult(read_res, wrapped), _)), _)) => {
        probe_buf = wrapped.into_parts().1;
        match read_res {
          // 对端 EOF 正常关闭（C# bytesTransferred == 0 判定）
          Ok(0) => Ok(RaceEnd::Disposed),
          // 活连接：竞速期到站的请求字节保全入会话接收缓冲供后续流水线消费
          //（C# 阻塞期字节滞留内核缓冲同语义），重建探测读续等执行体
          Ok(_) => {
            preserve_probe(session, entry, &probe_buf);
            probe_buf.clear();
            continue;
          }
          // 连接异常收场（C# SocketError != Success → Dispose）
          Err(e) if is_dead_conn(&e) => Ok(RaceEnd::Disposed),
          Err(e) => Err(e),
        }
      }
    };
  }
}

/// 探测到站字节平移入会话接收缓冲并就地镜像入向净增（竞速挤干臂/保全臂共用；
/// 空缓冲直接跳过）
#[inline]
fn preserve_probe<C: MessageConsumerFace>(
  session: &mut C,
  entry: Option<&Arc<ConsumerEntry>>,
  probe_buf: &[u8],
) {
  let n = probe_buf.len();
  if n == 0 {
    return;
  }
  let mut scratch = session.take_recv_scratch();
  scratch.extend_from_slice(probe_buf);
  session.return_recv_scratch(scratch);
  if let Some(entry) = entry {
    entry.add_net_bytes(n as u64, 0);
  }
}

/// 对端脱机/连接死亡的错误类别（C# SocketError != Success → Dispose 判据）：
/// 意外 EOF、连接重置、破管三类视为正常断连收场，其余上抛
#[inline]
fn is_dead_conn(e: &io::Error) -> bool {
  matches!(
    e.kind(),
    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
  )
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

/// 外出缓冲低水位容量收敛：曾超池基准规格的块就地缩回常驻水位
///
/// 对标 C# 响应缓冲恒为配置 sendBufferSize（GarnetTcpNetworkSender.cs:210
/// SendResponse 满 64KB 分片写出，RespServerSession.cs:1461 SendAndReset 复位
/// 游标不换块）与 NetworkHandler.cs:529 ShrinkNetworkReceiveBuffer 的低水位
/// 收缩语义。就地收敛不重新借出，连接存活全程仅占用建连时借出的单一池配额。
/// `shrink_to` 为容量下界请求，分配器未收敛到位时换配精确基准规格新块，
/// 保证归还端层级精确配平
#[inline]
fn shrink_to_base(buf: &mut Vec<u8>, base: usize) {
  if buf.capacity() > base {
    buf.shrink_to(base);
    if buf.capacity() > base {
      // 换块臂 try_reserve 收口（票 zcode-r55-alloc）：精确基准新块分配失败
      // 即保持原块——收缩是低水位优化非正确性依赖，失败退化为暂驻峰值容量，
      // 杜绝 std 扩容触顶 handle_alloc_error abort 全进程
      let mut fresh = Vec::new();
      if fresh.try_reserve(base).is_ok() {
        *buf = fresh;
      }
    }
  }
}
