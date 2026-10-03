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
  time,
};
use futures_util::{
  FutureExt as _,
  future::{Either, select},
};
use log::{debug, error};
use wbase::{pool::PooledRefBuffer, primed::PrimedVec, throttle::NetworkSenderThrottle};

use super::{
  NetworkHandler,
  buffer::{MIN_HANDSHAKE_BYTES, MIN_READ_SPACE},
  push::{PushOutcome, wait_read_or_push},
};
use crate::{
  net::stream::ConnectionStream,
  resp::resp_server_session::PROBE_PRESERVE_WATERMARK,
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
    // 尾帧有界、弃帧不弃注销：TLS 臂 close_notify 须把连接发送队列写尽 socket
    // 才返回（rustls Stream::poll_close），黑洞对端令其永久 Pending、dispose 永不
    // 执行（C# Dispose 链 Shutdown/Close 为 syscall 先行、注销无条件跟进，结构上
    // 不可残留）。故收场尾 shutdown 与泵内一切在途 await 同挂 KILL/停机令牌，并
    // 加确定性超时双边界（timeout 内层、取消外层，口径同 server.rs TLS 握手
    // 臂）：KILL 先行时令牌已触发即刻弃帧不吃超时界；超时即弃帧照常落 dispose
    // 收口——ConnectionStream 随连接任务返回 Drop，底层 fd 关闭发 FIN/RST 兜底
    // （对位 C# socket.Close 内核接管尾帧）。明文臂 shutdown 即刻就绪，包裹零
    // 成本无行为变化。
    let _ = killable(
      time::timeout(self.shutdown_timeout, stream.shutdown()),
      &self.kill_token,
    )
    .await;
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
          match killable(stream.read(PrimedVec::new(raw_buf)), &kill_token).await {
            Err(Cancelled) => return Ok(()),
            Ok(pair) => pair,
          };
        raw_buf = wrapped.into_inner();
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
        // 出网 armed 闩（与 resp_pooled 同步复位；停泊-续跑轮 AOF 出网闸补全）：
        // 会话不持网络发送器，应答经 take_output_into 出会话冲入本泵 resp_pooled
        // 后，内层循环重入解析流水线后续命令时 session.output 恒为空，
        // handle_aof_commit_mode 见 PING 等 AOF 无关命令即复位 wait_for_aof_blocking，
        // 出网臂读会话字段将漏等已积存的 AOF 相关应答——故每处冲应答出口以「出会话
        // 时点标记」为准就地闩位（判据：resp_pooled 非空且会话标记），出网臂据此等待；
        // 单点辅助见 [`arm_aof_latch`]，不新建第二套等待调用（wait_for_commit_async
        // 仍单点）；armed 随本轮 let 绑定复位，无需显式清零
        let mut armed = false;
        // 订阅推送顺带排空（有输入的订阅会话：推送帧随本轮应答写出；
        // 空闲订阅会话的即时投递由读段双路等待承担）
        session.drain_pubsub_into(resp_pooled.vec_mut());
        // 冲出口①（drain_pubsub_into 把推送帧随轮头冲入 resp_pooled）
        arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);
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
          // 冲出口②（Some 路径，轮尾 take_output_into 已冲出本轮应答）
          arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);

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
          if let Some((cmd, args)) = session.take_pending_auth_acl() {
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
            session.account_parked_auth_acl_failure(cmd);
            // 产应答型（命令游标已推进）：会话输出缓冲按流水线顺序冲出后
            // 续消费流水线余量
            session.flush_output_into(resp_pooled.vec_mut());
            // 冲出口③（flush_output_into 已把停车命令应答冲入 resp_pooled）
            arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);
            resumed = true;
          }

          // 脚本内挂起（EVAL 内 BLPOP 等命中阻塞/慢路径）：泵层 async 续跑
          // 脚本协程至完成（协程化承接，VM 同步绑定零内联收割；C# 侧脚本内
          // 命令在回调栈上同步收割的协程对位），应答随续跑窗口并入会话输出，
          // 重入消费时按流水线顺序冲出。三路竞速（竞速样板收口见
          // [`probe_race`]）：终止广播 × 续跑执行体 × 对端活性探测读。脚本臂
          // 承载无界阻塞等待（脚本内 BLPOP timeout=0），挂起窗不探测读即对端
          // 断连盲区——挂起体驱动点（blocked.resolve / slow.resolve）全在
          // resume future 内，FIN/RST 无人在场，挂起体局部 BlockedWait 因未
          // drop 不触发 Drop→abort 注销，观察者滞留经纪等待队列成僵尸，新写
          // 入元素被误弹出后写回 BrokenPipe 丢弃（同泵阻塞/慢臂自陈判据）；
          // 对端脱机/终止胜出即丢弃执行体退出泵循环——future drop 令挂起体
          // 局部随竞速败侧丢弃，BlockedWait Drop abort / 慢执行体内观察者随
          // ObserverDropGuard 注销，与 dispose 同取消口径。竞速期到站字节
          // 收场并入（见 Resolved 臂）
          if session.has_script_suspend() {
            let end = probe_race(
              stream,
              self.consumer_entry.as_ref(),
              session.resume_suspended_script_fut(resp_pooled.vec_mut()),
            )
            .await?;
            match end {
              RaceEnd::Disposed => {
                // 事务收口单点（泵脚本臂）：重放中途废弃即对 Running 事务补投
                // AOF 废弃终结符后复位，杜绝孤儿残组（判据与语义见
                // MessageConsumerFace::finish_abandoned_txn）
                session.finish_abandoned_txn();
                break 'drive;
              }
              RaceEnd::Resolved((), probe) => {
                // 收场并入：竞速 Resolved 时续跑 future 必已完成，脚本窗
                //（lua.rs 换出机制）关闭已把外层接收缓冲真身还原进会话，
                // 此刻直写即真身——挂起窗内的换出壳交互（redis.call 重入
                // clear+覆写）不再覆没字节，重入消费照常吃进
                preserve_probe(session, &probe);
                // 冲出口④（脚本续跑应答已并入 resp_pooled）
                arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);
                resumed = true;
              }
            }
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
            let end = probe_race(stream, self.consumer_entry.as_ref(), blocked.resolve()).await?;
            match end {
              RaceEnd::Disposed => {
                blocked.abort();
                // 事务收口单点（泵阻塞臂）：EXEC 重放段空键 BLPOP 的挂起被
                // 终止/脱机废弃即补投 AOF 废弃终结符后复位（判据与语义见
                // MessageConsumerFace::finish_abandoned_txn）
                session.finish_abandoned_txn();
                break 'drive;
              }
              RaceEnd::Resolved((cmd, result), probe) => {
                // 收场并入：竞速期到站字节一次并入会话接收缓冲（先于重入消费）
                preserve_probe(session, &probe);
                session.resolve_blocked_wait_into(cmd, result, resp_pooled.vec_mut());
                // 冲出口⑤（阻塞挂起应答先冲出会话缓冲内累积应答再直写本命令
                // 应答，出会话时点标记尚未被后续解析复位）
                arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);
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
            // 竞速先落定标志再上抛：错误臂（存储错即刻断连）也须复位
            // in_slow_wait，杜绝注销前注册表的滞留观测毛刺
            let end = match probe_race(stream, self.consumer_entry.as_ref(), slow.resolve()).await {
              Ok(end) => end,
              Err(err) => {
                if let Some(ref entry) = self.consumer_entry {
                  entry.set_in_slow_wait(false);
                }
                return Err(err);
              }
            };
            if let Some(ref entry) = self.consumer_entry {
              entry.set_in_slow_wait(false);
            }
            match end {
              // 执行体随竞速败侧丢弃（future drop 即取消），应答弃写；慢执行体内阻塞
              // 观察者经 ObserverDropGuard 注销，退出泵循环走收口注销
              RaceEnd::Disposed => {
                // 事务收口单点（泵慢臂）：重放段排队 KEYS/冷键降级挂起的执行
                // 体被废弃即补投 AOF 废弃终结符后复位（判据与语义见
                // MessageConsumerFace::finish_abandoned_txn）
                session.finish_abandoned_txn();
                break 'drive;
              }
              RaceEnd::Resolved(reply, probe) => {
                // 收场并入：竞速期到站字节一次并入会话接收缓冲（先于重入消费）
                preserve_probe(session, &probe);
                session.resolve_slow_wait_into(&reply, resp_pooled.vec_mut());
                // 冲出口⑥（慢路径挂起应答先冲出会话缓冲内累积应答再并入本
                // 应答，同阻塞臂：出会话时点标记未复位即闩位）
                arm_aof_latch(&mut armed, resp_pooled.vec_ref(), session);
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
          // 返回值（false = 无 AOF 跳过）如 C# 弃用，成功照常发出应答。
          // 停泊续跑轮须以出会话时点标记为准：应答字节经 take_output_into 出
          // 会话后，内层重入解析后续 AOF 无关命令会把会话字段复位，故叠加
          // armed 闩（六处冲出口按出会话时点标记置位，见 [`arm_aof_latch`]），
          // 令停泊前已积存的 AOF 相关应答出网前必等提交落盘，与 C# Send 内
          // dcurr==head 判据（含未出网字节）同口径收口；armed 随轮首 let 复位，
          // 本轮兑现后无需显式清零（下一轮重新判定）。
          // 在途提交等待同样挂取消钩（C# TryClose 直关套接字使一切 outstanding
          // requests 失败的对位）：取消即弃应答写回走 break 收场尾巴
          if armed || session.wait_for_aof_blocking() {
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
          let Some(write_res) = pooled_write(
            WriteStream::Owned(stream),
            &mut resp_pooled,
            &self.throttle,
            &kill_token,
            buffer_size,
          )
          .await
          else {
            break 'drive;
          };
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
          Bytes(BufResult<usize, PrimedVec>),
          Cancelled,
        }
        let read_end = match push_mailbox {
          Some(mailbox) => {
            // 读 future 存活于推送处理全程（绝不 drop 重建——半包字节
            // 会随 future 丢失）；无令牌形态以永不触发的哑令牌统一类型
            let token = kill_token.clone().unwrap_or_default();
            let mut read_fut = Box::pin(
              stream
                .read_shared(PrimedVec::from_primed(scratch, prime_key))
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
                  let Some(write_res) = pooled_write(
                    WriteStream::Shared(stream),
                    &mut resp_pooled,
                    &self.throttle,
                    &kill_token,
                    buffer_size,
                  )
                  .await
                  else {
                    break ReadEnd::Cancelled;
                  };
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
              stream.read(PrimedVec::from_primed(scratch, prime_key)),
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
        // into_parts 就地展开：代际键先读出，再消耗取缓冲（PrimedVec 单一实现）
        let prime_key = wrapped.prime_key();
        let scratch = wrapped.into_inner();
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

/// 写出端借用形态：独占（命令臂 `write_all`）/ 共享（推送臂 `write_all_shared`，
/// 读 future 挂起存活期唯一合法借用）。引用包装零成本
enum WriteStream<'a> {
  Owned(&'a mut ConnectionStream),
  Shared(&'a ConnectionStream),
}

/// 连接泵写出单点（命令臂 / 推送臂同构段收口）：节流进出 → 取池缓冲 →
/// 取消钩内在途写出 → 归还节流额度 → 缓冲清零复位回池基准水位。
///
/// 返回 `None` = 节流关闭或在途写出被取消（KILL/停机终止域，缓冲随 future
/// 失，compio 取消语义）；`Some(res)` = 写出终局，错误尾分派（断连 / 上抛）
/// 留调用方。
///
/// 归还臂注释对标 C# SendAndReset 分片复位——响应缓冲恒为配置规格不因大
/// 应答扩容驻留，不重新借出：重借会从池队列弹出闲置块、旧扩容块析构迁移至
/// 高级层级，突发大应答逐轮蚕食基准层级闲置配额致池空退化为堆分配抖动
#[inline]
async fn pooled_write(
  stream: WriteStream<'_>,
  resp_pooled: &mut PooledRefBuffer<'_>,
  throttle: &NetworkSenderThrottle,
  kill_token: &Option<CancelToken>,
  buffer_size: usize,
) -> Option<io::Result<()>> {
  if throttle.enter_send().await.is_err() {
    return None;
  }
  let payload = resp_pooled
    .take_buffer()
    .expect("pooled send buffer active");
  let write_outcome = match stream {
    WriteStream::Owned(stream) => killable(stream.write_all(payload), kill_token).await,
    WriteStream::Shared(stream) => killable(stream.write_all_shared(payload), kill_token).await,
  };
  throttle.exit_send();
  let BufResult(write_res, mut reclaimed) = match write_outcome {
    Err(Cancelled) => return None,
    Ok(pair) => pair,
  };
  reclaimed.clear();
  shrink_to_base(&mut reclaimed, buffer_size);
  resp_pooled.set_buffer(reclaimed);
  Some(write_res)
}

/// 出网 armed 闩单点（drive_loop 六处冲应答出口共用）
///
/// 应答字节经 take_output_into 出会话冲入 resp_pooled 后，内层循环重入消费
/// 解析流水线后续 AOF 无关命令（PING/ECHO 族）会把会话 wait_for_aof_blocking
/// 复位（复位判据 pending_output_len()==0，C# dcurr==head 含未出网字节故不存
/// 在此窗口）。本闩在每处冲出口以「出会话时点」读会话标记：resp_pooled 非空
/// 且标记为真即置 armed，出网臂按 armed || 会话字段等待，杜绝停泊-续跑轮漏等
/// 提交落盘。仅读标记、不新建第二套等待调用（wait_for_commit_async 仍出网臂单点）。
#[inline]
fn arm_aof_latch<C: MessageConsumerFace>(armed: &mut bool, resp_buf: &[u8], session: &C) {
  if !resp_buf.is_empty() && session.wait_for_aof_blocking() {
    *armed = true;
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

/// 挂起执行体三路竞速的收场形态（阻塞臂/慢臂/脚本臂共用）
enum RaceEnd<T> {
  /// 执行体完成：携带其输出与竞速期对端到站字节——调用方收场后经
  /// [`preserve_probe`] 一次并入（脚本臂窗口缓冲换出的换出壳交互见调用点；
  /// 入向记账已在竞速到达点就地完成）
  Resolved(T, Vec<u8>),
  /// KILL/停机广播或对端脱机：丢弃执行体（观察者随守卫注销），退出泵循环；
  /// 竞速期累积字节弃收（连接将死，与现形等价）
  Disposed,
}

/// 竞速单点：终止广播 × 挂起执行体 × 对端活性探测读三路 select（阻塞臂、慢臂
/// 与脚本臂共用，消除同形轮询/保全样板）。终止与执行体各建一次（超时计时与
/// 广播注册不随探测重建而重置），探测读每轮重建（上轮完成必须重新提交；
/// 竞速期累积达保全水位 PROBE_PRESERVE_WATERMARK 即停建，退化为两路 select，
/// 挂起窗驻留封顶——C# 内联阻塞窗字节滞留内核 SO_RCVBUF 的有界等价）。
///
/// 不收会话参数（调用方收场保全形态）：脚本臂执行体（resume_suspended_script
/// _fut）全程持会话可变借用，收 session 即 E0499 双重可变借用；阻塞/慢臂本可
/// 会话内保全，为三调用点单一样板统一收口——竞速期泵本不消费，字节并入时点
/// 后移至收场无观测差。对端到站字节竞速期本地累积（acc）、Resolved 胜出携带
/// 返回、Disposed 弃收；入向镜像记账不随后移——monitor_sample 是竞速期即可
/// 采样的外部观测面（票 zcode-r18-netin 到达即入账契约），字节到达点就地
/// add_net_bytes，仍先于下一读取段基线（收场并入先于读取段，before 基线含
/// 之，净增口径不双计）。
///
/// 双缓冲铁律：累积字节驻 acc 永不出借给在途读——执行体/终止胜出时在途读仍
/// Pending 即随竞速败侧 drop，读缓冲连内容一并随 future 失（compio 取消语
/// 义）；累积若借入在途读（单缓冲逐轮承接形）即确定性丢失，收场并入空缓冲，
/// 挂起窗到站流水线字节永无消费、客户端等应答挂死。在途读独占轮首空载的
/// read_buf，完成即抽干回 acc、空载续借下一轮。
///
/// 探测读用独立缓冲：绝不占会话接收缓冲——执行体胜出弃探测读时挂起 op 连缓冲一并
/// 取消，会话缓冲无恙（compio 取消语义，与读取消同口径）。
async fn probe_race<F>(
  stream: &mut ConnectionStream,
  entry: Option<&Arc<ConsumerEntry>>,
  resolve: F,
) -> io::Result<RaceEnd<F::Output>>
where
  F: Future,
{
  // 本地累积（永不出借给在途读；Resolved 携带返回）
  let mut acc = Vec::new();
  // 在途读缓冲（轮首恒空载，完成即抽干回 acc）
  let mut read_buf = Vec::with_capacity(MIN_READ_SPACE);
  let mut terminate_fut = pin!(wait_terminate(entry));
  let mut resolve_fut = pin!(resolve);
  loop {
    let mut probe_fut = pin!(stream.read(PrimedVec::new(take(&mut read_buf))));
    break match select(
      terminate_fut.as_mut(),
      select(resolve_fut.as_mut(), probe_fut.as_mut()),
    )
    .await
    {
      // KILL/停机广播胜出（应答弃写，执行体随竞速败侧丢弃，累积字节弃收）
      Either::Left(_) => Ok(RaceEnd::Disposed),
      // 执行体胜出：挤干探测读——已完成未取的结果取回抽干入累积；挂起中随
      // drop 取消，仅失轮首空载的 read_buf（compio 取消语义），累积无恙
      Either::Right((Either::Left((value, _)), _)) => {
        let mut cx = Context::from_waker(Waker::noop());
        if let Poll::Ready(BufResult(res, wrapped)) = probe_fut.as_mut().poll(&mut cx) {
          read_buf = wrapped.into_inner();
          match res {
            // 到达即入账（与保全臂同一就地模式）
            Ok(n) => {
              if let (Some(entry), true) = (entry, n > 0) {
                entry.add_net_bytes(n as u64, 0);
              }
              acc.append(&mut read_buf);
            }
            // 挤干臂读错误：本轮读缓冲内容无效，弃之不抽干
            Err(_) => read_buf.clear(),
          }
        }
        Ok(RaceEnd::Resolved(value, acc))
      }
      // 探测读胜出：对端脱机判定或活连接字节抽干入累积续等
      Either::Right((Either::Right((BufResult(read_res, wrapped), _)), _)) => {
        read_buf = wrapped.into_inner();
        // read_buf 轮首空载，长度即本轮到站字节数
        let arrived = read_buf.len();
        // 到达即入账：字节虽滞留本地累积未入会话缓冲，monitor_sample 采样的
        // 条目镜像在竞速期即须可见（外部观测面不等收场）
        if let (Some(entry), true) = (entry, arrived > 0) {
          entry.add_net_bytes(arrived as u64, 0);
        }
        acc.append(&mut read_buf);
        match read_res {
          // 对端 EOF 正常关闭（C# bytesTransferred == 0 判定）
          Ok(0) => Ok(RaceEnd::Disposed),
          // 活连接未达水位：竞速期到站的请求字节本地累积（C# 阻塞期字节滞留内核缓冲
          // 同语义），重建探测读续等执行体，收场时一次并入
          Ok(_) if acc.len() < PROBE_PRESERVE_WATERMARK => continue,
          // 保全水位门（票 task/ing/wnode-probe-preserve-unbounded-recv-buffer-oom）：
          // 竞速期本地累积达 PROBE_PRESERVE_WATERMARK 即停建探测读，循环退化为终止
          // 广播 × 执行体两路 select（ACL 停车臂同形样板）——挂起窗入向停止抽干，
          // 内核接收缓冲打满即 TCP 零窗背压，与 C# 内联阻塞窗逐点同构
          // （ListCommands.cs:283 网络线程内联 BlockingWait 零未决接收 +
          // TcpNetworkHandlerBase.cs:214 字节滞留内核 SO_RCVBUF 有界），单连接挂起窗
          // 驻留封顶。计数即 acc.len()，门与计数单份，严禁另立第二套；停建窗 FIN
          // 不可达系 C# 同形盲窗（脚本臂票裁定第 6 条在案），执行体 resolve 后消费段
          // 重见流尾照常收场，已累积字节收场一次保全不弃
          Ok(_) => match select(terminate_fut.as_mut(), resolve_fut.as_mut()).await {
            // KILL/停机广播胜出：丢弃执行体，累积弃收（与探测臂 Disposed 同口径）
            Either::Left(_) => Ok(RaceEnd::Disposed),
            // 执行体胜出：无在途探测读需挤干，累积照常携带收场
            Either::Right((value, _)) => Ok(RaceEnd::Resolved(value, acc)),
          },
          // 连接异常收场（C# SocketError != Success → Dispose）
          Err(e) if is_dead_conn(&e) => Ok(RaceEnd::Disposed),
          Err(e) => Err(e),
        }
      }
    };
  }
}

/// 竞速到站字节平移入会话接收缓冲（三臂竞速收场单点；空缓冲直接跳过）。
/// 入向镜像记账不在本点——字节到达点已在 [`probe_race`] 就地入账，此处纯
/// 字节并入。调用时点约束：脚本臂须在续跑 future 完成（脚本窗关闭、外层
/// 接收缓冲真身已还原）之后，否则直写的是换出空壳、字节随窗口关闭覆没
#[inline]
fn preserve_probe<C: MessageConsumerFace>(session: &mut C, probe_buf: &[u8]) {
  if probe_buf.is_empty() {
    return;
  }
  let mut scratch = session.take_recv_scratch();
  scratch.extend_from_slice(probe_buf);
  session.return_recv_scratch(scratch);
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
