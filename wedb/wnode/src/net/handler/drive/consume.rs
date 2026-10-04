//! 消费批派发循环（drive_loop 泵主循环本体：消费 → 写出 → 读取驱动序）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs`（Read → Process 循环序）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：握手段在
//! [`super::handshake`]，网络读取段在 [`super::read`]，本文件留消费批
//! 派发与写出驱动序（泵循环骨架）。

use std::{io, pin::pin, sync::Arc};

use compio::runtime::Cancelled;
use futures_util::future::{Either, select};
use log::{debug, error};
use wbase::pool::BufferKind;

use super::{
  super::NetworkHandler,
  PumpEnv,
  aof::arm_aof_latch,
  is_dead_conn, killable,
  race::{RaceEnd, preserve_probe, probe_race, wait_terminate},
  read::read_segment,
  write::{WriteStream, pooled_write},
};
use crate::{
  net::stream::ConnectionStream,
  traits::{MessageConsumerFace, SessionProviderFace},
};

impl<C: MessageConsumerFace> NetworkHandler<C> {
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
  pub(super) async fn drive_loop<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    stream: &mut ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // KILL/注销终止令牌（accept 侧预注册装配，C# handler.Start 前注册的
    // 对偶——握手期挂起读同样可被 CLIENT KILL / 停机排空秒断；哑桩形态
    // None 不挂取消）
    let kill_token = self.kill_token.clone();
    // send 借出基准规格（PR #2157 预算钳制施加面：C# BaseSendBufferSize =
    // budget.ClampSendBufferSize(configuredSendBufferSize)，连接建立读点取值；
    // 预算缺省即池 send 规格原值，随 network_buffer_size 配置单点取得）
    let buffer_size = self.buffer_pool.send_base_size();
    // 握手/读取段共用泵环境（散参聚合载体见 [`PumpEnv`]）
    let env = PumpEnv {
      session_provider: &session_provider,
      sender_id,
      kill_token: &kill_token,
      buffer_size,
    };

    // ── 握手段（分件见 [`Self::handshake`]）── false = 握手期收场（对端
    // 关闭 / 令牌取消 / EOF），泵循环就此退出
    if !self.handshake(stream, &env).await? {
      return Ok(());
    }

    // 消费驱动序（C# NetworkHandler.Read → Process 循环序的等价重排）：
    // 先消费缓冲中现有完整帧（含握手批迁移字节），再读取下一批网络字节
    // 池化发送缓冲（容量为池基准规格，连接生命周期内复用，RAII 自动归还
    // 句柄；零 Arc 开销借用）
    let mut resp_pooled = self.buffer_pool.get_ref_kind(buffer_size, BufferKind::Send);
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

        // ── 本轮写出段（缓冲复用；C# Send 的泵单点）──
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
          let Some(write_res) =
            pooled_write(WriteStream::Owned(stream), &mut resp_pooled, &kill_token).await
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

      // ── 网络读取段（下一批；读完回到循环头消费；分件见 [`read_segment`]）──
      // resp_pooled 持 buffer_pool 共享借用（借还锚定 handler 字段），本段
      // 不能整取 &mut self——按字段拆分借用（会话槽 / 注册条目 / 端点名）
      if !read_segment(
        &mut self.session,
        &self.consumer_entry,
        &self.remote_endpoint,
        stream,
        &mut resp_pooled,
        &env,
      )
      .await?
      {
        break 'drive;
      }
    }

    Ok(())
  }
}
