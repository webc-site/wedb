//! 竞速探测：挂起执行体三路竞速（终止广播 × 执行体 × 对端活性探测读）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/TcpNetworkHandlerBase.cs:214`（bytesTransferred == 0
//!   内核异步事件守护的对偶承接）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：本件为 drive_loop
//! 阻塞臂 / 慢臂 / 脚本臂的竞速阶段分体。

use std::{
  future::{Future, pending},
  io,
  mem::take,
  pin::pin,
  sync::Arc,
  task::{Context, Poll, Waker},
};

use compio::BufResult;
use futures_util::future::{Either, select};
use wbase::primed::PrimedVec;

use super::{super::buffer::MIN_READ_SPACE, is_dead_conn};
use crate::{
  net::stream::ConnectionStream, resp::resp_server_session::PROBE_PRESERVE_WATERMARK,
  servers::consumer_registry::ConsumerEntry, traits::MessageConsumerFace,
};

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
pub(super) async fn wait_terminate(entry: Option<&Arc<ConsumerEntry>>) {
  if let Some(entry) = entry {
    entry.wait_terminate().await;
  } else {
    pending::<()>().await;
  }
}

/// 挂起执行体三路竞速的收场形态（阻塞臂/慢臂/脚本臂共用）
pub(super) enum RaceEnd<T> {
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
pub(super) async fn probe_race<F>(
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
pub(super) fn preserve_probe<C: MessageConsumerFace>(session: &mut C, probe_buf: &[u8]) {
  if probe_buf.is_empty() {
    return;
  }
  let mut scratch = session.take_recv_scratch();
  scratch.extend_from_slice(probe_buf);
  session.return_recv_scratch(scratch);
}
