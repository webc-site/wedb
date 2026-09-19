//! 客户端读写双泵：写泵批量刷帧 + 读泵独立认领应答
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/ClientTcpNetworkSender.cs`（发送流刷 socket）
//! - `libs/client/GarnetClientTcpNetworkHandler.cs`
//! - `libs/client/GarnetClientProcessReplies.cs`（读循环）

use std::{
  collections::VecDeque,
  future::{Future, poll_fn},
  io,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::Poll,
  time::Duration,
};

use compio::{buf::BufResult, runtime::spawn, time::timeout};
use crossfire::{AsyncRx, MAsyncTx, TrySendError, mpsc};
use wbase::pool::{DEFAULT_MAX_POOL_SIZE, LimitedFixedBufferPool};

use super::{
  replies::{dispatch_replies, orphan_error_reply},
  stream::{OutStream, ReadHalf, WriteHalf},
};
use crate::{
  Error, Result,
  types::{CommandItem, MAX_UNFLUSHED_SEND_BYTES, PumpProgress, ReplyTx},
};

/// 读泵接收缓冲的池借出下界（实际块规格为池的缓冲尺寸，对标 C#
/// `NetworkBufferSettings.initialReceiveBufferSize` 的 Get 入参位）
const READ_CHUNK: usize = 16 * 1024;
/// 应答累积缓冲初始容量
const READ_BUF_CAP: usize = 8 * 1024;
/// 写泵单批命令等待上限：到期轮询读泵存活（断链对外可见的感知粒度）
const RECV_IDLE_PROBE: Duration = Duration::from_millis(250);

/// 读泵接收缓冲池解析单点（客户端与会话共用，无第二处口径）
///
/// 对应 C# GarnetClientSession 构造里的
/// `networkPool ?? networkBufferSettings.CreateBufferPool(...)`：调用方注入
/// 优先（复制/迁移链注入 `ReplicationManager` / `MigrationManager` 持有的池，
/// 与 C# 同源同池），未注入即本客户端自建一份，块规格为 [`READ_CHUNK`]。
/// 两态都走 [`LimitedFixedBufferPool`] 这一个池实现（全仓唯一缓冲取还机制，
/// 服务端读侧 wnode/src/net/handler/drive.rs 同源）。
pub(crate) fn resolve_network_pool(
  pool: Option<Arc<LimitedFixedBufferPool>>,
) -> Arc<LimitedFixedBufferPool> {
  pool.unwrap_or_else(|| LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_POOL_SIZE))
}

/// 网络循环主入口：拆分读写两半，spawn 写泵，本任务跑读泵直至退出
///
/// 在 garnet 中的相对路径:
/// - `libs/client/ClientTcpNetworkSender.cs`
/// - `libs/client/GarnetClientProcessReplies.cs`
///
/// `progress` 在位（客户端超时旋钮开启）时泵侧同步推进三进度计数，供
/// [`GarnetClient`](crate::client::GarnetClient) 的超时检查任务比较；
/// `gate` 为在途准入闸值：泵内在途队列按此定容（对标 C# tcsArray 按
/// maxOutstandingTasks 定长分配），会话侧调用方传 [`CHANNEL_CAP`](crate::types::CHANNEL_CAP)；
/// `network_pool` 为读泵接收缓冲的取还单点（对标 C# 客户端构造形参
/// `networkPool`，服务端读侧与客户端泵共用同一池实现，无第二套池机制）
pub(crate) async fn network_loop(
  stream: OutStream,
  rx: AsyncRx<mpsc::Array<CommandItem>>,
  progress: Option<Arc<PumpProgress>>,
  gate: usize,
  network_pool: Arc<LimitedFixedBufferPool>,
) -> Result<()> {
  let (read_half, write_half) = stream.split();
  // 在途命令队列：写泵按帧序推入，读泵按序认领（FIFO 与应答序严格对齐）；
  // 容量即准入闸（在途未回收数 ≤ gate，满即写泵挂起、背压传导至调用方）
  let (in_flight_tx, in_flight_rx) = mpsc::bounded_async::<CommandItem>(gate);
  // 读泵存活标志：读泵退出后写泵经限时收命令的超时窗感知，收场退出
  let reader_alive = Arc::new(AtomicBool::new(true));
  // 写泵计划内退出标志（调用方全部退场触发 shutdown 收场）：读泵随后读到的
  // EOF 属计划内关闭，不作为断链错误上抛
  let writer_done = Arc::new(AtomicBool::new(false));

  let reader_alive_w = Arc::clone(&reader_alive);
  let writer_done_w = Arc::clone(&writer_done);
  let progress_w = progress.clone();
  spawn(async move {
    if let Err(e) = write_pump(
      write_half,
      rx,
      in_flight_tx,
      reader_alive_w,
      writer_done_w,
      progress_w,
      MAX_UNFLUSHED_SEND_BYTES,
    )
    .await
    {
      log::error!("客户端写泵退出: {e}");
    }
  })
  .detach();

  let res = read_pump(read_half, in_flight_rx, progress.as_deref(), &network_pool).await;
  reader_alive.store(false, Ordering::Release);
  // 网络循环收场：置退休标志使超时检查任务退场（不空转残留）
  if let Some(p) = &progress {
    p.retire();
  }
  if writer_done.load(Ordering::Acquire) {
    return Ok(());
  }
  res
}

/// 写泵：限时收命令 → 非阻塞清空通道积压 → 带应答项先按帧序入在途队列再批量刷出
///
/// 在 garnet 中的相对路径: libs/client/ClientTcpNetworkSender.cs:ClientTcpNetworkSender
///
/// 先入队后写出，读泵认领顺序才与帧序一致；发出即忘帧不入队，
/// 写出即完成（对标 GarnetClientSession.ExecuteClusterAppendLog 无 tcs 路径）。
///
/// 超时窗兼作读泵存活轮询：读泵退出（断链/协议错）后写泵在此粒度内收场并
/// 丢弃 rx，`is_connected` 随之翻假。命令通道断连（调用方全部退场）时置
/// 计划内标志并 shutdown 写半，驱动读泵 EOF 收场，fd 两半全 drop 关闭连接。
///
/// `flush_threshold_bytes` 为单次在途拼批的字节分片阈值：攒批过程中 out_buf 达
/// 阈值即先刷出、清空后继续收批，使慢副本 + 大记录场景下写泵持有的在途缓冲有界
///（对标 C# 页满即刷出，NetworkWriter 未刷出字节钉在页环形缓冲内）。生产入口恒取
/// [`MAX_UNFLUSHED_SEND_BYTES`]，单测可注入小值以廉价输入触发分片路径。
async fn write_pump(
  mut stream: WriteHalf,
  rx: AsyncRx<mpsc::Array<CommandItem>>,
  in_flight_tx: MAsyncTx<mpsc::Array<CommandItem>>,
  reader_alive: Arc<AtomicBool>,
  writer_done: Arc<AtomicBool>,
  progress: Option<Arc<PumpProgress>>,
  flush_threshold_bytes: usize,
) -> Result<()> {
  // 写出缓冲跨批复用，避免每轮分配清零
  let mut out_buf = Vec::new();
  loop {
    match timeout(RECV_IDLE_PROBE, rx.recv()).await {
      // 新命令就绪：随后 try_recv 清空通道积压（批量写出摊薄 syscall 次数）
      Ok(Ok(first)) => {
        let mut pending = Some(first);
        while let Some(cur) = pending.take() {
          out_buf.extend_from_slice(&cur.frame);
          // 带应答帧先入队在途通道（读泵按此队列认领），再统一写出；在途满
          // 即闸生效：先把已积累帧刷出再挂起等待——挂起点必须保活应答回收链
          //（帧到对端、应答可回、在途槽位可腾），否则批量中途挂起会令帧滞留
          // 本地、应答永不到来、闸位永不腾空（死锁）
          if !matches!(cur.resp_tx, ReplyTx::None) {
            match in_flight_tx.try_send(cur) {
              Ok(()) => {}
              Err(TrySendError::Full(cur)) => {
                out_buf = flush_write_buf(&mut stream, out_buf, &progress).await?;
                if in_flight_tx.send(cur).await.is_err() {
                  // 读泵已退出：在途与未写出项的 oneshot 随通道销毁回传断连；
                  // shutdown 写半兜底唤醒读泵收场
                  let _ = stream.shutdown().await;
                  return Err(Error::ReadPumpExited);
                }
              }
              Err(TrySendError::Disconnected(_)) => {
                let _ = stream.shutdown().await;
                return Err(Error::ReadPumpExited);
              }
            }
            if let Some(p) = &progress {
              p.record_enqueued();
            }
          }
          // 字节维度分片：拼批攒够阈值即先刷一段，使慢副本 + 大记录场景下写泵持有
          // 的在途缓冲有界（对标 C# NetworkWriter 页满即刷出、未刷出字节钉在页内）
          if out_buf.len() >= flush_threshold_bytes {
            out_buf = flush_write_buf(&mut stream, out_buf, &progress).await?;
          }
          pending = rx.try_recv().ok();
        }
        // 余量刷出：本轮批次收尾，不足阈值的残留一并写出
        out_buf = flush_write_buf(&mut stream, out_buf, &progress).await?;
      }
      // 调用方全部退场：shutdown 写半驱动读泵 EOF 收场（对端已断时失败可忽略）
      Ok(Err(_)) => {
        writer_done.store(true, Ordering::Release);
        let _ = stream.shutdown().await;
        return Ok(());
      }
      // 超时窗存活轮询：读泵已退出即收场（rx 随之丢弃，断连对外可见）
      Err(_) if !reader_alive.load(Ordering::Acquire) => return Ok(()),
      Err(_) => {}
    }
  }
}

/// 写泵刷出缓冲单点：write_all 落 socket → 记进度 → 清空回接复用（写泵三处
/// 刷出点共用，杜绝重复）；失败即 shutdown 兜底驱动读泵收场并上抛
async fn flush_write_buf(
  stream: &mut WriteHalf,
  out_buf: Vec<u8>,
  progress: &Option<Arc<PumpProgress>>,
) -> Result<Vec<u8>> {
  let BufResult(res, mut buf) = stream.write_all(out_buf).await;
  if let Err(e) = res {
    let _ = stream.shutdown().await;
    return Err(e.into());
  }
  if let Some(p) = progress {
    p.record_sent(buf.len());
  }
  buf.clear();
  Ok(buf)
}

/// 读泵：驱动 ProcessReplies 的读循环
///
/// 独立自套接字读应答，在途队列补位后游标解析认领
///
/// 缓冲与读请求形态（对标 C# 客户端读侧单形态，无自造轮询）：接收缓冲经
/// [`wbase::pool::LimitedFixedBufferPool`] 于泵启动时**一次借出**、退出时 RAII
/// 归池（C# `TcpNetworkHandlerBase.cs:AllocateNetworkReceiveBuffer` 构造期
/// `networkPool.Get` → `Dispose` 归还，`HandleReceiveWithoutTLS` 携同一
/// `SocketAsyncEventArgs` 跨收包复用）；挂起的读请求同样**常驻存活**——
/// compio 读以所有权换完成回调，future 一析构缓冲即随弃，故本泵不设定时
/// 探测窗，唤醒一律事件驱动：
/// - 常驻读请求：对端回数据 / 断链 EOF / 写泵 shutdown 收场即醒；
/// - 滞留态（缓冲有未认领残段且无在途项：应答先于命令到达的合包残段）——
///   并挂在途通道收新项，命令入队立即醒、零延迟认领滞留应答（此态绝不
///   无限阻塞，防止滞留应答等死尚未入队的新命令）；
/// - 超时监控在位——并挂 [`PumpProgress`] 判成事件，检查任务判成即醒
///   （在途命令的收场通道；挂起前注册监听 + 注册后复查标志，消除丢唤醒窗口）。
async fn read_pump(
  stream: ReadHalf,
  in_flight_rx: AsyncRx<mpsc::Array<CommandItem>>,
  progress: Option<&PumpProgress>,
  network_pool: &LimitedFixedBufferPool,
) -> Result<()> {
  // 读流句柄随读请求转移（落定即外还，见 [`ReadHalf::read_owned`]）
  let mut stream = Some(stream);
  let mut queue: VecDeque<CommandItem> = VecDeque::new();
  let mut read_buf: Vec<u8> = Vec::with_capacity(READ_BUF_CAP);
  // 连接级单次池借出：整条连接生命周期内恒为这一块
  let mut pooled = network_pool.get_ref(READ_CHUNK);
  // 常驻读请求：None = 上一读已落定、待重投
  let mut read_fut: Option<ResidentRead> = None;
  // 在途通道断连（写泵退场）后不再等命令，避免每轮立即 Err 的空转
  let mut writer_gone = false;

  loop {
    dispatch_replies(&mut queue, &in_flight_rx, &mut read_buf, progress)?;

    // 超时判成感知：检查任务判成（在途无进展）即收场退出，在途 oneshot 随
    // 通道销毁断连，roundtrip 按判成标志结算 Error::Timeout
    if progress.is_some_and(|p| p.is_timed_out()) {
      return Err(Error::Timeout);
    }

    // 滞留错误应答感知：队列空时 read_buf 残留完整 `-ERR` 行 = fire-and-forget
    // 记录帧（APPENDLOG）已被写出即忘，服务端拒收回写无人认领——判流失效
    // 断连（C# 连接异常等价，驱动主端剔除副本转入重同步，防 shipped_watermark
    // 静默推进扩大主从分歧）
    if queue.is_empty()
      && let Some(err) = orphan_error_reply(&read_buf)
    {
      return Err(Error::Server(err));
    }

    // 重投读请求（仅上一读落定、缓冲已回池句柄时）：一条挂起读覆盖其后全部
    // 等待轮次，唤醒源非「读完成」时绝不弃置
    if read_fut.is_none() {
      let s = stream.take().expect("读流句柄随读请求转移，落定即外还");
      let chunk = pooled.take_buffer().expect("池化接收缓冲在位");
      read_fut = Some(Box::pin(ReadHalf::read_owned(s, chunk)));
    }

    // 三路竞速唤醒（读请求全程存活）
    let wake = {
      let stalled = queue.is_empty() && !read_buf.is_empty() && !writer_gone;
      let mut cmd_fut = stalled.then(|| Box::pin(in_flight_rx.recv()));
      let mut listener = progress.map(|p| Box::pin(p.listen_timeout()));
      poll_fn(|cx| {
        if let Poll::Ready(done) = read_fut.as_mut().expect("常驻读请求在位").as_mut().poll(cx)
        {
          return Poll::Ready(Wake::Read(done));
        }
        if let Some(f) = cmd_fut.as_mut() {
          match f.as_mut().poll(cx) {
            Poll::Ready(Ok(item)) => return Poll::Ready(Wake::Command(item)),
            // 在途通道断连：本轮起不再等命令（读侧自有 EOF 收场）
            Poll::Ready(Err(_)) => return Poll::Ready(Wake::WriterGone),
            Poll::Pending => {}
          }
        }
        if let Some(lis) = listener.as_mut() {
          if lis.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Wake::TimedOut);
          }
          // 注册后双检：判成可能先于本行完成（粘滞标志，唤醒丢失兜底）
          if progress.is_some_and(|p| p.is_timed_out()) {
            return Poll::Ready(Wake::TimedOut);
          }
        }
        Poll::Pending
      })
      .await
    };

    match wake {
      Wake::Read((s, BufResult(read_res, back))) => {
        stream = Some(s);
        read_fut = None;
        // 读得字节先并入累积缓冲，缓冲随即交回池句柄（裸 Vec 覆盖语义：
        // 下一读自缓冲头写入，残余不驻留此块）
        if let Ok(n) = &read_res {
          read_buf.extend_from_slice(&back[..*n]);
        }
        pooled.set_buffer(back);
        match read_res {
          Ok(0) => return Err(Error::Eof),
          Ok(_) => {}
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(Error::Eof),
          Err(e) => return Err(e.into()),
        }
      }
      // 新命令入队：回循环顶就地认领滞留应答
      Wake::Command(item) => queue.push_back(item),
      // 写泵已退场：转纯读等 EOF / 残余应答收场
      Wake::WriterGone => writer_gone = true,
      // 判成事件：回循环顶复查标志即按超时收场
      Wake::TimedOut => {}
    }
  }
}

/// 读请求落定输出：流句柄与读结果（所有权外还）
type ReadDone = (ReadHalf, BufResult<usize, Vec<u8>>);

/// 常驻读请求：流与缓冲所有权自持，跨等待轮次存活
type ResidentRead = Pin<Box<dyn Future<Output = ReadDone>>>;

/// 读泵三路竞速的唤醒源
enum Wake {
  /// 读完成（流与缓冲随之外还）
  Read(ReadDone),
  /// 滞留态新命令入队
  Command(CommandItem),
  /// 在途通道断连（写泵退场）
  WriterGone,
  /// 超时判成事件
  TimedOut,
}

#[cfg(test)]
mod tests {
  use compio::{
    io::AsyncRead,
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    time::sleep,
  };

  use super::*;

  /// 判据二回归（稳态零新分配）：读泵在「开启超时监控 + 零字节空闲链路」的稳态下，
  /// 接收缓冲仅于泵启动时一次池借出、跨全部等待轮次常驻复用，绝不因超窗换块。
  ///
  /// 消掉的旧病形态：`progress.is_some()` 时每 250ms 探测窗 `timeout(read)` 超时即
  /// `vec![0u8; READ_CHUNK]` 重投，`allocated_count` 随轮数线性增长；而取消弃读又把
  /// 「丢一块」变「泄漏一次借用」，`borrowed_count` 只增不减。现形态为常驻读 + 单次
  /// 借出 + RAII 归池，本用例以池计数坐实两点：① 空闲跨 `IDLE_PROBE_ROUNDS` 个探测窗
  /// 后 `allocated_count` 恒为 1（不随轮数增长）；② 连接收场后 `borrowed_count` 回基线
  /// 0、唯一接收块归池（`free_count` 为 1），无一次性泄漏。
  #[test]
  fn idle_pump_borrows_read_buffer_once() {
    // 空闲稳态跨 IDLE_PROBE_ROUNDS 个 250ms 探测窗（旧形态此间会换 IDLE_PROBE_ROUNDS 块）
    const IDLE_PROBE_ROUNDS: u32 = 8;

    Runtime::new().unwrap().block_on(async {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();

      // 假端点：收下连接后整窗静默（零字节空闲链路），到期 drop 令读侧得到 EOF
      spawn(async move {
        let (sock, _peer) = listener.accept().await.unwrap();
        sleep(RECV_IDLE_PROBE * IDLE_PROBE_ROUNDS).await;
        drop(sock);
      })
      .detach();

      let client = TcpStream::connect(addr).await.unwrap();
      let (read_half, _write_half) = OutStream::Tcp(client).split();
      // 读泵接收缓冲池：块规格取 READ_CHUNK，与泵内 get_ref 同界，故唯一块可归池复用
      let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_POOL_SIZE);
      let pump_pool = Arc::clone(&pool);
      // 超时监控在位（选定旧病分支）；全程不判成，令读泵挂于常驻读、绝不定时重投
      let progress = PumpProgress::new();
      // 在途通道空转保连（无命令流，仅维持链路存活）
      let (_in_flight_tx, in_flight_rx) =
        mpsc::bounded_async::<CommandItem>(IDLE_PROBE_ROUNDS as usize);

      let pump =
        spawn(async move { read_pump(read_half, in_flight_rx, Some(&progress), &pump_pool).await });

      // 有界轮询至读泵完成首次池借出并挂上常驻读（规避重载机器的调度抖动）
      let mut parked = false;
      for _ in 0..400 {
        if pool.borrowed_count() == 1 {
          parked = true;
          break;
        }
        sleep(Duration::from_millis(5)).await;
      }
      assert!(parked, "读泵未进入稳态挂读（池借出未就绪）");
      assert_eq!(pool.allocated_count(), 1, "单次借出仅一块，未随轮重分配");

      // 等端点收场（EOF）令读泵退出，pooled 句柄随函数返回 RAII 归池
      let res = pump.await.expect("读泵任务不应 panic");
      assert!(matches!(res, Err(Error::Eof)), "空闲链路收场应为 EOF");

      // 判据一：借出计数回基线（无「超时即丢块」式一次性泄漏）
      assert_eq!(pool.borrowed_count(), 0, "退出后借用必须归零，不得只增不减");
      // 判据二：跨 IDLE_PROBE_ROUNDS 个探测窗仍恰好一块，未随轮数增长（稳态零新分配）
      assert_eq!(pool.allocated_count(), 1, "空闲稳态不得再分配新块");
      // 唯一接收块经 set_buffer + Drop 归池，可供后续连接复用
      assert_eq!(pool.free_count(), 1, "接收缓冲必须归池复用");
    });
  }

  /// 写泵字节分片回归：注入远小于帧长的阈值，令拼批循环每帧即触发一次中途刷出，
  /// 验证分片后对端读到的总字节恰等于各帧编码长度之和（不丢帧、不少字节、不乱序）
  #[test]
  fn write_pump_byte_fragmentation_preserves_stream() {
    Runtime::new().unwrap().block_on(async {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();

      let accept = spawn(async move { listener.accept().await.unwrap().0 });
      let client = TcpStream::connect(addr).await.unwrap();
      let (_read_half, write_half) = OutStream::Tcp(client).split();
      let mut peer = accept.await.unwrap();

      let (tx, rx) = mpsc::bounded_async::<CommandItem>(8);
      // 在途队列仅需存活（全为 fire-and-forget 帧不入队）；接收端持有防过早断连
      let (in_flight_tx, _in_flight_rx) = mpsc::bounded_async::<CommandItem>(8);
      let reader_alive = Arc::new(AtomicBool::new(true));
      let writer_done = Arc::new(AtomicBool::new(false));

      // 三帧各 > 64B 阈值：每帧累积即触发一次分片刷出
      let mut expected = 0usize;
      for tag in [1u8, 2, 3] {
        let item = CommandItem::new_bytes(&[&[tag; 128][..]], ReplyTx::None);
        expected += item.frame.len();
        tx.send(item).await.unwrap();
      }
      drop(tx); // 调用方全部退场 → 写泵收场（shutdown 写半驱动对端 EOF）

      spawn(write_pump(
        write_half,
        rx,
        in_flight_tx,
        reader_alive,
        writer_done,
        None,
        64,
      ))
      .detach();

      let mut acc = Vec::new();
      let mut buf = vec![0u8; 4096];
      loop {
        let BufResult(res, next) = peer.read(buf).await;
        buf = next;
        match res.unwrap() {
          0 => break,
          n => acc.extend_from_slice(&buf[..n]),
        }
      }
      assert_eq!(acc.len(), expected, "分片刷出不得丢帧或丢字节");
    });
  }
}
