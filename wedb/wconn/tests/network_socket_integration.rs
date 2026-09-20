//! 读写双泵真连接集成测试（归位自 wconn::network::pump）
//!
//! 对标 C# test/standalone/Garnet.test/NetworkTests.cs
//! 覆盖读泵接收缓冲单次池借出与写泵字节分片不丢流。

use std::{
  sync::{Arc, atomic::AtomicBool},
  time::Duration,
};

use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
  time::sleep,
};
use crossfire::mpsc;
use wbase::pool::{DEFAULT_MAX_POOL_SIZE, LimitedFixedBufferPool};
use wconn::{
  Error,
  network::{OutStream, READ_CHUNK, RECV_IDLE_PROBE, read_pump, write_pump},
  types::{CommandItem, PumpProgress, ReplyTx},
};

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
