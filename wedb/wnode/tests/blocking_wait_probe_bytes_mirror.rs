//! 阻塞挂起期探测读保全字节计入 ConsumerEntry 入向镜像回归
//! （task/ing/zcode-r18-netin）
//!
//! 缺陷：阻塞三路竞速两处保全臂（阻塞结果胜出的挤干臂、探测读胜出的保全臂）
//! 把阻塞期违规到站的流水线请求字节 extend 进会话接收缓冲，但条目入向镜像的
//! 唯一喂点在轮末检查点 `add_net_bytes((net_in + handshake_net_in), written)`，
//! 而 `net_in` 只取网络读取段净增——下一批读取段的 before 基线已含保全字节，
//! 这批字节永不入账；`monitor_sample` 以条目镜像覆盖会话指标快照
//! （consumer_registry.rs monitor_sample 直读 `net_input_bytes`），监视器与
//! INFO 入向口径系统性漏计。同根次级缺口：`BlockEnd::Disposed` 提前
//! `break 'drive` 跳过检查点，最后一批入向字节同漏。
//!
//! 修复：保全臂就地 `entry.add_net_bytes(n, 0)`（与推送臂同一就地模式）；
//! 入向记账前移到字节到达点（握手迁移点、读取段净增就地入账），检查点收敛
//! 为纯出向 `add_net_bytes(0, written)`——任意提前退出路径不再漏计入向。
//!
//! 测试对标：C# 无镜像盲区（RespServerSession.cs:600 TryConsumeMessages 尾部
//! 对全部消费字节 `incr_total_net_input_bytes`，阻塞等待期字节滞留内核缓冲、
//! 唤醒后经标准接收环全部入账），按票面验证点构建真链路：BLPOP 挂起 +
//! 阻塞期流水线命令，断言条目镜像与 monitor_sample 入向字节等于实发字节；
//! CLIENT KILL 挂起连接时最后一批字节已在镜像。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use parking_lot::Mutex;
use wbase::pool::DEFAULT_BUFFER_SIZE;
use wnode::{
  GarnetServer, servers::consumer_registry::ConsumerRegistry, service::StorageSessionProvider,
};
use wnode_test::{SessionFactory, read_reply, send, send_cmd, session_factory};
use wtest_base::{resp_frame, test_store_config};

/// 两用例各起独立 compio 运行时与 HybridLog 实例，同进程并发互踩（SIGABRT），
/// 照 client_commands_tests 口径全局串行
static BLOCKING_MIRROR_TESTS_LOCK: Mutex<()> = Mutex::new(());

/// BLPOP 挂起确认余量（挂探测读后阻塞期字节才走保全臂）
const HANG_WAIT: Duration = Duration::from_millis(200);
/// 保全臂记账落定余量（同 pubsub 镜像票口径）
const MIRROR_WAIT: Duration = Duration::from_millis(100);
/// 条目注销归并余量
const DRAIN_WAIT: Duration = Duration::from_millis(80);

const QUIT_FRAME: &[u8] = b"*1\r\n$4\r\nQUIT\r\n";

/// 起单 worker 真存储服务器，返回消费者注册表句柄（对标
/// pubsub_push_net_bytes_mirror::spawn_server）
fn spawn_server() -> (
  GarnetServer<StorageSessionProvider<SessionFactory>>,
  SocketAddr,
  Arc<ConsumerRegistry>,
) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("blocking-net-bytes.db");
  let factory: SessionFactory = session_factory;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap(),
  );
  let registry = Arc::clone(&provider.registry);
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    DEFAULT_BUFFER_SIZE,
    8,
    provider,
  )
  .unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr, registry)
}

/// 期望字节数读完（累计，容忍 TCP 分段）
async fn read_until(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  let mut chunk = vec![0u8; 8192];
  while acc.len() < expect {
    let BufResult(res, ret) = stream.read(chunk).await;
    chunk = ret;
    let n = res.expect("对端提前关闭");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&chunk[..n]);
  }
}

/// BLPOP 挂起后阻塞期到站的流水线命令字节计入条目入向镜像：镜像与
/// monitor_sample 采出的 total_net_input_bytes 等于实发请求字节（修复前：
/// 保全臂字节漏记，入向恒为 BLPOP 帧字节）
#[test]
fn blocked_wait_probe_bytes_mirrored_into_entry_net_input() {
  let _lock = BLOCKING_MIRROR_TESTS_LOCK.lock();
  let (server, addr, registry) = spawn_server();

  // BLPOP 独占一批写出：挂起后到站的字节才走探测读保全臂
  let blpop = resp_frame(&[b"BLPOP", b"k", b"0"]);
  let get_a = resp_frame(&[b"GET", b"a"]);
  let ping = resp_frame(&[b"PING"]);
  let followups_len = get_a.len() + ping.len();
  // BLPOP k 唤醒应答（LPUSH k hello 后弹出）：*2 bulk(key) bulk(value)
  let blpop_reply: &[u8] = b"*2\r\n$1\r\nk\r\n$5\r\nhello\r\n";
  let expect_out = blpop_reply.len() + b"$-1\r\n".len() + b"+PONG\r\n".len();

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    send(&mut c1, &blpop).await.unwrap();
    sleep(HANG_WAIT).await;
    // 阻塞期流水线到站（探测读保全臂 extend 进会话缓冲并就地镜像）
    send(&mut c1, &get_a).await.unwrap();
    send(&mut c1, &ping).await.unwrap();
    sleep(MIRROR_WAIT).await;

    // 唤醒连接：LPUSH 供 BLPOP 弹出
    let mut waker = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut waker, &[b"LPUSH", b"k", b"hello"])
      .await
      .unwrap();
    let pushed = read_reply(&mut waker).await;
    assert_eq!(pushed.first(), Some(&b':'), "LPUSH 应答为整数");

    // c1 依序收 BLPOP 应答 + 流水线应答（GET a 未命中 $-1、PING +PONG）
    let mut acc = Vec::new();
    read_until(&mut c1, &mut acc, expect_out).await;
    assert_eq!(&acc[..blpop_reply.len()], blpop_reply);
    assert_eq!(&acc[blpop_reply.len()..], b"$-1\r\n+PONG\r\n");

    // 唤醒连接退场：采样仅剩 c1 条目
    send(&mut waker, QUIT_FRAME).await.unwrap();
    read_reply(&mut waker).await;
    drop(waker);
    sleep(DRAIN_WAIT).await;

    let sample = registry.monitor_sample();
    assert_eq!(sample.sessions.len(), 1, "仅剩 BLPOP 连接条目");
    let metrics = &sample.sessions[0].metrics;
    // 入向镜像 = BLPOP 帧 + 阻塞期流水线帧，严格等于实发请求字节
    assert_eq!(
      metrics.total_net_input_bytes,
      (blpop.len() + followups_len) as u64,
      "阻塞期保全字节必须计入条目入向镜像"
    );
    // 出向镜像 = BLPOP 应答 + 流水线应答实发字节
    assert_eq!(
      metrics.total_net_output_bytes, expect_out as u64,
      "唤醒后续消费应答计入出向镜像"
    );

    // c1 退场收口
    send(&mut c1, QUIT_FRAME).await.unwrap();
    read_reply(&mut c1).await;
    drop(c1);
    sleep(DRAIN_WAIT).await;
    assert_eq!(registry.connection_totals().2, 0, "全连接收场注销");
  });

  server.stop();
}

/// KILL 挂起连接：最后一批入向字节（阻塞期保全的 PING）在镜像中不丢
/// （修复前：保全字节漏记，且 KILL 经 BlockEnd::Disposed 提前 break 跳过
/// 轮末检查点，最后一批字节再无补记机会）
#[test]
fn kill_blocked_session_keeps_last_batch_bytes() {
  let _lock = BLOCKING_MIRROR_TESTS_LOCK.lock();
  let (server, addr, registry) = spawn_server();

  let blpop = resp_frame(&[b"BLPOP", b"k2", b"0"]);
  let ping = resp_frame(&[b"PING"]);

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let mut victim = TcpStream::connect(addr).await.unwrap();
    send(&mut victim, &blpop).await.unwrap();
    sleep(HANG_WAIT).await;
    // 阻塞期最后一批到站：保全臂就地镜像
    send(&mut victim, &ping).await.unwrap();
    sleep(MIRROR_WAIT).await;

    // KILL 前采样：victim 条目入向 = BLPOP 帧 + 保全的 PING 帧
    let sample = registry.monitor_sample();
    assert_eq!(sample.sessions.len(), 1, "仅挂起连接在场");
    assert_eq!(
      sample.sessions[0].metrics.total_net_input_bytes,
      (blpop.len() + ping.len()) as u64,
      "KILL 前最后一批字节必须已在入向镜像"
    );

    // KILL：终止广播胜出，BlockEnd::Disposed 提前 break（检查点跳过）
    let mut killer = TcpStream::connect(addr).await.unwrap();
    let victim_addr = victim.local_addr().unwrap().to_string();
    send_cmd(
      &mut killer,
      &[b"CLIENT", b"KILL", b"ADDR", victim_addr.as_bytes()],
    )
    .await
    .unwrap();
    let killed = read_reply(&mut killer).await;
    assert_eq!(killed, b":1\r\n", "KILL 命中挂起连接");

    // victim 被秒断（不读任何应答——阻塞等待弃答语义）
    let closed = timeout(Duration::from_millis(500), read_reply(&mut victim)).await;
    assert!(
      closed.is_err() || closed.unwrap().is_empty(),
      "挂起连接应被 KILL 秒断"
    );
    drop(victim);

    // 条目注销收场：采样仅剩 killer
    sleep(DRAIN_WAIT).await;
    let sample = registry.monitor_sample();
    assert_eq!(sample.sessions.len(), 1, "挂起条目已注销");

    killer.write_all(QUIT_FRAME.to_vec()).await.unwrap();
    read_reply(&mut killer).await;
    drop(killer);
    sleep(DRAIN_WAIT).await;
    assert_eq!(registry.connection_totals().2, 0, "全连接收场注销");
  });

  server.stop();
}
