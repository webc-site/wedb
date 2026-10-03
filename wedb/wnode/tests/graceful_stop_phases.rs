//! 停机三阶段时序锁（票 zcode-r167c-gracefulstop 案一/案二/案三）
//!
//! 对标 C# GarnetServer.cs:InternalDispose 三阶段时序（Phase 1 servers[i].
//! Close 先关监听阻断新连接 → Phase 2 排空活跃连接 → Phase 3
//! Provider.Dispose 级联后才 subscribeBroker.Dispose）与
//! test/standalone/Garnet.test/NetworkTests.cs 的连接排空观测面。
//! 全部为真实服务器 + 真实 TCP 客户端 + 真实 RESP 命令，零 mock；停机链
//! 内部时序经收敛窗口探针（stop 链主线程在清理收口内受控阻塞，worker
//! 运行时按设计仍存活排空）外化为端点可观测断言：
//! - 案一：进入向量清理收敛时协调器已置停机位（Phase 1 前置），收敛窗口内
//!   新连接即刻被拒，旧时序（清理先于停监听、窗口内监听全开）必红；
//! - 案二：dispose_pubsub 位于 worker join 之后（收口瞬间在途连接快照已
//!   归零、在途 PUBLISH 已投递），旧时序（排空前清表）必红；
//! - 案三：有界 join 原语对挂死线程在确定性上界内以超时臂收口（裸
//!   join 永不返回），正常/panic 臂分形可辨。

use std::{
  io::{Read, Write},
  net::TcpStream,
  panic::{set_hook, take_hook},
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, Receiver, SyncSender},
  },
  thread,
  time::{Duration, Instant},
};

use tempfile::tempdir;
use wnode::{
  RespSessionConsumer, SessionProviderFace, ShutdownCoordinator, WireFormat,
  server::join_worker_bounded, servers::consumer_registry::ConsumerRegistry,
  service::StorageSessionProvider,
};
use wnode_test::{SessionFactory, session_factory, start_server};
use wtest_base::{resp_frame, test_store_config};

type Inner = StorageSessionProvider<SessionFactory>;

/// 停机链时序探针
struct StopProbe {
  /// 案一窗口开关：置真时向量清理收敛入口阻塞待放行（仅案一用例启用，
  /// 其余用例 stop 链直通）
  gate: AtomicBool,
  /// 清理收敛入口通告（stop 链 → 测试线程）
  entered: SyncSender<()>,
  /// 窗口放行（测试线程 → stop 链）；std Receiver 非 Sync，入锁承接
  release: Mutex<Receiver<()>>,
  /// 向量清理收敛是否被调用
  cleanup_called: AtomicBool,
  /// 内层清理通道收敛结果（false=积压未全收敛，存在丢弃/孤儿泄漏面）
  cleanup_converged: AtomicBool,
  /// pubsub 收口瞬间在途连接快照（usize::MAX 哨兵=未被调用）
  pubsub_active_snapshot: AtomicUsize,
  /// pubsub 收口时向量清理收敛是否已先行发生
  pubsub_after_cleanup: AtomicBool,
  /// 服务器停机协调器（start 后注入，窗口内外查停机位真值）
  coordinator: Mutex<Option<ShutdownCoordinator>>,
}

/// 停机链时序探针装饰提供者：全链真实委托，仅在清理/pubsub 收口点挂观测
/// 窗（AOF 未点亮，wait_for_commit_async 走 trait 默认与内层 None 形态同形）
struct ProbeProvider {
  inner: Arc<Inner>,
  probe: Arc<StopProbe>,
}

impl SessionProviderFace for ProbeProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    self.inner.get_session(wire_format, network_sender_id)
  }

  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    self.inner.consumer_registry()
  }

  fn reset_revivification_stats(&self) {
    self.inner.reset_revivification_stats();
  }

  fn dispose_vector_cleanup(&self) -> bool {
    self.probe.cleanup_called.store(true, Ordering::Release);
    if self.probe.gate.load(Ordering::Acquire) {
      // 清理收口入口受控窗口（stop 链挂起于此，worker 按设计继续排空）：
      // 将 Phase 1 已停监听的事实外化为端点可观测断言，窗口不开积压以保持
      // 与运行时析构时序解耦（内层收口仍真实委托）
      let _ = self.probe.entered.try_send(());
      let _ = self.probe.release.lock().unwrap().recv();
    }
    let converged = self.inner.dispose_vector_cleanup();
    self
      .probe
      .cleanup_converged
      .store(converged, Ordering::Release);
    converged
  }

  fn dispose_item_broker(&self) {
    self.inner.dispose_item_broker();
  }

  fn dispose_pubsub(&self) {
    let active = self
      .inner
      .consumer_registry()
      .map(|r| r.active_consumers().len())
      .unwrap_or(usize::MAX);
    self
      .probe
      .pubsub_active_snapshot
      .store(active, Ordering::Release);
    self.probe.pubsub_after_cleanup.store(
      self.probe.cleanup_called.load(Ordering::Acquire),
      Ordering::Release,
    );
    self.inner.dispose_pubsub();
  }

  fn dispose_lua_timeout(&self) {
    self.inner.dispose_lua_timeout();
  }

  fn dispose_range_index(&self) {
    self.inner.dispose_range_index();
  }
}

/// 限时读一轮应答（超时/断连回已收字节）
fn read_some(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
  stream
    .set_read_timeout(Some(timeout))
    .expect("read timeout");
  let mut buf = [0u8; 512];
  match stream.read(&mut buf) {
    Ok(n) => buf[..n].to_vec(),
    Err(_) => Vec::new(),
  }
}

/// 读到 EOF/超时，收全部残余字节
fn drain_to_end(stream: &mut TcpStream) -> Vec<u8> {
  stream
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("read timeout");
  let mut out = Vec::new();
  let mut buf = [0u8; 256];
  loop {
    match stream.read(&mut buf) {
      Ok(0) => break,
      Ok(n) => out.extend_from_slice(&buf[..n]),
      Err(_) => break,
    }
  }
  out
}

fn new_probe(gate: bool) -> (Arc<StopProbe>, Receiver<()>, mpsc::Sender<()>) {
  let (entered_tx, entered_rx) = mpsc::sync_channel::<()>(1);
  let (release_tx, release_rx) = mpsc::channel::<()>();
  (
    Arc::new(StopProbe {
      gate: AtomicBool::new(gate),
      entered: entered_tx,
      release: Mutex::new(release_rx),
      cleanup_called: AtomicBool::new(false),
      cleanup_converged: AtomicBool::new(false),
      pubsub_active_snapshot: AtomicUsize::new(usize::MAX),
      pubsub_after_cleanup: AtomicBool::new(false),
      coordinator: Mutex::new(None),
    }),
    entered_rx,
    release_tx,
  )
}

/// 案一时序锁：协调器停监听（Phase 1）必须先于向量清理收口；收口入口
/// 窗口内新连接即刻被拒、旧连接按序排空。窗口不设向量积压（清理协程未
/// 拉起、内层收口即刻收敛）——时序断言与机器负载解耦，杜绝运行析构竞态
#[test]
fn stop_phase1_blocks_new_connections_before_vector_cleanup_convergence() {
  let dir = tempdir().expect("tempdir");
  let (probe, entered_rx, release_tx) = new_probe(true);
  let inner = Arc::new(
    Inner::open_with_config(
      test_store_config(),
      dir.path().join("graceful-p1.db"),
      session_factory as SessionFactory,
    )
    .expect("open provider"),
  );
  let provider = Arc::new(ProbeProvider {
    inner,
    probe: probe.clone(),
  });
  let (server, addr) = start_server(Arc::clone(&provider));
  *probe.coordinator.lock().unwrap() = Some(server.shutdown_coordinator().clone());

  // 真实连接 + 真实存储写读：SET/PING 经会话链落盘应答，证明在途连接活跃
  let mut client = TcpStream::connect(addr).expect("connect");
  client
    .write_all(&resp_frame(&[b"SET", b"k1", b"v1"]))
    .expect("set");
  assert_eq!(
    read_some(&mut client, Duration::from_secs(5)),
    b"+OK\r\n",
    "SET 应回 +OK（真实会话链存活）"
  );
  client.write_all(&resp_frame(&[b"PING"])).expect("ping");
  assert_eq!(
    read_some(&mut client, Duration::from_secs(5)),
    b"+PONG\r\n",
    "PING 应回 +PONG"
  );

  let stop_server = Arc::clone(&server);
  let stopper = thread::spawn(move || stop_server.stop());
  entered_rx
    .recv_timeout(Duration::from_secs(10))
    .expect("stop 链须进入向量清理收口");
  assert!(
    probe
      .coordinator
      .lock()
      .unwrap()
      .as_ref()
      .expect("coordinator")
      .is_stopped(),
    "进入清理收口时 Phase 1（停监听阻断新连接）必须已置位——旧时序清理先于 coordinator.stop，此处必假"
  );
  // 收口入口窗口内锤新连接：监听套接字已随停机取消桥关闭，有限轮询内必见拒绝
  let deadline = Instant::now() + Duration::from_secs(5);
  let mut refused = false;
  while Instant::now() < deadline {
    match TcpStream::connect(addr) {
      Ok(_) => thread::sleep(Duration::from_millis(20)),
      Err(_) => {
        refused = true;
        break;
      }
    }
  }
  assert!(
    refused,
    "清理收口窗口内新连接必须被即刻拒绝（监听未关即时序倒置回归）"
  );
  drop(client);

  // 放行窗口：stop 链续完后续收口与 join
  probe.gate.store(false, Ordering::Release);
  release_tx.send(()).expect("release");
  drop(release_tx);
  stopper.join().expect("stop 线程无 panic");
  assert!(
    probe.cleanup_converged.load(Ordering::Acquire),
    "清理通道收口必须全量收敛零丢弃（否则孤儿索引泄漏）"
  );
  assert!(TcpStream::connect(addr).is_err(), "stop 返回后端点必须关闭");
}

/// 案二时序锁：pubsub 中枢收口后移至 worker join 之后——收口瞬间在途连接
/// 快照归零、在途 PUBLISH 已在断开前投递；旧时序在排空进行中抢先清表必红
#[test]
fn stop_disposes_pubsub_after_workers_joined_with_inflight_publish_delivered() {
  let dir = tempdir().expect("tempdir");
  let (probe, _entered_rx, _release_tx) = new_probe(false);
  let inner = Arc::new(
    Inner::open_with_config(
      test_store_config(),
      dir.path().join("graceful-p2.db"),
      session_factory as SessionFactory,
    )
    .expect("open provider"),
  );
  let provider = Arc::new(ProbeProvider {
    inner: inner.clone(),
    probe: probe.clone(),
  });
  let (server, addr) = start_server(provider);

  // 订阅连接入订阅态；发布连接投在途 PUBLISH（同步投递计数 :1）
  let mut sub = TcpStream::connect(addr).expect("subscribe connect");
  sub
    .write_all(&resp_frame(&[b"SUBSCRIBE", b"ch"]))
    .expect("subscribe");
  let ack = read_some(&mut sub, Duration::from_secs(5));
  assert!(
    ack.starts_with(b"*3"),
    "SUBSCRIBE 应回三元素确认帧: {ack:?}"
  );
  let mut publisher = TcpStream::connect(addr).expect("publish connect");
  publisher
    .write_all(&resp_frame(&[b"PUBLISH", b"ch", b"hello"]))
    .expect("publish");
  assert_eq!(
    read_some(&mut publisher, Duration::from_secs(5)),
    b":1\r\n",
    "在途 PUBLISH 必须投递至订阅者计数"
  );
  // 推送写臂落网片刻：消息帧先于停机入订阅连接写缓冲
  thread::sleep(Duration::from_millis(100));

  server.stop();

  assert_eq!(
    probe.pubsub_active_snapshot.load(Ordering::Acquire),
    0,
    "pubsub 收口必须位于 worker join 之后（收口瞬间在途连接应已排空归零；\
     旧时序 dispose_pubsub 先于 join，此刻排空臂尚在运行、快照必然非零）"
  );
  assert!(
    probe.pubsub_after_cleanup.load(Ordering::Acquire),
    "pubsub 收口须在通道清理收敛之后（C# subscribeBroker.Dispose 排 Provider.Dispose 后）"
  );
  let broker = inner.pubsub.as_ref().expect("pubsub 默认装配");
  assert!(broker.is_disposed(), "stop 须置 pubsub 中枢收口位");
  assert!(broker.is_idle(), "收口后订阅表必须清空");
  // 在途消息安全投递面：订阅端断开前收到已发布消息帧
  let got = drain_to_end(&mut sub);
  assert!(
    got.windows(b"hello".len()).any(|w| w == b"hello"),
    "在途 PUBLISH 消息须在连接断开前投递到位: {got:?}"
  );
}

/// 案三防御档锁：有界 join 原语对挂死 worker 线程在确定性上界内以超时臂
/// 收口（裸 join 永挂），正常退出与 panic 退出分形可辨
#[test]
fn join_worker_bounded_escapes_stuck_worker_within_timeout() {
  // 挂死臂：线程体阻塞于通道接收——worker 线程内部同步死锁的可观测形；
  // 中介线程代持 join，主线程到期即脱身
  let (pin_tx, pin_rx) = mpsc::channel::<()>();
  let stuck = thread::Builder::new()
    .name("stuck-worker".into())
    .spawn(move || {
      let _ = pin_rx.recv();
    })
    .expect("spawn stuck worker");
  let begin = Instant::now();
  assert_eq!(
    join_worker_bounded(stuck, Duration::from_millis(200)),
    None,
    "挂死 worker 必须落入超时强收臂"
  );
  assert!(
    begin.elapsed() < Duration::from_secs(5),
    "上界生效，停机主线程不被挂死 worker 绑架"
  );
  drop(pin_tx); // 解除卡点，中介线程收尾自然退出（无泄漏滞留面）

  // 正常臂：立即 Some(true)
  let ok = thread::spawn(|| {});
  assert_eq!(join_worker_bounded(ok, Duration::from_secs(5)), Some(true));

  // panic 臂与超时臂分形可辨（临时静默 panic 打印，收口即复位）
  let prev = take_hook();
  set_hook(Box::new(|_| {}));
  let bad = thread::spawn(|| panic!("worker 防御档分形测试"));
  let panicked = join_worker_bounded(bad, Duration::from_secs(5));
  set_hook(prev);
  assert_eq!(panicked, Some(false), "panic 退出的 worker 须与超时臂区分");
}
