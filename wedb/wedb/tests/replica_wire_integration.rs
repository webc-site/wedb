#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::{Duration, Instant},
};

use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use parking_lot::Mutex;
use wbase::{
  hex::hex_str_u128,
  pool::{
    DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, EventWorkQueue, LimitedFixedBufferPool,
  },
};
use wconf::{NodeArgs, node_options::INFINITE_SYNC_TIMEOUT_SECS};
use wconn::{session::GarnetClientSession, types::MAX_UNFLUSHED_SEND_BYTES};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::replica_wire::{ShippedState, TcpSessionWire},
};
use wedb_test::de11_node_id::DE11_NODE_ID;
use wtest_base::bind_blackhole;

#[test]
fn tcp_wire_not_connected() {
  let client = GarnetClientSession::new("127.0.0.1:0".to_string(), None, None, None, None);
  let node_id = DE11_NODE_ID;
  let wire = TcpSessionWire {
    client,
    node_id_hex: hex_str_u128(node_id).into_boxed_str(),
    overflow: Arc::new(EventWorkQueue::new()),
    overflow_bytes: AtomicUsize::new(0),
    byte_cap: MAX_UNFLUSHED_SEND_BYTES,
    in_flight: Arc::new(AtomicUsize::new(0)),
    pump_alive: Arc::new(AtomicBool::new(true)),
    ratchets: Mutex::new(Vec::new()),
  };
  assert!(!wire.is_connected());
  let res = wire.advance_time(0, 1);
  assert!(res.is_err());
  assert_eq!(res.unwrap_err().kind(), io::ErrorKind::NotConnected);
}

/// 构造会话通道在位的 TcpSessionWire：连到静默 loopback 端点（无凭证
/// 握手零往返即成），常驻泵不启动、在途帧置位封死直发臂——溢流只积不排，
/// 以确定性形态触达条数/字节双封顶
async fn wire_pumpless_connected(byte_cap: usize) -> TcpSessionWire {
  let addr = bind_blackhole().await.to_string();
  let mut client = GarnetClientSession::new(addr, None, None, None, None);
  client.connect_async().await.unwrap();
  let node_id = DE11_NODE_ID;
  TcpSessionWire {
    client,
    node_id_hex: hex_str_u128(node_id).into_boxed_str(),
    overflow: Arc::new(EventWorkQueue::new()),
    overflow_bytes: AtomicUsize::new(0),
    byte_cap,
    in_flight: Arc::new(AtomicUsize::new(1)),
    pump_alive: Arc::new(AtomicBool::new(true)),
    ratchets: Mutex::new(Vec::new()),
  }
}

/// 字节维封顶：驻留字节触顶先于条数触顶断连（对标 C# NetworkWriter
/// 4 页环形缓冲字节硬顶），错误文案区分字节判据，超界帧不入溢流队列
#[compio::test]
async fn tcp_wire_overflow_byte_cap_disconnects() {
  let wire = wire_pumpless_connected(4096).await;
  let payload = vec![7u8; 2048];
  for _ in 0..2 {
    assert_eq!(
      wire.append_log(0, 0, 0, 2048, &payload).unwrap(),
      ShippedState::Queued
    );
  }
  assert_eq!(wire.overflow_bytes.load(Ordering::Acquire), 4096);
  let err = wire.append_log(0, 0, 0, 4096, &payload).unwrap_err();
  assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
  assert!(
    err.to_string().contains("byte"),
    "字节判据文案应可区分: {err}"
  );
  assert!(!wire.is_connected(), "字节触顶必须转断连");
  assert_eq!(wire.overflow.len(), 2, "超界帧不得入溢流队列");
}

/// 条数维封顶：字节远未触顶时条数触顶断连，错误文案区分条数判据
#[compio::test]
async fn tcp_wire_overflow_entry_cap_disconnects() {
  let wire = wire_pumpless_connected(usize::MAX).await;
  let payload = vec![7u8; 16];
  for _ in 0..TcpSessionWire::MAX_OVERFLOW_ENTRIES {
    assert_eq!(
      wire.append_log(0, 0, 0, 1, &payload).unwrap(),
      ShippedState::Queued
    );
  }
  let err = wire.append_log(0, 0, 0, 2, &payload).unwrap_err();
  assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
  assert!(
    err.to_string().contains("entry"),
    "条数判据文案应可区分: {err}"
  );
  assert!(!wire.is_connected(), "条数触顶必须转断连");
}

/// 对标 C# AofSyncTask readonly localNodeId：
/// 节点 id 在建连/构造时一次性渲染为 32 字符小写 hex 并缓存，
/// 连续 N 次 append_log 均复用同一缓存切片，无重复堆分配与渲染开销。
#[compio::test]
async fn tcp_wire_node_id_hex_cached_and_reused() {
  let wire = wire_pumpless_connected(usize::MAX).await;
  let cached_hex = wire.node_id_hex();
  assert_eq!(cached_hex, "0000000000000000000000000000de11");
  let cached_ptr = cached_hex.as_ptr();

  let payload = vec![7u8; 16];
  for i in 0..10 {
    assert_eq!(
      wire.append_log(0, 0, 0, i + 1, &payload).unwrap(),
      ShippedState::Queued
    );
    // 断言缓存字段指针在多次 append_log 之间稳定不变
    assert_eq!(wire.node_id_hex().as_ptr(), cached_ptr);
    assert_eq!(wire.node_id_hex(), "0000000000000000000000000000de11");
  }
}

/// 驱动退场会话拆连成链（对标 C# 持有方退场链 AofSyncTask.Dispose →
/// garnetClient?.Dispose 的等义面）：wire 在位时断连即双向拆出站会话，
/// 假端点在限时内观测 EOF/err——socket 本体真收场，非仅 rust 记账面翻假；
/// 静默对端下会话连接与池缓冲不再悬挂
#[compio::test]
async fn tcp_wire_disconnect_shuts_down_session_socket() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  // 假副本端点：accept 后持续读，直至对端拆连以 EOF/err 落定即记旗
  let peer_saw_eof = Arc::new(AtomicBool::new(false));
  let flag = Arc::clone(&peer_saw_eof);
  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; 512];
    loop {
      let BufResult(res, next) = sock.read(buf).await;
      buf = next;
      match res {
        Ok(n) if n > 0 => {}
        _ => break,
      }
    }
    flag.store(true, Ordering::Release);
  })
  .detach();

  let mut client = GarnetClientSession::new(addr, None, None, None, None);
  client.connect_async().await.unwrap();
  let node_id = DE11_NODE_ID;
  let wire = TcpSessionWire {
    client,
    node_id_hex: hex_str_u128(node_id).into_boxed_str(),
    overflow: Arc::new(EventWorkQueue::new()),
    overflow_bytes: AtomicUsize::new(0),
    byte_cap: MAX_UNFLUSHED_SEND_BYTES,
    in_flight: Arc::new(AtomicUsize::new(0)),
    pump_alive: Arc::new(AtomicBool::new(true)),
    ratchets: Mutex::new(Vec::new()),
  };
  assert!(wire.is_connected(), "断连前会话应报告已连接");

  wire.disconnect();
  assert!(
    !wire.client.is_connected(),
    "disconnect 后会话 is_connected 应翻假（dispose 幂等位）"
  );

  let saw = timeout(Duration::from_secs(5), async {
    while !peer_saw_eof.load(Ordering::Acquire) {
      sleep(Duration::from_millis(25)).await;
    }
  })
  .await;
  assert!(saw.is_ok(), "对端未在限时内观测到拆连：会话 socket 未收场");
}

/// 复制出站建连限时随活旋钮（本册锁「限时唯 RuntimeServerOptions.
/// replica_sync_timeout_secs 是从，无常量钉值」）：provider 选项槽折出的
/// 限时形参在静默端点上即收场窗——甲臂旋钮 1s 必以 TimedOut 收场，乙臂旋钮
/// 60s 于 10s 观察窗内仍挂起；限时若仍钉死常量则两臂同窗，乙臂必红。
/// 丙臂钉无限哨兵（`--repl-sync-timeout 0`/负值经投影折
/// `INFINITE_SYNC_TIMEOUT_SECS`）：值源折 None → 建连臂不挂计时器，观察窗内
/// 既不 panic 也不超时收场——修复前 None 缺位、u64::MAX 秒直折
/// `Duration::from_secs` 入 compio `sleep`（内部 `Instant::now() + d`）即溢出
/// panic，本臂即在断链点前炸出
#[compio::test]
async fn replica_sync_timeout_knob_drives_connect_window() {
  let provider = ClusterProvider::new();
  let addr = bind_blackhole().await.to_string();
  let pool = || LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL);

  provider.set_replica_sync_timeout_secs(1);
  let started = Instant::now();
  let connect = TcpSessionWire::connect(
    &addr,
    DE11_NODE_ID,
    0,
    (None, None),
    pool(),
    provider.replica_sync_timeout(),
    #[cfg(feature = "tls")]
    None,
  )
  .await;
  let err = match connect {
    Err(e) => e,
    Ok(_) => panic!("1s 旋钮下静默端点建连应超时"),
  };
  assert_eq!(
    err.kind(),
    io::ErrorKind::TimedOut,
    "超时错误码应为 TimedOut"
  );
  assert!(
    started.elapsed() < Duration::from_secs(20),
    "甲臂收场窗应贴 1s 旋钮而非 60s 量级"
  );

  provider.set_replica_sync_timeout_secs(60);
  let pending = timeout(
    Duration::from_millis(1500),
    TcpSessionWire::connect(
      &addr,
      DE11_NODE_ID,
      0,
      (None, None),
      pool(),
      provider.replica_sync_timeout(),
      #[cfg(feature = "tls")]
      None,
    ),
  )
  .await;
  assert!(
    pending.is_err(),
    "60s 旋钮下建连在 1.5s 观察窗内应仍挂起（收场窗随旋钮放宽，未在 1s 提前超时）"
  );

  // 丙臂：0 值经投影折无限哨兵入槽 → 限时形参 None → 不挂计时器，观察窗内
  // 恒挂起（既无溢出 panic，也无 TimedOut 收场；C# 同输入即 InfiniteTimeSpan）
  provider.set_replica_sync_timeout_secs(sentinel_sync_timeout_secs(0));
  assert_eq!(provider.replica_sync_timeout(), None, "哨兵槽须折 None");
  let pending = timeout(
    Duration::from_millis(300),
    TcpSessionWire::connect(
      &addr,
      DE11_NODE_ID,
      0,
      (None, None),
      pool(),
      provider.replica_sync_timeout(),
      #[cfg(feature = "tls")]
      None,
    ),
  )
  .await;
  assert!(
    pending.is_err(),
    "无限哨兵下建连在 300ms 观察窗内应恒挂起（不挂计时器、不 panic）"
  );
}

/// `<=0` 原始输入经 wconf 投影层折出的无限超时哨兵槽值（值源单链，本册不
/// 另立第二折算）
fn sentinel_sync_timeout_secs(raw: i32) -> u64 {
  NodeArgs {
    replica_sync_timeout_secs: raw,
    ..Default::default()
  }
  .runtime_server_options()
  .replica_sync_timeout_secs
}

/// 值源折叠口径（provider 面单点锁）：无限哨兵 → None（消费点经 wait_async
/// 不挂计时器），正值 → Some(from_secs(值))；修复前本口无条件
/// `Duration::from_secs` 直折，哨兵臂即落天文级时限并在定时器构造处溢出 panic
#[test]
fn replica_sync_timeout_sentinel_folds_to_none() {
  let provider = ClusterProvider::new();

  // 投影入槽的哨兵（0 / 负值三态）与显式哨兵常量，同一结论
  for raw in [0_i32, -1, i32::MIN] {
    provider.set_replica_sync_timeout_secs(sentinel_sync_timeout_secs(raw));
    assert_eq!(
      provider.replica_sync_timeout(),
      None,
      "{raw} 秒输入须折无限（None），不得挂巨大时限"
    );
  }
  provider.set_replica_sync_timeout_secs(INFINITE_SYNC_TIMEOUT_SECS);
  assert_eq!(
    provider.replica_sync_timeout(),
    None,
    "显式哨兵常量与投影臂同源同折"
  );

  for secs in [1_u64, 5, 77] {
    provider.set_replica_sync_timeout_secs(secs);
    assert_eq!(
      provider.replica_sync_timeout(),
      Some(Duration::from_secs(secs)),
      "正值 {secs} 秒原样限时"
    );
  }
}
