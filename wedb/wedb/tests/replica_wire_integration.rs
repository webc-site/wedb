use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
};

use compio::{net::TcpListener, runtime::spawn};
use parking_lot::Mutex;
use wbase::{hex::hex_str_u128, pool::EventWorkQueue};
use wconn::{session::GarnetClientSession, types::MAX_UNFLUSHED_SEND_BYTES};
use wedb::server::replication::replica_wire::{ShippedState, TcpSessionWire};

#[test]
fn tcp_wire_not_connected() {
  let client = GarnetClientSession::new("127.0.0.1:0".to_string(), None, None, None, None);
  let node_id = 0x0000_DE11;
  let wire = TcpSessionWire {
    client,
    node_id,
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
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  // 收下连接即静默（不读不写），套接字随任务滞留至测试收场
  spawn(async move {
    let mut held = Vec::new();
    while let Ok((sock, _)) = listener.accept().await {
      held.push(sock);
    }
  })
  .detach();
  let mut client = GarnetClientSession::new(addr, None, None, None, None);
  client.connect_async().await.unwrap();
  let node_id = 0x0000_DE11;
  TcpSessionWire {
    client,
    node_id,
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
      wire.append_log(0, 0, 0, 0, 2048, &payload).unwrap(),
      ShippedState::Queued
    );
  }
  assert_eq!(wire.overflow_bytes.load(Ordering::Acquire), 4096);
  let err = wire.append_log(0, 0, 0, 0, 4096, &payload).unwrap_err();
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
      wire.append_log(0, 0, 0, 0, 1, &payload).unwrap(),
      ShippedState::Queued
    );
  }
  let err = wire.append_log(0, 0, 0, 0, 2, &payload).unwrap_err();
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
      wire.append_log(0, 0, 0, 0, i + 1, &payload).unwrap(),
      ShippedState::Queued
    );
    // 断言缓存字段指针在多次 append_log 之间稳定不变
    assert_eq!(wire.node_id_hex().as_ptr(), cached_ptr);
    assert_eq!(wire.node_id_hex(), "0000000000000000000000000000de11");
  }
}
