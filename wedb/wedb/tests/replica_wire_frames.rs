//! r320 迁出：replica_wire 内联 tests 块（src/server/replication/replica_wire.rs:743-807）
//! 回调通道帧收发断连语义 + APPENDLOG 帧/初始化帧字节级布局锁（deviations 锁测，原样保留）

use std::sync::Arc;

use parking_lot::Mutex;
use wconn::session::{encode_append_log_frame, encode_append_log_init_frame};
use wedb::server::replication::replica_wire::test_wire::{CallbackWire, FrameSink};
use wresp::frame::parse_resp_frame;

/// 内存通道帧收发：回调收到编码帧且断连后拒绝发送
#[test]
fn callback_wire_frame_delivery_and_disconnect() {
  let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
  let wire = CallbackWire::new(received.clone());

  assert!(
    wire
      .append_log(0x0000_DE11, 0, 64, 64, 128, b"\x00\xffpayload")
      .is_ok()
  );
  let frames = received.lock();
  assert_eq!(frames.len(), 1);
  // 收到的帧可被二进制数组解析器还原为 8 元素
  let (_consumed, items) = parse_resp_frame(&frames[0])
    .expect("协议合法")
    .expect("complete");
  assert_eq!(items.len(), 8);
  // 节点 id 协议面渲染 32 字符小写 hex
  assert_eq!(items[2], b"0000000000000000000000000000de11");
  assert_eq!(items[7], b"\x00\xffpayload");

  drop(frames);
  wire.disconnect();
  assert!(!wire.is_connected());
  assert!(wire.append_log(0x0DE1, 0, 64, 64, 128, b"x").is_err());
}

/// 回调拒绝投递 → 通道转断连态（对标副本会话关闭语义）
#[test]
fn callback_wire_sink_reject_disconnects() {
  let wire = CallbackWire::new(FrameSink::Reject);
  assert!(wire.append_log(0x0, 0, 0, 0, 1, b"f").is_err());
  assert!(!wire.is_connected());
}

/// 记录帧布局：8 元素数组头 + CLUSTER + APPENDLOG + 节点 + 三个整数 + 二进制载荷
#[test]
fn append_log_frame_layout() {
  let frame = encode_append_log_frame("p1", 2, -1, 100, 200, b"rec");
  let expected = b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$2\r\np1\r\n\
$1\r\n2\r\n$2\r\n-1\r\n$3\r\n100\r\n$3\r\n200\r\n$3\r\nrec\r\n";
  assert_eq!(frame, expected);
}

/// 初始化帧布局：7 元素数组（三方地址 -1/-1/-1），无 payload 元素
#[test]
fn append_log_init_frame_layout() {
  let frame = encode_append_log_init_frame("primary-1", 0, -1, -1, -1);
  let expected = b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary-1\r\n\
$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n";
  assert_eq!(frame, expected);
}
