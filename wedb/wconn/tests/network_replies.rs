use std::collections::VecDeque;

use crossfire::{mpsc, oneshot};
use wconn::{
  network::dispatch_replies,
  types::{CommandItem, ReplyTx},
};

#[test]
fn test_dispatch_replies_cursor_flow() {
  let (_, in_flight_rx) = mpsc::bounded_async::<CommandItem>(16);
  let mut queue = VecDeque::new();
  let (tx1, mut rx1) = oneshot::oneshot();
  let (tx2, mut rx2) = oneshot::oneshot();
  queue.push_back(CommandItem::new(&["PING"], ReplyTx::Str(tx1)));
  queue.push_back(CommandItem::new(&["PING"], ReplyTx::Str(tx2)));

  let mut read_buf = Vec::new();
  read_buf.extend_from_slice(b"+PONG\r\n+PONG\r\n");
  let mut read_head = 0;

  dispatch_replies(
    &mut queue,
    &in_flight_rx,
    &mut read_buf,
    &mut read_head,
    None,
  )
  .unwrap();
  assert_eq!(read_head, 0);
  assert!(read_buf.is_empty(), "整段消费完毕清空 read_buf 并复位游标");
  assert_eq!(rx1.try_recv().unwrap().unwrap(), "PONG");
  assert_eq!(rx2.try_recv().unwrap().unwrap(), "PONG");
}

#[test]
fn test_dispatch_replies_partial_frame() {
  let (_, in_flight_rx) = mpsc::bounded_async::<CommandItem>(16);
  let mut queue = VecDeque::new();
  let (tx1, mut rx1) = oneshot::oneshot();
  let (tx2, mut rx2) = oneshot::oneshot();
  queue.push_back(CommandItem::new(&["PING"], ReplyTx::Str(tx1)));
  queue.push_back(CommandItem::new(&["PING"], ReplyTx::Str(tx2)));

  let mut read_buf = Vec::new();
  // 第一帧完整，第二帧半包
  read_buf.extend_from_slice(b"+PONG\r\n+PO");
  let mut read_head = 0;

  dispatch_replies(
    &mut queue,
    &in_flight_rx,
    &mut read_buf,
    &mut read_head,
    None,
  )
  .unwrap();
  assert_eq!(read_head, 7, "第一帧认领完成，推进游标，不搬字节");
  assert_eq!(&read_buf[read_head..], b"+PO");
  assert_eq!(rx1.try_recv().unwrap().unwrap(), "PONG");
  assert!(rx2.try_recv().is_err());

  // 补齐第二帧
  read_buf.extend_from_slice(b"NG\r\n");
  dispatch_replies(
    &mut queue,
    &in_flight_rx,
    &mut read_buf,
    &mut read_head,
    None,
  )
  .unwrap();
  assert_eq!(read_head, 0);
  assert!(read_buf.is_empty());
  assert_eq!(rx2.try_recv().unwrap().unwrap(), "PONG");
}

/// Str 形收含 null bulk 元素的数组应答（MGET 缺失键形）：首元素 null → 空串，
/// 整帧完整消费认领；同缓冲紧随的 +OK 应答被第二个在途项正常认领，无队头
/// 阻塞（旧实现 null 元素被 flatten 误判半包，游标永久停在帧头，后续应答
/// 全部滞留死等）
#[test]
fn test_dispatch_replies_str_array_null_bulk_element() {
  let (_, in_flight_rx) = mpsc::bounded_async::<CommandItem>(16);
  let mut queue = VecDeque::new();
  let (tx1, mut rx1) = oneshot::oneshot();
  let (tx2, mut rx2) = oneshot::oneshot();
  queue.push_back(CommandItem::new(&["MGET", "k1", "k2"], ReplyTx::Str(tx1)));
  queue.push_back(CommandItem::new(&["PING"], ReplyTx::Str(tx2)));

  let mut read_buf = Vec::new();
  read_buf.extend_from_slice(b"*2\r\n$-1\r\n$1\r\na\r\n+OK\r\n");
  let mut read_head = 0;

  dispatch_replies(
    &mut queue,
    &in_flight_rx,
    &mut read_buf,
    &mut read_head,
    None,
  )
  .unwrap();
  assert_eq!(read_head, 0);
  assert!(read_buf.is_empty(), "两帧整段消费完毕清空 read_buf 并复位游标");
  assert_eq!(rx1.try_recv().unwrap().unwrap(), "");
  assert_eq!(rx2.try_recv().unwrap().unwrap(), "OK");
}

/// Bytes 形收整数行元素数组应答（SMISMEMBER 形）：首元素行体字节交付且
/// dispatch_replies 正常返回（旧实现非 bulk 元素首字节掷 UnexpectedToken，
/// 整条连接被拆除）
#[test]
fn test_dispatch_replies_bytes_array_integer_elements() {
  let (_, in_flight_rx) = mpsc::bounded_async::<CommandItem>(16);
  let mut queue = VecDeque::new();
  let (tx1, mut rx1) = oneshot::oneshot();
  queue.push_back(CommandItem::new(
    &["SMISMEMBER", "k", "m"],
    ReplyTx::Bytes(tx1),
  ));

  let mut read_buf = Vec::new();
  read_buf.extend_from_slice(b"*2\r\n:1\r\n:0\r\n");
  let mut read_head = 0;

  dispatch_replies(
    &mut queue,
    &in_flight_rx,
    &mut read_buf,
    &mut read_head,
    None,
  )
  .unwrap();
  assert!(read_buf.is_empty());
  assert_eq!(rx1.try_recv().unwrap().unwrap(), b"1".to_vec());
}
