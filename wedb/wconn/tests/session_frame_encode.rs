//! 会话复制帧编码测试（归位自 wconn::session 内联测试，仅依赖 pub API）
//!
//! 覆盖 attach_sync 帧布局、append_log 帧编码与二进制数组解析往返、半包
//! 不消费、null/整数行元素整帧消费各面。

use wconn::{parser::RespReadResponseUtils, session::encode_attach_sync_frame};
use wresp::ext::RespVecExt;

/// attach_sync 帧布局：3 元素数组头 + CLUSTER + ATTACH_SYNC + 二进制元数据
#[test]
fn attach_sync_frame_layout() {
  let frame = encode_attach_sync_frame(&[0x00, 0xff, 0x42]);
  assert_eq!(
    frame,
    b"*3\r\n$7\r\nCLUSTER\r\n$11\r\nATTACH_SYNC\r\n$3\r\n\x00\xff\x42\r\n"
  );
}

/// append_log 帧编码与二进制数组解析往返：编码含二进制载荷的 8 元素数组，
/// 经生产数组读臂 try_read_byte_slice_array_with_length_header 还原逐元素字节
#[test]
fn append_log_frame_roundtrip() {
  let mut frame = Vec::new();
  frame.write_resp_array_len(8);
  frame.write_resp_bulk_string(b"CLUSTER");
  frame.write_resp_bulk_string(b"APPENDLOG");
  frame.write_resp_bulk_string(b"node-1");
  {
    let mut writer = frame.resp_writer2();
    writer.write_integer_as_bulk_string(0);
    writer.write_integer_as_bulk_string(64);
    writer.write_integer_as_bulk_string(-1);
    writer.write_integer_as_bulk_string(128);
  }
  // 二进制载荷（含非 UTF-8 字节与 \r\n 穿透）
  let payload: &[u8] = &[0x00, 0xff, b'\r', b'\n', 0x80, 0x42];
  frame.write_resp_bulk_string(payload);

  let mut data = frame.as_slice();
  let parsed = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data, 1)
    .expect("parse ok")
    .expect("complete");
  let items = parsed.expect("non-null");
  assert_eq!(items.len(), 8);
  assert_eq!(items[0], b"CLUSTER");
  assert_eq!(items[1], b"APPENDLOG");
  assert_eq!(items[2], b"node-1");
  assert_eq!(items[3], b"0");
  assert_eq!(items[4], b"64");
  assert_eq!(items[5], b"-1");
  assert_eq!(items[6], b"128");
  assert_eq!(items[7], payload);
  assert!(data.is_empty(), "帧应被完整消费");
}

/// 半包不消费：截断帧解析返回 None 且游标回滚
#[test]
fn byte_slice_array_partial_frame_not_consumed() {
  let mut frame = Vec::new();
  frame.write_resp_array_len(2);
  frame.write_resp_bulk_string(b"CLUSTER");
  frame.write_resp_bulk_string(b"APPENDLOG");
  // 截去末元素尾 CRLF 制造半包
  let partial = &frame[..frame.len() - 2];

  let mut data = partial;
  let res = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data, 1);
  assert!(matches!(res, Ok(None)));
  assert_eq!(data.len(), partial.len(), "不完整帧游标应回滚");
}

/// null bulk 元素（$-1，MGET 缺失键应答形）容为空切片：整帧完整消费，
/// 不再被 flatten 成 None 误判半包（旧实现会致读泵游标永久停在帧头死等）
#[test]
fn byte_slice_array_null_bulk_element_consumed() {
  let mut data: &[u8] = b"*2\r\n$-1\r\n$1\r\na\r\n";
  let parsed = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data, 1)
    .expect("parse ok")
    .expect("complete");
  let items = parsed.expect("non-null");
  assert_eq!(items, [b"".as_slice(), b"a".as_slice()]);
  assert!(data.is_empty(), "含 null 元素的整帧应被完整消费");
}

/// RESP3 null 元素（_\r\n）同形：容为空切片，整帧完整消费
#[test]
fn byte_slice_array_resp3_null_element_consumed() {
  let mut data: &[u8] = b"*1\r\n_\r\n";
  let parsed = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data, 1)
    .expect("parse ok")
    .expect("complete");
  let items = parsed.expect("non-null");
  assert_eq!(items, [b"".as_slice()]);
  assert!(data.is_empty(), "含 RESP3 null 元素的整帧应被完整消费");
}

/// 非 bulk 行元素（: 整数行，SMISMEMBER 应答形）按行读借用行体：整帧完整消费，
/// 不再因首字节非 '$' 掷 UnexpectedToken 拆连接
#[test]
fn byte_slice_array_integer_element_consumed() {
  let mut data: &[u8] = b"*2\r\n:1\r\n:0\r\n";
  let parsed = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data, 1)
    .expect("parse ok")
    .expect("complete");
  let items = parsed.expect("non-null");
  assert_eq!(items, [b"1".as_slice(), b"0".as_slice()]);
  assert!(data.is_empty(), "整数行元素整帧应被完整消费");
}
