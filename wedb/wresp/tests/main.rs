use aok::{OK, Void};
use log::info;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}
use wresp::read::*;

#[test]
fn test_length_encoding() {
  let mut length = 0;

  // Test unsigned length header "$5\r\n"
  let mut ptr = b"$5\r\n".as_slice();
  assert!(try_read_unsigned_length_header(&mut length, &mut ptr, b'$').unwrap());
  assert_eq!(length, 5);

  // Test signed length header "$-1\r\n" (NULL)
  let mut ptr = b"$-1\r\n".as_slice();
  assert!(try_read_signed_length_header(&mut length, &mut ptr, b'$').unwrap());
  assert_eq!(length, -1);

  // Test array length "*3\r\n"
  let mut ptr = b"*3\r\n".as_slice();
  assert!(try_read_unsigned_array_length(&mut length, &mut ptr).unwrap());
  assert_eq!(length, 3);
}

#[test]
fn test_skip_byte_array() {
  let mut ptr = b"$5\r\nhello\r\n".as_slice();
  assert!(try_skip_byte_array_with_length_header(&mut ptr).unwrap());
  assert!(ptr.is_empty());
}

#[test]
fn test_int32_boundary_length_headers() {
  // i32 边界：正最大与负最小（2^31 取负须经 i64，debug 构建不得溢出 panic）
  let mut ptr = b"$2147483647\r\n".as_slice();
  let mut length = 0;
  assert!(try_read_unsigned_length_header(&mut length, &mut ptr, b'$').unwrap());
  assert_eq!(length, i32::MAX);

  let mut ptr = b"$-2147483648\r\n".as_slice();
  let mut length = 0;
  assert!(try_read_signed_length_header(&mut length, &mut ptr, b'$').unwrap());
  assert_eq!(length, i32::MIN);

  // 越界长度报溢出而非回绕
  let mut ptr = b"$2147483648\r\n".as_slice();
  let mut length = 0;
  assert!(try_read_unsigned_length_header(&mut length, &mut ptr, b'$').is_err());
}

/// 512MB 上界（RespReadUtils.cs:700/763/870/935/1158 的
/// `length > MaxArgumentLengthBytes` 判定）：超界按"数据未到齐"返回 false，
/// 头部照常消费（与 C# 吞头后短路返回 false 的次序一致）
#[test]
fn test_max_argument_length_bound() {
  // 512MB + 1：slice/skip/ptr 三类消费端一致拒绝
  let oversized = format!("${}\r\n", wresp::MAX_ARGUMENT_LENGTH_BYTES + 1);
  let head: &[u8] = oversized.as_bytes();
  let mut ptr = head;
  let mut result: &[u8] = &[];
  assert!(!try_slice_with_length_header(&mut result, &mut ptr).unwrap());
  assert_eq!(ptr, &head[head.len()..], "超界时头部已消费、仅返 false");

  let mut ptr = head;
  assert!(!try_skip_byte_array_with_length_header(&mut ptr).unwrap());

  let mut ptr = head;
  let mut span: &[u8] = &[];
  assert!(!try_read_span_with_length_header(&mut span, &mut ptr).unwrap());

  let mut ptr = head;
  let mut opt: Option<&[u8]> = None;
  assert!(!try_read_ptr_with_signed_length_header(&mut opt, &mut ptr).unwrap());

  let mut ptr = head;
  let mut len = 0;
  let mut result: &[u8] = &[];
  assert!(!try_read_ptr_with_length_header(&mut result, &mut len, &mut ptr).unwrap());

  // NULL（$-1）不受上界影响：signed 形态照常放行
  let mut ptr = b"$-1\r\n".as_slice();
  let mut opt: Option<&[u8]> = None;
  assert!(try_read_ptr_with_signed_length_header(&mut opt, &mut ptr).unwrap());
  assert_eq!(opt, None);
  assert!(ptr.is_empty());

  // 恰为 512MB：上界内放行（此断言只证头部本身不因上界被拒；正文未到齐
  // 仍按 false 处理）
  let boundary = format!("${}\r\n", wresp::MAX_ARGUMENT_LENGTH_BYTES);
  let mut ptr: &[u8] = boundary.as_bytes();
  assert!(!try_skip_byte_array_with_length_header(&mut ptr).unwrap());
}
