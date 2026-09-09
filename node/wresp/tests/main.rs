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
