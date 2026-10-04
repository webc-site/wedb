use wbase::{ascii_sanitize, eq_ascii_case, eq_ascii_case_const};

#[test]
fn test_ascii_sanitize() {
  // 纯 ASCII 零拷贝借用
  assert_eq!(ascii_sanitize(b""), "");
  assert_eq!(ascii_sanitize(b"abc"), "abc");
  assert_eq!(ascii_sanitize(b"HELLO 123"), "HELLO 123");

  // 单字节 0xFF 折为 '?'
  assert_eq!(ascii_sanitize(&[0xFF]), "?");

  // 混合非 ASCII
  assert_eq!(ascii_sanitize(b"foo\xffbar"), "foo?bar");

  // 6 字节中文逐字节折为 6 个 '?'
  assert_eq!(ascii_sanitize("错误".as_bytes()), "??????");
  assert_eq!(ascii_sanitize("未知".as_bytes()), "??????");
}

#[test]
fn test_eq_ascii_case() {
  assert!(eq_ascii_case(b"", b""));
  assert!(eq_ascii_case(b"abc", b"ABC"));
  assert!(eq_ascii_case(b"Hello-World_123", b"hELLO-wORLD_123"));
  assert!(!eq_ascii_case(b"abc", b"abcd"));
  assert!(!eq_ascii_case(b"abcd", b"abc"));
  assert!(!eq_ascii_case(b"abc", b"abd"));
  assert!(!eq_ascii_case(b"\xff", b"\xfe"));
  assert!(eq_ascii_case(b"\xff", b"\xff"));

  // 编译期 const 验证
  const {
    assert!(eq_ascii_case_const(b"GET", b"get"));
    assert!(!eq_ascii_case_const(b"GET", b"set"));
  }
}
