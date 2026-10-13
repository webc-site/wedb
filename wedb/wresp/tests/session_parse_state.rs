#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbase::num::{strict_i32, strict_i64};
use wresp::{Error, argslice::ArgSlice, session_parse_state::SessionParseState};

/// 以宿主缓冲连续排布构造解析态（返回状态与宿主缓冲）
fn state_with_args(args: &[&[u8]]) -> (SessionParseState, Vec<u8>) {
  let mut buf = Vec::new();
  let mut slices = Vec::with_capacity(args.len());
  for arg in args {
    slices.push(ArgSlice::new(buf.len(), arg.len()));
    buf.extend_from_slice(arg);
  }
  let mut state = SessionParseState::new();
  state.initialize(slices.len());
  state.root_buffer[..slices.len()].copy_from_slice(&slices);
  (state, buf)
}

#[test]
fn serialize_roundtrip_layout() {
  let (state, buf) = state_with_args(&[b"SET", b"k", b"v"]);
  let len = state.get_serialized_length();
  let mut dest = vec![0u8; len];
  let written = state.serialize_to(&buf, &mut dest);
  assert_eq!(written, len);

  // 布局：[count i32][每参数 4B 长度前缀 + 数据]
  assert_eq!(&dest[..4], &3i32.to_le_bytes());
  assert_eq!(&dest[4..8], &3u32.to_le_bytes());
  assert_eq!(&dest[8..11], b"SET");
  assert_eq!(&dest[11..15], &1u32.to_le_bytes());
  assert_eq!(&dest[15..16], b"k");
  assert_eq!(&dest[16..20], &1u32.to_le_bytes());
  assert_eq!(&dest[20..21], b"v");
}

#[test]
fn strict_int_rejects_leading_zeros_and_allows_sign() {
  assert_eq!(strict_i64(b"007"), None);
  assert_eq!(strict_i64(b"-007"), None);
  assert_eq!(strict_i32(b"01"), None);
  assert_eq!(strict_i64(b"0"), Some(0));
  assert_eq!(strict_i64(b"-0"), Some(0));
  assert_eq!(strict_i64(b"+0"), Some(0));
  assert_eq!(strict_i64(b"+5"), Some(5));
  assert_eq!(strict_i64(b"-5"), Some(-5));
  assert_eq!(strict_i64(b"-9223372036854775808"), Some(i64::MIN));
  assert_eq!(strict_i64(b"-9223372036854775809"), None);
  assert_eq!(strict_i64(b"9223372036854775808"), None);
  assert_eq!(strict_i64(b"5 "), None);
  assert_eq!(strict_i64(b""), None);
  assert_eq!(strict_i64(b"1x"), None);
  assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
  assert_eq!(strict_i32(b"2147483648"), None);
  assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
}

#[test]
fn read_parses_bulk_argument_and_advances() {
  // *2\r\n$3\r\nSET\r\n$2\r\nv1\r\n 的参数段（命令名已被快路径消费）
  let buffer = b"$3\r\nSET\r\n$2\r\nv1\r\n";
  let mut state = SessionParseState::new();
  state.initialize(2);
  let mut ptr = 0usize;
  assert!(state.read(0, buffer, &mut ptr, buffer.len()).unwrap());
  assert_eq!(state.arg_in(buffer, 0), b"SET");
  assert_eq!(ptr, 9);
  assert!(state.read(1, buffer, &mut ptr, buffer.len()).unwrap());
  assert_eq!(state.arg_in(buffer, 1), b"v1");
  assert_eq!(ptr, buffer.len());

  // 负载未完整到达 → Ok(false) 且游标停在负载起点（可重试）
  let partial = b"$5\r\nab";
  let mut ptr = 0usize;
  assert!(!state.read(0, partial, &mut ptr, partial.len()).unwrap());

  // 空串参数 $0\r\n\r\n
  let empty = b"$0\r\n\r\n";
  let mut ptr = 0usize;
  assert!(state.read(0, empty, &mut ptr, empty.len()).unwrap());
  assert_eq!(state.arg_in(empty, 0), b"");

  // 负长度（C# ThrowInvalidStringLength）→ 协议违例
  let neg = b"$-1\r\n";
  let mut ptr = 0usize;
  assert_eq!(
    state.read(0, neg, &mut ptr, neg.len()),
    Err(Error::InvalidStringLength(-1))
  );

  // 头不完整
  let mut ptr = 0usize;
  assert!(!state.read(0, b"$1", &mut ptr, 2).unwrap());
}
