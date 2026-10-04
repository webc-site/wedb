use wtest_base::{a, parse_frame, parse_frame_slices, resp_frame, resp_frame_str, try_parse_frame};

#[test]
fn resp_frame_empty_and_simple() {
  assert_eq!(resp_frame(&[]), b"*0\r\n");
  assert_eq!(resp_frame_str(&[]), b"*0\r\n");
  assert_eq!(
    resp_frame(a![b"SET", b"k", b"v"]),
    b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
  );
}

#[test]
fn resp_frame_binary_payload_not_escaped() {
  // 含 CRLF 与 NUL 的二进制载荷：只按字节长度成帧，不转义不分片
  assert_eq!(resp_frame(a![b"a\r\nb\0c"]), b"*1\r\n$6\r\na\r\nb\0c\r\n");
}

#[test]
fn resp_frame_str_utf8_matches_byte_frame() {
  // 多字节 UTF-8：长度取字节数而非字符数，与 &[u8] 入口逐字节等价
  let mut expected = Vec::new();
  expected.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$8\r\n");
  expected.extend_from_slice("ключ".as_bytes());
  expected.extend_from_slice(b"\r\n");
  assert_eq!(resp_frame_str(&["GET", "ключ"]), expected);
  assert_eq!(
    resp_frame_str(&["GET", "ключ", "a\r\nb"]),
    resp_frame(a![b"GET", "ключ".as_bytes(), b"a\r\nb"])
  );
}

#[test]
fn parse_resp_frame_test() {
  let frame = b"*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n";
  assert_eq!(try_parse_frame(frame), Some(frame.len()));
  // 不完整帧
  assert_eq!(try_parse_frame(&frame[..frame.len() - 2]), None);
  assert_eq!(try_parse_frame(b"*1\r\n$4\r\nPI"), None);
  assert_eq!(try_parse_frame(b""), None);
  // 两帧连排：只解第一帧
  let two = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
  assert_eq!(try_parse_frame(two), Some(14));

  // 零拷贝切片解析
  let (len, slices) = parse_frame_slices(frame).unwrap();
  assert_eq!(len, frame.len());
  assert_eq!(slices, vec![&b"PING"[..], &b"foo"[..]]);

  // 兼容 owned 副本解析
  let (len2, owned) = parse_frame(frame).unwrap();
  assert_eq!(len2, frame.len());
  assert_eq!(owned, vec![b"PING".to_vec(), b"foo".to_vec()]);
}

/// 畸形头与边界守卫：`*\n` / `*\r\n` / `$\n` / `$\r\n` / 尾部缺 CRLF
#[test]
fn parse_frame_malformed_header_no_underflow() {
  assert_eq!(try_parse_frame(b"*A\r\n$1\r\nx\r\n"), None);
  assert_eq!(try_parse_frame(b"*1\r\n$\r\nx\r\n"), None);
  assert_eq!(try_parse_frame(b"*x\r\n$1\r\nx\r\n"), None);
  assert_eq!(try_parse_frame(b"*1\r\n$\r\n"), None);
  assert_eq!(try_parse_frame(b"*\n"), None);
  assert_eq!(try_parse_frame(b"*\r\n"), None);
  assert_eq!(try_parse_frame(b"*1\r\n$1\r\nxXX"), None); // 尾部非 \r\n
}

#[test]
fn resp_frame_macro_array_literal() {
  let key = "my_key";
  let val = b"my_val";
  let framed = resp_frame!(["SET", key, val]);
  assert_eq!(
    framed,
    b"*3\r\n$3\r\nSET\r\n$6\r\nmy_key\r\n$6\r\nmy_val\r\n"
  );

  let empty_framed = resp_frame!([]);
  assert_eq!(empty_framed, b"*0\r\n");

  let varargs_framed = resp_frame!("GET", key);
  assert_eq!(varargs_framed, b"*2\r\n$3\r\nGET\r\n$6\r\nmy_key\r\n");
}
