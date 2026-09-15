use wnode::resp::RespServerSession;

#[test]
fn test_resp_server_session_output_resp2_vs_resp3() {
  let mut session = RespServerSession::default();
  session.update_resp_protocol_version(2);

  session.output.clear();
  session.write_null();
  assert_eq!(&session.output, b"$-1\r\n");

  session.output.clear();
  session.write_null_array();
  assert_eq!(&session.output, b"*-1\r\n");

  session.output.clear();
  session.write_map_length(2);
  assert_eq!(&session.output, b"*4\r\n");

  session.output.clear();
  session.write_set_length(3);
  assert_eq!(&session.output, b"*3\r\n");

  session.output.clear();
  session.write_push_length(3);
  assert_eq!(&session.output, b"*3\r\n");

  session.output.clear();
  session.write_double_numeric(1.25);
  assert_eq!(&session.output, b"$4\r\n1.25\r\n");

  session.output.clear();
  session.write_empty_set();
  assert_eq!(&session.output, b"*0\r\n");

  session.output.clear();
  session.write_large_verbatim_string(b"hello", b"txt");
  assert_eq!(&session.output, b"$5\r\nhello\r\n");

  session.output.clear();
  session.write_bool(true);
  assert_eq!(&session.output, b":1\r\n");

  session.output.clear();
  session.write_bool(false);
  assert_eq!(&session.output, b":0\r\n");

  // 切到 RESP3
  session.update_resp_protocol_version(3);

  session.output.clear();
  session.write_null();
  assert_eq!(&session.output, b"_\r\n");

  session.output.clear();
  session.write_null_array();
  assert_eq!(&session.output, b"_\r\n");

  session.output.clear();
  session.write_map_length(2);
  assert_eq!(&session.output, b"%2\r\n");

  session.output.clear();
  session.write_set_length(3);
  assert_eq!(&session.output, b"~3\r\n");

  session.output.clear();
  session.write_push_length(3);
  assert_eq!(&session.output, b">3\r\n");

  session.output.clear();
  session.write_double_numeric(1.25);
  assert_eq!(&session.output, b",1.25\r\n");

  // NaN/±∞ 采用 C# 的 nan/inf 口径（而非 zmij 直写的 NaN）
  session.output.clear();
  session.write_double_numeric(f64::NAN);
  assert_eq!(&session.output, b",nan\r\n");

  session.output.clear();
  session.write_double_numeric(f64::NEG_INFINITY);
  assert_eq!(&session.output, b",-inf\r\n");

  session.output.clear();
  session.write_empty_set();
  assert_eq!(&session.output, b"~0\r\n");

  session.output.clear();
  session.write_large_verbatim_string(b"hello", b"txt");
  assert_eq!(&session.output, b"=9\r\ntxt:hello\r\n");

  session.output.clear();
  session.write_bool(true);
  assert_eq!(&session.output, b"#t\r\n");

  session.output.clear();
  session.write_bool(false);
  assert_eq!(&session.output, b"#f\r\n");
}
