use wresp::{Resp2, Resp3, RespMemoryWriter, RespVecExt, RespWriter};

#[test]
fn test_protocol_aware_lengths() {
  let mut w = RespMemoryWriter::new();
  w.write_map_length(2);
  assert_eq!(w.out, b"*4\r\n");
  w.out.clear();
  w.write_set_length(3);
  assert_eq!(w.out, b"*3\r\n");
  w.out.clear();
  w.write_push_length(5);
  assert_eq!(w.out, b"*5\r\n");
  w.out.clear();
  w.write_null();
  assert_eq!(w.out, b"$-1\r\n");
  w.out.clear();
  w.write_null_array();
  assert_eq!(w.out, b"*-1\r\n");
  w.out.clear();
  w.write_empty_set();
  assert_eq!(w.out, b"*0\r\n");
  w.out.clear();
  w.write_empty_map();
  assert_eq!(w.out, b"*0\r\n");

  let mut w = RespMemoryWriter::<Resp3>::new_p();
  w.write_map_length(2);
  assert_eq!(w.out, b"%2\r\n");
  w.out.clear();
  w.write_set_length(3);
  assert_eq!(w.out, b"~3\r\n");
  w.out.clear();
  w.write_push_length(5);
  assert_eq!(w.out, b">5\r\n");
  w.out.clear();
  w.write_null();
  assert_eq!(w.out, b"_\r\n");
  w.out.clear();
  w.write_null_array();
  assert_eq!(w.out, b"_\r\n");
  w.out.clear();
  w.write_empty_set();
  assert_eq!(w.out, b"~0\r\n");
  w.out.clear();
  w.write_empty_map();
  assert_eq!(w.out, b"%0\r\n");
}

#[test]
fn test_strings_and_ints() {
  let mut w = RespMemoryWriter::new();
  w.write_bulk_string(b"abc");
  assert_eq!(w.out, b"$3\r\nabc\r\n");
  w.out.clear();
  w.write_ascii_bulk_string("set");
  assert_eq!(w.out, b"$3\r\nset\r\n");
  w.out.clear();
  w.write_utf8_bulk_string("hello");
  assert_eq!(w.out, b"$5\r\nhello\r\n");
  w.out.clear();
  w.write_simple_string("fast");
  assert_eq!(w.out, b"+fast\r\n");
  w.out.clear();
  w.write_simple_string_bytes(b"OK");
  assert_eq!(w.out, b"+OK\r\n");
  w.out.clear();
  w.write_int32(-2);
  assert_eq!(w.out, b":-2\r\n");
  w.out.clear();
  w.write_int64(9223372036854775807);
  assert_eq!(w.out, b":9223372036854775807\r\n");
  w.out.clear();
  w.write_int32_as_bulk_string(42);
  assert_eq!(w.out, b"$2\r\n42\r\n");
  w.out.clear();
  w.write_int64_as_bulk_string(-100);
  assert_eq!(w.out, b"$4\r\n-100\r\n");
  w.out.clear();
  w.write_array_item(7);
  assert_eq!(w.out, b"$1\r\n7\r\n");
  w.out.clear();
  w.write_integer_from_bytes(b"12345");
  assert_eq!(w.out, b":12345\r\n");
  w.out.clear();
  w.write_zero();
  assert_eq!(w.out, b":0\r\n");
  w.out.clear();
  w.write_one();
  assert_eq!(w.out, b":1\r\n");
  w.out.clear();
  w.write_empty_array();
  assert_eq!(w.out, b"*0\r\n");
}

#[test]
fn test_double_numeric_and_bulk() {
  let mut w2 = RespMemoryWriter::<Resp2>::new();
  w2.write_double_numeric(1.25);
  assert_eq!(w2.out, b"$4\r\n1.25\r\n");
  w2.out.clear();
  w2.write_double_numeric(0.0);
  assert_eq!(w2.out, b"$1\r\n0\r\n");
  w2.out.clear();
  w2.write_double_numeric(2.0);
  assert_eq!(w2.out, b"$1\r\n2\r\n");
  w2.out.clear();
  w2.write_double_numeric(f64::NAN);
  assert_eq!(w2.out, b"$3\r\nnan\r\n");
  w2.out.clear();
  w2.write_double_numeric(f64::INFINITY);
  assert_eq!(w2.out, b"$3\r\ninf\r\n");
  w2.out.clear();
  w2.write_double_numeric(f64::NEG_INFINITY);
  assert_eq!(w2.out, b"$4\r\n-inf\r\n");

  let mut w3 = RespMemoryWriter::<Resp3>::new_p();
  w3.write_double_numeric(1.25);
  assert_eq!(w3.out, b",1.25\r\n");
  w3.out.clear();
  w3.write_double_numeric(0.0);
  assert_eq!(w3.out, b",0\r\n");
  w3.out.clear();
  w3.write_double_numeric(2.0);
  assert_eq!(w3.out, b",2\r\n");
  w3.out.clear();
  w3.write_double_numeric(f64::NAN);
  assert_eq!(w3.out, b",nan\r\n");
  w3.out.clear();
  w3.write_double_numeric(f64::INFINITY);
  assert_eq!(w3.out, b",inf\r\n");
  w3.out.clear();
  w3.write_double_numeric(f64::NEG_INFINITY);
  assert_eq!(w3.out, b",-inf\r\n");
}

#[test]
fn test_bool_and_verbatim_and_error() {
  let mut w2 = RespMemoryWriter::<Resp2>::new();
  w2.write_bool(true);
  assert_eq!(w2.out, b":1\r\n");
  w2.out.clear();
  w2.write_bool(false);
  assert_eq!(w2.out, b":0\r\n");
  w2.out.clear();
  w2.write_large_verbatim_string(b"hello", b"txt");
  assert_eq!(w2.out, b"$5\r\nhello\r\n");
  w2.out.clear();
  w2.write_error("some error");
  assert_eq!(w2.out, b"-some error\r\n");
  w2.out.clear();
  w2.write_error_with_prefix("ERR", "prefixed error");
  assert_eq!(w2.out, b"-ERR prefixed error\r\n");
  w2.out.clear();
  w2.write_error_bytes(b"ERR raw byte error");
  assert_eq!(w2.out, b"-ERR raw byte error\r\n");

  let mut w3 = RespMemoryWriter::<Resp3>::new_p();
  w3.write_bool(true);
  assert_eq!(w3.out, b"#t\r\n");
  w3.out.clear();
  w3.write_bool(false);
  assert_eq!(w3.out, b"#f\r\n");
  w3.out.clear();
  w3.write_large_verbatim_string(b"hello", b"txt");
  assert_eq!(w3.out, b"=9\r\ntxt:hello\r\n");
}

#[test]
fn test_resp_writer_borrowed_and_ext() {
  let mut buf = Vec::new();
  {
    let mut writer = RespWriter::<_, Resp3>::new_ref_p(&mut buf);
    writer.write_simple_string("PONG");
    writer.write_int64(42);
    writer.write_double(3.125);
  }
  assert_eq!(buf, b"+PONG\r\n:42\r\n,3.125\r\n");

  let mut ext_buf = Vec::new();
  ext_buf.write_resp_int(10);
  ext_buf.write_resp_bulk_string(b"item");
  ext_buf.write_resp_array_len(1);
  ext_buf.write_resp_error("fail");
  ext_buf.write_resp_simple_string("OK");
  ext_buf.write_resp_null();
  assert_eq!(
    ext_buf,
    b":10\r\n$4\r\nitem\r\n*1\r\n-ERR fail\r\n+OK\r\n$-1\r\n"
  );
}

#[test]
fn test_generic_serializable_and_writer_features() {
  use wresp::{IRespSerializable, RespBuffer, RespProtocol};

  struct Sample(i64, &'static str);

  impl IRespSerializable for Sample {
    fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
      writer.write_array_len(2);
      writer.write_int64(self.0);
      writer.write_ascii_bulk_string(self.1);
    }
  }

  let sample = Sample(123, "hello");

  // 1) 序列化至拥有独立堆缓冲的 RespMemoryWriter (Resp2)
  let mut mem = RespMemoryWriter::default();
  sample.to_resp_format(&mut mem);
  assert_eq!(mem.as_ref(), b"*2\r\n:123\r\n$5\r\nhello\r\n");

  // 2) 序列化至借用缓冲的 RespWriter (Resp3)
  let mut target = Vec::new();
  {
    let mut ref_writer = RespWriter::<_, Resp3>::new_ref_p(&mut target);
    sample.to_resp_format(&mut ref_writer);
  }
  assert_eq!(target, b"*2\r\n:123\r\n$5\r\nhello\r\n");
}
