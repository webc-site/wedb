mod support;

use core::str;

use support::with_batch;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

const RESP_ERR_WRONG_TYPE_HLL: &[u8] =
  b"-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n";

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:SimpleHyperLogLogAddCount
#[test]
fn simple_hyper_log_log_add_count() {
  with_batch(|sess, batch| {
    let data: [&[u8]; 6] = [b"a", b"b", b"c", b"d", b"e", b"f"];
    let key: &[u8] = b"hllKey";

    // HLL updated
    for item in &data {
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key, item], batch, &mut out)
        .unwrap();
      assert_eq!(parse_resp_int(&out), 1);
    }

    // HLL not updated
    for item in &data {
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key, item], batch, &mut out)
        .unwrap();
      assert_eq!(parse_resp_int(&out), 0);
    }

    // estimate cardinality
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 6);
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:SimpleHyperLogLogMerge
#[test]
fn simple_hyper_log_log_merge() {
  with_batch(|sess, batch| {
    let key_x: &[u8] = b"x";
    let key_y: &[u8] = b"y";
    let key_w: &[u8] = b"w";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_x, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_x], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_y, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_y], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 5);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_y], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 7);
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogSimpleInvalidHLLTypeTest
#[test]
fn hyper_log_log_simple_invalid_hll_type_test() {
  with_batch(|sess, batch| {
    let key_x: &[u8] = b"x";
    let key_y: &[u8] = b"y";
    let key_w: &[u8] = b"w";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_x, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_y, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let _ = batch.try_upsert_sync(key_w, b"100");

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_w, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_y, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_w, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_x, key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogMultiCountTest
#[test]
fn hyper_log_log_multi_count_test() {
  with_batch(|sess, batch| {
    let key_a: &[u8] = b"HyperLogLogMultiCountTestA";
    let key_b: &[u8] = b"HyperLogLogMultiCountTestB";
    let key_c: &[u8] = b"HyperLogLogMultiCountTestC";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_a, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    sess
      .hyper_log_log_add(&[key_b, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    sess
      .hyper_log_log_add(
        &[key_c, b"a", b"b", b"c", b"d", b"e", b"f"],
        batch,
        &mut out,
      )
      .unwrap();

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_a], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_b], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 5);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_c], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 6);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_a, key_b, key_c], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 11);
  });
}
