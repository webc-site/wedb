use core::str;

use wkv::StoreResult;
use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE_HLL;

fn parse_resp_int(out: &[u8]) -> i64 {
  eprintln!("out = {:?}", String::from_utf8_lossy(out));
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

/// 期望 HLL WRONGTYPE 帧：由 wresp 单点常量派生（对标 CmdStrings.RESP_ERR_WRONG_TYPE_HLL）
fn hll_wrongtype_frame() -> Vec<u8> {
  err_frame(RESP_ERR_WRONG_TYPE_HLL)
}

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
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_y, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_w, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_x, key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());
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

/// PFADD 零元素：对标 C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
/// 不建键，直答 :0
#[test]
fn pfadd_without_elements_skips_storage() {
  with_batch(|sess, batch| {
    let key: &[u8] = b"pfadd-no-elem";

    let mut out = Vec::new();
    sess.hyper_log_log_add(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 不建键：String 域内存直读确认不存在
    assert_eq!(
      batch.try_read_sync(key, |_| true).unwrap(),
      StoreResult::NotFound,
      "零元素 PFADD 不得创建键"
    );
  });
}

/// PFMERGE 零源：对标 C# HyperLogLogMerge 源循环零次，不 GET 不 SET，dest 不建
/// 亦不探测类型，直答 +OK
#[test]
fn pfmerge_without_sources_skips_storage() {
  with_batch(|sess, batch| {
    let dest: &[u8] = b"pfmerge-no-src";

    // 缺失 dest：+OK 且不建键
    let mut out = Vec::new();
    sess.hyper_log_log_merge(&[dest], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(
      batch.try_read_sync(dest, |_| true).unwrap(),
      StoreResult::NotFound,
      "零源 PFMERGE 不得创建 dest"
    );

    // String 键 dest：零源不探测 WRONGTYPE，仍 +OK（C# 循环零次同款）
    let _ = batch.try_upsert_sync(b"pfmerge-str", b"100");
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[b"pfmerge-str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  });
}

/// PFCOUNT 单键短路：稀疏、稠密、缺失键以及多键联合计数测试
#[test]
fn pfcount_single_key_sparse_and_dense_and_missing() {
  with_batch(|sess, batch| {
    let key_missing: &[u8] = b"hll-missing";
    let key_sparse: &[u8] = b"hll-sparse";
    let key_dense: &[u8] = b"hll-dense";

    // 1. 缺失键单键 PFCOUNT 返回 0
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_missing], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 0);

    // 2. 稀疏单键 PFCOUNT
    for i in 0..10 {
      let elem = format!("sparse_elem_{i}");
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key_sparse, elem.as_bytes()], batch, &mut out)
        .unwrap();
    }
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_sparse], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 10);

    // 3. 稠密单键 PFCOUNT（插入大量不同元素触发稀疏转稠密）
    for i in 0..2000 {
      let elem = format!("dense_elem_{i}");
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key_dense, elem.as_bytes()], batch, &mut out)
        .unwrap();
    }
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_dense], batch, &mut out)
      .unwrap();
    let dense_card = parse_resp_int(&out);
    // HLL 估算误差在合理范围内（2000 左右）
    assert!((dense_card - 2000).abs() < 100);

    // 4. 多键联合 PFCOUNT（包含缺失键、稀疏键、稠密键）
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_missing, key_sparse, key_dense], batch, &mut out)
      .unwrap();
    let union_card = parse_resp_int(&out);
    assert!((union_card - (10 + 2000)).abs() < 100);
  });
}
