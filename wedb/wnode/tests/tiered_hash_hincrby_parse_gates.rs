//! 分层态哈希 HINCRBY/HINCRBYFLOAT 解析基座与无穷门集成测试（自
//! src/resp/objects/tiered_collection_ops/hash.rs 内联测迁入，断言与覆盖
//! 原样保留；暴露面经 [`doc(hidden)`] 测试专用口 `exec_tiered_hash`）

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wcol::{hash::hash_object::HashOperation, types::member_ttl::encode_member};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::objects::tiered_collection_ops::{
  TieredCollectionArgs, TieredCtx, exec_tiered_hash,
};
use wval::GarnetObjectType;

#[test]
fn test_tiered_hash_hincrby_parse_base() {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("tiered_hash_hincrby.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let rt = Runtime::new().unwrap();
  let sess = store.new_session().unwrap();
  let key = b"tiered_hash_key";

  // 灌入初始字段并升阶为分层态
  let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
    (b"f_valid".to_vec(), encode_member(b"10", None)),
    (b"f_str".to_vec(), encode_member(b"abc", None)),
    (b"f_lead0".to_vec(), encode_member(b"010", None)),
    (b"f_space".to_vec(), encode_member(b" 10", None)),
    (b"f_float".to_vec(), encode_member(b"10.5", None)),
  ];

  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::Hash,
    entries,
    i64::MAX,
    false,
  ))
  .unwrap();

  let batch = sess.enter_batch();
  let (mut meta, mut stub) = rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();

  let run_hincrby = |field: &[u8], incr: &[u8], meta: &mut _, stub: &mut _| {
    let mut ctx = TieredCtx::new(meta, stub);
    let mut output = Vec::new();
    let args: &[&[u8]] = &[field, incr];
    let call = TieredCollectionArgs::new(HashOperation::Hincrby, (0, 0), args, 2);
    let res = rt.block_on(exec_tiered_hash(&batch, key, &mut ctx, call, &mut output));
    (res, output)
  };

  // 1. 合法整数旧值：累加成功
  {
    let (res, output) = run_hincrby(b"f_valid", b"5", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":15\r\n");
  }

  // 2. 字符串旧值：解析失败，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
  {
    let (res, output) = run_hincrby(b"f_str", b"5", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
  }

  // 3. 前导零旧值 "010"：NumUtils.TryParse 对位基座接受前导零，010=10，+5=15
  // C# HashIncrement 用 NumUtils.TryParse（Utf8Parser），前导零合法；rust 两层统一对齐
  {
    let (res, output) = run_hincrby(b"f_lead0", b"5", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":15\r\n");
  }

  // 4. 前导空格旧值 " 10"：FromStr 整体消费拒绝，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
  {
    let (res, output) = run_hincrby(b"f_space", b"5", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
  }

  // 5. 浮点数字符串旧值：回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
  {
    let (res, output) = run_hincrby(b"f_float", b"5", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
  }

  // 6. 入参增量非整数：回 RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
  {
    let (res, output) = run_hincrby(b"f_valid", b"abc", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    // 对位 C# RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER（CmdStrings.cs:246 原文带句点）
    assert_eq!(output, b"-ERR value is not an integer or out of range.\r\n");
  }

  // 7. 新字段：直接写入增量原文
  {
    let (res, output) = run_hincrby(b"f_new", b"7", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":7\r\n");
  }

  // 8. 增量 "007"（新字段）：NumUtils.TryParse 接受前导零，存/回原文
  {
    let (res, output) = run_hincrby(b"f_007", b"007", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":007\r\n");
  }

  // 9. 增量 "+7"（新字段）：接受 + 号，存/回原文
  {
    let (res, output) = run_hincrby(b"f_plus7", b"+7", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":+7\r\n");
  }

  // 10. 存量 "007" 累加：与信封态 hincrby_parse_base_envelope 同判据点
  {
    let (res, _) = run_hincrby(b"f_stock007", b"007", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    let (res, output) = run_hincrby(b"f_stock007", b"1", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b":8\r\n");
  }
}

/// 分层态 HINCRBYFLOAT inf 词形门控（对齐 C# TryGetDouble/TryParseWithInfinity 口径）：
/// 增量/存量 "inf"/"+inf"/"-inf" 解析成功后落无穷门，与信封态同文案
#[test]
fn test_tiered_hash_hincrbyfloat_infinity() {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("tiered_hash_hincrbyfloat.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let rt = Runtime::new().unwrap();
  let sess = store.new_session().unwrap();
  let key = b"tiered_hash_float_key";

  let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
    (b"f_inf".to_vec(), encode_member(b"inf", None)),
    (b"f_neg_inf".to_vec(), encode_member(b"-inf", None)),
    (b"f_valid".to_vec(), encode_member(b"1.5", None)),
  ];

  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::Hash,
    entries,
    i64::MAX,
    false,
  ))
  .unwrap();

  let batch = sess.enter_batch();
  let (mut meta, mut stub) = rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();

  let run_float = |field: &[u8], incr: &[u8], meta: &mut _, stub: &mut _| {
    let mut ctx = TieredCtx::new(meta, stub);
    let mut output = Vec::new();
    let args: &[&[u8]] = &[field, incr];
    let call = TieredCollectionArgs::new(HashOperation::Hincrbyfloat, (0, 0), args, 2);
    let res = rt.block_on(exec_tiered_hash(&batch, key, &mut ctx, call, &mut output));
    (res, output)
  };

  // 增量 "inf"：新字段，解析成功后落无穷门（非 NOT_VALID_FLOAT），与信封态同文案
  {
    let (res, output) = run_float(b"f_new_inf", b"inf", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR value is NaN or Infinity\r\n");
  }

  // 增量 "-inf"：同号
  {
    let (res, output) = run_float(b"f_new_ninf", b"-inf", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR value is NaN or Infinity\r\n");
  }

  // 存量 "inf" + 增量 "1"：落增量无穷门，与信封态同文案
  {
    let (res, output) = run_float(b"f_inf", b"1", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR increment would produce NaN or Infinity\r\n");
  }

  // 存量 "-inf" + 增量 "1"：同文案
  {
    let (res, output) = run_float(b"f_neg_inf", b"1", &mut meta, &mut stub);
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR increment would produce NaN or Infinity\r\n");
  }
}
