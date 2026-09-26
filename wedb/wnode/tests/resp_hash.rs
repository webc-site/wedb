use core::str;

use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::a;

fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() {
    if frame[pos] != b'$' {
      break;
    }
    let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
    let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
      .unwrap()
      .parse()
      .unwrap();
    let start = len_end + 1;
    items.push(frame[start..start + len].to_vec());
    pos = start + len + 2;
  }
  items
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanSetAndGetOnePair
#[test]
fn can_set_and_get_one_pair() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanSetAndGetMultiPair
#[test]
fn can_set_and_get_multi_pair() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nWorld\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDelSingleField
#[test]
fn can_del_single_field() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_delete(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_delete(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDeleteMultipleFields
#[test]
fn can_delete_multiple_fields() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_delete(&[b"myhash", b"field1", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// HDEL key 零字段接受面（HashCommands.cs:HashDelete 仅判 Count < 1，:405 以
/// startIdx:1 进对象层，零字段 removed=0；缺失键与命中键两态均回 :0）
#[test]
fn hdel_without_fields_returns_zero() {
  with_batch(|s, batch| {
    // 缺失键 → NOTFOUND 臂同款应答 :0
    let mut out = Vec::new();
    s.hash_delete(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 命中非空哈希、零字段 → :0 且键不消失、字段不删
    s.hash_set(&[b"h2", b"f1", b"v1"], batch, &mut out).unwrap();
    out.clear();
    s.hash_delete(&[b"h2"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    s.network_exists(&[b"h2"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckEmptyHashKeyRemoved
#[test]
fn check_empty_hash_key_removed() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"h", b"f1", b"v1"], batch, &mut out).unwrap();

    out.clear();
    s.hash_delete(&[b"h", b"f1"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_exists(&[b"h"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckHashOperationsOnWrongTypeObjectSE
#[test]
fn check_hash_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"str", b"plain"], batch, None, &mut out)
      .unwrap();

    out.clear();
    s.hash_set(&[b"str", b"f", b"v"], batch, &mut out).unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHLen
#[test]
fn can_do_hlen() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_length(&[b"myhash"], batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.hash_length(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoGetAll
#[test]
fn can_do_get_all() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_get_all(&[b"myhash"], batch, &mut out).unwrap();
    let mut items = parse_bulk_array(&out);
    items.sort();
    assert_eq!(
      items,
      vec![
        b"Hello".to_vec(),
        b"World".to_vec(),
        b"field1".to_vec(),
        b"field2".to_vec()
      ]
    );

    out.clear();
    s.hash_get_all(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHExists
#[test]
fn can_do_hexists() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_exists(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_exists(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.hash_exists(&[b"nokey", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHStrLen
#[test]
fn can_do_hstrlen() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_str_length(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":5\r\n");

    out.clear();
    s.hash_str_length(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHKeys
#[test]
fn can_do_hkeys() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_keys(&[b"myhash"], batch, &mut out, true).unwrap();
    let mut keys = parse_bulk_array(&out);
    keys.sort();
    assert_eq!(keys, vec![b"field1".to_vec(), b"field2".to_vec()]);

    out.clear();
    s.hash_keys(&[b"nokey"], batch, &mut out, true).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHVals
#[test]
fn can_do_hvals() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_vals(&[b"myhash"], batch, &mut out).unwrap();
    let mut vals = parse_bulk_array(&out);
    vals.sort();
    assert_eq!(vals, vec![b"Hello".to_vec(), b"World".to_vec()]);
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHMGET
#[test]
fn can_do_hmget() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_get_multiple(&[b"myhash", b"field1", b"nofield"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nHello\r\n$-1\r\n");

    out.clear();
    s.hash_get_multiple(&[b"nokey", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHSETNXCommand
#[test]
fn can_do_hsetnx_command() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set_nx(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_set_nx(&[b"myhash", b"field1", b"World"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHIncrBy
#[test]
fn can_do_hincr_by() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field", b"10"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":11\r\n");

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"-1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"-10"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckHashIncrementDoublePrecision
#[test]
fn check_hash_increment_double_precision() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_increment(&[b"mykey", b"field", b"10.5"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.5\r\n");

    out.clear();
    s.hash_increment(&[b"mykey", b"field", b"0.1"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.6\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanFieldPersistAndGetTimeToLive
#[test]
fn can_field_persist_and_get_time_to_live() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_expire(
      "HEXPIRE",
      &[b"myhash", b"3600", b"FIELDS", b"1", b"field1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");

    out.clear();
    s.hash_time_to_live(
      "HTTL",
      &[b"myhash", b"FIELDS", b"1", b"field1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    let payload = String::from_utf8_lossy(&out);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .unwrap()
      .trim_start_matches(':')
      .parse()
      .unwrap();
    assert!((3590..=3600).contains(&ttl));

    out.clear();
    s.hash_persist(&[b"myhash", b"FIELDS", b"1", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHRANDFIELDCommandLC
#[test]
fn can_do_hrandfield_command_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"coin", b"heads", b"obverse"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_random_field(&[b"coin"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nheads\r\n");

    out.clear();
    s.hash_random_field(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHIncrBy Throws 臂（对 StringValue 字段 HashIncrement，非法整数）
#[test]
fn hincrby_non_integer_field() {
  with_batch(|s, batch| {
    s.hash_set(a![b"h7", b"f", b"str"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.hash_increment(a![b"h7", b"f", b"5"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"-ERR hash value is not an integer.\r\n");
  });
}

/// 信封态 HINCRBY 族解析基座档位（对齐 C# NumUtils.TryParse 前导零/inf 词形语义，
/// 与分层态 test_tiered_hash_hincrby_parse_base 同判据点）
#[test]
fn hincrby_parse_base_envelope() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // 增量 "007"：NumUtils.TryParse 接受前导零，新字段存/回原文
    s.hash_increment(a![b"hb", b"z", b"007"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":007\r\n");

    // 增量 "+7"：接受 + 号，新字段存/回原文
    out.clear();
    s.hash_increment(a![b"hb", b"p", b"+7"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":+7\r\n");

    // 存量 "007"：可解析，累加后按十进制规范化写回
    s.hash_set(a![b"hc", b"f", b"007"], batch, &mut Vec::new())
      .unwrap();
    out.clear();
    s.hash_increment(a![b"hc", b"f", b"1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":8\r\n");

    // HINCRBYFLOAT 增量 inf 词形：解析成功后落无穷门（非 NOT_VALID_FLOAT）
    out.clear();
    s.hash_increment(a![b"hd", b"f", b"inf"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR value is NaN or Infinity\r\n");

    // HINCRBYFLOAT 存量 "inf"：TryParseWithInfinity 放行后落增量无穷门
    s.hash_set(a![b"he", b"f", b"inf"], batch, &mut Vec::new())
      .unwrap();
    out.clear();
    s.hash_increment(a![b"he", b"f", b"1"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR increment would produce NaN or Infinity\r\n");
  });
}

/// 案一（zcode-r151c-hincrby）信封态 HINCRBYFLOAT 求和溢出特值词形锁：与分层态
/// tiered_field_ttl.rs::tiered_hincrbyfloat_sum_overflow_inf_wordform 同判据点、
/// 双臂逐字节全等（§80 同族第二消费位，deviations §1 尾回指注在册）。1e308+1e308
/// 和逾 DBL_MAX，双侧均无求和结果门（真 Redis would-produce 拒改形不复刻），应答与
/// 落盘恒走 format_double 单源锁现树 3 字节 "inf"——禁按 C# TryFormat G 的
/// "Infinity" 八字节回改（该形仅在册对照，不入帧锁），亦禁补门
#[test]
fn hincrbyfloat_sum_overflow_inf_wordform_envelope() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"hovf", b"f", b"1e308"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();

    // 首轮求和溢出：无结果门，改值+落盘+应答同串 "inf"（$3 三字节）
    s.hash_increment(a![b"hovf", b"f", b"1e308"], batch, &mut out, true)
      .unwrap();
    assert_eq!(
      out, b"$3\r\ninf\r\n",
      "求和溢出锁现树 inf 词形，禁回改 Infinity"
    );
    out.clear();

    // 落盘复验：同批重读存储载荷（obj_load_typed_sync），serialize_wire
    // （hash_object.rs:217 起）不改写词形，字段仍逐字节 "inf"、不漂第三形
    s.hash_get(&[b"hovf", b"f"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\ninf\r\n");
    out.clear();

    // HSTRLEN 按 rust 现树 3 字节形锁（C# 侧 "Infinity" 8 字节仅在册对照不入帧；
    // 记账面 round_up_ptr(3)=round_up_ptr(8)=8 同槽不外显，wbase/src/heap.rs:48-49）
    s.hash_str_length(&[b"hovf", b"f"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");
    out.clear();

    // 次轮增量：存量 "inf" 经 §2 词形放行后落存量无穷门（RESP_ERR_GENERIC_
    // NAN_INFINITY_INCR），错帧不改值（求和溢出形与存量无穷形收敛同款门）
    s.hash_increment(a![b"hovf", b"f", b"1"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR increment would produce NaN or Infinity\r\n");
    out.clear();
    s.hash_get(&[b"hovf", b"f"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\ninf\r\n", "存量门错帧不得覆写 inf 文本");
    out.clear();

    // 整数族存量门对特值文本同拒（错帧存续面由 should_write_back '-' 门守住，
    // hash_read_ttl_solidify.rs:309-341 在册对照）
    s.hash_increment(a![b"hovf", b"f", b"1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"-ERR hash value is not an integer.\r\n");
    out.clear();
    s.hash_get(&[b"hovf", b"f"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\ninf\r\n");
  });
}
