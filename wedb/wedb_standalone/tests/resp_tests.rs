mod support;

use core::str;

use support::with_batch;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}
use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND},
  crc64::hash as rdb_crc64_hash,
  time::now_ticks,
};
use wnode::resp::{
  basic_commands::IncrCmd,
  key_admin_commands::{ExpireCmd, TtlCmd},
  resp_server_session::RespServerSession,
  ttl_sync::{put_ttl_sync, ttl_of_sync},
};

/// test/standalone/Garnet.test/RespTests.cs:SingleSetGet
#[test]
fn single_set_get() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"mykey", b"abcdefg"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"mykey"], batch, &mut out).unwrap();
    assert_eq!(out, b"$7\r\nabcdefg\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"missing"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:StringCommandsWrongArityReturnErrorAndKeepSessionAlive
#[test]
fn string_commands_wrong_arity_return_error_and_keep_session_alive() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR wrong number of arguments for 'SET' command\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:AppendTest
#[test]
fn append_test() {
  with_batch(|s, batch| {
    let key = b"myKey";
    let val = b"myKeyValue";
    let val2 = b"myKeyValue2";

    let mut out = Vec::new();
    s.network_set(&[key, val], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_append(&[key, val2], batch, &mut out).unwrap();
    assert_eq!(out, format!(":{}\r\n", val.len() + val2.len()).into_bytes());

    let mut out = Vec::new();
    s.network_get(&[key], batch, &mut out).unwrap();
    let mut expected = format!("${}\r\n", val.len() + val2.len()).into_bytes();
    expected.extend_from_slice(val);
    expected.extend_from_slice(val2);
    expected.extend_from_slice(b"\r\n");
    assert_eq!(out, expected);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:StrlenTest
#[test]
fn strlen_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"mykey", b"foo bar"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_strlen(&[b"mykey"], batch, &mut out).unwrap();
    assert_eq!(out, b":7\r\n");

    let mut out = Vec::new();
    s.network_strlen(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetIfNotExistWithExistingKey
#[test]
fn set_if_not_exist_with_existing_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"key1", b"val1"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_setnx(&[b"key1", b"val2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"key1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$4\r\nval1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetIfNotExistWithNewKey
#[test]
fn set_if_not_exist_with_new_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setnx(&[b"newkey", b"val"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"newkey"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\nval\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetNXCorrectResponse
#[test]
fn set_nx_correct_response() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setnx(&[b"key1", b"2"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_setnx(&[b"key1", b"3"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"key1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\n2\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleIncr
#[test]
fn single_incr() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"key1", b"-100000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"key1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-99999\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"key1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$6\r\n-99999\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleIncrBy
#[test]
fn single_incr_by() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"key1", b"1000"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_increment(IncrCmd::IncrBy, &[b"key1", b"41"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1041\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"key1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$4\r\n1041\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleDecr
#[test]
fn single_decr() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"key1", b"1000"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_increment(IncrCmd::Decr, &[b"key1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":999\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleDecrBy
#[test]
fn single_decr_by() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"key1", b"1000"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_increment(IncrCmd::DecrBy, &[b"key1", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":999\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SimpleIncrementInvalidValue
#[test]
fn simple_increment_invalid_value() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment(IncrCmd::IncrBy, &[b"n", b"x"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    s.network_set(&[b"s", b"abc"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"s"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SimpleIncrementOverflow
#[test]
fn simple_increment_overflow() {
  with_batch(|s, batch| {
    s.network_set(&[b"m", b"9223372036854775807"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"m"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"m"], batch, &mut out).unwrap();
    assert_eq!(out, b"$19\r\n9223372036854775807\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SimpleIncrementByFloat
#[test]
fn simple_increment_by_float() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"10.5"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.5\r\n");

    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"0.1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.6\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SimpleIncrementByFloatWithInvalidFloat
#[test]
fn simple_increment_by_float_with_invalid_float() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"nan"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");

    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"abc"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SimpleIncrementByFloatWithOutOfRangeFloat
#[test]
fn simple_increment_by_float_with_out_of_range_float() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"inf"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR increment would produce NaN or Infinity\r\n");

    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"1e999"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR increment would produce NaN or Infinity\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetExpiry
#[test]
fn set_expiry() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setex(&[b"k", b"100", b"v"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
    assert!(ttl > now_ticks() + 99 * TICKS_PER_SECOND);

    let mut out = Vec::new();
    s.network_setex(&[b"k", b"0", b"v"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR invalid expire time in 'set' command\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetExpiryHighPrecision
#[test]
fn set_expiry_high_precision() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_psetex(&[b"p", b"500", b"v"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let ttl = ttl_of_sync(batch, b"p").unwrap().unwrap().unwrap();
    assert!(ttl > now_ticks() + 400 * TICKS_PER_MILLISECOND);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetExpiryWithExpireOptions
#[test]
fn get_expiry_with_expire_options() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_getex(&[b"k", b"EX", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");
    let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
    assert!(ttl > now_ticks() + 99 * TICKS_PER_SECOND);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetExpiryWithPersistOptions
#[test]
fn get_expiry_with_persist_options() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();
    let _ = put_ttl_sync(batch, b"k", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();

    let mut out = Vec::new();
    s.network_getex(&[b"k", b"PERSIST"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");
    assert_eq!(ttl_of_sync(batch, b"k").unwrap(), Some(None));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetExpiryOutOfRangeIsRejectedWithoutKillingSession
#[test]
fn get_expiry_out_of_range_is_rejected_without_killing_session() {
  with_batch(|s, batch| {
    let key = b"GetExpiryOutOfRangeIsRejectedWithoutKillingSession";
    let mut out = Vec::new();
    s.network_set(&[key, b"Value"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut itoa_buf = itoa::Buffer::new();
    let max_i64 = itoa_buf.format(i64::MAX).as_bytes();

    // 1. EX 秒数超大：超过 MAX_TIMESPAN_SECONDS
    out.clear();
    let alive = s
      .network_getex(&[key, b"EX", max_i64], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"-ERR invalid expire time in 'getex' command\r\n");

    // 2. PX 毫秒数超大：超过 MAX_TIMESPAN_MILLISECONDS
    out.clear();
    let alive = s
      .network_getex(&[key, b"PX", max_i64], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"-ERR invalid expire time in 'getex' command\r\n");

    // 3. EXAT Unix 秒数超大：超过 MAX_UNIX_TIME_SECONDS
    out.clear();
    let alive = s
      .network_getex(&[key, b"EXAT", max_i64], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"-ERR invalid expire time in 'getex' command\r\n");

    // 4. PXAT Unix 毫秒数超大：超过 MAX_UNIX_TIME_MILLISECONDS
    out.clear();
    let alive = s
      .network_getex(&[key, b"PXAT", max_i64], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"-ERR invalid expire time in 'getex' command\r\n");

    let ticks_space = i64::MAX - now_ticks();
    let overflow_seconds = (ticks_space / TICKS_PER_SECOND) + 1;
    let overflow_milliseconds = (ticks_space / TICKS_PER_MILLISECOND) + 1;

    let mut buf_sec = itoa::Buffer::new();
    let overflow_sec_bytes = buf_sec.format(overflow_seconds).as_bytes();
    let mut buf_ms = itoa::Buffer::new();
    let overflow_ms_bytes = buf_ms.format(overflow_milliseconds).as_bytes();

    // 5. EX 日期溢出：未超过 MAX_TIMESPAN_SECONDS 但 now + ts_ticks 溢出
    out.clear();
    let alive = s
      .network_getex(&[key, b"EX", overflow_sec_bytes], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(
      out,
      b"-ERR expire time overflows date in 'getex' command\r\n"
    );

    // 6. PX 日期溢出：未超过 MAX_TIMESPAN_MILLISECONDS 但 now + ts_ticks 溢出
    out.clear();
    let alive = s
      .network_getex(&[key, b"PX", overflow_ms_bytes], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(
      out,
      b"-ERR expire time overflows date in 'getex' command\r\n"
    );

    // TTL 未变（未设置 TTL）且 key 依然存在
    assert_eq!(ttl_of_sync(batch, key).unwrap(), Some(None));

    out.clear();
    s.network_exists(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_get(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nValue\r\n");

    // 7. 缺失键 GETEX：返回 $-1\r\n 且不创建键或 TTL
    out.clear();
    let alive = s
      .network_getex(&[b"non_existent_key", b"EX", b"100"], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"$-1\r\n");
    assert_eq!(ttl_of_sync(batch, b"non_existent_key").unwrap(), Some(None));

    // 8. EXAT 过去时刻：折算为移除过期（PERSIST 语义）
    out.clear();
    put_ttl_sync(batch, key, now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    let alive = s
      .network_getex(&[key, b"EXAT", b"1000"], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
    assert_eq!(ttl_of_sync(batch, key).unwrap(), Some(None));

    // 9. 无选项 GETEX key：等同于普通读
    out.clear();
    let alive = s.network_getex(&[key], batch, &mut out).unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetExpiryWitInvalidOptions
#[test]
fn get_expiry_with_invalid_options() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();
    s.network_set(&[key, b"ValueA"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    // EX 0：非正数
    out.clear();
    s.network_getex(&[key, b"EX", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

    // EX 10 PERSIST：参数超限（4 参）
    out.clear();
    s.network_getex(&[key, b"EX", b"10", b"PERSIST"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'GETEX' command\r\n"
    );

    // EX test：非整数
    out.clear();
    s.network_getex(&[key, b"EX", b"test"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

    // EX -1：负数
    out.clear();
    s.network_getex(&[key, b"EX", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

    // PXAT 0：非正数
    out.clear();
    s.network_getex(&[key, b"PXAT", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

    // UNKNOWN（两参缺数值）：报 out of range
    out.clear();
    s.network_getex(&[key, b"UNKNOWN"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

    // UNKNOWN 100（三参合法数值）：报不支持选项
    out.clear();
    s.network_getex(&[key, b"UNKNOWN", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR Unsupported option UNKNOWN\r\n");

    // 0 参
    out.clear();
    s.network_getex(&[], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'GETEX' command\r\n"
    );
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetSetWithExistingKey
#[test]
fn get_set_with_existing_key() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_getset(&[b"k", b"v2"], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\nv1\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\nv2\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetSetWithNewKey
#[test]
fn get_set_with_new_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_getset(&[b"k_new", b"v_new"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"k_new"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nv_new\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetExpiryNx
#[test]
fn set_expiry_nx() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setexnx(&[b"k", b"v1", b"NX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_setexnx(&[b"k", b"v2", b"NX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetXx
#[test]
fn set_xx() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setexnx(&[b"missing", b"v", b"XX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    s.network_set(&[b"k", b"v1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_setexnx(&[b"k", b"v2", b"XX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetGet
#[test]
fn set_get() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_setexnx(&[b"k", b"v2", b"GET"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\nv1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:KeepTtlTest
#[test]
fn keep_ttl_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_setexnx(&[b"t", b"v", b"EX", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_setexnx(&[b"t", b"v2", b"KEEPTTL"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let ttl = ttl_of_sync(batch, b"t").unwrap().unwrap().unwrap();
    assert!(ttl > now_ticks() + 90 * TICKS_PER_SECOND);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SetRangeTest
#[test]
fn set_range_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set_range(&[b"k", b"1", b"ab"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    let mut out = Vec::new();
    s.network_set_range(&[b"k", b"5", b"cd"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":7\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b"$7\r\n\x00ab\x00\x00cd\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetSliceTest
#[test]
fn get_slice_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"\x00ab\x00\x00cd"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-2", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\ncd\r\n");

    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"0", b"999"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$7\r\n\x00ab\x00\x00cd\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PingTest
#[test]
fn ping_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");

  let mut out = Vec::new();
  s.network_ping(&[b"hey"], &mut out).unwrap();
  assert_eq!(out, b"$3\r\nhey\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:AskingTest
#[test]
fn asking_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_asking(&mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(s.session_asking, 2);

  s.session_asking = 0;
  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(wresp::RespCommand::Asking));
  assert_eq!(s.take_output(), b"+OK\r\n");
  assert_eq!(s.session_asking, 2);
}

/// test/standalone/Garnet.test/RespTests.cs:HelloTest1
#[test]
fn hello_test1() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_hello(&[], batch, &mut out).unwrap();
    assert!(out.starts_with(b"*16\r\n$6\r\nserver\r\n$5\r\nredis\r\n"));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:HelloAuthErrorTest
#[test]
fn hello_auth_error_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.process_hello_command(Some(3), b"user", None, batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-WRONGPASS Invalid username/password combination\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:AsyncTest1
#[test]
fn async_test1() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_async(&[b"ON"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR command not supported in RESP2\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:CanSelectCommand
#[test]
fn can_select_command() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_select(&[b"0"], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  s.network_select(&[b"-1"], &mut out).unwrap();
  assert_eq!(out, b"-ERR DB index is out of range.\r\n");

  let mut out = Vec::new();
  s.network_select(&[b"16"], &mut out).unwrap();
  assert_eq!(out, b"-ERR DB index is out of range.\r\n");
}

/// test/standalone/Garnet.test.scripting/MultiDatabaseTests.cs:SWAPDB
#[test]
fn swapdb_command_validation() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_swapdb(&[b"0", b"1"], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  s.network_swapdb(&[b"a", b"0"], &mut out).unwrap();
  assert_eq!(out, b"-ERR invalid first DB index.\r\n");

  let mut out = Vec::new();
  s.network_swapdb(&[b"1", b"b"], &mut out).unwrap();
  assert_eq!(out, b"-ERR invalid second DB index.\r\n");

  let mut out = Vec::new();
  s.network_swapdb(&[b"-1", b"0"], &mut out).unwrap();
  assert_eq!(out, b"-ERR DB index is out of range.\r\n");

  let mut out = Vec::new();
  s.network_swapdb(&[b"17", b"1"], &mut out).unwrap();
  assert_eq!(out, b"-ERR DB index is out of range.\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:SingleRestore32Bit
#[test]
fn single_restore_32bit() {
  with_batch(|s, batch| {
    // 构造有效 DUMP 载荷：0x00 + 长度 + val + 0x0B, 0x00 + CRC
    let mut payload = vec![0x00, 0x03, b'v', b'a', b'l', 0x0b, 0x00];
    let crc = rdb_crc64_hash(&payload);
    payload.extend_from_slice(&crc);

    let mut out = Vec::new();
    s.network_restore(&[b"mykey", b"0", &payload], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"mykey"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\nval\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleDump6Bit
#[test]
fn single_dump_6bit() {
  with_batch(|s, batch| {
    s.network_set(&[b"mykey", b"val"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_dump(&[b"mykey"], batch, &mut out).unwrap();
    assert!(out.starts_with(b"$"));
    let type_pos = out.iter().position(|b| *b == 0).unwrap();
    let payload = &out[type_pos..out.len() - 2];
    assert_eq!(&payload[..5], &[0x00, 0x03, b'v', b'a', b'l']);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:TryRestoreExistingKey
#[test]
fn try_restore_existing_key() {
  with_batch(|s, batch| {
    s.network_set(&[b"mykey", b"orig"], batch, &mut Vec::new())
      .unwrap();

    let mut payload = vec![0x00, 0x03, b'v', b'a', b'l', 0x0b, 0x00];
    let crc = rdb_crc64_hash(&payload);
    payload.extend_from_slice(&crc);

    let mut out = Vec::new();
    s.network_restore(&[b"mykey", b"0", &payload], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-BUSYKEY Target key name already exists.\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleRename
#[test]
fn single_rename() {
  with_batch(|s, batch| {
    s.network_set(&[b"src", b"val"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_rename(&[b"src", b"dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"src"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"dst"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\nval\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleRenameNx
#[test]
fn single_rename_nx() {
  with_batch(|s, batch| {
    s.network_set(&[b"src", b"val"], batch, &mut Vec::new())
      .unwrap();
    s.network_set(&[b"dst", b"exist"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_renamenx(&[b"src", b"dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    let mut out = Vec::new();
    s.network_renamenx(&[b"src", b"new_dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleRenameWithExpiry
#[test]
fn single_rename_with_expiry() {
  with_batch(|s, batch| {
    s.network_set(&[b"src", b"val"], batch, &mut Vec::new())
      .unwrap();
    let _ = put_ttl_sync(batch, b"src", now_ticks() + 100 * TICKS_PER_SECOND).unwrap();

    let mut out = Vec::new();
    s.network_rename(&[b"src", b"dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    assert_eq!(ttl_of_sync(batch, b"src").unwrap(), Some(None));
    let ttl = ttl_of_sync(batch, b"dst").unwrap().unwrap().unwrap();
    assert!(ttl > now_ticks() + 90 * TICKS_PER_SECOND);
  });
}

/// test/standalone/Garnet.test/RespTests.cs:KeyExpireStringTest
#[test]
fn key_expire_string_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Expire, &[b"k", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Pexpire, &[b"k", b"500000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PersistTTLTest
#[test]
fn persist_ttl_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();
    let _ = put_ttl_sync(batch, b"k", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();

    let mut out = Vec::new();
    s.network_ttl(TtlCmd::Ttl, &[b"k"], batch, &mut out)
      .unwrap();
    let ttl = parse_resp_int(&out);
    assert!((55..=60).contains(&ttl));

    let mut out = Vec::new();
    s.network_persist(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_ttl(TtlCmd::Ttl, &[b"k"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PersistTest
#[test]
fn persist_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_persist(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:GetDelTest
#[test]
fn get_del_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_getdel(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");

    let mut out = Vec::new();
    s.network_getdel(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:SingleExists
#[test]
fn single_exists() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"v"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_exists(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_exists(&[b"missing"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:LCSBasicTest
#[test]
fn lcs_basic_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"ohmytext"], batch, &mut Vec::new())
      .unwrap();
    s.network_set(&[b"key2", b"mynewtext"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_lcs(&[b"key1", b"key2"], batch, &mut out).unwrap();
    assert_eq!(out, b"$6\r\nmytext\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:LCSWithLenOption
#[test]
fn lcs_with_len_option() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"hello"], batch, &mut Vec::new())
      .unwrap();
    s.network_set(&[b"key2", b"world"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_lcs(&[b"key1", b"key2", b"LEN"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:LCSWithIdxOption
#[test]
fn lcs_with_idx_option() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"a"], batch, &mut Vec::new())
      .unwrap();
    s.network_set(&[b"key2", b"b"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_lcs(&[b"key1", b"key2", b"IDX"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*4\r\n"));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ClientGetNameBasicTest
#[test]
fn client_get_name_basic_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_clientgetname(&[], &mut out).unwrap();
  assert_eq!(out, b"$-1\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:ClientSetNameTest
#[test]
fn client_set_name_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_clientsetname(&[b"my-client"], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  let mut out = Vec::new();
  s.network_clientgetname(&[], &mut out).unwrap();
  assert_eq!(out, b"$9\r\nmy-client\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:ClientSetInfoSingleOptionTest
#[test]
fn client_set_info_single_option_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_clientsetinfo(&[&b"LIB-NAME"[..], &b"my-lib"[..]], &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:ClientUnblockBasicTest
#[test]
fn client_unblock_basic_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_clientunblock(&[b"99999"], &mut out).unwrap();
  assert_eq!(out, b":0\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:GetRole
#[test]
fn get_role() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_role(&[], &mut out).unwrap();
  assert!(out.starts_with(b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"));
}

#[test]
fn command_count_returns_non_zero() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    let ok = s.network_command_count(&[], batch, &mut out).unwrap();
    assert!(ok);
    let count = parse_resp_int(&out);
    assert!(count > 100, "COMMAND COUNT should be > 100, got {count}");
  });
}

#[test]
fn command_info_returns_valid_data() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    let ok = s
      .network_command_info(&[b"get", b"set"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert!(out.starts_with(b"*2\r\n"));
  });
}

#[test]
fn command_getkeys_extracts_correct_keys() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    let ok = s
      .network_command_getkeys(&[b"MSET", b"k1", b"v1", b"k2", b"v2"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
  });
}

#[test]
fn command_getkeys_with_subcommand() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    let ok = s
      .network_command_getkeys(&[b"OBJECT", b"ENCODING", b"k1"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*1\r\n$2\r\nk1\r\n");

    let mut out_flags = Vec::new();
    let ok = s
      .network_command_getkeysandflags(&[b"OBJECT", b"ENCODING", b"k1"], batch, &mut out_flags)
      .unwrap();
    assert!(ok);
    // 包含键名及只读标志
    assert!(out_flags.starts_with(b"*1\r\n*2\r\n$2\r\nk1\r\n"));
  });
}

#[test]
fn command_docs_returns_metadata() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    let ok = s.network_command_docs(&[b"get"], batch, &mut out).unwrap();
    assert!(ok);
    assert!(out.contains(&b'g'));
  });
}

#[test]
fn dbsize_keys_scan_fallback_to_async() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let ok = s.network_dbsize(&[], &mut out).unwrap();
  assert!(!ok, "dbsize should return false to degrade to async");

  let mut out = Vec::new();
  let ok = s.network_keys(&[b"*"], &mut out).unwrap();
  assert!(!ok, "keys should return false to degrade to async");

  let mut out = Vec::new();
  let ok = s.network_scan(&[b"0"], &mut out).unwrap();
  assert!(!ok, "scan should return false to degrade to async");
}
