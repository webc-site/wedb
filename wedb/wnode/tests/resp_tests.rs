use core::str;

use itoa::Buffer;
use wnode_test::with_batch;
use wresp::command::RespCommand;
use wtest_base::a;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}
use wbase::{
  convert::{
    TICKS_PER_MILLISECOND, TICKS_PER_SECOND, unix_time_in_milliseconds_from_ticks,
    unix_time_in_seconds_from_ticks,
  },
  crc64::hash as rdb_crc64_hash,
  time::now_ticks,
};
use wnode::{
  resp::{
    basic_commands::{IncrCmd, ObjectSubCmd},
    key_admin_commands::{ExpireCmd, ExpireTimeCmd, TtlCmd},
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::{put_ttl_sync, ttl_of_sync},
};
use wnode_test::{drain_output, err_frame};
use wresp::cmd_strings::{
  RESP_ERR_ASYNC_REQUIRED, RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE,
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

    // 旧值含前导零非严格整数：C# IsValidNumber → NumUtils.TryReadInt64 拒
    // "01"，报错且保值不落写
    s.network_set(&[b"z", b"01"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"z"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    let mut out = Vec::new();
    s.network_get(&[b"z"], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\n01\r\n");

    // 对照：无前导零旧值正常自增
    s.network_set(&[b"o", b"1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// 对标 C# NetworkIncrement 下界 arity 门：INCR/DECR 只拒 0 参、
/// INCRBY/DECRBY 只拒 <2 参
#[test]
fn increment_lower_bound_arity() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'INCR' command\r\n"
    );

    let mut out = Vec::new();
    s.network_increment(IncrCmd::IncrBy, &[b"k"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'INCRBY' command\r\n"
    );
  });
}

/// 对标 C# NetworkIncrement 宽松 arity：多余实参被忽略——INCR 第二参是整数
/// 时弃用（RMW 固定增量 ±1），INCRBY 第三参整体不读
#[test]
fn increment_ignores_extra_arguments() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"5"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"k", b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":6\r\n");

    let mut out = Vec::new();
    s.network_increment(IncrCmd::DecrBy, &[b"k", b"3", b"junk"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");
  });
}

/// 对标 C# NetworkIncrement：`if (Count > 1 && !TryGetLong(1))`——INCR/DECR
/// 的第二参虽被弃用，仍须通过整数校验，否则回 not-integer
#[test]
fn increment_rejects_non_integer_extra_argument() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"k", b"extra"], batch, &mut out)
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

/// INCRBYFLOAT 极小值精度：1e-20 不得被定点 17 位截断为 "0"，落盘文本按最短
/// 往返记法存储并可原值还原（对标 C# NumUtils.WriteDouble 的 zmij 最短表示口径；
/// 自 StorageSession 的死 RMW 包装面迁至命令端唯一实现）
#[test]
fn increment_by_float_tiny_value_keeps_roundtrip_text() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"ftiny", b"1e-20"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\n1e-20\r\n");

    let mut out = Vec::new();
    s.network_get(&[b"ftiny"], batch, &mut out).unwrap();
    // RESP bulk 帧 $<len>\r\n<payload>\r\n（len 单位数）
    let payload = &out[4..out.len() - 2];
    assert_ne!(payload, b"0", "1e-20 不得被定点截断为 0，实际 {out:?}");
    assert_eq!(
      str::from_utf8(payload).unwrap().parse::<f64>().ok(),
      Some(1e-20),
      "落盘文本须往返还原"
    );
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

/// 对标 C# NetworkIncrementByFloat 无 arity 门：`INCRBYFLOAT k`（1 参）走
/// TryGetDouble(1) 失败回 not-valid-float；多余实参忽略
#[test]
fn increment_by_float_missing_or_extra_argument() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");

    s.network_set(&[b"f2", b"1.5"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f2", b"0.25", b"junk"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\n1.75\r\n");
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

/// 对标 C# 存储层结果非有限旗标（PrivateMethods.cs TryInPlaceUpdateNumber
/// double 版 `!double.IsFinite(val)` → InvalidTypeError）：有限旧值 + 有限增量
/// 相加溢出无穷大报 ERR value is not a valid float（非旧值 inf 场景的
/// NaN/Infinity 文案），且错误路径不落写
#[test]
fn simple_increment_by_float_result_overflow_reports_not_valid_float() {
  with_batch(|s, batch| {
    s.network_set(&[b"f", b"1.7e308"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"1.7e308"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");

    // C# InvalidTypeError 分支不更新记录：值保持原样
    let mut out = Vec::new();
    s.network_get(&[b"f"], batch, &mut out).unwrap();
    assert_eq!(out, b"$7\r\n1.7e308\r\n");
  });
}

/// 对标 C# PrivateMethods.cs:IsValidDouble：旧值自身为 ±inf（合法 RESP 无穷
/// 词形）→ NaNOrInfinityError 旗标 → NaN/Infinity 文案
#[test]
fn simple_increment_by_float_infinite_old_value_reports_nan_infinity() {
  with_batch(|s, batch| {
    s.network_set(&[b"f", b"inf"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_increment_by_float(&[b"f", b"1.5"], batch, &mut out)
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
    let ttl = ttl_of_sync(batch, b"k").unwrap().value().unwrap().unwrap();
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
    let ttl = ttl_of_sync(batch, b"p").unwrap().value().unwrap().unwrap();
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
    let ttl = ttl_of_sync(batch, b"k").unwrap().value().unwrap().unwrap();
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
    assert_eq!(ttl_of_sync(batch, b"k").unwrap().value(), Some(None));
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

    let mut itoa_buf = Buffer::new();
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

    let mut buf_sec = Buffer::new();
    let overflow_sec_bytes = buf_sec.format(overflow_seconds).as_bytes();
    let mut buf_ms = Buffer::new();
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
    assert_eq!(ttl_of_sync(batch, key).unwrap().value(), Some(None));

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
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
    assert_eq!(
      ttl_of_sync(batch, b"non_existent_key").unwrap().value(),
      Some(None)
    );

    // 8. EXAT 过去时刻：C# tsExpiry.Ticks <= 0 → expiry=0，既有 TTL 保留
    // （RMWMethods.cs GETEX 分支 arg1==0 且非 PERSIST 时 NotUpdated）
    out.clear();
    put_ttl_sync(batch, key, now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    let alive = s
      .network_getex(&[key, b"EXAT", b"1000"], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
    let kept = ttl_of_sync(batch, key)
      .unwrap()
      .value()
      .unwrap()
      .expect("EXAT 过去时刻不得清除既有 TTL");
    assert!(kept > now_ticks() + 59 * TICKS_PER_SECOND);

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
    let ttl = ttl_of_sync(batch, b"t").unwrap().value().unwrap().unwrap();
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
///
/// 附 GETRANGE 负起点归一化怪癖回归（1:1 复刻 C# PrivateMethods.cs 387 行）：
/// start < 0 分支 end == len 经 end % len 折 0 判空，而 start >= 0 分支
/// end == len 钳为 len 不折叠，两分支刻意不同
#[test]
fn get_slice_test() {
  with_batch(|s, batch| {
    s.network_set(&[b"k", b"\x00ab\x00\x00cd"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-2", b"-1"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$2\r\ncd\r\n");

    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"0", b"999"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$7\r\n\x00ab\x00\x00cd\r\n");

    // 怪癖锚点：len=7、start=-2、end=7（end==len 折 0 判空），C# 回空串
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-2", b"7"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$0\r\n\r\n");

    // 常规区间不回归：负起点、end>len 钳为 len
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-3", b"999"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$3\r\n\x00cd\r\n");

    // 负起点 start==end 折 end+1 单字节（C# 391 行）
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-5", b"2"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$1\r\nb\r\n");

    // 正起点 start==end 常规单字节（C# 380 行 end+1）
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"3", b"3"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$1\r\n\x00\r\n");

    // 负终点归一后小于起点判空
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"-3", b"-5"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$0\r\n\r\n");

    // start 超出 len 判空
    let mut out = Vec::new();
    s.network_get_range(&[b"k", b"10", b"20"], batch, &mut out, "GETRANGE")
      .unwrap();
    assert_eq!(out, b"$0\r\n\r\n");
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

/// 订阅会话 PING 差分（对位 C# BasicCommands.cs:986 NetworkPING：
/// isSubscriptionSession && respProtocolVersion==2 → SUSCRIBE_PONG 整帧）
/// RESP2 订阅会话回两元素数组 ["pong",""]，RESP3 订阅会话回普通 +PONG，
/// 带 1 参仍回显 bulk 不受订阅模式影响
#[test]
fn ping_subscription_session_test() {
  let mut s = RespServerSession::default();
  s.is_subscription_session = true;
  s.resp_protocol_version = 2;
  let mut out = Vec::new();
  s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");

  let mut out = Vec::new();
  s.network_ping(&[b"hey"], &mut out).unwrap();
  assert_eq!(out, b"$3\r\nhey\r\n");

  let mut s = RespServerSession::default();
  s.is_subscription_session = true;
  s.resp_protocol_version = 3;
  let mut out = Vec::new();
  s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");
}

/// 普通（非订阅）会话 RESP2 零参 PING 恒 +PONG，SUSCRIBE_PONG 帧不外溢
#[test]
fn ping_non_subscription_session_never_frame() {
  let mut s = RespServerSession::default();
  s.is_subscription_session = false;
  s.resp_protocol_version = 2;
  let mut out = Vec::new();
  s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");
}

/// test/standalone/Garnet.test/RespTests.cs:AskingTest
#[test]
fn asking_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  s.network_asking(&mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  // 命令路径分派同样 +OK
  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(RespCommand::Asking));
  assert_eq!(drain_output(&mut s), b"+OK\r\n");
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
    s.process_hello_command(Some(3), b"user", b"", None, batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-WRONGPASS Invalid username/password combination\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:HelloAuthSyntaxErrorTest
#[test]
fn hello_auth_syntax_error_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // AUTH 缺少密码参数 -> 语法错误
    s.network_hello(&[b"3", b"AUTH", b"alice"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR Syntax error in HELLO option 'AUTH'\r\n");
  });
}

/// 验证 HELLO 3 AUTH user pass SETNAME client 在认证失败时阻断协议升级与 client name 设置
#[test]
fn hello_auth_failure_blocks_protocol_and_name_change() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_hello(
      &[
        b"3",
        b"AUTH",
        b"user",
        b"wrongpass",
        b"SETNAME",
        b"myclient",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"-WRONGPASS Invalid username/password combination\r\n");
    assert_eq!(s.resp_protocol_version, 2);
    assert_eq!(s.client_name, None);
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
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_select(&[b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_select(&[b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR DB index is out of range.\r\n");

    let mut out = Vec::new();
    s.network_select(&[b"16"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR DB index is out of range.\r\n");

    // 线面值域按 C# TryGetInt（SessionParseState.cs:TryGetInt →
    // ParseUtils.cs:TryReadInt，int32 档 + 整段消费 + 拒前导零）逐组对位：
    // 超 int32 属「不是整数」档，int32 域内负数与域内越界属「库号越界」档
    for (raw, want) in [
      // i32 上界本身合法，落 MaxDatabases 门
      ("2147483647", "-ERR DB index is out of range.\r\n"),
      // 超 i32 上界/下界（含 -2147483649）→ 非整数档
      (
        "2147483648",
        "-ERR value is not an integer or out of range.\r\n",
      ),
      (
        "3000000000",
        "-ERR value is not an integer or out of range.\r\n",
      ),
      (
        "18446744073709551615",
        "-ERR value is not an integer or out of range.\r\n",
      ),
      (
        "-2147483649",
        "-ERR value is not an integer or out of range.\r\n",
      ),
      // i32 域内最小负数：C# 合法 int，落 `index < 0` 门
      ("-2147483648", "-ERR DB index is out of range.\r\n"),
      // 前导零、尾随垃圾、空串 → 非整数档
      ("007", "-ERR value is not an integer or out of range.\r\n"),
      ("1a", "-ERR value is not an integer or out of range.\r\n"),
      ("", "-ERR value is not an integer or out of range.\r\n"),
    ] {
      let mut out = Vec::new();
      s.network_select(&[raw.as_bytes()], batch, &mut out)
        .unwrap();
      assert_eq!(out, want.as_bytes(), "SELECT {raw}");
    }
  });
}

/// test/standalone/Garnet.test.scripting/MultiDatabaseTests.cs:SWAPDB
///
/// 异库交换须跨库搬移键值（wkv 前缀模型），同步执行域无法闭环，按降级
/// 约定返回 Ok(false)（分派层写 ERR command requires asynchronous completion）
#[test]
fn swapdb_command_validation() {
  let mut s = RespServerSession::default();
  // 同库交换：C# TrySwapDatabases 短路成功
  let mut out = Vec::new();
  assert!(s.network_swapdb(&[b"0", b"0"], &mut out).unwrap());
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

  // 线面值域同 C# TryGetInt int32 档：超范围/前导零/尾随垃圾落
  // ArrayCommands.cs:NetworkSWAPDB 的 invalid first|second DB index 文案，
  // i32 域内合法值才继续走 DB index is out of range 门
  for (idx1, idx2, want) in [
    ("3000000000", "1", "-ERR invalid first DB index.\r\n"),
    ("2147483648", "1", "-ERR invalid first DB index.\r\n"),
    ("-2147483649", "1", "-ERR invalid first DB index.\r\n"),
    ("007", "1", "-ERR invalid first DB index.\r\n"),
    ("1", "3000000000", "-ERR invalid second DB index.\r\n"),
    ("1", "2147483648", "-ERR invalid second DB index.\r\n"),
    ("1", "1a", "-ERR invalid second DB index.\r\n"),
    ("2147483647", "1", "-ERR DB index is out of range.\r\n"),
    ("1", "-2147483648", "-ERR DB index is out of range.\r\n"),
  ] {
    let mut out = Vec::new();
    s.network_swapdb(&[idx1.as_bytes(), idx2.as_bytes()], &mut out)
      .unwrap();
    assert_eq!(out, want.as_bytes(), "SWAPDB {idx1} {idx2}");
  }

  // 异库交换：校验通过后降级异步（真实搬移由数据库管理器异步域承接）
  let mut out = Vec::new();
  assert!(!s.network_swapdb(&[b"0", b"1"], &mut out).unwrap());
  assert!(out.is_empty());
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
    s.network_rename(&[b"src", b"dst"], batch, None, &mut out)
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
    s.network_renamenx(&[b"src", b"dst"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    let mut out = Vec::new();
    s.network_renamenx(&[b"src", b"new_dst"], batch, None, &mut out)
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
    s.network_rename(&[b"src", b"dst"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    assert_eq!(ttl_of_sync(batch, b"src").unwrap().value(), Some(None));
    let ttl = ttl_of_sync(batch, b"dst")
      .unwrap()
      .value()
      .unwrap()
      .unwrap();
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

/// test/standalone/Garnet.test/RespTests.cs:ExpiretimeWithStringValue
#[test]
fn expiretime_with_string_value() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"test1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Expire, &[b"key1", b"60"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[b"key1"], batch, None, &mut out)
      .unwrap();
    let actual = parse_resp_int(&out);
    assert!(actual >= unix_time_in_seconds_from_ticks(now_ticks()));
    assert!(actual <= unix_time_in_seconds_from_ticks(now_ticks() + 60 * TICKS_PER_SECOND));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ExpiretimeWithUnknownKey
#[test]
fn expiretime_with_unknown_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[b"keyZ"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":-2\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ExpiretimeWithNoKeyExpiration
#[test]
fn expiretime_with_no_key_expiration() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"test1"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[b"key1"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ExpiretimeWithObjectValue
#[test]
fn expiretime_with_object_value() {
  with_batch(|s, batch| {
    for m in [&b"a"[..], b"b", b"c", b"d"] {
      s.list_push(&[b"key1", m], batch, &mut Vec::new(), false)
        .unwrap();
    }
    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Expire, &[b"key1", b"60"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[b"key1"], batch, None, &mut out)
      .unwrap();
    let actual = parse_resp_int(&out);
    assert!(actual >= unix_time_in_seconds_from_ticks(now_ticks()));
    assert!(actual <= unix_time_in_seconds_from_ticks(now_ticks() + 60 * TICKS_PER_SECOND));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ExpiretimeWithNoKeyExpirationForObjectValue
#[test]
fn expiretime_with_no_key_expiration_for_object_value() {
  with_batch(|s, batch| {
    for m in [&b"a"[..], b"b", b"c", b"d"] {
      s.list_push(&[b"key1", m], batch, &mut Vec::new(), false)
        .unwrap();
    }

    let mut out = Vec::new();
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[b"key1"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PExpiretimeWithStingValue
#[test]
fn pexpiretime_with_string_value() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"test1"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Expire, &[b"key1", b"60"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_expiretime(
      ExpireTimeCmd::Pexpiretime,
      &[b"key1"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    let actual = parse_resp_int(&out);
    assert!(actual >= unix_time_in_milliseconds_from_ticks(now_ticks()));
    assert!(actual <= unix_time_in_milliseconds_from_ticks(now_ticks() + 60 * TICKS_PER_SECOND));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PExpiretimeWithUnknownKey
#[test]
fn pexpiretime_with_unknown_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_expiretime(
      ExpireTimeCmd::Pexpiretime,
      &[b"keyZ"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":-2\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PExpiretimeWithNoKeyExpiration
#[test]
fn pexpiretime_with_no_key_expiration() {
  with_batch(|s, batch| {
    s.network_set(&[b"key1", b"test1"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.network_expiretime(
      ExpireTimeCmd::Pexpiretime,
      &[b"key1"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":-1\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PExpiretimeWithObjectValue
#[test]
fn pexpiretime_with_object_value() {
  with_batch(|s, batch| {
    for m in [&b"a"[..], b"b", b"c", b"d"] {
      s.list_push(&[b"key1", m], batch, &mut Vec::new(), false)
        .unwrap();
    }
    let mut out = Vec::new();
    s.network_expire(ExpireCmd::Expire, &[b"key1", b"60"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_expiretime(
      ExpireTimeCmd::Pexpiretime,
      &[b"key1"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    let actual = parse_resp_int(&out);
    assert!(actual >= unix_time_in_milliseconds_from_ticks(now_ticks()));
    assert!(actual <= unix_time_in_milliseconds_from_ticks(now_ticks() + 60 * TICKS_PER_SECOND));
  });
}

/// test/standalone/Garnet.test/RespTests.cs:PExpiretimeWithNoKeyExpirationForObjectValue
#[test]
fn pexpiretime_with_no_key_expiration_for_object_value() {
  with_batch(|s, batch| {
    for m in [&b"a"[..], b"b", b"c", b"d"] {
      s.list_push(&[b"key1", m], batch, &mut Vec::new(), false)
        .unwrap();
    }

    let mut out = Vec::new();
    s.network_expiretime(
      ExpireTimeCmd::Pexpiretime,
      &[b"key1"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":-1\r\n");
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
    s.network_ttl(TtlCmd::Ttl, &[b"k"], batch, None, &mut out)
      .unwrap();
    let ttl = parse_resp_int(&out);
    assert!((55..=60).contains(&ttl));

    let mut out = Vec::new();
    s.network_persist(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_ttl(TtlCmd::Ttl, &[b"k"], batch, None, &mut out)
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
    s.network_exists(&[b"k"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_exists(&[b"missing"], batch, None, &mut out)
      .unwrap();
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

    // 任一键缺失：OK + 空 bulk string（C# LCSInternal NOTFOUND 照常写出空应答）
    let mut out = Vec::new();
    s.network_lcs(&[b"no_a", b"no_b"], batch, &mut out).unwrap();
    assert_eq!(out, b"$0\r\n\r\n");
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

/// 差分钉住父子命令组合查找（rust 有意增强，声明见 prepare_command_keys_context）：
/// `COMMAND GETKEYS OBJECT ENCODING k` rust 组合查中 OBJECT_ENCODING 规格回键，
/// C# TryGetSimpleCommandInfo 只按首参单名落无 KeySpecifications 的 OBJECT parent、
/// 回 -The command has no key arguments；`COMMAND GETKEYS CONFIG GET k` 的 CONFIG_GET
/// 两侧目录均无键规格，同报错帧（无分叉）。
#[test]
fn command_getkeys_parent_sub_lookup() {
  with_batch(|s, batch| {
    // 子命令规格在场：rust 组合查找成功提取键（C# 同输入报错，有意分叉）
    let mut out = Vec::new();
    let ok = s
      .network_command_getkeys(&[b"OBJECT", b"ENCODING", b"kk"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*1\r\n$2\r\nkk\r\n");

    // 子命令规格缺席：两侧同回 no-key-args 错误帧
    let mut out = Vec::new();
    let ok = s
      .network_command_getkeys(&[b"CONFIG", b"GET", b"k"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"-The command has no key arguments\r\n");

    let mut out = Vec::new();
    let ok = s
      .network_command_getkeysandflags(&[b"CONFIG", b"GET", b"k"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"-The command has no key arguments\r\n");
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

/// test/standalone/Garnet.test/RespTests.cs:CanDoExpireNX_XX_GT_LT
#[test]
fn expire_options_nx_gt_lt() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(a![b"ex7", b"v"], batch, &mut out).unwrap();

    // NX：无 TTL 时成功，已有 TTL 时拒绝
    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"100", b"NX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"100", b"NX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":0\r\n");

    // GT：新 TTL 不大于现有 → :0
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"50", b"GT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // LT：新 TTL 小于现有 → :1
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"10", b"LT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // NX+XX 不兼容
    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"10", b"NX", b"XX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
    );

    // 负过期 → INVALID_EXPIRE_TIME
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR invalid expire time, must be >= 0\r\n");
  });
}

/// test/standalone/Garnet.test/RespTests.cs:ObjectEncodingTypesAndExpiration
#[test]
fn object_encoding_types_and_expiration() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // 1. 普通字符串 -> raw
    s.network_set(&[b"str_key", b"hello"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_object(ObjectSubCmd::Encoding, &[b"str_key"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\nraw\r\n");

    // 2. Hash -> hashtable
    out.clear();
    s.hash_set(&[b"hash_key", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_object(
      ObjectSubCmd::Encoding,
      &[b"hash_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$9\r\nhashtable\r\n");

    // 3. List -> quicklist
    out.clear();
    s.list_push(&[b"list_key", b"e1"], batch, &mut out, true)
      .unwrap();
    out.clear();
    s.network_object(
      ObjectSubCmd::Encoding,
      &[b"list_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$9\r\nquicklist\r\n");

    // 4. Set -> hashtable
    out.clear();
    s.set_add(&[b"set_key", b"m1"], batch, &mut out).unwrap();
    out.clear();
    s.network_object(ObjectSubCmd::Encoding, &[b"set_key"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"$9\r\nhashtable\r\n");

    // 5. SortedSet -> skiplist
    out.clear();
    s.sorted_set_add(&[b"zset_key", b"1.0", b"m1"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_object(
      ObjectSubCmd::Encoding,
      &[b"zset_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$8\r\nskiplist\r\n");

    // 6. 不存在的键 -> nil ($-1\r\n)
    out.clear();
    s.network_object(
      ObjectSubCmd::Encoding,
      &[b"nonexistent_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 7. 过期键（内存过期） -> nil ($-1\r\n)
    out.clear();
    s.network_set(&[b"exp_key", b"val"], batch, &mut out)
      .unwrap();
    let _ = put_ttl_sync(batch, b"exp_key", now_ticks() - 1000).unwrap();
    out.clear();
    s.network_object(ObjectSubCmd::Encoding, &[b"exp_key"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 8. 过期键（EXPIREAT 过去时间戳删除） -> nil ($-1\r\n)
    out.clear();
    s.network_set(&[b"exp_key2", b"val"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_expire(ExpireCmd::Expireat, &[b"exp_key2", b"1"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_object(
      ObjectSubCmd::Encoding,
      &[b"exp_key2"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 9. REFCOUNT: 存在键 -> :1, 不存在键 -> nil
    out.clear();
    s.network_object(ObjectSubCmd::Refcount, &[b"str_key"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    s.network_object(
      ObjectSubCmd::Refcount,
      &[b"nonexistent_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 10. IDLETIME: 存在键 -> :0, 不存在键 -> nil
    out.clear();
    s.network_object(ObjectSubCmd::Idletime, &[b"str_key"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    s.network_object(
      ObjectSubCmd::Idletime,
      &[b"nonexistent_key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // 11. FREQ: 恒不支持报错
    out.clear();
    s.network_object(ObjectSubCmd::Freq, &[b"str_key"], batch, None, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-ERR OBJECT FREQ is not supported"));

    // 12. OBJECT HELP: 11 行应答
    out.clear();
    s.network_objecthelp(&[], batch, &mut out).unwrap();
    assert!(out.starts_with(b"*11\r\n"));

    // 13. 参数数量错误
    out.clear();
    s.network_object(ObjectSubCmd::Encoding, &[], batch, None, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'object|encoding' command\r\n"
    );
  });
}

/// test/standalone/Garnet.test/RespTests.cs:AsyncCommandResp2AndResp3
#[test]
fn async_command_resp2_and_resp3() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // 默认 RESP2 下：ASYNC ON/OFF/BARRIER 均报错
    assert_eq!(s.resp_protocol_version, 2);
    s.network_async(&[b"ON"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR command not supported in RESP2\r\n");

    out.clear();
    s.network_async(&[b"OFF"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR command not supported in RESP2\r\n");

    out.clear();
    s.network_async(&[b"BARRIER"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR command not supported in RESP2\r\n");

    // 升级至 RESP3
    out.clear();
    s.network_hello(&[b"3"], batch, &mut out).unwrap();
    assert_eq!(s.resp_protocol_version, 3);

    // RESP3 下：本仓不移植 C# AsyncProcessor 面（js/check/ignore/server.yml 登记），
    // ON/OFF/BARRIER 三臂统一回异步完成通道错误，不再伪造 +OK
    let async_required = err_frame(RESP_ERR_ASYNC_REQUIRED);
    for param in [b"ON".as_slice(), b"OFF", b"BARRIER", b"on", b"Barrier"] {
      out.clear();
      s.network_async(&[param], batch, &mut out).unwrap();
      assert_eq!(out, async_required);
    }

    // 非法参数 -> ERR syntax error
    out.clear();
    s.network_async(&[b"INVALID"], batch, &mut out).unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // 参数数量错误（0 参或多参）
    out.clear();
    s.network_async(&[], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ASYNC' command\r\n"
    );

    out.clear();
    s.network_async(&[b"ON", b"EXTRA"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ASYNC' command\r\n"
    );
  });
}

/// 测试对包含 Hash / List / Set / ZSet 集合对象的键执行字符串读写命令时返回 WRONGTYPE
#[test]
fn string_commands_on_collection_keys_return_wrongtype() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // 构造四种集合对象键
    s.hash_set(&[b"obj_hash", b"field1", b"val1"], batch, &mut out)
      .unwrap();
    s.list_push(&[b"obj_list", b"elem1"], batch, &mut out, true)
      .unwrap();
    s.set_add(&[b"obj_set", b"member1"], batch, &mut out)
      .unwrap();
    s.sorted_set_add(&[b"obj_zset", b"1.0", b"member1"], batch, &mut out)
      .unwrap();

    let wrong_type_err = err_frame(RESP_ERR_WRONG_TYPE);
    const KEYS: [&[u8]; 4] = [b"obj_hash", b"obj_list", b"obj_set", b"obj_zset"];

    for key in KEYS {
      // 1. STRLEN
      out.clear();
      s.network_strlen(&[key], batch, &mut out).unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 2. INCR / DECR / INCRBY / DECRBY
      out.clear();
      s.network_increment(IncrCmd::Incr, &[key], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      out.clear();
      s.network_increment(IncrCmd::Decr, &[key], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      out.clear();
      s.network_increment(IncrCmd::IncrBy, &[key, b"10"], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      out.clear();
      s.network_increment(IncrCmd::DecrBy, &[key, b"10"], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 3. INCRBYFLOAT
      out.clear();
      s.network_increment_by_float(&[key, b"3.14"], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 4. APPEND
      out.clear();
      s.network_append(&[key, b"extra"], batch, &mut out).unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 5. GETRANGE
      out.clear();
      s.network_get_range(&[key, b"0", b"10"], batch, &mut out, "GETRANGE")
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 6. SETRANGE
      out.clear();
      s.network_set_range(&[key, b"0", b"newval"], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 7. GETSET
      out.clear();
      s.network_getset(&[key, b"newval"], batch, &mut out)
        .unwrap();
      assert_eq!(out, wrong_type_err.clone());

      // 8. GET / GETEX
      out.clear();
      s.network_get(&[key], batch, &mut out).unwrap();
      assert_eq!(out, wrong_type_err.clone());

      out.clear();
      s.network_getex(&[key], batch, &mut out).unwrap();
      assert_eq!(out, wrong_type_err.clone());
    }

    // 验证上述命令未破坏底层集合数据
    out.clear();
    s.hash_get(&[b"obj_hash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\nval1\r\n");

    out.clear();
    s.list_length(&[b"obj_list"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_is_member(&[b"obj_set", b"member1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_score(&[b"obj_zset", b"member1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\n1\r\n");
  });
}
