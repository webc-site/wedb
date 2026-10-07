#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 范围索引（RI.CREATE / RI.COUNT 等）会话参数解析集成测试
//! （对应 libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs）

use wnode::resp::range_index::resp_server_session_range_index::{
  parse_ricreate_options, ri_option_long,
};

/// ri_option_long 对标 C# parseState.GetLong 两态（ParseUtils.cs:64 +
/// RespReadUtils.cs:126，allowLeadingZeros 默认 true）
#[test]
fn ri_option_long_matches_csharp_getlong() {
  // 合法（含前导零与符号——C# GetLong 无前导零拒绝）
  assert_eq!(ri_option_long(&[b"0"], 0).ok(), Some(0));
  assert_eq!(ri_option_long(&[b"007"], 0).ok(), Some(7));
  assert_eq!(ri_option_long(&[b"-007"], 0).ok(), Some(-7));
  assert_eq!(ri_option_long(&[b"+5"], 0).ok(), Some(5));
  assert_eq!(
    ri_option_long(&[b"9223372036854775807"], 0).ok(),
    Some(i64::MAX)
  );
  assert_eq!(
    ri_option_long(&[b"-9223372036854775808"], 0).ok(),
    Some(i64::MIN)
  );

  // 非数字 / 尾随垃圾 / u64 溢出 → ThrowNotANumber（回显原始参数）
  for raw in ["abc", "12x", "", "99999999999999999999"] {
    let err = ri_option_long(&[raw.as_bytes()], 0).expect_err(raw);
    assert_eq!(
      err,
      format!("ERR Protocol Error: Unable to parse number: {raw}")
    );
  }

  // u64 域内超 i64 → ThrowIntegerOverflow（数字串不含符号）
  let err = ri_option_long(&[b"9223372036854775808"], 0).expect_err("overflow");
  assert_eq!(
    err,
    "ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775808"
  );
  let err = ri_option_long(&[b"-9223372036854775809"], 0).expect_err("overflow");
  assert_eq!(
    err,
    "ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775809"
  );
}

/// RI.CREATE 选项解析：数值走 GetLong 两态，值缺失/未知选项文案不变
#[test]
fn parse_ricreate_options_two_state_errors() {
  let ok = parse_ricreate_options(&[b"k", b"CACHESIZE", b"007"]).expect("ok");
  assert_eq!(ok.cache_size, 7);

  let err = parse_ricreate_options(&[b"k", b"MINRECORD", b"xyz"]).expect_err("not a number");
  assert_eq!(err, "ERR Protocol Error: Unable to parse number: xyz");

  let err = parse_ricreate_options(&[b"k", b"PAGESIZE"]).expect_err("missing value");
  assert_eq!(err, "ERR PAGESIZE requires a value");

  let err = parse_ricreate_options(&[b"k", b"WHATEVER"]).expect_err("unknown");
  assert_eq!(err, "ERR unknown option");
}
