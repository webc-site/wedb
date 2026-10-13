#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wconn::parser::RespReadResponseUtils;

#[test]
fn test_resp_parser() {
  let mut data = &b"$3\r\nfoo\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap();
  assert_eq!(res, Some(Some("foo".to_string())));
  assert_eq!(data, b"");

  let mut data = &b"*-1\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap();
  assert_eq!(res, Some(None));
  assert_eq!(data, b"");
}

/// 长度头词法对齐 C# TryReadUInt64：'+' 号与空数字头拒绝，负号仅允许单前导
#[test]
fn length_header_lexer_rejects_signed_and_empty() {
  for bad in [
    &b"$+3\r\nX"[..],
    b"$\r\nX",
    b"$-\r\nX",
    b"$1x\r\nX",
    b"$--1\r\nX",
  ] {
    let mut data = bad;
    assert!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err(),
      "{:?} 应判协议错误",
      bad
    );
  }
  // 前导零合法（TryReadUInt64 允许）
  let mut data = &b"$003\r\nabc\r\n"[..];
  assert_eq!(
    RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
    Some(Some("abc".to_string()))
  );
  // -0 折算为 0（C# -(int)0 同为 0）
  let mut data = &b"$-0\r\n\r\n"[..];
  assert_eq!(
    RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
    Some(Some(String::new()))
  );
}

/// 头部值域按 C# int 承载：i32 越界判协议错误，int.MinValue 边界放行为 null
#[test]
fn length_header_value_range_is_i32() {
  // 超过 i32::MAX（合法十进制但越 int 值域）
  let mut data = &b"$2147483648\r\n"[..];
  assert!(RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err());
  // 超过 u64 域的超长数字串
  let mut data = &b"$99999999999999999999\r\n"[..];
  assert!(RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err());
  // int.MinValue 边界：负向放行，按 null bulk 处理
  let mut data = &b"$-2147483648\r\n"[..];
  assert_eq!(
    RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
    Some(None)
  );
}

/// 超 512MB 上限的 bulk 头按"未到齐"挂起（对齐 RespReadResponseUtils.cs:123）；
/// 超长数组头不再按元素数精确预分配（防分配中止），首元素缺失按未到齐处理
#[test]
fn oversized_headers_do_not_allocate_or_error() {
  let mut data = &b"$536870913\r\n"[..]; // 512MB + 1
  assert_eq!(
    RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
    None
  );
  // 2000 万元素：合法 i32 值域，预分配被截断后按未到齐挂起而非中止进程
  let mut data = &b"*20000000\r\n"[..];
  assert_eq!(
    RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap(),
    None
  );
}

#[test]
fn partial_frame_rollback_and_resp3_null() {
  // 部分到达的 bulk string：游标完整回滚
  let original = &b"$5\r\nhel"[..];
  let mut data = original;
  assert_eq!(
    RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
    None
  );
  assert_eq!(data, original, "游标必须完整回滚");

  // 部分到达的 array：游标完整回滚
  let original_arr = &b"*2\r\n$3\r\nfoo\r\n$3\r\nba"[..];
  let mut data = original_arr;
  assert_eq!(
    RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap(),
    None
  );
  assert_eq!(data, original_arr, "数组未完整到达时游标必须完整回滚");

  // RESP3 null 测试
  let mut data = &b"_\r\n"[..];
  assert_eq!(
    RespReadResponseUtils::try_read_null(&mut data).unwrap(),
    Some(())
  );
  assert_eq!(data, b"");

  // 数组中包含 RESP3 null
  let mut data = &b"*2\r\n_\r\n$3\r\nbar\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap();
  assert_eq!(res, Some(Some(vec![String::new(), "bar".to_string()])));
  assert_eq!(data, b"");
}

#[test]
fn resp3_collections_and_scalars() {
  // RESP3 集合类型 ~2
  let mut data = &b"~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap();
  assert_eq!(res, Some(Some(vec!["foo".to_string(), "bar".to_string()])));
  assert_eq!(data, b"");

  // RESP3 推送类型 >1
  let mut data = &b">1\r\n+pubsub\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap();
  assert_eq!(res, Some(Some(vec!["pubsub".to_string()])));
  assert_eq!(data, b"");

  // 集合内含浮点与布尔
  let mut data = &b"*2\r\n,3.14\r\n#t\r\n"[..];
  let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data, 1).unwrap();
  assert_eq!(res, Some(Some(vec!["3.14".to_string(), "t".to_string()])));
  assert_eq!(data, b"");
}

/// bulk 读（生产 `$` 臂单点 try_read_byte_slice_with_length_header）：
/// 正常体、$-1 null bulk、半包回滚三态
#[test]
fn byte_slice_with_length_header() {
  let mut data = &b"$5\r\nhello\r\n"[..];
  let res = RespReadResponseUtils::try_read_byte_slice_with_length_header(&mut data).unwrap();
  assert_eq!(res, Some(Some(&b"hello"[..])));
  assert_eq!(data, b"");

  let mut data = &b"$-1\r\n"[..];
  let res = RespReadResponseUtils::try_read_byte_slice_with_length_header(&mut data).unwrap();
  assert_eq!(res, Some(None));
  assert_eq!(data, b"");

  let mut data = &b"$5\r\nhel"[..];
  let res = RespReadResponseUtils::try_read_byte_slice_with_length_header(&mut data).unwrap();
  assert_eq!(res, None);
  assert_eq!(data, &b"$5\r\nhel"[..]);
}
