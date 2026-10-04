use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS},
  time::now_ticks,
};
use wnode::resp::{
  RespServerSession,
  objects::object_store_utils::{
    ElementHeaderKind, compute_expiration_ticks, parse_elements_header,
  },
};

#[test]
fn abort_frames_match_csharp_text() {
  let mut sess = RespServerSession::default();
  let mut out = Vec::new();
  assert!(sess.abort_with_wrong_number_of_arguments("ZADD", &mut out));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ZADD' command\r\n"
  );

  out.clear();
  assert!(sess.abort_with_error_message(b"ERR custom", &mut out));
  assert_eq!(out, b"-ERR custom\r\n");
}

#[test]
fn test_parse_elements_header_fields_and_members() {
  let mut out = Vec::new();

  // 正常 FIELDS
  let args: &[&[u8]] = &[b"key", b"FIELDS", b"2", b"f1", b"f2"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, Some((3, 2)));
  assert!(out.is_empty());

  // 正常 MEMBERS
  let args: &[&[u8]] = &[b"key", b"MEMBERS", b"1", b"m1"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
  assert_eq!(res, Some((3, 1)));
  assert!(out.is_empty());

  // 缺失 FIELDS
  out.clear();
  let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-Mandatory argument FIELDS is missing or not at the right position\r\n"
  );

  // 缺失 MEMBERS
  out.clear();
  let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
  );

  // num 为 0 且计数吻合：C# 无值域门，放行零元素（HashCommands 的 HashExpire 同臂）
  out.clear();
  let args: &[&[u8]] = &[b"key", b"MEMBERS", b"0"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
  assert_eq!(res, Some((3, 0)));
  assert!(out.is_empty());

  // num 为 0 但计数不吻合 → must match（非 greater-than-0）
  out.clear();
  let args: &[&[u8]] = &[b"key", b"FIELDS", b"0", b"x"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-The `numFields` parameter must match the number of arguments\r\n"
  );

  // 负数：C# TryGetInt 接受负值后落 Count 比对臂 → must match
  out.clear();
  let args: &[&[u8]] = &[b"key", b"FIELDS", b"-1"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-The `numFields` parameter must match the number of arguments\r\n"
  );

  // i32 极值：usize 转域溢出回归护栏（带符号域比对，debug 构建不 panic）
  out.clear();
  let args: &[&[u8]] = &[b"key", b"MEMBERS", b"-2147483648"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-The `numMembers` parameter must match the number of arguments\r\n"
  );

  out.clear();
  let args: &[&[u8]] = &[b"key", b"FIELDS", b"2147483647"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-The `numFields` parameter must match the number of arguments\r\n"
  );

  // num 不是数字
  out.clear();
  let args: &[&[u8]] = &[b"key", b"FIELDS", b"abc"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-ERR Parameter `numFields` should be greater than 0\r\n"
  );

  // 参数数量不匹配
  out.clear();
  let args: &[&[u8]] = &[b"key", b"MEMBERS", b"2", b"m1"];
  let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
  assert_eq!(res, None);
  assert_eq!(
    out,
    b"-The `numMembers` parameter must match the number of arguments\r\n"
  );
}

#[test]
fn test_compute_expiration_ticks() {
  let now_before = now_ticks();
  let ticks_sec = compute_expiration_ticks(10, false, false);
  let now_after = now_ticks();
  assert!(ticks_sec >= now_before + 10 * TICKS_PER_SECOND);
  assert!(ticks_sec <= now_after + 10 * TICKS_PER_SECOND);

  let ticks_ms = compute_expiration_ticks(500, true, false);
  assert!(ticks_ms >= now_before + 500 * TICKS_PER_MILLISECOND);

  let ts_sec = compute_expiration_ticks(1_700_000_000, false, true);
  assert_eq!(ts_sec, UNIX_EPOCH_TICKS + 1_700_000_000 * TICKS_PER_SECOND);

  let ts_ms = compute_expiration_ticks(1_700_000_000_000, true, true);
  assert_eq!(
    ts_ms,
    UNIX_EPOCH_TICKS + 1_700_000_000_000 * TICKS_PER_MILLISECOND
  );

  // 溢出防护（saturating）
  let max_ticks = compute_expiration_ticks(i64::MAX, false, false);
  assert_eq!(max_ticks, i64::MAX);
}
