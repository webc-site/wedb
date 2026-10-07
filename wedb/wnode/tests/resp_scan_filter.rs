#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! SCAN 命令参数与类型过滤解析契约测试（array_commands/scan.rs 内联测试迁出）

use wnode::{
  resp::array_commands::parse_scan_filter,
  storage::session::common::array_key_iteration_functions::ScanTypeFilter,
};
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};
use wval::GarnetObjectType;

#[test]
fn test_parse_scan_filter_cursor_validation() {
  assert_eq!(
    parse_scan_filter(&[b"abc"]).unwrap_err(),
    RESP_ERR_GENERIC_INVALIDCURSOR
  );
  assert_eq!(
    parse_scan_filter(&[b"-1"]).unwrap_err(),
    RESP_ERR_GENERIC_INVALIDCURSOR
  );
  assert_eq!(
    parse_scan_filter(&[b"0", b"COUNT", b"x"]).unwrap_err(),
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
  );

  let res = parse_scan_filter(&[b"0"]).unwrap();
  assert_eq!(res.cursor, 0);
  assert_eq!(res.count, 10);
  assert!(res.all_keys);
}

#[test]
fn test_parse_scan_filter_type_exact_forms() {
  let res = parse_scan_filter(&[b"0", b"type", b"zset"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet))
  );
  assert!(!res.type_unknown);

  let res = parse_scan_filter(&[b"0", b"TYPE", b"LIST"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::List))
  );

  let res = parse_scan_filter(&[b"0", b"TyPe", b"SET"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::Set))
  );

  let res = parse_scan_filter(&[b"0", b"type", b"hash"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::Hash))
  );

  let res = parse_scan_filter(&[b"0", b"type", b"STRING"]).unwrap();
  assert_eq!(res.type_filter, Some(ScanTypeFilter::String));

  // 混合大小写不匹配 C# 双常量（SequenceEqual 全大写/全小写）→ 未知类型回空
  let res = parse_scan_filter(&[b"0", b"type", b"zSet"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  let res = parse_scan_filter(&[b"0", b"type", b"HaSh"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  let res = parse_scan_filter(&[b"0", b"type", b"StRiNg"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  // 其它未知类型（如 stream 两形态）
  let res = parse_scan_filter(&[b"0", b"type", b"stream"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  let res = parse_scan_filter(&[b"0", b"type", b"STREAM"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  // TYPE 缺少参数
  assert_eq!(
    parse_scan_filter(&[b"0", b"type"]).unwrap_err(),
    RESP_ERR_GENERIC_SYNTAX_ERROR
  );
}

#[test]
fn parse_scan_filter_duplicate_type_last_valid_overrides() {
  let res = parse_scan_filter(&[b"0", b"type", b"stream", b"type", b"hash"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::Hash))
  );
  assert!(!res.type_unknown);
  assert!(res.type_given);

  let res = parse_scan_filter(&[b"0", b"type", b"hash", b"type", b"stream"]).unwrap();
  assert_eq!(res.type_filter, None);
  assert!(res.type_unknown);

  let res = parse_scan_filter(&[b"0", b"type", b""]).unwrap();
  assert!(res.type_unknown);
  let res = parse_scan_filter(&[b"0", b"type", b"", b"type", b"zset"]).unwrap();
  assert_eq!(
    res.type_filter,
    Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet))
  );
  assert!(!res.type_unknown);
}
