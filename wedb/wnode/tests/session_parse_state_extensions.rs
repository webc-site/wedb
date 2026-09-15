//! 解析态扩展选项解析族集成测试（自 src/session_parse_state_extensions.rs 迁出）
//!
//! 对标 libs/server/SessionParseStateExtensions.cs：INFO / LATENCY 度量类型、
//! CLIENT 名与类型、BITFIELD 编码 / 偏移、过期选项、超时边界、GEOSEARCH
//! 全参数、键规格提取与严格数值解析语义。

use std::slice::from_ref;

use wbitmap::BitFieldOverflow;
use wcol::{
  geo::{GeoDistanceUnitType, GeoOrder, GeoOriginType, GeoSearchType},
  list::list_object::OperationDirection,
};
use wmetric::{InfoMetricsType, LatencyMetricsType};
use wnode::{
  key_spec::{SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys},
  session_parse_state_extensions::{
    ClientType, extract_command_keys, extract_command_keys_and_flags, try_get_bit_field_overflow,
    try_get_bitfield_encoding, try_get_bitfield_offset, try_get_client_name, try_get_client_type,
    try_get_expiration_option, try_get_expire_option, try_get_geo_search_options,
    try_get_info_metrics_type, try_get_key_search_args_from_simple_key_spec,
    try_get_latency_metrics_type, try_get_manager_type, try_get_operation_direction,
    try_get_sorted_set_add_option, try_get_sorted_set_aggregate_type, try_get_timeout,
  },
};
use wresp::{
  ArgSlice, ExpirationOption, ExpireOption, KeySpecificationFlags, SessionParseState,
  SortedSetAddOption, SortedSetAggregateType as ZSetAggregate,
};

/// C# CmdStrings 同名常量（src 本地对齐文本；此处同文内联断言）
const RESP_ERR_TIMEOUT_IS_NEGATIVE: &str = "ERR timeout is negative";
const RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE: &str = "ERR timeout is out of range";
const RESP_ERR_NOT_VALID_RADIUS: &str = "ERR need numeric radius";

fn state_of(args: &[&[u8]]) -> SessionParseState {
  let slices: Vec<ArgSlice> = args
    .iter()
    .map(|a| ArgSlice::new(a.as_ptr(), a.len()))
    .collect();
  let mut state = SessionParseState::new();
  state.initialize_with_args(&slices);
  state
}

#[test]
fn info_metrics_types_parse() {
  let state = state_of(&[b"server", b"KEYSPACE", b"nope"]);
  assert_eq!(
    try_get_info_metrics_type(&state, 0),
    Some(InfoMetricsType::Server)
  );
  assert_eq!(
    try_get_info_metrics_type(&state, 1),
    Some(InfoMetricsType::Keyspace)
  );
  assert_eq!(try_get_info_metrics_type(&state, 2), None);
}

#[test]
fn latency_metrics_types_parse() {
  let state = state_of(&[b"net_rs_bytes"]);
  assert_eq!(
    try_get_latency_metrics_type(&state, 0),
    Some(LatencyMetricsType::NetRsBytes)
  );
  assert_ne!(
    try_get_latency_metrics_type(&state, 0),
    Some(LatencyMetricsType::NetRsOps)
  );
}

#[test]
fn client_name_printable_rule() {
  let state = state_of(&[b"ok-name", b"bad name", b""]);
  assert_eq!(try_get_client_name(&state, 0), Some("ok-name"));
  assert_eq!(try_get_client_name(&state, 1), None);
  assert_eq!(try_get_client_name(&state, 2), Some(""));
}

#[test]
fn client_types_parse() {
  let state = state_of(&[b"master", b"PUBSUB", b"other"]);
  assert_eq!(try_get_client_type(&state, 0), Some(ClientType::Master));
  assert_eq!(try_get_client_type(&state, 1), Some(ClientType::Pubsub));
  assert_eq!(try_get_client_type(&state, 2), None);
}

#[test]
fn bitfield_encoding_and_offset() {
  let state = state_of(&[b"i32", b"u64", b"u0", b"#5", b"7", b"i65", b"#"]);
  assert_eq!(try_get_bitfield_encoding(&state, 0), Some((32, true)));
  // C# 约束：无符号位宽须 < 64（u64 非法），u63 合法
  assert_eq!(try_get_bitfield_encoding(&state, 1), None);
  assert_eq!(
    try_get_bitfield_encoding(&state_of(&[b"u63"]), 0),
    Some((63, false))
  );
  assert_eq!(try_get_bitfield_encoding(&state, 2), None);
  assert_eq!(try_get_bitfield_encoding(&state, 5), None);

  assert_eq!(try_get_bitfield_offset(&state, 3), Some((5, true)));
  assert_eq!(try_get_bitfield_offset(&state, 4), Some((7, false)));
  assert_eq!(try_get_bitfield_offset(&state, 6), None);
}

#[test]
fn expire_and_expiration_options() {
  let state = state_of(&[b"gt", b"KEEPTTL", b"pxat", b"bad"]);
  assert_eq!(try_get_expire_option(&state, 0), Some(ExpireOption::Gt));
  assert_eq!(
    try_get_expiration_option(&state, 1),
    Some(ExpirationOption::Keepttl)
  );
  assert_eq!(
    try_get_expiration_option(&state, 2),
    Some(ExpirationOption::Pxat)
  );
  assert_eq!(try_get_expiration_option(&state, 3), None);
}

#[test]
fn timeout_bounds() {
  let state = state_of(&[b"30", b"-1", b"99999999"]);
  let (v, e) = try_get_timeout(&state, 0);
  assert_eq!(v, Some(30.0));
  assert!(e.is_none());
  let (v, e) = try_get_timeout(&state, 1);
  assert!(v.is_none());
  assert_eq!(e.as_deref(), Some(RESP_ERR_TIMEOUT_IS_NEGATIVE.as_bytes()));
  let (_, e) = try_get_timeout(&state, 2);
  assert_eq!(
    e.as_deref(),
    Some(RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE.as_bytes())
  );
}

#[test]
fn operation_direction_and_aggregate() {
  let state = state_of(&[b"left", b"MAX"]);
  assert_eq!(
    try_get_operation_direction(&state, 0),
    Some(OperationDirection::Left)
  );
  assert_eq!(
    try_get_sorted_set_aggregate_type(&state, 1),
    Some(ZSetAggregate::Max)
  );
  assert_eq!(
    try_get_sorted_set_add_option(&state_of(&[b"ch"]), 0),
    Some(SortedSetAddOption::Ch)
  );
  assert_eq!(
    try_get_bit_field_overflow(&state_of(&[b"SAT"]), 0),
    Some(BitFieldOverflow::Sat)
  );
  assert_eq!(try_get_manager_type(&state_of(&[b"nope"]), 0), None);
}

#[test]
fn geo_search_options_full_parse() {
  // GEOSEARCH key FROMLONLAT 1 2 BYRADIUS 30 km ASC COUNT 10 ANY WITHCOORD
  let state = state_of(&[
    b"FROMLONLAT",
    b"1",
    b"2",
    b"BYRADIUS",
    b"30",
    b"KM",
    b"ASC",
    b"COUNT",
    b"10",
    b"ANY",
    b"WITHCOORD",
  ]);
  let (opts, dest, err) = try_get_geo_search_options(&state, "GEOSEARCH");
  assert!(
    err.is_none(),
    "unexpected error: {:?}",
    err.map(|e| String::from_utf8_lossy(&e).into_owned())
  );
  assert_eq!(dest, -1);
  let opts = opts.unwrap();
  assert_eq!(opts.origin, GeoOriginType::FromLonLat);
  assert_eq!(opts.search_type, GeoSearchType::ByRadius);
  assert_eq!(opts.radius, 30.0);
  assert_eq!(opts.unit, GeoDistanceUnitType::Km);
  assert_eq!(opts.sort, GeoOrder::Ascending);
  assert_eq!(opts.count_value, 10);
  assert!(opts.with_count_any);
  assert!(opts.with_coord);
}

#[test]
fn geo_search_requires_origin_and_shape() {
  let state = state_of(&[b"ASC"]);
  let (opts, _, err) = try_get_geo_search_options(&state, "GEOSEARCH");
  assert!(opts.is_none());
  assert_eq!(
    err.as_deref(),
    Some("ERR wrong number of arguments for 'GEOSEARCH' command".as_bytes())
  );
}

#[test]
fn key_search_args_range_type() {
  // MSET key1 val1 key2 val2：begin_search_index=1（扣除命令名后），lastkey=4，step=2
  let state = state_of(&[b"key1", b"val1", b"key2", b"val2"]);
  let spec = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: 1,
      is_index_type: true,
      keyword: Vec::new(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 2,
      is_range_type: true,
      last_key_or_limit: -1,
      ..Default::default()
    },
    flags: KeySpecificationFlags::NONE,
  };
  let keys = extract_command_keys(&state, &[spec], false);
  assert_eq!(keys, vec![&b"key1"[..], &b"key2"[..]]);
}

#[test]
fn key_search_args_keyword_type() {
  let state = state_of(&[b"KEY", b"k1", b"k2"]);
  let spec = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: 1,
      is_index_type: false,
      keyword: b"KEY".to_vec(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 1,
      is_range_type: true,
      last_key_or_limit: 1,
      ..Default::default()
    },
    flags: KeySpecificationFlags::RW,
  };
  let pairs = extract_command_keys_and_flags(&state, from_ref(&spec), false);
  assert_eq!(pairs, vec![(&b"k1"[..], 1), (&b"k2"[..], 1)]);
  let args = try_get_key_search_args_from_simple_key_spec(&state, &spec, false);
  assert_eq!(args, Some((1, 2, 1)));
}

#[test]
fn key_search_args_reverse_keyword_scan_terminates() {
  // 负下标 = 逆序关键字扫描；关键字未命中时安全终止（下界护栏）
  let state = state_of(&[b"KEY", b"k1", b"k2"]);
  let spec = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: -1,
      is_index_type: false,
      keyword: b"MISSING".to_vec(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 1,
      is_range_type: true,
      last_key_or_limit: 1,
      ..Default::default()
    },
    flags: KeySpecificationFlags::NONE,
  };
  assert_eq!(
    try_get_key_search_args_from_simple_key_spec(&state, &spec, false),
    None
  );

  // 逆序扫描能命中参数区内的关键字（C# 语义：自尾部向前找）
  let spec_hit = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: -2,
      is_index_type: false,
      keyword: b"KEY".to_vec(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 1,
      is_range_type: true,
      last_key_or_limit: 1,
      ..Default::default()
    },
    flags: KeySpecificationFlags::NONE,
  };
  assert_eq!(
    try_get_key_search_args_from_simple_key_spec(&state, &spec_hit, false),
    Some((1, 2, 1))
  );
}

#[test]
fn bitfield_strict_int_semantics() {
  // C# TryReadInt64Safe（allowLeadingZeros: false）：前导零拒绝
  assert_eq!(try_get_bitfield_encoding(&state_of(&[b"i032"]), 0), None);
  assert_eq!(try_get_bitfield_offset(&state_of(&[b"#05"]), 0), None);
  // 可选符号接受（C# TryReadSign 允许 +）
  assert_eq!(
    try_get_bitfield_offset(&state_of(&[b"+5"]), 0),
    Some((5, false))
  );
  assert_eq!(
    try_get_bitfield_offset(&state_of(&[b"#+7"]), 0),
    Some((7, true))
  );
}

#[test]
fn geo_radius_accepts_inf_literal() {
  // C# TryGetDouble 默认 canBeInfinite: true → INF 半径合法（仅拒绝负半径）
  //（GEORADIUSBYMEMBER 以成员为原点，避免 GEORADIUS 的经纬度先行读取）
  let state = state_of(&[b"member", b"INF", b"KM"]);
  let (opts, dest, err) = try_get_geo_search_options(&state, "GEORADIUSBYMEMBER");
  assert!(
    err.is_none(),
    "unexpected: {:?}",
    err.map(|e| String::from_utf8_lossy(&e).into_owned())
  );
  assert_eq!(dest, -1);
  assert_eq!(opts.unwrap().radius, f64::INFINITY);

  // "Infinity" 非 3/4 字节白名单 → not a valid radius
  let state = state_of(&[b"member", b"Infinity", b"KM"]);
  let (opts, _, err) = try_get_geo_search_options(&state, "GEORADIUSBYMEMBER");
  assert!(opts.is_none());
  assert_eq!(err.as_deref(), Some(RESP_ERR_NOT_VALID_RADIUS.as_bytes()));

  // 负 infinity 全拼同样词形拒绝（未过词形门，走不到负半径判定）
  let state = state_of(&[b"member", b"-infinity", b"KM"]);
  let (opts, _, err) = try_get_geo_search_options(&state, "GEORADIUSBYMEMBER");
  assert!(opts.is_none());
  assert_eq!(err.as_deref(), Some(RESP_ERR_NOT_VALID_RADIUS.as_bytes()));
}
