//! 解析态扩展选项解析族集成测试（自 src/session_parse_state_extensions.rs 迁出）
//!
//! 对标 libs/server/SessionParseStateExtensions.cs：CLIENT 名与类型（下标口）、
//! ManagerType / OperationDirection 令牌、超时边界与严格数值解析语义。
//! 选项枚举解析（EXPIRE/OBJ/ZADD/聚合）与 BITFIELD 编码、INFO/LATENCY 段名解析
//! 的用例随各自的字节核归属：wresp::options、wbitmap::bitfield、
//! wresp::metrics::InfoMetricsType、wmetric::LatencyMetricsType。

use wcol::list::list_object::OperationDirection;
use wnode::session_parse_state_extensions::{
  ClientType, ManagerType, manager_type_from_token, operation_direction_from_token,
  try_get_client_name_bytes, try_get_client_type, try_get_timeout_bytes,
};
use wresp::{
  argslice::ArgSlice,
  cmd_strings::{RESP_ERR_TIMEOUT_IS_NEGATIVE, RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE},
  session_parse_state::SessionParseState,
};

/// 以宿主缓冲连续排布构造解析态（槽位记 offset，返回状态与宿主缓冲）
fn state_of(args: &[&[u8]]) -> (SessionParseState, Vec<u8>) {
  let mut buf = Vec::new();
  let mut slices = Vec::with_capacity(args.len());
  for arg in args {
    slices.push(ArgSlice::new(buf.len(), arg.len()));
    buf.extend_from_slice(arg);
  }
  let mut state = SessionParseState::new();
  state.initialize_with_args(&slices);
  (state, buf)
}

#[test]
fn client_name_printable_rule() {
  assert_eq!(try_get_client_name_bytes(b"ok-name"), Some("ok-name"));
  assert_eq!(try_get_client_name_bytes(b"bad name"), None);
  assert_eq!(try_get_client_name_bytes(b""), Some(""));
  // 非 UTF-8 视为失败（C# GetString 解码失败回 false）
  assert_eq!(try_get_client_name_bytes(&[0xff, 0xfe]), None);
}

#[test]
fn client_types_parse() {
  let (state, buf) = state_of(&[b"master", b"PUBSUB", b"other"]);
  assert_eq!(
    try_get_client_type(&state, &buf, 0),
    Some(ClientType::Master)
  );
  assert_eq!(
    try_get_client_type(&state, &buf, 1),
    Some(ClientType::Pubsub)
  );
  assert_eq!(try_get_client_type(&state, &buf, 2), None);
  // 越界下标（C# parseState 边界判定）
  assert_eq!(try_get_client_type(&state, &buf, 3), None);
}

#[test]
fn manager_and_operation_direction_tokens() {
  assert_eq!(
    operation_direction_from_token(b"left"),
    Some(OperationDirection::Left)
  );
  assert_eq!(operation_direction_from_token(b"MAX"), None);
  assert_eq!(
    manager_type_from_token(b"SERVERLISTENER"),
    Some(ManagerType::ServerListener)
  );
  // 大小写不敏感
  assert_eq!(
    manager_type_from_token(b"replicationmanager"),
    Some(ManagerType::ReplicationManager)
  );
  assert_eq!(manager_type_from_token(b"nope"), None);
}

#[test]
fn timeout_bounds() {
  assert_eq!(try_get_timeout_bytes(b"30"), Ok(30.0));
  assert_eq!(
    try_get_timeout_bytes(b"-1"),
    Err(RESP_ERR_TIMEOUT_IS_NEGATIVE)
  );
  assert_eq!(
    try_get_timeout_bytes(b"99999999"),
    Err(RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE)
  );
}
