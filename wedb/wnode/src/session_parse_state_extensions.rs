//! 解析态扩展选项解析族（对标 libs/server/SessionParseStateExtensions.cs）
//!
//! C# 以 `(this SessionParseState, int idx)` 扩展方法形态提供「按下标取参数再解析」
//! 的入口；rust 侧解析单一入口是「调用方按 `SessionParseState` 下标取 token + 字节核
//! （`&[u8]` 入参）解析」两步组合，下标壳不单独转写。故 C# 各 `TryGet*` 的解析语义
//! 一处落在字节核上，`libs/server/SessionParseStateExtensions.cs` 的方法锚点随字节核
//! 归属登记（本模块、`wresp::options`、`wbitmap::bitfield::parse`、
//! `wresp::metrics::InfoMetricsType::from_name`）；仅 CLIENT 子命令需要的
//! `ClientType` 仍保留下标形态口。
//!
//! 注：C# `RespCommand` 参数在此以 `&str` 命令名承接（GEOSEARCH 族判定仅比
//! 较命令名；rust resp 命令域的 RespCommand 元数据表由并行域落地）。

use std::str::from_utf8;

use wbase::num::strict_f64;
use wcol::list::list_object::OperationDirection;
use wresp::{cmd_strings, session_parse_state::SessionParseState};

/// CLIENT 子命令的客户端类型（Garnet.common:ClientType 语义；Invalid 为哨兵）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
  /// 非法（解析失败哨兵，对齐 C# ClientType.Invalid）
  Invalid,
  /// 普通连接
  Normal,
  /// 主节点
  Master,
  /// 副本
  Replica,
  /// 订阅连接
  Pubsub,
  /// 副本（旧称）
  Slave,
}

/// 集群管理器种类（Garnet.server:ManagerType）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerType {
  /// 迁移管理器
  MigrationManager,
  /// 复制管理器
  ReplicationManager,
  /// 服务器监听器
  ServerListener,
}

/// 大小写不敏感匹配（C# EqualsUpperCaseSpanIgnoringCase）
#[inline]
fn eq_upper_ignore_case(raw: &[u8], upper: &[u8]) -> bool {
  raw.eq_ignore_ascii_case(upper)
}

/// 解析态序列化快照（C# parseState.SerializeTo 的安全封装，
/// 供慢日志入库等快照场景使用）
pub fn serialize_snapshot(parse_state: &SessionParseState, buf: &[u8]) -> Vec<u8> {
  let len = parse_state.get_serialized_length();
  let mut dest = vec![0u8; len];
  if len > 0 {
    parse_state.serialize_to(buf, &mut dest);
  }
  dest
}

/// libs/server/SessionParseStateExtensions.cs:TryGetClientName
///
/// 33..=126 可打印字符，空串允许（清名语义）；非 UTF-8 视为失败
pub fn try_get_client_name_bytes(raw: &[u8]) -> Option<&str> {
  let name = from_utf8(raw).ok()?;
  if name.is_empty() {
    return Some(name);
  }
  name
    .bytes()
    .all(|c| (33..=126).contains(&c))
    .then_some(name)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetClientType
///
/// CLIENT 子命令的下标形态口：调用方持宿主缓冲，越界视为解析失败
pub fn try_get_client_type(
  parse_state: &SessionParseState,
  buf: &[u8],
  idx: usize,
) -> Option<ClientType> {
  if idx >= parse_state.count {
    return None;
  }
  let arg = parse_state.arg_in(buf, idx);
  let value = if eq_upper_ignore_case(arg, b"NORMAL") {
    ClientType::Normal
  } else if eq_upper_ignore_case(arg, b"MASTER") {
    ClientType::Master
  } else if eq_upper_ignore_case(arg, b"REPLICA") {
    ClientType::Replica
  } else if eq_upper_ignore_case(arg, b"PUBSUB") {
    ClientType::Pubsub
  } else if eq_upper_ignore_case(arg, b"SLAVE") {
    ClientType::Slave
  } else {
    ClientType::Invalid
  };
  (value != ClientType::Invalid).then_some(value)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetManagerType
///
/// ManagerType 字节令牌解析（ASCII 大小写不敏感；DEBUG PURGEBP 切片侧复用）
pub fn manager_type_from_token(arg: &[u8]) -> Option<ManagerType> {
  let value = if eq_upper_ignore_case(arg, b"MIGRATIONMANAGER") {
    ManagerType::MigrationManager
  } else if eq_upper_ignore_case(arg, b"REPLICATIONMANAGER") {
    ManagerType::ReplicationManager
  } else if eq_upper_ignore_case(arg, b"SERVERLISTENER") {
    ManagerType::ServerListener
  } else {
    return None;
  };
  Some(value)
}

impl ManagerType {
  /// PurgeBPCommand.cs:ManagerTypeExtensions.ToReadOnlySpan——清洗完成简单串
  pub fn gc_completed_text(self) -> &'static str {
    match self {
      ManagerType::MigrationManager => "GC completed for MigrationManager",
      ManagerType::ReplicationManager => "GC completed for ReplicationManager",
      ManagerType::ServerListener => "GC completed for ServerListener",
    }
  }
}

/// libs/server/SessionParseStateExtensions.cs:TryGetOperationDirection
///
/// LEFT / RIGHT 字节令牌解析（BLMOVE 族方向参数共用）
pub fn operation_direction_from_token(arg: &[u8]) -> Option<OperationDirection> {
  if eq_upper_ignore_case(arg, b"LEFT") {
    Some(OperationDirection::Left)
  } else if eq_upper_ignore_case(arg, b"RIGHT") {
    Some(OperationDirection::Right)
  } else {
    None
  }
}

/// libs/server/SessionParseStateExtensions.cs:TryGetTimeout
///
/// 超时（秒）字节令牌解析：非负且 ≤ i32::MAX/1000（.NET API 毫秒上限）；
/// 失败返回 C# 错误文案
pub fn try_get_timeout_bytes(raw: &[u8]) -> Result<f64, &'static str> {
  const MAX_TIMEOUT: f64 = i32::MAX as f64 / 1000.0;
  let Some(timeout) = strict_f64(raw, true) else {
    return Err(cmd_strings::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
  };
  if timeout < 0.0 {
    return Err(cmd_strings::RESP_ERR_TIMEOUT_IS_NEGATIVE);
  }
  if timeout > MAX_TIMEOUT {
    return Err(cmd_strings::RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE);
  }
  Ok(timeout)
}
