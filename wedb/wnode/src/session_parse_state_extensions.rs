//! 解析态扩展选项解析族（对标 libs/server/SessionParseStateExtensions.cs:SessionParseStateExtensions）
//!
//! C# 以扩展方法挂在 `SessionParseState` 上；Rust 侧以
//! `SessionParseStateExtensions` 关联函数承接同一 API 面。枚举 / 数值解析
//! 语义与错误文案逐项对齐 C#（大小写不敏感匹配 + 严格数值解析）。
//!
//! 注：C# `RespCommand` 参数在此以 `&str` 命令名承接（GEOSEARCH 族判定仅比
//! 较命令名；rust resp 命令域的 RespCommand 元数据表由并行域落地）。

use std::str::from_utf8;

use wbase::num::{strict_f64, strict_i32, strict_i64};
use wbitmap::{BitFieldOverflow, parse_bitfield_overflow_slice};
use wcol::{
  geo::{GeoOrder, GeoOriginType, GeoSearchOptions, GeoSearchType, geo_hash::GeoDistanceUnitType},
  list::list_object::OperationDirection,
};
use wmetric::{InfoMetricsType, LatencyMetricsType};
use wresp::{
  ExpirationOption, ExpireOption, SessionParseState, SortedSetAddOption,
  SortedSetAggregateType as ZSetAggregate,
  cmd_strings::{
    self, RESP_ERR_COUNT_IS_NOT_POSITIVE, RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE,
    RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT, RESP_ERR_NOT_VALID_HEIGHT, RESP_ERR_NOT_VALID_RADIUS,
    RESP_ERR_NOT_VALID_WIDTH, RESP_ERR_RADIUS_IS_NEGATIVE,
  },
};

pub use crate::key_spec::*;

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

/// BITFIELD 族错误文案（C# CmdStrings 同名常量；GEO 族文案已收归
/// wresp::cmd_strings 单点，经 `cmd_strings::` 前缀引用）
const RESP_ERR_TIMEOUT_IS_NEGATIVE: &str = "ERR timeout is negative";
const RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE: &str = "ERR timeout is out of range";

/// 大小写不敏感匹配（C# EqualsUpperCaseSpanIgnoringCase）
#[inline]
fn eq_upper_ignore_case(raw: &[u8], upper: &[u8]) -> bool {
  raw.eq_ignore_ascii_case(upper)
}

/// 解析态字节访问面（避免扩展层直接触碰 ArgSlice 指针）
pub trait SessionParseStateExtensionAccess {
  /// 下标处参数的 UTF-8 视图
  fn ext_string(&self, idx: usize) -> Option<&str>;
  /// 下标处参数字节；越界返回 None
  fn ext_bytes(&self, idx: usize) -> Option<&[u8]>;
}

impl SessionParseStateExtensionAccess for SessionParseState {
  #[inline]
  fn ext_string(&self, idx: usize) -> Option<&str> {
    from_utf8(self.get_arg_slice_by_ref(idx).as_slice()).ok()
  }

  #[inline]
  fn ext_bytes(&self, idx: usize) -> Option<&[u8]> {
    (idx < self.count).then(|| self.get_arg_slice_by_ref(idx).as_slice())
  }
}

/// 解析态序列化快照（C# parseState.SerializeTo 的安全零拷贝封装，
/// 供慢日志入库等快照场景使用）
pub fn serialize_snapshot(parse_state: &SessionParseState) -> Vec<u8> {
  let len = parse_state.get_serialized_length();
  let mut buf = vec![0u8; len];
  if len > 0 {
    unsafe {
      parse_state.serialize_to(buf.as_mut_ptr(), len);
    }
  }
  buf
}

/// libs/server/SessionParseStateExtensions.cs:TryGetInfoMetricsType
#[inline]
pub fn try_get_info_metrics_type(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<InfoMetricsType> {
  let arg = parse_state.ext_bytes(idx)?;
  InfoMetricsType::from_name(arg)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetLatencyMetricsType
#[inline]
pub fn try_get_latency_metrics_type(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<LatencyMetricsType> {
  let arg = parse_state.ext_bytes(idx)?;
  LatencyMetricsType::from_name(arg)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetClientName
///
/// 33..=126 可打印字符，空串允许（清名语义）；非 UTF-8 视为失败
pub fn try_get_client_name(parse_state: &SessionParseState, idx: usize) -> Option<&str> {
  let name = parse_state.ext_string(idx)?;
  try_get_client_name_str(name)
}

pub fn try_get_client_name_str(name: &str) -> Option<&str> {
  if name.is_empty() {
    return Some(name);
  }
  name
    .bytes()
    .all(|c| (33..=126).contains(&c))
    .then_some(name)
}

pub fn try_get_client_name_bytes(raw: &[u8]) -> Option<&str> {
  let name = from_utf8(raw).ok()?;
  try_get_client_name_str(name)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetClientType
pub fn try_get_client_type(parse_state: &SessionParseState, idx: usize) -> Option<ClientType> {
  let arg = parse_state.ext_bytes(idx)?;
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

/// libs/server/SessionParseStateExtensions.cs:TryGetBitFieldOverflow
pub fn try_get_bit_field_overflow(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<BitFieldOverflow> {
  parse_bitfield_overflow_slice(parse_state.ext_bytes(idx)?)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetBitfieldEncoding
///
/// 解析 `i<位宽>` / `u<位宽>`：位宽 > 0，有符号 ≤ 64，无符号 < 64；
/// 数值走 C# TryReadInt64Safe 严格口径（可选符号、拒绝前导零、整体消费）
pub fn try_get_bitfield_encoding(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<(i64, bool)> {
  let raw = parse_state.ext_bytes(idx)?;
  if raw.len() <= 1 {
    return None;
  }
  let is_signed = match raw[0] {
    b'i' => true,
    b'u' => false,
    _ => return None,
  };
  let bit_count = strict_i64(&raw[1..])?;
  (bit_count > 0
    && (if is_signed {
      bit_count <= 64
    } else {
      bit_count < 64
    }))
  .then_some((bit_count, is_signed))
}

/// libs/server/SessionParseStateExtensions.cs:TryGetBitfieldOffset
///
/// `#<n>` 前缀表示按位宽倍乘（multiplyOffset = true），否则为位偏移；
/// 数值同走严格 i64 口径；值须 ≥ 0
pub fn try_get_bitfield_offset(parse_state: &SessionParseState, idx: usize) -> Option<(i64, bool)> {
  let raw = parse_state.ext_bytes(idx)?;
  let (digits, multiply_offset) = match raw {
    [b'#', rest @ ..] if !rest.is_empty() => (rest, true),
    [] => return None,
    _ => (raw, false),
  };
  let offset = strict_i64(digits)?;
  (offset >= 0).then_some((offset, multiply_offset))
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoSearchOptions
///
/// GEOSEARCH 族命令选项解析；返回 (选项, 目标键下标（STORE/STOREDIST），错误文案)。
/// `command` 取 GEOSEARCH/GEOSEARCHSTORE/`GEORADIUS[RO]`/`GEORADIUSBYMEMBER[RO]` 之一，
/// `count` 为参数总数，`arg(i)` 取第 i 个参数字节。
pub fn try_get_geo_search_options(
  parse_state: &SessionParseState,
  command: &str,
) -> (Option<GeoSearchOptions>, isize, Option<Vec<u8>>) {
  let count = parse_state.count;
  let is_geosearch = command == "GEOSEARCH";
  let is_geosearchstore = command == "GEOSEARCHSTORE";
  let is_by_member = command == "GEORADIUSBYMEMBER" || command == "GEORADIUSBYMEMBER_RO";
  let is_georadius = command == "GEORADIUS" || command == "GEORADIUS_RO";
  let read_only = is_geosearch || command == "GEORADIUS_RO" || command == "GEORADIUSBYMEMBER_RO";
  let supports_store = command == "GEORADIUS" || is_by_member;

  let mut opts = GeoSearchOptions::default();
  let mut dest_idx: isize = if is_geosearchstore { 0 } else { -1 };
  let mut arg_num_error = false;
  let mut store_dist = false;
  let mut token = 0usize;

  let err = |e: &str| -> Option<Vec<u8>> { Some(e.as_bytes().to_vec()) };
  let num_err = || -> Option<Vec<u8>> {
    Some(
      cmd_strings::GENERIC_ERR_WRONG_NUM_ARGS
        .replace("{0}", command)
        .into_bytes(),
    )
  };

  // GEORADIUS(BYMEMBER)[RO] 的位置参数（原点 + 半径 + 单位）先行读取
  if is_georadius || is_by_member {
    if is_by_member {
      opts.from_member = parse_state
        .ext_bytes(token)
        .map_or_else(Vec::new, Vec::from);
      token += 1;
      opts.origin = GeoOriginType::FromMember;
    } else {
      match try_geo_lon_lat_pair(parse_state, token) {
        Some((lon, lat, None)) => {
          opts.lon = lon;
          opts.lat = lat;
          token += 2;
          opts.origin = GeoOriginType::FromLonLat;
        }
        Some((_, _, e)) => {
          return (
            None,
            dest_idx,
            e.or_else(|| err(cmd_strings::RESP_ERR_NOT_VALID_FLOAT)),
          );
        }
        None => return (None, dest_idx, err(cmd_strings::RESP_ERR_NOT_VALID_FLOAT)),
      }
    }

    // 半径
    let Some(radius) = parse_state.ext_bytes(token).and_then(strict_double) else {
      return (None, dest_idx, err(RESP_ERR_NOT_VALID_RADIUS));
    };
    token += 1;
    if radius < 0.0 {
      return (None, dest_idx, err(RESP_ERR_RADIUS_IS_NEGATIVE));
    }
    opts.radius = radius;
    opts.search_type = GeoSearchType::ByRadius;
    match parse_state.ext_bytes(token).and_then(geo_distance_unit) {
      Some(unit) => opts.unit = unit,
      None => {
        return (None, dest_idx, err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT));
      }
    }
    token += 1;
  }

  // 逐 token 选项循环
  while token < count {
    let Some(token_bytes) = parse_state.ext_bytes(token) else {
      break;
    };
    token += 1;

    if is_geosearch || is_geosearchstore {
      if eq_upper_ignore_case(token_bytes, b"FROMMEMBER") {
        if opts.origin != GeoOriginType::Undefined {
          return (
            None,
            dest_idx,
            err(cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR),
          );
        }
        if count == token {
          arg_num_error = true;
          break;
        }
        opts.from_member = parse_state
          .ext_bytes(token)
          .map_or_else(Vec::new, Vec::from);
        token += 1;
        opts.origin = GeoOriginType::FromMember;
        continue;
      }
      if eq_upper_ignore_case(token_bytes, b"FROMLONLAT") {
        if opts.origin != GeoOriginType::Undefined {
          return (
            None,
            dest_idx,
            err(cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR),
          );
        }
        if count - token < 2 {
          arg_num_error = true;
          break;
        }
        match try_geo_lon_lat_pair(parse_state, token) {
          Some((lon, lat, None)) => {
            opts.lon = lon;
            opts.lat = lat;
          }
          Some((_, _, e)) => {
            return (
              None,
              dest_idx,
              e.or_else(|| err(cmd_strings::RESP_ERR_NOT_VALID_FLOAT)),
            );
          }
          None => return (None, dest_idx, err(cmd_strings::RESP_ERR_NOT_VALID_FLOAT)),
        }
        token += 2;
        opts.origin = GeoOriginType::FromLonLat;
        continue;
      }
      if eq_upper_ignore_case(token_bytes, b"BYRADIUS") {
        if opts.search_type != GeoSearchType::Undefined {
          return (
            None,
            dest_idx,
            err(cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR),
          );
        }
        if count - token < 2 {
          arg_num_error = true;
          break;
        }
        match parse_state.ext_bytes(token).and_then(strict_double) {
          Some(radius) => opts.radius = radius,
          None => return (None, dest_idx, err(RESP_ERR_NOT_VALID_RADIUS)),
        }
        token += 1;
        if opts.radius < 0.0 {
          return (None, dest_idx, err(RESP_ERR_RADIUS_IS_NEGATIVE));
        }
        opts.search_type = GeoSearchType::ByRadius;
        match parse_state.ext_bytes(token).and_then(geo_distance_unit) {
          Some(unit) => opts.unit = unit,
          None => {
            return (None, dest_idx, err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT));
          }
        }
        token += 1;
        continue;
      }
      if eq_upper_ignore_case(token_bytes, b"BYBOX") {
        if opts.search_type != GeoSearchType::Undefined {
          return (
            None,
            dest_idx,
            err(cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR),
          );
        }
        opts.search_type = GeoSearchType::ByBox;
        if count - token < 3 {
          arg_num_error = true;
          break;
        }
        match parse_state.ext_bytes(token).and_then(strict_double) {
          Some(w) => opts.box_width = w,
          None => return (None, dest_idx, err(RESP_ERR_NOT_VALID_WIDTH)),
        }
        token += 1;
        match parse_state.ext_bytes(token).and_then(strict_double) {
          // C# boxHeight getter/setter 即 radius（BYBOX 高度复用半径字段）
          Some(h) => opts.radius = h,
          None => return (None, dest_idx, err(RESP_ERR_NOT_VALID_HEIGHT)),
        }
        token += 1;
        if opts.box_width < 0.0 || opts.radius < 0.0 {
          return (None, dest_idx, err(RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE));
        }
        match parse_state.ext_bytes(token).and_then(geo_distance_unit) {
          Some(unit) => opts.unit = unit,
          None => {
            return (None, dest_idx, err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT));
          }
        }
        token += 1;
        continue;
      }
    }

    if eq_upper_ignore_case(token_bytes, b"ASC") {
      opts.sort = GeoOrder::Ascending;
      continue;
    }
    if eq_upper_ignore_case(token_bytes, b"DESC") {
      opts.sort = GeoOrder::Descending;
      continue;
    }
    if eq_upper_ignore_case(token_bytes, b"COUNT") {
      if count == token {
        arg_num_error = true;
        break;
      }
      // C# TryGetInt 严格口径
      let parsed = parse_state
        .ext_bytes(token)
        .and_then(strict_i32)
        .map(i64::from);
      match parsed {
        Some(count_value) => opts.count_value = count_value,
        None => {
          return (
            None,
            dest_idx,
            err(cmd_strings::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
          );
        }
      }
      token += 1;
      if opts.count_value <= 0 {
        return (None, dest_idx, err(RESP_ERR_COUNT_IS_NOT_POSITIVE));
      }
      if count > token
        && parse_state
          .ext_bytes(token)
          .is_some_and(|p| eq_upper_ignore_case(p, b"ANY"))
      {
        opts.with_count_any = true;
        token += 1;
      }
      continue;
    }

    if !read_only {
      if supports_store && eq_upper_ignore_case(token_bytes, b"STORE") {
        if count == token {
          arg_num_error = true;
          break;
        }
        token += 1;
        dest_idx = token as isize;
        continue;
      }
      if eq_upper_ignore_case(token_bytes, b"STOREDIST") {
        if supports_store {
          if count == token {
            arg_num_error = true;
            break;
          }
          token += 1;
          dest_idx = token as isize;
        }
        store_dist = true;
        continue;
      }
    }

    if eq_upper_ignore_case(token_bytes, b"WITHCOORD") {
      opts.with_coord = true;
      continue;
    }
    if eq_upper_ignore_case(token_bytes, b"WITHDIST") {
      opts.with_dist = true;
      continue;
    }
    if eq_upper_ignore_case(token_bytes, b"WITHHASH") {
      opts.with_hash = true;
      continue;
    }

    return (
      None,
      dest_idx,
      err(cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR),
    );
  }

  // 必选项校验：原点与形状缺一即参数错误
  if opts.origin == GeoOriginType::Undefined || opts.search_type == GeoSearchType::Undefined {
    arg_num_error = true;
  }
  if arg_num_error {
    return (None, dest_idx, num_err());
  }
  if dest_idx != -1 {
    if opts.with_dist || opts.with_coord || opts.with_hash {
      return (
        None,
        dest_idx,
        Some(
          format!(
            "ERR STORE option in {command} is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
          )
          .into_bytes(),
        ),
      );
    }
    opts.with_dist = store_dist;
    // 存入 ZSET 的评分须为距离或哈希之一
    if !opts.with_dist && !opts.with_hash {
      opts.with_hash = true;
    }
  }
  (Some(opts), dest_idx, None)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetManagerType
pub fn try_get_manager_type(parse_state: &SessionParseState, idx: usize) -> Option<ManagerType> {
  manager_type_from_token(parse_state.ext_bytes(idx)?)
}

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
pub fn try_get_operation_direction(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<OperationDirection> {
  let arg = parse_state.ext_bytes(idx)?;
  operation_direction_from_token(arg)
}

pub fn operation_direction_from_token(arg: &[u8]) -> Option<OperationDirection> {
  if eq_upper_ignore_case(arg, b"LEFT") {
    Some(OperationDirection::Left)
  } else if eq_upper_ignore_case(arg, b"RIGHT") {
    Some(OperationDirection::Right)
  } else {
    None
  }
}

/// libs/server/SessionParseStateExtensions.cs:TryGetSortedSetAddOption
pub fn try_get_sorted_set_add_option(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<SortedSetAddOption> {
  let arg = parse_state.ext_bytes(idx)?;
  wresp::try_get_sorted_set_add_option(arg)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetExpireOption
pub fn try_get_expire_option(parse_state: &SessionParseState, idx: usize) -> Option<ExpireOption> {
  let arg = parse_state.ext_bytes(idx)?;
  wresp::try_get_expire_option(arg)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetSortedSetAggregateType
pub fn try_get_sorted_set_aggregate_type(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<ZSetAggregate> {
  let arg = parse_state.ext_bytes(idx)?;
  wresp::try_get_sorted_set_aggregate_type(arg)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetExpirationOption
pub fn try_get_expiration_option(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<ExpirationOption> {
  let token = parse_state.ext_bytes(idx)?;
  wresp::try_get_expiration_option(token)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoDistanceUnit
pub fn geo_distance_unit(raw: &[u8]) -> Option<GeoDistanceUnitType> {
  let value = if eq_upper_ignore_case(raw, b"M") {
    GeoDistanceUnitType::M
  } else if eq_upper_ignore_case(raw, b"KM") {
    GeoDistanceUnitType::Km
  } else if eq_upper_ignore_case(raw, b"MI") {
    GeoDistanceUnitType::Mi
  } else if eq_upper_ignore_case(raw, b"FT") {
    GeoDistanceUnitType::Ft
  } else {
    return None;
  };
  Some(value)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat
///
/// 经纬度对解析 + 值域校验（C# GeoHash.Longitude/Latitude 边界）；
/// 返回 (经度, 纬度, 错误文案)
pub fn try_get_geo_lon_lat(
  parse_state: &SessionParseState,
  idx: usize,
) -> (Option<f64>, Option<f64>, Option<Vec<u8>>) {
  match try_geo_lon_lat_pair(parse_state, idx) {
    Some((lon, lat, None)) => (Some(lon), Some(lat), None),
    Some((lon, lat, e)) => (Some(lon), Some(lat), e),
    None => (
      None,
      None,
      Some(cmd_strings::RESP_ERR_NOT_VALID_FLOAT.as_bytes().to_vec()),
    ),
  }
}

/// 经纬度对内部实现：Some((lon, lat, 错误))；None 表示浮点解析失败
fn try_geo_lon_lat_pair(
  parse_state: &SessionParseState,
  idx: usize,
) -> Option<(f64, f64, Option<Vec<u8>>)> {
  let lon_raw = parse_state.ext_bytes(idx)?;
  let lat_raw = parse_state.ext_bytes(idx + 1)?;
  let (Some(lon), Some(lat)) = (strict_double(lon_raw), strict_double(lat_raw)) else {
    return Some((
      0.0,
      0.0,
      Some(cmd_strings::RESP_ERR_NOT_VALID_FLOAT.as_bytes().to_vec()),
    ));
  };
  const LONGITUDE_MIN: f64 = -180.0;
  const LONGITUDE_MAX: f64 = 180.0;
  const LATITUDE_MIN: f64 = -85.05112878;
  const LATITUDE_MAX: f64 = 85.05112878;
  if !(LONGITUDE_MIN..=LONGITUDE_MAX).contains(&lon)
    || !(LATITUDE_MIN..=LATITUDE_MAX).contains(&lat)
  {
    return Some((
      lon,
      lat,
      Some(format!("ERR invalid longitude,latitude pair {lon:.6},{lat:.6}").into_bytes()),
    ));
  }
  Some((lon, lat, None))
}

/// libs/server/SessionParseStateExtensions.cs:TryGetTimeout
///
/// 超时（秒）解析：非负且 ≤ i32::MAX/1000（.NET API 毫秒上限）
pub fn try_get_timeout(
  parse_state: &SessionParseState,
  idx: usize,
) -> (Option<f64>, Option<Vec<u8>>) {
  let raw = parse_state.ext_bytes(idx);
  match raw.map(try_get_timeout_bytes) {
    Some(Ok(timeout)) => (Some(timeout), None),
    Some(Err(error)) => (None, Some(error.as_bytes().to_vec())),
    None => (
      None,
      Some(
        cmd_strings::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT
          .as_bytes()
          .to_vec(),
      ),
    ),
  }
}

/// TryGetTimeout 的字节参数形态（阻塞命令域共用解析核心：非负且 ≤
/// i32::MAX/1000，.NET API 毫秒上限）；失败返回 C# 错误文案
pub fn try_get_timeout_bytes(raw: &[u8]) -> Result<f64, &'static str> {
  const MAX_TIMEOUT: f64 = i32::MAX as f64 / 1000.0;
  let Some(timeout) = strict_double(raw) else {
    return Err(cmd_strings::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
  };
  if timeout < 0.0 {
    return Err(RESP_ERR_TIMEOUT_IS_NEGATIVE);
  }
  if timeout > MAX_TIMEOUT {
    return Err(RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE);
  }
  Ok(timeout)
}

/// libs/server/SessionParseStateExtensions.cs:ExtractCommandKeys
///
/// 从简化命令元数据的键规格提取参数区中的键（按下标升序）
pub fn extract_command_keys<'a>(
  parse_state: &'a SessionParseState,
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> Vec<&'a [u8]> {
  let mut keys: Vec<(&'a [u8], usize)> = Vec::new();
  for spec in key_specs {
    try_append_keys_from_spec(parse_state, spec, is_sub_command, &mut keys);
  }
  if key_specs.len() > 1 {
    keys.sort_unstable_by_key(|(_, i)| *i);
  }
  keys.into_iter().map(|(k, _)| k).collect()
}

/// libs/server/SessionParseStateExtensions.cs:ExtractCommandKeysAndFlags
///
/// 从简化命令元数据的键规格提取键 + 标志（按下标升序，零拷贝）
pub fn extract_command_keys_and_flags<'a>(
  parse_state: &'a SessionParseState,
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> Vec<(&'a [u8], u8)> {
  let mut keys_flags: Vec<(&'a [u8], u8, usize)> = Vec::new();
  for spec in key_specs {
    try_append_keys_and_flags_from_spec(parse_state, spec, is_sub_command, &mut keys_flags);
  }
  if key_specs.len() > 1 {
    keys_flags.sort_unstable_by_key(|(_, _, i)| *i);
  }
  keys_flags.into_iter().map(|(k, f, _)| (k, f)).collect()
}

/// libs/server/SessionParseStateExtensions.cs:TryAppendKeysFromSpec
fn try_append_keys_from_spec<'a>(
  parse_state: &'a SessionParseState,
  key_spec: &SimpleRespKeySpec,
  is_sub_command: bool,
  keys_to_indexes: &mut Vec<(&'a [u8], usize)>,
) -> bool {
  let Some((first_idx, last_idx, step)) =
    try_get_key_search_args_from_simple_key_spec(parse_state, key_spec, is_sub_command)
  else {
    return false;
  };
  let mut i = first_idx;
  while i <= last_idx {
    if let Some(bytes) = parse_state.ext_bytes(i).filter(|b| !b.is_empty()) {
      keys_to_indexes.push((bytes, i));
    }
    i += step;
  }
  true
}

/// libs/server/SessionParseStateExtensions.cs:TryAppendKeysAndFlagsFromSpec
fn try_append_keys_and_flags_from_spec<'a>(
  parse_state: &'a SessionParseState,
  key_spec: &SimpleRespKeySpec,
  is_sub_command: bool,
  keys_and_flags: &mut Vec<(&'a [u8], u8, usize)>,
) -> bool {
  let Some((first_idx, last_idx, step)) =
    try_get_key_search_args_from_simple_key_spec(parse_state, key_spec, is_sub_command)
  else {
    return false;
  };
  let mut i = first_idx;
  while i <= last_idx {
    if let Some(bytes) = parse_state.ext_bytes(i).filter(|b| !b.is_empty()) {
      keys_and_flags.push((bytes, key_spec.flags.0 as u8, i));
    }
    i += step;
  }
  true
}

/// 依简化键规格计算 (firstIdx, lastIdx, step)；直接委托 wnode::key_spec::SimpleRespKeySpec
#[inline]
pub fn try_get_key_search_args_from_simple_key_spec(
  parse_state: &SessionParseState,
  key_spec: &SimpleRespKeySpec,
  is_sub_command: bool,
) -> Option<(usize, usize, usize)> {
  key_spec.try_get_key_search_args(
    parse_state.count,
    |i| parse_state.ext_bytes(i),
    is_sub_command,
  )
}

/// 严格 f64 解析（C# parseState.TryGetDouble 默认 canBeInfinite: true：
/// INF/+INF/-INF 白名单接受、NaN 拒绝；唯一实现位于 wbase::num）
fn strict_double(raw: &[u8]) -> Option<f64> {
  strict_f64(raw, true)
}
