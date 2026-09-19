//! GEO RESP 命令（对标 libs/server/Resp/Objects/SortedSetGeoCommands.cs）
//!
//! GEOADD / GEOHASH / GEODIST / GEOPOS / GEOSEARCH / GEOSEARCHSTORE /
//! GEORADIUS(_RO) / GEORADIUSBYMEMBER(_RO)。语义经对象层
//! [`SortedSetObject`] 的 geo_* 分片执行；选项解析对标
//! SessionParseStateExtensions.TryGetGeoSearchOptions 的命令分派文法
//! （GEOSEARCH 族走 FROMMEMBER/FROMLONLAT + BYRADIUS/BYBOX 关键字，
//! GEORADIUS 族为位置参数）；存取经与 storage 会话域共享的信封编解码。

use std::borrow::Cow;

use wbase::num::{strict_f64, strict_i32};
use wcol::{
  ObjectOutput,
  geo::{
    GeoAddOptions, GeoOrder, GeoOriginType, GeoSearchOptions, GeoSearchType,
    geo_hash::GeoDistanceUnitType,
  },
  parse_utils::{GeoLonLatError, try_get_geo_distance_unit, try_get_geo_lon_lat},
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
  options::equals_ignore_case,
};

use crate::{
  resp::{
    objects::sorted_set_commands::{
      ZsetLoad, parse_pairs_payload, zset_load_sync, zset_save_or_gc,
    },
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::del_ttl_sync,
};
/// GEOSEARCH 族命令形态（决定选项文法与存储语义）
///
/// 对标 RespCommand.GEOSEARCH/GEOSEARCHSTORE/GEORADIUS(_RO)/GEORADIUSBYMEMBER(_RO)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoSearchCommandKind {
  /// GEOSEARCH key [FROMMEMBER m|FROMLONLAT lon lat] BYRADIUS r u|BYBOX w h u \[修饰词\]
  GeoSearch,
  /// GEOSEARCHSTORE dest src …（同 GEOSEARCH 文法，结果落目标键）
  GeoSearchStore,
  /// GEORADIUS key lon lat radius unit \[修饰词\] [STORE dest|STOREDIST dest]
  GeoRadius,
  /// GEORADIUS_RO（只读，无 STORE/STOREDIST）
  GeoRadiusRo,
  /// GEORADIUSBYMEMBER key member radius unit …
  GeoRadiusByMember,
  /// GEORADIUSBYMEMBER_RO
  GeoRadiusByMemberRo,
}

impl GeoSearchCommandKind {
  /// 命令名（错误文本用）
  fn name(self) -> &'static str {
    match self {
      Self::GeoSearch => "GEOSEARCH",
      Self::GeoSearchStore => "GEOSEARCHSTORE",
      Self::GeoRadius => "GEORADIUS",
      Self::GeoRadiusRo => "GEORADIUS_RO",
      Self::GeoRadiusByMember => "GEORADIUSBYMEMBER",
      Self::GeoRadiusByMemberRo => "GEORADIUSBYMEMBER_RO",
    }
  }

  /// 最少参数个数（含键；GEOSEARCHSTORE 含目标键，对标 C# paramsRequiredInCommand）
  fn params_required(self) -> usize {
    match self {
      Self::GeoRadius | Self::GeoRadiusRo => 5,
      Self::GeoRadiusByMember | Self::GeoRadiusByMemberRo => 4,
      Self::GeoSearch => 6,
      Self::GeoSearchStore => 7,
    }
  }

  /// 是否允许 STORE/STOREDIST 携带目标键（GEORADIUS 写变体）
  fn store_allowed(self) -> bool {
    matches!(self, Self::GeoRadius | Self::GeoRadiusByMember)
  }
}

/// GEOSEARCH 选项束解析结果
struct ParsedGeoSearch {
  opts: GeoSearchOptions,
  /// STORE/STOREDIST 目标键
  dest: Option<Vec<u8>>,
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat 两态错误组装：
/// 非浮点 → RESP_ERR_NOT_VALID_FLOAT；越界 → GenericErrLonLat 回显坐标
///（{lon:F6},{lat:F6}，F6 定点渲染见 [`format_f6`]）
fn geo_lon_lat_checked(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Cow<'static, str>> {
  try_get_geo_lon_lat(lon, lat).map_err(|e| match e {
    GeoLonLatError::NotFloat => Cow::Borrowed(cs::RESP_ERR_NOT_VALID_FLOAT),
    GeoLonLatError::OutOfRange(lon, lat) => Cow::Owned(
      cs::GENERIC_ERR_LON_LAT
        .replace("{0}", &format_f6(lon))
        .replace("{1}", &format_f6(lat)),
    ),
  })
}

/// C# `double.ToString("F6")` 逐字节对齐渲染：无穷词形为 Infinity/-Infinity
///（Rust {:.6} 输出 inf）；有限值定点六位、半点远离零舍入。二元浮点在十进
/// 制第 7 位存在精确半点（如 1/128 = 0.0078125，.NET 舍入 0.007813，{:.6}
/// 半到偶得 0.007812），故按 m·2^exp 精确有理舍入；a ≥ 2^53 必为整数值，
/// {:.6} 定点展开即精确。NaN 不可达（解析层恒拒）
fn format_f6(v: f64) -> String {
  if v == f64::INFINITY {
    return "Infinity".into();
  }
  if v == f64::NEG_INFINITY {
    return "-Infinity".into();
  }
  let sign = if v.is_sign_negative() { "-" } else { "" };
  let a = v.abs();
  if a < 5e-7 {
    // 精确半点 5e-7 非二元可表示，恒舍入为零
    return format!("{sign}0.000000");
  }
  if a >= 1e18 {
    // ≥ 2^53 整数值，无小数展开
    return format!("{sign}{a:.6}");
  }
  // a = mantissa·2^exp ∈ [5e-7, 1e18)：exp ∈ [-75, 7]，n < 1e24 < 2^80
  let bits = a.to_bits();
  let mantissa = (bits & 0xf_ffff_ffff_ffff) | (1 << 52);
  let exp = (((bits >> 52) & 0x7ff) as i32) - 1075;
  let p = u128::from(mantissa) * 1_000_000;
  let n = if exp >= 0 {
    // 整数值，无舍入
    p << exp
  } else {
    // n = round_half_up(p / 2^-exp)
    let q = 1u128 << (-exp);
    (2 * p + q) / (2 * q)
  };
  format!("{sign}{}.{:06}", n / 1_000_000, n % 1_000_000)
}

/// 解析双精度（TryGetDouble 严格语义；单一实现 [`strict_f64`]，
/// canBeInfinite: true 对齐 parseState.TryGetDouble 默认值）
fn parse_double(token: &[u8]) -> Option<f64> {
  strict_f64(token, true)
}

/// GEOSEARCH 族选项解析（对标 SessionParseStateExtensions.TryGetGeoSearchOptions）
///
/// args 为源键之后的参数序列
fn try_get_geo_search_options(
  args: &[&[u8]],
  kind: GeoSearchCommandKind,
) -> Result<ParsedGeoSearch, Cow<'static, str>> {
  let mut opts = GeoSearchOptions {
    unit: GeoDistanceUnitType::M,
    ..Default::default()
  };
  let mut dest: Option<Vec<u8>> = None;
  let mut store_dist = false;
  let mut arg_num_error = false;
  let geo_search_family = matches!(
    kind,
    GeoSearchCommandKind::GeoSearch | GeoSearchCommandKind::GeoSearchStore
  );
  let mut idx = 0_usize;

  if !geo_search_family {
    // GEORADIUS 族：圆心与形状为位置参数
    if matches!(
      kind,
      GeoSearchCommandKind::GeoRadiusByMember | GeoSearchCommandKind::GeoRadiusByMemberRo
    ) {
      let Some(member) = args.first() else {
        return Err(wrong_args(kind));
      };
      opts.from_member = member.to_vec();
      opts.origin = GeoOriginType::FromMember;
      idx = 1;
    } else {
      let (Some(lon_tok), Some(lat_tok)) = (args.first().copied(), args.get(1).copied()) else {
        return Err(wrong_args(kind));
      };
      let (lon, lat) = geo_lon_lat_checked(lon_tok, lat_tok)?;
      opts.lon = lon;
      opts.lat = lat;
      opts.origin = GeoOriginType::FromLonLat;
      idx = 2;
    }

    let Some(radius_tok) = args.get(idx).copied() else {
      return Err(wrong_args(kind));
    };
    let Some(radius) = parse_double(radius_tok) else {
      return Err(cs::RESP_ERR_NOT_VALID_RADIUS.into());
    };
    if radius < 0.0 {
      return Err(cs::RESP_ERR_RADIUS_IS_NEGATIVE.into());
    }
    opts.radius = radius;
    opts.search_type = GeoSearchType::ByRadius;
    idx += 1;

    let Some(unit_tok) = args.get(idx).copied() else {
      return Err(wrong_args(kind));
    };
    let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
      return Err(cs::RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT.into());
    };
    opts.unit = unit;
    idx += 1;
  }

  // 修饰词 / GEOSEARCH 族的圆心与形状关键字
  while idx < args.len() {
    let token = args[idx];
    idx += 1;

    if geo_search_family {
      if equals_ignore_case(token, b"FROMMEMBER") {
        if opts.origin != GeoOriginType::Undefined {
          return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
        }
        let Some(member) = args.get(idx) else {
          arg_num_error = true;
          break;
        };
        opts.from_member = member.to_vec();
        opts.origin = GeoOriginType::FromMember;
        idx += 1;
        continue;
      }

      if equals_ignore_case(token, b"FROMLONLAT") {
        if opts.origin != GeoOriginType::Undefined {
          return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
        }
        let (Some(lon_tok), Some(lat_tok)) = (args.get(idx).copied(), args.get(idx + 1).copied())
        else {
          arg_num_error = true;
          break;
        };
        let (lon, lat) = geo_lon_lat_checked(lon_tok, lat_tok)?;
        opts.lon = lon;
        opts.lat = lat;
        opts.origin = GeoOriginType::FromLonLat;
        idx += 2;
        continue;
      }

      if equals_ignore_case(token, b"BYRADIUS") {
        if opts.search_type != GeoSearchType::Undefined {
          return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
        }
        let (Some(radius_tok), Some(unit_tok)) =
          (args.get(idx).copied(), args.get(idx + 1).copied())
        else {
          arg_num_error = true;
          break;
        };
        let Some(radius) = parse_double(radius_tok) else {
          return Err(cs::RESP_ERR_NOT_VALID_RADIUS.into());
        };
        if radius < 0.0 {
          return Err(cs::RESP_ERR_RADIUS_IS_NEGATIVE.into());
        }
        let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
          return Err(cs::RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT.into());
        };
        opts.radius = radius;
        opts.search_type = GeoSearchType::ByRadius;
        opts.unit = unit;
        idx += 2;
        continue;
      }

      if equals_ignore_case(token, b"BYBOX") {
        if opts.search_type != GeoSearchType::Undefined {
          return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
        }
        let (Some(width_tok), Some(height_tok), Some(unit_tok)) = (
          args.get(idx).copied(),
          args.get(idx + 1).copied(),
          args.get(idx + 2).copied(),
        ) else {
          arg_num_error = true;
          break;
        };
        let Some(width) = parse_double(width_tok) else {
          return Err(cs::RESP_ERR_NOT_VALID_WIDTH.into());
        };
        let Some(height) = parse_double(height_tok) else {
          return Err(cs::RESP_ERR_NOT_VALID_HEIGHT.into());
        };
        if width < 0.0 || height < 0.0 {
          return Err(cs::RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE.into());
        }
        let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
          return Err(cs::RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT.into());
        };
        opts.box_width = width;
        // 高度复用 radius 槽位（C# GeoSearchOptions.boxHeight 即 radius）
        opts.radius = height;
        opts.search_type = GeoSearchType::ByBox;
        opts.unit = unit;
        idx += 3;
        continue;
      }
    }

    if equals_ignore_case(token, b"ASC") {
      opts.sort = GeoOrder::Ascending;
      continue;
    }
    if equals_ignore_case(token, b"DESC") {
      opts.sort = GeoOrder::Descending;
      continue;
    }

    if equals_ignore_case(token, cs::COUNT) {
      let Some(count_tok) = args.get(idx) else {
        arg_num_error = true;
        break;
      };
      // C# TryGetInt（int32）：溢出即非整数（SessionParseStateExtensions.cs:478）
      let Some(v) = strict_i32(count_tok) else {
        return Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.into());
      };
      if v <= 0 {
        return Err(cs::RESP_ERR_COUNT_IS_NOT_POSITIVE.into());
      }
      opts.count_value = i64::from(v);
      idx += 1;
      if let Some(peek) = args.get(idx)
        && equals_ignore_case(peek, b"ANY")
      {
        opts.with_count_any = true;
        idx += 1;
      }
      continue;
    }

    // STORE/STOREDIST：仅 GEORADIUS 写变体可携带目标键；GEOSEARCHSTORE 的
    // STOREDIST 只置距离标记（目标键为命令首参）
    if kind.store_allowed() && equals_ignore_case(token, b"STORE") {
      let Some(dest_tok) = args.get(idx) else {
        arg_num_error = true;
        break;
      };
      dest = Some(dest_tok.to_vec());
      idx += 1;
      continue;
    }
    if !matches!(
      kind,
      GeoSearchCommandKind::GeoSearch
        | GeoSearchCommandKind::GeoRadiusRo
        | GeoSearchCommandKind::GeoRadiusByMemberRo
    ) && equals_ignore_case(token, b"STOREDIST")
    {
      if kind.store_allowed() {
        let Some(dest_tok) = args.get(idx) else {
          arg_num_error = true;
          break;
        };
        dest = Some(dest_tok.to_vec());
        idx += 1;
      }
      store_dist = true;
      continue;
    }

    if equals_ignore_case(token, b"WITHCOORD") {
      opts.with_coord = true;
      continue;
    }
    if equals_ignore_case(token, b"WITHDIST") {
      opts.with_dist = true;
      continue;
    }
    if equals_ignore_case(token, b"WITHHASH") {
      opts.with_hash = true;
      continue;
    }

    return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
  }

  // 圆心与形状均必填
  if opts.origin == GeoOriginType::Undefined || opts.search_type == GeoSearchType::Undefined {
    arg_num_error = true;
  }
  if arg_num_error {
    return Err(wrong_args(kind));
  }

  // 存储变体：WITH* 互斥；分数取 GeoHash 或距离（二者必居其一）
  if dest.is_some() || kind == GeoSearchCommandKind::GeoSearchStore {
    if opts.with_dist || opts.with_coord || opts.with_hash {
      return Err(store_incompat(kind));
    }
    opts.with_dist = store_dist;
    if !opts.with_dist {
      opts.with_hash = true;
    }
  }

  Ok(ParsedGeoSearch { opts, dest })
}

/// wrong number of arguments 错误帧
///（GenericErrWrongNumArgs 模板按命令名展开，错误冷路径允许一次堆分配）
fn wrong_args(kind: GeoSearchCommandKind) -> Cow<'static, str> {
  Cow::Owned(cs::GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", kind.name()))
}

/// STORE 与 WITH* 互斥错误帧（对标 CmdStrings.GenericErrStoreCommand，单模板
/// 按命令名展开，同 wrong_args 的 GenericErrWrongNumArgs 范式）
fn store_incompat(kind: GeoSearchCommandKind) -> Cow<'static, str> {
  Cow::Owned(cs::GENERIC_ERR_STORE_COMMAND.replace("{0}", kind.name()))
}

impl RespServerSession {
  /// GEOADD key [NX|XX|CH] longitude latitude member [longitude latitude member ...]
  ///
  /// libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoAdd
  pub fn geo_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4.., output, "GEOADD");

    let key = parse_state[0];

    // 选项词元
    let mut curr_token_idx = 1;
    let mut add_option = GeoAddOptions::NONE;
    while curr_token_idx < parse_state.len() {
      let token = parse_state[curr_token_idx];
      let option = if equals_ignore_case(token, b"CH") {
        GeoAddOptions::CH
      } else if equals_ignore_case(token, b"NX") {
        GeoAddOptions::NX
      } else if equals_ignore_case(token, b"XX") {
        GeoAddOptions::XX
      } else {
        break;
      };
      add_option |= option;
      curr_token_idx += 1;
    }

    if add_option.contains(GeoAddOptions::NX) && add_option.contains(GeoAddOptions::XX) {
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    // 成员三元组校验（C# do-while 至少执行一次：选项词元吞掉全部实参时
    // idx == len，同样触发 syntax error，而非静默空集 :0）
    let member_start = curr_token_idx;
    let mut idx = curr_token_idx;
    loop {
      if idx > parse_state.len() - 3 {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      if let Err(e) = geo_lon_lat_checked(parse_state[idx], parse_state[idx + 1]) {
        cs::write_error_raw(output, &e);
        return Ok(true);
      }
      idx += 3;
      if idx >= parse_state.len() {
        break;
      }
    }

    let (mut obj, existed) = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => (SortedSetObject::new(), false),
      ZsetLoad::Present(o) => (o, true),
    };

    let mut obj_out = ObjectOutput::mount(output);
    obj.operate(
      SortedSetOperation::Geoadd as u8,
      &parse_state[member_start..],
      i32::from(add_option.bits()),
      0,
      &mut obj_out,
      self.resp_protocol_version,
    );

    // 回写：错误回复不落库；缺失键上仍空则不创建（三元组全被拒等场景）；
    // 写回失败回退挂载点（Degrade 整体重放 / 错误帧独占应答）
    if obj_out.payload_view().first() != Some(&b'-') && (existed || !obj.sorted_set_dict.is_empty())
    {
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => {
          obj_out.reset();
          return Ok(false);
        }
        Err(_) => {
          obj_out.reset();
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }
    Ok(true)
  }

  /// GEOHASH / GEODIST / GEOPOS key ...
  ///
  /// libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoCommands
  pub fn geo_commands<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    op: SortedSetOperation,
  ) -> wresp::Result<bool> {
    // 最少参数个数对齐 C# paramsRequiredInCommand（GEODIST=3，GEOHASH/GEOPOS=1）；
    // 参数个数错误命令名恒报 "command"（C# `var cmd = nameof(command)` quirk，
    // 三命令同文案，SortedSetGeoCommands.cs:119）
    let min_args = match op {
      SortedSetOperation::Geodist => 3,
      SortedSetOperation::Geohash => 1,
      _ => 1,
    };
    check_arg_count!(parse_state, min_args.., output, "command");

    // GEODIST key m1 m2 [unit]：C# Count>3 即校验第 4 参单位词元合法性
    //（len>=5 亦不跳过），失败报 RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT
    if op == SortedSetOperation::Geodist
      && parse_state.len() > 3
      && try_get_geo_distance_unit(parse_state[3]).is_none()
    {
      cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT);
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        // 键缺失：GEODIST → null；GEOHASH/GEOPOS → 每成员 null 数组项
        if op == SortedSetOperation::Geodist {
          output.write_resp_null_ver(self.resp_protocol_version);
        } else {
          output.write_resp_array_len(parse_state.len() - 1);
          for _ in 1..parse_state.len() {
            output.write_resp_null_array_ver(self.resp_protocol_version);
          }
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    obj.operate(
      op as u8,
      &parse_state[1..],
      0,
      0,
      &mut ObjectOutput::mount(output),
      self.resp_protocol_version,
    );
    Ok(true)
  }

  /// GEOSEARCH / GEOSEARCHSTORE / GEORADIUS(_RO) / GEORADIUSBYMEMBER(_RO)
  ///
  /// libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoSearchCommands
  pub fn geo_search_commands<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    kind: GeoSearchCommandKind,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, kind.params_required().., output, kind.name());

    // GEOSEARCHSTORE：首参为目标键，源为第二参
    let source_idx = usize::from(kind == GeoSearchCommandKind::GeoSearchStore);
    let key = parse_state[source_idx];
    let args = &parse_state[source_idx + 1..];

    let parsed = match try_get_geo_search_options(args, kind) {
      Ok(p) => p,
      // 具体错误文本（参数个数/单位/半径/COUNT/STORE 互斥等）逐字透传
      Err(e) => {
        cs::write_error_raw(output, &e);
        return Ok(true);
      }
    };
    let mut opts = parsed.opts;
    // 存储变体的目标键：STORE/STOREDIST 携带，或 GEOSEARCHSTORE 的命令首参
    let store_dest = match parsed.dest {
      Some(d) => Some(d),
      None if kind == GeoSearchCommandKind::GeoSearchStore => Some(parse_state[0].to_vec()),
      None => None,
    };

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：读变体空数组；存储变体删除目标键后回 :0（C# EXPIRE(destination, 0)）
        match &store_dest {
          Some(dest) => match zset_save_or_gc(store, dest, &SortedSetObject::new()) {
            Ok(true) => output.write_resp_int(0),
            Ok(false) => return Ok(false),
            Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
          },
          None => output.write_resp_array_len(0),
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    match store_dest {
      None => {
        obj.geo_search(
          &mut opts,
          &mut ObjectOutput::mount(output),
          self.resp_protocol_version,
          true,
        );
      }
      Some(dest) => {
        // 存储变体（解析层已强制 withHash 或 withDist）：分值取 GeoHash 或距离，
        // 命中成员成对落目标集合（对标 C# GeoSearchStore 的 ZADD 收尾）；
        // 解析消费非回显：负载挂本地 sink（错误臂冷路径透传一次）
        let mut sink = Vec::new();
        let mut obj_out = ObjectOutput::mount(&mut sink);
        obj.geo_search(&mut opts, &mut obj_out, self.resp_protocol_version, false);
        if obj_out.payload_view().first() == Some(&b'-') {
          // FROMMEMBER 圆心缺失等对象层错误透传
          drop(obj_out);
          output.append(&mut sink);
          return Ok(true);
        }
        let dst = SortedSetObject::from_entries(parse_pairs_payload(obj_out.payload_view()));
        let count = dst.sorted_set_dict.len();
        // STORE 族目标键为 SET 语义（清既有 key 级 TTL）：对标 C# GeoSearchStore
        // 先统一面 Delete dst 再 RMW ZADD（ObjectStore/SortedSetOps.cs），信封域
        // upsert 默认保留 TTL，故显式清退
        match del_ttl_sync(store, &dest) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
        match zset_save_or_gc(store, &dest, &dst) {
          Ok(true) => output.write_resp_int(count as i64),
          Ok(false) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
    }
    Ok(true)
  }
}
/// 慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/Objects/SortedSetGeoCommands.cs 各命令经 Tsavorite
/// pending 读 CompletePending 后重放的异步形态；选项解析复用同步段
/// try_get_geo_search_options 单源。`Err(())` 为存储 IO 失败，由 exec_slow
/// 统一应答 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wcol::{
    ObjLoad, ObjectOutput,
    geo::GeoAddOptions,
    object_payload::GarnetObjectPayload,
    zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
  };
  use wresp::{
    cmd_strings as cs, command::RespCommand, ext::RespVecExt, options::equals_ignore_case,
  };
  use wval::GarnetObjectType;

  use super::{GeoSearchCommandKind, parse_pairs_payload, try_get_geo_search_options};
  use crate::{
    resp::objects::object_store_utils::{
      obj_load_typed_async, obj_writeback_tiered, tiered_materialize_blob,
    },
    storage::session::storage_session::StorageSession,
  };

  /// 装载结果：对象 + 是否来自分层树物化（写回按升阶/降阶判据分派）
  type LoadedGeo = Option<Option<(SortedSetObject, bool)>>;

  /// 单键异步装载（升阶键物化降级）：外层 `None` = WRONGTYPE 错误行已写出；
  /// 内层 `None` = MISSING（调用方定短路应答）；bool = 物化自分层树
  async fn load_typed(
    storage: &StorageSession<'_, impl wdev::Device>,
    key: &[u8],
    output: &mut Vec<u8>,
  ) -> Result<LoadedGeo, ()> {
    let loaded = obj_load_typed_async(
      storage,
      key,
      GarnetObjectType::SortedSet,
      output,
      SortedSetObject::from_blob,
    )
    .await
    .map_err(|_| ())?;
    Ok(match loaded {
      // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存对象走对象层
      // 单源（C# GeoAdd/GeoSearch 无规模上限语义）
      ObjLoad::Degrade => {
        let Some(blob) =
          tiered_materialize_blob(&storage.batch, key, GarnetObjectType::SortedSet).await?
        else {
          return Err(());
        };
        // 物化载荷解码 fail-fast：畸形落错中止，不回退空对象销毁原键
        match SortedSetObject::from_blob(&blob) {
          Some(obj) => Some(Some((obj, true))),
          None => {
            log::error!(
              "geo load_typed: corrupted materialized payload, key='{}'",
              String::from_utf8_lossy(key)
            );
            return Err(());
          }
        }
      }
      ObjLoad::WrongType => None,
      ObjLoad::Missing => Some(None),
      ObjLoad::Present(o) => Some(Some((o, false))),
    })
  }

  /// GEO 慢路径写回收尾：复用分层感知统一漏斗（删空自愈 / 超阈值重灌 /
  /// 懒降阶信封 + 树清退 / 常规信封写回）
  async fn geo_save_back(
    storage: &StorageSession<'_, impl wdev::Device>,
    key: &[u8],
    obj: &SortedSetObject,
    _was_tiered: bool,
  ) -> Result<(), ()> {
    obj_writeback_tiered(storage, key, GarnetObjectType::SortedSet, obj).await
  }

  /// RESP 命令 → GEOSEARCH 族形态
  fn search_kind(cmd: RespCommand) -> Option<GeoSearchCommandKind> {
    match cmd {
      RespCommand::Geosearch => Some(GeoSearchCommandKind::GeoSearch),
      RespCommand::Geosearchstore => Some(GeoSearchCommandKind::GeoSearchStore),
      RespCommand::Georadius => Some(GeoSearchCommandKind::GeoRadius),
      RespCommand::GeoradiusRo => Some(GeoSearchCommandKind::GeoRadiusRo),
      RespCommand::Georadiusbymember => Some(GeoSearchCommandKind::GeoRadiusByMember),
      RespCommand::GeoradiusbymemberRo => Some(GeoSearchCommandKind::GeoRadiusByMemberRo),
      _ => None,
    }
  }

  /// GEO 命令统一慢路径分派
  pub(crate) async fn geo(
    storage: &StorageSession<'_, impl wdev::Device>,
    cmd: RespCommand,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let resp_version = storage.resp_protocol_version();
    let key = refs.first().copied().unwrap_or(&[]);
    let args = refs.get(1..).unwrap_or(&[]);

    // ---- GEOADD：选项词元扫描 + 三元组起始位对位同步段
    if cmd == RespCommand::Geoadd {
      let mut curr_token_idx = 1;
      let mut add_option = GeoAddOptions::NONE;
      while curr_token_idx < refs.len() {
        let token = refs[curr_token_idx];
        let option = if equals_ignore_case(token, b"CH") {
          GeoAddOptions::CH
        } else if equals_ignore_case(token, b"NX") {
          GeoAddOptions::NX
        } else if equals_ignore_case(token, b"XX") {
          GeoAddOptions::XX
        } else {
          break;
        };
        add_option |= option;
        curr_token_idx += 1;
      }
      let members = refs.get(curr_token_idx..).unwrap_or(&[]);

      let Some(loaded) = load_typed(storage, key, output).await? else {
        return Ok(());
      };
      // Missing → 新建对象（existed = false，同步段 Missing 分支对位）
      let (mut obj, existed, was_tiered) = match loaded {
        Some((o, tiered)) => (o, true, tiered),
        None => (SortedSetObject::new(), false, false),
      };
      let mut obj_out = ObjectOutput::mount(output);
      obj.operate(
        SortedSetOperation::Geoadd as u8,
        members,
        i32::from(add_option.bits()),
        0,
        &mut obj_out,
        resp_version,
      );
      // 回写：错误回复不落库；缺失键上仍空则不创建（三元组全被拒等场景）；
      // 写回失败回退挂载点再落错（慢路径统一应答前清场）
      if obj_out.payload_view().first() != Some(&b'-')
        && (existed || !obj.sorted_set_dict.is_empty())
        && geo_save_back(storage, key, &obj, was_tiered).await.is_err()
      {
        obj_out.reset();
        return Err(());
      }
      return Ok(());
    }

    // ---- GEOHASH / GEODIST / GEOPOS（单键 operate；op 同步段直传）
    let geo_op = match cmd {
      RespCommand::Geodist => Some(SortedSetOperation::Geodist),
      RespCommand::Geohash => Some(SortedSetOperation::Geohash),
      RespCommand::Geopos => Some(SortedSetOperation::Geopos),
      _ => None,
    };
    if let Some(op) = geo_op {
      // 键缺失：GEODIST → null；GEOHASH/GEOPOS → 每成员 null 数组项
      let Some(loaded) = load_typed(storage, key, output).await? else {
        return Ok(());
      };
      let mut obj = match loaded {
        Some((o, _)) => o,
        None => {
          if op == SortedSetOperation::Geodist {
            output.write_resp_null_ver(resp_version);
          } else {
            output.write_resp_array_len(refs.len() - 1);
            for _ in 1..refs.len() {
              output.write_resp_null_array_ver(resp_version);
            }
          }
          return Ok(());
        }
      };
      obj.operate(
        op as u8,
        args,
        0,
        0,
        &mut ObjectOutput::mount(output),
        resp_version,
      );
      return Ok(());
    }

    // ---- GEOSEARCH 族
    let Some(kind) = search_kind(cmd) else {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      return Ok(());
    };
    let source_idx = usize::from(kind == GeoSearchCommandKind::GeoSearchStore);
    let src_key = refs.get(source_idx).copied().unwrap_or(&[]);
    let sargs = refs.get(source_idx + 1..).unwrap_or(&[]);
    // 具体错误文本已在快路径写出（快路径已校验，防御臂不应抵达）
    let Ok(parsed) = try_get_geo_search_options(sargs, kind) else {
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      return Ok(());
    };
    let mut opts = parsed.opts;
    let store_dest = match parsed.dest {
      Some(d) => Some(d),
      None if kind == GeoSearchCommandKind::GeoSearchStore => Some(refs[0].to_vec()),
      None => None,
    };

    let Some(loaded) = load_typed(storage, src_key, output).await? else {
      return Ok(());
    };
    let mut obj = match loaded {
      // 源缺失：读变体空数组；存储变体删除目标键后回 :0（C# EXPIRE(destination, 0)）
      None => {
        if let Some(dest) = &store_dest {
          storage
            .delete_string(dest)
            .await
            .map(|_| ())
            .map_err(|_| ())?;
          output.write_resp_int(0);
        } else {
          output.write_resp_array_len(0);
        }
        return Ok(());
      }
      Some((o, _)) => o,
    };

    match store_dest {
      None => {
        obj.geo_search(
          &mut opts,
          &mut ObjectOutput::mount(output),
          resp_version,
          true,
        );
      }
      Some(dest) => {
        // 存储变体：分值取 GeoHash 或距离，命中成员成对落目标集合；
        // 解析消费非回显：负载挂本地 sink（错误臂冷路径透传一次）
        let mut sink = Vec::new();
        let mut obj_out = ObjectOutput::mount(&mut sink);
        obj.geo_search(&mut opts, &mut obj_out, resp_version, false);
        if obj_out.payload_view().first() == Some(&b'-') {
          // FROMMEMBER 圆心缺失等对象层错误透传
          drop(obj_out);
          output.append(&mut sink);
          return Ok(());
        }
        let dst = SortedSetObject::from_entries(parse_pairs_payload(obj_out.payload_view()));
        let count = dst.sorted_set_dict.len();
        // STORE 族目标键为 SET 语义（清既有 key 级 TTL）：对标 C# GeoSearchStore
        // 先统一面 Delete dst 再 RMW ZADD，信封域 upsert 默认保留须显式清退
        storage.persist_key(&dest).await.map_err(|_| ())?;
        if dst.sorted_set_dict.is_empty() {
          storage
            .delete_string(&dest)
            .await
            .map(|_| ())
            .map_err(|_| ())?;
        } else {
          storage
            .obj_save(&dest, GarnetObjectType::SortedSet, &dst.to_blob())
            .await
            .map_err(|_| ())?;
        }
        output.write_resp_int(count as i64);
      }
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::format_f6;

  /// C# double.ToString("F6") 逐字节对齐（Infinity 词形与半点远离零舍入）
  #[test]
  fn format_f6_matches_csharp_fixed6() {
    // 无穷词形（.NET 为 Infinity，Rust {:.6} 为 inf）
    assert_eq!(format_f6(f64::INFINITY), "Infinity");
    assert_eq!(format_f6(f64::NEG_INFINITY), "-Infinity");
    // 常规定点
    assert_eq!(format_f6(181.0), "181.000000");
    assert_eq!(format_f6(12.0), "12.000000");
    assert_eq!(format_f6(13.361389), "13.361389");
    // 十进制第 7 位精确半点远离零（{:.6} 半到偶会得 .007812）
    assert_eq!(format_f6(0.0078125), "0.007813");
    assert_eq!(format_f6(180.0078125), "180.007813");
    assert_eq!(format_f6(-0.0078125), "-0.007813");
    // 负号保留（含舍入为零与 -0.0）
    assert_eq!(format_f6(-1e-7), "-0.000000");
    assert_eq!(format_f6(-0.0), "-0.000000");
    // 整数值大数定点展开
    assert_eq!(format_f6(1e20), "100000000000000000000.000000");
  }
}
