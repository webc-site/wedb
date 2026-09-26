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
  parse_utils::{
    GeoLonLatError, try_get_geo_add_option, try_get_geo_distance_unit, try_get_geo_lon_lat,
  },
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wdev::Device;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
};

use crate::resp::{
  objects::{
    object_store_utils::{SyncStoreWindow, obj_writeback_recheck_sync},
    sorted_set_commands::{
      OptCursor, ZsetLoad, parse_pairs_payload, should_write_back, writeback_sync, zset_load_sync,
      zset_save_or_gc,
    },
  },
  resp_server_session::RespServerSession,
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

  /// GEOSEARCH/GEOSEARCHSTORE 文法族（圆心与形状走 FROMMEMBER/FROMLONLAT +
  /// BYRADIUS/BYBOX 关键字，而非位置参数）
  fn is_search_family(self) -> bool {
    matches!(self, Self::GeoSearch | Self::GeoSearchStore)
  }

  /// GEORADIUSBYMEMBER 族（圆心为位置参数成员名）
  fn by_member(self) -> bool {
    matches!(self, Self::GeoRadiusByMember | Self::GeoRadiusByMemberRo)
  }

  /// 是否识别 STOREDIST 词元（写变体带目标键，GEOSEARCHSTORE 仅置距离标记，
  /// RO 与 GEOSEARCH 读变体恒语法错误）
  fn store_dist_allowed(self) -> bool {
    self.store_allowed() || self == Self::GeoSearchStore
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

/// C# `double.ToString("F6")` 六位定点的十进制放大倍数（编译期常量，逐处复用）
const F6_SCALE: u128 = 1_000_000;
/// 六位定点恒舍为零的绝对值上界（精确半点 5e-7 非二元可表示）
const F6_ZERO_CEIL: f64 = 5e-7;
/// 此界以上二元浮点必为整数值（≥ 2^53 无小数位），`{:.6}` 定点展开即精确
const F6_INT_FLOOR: f64 = 1e18;

/// C# `double.ToString("F6")` 逐字节对齐渲染：无穷词形为 Infinity/-Infinity
///（Rust {:.6} 输出 inf）；有限值定点六位、半点远离零舍入。二元浮点在十进
/// 制第 7 位存在精确半点（如 1/128 = 0.0078125，.NET 舍入 0.007813，{:.6}
/// 半到偶得 0.007812），故按 m·2^exp 精确有理舍入；a ≥ 2^53 必为整数值，
/// `{:.6}` 定点展开即精确。NaN 不可达（解析层恒拒）
fn format_f6(v: f64) -> String {
  if v == f64::INFINITY {
    return "Infinity".into();
  }
  if v == f64::NEG_INFINITY {
    return "-Infinity".into();
  }
  let sign = if v.is_sign_negative() { "-" } else { "" };
  let a = v.abs();
  if a < F6_ZERO_CEIL {
    // 精确半点 5e-7 非二元可表示，恒舍入为零
    return format!("{sign}0.000000");
  }
  if a >= F6_INT_FLOOR {
    // ≥ 2^53 整数值，无小数展开
    return format!("{sign}{a:.6}");
  }
  // a = mantissa·2^exp ∈ [5e-7, 1e18)：exp ∈ [-75, 7]，n < 1e24 < 2^80
  let bits = a.to_bits();
  let mantissa = (bits & 0xf_ffff_ffff_ffff) | (1 << 52);
  let exp = (((bits >> 52) & 0x7ff) as i32) - 1075;
  let p = u128::from(mantissa) * F6_SCALE;
  let n = if exp >= 0 {
    // 整数值，无舍入
    p << exp
  } else {
    // n = round_half_up(p / 2^-exp)
    let q = 1u128 << (-exp);
    (2 * p + q) / (2 * q)
  };
  format!("{sign}{}.{:06}", n / F6_SCALE, n % F6_SCALE)
}

/// 解析双精度（TryGetDouble 严格语义；单一实现 [`strict_f64`]，
/// canBeInfinite: true 对齐 parseState.TryGetDouble 默认值）
fn parse_double(token: &[u8]) -> Option<f64> {
  strict_f64(token, true)
}

/// GEO 选项解析结果束：错误文本 Cow 直透传至应答帧
type GeoResult<T> = Result<T, Cow<'static, str>>;

/// 半径词元（非浮点 → NOT_VALID_RADIUS，负值 → RADIUS_IS_NEGATIVE，
/// GEORADIUS 位置参与 BYRADIUS 同核同序）
fn parse_radius(tok: &[u8]) -> GeoResult<f64> {
  let radius = parse_double(tok).ok_or(cs::RESP_ERR_NOT_VALID_RADIUS)?;
  if radius < 0.0 {
    return Err(cs::RESP_ERR_RADIUS_IS_NEGATIVE.into());
  }
  Ok(radius)
}

/// 距离单位词元（BYRADIUS / BYBOX / GEORADIUS 位置参 / GEODIST 尾参同核）
fn parse_unit(tok: &[u8]) -> GeoResult<GeoDistanceUnitType> {
  try_get_geo_distance_unit(tok).ok_or(cs::RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT.into())
}

/// GEOADD 选项词元连吞（NX / XX / CH）：返回（选项束, 首个成员三元组下标）
///
/// 首参为键，故自下标 1 起扫；遇非选项词元即止（余下交三元组校验）
fn scan_geo_add_options(args: &[&[u8]]) -> (GeoAddOptions, usize) {
  let mut add_option = GeoAddOptions::NONE;
  let mut idx = 1;
  while let Some(option) = args.get(idx).copied().and_then(try_get_geo_add_option) {
    add_option |= option;
    idx += 1;
  }
  (add_option, idx)
}

/// GEOSEARCH 族选项解析（对标 SessionParseStateExtensions.TryGetGeoSearchOptions）
///
/// args 为源键之后的参数序列；GEORADIUS 族先吞位置参数圆心/形状，其余修饰词
/// 与 GEOSEARCH 族共用同一词元游标单扫（缺失操作数即 wrong-num-args）
fn try_get_geo_search_options(
  args: &[&[u8]],
  kind: GeoSearchCommandKind,
) -> GeoResult<ParsedGeoSearch> {
  let mut opts = GeoSearchOptions {
    unit: GeoDistanceUnitType::M,
    ..Default::default()
  };
  let mut dest: Option<Vec<u8>> = None;
  let mut store_dist = false;
  // 操作数缺失收口为 wrong number of arguments（对标 C# break 后 argNumError）
  let short = || wrong_args(kind);
  let search_family = kind.is_search_family();

  // GEORADIUS 族：圆心与形状为位置参数，游标停在形状之后
  let mut cur = if search_family {
    OptCursor::new(args)
  } else {
    let mut pos = OptCursor::new(args);
    if kind.by_member() {
      opts.from_member = pos.one().ok_or_else(short)?.to_vec();
      opts.origin = GeoOriginType::FromMember;
    } else {
      let [lon_tok, lat_tok] = pos.take_arr::<2>().ok_or_else(short)?;
      let (lon, lat) = geo_lon_lat_checked(lon_tok, lat_tok)?;
      opts.lon = lon;
      opts.lat = lat;
      opts.origin = GeoOriginType::FromLonLat;
    }
    opts.radius = parse_radius(pos.one().ok_or_else(short)?)?;
    opts.search_type = GeoSearchType::ByRadius;
    opts.unit = parse_unit(pos.one().ok_or_else(short)?)?;
    pos
  };

  // 修饰词 / GEOSEARCH 族的圆心与形状关键字
  while let Some(token) = cur.one() {
    if search_family && token.eq_ignore_ascii_case(b"FROMMEMBER") {
      if opts.origin != GeoOriginType::Undefined {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
      }
      opts.from_member = cur.one().ok_or_else(short)?.to_vec();
      opts.origin = GeoOriginType::FromMember;
    } else if search_family && token.eq_ignore_ascii_case(b"FROMLONLAT") {
      if opts.origin != GeoOriginType::Undefined {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
      }
      let [lon_tok, lat_tok] = cur.take_arr::<2>().ok_or_else(short)?;
      let (lon, lat) = geo_lon_lat_checked(lon_tok, lat_tok)?;
      opts.lon = lon;
      opts.lat = lat;
      opts.origin = GeoOriginType::FromLonLat;
    } else if search_family && token.eq_ignore_ascii_case(b"BYRADIUS") {
      if opts.search_type != GeoSearchType::Undefined {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
      }
      let [radius_tok, unit_tok] = cur.take_arr::<2>().ok_or_else(short)?;
      opts.radius = parse_radius(radius_tok)?;
      opts.search_type = GeoSearchType::ByRadius;
      opts.unit = parse_unit(unit_tok)?;
    } else if search_family && token.eq_ignore_ascii_case(b"BYBOX") {
      if opts.search_type != GeoSearchType::Undefined {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
      }
      let [width_tok, height_tok, unit_tok] = cur.take_arr::<3>().ok_or_else(short)?;
      let Some(width) = parse_double(width_tok) else {
        return Err(cs::RESP_ERR_NOT_VALID_WIDTH.into());
      };
      let Some(height) = parse_double(height_tok) else {
        return Err(cs::RESP_ERR_NOT_VALID_HEIGHT.into());
      };
      // 宽高各校验后再并判负值（C# 判定序，先负值后单位）
      if width < 0.0 || height < 0.0 {
        return Err(cs::RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE.into());
      }
      opts.box_width = width;
      // 高度复用 radius 槽位（C# GeoSearchOptions.boxHeight 即 radius）
      opts.radius = height;
      opts.search_type = GeoSearchType::ByBox;
      opts.unit = parse_unit(unit_tok)?;
    } else if token.eq_ignore_ascii_case(b"ASC") {
      opts.sort = GeoOrder::Ascending;
    } else if token.eq_ignore_ascii_case(b"DESC") {
      opts.sort = GeoOrder::Descending;
    } else if token.eq_ignore_ascii_case(b"COUNT") {
      // C# TryGetInt（int32）：溢出即非整数（SessionParseStateExtensions.cs:478）；
      // 前导零拒收系 rust 严格收口（C# TryGetInt 因死参实际放行 007，见 doc/zh/deviations.md §32）
      let Some(v) = strict_i32(cur.one().ok_or_else(short)?) else {
        return Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.into());
      };
      if v <= 0 {
        return Err(cs::RESP_ERR_COUNT_IS_NOT_POSITIVE.into());
      }
      opts.count_value = i64::from(v);
      // ANY 为 COUNT 的紧邻可选修饰词
      if cur.eat(b"ANY") {
        opts.with_count_any = true;
      }
    } else if kind.store_allowed() && token.eq_ignore_ascii_case(b"STORE") {
      dest = Some(cur.one().ok_or_else(short)?.to_vec());
    } else if kind.store_dist_allowed() && token.eq_ignore_ascii_case(b"STOREDIST") {
      if kind.store_allowed() {
        dest = Some(cur.one().ok_or_else(short)?.to_vec());
      }
      store_dist = true;
    } else if token.eq_ignore_ascii_case(b"WITHCOORD") {
      opts.with_coord = true;
    } else if token.eq_ignore_ascii_case(b"WITHDIST") {
      opts.with_dist = true;
    } else if token.eq_ignore_ascii_case(b"WITHHASH") {
      opts.with_hash = true;
    } else {
      return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR.into());
    }
  }

  // 圆心与形状均必填
  if opts.origin == GeoOriginType::Undefined || opts.search_type == GeoSearchType::Undefined {
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
  pub fn geo_add<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 4.., output, "GEOADD");

    let key = parse_state[0];

    // 选项词元
    let (add_option, member_start) = scan_geo_add_options(parse_state);
    if add_option.contains(GeoAddOptions::NX) && add_option.contains(GeoAddOptions::XX) {
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    // 成员三元组校验（C# do-while 至少执行一次：选项词元吞掉全部实参时
    // idx == len，同样触发 syntax error，而非静默空集 :0）
    let mut idx = member_start;
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

    // 装载型写臂双保护·同步档：装载前取 rmw 窗（run_sync_rmw 同锁源），
    // 落笔前按装载态复验域归属
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
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

    // 回写门并入 zset 族单源 should_write_back（票 zcode-r147c-geozadd 案一·点位 2）：
    // Geoadd 非只读落默认臂，等价性——geo_add 对象层唯一出口 write_int64 恒出
    // :N 帧（wcol/src/zset/geo_impl.rs），故 out.written() 对本 op 恒真；错误帧
    // 短路支对 GEOADD 恒不成立（对象层无错误帧出口），缺键空不建键由首式承接。
    // 写回失败回退挂载点（Degrade 整体重放 / 错误帧独占应答）
    if should_write_back(SortedSetOperation::Geoadd, &obj_out, &obj, existed) {
      if !obj_writeback_recheck_sync(store, key, existed) {
        obj_out.reset();
        return Ok(false);
      }
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
  pub fn geo_commands<'a, D: Device>(
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
      && parse_unit(parse_state[3]).is_err()
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
  pub fn geo_search_commands<'a, D: Device>(
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
    // 装载型写臂先窗后装·同步档（§87 取窗点前移纪律，对标 C# GeoSearchStore
    // 入口即持 dst Exclusive 罩读算写全程，SortedSetGeoOps.cs:127-130）：dest
    // rmw 窗先于源装载/搜索求值取得并持至闭环，消除自指形（dest∈src）「窗外
    // 装载求值 → 开窗」间隙对面已确认写被陈旧快照整写的非可串行化；begin 失败
    // 与异构域拒写沿既有 Ok(false) 异步重放通道；读变体零改动无窗
    let window = match &store_dest {
      Some(dest) => {
        let Some(window) = SyncStoreWindow::begin(store, dest) else {
          return Ok(false);
        };
        Some(window)
      }
      None => None,
    };

    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::WrongType => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：读变体空数组；存储变体删除目标键后回 :0（C# EXPIRE(destination, 0)）。
        // 回收臂复用装载前预取的目标键窗并按开窗时刻存活域复验（同键同单窗禁
        // 双取，try_rmw_window 不可重入）：分层态目标键 begin 即拒写转 Ok(false)
        // 慢路径，树残留交 store_dest_cold → retire_tiered_dest 单点清退（票
        // zcode-r18-geo 发现一），同步臂禁裸树清退
        match &store_dest {
          Some(dest) => {
            // 存储变体取窗失败已在装载前早退，此组合不可达（兜底转异步重放）
            let Some(window) = window else {
              return Ok(false);
            };
            if !window.recheck() {
              return Ok(false);
            }
            // 空集合落笔即删空回收（clear_ttl 于空对象为 no-op，同旧语义无尾笔）
            if let Some(ret) =
              writeback_sync(store, dest, &SortedSetObject::new(), true, output).terminal()
            {
              return Ok(ret);
            }
            output.write_resp_int(0);
          }
          None => output.write_resp_array_len(0),
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    match &store_dest {
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
        obj.geo_search(
          &mut opts,
          &mut ObjectOutput::mount(&mut sink),
          self.resp_protocol_version,
          false,
        );
        if sink.first() == Some(&b'-') {
          // FROMMEMBER 圆心缺失等对象层错误透传
          output.append(&mut sink);
          return Ok(true);
        }
        let dst = SortedSetObject::from_entries(parse_pairs_payload(&sink));
        let count = dst.sorted_set_dict.len();
        // STORE 覆写族目标键双保护·同步档：装载前预取的 dest rmw 窗跨「信封写回
        // → TTL 清退」全程（装载态取开窗时刻存活域），落笔前复验域归属拒盲写
        let Some(window) = window else {
          return Ok(false);
        };
        // STORE 族目标键为 SET 语义（清既有 key 级 TTL）：对标 C# GeoSearchStore
        // 的 Delete(dst)+ZADD 融合收尾（SortedSetGeoOps.cs:178-181，错误臂先于
        // Delete 返回、dest 全程排他锁），信封域 upsert 默认保留 TTL，故写回成功
        // 后随写显式清退（票 zcode-r122c-setstore1 序纪律：写回先行、清退随后，
        // 失败臂零清退即原态；空结果 TTL 已随删空臂级联清退，writeback_sync 内聚
        // 免尾笔）
        if !window.recheck() {
          return Ok(false);
        }
        if let Some(ret) = writeback_sync(store, dest, &dst, true, output).terminal() {
          return Ok(ret);
        }
        output.write_resp_int(count as i64);
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
    ObjectOutput,
    zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
  };
  use wdev::Device;
  use wkv::SwapInWindowGuard;
  use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};
  use wval::GarnetObjectType;

  use super::{
    GeoSearchCommandKind, parse_pairs_payload, scan_geo_add_options, try_get_geo_search_options,
  };
  use crate::{
    resp::objects::{
      object_store_utils::{
        SealedLoad, load_typed_sealed, obj_writeback_recheck_async, obj_writeback_tiered,
      },
      sorted_set_commands::{should_write_back, slow::store_dest_cold},
    },
    storage::session::storage_session::StorageSession,
  };

  /// 装载结果：对象 + 封窗守卫（物化自分层树时为 Some，须存活至写回收尾；
  /// 信封域装载为 None，写回面按守卫在持与否分派升阶/降阶判据）
  type LoadedGeo = Option<Option<(SortedSetObject, Option<SwapInWindowGuard>)>>;

  /// 单键异步装载（升阶键物化降级）：出参口径见 [`LoadedGeo`]；物化降级
  /// 装配单点转引 [`load_typed_sealed`]
  async fn load_typed(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    output: &mut Vec<u8>,
  ) -> Result<LoadedGeo, ()> {
    Ok(
      match load_typed_sealed(storage, key, GarnetObjectType::SortedSet, output).await? {
        SealedLoad::WrongType => None,
        SealedLoad::Missing => Some(None),
        SealedLoad::Present(o, window) => Some(Some((o, window))),
      },
    )
  }

  /// GEO 慢路径写回收尾：复用分层感知统一漏斗（删空自愈 / 超阈值重灌 /
  /// 懒降阶信封 + 树清退 / 常规信封写回）；非封窗（信封域）落笔前先按装载态
  /// 复验域归属（票 load-type-rmw-window 异步档），窗内 DEL/SET 交叠即
  /// `Err(())` 按存储忙拒写；封窗臂（SwapInWindowGuard）物化域已钉死，免复验
  async fn geo_save_back(
    storage: &StorageSession<'_, impl Device>,
    key: &[u8],
    obj: &SortedSetObject,
    sealed: bool,
    existed: bool,
  ) -> Result<(), ()> {
    if !sealed {
      obj_writeback_recheck_async(storage, key, existed).await?;
    }
    // sealed = 物化自分层树（封窗守卫在持）：键必为树态且门禁装载会被自身
    // claim 拒，跳过探测直接按分层收尾
    obj_writeback_tiered(storage, key, GarnetObjectType::SortedSet, obj, sealed).await
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
    storage: &StorageSession<'_, impl Device>,
    cmd: RespCommand,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let resp_version = storage.resp_version;
    let key = refs.first().copied().unwrap_or(&[]);
    let args = refs.get(1..).unwrap_or(&[]);

    // ---- GEOADD：选项词元扫描 + 三元组起始位对位同步段
    if cmd == RespCommand::Geoadd {
      let (add_option, member_start) = scan_geo_add_options(refs);
      let members = refs.get(member_start..).unwrap_or(&[]);

      // 装载型写臂双保护·异步档：装载前取 rmw 窗跨全程（Missing 新建创建域
      // 亦在窗内钉住），落笔复验内聚于 geo_save_back
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let Some(loaded) = load_typed(storage, key, output).await? else {
        return Ok(());
      };
      // Missing → 新建对象（existed = false，同步段 Missing 分支对位）
      let (mut obj, existed, swap_in_window) = match loaded {
        Some((o, w)) => (o, true, w),
        None => (SortedSetObject::new(), false, None),
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
      // 回写门并入 zset 族单源 should_write_back（同步段对位，等价性见其注记：
      // Geoadd 恒出 :N 帧 written 恒真）；写回失败回退挂载点再落错（慢路径统一
      // 应答前清场）
      if should_write_back(SortedSetOperation::Geoadd, &obj_out, &obj, existed)
        && geo_save_back(storage, key, &obj, swap_in_window.is_some(), existed)
          .await
          .is_err()
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
        // 纯读面（GEOHASH/GEODIST/GEOPOS）：零写回，封窗守卫解构即释
        Some((o, _w)) => o,
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
    // 目标键 rmw 窗 / 域快照 / 落笔复验 / 信封写回·随写清 TTL·删空回收 / 树态
    // 残留清退全链收口于 store_dest_cold 单点（与 ZRANGESTORE 慢路径同构，票
    // zcode-r18-geo 发现一；窗序一处定义见 store_dest_cold_common 头注——TTL 动作
    // 只落复验通过后的持窗写临界区，错误臂天然零清退）
    // 装载型写臂先窗后装·异步档（§87 取窗点前移，与同步臂同纪律）：存储变体
    // 于源装载之前预取 dest rmw 窗跨「源装载 → 搜索求值 → 落笔」全程，消除
    // 自指形（dest∈src）窗外装载求值间隙对面已确认写被陈旧快照整写；窗句柄
    // 传入收尾单点复用（同键同单窗禁双取）；取窗失败 Err(()) fail-closed
    // 拒写通道不变；读变体零改动无窗
    let store_window = match &store_dest {
      Some(dest) => Some(storage.batch.rmw_window(dest).await.map_err(|_| ())?),
      None => None,
    };
    let Some(loaded) = load_typed(storage, src_key, output).await? else {
      return Ok(());
    };
    let mut obj = match loaded {
      // 源缺失：读变体空数组；存储变体经 store_dest_cold 空结果臂删空回收
      // （含树态目标 retire_tiered_dest 两域齐清）后回 :0（C# EXPIRE(destination, 0)），
      // 复用装载前预取窗
      None => {
        if let Some(dest) = &store_dest {
          // 存储变体取窗失败已前置早退，此组合不可达（fail-closed 兜底）
          let Some(window) = store_window else {
            return Err(());
          };
          store_dest_cold(storage, dest, &SortedSetObject::new(), window).await?;
          output.write_resp_int(0);
        } else {
          output.write_resp_array_len(0);
        }
        return Ok(());
      }
      // 源键只读（结果仅落 dest），封窗守卫解构即释
      Some((o, _w)) => o,
    };

    match &store_dest {
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
        obj.geo_search(
          &mut opts,
          &mut ObjectOutput::mount(&mut sink),
          resp_version,
          false,
        );
        if sink.first() == Some(&b'-') {
          // FROMMEMBER 圆心缺失等对象层错误透传
          output.append(&mut sink);
          return Ok(());
        }
        let dst = SortedSetObject::from_entries(parse_pairs_payload(&sink));
        // 收尾并回 zset STORE 族单点 store_dest_cold（票 zcode-r18-geo 发现一 /
        // zcode-r32-retirematrix）：装载前预取的 rmw 窗承接传入，跨「域快照 → 落笔
        // 复验 → 旧 String 域清退 → 信封写回·随写清 TTL/删空回收」→ 窗释放后
        // retire_tiered_dest 按结果分流清退树态残留，杜绝 Meta 存根 + 旧树遮蔽或
        // String 残域双域并存（修复前信封盲写被旧树遮蔽或与旧字符串并存）
        let Some(window) = store_window else {
          return Err(());
        };
        let count = store_dest_cold(storage, dest, &dst, window).await?;
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
