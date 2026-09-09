//! GEO RESP 命令（对标 libs/server/Resp/Objects/SortedSetGeoCommands.cs）
//!
//! GEOADD / GEOHASH / GEODIST / GEOPOS / GEOSEARCH / GEOSEARCHSTORE /
//! GEORADIUS(_RO) / GEORADIUSBYMEMBER(_RO)。语义经对象层
//! [`SortedSetObject`] 的 geo_* 分片执行；选项解析对标
//! SessionParseStateExtensions.TryGetGeoSearchOptions 的命令分派文法
//! （GEOSEARCH 族走 FROMMEMBER/FROMLONLAT + BYRADIUS/BYBOX 关键字，
//! GEORADIUS 族为位置参数）；存取经与 storage 会话域共享的信封编解码。

use crate::{
  objects::{
    parse_utils::{equals_ignore_case, try_get_geo_distance_unit, try_get_geo_lon_lat},
    sortedset::sorted_set_object::{SortedSetEntry, SortedSetObject, SortedSetOperation},
    sortedsetgeo::{
      geo_hash::GeoDistanceUnitType,
      sorted_set_geo_object_impl::{
        GeoAddOptions, GeoOrder, GeoOriginType, GeoSearchOptions, GeoSearchType,
      },
    },
    types::object_output::ObjectOutput,
  },
  resp::{
    objects::sorted_set_commands::{
      ZsetLoad, make_input_for_geo, parse_pairs_payload, zset_load_sync, zset_save_or_gc,
    },
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
};

/// GEOSEARCH 族命令形态（决定选项文法与存储语义）
///
/// 对标 RespCommand.GEOSEARCH/GEOSEARCHSTORE/GEORADIUS(_RO)/GEORADIUSBYMEMBER(_RO)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoSearchCommandKind {
  /// GEOSEARCH key [FROMMEMBER m|FROMLONLAT lon lat] BYRADIUS r u|BYBOX w h u [修饰词]
  GeoSearch,
  /// GEOSEARCHSTORE dest src …（同 GEOSEARCH 文法，结果落目标键）
  GeoSearchStore,
  /// GEORADIUS key lon lat radius unit [修饰词] [STORE dest|STOREDIST dest]
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

// ---- CmdStrings 中 GEO 族错误串（对标 libs/server/Resp/CmdStrings.cs） ----

const RESP_ERR_NOT_VALID_RADIUS: &[u8] = b"ERR need numeric radius";
const RESP_ERR_RADIUS_IS_NEGATIVE: &[u8] = b"ERR radius cannot be negative";
const RESP_ERR_NOT_VALID_WIDTH: &[u8] = b"ERR need numeric width";
const RESP_ERR_NOT_VALID_HEIGHT: &[u8] = b"ERR need numeric height";
const RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE: &[u8] = b"ERR height or width cannot be negative";
const RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT: &[u8] =
  b"ERR unsupported unit provided. please use M, KM, FT, MI";
const RESP_ERR_COUNT_IS_NOT_POSITIVE: &[u8] = b"ERR COUNT must be > 0";
const RESP_ERR_INVALID_LON_LAT: &[u8] = b"ERR invalid longitude,latitude pair";

/// 解析双精度（TryGetDouble 语义）
fn parse_double(token: &[u8]) -> Option<f64> {
  str::from_utf8(token).ok()?.parse::<f64>().ok()
}

/// GEOSEARCH 族选项解析（对标 SessionParseStateExtensions.TryGetGeoSearchOptions）
///
/// args 为源键之后的参数序列
fn try_get_geo_search_options(
  args: &[&[u8]],
  kind: GeoSearchCommandKind,
) -> Result<ParsedGeoSearch, &'static [u8]> {
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
      let Some((lon, lat)) = try_get_geo_lon_lat(lon_tok, lat_tok) else {
        return Err(RESP_ERR_INVALID_LON_LAT);
      };
      opts.lon = lon;
      opts.lat = lat;
      opts.origin = GeoOriginType::FromLonLat;
      idx = 2;
    }

    let Some(radius_tok) = args.get(idx).copied() else {
      return Err(wrong_args(kind));
    };
    let Some(radius) = parse_double(radius_tok) else {
      return Err(RESP_ERR_NOT_VALID_RADIUS);
    };
    if radius < 0.0 {
      return Err(RESP_ERR_RADIUS_IS_NEGATIVE);
    }
    opts.radius = radius;
    opts.search_type = GeoSearchType::ByRadius;
    idx += 1;

    let Some(unit_tok) = args.get(idx).copied() else {
      return Err(wrong_args(kind));
    };
    let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
      return Err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT);
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
          return Err(b"ERR syntax error");
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
          return Err(b"ERR syntax error");
        }
        let (Some(lon_tok), Some(lat_tok)) = (args.get(idx).copied(), args.get(idx + 1).copied())
        else {
          arg_num_error = true;
          break;
        };
        let Some((lon, lat)) = try_get_geo_lon_lat(lon_tok, lat_tok) else {
          return Err(RESP_ERR_INVALID_LON_LAT);
        };
        opts.lon = lon;
        opts.lat = lat;
        opts.origin = GeoOriginType::FromLonLat;
        idx += 2;
        continue;
      }

      if equals_ignore_case(token, b"BYRADIUS") {
        if opts.search_type != GeoSearchType::Undefined {
          return Err(b"ERR syntax error");
        }
        let (Some(radius_tok), Some(unit_tok)) =
          (args.get(idx).copied(), args.get(idx + 1).copied())
        else {
          arg_num_error = true;
          break;
        };
        let Some(radius) = parse_double(radius_tok) else {
          return Err(RESP_ERR_NOT_VALID_RADIUS);
        };
        if radius < 0.0 {
          return Err(RESP_ERR_RADIUS_IS_NEGATIVE);
        }
        let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
          return Err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT);
        };
        opts.radius = radius;
        opts.search_type = GeoSearchType::ByRadius;
        opts.unit = unit;
        idx += 2;
        continue;
      }

      if equals_ignore_case(token, b"BYBOX") {
        if opts.search_type != GeoSearchType::Undefined {
          return Err(b"ERR syntax error");
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
          return Err(RESP_ERR_NOT_VALID_WIDTH);
        };
        let Some(height) = parse_double(height_tok) else {
          return Err(RESP_ERR_NOT_VALID_HEIGHT);
        };
        if width < 0.0 || height < 0.0 {
          return Err(RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE);
        }
        let Some(unit) = try_get_geo_distance_unit(unit_tok) else {
          return Err(RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT);
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

    if equals_ignore_case(token, b"COUNT") {
      let Some(count_tok) = args.get(idx) else {
        arg_num_error = true;
        break;
      };
      let Some(v) = count_tok.try_parse_i64() else {
        return Err(b"ERR value is not an integer or out of range");
      };
      if v <= 0 {
        return Err(RESP_ERR_COUNT_IS_NOT_POSITIVE);
      }
      opts.count_value = v;
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

    return Err(b"ERR syntax error");
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

/// wrong number of arguments 错误帧（按命令名展开）
fn wrong_args(kind: GeoSearchCommandKind) -> &'static [u8] {
  match kind {
    GeoSearchCommandKind::GeoSearch => b"ERR wrong number of arguments for 'GEOSEARCH' command",
    GeoSearchCommandKind::GeoSearchStore => {
      b"ERR wrong number of arguments for 'GEOSEARCHSTORE' command"
    }
    GeoSearchCommandKind::GeoRadius => b"ERR wrong number of arguments for 'GEORADIUS' command",
    GeoSearchCommandKind::GeoRadiusRo => {
      b"ERR wrong number of arguments for 'GEORADIUS_RO' command"
    }
    GeoSearchCommandKind::GeoRadiusByMember => {
      b"ERR wrong number of arguments for 'GEORADIUSBYMEMBER' command"
    }
    GeoSearchCommandKind::GeoRadiusByMemberRo => {
      b"ERR wrong number of arguments for 'GEORADIUSBYMEMBER_RO' command"
    }
  }
}

/// STORE 与 WITH* 互斥错误帧（对标 CmdStrings.GenericErrStoreCommand）
fn store_incompat(kind: GeoSearchCommandKind) -> &'static [u8] {
  match kind {
    GeoSearchCommandKind::GeoSearchStore => b"ERR STORE option in GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
    GeoSearchCommandKind::GeoRadius => b"ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
    GeoSearchCommandKind::GeoRadiusByMember => b"ERR STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
    _ => b"ERR syntax error",
  }
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
    if parse_state.len() < 4 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'GEOADD' command\r\n");
      return Ok(true);
    }

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
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    }

    // 成员三元组校验
    let member_start = curr_token_idx;
    let mut idx = curr_token_idx;
    while idx < parse_state.len() {
      if idx > parse_state.len() - 3 {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return Ok(true);
      }
      if try_get_geo_lon_lat(parse_state[idx], parse_state[idx + 1]).is_none() {
        output.extend_from_slice(b"-ERR invalid longitude,latitude pair\r\n");
        return Ok(true);
      }
      idx += 3;
    }

    let (mut obj, existed) = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => (SortedSetObject::new(), false),
      ZsetLoad::Present(o) => (o, true),
    };

    let (input, _backing) = make_input_for_geo(
      SortedSetOperation::Geoadd,
      &parse_state[member_start..],
      add_option.bits() as i32,
      0,
    );
    let mut obj_out = ObjectOutput::new();
    obj.operate(&input, &mut obj_out, 2);

    // 回写：错误回复不落库；缺失键上仍空则不创建（三元组全被拒等场景）
    if obj_out.payload.first() != Some(&b'-')
      && !obj_out.has_wrong_type()
      && (existed || !obj.sorted_set_dict.is_empty())
    {
      match zset_save_or_gc(store, key, &obj) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }
    output.extend_from_slice(&obj_out.payload);
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
    let name = match op {
      SortedSetOperation::Geodist => {
        // GEODIST key m1 m2 [unit]：单位词元合法性前置校验
        if parse_state.len() == 4 && try_get_geo_distance_unit(parse_state[3]).is_none() {
          output.extend_from_slice(b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n");
          return Ok(true);
        }
        "GEODIST"
      }
      SortedSetOperation::Geohash => "GEOHASH",
      _ => "GEOPOS",
    };
    if parse_state.is_empty() {
      output.extend_from_slice(
        format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes(),
      );
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match zset_load_sync(store, key, output) {
      ZsetLoad::Degrade => return Ok(false),
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 键缺失：GEODIST → null；GEOHASH/GEOPOS → 每成员 null 数组项
        if op == SortedSetOperation::Geodist {
          output.write_resp_null();
        } else {
          output.write_resp_array_len(parse_state.len() - 1);
          for _ in 1..parse_state.len() {
            output.extend_from_slice(b"*-1\r\n");
          }
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    let (input, _backing) = make_input_for_geo(op, &parse_state[1..], 0, 0);
    let mut obj_out = ObjectOutput::new();
    obj.operate(&input, &mut obj_out, 2);
    output.extend_from_slice(&obj_out.payload);
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
    if parse_state.len() < kind.params_required() {
      output.extend_from_slice(
        format!(
          "-ERR wrong number of arguments for '{}' command\r\n",
          kind.name()
        )
        .as_bytes(),
      );
      return Ok(true);
    }

    // GEOSEARCHSTORE：首参为目标键，源为第二参
    let source_idx = usize::from(kind == GeoSearchCommandKind::GeoSearchStore);
    let key = parse_state[source_idx];
    let args = &parse_state[source_idx + 1..];

    let parsed = match try_get_geo_search_options(args, kind) {
      Ok(p) => p,
      // 具体错误文本（参数个数/单位/半径/COUNT/STORE 互斥等）逐字透传
      Err(e) => {
        output.push(b'-');
        output.extend_from_slice(e);
        output.extend_from_slice(b"\r\n");
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
      ZsetLoad::Error => return Ok(true),
      ZsetLoad::Missing => {
        // 源缺失：读变体空数组；存储变体删除目标键后回 :0（C# EXPIRE(destination, 0)）
        match &store_dest {
          Some(dest) => match zset_save_or_gc(store, dest, &SortedSetObject::new()) {
            Ok(true) => output.extend_from_slice(b":0\r\n"),
            Ok(false) => return Ok(false),
            Err(()) => output.write_resp_error("generic error"),
          },
          None => output.extend_from_slice(b"*0\r\n"),
        }
        return Ok(true);
      }
      ZsetLoad::Present(o) => o,
    };

    match store_dest {
      None => {
        let mut obj_out = ObjectOutput::new();
        obj.geo_search(&mut opts, &mut obj_out, 2, true);
        output.extend_from_slice(&obj_out.payload);
      }
      Some(dest) => {
        // 存储变体（解析层已强制 withHash 或 withDist）：分值取 GeoHash 或距离，
        // 命中成员成对落目标集合（对标 C# GeoSearchStore 的 ZADD 收尾）
        let mut obj_out = ObjectOutput::new();
        obj.geo_search(&mut opts, &mut obj_out, 2, false);
        if obj_out.payload.first() == Some(&b'-') {
          // FROMMEMBER 圆心缺失等对象层错误透传
          output.extend_from_slice(&obj_out.payload);
          return Ok(true);
        }
        let mut dst = SortedSetObject::new();
        for (member, score) in parse_pairs_payload(&obj_out.payload) {
          dst.sorted_set_dict.insert(member.clone(), score);
          dst.sorted_set.insert(SortedSetEntry { score, member });
        }
        let count = dst.sorted_set_dict.len();
        match zset_save_or_gc(store, &dest, &dst) {
          Ok(true) => output.write_resp_int(count as i64),
          Ok(false) => return Ok(false),
          Err(()) => output.write_resp_error("generic error"),
        }
      }
    }
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  /// 读回键的原始信封载荷
  fn raw_value(batch: &wkv::BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> Option<Vec<u8>> {
    batch
      .try_read_sync(key, |v| v.to_vec())
      .ok()
      .flatten()
      .flatten()
  }

  #[test]
  fn geoadd_geohash_geopos_geodist_flow() {
    let (_dir, _store, session) = fixture("geo.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    // GEOADD
    sess
      .geo_add(
        &[
          b"cities",
          b"-122.4194",
          b"37.7749",
          b"sf",
          b"2.3522",
          b"48.8566",
          b"paris",
        ],
        &batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // GEOADD 重复（NX 下不变）
    out.clear();
    sess
      .geo_add(&[b"cities", b"NX", b"0.0", b"0.0", b"sf"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // GEOADD NX 新成员照常新增（NX 只挡更新，对齐 C#/Redis）
    out.clear();
    sess
      .geo_add(
        &[b"cities", b"NX", b"0.0", b"0.0", b"nyc"],
        &batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // GEOADD XX 新成员不落地
    out.clear();
    sess
      .geo_add(&[b"cities", b"XX", b"0.0", b"0.0", b"la"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    out.clear();
    sess
      .geo_commands(
        &[b"cities", b"la"],
        &batch,
        &mut out,
        SortedSetOperation::Geopos,
      )
      .unwrap();
    assert!(
      out.windows(5).any(|w| w == b"*-1\r\n") || out.windows(4).any(|w| w == b"$-1\r\n"),
      "{out:?}"
    );

    // GEOHASH
    out.clear();
    sess
      .geo_commands(
        &[b"cities", b"sf", b"missing"],
        &batch,
        &mut out,
        SortedSetOperation::Geohash,
      )
      .unwrap();
    assert_eq!(out, b"*2\r\n$11\r\n9q8yyk8ytp0\r\n$-1\r\n");

    // GEOPOS
    out.clear();
    sess
      .geo_commands(
        &[b"cities", b"sf"],
        &batch,
        &mut out,
        SortedSetOperation::Geopos,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("-122.4"), "{payload}");

    // GEODIST km ≈ 8967
    out.clear();
    sess
      .geo_commands(
        &[b"cities", b"sf", b"paris", b"km"],
        &batch,
        &mut out,
        SortedSetOperation::Geodist,
      )
      .unwrap();
    let dist: f64 = String::from_utf8_lossy(&out)
      .lines()
      .nth(1)
      .unwrap()
      .parse()
      .unwrap();
    assert!((dist - 8967.0).abs() < 30.0);

    // WRONGTYPE：非 zset 信封键上 GEOADD 拒绝且不覆盖
    let _ = batch.try_upsert_sync(b"str", b"plain-string-value");
    out.clear();
    sess
      .geo_add(&[b"str", b"0.0", b"0.0", b"m"], &batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"), "{out:?}");
    assert_eq!(
      batch
        .try_read_sync(b"str", |v| v.to_vec())
        .ok()
        .flatten()
        .flatten(),
      Some(b"plain-string-value".to_vec())
    );
  }

  #[test]
  fn geosearch_keyword_grammar() {
    let (_dir, _store, session) = fixture("gsearch.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .geo_add(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"sf",
          b"-118.2437",
          b"34.0522",
          b"la",
        ],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    // GEOSEARCH pts FROMLONLAT lon lat BYRADIUS 100 km WITHDIST（标准文法）
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"FROMLONLAT",
          b"-122.4194",
          b"37.7749",
          b"BYRADIUS",
          b"100",
          b"km",
          b"WITHDIST",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("sf"), "{payload}");
    assert!(!payload.contains("la"), "{payload}");

    // GEOSEARCH FROMMEMBER 关键字
    out.clear();
    sess
      .geo_search_commands(
        &[b"pts", b"FROMMEMBER", b"la", b"BYRADIUS", b"600", b"km"],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("la"), "{payload}");
    assert!(payload.contains("sf"), "{payload}");

    // GEOSEARCH BYBOX 关键字（300km × 300km 覆盖 sf，不含 la）
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"FROMLONLAT",
          b"-122.4194",
          b"37.7749",
          b"BYBOX",
          b"300",
          b"300",
          b"km",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("sf"), "{payload}");
    assert!(!payload.contains("la"), "{payload}");

    // 缺形状关键字 → 参数个数错误
    out.clear();
    sess
      .geo_search_commands(
        &[b"pts", b"FROMLONLAT", b"-122.4194", b"37.7749"],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    assert!(
      out.starts_with(b"-ERR wrong number of arguments for 'GEOSEARCH'"),
      "{out:?}"
    );

    // 圆心重复 → 语法错误
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"FROMMEMBER",
          b"la",
          b"FROMLONLAT",
          b"1",
          b"2",
          b"BYRADIUS",
          b"10",
          b"km",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    assert!(out.starts_with(b"-ERR syntax error"), "{out:?}");

    // COUNT 非正数 → 专用错误
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"FROMMEMBER",
          b"la",
          b"BYRADIUS",
          b"600",
          b"km",
          b"COUNT",
          b"0",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearch,
      )
      .unwrap();
    assert!(out.starts_with(b"-ERR COUNT must be > 0"), "{out:?}");
  }

  #[test]
  fn georadius_positional_and_store() {
    let (_dir, _store, session) = fixture("gradius.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .geo_add(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"sf",
          b"-118.2437",
          b"34.0522",
          b"la",
        ],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    // GEORADIUS 位置文法 + STOREDIST：目标以"距离"为分值
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"400",
          b"km",
          b"STOREDIST",
          b"near",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoRadius,
      )
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // near 中仅 sf，分值为请求单位下的距离（km → 数值 < 400）
    out.clear();
    sess
      .sorted_set_score(&[b"near", b"sf"], &batch, &mut out)
      .unwrap();
    let score: f64 = String::from_utf8_lossy(&out)
      .lines()
      .nth(1)
      .unwrap()
      .parse()
      .unwrap();
    assert!((0.0..400.0).contains(&score), "{score}");

    // STORE：分值为 52 位 GeoHash 整数
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"400",
          b"km",
          b"STORE",
          b"nearhash",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoRadius,
      )
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    sess
      .sorted_set_score(&[b"nearhash", b"sf"], &batch, &mut out)
      .unwrap();
    let hash_score: f64 = String::from_utf8_lossy(&out)
      .lines()
      .nth(1)
      .unwrap()
      .parse()
      .unwrap();
    assert!(
      hash_score > 0.0 && (hash_score as u64) < (1_u64 << 52),
      "{hash_score}"
    );

    // STORE + WITHDIST 互斥
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"400",
          b"km",
          b"STORE",
          b"x",
          b"WITHDIST",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoRadius,
      )
      .unwrap();
    assert!(
      out.starts_with(b"-ERR STORE option in GEORADIUS is not compatible"),
      "{out:?}"
    );

    // GEORADIUSBYMEMBER 位置文法
    out.clear();
    sess
      .geo_search_commands(
        &[b"pts", b"la", b"600", b"km"],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoRadiusByMember,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("sf"), "{payload}");
  }

  #[test]
  fn geosearchstore_dest_is_first_arg() {
    let (_dir, _store, session) = fixture("gstore.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    sess
      .geo_add(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"sf",
          b"2.3522",
          b"48.8566",
          b"paris",
        ],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();

    // 目标键必须是 parse_state[0]（dest），源为第二参；此前误用源键会覆写 pts
    sess
      .geo_search_commands(
        &[
          b"store",
          b"pts",
          b"FROMLONLAT",
          b"-122.4194",
          b"37.7749",
          b"BYRADIUS",
          b"100",
          b"km",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearchStore,
      )
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // store 命中 sf（GeoHash 分值），pts 原样保留两成员
    out.clear();
    sess
      .geo_commands(
        &[b"store", b"sf"],
        &batch,
        &mut out,
        SortedSetOperation::Geohash,
      )
      .unwrap();
    assert!(out.starts_with(b"*1\r\n$11\r\n9q8yy"), "{out:?}");
    out.clear();
    sess
      .geo_commands(
        &[b"pts", b"sf", b"paris"],
        &batch,
        &mut out,
        SortedSetOperation::Geohash,
      )
      .unwrap();
    assert!(out.starts_with(b"*2\r\n"), "{out:?}");

    // 源缺失：目标删除 + :0
    let _ = batch.try_upsert_sync(b"gone", b"x");
    out.clear();
    sess
      .geo_search_commands(
        &[
          b"gone",
          b"no-such",
          b"FROMLONLAT",
          b"0",
          b"0",
          b"BYRADIUS",
          b"10",
          b"km",
        ],
        &batch,
        &mut out,
        GeoSearchCommandKind::GeoSearchStore,
      )
      .unwrap();
    assert_eq!(out, b":0\r\n");
    assert!(raw_value(&batch, b"gone").is_none());
  }
}
