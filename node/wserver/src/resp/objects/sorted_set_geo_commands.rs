//! GEO RESP 命令（对标 libs/server/Resp/Objects/SortedSetGeoCommands.cs）
//!
//! GEOADD / GEOHASH / GEODIST / GEOPOS / GEOSEARCH（含 GEOSEARCHSTORE 与
//! GEORADIUS/GEORADIUSBYMEMBER 族）。语义经对象层 [`SortedSetObject`] 的
//! geo_* 分片执行；GEOSEARCH 选项解析对标 TryGetGeoSearchOptions。

use std::io::Cursor;

use wobject::sorted_set::sorted_set_object::SortedSetObject as WoSortedSetObject;

use crate::{
  objects::{
    parse_utils::{equals_ignore_case, try_get_geo_distance_unit, try_get_geo_lon_lat},
    sortedset::sorted_set_object::SortedSetObject,
    sortedsetgeo::{
      geo_hash::GeoDistanceUnitType,
      sorted_set_geo_object_impl::{
        GeoAddOptions, GeoOrder, GeoOriginType, GeoSearchOptions, GeoSearchType,
      },
    },
    types::object_output::ObjectOutput,
  },
  resp::{
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
};

/// 载荷互转（与 sorted_set_commands 同一 bitcode 兼容路径）
fn zset_from_blob(raw: &[u8]) -> SortedSetObject {
  match WoSortedSetObject::deserialize(&mut Cursor::new(raw)) {
    Ok(wo) => {
      let pin = wo.dict.pin();
      let entries: Vec<(Vec<u8>, f64)> = pin.iter().map(|(k, v)| (k.clone(), *v)).collect();
      SortedSetObject::from_entries(entries)
    }
    Err(_) => SortedSetObject::new(),
  }
}

fn zset_to_blob(obj: &SortedSetObject) -> Vec<u8> {
  let wo = WoSortedSetObject::new();
  {
    let pin = wo.dict.pin();
    let mut tree = wo.tree.lock();
    for (member, score) in obj.to_entries() {
      pin.insert(member.clone(), score);
      tree.insert(wobject::sorted_set::sorted_set_object::SortedSetEntry { score, member });
    }
  }
  let mut out = Vec::new();
  let _ = wo.serialize(&mut out);
  out
}

/// GEOSEARCH 选项束解析结果
#[derive(Debug, Default)]
struct ParsedGeoSearch {
  opts: GeoSearchOptions,
  /// GEOSEARCHSTORE 的目标键下标（无存储则 None）
  dest_idx: Option<usize>,
  /// 解析终止位置（经 FromLonLat 消耗 lon lat 两参）
  end_idx: usize,
}

/// GEOSEARCH 族选项解析（对标 SessionParseStateExtensions.TryGetGeoSearchOptions）
///
/// args 为 key 之后的参数序列（GEOSEARCHSTORE 已剥掉 dst，首参为 src）
fn try_get_geo_search_options(
  args: &[&[u8]],
  by_member: bool,
  by_box: bool,
) -> Result<ParsedGeoSearch, &'static [u8]> {
  let mut parsed = ParsedGeoSearch {
    opts: GeoSearchOptions {
      search_type: if by_box {
        GeoSearchType::ByBox
      } else {
        GeoSearchType::ByRadius
      },
      unit: GeoDistanceUnitType::M,
      ..Default::default()
    },
    dest_idx: None,
    end_idx: 0,
  };

  let Some(unit_token) = args.first().copied() else {
    return Err(b"ERR syntax error");
  };

  if by_member {
    // FROMMEMBER member
    parsed.opts.origin = GeoOriginType::FromMember;
    parsed.opts.from_member = unit_token.to_vec();
    parsed.end_idx = 1;
  } else {
    // FROMLONLON longitude latitude
    let Some(latitude) = args.get(1).copied() else {
      return Err(b"ERR syntax error");
    };
    let Some((lon, lat)) = try_get_geo_lon_lat(unit_token, latitude) else {
      return Err(b"ERR invalid longitude,latitude pair");
    };
    parsed.opts.origin = GeoOriginType::FromLonLat;
    parsed.opts.lon = lon;
    parsed.opts.lat = lat;
    parsed.end_idx = 2;
  }

  // 形状关键词可选（GEOSEARCH 显式 BYRADIUS/BYBOX；RADIUS 族省略）
  if matches!(args.get(parsed.end_idx), Some(t) if equals_ignore_case(t, b"BYRADIUS") || equals_ignore_case(t, b"BYBOX"))
  {
    if equals_ignore_case(args[parsed.end_idx], b"BYBOX") {
      parsed.opts.search_type = GeoSearchType::ByBox;
    }
    parsed.end_idx += 1;
  }

  // 半径/宽高 + 单位
  let Some(shape_str) = args.get(parsed.end_idx).copied() else {
    return Err(b"ERR syntax error");
  };
  let Some(shape) = str::from_utf8(shape_str).unwrap_or("").parse::<f64>().ok() else {
    return Err(b"ERR value is not a valid float");
  };
  let Some(unit_token) = args.get(parsed.end_idx + 1).copied() else {
    return Err(b"ERR syntax error");
  };
  let Some(unit) = try_get_geo_distance_unit(unit_token) else {
    return Err(b"ERR unsupported unit provided. please use m, km, ft, mi");
  };
  if by_box {
    parsed.opts.box_width = shape;
    // BYBOX 需要宽高两值：宽=shape，高=下一数值（C# 中 box_height 复用 radius 槽）
    let Some(height_str) = args.get(parsed.end_idx + 2).copied() else {
      return Err(b"ERR syntax error");
    };
    let Some(height) = str::from_utf8(height_str).unwrap_or("").parse::<f64>().ok() else {
      return Err(b"ERR value is not a valid float");
    };
    parsed.opts.radius = height;
    parsed.end_idx += 3;
  } else {
    parsed.opts.radius = shape;
    parsed.end_idx += 2;
  }
  parsed.opts.unit = unit;

  // 其余修饰词
  let mut count = -1_i64;
  while parsed.end_idx < args.len() {
    let token = args[parsed.end_idx];
    if equals_ignore_case(token, b"ASC") {
      parsed.opts.sort = GeoOrder::Ascending;
      parsed.end_idx += 1;
    } else if equals_ignore_case(token, b"DESC") {
      parsed.opts.sort = GeoOrder::Descending;
      parsed.end_idx += 1;
    } else if equals_ignore_case(token, b"WITHCOORD") {
      parsed.opts.with_coord = true;
      parsed.end_idx += 1;
    } else if equals_ignore_case(token, b"WITHDIST") {
      parsed.opts.with_dist = true;
      parsed.end_idx += 1;
    } else if equals_ignore_case(token, b"WITHHASH") {
      parsed.opts.with_hash = true;
      parsed.end_idx += 1;
    } else if equals_ignore_case(token, b"COUNT") {
      let Some(v) = args.get(parsed.end_idx + 1).and_then(|t| t.try_parse_i64()) else {
        return Err(b"ERR syntax error");
      };
      count = v;
      parsed.end_idx += 2;
      // COUNT 后可选 ANY
      if args
        .get(parsed.end_idx)
        .is_some_and(|t| equals_ignore_case(t, b"ANY"))
      {
        parsed.opts.with_count_any = true;
        parsed.end_idx += 1;
      }
    } else if equals_ignore_case(token, b"STORE") || equals_ignore_case(token, b"STOREDIST") {
      let Some(dest) = args.get(parsed.end_idx + 1) else {
        return Err(b"ERR syntax error");
      };
      parsed.dest_idx = Some(args.len());
      // 目标键由调用方以独立参数传入，此处仅记录标记并跳过词元
      let _ = dest;
      parsed.end_idx += 2;
    } else {
      return Err(b"ERR syntax error");
    }
  }

  parsed.opts.count_value = count;
  Ok(parsed)
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

    let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => SortedSetObject::new(),
    };

    let mut obj_out = ObjectOutput::new();
    let (input, _backing) = crate::resp::objects::sorted_set_commands::make_input_for_geo(
      crate::objects::sortedset::sorted_set_object::SortedSetOperation::Geoadd,
      &parse_state[member_start..],
      add_option.bits() as i32,
      0,
    );
    obj.operate(&input, &mut obj_out, 2);
    let _ = store.try_upsert_sync(key, &zset_to_blob(&obj));
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
    op: crate::objects::sortedset::sorted_set_object::SortedSetOperation,
  ) -> wresp::Result<bool> {
    let required = match op {
      crate::objects::sortedset::sorted_set_object::SortedSetOperation::Geodist => 3,
      _ => 2,
    };
    if parse_state.len() < required {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => SortedSetObject::new(),
    };

    let mut obj_out = ObjectOutput::new();
    let (input, _backing) =
      crate::resp::objects::sorted_set_commands::make_input_for_geo(op, &parse_state[1..], 0, 0);
    obj.operate(&input, &mut obj_out, 2);
    output.extend_from_slice(&obj_out.payload);
    Ok(true)
  }

  /// GEOSEARCH / GEOSEARCHSTORE / GEORADIUS / GEORADIUSBYMEMBER（含 _RO 族）
  ///
  /// libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoSearchCommands
  ///
  /// - `by_member`：圆心取自成员（GEORADIUSBYMEMBER/GEOSEARCH FROMMEMBER）
  /// - `by_box`：矩形（GEOSEARCH BYBOX）
  /// - `store_dist`：GEOSEARCHSTORE 存距离
  pub fn geo_search_commands<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    by_member: bool,
    by_box: bool,
    store_dist: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }

    // GEOSEARCHSTORE：首参为目标键，源为第二参
    let (dest_key, src_idx) = if store_dist {
      match parse_state.get(1).copied() {
        Some(dst) => (Some(dst), 1),
        None => {
          output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
          return Ok(true);
        }
      }
    } else {
      (None, 0)
    };
    let key = parse_state[src_idx];

    let mut obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(raw))) => zset_from_blob(&raw),
      _ => {
        output.extend_from_slice(b"*0\r\n");
        return Ok(true);
      }
    };

    let rest = &parse_state[src_idx + 1..];

    let Ok(parsed) = try_get_geo_search_options(rest, by_member, by_box) else {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    };

    let mut opts = parsed.opts;
    let mut obj_out = ObjectOutput::new();
    obj.geo_search(&mut opts, &mut obj_out, 2, true);

    match dest_key {
      None => output.extend_from_slice(&obj_out.payload),
      Some(dest) => {
        // GEOSEARCHSTORE：命中成员以 GeoHash 分值落入目标集合
        let _ = store_dist;
        let mut dst = SortedSetObject::new();
        for line in obj_out.payload.split(|&b| b == b'\n') {
          let _ = line;
        }
        // 成员直接来自对象层排序结果：此处按 opts 重新计算命中集并入目标
        let mut probe = ObjectOutput::new();
        let mut read_opts = opts.clone();
        read_opts.with_coord = false;
        read_opts.with_dist = false;
        read_opts.with_hash = false;
        read_opts.sort =
          crate::objects::sortedsetgeo::sorted_set_geo_object_impl::GeoOrder::Ascending;
        obj.geo_search(&mut read_opts, &mut probe, 2, true);
        let members: Vec<Vec<u8>> = extract_bulk_members(&probe.payload);
        for member in members {
          if let Some(score) = obj.sorted_set_dict.get(&member).copied() {
            dst.sorted_set_dict.insert(member.clone(), score);
            dst.sorted_set.insert(
              crate::objects::sortedset::sorted_set_object::SortedSetEntry { score, member },
            );
          }
        }
        let _ = store.try_upsert_sync(dest, &zset_to_blob(&dst));
        output.write_resp_int(dst.sorted_set_dict.len() as i64);
      }
    }
    Ok(true)
  }
}

/// 从 GEOSEARCH 回复中提取成员序列（扁平与成对负载兼容）
fn extract_bulk_members(payload: &[u8]) -> Vec<Vec<u8>> {
  let mut members = Vec::new();
  let mut pos = 0;

  // 跳过外层数组头
  if payload.first() == Some(&b'*')
    && let Some(end) = find_crlf(payload, 0)
  {
    pos = end + 2;
  }

  while pos < payload.len() {
    if payload[pos] != b'$' {
      break;
    }
    let Some(line_end) = find_crlf(payload, pos) else {
      break;
    };
    let Ok(len) = str::from_utf8(&payload[pos + 1..line_end])
      .unwrap_or("")
      .parse::<usize>()
    else {
      break;
    };
    let start = line_end + 2;
    let end = start + len;
    if end + 2 > payload.len() {
      break;
    }
    members.push(payload[start..end].to_vec());
    pos = end + 2;

    // 嵌套数组头跳过（*<n>\r\n）
    if payload.get(pos) == Some(&b'*')
      && let Some(end) = find_crlf(payload, pos)
    {
      pos = end + 2;
    }
  }

  members
}

fn find_crlf(payload: &[u8], from: usize) -> Option<usize> {
  (from..payload.len().saturating_sub(1)).find(|&i| payload[i] == b'\r' && payload[i + 1] == b'\n')
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

    // GEOHASH
    out.clear();
    sess
      .geo_commands(
        &[b"cities", b"sf", b"missing"],
        &batch,
        &mut out,
        crate::objects::sortedset::sorted_set_object::SortedSetOperation::Geohash,
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
        crate::objects::sortedset::sorted_set_object::SortedSetOperation::Geopos,
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
        crate::objects::sortedset::sorted_set_object::SortedSetOperation::Geodist,
      )
      .unwrap();
    let dist: f64 = String::from_utf8_lossy(&out)
      .lines()
      .nth(1)
      .unwrap()
      .parse()
      .unwrap();
    assert!((dist - 8967.0).abs() < 30.0, "{dist}");
  }

  #[test]
  fn geosearch_by_radius_from_lonlat() {
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

    // GEOSEARCH pts FROMLONLAT -122.4194 37.7749 BYRADIUS 100 km
    sess
      .geo_search_commands(
        &[
          b"pts",
          b"-122.4194",
          b"37.7749",
          b"BYRADIUS",
          b"100",
          b"km",
          b"WITHDIST",
        ],
        &batch,
        &mut out,
        false,
        false,
        false,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("sf"), "{payload}");
    assert!(!payload.contains("la"), "{payload}");

    // FROMMEMBER
    out.clear();
    sess
      .geo_search_commands(
        &[b"pts", b"la", b"BYRADIUS", b"400", b"km"],
        &batch,
        &mut out,
        true,
        false,
        false,
      )
      .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("la"), "{payload}");
    assert!(!payload.contains("sf"), "{payload}");

    // GEOSEARCHSTORE
    out.clear();
    sess
      .geo_search_commands(
        &[b"store", b"pts", b"la", b"BYRADIUS", b"400", b"km"],
        &batch,
        &mut out,
        true,
        false,
        true,
      )
      .unwrap();
    assert_eq!(out, b":1\r\n");
  }
}
