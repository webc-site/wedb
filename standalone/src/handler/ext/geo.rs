use crate::handler::resp_util::float_to_blob;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{
  DistanceSort, DistanceUnit, Error, GeoRadiusOption, GeoSearch, GeoSearchStoreOption, GeoShape,
  OriginPoint, Result, WeDb, ZAdd,
};
use wedb_resp::RespValue;

/// 处理所有 Geo (空间地理位置) 命令
pub async fn handle_geo(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::GeoAdd {
      key,
      items,
      nx,
      xx,
      ch,
    } => {
      let m: Vec<(f64, f64, &[u8])> = items
        .iter()
        .map(|(lon, lat, mem)| (*lon, *lat, mem.as_bytes()))
        .collect();
      let mut opts = Vec::new();
      if nx {
        opts.push(ZAdd::Nx);
      }
      if xx {
        opts.push(ZAdd::Xx);
      }
      if ch {
        opts.push(ZAdd::Ch);
      }
      let count = db.geoadd_opts(key.as_bytes(), &m, opts)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::GeoDist { key, m1, m2, unit } => {
      let dist = db.geodist(
        key.as_bytes(),
        m1.as_bytes(),
        m2.as_bytes(),
        Some(unit.as_str()),
      )?;
      match dist {
        Some(d) => Ok(float_to_blob(d)),
        None => Ok(RespValue::Null),
      }
    }
    Cmd::GeoHash(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(|s| s.as_bytes()).collect();
      let hashes = db.geohash(key.as_bytes(), &m)?;
      let arr = hashes
        .into_iter()
        .map(|h| match h {
          Some(s) => RespValue::Blob(s.into_bytes()),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::GeoPos(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(|s| s.as_bytes()).collect();
      let positions = db.geopos(key.as_bytes(), &m)?;
      let arr = positions
        .into_iter()
        .map(|p| match p {
          Some((lon, lat)) => RespValue::Arr(vec![float_to_blob(lon), float_to_blob(lat)]),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::GeoRadius {
      key,
      lon,
      lat,
      radius,
      unit,
      with_coord,
      with_dist,
      with_hash,
      count,
      any,
      sort_asc,
      sort_desc,
      store,
      store_dist,
    } => {
      let u = DistanceUnit::parse(&unit).unwrap_or(DistanceUnit::Meters);
      let sort = if sort_asc {
        DistanceSort::Asc
      } else if sort_desc {
        DistanceSort::Desc
      } else {
        DistanceSort::None
      };
      let opt = GeoRadiusOption {
        unit: u,
        with_coord,
        with_dist,
        with_hash,
        count,
        any,
        sort,
        store_key: store,
        store_dist_key: store_dist,
      };
      let points = db.georadius(key.as_bytes(), lon, lat, radius, &opt)?;
      if opt.store_key.is_some() || opt.store_dist_key.is_some() {
        return Ok(RespValue::Int(points.len() as i64));
      }
      let mut results = Vec::with_capacity(points.len());
      for p in points {
        if !with_coord && !with_dist && !with_hash {
          results.push(RespValue::Blob(p.member.into_bytes()));
        } else {
          let mut entry = vec![RespValue::Blob(p.member.into_bytes())];
          if with_dist {
            entry.push(float_to_blob(p.dist));
          }
          if with_hash {
            entry.push(RespValue::Int(p.score as i64));
          }
          if with_coord {
            entry.push(RespValue::Arr(vec![
              float_to_blob(p.longitude),
              float_to_blob(p.latitude),
            ]));
          }
          results.push(RespValue::Arr(entry));
        }
      }
      Ok(RespValue::Arr(results))
    }
    Cmd::GeoRadiusByMember {
      key,
      member,
      radius,
      unit,
      with_coord,
      with_dist,
      with_hash,
      count,
      any,
      sort_asc,
      sort_desc,
      store,
      store_dist,
    } => {
      let u = DistanceUnit::parse(&unit).unwrap_or(DistanceUnit::Meters);
      let sort = if sort_asc {
        DistanceSort::Asc
      } else if sort_desc {
        DistanceSort::Desc
      } else {
        DistanceSort::None
      };
      let opt = GeoRadiusOption {
        unit: u,
        with_coord,
        with_dist,
        with_hash,
        count,
        any,
        sort,
        store_key: store,
        store_dist_key: store_dist,
      };
      let points = db.georadiusbymember(key.as_bytes(), member.as_bytes(), radius, &opt)?;
      if opt.store_key.is_some() || opt.store_dist_key.is_some() {
        return Ok(RespValue::Int(points.len() as i64));
      }
      let mut results = Vec::with_capacity(points.len());
      for p in points {
        if !with_coord && !with_dist && !with_hash {
          results.push(RespValue::Blob(p.member.into_bytes()));
        } else {
          let mut entry = vec![RespValue::Blob(p.member.into_bytes())];
          if with_dist {
            entry.push(float_to_blob(p.dist));
          }
          if with_hash {
            entry.push(RespValue::Int(p.score as i64));
          }
          if with_coord {
            entry.push(RespValue::Arr(vec![
              float_to_blob(p.longitude),
              float_to_blob(p.latitude),
            ]));
          }
          results.push(RespValue::Arr(entry));
        }
      }
      Ok(RespValue::Arr(results))
    }
    Cmd::GeoSearch {
      key,
      from_lon_lat,
      from_member,
      by_radius,
      by_box,
      sort_asc,
      sort_desc,
      count,
      any,
      with_coord,
      with_dist,
      with_hash,
    } => {
      let origin = if let Some((lon, lat)) = from_lon_lat {
        OriginPoint::Coord { lon, lat }
      } else if let Some(m) = from_member {
        OriginPoint::Member(m)
      } else {
        return Err(Error::invalid_data("ERR missing FROM options in GEOSEARCH"));
      };

      let (mut shape, unit) = if let Some((r, u_str)) = by_radius {
        let u = DistanceUnit::parse(&u_str).unwrap_or(DistanceUnit::Meters);
        (GeoShape::new_circular_with_unit(0.0, 0.0, r, u), u)
      } else if let Some((w, h, u_str)) = by_box {
        let u = DistanceUnit::parse(&u_str).unwrap_or(DistanceUnit::Meters);
        (GeoShape::new_rectangular_with_unit(0.0, 0.0, w, h, u), u)
      } else {
        return Err(Error::invalid_data("ERR missing BY options in GEOSEARCH"));
      };

      let sort = if sort_asc {
        DistanceSort::Asc
      } else if sort_desc {
        DistanceSort::Desc
      } else {
        DistanceSort::None
      };
      let search_opt = GeoSearch {
        unit,
        with_coord,
        with_dist,
        with_hash,
        count,
        any,
        asc: sort_asc,
        sort,
      };

      let points = db.geosearch(key.as_bytes(), &origin, &mut shape, &search_opt)?;
      let mut results = Vec::with_capacity(points.len());
      for p in points {
        if !with_coord && !with_dist && !with_hash {
          results.push(RespValue::Blob(p.member.into_bytes()));
        } else {
          let mut entry = vec![RespValue::Blob(p.member.into_bytes())];
          if with_dist {
            entry.push(float_to_blob(p.dist));
          }
          if with_hash {
            entry.push(RespValue::Int(p.score as i64));
          }
          if with_coord {
            entry.push(RespValue::Arr(vec![
              float_to_blob(p.longitude),
              float_to_blob(p.latitude),
            ]));
          }
          results.push(RespValue::Arr(entry));
        }
      }
      Ok(RespValue::Arr(results))
    }
    Cmd::GeoSearchStore {
      dst,
      src,
      from_lon_lat,
      from_member,
      by_radius,
      by_box,
      sort_asc,
      sort_desc,
      count,
      any,
      store_dist,
    } => {
      let origin = if let Some((lon, lat)) = from_lon_lat {
        OriginPoint::Coord { lon, lat }
      } else if let Some(m) = from_member {
        OriginPoint::Member(m)
      } else {
        return Err(Error::invalid_data("ERR missing FROM options"));
      };

      let (mut shape, unit) = if let Some((r, u_str)) = by_radius {
        let u = DistanceUnit::parse(&u_str).unwrap_or(DistanceUnit::Meters);
        (GeoShape::new_circular_with_unit(0.0, 0.0, r, u), u)
      } else if let Some((w, h, u_str)) = by_box {
        let u = DistanceUnit::parse(&u_str).unwrap_or(DistanceUnit::Meters);
        (GeoShape::new_rectangular_with_unit(0.0, 0.0, w, h, u), u)
      } else {
        return Err(Error::invalid_data("ERR missing BY options"));
      };

      let sort = if sort_asc {
        DistanceSort::Asc
      } else if sort_desc {
        DistanceSort::Desc
      } else {
        DistanceSort::None
      };
      let search_opt = GeoSearchStoreOption {
        count,
        any,
        sort,
        store_dist,
        unit,
      };

      let stored = db.geosearchstore(
        dst.as_bytes(),
        src.as_bytes(),
        &origin,
        &mut shape,
        &search_opt,
      )?;
      Ok(RespValue::Int(stored as i64))
    }
    _ => Err(Error::internal("unsupported geo command")),
  }
}
