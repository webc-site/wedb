use rapidhash::RapidHashSet;
use std::cmp::Ordering;
use std::str::from_utf8;
use std::sync::Arc;

use super::context::{ConnectionContext, KeyComposer};
use super::zset::{get_member_score, handle_zset};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::geo::{
  GeoPoint, GeoShape, GeoShapeType, convert_meters_to_unit, convert_unit_to_meters, decode_geohash,
  encode_geohash, geohash_to_base32, get_areas_by_shape_wgs84, haversine_distance,
  scores_of_geohash_box,
};
use crate::redis::protocol::RespValue;

#[inline]
async fn get_member_coord(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  member: &str,
) -> Result<Option<(f64, f64, u64)>> {
  if let Some(score) = get_member_score(node, kc, key, member.as_bytes()).await? {
    let hash = score as u64;
    let (lon, lat) = decode_geohash(hash);
    return Ok(Some((lon, lat, hash)));
  }
  Ok(None)
}

async fn perform_geo_search(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  key: &str,
  mut shape: GeoShape,
  sort_asc: bool,
  sort_desc: bool,
  count: Option<usize>,
) -> Result<Vec<GeoPoint>> {
  let radius_info = get_areas_by_shape_wgs84(&mut shape);
  let mut boxes = Vec::with_capacity(9);
  if !radius_info.hash.is_zero() {
    boxes.push(radius_info.hash);
  }
  for n in [
    radius_info.neighbors.north,
    radius_info.neighbors.south,
    radius_info.neighbors.east,
    radius_info.neighbors.west,
    radius_info.neighbors.north_east,
    radius_info.neighbors.north_west,
    radius_info.neighbors.south_east,
    radius_info.neighbors.south_west,
  ] {
    if !n.is_zero() && !boxes.contains(&n) {
      boxes.push(n);
    }
  }

  let mut matched = Vec::new();
  let mut seen_members = RapidHashSet::default();

  let mut itoa_buf1 = itoa::Buffer::new();
  let mut itoa_buf2 = itoa::Buffer::new();

  for h in boxes {
    let (min_s, max_s) = scores_of_geohash_box(h);
    let min_str = itoa_buf1.format(min_s).to_string();
    let max_str = itoa_buf2.format(max_s).to_string();
    let zset_items = match Box::pin(handle_zset(
      node,
      ctx,
      RedisCommand::ZRange {
        key: key.to_string(),
        min: min_str,
        max: max_str,
        by_score: true,
        by_lex: false,
        rev: false,
        offset: 0,
        count: None,
        with_scores: true,
      },
    ))
    .await?
    {
      RespValue::Arr(items) => items,
      _ => Vec::new(),
    };

    for chunk in zset_items.chunks(2) {
      if chunk.len() == 2
        && let (RespValue::Blob(m), RespValue::Blob(s_bytes)) = (&chunk[0], &chunk[1])
      {
        if !seen_members.insert(m.clone()) {
          continue;
        }
        let hash = from_utf8(s_bytes)
          .ok()
          .and_then(|s| s.parse::<f64>().ok())
          .unwrap_or(0.0) as u64;
        let (item_lon, item_lat) = decode_geohash(hash);
        let dist = haversine_distance(shape.center_lon, shape.center_lat, item_lon, item_lat);

        let inside = match shape.shape_type {
          GeoShapeType::Circular => dist <= shape.radius,
          GeoShapeType::Rectangular => {
            item_lon >= shape.bounds[0]
              && item_lon <= shape.bounds[2]
              && item_lat >= shape.bounds[1]
              && item_lat <= shape.bounds[3]
          }
          GeoShapeType::None => true,
        };

        if inside {
          matched.push(GeoPoint {
            longitude: item_lon,
            latitude: item_lat,
            member: String::from_utf8_lossy(m).to_string(),
            dist,
            score: hash as f64,
          });
        }
      }
    }
  }

  if sort_asc {
    matched.sort_unstable_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
  } else if sort_desc {
    matched.sort_unstable_by(|a, b| b.dist.partial_cmp(&a.dist).unwrap_or(Ordering::Equal));
  }

  if let Some(limit) = count {
    matched.truncate(limit);
  }

  Ok(matched)
}

/// 地理位置 (Geo) 命令主调度处理器
pub async fn handle_geo(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::GeoAdd {
      key,
      nx,
      xx,
      ch,
      items,
    } => {
      let mut zset_members = Vec::with_capacity(items.len());
      for (lon, lat, member) in items {
        let hash = encode_geohash(lon, lat);
        zset_members.push((hash as f64, member.into_bytes()));
      }
      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZAdd {
          key,
          nx,
          xx,
          gt: false,
          lt: false,
          ch,
          incr: false,
          members: zset_members,
        },
      ))
      .await
    }
    RedisCommand::GeoDist { key, m1, m2, unit } => {
      let p1 = get_member_coord(node, &kc, &key, &m1).await?;
      let p2 = get_member_coord(node, &kc, &key, &m2).await?;
      if let (Some((lon1, lat1, _)), Some((lon2, lat2, _))) = (p1, p2) {
        let dist_meters = haversine_distance(lon1, lat1, lon2, lat2);
        let dist = convert_meters_to_unit(dist_meters, &unit);
        Ok(RespValue::Blob(format!("{dist:.4}").into_bytes()))
      } else {
        Ok(RespValue::Null)
      }
    }
    RedisCommand::GeoHash(key, members) => {
      let mut list = Vec::with_capacity(members.len());
      for m in members {
        if let Some((_, _, hash)) = get_member_coord(node, &kc, &key, &m).await? {
          list.push(RespValue::Blob(geohash_to_base32(hash).into_bytes()));
        } else {
          list.push(RespValue::Null);
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::GeoPos(key, members) => {
      let mut list = Vec::with_capacity(members.len());
      for m in members {
        if let Some((lon, lat, _)) = get_member_coord(node, &kc, &key, &m).await? {
          list.push(RespValue::Arr(vec![
            RespValue::Blob(format!("{lon:.6}").into_bytes()),
            RespValue::Blob(format!("{lat:.6}").into_bytes()),
          ]));
        } else {
          list.push(RespValue::Null);
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::GeoRadius {
      key,
      lon,
      lat,
      radius,
      unit,
      with_coord,
      with_dist,
      with_hash,
      count,
      any: _,
      sort_asc,
      sort_desc,
      store,
      store_dist,
    } => {
      let radius_meters = convert_unit_to_meters(radius, &unit);
      let shape = GeoShape::new_circular(lon, lat, radius_meters);
      let matched = perform_geo_search(node, ctx, &key, shape, sort_asc, sort_desc, count).await?;

      if let Some(store_k) = store {
        let zset_members: Vec<_> = matched
          .into_iter()
          .map(|p| (p.score, p.member.into_bytes()))
          .collect();
        let len = zset_members.len();
        Box::pin(handle_zset(
          node,
          ctx,
          RedisCommand::ZAdd {
            key: store_k,
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            incr: false,
            members: zset_members,
          },
        ))
        .await?;
        Ok(RespValue::Int(len as i64))
      } else if let Some(store_dist_k) = store_dist {
        let zset_members: Vec<_> = matched
          .into_iter()
          .map(|p| (convert_meters_to_unit(p.dist, &unit), p.member.into_bytes()))
          .collect();
        let len = zset_members.len();
        Box::pin(handle_zset(
          node,
          ctx,
          RedisCommand::ZAdd {
            key: store_dist_k,
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            incr: false,
            members: zset_members,
          },
        ))
        .await?;
        Ok(RespValue::Int(len as i64))
      } else {
        let mut results = Vec::with_capacity(matched.len());
        for p in matched {
          if !with_coord && !with_dist && !with_hash {
            results.push(RespValue::Blob(p.member.into_bytes()));
          } else {
            let mut entry = vec![RespValue::Blob(p.member.into_bytes())];
            if with_dist {
              let dist_val = convert_meters_to_unit(p.dist, &unit);
              entry.push(RespValue::Blob(format!("{dist_val:.4}").into_bytes()));
            }
            if with_hash {
              entry.push(RespValue::Int(p.score as i64));
            }
            if with_coord {
              let lon = p.longitude;
              let lat = p.latitude;
              entry.push(RespValue::Arr(vec![
                RespValue::Blob(format!("{lon:.6}").into_bytes()),
                RespValue::Blob(format!("{lat:.6}").into_bytes()),
              ]));
            }
            results.push(RespValue::Arr(entry));
          }
        }
        Ok(RespValue::Arr(results))
      }
    }
    RedisCommand::GeoRadiusByMember {
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
      if let Some((lon, lat, _)) = get_member_coord(node, &kc, &key, &member).await? {
        Box::pin(handle_geo(
          node,
          ctx,
          RedisCommand::GeoRadius {
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
          },
        ))
        .await
      } else {
        Err(Error::invalid_data(
          "ERR could not decode requested zset member",
        ))
      }
    }
    RedisCommand::GeoSearch {
      key,
      from_lon_lat,
      from_member,
      by_radius,
      by_box,
      sort_asc,
      sort_desc,
      count,
      any: _,
      with_coord,
      with_dist,
      with_hash,
    } => {
      let (center_lon, center_lat) = if let Some((lon, lat)) = from_lon_lat {
        (lon, lat)
      } else if let Some(ref m) = from_member {
        if let Some((lon, lat, _)) = get_member_coord(node, &kc, &key, m).await? {
          (lon, lat)
        } else {
          return Err(Error::invalid_data(
            "ERR could not decode requested zset member",
          ));
        }
      } else {
        return Err(Error::invalid_data(
          "ERR exactly one of FROMLONLAT or FROMMEMBER must be specified",
        ));
      };

      let (shape, unit) = if let Some((r, u)) = by_radius {
        let r_m = convert_unit_to_meters(r, &u);
        (GeoShape::new_circular(center_lon, center_lat, r_m), u)
      } else if let Some((w, h, u)) = by_box {
        let w_m = convert_unit_to_meters(w, &u);
        let h_m = convert_unit_to_meters(h, &u);
        (
          GeoShape::new_rectangular(center_lon, center_lat, w_m, h_m),
          u,
        )
      } else {
        return Err(Error::invalid_data(
          "ERR exactly one of BYRADIUS or BYBOX must be specified",
        ));
      };

      let matched = perform_geo_search(node, ctx, &key, shape, sort_asc, sort_desc, count).await?;
      let mut results = Vec::with_capacity(matched.len());
      for p in matched {
        if !with_coord && !with_dist && !with_hash {
          results.push(RespValue::Blob(p.member.into_bytes()));
        } else {
          let mut entry = vec![RespValue::Blob(p.member.into_bytes())];
          if with_dist {
            let dist_val = convert_meters_to_unit(p.dist, &unit);
            entry.push(RespValue::Blob(format!("{dist_val:.4}").into_bytes()));
          }
          if with_hash {
            entry.push(RespValue::Int(p.score as i64));
          }
          if with_coord {
            let lon = p.longitude;
            let lat = p.latitude;
            entry.push(RespValue::Arr(vec![
              RespValue::Blob(format!("{lon:.6}").into_bytes()),
              RespValue::Blob(format!("{lat:.6}").into_bytes()),
            ]));
          }
          results.push(RespValue::Arr(entry));
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::GeoSearchStore {
      dst,
      src,
      from_lon_lat,
      from_member,
      by_radius,
      by_box,
      sort_asc,
      sort_desc,
      count,
      any: _,
      store_dist,
    } => {
      let (center_lon, center_lat) = if let Some((lon, lat)) = from_lon_lat {
        (lon, lat)
      } else if let Some(ref m) = from_member {
        if let Some((lon, lat, _)) = get_member_coord(node, &kc, &src, m).await? {
          (lon, lat)
        } else {
          return Err(Error::invalid_data(
            "ERR could not decode requested zset member",
          ));
        }
      } else {
        return Err(Error::invalid_data(
          "ERR exactly one of FROMLONLAT or FROMMEMBER must be specified",
        ));
      };

      let (shape, unit) = if let Some((r, u)) = by_radius {
        let r_m = convert_unit_to_meters(r, &u);
        (GeoShape::new_circular(center_lon, center_lat, r_m), u)
      } else if let Some((w, h, u)) = by_box {
        let w_m = convert_unit_to_meters(w, &u);
        let h_m = convert_unit_to_meters(h, &u);
        (
          GeoShape::new_rectangular(center_lon, center_lat, w_m, h_m),
          u,
        )
      } else {
        return Err(Error::invalid_data(
          "ERR exactly one of BYRADIUS or BYBOX must be specified",
        ));
      };

      let matched = perform_geo_search(node, ctx, &src, shape, sort_asc, sort_desc, count).await?;
      let mut zset_members = Vec::with_capacity(matched.len());
      for p in matched {
        let score = if store_dist {
          convert_meters_to_unit(p.dist, &unit)
        } else {
          p.score
        };
        zset_members.push((score, p.member.into_bytes()));
      }
      let len = zset_members.len();

      Box::pin(handle_zset(
        node,
        ctx,
        RedisCommand::ZAdd {
          key: dst,
          nx: false,
          xx: false,
          gt: false,
          lt: false,
          ch: false,
          incr: false,
          members: zset_members,
        },
      ))
      .await?;

      Ok(RespValue::Int(len as i64))
    }
    _ => Err(Error::internal("unsupported geo command")),
  }
}
