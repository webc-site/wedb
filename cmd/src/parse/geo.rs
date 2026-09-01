use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::{Error, Result};

#[inline]
fn parse_geo_unit(u: &[u8]) -> Result<String> {
  if u.eq_ignore_ascii_case(b"m") {
    Ok("m".to_string())
  } else if u.eq_ignore_ascii_case(b"km") {
    Ok("km".to_string())
  } else if u.eq_ignore_ascii_case(b"ft") {
    Ok("ft".to_string())
  } else if u.eq_ignore_ascii_case(b"mi") {
    Ok("mi".to_string())
  } else {
    Err(Error::invalid_data(
      "ERR unsupported unit provided. please use m, km, ft, mi",
    ))
  }
}

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: GEOADD key [NX | XX] [CH] lon lat member [lon lat member ...]
    "geoadd" => {
      check_min_args(cmd_name, args, 5)?;
      let key = arg_string(args[1]);
      let mut i = 2;
      let mut nx = false;
      let mut xx = false;
      let mut ch = false;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"nx") {
          nx = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"xx") {
          xx = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"ch") {
          ch = true;
          i += 1;
        } else {
          break;
        }
      }
      if (args.len() - i) < 3 || !(args.len() - i).is_multiple_of(3) {
        return Err(Error::invalid_data(
          "ERR syntax error, GEOADD requires (longitude, latitude, member) triplets",
        ));
      }
      let mut items = Vec::with_capacity((args.len() - i) / 3);
      while i < args.len() {
        let lon = parse_float_strict(args[i])?;
        let lat = parse_float_strict(args[i + 1])?;
        if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
          return Err(Error::invalid_data(
            "ERR -85.05112878 <= latitude <= 85.05112878 and -180 <= longitude <= 180 are required",
          ));
        }
        let member = arg_string(args[i + 2]);
        items.push((lon, lat, member));
        i += 3;
      }
      Ok(Cmd::GeoAdd {
        key,
        nx,
        xx,
        ch,
        items,
      })
    }
    // @cmd: GEODIST key member1 member2 [M | KM | FT | MI]
    "geodist" => {
      check_min_args(cmd_name, args, 4)?;
      let unit = if args.len() > 4 {
        parse_geo_unit(args[4])?
      } else {
        "m".to_string()
      };
      Ok(Cmd::GeoDist {
        key: arg_string(args[1]),
        m1: arg_string(args[2]),
        m2: arg_string(args[3]),
        unit,
      })
    }
    // @cmd: GEOHASH key member [member ...]
    "geohash" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| arg_string(m)).collect();
      Ok(Cmd::GeoHash(key, members))
    }
    // @cmd: GEOPOS key member [member ...]
    "geopos" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| arg_string(m)).collect();
      Ok(Cmd::GeoPos(key, members))
    }
    // @cmd: GEORADIUS / GEORADIUS_RO key lon lat radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
    "georadius" | "georadius_ro" => {
      check_min_args(cmd_name, args, 6)?;
      let key = arg_string(args[1]);
      let lon = parse_float_strict(args[2])?;
      let lat = parse_float_strict(args[3])?;
      let radius = parse_float_strict(args[4])?;
      let unit = parse_geo_unit(args[5])?;

      let mut with_coord = false;
      let mut with_dist = false;
      let mut with_hash = false;
      let mut count = None;
      let mut any = false;
      let mut sort_asc = false;
      let mut sort_desc = false;
      let mut store = None;
      let mut store_dist = None;

      let mut i = 6;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"withcoord") {
          with_coord = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"withdist") {
          with_dist = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"withhash") {
          with_hash = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
          if i < args.len() && args[i].eq_ignore_ascii_case(b"any") {
            any = true;
            i += 1;
          }
        } else if opt.eq_ignore_ascii_case(b"asc") {
          sort_asc = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"desc") {
          sort_desc = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"store") && i + 1 < args.len() {
          store = Some(arg_string(args[i + 1]));
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"storedist") && i + 1 < args.len() {
          store_dist = Some(arg_string(args[i + 1]));
          i += 2;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::GeoRadius {
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
      })
    }
    // @cmd: GEORADIUSBYMEMBER / GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
    "georadiusbymember" | "georadiusbymember_ro" => {
      check_min_args(cmd_name, args, 5)?;
      let key = arg_string(args[1]);
      let member = arg_string(args[2]);
      let radius = parse_float_strict(args[3])?;
      let unit = parse_geo_unit(args[4])?;

      let mut with_coord = false;
      let mut with_dist = false;
      let mut with_hash = false;
      let mut count = None;
      let mut any = false;
      let mut sort_asc = false;
      let mut sort_desc = false;
      let mut store = None;
      let mut store_dist = None;

      let mut i = 5;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"withcoord") {
          with_coord = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"withdist") {
          with_dist = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"withhash") {
          with_hash = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
          if i < args.len() && args[i].eq_ignore_ascii_case(b"any") {
            any = true;
            i += 1;
          }
        } else if opt.eq_ignore_ascii_case(b"asc") {
          sort_asc = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"desc") {
          sort_desc = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"store") && i + 1 < args.len() {
          store = Some(arg_string(args[i + 1]));
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"storedist") && i + 1 < args.len() {
          store_dist = Some(arg_string(args[i + 1]));
          i += 2;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::GeoRadiusByMember {
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
      })
    }
    // @cmd: GEOSEARCH key [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
    "geosearch" => {
      check_min_args(cmd_name, args, 3)?;
      let p = parse_geosearch_args(args)?;
      Ok(Cmd::GeoSearch {
        key: p.key,
        from_lon_lat: p.from_lon_lat,
        from_member: p.from_member,
        by_radius: p.by_radius,
        by_box: p.by_box,
        sort_asc: p.sort_asc,
        sort_desc: p.sort_desc,
        count: p.count,
        any: p.any,
        with_coord: p.with_coord,
        with_dist: p.with_dist,
        with_hash: p.with_hash,
      })
    }
    // @cmd: GEOSEARCHSTORE dest src [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
    "geosearchstore" => {
      check_min_args(cmd_name, args, 4)?;
      let dst = arg_string(args[1]);
      let p = parse_geosearch_args(&args[1..])?;
      Ok(Cmd::GeoSearchStore {
        dst,
        src: p.key,
        from_lon_lat: p.from_lon_lat,
        from_member: p.from_member,
        by_radius: p.by_radius,
        by_box: p.by_box,
        sort_asc: p.sort_asc,
        sort_desc: p.sort_desc,
        count: p.count,
        any: p.any,
        store_dist: p.with_dist,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

struct ParsedGeoSearchArgs {
  key: String,
  from_lon_lat: Option<(f64, f64)>,
  from_member: Option<String>,
  by_radius: Option<(f64, String)>,
  by_box: Option<(f64, f64, String)>,
  sort_asc: bool,
  sort_desc: bool,
  count: Option<usize>,
  any: bool,
  with_coord: bool,
  with_dist: bool,
  with_hash: bool,
}

fn parse_geosearch_args(args: &[&[u8]]) -> Result<ParsedGeoSearchArgs> {
  let key = arg_string(args[1]);
  let mut from_lon_lat = None;
  let mut from_member = None;
  let mut by_radius = None;
  let mut by_box = None;
  let mut sort_asc = false;
  let mut sort_desc = false;
  let mut count = None;
  let mut any = false;
  let mut with_coord = false;
  let mut with_dist = false;
  let mut with_hash = false;

  let mut i = 2;
  while i < args.len() {
    let opt = args[i];
    if opt.eq_ignore_ascii_case(b"fromlonlat") && i + 2 < args.len() {
      let lon = parse_float_strict(args[i + 1])?;
      let lat = parse_float_strict(args[i + 2])?;
      from_lon_lat = Some((lon, lat));
      i += 3;
    } else if opt.eq_ignore_ascii_case(b"frommember") && i + 1 < args.len() {
      from_member = Some(arg_string(args[i + 1]));
      i += 2;
    } else if opt.eq_ignore_ascii_case(b"byradius") && i + 2 < args.len() {
      let r = parse_float_strict(args[i + 1])?;
      let u = parse_geo_unit(args[i + 2])?;
      by_radius = Some((r, u));
      i += 3;
    } else if opt.eq_ignore_ascii_case(b"bybox") && i + 3 < args.len() {
      let w = parse_float_strict(args[i + 1])?;
      let h = parse_float_strict(args[i + 2])?;
      let u = parse_geo_unit(args[i + 3])?;
      by_box = Some((w, h, u));
      i += 4;
    } else if opt.eq_ignore_ascii_case(b"asc") {
      sort_asc = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"desc") {
      sort_desc = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
      count = arg_usize(args[i + 1]);
      i += 2;
      if i < args.len() && args[i].eq_ignore_ascii_case(b"any") {
        any = true;
        i += 1;
      }
    } else if opt.eq_ignore_ascii_case(b"withcoord") {
      with_coord = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"withdist") || opt.eq_ignore_ascii_case(b"storedist") {
      with_dist = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"withhash") {
      with_hash = true;
      i += 1;
    } else {
      i += 1;
    }
  }

  Ok(ParsedGeoSearchArgs {
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
  })
}
