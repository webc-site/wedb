//! 地理空间操作（对标 libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs，C# 为 StorageSession partial）
//!
//! 与 C# 相同的 GEO 底层：坐标编码为 52 位 interleave geohash 作为有序集合
//! 分值（libs/server/Objects/SortedSet/GeoUtils 语义），检索以成员解码 +
//! Haversine 距离过滤实现。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::types::GarnetStatus;

/// GEO 检索圆心（按已有成员或坐标）
#[derive(Debug, Clone, Copy)]
pub enum GeoCenter<'k> {
  /// 以成员当前位置为圆心
  Member(&'k [u8]),
  /// 以经纬度为圆心
  Coord(f64, f64),
}

/// GEO 子命令分发（DIST / GEOHASH / GEOPOS）
#[derive(Debug, Clone, Copy)]
pub enum GeoCmd<'k> {
  /// GEODIST：两成员球面距离（米）
  Dist(&'k [u8], &'k [u8]),
  /// GEOHASH：成员 52 位分值文本
  Hash(&'k [&'k [u8]]),
  /// GEOPOS：成员经纬度文本
  Pos(&'k [&'k [u8]]),
}

/// Web Mercator 纬度上限（对齐 Redis GEO_LAT_MAX）
const GEO_LAT_MAX: f64 = 85.05112878;

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// GEOADD：坐标编码为 geohash 分值后按有序集合添加
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:GeoAdd
  pub async fn geo_add(
    &self,
    key: &[u8],
    items: &[(f64, f64, &[u8])],
    nx: bool,
    ch: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let members: Vec<(&[u8], f64)> = items
      .iter()
      .map(|&(lon, lat, m)| (m, geohash_encode(lon, lat)))
      .collect();
    self
      .sorted_set_add(key, &members, nx, false, false, ch)
      .await
  }

  /// GEO 子命令统一分发入口
  ///
  /// 键级三态先行：缺键 NOTFOUND / 错误类型 WRONGTYPE（对齐 C# GeoCommands
  /// → ReadObjectStoreOperation）；键命中后成员缺失以 nil 占位（OK 应答）。
  /// 键单次装载，成员查询共用同一字典快照。
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:GeoCommands
  pub async fn geo_commands(
    &self,
    key: &[u8],
    cmd: GeoCmd<'_>,
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    let obj = match self.zset_load(key).await? {
      Err(s) => return Ok((s, Vec::new())),
      Ok(None) => return Ok((GarnetStatus::NotFound, Vec::new())),
      Ok(Some(o)) => o,
    };
    match cmd {
      GeoCmd::Dist(m1, m2) => {
        let dict = &obj.sorted_set_dict;
        let (Some(a), Some(b)) = (dict.get(m1).copied(), dict.get(m2).copied()) else {
          // 成员缺失：OK 应答 nil（C# 键命中后由对象层写 null）
          return Ok((GarnetStatus::Ok, vec![None]));
        };
        let dist = haversine_m(geohash_decode(a), geohash_decode(b));
        let text = format!("{dist:.4}");
        Ok((GarnetStatus::Ok, vec![Some(text.into_bytes())]))
      }
      GeoCmd::Hash(members) => {
        let dict = &obj.sorted_set_dict;
        let mut itoa_buf = itoa::Buffer::new();
        let out = members
          .iter()
          .map(|m| {
            dict
              .get(*m)
              .copied()
              .map(|s| itoa_buf.format(s as i64).as_bytes().to_vec())
          })
          .collect();
        Ok((GarnetStatus::Ok, out))
      }
      GeoCmd::Pos(members) => {
        let dict = &obj.sorted_set_dict;
        let out = members
          .iter()
          .map(|m| {
            dict.get(*m).copied().map(|s| {
              let (lon, lat) = geohash_decode(s);
              format!("{lon:.6},{lat:.6}").into_bytes()
            })
          })
          .collect();
        Ok((GarnetStatus::Ok, out))
      }
    }
  }

  /// GEOSEARCH 只读半径检索（按距离升序返回 (成员, 距离米)）
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:GeoSearchReadOnly
  pub async fn geo_search_read_only(
    &self,
    key: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, f64)>)> {
    let (clon, clat) = match center {
      GeoCenter::Coord(lon, lat) => (lon, lat),
      GeoCenter::Member(m) => match self.sorted_set_score(key, m).await? {
        (GarnetStatus::Ok, Some(s)) => geohash_decode(s),
        (status, _) => return Ok((status, Vec::new())),
      },
    };
    let (status, entries) = self.sorted_set_range(key, 0, -1, false, true).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, Vec::new()));
    }
    let mut hits: Vec<(Vec<u8>, f64)> = entries
      .into_iter()
      .filter_map(|(m, s)| {
        let d = haversine_m((clon, clat), geohash_decode(s?));
        (d <= radius_m).then_some((m, d))
      })
      .collect();
    hits.sort_by(|a, b| a.1.total_cmp(&b.1));
    Ok((GarnetStatus::Ok, hits))
  }

  /// GEOSEARCHSTORE：检索结果以距离为分值写入目标键
  ///
  /// libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:GeoSearchStore
  pub async fn geo_search_store(
    &self,
    dest: &[u8],
    src: &[u8],
    center: GeoCenter<'_>,
    radius_m: f64,
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, hits) = self.geo_search_read_only(src, center, radius_m).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let refs: Vec<(&[u8], f64)> = hits.iter().map(|(m, d)| (m.as_slice(), *d)).collect();
    let _ = self.delete_string(dest).await?;
    let (_, n) = self
      .sorted_set_add(dest, &refs, false, false, false, false)
      .await?;
    Ok((GarnetStatus::Ok, n as usize))
  }
}

/// 坐标 → 52 位 interleave geohash 分值（Redis GEO 语义）
pub fn geohash_encode(lon: f64, lat: f64) -> f64 {
  let lon = lon.clamp(-180.0, 180.0);
  let lat = lat.clamp(-GEO_LAT_MAX, GEO_LAT_MAX);
  let x = ((lon + 180.0) / 360.0 * (1u64 << 26) as f64) as u64;
  let y = ((lat + GEO_LAT_MAX) / (2.0 * GEO_LAT_MAX) * (1u64 << 26) as f64) as u64;
  // 隔位交织（MSB 优先）：偶数位经度、奇数位纬度
  let mut hash = 0u64;
  for i in (0..26u32).rev() {
    hash = (hash << 1) | ((x >> i) & 1);
    hash = (hash << 1) | ((y >> i) & 1);
  }
  hash as f64
}

/// geohash 分值 → 坐标（encode 的逆映射）
pub fn geohash_decode(score: f64) -> (f64, f64) {
  let hash = score as u64;
  let mut x = 0u64;
  let mut y = 0u64;
  for pos in 0..52u32 {
    let bit = (hash >> (51 - pos)) & 1;
    if pos % 2 == 0 {
      x = (x << 1) | bit;
    } else {
      y = (y << 1) | bit;
    }
  }
  let lon = x as f64 / (1u64 << 26) as f64 * 360.0 - 180.0;
  let lat = y as f64 / (1u64 << 26) as f64 * (2.0 * GEO_LAT_MAX) - GEO_LAT_MAX;
  (lon, lat)
}

/// Haversine 球面距离（米，地球半径 6_372_800，对齐 Redis GEODIST）
fn haversine_m(a: (f64, f64), b: (f64, f64)) -> f64 {
  const R: f64 = 6_372_800.0;
  let (lon1, lat1) = a.map_deg_rad();
  let (lon2, lat2) = b.map_deg_rad();
  let dlat = lat2 - lat1;
  let dlon = lon2 - lon1;
  let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
  2.0 * R * h.sqrt().asin()
}

/// (经度, 纬度) 元组的弧度转换辅助
trait DegRad {
  fn map_deg_rad(self) -> (f64, f64);
}

impl DegRad for (f64, f64) {
  fn map_deg_rad(self) -> (f64, f64) {
    (self.0.to_radians(), self.1.to_radians())
  }
}
