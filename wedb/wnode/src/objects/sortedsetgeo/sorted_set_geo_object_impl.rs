//! 有序集合 GEO 命令语义（对标 libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs
//! 与 GeoSearchOptions.cs，C# 为 SortedSetObject 的 partial 分片）
//!
//! GEOADD 以 52 位 GeoHash 整数为分值入集合；GEOSEARCH 按 radius/box 过滤
//! 并按距离排序输出。

use bitflags::bitflags;

use crate::{
  inputs::ObjectInput,
  objects::{
    parse_utils::{try_get_geo_distance_unit, try_get_geo_lon_lat},
    sortedset::sorted_set_object::{SortedSetEntry, SortedSetObject},
    sortedsetgeo::geo_hash::{GeoDistanceUnitType, GeoHash},
    types::object_output::ObjectOutput,
  },
};

bitflags! {
  /// GEOADD 选项
  ///
  /// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoAddOptions
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct GeoAddOptions: u8 {
    /// 无选项
    const NONE = 0;
    /// 仅新增
    const NX = 1 << 0;
    /// 仅更新
    const XX = 1 << 1;
    /// 返回"新增+变更"总数
    const CH = 1 << 2;
  }
}

/// GEOSEARCH 结果排序方向
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoOrder
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoOrder {
  /// 不排序
  #[default]
  None = 0,
  /// 距离升序
  Ascending,
  /// 距离降序
  Descending,
}

/// GEOSEARCH 圆心来源
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoOriginType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoOriginType {
  #[default]
  Undefined = 0,
  /// 显式经纬度
  FromLonLat,
  /// 既有成员
  FromMember,
}

/// GEOSEARCH 形状
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoSearchType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoSearchType {
  #[default]
  Undefined = 0,
  /// 圆形（BYRADIUS）
  ByRadius,
  /// 轴对齐矩形（BYBOX）
  ByBox,
}

/// GEOSEARCH 选项束
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoSearchOptions
#[derive(Debug, Clone, Default)]
pub struct GeoSearchOptions {
  pub search_type: GeoSearchType,
  pub unit: GeoDistanceUnitType,
  /// 半径（BYRADIUS）；BYBOX 时复用为高度（box_height 的 C# getter/setter 即 radius）
  pub radius: f64,
  /// 矩形宽（BYBOX）
  pub box_width: f64,
  pub count_value: i64,
  pub origin: GeoOriginType,
  /// FROMMEMBER 的成员名
  pub from_member: Vec<u8>,
  /// 圆心坐标
  pub lon: f64,
  pub lat: f64,
  pub with_coord: bool,
  pub with_hash: bool,
  pub with_count_any: bool,
  pub with_dist: bool,
  pub sort: GeoOrder,
}

/// GEOSEARCH 单条命中
///
/// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoSearchData
#[derive(Debug, Clone)]
pub struct GeoSearchData {
  pub member: Vec<u8>,
  pub distance: f64,
  pub geo_hash: i64,
  pub geo_hash_code: [u8; GeoHash::CODE_LENGTH],
  pub coordinates: (f64, f64),
}

/// 取第 i 个参数字节
///
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

/// GEOSEARCH FROMMEMBER 圆心缺失错误
/// （对标 CmdStrings.RESP_ERR_ZSET_MEMBER）
const RESP_ERR_ZSET_MEMBER: &[u8] = b"ERR could not decode requested zset member";

impl SortedSetObject {
  /// GEOADD：以 GeoHash 整数为分值批量登记成员
  ///
  /// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoAdd
  pub(crate) fn geo_add(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    self.delete_expired_items();

    // 缺省：新增并更新既有成员
    let options = GeoAddOptions::from_bits_truncate(input.arg1 as u8);

    let count = input.parse_state.count;
    let mut curr_token_idx = 0;

    let mut elements_added = 0_i64;
    let mut elements_changed = 0_i64;

    while curr_token_idx < count {
      // C# 对 TryGetGeoLonLat 失败仅 Debug.Assert，release 下 out 参数落回 0.0，
      // 成员仍以 (0,0) 坐标入集合；1:1 对齐该兜底（命令层已保证三元组）
      let (longitude, latitude) = if curr_token_idx + 1 < count {
        try_get_geo_lon_lat(arg(input, curr_token_idx), arg(input, curr_token_idx + 1))
          .unwrap_or((0.0, 0.0))
      } else {
        (0.0, 0.0)
      };
      curr_token_idx += 2;

      // member（防御奇数尾巴，C# 为越界读）
      if curr_token_idx >= count {
        break;
      }
      let member = arg(input, curr_token_idx).to_vec();
      curr_token_idx += 1;

      let score = GeoHash::geo_to_long_value(latitude, longitude);
      if score == -1 {
        continue;
      }

      match self.sorted_set_dict.get(&member).copied() {
        None => {
          // XX 时仅更新既有成员（对齐 C# (options & XX) == 0 才新增；
          // NX 只挡更新分支，新成员照常新增——与 Redis 语义一致）
          if !options.contains(GeoAddOptions::XX) {
            self.sorted_set_dict.insert(member.clone(), score as f64);
            self.sorted_set.insert(SortedSetEntry {
              score: score as f64,
              member: member.clone(),
            });
            elements_added += 1;

            self.update_size(&member, true);
            elements_changed += 1;
          }
        }
        Some(score_stored) => {
          // 非 NX 且分值变化时更新
          if !options.contains(GeoAddOptions::NX) && score_stored != score as f64 {
            self.sorted_set_dict.insert(member.clone(), score as f64);
            self.sorted_set.remove(&SortedSetEntry {
              score: score_stored,
              member: member.clone(),
            });
            self.sorted_set.insert(SortedSetEntry {
              score: score as f64,
              member: member.clone(),
            });
            elements_changed += 1;
          }
        }
      }
    }

    let result = if !options.contains(GeoAddOptions::CH) {
      elements_added
    } else {
      elements_changed
    };
    output.write_int64(result);
  }

  /// GEOHASH：成员的 base-32 GeoHash 文本
  ///
  /// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoHash
  pub(crate) fn geo_hash(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let member = arg(input, i);

      match self.sorted_set_dict.get(member).copied() {
        Some(value52_int) => {
          let geo_hash = GeoHash::get_geo_hash_code(value52_int as i64);
          output.write_ascii_bulk_string(&geo_hash);
        }
        None => output.write_null(resp_protocol_version),
      }
    }
  }

  /// GEODIST：两成员球面距离（按单位换算）
  ///
  /// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoDistance
  pub(crate) fn geo_distance(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    let member1 = arg(input, 0);
    let member2 = arg(input, 1);

    // 缺省米；C# 对解析失败仅 Debug.Assert，release 下 out 参数落回 default(M)
    let mut units = GeoDistanceUnitType::M;
    if input.parse_state.count > 2 {
      units = try_get_geo_distance_unit(arg(input, 2)).unwrap_or(GeoDistanceUnitType::M);
    }

    match (
      self.sorted_set_dict.get(member1).copied(),
      self.sorted_set_dict.get(member2).copied(),
    ) {
      (Some(score_member1), Some(score_member2)) => {
        let first = GeoHash::get_coordinates_from_long(score_member1 as i64);
        let second = GeoHash::get_coordinates_from_long(score_member2 as i64);

        let distance = GeoHash::distance(first.0, first.1, second.0, second.1);
        output.write_double_bulk_string(GeoHash::convert_meters_to_units(distance, units));
      }
      _ => output.write_null(resp_protocol_version),
    }
  }

  /// GEOPOS：成员坐标
  ///
  /// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoPosition
  pub(crate) fn geo_position(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) {
    output.write_array_length(input.parse_state.count);

    for i in 0..input.parse_state.count {
      let member = arg(input, i);

      match self.sorted_set_dict.get(member).copied() {
        Some(score_member) => {
          let (lat, lon) = GeoHash::get_coordinates_from_long(score_member as i64);

          output.write_array_length(2);
          output.write_double_numeric(lon, resp_protocol_version);
          output.write_double_numeric(lat, resp_protocol_version);
        }
        None => output.write_null_array(resp_protocol_version),
      }
    }
  }

  /// GEOSEARCH：圆/矩形范围查询（含 WITHDIST/WITHHASH/WITHCOORD/COUNT/ASC|DESC）
  ///
  /// libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoSearch
  ///
  /// C# 中由 StorageSession（GeoSearchReadOnly/GeoSearchStore）直入而非经
  /// Operate 分派；Rust 侧同为对象公共面，命令层接线在 Resp 域周期完成
  pub fn geo_search(
    &mut self,
    opts: &mut GeoSearchOptions,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
    read_only: bool,
  ) {
    // FROMMEMBER：圆心取成员坐标
    if opts.origin == GeoOriginType::FromMember {
      let Some(center_point_score) = self.sorted_set_dict.get(&opts.from_member).copied() else {
        output.write_error(RESP_ERR_ZSET_MEMBER);
        return;
      };

      (opts.lat, opts.lon) = GeoHash::get_coordinates_from_long(center_point_score as i64);
    }

    let mut response_data: Vec<GeoSearchData> = Vec::with_capacity(
      if opts.with_count_any
        && opts.count_value > 0
        && (opts.count_value as usize) < self.sorted_set.len()
      {
        opts.count_value as usize
      } else {
        self.sorted_set.len()
      },
    );

    for point in &self.sorted_set {
      let coor_in_item = GeoHash::get_coordinates_from_long(point.score as i64);

      let distance = if opts.search_type == GeoSearchType::ByBox {
        let Some(d) = GeoHash::get_distance_when_in_rectangle(
          GeoHash::convert_value_to_meters(opts.box_width, opts.unit),
          GeoHash::convert_value_to_meters(opts.radius, opts.unit),
          opts.lat,
          opts.lon,
          coor_in_item.0,
          coor_in_item.1,
        ) else {
          continue;
        };
        d
      } else {
        // byRadius
        let Some(d) = GeoHash::is_point_within_radius(
          GeoHash::convert_value_to_meters(opts.radius, opts.unit),
          opts.lat,
          opts.lon,
          coor_in_item.0,
          coor_in_item.1,
        ) else {
          continue;
        };
        d
      };

      // 点落在形状内
      response_data.push(GeoSearchData {
        member: point.member.clone(),
        distance,
        geo_hash: point.score as i64,
        geo_hash_code: GeoHash::get_geo_hash_code(point.score as i64),
        coordinates: GeoHash::get_coordinates_from_long(point.score as i64),
      });

      if opts.with_count_any && response_data.len() == opts.count_value as usize {
        break;
      }
    }

    if response_data.is_empty() {
      output.write_empty_array();
      return;
    }

    let mut inner_array_length = 1_usize;
    if opts.with_dist {
      inner_array_length += 1;
    }
    if opts.with_hash {
      inner_array_length += 1;
    }
    if opts.with_coord {
      inner_array_length += 1;
    }

    // 距离排序（C# LINQ OrderBy/OrderByDescending）
    match opts.sort {
      GeoOrder::Descending => response_data.sort_by(|a, b| b.distance.total_cmp(&a.distance)),
      GeoOrder::Ascending => response_data.sort_by(|a, b| a.distance.total_cmp(&b.distance)),
      GeoOrder::None => {
        if !opts.with_count_any && opts.count_value > 0 {
          response_data.sort_by(|a, b| a.distance.total_cmp(&b.distance));
        }
      }
    }

    if opts.count_value > 0 && (opts.count_value as usize) < response_data.len() {
      response_data.truncate(opts.count_value as usize);
      output.write_array_length(opts.count_value as usize);
    } else {
      output.write_array_length(response_data.len());
    }

    for item in &response_data {
      if inner_array_length > 1 {
        output.write_array_length(inner_array_length);
      }

      output.write_bulk_string(&item.member);

      if opts.with_dist {
        output.write_double_bulk_string(GeoHash::convert_meters_to_units(item.distance, opts.unit));
      }

      if opts.with_hash {
        if read_only {
          output.write_int64(item.geo_hash);
        } else {
          output.write_array_item(item.geo_hash);
        }
      }

      if opts.with_coord {
        output.write_array_length(2);
        output.write_double_numeric(item.coordinates.1, resp_protocol_version);
        output.write_double_numeric(item.coordinates.0, resp_protocol_version);
      }
    }
  }
}
