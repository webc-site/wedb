use wnode::objects::sortedsetgeo::geo_hash::GeoHash;

/// test/standalone/Garnet.test.collections/GeoHashTests.cs:CanEncodeAndDecodeCoordinates
#[test]
fn can_encode_and_decode_coordinates() {
  const EPSILON: f64 = 0.00001;
  let cases: &[(f64, f64)] = &[
    (30.5388942218, 104.0555758833),
    (27.988056, 86.925278),
    (37.502669, 15.087269),
    (38.115556, 13.361389),
    (38.918250, -77.427944),
    (-90.0, -180.0),
    (0.0, 0.0),
    (f64::from_bits(1), f64::from_bits(1)),
    (-f64::from_bits(1), -f64::from_bits(1)),
    (90.0, 180.0),
    (89.99999999999999, 179.99999999999997),
  ];

  for &(lat, lon) in cases {
    let hashinteger = GeoHash::geo_to_long_value(lat, lon);
    let (actual_lat, actual_lon) = GeoHash::get_coordinates_from_long(hashinteger);
    let lat_error = (lat - actual_lat).abs();
    let lon_error = (lon - actual_lon).abs();
    assert!(
      lat_error <= EPSILON,
      "Math.Abs(latError)={lat_error:.16} for ({lat}, {lon})"
    );
    assert!(
      lon_error <= EPSILON,
      "Math.Abs(lonError)={lon_error:.16} for ({lat}, {lon})"
    );
  }
}

/// test/standalone/Garnet.test.collections/GeoHashTests.cs:CanEncodeAndDecodeCoordinatesWithGeoHashCode
#[test]
fn can_encode_and_decode_coordinates_with_geo_hash_code() {
  let cases: &[(f64, f64, i64, &str)] = &[
    (
      30.5388942218,
      104.0555758833,
      4024744861876082,
      "wm3vxz6vyw0",
    ),
    (27.988056, 86.925278, 3636631039000829, "tuvz4p141z0"),
    (37.502669, 15.087269, 3476216502357864, "sqdtr74hyu0"),
    (38.115556, 13.361389, 3476004292229755, "sqc8b49rny0"),
    (38.918250, -77.427944, 1787100258949719, "dqbvqhfenp0"),
    (0.0, 0.0, 0xC000000000000, "s0000000000"),
    (-90.0, -180.0, 0, "00000000000"),
    (90.0, 180.0, 0xFFFFFFFFFFFFF, "zzzzzzzzzz0"),
    (
      89.99999999999999,
      179.99999999999997,
      0xFFFFFFFFFFFFF,
      "zzzzzzzzzz0",
    ),
  ];

  for &(lat, lon, expected_hash_integer, expected_hash) in cases {
    let hash_integer = GeoHash::geo_to_long_value(lat, lon);
    let hash = GeoHash::get_geo_hash_code(hash_integer);
    assert_eq!(expected_hash_integer, hash_integer);
    assert_eq!(expected_hash.as_bytes(), &hash);
  }
}
