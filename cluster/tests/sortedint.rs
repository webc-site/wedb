use wedb_cluster::redis::sortedint::{
  SortedIntSet, decode_hex_u64, encode_hex_u64, parse_range_spec,
};

#[test]
fn test_hex_u64_codec() {
  let test_cases = [0u64, 1, 15, 16, 255, 1000, 1234567890123456789, u64::MAX];
  for &val in &test_cases {
    let encoded = encode_hex_u64(val);
    let decoded = decode_hex_u64(&encoded).expect("Decode should succeed");
    assert_eq!(val, decoded);

    let expected_hex = format!("{val:016x}");
    assert_eq!(encoded, expected_hex.as_bytes());
  }

  assert_eq!(decode_hex_u64(b"invalid_length"), None);
  assert_eq!(decode_hex_u64(b"000000000000000g"), None);
}

#[test]
fn test_parse_range_spec() {
  let spec = parse_range_spec("-inf", "+inf").unwrap();
  assert_eq!(spec.min, u64::MIN);
  assert!(!spec.minex);
  assert_eq!(spec.max, u64::MAX);
  assert!(!spec.maxex);

  let spec = parse_range_spec("(10", "[100").unwrap();
  assert_eq!(spec.min, 10);
  assert!(spec.minex);
  assert_eq!(spec.max, 100);
  assert!(!spec.maxex);

  let spec = parse_range_spec("20", "(200").unwrap();
  assert_eq!(spec.min, 20);
  assert!(!spec.minex);
  assert_eq!(spec.max, 200);
  assert!(spec.maxex);

  assert!(parse_range_spec("+inf", "100").is_err());
  assert!(parse_range_spec("100", "-inf").is_err());
  assert!(parse_range_spec("invalid", "100").is_err());
}

#[test]
fn test_sorted_int_set_in_memory() {
  let mut s = SortedIntSet::new();
  assert_eq!(s.card(), 0);
  assert!(s.add(10));
  assert!(s.add(20));
  assert!(s.add(30));
  assert!(!s.add(20));
  assert_eq!(s.card(), 3);
  assert!(s.exists(20));
  assert!(!s.exists(40));

  assert_eq!(s.range(0, 10), vec![10, 20, 30]);
  assert_eq!(s.range(10, 10), vec![20, 30]);
  assert_eq!(s.rev_range(0, 10), vec![30, 20, 10]);
  assert_eq!(s.rev_range(30, 10), vec![20, 10]);

  assert!(s.remove(20));
  assert_eq!(s.card(), 2);
  assert_eq!(s.range(0, 10), vec![10, 30]);
}
