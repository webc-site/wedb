use wnode::servers::server_options::ServerOptions;

/// test/standalone/Garnet.test/GarnetServerConfigTests.cs:DefaultConfigurationOptionsCoverage
#[test]
fn default_configuration_options_coverage() {
  assert_eq!(ServerOptions::parse_size("1024"), (1024, 4));
  assert_eq!(ServerOptions::parse_size("4k"), (4 * 1024, 2));
  assert_eq!(ServerOptions::parse_size("4kb"), (4 * 1024, 3));
  assert_eq!(ServerOptions::parse_size("16m"), (16 * 1024 * 1024, 3));
  assert_eq!(
    ServerOptions::parse_size("2g"),
    (2i64 * 1024 * 1024 * 1024, 2)
  );
  assert_eq!(ServerOptions::parse_size("1t"), (1i64 << 40, 2));
  assert_eq!(ServerOptions::parse_size(""), (0, 0));
  assert_eq!(ServerOptions::try_parse_size("16m"), Some(16 * 1024 * 1024));
  assert_eq!(
    ServerOptions::try_parse_size("16mb"),
    Some(16 * 1024 * 1024)
  );
  assert_eq!(ServerOptions::try_parse_size("16mx"), None);

  assert_eq!(ServerOptions::previous_power_of_2(1000), 512);
  assert_eq!(ServerOptions::previous_power_of_2(1024), 1024);
  assert_eq!(ServerOptions::next_power_of_2(1000), 1024);
  assert_eq!(ServerOptions::next_power_of_2(1024), 1024);

  let mut options = ServerOptions::new();
  assert_eq!(options.pub_sub_page_size_bytes(), 4096);
  options.pub_sub_page_size = "3k".into();
  assert_eq!(options.pub_sub_page_size_bytes(), 2048);

  let options = ServerOptions::new();
  assert_eq!(
    options
      .index_size_cachelines("IndexMemorySize", "128m")
      .expect("合法索引"),
    128 * 1024 * 1024 / 64
  );
  assert!(options.index_size_cachelines("idx", "32").is_err());
  assert!(options.index_size_cachelines("idx", "64g").is_ok());

  assert_eq!(ServerOptions::pretty_size(1024), "1k");
  assert_eq!(ServerOptions::pretty_size(1536), "1.5k");
  assert_eq!(ServerOptions::pretty_size(500), "500");
  assert_eq!(ServerOptions::pretty_size(4 * 1024 * 1024 * 1024), "4g");
  assert_eq!(ServerOptions::pretty_size(i64::MAX), "8");
}

/// test/standalone/Garnet.test/GarnetServerConfigTests.cs:MinimumPageSize
#[test]
fn minimum_page_size() {
  let mut options = ServerOptions::new();
  assert_eq!(options.memory_size_bits(), 34);
  assert_eq!(options.page_size_bits().expect("默认页合法"), 24);
  options.log_memory_size = "10g".into();
  assert_eq!(options.memory_size_bits(), 33);

  let options = ServerOptions::new();
  assert!(options.validated_page_size_bits("4k", "PageSize").is_ok());
  let too_small = options.validated_page_size_bits("64", "PageSize");
  assert!(too_small.is_err());
  let mut relaxed = ServerOptions::new();
  relaxed.min_page_size_bytes = 16;
  assert!(relaxed.validated_page_size_bits("64", "PageSize").is_ok());
}

/// test/standalone/Garnet.test/GarnetServerConfigTests.cs:AofSegmentSizeFlowsToTsavoriteLogSettings
#[test]
fn segment_size_bits_selects_log() {
  let mut options = ServerOptions::new();
  assert_eq!(options.segment_size_bits(false), 30);
  assert_eq!(options.segment_size_bits(true), 30);
  options.object_log_segment_size = "512m".into();
  assert_eq!(options.segment_size_bits(true), 29);
}
