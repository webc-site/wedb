use wconf::size::{
  MIN_PAGE_SIZE_BYTES, PageSizeError, is_flag_size_str, log2_exact, parse_size, parse_size_bytes,
  pretty_size, previous_power_of_2, try_parse_size, try_parse_size_bytes, validated_page_size_bits,
};

#[test]
fn test_parse_size() {
  assert_eq!(parse_size("1k"), (1024, 2));
  assert_eq!(parse_size("64kb"), (64 * 1024, 4));
  assert_eq!(parse_size("16m"), (16 * 1024 * 1024, 3));
  assert_eq!(parse_size("1gb"), (1024 * 1024 * 1024, 3));
  assert_eq!(try_parse_size("1gb"), Some(1024 * 1024 * 1024));
  assert_eq!(try_parse_size(""), Some(0));
  assert_eq!(try_parse_size("invalid"), None);

  // 字节切片形态与后缀
  assert_eq!(parse_size_bytes(b"32m"), (32 * 1024 * 1024, 3));
  assert_eq!(try_parse_size_bytes(b"4kb"), Some(4 * 1024));
  assert_eq!(try_parse_size_bytes(b"4kbx"), None);
  assert_eq!(try_parse_size_bytes(b"1024"), Some(1024));
  assert_eq!(try_parse_size_bytes(b"1k"), Some(1024));
  assert_eq!(try_parse_size_bytes(b"1K"), Some(1024));
  assert_eq!(try_parse_size_bytes(b"2m"), Some(2 * 1024 * 1024));
  assert_eq!(try_parse_size_bytes(b"1g"), Some(1024 * 1024 * 1024));
  assert_eq!(try_parse_size_bytes(b"1t"), Some(1024i64.pow(4)));
  assert_eq!(try_parse_size_bytes(b"1p"), Some(1024i64.pow(5)));
  assert_eq!(try_parse_size_bytes(b"1x"), None);
  assert_eq!(try_parse_size_bytes(b"1k2"), None);
  assert_eq!(try_parse_size_bytes(b""), Some(0));
}

#[test]
fn test_powers_of_2() {
  assert_eq!(previous_power_of_2(1000), 512);
  assert_eq!(previous_power_of_2(1024), 1024);
  assert_eq!(previous_power_of_2(1), 1);
  assert_eq!(previous_power_of_2(3), 2);
  // 非正值归 0（AOF 限额 / 索引桶数投影依赖的口径）
  assert_eq!(previous_power_of_2(0), 0);
  assert_eq!(previous_power_of_2(-1024), 0);
  assert_eq!(previous_power_of_2(-4096), 0);
  // i64 正域上界：MSB 位 62
  assert_eq!(previous_power_of_2(i64::MAX), 1 << 62);
  assert_eq!(log2_exact(1024), 10);
  assert_eq!(log2_exact(16 * 1024 * 1024 * 1024), 34);
  assert_eq!(log2_exact(0), 0);
  assert_eq!(log2_exact(-1), 0);
}

/// 页尺寸校验核（对标 ServerOptions.cs:ValidatedPageSizeBits）：下限判定落在
/// 取幂后的生效值上，恰界合法、跌破即点名属性名拒
#[test]
fn test_validated_page_size_bits() {
  // 恰在下限：512 合法，位宽 9；默认主存页 16m 位宽 24
  assert_eq!(validated_page_size_bits(512, "page"), Ok(9));
  assert_eq!(validated_page_size_bits(16 * 1024 * 1024, "page"), Ok(24));
  // 取幂折损但仍在下界：600 → 生效 512
  assert_eq!(validated_page_size_bits(600, "page"), Ok(9));
  // 256 页尺寸不得被静默接受
  assert_eq!(
    validated_page_size_bits(256, "page"),
    Err(PageSizeError {
      prop_name: "page",
      effective: 256
    })
  );
  // 向下取幂后跌破下限：511 本身高于生效档，取幂归 256 才判负（报错给生效值）
  assert_eq!(
    validated_page_size_bits(511, "page"),
    Err(PageSizeError {
      prop_name: "page",
      effective: 256
    })
  );
  // 文案点名属性名与下限字节
  let msg = validated_page_size_bits(0, "aof-page-size")
    .expect_err("零页容量必拒")
    .to_string();
  assert!(
    msg.contains("aof-page-size") && msg.contains(&MIN_PAGE_SIZE_BYTES.to_string()),
    "文案须点名属性名与下限字节: {msg}"
  );
}

#[test]
fn test_is_flag_size_str() {
  assert!(is_flag_size_str("128m"));
  assert!(is_flag_size_str("1g"));
  assert!(is_flag_size_str("1GB"));
  assert!(is_flag_size_str("64kb"));
  assert!(is_flag_size_str("1024"));
  assert!(is_flag_size_str("0"));

  assert!(!is_flag_size_str(""));
  assert!(!is_flag_size_str("m"));
  assert!(!is_flag_size_str("1t"));
  assert!(!is_flag_size_str("2p"));
  assert!(!is_flag_size_str("128mbx"));
  assert!(!is_flag_size_str("1m b"));
  assert!(!is_flag_size_str("invalid"));
}

#[test]
fn test_pretty_size() {
  assert_eq!(pretty_size(16 * 1024 * 1024 * 1024), "16g");
  assert_eq!(pretty_size(32 * 1024 * 1024), "32m");
  assert_eq!(pretty_size(4 * 1024), "4k");
  assert_eq!(pretty_size(512), "512");
  assert_eq!(pretty_size(1000), "0.9765625k");
}
