#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wresp::metrics::{InfoMetricsType, fmt_n2};

/// 非负有限值：wconn 与 wmetric 两侧原有用例并入此单点（两侧不再各留一份）
#[test]
fn n2_formatting() {
  assert_eq!(fmt_n2(0.0), "0.00");
  assert_eq!(fmt_n2(1.5), "1.50");
  assert_eq!(fmt_n2(12.5), "12.50");
  assert_eq!(fmt_n2(987.654), "987.65");
  assert_eq!(fmt_n2(1234.05), "1,234.05");
  assert_eq!(fmt_n2(1000.0), "1,000.00");
  assert_eq!(fmt_n2(1_234_567.89), "1,234,567.89");
  assert_eq!(fmt_n2(1_234_567.891), "1,234,567.89");
  // 四舍五入进位跨分组
  assert_eq!(fmt_n2(999.999), "1,000.00");
  // 大值分组（微秒口径上界 1e8，此为裕量验证）
  assert_eq!(fmt_n2(1e15), "1,000,000,000,000,000.00");
}

/// 符号与非有限值：锁定收敛后的单一口径
#[test]
fn n2_sign_and_non_finite() {
  assert_eq!(fmt_n2(-1234.56), "-1,234.56");
  assert_eq!(fmt_n2(-0.0), "-0.00");
  // 可精确表示的中点：远离零进位
  assert_eq!(fmt_n2(0.125), "0.13");
  assert_eq!(fmt_n2(-0.125), "-0.13");
  // 十进制中点（double 值 1.00499999999999989…）按本体舍入，不做 epsilon 推挤
  assert_eq!(fmt_n2(1.005), "1.00");
  assert_eq!(fmt_n2(f64::NAN), "NaN");
  assert_eq!(fmt_n2(f64::INFINITY), "Infinity");
  assert_eq!(fmt_n2(f64::NEG_INFINITY), "-Infinity");
}

/// 段名解析（大小写不敏感 + STATISTICS 别名 + 未知段回 None）
#[test]
fn from_name_matches_cs_section_names() {
  assert_eq!(
    InfoMetricsType::from_name(b"server"),
    Some(InfoMetricsType::Server)
  );
  assert_eq!(
    InfoMetricsType::from_name(b"KEYSPACE"),
    Some(InfoMetricsType::Keyspace)
  );
  assert_eq!(
    InfoMetricsType::from_name(b"statistics"),
    Some(InfoMetricsType::Stats)
  );
  for t in InfoMetricsType::ALL {
    assert_eq!(
      InfoMetricsType::from_name(t.as_cs_name().as_bytes()),
      Some(t)
    );
  }
  assert_eq!(InfoMetricsType::from_name(b"nope"), None);
}
