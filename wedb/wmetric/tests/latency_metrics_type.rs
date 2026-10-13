#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wmetric::LatencyMetricsType;

#[test]
fn parse_all_types_case_insensitive() {
  for t in LatencyMetricsType::ALL {
    assert_eq!(
      LatencyMetricsType::from_name(t.cs_name().as_bytes()),
      Some(t)
    );
    assert_eq!(
      LatencyMetricsType::from_name(t.cs_name().to_ascii_lowercase().as_bytes()),
      Some(t)
    );
  }
  assert_eq!(LatencyMetricsType::from_name(b"invalid"), None);
}
