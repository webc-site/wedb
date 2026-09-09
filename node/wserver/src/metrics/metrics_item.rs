/// 信息指标项（一行）：指标名 + 取值。
///（对标 libs/common/Metrics/MetricsItem.cs:MetricsItem）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsItem {
  /// 指标名；多行复合值场景下可为空串（对齐 C# 空名约定）。
  pub name: String,
  /// 指标值。
  pub value: String,
}

impl MetricsItem {
  /// libs/common/Metrics/MetricsItem.cs:MetricsItem（构造）。
  pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: value.into(),
    }
  }
}
