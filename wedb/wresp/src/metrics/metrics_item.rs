//! 信息指标项与 INFO 段序列化模型（对标 libs/common/Metrics/MetricsItem.cs）
//!
//! C# 为 readonly struct（Name/Value 均 string），客户端延迟百分位
//!（libs/client/GarnetClientMetrics.cs）与服务端 INFO 段
//!（libs/server/Metrics/Info/GarnetInfoMetrics.cs）共用同一形态，故与
//! `InfoMetricsType` 同置 client/server 共引的协议层单点。
//! rust 以 `Cow<'static, str>` 承接 C# 的字符串字段语义：字面量名零分配借用，
//! 运行期拼名（如 `slave0`、`MainStore_HLog_1`）落 Owned。

use std::borrow::Cow;

use itoa::Buffer;

/// 信息指标项：指标名 + 取值
///（libs/common/Metrics/MetricsItem.cs:MetricsItem）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsItem {
  /// 指标名
  pub name: Cow<'static, str>,
  /// 指标值
  pub value: String,
}

impl MetricsItem {
  /// 创建指标项（libs/common/Metrics/MetricsItem.cs:MetricsItem 构造）
  #[inline]
  pub fn new(name: impl Into<Cow<'static, str>>, value: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: value.into(),
    }
  }

  /// 基于 itoa 格式化 i64 指标值
  #[inline]
  pub fn from_i64(name: impl Into<Cow<'static, str>>, val: i64) -> Self {
    let mut buf = Buffer::new();
    Self::new(name, buf.format(val))
  }

  /// 基于 itoa 格式化 usize 指标值
  #[inline]
  pub fn from_usize(name: impl Into<Cow<'static, str>>, val: usize) -> Self {
    let mut buf = Buffer::new();
    Self::new(name, buf.format(val))
  }
}
