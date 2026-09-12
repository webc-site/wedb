//! 信息指标模型与 INFO 序列化器（对标 libs/common/Metrics/MetricsItem.cs）

/// 信息指标项：指标名 + 取值
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsItem {
  /// 指标名
  pub name: String,
  /// 指标值
  pub value: String,
}

impl MetricsItem {
  /// 创建指标项
  #[inline]
  pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: value.into(),
    }
  }
}

/// 追加格式化单个段为 Redis INFO 协议响应格式
///
/// 结构：`# <header>\r\nkey:value\r\n`；
/// 单次预估容量，零额外 `format!` 堆分配，直接追加写入 `out`。
#[inline]
pub fn format_info_section(section_header: &str, items: Option<&[MetricsItem]>, out: &mut String) {
  let header_len = 2 + section_header.len() + 2;
  let items_len = match items {
    Some(items) => items
      .iter()
      .map(|it| it.name.len() + it.value.len() + 3)
      .sum::<usize>(),
    None => 0,
  };
  out.reserve(header_len + items_len);

  out.push_str("# ");
  out.push_str(section_header);
  out.push_str("\r\n");
  let Some(items) = items else {
    return;
  };
  if items.first().is_some_and(|item| item.name.is_empty()) {
    out.push_str(&items[0].value);
    out.push_str("\r\n");
  } else {
    for item in items {
      out.push_str(&item.name);
      out.push(':');
      out.push_str(&item.value);
      out.push_str("\r\n");
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_format_info_section() {
    let mut out = String::new();
    let items = vec![
      MetricsItem::new("version", "1.0.0"),
      MetricsItem::new("uptime", "3600"),
    ];
    format_info_section("Server", Some(&items), &mut out);
    assert_eq!(out, "# Server\r\nversion:1.0.0\r\nuptime:3600\r\n");

    let mut out_empty = String::new();
    format_info_section("Empty", None, &mut out_empty);
    assert_eq!(out_empty, "# Empty\r\n");
  }
}
