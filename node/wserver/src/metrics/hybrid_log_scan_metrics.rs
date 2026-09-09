use gxhash::HashMap;

/// 混合日志扫描的区域/状态分布统计
///（对标 libs/server/Metrics/HybridLogScanMetrics.cs:HybridLogScanMetrics）。
///
/// C# 用 `Dictionary<string, Dictionary<string, (long count, long size)>>`；
/// 输出转储要求稳定的区域顺序，故区域层以 `Vec` 保序 + 哈希索引，
/// 状态层同构（每区域状态数有限，线性查找开销可忽略）。
#[derive(Default)]
pub struct HybridLogScanMetrics {
  /// 区域 → (状态 → (条数, 字节数))；区域按首次插入顺序排列。
  scan_metrics: Vec<RegionMetrics>,
}

/// 单区域统计：状态聚合 + 哈希索引（状态名 → 下标）。
#[derive(Default)]
struct RegionMetrics {
  region: String,
  states: Vec<(String, (i64, i64))>,
  index: HashMap<String, usize>,
}

impl HybridLogScanMetrics {
  /// libs/server/Metrics/HybridLogScanMetrics.cs:AddScanMetric
  ///
  /// 记录一次扫描命中：区域 `region` 下状态 `state` 的条数加一、字节数累加。
  pub fn add_scan_metric(&mut self, region: &str, state: &str, size: i64) {
    let region_idx = match self.scan_metrics.iter().position(|r| r.region == region) {
      Some(idx) => idx,
      None => {
        self.scan_metrics.push(RegionMetrics {
          region: region.into(),
          states: Vec::new(),
          index: HashMap::with_hasher(gxhash::GxBuildHasher::default()),
        });
        self.scan_metrics.len() - 1
      }
    };
    let region_metrics = &mut self.scan_metrics[region_idx];
    match region_metrics.index.get(state).copied() {
      Some(state_idx) => {
        let entry = &mut region_metrics.states[state_idx].1;
        entry.0 += 1;
        entry.1 += size;
      }
      None => {
        region_metrics
          .index
          .insert(state.into(), region_metrics.states.len());
        region_metrics.states.push((state.into(), (1, size)));
      }
    }
  }

  /// libs/server/Metrics/HybridLogScanMetrics.cs:DumpScanMetricsInfo
  ///
  /// 转储为 INFO 多行文本；空统计返回空串（对齐 C# 仅输出起始换行前的空内容）。
  pub fn dump_scan_metrics_info(&self) -> String {
    if self.scan_metrics.is_empty() {
      return String::new();
    }
    let mut out = String::from("\n");
    for region in &self.scan_metrics {
      out.push_str("# Region: ");
      out.push_str(&region.region);
      out.push('\n');
      for (state, (count, size)) in &region.states {
        out.push_str(&format!("  State: {state}, Count: {count}, Size: {size}\n"));
      }
    }
    out
  }
}

#[cfg(test)]
mod tests {
  use super::HybridLogScanMetrics;

  #[test]
  fn aggregate_and_dump() {
    let mut m = HybridLogScanMetrics::default();
    m.add_scan_metric("Mutable", "Inline", 64);
    m.add_scan_metric("Mutable", "Inline", 32);
    m.add_scan_metric("Mutable", "OverflowBucket", 128);
    m.add_scan_metric("ReadCache", "Inline", 16);

    let dump = m.dump_scan_metrics_info();
    assert!(dump.contains("# Region: Mutable\n"));
    assert!(dump.contains("  State: Inline, Count: 2, Size: 96\n"));
    assert!(dump.contains("  State: OverflowBucket, Count: 1, Size: 128\n"));
    assert!(dump.contains("# Region: ReadCache\n"));
    // 区域保持首次插入顺序。
    assert!(dump.find("Mutable").unwrap() < dump.find("ReadCache").unwrap());
  }

  #[test]
  fn empty_dump_is_empty() {
    assert_eq!(HybridLogScanMetrics::default().dump_scan_metrics_info(), "");
  }
}
