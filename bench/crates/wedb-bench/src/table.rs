//! 结果表呈现：与 redb-bench 的 `print_results_table` 同一套列序、单位标注与加粗规则。

use std::iter::once;

use crate::result::ResultType;

/// 一列引擎的结果：`(列名, [(行名, 值)])`
pub type EngineResults = (String, Vec<(String, ResultType)>);

/// 生成 markdown 结果表：每列一个引擎，每行一段 workload，
/// 行内最优值加粗；并列最优（在当前显示精度下渲染相同）全部加粗。
/// 速率的单位只写在行标题里，避免每格重复导致表格过宽。
pub fn results_table_markdown(results: &[EngineResults]) -> String {
  let (_, first) = results.first().expect("no results to print");

  let mut table = comfy_table::Table::new();
  // comfy-table 8：预设样式经 load_style 应用（v7 的 load_preset 已并入此处）
  table.load_style(comfy_table::presets::ASCII_MARKDOWN);
  table.set_width(100);
  let header: Vec<&str> = once("")
    .chain(results.iter().map(|(name, _)| name.as_str()))
    .collect();
  table.set_header(header);

  for (i, (name, _)) in first.iter().enumerate() {
    let row: Vec<&ResultType> = results.iter().map(|(_, r)| &r[i].1).collect();

    // 不支持该段的引擎报 N/A，单位取第一个真正跑过它的引擎
    let label = match row.iter().find_map(|result| result.unit_label()) {
      Some(unit) => format!("{name} ({unit})"),
      None => name.clone(),
    };

    // 只有一个引擎时无从比较，不标最优
    let mut best: Option<usize> = None;
    if results.len() > 1 {
      for (j, result) in row.iter().enumerate() {
        if matches!(result, ResultType::NA) {
          continue;
        }
        if best.is_none_or(|previous| result.is_better_than(row[previous])) {
          best = Some(j);
        }
      }
    }

    let best = best.map(|j| row[j].to_string());
    let mut cells = vec![label];
    cells.extend(row.iter().map(|result| {
      let value = result.to_string();
      if best.as_deref() == Some(value.as_str()) {
        format!("**{value}**")
      } else {
        value
      }
    }));
    table.add_row(cells);
  }

  format!("{table}")
}

pub fn print_results_table(results: &[EngineResults]) {
  println!();
  println!("{}", results_table_markdown(results));
}
