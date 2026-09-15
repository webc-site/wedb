use std::{fs, path::Path, time::Duration};

use crate::{
  error::Result,
  i18n::I18nTexts,
  sys_info::MachineInfo,
  types::{BenchmarkConfig, ResultType},
};

pub type EngineBenchResult = (&'static str, &'static str, Vec<(String, ResultType)>);

/// 生成渲染 Markdown 表格及硬件报告
pub fn generate_markdown_report(
  info: &MachineInfo,
  cfg: &BenchmarkConfig,
  i18n: &I18nTexts,
  results: &[EngineBenchResult],
) -> String {
  let mut md = String::with_capacity(4096);

  // 1. 标题 (直接突出标题，无冗余描述)
  md.push_str(&format!("# {}\n\n", i18n.title));

  // 2. 性能评测结果表格（置顶突出）
  if !results.is_empty() {
    let first_result = &results[0].2;

    md.push_str(&format!("| {} |", i18n.metric_col));
    for (name, url, _) in results {
      md.push_str(&format!(" [{name}]({url}) |"));
    }
    md.push_str("\n|:---|");
    for _ in results {
      md.push_str("---:|");
    }
    md.push('\n');

    for (metric_idx, (metric_key, _)) in first_result.iter().enumerate() {
      let mut best_idx: Option<usize> = None;
      let mut min_rate = f64::INFINITY;
      let mut max_duration: Option<Duration> = None;

      for (idx, (_, _, r)) in results.iter().enumerate() {
        let res = &r[metric_idx].1;
        if !matches!(res, ResultType::NA) {
          if best_idx.is_none_or(|b| res.is_better_than(&results[b].2[metric_idx].1)) {
            best_idx = Some(idx);
          }
          match res {
            ResultType::Throughput { .. } => {
              let rate = res.rate();
              if rate > 0.0 && rate < min_rate {
                min_rate = rate;
              }
            }
            ResultType::Latency(d)
              if !d.is_zero() && max_duration.is_none_or(|max_d| *d > max_d) =>
            {
              max_duration = Some(*d);
            }
            _ => {}
          }
        }
      }

      let localized_metric = i18n.metric_name(metric_key);
      md.push_str(&format!("| {localized_metric} |"));

      for (idx, (_, _, r)) in results.iter().enumerate() {
        let res = &r[metric_idx].1;
        if matches!(res, ResultType::NA) {
          md.push_str(" N/A |");
          continue;
        }

        let is_best = best_idx == Some(idx);

        match res {
          ResultType::Throughput { .. } => {
            let tp_str = res.format_value();
            let mult_str = if min_rate.is_finite() && min_rate > 0.0 {
              let mult = res.rate() / min_rate;
              format!("{mult:.2}X")
            } else {
              "1.00X".to_string()
            };

            // 倍数和吞吐之间用 <br> 换行
            if is_best {
              md.push_str(&format!(" **{mult_str}**<br>**{tp_str}** |"));
            } else {
              md.push_str(&format!(" {mult_str}<br>{tp_str} |"));
            }
          }
          ResultType::Latency(d) => {
            let lat_str = res.format_value();
            let mult_str = if let Some(max_d) = max_duration {
              let mult = max_d.as_secs_f64() / d.as_secs_f64().max(1e-6);
              format!("{mult:.2}X")
            } else {
              "1.00X".to_string()
            };

            if is_best {
              md.push_str(&format!(" **{mult_str}**<br>**{lat_str}** |"));
            } else {
              md.push_str(&format!(" {mult_str}<br>{lat_str} |"));
            }
          }
          _ => {
            let formatted = res.format_value();
            if is_best {
              md.push_str(&format!(" **{formatted}** |"));
            } else {
              md.push_str(&format!(" {formatted} |"));
            }
          }
        }
      }
      md.push('\n');
    }

    md.push_str(&format!("\n> {}\n\n", i18n.notes));
  }

  // 3. 评测配置 (测试用的 key 大小等信息，放置在表格下方)
  md.push_str(&format!("## {}\n\n", i18n.params_title));
  md.push_str(&format!(
    "| {} | {} |\n|:---|:---|\n",
    i18n.param_col, i18n.val_col
  ));
  md.push_str(&format!("| **{}** | {} B |\n", i18n.key_size, cfg.key_size));
  md.push_str(&format!(
    "| **{}** | {} B |\n",
    i18n.value_size, cfg.value_size
  ));
  md.push_str(&format!(
    "| **{}** | {} MiB |\n",
    i18n.cache_size,
    cfg.cache_size / (1024 * 1024)
  ));
  md.push_str(&format!(
    "| **{}** | {} |\n\n",
    i18n.elements, cfg.bulk_elements
  ));

  // 4. 测试环境（置底）
  md.push_str(&format!("## {}\n\n", i18n.system_info_title));
  md.push_str(&format!(
    "| {} | {} |\n|:---|:---|\n",
    i18n.hardware_col, i18n.spec_col
  ));
  md.push_str(&format!("| **{}** | {} |\n", i18n.cpu, info.cpu_brand));
  md.push_str(&format!(
    "| **{}** | {} |\n",
    i18n.cores,
    i18n.format_cores(info.physical_cores, info.logical_cores)
  ));
  md.push_str(&format!("| **{}** | {} |\n", i18n.arch, info.arch));
  md.push_str(&format!(
    "| **{}** | {:.2} GiB |\n",
    i18n.memory, info.total_memory_gib
  ));
  md.push_str(&format!(
    "| **{}** | {} |\n",
    i18n.disk_type, info.disk_type
  ));
  md.push_str(&format!("| **{}** | {} |\n", i18n.os, info.os_info));
  md.push_str(&format!(
    "| **{}** | {} |\n\n",
    i18n.kernel, info.kernel_version
  ));

  md
}

/// 将 Markdown 写入到文件
pub fn write_report(path: impl AsRef<Path>, content: &str) -> Result<()> {
  if let Some(parent) = path.as_ref().parent() {
    fs::create_dir_all(parent)?;
  }
  fs::write(path, content)?;
  Ok(())
}

/// 在控制台打印 Markdown 表格
pub fn print_console_table(i18n: &I18nTexts, results: &[EngineBenchResult]) {
  if results.is_empty() {
    return;
  }
  let first_result = &results[0].2;

  println!();
  print!("| {:<30} |", i18n.metric_col);
  for (name, ..) in results {
    print!(" {:<20} |", name);
  }
  println!();
  print!("|:{:-<29}-|", "");
  for _ in results {
    print!(":{:-<19}-|", "");
  }
  println!();

  for (metric_idx, (metric_key, _)) in first_result.iter().enumerate() {
    let mut best_idx: Option<usize> = None;
    let mut min_rate = f64::INFINITY;
    let mut max_duration: Option<Duration> = None;

    for (idx, (_, _, r)) in results.iter().enumerate() {
      let res = &r[metric_idx].1;
      if !matches!(res, ResultType::NA) {
        if best_idx.is_none_or(|b| res.is_better_than(&results[b].2[metric_idx].1)) {
          best_idx = Some(idx);
        }
        match res {
          ResultType::Throughput { .. } => {
            let rate = res.rate();
            if rate > 0.0 && rate < min_rate {
              min_rate = rate;
            }
          }
          ResultType::Latency(d) if !d.is_zero() && max_duration.is_none_or(|max_d| *d > max_d) => {
            max_duration = Some(*d);
          }
          _ => {}
        }
      }
    }

    let localized_metric = i18n.metric_name(metric_key);

    print!("| {:<30} |", localized_metric);
    for (idx, (_, _, r)) in results.iter().enumerate() {
      let res = &r[metric_idx].1;
      if matches!(res, ResultType::NA) {
        print!(" {:<20} |", "N/A");
        continue;
      }

      let is_best = best_idx == Some(idx);

      let text = match res {
        ResultType::Throughput { .. } => {
          let tp_str = res.format_value();
          let mult_str = if min_rate.is_finite() && min_rate > 0.0 {
            let mult = res.rate() / min_rate;
            format!("{mult:.2}X")
          } else {
            "1.00X".to_string()
          };
          format!("{mult_str} ({tp_str})")
        }
        ResultType::Latency(d) => {
          let lat_str = res.format_value();
          let mult_str = if let Some(max_d) = max_duration {
            let mult = max_d.as_secs_f64() / d.as_secs_f64().max(1e-6);
            format!("{mult:.2}X")
          } else {
            "1.00X".to_string()
          };
          format!("{mult_str} ({lat_str})")
        }
        _ => res.format_value(),
      };

      if is_best {
        print!(" {:<20} |", format!("**{text}**"));
      } else {
        print!(" {:<20} |", text);
      }
    }
    println!();
  }
  println!();
}
