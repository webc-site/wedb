use std::time::Duration;

use crate::{engines::EngineBenchResult, i18n::I18nTexts, types::ResultType};

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
      if !matches!(res, ResultType::NA | ResultType::Timeout { .. }) {
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
