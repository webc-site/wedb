//! CI 侧报表工具：合并单列结果、重渲平台表、累积历史。
//!
//! 合并与渲染的逻辑在 `wedb_bench::report`（有单元测试守着「JSON 往返无损」这条承诺），
//! 这个二进制只做参数解析与文件读写。

use std::{
  env::args,
  fs::{create_dir_all, read_to_string, write},
  mem::take,
  path::{Path, PathBuf},
  process::exit,
};

use wedb_bench::{
  json::{JSON_SCHEMA, JsonHistory, JsonReport, JsonRun},
  report::{fill_missing, merge_history, merge_platforms, render_report, render_run},
};

const USAGE: &str = "用法：benchreport <命令> [选项]

  merge 合并单列结果为平台表
    benchreport merge [选项] <run.json>...
      --out FILE         写出合并后的 report JSON
      --markdown FILE    写出 markdown 表（供 CI 汇总）
      --expect 列名,来源  预期列；没产出结果的补成整列 N/A（可重复）
  table 从 report JSON 重渲平台表
    benchreport table [--platform NAME] <report.json>
  history 把本次 report 追加进历史（同 commit 同平台覆盖，保留最近 N 次）
    benchreport history --out FILE [--keep N] <history.json> <report.json>
";

fn main() {
  if let Err(detail) = run() {
    eprintln!("benchreport: {detail}");
    exit(1);
  }
}

fn run() -> Result<(), String> {
  let argv: Vec<String> = args().skip(1).collect();
  let Some(command) = argv.first() else {
    return Err(USAGE.to_string());
  };
  let rest = &argv[1..];
  match command.as_str() {
    "merge" => merge(rest),
    "table" => render(rest),
    "history" => accumulate(rest),
    "--help" | "-h" => {
      println!("{USAGE}");
      Ok(())
    }
    other => Err(format!("未知命令：{other}\n{USAGE}")),
  }
}

fn flag_value(argv: &[String], index: &mut usize, name: &str) -> Result<String, String> {
  *index += 1;
  argv
    .get(*index)
    .cloned()
    .ok_or_else(|| format!("{name} 缺少取值"))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
  let text = read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
  serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn write_file(path: &Path, text: String) -> Result<(), String> {
  if let Some(parent) = path.parent() {
    let _ = create_dir_all(parent);
  }
  write(path, text).map_err(|e| format!("{}: {e}", path.display()))
}

fn write_json<T: serde::Serialize>(value: &T, path: &Path) -> Result<(), String> {
  let text = serde_json::to_string_pretty(value).map_err(|e| format!("序列化失败：{e}"))?;
  write_file(path, text)
}

/// 历史文件是站点的静态资源，单行紧凑写出：体积小一个量级，人不直接读它
fn write_compact<T: serde::Serialize>(value: &T, path: &Path) -> Result<(), String> {
  let text = serde_json::to_string(value).map_err(|e| format!("序列化失败：{e}"))?;
  write_file(path, text)
}

fn write_text(text: &str, path: &Path) -> Result<(), String> {
  write_file(path, text.to_string())
}

fn merge(argv: &[String]) -> Result<(), String> {
  let mut out: Option<PathBuf> = None;
  let mut markdown: Option<PathBuf> = None;
  let mut expected: Vec<(String, String)> = Vec::new();
  let mut files: Vec<PathBuf> = Vec::new();
  let mut index = 0usize;
  while index < argv.len() {
    match argv[index].as_str() {
      "--out" => out = Some(PathBuf::from(flag_value(argv, &mut index, "--out")?)),
      "--markdown" => markdown = Some(PathBuf::from(flag_value(argv, &mut index, "--markdown")?)),
      "--expect" => {
        let value = flag_value(argv, &mut index, "--expect")?;
        let (name, source) = value
          .split_once(',')
          .ok_or_else(|| format!("--expect 需要 列名,来源：{value}"))?;
        expected.push((name.to_string(), source.to_string()));
      }
      other => files.push(PathBuf::from(other)),
    }
    index += 1;
  }
  if files.is_empty() {
    return Err(format!("merge 需要至少一个 run JSON\n{USAGE}"));
  }

  let runs = files
    .iter()
    .map(|file| read_json::<JsonRun>(file))
    .collect::<Result<Vec<_>, _>>()?;
  let mut merged = merge_platforms(runs)?;
  for run in &mut merged {
    fill_missing(run, &expected);
  }
  let report = JsonReport::new(merged);

  let text = render_report(&report);
  print!("{text}");
  if let Some(path) = &out {
    write_json(&report, path)?;
    println!("合并报表：{}", path.display());
  }
  if let Some(path) = &markdown {
    write_text(&text, path)?;
  }
  Ok(())
}

fn render(argv: &[String]) -> Result<(), String> {
  let mut platform: Option<String> = None;
  let mut file: Option<PathBuf> = None;
  let mut index = 0usize;
  while index < argv.len() {
    match argv[index].as_str() {
      "--platform" => platform = Some(flag_value(argv, &mut index, "--platform")?),
      other if file.is_some() => return Err(format!("table 只接受一个 report JSON：{other}")),
      other => file = Some(PathBuf::from(other)),
    }
    index += 1;
  }
  let path = file.ok_or_else(|| format!("table 缺少 report JSON\n{USAGE}"))?;
  let report: JsonReport = read_json(&path)?;

  match platform {
    Some(name) => {
      let run = report
        .runs
        .iter()
        .find(|run| run.platform == name)
        .ok_or_else(|| format!("报表里没有平台 {name}"))?;
      print!("{}", render_run(run));
    }
    None => print!("{}", render_report(&report)),
  }
  Ok(())
}

fn accumulate(argv: &[String]) -> Result<(), String> {
  let mut out: Option<PathBuf> = None;
  let mut keep = 60usize;
  let mut files: Vec<PathBuf> = Vec::new();
  let mut index = 0usize;
  while index < argv.len() {
    match argv[index].as_str() {
      "--out" => out = Some(PathBuf::from(flag_value(argv, &mut index, "--out")?)),
      "--keep" => {
        let text = flag_value(argv, &mut index, "--keep")?;
        keep = text
          .parse::<usize>()
          .map_err(|_| format!("--keep 取值无效：{text}"))?;
      }
      other => files.push(PathBuf::from(other)),
    }
    index += 1;
  }
  if files.len() != 2 {
    return Err(format!(
      "history 需要 <history.json> <report.json> 两个入参\n{USAGE}"
    ));
  }
  let out = out.ok_or_else(|| format!("history 缺少 --out\n{USAGE}"))?;

  // 首次累积没有历史文件，空表起步
  let mut history = if files[0].exists() {
    read_json::<JsonHistory>(&files[0])?
  } else {
    JsonHistory::empty()
  };
  if history.schema != JSON_SCHEMA {
    return Err(format!(
      "历史文件 schema {} 与当前 {JSON_SCHEMA} 不兼容",
      history.schema
    ));
  }
  let report: JsonReport = read_json(&files[1])?;

  let (reports, replaced) = merge_history(take(&mut history.reports), report);
  history.reports = reports;
  if keep > 0 && history.reports.len() > keep {
    let cut = history.reports.len() - keep;
    history.reports.drain(0..cut);
  }
  history.schema = JSON_SCHEMA;
  history.generated_at_unix = JsonRun::unix_now();
  write_compact(&history, &out)?;
  println!(
    "历史累积：{} 次报表（覆盖同 commit 同平台 {replaced} 格）→ {}",
    history.reports.len(),
    out.display()
  );
  Ok(())
}
