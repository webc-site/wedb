//! CI 侧的报表合并与渲染：把散在各 runner 的「单平台单列」结果并成平台表，并累积历史。
//!
//! 表格渲染全部走 `table::results_table_markdown`，与单进程直出的表同源；
//! 网站只消费 JSON，不在 JS 里重抄一遍格式化规则。

use std::collections::BTreeMap;

use crate::{
  harness::na_rows,
  json::{JsonEngine, JsonReport, JsonRow, JsonRun},
  table::results_table_markdown,
};

/// 表的列序：自家引擎在最左，对照引擎按 redb 惯例排在后
pub const ENGINE_ORDER: &[&str] = &["hash", "bftree", "fjall", "rocksdb", "sqlite"];

/// 列序键：未知列名按字母序排在已知列之后
pub fn engine_rank(name: &str) -> (u8, String) {
  match ENGINE_ORDER.iter().position(|known| *known == name) {
    Some(index) => (0, format!("{index:02}")),
    None => (1, name.to_string()),
  }
}

fn engine_names(engines: &[JsonEngine]) -> String {
  engines
    .iter()
    .map(|engine| engine.name.as_str())
    .collect::<Vec<_>>()
    .join(", ")
}

/// 机器口径是否与原列一致：跨 runner 合并时只有这三项会影响可比性
fn same_machine(a: &JsonRun, b: &JsonRun) -> bool {
  a.machine.cpu_brand == b.machine.cpu_brand
    && a.machine.logical_cores == b.machine.logical_cores
    && a.machine.total_memory_gib == b.machine.total_memory_gib
    && a.workload.cache_size == b.workload.cache_size
}

/// 各平台单列结果并成「一平台一表」；平台按名字典序输出，报表形态与入参顺序无关
pub fn merge_platforms(runs: Vec<JsonRun>) -> Result<Vec<JsonRun>, String> {
  let mut grouped: BTreeMap<String, Vec<JsonRun>> = BTreeMap::new();
  for run in runs {
    if run.engines.is_empty() {
      return Err(format!("平台 {} 没有任何引擎列", run.platform));
    }
    grouped.entry(run.platform.clone()).or_default().push(run);
  }
  grouped
    .into_values()
    .map(merge_platform)
    .collect::<Result<Vec<_>, _>>()
}

/// 同平台的多个单列 run 并成一张表：元数据取首个 run，其余 run 的口径差异写进备注
pub fn merge_platform(mut group: Vec<JsonRun>) -> Result<JsonRun, String> {
  group.sort_by_key(|run| {
    (
      run
        .engines
        .iter()
        .map(|engine| engine_rank(&engine.name))
        .min()
        .unwrap_or((2, String::new())),
      run.generated_at_unix,
    )
  });
  let mut base = group.remove(0);

  for run in &mut group {
    if run.commit != base.commit {
      base.notes.push(format!(
        "列 {} 采自提交 {}，与主列 {} 不同",
        engine_names(&run.engines),
        run.commit,
        base.commit
      ));
    }
    if !same_machine(run, &base) {
      base.notes.push(format!(
        "列 {} 采于 {}（{} 核 / {:.0} GiB / 缓存 {:.2} GiB），机器口径与首列不同",
        engine_names(&run.engines),
        run.machine.cpu_brand,
        run.machine.logical_cores,
        run.machine.total_memory_gib,
        run.workload.cache_size as f64 / (1024.0 * 1024.0 * 1024.0)
      ));
    }
    base.engines.append(&mut run.engines);
  }

  let mut seen: Vec<String> = Vec::new();
  base.engines.retain(|engine| {
    let duplicate = seen.contains(&engine.name);
    if !duplicate {
      seen.push(engine.name.clone());
    }
    !duplicate
  });
  base
    .engines
    .sort_by_key(|engine| (engine_rank(&engine.name), engine.name.clone()));

  let keys: Vec<&str> = base.engines[0]
    .rows
    .iter()
    .map(|row| row.key.as_str())
    .collect();
  for engine in &base.engines[1..] {
    let mismatched = engine.rows.len() != keys.len()
      || engine
        .rows
        .iter()
        .zip(&keys)
        .any(|(row, key)| row.key != *key);
    if mismatched {
      return Err(format!(
        "列 {} 的行序与首列不一致，可能是各 job 的二进制版本不同",
        engine.name
      ));
    }
  }

  base.mark_winners();
  Ok(base)
}

/// 缺失单元格补成整列 N/A：某个引擎 job 构建失败、超时或被跳过时，
/// 平台表仍然出满预期的列，读者一眼看出哪格空了
pub fn fill_missing(run: &mut JsonRun, expected: &[(String, String)]) {
  for (name, source) in expected {
    if run.engines.iter().any(|engine| engine.name == *name) {
      continue;
    }
    run.notes.push(format!(
      "列 {name} 未产出结果（该平台的 job 失败或被跳过），整列记 N/A"
    ));
    run.engines.push(JsonEngine {
      name: name.clone(),
      source: source.clone(),
      status: "skipped".to_string(),
      detail: Some("未取得该机读结果文件".to_string()),
      peak_memory_bytes: None,
      rows: na_rows(&run.workload)
        .iter()
        .map(|(row_name, result)| JsonRow::from_result(row_name, result))
        .collect(),
    });
  }
  run
    .engines
    .sort_by_key(|engine| (engine_rank(&engine.name), engine.name.clone()));
}

/// 按 report 里的平台顺序逐张渲染表格
pub fn render_report(report: &JsonReport) -> String {
  report
    .runs
    .iter()
    .map(render_run)
    .collect::<Vec<_>>()
    .join("")
}

pub fn render_run(run: &JsonRun) -> String {
  let mut text = format!(
    "## {} — {}（{} 核 / {:.1} GiB）\n\n",
    run.platform, run.machine.cpu_brand, run.machine.logical_cores, run.machine.total_memory_gib
  );
  text.push_str(&results_table_markdown(&run.to_table_results()));
  text.push('\n');
  for engine in &run.engines {
    if engine.status != "ok" {
      text.push_str(&format!(
        "- 列 {}：{} — {}\n",
        engine.name,
        engine.status,
        engine.detail.clone().unwrap_or_default()
      ));
    }
  }
  for note in &run.notes {
    text.push_str(&format!("- 备注：{note}\n"));
  }
  text.push('\n');
  text
}

/// 历史按 `(commit, platform)` 落位：重跑某个平台只替换那一格，
/// 同 commit 的别平台结果保留；同 commit 的各平台并回一份 report。
pub fn merge_history(existing: Vec<JsonReport>, incoming: JsonReport) -> (Vec<JsonReport>, usize) {
  let mut runs: Vec<JsonRun> = existing
    .into_iter()
    .flat_map(|report| report.runs)
    .collect();
  let mut replaced = 0usize;
  for run in incoming.runs {
    let before = runs.len();
    runs.retain(|kept| !(kept.commit == run.commit && kept.platform == run.platform));
    replaced += before - runs.len();
    runs.push(run);
  }
  // 时间升序：网站趋势图直接按 x 轴读，旧结果在前
  runs.sort_by_key(|run| run.generated_at_unix);

  let mut grouped: Vec<(String, Vec<JsonRun>)> = Vec::new();
  for run in runs {
    match grouped.iter_mut().find(|(commit, _)| *commit == run.commit) {
      Some((_, group)) => group.push(run),
      None => grouped.push((run.commit.clone(), vec![run])),
    }
  }
  (
    grouped
      .into_iter()
      .map(|(_, runs)| JsonReport::new(runs))
      .collect(),
    replaced,
  )
}

#[cfg(test)]
mod tests {
  use std::{env::temp_dir, time::Duration};

  use super::*;
  use crate::{
    config::Workload,
    harness,
    json::{JSON_SCHEMA, JsonRow, JsonRun},
    machine::MachineInfo,
    result::{ResultType, ThroughputUnit},
    table::EngineResults,
  };

  fn machine(platform: &str, cores: usize, cache_gib: f64) -> (MachineInfo, Workload) {
    let dir = temp_dir();
    let mut machine = MachineInfo::detect(&dir, &dir);
    machine.platform = platform.to_string();
    machine.logical_cores = cores;
    let mut workload = Workload::redb_standard();
    workload.cache_size = (cache_gib * 1024.0 * 1024.0 * 1024.0) as usize;
    (machine, workload)
  }

  /// 一列的结果：`(行名, 值)`，行序固定，够覆盖四种 kind
  fn sample_rows(engine: &str) -> Vec<(String, ResultType)> {
    let fast = engine == "hash";
    vec![
      (
        "bulk load".to_string(),
        ResultType::Throughput {
          count: 5_000_000,
          duration: Duration::from_millis(if fast { 100 } else { 200 }),
          unit: ThroughputUnit::Key,
        },
      ),
      (
        "individual writes".to_string(),
        ResultType::Throughput {
          count: 1_000,
          duration: Duration::from_millis(5),
          unit: ThroughputUnit::Transaction,
        },
      ),
      (
        "uncompacted size".to_string(),
        ResultType::SizeInBytes(1 << 30),
      ),
      ("pop".to_string(), ResultType::NA),
    ]
  }

  fn single_run(platform: &str, engine: &str, commit: &str, at: u64) -> JsonRun {
    let (machine, workload) = machine(platform, if engine == "hash" { 8 } else { 16 }, 4.0);
    JsonRun {
      schema: JSON_SCHEMA,
      generated_at_unix: at,
      commit: commit.to_string(),
      branch: "main".to_string(),
      platform: platform.to_string(),
      machine,
      workload,
      notes: Vec::new(),
      engines: vec![JsonEngine {
        name: engine.to_string(),
        source: "test".to_string(),
        status: "ok".to_string(),
        detail: None,
        peak_memory_bytes: Some(1 << 20),
        rows: sample_rows(engine)
          .iter()
          .map(|(name, result)| JsonRow::from_result(name, result))
          .collect(),
      }],
    }
  }

  /// 机读侧车的核心承诺：JSON 往返无损，合并后的表与运行时直出的表逐字节一致
  #[test]
  fn json_round_trip_reproduces_table() {
    let results: Vec<EngineResults> = ["hash", "fjall"]
      .iter()
      .map(|name| (name.to_string(), sample_rows(name)))
      .collect();
    let direct = results_table_markdown(&results);

    let (machine, workload) = machine("linux-x64", 8, 4.0);
    let mut run = JsonRun {
      schema: JSON_SCHEMA,
      generated_at_unix: 0,
      commit: "abc".to_string(),
      branch: "main".to_string(),
      platform: "linux-x64".to_string(),
      machine,
      workload,
      notes: Vec::new(),
      engines: results
        .iter()
        .map(|(name, rows)| JsonEngine {
          name: name.clone(),
          source: "test".to_string(),
          status: "ok".to_string(),
          detail: None,
          peak_memory_bytes: None,
          rows: rows
            .iter()
            .map(|(row_name, result)| JsonRow::from_result(row_name, result))
            .collect(),
        })
        .collect(),
    };
    run.mark_winners();

    let text = serde_json::to_string(&run).unwrap();
    let back: JsonRun = serde_json::from_str(&text).unwrap();
    assert_eq!(direct, results_table_markdown(&back.to_table_results()));
    // 并列最优（individual writes 两列同值）必须两列都标出
    assert_eq!(
      back
        .engines
        .iter()
        .filter(|engine| engine.rows[1].winner)
        .count(),
      2
    );
    // N/A 不参与最优比较，也不该被标成 winner
    assert!(back.engines.iter().all(|engine| {
      !engine
        .rows
        .iter()
        .find(|row| row.key == "pop")
        .unwrap()
        .winner
    }));
  }

  #[test]
  fn merge_orders_columns_and_marks_winners() {
    let merged = merge_platforms(vec![
      single_run("macos-arm64", "fjall", "abc", 10),
      single_run("linux-x64", "fjall", "abc", 10),
      single_run("linux-x64", "hash", "abc", 11),
    ])
    .unwrap();

    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].platform, "linux-x64");
    assert_eq!(
      merged[0]
        .engines
        .iter()
        .map(|engine| engine.name.as_str())
        .collect::<Vec<_>>(),
      vec!["hash", "fjall"]
    );
    // hash 的 bulk load 更快，只有它被加粗
    let winners: Vec<&str> = merged[0]
      .engines
      .iter()
      .filter(|engine| engine.rows[0].winner)
      .map(|engine| engine.name.as_str())
      .collect();
    assert_eq!(winners, vec!["hash"]);
    // 两列核数不同，合并时必须留下口径备注
    assert!(merged[0].notes.iter().any(|note| note.contains("机器口径")));
  }

  #[test]
  fn merge_rejects_row_order_skew() {
    let mut other = single_run("linux-x64", "fjall", "abc", 10);
    other.engines[0].rows.remove(0);
    let err = merge_platform(vec![single_run("linux-x64", "hash", "abc", 11), other]).unwrap_err();
    assert!(err.contains("行序"));
  }

  #[test]
  fn fill_missing_pads_dead_cells() {
    let mut merged = merge_platform(vec![single_run("linux-x64", "hash", "abc", 10)]).unwrap();
    fill_missing(
      &mut merged,
      &[
        ("hash".to_string(), "s1".to_string()),
        ("rocksdb".to_string(), "s2".to_string()),
      ],
    );

    assert_eq!(
      merged
        .engines
        .iter()
        .map(|engine| engine.name.as_str())
        .collect::<Vec<_>>(),
      vec!["hash", "rocksdb"]
    );
    let dead = &merged.engines[1];
    assert_eq!(dead.status, "skipped");
    assert_eq!(dead.source, "s2");
    // 补齐列按 harness 的完整行集出 N/A，段名与活列同源
    let expected = harness::na_rows(&Workload::redb_standard());
    assert_eq!(dead.rows.len(), expected.len());
    assert!(dead.rows.iter().all(|row| row.kind == "na"));
    assert!(
      merged
        .notes
        .iter()
        .any(|note| note.contains("rocksdb") && note.contains("N/A"))
    );
  }

  #[test]
  fn history_replaces_only_the_same_cell() {
    let first = JsonReport::new(vec![
      single_run("linux-x64", "hash", "aaa", 1),
      single_run("macos-arm64", "hash", "aaa", 2),
    ]);
    let incoming = JsonReport::new(vec![single_run("linux-x64", "hash", "aaa", 5)]);
    let (reports, replaced) = merge_history(vec![first], incoming);

    assert_eq!(replaced, 1);
    assert_eq!(reports.len(), 1);
    let platforms: Vec<&str> = reports[0]
      .runs
      .iter()
      .map(|run| run.platform.as_str())
      .collect();
    // 重跑 linux 不能把 macos 那一格挤掉
    assert_eq!(platforms, vec!["macos-arm64", "linux-x64"]);
  }
}
