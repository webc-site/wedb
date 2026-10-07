//! 评测驱动：引擎注册表、子进程隔离、超时兜底与结果落盘。
//!
//! 每个引擎一个子进程：单列崩溃或超时只把那一列折成 N/A，
//! 不影响其余列出表；同时让峰值内存归属干净。

use std::{
  env,
  env::{current_exe, temp_dir},
  fs::{File, create_dir_all, read_to_string, remove_dir_all, remove_file, write},
  io::Write,
  path::{Path, PathBuf},
  process::{Child, Command, Stdio, id},
  str::FromStr,
  thread::sleep,
  time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
  config::Workload,
  harness::na_rows,
  json::{JSON_SCHEMA, JsonEngine, JsonRow, JsonRun},
  machine::{MachineInfo, get_process_physical_memory, is_ram_backed_fs, plain_root},
  report::{column_label, labeled_results},
  result::ResultType,
  table::print_results_table,
};

/// 打开引擎、跑完 18 段、返回行结果；错误串用于列状态 `crashed` 的说明
pub type EngineRunFn = fn(&Path, &Workload) -> Result<Vec<(String, ResultType)>, String>;

#[derive(Copy, Clone)]
pub struct EngineSpec {
  /// 表列名
  pub name: &'static str,
  /// 被测实现位置或上游主页
  pub source: &'static str,
  pub run: EngineRunFn,
}

/// 子进程回传的列结果
#[derive(Debug, Serialize, Deserialize)]
struct ChildPayload {
  engine: JsonEngine,
}

const USAGE: &str = "用法：wedb-bench [选项]

  --only a,b         只跑指定引擎列（缺省跑全部已编译的列）
  --scale F          按 F 缩放条目量级（redb 标准档为 1.0）
  --quick            等价 --scale 0.02，用于冒烟与 CI 调试
  --cache-mb N       覆盖引擎缓存预算（默认 4096 MiB，按物理内存自动收敛）
  --timeout-secs N   单引擎墙钟预算，超时该列折 N/A
  --data-path DIR    评测数据目录（默认 OS 临时目录下 wedb-bench）
  --json FILE        写出合并后的机读结果
  --commit SHA       标注提交（默认取 GITHUB_SHA）
  --branch NAME      标注分支（默认取 GITHUB_REF_NAME）
  --version VER      标注版本身份（CI 传 git describe --tags --always）
  --list             列出已编译的引擎列
";

#[derive(Debug, Default, Clone)]
pub struct Args {
  /// 子进程模式：仅跑这一个引擎
  pub engine: Option<String>,
  pub workload_json: Option<String>,
  pub data_path: Option<PathBuf>,
  pub out: Option<PathBuf>,
  pub merged_out: Option<PathBuf>,
  pub scale: Option<f64>,
  pub cache_mb: Option<usize>,
  pub timeout_secs: Option<u64>,
  pub quick: bool,
  pub list: bool,
  pub help: bool,
  pub commit: Option<String>,
  pub branch: Option<String>,
  pub version: Option<String>,
  pub only: Vec<String>,
}

fn take_value(argv: &[String], index: &mut usize, name: &str) -> Result<String, String> {
  *index += 1;
  argv
    .get(*index)
    .cloned()
    .ok_or_else(|| format!("{name} 缺少取值"))
}

fn parse_number<T: FromStr>(text: &str, name: &str) -> Result<T, String> {
  text
    .parse::<T>()
    .map_err(|_| format!("{name} 取值无效：{text}"))
}

pub fn parse_args(argv: &[String]) -> Result<Args, String> {
  let mut args = Args::default();
  let mut index = 0usize;
  while index < argv.len() {
    let flag = argv[index].as_str();
    match flag {
      "--engine" => args.engine = Some(take_value(argv, &mut index, "--engine")?),
      "--workload-json" => {
        args.workload_json = Some(take_value(argv, &mut index, "--workload-json")?)
      }
      "--data-path" => {
        args.data_path = Some(PathBuf::from(take_value(argv, &mut index, "--data-path")?))
      }
      "--out" => args.out = Some(PathBuf::from(take_value(argv, &mut index, "--out")?)),
      "--json" => args.merged_out = Some(PathBuf::from(take_value(argv, &mut index, "--json")?)),
      "--scale" => {
        args.scale = Some(parse_number(
          take_value(argv, &mut index, "--scale")?.as_str(),
          "--scale",
        )?)
      }
      "--cache-mb" => {
        args.cache_mb = Some(parse_number(
          take_value(argv, &mut index, "--cache-mb")?.as_str(),
          "--cache-mb",
        )?)
      }
      "--timeout-secs" => {
        args.timeout_secs = Some(parse_number(
          take_value(argv, &mut index, "--timeout-secs")?.as_str(),
          "--timeout-secs",
        )?)
      }
      "--only" => {
        args.only = take_value(argv, &mut index, "--only")?
          .split(',')
          .map(|s| s.trim().to_string())
          .filter(|s| !s.is_empty())
          .collect()
      }
      "--commit" => args.commit = Some(take_value(argv, &mut index, "--commit")?),
      "--branch" => args.branch = Some(take_value(argv, &mut index, "--branch")?),
      "--version" => args.version = Some(take_value(argv, &mut index, "--version")?),
      "--quick" => args.quick = true,
      "--list" => args.list = true,
      "--help" | "-h" => args.help = true,
      // cargo bench 会把自己的目标选择标志原样转交给 harness=false 的 bench 二进制
      "--bench" | "--test" => {}
      other => return Err(format!("未知参数：{other}\n{USAGE}")),
    }
    index += 1;
  }
  Ok(args)
}

fn default_data_path(args: &Args) -> PathBuf {
  if let Some(path) = &args.data_path {
    return path.clone();
  }
  if let Ok(dir) = env::var("WEDB_BENCHMARK_DIR") {
    return PathBuf::from(dir);
  }
  temp_dir().join("wedb-bench")
}

fn resolve_workload(args: &Args, machine: &MachineInfo) -> (Workload, Vec<String>) {
  let mut notes = Vec::new();
  let scale = if args.quick {
    args.scale.unwrap_or(0.02)
  } else {
    args.scale.unwrap_or(1.0)
  };
  let mut workload = Workload::scaled(scale);

  if let Some(cache_mb) = args.cache_mb {
    workload.cache_size = cache_mb * 1024 * 1024;
  }

  if workload.cap_cache_to_memory(machine.total_memory_gib) {
    notes.push(format!(
      "缓存预算按物理内存收敛到 {:.2} GiB（redb 标准档为 4 GiB）",
      workload.cache_size as f64 / (1024.0 * 1024.0 * 1024.0)
    ));
  }

  if scale != 1.0 {
    notes.push(format!("负载按 {scale}× 缩放，非 redb 标准档"));
  }

  (workload, notes)
}

/// 子进程工作目录：退出（含 panic）时删除，避免残留下百 GB 数据
struct WorkDir {
  path: PathBuf,
}

impl Drop for WorkDir {
  fn drop(&mut self) {
    let _ = remove_dir_all(&self.path);
  }
}

fn child_work_dir(data_root: &Path, engine: &str) -> Result<WorkDir, String> {
  create_dir_all(data_root).map_err(|e| e.to_string())?;
  let unique = format!("{engine}-{}-{}", id(), fastrand::u64(..));
  let path = data_root.join(unique);
  create_dir_all(&path).map_err(|e| e.to_string())?;
  Ok(WorkDir { path })
}

pub fn run_child(specs: &[EngineSpec], args: &Args) -> Result<(), String> {
  let name = args.engine.clone().ok_or("--engine 缺失".to_string())?;
  let workload_json = args
    .workload_json
    .as_ref()
    .ok_or("--workload-json 缺失".to_string())?;
  let workload: Workload = serde_json::from_str(workload_json).map_err(|e| e.to_string())?;
  let spec = specs
    .iter()
    .find(|s| s.name == name.as_str())
    .ok_or_else(|| format!("引擎 {name} 未编译进本二进制"))?;

  let data_root = args
    .data_path
    .clone()
    .unwrap_or_else(|| temp_dir().join("wedb-bench"));
  let work = child_work_dir(&data_root, name.as_str())?;
  let out_file = args.out.clone().ok_or("--out 缺失".to_string())?;

  let rows = (spec.run)(&work.path, &workload).map_err(|detail| {
    println!("{}: 中止 — {detail}", spec.name);
    detail
  })?;

  let engine = JsonEngine {
    name,
    source: spec.source.to_string(),
    status: "ok".to_string(),
    detail: None,
    peak_memory_bytes: Some(get_process_physical_memory()),
    rows: rows
      .iter()
      .map(|(row_name, result)| JsonRow::from_result(row_name, result))
      .collect(),
  };
  let payload = ChildPayload { engine };
  let text = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
  let mut file = File::create(&out_file).map_err(|e| e.to_string())?;
  file.write_all(text.as_bytes()).map_err(|e| e.to_string())?;
  Ok(())
}

fn spawn_child(
  name: &str,
  workload: &Workload,
  data_root: &Path,
  out: &Path,
) -> Result<Child, String> {
  let exe = current_exe().map_err(|e| e.to_string())?;
  let workload_json = serde_json::to_string(workload).map_err(|e| e.to_string())?;
  Command::new(exe)
    .arg("--engine")
    .arg(name)
    .arg("--workload-json")
    .arg(workload_json)
    .arg("--data-path")
    .arg(data_root)
    .arg("--out")
    .arg(out)
    .stdout(Stdio::inherit())
    .stderr(Stdio::inherit())
    .spawn()
    .map_err(|e| e.to_string())
}

fn wait_child(child: &mut Child, budget: Duration) -> Result<(), String> {
  let start = Instant::now();
  loop {
    match child.try_wait() {
      Ok(Some(status)) => {
        return if status.success() {
          Ok(())
        } else {
          Err(format!("退出码 {}", status.code().unwrap_or(-1)))
        };
      }
      Ok(None) => {}
      Err(e) => return Err(e.to_string()),
    }
    if start.elapsed() > budget {
      let _ = child.kill();
      let _ = child.wait();
      return Err(format!("超过 {budget:?} 墙钟预算被终止"));
    }
    sleep(Duration::from_millis(200));
  }
}

fn na_engine(
  name: &str,
  source: &str,
  workload: &Workload,
  status: &str,
  detail: String,
) -> JsonEngine {
  JsonEngine {
    name: name.to_string(),
    source: source.to_string(),
    status: status.to_string(),
    detail: Some(detail),
    peak_memory_bytes: None,
    rows: na_rows(workload)
      .iter()
      .map(|(row_name, result)| JsonRow::from_result(row_name, result))
      .collect(),
  }
}

pub fn run_parent(specs: &[EngineSpec], args: &Args) -> Result<(), String> {
  let selected: Vec<&EngineSpec> = if args.only.is_empty() {
    specs.iter().collect()
  } else {
    specs
      .iter()
      .filter(|s| args.only.iter().any(|only| only.as_str() == s.name))
      .collect()
  };
  if selected.is_empty() {
    return Err("没有匹配的引擎列（--list 查看已编译引擎）".to_string());
  }

  let data_root = default_data_path(args);
  create_dir_all(&data_root).map_err(|e| e.to_string())?;
  let data_root = plain_root(data_root.canonicalize().unwrap_or(data_root));

  let machine = MachineInfo::detect(&data_root, &data_root);
  let (workload, mut notes) = resolve_workload(args, &machine);
  if is_ram_backed_fs(&machine.data_fs) {
    notes.push(format!(
      "数据目录 {} 位于 {}（内存盘），尺寸段不反映物理盘占用",
      machine.data_dir, machine.data_fs
    ));
  }

  let budget = Duration::from_secs(
    args
      .timeout_secs
      .unwrap_or_else(|| workload.derived_timeout_secs()),
  );

  println!(
    "平台 {} · {} 核 · {:.1} GiB · 数据目录 {} ({})",
    machine.platform,
    machine.logical_cores,
    machine.total_memory_gib,
    machine.data_dir,
    machine.data_fs
  );
  println!(
    "负载 bulk {} / sorted {} / reads {} · 缓存 {:.2} GiB · 单引擎预算 {}s",
    workload.bulk_elements,
    workload.sorted_elements,
    workload.num_reads,
    workload.cache_size as f64 / (1024.0 * 1024.0 * 1024.0),
    budget.as_secs()
  );

  let mut engines: Vec<JsonEngine> = Vec::new();
  for spec in &selected {
    println!("=== {} ({}) ===", spec.name, spec.source);
    let out = data_root.join(format!("{}-{}.json", spec.name, id()));
    let started = Instant::now();
    let outcome = match spawn_child(spec.name, &workload, &data_root, &out) {
      Ok(mut child) => match wait_child(&mut child, budget) {
        Ok(()) => read_to_string(&out)
          .map_err(|e| e.to_string())
          .and_then(|text| serde_json::from_str::<ChildPayload>(&text).map_err(|e| e.to_string())),
        Err(detail) => Err(detail),
      },
      Err(detail) => Err(detail),
    };

    let engine = match outcome {
      Ok(payload) => {
        println!(
          "--- {} 用时 {:.1}s",
          spec.name,
          started.elapsed().as_secs_f64()
        );
        payload.engine
      }
      Err(detail) => {
        let status = if detail.contains("墙钟预算") {
          "timeout"
        } else {
          "crashed"
        };
        println!("--- {} {}：{}", spec.name, status, detail);
        na_engine(spec.name, spec.source, &workload, status, detail)
      }
    };
    let _ = remove_file(&out);
    engines.push(engine);
  }

  let mut run = JsonRun {
    schema: JSON_SCHEMA,
    generated_at_unix: JsonRun::unix_now(),
    commit: env_or("GITHUB_SHA", args.commit.clone(), "local"),
    branch: env_or("GITHUB_REF_NAME", args.branch.clone(), "local"),
    version: env_or("WEDB_BENCH_VERSION", args.version.clone(), ""),
    platform: machine.platform.clone(),
    machine,
    workload: workload.clone(),
    notes,
    engines,
  };
  run.mark_winners();

  let rows = labeled_results(&run);
  print_results_table(&rows);

  for engine in &run.engines {
    if engine.status != "ok" {
      println!(
        "列 {}：{} — {}",
        column_label(&engine.name),
        engine.status,
        engine.detail.clone().unwrap_or_default()
      );
    }
  }
  for note in &run.notes {
    println!("备注：{note}");
  }

  if let Some(path) = &args.merged_out {
    if let Some(parent) = path.parent() {
      let _ = create_dir_all(parent);
    }
    let text = serde_json::to_string_pretty(&run).map_err(|e| e.to_string())?;
    write(path, text).map_err(|e| e.to_string())?;
    println!("机读结果：{}", path.display());
  }

  Ok(())
}

fn env_or(var: &str, explicit: Option<String>, fallback: &str) -> String {
  explicit.unwrap_or_else(|| env::var(var).unwrap_or_else(|_| fallback.to_string()))
}

/// 二进制入口：`--engine` 即子进程模式，否则按平台出一份合并结果
pub fn main_logic(specs: &[EngineSpec], argv: &[String]) -> Result<(), String> {
  let args = parse_args(argv)?;
  if args.help {
    println!("{USAGE}");
    return Ok(());
  }
  if args.list {
    for spec in specs {
      println!("{}\t{}", spec.name, spec.source);
    }
    return Ok(());
  }
  if args.engine.is_some() {
    return run_child(specs, &args);
  }
  run_parent(specs, &args)
}
