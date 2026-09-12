use std::{env, fs, path::PathBuf, process};

use bench::{
  engines::{run_all_engines, run_single_engine},
  error::Result,
  i18n::I18nTexts,
  report::print_console_table,
  sys_info::MachineInfo,
  types::{BenchmarkConfig, JsonBenchmarkData, JsonEngineResult, JsonMetric},
};

fn find_project_root() -> PathBuf {
  let cur = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
  // 如果当前在 bench/bench 或 bench 目录，向上寻找包含 wedb 与 bench 的根目录
  for ancestor in cur.ancestors() {
    if ancestor.join("wedb").exists() && ancestor.join("bench").exists() {
      return ancestor.to_path_buf();
    }
  }
  cur
}

fn main() -> Result<()> {
  let args: Vec<String> = env::args().collect();

  // 如果指定了 --engine 参数，作为独立子进程仅执行该引擎评测，输出结果后退出
  if let Some(pos) = args.iter().position(|a| a == "--engine")
    && let Some(engine_name) = args.get(pos + 1)
  {
    let cfg = if let Some(cfg_pos) = args.iter().position(|a| a == "--cfg-json")
      && let Some(cfg_str) = args.get(cfg_pos + 1)
    {
      sonic_rs::from_str::<BenchmarkConfig>(cfg_str).unwrap_or_default()
    } else {
      BenchmarkConfig::default()
    };

    let work_dir =
      env::temp_dir().join(format!("wedb_bench_sub_{}_{}", engine_name, process::id()));
    let _ = fs::create_dir_all(&work_dir);

    if let Some(metrics) = run_single_engine(engine_name, &work_dir, &cfg)
      && let Some(out_pos) = args.iter().position(|a| a == "--out-file")
      && let Some(out_path) = args.get(out_pos + 1)
    {
      let json_str = sonic_rs::to_string(&metrics).unwrap_or_default();
      let _ = fs::write(out_path, json_str);
    }
    let _ = fs::remove_dir_all(&work_dir);
    return Ok(());
  }

  let root_dir = find_project_root();
  let bench_dir = root_dir.join("bench");
  let zh_i18n_path = bench_dir.join("js/i18n/zh.yml");
  let zh_i18n = I18nTexts::load(&zh_i18n_path)?;

  let mut cfg = if args.iter().any(|a| a == "--quick") {
    println!(">>> 启用快速评测模式");
    BenchmarkConfig::quick()
  } else if args.iter().any(|a| a == "--large") {
    println!(">>> 启用特大数据量模式 (大于内存)");
    BenchmarkConfig {
      bulk_elements: 10_000_000,
      value_size: 1024,
      cache_size: 2 * 1024 * 1024 * 1024, // 2GB 缓存，数据量 10GB+
      num_reads: 1_000_000,
      removals: 1_000_000,
      ..BenchmarkConfig::default()
    }
  } else if args.iter().any(|a| a == "--5m") {
    println!(">>> 启用 500 万标准评测模式");
    BenchmarkConfig::standard_5m()
  } else {
    BenchmarkConfig::default()
  };

  let mut i = 1;
  while i < args.len() {
    match args[i].as_str() {
      "--elements" if i + 1 < args.len() => {
        if let Ok(n) = args[i + 1].parse::<usize>() {
          cfg.bulk_elements = n;
          cfg.num_reads = n / 2;
          cfg.removals = n / 2;
        }
        i += 1;
      }
      "--value-size" if i + 1 < args.len() => {
        if let Ok(n) = args[i + 1].parse::<usize>() {
          cfg.value_size = n;
        }
        i += 1;
      }
      "--cache-size-mb" if i + 1 < args.len() => {
        if let Ok(n) = args[i + 1].parse::<usize>() {
          cfg.cache_size = n * 1024 * 1024;
        }
        i += 1;
      }
      _ => {}
    }
    i += 1;
  }

  if let Ok(val) = env::var("BENCH_ELEMENTS")
    && let Ok(n) = val.parse::<usize>()
  {
    println!(">>> 从环境变量覆盖基准数据量: {n}");
    cfg.bulk_elements = n;
    cfg.num_reads = n / 2;
    cfg.removals = n / 2;
  }

  println!(">>> 正在探测主机环境...");
  let machine_info = MachineInfo::detect(&bench_dir);
  println!("CPU: {}", machine_info.cpu_brand);
  println!(
    "核心: {} 物理 / {} 逻辑",
    machine_info.physical_cores, machine_info.logical_cores
  );
  println!("内存: {:.2} GiB", machine_info.total_memory_gib);
  println!("系统: {}", machine_info.os_info);
  println!("磁盘: {}", machine_info.disk_type);

  println!("\n>>> 开始执行各引擎性能评测...");
  let results = run_all_engines(&bench_dir, &cfg);

  println!("\n>>> 导出原始评测结果...");
  let json_engines: Vec<JsonEngineResult> = results
    .iter()
    .map(|(name, url, metrics)| JsonEngineResult {
      name: name.to_string(),
      url: url.to_string(),
      metrics: metrics
        .iter()
        .map(|(k, v)| JsonMetric::from_result(k, v))
        .collect(),
    })
    .collect();

  let json_data = JsonBenchmarkData {
    machine: machine_info,
    config: cfg,
    engines: json_engines,
  };

  let data_dir = bench_dir.join("data");
  fs::create_dir_all(&data_dir)?;
  let json_str = sonic_rs::to_string_pretty(&json_data)?;
  let json_path = data_dir.join("latest.json");
  fs::write(&json_path, json_str)?;
  println!(">>> 评测原始数据已保存至: {}", json_path.display());

  print_console_table(&zh_i18n, &results);

  Ok(())
}
