#[cfg(feature = "fjall")]
pub mod fjall_engine;
#[cfg(feature = "redb")]
pub mod redb_engine;
#[cfg(feature = "rocksdb")]
pub mod rocksdb_engine;
#[cfg(feature = "sqlite")]
pub mod sqlite_engine;
#[cfg(feature = "wbftree")]
pub mod wbftree_engine;
#[cfg(feature = "wkv")]
pub mod wkv_engine;

use std::{
  env,
  fmt::Display,
  fs,
  path::Path,
  process, thread,
  time::{Duration, Instant},
};

use tempfile::TempDir;

use crate::{
  harness::run_benchmark,
  traits::BenchDatabase,
  types::{BenchmarkConfig, METRIC_KEYS, ResultType},
};

const WKV_URL: &str = "https://github.com/webc-site/wedb/tree/main/wedb/wkv";
const WBFTREE_URL: &str = "https://github.com/webc-site/wedb/tree/main/wedb/wbftree";
const REDB_URL: &str = "https://github.com/cberner/redb";
const FJALL_URL: &str = "https://github.com/fjall-rs/fjall";
const ROCKSDB_URL: &str = "https://github.com/facebook/rocksdb";
const SQLITE_URL: &str = "https://www.sqlite.org";

/// 评测引擎元数据与结果
pub type EngineBenchResult = (&'static str, &'static str, Vec<(String, ResultType)>);

pub const ENGINES: &[(&str, &str)] = &[
  #[cfg(feature = "wkv")]
  ("wkv", WKV_URL),
  #[cfg(feature = "wbftree")]
  ("wbftree", WBFTREE_URL),
  #[cfg(feature = "redb")]
  ("redb", REDB_URL),
  #[cfg(feature = "fjall")]
  ("fjall", FJALL_URL),
  #[cfg(feature = "rocksdb")]
  ("rocksdb", ROCKSDB_URL),
  #[cfg(feature = "sqlite")]
  ("sqlite", SQLITE_URL),
];

/// 执行指定引擎的评测闭环（统一临时目录、日志输出与错误处理）
fn execute_engine_bench<E: BenchDatabase + 'static, F, Err: Display>(
  name: &str,
  work_dir: &Path,
  cfg: &BenchmarkConfig,
  open_fn: F,
) -> Option<Vec<(String, ResultType)>>
where
  F: FnOnce(&Path) -> Result<E, Err>,
{
  println!("\n=== 开始评测 {name} ===");
  let tmpdir = TempDir::new_in(work_dir).ok()?;
  match open_fn(tmpdir.path()) {
    Ok(engine) => Some(run_benchmark(engine, tmpdir.path(), cfg)),
    Err(err) => {
      eprintln!("{name} 初始化失败: {err}");
      None
    }
  }
}

/// 运行单个引擎评测
pub fn run_single_engine(
  name: &str,
  work_dir: &Path,
  cfg: &BenchmarkConfig,
) -> Option<Vec<(String, ResultType)>> {
  match name {
    #[cfg(feature = "wkv")]
    "wkv" => execute_engine_bench("wkv", work_dir, cfg, |dir| {
      wkv_engine::WkvEngine::open(&dir.join("bench.wkv"), cfg.cache_size, cfg.bulk_elements)
    }),
    #[cfg(feature = "wbftree")]
    "wbftree" => execute_engine_bench("wbftree", work_dir, cfg, |dir| {
      wbftree_engine::WbftreeEngine::open(&dir.join("bench.bftree"), cfg.cache_size)
    }),
    #[cfg(feature = "redb")]
    "redb" => execute_engine_bench("redb", work_dir, cfg, |dir| {
      redb_engine::RedbEngine::open(&dir.join("bench.redb"), cfg.cache_size)
    }),
    #[cfg(feature = "fjall")]
    "fjall" => execute_engine_bench("fjall", work_dir, cfg, |dir| {
      fjall_engine::FjallEngine::open(dir, cfg.cache_size)
    }),
    #[cfg(feature = "rocksdb")]
    "rocksdb" => execute_engine_bench("rocksdb", work_dir, cfg, |dir| {
      rocksdb_engine::RocksdbEngine::open(dir, cfg.cache_size)
    }),
    #[cfg(feature = "sqlite")]
    "sqlite" => execute_engine_bench("sqlite", work_dir, cfg, |dir| {
      sqlite_engine::SqliteEngine::open(&dir.join("bench.sqlite"), cfg.cache_size)
    }),
    _ => None,
  }
}

/// 以段落键清单补齐整列标记行，保证未跑完引擎不缺行
/// （对标 C# EntryPoint.cs 逐相即时产出、结果面完整：缺席即异常，不允许静默出局）
fn pad_metrics(res: ResultType) -> Vec<(String, ResultType)> {
  METRIC_KEYS.iter().map(|k| (k.to_string(), res)).collect()
}

/// 运行所有已启用的引擎评测（采用独立子进程隔离，保证结束时常驻内存指标 100% 纯净准确）
///
/// data_root 为评测数据目录根（主进程 --data-path 透传，缺省 OS 临时目录），
/// 派生 work_dir 子目录并透传给子进程命令行，保持单一路径来源
pub fn run_all_engines(data_root: &Path, cfg: &BenchmarkConfig) -> Vec<EngineBenchResult> {
  let mut all_results = Vec::new();
  let Ok(exe) = env::current_exe() else {
    eprintln!("无法获取当前程序路径，回退到进程内执行");
    return run_all_engines_in_process(data_root, cfg);
  };
  let Ok(cfg_json) = sonic_rs::to_string(cfg) else {
    return run_all_engines_in_process(data_root, cfg);
  };

  let work_dir = data_root.join(format!("wedb_bench_proc_{}", process::id()));
  let _ = fs::create_dir_all(&work_dir);

  for &(name, url) in ENGINES {
    let out_file = work_dir.join(format!("{name}_res.json"));
    let mut child = match process::Command::new(&exe)
      .arg("--engine")
      .arg(name)
      .arg("--cfg-json")
      .arg(&cfg_json)
      .arg("--data-path")
      .arg(data_root)
      .arg("--out-file")
      .arg(&out_file)
      .spawn()
    {
      Ok(c) => c,
      Err(err) => {
        eprintln!("{name} 子进程启动失败: {err}");
        all_results.push((name, url, pad_metrics(ResultType::NA)));
        continue;
      }
    };

    // 每引擎超时预算来自 cfg.timeout_secs（缺省按数据量推导，--timeout-secs 显式覆盖），
    // 超时不再静默出局：kill 后补齐整列 Timeout 标记行
    let timeout = Duration::from_secs(cfg.timeout_secs.max(1));
    let start = Instant::now();
    let mut finished = false;

    while start.elapsed() < timeout {
      match child.try_wait() {
        Ok(Some(status)) => {
          let metrics = if status.success() {
            fs::read_to_string(&out_file)
              .ok()
              .and_then(|content| sonic_rs::from_str::<Vec<(String, ResultType)>>(&content).ok())
              .unwrap_or_else(|| {
                eprintln!("{name} 子进程结果缺失或解析失败，按 N/A 补行");
                pad_metrics(ResultType::NA)
              })
          } else {
            eprintln!("{name} 子进程执行异常退出: {status:?}");
            pad_metrics(ResultType::NA)
          };
          let _ = fs::remove_file(&out_file);
          all_results.push((name, url, metrics));
          finished = true;
          break;
        }
        Ok(None) => {
          thread::sleep(Duration::from_millis(200));
        }
        Err(err) => {
          eprintln!("{name} 状态检查失败: {err}");
          all_results.push((name, url, pad_metrics(ResultType::NA)));
          finished = true;
          break;
        }
      }
    }

    if !finished {
      eprintln!("{name} 执行超时（超过 {timeout:?}），强制终止进程");
      let child_id = child.id();
      let _ = child.kill();
      let _ = child.wait();
      let _ = fs::remove_file(&out_file);
      let sub_work_dir = data_root.join(format!("wedb_bench_sub_{name}_{child_id}"));
      let _ = fs::remove_dir_all(sub_work_dir);
      all_results.push((
        name,
        url,
        pad_metrics(ResultType::Timeout { limit: timeout }),
      ));
    }
  }

  let _ = fs::remove_dir_all(&work_dir);
  all_results
}

fn run_all_engines_in_process(data_root: &Path, cfg: &BenchmarkConfig) -> Vec<EngineBenchResult> {
  let mut all_results = Vec::new();
  let work_dir = data_root.join(format!("wedb_bench_inproc_{}", process::id()));
  let _ = fs::create_dir_all(&work_dir);

  for &(name, url) in ENGINES {
    let metrics = match run_single_engine(name, &work_dir, cfg) {
      Some(res) => res,
      None => {
        eprintln!("{name} 引擎初始化失败，按 N/A 补行");
        pad_metrics(ResultType::NA)
      }
    };
    all_results.push((name, url, metrics));
  }

  let _ = fs::remove_dir_all(&work_dir);
  all_results
}
