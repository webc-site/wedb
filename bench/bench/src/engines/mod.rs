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
  types::{BenchmarkConfig, ResultType},
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

/// 运行所有已启用的引擎评测（采用独立子进程隔离，保证结束时常驻内存指标 100% 纯净准确）
pub fn run_all_engines(_bench_dir: &Path, cfg: &BenchmarkConfig) -> Vec<EngineBenchResult> {
  let mut all_results = Vec::new();
  let Ok(exe) = env::current_exe() else {
    eprintln!("无法获取当前程序路径，回退到进程内执行");
    return run_all_engines_in_process(cfg);
  };
  let Ok(cfg_json) = sonic_rs::to_string(cfg) else {
    return run_all_engines_in_process(cfg);
  };

  let work_dir = env::temp_dir().join(format!("wedb_bench_proc_{}", process::id()));
  let _ = fs::create_dir_all(&work_dir);

  for &(name, url) in ENGINES {
    let out_file = work_dir.join(format!("{name}_res.json"));
    let mut child = match process::Command::new(&exe)
      .arg("--engine")
      .arg(name)
      .arg("--cfg-json")
      .arg(&cfg_json)
      .arg("--out-file")
      .arg(&out_file)
      .spawn()
    {
      Ok(c) => c,
      Err(err) => {
        eprintln!("{name} 子进程启动失败: {err}");
        continue;
      }
    };

    // 每个引擎评测默认超时 5 分钟 (300 秒)，超时则强制 kill 终止，避免卡死
    let timeout = Duration::from_secs(300);
    let start = Instant::now();
    let mut finished = false;

    while start.elapsed() < timeout {
      match child.try_wait() {
        Ok(Some(status)) => {
          if status.success() && out_file.exists() {
            if let Ok(content) = fs::read_to_string(&out_file)
              && let Ok(metrics) = sonic_rs::from_str::<Vec<(String, ResultType)>>(&content)
            {
              all_results.push((name, url, metrics));
            }
            let _ = fs::remove_file(&out_file);
          } else {
            eprintln!("{name} 子进程执行异常退出: {status:?}");
          }
          finished = true;
          break;
        }
        Ok(None) => {
          thread::sleep(Duration::from_millis(200));
        }
        Err(err) => {
          eprintln!("{name} 状态检查失败: {err}");
          finished = true;
          break;
        }
      }
    }

    if !finished {
      eprintln!("{name} 执行超时（超过 {:?}），强制终止进程", timeout);
      let child_id = child.id();
      let _ = child.kill();
      let _ = child.wait();
      let _ = fs::remove_file(&out_file);
      let sub_work_dir = env::temp_dir().join(format!("wedb_bench_sub_{name}_{child_id}"));
      let _ = fs::remove_dir_all(sub_work_dir);
    }
  }

  let _ = fs::remove_dir_all(&work_dir);
  all_results
}

fn run_all_engines_in_process(cfg: &BenchmarkConfig) -> Vec<EngineBenchResult> {
  let mut all_results = Vec::new();
  let work_dir = env::temp_dir().join(format!("wedb_bench_inproc_{}", process::id()));
  let _ = fs::create_dir_all(&work_dir);

  for &(name, url) in ENGINES {
    if let Some(res) = run_single_engine(name, &work_dir, cfg) {
      all_results.push((name, url, res));
    }
  }

  let _ = fs::remove_dir_all(&work_dir);
  all_results
}
