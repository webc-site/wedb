//! 测试日志捕获（进程级双面 logger）：ctor 安装的全局 logger 在保留 stdout
//! 诊断面的同时镜像记录进带序号的环形缓冲，测试用例以 mark 增量检索断言
//! warn/error 留痕（对标 C# TestBase 日志断言形态）。
//!
//! 为什么不由测试各自 `log::set_boxed_logger`：log 全局 logger 每进程仅可
//! 安装一次，本 crate ctor 先行装配后，测试内再装恒失败且 `let _` 吞错，
//! 捕获缓冲恒空（cluster_migration slots-vec 两测出生红根因，2026-09-25）。

use std::{
  env::var,
  sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
  },
};

use log::{Level, LevelFilter, Log, Metadata, Record};

static RECORDS: Mutex<Vec<(u64, Level, String)>> = Mutex::new(Vec::new());
static SEQ: AtomicU64 = AtomicU64::new(0);
const CAP: usize = 8192;

struct CaptureLogger;

impl Log for CaptureLogger {
  fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
    true
  }

  fn log(&self, record: &Record<'_>) {
    let msg = record.args().to_string();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut buf = RECORDS.lock().unwrap();
    buf.push((seq, record.level(), msg));
    if buf.len() > CAP {
      let excess = buf.len() - CAP;
      buf.drain(..excess);
    }
    println!("[{}] {}", record.level(), record.args());
  }

  fn flush(&self) {}
}

/// RUST_LOG 全局级别粗解析（取首个裸级别段；解析不出回落 Debug），保留
/// env.sh 门禁环境的主要降噪语义
fn max_level_from_env() -> LevelFilter {
  var("RUST_LOG")
    .ok()
    .and_then(|v| {
      v.split(',')
        .find(|seg| !seg.contains('=') && !seg.trim().is_empty())
        .map(|seg| seg.trim().to_string())
    })
    .and_then(|seg| seg.parse().ok())
    .unwrap_or(LevelFilter::Debug)
}

/// ctor 调用点：抢先安装全局 logger（本 crate 链接面内唯一的安装时机）
pub(crate) fn install() {
  let _ = log::set_boxed_logger(Box::new(CaptureLogger));
  log::set_max_level(max_level_from_env());
}

/// 当前序号 mark（捕获起点，配合 [`log_capture_records_since`] 增量检索）
pub fn log_capture_mark() -> u64 {
  SEQ.load(Ordering::Relaxed)
}

/// 检索 mark 之后的记录（级别 + 文案原文）
pub fn log_capture_records_since(mark: u64) -> Vec<(Level, String)> {
  RECORDS
    .lock()
    .unwrap()
    .iter()
    .filter(|(seq, ..)| *seq > mark)
    .map(|(_, level, msg)| (*level, msg.clone()))
    .collect()
}
