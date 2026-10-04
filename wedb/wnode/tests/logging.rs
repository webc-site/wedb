//! 节点日志基础设施集成测试
//! （对应 libs/common/Logging/FileLoggerProvider.cs、libs/common/Logging/LogFormatter.cs 与 libs/host/MemoryLogger.cs）

use std::{env::temp_dir, fs, io, process::id, sync::Arc};

use jiff::Timestamp;
use log::{Level, LevelFilter, Log, Record};
use parking_lot::RwLock;
use wconf::{DEFAULT_LOG_FLUSH_INTERVAL, NodeArgs};
use wnode::logging::{
  FileLoggerOutput, FileLoggerProvider, INIT_LOG_CATEGORY, LogFormatter, LogTarget, LoggingBuilder,
  MemoryForwardLogger, MemoryLogger, MemoryLoggerProvider,
};

#[test]
fn formatter_shapes_timestamp() {
  let stamp = "2023-11-14T22:13:20.123456789Z"
    .parse::<Timestamp>()
    .unwrap();
  let date = LogFormatter::format_date(stamp);
  assert_eq!(date, "2023-11-14 22:13:20.1234");
}

#[test]
fn builder_from_node_projects_logger_args() {
  let file = format!("/tmp/wnode-from-node-{}.log", id());
  let node = NodeArgs {
    file_logger: Some(file.clone()),
    log_level: Some("debug".into()),
    ..NodeArgs::default()
  };
  let logging = LoggingBuilder::from_node(&node);
  assert_eq!(logging.minimum_level, LevelFilter::Debug);
  assert_eq!(logging.files, vec![(file, DEFAULT_LOG_FLUSH_INTERVAL)]);

  // 未配置 file_logger：仅控制台目标，级别缺省 Warning
  let logging = LoggingBuilder::from_node(&NodeArgs::default());
  assert!(logging.files.is_empty());
  assert_eq!(logging.minimum_level, LevelFilter::Warn);
  assert!(!logging.disable_console);
}

#[test]
fn from_node_wires_disable_console_logger() {
  let node = NodeArgs {
    disable_console_logger: true,
    ..NodeArgs::default()
  };
  assert!(LoggingBuilder::from_node(&node).disable_console);
}

#[test]
fn build_gates_console_sink_and_propagates_file_error() {
  let dir = temp_dir().join(format!("wnode-build-gate-{}", id()));
  fs::create_dir_all(&dir).unwrap();
  let face = LoggingBuilder::new()
    .disable_console()
    .add_file(dir.join("gate.log").display().to_string(), 0)
    .build()
    .unwrap();
  assert!(matches!(face, LogTarget::File(_)));

  // 缺省装配：仅控制台一个目标（单目标免扇出包装直还）
  let face = LoggingBuilder::new().build().unwrap();
  assert!(matches!(face, LogTarget::Console { .. }));
  fs::remove_dir_all(&dir).ok();

  // 文件不可建（目录缺失）→ 明确 IO 错误向上抛
  let missing = temp_dir()
    .join(format!("wnode-missing-dir-{}", id()))
    .join("no.log");
  let err = LoggingBuilder::new()
    .add_file(missing.display().to_string(), 0)
    .build()
    .err()
    .expect("目录缺失时 build 必须报 IO 错");
  assert_eq!(err.kind(), io::ErrorKind::NotFound);

  let forward = MemoryForwardLogger {
    memory: Arc::new(MemoryLogger::default()),
    destination: RwLock::new(None),
  };
  let err = LoggingBuilder::new()
    .add_file(missing.display().to_string(), 0)
    .flush_into(&forward)
    .expect_err("目录缺失时 flush_into 必须报 IO 错");
  assert_eq!(err.kind(), io::ErrorKind::NotFound);
  assert!(!forward.promoted(), "装配失败不得挂上目标面");
}

#[test]
fn memory_logger_collects_then_flushes() {
  let provider = MemoryLoggerProvider::default();
  let logger = provider.create_logger(INIT_LOG_CATEGORY);
  assert!(Arc::ptr_eq(
    &logger,
    &provider.create_logger(INIT_LOG_CATEGORY)
  ));
  logger.log(
    &Record::builder()
      .level(Level::Warn)
      .args(format_args!("装配告警"))
      .target(INIT_LOG_CATEGORY)
      .build(),
  );
  assert_eq!(logger.memory_log.lock().len(), 1);

  let sink = MemoryLogger::default();
  logger.flush_logger(&sink);
  assert!(logger.memory_log.lock().is_empty());
  assert_eq!(sink.memory_log.lock().len(), 1);

  provider.dispose();
  let refilled = provider.create_logger(INIT_LOG_CATEGORY);
  assert!(!Arc::ptr_eq(&logger, &refilled));
  assert!(refilled.memory_log.lock().is_empty());
}

#[test]
fn file_output_appends_and_formats() {
  let dir = temp_dir().join(format!("wnode-log-test-{}", id()));
  fs::create_dir_all(&dir).unwrap();
  let path = dir.join("append.log");
  let output = Arc::new(FileLoggerOutput::new(&path, 0).unwrap());
  let provider = FileLoggerProvider::new(Arc::clone(&output));
  provider.log_record(Level::Info, "ArgParser", &format_args!("节点启动"));
  output.dispose();

  let content = fs::read_to_string(&path).unwrap();
  assert!(content.contains("(INFO) <ArgParser> 节点启动"));
  assert!(content.starts_with('['));
  fs::remove_dir_all(&dir).ok();
}

#[test]
fn forward_logger_buffers_until_promoted() {
  let forward = MemoryForwardLogger::install().expect("全局日志器仅测试进程首次安装");
  assert!(!forward.promoted());
  forward.log(
    &Record::builder()
      .level(Level::Error)
      .args(format_args!("先行缓冲"))
      .target("test")
      .build(),
  );
  assert_eq!(forward.memory.memory_log.lock().len(), 1);

  let dst = MemoryLogger::default();
  forward.promote(dst.clone());
  assert!(forward.promoted());
  assert!(forward.memory.memory_log.lock().is_empty());
  assert_eq!(dst.memory_log.lock().len(), 1);
}
