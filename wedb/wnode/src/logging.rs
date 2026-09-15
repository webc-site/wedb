//! 节点日志基础设施（对标 libs/common/Logging/FileLoggerProvider.cs、
//! libs/common/Logging/LogFormatter.cs 与 libs/host/MemoryLogger.cs）
//!
//! C# 经 Microsoft.Extensions.Logging 的 Provider 抽象装配；rust 侧统一为
//! [`log`] 门面的 [`log::Log`] 实现：
//! - [`LogFormatter`]：时间格式化原语（C# LogFormatter）
//! - [`FileLoggerProvider`] / [`FileLoggerOutput`]：日志追加落文件
//! - [`MemoryLogger`] / [`MemoryLoggerProvider`]：内存缓冲收集器
//! - [`MemoryForwardLogger`]：装配期先行缓冲、目标就绪后切换回灌
//!   （C# GarnetServer 构造器的 initLogger + FlushMemoryLogger 生命周期，
//!   libs/host/GarnetServer.cs:FlushMemoryLogger）
//! - [`LoggingBuilder`]：日志面装配器（C# LoggerFactory.Create(builder => …)
//!   的 AddSimpleConsole / AddFile / SetMinimumLevel 投影）

use std::{
  fmt::Arguments,
  fs, io,
  io::{BufWriter, Write, stderr},
  path::Path,
  sync::Arc,
};

use gxhash::{HashMap as GxHashMap, HashSet as GxHashSet};
use jiff::Timestamp;
use log::{Level, LevelFilter, Log, Metadata, Record};
use parking_lot::{Mutex, RwLock};

/// C# 事件 id 无 log 门面对应物，记录行以时间戳开头（C# `[{eventId:D3}.{date}]`
/// 的事件段省略）。
const RECORD_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// libs/common/Logging/LogFormatter.cs:LogFormatter
///
/// 日志时间格式化原语（日期 `yyyy-MM-dd HH:mm:ss.ffff`、时间 `HH:mm:ss.ffff`）。
pub struct LogFormatter;

impl LogFormatter {
  /// libs/common/Logging/LogFormatter.cs:FormatDate（`yyyy-MM-dd HH:mm:ss.ffff`）
  pub fn format_date(time: Timestamp) -> String {
    format!(
      "{}.{:04}",
      time.strftime(RECORD_TIME_FORMAT),
      fraction_fourths(time)
    )
  }

  /// libs/common/Logging/LogFormatter.cs:FormatTime（`HH:mm:ss.ffff`）
  pub fn format_time(time: Timestamp) -> String {
    format!(
      "{}.{:04}",
      time.strftime("%H:%M:%S"),
      fraction_fourths(time)
    )
  }
}

/// 万分之一秒小数段（C# `ffff` 格式，取微秒段前 4 位）。
fn fraction_fourths(time: Timestamp) -> i64 {
  time.as_microsecond() % 1_000_000 / 100
}

/// 记录行统一格式（C# FileLoggerOutput.Log 的单行模板）：
/// `[{date}] ({level}) <{category}> {message}`
fn format_record(level: Level, category: &str, args: &Arguments<'_>) -> String {
  format!(
    "[{}] ({}) <{category}> {args}",
    LogFormatter::format_date(Timestamp::now()),
    level
  )
}

/// 输出到文件的日志面（C# FileLoggerOutput：追加写 + 逐行刷盘）。
pub struct FileLoggerOutput {
  writer: Mutex<BufWriter<fs::File>>,
}

impl FileLoggerOutput {
  /// C# FileLoggerOutput(string filename, int flushInterval = default)
  ///
  /// 以追加模式打开日志文件。`flush_interval` 毫秒参数对齐 C# 签名（C#
  /// 实现同样未消费该参数，逐行同步刷盘）。
  pub fn new(filename: impl AsRef<Path>, flush_interval: i32) -> io::Result<Self> {
    let _ = flush_interval;
    let file = fs::OpenOptions::new()
      .create(true)
      .append(true)
      .open(filename)?;
    Ok(Self {
      writer: Mutex::new(BufWriter::new(file)),
    })
  }

  /// C# FileLoggerOutput.Log：写一行并立即刷盘
  fn write_line(&self, msg: &str) {
    let mut writer = self.writer.lock();
    let _ = writeln!(writer, "{msg}");
    let _ = writer.flush();
  }

  /// C# FileLoggerOutput.Dispose：刷盘收尾
  pub fn dispose(&self) {
    let _ = self.writer.lock().flush();
  }
}

/// 落文件的日志提供器（C# FileLoggerProvider；[`log::Log`] 门面实现，
/// `category` 取记录的 `target`）。
#[derive(Clone)]
pub struct FileLoggerProvider {
  logger_output: Arc<FileLoggerOutput>,
}

impl FileLoggerProvider {
  /// C# FileLoggerProvider(FileLoggerOutput)
  pub fn new(logger_output: Arc<FileLoggerOutput>) -> Self {
    Self { logger_output }
  }

  /// C# FileLoggerOutput.Log
  pub fn log_record(&self, level: Level, category: &str, args: &Arguments<'_>) {
    self
      .logger_output
      .write_line(&format_record(level, category, args));
  }
}

impl Log for FileLoggerProvider {
  fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
    // C# FileLogger.IsEnabled => true
    true
  }

  fn log(&self, record: &Record<'_>) {
    if !self.enabled(record.metadata()) {
      return;
    }
    self.log_record(record.level(), record.target(), record.args());
  }

  fn flush(&self) {
    self.logger_output.dispose();
  }
}

/// 写入内存的日志收集器（C# MemoryLogger：缓存条目，事后经
/// [`Self::flush_logger`] 转发到目标日志面）。
#[derive(Default, Clone)]
pub struct MemoryLogger {
  /// 缓冲条目 `(级别, 消息)`；C# `(LogLevel, Exception, string)` 三元组中
  /// 的异常在 log 门面下经 error! 消息承载。
  memory_log: Arc<Mutex<Vec<(Level, String)>>>,
}

impl MemoryLogger {
  /// C# MemoryLogger.FlushLogger(ILogger dstLogger)
  pub fn flush_logger(&self, dst_logger: &impl Log) {
    let mut entries = self.memory_log.lock();
    for (level, message) in entries.drain(..) {
      dst_logger.log(
        &Record::builder()
          .level(level)
          .args(format_args!("{message}"))
          .target("MemoryLogger")
          .build(),
      );
    }
  }

  /// 缓冲条目数。
  pub fn len(&self) -> usize {
    self.memory_log.lock().len()
  }

  /// 是否无缓冲条目。
  pub fn is_empty(&self) -> bool {
    self.memory_log.lock().is_empty()
  }
}

impl Log for MemoryLogger {
  fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
    // C# MemoryLogger.IsEnabled => true
    true
  }

  fn log(&self, record: &Record<'_>) {
    self
      .memory_log
      .lock()
      .push((record.level(), record.args().to_string()));
  }

  fn flush(&self) {}
}

/// 内存日志收集器提供器（C# MemoryLoggerProvider：按类别 GetOrAdd 缓存）。
#[derive(Default)]
pub struct MemoryLoggerProvider {
  memory_loggers: Mutex<GxHashMap<Box<str>, Arc<MemoryLogger>>>,
}

impl MemoryLoggerProvider {
  /// C# MemoryLoggerProvider.CreateLogger(categoryName)：按类别取或建收集器
  pub fn create_logger(&self, category_name: &str) -> Arc<MemoryLogger> {
    self
      .memory_loggers
      .lock()
      .entry(category_name.into())
      .or_default()
      .clone()
  }

  /// 已创建的类别名集合。
  pub fn categories(&self) -> GxHashSet<Box<str>> {
    self.memory_loggers.lock().keys().cloned().collect()
  }

  /// C# MemoryLoggerProvider.Dispose：清空缓存
  pub fn dispose(&self) {
    self.memory_loggers.lock().clear();
  }
}

/// 日志目标静态枚举（消除虚表开销与两次堆寻址）
#[derive(Clone)]
pub enum LogTarget {
  Console(ConsoleLogger),
  File(FileLoggerProvider),
  Memory(MemoryLogger),
  Fanout(Arc<FanoutLogger>),
}

impl Log for LogTarget {
  fn enabled(&self, metadata: &Metadata<'_>) -> bool {
    match self {
      Self::Console(l) => l.enabled(metadata),
      Self::File(l) => l.enabled(metadata),
      Self::Memory(l) => l.enabled(metadata),
      Self::Fanout(l) => l.enabled(metadata),
    }
  }

  fn log(&self, record: &Record<'_>) {
    match self {
      Self::Console(l) => l.log(record),
      Self::File(l) => l.log(record),
      Self::Memory(l) => l.log(record),
      Self::Fanout(l) => l.log(record),
    }
  }

  fn flush(&self) {
    match self {
      Self::Console(l) => l.flush(),
      Self::File(l) => l.flush(),
      Self::Memory(l) => l.flush(),
      Self::Fanout(l) => l.flush(),
    }
  }
}

impl From<ConsoleLogger> for LogTarget {
  fn from(l: ConsoleLogger) -> Self {
    Self::Console(l)
  }
}

impl From<FileLoggerProvider> for LogTarget {
  fn from(l: FileLoggerProvider) -> Self {
    Self::File(l)
  }
}

impl From<MemoryLogger> for LogTarget {
  fn from(l: MemoryLogger) -> Self {
    Self::Memory(l)
  }
}

impl From<FanoutLogger> for LogTarget {
  fn from(l: FanoutLogger) -> Self {
    Self::Fanout(Arc::new(l))
  }
}

/// 装配期先行缓冲日志器：目标日志面就绪前经内存收集器缓冲，就绪后
/// [`Self::promote`] 切换直写并回灌存量（C# GarnetServer 构造器
/// initLogger 收集 ArgParser 日志、FlushMemoryLogger 转真实 loggerFactory
/// 的生命周期）。
pub struct MemoryForwardLogger {
  memory: MemoryLogger,
  destination: RwLock<Option<LogTarget>>,
}

impl MemoryForwardLogger {
  /// 以先行缓冲形态安装为全局日志器；重复安装返回 `Err`。
  pub fn install(minimum_level: LevelFilter) -> Result<Arc<Self>, log::SetLoggerError> {
    let forward = Arc::new(Self {
      memory: MemoryLogger::default(),
      destination: RwLock::new(None),
    });
    log::set_max_level(minimum_level);
    log::set_boxed_logger(Box::new(forward.clone()))?;
    Ok(forward)
  }

  /// 切换到目标日志面并回灌缓冲条目（C# FlushMemoryLogger）。
  pub fn promote(&self, destination: impl Into<LogTarget>) {
    let target = destination.into();
    *self.destination.write() = Some(target.clone());
    self.memory.flush_logger(&target);
  }

  /// 当前目标是否已就绪。
  pub fn promoted(&self) -> bool {
    self.destination.read().is_some()
  }
}

impl Log for MemoryForwardLogger {
  fn enabled(&self, metadata: &Metadata<'_>) -> bool {
    match self.destination.read().as_ref() {
      Some(dst) => dst.enabled(metadata),
      None => true,
    }
  }

  fn log(&self, record: &Record<'_>) {
    let dst = self.destination.read().clone();
    match dst {
      Some(dst) => dst.log(record),
      None => self.memory.log(record),
    }
  }

  fn flush(&self) {
    if let Some(dst) = self.destination.read().as_ref() {
      dst.flush();
    }
  }
}

/// 单行控制台日志面（C# GarnetServer 构造器 `builder.AddSimpleConsole` 的
/// 最小投影：单行格式 + `hh:mm:ss ` 时间戳，输出到 stderr）。
#[derive(Clone)]
pub struct ConsoleLogger {
  minimum_level: LevelFilter,
}

impl ConsoleLogger {
  /// 单行控制台日志面 + 全局安装；重复安装返回 `Err`。
  pub fn install(minimum_level: LevelFilter) -> Result<Self, log::SetLoggerError> {
    let logger = Self { minimum_level };
    log::set_max_level(minimum_level);
    log::set_boxed_logger(Box::new(logger.clone()))?;
    Ok(logger)
  }
}

impl Log for ConsoleLogger {
  fn enabled(&self, metadata: &Metadata<'_>) -> bool {
    metadata.level() <= self.minimum_level
  }

  fn log(&self, record: &Record<'_>) {
    if !self.enabled(record.metadata()) {
      return;
    }
    let line = format!(
      "[{}] ({}) <{target}> {args}",
      Timestamp::now().strftime("%H:%M:%S "),
      record.level(),
      target = record.target(),
      args = record.args(),
    );
    let _ = writeln!(stderr(), "{line}");
  }

  fn flush(&self) {
    let _ = stderr().flush();
  }
}

/// 日志面装配器（C# `LoggerFactory.Create(builder => { AddSimpleConsole;
/// builder.AddFile(serverSettings.FileLogger); builder.SetMinimumLevel(...); })`
/// 的 builder 投影）。
pub struct LoggingBuilder {
  /// 落文件目标（路径, 刷盘间隔毫秒）；C# serverSettings.FileLogger。
  files: Vec<(String, i32)>,
  /// 最低日志级别；C# serverSettings.LogLevel（默认 Information）。
  minimum_level: LevelFilter,
  /// 是否禁用控制台输出；C# serverSettings.DisableConsoleLogger。
  disable_console: bool,
}

impl Default for LoggingBuilder {
  fn default() -> Self {
    Self::new()
  }
}

impl LoggingBuilder {
  /// 新装配器：默认控制台 + Information 级别（C# 构造默认值）。
  pub fn new() -> Self {
    Self {
      files: Vec::new(),
      minimum_level: LevelFilter::Info,
      disable_console: false,
    }
  }

  /// builder.AddFile：追加落文件目标
  pub fn add_file(mut self, filename: impl Into<String>, flush_interval: i32) -> Self {
    self.files.push((filename.into(), flush_interval));
    self
  }

  /// builder.SetMinimumLevel
  pub fn with_minimum_level(mut self, minimum_level: LevelFilter) -> Self {
    self.minimum_level = minimum_level;
    self
  }

  /// serverSettings.DisableConsoleLogger
  pub fn disable_console(mut self) -> Self {
    self.disable_console = true;
    self
  }

  /// 安装全局日志面（C# GarnetServer 构造器的 loggerFactory 装配段）：
  /// 控制台 + 全部落文件目标组合为扇出日志器；全局 logger 仅可安装一次，
  /// 重复安装返回 `Err`。
  pub fn install(self) -> Result<(), log::SetLoggerError> {
    let destinations: Vec<LogTarget> = (!self.disable_console)
      .then(|| {
        LogTarget::Console(ConsoleLogger {
          minimum_level: self.minimum_level,
        })
      })
      .into_iter()
      .chain(self.files.into_iter().filter_map(|(path, interval)| {
        // 文件打开失败（目录缺失 / 权限）按 C# 异常装配面收敛为跳过该目标
        FileLoggerOutput::new(&path, interval)
          .ok()
          .map(|output| LogTarget::File(FileLoggerProvider::new(Arc::new(output))))
      }))
      .collect();

    log::set_max_level(self.minimum_level);
    log::set_boxed_logger(Box::new(FanoutLogger {
      destinations: destinations.into_boxed_slice(),
    }))
  }
}

/// 多目标扇出日志器（C# ILoggerFactory 聚合多个 Provider 的广播语义）。
#[derive(Default, Clone)]
pub struct FanoutLogger {
  destinations: Box<[LogTarget]>,
}

impl Log for FanoutLogger {
  fn enabled(&self, metadata: &Metadata<'_>) -> bool {
    self.destinations.iter().any(|dst| dst.enabled(metadata))
  }

  fn log(&self, record: &Record<'_>) {
    for dst in &self.destinations {
      dst.log(record);
    }
  }

  fn flush(&self) {
    for dst in &self.destinations {
      dst.flush();
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{env::temp_dir, process::id};

  use log::Level;

  use super::*;

  #[test]
  fn formatter_shapes_timestamp() {
    let stamp = "2023-11-14T22:13:20.123456789Z"
      .parse::<Timestamp>()
      .unwrap();
    let date = LogFormatter::format_date(stamp);
    assert_eq!(date, "2023-11-14 22:13:20.1234");
    assert_eq!(LogFormatter::format_time(stamp), "22:13:20.1234");
  }

  #[test]
  fn memory_logger_collects_then_flushes() {
    let provider = MemoryLoggerProvider::default();
    let logger = provider.create_logger("ArgParser");
    logger.log(
      &Record::builder()
        .level(Level::Warn)
        .args(format_args!("装配告警"))
        .target("ArgParser")
        .build(),
    );
    assert_eq!(logger.len(), 1);
    assert_eq!(provider.categories().len(), 1);

    let sink = MemoryLogger::default();
    logger.flush_logger(&sink);
    assert!(logger.is_empty());
    assert_eq!(sink.len(), 1);
    provider.dispose();
    assert!(provider.categories().is_empty());
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
    let forward =
      MemoryForwardLogger::install(LevelFilter::Trace).expect("全局日志器仅测试进程首次安装");
    assert!(!forward.promoted());
    forward.log(
      &Record::builder()
        .level(Level::Error)
        .args(format_args!("先行缓冲"))
        .target("test")
        .build(),
    );
    assert_eq!(forward.memory.len(), 1);

    let dst = MemoryLogger::default();
    forward.promote(dst.clone());
    assert!(forward.promoted());
    assert!(forward.memory.is_empty());
    assert_eq!(dst.len(), 1);
  }
}
