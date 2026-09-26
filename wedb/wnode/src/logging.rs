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
//!   libs/host/GarnetServer.cs:FlushMemoryLogger）；宿主 main 在参数解析前
//!   [`MemoryForwardLogger::install`]、日志面装配处
//!   [`LoggingBuilder::flush_into`] 回灌，为全仓唯一的日志装配链
//! - [`LoggingBuilder`]：日志面装配器（C# LoggerFactory.Create(builder => …)
//!   的 AddSimpleConsole / AddFile / SetMinimumLevel 投影）

use std::{
  fmt::Arguments,
  fs, io,
  io::{BufWriter, Write, stderr},
  path::Path,
  sync::Arc,
};

use jiff::Timestamp;
use log::{Level, LevelFilter, Log, Metadata, Record};
use parking_lot::{Mutex, RwLock};
use wbase::map::HashMap as GxHashMap;
use wconf::{DEFAULT_LOG_FLUSH_INTERVAL, NodeArgs};

/// C# 事件 id 无 log 门面对应物，记录行以时间戳开头（C# `[{eventId:D3}.{date}]`
/// 的事件段省略）。
const RECORD_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// 先行缓冲收集器的类别名（C# GarnetServer 构造器 :88
/// `CreateLogger("ArgParser")`：参数解析期日志的登记类别）。
const INIT_LOG_CATEGORY: &str = "ArgParser";

/// libs/common/Logging/LogFormatter.cs:LogFormatter
///
/// 日志时间格式化原语（日期 `yyyy-MM-dd HH:mm:ss.ffff`、时间 `HH:mm:ss.ffff`）。
pub(crate) struct LogFormatter;

impl LogFormatter {
  /// libs/common/Logging/LogFormatter.cs:FormatDate（`yyyy-MM-dd HH:mm:ss.ffff`）
  pub fn format_date(time: Timestamp) -> String {
    format!(
      "{}.{:04}",
      time.strftime(RECORD_TIME_FORMAT),
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
  /// 先行缓冲收集器（C# GarnetServer 构造器 :88 的 `initLogger`：经
  /// [`MemoryLoggerProvider`] 按 [`INIT_LOG_CATEGORY`] 取得，提供器随即弃置）。
  memory: Arc<MemoryLogger>,
  destination: RwLock<Option<LogTarget>>,
}

impl MemoryForwardLogger {
  /// 以先行缓冲形态安装为全局日志器（宿主 main 在参数解析前调用，对标 C#
  /// GarnetServer 构造器 :86-88 的 initLogger 段）；重复安装返回 `Err`。
  ///
  /// 缓冲阶段收全量记录（C# `MemoryLogger.IsEnabled => true`），级别过滤由
  /// [`LoggingBuilder::flush_into`] 装配的目标面与 `log` 门面阈值承接。
  pub fn install() -> Result<Arc<Self>, log::SetLoggerError> {
    // C# :86-88 `using (var memLogProvider = new MemoryLoggerProvider())` →
    // `CreateLogger("ArgParser")`：提供器出作用域即清表（其 Dispose），收集器
    // 由本日志器持有存续
    let provider = MemoryLoggerProvider::default();
    let memory = provider.create_logger(INIT_LOG_CATEGORY);
    provider.dispose();
    let forward = Arc::new(Self {
      memory,
      destination: RwLock::new(None),
    });
    log::set_max_level(LevelFilter::Trace);
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

  /// 节点参数驱动的日志装配单点（对标 C# GarnetServer.cs:115-130 构造器
  /// loggerFactory 段：控制台（DisableConsoleLogger 未禁用时 AddSimpleConsole，
  /// `!serverSettings.DisableConsoleLogger.GetValueOrDefault()`）+ 可选落文件
  /// （serverSettings.FileLogger，刷盘间隔取 C# 省略形参的
  /// [`DEFAULT_LOG_FLUSH_INTERVAL`]）+ 最低级别（serverSettings.LogLevel））。
  pub fn from_node(node: &NodeArgs) -> Self {
    let mut logging = Self::new().with_minimum_level(node.minimum_log_level());
    // DisableConsoleLogger 门禁（C# GarnetServer.cs:115-122）：置位即不装控制台 sink
    if node.disable_console_logger {
      logging = logging.disable_console();
    }
    if let Some(file) = &node.file_logger {
      logging = logging.add_file(file, DEFAULT_LOG_FLUSH_INTERVAL);
    }
    logging
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

  /// 生成日志面（C# GarnetServer 构造器的 loggerFactory 段）：控制台 + 全部落
  /// 文件目标组合为扇出日志器，并把 `log` 门面阈值收口到
  /// [`Self::minimum_level`]。不触碰全局安装口——全局 logger 唯一实例是
  /// [`MemoryForwardLogger`]，目标面经 [`Self::flush_into`] 挂上去。
  ///
  /// 日志文件无法创建（目录缺失 / 权限）时返回 `Err`：对标 C#
  /// FileLoggerProvider.cs:50 构造期 `File.Open` 抛错致进程启动失败的拒启
  /// 语义，严禁静默吞错丢日志。
  fn build(self) -> io::Result<FanoutLogger> {
    let mut destinations: Vec<LogTarget> = Vec::with_capacity(1 + self.files.len());
    // DisableConsoleLogger 门禁（C# GarnetServer.cs:115-122）：未禁用才装控制台
    if !self.disable_console {
      destinations.push(LogTarget::Console(ConsoleLogger {
        minimum_level: self.minimum_level,
      }));
    }
    // 文件目标逐个创建，打开失败显式向上抛（C# File.Open 抛错拒启对位）
    for (path, interval) in self.files {
      let output = FileLoggerOutput::new(&path, interval)?;
      destinations.push(LogTarget::File(FileLoggerProvider::new(Arc::new(output))));
    }

    log::set_max_level(self.minimum_level);
    Ok(FanoutLogger {
      destinations: destinations.into_boxed_slice(),
    })
  }

  /// 日志装配收口（C# GarnetServer 构造器 loggerFactory 段 + `FlushMemoryLogger`
  /// 调用点 :97/:232）：生成正式日志面，挂到参数解析前
  /// [`MemoryForwardLogger::install`] 装好的先行缓冲上，并回灌装配前暂存的条目；
  /// 其后的记录直写目标面。
  ///
  /// 全仓仅此一条日志装配链：`log` 全局 logger 自始至终是先行缓冲日志器，
  /// 不存在绕过它的第二套安装口。日志文件无法创建时返回 `Err` 且不挂目标面
  /// （C# File.Open 抛错拒启对位），宿主 main 转换 `Error::LogInstall` 退出。
  pub fn flush_into(self, forward: &MemoryForwardLogger) -> io::Result<()> {
    forward.promote(self.build()?);
    Ok(())
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
  }

  #[test]
  fn builder_from_node_projects_logger_args() {
    // 日志装配单点（C# GarnetServer.cs:115-130 构造器 loggerFactory 段）：
    // log_level → 最低级别，file_logger → 落文件目标（刷盘间隔取常量单点）
    let file = format!("/tmp/wnode-from-node-{}.log", id());
    let node = NodeArgs {
      file_logger: Some(file.clone()),
      log_level: Some("debug".into()),
      ..NodeArgs::default()
    };
    let logging = LoggingBuilder::from_node(&node);
    assert_eq!(logging.minimum_level, LevelFilter::Debug);
    assert_eq!(logging.files, vec![(file, DEFAULT_LOG_FLUSH_INTERVAL)]);

    // 未配置 file_logger：仅控制台目标，级别缺省 Warning（对标 defaults.conf:280）
    let logging = LoggingBuilder::from_node(&NodeArgs::default());
    assert!(logging.files.is_empty());
    assert_eq!(logging.minimum_level, LevelFilter::Warn);
    // 缺省控制台开（C# DisableConsoleLogger 未禁用）
    assert!(!logging.disable_console);
  }

  #[test]
  fn from_node_wires_disable_console_logger() {
    // DisableConsoleLogger 接线（C# GarnetServer.cs:115-122 门禁的参数入口：
    // `!serverSettings.DisableConsoleLogger.GetValueOrDefault()` 才装控制台）
    let node = NodeArgs {
      disable_console_logger: true,
      ..NodeArgs::default()
    };
    assert!(LoggingBuilder::from_node(&node).disable_console);
  }

  #[test]
  fn build_gates_console_sink_and_propagates_file_error() {
    // 禁控制台 + 单文件：目标面只含 File sink，无 Console（C# GarnetServer.cs:115-122）
    let dir = temp_dir().join(format!("wnode-build-gate-{}", id()));
    fs::create_dir_all(&dir).unwrap();
    let face = LoggingBuilder::new()
      .disable_console()
      .add_file(dir.join("gate.log").display().to_string(), 0)
      .build()
      .unwrap();
    assert_eq!(face.destinations.len(), 1);
    assert!(matches!(face.destinations[0], LogTarget::File(_)));

    // 缺省装配：仅控制台一个目标
    let face = LoggingBuilder::new().build().unwrap();
    assert_eq!(face.destinations.len(), 1);
    assert!(matches!(face.destinations[0], LogTarget::Console(_)));
    fs::remove_dir_all(&dir).ok();

    // 文件不可建（目录缺失）→ 明确 IO 错误向上抛，不再 .ok() 静默成功
    //（C# FileLoggerProvider.cs:50 构造期 File.Open 抛错拒启）
    let missing = temp_dir()
      .join(format!("wnode-missing-dir-{}", id()))
      .join("no.log");
    let err = LoggingBuilder::new()
      .add_file(missing.display().to_string(), 0)
      .build()
      .err()
      .expect("目录缺失时 build 必须报 IO 错");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    // flush_into 同面：装配失败时不挂目标面、错误上抛（宿主转 LogInstall 拒启）
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
    // 同类别取回同一收集器（C# GetOrAdd 语义）
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

    // C# `using` 作用域出表（Dispose 清类别表）：同类别另起新收集器
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
}
