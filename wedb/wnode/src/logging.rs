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
pub const INIT_LOG_CATEGORY: &str = "ArgParser";

/// libs/common/Logging/LogFormatter.cs:LogFormatter
///
/// libs/common/Logging/LogFormatter.cs:FormatTime（时间形 `HH:mm:ss.ffff` 由
/// 本家 strftime 内联承接，见 log_message 渲染点）
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
  pub memory_log: Arc<Mutex<Vec<(Level, String)>>>,
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
  /// 单行控制台日志面（C# GarnetServer 构造器 `builder.AddSimpleConsole` 的
  /// 最小投影：单行格式 + `hh:mm:ss ` 时间戳，输出到 stderr；级别过滤由
  /// `minimum_level` 承接 SetMinimumLevel）
  Console {
    minimum_level: LevelFilter,
  },
  File(FileLoggerProvider),
  Memory(MemoryLogger),
  /// 多目标扇出（C# ILoggerFactory 聚合多个 Provider 的广播语义）
  Fanout {
    destinations: Box<[LogTarget]>,
  },
}

impl Log for LogTarget {
  fn enabled(&self, metadata: &Metadata<'_>) -> bool {
    match self {
      Self::Console { minimum_level } => metadata.level() <= *minimum_level,
      Self::File(l) => l.enabled(metadata),
      Self::Memory(l) => l.enabled(metadata),
      Self::Fanout { destinations } => destinations.iter().any(|dst| dst.enabled(metadata)),
    }
  }

  fn log(&self, record: &Record<'_>) {
    match self {
      Self::Console { minimum_level } => {
        if record.level() > *minimum_level {
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
      Self::File(l) => l.log(record),
      Self::Memory(l) => l.log(record),
      Self::Fanout { destinations } => {
        for dst in destinations {
          dst.log(record);
        }
      }
    }
  }

  fn flush(&self) {
    match self {
      Self::Console { .. } => {
        let _ = stderr().flush();
      }
      Self::File(l) => l.flush(),
      Self::Memory(l) => l.flush(),
      Self::Fanout { destinations } => {
        for dst in destinations {
          dst.flush();
        }
      }
    }
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

/// 装配期先行缓冲日志器：目标日志面就绪前经内存收集器缓冲，就绪后
/// [`Self::promote`] 切换直写并回灌存量（C# GarnetServer 构造器
/// initLogger 收集 ArgParser 日志、FlushMemoryLogger 转真实 loggerFactory
/// 的生命周期）。
pub struct MemoryForwardLogger {
  /// 先行缓冲收集器（C# GarnetServer 构造器 :88 的 `initLogger`：经
  /// [`MemoryLoggerProvider`] 按 [`INIT_LOG_CATEGORY`] 取得，提供器随即弃置）。
  pub memory: Arc<MemoryLogger>,
  pub destination: RwLock<Option<LogTarget>>,
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
    // 读守卫直派（目标面 log 不回环触 destination，锁内持有与 enabled 同形；
    // 免逐条克隆目标面，Fanout 臂零拷贝）
    match self.destination.read().as_ref() {
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

/// 日志面装配器（C# `LoggerFactory.Create(builder => { AddSimpleConsole;
/// builder.AddFile(serverSettings.FileLogger); builder.SetMinimumLevel(...); })`
/// 的 builder 投影）。
pub struct LoggingBuilder {
  /// 落文件目标（路径, 刷盘间隔毫秒）；C# serverSettings.FileLogger。
  pub files: Vec<(String, i32)>,
  /// 最低日志级别；C# serverSettings.LogLevel（默认 Information）。
  pub minimum_level: LevelFilter,
  /// 是否禁用控制台输出；C# serverSettings.DisableConsoleLogger。
  pub disable_console: bool,
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

  /// builder.SetMinimumLevel（装配期单源，[`Self::from_node`] 经此投影节点级别）
  fn with_minimum_level(mut self, minimum_level: LevelFilter) -> Self {
    self.minimum_level = minimum_level;
    self
  }

  /// serverSettings.DisableConsoleLogger
  pub fn disable_console(mut self) -> Self {
    self.disable_console = true;
    self
  }

  /// 生成日志面（C# GarnetServer 构造器的 loggerFactory 段）：控制台 + 全部落
  /// 文件目标组合为扇出日志面，并把 `log` 门面阈值收口到
  /// [`Self::minimum_level`]。不触碰全局安装口——全局 logger 唯一实例是
  /// [`MemoryForwardLogger`]，目标面经 [`Self::flush_into`] 挂上去；单目标
  /// 免扇出包装直还目标本身（C# 单 Provider 工厂同形）。
  ///
  /// 日志文件无法创建（目录缺失 / 权限）时返回 `Err`：对标 C#
  /// FileLoggerProvider.cs:50 构造期 `File.Open` 抛错致进程启动失败的拒启
  /// 语义，严禁静默吞错丢日志。
  pub fn build(self) -> io::Result<LogTarget> {
    let mut destinations: Vec<LogTarget> = Vec::with_capacity(1 + self.files.len());
    // DisableConsoleLogger 门禁（C# GarnetServer.cs:115-122）：未禁用才装控制台
    if !self.disable_console {
      destinations.push(LogTarget::Console {
        minimum_level: self.minimum_level,
      });
    }
    // 文件目标逐个创建，打开失败显式向上抛（C# File.Open 抛错拒启对位）
    for (path, interval) in self.files {
      let output = FileLoggerOutput::new(&path, interval)?;
      destinations.push(LogTarget::File(FileLoggerProvider::new(Arc::new(output))));
    }

    log::set_max_level(self.minimum_level);
    // 单目标直还；其余（含零目标空面）扇出包装（len==1 已判，swap_remove 不越界）
    Ok(if destinations.len() == 1 {
      destinations.swap_remove(0)
    } else {
      LogTarget::Fanout {
        destinations: destinations.into_boxed_slice(),
      }
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
