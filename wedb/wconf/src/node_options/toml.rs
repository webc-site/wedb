//! TOML/命令行三层配置解析与合并：from_toml 族、显式项覆盖与服务端参数多态契约
//!
//! 对标 C# libs/host/ServerSettingsManager.cs:TryParseCommandLineArguments 的
//! 结构体默认值 → --config toml 文件 → 命令行显式项覆盖三层合并。

use std::{
  ffi::OsString,
  fs,
  path::{Path, PathBuf},
  process::exit,
  sync::Arc,
};

use clap::{ArgMatches, error::ErrorKind};
use toml_spanner::Arena;
use wbase::cfg::LogCompactionType;

use crate::node_options::{NodeArgs, NodeOptionsError};

/// 命令行档名解析（对标 C# Options.cs:271 `--compaction-type` 的 Enum.Parse
/// 忽略大小写语义；复用 wbase::cfg::LogCompactionType::try_parse 单点，非法
/// 档名在 clap 解析期即拒启）
pub(super) fn parse_log_compaction_type(value: &str) -> Result<LogCompactionType, String> {
  LogCompactionType::try_parse(value)
    .ok_or_else(|| format!("compaction-type 取值须为 None/Shift/Lookup/Scan 之一，当前为 {value}"))
}

pub mod toml_log_compaction_type {
  use toml_spanner::{Arena, Context, Failed, Item, ToTomlError};
  use wbase::cfg::LogCompactionType;

  pub fn to_toml<'a>(value: &LogCompactionType, arena: &'a Arena) -> Result<Item<'a>, ToTomlError> {
    Ok(Item::string(arena.alloc_str(value.as_name())))
  }

  pub fn from_toml<'de>(
    ctx: &mut Context<'de>,
    item: &Item<'de>,
  ) -> Result<LogCompactionType, Failed> {
    let Some(s) = item.as_str() else {
      return Err(ctx.report_expected_but_found(&"a string", item));
    };
    LogCompactionType::try_parse(s).ok_or_else(|| {
      ctx.report_custom_error(
        format!("compaction-type 取值须为 None/Shift/Lookup/Scan 之一，当前为 {s}"),
        item,
      )
    })
  }
}

impl NodeArgs {
  /// 从 TOML 字符串解析配置（唯一的配置文件格式）
  pub fn from_toml_str(s: &str) -> Result<Self, NodeOptionsError> {
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(s, &arena)?;
    Ok(doc.to()?)
  }

  /// 从配置文件加载配置
  pub fn from_file(path: impl AsRef<Path>) -> Result<Self, NodeOptionsError> {
    let content = fs::read_to_string(path.as_ref())?;
    Self::from_toml_str(&content)
  }

  /// 命令行显式项覆盖文件/默认基线
  ///
  /// clap_derive 的 arg id 为字段名原样（long 才是 kebab-case），故以
  /// stringify!(字段名) 作 value_source 查询键；hlog 配置段为 flatten 嵌套，
  /// id 前缀 hlog_ 单独合并。
  ///
  /// C# 侧以整对象二次解析（工厂 = 文件合并后的 Options）自动覆盖全部显式
  /// 项，本处为手工逐字段清单——NodeArgs 新增字段必须同步入列，漏项即
  /// 配合 --config 时该 CLI 显式项被静默丢弃
  pub fn override_explicit(&mut self, matches: &ArgMatches, cli: Self) {
    use clap::parser::ValueSource;
    macro_rules! over {
      // 顶层字段：clap arg id 即字段名
      ($($f:ident),+ $(,)?) => {
        $(if matches.value_source(stringify!($f)) == Some(ValueSource::CommandLine) {
          self.$f = cli.$f.clone();
        })+
      };
      // flatten 嵌套段字段：id 与字段路径不同源（hlog 段部分带 hlog_ 前缀），逐点给出
      ($($id:literal => $sec:ident . $f:ident),+ $(,)?) => {
        $(if matches.value_source($id) == Some(ValueSource::CommandLine) {
          self.$sec.$f = cli.$sec.$f.clone();
        })+
      };
    }
    over![
      bind,
      port,
      unixsocket,
      unixsocket_perm,
      dir,
      wal_dir,
      checkpoint_dir,
      requirepass,
      tls_cert,
      tls_key,
      tls_client_cert_required,
      tls_client_target_host,
      tls_server_cert_required,
      tls_issuer_cert,
      tls_cert_refresh_freq,
      threads,
      network_connection_limit,
      network_buffer_memory_budget,
      network_buffer_memory_budget,
      aof,
      disable_pubsub,
      recover,
      aof_commit_ms,
      aof_commit_wait,
      repl_diskless_sync,
      fast_aof_truncate,
      on_demand_checkpoint,
      file_logger,
      log_level,
      quiet,
      disable_console_logger,
      slow_log_threshold,
      slow_log_max_entries,
      max_databases,
      protected_mode,
      enable_debug_command,
      object_scan_count_limit,
      metrics_sampling_frequency_secs,
      latency_monitor,
      commandstats_monitor,
      enable_lua,
      lua_script_timeout_ms,
      lua_memory_management_mode,
      lua_script_memory_limit,
      lua_logging_mode,
      lua_allowed_functions,
      aof_size_limit,
      aof_memory_size,
      aof_page_size,
      aof_segment_size,
      aof_size_limit_enforce_frequency_secs,
      index_max_size,
      index_resize_frequency_secs,
      index_resize_threshold,
      replica_sync_timeout_secs,
      replica_attach_timeout_secs,
      replica_sync_delay_ms,
      aof_sync_max_lag_bytes,
      aof_tail_witness_freq_ms,
      cluster_replication_reestablishment_timeout,
      vector_set_quantization_task_count,
      compaction_type,
      compaction_max_segments,
      enable_scatter_gather_get,
      aof_replay_max_lag_bytes,
      replica_diskless_sync_delay,
      enable_vector_set_preview,
      cluster_announce_hostname,
      cluster_username,
      cluster_password,
      expired_object_collection_frequency_secs,
      expired_key_deletion_scan_frequency_secs,
    ];
    // hlog 配置段：flatten 嵌套字段，arg id 带 hlog_ 前缀（显式 CLI 项覆盖）
    over![
      "hlog_page_size" => hlog.page_size,
      "hlog_memory_size" => hlog.memory_size,
      "hlog_mutable_percent" => hlog.mutable_percent,
      "read_cache" => hlog.read_cache,
      "read_cache_memory_size" => hlog.read_cache_memory_size,
      "tree_cache_budget" => hlog.tree_cache_budget,
      "reviv" => hlog.reviv,
      "reviv_fraction" => hlog.reviv_fraction,
      "copy_reads_to_tail" => hlog.copy_reads_to_tail,
    ];
  }

  /// 导出生效配置到 TOML 文件（对标 Options.cs:501 ConfigExportPath；
  /// 导出全量字段差异见 doc/zh/deviations.md 第 76 条）
  pub fn export_config(&self, path: &Path) -> Result<(), NodeOptionsError> {
    let toml_str = self.to_toml_string()?;
    fs::write(path, toml_str)?;
    Ok(())
  }

  /// 将配置序列化为 TOML 字符串
  pub fn to_toml_string(&self) -> Result<String, NodeOptionsError> {
    Ok(toml_spanner::to_string(self)?)
  }
}

/// 三层配置合并解析契约：结构体默认值 → --config toml 文件 → 命令行显式项覆盖
///
/// 对标 libs/host/ServerSettingsManager.cs:TryParseCommandLineArguments：C#
/// 命令行解析两遍，第二遍以文件合并后的对象为工厂仅覆盖显式给出的项；
/// Rust 以 ArgMatches::value_source 判定显式项一次合并完成。
pub trait ConfigFileArgs: clap::CommandFactory + clap::FromArgMatches + Sized {
  /// 从 matches 物化：配置文件基线 + 命令行显式项覆盖
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError>;

  /// 从命令行参数迭代器解析（首个元素为程序名）
  fn from_args_iter<I, T>(itr: I) -> Result<Self, NodeOptionsError>
  where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
  {
    let matches = Self::command().try_get_matches_from(itr)?;
    Self::from_layered_matches(&matches)
  }

  /// 命令行解析入口收口：`--help` / `--version` 请求走用户交互路径——干净全文
  /// 打 stdout、进程以 0 退出（对标 C# ServerSettingsManager.cs:237-258
  /// TryParseCommandLineArguments 的 Console.WriteLine(helpText) 与
  /// GarnetServer.cs:92-94 exitGracefully → Environment.Exit(0)）；其余解析
  /// 错误维持原错误路径交由调用方处置
  fn from_args_iter_or_exit<I, T>(itr: I) -> Result<Self, NodeOptionsError>
  where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
  {
    match Self::from_args_iter(itr) {
      Err(NodeOptionsError::Cli(e))
        if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) =>
      {
        let _ = e.print();
        exit(0);
      }
      other => other,
    }
  }
}

impl ConfigFileArgs for NodeArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    let cli = <Self as clap::FromArgMatches>::from_arg_matches(matches)?;
    // config / config_export_path 为 CLI 专属（serde skip，文件基线恒 None），
    // 合并前先行拷出
    let (import_path, export_path) = (cli.config.clone(), cli.config_export_path.clone());
    let mut merged = match import_path.as_deref() {
      Some(path) => Self::from_file(path)?,
      None => Self::default(),
    };
    merged.override_explicit(matches, cli);
    merged.config = import_path;
    merged.config_export_path = export_path;
    merged.validate()?;
    if let Some(path) = &merged.config_export_path
      && let Err(err) = merged.export_config(path)
    {
      log::warn!("导出配置到 {} 失败: {err}，继续启动", path.display());
    }
    Ok(merged)
  }
}

/// 服务端参数多态契约（泛型解耦单机与集群扩展参数）
pub trait ServerArgs: Send + Sync + 'static {
  /// 获取通用节点参数引用
  fn node_args(&self) -> &NodeArgs;

  /// 获取配置的所有网络监听端点
  #[inline]
  fn endpoints(&self) -> Result<Vec<String>, NodeOptionsError> {
    self.node_args().endpoints()
  }

  /// 获取服务监听端口
  #[inline]
  fn port(&self) -> u16 {
    self.node_args().port
  }

  /// 获取工作线程数
  #[inline]
  fn threads(&self) -> Option<usize> {
    self.node_args().threads
  }

  /// 获取 WAL 日志存储目录
  #[inline]
  fn wal_dir(&self) -> PathBuf {
    self.node_args().wal_dir()
  }

  /// 是否启用 AOF 持久化日志
  #[inline]
  fn aof(&self) -> bool {
    self.node_args().aof
  }

  /// 是否启用 Vector Set 预览
  #[inline]
  fn enable_vector_set_preview(&self) -> bool {
    self.node_args().enable_vector_set_preview
  }
}

impl ServerArgs for NodeArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    self
  }
}

impl<T: ServerArgs> ServerArgs for Arc<T> {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    (**self).node_args()
  }
}
