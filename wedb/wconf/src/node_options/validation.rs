//! 节点参数校验：错误类型、日志级别解析与 [`NodeArgs::validate`] 定界漏斗
//!
//! 对标 C# OptionsValidators.cs + ServerSettingsManager.cs:149 options.IsValid 的
//! IntRangeValidation 漏斗末端校验；validate 按主题拆私有子 fn，检查顺序与
//! 原单函数逐段一致（错误优先级不变）。

use std::io::Error;

use log::LevelFilter;
use wbase::cfg::{MAX_DATABASES_MAX, MAX_DATABASES_MIN};

use crate::{
  lua_option_modes::LuaMemoryManagementMode,
  node_options::{DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS, NodeArgs},
  size::{is_flag_size_str, previous_power_of_2, try_parse_size},
};

/// unixsocketperm 八进制数字字面上界（对标 C# Options.cs:683
/// IntRangeValidation(0, 777)：十进制写法即八进制口径，600 表 0o600）
pub(crate) const UNIX_SOCKET_PERM_MAX: i32 = 777;

/// 索引内存上限的最小合法字节（对标 ServerOptions.cs:208 IndexSizeCachelines
/// 的 `adjustedSize < 64` 拒绝界；64B 恰为一桶）
pub const INDEX_MAX_SIZE_MIN_BYTES: i64 = 64;
/// 索引内存上限的最大合法字节（对标 ServerOptions.cs:208 `adjustedSize > (1L << 37)`
/// 拒绝界）
pub const INDEX_MAX_SIZE_MAX_BYTES: i64 = 1 << 37;
/// 慢日志阈值非零时的最小合法微秒数（对标 Options.cs:860-863
/// `SlowLogThreshold > 0 && < 100` 抛「must be at least 100 microseconds」）
pub const SLOW_LOG_THRESHOLD_MIN_MICROS: i32 = 100;

/// 节点参数加载错误（命令行解析、NestedText 配置解析、hlog 校验与文件读取；依赖库错误透明转发）。
#[derive(Debug, thiserror::Error)]
pub enum NodeOptionsError {
  /// 命令行解析失败。
  #[error(transparent)]
  Cli(#[from] clap::Error),
  /// TOML 配置文件解析失败。
  #[error(transparent)]
  Toml(#[from] toml_spanner::Error),
  /// TOML 反序列化失败。
  #[error(transparent)]
  TomlDe(#[from] toml_spanner::FromTomlError),
  /// TOML 序列化失败。
  #[error(transparent)]
  TomlSer(#[from] toml_spanner::ToTomlError),
  /// 配置文件读取失败。
  #[error(transparent)]
  Io(#[from] Error),
  /// hlog 配置段显式项校验失败。
  #[error("hlog 配置非法: {0}")]
  Hlog(String),
  /// 数值配置项越界（对标 C# RangeValidationAttribute 校验失败的启动期拒绝面）。
  #[error("{0} expected to be in range [{1}, {2}]. Actual value: {3}")]
  ValueOutOfRange(&'static str, i32, i32, i32),
  /// 尺寸字符串非法（活臂：尺寸解析实际产出口，见 [`NodeOptionsError::InvalidSize`]）。
  #[error("尺寸参数 {0} 格式非法: {1}")]
  InvalidSizeStr(&'static str, String),
  #[error("{0} expected to be at least {1}. Actual value: {2}")]
  SizeOutOfRange(&'static str, i64, i64),
  /// AOF 提交组合非法（对标 C# GarnetServer.cs:508 CreateAOF 的
  /// 「!EnableAOF && (CommitFrequencyMs != 0 || WaitForCommit) 即拒启」：
  /// AOF 未开时提交节拍与提交等待档皆无生效面，静默失效即语义欺骗）。
  #[error("未启用 AOF 时不可配置 aof-commit-ms / aof-commit-wait")]
  AofCommitWithoutAof,
  /// unixsocketperm 含非八进制数字位（对标 C# Options.cs:817
  /// Convert.ToInt32(_, 8) 对含 8/9 数字的转换失败面；0-777 界校验复用
  /// [`NodeOptionsError::ValueOutOfRange`]）。
  #[error("unixsocketperm 须由八进制数字（0-7）组成: {0}")]
  UnixSocketPermDigits(i32),
  /// 延迟监视缺采样节拍（对标 C# GarnetServerOptions.cs:839-840
  /// `LatencyMonitor && MetricsSamplingFrequency == 0` 即拒启：延迟表随监视器
  /// 采样循环聚合并周期落盘，节拍为 0 时监视器不启动，开关静默失效）
  #[error("LatencyMonitor requires MetricsSamplingFrequency to be set")]
  LatencyMonitorWithoutMetrics,
  /// FastAofTruncate 要求手动提交（对标 C# GarnetServer.cs:513-515
  /// `FastAofTruncate requires CommitFrequencyMs to be -1`）。
  #[error("FastAofTruncate requires manual commit (CommitFrequencyMs = -1)")]
  FastAofTruncateRequiresManualCommit,
  /// 手动提交不可与 aof_commit_wait 联用（对标 C# GarnetServer.cs:517-519
  /// `WaitForCommit cannot be used with manual commit`）。
  #[error("WaitForCommit cannot be used with manual commit (CommitFrequencyMs < 0)")]
  CommitWaitWithManualCommit,
  /// 尺寸字符串非法（对标 C# ServerOptions.cs:206-211 IndexSizeCachelines 与
  /// AofSizeLimitSizeBits 的 `ParseSize` 解析失败 / 取 2 的幂后越出合法档位即抛
  /// 的启动期拒启面）：点名参数与实际字符串，杜绝投影侧静默跳过的第二套口径。
  ///
  /// C# 对位保留变体：生产零构造，活臂为 [`NodeOptionsError::InvalidSizeStr`]，
  /// 勿经本变体新增构造（E1 票判据「对位存在=保留」）。
  #[error("尺寸参数 {0}=\"{1}\" 非法：解析失败或取 2 的幂后越出合法档位")]
  InvalidSize(&'static str, String),
  /// bind 条目格式非法（对标 C# OptionsValidators.cs:373 "Expected string in IPv4 / IPv6 format ... Actual value: ..."）。
  #[error(
    "Expected string in IPv4 / IPv6 format (e.g. 127.0.0.1 / 0:0:0:0:0:0:0:1) or 'localhost' or valid hostname. Actual value: {0}"
  )]
  InvalidAddress(String),
  /// Lua 脚本超时越界（对标 C# Options.cs:651 IntRangeValidation(10, int.MaxValue,
  /// isRequired: false) 的启动期拒绝面：0 = 禁用为合法缺省，其余须落 [10, 2147483647]）。
  #[error(
    "lua-script-timeout expected to be in range [10, 2147483647] or 0 (disabled). Actual value: {0}"
  )]
  LuaScriptTimeoutOutOfRange(i64),
  /// Lua 内存限额与 Native 模式互斥（对标 C# Options.cs:645 ForbiddenWithOption
  /// (LuaMemoryManagementMode.Native)：Native 档不感知宿主分配，限额无生效面，
  /// 同设即语义欺骗，启动拒启）。
  #[error("lua-script-memory-limit 不可与 lua-memory-management-mode = native 同时设置")]
  LuaMemoryLimitWithNative,
  /// 日志级别配置非法（对标 C# Options.cs:366-367 LogLevel 枚举解析失败的拒启面）。
  #[error(
    "log-level 取值须为 Trace/Debug/Information/Warning/Error/Critical/None（或别名 verbose/notice/nothing、数字 0-6）之一，当前为 \"{0}\""
  )]
  InvalidLogLevel(String),
  /// NodeArgs 时间旋钮越 C# 契约带上界（对标 Options.cs:464-466
  /// ReplicaAttachTimeout、:358-360 MetricsSamplingFrequency、:616-618
  /// IndexResizeFrequencySecs 三处 IntRangeValidation 上界 int.MaxValue 秒的
  /// 启动期拒绝面）：越界正值经投影原样进秒槽、由 get_time_span /
  /// Duration::from_secs 直送 compio 定时器，入口先拒。首参点名 CLI 长名，
  /// 次参携实际值（拒收臂恒为正，u64 无损承载 i64/u64 两域）。
  #[error(
    "{0} expected to be at most 2147483647 seconds (C# IntRangeValidation upper bound). Actual value: {1}"
  )]
  TimeKnobAboveContractUpper(&'static str, u64),
}

/// 解析日志级别字符串为 LevelFilter（忽略大小写，支持已声明成员、别名与 CLI 序数）
///
/// 对齐 C# Options.cs:366-367 LogLevel 枚举成员名（Trace/Debug/Information/Warning/Error/Critical/None）
/// 及 redis.conf 别名（verbose->Trace, notice->Info, nothing->Off），大小写不敏感；
/// 支持 CLI 序数 0-6（0->Trace, 1->Debug, 2->Info, 3->Warn, 4->Error, 5->Critical, 6->Off）；
/// 未知值或非法数字返回 None。
pub(crate) fn try_parse_log_level(value: &str) -> Option<LevelFilter> {
  let trimmed = value.trim();
  if trimmed.is_empty() {
    return None;
  }
  if let Ok(raw) = trimmed.parse::<u8>() {
    return match raw {
      0 => Some(LevelFilter::Trace),
      1 => Some(LevelFilter::Debug),
      2 => Some(LevelFilter::Info),
      3 => Some(LevelFilter::Warn),
      4 => Some(LevelFilter::Error),
      5 => Some(LevelFilter::Error),
      6 => Some(LevelFilter::Off),
      _ => None,
    };
  }
  if trimmed.eq_ignore_ascii_case("trace") || trimmed.eq_ignore_ascii_case("verbose") {
    Some(LevelFilter::Trace)
  } else if trimmed.eq_ignore_ascii_case("debug") {
    Some(LevelFilter::Debug)
  } else if trimmed.eq_ignore_ascii_case("info")
    || trimmed.eq_ignore_ascii_case("information")
    || trimmed.eq_ignore_ascii_case("notice")
  {
    Some(LevelFilter::Info)
  } else if trimmed.eq_ignore_ascii_case("warn") || trimmed.eq_ignore_ascii_case("warning") {
    Some(LevelFilter::Warn)
  } else if trimmed.eq_ignore_ascii_case("error") || trimmed.eq_ignore_ascii_case("critical") {
    Some(LevelFilter::Error)
  } else if trimmed.eq_ignore_ascii_case("off")
    || trimmed.eq_ignore_ascii_case("none")
    || trimmed.eq_ignore_ascii_case("nothing")
  {
    Some(LevelFilter::Off)
  } else {
    None
  }
}

/// 命令行日志级别解析（对标 C# Options.cs:366 `--log-level` 的 Enum.Parse
/// 忽略大小写语义；非法值启动期拒启）
pub(crate) fn parse_log_level(value: &str) -> Result<String, String> {
  try_parse_log_level(value)
    .map(|_| value.to_string())
    .ok_or_else(|| {
      format!(
        "log-level 取值须为 Trace/Debug/Information/Warning/Error/Critical/None（或别名 verbose/notice/nothing、数字 0-6）之一，当前为 {value}"
      )
    })
}

/// 整数定界拒启（对标 C# IntRangeValidation 漏斗末端的统一构造口）：越界即
/// [`NodeOptionsError::ValueOutOfRange`]，文案由变体属性单点持有，调用点不再
/// 各写一遍同形样板
fn check_range(name: &'static str, value: i32, lo: i32, hi: i32) -> Result<(), NodeOptionsError> {
  if (lo..=hi).contains(&value) {
    Ok(())
  } else {
    Err(NodeOptionsError::ValueOutOfRange(name, lo, hi, value))
  }
}

/// 尺寸串定界拒启（对标 C# ParseSize + adjustedSize 区间体检）：解析失败吐
/// [`NodeOptionsError::InvalidSizeStr`]、下取 2 的幂后越出 `[lo, hi]` 吐
/// [`NodeOptionsError::SizeOutOfRange`]（第二项仍为下界、第三项仍为原始字节数），
/// 与原逐选项手写臂逐字同形
fn check_pow2_size(
  name: &'static str,
  raw: &str,
  lo: i64,
  hi: i64,
) -> Result<(), NodeOptionsError> {
  if !is_flag_size_str(raw) {
    return Err(NodeOptionsError::InvalidSizeStr(name, raw.to_string()));
  }
  let size =
    try_parse_size(raw).ok_or_else(|| NodeOptionsError::InvalidSizeStr(name, raw.to_string()))?;
  let adjusted = previous_power_of_2(size);
  if (lo..=hi).contains(&adjusted) {
    Ok(())
  } else {
    Err(NodeOptionsError::SizeOutOfRange(name, lo, size))
  }
}

impl NodeArgs {
  /// 启动期数值定界与组合互校验（对标 C# ServerSettingsManager.cs:149
  /// options.IsValid 的 IntRangeValidation 漏斗末端校验 + GarnetServer.cs:508-519
  /// CreateAOF 的提交组合拒启面 + GarnetServerOptions.cs:839-840 的延迟监视
  /// 伴采样节拍校验；max_databases 界锚 Options.cs:687
  /// IntRangeValidation(1, 256)，堵死配置派生大库号的 OOM 通路）
  pub fn validate(&self) -> Result<(), NodeOptionsError> {
    self.check_bind_endpoints()?;
    self.check_int_ranges()?;
    self.check_size_knobs()?;
    self.check_db_network_ranges()?;
    self.check_aof_commit_combo()?;
    self.check_unixsocket_perm()?;
    self.check_latency_monitor()?;
    self.check_time_knobs()?;
    self.check_lua_knobs()?;
    self.check_log_level()?;
    Ok(())
  }

  /// bind 端点形态合法性校验（拦截 UDS 路径混入 bind）
  fn check_bind_endpoints(&self) -> Result<(), NodeOptionsError> {
    self.endpoints()?;
    Ok(())
  }

  /// C# IntRangeValidation 启动臂下界族十二枚补闸 + 慢日志阈值正臂下限
  fn check_int_ranges(&self) -> Result<(), NodeOptionsError> {
    // C# IntRangeValidation 启动臂下界族十二枚补闸（对标 Options.cs 属性行逐枚
    // 钉锚，负值经 CLI 等号形与 TOML 文件臂直通消费者即拒启；各枚缺省值全落
    // 区间内自然放行，hi 统一契约带上界 i32::MAX；isRequired:false 两枚因缺省
    // 0 在区间内，无条件闸与 C# 非缺省恒检两态等价）：
    // C# Options.cs:243 IntRangeValidation(0, int.MaxValue)
    check_range(
      "aof-tail-witness-freq",
      self.aof_tail_witness_freq_ms,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:267 IntRangeValidation(0, int.MaxValue)
    check_range(
      "expired-object-collection-freq",
      self.expired_object_collection_frequency_secs,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:278 IntRangeValidation(0, int.MaxValue)
    check_range(
      "compaction-max-segments",
      self.compaction_max_segments,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:350 IntRangeValidation(0, int.MaxValue)：负值于此拒启，须置
    // 于下方 :860-863「至少 100µs」正臂之前，两臂分立不合并（0 禁用档保留）
    check_range("slow-log-threshold", self.slow_log_threshold, 0, i32::MAX)?;
    // C# Options.cs:354 IntRangeValidation(0, int.MaxValue)
    check_range("slowlog-max-len", self.slow_log_max_entries, 0, i32::MAX)?;
    // C# Options.cs:433 IntRangeValidation(0, int.MaxValue)（0 = 关闭节流档保留）
    check_range(
      "replica-sync-delay",
      self.replica_sync_delay_ms,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:437 IntRangeValidation(-1, int.MaxValue)：-1 = 无限滞后档保留
    check_range(
      "aof-replay-max-lag-bytes",
      self.aof_replay_max_lag_bytes,
      -1,
      i32::MAX,
    )?;
    // C# Options.cs:589 IntRangeValidation(0, int.MaxValue)
    check_range(
      "object-scan-count-limit",
      self.object_scan_count_limit,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:695 IntRangeValidation(0, int.MaxValue, isRequired: false)
    check_range(
      "cluster-replication-reestablishment-timeout",
      self.cluster_replication_reestablishment_timeout,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:460 IntRangeValidation(0, int.MaxValue)（0 = 立即开窗档保留）
    check_range(
      "repl-diskless-sync-delay",
      self.replica_diskless_sync_delay,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:716 IntRangeValidation(0, int.MaxValue, isRequired: false)
    check_range(
      "vector-set-quantization-task-count",
      self.vector_set_quantization_task_count,
      0,
      i32::MAX,
    )?;
    // C# Options.cs:247 IntRangeValidation(-1, int.MaxValue)（CommitFrequencyMs，
    // 随本票同段收口）：-1 = 手动提交档保留
    if let Some(ms) = self.aof_commit_ms {
      check_range("aof-commit-ms", ms, -1, i32::MAX)?;
    }

    // 慢日志阈值下限（对标 C# Options.cs:860-863 `SlowLogThreshold > 0 && < 100`
    // 抛「must be at least 100 microseconds」）：0 为禁用合法值故不入拒绝区间，
    // 下限单点取 [`SLOW_LOG_THRESHOLD_MIN_MICROS`]，杜绝文案与判定分叉
    if self.slow_log_threshold > 0 {
      check_range(
        "slow-log-threshold",
        self.slow_log_threshold,
        SLOW_LOG_THRESHOLD_MIN_MICROS,
        i32::MAX,
      )?;
    }
    Ok(())
  }

  /// 尺寸串旋钮定界（aof-size-limit / index-max-size 双界 + AOF 三面旗标可解析性）
  fn check_size_knobs(&self) -> Result<(), NodeOptionsError> {
    if let Some(raw) = self.aof_size_limit.as_deref().filter(|s| !s.is_empty()) {
      check_pow2_size("aof-size-limit", raw, 1, i64::MAX)?;
    }
    if let Some(raw) = self.index_max_size.as_deref().filter(|s| !s.is_empty()) {
      // 双界同抛对标 ServerOptions.cs:208 `adjustedSize < 64 || adjustedSize >
      // (1L << 37)`：上界缺席即让 CLI 超大值直通 grow_index_if_needed 的
      // `current_size < index_max_size` 扩容闸，IndexAutoGrowTask 逐轮翻倍
      // 至 OOM，把可控的配置错误升级成进程级崩溃
      check_pow2_size(
        "index-max-size",
        raw,
        INDEX_MAX_SIZE_MIN_BYTES,
        INDEX_MAX_SIZE_MAX_BYTES,
      )?;
    }
    // AOF 三面尺寸旋钮旗标级可解析性定界（对标 C# Options.cs:211-221 三面
    // [MemorySizeValidation] 的入口侧：无法整体解析即启动期拒，不拖到装配）。
    // 组合互校验（memory >= 2*page、page <= segment、page >= 2*主存页）不在
    // wconf 复校——唯一真源是 wnode::aof::AofSettings::from_options（C#
    // GarnetServerOptions.cs:1050 GetAofSettings :1063/:1075/:1096 三条同位），
    // 本文件禁二次解析成字节、禁第二套体检函数
    for (opt, raw) in [
      ("aof-memory", self.aof_memory_size.as_deref()),
      ("aof-page-size", self.aof_page_size.as_deref()),
      ("aof-segment-size", self.aof_segment_size.as_deref()),
      // 网络缓冲内存预算（C# Options.cs:433 [MemorySizeValidation(false)]，
      // PR #2157）：入口侧旗标级可解析性定界，语义分派（0 = 禁用）在装配侧
      // wnode 网络预算装配单点
      (
        "network-buffer-memory-budget",
        self.network_buffer_memory_budget.as_deref(),
      ),
    ] {
      if let Some(text) = raw
        && (!is_flag_size_str(text) || try_parse_size(text).is_none())
      {
        return Err(NodeOptionsError::InvalidSizeStr(opt, text.to_string()));
      }
    }
    Ok(())
  }

  /// 库数 / 网络连接数 / 过期删除扫描频率定界
  fn check_db_network_ranges(&self) -> Result<(), NodeOptionsError> {
    check_range(
      "max-databases",
      self.max_databases,
      MAX_DATABASES_MIN,
      MAX_DATABASES_MAX,
    )?;
    // C# Options.cs:401 IntRangeValidation(-1, int.MaxValue)：下界 -1（不限）
    // 固定，与缺省值 10000（PR #2157 对齐 Redis）无关——下界误绑缺省即把
    // 调低上限的合法配置误拒
    check_range(
      "network-connection-limit",
      self.network_connection_limit,
      -1,
      i32::MAX,
    )?;
    // C# Options.cs:691 IntRangeValidation(-1, int.MaxValue)：-1 以下无
    // 「禁用」以外的语义，槽位下界同为 -1（RuntimeServerConfig.cs:213）
    check_range(
      "expired-key-deletion-scan-freq",
      self.expired_key_deletion_scan_frequency_secs,
      DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
      i32::MAX,
    )?;
    Ok(())
  }

  /// AOF 提交组合拒启面（GarnetServer.cs:508-519 CreateAOF 三连）
  fn check_aof_commit_combo(&self) -> Result<(), NodeOptionsError> {
    // C# GarnetServer.cs:508 `!EnableAOF && (CommitFrequencyMs != 0 || WaitForCommit)`
    // → 缺省 0（defaults.conf:182）即「未显式配非零节拍」，与 C# 同判不拒
    if !self.aof && (self.aof_commit_wait || matches!(self.aof_commit_ms, Some(ms) if ms != 0)) {
      return Err(NodeOptionsError::AofCommitWithoutAof);
    }
    // C# GarnetServer.cs:513-515 FastAofTruncate requires manual commit (CommitFrequencyMs = -1)
    if self.fast_aof_truncate && self.aof && self.aof_commit_ms != Some(-1) {
      return Err(NodeOptionsError::FastAofTruncateRequiresManualCommit);
    }
    // C# GarnetServer.cs:517-519 WaitForCommit cannot be used with manual commit (CommitFrequencyMs < 0)
    if matches!(self.aof_commit_ms, Some(ms) if ms < 0) && self.aof_commit_wait {
      return Err(NodeOptionsError::CommitWaitWithManualCommit);
    }
    Ok(())
  }

  /// unixsocketperm 启动期定界与八进制数字位校验
  fn check_unixsocket_perm(&self) -> Result<(), NodeOptionsError> {
    // unixsocketperm 启动期定界（C# Options.cs:683 IntRangeValidation(0, 777)）
    // 与八进制数字位校验（C# Options.cs:817 Convert.ToInt32(_, 8) 转换失败面）
    if let Some(perm) = self.unixsocket_perm {
      check_range("unixsocketperm", perm, 0, UNIX_SOCKET_PERM_MAX)?;
      if perm.to_string().bytes().any(|d| d > b'7') {
        return Err(NodeOptionsError::UnixSocketPermDigits(perm));
      }
    }
    Ok(())
  }

  /// 延迟监视伴采样节拍校验
  fn check_latency_monitor(&self) -> Result<(), NodeOptionsError> {
    // C# GarnetServerOptions.cs:839-840 `LatencyMonitor && MetricsSamplingFrequency
    // == 0` 即拒启——全仓唯一校验点（禁各装配路径重复判定）
    if self.latency_monitor && self.metrics_sampling_frequency_secs == 0 {
      return Err(NodeOptionsError::LatencyMonitorWithoutMetrics);
    }
    Ok(())
  }

  /// 时间旋钮四枚契约带上界闸（i32::MAX 秒）
  fn check_time_knobs(&self) -> Result<(), NodeOptionsError> {
    // 时间旋钮三枚上限闸（对标 C# Options.cs:464-466 / :358-360 / :616-618
    // IntRangeValidation 上界 int.MaxValue 秒契约带）：字段域 i64/u64 宽于 C#
    // int 域、clap 仅按类型收值不窄化，且 check_range 为 i32-only 签名不适用，
    // 故循下方 lua_script_timeout_ms 手闸形制上限拒启。越界正值经投影直通
    // RuntimeServerConfig 秒槽后由 get_time_span 的 Some(Duration::from_secs)
    // 直送 compio 定时器（真实必炸域仅 i64::MAX 近端，(i32::MAX, 该窗) 为
    // 不炸但永不燃尽的限时器），C# 侧 CLI 闸入口即拒、根不到运行期。
    // attach 非正臂「<=0 折 0 即永等」与 metrics 0 档「禁用监视器」语义均维持
    // 不动；index_resize 低边 0 档经 service.rs .max(1) 折 1 秒现放行不动，
    // 本闸只补高边。
    if self.replica_attach_timeout_secs > i32::MAX as i64 {
      return Err(NodeOptionsError::TimeKnobAboveContractUpper(
        "repl-attach-timeout",
        self.replica_attach_timeout_secs as u64,
      ));
    }
    if self.index_resize_frequency_secs > i32::MAX as u64 {
      return Err(NodeOptionsError::TimeKnobAboveContractUpper(
        "index-resize-frequency",
        self.index_resize_frequency_secs,
      ));
    }
    if self.metrics_sampling_frequency_secs > i32::MAX as u64 {
      return Err(NodeOptionsError::TimeKnobAboveContractUpper(
        "metrics-sampling-freq",
        self.metrics_sampling_frequency_secs,
      ));
    }
    // tls_cert_refresh_freq 同族闸（字段文档自证 C# Options.cs:329-330
    // IntRangeValidation(0, int.MaxValue)，0 = 禁用档）：>0 段越 i32 契约上界
    // 即拒——穿透链经 wtls 刷新循环 sleep_secs 转 Instant 加法，> i64::MAX
    // 秒在启动运行时内 std 溢出 panic，(i32::MAX, 溢出窗) 段静默生成永不
    // 燃尽的计时器，同族收口理由
    if self.tls_cert_refresh_freq > i32::MAX as u64 {
      return Err(NodeOptionsError::TimeKnobAboveContractUpper(
        "tls-cert-refresh-freq",
        self.tls_cert_refresh_freq,
      ));
    }
    Ok(())
  }

  /// Lua 脚本超时与内存限额两面校验
  fn check_lua_knobs(&self) -> Result<(), NodeOptionsError> {
    // Lua 脚本超时 IntRangeValidation(10, int.MaxValue, isRequired: false)（对标
    // C# Options.cs:651）：0 = 禁用（InfiniteTimeSpan）为合法缺省，其余一律须落
    // [10, int.MaxValue]，负值/过小值静默折无限即语义欺骗，改启动期拒启
    let timeout = self.lua_script_timeout_ms;
    if timeout != 0 && !(10..=(i32::MAX as i64)).contains(&timeout) {
      return Err(NodeOptionsError::LuaScriptTimeoutOutOfRange(timeout));
    }
    // Lua 内存限额两面（对标 C# Options.cs:645-647）：MemorySizeValidation(false)
    // 非空即须可整体解析（与 aof 尺寸旗标同级入口校验）；ForbiddenWithOption
    // (Native) 限额与 Native 模式同时设置拒启（Native 档无生效面，静默忽略即欺骗）
    if let Some(raw) = self
      .lua_script_memory_limit
      .as_deref()
      .filter(|s| !s.is_empty())
    {
      if !is_flag_size_str(raw) || try_parse_size(raw).is_none() {
        return Err(NodeOptionsError::InvalidSizeStr(
          "lua-script-memory-limit",
          raw.to_string(),
        ));
      }
      if self.lua_memory_management_mode == LuaMemoryManagementMode::Native {
        return Err(NodeOptionsError::LuaMemoryLimitWithNative);
      }
    }
    Ok(())
  }

  /// log-level 启动期文法门禁
  fn check_log_level(&self) -> Result<(), NodeOptionsError> {
    // log-level 启动期文法门禁（对标 C# Options.cs:366-367 LogLevel 枚举解析）：
    // 非法值与未知字符串启动期拒启，禁止宽容静默回退
    if let Some(raw) = self.log_level.as_deref()
      && try_parse_log_level(raw).is_none()
    {
      return Err(NodeOptionsError::InvalidLogLevel(raw.to_string()));
    }
    Ok(())
  }
}
