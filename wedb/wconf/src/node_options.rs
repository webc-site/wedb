//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。
//! 配置文件格式钦定 TOML（C# GarnetConf/RedisConf 双格式不转写）。
//!
//! 自研依据: 节点选项族（C# 对应 GarnetServerOptions）

use std::{
  ffi::OsString,
  fs,
  io::Error,
  net::Ipv6Addr,
  path::{Path, PathBuf},
  process::exit,
  sync::Arc,
};

use clap::{ArgMatches, Parser, error::ErrorKind};
use log::LevelFilter;
use toml_spanner::Arena;
use wbase::{
  cfg,
  cfg::{LogCompactionType, MAX_DATABASES_MAX, MAX_DATABASES_MIN},
  endpoint::uds_path,
};

use crate::{
  connection_protection_option::ConnectionProtectionOption,
  lua_option_modes::{LuaLoggingMode, LuaMemoryManagementMode},
  runtime_server_options::{
    DEFAULT_AOF_REPLAY_MAX_LAG_BYTES, DEFAULT_COMPACTION_MAX_SEGMENTS,
    DEFAULT_ENABLE_SCATTER_GATHER_GET, DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS,
    RuntimeServerOptions,
  },
  size::{previous_power_of_2, try_parse_size, validated_page_size_bits},
};

/// 生产默认主存日志页容量字节：取值下沉基座 [`wbase::cfg::DEFAULT_HLOG_PAGE_SIZE`]
/// （对标 C# ServerOptions.cs:46 PageSize = "16m"），本处仅按配置层既有对外
/// 口径保留同名常量，杜绝第二套定义（票
/// task/todo/whlog-waof-wconf-inline-dep-reverse-layering.md）。
pub const DEFAULT_HLOG_PAGE_SIZE: usize = cfg::DEFAULT_HLOG_PAGE_SIZE;

/// 默认监听端口
pub const DEFAULT_PORT: u16 = 6379;
/// 默认监听地址（保护模式双回环回退；对标 C# Format.defaultBindLoopBack: [127.0.0.1, ::1]）
pub const DEFAULT_BIND: &str = "127.0.0.1,::1";
/// 非保护模式监听地址（对标 C# Format.defaultBindAny: [0.0.0.0, ::]）
pub const DEFAULT_BIND_ANY: &str = "0.0.0.0,::";
/// 默认工作目录
pub const DEFAULT_DIR: &str = "./data";

/// 数据文件名（`{dir}/wedb.db`，单机与集群共用同一物理件）
///
/// 对标 C# 单一命名方案：`GarnetServer.cs:479-484` 集群与单机两臂共用同一
/// `defaultNamingScheme`（仅 CheckpointManager 类型分叉），`Options.cs:790-793`
/// LogDir/CheckpointDir 单套、`EnableCluster` 不换文件布局，模式切换复用同一
/// 数据文件。本仓检查点目录默认 `{dir}/Store/checkpoints`（`--checkpoint-dir`
/// 可改基目录，见 [`NodeArgs::checkpoint_base_dir`]）、WAL 落
/// `{--wal-dir 或 dir/wal}/wal.log`，皆与模式无关，故数据文件名亦不随模式区分
/// ——否则集群二进制指向单机遗留目录会新建空数据文件当恢复设备、加载单机检查点
/// 索引（索引地址指向另一数据文件）、续写同一 wal.log，恢复静默错乱并写坏共享
/// 物理件。
pub const DATA_FILE: &str = "wedb.db";

/// 默认 RESP 协议版本（对标 libs/server/Servers/ServerOptions.cs:DEFAULT_RESP_VERSION）
pub const DEFAULT_RESP_VERSION: u8 = 2;

/// 慢日志记录阈值微秒（0 = 禁用；对标 C# Options.cs:351 SlowLogThreshold）
pub const DEFAULT_SLOW_LOG_THRESHOLD: i32 = 0;
/// 慢日志容量上限（对标 GarnetServerOptions.cs:292 SlowLogMaxEntries）
pub const DEFAULT_SLOW_LOG_MAX_ENTRIES: i32 = 128;
/// 默认逻辑数据库数量上限（对标 GarnetServerOptions.cs:615 MaxDatabases）
pub const DEFAULT_MAX_DATABASES: i32 = 16;
/// unixsocketperm 八进制数字字面上界（对标 C# Options.cs:683
/// IntRangeValidation(0, 777)：十进制写法即八进制口径，600 表 0o600）
pub const UNIX_SOCKET_PERM_MAX: i32 = 777;
/// 默认保护模式（对标 defaults.conf ProtectedMode = "yes"）
pub const DEFAULT_PROTECTED_MODE: bool = true;
/// 默认按需检查点开关（对标 C# defaults.conf:346 OnDemandCheckpoint = true 与
/// GarnetServerOptions.cs:405 字段初始化器）
pub const DEFAULT_ON_DEMAND_CHECKPOINT: bool = true;
/// 默认 *SCAN 单次迭代返回项数上限（对标 ObjectScanCountLimit 默认 1000）
pub const DEFAULT_OBJECT_SCAN_COUNT_LIMIT: i32 = 1000;
/// 默认周期对象过期收集频率秒数（0 = 禁用，对标
/// GarnetServerOptions.ExpiredObjectCollectionFrequencySecs 默认 0）。
/// 注：本仓该旋钮同时门控分层键后台降阶评估轮唯一生产宿主任务
/// （wnode `primary_tasks::object_collect_loop` → `tiered_demote_round`），
/// 缺省 0 时冷分层键无后台降阶评估点（doc/zh/deviations.md §120）
pub const DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS: i32 = 0;
/// 默认过期键后台删除扫描周期秒数（-1 = 禁用后台扫描，按需 EXPDELSCAN 兜底；
/// 对标 C# defaults.conf:524 ExpiredKeyDeletionScanFrequencySecs = -1 与
/// GarnetServerOptions.cs:162 字段初值）
pub const DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS: i32 = -1;
/// 默认指标监视器采样周期秒数（0 = 禁用采样任务）
pub const DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS: u64 = 0;
/// 默认最大并发网络连接数（-1 = 不限；对标 defaults.conf:304
/// NetworkConnectionLimit = -1 与 GarnetServerOptions.cs:347）
pub const DEFAULT_NETWORK_CONNECTION_LIMIT: i32 = -1;
/// 日志文件刷盘间隔毫秒数（0 = 逐行立即刷盘；对标 C#
/// GarnetServer.cs:128 `builder.AddFile(serverSettings.FileLogger)` 省略
/// flushInterval 形参，取 FileLoggerProvider.cs:26 `int flushInterval = default`
/// 的零值默认）
pub const DEFAULT_LOG_FLUSH_INTERVAL: i32 = 0;

/// 主存混合日志（hlog）配置段（对标 libs/server/Servers/GarnetServerOptions.cs
/// 主存日志选项：PageSize = "16m"、LogMemorySize = "16g"；MutablePercent C# 缺省
/// 为 90（Servers/ServerOptions.cs:77、host/defaults.conf:64），rust 引擎缺省比例
/// 0.5 与之系未裁决分叉，见 doc/zh/deviations.md §93 留槽，严禁按任一方径改）
///
/// 全部字段可缺省：None 项不覆盖装配基线，由 `StoreConfig::auto()` 的内存预算
/// 规划器推导（大机预算 ≥ 1GB 时推导 [`DEFAULT_HLOG_PAGE_SIZE`] 页）。
/// TOML 形态：
///
/// ```toml
/// [hlog]
/// page_size = 16777216
/// memory_size = 4294967296
/// mutable_percent = 50
/// ```
pub mod usize_u64 {
  use toml_spanner::{Arena, Context, Failed, Item, ToTomlError};

  pub fn to_toml<'a>(value: &usize, _: &'a Arena) -> Result<Item<'a>, ToTomlError> {
    Ok(Item::from(*value as i128))
  }

  pub fn from_toml<'de>(ctx: &mut Context<'de>, item: &Item<'de>) -> Result<usize, Failed> {
    let Some(i) = item.as_i64() else {
      return Err(ctx.report_expected_but_found(&"an integer", item));
    };
    if i < 0 {
      return Err(ctx.report_custom_error("expected non-negative integer", item));
    }
    Ok(i as usize)
  }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, clap::Args, toml_spanner::Toml)]
#[toml(FromToml, ToToml, ignore_unknown_fields)]
pub struct HlogOptions {
  /// 主存日志单页容量字节（必须为 2 的幂且为扇区大小整数倍，且不低于
  /// [`crate::size::MIN_PAGE_SIZE_BYTES`]；未配置时由内存预算规划器推导，对标 C#
  /// GarnetServerOptions.cs PageSize = "16m"）。
  ///
  /// 页容量决定单条内联记录上限：值 ≤ 页容量 - 记录头 - 键 即可内联存储
  /// （16MB 页可承载 C# DefaultMaxInlineValueSize = 1MB 基线的大值）。
  #[arg(id = "hlog_page_size", long = "hlog-page-size")]
  #[toml(with = usize_u64)]
  pub page_size: Option<usize>,

  /// 主存日志内存环形缓冲预算字节（未配置时由内存预算规划器推导；
  /// 对标 C# GarnetServerOptions.cs LogMemorySize = "16g" 的 pageCount 推导：
  /// `num_pages = next_power_of_2(memory_size / page_size)`）
  #[arg(id = "hlog_memory_size", long = "hlog-memory-size")]
  #[toml(with = usize_u64)]
  pub memory_size: Option<usize>,

  /// 内存可变区百分比（10..=95，对标 C# GarnetServerOptions.cs:747-748 的
  /// GetSettings 区间校验；C# 缺省值为 90（ServerOptions.cs:77），未配置时取
  /// rust 引擎默认比例 0.5——两者差异系未裁决分叉，见 doc/zh/deviations.md §93
  /// 留槽，严禁按 90 或 50 任何一方径改行为）
  #[arg(id = "hlog_mutable_percent", long = "hlog-mutable-percent")]
  pub mutable_percent: Option<u8>,

  /// 是否启用 ReadCache 独立只读非脏页内存日志（对标 C# GarnetServerOptions.cs:582
  /// EnableReadCache，默认 false）
  #[arg(id = "read_cache", long = "read-cache")]
  #[toml(default)]
  pub read_cache: bool,

  /// ReadCache 内存预算字节（仅 `read_cache` 开启时参与页数推导：预算 / 主日志
  /// 页容量向下取 2 的幂；对标 C# GarnetServerOptions.cs:587
  /// ReadCacheMemorySize = "1g"。未配置时取 [`DEFAULT_READ_CACHE_MEMORY_SIZE`]）
  #[arg(id = "read_cache_memory_size", long = "read-cache-memory-size")]
  #[toml(with = usize_u64)]
  pub read_cache_memory_size: Option<usize>,

  /// 升阶树页缓存全局总预算字节（长驻升阶树页环与并发升阶 scratch 环共用的
  /// 字节总闸；C# RangeIndexManager 无预算机制、CacheSizeTracker 只跟主日志与
  /// 读缓存，本闸为本仓分层架构自研组件，观测面对标 CacheSizeTracker 的
  /// TargetSize 高水位语义，见 doc/zh/collection.md。0 = 不设限，未配置时取
  /// [`DEFAULT_TREE_CACHE_BUDGET_BYTES`]。启动期一次性注入 RangeIndexManager，
  /// 不做热更）
  #[arg(id = "tree_cache_budget", long = "tree-cache-budget")]
  #[toml(with = usize_u64)]
  pub tree_cache_budget: Option<usize>,

  /// 是否启用空间复活回收池与链内原地复活（对标 C# Options.cs:564-567
  /// EnableRevivification，命令行 `--reviv`，默认 false）
  #[arg(long = "reviv")]
  #[toml(default)]
  pub reviv: bool,

  /// 复活区间比例（对标 C# Options.cs:558-561 RevivifiableFraction，命令行
  /// `--reviv-fraction`，DoubleRangeValidation(0, 1)；None = 未配置，不覆盖
  /// 引擎默认值）。区间校验单点在 wkv `StoreConfig::validate`
  /// （(0, mutable_fraction]），本处不复校验，避免第二套真源
  #[arg(long = "reviv-fraction")]
  pub reviv_fraction: Option<f64>,

  /// 冷区/磁盘读取成功后是否将记录复制晋升到日志 Tail（对标 C#
  /// Options.cs:126-128 CopyReadsToTail，命令行 `--copy-reads-to-tail`，
  /// 默认 false；C# 经 GarnetServerOptions.cs:899-900 投影进
  /// kvSettings.ReadCopyOptions，store 级而非会话私有）
  #[arg(long = "copy-reads-to-tail")]
  #[toml(default)]
  pub copy_reads_to_tail: bool,
}

/// 默认 ReadCache 内存预算字节（1GB，对标 C# GarnetServerOptions.cs
/// ReadCacheMemorySize = "1g" 默认值）
pub const DEFAULT_READ_CACHE_MEMORY_SIZE: usize = 1024 * 1024 * 1024;

/// hlog 配置段覆盖项投影（None 项原样透传装配基线）
pub struct HlogProjection {
  /// 主存日志单页容量字节
  pub page_size: Option<usize>,
  /// 主存日志内存环形缓冲预算字节
  pub memory_size: Option<usize>,
  /// 内存可变区比例（(0, 1]）
  pub mutable_fraction: Option<f64>,
  /// 是否启用 ReadCache 独立读缓存
  pub read_cache: bool,
  /// ReadCache 内存预算字节
  pub read_cache_memory_size: Option<usize>,
  /// 升阶树页缓存全局总预算字节（0 = 不设限）
  pub tree_cache_budget: Option<usize>,
  /// 是否启用空间复活回收池（对标 C# Options.cs:564-567 reviv）
  pub reviv: bool,
  /// 复活区间比例（对标 C# Options.cs:559 reviv-fraction；区间校验单点在
  /// wkv `StoreConfig::validate`，本处只透传）
  pub reviv_fraction: Option<f64>,
  /// 冷读复制晋升 Tail（对标 C# Options.cs:128 CopyReadsToTail）
  pub copy_reads_to_tail: bool,
}

/// hlog 页容量扇区对齐字节（4KB 高级格式扇区，与 wkv `StoreConfig::validate`
/// 的 `wbase::DEFAULT_SECTOR_SIZE` 校验口径一致）
pub const HLOG_PAGE_SIZE_SECTOR_BYTES: usize = 4096;

/// hlog 页容量配置属性名（校验文案点名用，与命令行长线名同字面）
const HLOG_PAGE_SIZE_PROP: &str = "hlog-page-size";

impl HlogOptions {
  /// 校验显式配置项合法性（页容量下限走 [`crate::size::validated_page_size_bits`] 校验核，
  /// 其余对标 C# GarnetServerOptions.GetSettings 装配期校验）
  ///
  /// 返回投影（百分比已换算为 (0, 1] 比例，C# MutableFraction =
  /// MutablePercent / 100），None 项原样透传。
  pub fn validated(&self) -> Result<HlogProjection, NodeOptionsError> {
    // 页容量投影单点（主存日志与 read cache 共用本页容量，对标 C# PageSizeBits /
    // ReadCachePageSizeBits 同调 ValidatedPageSizeBits）：取幂 + MIN_PAGE_SIZE_BYTES
    // 下限由校验核裁决；引擎侧另须页容量恰为 2 的幂且扇区对齐，故核有折损即非法。
    if let Some(p) = self.page_size {
      let bits = validated_page_size_bits(p as i64, HLOG_PAGE_SIZE_PROP)
        .map_err(|e| NodeOptionsError::Hlog(e.to_string()))?;
      if 1u64 << bits != p as u64 || !p.is_multiple_of(HLOG_PAGE_SIZE_SECTOR_BYTES) {
        return Err(NodeOptionsError::Hlog(format!(
          "hlog page_size 必须为 2 的幂且为 {HLOG_PAGE_SIZE_SECTOR_BYTES} 的整数倍，当前为 {p}"
        )));
      }
    }
    if self.memory_size.is_some_and(|m| m == 0) {
      return Err(NodeOptionsError::Hlog("hlog memory_size 必须大于 0".into()));
    }
    // MutablePercent is < 10 or > 95 → throw（GarnetServerOptions.GetSettings）
    if let Some(pct) = self.mutable_percent
      && !(10..=95).contains(&pct)
    {
      return Err(NodeOptionsError::Hlog(format!(
        "MutablePercent must be between 10 and 95, 当前为 {pct}"
      )));
    }
    if self.read_cache_memory_size.is_some_and(|m| m == 0) {
      return Err(NodeOptionsError::Hlog(
        "hlog read_cache_memory_size 必须大于 0".into(),
      ));
    }
    Ok(HlogProjection {
      page_size: self.page_size,
      memory_size: self.memory_size,
      mutable_fraction: self.mutable_percent.map(|p| f64::from(p) / 100.0),
      read_cache: self.read_cache,
      read_cache_memory_size: self.read_cache_memory_size,
      // 0 = 不设限为合法显式值，无区间校验
      tree_cache_budget: self.tree_cache_budget,
      // reviv 三旋钮纯透传：reviv_fraction 的区间校验单点在 wkv
      // StoreConfig::validate（(0, mutable_fraction]），此处不复校，避免第二套真源
      reviv: self.reviv,
      reviv_fraction: self.reviv_fraction,
      copy_reads_to_tail: self.copy_reads_to_tail,
    })
  }
}

/// AOF 体积限额检查周期秒数（对标 GarnetServerOptions.cs:206
/// AofSizeLimitEnforceFrequencySecs = 5）
pub const DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS: u64 = 5;

/// 索引周期自动扩容检测周期秒数（对标 GarnetServerOptions.cs:167
/// IndexResizeFrequencySecs = 60）
pub const DEFAULT_INDEX_RESIZE_FREQUENCY_SECS: u64 = 60;

/// 索引自动扩容触发阈值：溢出桶数超过 index_size × 阈值% 即扩容
///（对标 GarnetServerOptions.cs:172 IndexResizeThreshold = 50）
pub const DEFAULT_INDEX_RESIZE_THRESHOLD: i64 = 50;

/// 索引内存上限的最小合法字节（对标 ServerOptions.cs:208 IndexSizeCachelines
/// 的 `adjustedSize < 64` 拒绝界；64B 恰为一桶）
pub const INDEX_MAX_SIZE_MIN_BYTES: i64 = 64;
/// 索引内存上限的最大合法字节（对标 ServerOptions.cs:208 `adjustedSize > (1L << 37)`
/// 拒绝界）
pub const INDEX_MAX_SIZE_MAX_BYTES: i64 = 1 << 37;
/// 慢日志阈值非零时的最小合法微秒数（对标 Options.cs:860-863
/// `SlowLogThreshold > 0 && < 100` 抛「must be at least 100 microseconds」）
pub const SLOW_LOG_THRESHOLD_MIN_MICROS: i32 = 100;
/// 复制同步超时缺省秒数（对标 GarnetServerOptions.cs:420 ReplicaSyncTimeout = 5；
/// <=0 = 无限超时哨兵）
pub const DEFAULT_REPLICA_SYNC_TIMEOUT_SECS: i32 = 5;
/// 副本 attach 超时缺省秒数（对标 GarnetServerOptions.cs:425
/// ReplicaAttachTimeout = 60；<=0 = 无限超时）
pub const DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS: i64 = 60;
/// 副本同步节流缺省毫秒数（对标 GarnetServerOptions.cs:382 ReplicaSyncDelayMs；
/// 0 = 关闭节流）
pub const DEFAULT_REPLICA_SYNC_DELAY_MS: i32 = 5;
/// 主端 AOF 追加背压滞后预算缺省字节（对标 GarnetServerOptions.cs:395
/// AofSyncMaxLagBytes = -1；-1 = 关闭）
pub const DEFAULT_AOF_SYNC_MAX_LAG_BYTES: i64 = -1;
/// AOF 尾位点后台前移轮询缺省毫秒数（对标 defaults.conf:179
/// AofTailWitnessFreqMs = 10；仅物理子日志 >1 生效）
pub const DEFAULT_AOF_TAIL_WITNESS_FREQ_MS: i32 = 10;
/// 集群复制重连轮询缺省秒数（对标 GarnetServerOptions.cs:640
/// ClusterReplicationReestablishmentTimeout = 0；0 = 禁用自动重连）
pub const DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT: i32 = 0;
/// Vector Set 量化任务数缺省值（对标 Options.cs:717 / defaults.conf:542
/// VectorSetQuantizationTaskCount = 0；0 = 按物理核数自动对齐）
pub const DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT: i32 = 0;

/// 节点参数加载错误（命令行解析、NestedText 配置解析、hlog 校验与文件读取；依赖库错误透明转发）。
#[derive(Debug, thiserror::Error)]
pub enum NodeOptionsError {
  /// 命令行解析失败。
  #[error(transparent)]
  Cli(#[from] clap::Error),
  /// 配置文件解析失败。
  #[error("配置文件解析失败: {0}")]
  Config(String),
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
}

/// 统一节点基础参数配置
#[derive(Debug, Clone, Parser, toml_spanner::Toml)]
#[command(author, version, about = "WeDB 高性能分布式数据库服务")]
#[toml(FromToml, ToToml, ignore_unknown_fields)]
pub struct NodeArgs {
  /// 主存混合日志（hlog）配置段（TOML 嵌套表 `[hlog]`；未配置项交由
  /// 存储引擎内存预算规划器推导，见 [`HlogOptions`]）
  #[command(flatten)]
  #[toml(default)]
  pub hlog: HlogOptions,

  /// 绑定监听 IP 地址（未指定时按 protected-mode 回退：保护回环 / 非保护全接口）
  #[arg(short = 'b', long)]
  pub bind: Option<String>,

  /// 业务监听端口
  #[arg(short = 'p', long, default_value_t = DEFAULT_PORT)]
  #[toml(default = DEFAULT_PORT)]
  pub port: u16,

  /// Unix 域套接字路径
  #[arg(long)]
  pub unixsocket: Option<String>,

  /// Unix 域套接字文件权限（八进制数字字面量，如 600 表 0o600；Redis 兼容名
  /// --unixsocketperm；对标 C# Options.cs:684 UnixSocketPermission，默认
  /// None = 不设置，与 C# 默认 0 时 GarnetServerTcp.cs:149
  /// `unixSocketPermission != default` 跳过 chmod 同向；界与八进制位校验
  /// 单点落 validate，绑定侧不设二次校验）
  #[arg(long = "unixsocketperm")]
  pub unixsocket_perm: Option<i32>,

  /// 数据与持久化存储工作目录
  #[arg(short = 'd', long, default_value = DEFAULT_DIR)]
  #[toml(default = PathBuf::from(DEFAULT_DIR))]
  pub dir: PathBuf,

  /// WAL / AOF 物理日志存储路径（未显式指定时默认为 <dir>/wal）
  #[arg(long)]
  pub wal_dir: Option<PathBuf>,

  /// 检查点基目录（未显式指定时回落数据目录；对标 C# -c/--checkpointdir 旋钮
  /// Options.cs:134-136 与 CheckpointDirValidation，检查点快照基线可置独立卷，
  /// 数据盘故障时快照基线可存活。CONFIG GET dir 回显同源）
  #[arg(long = "checkpoint-dir")]
  pub checkpoint_dir: Option<PathBuf>,

  /// 访问认证密码
  #[arg(long)]
  pub requirepass: Option<String>,

  /// TLS 证书文件路径（PEM 格式）
  #[arg(long)]
  pub tls_cert: Option<PathBuf>,

  /// TLS 私钥文件路径（PEM 格式）
  #[arg(long)]
  pub tls_key: Option<PathBuf>,

  /// 入站 TLS 是否要求客户端证书（mTLS 双向认证；对标 C# defaults.conf:250
  /// ClientCertificateRequired: true 生效默认——Options.cs:334 属性虽为 bool?，
  /// 但 defaults.conf 启动恒先导入，:950 GetValueOrDefault 折叠仅对用户显式
  /// 清空生效。C# 开启 TLS 后默认要求客户端证书，缺席时 GarnetTlsOptions.cs:236-238
  /// 明告警 "Remote certificate validation will always succeed"）。true 且给出
  /// tls_issuer_cert → 以该 CA 校验客户端证书链；true 而未给 → 要求证书但
  /// 不校验颁发者链（GarnetTlsOptions.cs:273 告警语义）；false 即单向 TLS
  /// 零变化。C# 另有 Options.cs:336 certificate-revocation-check-mode 吊销
  /// 检查：rustls 仅 unstable CRL 面未用且本仓无此装配链，旋钮删员缺席已在
  /// deviations.md §124d) 实条在册（C# 生效默认 NoCheck 与 rust 实际行为
  /// 全等；如需实现另立单）
  #[arg(
    long,
    default_value_t = true,
    num_args = 0..=1,
    default_missing_value = "true",
    action = clap::ArgAction::Set
  )]
  #[toml(default = true)]
  pub tls_client_cert_required: bool,

  /// 集群出站 TLS 目标主机名（对标 C# Options.cs:307 ClusterTlsClientTargetHost；
  /// 空 = 建连时回落对端地址的 host 段）
  #[arg(long)]
  pub tls_client_target_host: Option<String>,

  /// 出站方向是否校验远端证书（对标 C# Options.cs:311 ServerCertificateRequired，
  /// defaults.conf:253 默认 true；false 即不安全恒真模式）
  #[arg(
    long,
    default_value_t = true,
    num_args = 0..=1,
    default_missing_value = "true",
    action = clap::ArgAction::Set
  )]
  #[toml(default = true)]
  pub tls_server_cert_required: bool,

  /// TLS 签发者 CA 证书路径（对标 C# Options.cs:339 IssuerCertificatePath，
  /// C# 单字段双用）：入站 mTLS（tls_client_cert_required=true）时作客户端
  /// 证书校验根；出站校验（tls_server_cert_required=true）时作远端证书根，
  /// 未指定时出站用内置 webpki 根、入站回落宽松不校验链模式
  #[arg(long)]
  pub tls_issuer_cert: Option<PathBuf>,

  /// TLS 证书定时刷新周期秒数（对标 C# Options.cs:329-330
  /// CertificateRefreshFrequency，命令行 --cert-refresh-freq，
  /// IntRangeValidation(0, int.MaxValue)；0 = 禁用定时刷新）。> 0 时服务端
  /// 后台按周期重读证书文件原子换装活跃证书链，Let's Encrypt 等 ACME 自动
  /// 轮换场景新握手无缝切换不断已有连接；重载失败按 5 秒退避重试
  ///（对标 ServerCertificateSelector.certificateRefreshRetryInterval）
  #[arg(
    long = "cert-refresh-freq",
    alias = "tls-cert-refresh-freq",
    default_value_t = 0
  )]
  #[toml(default)]
  pub tls_cert_refresh_freq: u64,

  /// 工作线程数（默认按可用 CPU 物理核心数）
  #[arg(short = 't', long)]
  #[toml(with = usize_u64)]
  pub threads: Option<usize>,

  /// 最大并发网络连接数（-1 = 不限；对标 C# Options.cs:399 键
  /// network-connection-limit、IntRangeValidation(-1, int.MaxValue) 与
  /// defaults.conf:304 默认 -1；accept 成功即刻计量在途数，超限臂即刻
  /// 关闭新连接且不写任何 RESP 应答，为 FD/内存耗尽的平台侧护栏）。
  /// 纯启动期旋钮：C# ServerConfigType 枚举不含此项（非 CONFIG GET/SET
  /// 运行时项），自动纳入 TOML 导入/导出面
  #[arg(
    long = "network-connection-limit",
    default_value_t = DEFAULT_NETWORK_CONNECTION_LIMIT,
    allow_hyphen_values = true
  )]
  #[toml(default = DEFAULT_NETWORK_CONNECTION_LIMIT)]
  pub network_connection_limit: i32,

  /// 是否启用 AOF 持久化日志（对标 C# Options.cs:209 EnableAOF）
  #[arg(long, default_value_t = false)]
  #[toml(default)]
  pub aof: bool,

  /// 是否禁用发布订阅功能（对标 C# ServerOptions.cs:107 DisablePubSub，
  /// C# 默认 false 即默认启用 pubsub）
  #[arg(long = "disable-pubsub", default_value_t = false)]
  #[toml(default)]
  pub disable_pubsub: bool,

  /// 启动时从最新检查点与 AOF 日志恢复（若存在；对标 C# Options.cs:139 Recover）
  #[arg(short = 'r', long, default_value_t = false)]
  #[toml(default)]
  pub recover: bool,

  /// AOF 周期提交毫秒数（对标 C# Options.cs:250 CommitFrequencyMs，默认 0；-1 为手动提交）
  #[arg(long = "aof-commit-ms", allow_hyphen_values = true)]
  pub aof_commit_ms: Option<i32>,

  /// AOF 提交等待档（对标 C# Options.cs:253 WaitForCommit，选项
  /// `--aof-commit-wait`，默认 false）：置位后会话解析期按命令依赖性维护
  /// `wait_for_aof_blocking`，应答出网前阻塞等待 AOF 提交落盘
  ///（C# RespServerSession.Send 读点；代价为逐命令延迟大幅上升）
  #[arg(long = "aof-commit-wait", default_value_t = false)]
  #[toml(default)]
  pub aof_commit_wait: bool,

  /// 无盘（diskless）复制同步开关（对标 C# Options.cs:458 ReplicaDisklessSync，
  /// defaults.conf:349 与 GarnetServerOptions.cs:410 默认 false）：副本侧发起
  /// 同步时按本开关在 diskless（副本经 CLUSTER ATTACH_SYNC 主动接入主端、主端
  /// 流式快照直推、零本地检查点文件）与 diskbased（检查点传输）两支选路，
  /// 消费面统一经 ClusterProvider 的 replica_diskless_sync 访问器读取
  #[arg(long = "repl-diskless-sync", default_value_t = false)]
  #[toml(default)]
  pub repl_diskless_sync: bool,

  /// 复制同步超时秒数（对标 C# Options.cs:466 `--repl-sync-timeout`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs:420 默认 5 秒；
  /// 0 = 无限超时，对齐 Options.cs:995 `<=0 ? InfiniteTimeSpan`）。消费面：
  /// 副本一致读等待回放推进与回放对齐栅栏的阻塞上界
  #[arg(
    long = "repl-sync-timeout",
    default_value_t = DEFAULT_REPLICA_SYNC_TIMEOUT_SECS,
    allow_negative_numbers = true
  )]
  #[toml(default = DEFAULT_REPLICA_SYNC_TIMEOUT_SECS)]
  pub replica_sync_timeout_secs: i32,

  /// 副本 attach 超时秒数（对标 C# Options.cs:462 `--repl-attach-timeout`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs:425 默认 60 秒；
  /// <= 0 = 无限超时，经 seconds_from_time_span 归 0 表达）。
  #[arg(
    long = "repl-attach-timeout",
    default_value_t = DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS,
    allow_negative_numbers = true
  )]
  #[toml(default = DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS)]
  pub replica_attach_timeout_secs: i64,

  /// 副本同步节流毫秒数（对标 C# Options.cs:433 `--replica-sync-delay`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs:382 默认 5；
  /// 0 = 关闭节流）
  #[arg(long = "replica-sync-delay", default_value_t = DEFAULT_REPLICA_SYNC_DELAY_MS)]
  #[toml(default = DEFAULT_REPLICA_SYNC_DELAY_MS)]
  pub replica_sync_delay_ms: i32,

  /// 主端 AOF 追加背压滞后预算字节（对标 C# Options.cs:442
  /// `--aof-sync-max-lag-bytes`（long，无 IntRangeValidation）与
  /// GarnetServerOptions.cs:395 默认 -1；-1 = 关闭，>=1 = 每子日志按 1/n 份额
  /// 滞停主端 AOF 追加）
  #[arg(
    long = "aof-sync-max-lag-bytes",
    default_value_t = DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
    allow_negative_numbers = true
  )]
  #[toml(default = DEFAULT_AOF_SYNC_MAX_LAG_BYTES)]
  pub aof_sync_max_lag_bytes: i64,

  /// AOF 尾位点后台前移轮询毫秒数（对标 C# Options.cs:243
  /// `--aof-tail-witness-freq` IntRangeValidation(0, int.MaxValue) 与
  /// defaults.conf:179 生效值 10；仅物理子日志 >1 生效）
  #[arg(long = "aof-tail-witness-freq", default_value_t = DEFAULT_AOF_TAIL_WITNESS_FREQ_MS)]
  #[toml(default = DEFAULT_AOF_TAIL_WITNESS_FREQ_MS)]
  pub aof_tail_witness_freq_ms: i32,

  /// 集群复制重连轮询秒数（对标 C# Options.cs:696
  /// `--cluster-replication-reestablishment-timeout` IntRangeValidation(0, max,
  /// includeMin, isRequired:false) 与 GarnetServerOptions.cs:640 默认 0；
  /// 0 = 禁用副本自动重连）
  #[arg(
    long = "cluster-replication-reestablishment-timeout",
    default_value_t = DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT
  )]
  #[toml(default)]
  pub cluster_replication_reestablishment_timeout: i32,

  /// Vector Set 量化任务数（对标 C# Options.cs:717
  /// `--vector-set-quantization-task-count` IntRangeValidation(0, max) 与
  /// defaults.conf:542 默认 0；0 = 按物理核数自动对齐；装配点 max(0) 折叠
  /// + 消费槽 1024 钳制）。
  ///
  /// 注：C# 另有 `--vector-set-replay-task-count`，本仓副本向量重放折叠为单
  /// 消费者、无并行重放子系统可接，加旋钮即占位实现，故不落地（见票结论）
  #[arg(
    long = "vector-set-quantization-task-count",
    default_value_t = DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
    allow_hyphen_values = true
  )]
  #[toml(default)]
  pub vector_set_quantization_task_count: i32,

  /// 混合日志紧缩档位（对标 C# Options.cs:271 `--compaction-type`，
  /// GetServerOptions:939 直取投影进 GarnetServerOptions.CompactionType，缺省
  /// 枚举零值 None：None 短路、Shift 平移 begin address、Lookup/Scan 走活跃性
  /// 检查。经本口播种 CONFIG 槽 16，消费面 wnode service 每轮现取回灌
  /// GcConfig，wkv gc 按档分派；非法档名 clap/serde 解析期拒启，无第二套校验）
  #[arg(
    long = "compaction-type",
    value_parser = parse_log_compaction_type,
    default_value = "None"
  )]
  #[toml(with = toml_log_compaction_type, default = LogCompactionType::None)]
  pub compaction_type: LogCompactionType,

  /// 哈希索引紧缩触发段数上限（对标 C# Options.cs:279 `--compaction-max-segments`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs 缺省 32：磁盘
  /// 日志段数达此值即触发紧缩。区间单点在 META 槽 15，本处不设第二套定界）
  #[arg(long = "compaction-max-segments", default_value_t = DEFAULT_COMPACTION_MAX_SEGMENTS)]
  #[toml(default = DEFAULT_COMPACTION_MAX_SEGMENTS)]
  pub compaction_max_segments: i32,

  /// GET 散集合并开关（对标 C# Options.cs:430 `--sg-get`（bool?）经
  /// GetServerOptions:987 `GetValueOrDefault(true)` 落默认 true，与
  /// GarnetServerOptions.cs:376 EnableScatterGatherGet 缺省一致：连续 GET 走
  /// scatter-gather IO 以饱和随机读盘；经本口播种 CONFIG 槽 19，会话级
  /// get.rs 现取）
  #[arg(
    long = "sg-get",
    default_value_t = DEFAULT_ENABLE_SCATTER_GATHER_GET,
    action = clap::ArgAction::Set
  )]
  #[toml(default = DEFAULT_ENABLE_SCATTER_GATHER_GET)]
  pub enable_scatter_gather_get: bool,

  /// 副本 AOF 回放滞后节流预算字节（对标 C# Options.cs:438
  /// `--aof-replay-max-lag-bytes` IntRangeValidation(-1, int.MaxValue) 与
  /// GarnetServerOptions.cs:387 缺省 -1；0=同步回放、>=1=后台回放按此滞后、
  /// -1=无限滞后不节流。经本口播种 CONFIG 槽 9 并由 boot.rs 直读注入
  /// ClusterProvider 推流门限）
  #[arg(
    long = "aof-replay-max-lag-bytes",
    default_value_t = DEFAULT_AOF_REPLAY_MAX_LAG_BYTES,
    allow_negative_numbers = true
  )]
  #[toml(default = DEFAULT_AOF_REPLAY_MAX_LAG_BYTES)]
  pub aof_replay_max_lag_bytes: i32,

  /// 无盘同步宽限期秒数（对标 C# Options.cs:461 `--repl-diskless-sync-delay`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs:415 缺省 5；
  /// 0=立即启动无盘复制同步。经本口播种 CONFIG 槽 12 并由 boot.rs 直读注入
  /// ClusterProvider 主端攒批开窗等待时长）
  #[arg(
    long = "repl-diskless-sync-delay",
    default_value_t = DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS
  )]
  #[toml(default = DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS)]
  pub replica_diskless_sync_delay: i32,

  /// defaults.conf:343 与 GarnetServerOptions.cs:400 默认 false）：副本喂完即截断
  /// AOF（不等检查点提交），消费面为副本接收面跳跃重对齐与
  /// ClusterProvider::allow_data_loss 派生式
  #[arg(long = "fast-aof-truncate", default_value_t = false)]
  #[toml(default)]
  pub fast_aof_truncate: bool,

  /// 按需检查点开关（对标 C# Options.cs:453-454 OnDemandCheckpoint，
  /// defaults.conf:346 与 GarnetServerOptions.cs:405 默认 true）：与
  /// fast-aof-truncate 配套——主端在副本 attach 前发现检查点覆盖起点已落后截断线时
  /// 补拍一次检查点，避免直推 AOF 丢数据。两处读者为按需重拍判据与
  /// ClusterProvider::allow_data_loss 派生式（C# ReplicaSyncSession.cs:190、:280）
  #[arg(
    long = "on-demand-checkpoint",
    default_value_t = DEFAULT_ON_DEMAND_CHECKPOINT,
    action = clap::ArgAction::Set
  )]
  #[toml(default = DEFAULT_ON_DEMAND_CHECKPOINT)]
  pub on_demand_checkpoint: bool,

  /// 日志追加文件路径（设置后日志同步落文件；对标 serverSettings.FileLogger）
  #[arg(long)]
  pub file_logger: Option<String>,

  /// 控制台日志最低级别（trace/debug/info/warn/error；对标 serverSettings.LogLevel）
  #[arg(long)]
  pub log_level: Option<String>,

  /// 启动静默开关：不输出启动横幅与监听就绪文本（对标 C# Options.cs:363-364
  /// QuietMode，短选项 `-q`、别名 quiet_mode；C# bool? 缺省经
  /// GetValueOrDefault 折 false。消费面 wnode ServerBootstrap::run_async，
  /// 门禁对位 GarnetServer.cs:174 横幅与 :535 `* Ready to accept connections`）
  #[arg(short = 'q', long, alias = "quiet_mode", default_value_t = false)]
  #[toml(default)]
  pub quiet: bool,

  /// 关闭控制台日志 sink（对标 C# Options.cs:374-375 DisableConsoleLogger，
  /// 别名 DisableConsoleLogger；消费面 wnode LoggingBuilder::from_node，
  /// 门禁对位 GarnetServer.cs:115-122 仅在未禁用时 AddSimpleConsole）
  #[arg(
    long = "disable-console-logger",
    alias = "DisableConsoleLogger",
    default_value_t = false
  )]
  #[toml(default)]
  pub disable_console_logger: bool,

  /// 慢日志记录阈值微秒（0 = 禁用记录；对标 C# Options.cs:351 SlowLogThreshold）
  #[arg(
    long = "slowlog-log-slower-than",
    alias = "slow-log-threshold",
    default_value_t = DEFAULT_SLOW_LOG_THRESHOLD
  )]
  #[toml(default)]
  pub slow_log_threshold: i32,

  /// 慢日志容量上限（对标 C# Options.cs:355 SlowLogMaxEntries，默认 128 =
  /// GarnetServerOptions.cs:292）
  #[arg(long = "slowlog-max-len", default_value_t = DEFAULT_SLOW_LOG_MAX_ENTRIES)]
  #[toml(default = DEFAULT_SLOW_LOG_MAX_ENTRIES)]
  pub slow_log_max_entries: i32,

  /// 逻辑数据库数量上限（对标 C# Options.cs:688 MaxDatabases，默认 16；
  /// C# GarnetServer.cs:314 集群形态恒 1，本仓自定义设计删除集群限制，
  /// 单机与集群全模式采纳本配置自由切库）
  #[arg(long = "max-databases", default_value_t = DEFAULT_MAX_DATABASES)]
  #[toml(default = DEFAULT_MAX_DATABASES)]
  pub max_databases: i32,

  /// 保护模式：bind 未显式指定时回退回环监听（true）或监听全部接口（false；
  /// 对标 C# Options.cs:602 ProtectedMode，默认 yes，C# Format.TryParseAddressList）
  #[arg(
    long = "protected-mode",
    default_value_t = DEFAULT_PROTECTED_MODE,
    action = clap::ArgAction::Set
  )]
  #[toml(default = DEFAULT_PROTECTED_MODE)]
  pub protected_mode: bool,

  /// DEBUG 命令连接保护档 no/local/yes（no = 全拒、local = 仅本机回环与
  /// Unix 套接字放行、yes = 全放行；对标 C# Options.cs:594
  /// [Option("enable-debug-command")] ConnectionProtectionOption，经
  /// Options.cs:1018 投影进 GarnetServerOptions.cs:561 EnableDebugCommand，
  /// C# 枚举零值 No 即缺省。C# RuntimeServerConfig 未设该 CONFIG 名额，
  /// 本仓 CONFIG GET/SET 面同样不暴露）
  #[arg(
    long = "enable-debug-command",
    default_value_t = ConnectionProtectionOption::No
  )]
  #[toml(default = ConnectionProtectionOption::No)]
  pub enable_debug_command: ConnectionProtectionOption,

  /// 本节点向集群其他节点宣告的连接主机名（对标 C# Options.cs:56
  /// ClusterAnnounceHostname / libs/host/defaults.conf:19 默认空）：非空即直取
  /// 该值；空则由集群装配回退一次 OS 主机名（C# Format.GetHostName）。经
  /// NodeArgs 的 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "cluster-announce-hostname", default_value = "")]
  #[toml(default)]
  pub cluster_announce_hostname: String,

  /// 集群节点间互信认证用户名（对标 C# Options.cs:177-178
  /// `[Option("cluster-username")]` ClusterUsername → GarnetServerOptions.cs:266
  /// → ClusterProvider.cs:56 构造期注入 AuthContainer）。启动即建互信，杜绝
  /// 冷启动握手被对端拒后须外部 CONFIG SET 补救；未配置 None = 明文集群
  #[arg(long = "cluster-username")]
  pub cluster_username: Option<String>,

  /// 集群节点间互信认证密码（对标 C# Options.cs:181-182
  /// `[HiddenOption][Option("cluster-password")]` ClusterPassword →
  /// GarnetServerOptions.cs:271 → ClusterProvider.cs:57 构造期注入
  /// AuthContainer）。与用户名同为启动期互信凭据；口令属机密，沿用 C# 隐藏项
  /// 形态不上 --help
  #[arg(long = "cluster-password", hide = true)]
  pub cluster_password: Option<String>,

  /// *SCAN 命令单次迭代返回项数上限（对标 C# Options.cs:590
  /// ObjectScanCountLimit，默认 1000）
  #[arg(
    long = "object-scan-count-limit",
    default_value_t = DEFAULT_OBJECT_SCAN_COUNT_LIMIT
  )]
  #[toml(default = DEFAULT_OBJECT_SCAN_COUNT_LIMIT)]
  pub object_scan_count_limit: i32,

  /// 周期对象过期收集频率秒数（0 = 禁用，按需 HCOLLECT/ZCOLLECT 兜底；
  /// 对标 C# Options.cs:269 ExpiredObjectCollectionFrequencySecs /
  /// GarnetServerOptions.cs:216，默认 0）。副作用注：该旋钮兼分层键后台
  /// 降阶评估轮启动门（collection.md 3.3），置 0 时过期收集与冷分层键
  /// 降阶评估一并不跑，登记见 doc/zh/deviations.md §120
  #[arg(
    long = "expired-object-collection-freq",
    default_value_t = DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS
  )]
  #[toml(default)]
  pub expired_object_collection_frequency_secs: i32,

  /// 过期键后台删除扫描周期秒数（<= 0 = 禁用后台扫描，按需 EXPDELSCAN 兜底；
  /// > 0 = 扫描节拍）对标 C# Options.cs:691-693
  /// > --expired-key-deletion-scan-freq IntRangeValidation(-1, int.MaxValue) /
  /// > Options.cs:1033 → GarnetServerOptions.cs:162，默认 -1（defaults.conf:524）
  #[arg(
    long = "expired-key-deletion-scan-freq",
    default_value_t = DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS
  )]
  #[toml(default = DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS)]
  pub expired_key_deletion_scan_frequency_secs: i32,

  /// 指标监视器采样周期秒数（0 = 禁用采样任务；对标 C# Options.cs:359
  /// MetricsSamplingFrequency）
  #[arg(
    long = "metrics-sampling-freq",
    default_value_t = DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
  )]
  #[toml(default)]
  pub metrics_sampling_frequency_secs: u64,

  /// 是否启用延迟监视（跟踪各事件类别延迟分布；对标 C# Options.cs:344
  /// LatencyMonitor）
  #[arg(
    long = "latency-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[toml(default)]
  pub latency_monitor: bool,

  /// 是否启用逐命令使用统计（calls / failed / rejected，经 INFO COMMANDSTATS
  /// 输出；对标 C# Options.cs:348 CommandStatsMonitor）
  #[arg(
    long = "commandstats-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[toml(default)]
  pub commandstats_monitor: bool,

  /// 是否启用 Lua 脚本（对标 C# Options.cs:284 EnableLua，GarnetServerOptions.cs:91
  /// 默认 false）
  #[arg(long, default_value_t = false)]
  #[toml(default)]
  pub enable_lua: bool,

  /// Lua 脚本单次执行超时毫秒数（0 = 无限；对标 C# Options.cs:651 LuaScriptTimeoutMs
  /// 带 IntRangeValidation(10, int.MaxValue, isRequired: false)：0 为禁用合法值、
  /// 其余须落 [10, int.MaxValue]，负值/过小值启动拒启（见 [`NodeArgs::validate`]），
  /// 0 → C# :1029 Timeout.InfiniteTimeSpan 即不建超时管理器）
  #[arg(long, default_value_t = 0)]
  #[toml(default)]
  pub lua_script_timeout_ms: i64,

  /// Lua 脚本内存管理模式（对标 C# Options.cs:642 LuaMemoryManagementMode，
  /// 默认 Native）。仅 Tracked/Managed 施加脚本限额，Native 与限额互斥（见
  /// [`NodeArgs::validate`]）。缺省真源与 wlua 消费端 `LuaOptions::default`
  /// 同值（Native），不另立常量。经 NodeArgs 的 toml 派生自动纳入 TOML 导入/导出面
  #[arg(
    long = "lua-memory-management-mode",
    default_value_t = LuaMemoryManagementMode::Native
  )]
  #[toml(default = LuaMemoryManagementMode::Native)]
  pub lua_memory_management_mode: LuaMemoryManagementMode,

  /// Lua 脚本单次执行内存限额（尺寸字符串如 "10mb"；对标 C# Options.cs:647
  /// LuaScriptMemoryLimit，带 [MemorySizeValidation(false)] + [ForbiddenWithOption
  /// (LuaMemoryManagementMode.Native)]：缺省 None = 无限制；与 Native 模式同时
  /// 设置启动拒启。字节量单点投影见 [`NodeArgs::lua_memory_limit_bytes`]，
  /// [1K, 2GB] 值域闸复用 wlua `LuaOptions::get_memory_limit_bytes` 单点，本层
  /// 不复刻第二道闸。经 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "lua-script-memory-limit")]
  pub lua_script_memory_limit: Option<String>,

  /// redis.log 行为模式（对标 C# Options.cs:655 LuaLoggingMode，默认 Enable——
  /// defaults.conf:509 在册生效默认，与 wlua `LuaOptions::default` 同值同源）。
  /// 经 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "lua-logging-mode", default_value_t = LuaLoggingMode::Enable)]
  #[toml(default = LuaLoggingMode::Enable)]
  pub lua_logging_mode: LuaLoggingMode,

  /// 沙箱导出函数白名单（逗号分隔；对标 C# Options.cs:664 LuaAllowedFunctions，
  /// [Option(..., Separator = ',')]，默认空 = 不裁剪走默认集）。CLI 以逗号切分、
  /// TOML 以数组形态导入导出
  #[arg(long = "lua-allowed-functions", value_delimiter = ',')]
  #[toml(default)]
  pub lua_allowed_functions: Vec<String>,

  /// 是否启用 Vector Set 预览（VADD/VSIM/VMGET 等向量集合命令；对标 C#
  /// Options.cs:704 EnableVectorSetPreview，经 Options.cs:1036 投影进
  /// GarnetServerOptions.cs:661，defaults.conf:533 默认 false）。默认口径
  /// 照 C# 取 false：预览特性未稳定，生产按需显式开启；开启后向量管理器
  /// 量化/清理后台链随首会话拉起。经 NodeArgs 的 toml 派生自动纳入
  /// TOML 导入/导出面
  #[arg(long = "enable-vector-set-preview", default_value_t = false)]
  #[toml(default)]
  pub enable_vector_set_preview: bool,

  /// AOF 体积限额（尺寸字符串如 "64mb"，向下取 2 的幂；周期检查超限即自动
  /// checkpoint 并截断 AOF 防磁盘无界增长。空 = 关闭（默认，对标 C#
  /// Options.cs:256 AofSizeLimit = ""）；须与 AOF 同启）
  #[arg(long = "aof-size-limit")]
  pub aof_size_limit: Option<String>,

  /// AOF 常驻内存窗口上限（尺寸字符串如 "128m"，就近下取 2 的幂，超出即溢盘；
  /// 对标 C# Options.cs:211-213 AofMemorySize 旗标 `--aof-memory`
  ///（[MemorySizeValidation]）。缺省唯一真源在 `RuntimeServerOptions::default()`
  ///（"128m"），NodeArgs 不携带第二套缺省常量；组合互校验（须至少为 aof-page-size
  /// 的两倍）唯一真源在 wnode `AofSettings::from_options`，启动期执行。经
  /// NodeArgs 的 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "aof-memory")]
  pub aof_memory_size: Option<String>,

  /// AOF 日志页容量（尺寸字符串如 "32m"，就近下取 2 的幂；对标 C#
  /// Options.cs:215-217 AofPageSize 旗标 `--aof-page-size`
  ///（[MemorySizeValidation]）。缺省唯一真源在 RuntimeServerOptions（"32m"）；
  /// 页容量下限由 wconf 页尺寸校验核（`size::validated_page_size_bits`）承担，
  /// 组合互校验（须至少为主存日志页的两倍、且不得大于 aof-segment-size）唯一
  /// 真源在 wnode `AofSettings::from_options`。经 toml 派生自动纳入
  /// TOML 导入/导出面
  #[arg(long = "aof-page-size")]
  pub aof_page_size: Option<String>,

  /// AOF 物理段（文件）容量（尺寸字符串如 "1g"，就近下取 2 的幂；段文件创建与
  /// 回收粒度。对标 C# Options.cs:219-221 AofSegmentSize 旗标
  /// `--aof-segment-size`（[MemorySizeValidation]）。缺省唯一真源在
  /// RuntimeServerOptions（"1g"）；组合互校验（页不得大于段）唯一真源在 wnode
  /// `AofSettings::from_options`。经 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "aof-segment-size")]
  pub aof_segment_size: Option<String>,

  /// AOF 体积限额检查周期秒数（对标 C# Options.cs:260
  /// AofSizeLimitEnforceFrequencySecs，默认 5）
  #[arg(
    long = "aof-size-limit-enforce-frequency",
    default_value_t = DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS
  )]
  #[toml(default = DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS)]
  pub aof_size_limit_enforce_frequency_secs: u64,

  /// 哈希索引内存上限（尺寸字符串，向下取 2 的幂再折算 64B 桶数；周期检测
  /// 溢出桶超阈值即自动翻倍扩容直至上限。空 = 关闭（默认，对标 C#
  /// Options.cs:616 IndexMaxMemorySize = "" → AdjustedIndexMaxCacheLines = 0
  /// 不注册任务））
  #[arg(long = "index-max-size")]
  pub index_max_size: Option<String>,

  /// 索引自动扩容检测周期秒数（对标 C# Options.cs:618
  /// IndexResizeFrequencySecs，默认 60）
  #[arg(
    long = "index-resize-frequency",
    default_value_t = DEFAULT_INDEX_RESIZE_FREQUENCY_SECS
  )]
  #[toml(default = DEFAULT_INDEX_RESIZE_FREQUENCY_SECS)]
  pub index_resize_frequency_secs: u64,

  /// 索引自动扩容触发阈值百分比：溢出桶数超过 index_size × 阈值% 即扩容
  ///（对标 C# Options.cs:620 IndexResizeThreshold，默认 50；0 = 一有溢出即扩）
  #[arg(
    long = "index-resize-threshold",
    default_value_t = DEFAULT_INDEX_RESIZE_THRESHOLD
  )]
  #[toml(default = DEFAULT_INDEX_RESIZE_THRESHOLD)]
  pub index_resize_threshold: i64,

  /// TOML 配置文件路径（对标 C# Options.cs:483 ConfigImportPath；
  /// 配置文件自身不可嵌套本项）
  #[arg(long, value_name = "FILE")]
  #[toml(skip)]
  pub config: Option<PathBuf>,

  /// 导出合并后的生效配置到 TOML 文件（对标 C# Options.cs:501
  /// ConfigExportPath；C# 仅导出非默认项，Rust 导出全量字段）
  #[arg(long, value_name = "FILE")]
  #[toml(skip)]
  pub config_export_path: Option<PathBuf>,
}

/// 出站远端证书校验缺省开（对标 garnet/libs/host/defaults.conf:253
/// ServerCertificateRequired: true）
/// 命令行档名解析（对标 C# Options.cs:271 `--compaction-type` 的 Enum.Parse
/// 忽略大小写语义；复用 wbase::cfg::LogCompactionType::try_parse 单点，非法
/// 档名在 clap 解析期即拒启）
fn parse_log_compaction_type(value: &str) -> Result<LogCompactionType, String> {
  LogCompactionType::try_parse(value)
    .ok_or_else(|| format!("compaction-type 取值须为 None/Shift/Lookup/Scan 之一，当前为 {value}"))
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
  let size =
    try_parse_size(raw).ok_or_else(|| NodeOptionsError::InvalidSizeStr(name, raw.to_string()))?;
  let adjusted = previous_power_of_2(size);
  if (lo..=hi).contains(&adjusted) {
    Ok(())
  } else {
    Err(NodeOptionsError::SizeOutOfRange(name, lo, size))
  }
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

impl Default for NodeArgs {
  fn default() -> Self {
    Self {
      hlog: HlogOptions::default(),
      bind: None,
      port: DEFAULT_PORT,
      unixsocket: None,
      unixsocket_perm: None,
      dir: PathBuf::from(DEFAULT_DIR),
      wal_dir: None,
      checkpoint_dir: None,
      requirepass: None,
      tls_cert: None,
      tls_key: None,
      tls_client_cert_required: true,
      tls_client_target_host: None,
      tls_server_cert_required: true,
      tls_issuer_cert: None,
      tls_cert_refresh_freq: 0,
      threads: None,
      network_connection_limit: DEFAULT_NETWORK_CONNECTION_LIMIT,
      aof: false,
      disable_pubsub: false,
      recover: false,
      aof_commit_ms: None,
      aof_commit_wait: false,
      repl_diskless_sync: false,
      fast_aof_truncate: false,
      on_demand_checkpoint: DEFAULT_ON_DEMAND_CHECKPOINT,
      file_logger: None,
      log_level: None,
      quiet: false,
      disable_console_logger: false,
      slow_log_threshold: DEFAULT_SLOW_LOG_THRESHOLD,
      slow_log_max_entries: DEFAULT_SLOW_LOG_MAX_ENTRIES,
      max_databases: DEFAULT_MAX_DATABASES,
      protected_mode: DEFAULT_PROTECTED_MODE,
      enable_debug_command: ConnectionProtectionOption::No,
      cluster_announce_hostname: String::new(),
      cluster_username: None,
      cluster_password: None,
      object_scan_count_limit: DEFAULT_OBJECT_SCAN_COUNT_LIMIT,
      expired_object_collection_frequency_secs: DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS,
      expired_key_deletion_scan_frequency_secs: DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
      metrics_sampling_frequency_secs: DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS,
      latency_monitor: false,
      commandstats_monitor: false,
      enable_lua: false,
      lua_script_timeout_ms: 0,
      lua_memory_management_mode: LuaMemoryManagementMode::Native,
      lua_script_memory_limit: None,
      lua_logging_mode: LuaLoggingMode::Enable,
      lua_allowed_functions: Vec::new(),
      enable_vector_set_preview: false,
      aof_size_limit: None,
      aof_memory_size: None,
      aof_page_size: None,
      aof_segment_size: None,
      aof_size_limit_enforce_frequency_secs: DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS,
      index_max_size: None,
      index_resize_frequency_secs: DEFAULT_INDEX_RESIZE_FREQUENCY_SECS,
      index_resize_threshold: DEFAULT_INDEX_RESIZE_THRESHOLD,
      replica_sync_timeout_secs: DEFAULT_REPLICA_SYNC_TIMEOUT_SECS,
      replica_attach_timeout_secs: DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS,
      replica_sync_delay_ms: DEFAULT_REPLICA_SYNC_DELAY_MS,
      aof_sync_max_lag_bytes: DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
      aof_tail_witness_freq_ms: DEFAULT_AOF_TAIL_WITNESS_FREQ_MS,
      cluster_replication_reestablishment_timeout:
        DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
      vector_set_quantization_task_count: DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
      compaction_type: LogCompactionType::None,
      compaction_max_segments: DEFAULT_COMPACTION_MAX_SEGMENTS,
      enable_scatter_gather_get: DEFAULT_ENABLE_SCATTER_GATHER_GET,
      aof_replay_max_lag_bytes: DEFAULT_AOF_REPLAY_MAX_LAG_BYTES,
      replica_diskless_sync_delay: DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS,
      config: None,
      config_export_path: None,
    }
  }
}

impl NodeArgs {
  /// 是否配置了任何 TLS 证书或出站配置选项
  #[inline]
  pub fn has_tls(&self) -> bool {
    self.tls_cert.is_some()
      || self.tls_key.is_some()
      || self.tls_issuer_cert.is_some()
      || self.tls_client_target_host.is_some()
  }

  /// 解析最低日志级别（serverSettings.LogLevel；缺省 Warning 对标
  /// defaults.conf:280，未知级别宽容回退 Information）
  ///
  /// libs/host/Configuration/TypeConverters.cs:RedisLogLevelTypeConverter 解析投影
  pub fn minimum_log_level(&self) -> LevelFilter {
    match self.log_level.as_deref() {
      Some(s) if s.eq_ignore_ascii_case("trace") => LevelFilter::Trace,
      Some(s) if s.eq_ignore_ascii_case("debug") => LevelFilter::Debug,
      Some(s) if s.eq_ignore_ascii_case("warn") || s.eq_ignore_ascii_case("warning") => {
        LevelFilter::Warn
      }
      Some(s) if s.eq_ignore_ascii_case("error") || s.eq_ignore_ascii_case("critical") => {
        LevelFilter::Error
      }
      Some(s) if s.eq_ignore_ascii_case("off") || s.eq_ignore_ascii_case("none") => {
        LevelFilter::Off
      }
      // 未知显式级别（含 info/information）宽容回退
      Some(_) => LevelFilter::Info,
      // 未配置缺省 Warning（defaults.conf:280 LogLevel；C# GarnetServerOptions.cs:302
      // 库级字段 Error 亦非 Information，取 conf 生效侧）
      None => LevelFilter::Warn,
    }
  }

  /// 生成网络端点定义列表（bind 多地址拆分唯一落点）
  ///
  /// 对标 libs/common/Format.cs:TryParseAddressList：bind 未指定或全空白时按
  /// protected-mode 回退（保护→回环 / 非保护→全接口；C# defaultBindLoopBack 与
  /// defaultBindAny 产出双端点）；否则按逗号与空格切分多地址（TrimEntries |
  /// RemoveEmptyEntries 语义，Format.cs:64），逐地址与 port 组合成端点。
  /// 若 bind 条目命中 UDS 路径形态（uds_path），显式报错拒启（对标 C#
  /// OptionsValidators.cs:373 拒绝非 TCP 地址），UDS 仅允许经 unixsocket 独立选项配置。
  /// unixsocket 尾部追加（对标 Options.cs:813-814）。
  /// 全空白条目被剔除后可能产出空列表，对应 C# Options.cs:796
  /// `endpoints.Length == 0` 的拒启臂，由消费侧 GarnetServer::new 校验。
  pub fn endpoints(&self) -> Result<Vec<String>, NodeOptionsError> {
    let raw = self.bind.as_deref().unwrap_or_default().trim();
    let bind = if raw.is_empty() {
      if self.protected_mode {
        DEFAULT_BIND
      } else {
        DEFAULT_BIND_ANY
      }
    } else {
      raw
    };
    let mut eps = Vec::new();
    for a in bind
      .split([',', ' '])
      .map(str::trim)
      .filter(|a| !a.is_empty())
    {
      if uds_path(a).is_some() {
        return Err(NodeOptionsError::InvalidAddress(a.to_string()));
      }
      eps.push(format_bind_endpoint(a, self.port));
    }
    if let Some(ref u) = self.unixsocket {
      eps.push(format!("unix:{u}"));
    }
    Ok(eps)
  }

  /// UDS 套接字文件权限模式位（八进制数字字面量折算真实位值：600 → 0o600，
  /// 对标 Options.cs:816-817 Convert.ToInt32(_, 8) 转 UnixFileMode；None 或
  /// 值 0 返回 None = 不设置，对标 GarnetServerTcp.cs:149
  /// `unixSocketPermission != default` 跳过臂；数字位有效性由 validate 单点
  /// 拒启保证，此处纯算术折算无二次校验）
  ///
  /// libs/host/Configuration/Options.cs:816-817
  #[must_use]
  pub fn unix_socket_mode(&self) -> Option<u32> {
    let perm = self.unixsocket_perm.filter(|p| *p != 0)?;
    // 逐位权展开即八进制折算（各位 ≤7 已由 validate 保证）
    Some((perm / 100) as u32 * 64 + (perm / 10 % 10) as u32 * 8 + (perm % 10) as u32)
  }

  /// 获取计算后的 WAL / AOF 日志工作目录（优先使用显式 wal_dir，否则为 dir/wal）
  pub fn wal_dir(&self) -> PathBuf {
    self.wal_dir.clone().unwrap_or_else(|| self.dir.join("wal"))
  }

  /// 获取计算后的检查点基目录（优先显式 checkpoint_dir，否则回落数据目录；
  /// C# CheckpointBaseDirectory = CheckpointDir ?? LogDir，对标
  /// GarnetServerOptions.cs:625。CONFIG GET dir 与 wnode 检查点落盘同源本口）
  pub fn checkpoint_base_dir(&self) -> PathBuf {
    self
      .checkpoint_dir
      .clone()
      .unwrap_or_else(|| self.dir.clone())
  }

  /// 数据文件完整路径（`{dir}/wedb.db`，单机与集群唯一真源）
  ///
  /// 两宿主二进制均经此口取数据路径，杜绝模式各写各的文件名（对标 C#
  /// 单一命名方案 `GarnetServer.cs:479-484`）；见 [`DATA_FILE`] 的互踩说明。
  pub fn data_path(&self) -> PathBuf {
    self.dir.join(DATA_FILE)
  }

  /// 启动期数值定界与组合互校验（对标 C# ServerSettingsManager.cs:149
  /// options.IsValid 的 IntRangeValidation 漏斗末端校验 + GarnetServer.cs:508-519
  /// CreateAOF 的提交组合拒启面 + GarnetServerOptions.cs:839-840 的延迟监视
  /// 伴采样节拍校验；max_databases 界锚 Options.cs:687
  /// IntRangeValidation(1, 256)，堵死配置派生大库号的 OOM 通路）
  pub fn validate(&self) -> Result<(), NodeOptionsError> {
    // bind 端点形态合法性校验（拦截 UDS 路径混入 bind）
    self.endpoints()?;

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
    ] {
      if let Some(text) = raw
        && try_parse_size(text).is_none()
      {
        return Err(NodeOptionsError::InvalidSizeStr(opt, text.to_string()));
      }
    }
    check_range(
      "max-databases",
      self.max_databases,
      MAX_DATABASES_MIN,
      MAX_DATABASES_MAX,
    )?;
    // C# Options.cs:398 IntRangeValidation(-1, int.MaxValue)
    check_range(
      "network-connection-limit",
      self.network_connection_limit,
      DEFAULT_NETWORK_CONNECTION_LIMIT,
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
    // unixsocketperm 启动期定界（C# Options.cs:683 IntRangeValidation(0, 777)）
    // 与八进制数字位校验（C# Options.cs:817 Convert.ToInt32(_, 8) 转换失败面）
    if let Some(perm) = self.unixsocket_perm {
      check_range("unixsocketperm", perm, 0, UNIX_SOCKET_PERM_MAX)?;
      if perm.to_string().bytes().any(|d| d > b'7') {
        return Err(NodeOptionsError::UnixSocketPermDigits(perm));
      }
    }
    // C# GarnetServerOptions.cs:839-840 `LatencyMonitor && MetricsSamplingFrequency
    // == 0` 即拒启——全仓唯一校验点（禁各装配路径重复判定）
    if self.latency_monitor && self.metrics_sampling_frequency_secs == 0 {
      return Err(NodeOptionsError::LatencyMonitorWithoutMetrics);
    }
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
      if try_parse_size(raw).is_none() {
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

  /// 投影运行时服务选项（对标 C# Options.GetServerOptions 的服务选项装配段；
  /// RuntimeServerOptions 为 RuntimeServerConfig 播种的运行时单一真源）
  ///
  /// libs/host/Configuration/Options.cs:GetServerOptions
  pub fn runtime_server_options(&self) -> RuntimeServerOptions {
    let mut opts = RuntimeServerOptions::default();
    if let Some(ms) = self.aof_commit_ms {
      opts.commit_frequency_ms = ms;
    }
    opts.slow_log_threshold = self.slow_log_threshold;
    opts.slow_log_max_entries = self.slow_log_max_entries;
    opts.max_databases = self.max_databases;
    opts.object_scan_count_limit = self.object_scan_count_limit;
    opts.expired_object_collection_frequency_secs = self.expired_object_collection_frequency_secs;
    // C# Options.cs:1033 → GarnetServerOptions.ExpiredKeyDeletionScanFrequencySecs
    // → RuntimeServerConfig.cs:264 槽位播种 → StoreWrapper.cs:994-999
    // TryStartExpiredKeyDeletionTask 的启动期唯一写入路径
    opts.expired_key_deletion_scan_frequency_secs = self.expired_key_deletion_scan_frequency_secs;
    // C# Options.cs:934 WaitForCommit → GarnetServerOptions.WaitForCommit 投影
    opts.wait_for_commit = self.aof_commit_wait;
    // C# Options.cs:991-992 FastAofTruncate / OnDemandCheckpoint 投影（rust 不接
    // --main-memory-replication 弃用别名，GetFastAofTruncate 即直取本值）
    opts.fast_aof_truncate = self.fast_aof_truncate;
    opts.on_demand_checkpoint = self.on_demand_checkpoint;

    // <=0 折无限超时哨兵 u64::MAX 秒（对标 Options.cs:995
    // `<=0 ? InfiniteTimeSpan`）：直强转会落 Duration::from_secs(0)，副本一致
    // 读在等待回放推进处立即超时，语义完全反转；负值同臂归哨兵
    opts.replica_sync_timeout_secs = if self.replica_sync_timeout_secs <= 0 {
      u64::MAX
    } else {
      self.replica_sync_timeout_secs as u64
    };
    opts.replica_attach_timeout_secs = self.replica_attach_timeout_secs;
    opts.replica_sync_delay_ms = self.replica_sync_delay_ms;
    opts.cluster_replication_reestablishment_timeout =
      self.cluster_replication_reestablishment_timeout;
    opts.aof_tail_witness_freq_ms = self.aof_tail_witness_freq_ms;
    opts.aof_sync_max_lag_bytes = self.aof_sync_max_lag_bytes;
    opts.vector_set_quantization_task_count = self.vector_set_quantization_task_count;

    // —— 五旋钮启动段承接（对标 C# Options.cs GetServerOptions 逐字段直取投影；
    // 缺省即引同一 DEFAULT_* 常量，故未显式给值时投影等于 default()，零分叉）——
    // C# Options.cs:939/:941 CompactionType / CompactionMaxSegments → 槽 16/15 播种
    // → wnode service 每轮现取回灌 GcConfig，wkv gc 按档分派
    opts.compaction_type = self.compaction_type;
    opts.compaction_max_segments = self.compaction_max_segments;
    // C# Options.cs:987 EnableScatterGatherGet → 槽 19 播种 → get.rs 会话级现取
    opts.enable_scatter_gather_get = self.enable_scatter_gather_get;
    // C# Options.cs:989/:994 AofReplayMaxLagBytes / ReplicaDisklessSyncDelay →
    // 槽 9/12 播种 + boot.rs 直读注入 ClusterProvider 推流门限与攒批开窗等待
    opts.aof_replay_max_lag_bytes = self.aof_replay_max_lag_bytes;
    opts.replica_diskless_sync_delay = self.replica_diskless_sync_delay;

    // —— 只读回显字段（CONFIG GET/INFO 经格式器直读，C# Options.cs:909-910、:921、
    // :935、:1030 逐字段投影进 GarnetServerOptions 的同源装配） ——
    // rust `checkpoint_base_dir()` ↔ C# CheckpointBaseDirectory（GarnetServerOptions.cs:625
    // CheckpointDir ?? LogDir 回落根；显式 --checkpoint-dir 优先，缺省回落单根 dir）
    opts.checkpoint_base_directory = self.checkpoint_base_dir().display().to_string();
    // rust `wal_dir()` ↔ C# LogDir（Options.cs:909 物理日志设备根；缺省 <dir>/wal
    // 恒有目录，口径单点取本结构 wal_dir()，路径落串统一 display 写法）
    opts.log_dir = Some(self.wal_dir().display().to_string());
    // C# Options.cs:1030 UnixSocketPath：与绑定侧 endpoints() 同读 unixsocket，
    // 此处仅供展示回显，不另起绑定链
    opts.unix_socket_path = self.unixsocket.clone();
    // C# Options.cs:921 EnableAOF：APPENDONLY 只读格式器直读源
    opts.enable_aof = self.aof;
    // C# Options.cs:935 AofSizeLimit 原样字符串入展示面；行为侧字节折算另走
    // aof_size_limit_bytes() 单点，不在展示侧解析
    opts.aof_size_limit = self.aof_size_limit.clone();
    // C# Options.cs:924-926 AofMemorySize / AofPageSize / AofSegmentSize 原样
    // 字符串投影：Some 即覆盖、None 保留 RuntimeServerOptions::default() 的
    // "128m"/"32m"/"1g"（缺省唯一真源，杜绝第二套缺省常量）；字节折算与组合
    // 体检统一在消费侧 wnode AofSettings::from_options，装配链不再二次解析
    opts.aof_memory_size = self.aof_memory_size.clone().or(opts.aof_memory_size);
    opts.aof_page_size = self.aof_page_size.clone().or(opts.aof_page_size);
    opts.aof_segment_size = self.aof_segment_size.clone().or(opts.aof_segment_size);
    // AofSizeLimitEnforceFrequencySecs 播种运行期槽位（C# 检查周期任务每轮
    // runtimeConfig.GetInt 现取的唯一真值链；u64 饱和收窄 i32 槽宽）
    opts.aof_size_limit_enforce_frequency_secs = self
      .aof_size_limit_enforce_frequency_secs
      .min(i32::MAX as u64) as i32;
    opts
  }

  /// AOF 体积限额字节（配置尺寸向下取 2 的幂；未配置或解析失败返回 None）
  ///
  /// libs/server/Servers/GarnetServerOptions.cs:AofSizeLimitSizeBits
  ///（`1L << bits` 等价 PreviousPowerOf2(size)）
  #[must_use]
  pub fn aof_size_limit_bytes(&self) -> Option<u64> {
    let raw = self.aof_size_limit.as_deref().filter(|s| !s.is_empty())?;
    let size = try_parse_size(raw)?;
    let adjusted = previous_power_of_2(size);
    Some(adjusted as u64)
  }

  /// 哈希索引内存上限桶数（尺寸向下取 2 的幂再按 64B/桶折算）
  ///
  /// libs/server/Servers/ServerOptions.cs:IndexSizeCachelines
  ///（adjustedSize / 64，每 cache line 64B 恰为一桶；越出 [<64, >1<<37]
  /// 双界即 None，与 C# throw 同口径。拒启单点在 [`NodeArgs::validate`]，
  /// 本处为投影侧兜底：直接经本口取值的调用链不吃 validate 时也不越闸）
  #[must_use]
  pub fn index_max_size_buckets(&self) -> Option<usize> {
    let raw = self.index_max_size.as_deref().filter(|s| !s.is_empty())?;
    let size = try_parse_size(raw)?;
    if size < INDEX_MAX_SIZE_MIN_BYTES {
      return None;
    }
    let adjusted = previous_power_of_2(size);
    if !(INDEX_MAX_SIZE_MIN_BYTES..=INDEX_MAX_SIZE_MAX_BYTES).contains(&adjusted) {
      return None;
    }
    Some((adjusted / INDEX_MAX_SIZE_MIN_BYTES) as usize)
  }

  /// Lua 脚本内存限额字节（尺寸串整体解析为 i64 字节量；未配置或解析失败 None）
  ///
  /// 对标 C# Options.cs:647 LuaScriptMemoryLimit 经 LuaOptions.cs:GetMemoryLimitBytes
  /// 的 `ParseSize` 折算臂。[1K, 2GB] 值域闸与 Native 忽略判定复用 wlua
  /// `LuaOptions::get_memory_limit_bytes` 单点，本层不复刻；Native 档的限额由
  /// [`NodeArgs::validate`] 的 ForbiddenWithOption 前置拒启，本处仅对合法形态
  /// 产出字节量供 attach 单点投影
  #[must_use]
  pub fn lua_memory_limit_bytes(&self) -> Option<i64> {
    let raw = self
      .lua_script_memory_limit
      .as_deref()
      .filter(|s| !s.is_empty())?;
    try_parse_size(raw)
  }

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

/// 将单条 bind 监听地址与端口组装为端点字符串
///
/// IPv6 地址若未包含方括号包裹，自动包裹方括号（对标 C# Format.cs 的 TryParseAddressList 方法），
/// 确保可被 `SocketAddr` / `ServerEndpoint` 正常解析
#[inline]
pub fn format_bind_endpoint(addr: &str, port: u16) -> String {
  let trimmed = addr.trim();
  if trimmed.starts_with('[') && trimmed.ends_with(']') {
    format!("{trimmed}:{port}")
  } else if trimmed.parse::<Ipv6Addr>().is_ok() {
    format!("[{trimmed}]:{port}")
  } else {
    format!("{trimmed}:{port}")
  }
}
