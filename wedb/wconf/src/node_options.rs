//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。
//! 配置文件格式钦定 nested_text（C# GarnetConf/RedisConf 双格式不转写）。

use std::{
  env::args_os,
  ffi::OsString,
  fs,
  io::Error,
  path::{Path, PathBuf},
  sync::Arc,
};

use clap::{ArgMatches, Parser};
use log::LevelFilter;
use serde::{Deserialize, Serialize};

use crate::{
  connection_protection_option::ConnectionProtectionOption,
  runtime_server_options::RuntimeServerOptions,
  size::{previous_power_of_2, try_parse_size, validated_page_size_bits},
};

/// 生产默认主存日志页容量字节（16MB，对标 C# ServerOptions.cs:46 PageSize = "16m"）
pub const DEFAULT_HLOG_PAGE_SIZE: usize = 16 * 1024 * 1024;

/// 默认监听端口
pub const DEFAULT_PORT: u16 = 6379;
/// 默认监听地址（保护模式回环回退；对标 C# Format.defaultBindLoopBack）
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// 非保护模式监听地址（对标 C# Format.defaultBindAny；C# 为 IPv4/IPv6 双栈
/// any，Rust 收敛 IPv4 any）
pub const DEFAULT_BIND_ANY: &str = "0.0.0.0";
/// 默认工作目录
pub const DEFAULT_DIR: &str = "./data";

/// 数据文件名（`{dir}/wedb.db`，单机与集群共用同一物理件）
///
/// 对标 C# 单一命名方案：`GarnetServer.cs:479-484` 集群与单机两臂共用同一
/// `defaultNamingScheme`（仅 CheckpointManager 类型分叉），`Options.cs:790-793`
/// LogDir/CheckpointDir 单套、`EnableCluster` 不换文件布局，模式切换复用同一
/// 数据文件。本仓检查点目录恒 `{dir}/Store/checkpoints`、WAL 恒 `{dir}/wal/wal.log`
/// 皆与模式无关，故数据文件名亦不随模式区分——否则集群二进制指向单机遗留目录会
/// 新建空数据文件当恢复设备、加载单机检查点索引（索引地址指向另一数据文件）、
/// 续写同一 wal.log，恢复静默错乱并写坏共享物理件。
pub const DATA_FILE: &str = "wedb.db";

/// 默认 RESP 协议版本（对标 libs/server/Servers/ServerOptions.cs:DEFAULT_RESP_VERSION）
pub const DEFAULT_RESP_VERSION: u8 = 2;

/// 慢日志记录阈值微秒（0 = 禁用；对标 C# Options.cs:351 SlowLogThreshold）
pub const DEFAULT_SLOW_LOG_THRESHOLD: i32 = 0;
/// 慢日志容量上限（对标 GarnetServerOptions.cs:292 SlowLogMaxEntries）
pub const DEFAULT_SLOW_LOG_MAX_ENTRIES: i32 = 128;
/// 默认逻辑数据库数量上限（对标 GarnetServerOptions.cs:615 MaxDatabases）
pub const DEFAULT_MAX_DATABASES: i32 = 16;
/// 逻辑数据库数量上限下界（对标 C# Options.cs:687 MaxDatabases 的
/// IntRangeValidation(1, 256, isRequired: true) 启动期定界）
pub const MAX_DATABASES_MIN: i32 = 1;
/// 逻辑数据库数量上限上界（对标 C# Options.cs:687 MaxDatabases 的
/// IntRangeValidation(1, 256, isRequired: true) 启动期定界）
pub const MAX_DATABASES_MAX: i32 = 256;
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
/// GarnetServerOptions.ExpiredObjectCollectionFrequencySecs 默认 0）
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
/// 主存日志选项：PageSize = "16m"、LogMemorySize = "16g"、MutablePercent = 50）
///
/// 全部字段可缺省：None 项不覆盖装配基线，由 `StoreConfig::auto()` 的内存预算
/// 规划器推导（大机预算 ≥ 1GB 时推导 [`DEFAULT_HLOG_PAGE_SIZE`] 页）。
/// nested_text 形态：
///
/// ```text
/// hlog:
///   page_size: 16777216
///   memory_size: 4294967296
///   mutable_percent: 50
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, clap::Args)]
pub struct HlogOptions {
  /// 主存日志单页容量字节（必须为 2 的幂且为扇区大小整数倍，且不低于
  /// [`crate::size::MIN_PAGE_SIZE_BYTES`]；未配置时由内存预算规划器推导，对标 C#
  /// GarnetServerOptions.cs PageSize = "16m"）。
  ///
  /// 页容量决定单条内联记录上限：值 ≤ 页容量 - 记录头 - 键 即可内联存储
  /// （16MB 页可承载 C# DefaultMaxInlineValueSize = 1MB 基线的大值）。
  #[arg(id = "hlog_page_size", long = "hlog-page-size")]
  #[serde(default)]
  pub page_size: Option<usize>,

  /// 主存日志内存环形缓冲预算字节（未配置时由内存预算规划器推导；
  /// 对标 C# GarnetServerOptions.cs LogMemorySize = "16g" 的 pageCount 推导：
  /// `num_pages = next_power_of_2(memory_size / page_size)`）
  #[arg(id = "hlog_memory_size", long = "hlog-memory-size")]
  #[serde(default)]
  pub memory_size: Option<usize>,

  /// 内存可变区百分比（10..=95，对标 C# GarnetServerOptions.cs MutablePercent
  /// = 50 与 GetSettings 的区间校验；未配置时取引擎默认比例）
  #[arg(id = "hlog_mutable_percent", long = "hlog-mutable-percent")]
  #[serde(default)]
  pub mutable_percent: Option<u8>,

  /// 是否启用 ReadCache 独立只读非脏页内存日志（对标 C# GarnetServerOptions.cs:582
  /// EnableReadCache，默认 false）
  #[arg(id = "read_cache", long = "read-cache")]
  #[serde(default)]
  pub read_cache: bool,

  /// ReadCache 内存预算字节（仅 `read_cache` 开启时参与页数推导：预算 / 主日志
  /// 页容量向下取 2 的幂；对标 C# GarnetServerOptions.cs:587
  /// ReadCacheMemorySize = "1g"。未配置时取 [`DEFAULT_READ_CACHE_MEMORY_SIZE`]）
  #[arg(id = "read_cache_memory_size", long = "read-cache-memory-size")]
  #[serde(default)]
  pub read_cache_memory_size: Option<usize>,

  /// 是否启用空间复活回收池与链内原地复活（对标 C# Options.cs:564-567
  /// EnableRevivification，命令行 `--reviv`，默认 false）
  #[arg(long = "reviv")]
  #[serde(default)]
  pub reviv: bool,

  /// 复活区间比例（对标 C# Options.cs:558-561 RevivifiableFraction，命令行
  /// `--reviv-fraction`，DoubleRangeValidation(0, 1)；None = 未配置，不覆盖
  /// 引擎默认值）。区间校验单点在 wkv `StoreConfig::validate`
  /// （(0, mutable_fraction]），本处不复校验，避免第二套真源
  #[arg(long = "reviv-fraction")]
  #[serde(default)]
  pub reviv_fraction: Option<f64>,

  /// 冷区/磁盘读取成功后是否将记录复制晋升到日志 Tail（对标 C#
  /// Options.cs:126-128 CopyReadsToTail，命令行 `--copy-reads-to-tail`，
  /// 默认 false；C# 经 GarnetServerOptions.cs:899-900 投影进
  /// kvSettings.ReadCopyOptions，store 级而非会话私有）
  #[arg(long = "copy-reads-to-tail")]
  #[serde(default)]
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

/// 节点参数加载错误（命令行解析、NestedText 配置解析、hlog 校验与文件读取；依赖库错误透明转发）。
#[derive(Debug, thiserror::Error)]
pub enum NodeOptionsError {
  /// 命令行解析失败。
  #[error(transparent)]
  Cli(#[from] clap::Error),
  /// NestedText 配置解析失败。
  #[error(transparent)]
  NestedText(#[from] nested_text::Error),
  /// 配置文件读取失败。
  #[error(transparent)]
  Io(#[from] Error),
  /// hlog 配置段显式项校验失败。
  #[error("hlog 配置非法: {0}")]
  Hlog(String),
  /// 数值配置项越界（对标 C# RangeValidationAttribute 校验失败的启动期拒绝面）。
  #[error("{0} expected to be in range [{1}, {2}]. Actual value: {3}")]
  ValueOutOfRange(&'static str, i32, i32, i32),
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
}

/// 统一节点基础参数配置
#[derive(Debug, Clone, Serialize, Deserialize, Parser)]
#[command(author, version, about = "WeDB 高性能分布式数据库服务")]
pub struct NodeArgs {
  /// 主存混合日志（hlog）配置段（nested_text 嵌套节 `hlog:`；未配置项交由
  /// 存储引擎内存预算规划器推导，见 [`HlogOptions`]）
  #[command(flatten)]
  #[serde(default)]
  pub hlog: HlogOptions,

  /// 绑定监听 IP 地址（未指定时按 protected-mode 回退：保护回环 / 非保护全接口）
  #[arg(short = 'b', long)]
  #[serde(default)]
  pub bind: Option<String>,

  /// 业务监听端口
  #[arg(short = 'p', long, default_value_t = DEFAULT_PORT)]
  #[serde(default = "default_port")]
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
  #[serde(default)]
  pub unixsocket_perm: Option<i32>,

  /// 数据与持久化存储工作目录
  #[arg(short = 'd', long, default_value = DEFAULT_DIR)]
  #[serde(default = "default_dir")]
  pub dir: PathBuf,

  /// WAL / AOF 物理日志存储路径（未显式指定时默认为 <dir>/wal）
  #[arg(long)]
  pub wal_dir: Option<PathBuf>,

  /// 访问认证密码
  #[arg(long)]
  pub requirepass: Option<String>,

  /// TLS 证书文件路径（PEM 格式）
  #[arg(long)]
  pub tls_cert: Option<PathBuf>,

  /// TLS 私钥文件路径（PEM 格式）
  #[arg(long)]
  pub tls_key: Option<PathBuf>,

  /// 入站 TLS 是否要求客户端证书（mTLS 双向认证；对标 C# Options.cs:333
  /// ClientCertificateRequired，C# 为 bool? 缺省 null 即 false）。true 且给出
  /// tls_issuer_cert → 以该 CA 校验客户端证书链；true 而未给 → 要求证书但
  /// 不校验颁发者链（GarnetTlsOptions.cs:273 告警语义）；false 即单向 TLS
  /// 零变化。C# 另有 Options.cs:336 certificate-revocation-check-mode 吊销
  /// 检查：rustls 需显式 CRL 输入面且本仓无此装配链，登记缺席不随本票落地
  /// （如需实现另立单）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub tls_client_cert_required: bool,

  /// 集群出站 TLS 目标主机名（对标 C# Options.cs:307 ClusterTlsClientTargetHost；
  /// 空 = 建连时回落对端地址的 host 段）
  #[arg(long)]
  pub tls_client_target_host: Option<String>,

  /// 出站方向是否校验远端证书（对标 C# Options.cs:311 ServerCertificateRequired，
  /// defaults.conf:253 默认 true；false 即不安全恒真模式）
  #[arg(long, default_value_t = true)]
  #[serde(default = "default_tls_server_cert_required")]
  pub tls_server_cert_required: bool,

  /// TLS 签发者 CA 证书路径（对标 C# Options.cs:339 IssuerCertificatePath，
  /// C# 单字段双用）：入站 mTLS（tls_client_cert_required=true）时作客户端
  /// 证书校验根；出站校验（tls_server_cert_required=true）时作远端证书根，
  /// 未指定时出站用内置 webpki 根、入站回落宽松不校验链模式
  #[arg(long)]
  pub tls_issuer_cert: Option<PathBuf>,

  /// 工作线程数（默认按可用 CPU 物理核心数）
  #[arg(short = 't', long)]
  pub threads: Option<usize>,

  /// 最大并发网络连接数（-1 = 不限；对标 C# Options.cs:399 键
  /// network-connection-limit、IntRangeValidation(-1, int.MaxValue) 与
  /// defaults.conf:304 默认 -1；accept 成功即刻计量在途数，超限臂即刻
  /// 关闭新连接且不写任何 RESP 应答，为 FD/内存耗尽的平台侧护栏）。
  /// 纯启动期旋钮：C# ServerConfigType 枚举不含此项（非 CONFIG GET/SET
  /// 运行时项），经 serde 派生自动纳入 nested_text 导入/导出面
  #[arg(
    long = "network-connection-limit",
    default_value_t = DEFAULT_NETWORK_CONNECTION_LIMIT,
    allow_hyphen_values = true
  )]
  #[serde(default = "default_network_connection_limit")]
  pub network_connection_limit: i32,

  /// 是否启用 AOF 持久化日志（对标 C# Options.cs:209 EnableAOF）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub aof: bool,

  /// 是否禁用发布订阅功能（对标 C# ServerOptions.cs:107 DisablePubSub，
  /// C# 默认 false 即默认启用 pubsub）
  #[arg(long = "disable-pubsub", default_value_t = false)]
  #[serde(default)]
  pub disable_pubsub: bool,

  /// 启动时从最新检查点与 AOF 日志恢复（若存在；对标 C# Options.cs:139 Recover）
  #[arg(short = 'r', long, default_value_t = false)]
  #[serde(default)]
  pub recover: bool,

  /// AOF 周期提交毫秒数（对标 C# Options.cs:250 CommitFrequencyMs，默认 0；-1 为手动提交）
  #[arg(long = "aof-commit-ms", allow_hyphen_values = true)]
  #[serde(default)]
  pub aof_commit_ms: Option<i32>,

  /// AOF 提交等待档（对标 C# Options.cs:253 WaitForCommit，选项
  /// `--aof-commit-wait`，默认 false）：置位后会话解析期按命令依赖性维护
  /// `wait_for_aof_blocking`，应答出网前阻塞等待 AOF 提交落盘
  ///（C# RespServerSession.Send 读点；代价为逐命令延迟大幅上升）
  #[arg(long = "aof-commit-wait", default_value_t = false)]
  #[serde(default)]
  pub aof_commit_wait: bool,

  /// 无盘（diskless）复制同步开关（对标 C# Options.cs:458 ReplicaDisklessSync，
  /// defaults.conf:349 与 GarnetServerOptions.cs:410 默认 false）：副本侧发起
  /// 同步时按本开关在 diskless（副本经 CLUSTER ATTACH_SYNC 主动接入主端、主端
  /// 流式快照直推、零本地检查点文件）与 diskbased（检查点传输）两支选路，
  /// 消费面统一经 ClusterProvider 的 replica_diskless_sync 访问器读取
  #[arg(long = "repl-diskless-sync", default_value_t = false)]
  #[serde(default)]
  pub repl_diskless_sync: bool,

  /// 快速 AOF 截断开关（对标 C# Options.cs:449-450 FastAofTruncate，
  /// defaults.conf:343 与 GarnetServerOptions.cs:400 默认 false）：副本喂完即截断
  /// AOF（不等检查点提交），消费面为副本接收面跳跃重对齐与
  /// ClusterProvider::allow_data_loss 派生式
  #[arg(long = "fast-aof-truncate", default_value_t = false)]
  #[serde(default)]
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
  #[serde(default = "default_on_demand_checkpoint")]
  pub on_demand_checkpoint: bool,

  /// 日志追加文件路径（设置后日志同步落文件；对标 serverSettings.FileLogger）
  #[arg(long)]
  pub file_logger: Option<String>,

  /// 控制台日志最低级别（trace/debug/info/warn/error；对标 serverSettings.LogLevel）
  #[arg(long)]
  pub log_level: Option<String>,

  /// 慢日志记录阈值微秒（0 = 禁用记录；对标 C# Options.cs:351 SlowLogThreshold）
  #[arg(
    long = "slowlog-log-slower-than",
    default_value_t = DEFAULT_SLOW_LOG_THRESHOLD
  )]
  #[serde(default = "default_slow_log_threshold")]
  pub slow_log_threshold: i32,

  /// 慢日志容量上限（对标 C# Options.cs:355 SlowLogMaxEntries，默认 128 =
  /// GarnetServerOptions.cs:292）
  #[arg(long = "slowlog-max-len", default_value_t = DEFAULT_SLOW_LOG_MAX_ENTRIES)]
  #[serde(default = "default_slow_log_max_entries")]
  pub slow_log_max_entries: i32,

  /// 逻辑数据库数量上限（对标 C# Options.cs:688 MaxDatabases，默认 16；
  /// C# GarnetServer.cs:314 集群形态恒 1，本仓自定义设计删除集群限制，
  /// 单机与集群全模式采纳本配置自由切库）
  #[arg(long = "max-databases", default_value_t = DEFAULT_MAX_DATABASES)]
  #[serde(default = "default_max_databases")]
  pub max_databases: i32,

  /// 保护模式：bind 未显式指定时回退回环监听（true）或监听全部接口（false；
  /// 对标 C# Options.cs:602 ProtectedMode，默认 yes，C# Format.TryParseAddressList）
  #[arg(
    long = "protected-mode",
    default_value_t = DEFAULT_PROTECTED_MODE,
    action = clap::ArgAction::Set
  )]
  #[serde(default = "default_protected_mode")]
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
  #[serde(default)]
  pub enable_debug_command: ConnectionProtectionOption,

  /// 本节点向集群其他节点宣告的连接主机名（对标 C# Options.cs:56
  /// ClusterAnnounceHostname / libs/host/defaults.conf:19 默认空）：非空即直取
  /// 该值；空则由集群装配回退一次 OS 主机名（C# Format.GetHostName）。经
  /// NodeArgs 的 serde 派生自动纳入 nested_text 导入/导出面
  #[arg(long = "cluster-announce-hostname", default_value = "")]
  #[serde(default)]
  pub cluster_announce_hostname: String,

  /// *SCAN 命令单次迭代返回项数上限（对标 C# Options.cs:590
  /// ObjectScanCountLimit，默认 1000）
  #[arg(
    long = "object-scan-count-limit",
    default_value_t = DEFAULT_OBJECT_SCAN_COUNT_LIMIT
  )]
  #[serde(default = "default_object_scan_count_limit")]
  pub object_scan_count_limit: i32,

  /// 周期对象过期收集频率秒数（0 = 禁用，按需 HCOLLECT/ZCOLLECT 兜底；
  /// 对标 C# Options.cs:269 ExpiredObjectCollectionFrequencySecs /
  /// GarnetServerOptions.cs:216，默认 0）
  #[arg(
    long = "expired-object-collection-freq",
    default_value_t = DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS
  )]
  #[serde(default = "default_expired_object_collection_frequency_secs")]
  pub expired_object_collection_frequency_secs: i32,

  /// 过期键后台删除扫描周期秒数（<= 0 = 禁用后台扫描，按需 EXPDELSCAN 兜底；
  /// > 0 = 扫描节拍）对标 C# Options.cs:691-693
  /// > --expired-key-deletion-scan-freq IntRangeValidation(-1, int.MaxValue) /
  /// > Options.cs:1033 → GarnetServerOptions.cs:162，默认 -1（defaults.conf:524）
  #[arg(
    long = "expired-key-deletion-scan-freq",
    default_value_t = DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS
  )]
  #[serde(default = "default_expired_key_deletion_scan_frequency_secs")]
  pub expired_key_deletion_scan_frequency_secs: i32,

  /// 指标监视器采样周期秒数（0 = 禁用采样任务；对标 C# Options.cs:359
  /// MetricsSamplingFrequency）
  #[arg(
    long = "metrics-sampling-freq",
    default_value_t = DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
  )]
  #[serde(default = "default_metrics_sampling_frequency_secs")]
  pub metrics_sampling_frequency_secs: u64,

  /// 是否启用延迟监视（跟踪各事件类别延迟分布；对标 C# Options.cs:344
  /// LatencyMonitor）
  #[arg(
    long = "latency-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[serde(default)]
  pub latency_monitor: bool,

  /// 是否启用逐命令使用统计（calls / failed / rejected，经 INFO COMMANDSTATS
  /// 输出；对标 C# Options.cs:348 CommandStatsMonitor）
  #[arg(
    long = "commandstats-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[serde(default)]
  pub commandstats_monitor: bool,

  /// 是否启用 Lua 脚本（对标 C# Options.cs:284 EnableLua，GarnetServerOptions.cs:91
  /// 默认 false）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub enable_lua: bool,

  /// Lua 脚本单次执行超时毫秒数（0 = 无限；对标 C# Options.cs LuaScriptTimeoutMs，
  /// 0 映射 Timeout.InfiniteTimeSpan 即不建超时管理器）
  #[arg(long, default_value_t = 0)]
  #[serde(default)]
  pub lua_script_timeout_ms: i64,

  /// 是否启用 Vector Set 预览（VADD/VSIM/VMGET 等向量集合命令；对标 C#
  /// Options.cs:704 EnableVectorSetPreview，经 Options.cs:1036 投影进
  /// GarnetServerOptions.cs:661，defaults.conf:533 默认 false）。默认口径
  /// 照 C# 取 false：预览特性未稳定，生产按需显式开启；开启后向量管理器
  /// 量化/清理后台链随首会话拉起。经 NodeArgs 的 serde 派生自动纳入
  /// nested_text 导入/导出面
  #[arg(long = "enable-vector-set-preview", default_value_t = false)]
  #[serde(default)]
  pub enable_vector_set_preview: bool,

  /// AOF 体积限额（尺寸字符串如 "64mb"，向下取 2 的幂；周期检查超限即自动
  /// checkpoint 并截断 AOF 防磁盘无界增长。空 = 关闭（默认，对标 C#
  /// Options.cs:256 AofSizeLimit = ""）；须与 AOF 同启）
  #[arg(long = "aof-size-limit")]
  #[serde(default)]
  pub aof_size_limit: Option<String>,

  /// AOF 体积限额检查周期秒数（对标 C# Options.cs:260
  /// AofSizeLimitEnforceFrequencySecs，默认 5）
  #[arg(
    long = "aof-size-limit-enforce-frequency",
    default_value_t = DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS
  )]
  #[serde(default = "default_aof_size_limit_enforce_frequency_secs")]
  pub aof_size_limit_enforce_frequency_secs: u64,

  /// 哈希索引内存上限（尺寸字符串，向下取 2 的幂再折算 64B 桶数；周期检测
  /// 溢出桶超阈值即自动翻倍扩容直至上限。空 = 关闭（默认，对标 C#
  /// Options.cs:616 IndexMaxMemorySize = "" → AdjustedIndexMaxCacheLines = 0
  /// 不注册任务））
  #[arg(long = "index-max-size")]
  #[serde(default)]
  pub index_max_size: Option<String>,

  /// 索引自动扩容检测周期秒数（对标 C# Options.cs:618
  /// IndexResizeFrequencySecs，默认 60）
  #[arg(
    long = "index-resize-frequency",
    default_value_t = DEFAULT_INDEX_RESIZE_FREQUENCY_SECS
  )]
  #[serde(default = "default_index_resize_frequency_secs")]
  pub index_resize_frequency_secs: u64,

  /// 索引自动扩容触发阈值百分比：溢出桶数超过 index_size × 阈值% 即扩容
  ///（对标 C# Options.cs:620 IndexResizeThreshold，默认 50；0 = 一有溢出即扩）
  #[arg(
    long = "index-resize-threshold",
    default_value_t = DEFAULT_INDEX_RESIZE_THRESHOLD
  )]
  #[serde(default = "default_index_resize_threshold")]
  pub index_resize_threshold: i64,

  /// nested_text 配置文件路径（对标 C# Options.cs:483 ConfigImportPath；
  /// 配置文件自身不可嵌套本项，serde skip 同 C# JsonIgnore 语义）
  #[arg(long, value_name = "FILE")]
  #[serde(skip)]
  pub config: Option<PathBuf>,

  /// 导出合并后的生效配置到 nested_text 文件（对标 C# Options.cs:501
  /// ConfigExportPath；C# 仅导出非默认项，Rust 导出全量字段）
  #[arg(long, value_name = "FILE")]
  #[serde(skip)]
  pub config_export_path: Option<PathBuf>,
}

fn default_port() -> u16 {
  DEFAULT_PORT
}

fn default_dir() -> PathBuf {
  PathBuf::from(DEFAULT_DIR)
}

/// 出站远端证书校验缺省开（对标 garnet/libs/host/defaults.conf:253
/// ServerCertificateRequired: true）
const fn default_tls_server_cert_required() -> bool {
  true
}

const fn default_slow_log_threshold() -> i32 {
  DEFAULT_SLOW_LOG_THRESHOLD
}

const fn default_slow_log_max_entries() -> i32 {
  DEFAULT_SLOW_LOG_MAX_ENTRIES
}

const fn default_max_databases() -> i32 {
  DEFAULT_MAX_DATABASES
}

const fn default_protected_mode() -> bool {
  DEFAULT_PROTECTED_MODE
}

const fn default_on_demand_checkpoint() -> bool {
  DEFAULT_ON_DEMAND_CHECKPOINT
}

const fn default_object_scan_count_limit() -> i32 {
  DEFAULT_OBJECT_SCAN_COUNT_LIMIT
}

const fn default_expired_object_collection_frequency_secs() -> i32 {
  DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS
}

const fn default_expired_key_deletion_scan_frequency_secs() -> i32 {
  DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS
}

const fn default_metrics_sampling_frequency_secs() -> u64 {
  DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
}

const fn default_network_connection_limit() -> i32 {
  DEFAULT_NETWORK_CONNECTION_LIMIT
}

const fn default_aof_size_limit_enforce_frequency_secs() -> u64 {
  DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS
}

const fn default_index_resize_frequency_secs() -> u64 {
  DEFAULT_INDEX_RESIZE_FREQUENCY_SECS
}

const fn default_index_resize_threshold() -> i64 {
  DEFAULT_INDEX_RESIZE_THRESHOLD
}

impl Default for NodeArgs {
  fn default() -> Self {
    Self {
      hlog: HlogOptions::default(),
      bind: None,
      port: default_port(),
      unixsocket: None,
      unixsocket_perm: None,
      dir: default_dir(),
      wal_dir: None,
      requirepass: None,
      tls_cert: None,
      tls_key: None,
      tls_client_cert_required: false,
      tls_client_target_host: None,
      tls_server_cert_required: default_tls_server_cert_required(),
      tls_issuer_cert: None,
      threads: None,
      network_connection_limit: default_network_connection_limit(),
      aof: false,
      disable_pubsub: false,
      recover: false,
      aof_commit_ms: None,
      aof_commit_wait: false,
      repl_diskless_sync: false,
      fast_aof_truncate: false,
      on_demand_checkpoint: default_on_demand_checkpoint(),
      file_logger: None,
      log_level: None,
      slow_log_threshold: default_slow_log_threshold(),
      slow_log_max_entries: default_slow_log_max_entries(),
      max_databases: default_max_databases(),
      protected_mode: default_protected_mode(),
      enable_debug_command: ConnectionProtectionOption::No,
      cluster_announce_hostname: String::new(),
      object_scan_count_limit: default_object_scan_count_limit(),
      expired_object_collection_frequency_secs: default_expired_object_collection_frequency_secs(),
      expired_key_deletion_scan_frequency_secs: default_expired_key_deletion_scan_frequency_secs(),
      metrics_sampling_frequency_secs: default_metrics_sampling_frequency_secs(),
      latency_monitor: false,
      commandstats_monitor: false,
      enable_lua: false,
      lua_script_timeout_ms: 0,
      enable_vector_set_preview: false,
      aof_size_limit: None,
      aof_size_limit_enforce_frequency_secs: default_aof_size_limit_enforce_frequency_secs(),
      index_max_size: None,
      index_resize_frequency_secs: default_index_resize_frequency_secs(),
      index_resize_threshold: default_index_resize_threshold(),
      config: None,
      config_export_path: None,
    }
  }
}

impl NodeArgs {
  /// 解析最低日志级别（serverSettings.LogLevel；缺省 Information，
  /// 未知级别回退 Information）
  ///
  /// libs/host/Configuration/TypeConverters.cs:RedisLogLevelTypeConverter 解析投影
  pub fn minimum_log_level(&self) -> LevelFilter {
    match self.log_level.as_deref() {
      Some(s) if s.eq_ignore_ascii_case("trace") => LevelFilter::Trace,
      Some(s) if s.eq_ignore_ascii_case("debug") => LevelFilter::Debug,
      Some(s) if s.eq_ignore_ascii_case("warn") || s.eq_ignore_ascii_case("warning") => {
        LevelFilter::Warn
      }
      Some(s) if s.eq_ignore_ascii_case("error") => LevelFilter::Error,
      Some(s) if s.eq_ignore_ascii_case("off") => LevelFilter::Off,
      _ => LevelFilter::Info,
    }
  }

  /// 生成网络端点定义列表（bind 多地址拆分唯一落点）
  ///
  /// 对标 libs/common/Format.cs:TryParseAddressList：bind 未指定或全空白时按
  /// protected-mode 回退（保护→回环 / 非保护→全接口）；否则按逗号与空格切分
  /// 多地址（TrimEntries | RemoveEmptyEntries 语义，Format.cs:64），逐地址与
  /// port 组合成端点；unixsocket 尾部追加（对标 Options.cs:813-814）。
  /// 全空白条目被剔除后可能产出空列表，对应 C# Options.cs:796
  /// `endpoints.Length == 0` 的拒启臂，由消费侧 GarnetServer::new 校验。
  pub fn endpoints(&self) -> Vec<String> {
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
    let mut eps: Vec<String> = bind
      .split([',', ' '])
      .map(str::trim)
      .filter(|a| !a.is_empty())
      .map(|a| format!("{a}:{}", self.port))
      .collect();
    if let Some(ref u) = self.unixsocket {
      eps.push(format!("unix:{u}"));
    }
    eps
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
  fn validate(&self) -> Result<(), NodeOptionsError> {
    if !(MAX_DATABASES_MIN..=MAX_DATABASES_MAX).contains(&self.max_databases) {
      return Err(NodeOptionsError::ValueOutOfRange(
        "max-databases",
        MAX_DATABASES_MIN,
        MAX_DATABASES_MAX,
        self.max_databases,
      ));
    }
    // C# Options.cs:398 IntRangeValidation(-1, int.MaxValue)
    if self.network_connection_limit < -1 {
      return Err(NodeOptionsError::ValueOutOfRange(
        "network-connection-limit",
        DEFAULT_NETWORK_CONNECTION_LIMIT,
        i32::MAX,
        self.network_connection_limit,
      ));
    }
    // C# Options.cs:691 IntRangeValidation(-1, int.MaxValue)：-1 以下无
    // 「禁用」以外的语义，槽位下界同为 -1（RuntimeServerConfig.cs:213）
    if self.expired_key_deletion_scan_frequency_secs < -1 {
      return Err(NodeOptionsError::ValueOutOfRange(
        "expired-key-deletion-scan-freq",
        DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
        i32::MAX,
        self.expired_key_deletion_scan_frequency_secs,
      ));
    }
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
      if !(0..=UNIX_SOCKET_PERM_MAX).contains(&perm) {
        return Err(NodeOptionsError::ValueOutOfRange(
          "unixsocketperm",
          0,
          UNIX_SOCKET_PERM_MAX,
          perm,
        ));
      }
      if perm.to_string().bytes().any(|d| d > b'7') {
        return Err(NodeOptionsError::UnixSocketPermDigits(perm));
      }
    }
    // C# GarnetServerOptions.cs:839-840 `LatencyMonitor && MetricsSamplingFrequency
    // == 0` 即拒启——全仓唯一校验点（禁各装配路径重复判定）
    if self.latency_monitor && self.metrics_sampling_frequency_secs == 0 {
      return Err(NodeOptionsError::LatencyMonitorWithoutMetrics);
    }
    Ok(())
  }

  /// 投影运行时服务选项（对标 C# Options.GetServerOptions 的服务选项装配段；
  /// RuntimeServerOptions 为 RuntimeServerConfig 播种的运行时单一真源）
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
    // —— 只读回显字段（CONFIG GET/INFO 经格式器直读，C# Options.cs:909-910、:921、
    // :935、:1030 逐字段投影进 GarnetServerOptions 的同源装配） ——
    // rust `dir` ↔ C# CheckpointBaseDirectory（GarnetServerOptions.cs:625
    // (CheckpointDir ?? LogDir) ?? "" 回落根；rust 单根 dir 恒有缺省，无二级回落）
    opts.checkpoint_base_directory = self.dir.display().to_string();
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
    let raw = self.aof_size_limit.as_deref()?;
    let size = try_parse_size(raw)?;
    let adjusted = previous_power_of_2(size);
    u64::try_from(adjusted).ok().filter(|v| *v > 0)
  }

  /// 哈希索引内存上限桶数（尺寸向下取 2 的幂再按 64B/桶折算；未配置或解析
  /// 失败返回 None）
  ///
  /// libs/server/Servers/ServerOptions.cs:IndexSizeCachelines
  ///（adjustedSize / 64，每 cache line 64B 恰为一桶；< 64B 拒绝与 C# 一致）
  #[must_use]
  pub fn index_max_size_buckets(&self) -> Option<usize> {
    let raw = self.index_max_size.as_deref()?;
    let size = try_parse_size(raw)?;
    let adjusted = previous_power_of_2(size);
    if adjusted < INDEX_MAX_SIZE_MIN_BYTES {
      return None;
    }
    usize::try_from(adjusted / INDEX_MAX_SIZE_MIN_BYTES).ok()
  }

  /// 从 NestedText 字符串解析配置（唯一的配置文件格式）
  pub fn from_nested_text_str(s: &str) -> Result<Self, NodeOptionsError> {
    nested_text::from_str(s).map_err(Into::into)
  }

  /// 从 NestedText 文件加载配置
  pub fn from_file(path: impl AsRef<Path>) -> Result<Self, NodeOptionsError> {
    let content = fs::read_to_string(path.as_ref())?;
    Self::from_nested_text_str(&content)
  }

  /// 命令行显式项覆盖文件/默认基线
  ///
  /// clap_derive 的 arg id 为字段名原样（long 才是 kebab-case），故以
  /// stringify!(字段名) 作 value_source 查询键；hlog 配置段为 flatten 嵌套，
  /// id 前缀 hlog_ 单独合并
  fn override_explicit(&mut self, matches: &ArgMatches, cli: Self) {
    use clap::parser::ValueSource;
    macro_rules! over {
      ($($f:ident),+ $(,)?) => {
        $(if matches.value_source(stringify!($f)) == Some(ValueSource::CommandLine) {
          self.$f = cli.$f.clone();
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
      requirepass,
      tls_cert,
      tls_key,
      tls_client_cert_required,
      tls_client_target_host,
      tls_server_cert_required,
      tls_issuer_cert,
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
      aof_size_limit,
      aof_size_limit_enforce_frequency_secs,
      index_max_size,
      index_resize_frequency_secs,
      index_resize_threshold,
    ];
    // hlog 配置段：flatten 嵌套字段，arg id 带 hlog_ 前缀（显式 CLI 项覆盖）
    if matches.value_source("hlog_page_size") == Some(ValueSource::CommandLine) {
      self.hlog.page_size = cli.hlog.page_size;
    }
    if matches.value_source("hlog_memory_size") == Some(ValueSource::CommandLine) {
      self.hlog.memory_size = cli.hlog.memory_size;
    }
    if matches.value_source("hlog_mutable_percent") == Some(ValueSource::CommandLine) {
      self.hlog.mutable_percent = cli.hlog.mutable_percent;
    }
    if matches.value_source("read_cache") == Some(ValueSource::CommandLine) {
      self.hlog.read_cache = cli.hlog.read_cache;
    }
    if matches.value_source("read_cache_memory_size") == Some(ValueSource::CommandLine) {
      self.hlog.read_cache_memory_size = cli.hlog.read_cache_memory_size;
    }
    if matches.value_source("reviv") == Some(ValueSource::CommandLine) {
      self.hlog.reviv = cli.hlog.reviv;
    }
    if matches.value_source("reviv_fraction") == Some(ValueSource::CommandLine) {
      self.hlog.reviv_fraction = cli.hlog.reviv_fraction;
    }
    if matches.value_source("copy_reads_to_tail") == Some(ValueSource::CommandLine) {
      self.hlog.copy_reads_to_tail = cli.hlog.copy_reads_to_tail;
    }
  }

  /// 导出生效配置到 nested_text 文件（对标 Options.cs:501 ConfigExportPath；
  /// C# 仅导出非默认项，Rust 导出全量字段，差异登记）
  fn export_config(&self, path: &Path) -> Result<(), NodeOptionsError> {
    fs::write(path, nested_text::to_string(self)?)?;
    Ok(())
  }
}

/// 三层配置合并解析契约：结构体默认值 → --config nested_text 文件 → 命令行显式项覆盖
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

  /// 从进程命令行参数解析（C# Options 命令行解析入口的对标形态）
  fn from_args() -> Result<Self, NodeOptionsError> {
    Self::from_args_iter(args_os())
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
    if let Some(path) = &merged.config_export_path {
      merged.export_config(path)?;
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
  fn endpoints(&self) -> Vec<String> {
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

#[cfg(test)]
mod tests {
  use std::env::temp_dir;

  use super::*;

  /// 写临时 nested_text 配置文件（进程内唯一名，测试结束自清理）
  fn temp_config(name: &str, content: &str) -> PathBuf {
    let path = temp_dir().join(format!("wedb-node-options-{name}.nt"));
    fs::write(&path, content).unwrap();
    path
  }

  #[test]
  fn test_node_args_defaults() {
    let args = NodeArgs::default();
    // 保护模式默认开，bind 未显式 → 回环回退
    assert_eq!(args.bind, None);
    assert!(args.protected_mode);
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
    assert_eq!(args.port, 6379);
    assert_eq!(args.dir, PathBuf::from("./data"));
    assert_eq!(args.wal_dir(), PathBuf::from("./data/wal"));
    assert!(!args.aof);
    // 慢日志 / 扫描限额 / 数据库数默认对齐 C#
    assert_eq!(args.slow_log_threshold, 0);
    assert_eq!(args.slow_log_max_entries, 128);
    assert_eq!(args.max_databases, 16);
    assert_eq!(args.object_scan_count_limit, 1000);
    assert_eq!(args.metrics_sampling_frequency_secs, 0);
    // C# defaults.conf:304 NetworkConnectionLimit = -1：连接上限默认不限
    assert_eq!(args.network_connection_limit, -1);
    // C# 默认链：EnableLua=false、LuaScriptTimeoutMs=0（无限）。
    assert!(!args.enable_lua);
    assert_eq!(args.lua_script_timeout_ms, 0);
    // C# defaults.conf:533 EnableVectorSetPreview=false：向量预览默认关
    assert!(!args.enable_vector_set_preview);
    // 后台任务默认链：AofSizeLimit=""（关闭）、EnforceFrequencySecs=5、
    // IndexMaxMemorySize=""（关闭）、IndexResizeFrequencySecs=60、
    // IndexResizeThreshold=50（GarnetServerOptions.cs:167/172/201/206）
    assert_eq!(args.aof_size_limit, None);
    assert_eq!(args.aof_size_limit_enforce_frequency_secs, 5);
    assert_eq!(args.index_max_size, None);
    assert_eq!(args.index_resize_frequency_secs, 60);
    assert_eq!(args.index_resize_threshold, 50);
    assert_eq!(args.aof_size_limit_bytes(), None);
    assert_eq!(args.index_max_size_buckets(), None);
    // C# Options.cs:685 UnixSocketPermission 默认 0 = 不设置（跳过 chmod 臂）
    assert_eq!(args.unixsocket_perm, None);
    assert_eq!(args.unix_socket_mode(), None);
  }

  #[test]
  fn test_background_task_size_parsing() {
    // 尺寸换算对标 GarnetServerOptions.AofSizeLimitSizeBits（向下取 2 的幂）
    // 与 ServerOptions.IndexSizeCachelines（向下取 2 的幂再按 64B/桶折算）
    let args = NodeArgs {
      aof_size_limit: Some("64mb".into()),
      index_max_size: Some("16k".into()),
      ..Default::default()
    };
    assert_eq!(args.aof_size_limit_bytes(), Some(64 * 1024 * 1024));
    // 非 2 的幂输入向下取整（"100m" → 64m）
    let args = NodeArgs {
      aof_size_limit: Some("100m".into()),
      ..Default::default()
    };
    assert_eq!(args.aof_size_limit_bytes(), Some(64 * 1024 * 1024));
    // 索引上限："16k" 字节 → 2^14/64 = 256 桶
    let args = NodeArgs {
      index_max_size: Some("16k".into()),
      ..Default::default()
    };
    assert_eq!(args.index_max_size_buckets(), Some(256));
    // 低于 64B 最小界拒绝（C# adjustedSize < 64 throw）
    let args = NodeArgs {
      index_max_size: Some("32".into()),
      ..Default::default()
    };
    assert_eq!(args.index_max_size_buckets(), None);
    // 非法尺寸字符串拒绝
    let args = NodeArgs {
      aof_size_limit: Some("abc".into()),
      ..Default::default()
    };
    assert_eq!(args.aof_size_limit_bytes(), None);
  }

  #[test]
  fn test_node_args_lua_cli_flags() {
    let args =
      NodeArgs::try_parse_from(["wedb", "--enable-lua", "--lua-script-timeout-ms", "5000"])
        .unwrap();
    assert!(args.enable_lua);
    assert_eq!(args.lua_script_timeout_ms, 5000);
    // lua 事务模式选项已整链删除（task/done/lua-txn-mode-drop-placeholder.md），
    // 传入即未知选项硬失败，不做旧配置兼容
    assert!(NodeArgs::try_parse_from(["wedb", "--lua-transaction-mode"]).is_err());
  }

  #[test]
  fn test_vector_set_preview_flag_and_nested_text() {
    // CLI 旗标开启
    let args = NodeArgs::try_parse_from(["wedb", "--enable-vector-set-preview"]).unwrap();
    assert!(args.enable_vector_set_preview);
    // nested_text 导入面（键名 = serde 字段名）
    let nt = "enable_vector_set_preview: true\n";
    let conf = NodeArgs::from_nested_text_str(nt).unwrap();
    assert!(conf.enable_vector_set_preview);
    // nested_text 导出面（ConfigExportPath 落盘内容含该开关，缺省 false）
    let exported = nested_text::to_string(&NodeArgs::default()).unwrap();
    assert!(exported.contains("enable_vector_set_preview: false"));
  }

  #[test]
  fn test_node_args_cli_aof_and_wal_dir() {
    let args = NodeArgs::try_parse_from([
      "wedb",
      "--aof",
      "--wal-dir",
      "/mnt/wal",
      "--aof-commit-wait",
    ])
    .unwrap();
    assert!(args.aof);
    assert_eq!(args.wal_dir, Some(PathBuf::from("/mnt/wal")));
    assert_eq!(args.wal_dir(), PathBuf::from("/mnt/wal"));
    // WAIT-FOR-COMMIT 档参数源（C# Options.cs:253 [Option("aof-commit-wait")]）
    assert!(args.aof_commit_wait);
    assert!(args.runtime_server_options().wait_for_commit);

    let args_no_aof = NodeArgs::try_parse_from(["wedb"]).unwrap();
    assert!(!args_no_aof.aof);
    assert_eq!(args_no_aof.wal_dir, None);
    assert_eq!(args_no_aof.wal_dir(), PathBuf::from("./data/wal"));
    assert!(!args_no_aof.aof_commit_wait);
    assert!(!args_no_aof.runtime_server_options().wait_for_commit);
  }

  /// fast-aof-truncate 与 on-demand-checkpoint 两旋钮的三面（CLI / nested_text /
  /// 导出）与 runtime_server_options 投影（对标 C# Options.cs:991-992 落入
  /// GarnetServerOptions，供 ClusterProvider::allow_data_loss 派生单点读取）
  #[test]
  fn test_truncate_odc_knobs_parse_store_project() {
    // 缺省即 C# defaults.conf:343 false / :346 true
    let args = NodeArgs::try_parse_from(["wedb"]).unwrap();
    assert!(!args.fast_aof_truncate);
    assert!(args.on_demand_checkpoint);
    let opts = args.runtime_server_options();
    assert!(!opts.fast_aof_truncate);
    assert!(opts.on_demand_checkpoint);

    // CLI 显式置位（默认真值旋钮取 ArgAction::Set，须带值）
    let args = NodeArgs::try_parse_from([
      "wedb",
      "--fast-aof-truncate",
      "--on-demand-checkpoint",
      "false",
    ])
    .unwrap();
    let opts = args.runtime_server_options();
    assert!(opts.fast_aof_truncate);
    assert!(!opts.on_demand_checkpoint);

    // nested_text 配置面（键名 = serde 字段名）；未写项回落 C# 默认
    let conf =
      NodeArgs::from_nested_text_str("fast_aof_truncate: true\non_demand_checkpoint: false\n")
        .unwrap();
    let opts = conf.runtime_server_options();
    assert!(opts.fast_aof_truncate);
    assert!(!opts.on_demand_checkpoint);
    let conf = NodeArgs::from_nested_text_str("port: 7100\n").unwrap();
    assert!(!conf.fast_aof_truncate);
    assert!(conf.on_demand_checkpoint);

    // 三层合并：文件为基、CLI 显式覆盖
    let file = temp_config(
      "truncate-odc",
      "port: 7100\nfast_aof_truncate: true\non_demand_checkpoint: false\n",
    );
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--on-demand-checkpoint",
      "true",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    let opts = args.runtime_server_options();
    assert!(opts.fast_aof_truncate, "文件值未被显式覆盖项须保留");
    assert!(opts.on_demand_checkpoint, "CLI 显式项覆盖文件值");

    // 导出面含两旋钮
    let exported = nested_text::to_string(&NodeArgs::default()).unwrap();
    assert!(exported.contains("fast_aof_truncate: false"));
    assert!(exported.contains("on_demand_checkpoint: true"));
  }

  /// C# GarnetServer.cs:508 提交组合互校验（未开 AOF 却配提交节拍/等待档
  /// 即拒启；节拍显式 0 与 C# 缺省同值，不拒）。
  ///
  /// 走 `ConfigFileArgs::from_args_iter` 而非 `try_parse_from`：validate 在
  /// 后者之外的分层合并末端（from_layered_matches）才跑，用前者才真触拒启面
  #[test]
  fn test_aof_commit_combination_validation() {
    let err = NodeArgs::from_args_iter(["wedb", "--aof-commit-wait"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::AofCommitWithoutAof),
      "{err:?}"
    );

    let err = NodeArgs::from_args_iter(["wedb", "--aof-commit-ms", "5"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::AofCommitWithoutAof),
      "{err:?}"
    );

    // 开 AOF 即两档皆合法（会话门 enable_aof && wait_for_commit 自此可开）
    assert!(
      NodeArgs::from_args_iter(["wedb", "--aof", "--aof-commit-wait"]).is_ok(),
      "开 AOF 时等待档须合法"
    );
    // 显式 0 即 C# defaults.conf:182 缺省值，`!= 0` 判据不成立
    assert!(
      NodeArgs::from_args_iter(["wedb", "--aof-commit-ms", "0"]).is_ok(),
      "缺省同值的显式 0 节拍不拒"
    );
  }

  /// C# GarnetServerOptions.cs:839-840「LatencyMonitor requires
  /// MetricsSamplingFrequency to be set」的启动期拒启面（全仓唯一校验点，
  /// 装配侧不再各判一次）。同走 `from_args_iter` 以触达分层合并末端的 validate
  #[test]
  fn test_latency_monitor_requires_metrics_sampling_frequency() {
    let err = NodeArgs::from_args_iter(["wedb", "--latency-monitor", "true"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::LatencyMonitorWithoutMetrics),
      "{err:?}"
    );

    // 配了采样节拍即合法；关闭延迟监视时节拍缺省 0 亦不拒
    assert!(
      NodeArgs::from_args_iter([
        "wedb",
        "--latency-monitor",
        "true",
        "--metrics-sampling-freq",
        "5"
      ])
      .is_ok(),
      "有采样节拍时延迟监视须合法"
    );
    assert!(NodeArgs::from_args_iter(["wedb", "--latency-monitor", "false"]).is_ok());
  }

  #[test]
  fn test_node_args_with_custom_wal_dir() {
    let args = NodeArgs {
      wal_dir: Some(PathBuf::from("/mnt/fast_ssd/wal")),
      ..Default::default()
    };
    assert_eq!(args.wal_dir(), PathBuf::from("/mnt/fast_ssd/wal"));
  }

  /// 数据路径唯一真源：仅由 dir 派生，不含任何模式维度（单机与集群共用同一
  /// 物理件，对标 C# 单一命名方案 GarnetServer.cs:479-484 两臂共用
  /// defaultNamingScheme、Options.cs:790-793 LogDir/CheckpointDir 单套）
  #[test]
  fn test_data_path_is_mode_agnostic_single_source() {
    // 默认目录回落 {dir}/wedb.db
    let args = NodeArgs::default();
    assert_eq!(args.data_path(), PathBuf::from(DEFAULT_DIR).join(DATA_FILE));
    assert_eq!(args.data_path(), PathBuf::from("./data/wedb.db"));
    // 显式 dir：数据文件、WAL 同根派生，一套物理布局
    let args = NodeArgs {
      dir: PathBuf::from("/srv/wedb"),
      ..Default::default()
    };
    assert_eq!(args.data_path(), PathBuf::from("/srv/wedb/wedb.db"));
    assert_eq!(args.wal_dir(), PathBuf::from("/srv/wedb/wal"));
  }

  #[test]
  fn test_node_args_with_unixsocket() {
    let cfg = NodeArgs {
      unixsocket: Some("/tmp/wedb.sock".to_string()),
      ..Default::default()
    };
    assert_eq!(
      cfg.endpoints(),
      vec!["127.0.0.1:6379", "unix:/tmp/wedb.sock"]
    );
  }

  /// unixsocketperm 旋钮全链（对标 C# Options.cs:684 选项 + :816-817
  /// 八进制折算 + GarnetServerTcp.cs:149-151 跳默认臂）：CLI / nested_text
  /// 双入口同口径、权限值不进端点字符串、非法值在 validate 期拒启
  #[test]
  fn test_unixsocketperm_knob() {
    // CLI 八进制字面量口径：660 → 0o660；权限值不落端点字符串
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--unixsocket",
      "/tmp/wedb.sock",
      "--unixsocketperm",
      "660",
    ])
    .unwrap();
    assert_eq!(args.unixsocket_perm, Some(660));
    assert_eq!(args.unix_socket_mode(), Some(0o660));
    assert_eq!(
      args.endpoints(),
      vec!["127.0.0.1:6379", "unix:/tmp/wedb.sock"]
    );

    // nested_text 单配置机制同口径
    let conf =
      NodeArgs::from_nested_text_str("unixsocket: /tmp/wedb.sock\nunixsocket_perm: 600\n").unwrap();
    assert_eq!(conf.unix_socket_mode(), Some(0o600));

    // C# 默认 0 即不设置（unixSocketPermission != default 跳过臂）
    let args = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "0"]).unwrap();
    assert_eq!(args.unix_socket_mode(), None);

    // 越界（C# IntRangeValidation(0, 777)）与非法八进制位（Convert 失败面）
    // 皆在 validate 期拒启，不落 bind
    let err = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "778"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::ValueOutOfRange(_, 0, 777, 778)),
      "{err:?}"
    );
    let err = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "80"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::UnixSocketPermDigits(80)),
      "{err:?}"
    );
    // 负值直构 validate（clap 负号输入属命令行语法层，非本校验面）
    let err = NodeArgs {
      unixsocket_perm: Some(-1),
      ..Default::default()
    }
    .validate()
    .err()
    .unwrap();
    assert!(
      matches!(err, NodeOptionsError::ValueOutOfRange(_, 0, 777, -1)),
      "{err:?}"
    );
  }

  #[test]
  fn test_nested_text_parse() {
    let nt = r#"
bind: 192.168.1.100
port: 6380
dir: /tmp/wedb_data
"#;
    let conf = NodeArgs::from_nested_text_str(nt).unwrap();
    assert_eq!(conf.bind.as_deref(), Some("192.168.1.100"));
    assert_eq!(conf.port, 6380);
    assert_eq!(conf.dir, PathBuf::from("/tmp/wedb_data"));
  }

  #[test]
  fn test_protected_mode_bind_fallback() {
    // C# Format.TryParseAddressList：保护模式 + 空 bind → 回环；
    // 非保护 + 空 bind → 全接口
    let args = NodeArgs::try_parse_from(["wedb", "--protected-mode", "false"]).unwrap();
    assert_eq!(args.endpoints(), vec!["0.0.0.0:6379"]);

    let args =
      NodeArgs::try_parse_from(["wedb", "--protected-mode", "false", "--bind", "10.0.0.8"])
        .unwrap();
    assert_eq!(args.endpoints(), vec!["10.0.0.8:6379"]);

    let args = NodeArgs::try_parse_from(["wedb"]).unwrap();
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
  }

  #[test]
  fn test_multi_bind_split() {
    // C# Format.cs:64：bind 按逗号与空格切分多地址，逐地址组合 port 成端点
    let args =
      NodeArgs::try_parse_from(["wedb", "--bind", "127.0.0.1, 10.0.0.8 ,192.168.1.8"]).unwrap();
    assert_eq!(
      args.endpoints(),
      vec!["127.0.0.1:6379", "10.0.0.8:6379", "192.168.1.8:6379"]
    );
    // 全分隔符/全空白输入：条目剔除后端点列表为空（对应 C# endpoints.Length==0 拒启臂）
    let args = NodeArgs {
      bind: Some(" , ".to_string()),
      ..Default::default()
    };
    assert_eq!(args.endpoints(), Vec::<String>::new());
    // 纯空白 bind 视同未指定，走保护模式回退臂（C# IsNullOrWhiteSpace）
    let args = NodeArgs {
      bind: Some("   ".to_string()),
      ..Default::default()
    };
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
  }

  #[test]
  fn test_config_file_cli_override() {
    // 端到端：文件为基 → CLI 显式覆盖（对标 ServerSettingsManager 三层合并）
    let file = temp_config(
      "override",
      "port: 7000\nslow_log_threshold: 5000\nmax_databases: 4\nbind: 192.168.1.100\n",
    );
    let args =
      NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap(), "--port", "7001"])
        .unwrap();
    fs::remove_file(&file).ok();
    // CLI 覆盖文件值
    assert_eq!(args.port, 7001);
    // 文件值生效
    assert_eq!(args.slow_log_threshold, 5000);
    assert_eq!(args.max_databases, 4);
    assert_eq!(args.bind.as_deref(), Some("192.168.1.100"));
    // 未涉及项取默认
    assert_eq!(args.object_scan_count_limit, 1000);
    assert_eq!(args.endpoints(), vec!["192.168.1.100:7001"]);
  }

  #[test]
  fn test_config_file_kebab_field_override() {
    // kebab-case 长名（--wal-dir）字段的显式判定：CLI 覆盖文件值，
    // 文件里的其余项（slow_log_max_entries）原样保留
    let file = temp_config("kebab", "wal_dir: /file/wal\nslow_log_max_entries: 8192\n");
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--wal-dir",
      "/cli/wal",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    assert_eq!(args.wal_dir, Some(PathBuf::from("/cli/wal")));
    assert_eq!(args.slow_log_max_entries, 8192);
  }

  /// 连接上限三面：CLI 显式项、nested_text snake_case 覆盖、越界拒启
  ///（C# Options.cs:398 IntRangeValidation(-1, int.MaxValue) 的启动期拒绝面）
  #[test]
  fn test_network_connection_limit_surfaces() {
    let args = NodeArgs::try_parse_from(["wedb", "--network-connection-limit", "512"]).unwrap();
    assert_eq!(args.network_connection_limit, 512);

    // nested_text 蛇形键覆盖（-1 显式不限与 C# defaults.conf 同形态）
    let file = temp_config("net-limit", "network_connection_limit: 32\n");
    let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
    fs::remove_file(&file).ok();
    assert_eq!(args.network_connection_limit, 32);

    // 越界（< -1）拒启；validate 在 from_args_iter 分层合并末端才跑
    let err = NodeArgs::from_args_iter(["wedb", "--network-connection-limit", "-2"]).unwrap_err();
    assert!(
      matches!(
        err,
        NodeOptionsError::ValueOutOfRange("network-connection-limit", -1, i32::MAX, -2)
      ),
      "{err:?}"
    );
    // -1 边界合法（缺省不限）
    assert!(NodeArgs::from_args_iter(["wedb", "--network-connection-limit", "-1"]).is_ok());
  }

  #[test]
  fn test_config_export_round_trip() {
    // 导出合并后配置 → 重新加载一致
    let file = temp_config(
      "export-src",
      "port: 7002\nslow_log_max_entries: 64\ndir: /file/data\nunixsocket: /file/x.sock\naof: true\naof_size_limit: 32mb\n",
    );
    let export = temp_dir().join("wedb-node-options-export-out.nt");
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--config-export-path",
      export.to_str().unwrap(),
      "--object-scan-count-limit",
      "256",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    let reloaded = NodeArgs::from_file(&export).unwrap();
    fs::remove_file(&export).ok();
    assert_eq!(reloaded.port, 7002);
    assert_eq!(reloaded.slow_log_max_entries, 64);
    assert_eq!(reloaded.object_scan_count_limit, 256);
    assert_eq!(reloaded.port, args.port);
    // --config-file 形态下只读回显五字段经投影等于入参，且导出/重载往返一致
    let opts = args.runtime_server_options();
    assert_eq!(opts.checkpoint_base_directory, "/file/data");
    assert_eq!(opts.log_dir.as_deref(), Some("/file/data/wal"));
    assert_eq!(opts.unix_socket_path.as_deref(), Some("/file/x.sock"));
    assert!(opts.enable_aof);
    assert_eq!(opts.aof_size_limit.as_deref(), Some("32mb"));
    assert_eq!(
      reloaded.runtime_server_options().unix_socket_path,
      opts.unix_socket_path
    );
    assert_eq!(
      reloaded.runtime_server_options().aof_size_limit,
      opts.aof_size_limit
    );
  }

  #[test]
  fn test_runtime_server_options_projection() {
    // 对标 C# Options.GetServerOptions 选项装配段
    let args = NodeArgs {
      aof_commit_ms: Some(20),
      aof_commit_wait: true,
      slow_log_threshold: 800,
      slow_log_max_entries: 32,
      max_databases: 8,
      object_scan_count_limit: 512,
      dir: PathBuf::from("/data/ro"),
      wal_dir: Some(PathBuf::from("/mnt/wal")),
      unixsocket: Some("/tmp/x.sock".into()),
      aof: true,
      aof_size_limit: Some("64mb".into()),
      ..Default::default()
    };
    let opts = args.runtime_server_options();
    assert_eq!(opts.commit_frequency_ms, 20);
    assert!(opts.wait_for_commit);
    assert_eq!(opts.slow_log_threshold, 800);
    assert_eq!(opts.slow_log_max_entries, 32);
    assert_eq!(opts.max_databases, 8);
    assert_eq!(opts.object_scan_count_limit, 512);
    // 只读回显五字段（C# Options.cs:909-910、:921、:935、:1030 投影同源）
    assert_eq!(opts.checkpoint_base_directory, "/data/ro");
    assert_eq!(opts.log_dir.as_deref(), Some("/mnt/wal"));
    assert_eq!(opts.unix_socket_path.as_deref(), Some("/tmp/x.sock"));
    assert!(opts.enable_aof);
    assert_eq!(opts.aof_size_limit.as_deref(), Some("64mb"));

    // 未设置项保持 C# 默认：dir 恒回落 ./data（非空），wal_dir 落 <dir>/wal，
    // unixsocket/aof_size_limit 未配置为 None（格式器吐 ""，与 C# 空串/false 口径一致）
    let opts = NodeArgs::default().runtime_server_options();
    assert_eq!(opts.commit_frequency_ms, 0);
    assert!(!opts.wait_for_commit);
    assert_eq!(opts.slow_log_threshold, 0);
    assert_eq!(opts.slow_log_max_entries, 128);
    assert_eq!(opts.max_databases, 16);
    assert_eq!(opts.object_scan_count_limit, 1000);
    assert_eq!(opts.checkpoint_base_directory, DEFAULT_DIR);
    assert_eq!(opts.log_dir.as_deref(), Some("./data/wal"));
    assert!(opts.unix_socket_path.is_none());
    assert!(!opts.enable_aof);
    assert!(opts.aof_size_limit.is_none());
  }

  #[test]
  fn test_hlog_section_defaults_and_projection() {
    // 默认全 None + read_cache 关闭：不覆盖装配基线（页容量交由内存预算规划器推导）
    let args = NodeArgs::default();
    assert_eq!(args.hlog, HlogOptions::default());
    let p = args.hlog.validated().unwrap();
    assert_eq!(p.page_size, None);
    assert_eq!(p.memory_size, None);
    assert_eq!(p.mutable_fraction, None);
    assert!(!p.read_cache);
    assert_eq!(p.read_cache_memory_size, None);
    // reviv 三旋钮默认态 = 现状硬编码基线（对标 C# defaults：reviv 关、
    // reviv-fraction 未配置、copy-reads-to-tail 关）
    assert!(!p.reviv);
    assert_eq!(p.reviv_fraction, None);
    assert!(!p.copy_reads_to_tail);

    // 显式项投影：对标 C# GetSettings（page 16m / memory 4g / mutable 50）
    let args = NodeArgs {
      hlog: HlogOptions {
        page_size: Some(DEFAULT_HLOG_PAGE_SIZE),
        memory_size: Some(4 * 1024 * 1024 * 1024),
        mutable_percent: Some(50),
        read_cache: true,
        read_cache_memory_size: Some(512 * 1024 * 1024),
        reviv: true,
        reviv_fraction: Some(0.5),
        copy_reads_to_tail: true,
      },
      ..Default::default()
    };
    let (page, memory, fraction, read_cache, rc_memory, reviv, reviv_fraction, crt) = {
      let p = args.hlog.validated().unwrap();
      (
        p.page_size,
        p.memory_size,
        p.mutable_fraction,
        p.read_cache,
        p.read_cache_memory_size,
        p.reviv,
        p.reviv_fraction,
        p.copy_reads_to_tail,
      )
    };
    assert_eq!(page, Some(16 * 1024 * 1024));
    assert_eq!(memory, Some(4 * 1024 * 1024 * 1024));
    assert_eq!(fraction, Some(0.5));
    assert!(read_cache);
    assert_eq!(rc_memory, Some(512 * 1024 * 1024));
    // 三旋钮透传：validated 不复校 reviv_fraction（单点在 StoreConfig::validate）
    assert!(reviv);
    assert_eq!(reviv_fraction, Some(0.5));
    assert!(crt);
  }

  #[test]
  fn test_hlog_section_validation_rejects() {
    // 对标 GarnetServerOptions.GetSettings：MutablePercent < 10 或 > 95 → throw
    let bad = HlogOptions {
      mutable_percent: Some(9),
      ..HlogOptions::default()
    };
    assert!(bad.validated().is_err());
    let bad = HlogOptions {
      mutable_percent: Some(96),
      ..HlogOptions::default()
    };
    assert!(bad.validated().is_err());
    // 边界 10 / 95 合法
    assert!(
      HlogOptions {
        mutable_percent: Some(10),
        ..HlogOptions::default()
      }
      .validated()
      .is_ok()
    );
    assert!(
      HlogOptions {
        mutable_percent: Some(95),
        ..HlogOptions::default()
      }
      .validated()
      .is_ok()
    );
    // 页容量必须为 2 的幂
    let bad = HlogOptions {
      page_size: Some(4095),
      ..HlogOptions::default()
    };
    assert!(bad.validated().is_err());
    // 下限校验核在场：256 页容量须被点名拒（证伪「小页静默接受」，对标 C#
    // C# ServerOptions.ValidatedPageSizeBits 的 MIN_PAGE_SIZE_BYTES 判定）
    let bad = HlogOptions {
      page_size: Some(256),
      ..HlogOptions::default()
    };
    assert!(
      matches!(
        bad.validated(),
        Err(NodeOptionsError::Hlog(msg))
          if msg.contains("hlog-page-size") && msg.contains("512") && msg.contains("256")
      ),
      "256 页容量须被下限校验核点名拒绝: {bad:?}"
    );
    // 向下取幂后跌破下限：511 取幂归 256 才判负，文案给出取幂生效值
    let bad = HlogOptions {
      page_size: Some(511),
      ..HlogOptions::default()
    };
    assert!(
      matches!(
        bad.validated(),
        Err(NodeOptionsError::Hlog(msg)) if msg.contains("生效 256 字节") && msg.contains("512")
      ),
      "取幂后跌破下限须报生效值 256: {bad:?}"
    );
    // 页容量扇区口径与 wkv 一致（4096 整数倍）：2048 为 2 的幂但非 4KB 扇区
    // 倍数，同样拒绝；4096 恰为一扇区，合法
    let bad = HlogOptions {
      page_size: Some(2048),
      ..HlogOptions::default()
    };
    assert!(matches!(
      bad.validated(),
      Err(NodeOptionsError::Hlog(msg)) if msg.contains("4096")
    ));
    assert!(
      HlogOptions {
        page_size: Some(4096),
        ..HlogOptions::default()
      }
      .validated()
      .is_ok()
    );
    // 内存预算必须为正
    let bad = HlogOptions {
      memory_size: Some(0),
      ..HlogOptions::default()
    };
    assert!(matches!(bad.validated(), Err(NodeOptionsError::Hlog(_))));
    // ReadCache 内存预算必须为正
    let bad = HlogOptions {
      read_cache_memory_size: Some(0),
      ..HlogOptions::default()
    };
    assert!(matches!(bad.validated(), Err(NodeOptionsError::Hlog(_))));
  }

  #[test]
  fn test_hlog_read_cache_section_parse() {
    // CLI 开关 + 预算显式覆盖（对标 C# EnableReadCache / ReadCacheMemorySize）
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--read-cache",
      "--read-cache-memory-size",
      "536870912",
    ])
    .unwrap();
    assert!(args.hlog.read_cache);
    assert_eq!(args.hlog.read_cache_memory_size, Some(512 * 1024 * 1024));

    // 缺省关闭且预算透传 None（装配侧取 DEFAULT_READ_CACHE_MEMORY_SIZE 推导）
    let args = NodeArgs::from_nested_text_str("port: 7100\n").unwrap();
    assert!(!args.hlog.read_cache);
    assert_eq!(args.hlog.read_cache_memory_size, None);
    assert_eq!(
      DEFAULT_READ_CACHE_MEMORY_SIZE,
      1024 * 1024 * 1024,
      "对标 C# GarnetServerOptions ReadCacheMemorySize = \"1g\""
    );
  }

  /// reviv 三旋钮（reviv / reviv-fraction / copy-reads-to-tail）CLI / nested_text
  /// 三面（对标 C# Options.cs:564-567、:559、:128 命令行长名）；override_explicit
  /// 合并 id 与 clap Args 字段名同源，显式项不静默丢失
  #[test]
  fn test_hlog_reviv_knobs_parse_and_merge() {
    // CLI 显式项
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--reviv",
      "--reviv-fraction",
      "0.25",
      "--copy-reads-to-tail",
    ])
    .unwrap();
    assert!(args.hlog.reviv);
    assert_eq!(args.hlog.reviv_fraction, Some(0.25));
    assert!(args.hlog.copy_reads_to_tail);

    // 缺省未配置（默认 false/None/false = 现状基线）
    let args = NodeArgs::from_nested_text_str("port: 7100\n").unwrap();
    assert!(!args.hlog.reviv);
    assert_eq!(args.hlog.reviv_fraction, None);
    assert!(!args.hlog.copy_reads_to_tail);

    // nested_text hlog 嵌套节 + validated 透传
    let file = temp_config(
      "hlog-reviv",
      "hlog:\n  reviv: true\n  reviv_fraction: 0.5\n  copy_reads_to_tail: true\n",
    );
    let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
    fs::remove_file(&file).ok();
    let p = args.hlog.validated().unwrap();
    assert!(p.reviv);
    assert_eq!(p.reviv_fraction, Some(0.5));
    assert!(p.copy_reads_to_tail);
  }

  #[test]
  fn test_hlog_nested_text_parse_and_cli_override() {
    // nested_text 嵌套节 `hlog:` 解析 + CLI 显式覆盖
    let file = temp_config(
      "hlog",
      "port: 7100\nhlog:\n  page_size: 8388608\n  memory_size: 268435456\n  mutable_percent: 60\n",
    );
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--hlog-page-size",
      "16777216",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    // CLI 覆盖文件值
    assert_eq!(args.hlog.page_size, Some(16 * 1024 * 1024));
    // 文件值生效
    assert_eq!(args.hlog.memory_size, Some(256 * 1024 * 1024));
    assert_eq!(args.hlog.mutable_percent, Some(60));
    // hlog 段缺省 → 全 None（旧配置文件向后兼容）
    let args = NodeArgs::from_nested_text_str("port: 7100\n").unwrap();
    assert_eq!(args.hlog, HlogOptions::default());
  }

  #[test]
  fn test_hlog_config_export_round_trip() {
    // 导出合并后配置 → 重新加载一致（hlog 嵌套节全量往返）
    let export = temp_dir().join("wedb-node-options-hlog-export.nt");
    let _args = NodeArgs::from_args_iter([
      "wedb",
      "--config-export-path",
      export.to_str().unwrap(),
      "--hlog-page-size",
      "16777216",
      "--hlog-mutable-percent",
      "55",
    ])
    .unwrap();
    let reloaded = NodeArgs::from_file(&export).unwrap();
    fs::remove_file(&export).ok();
    assert_eq!(reloaded.hlog.page_size, Some(DEFAULT_HLOG_PAGE_SIZE));
    assert_eq!(reloaded.hlog.mutable_percent, Some(55));
    assert_eq!(reloaded.hlog.memory_size, None);
  }

  #[test]
  fn test_fast_aof_truncate_and_commit_wait_validations() {
    // 1. fast_aof_truncate + aof + aof_commit_ms != -1 => 拒启
    let args = NodeArgs {
      fast_aof_truncate: true,
      aof: true,
      aof_commit_ms: Some(20),
      ..Default::default()
    };
    let err = args.validate().unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::FastAofTruncateRequiresManualCommit),
      "{err:?}"
    );

    // fast_aof_truncate + aof + aof_commit_ms == -1 => 合法
    let args = NodeArgs {
      fast_aof_truncate: true,
      aof: true,
      aof_commit_ms: Some(-1),
      ..Default::default()
    };
    assert!(args.validate().is_ok());

    // 2. aof_commit_ms < 0 + aof_commit_wait => 拒启
    let args = NodeArgs {
      aof: true,
      aof_commit_ms: Some(-1),
      aof_commit_wait: true,
      ..Default::default()
    };
    let err = args.validate().unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::CommitWaitWithManualCommit),
      "{err:?}"
    );

    // aof_commit_ms >= 0 + aof_commit_wait => 合法
    let args = NodeArgs {
      aof: true,
      aof_commit_ms: Some(20),
      aof_commit_wait: true,
      ..Default::default()
    };
    assert!(args.validate().is_ok());

    // 命令行解析 --aof-commit-ms -1 负数
    let parsed = NodeArgs::from_args_iter(["wedb", "--aof", "--aof-commit-ms", "-1"]).unwrap();
    assert_eq!(parsed.aof_commit_ms, Some(-1));
    assert_eq!(parsed.runtime_server_options().commit_frequency_ms, -1);
  }
}
