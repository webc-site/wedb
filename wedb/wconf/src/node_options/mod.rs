//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。
//! 配置文件格式钦定 TOML（C# GarnetConf/RedisConf 双格式不转写）。
//!
//! 自研依据: 节点选项族（C# 对应 GarnetServerOptions）

use std::path::PathBuf;

use clap::Parser;
use wbase::cfg::LogCompactionType;

use crate::{
  connection_protection_option::ConnectionProtectionOption,
  lua_option_modes::{LuaLoggingMode, LuaMemoryManagementMode},
  runtime_server_options::{
    DEFAULT_AOF_REPLAY_MAX_LAG_BYTES, DEFAULT_COMPACTION_MAX_SEGMENTS,
    DEFAULT_ENABLE_SCATTER_GATHER_GET, DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS,
  },
};

mod hlog_options;
mod toml;
mod validation;
mod views;

use hlog_options::usize_u64;
pub use hlog_options::{
  DEFAULT_HLOG_PAGE_SIZE, DEFAULT_READ_CACHE_MEMORY_SIZE, HlogOptions, HlogProjection,
};
pub use toml::{ConfigFileArgs, ServerArgs};
use toml::{parse_log_compaction_type, toml_log_compaction_type};
pub use validation::{
  INDEX_MAX_SIZE_MAX_BYTES, INDEX_MAX_SIZE_MIN_BYTES, NodeOptionsError,
  SLOW_LOG_THRESHOLD_MIN_MICROS,
};
pub(crate) use validation::{parse_log_level, try_parse_log_level};
pub use views::format_bind_endpoint;

mod defaults;

pub use defaults::{
  DATA_FILE, DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS, DEFAULT_AOF_SYNC_MAX_LAG_BYTES,
  DEFAULT_AOF_TAIL_WITNESS_FREQ_MS, DEFAULT_BIND, DEFAULT_BIND_ANY,
  DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT, DEFAULT_DIR,
  DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
  DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS, DEFAULT_LOG_FLUSH_INTERVAL,
  DEFAULT_MAX_DATABASES, DEFAULT_NETWORK_BUFFER_MEMORY_BUDGET, DEFAULT_NETWORK_CONNECTION_LIMIT,
  DEFAULT_OBJECT_SCAN_COUNT_LIMIT, DEFAULT_ON_DEMAND_CHECKPOINT, DEFAULT_PORT,
  DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS, DEFAULT_REPLICA_SYNC_DELAY_MS,
  DEFAULT_REPLICA_SYNC_TIMEOUT_SECS, DEFAULT_RESP_VERSION, DEFAULT_SLOW_LOG_MAX_ENTRIES,
  DEFAULT_SLOW_LOG_THRESHOLD, DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
  INFINITE_SYNC_TIMEOUT_SECS,
};
pub(crate) use defaults::{
  DEFAULT_INDEX_RESIZE_FREQUENCY_SECS, DEFAULT_INDEX_RESIZE_THRESHOLD,
  DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS, DEFAULT_PROTECTED_MODE,
};

/// 统一节点基础参数配置
#[derive(Debug, Parser, toml_spanner::Toml, Clone)]
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

  /// WAL / AOF 物理日志存储路径（未显式指定时默认为 `<dir>`/wal）
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

  /// 最大并发网络连接数（-1 = 不限；对标 C# Options.cs:401-403 键
  /// network-connection-limit、IntRangeValidation(-1, int.MaxValue) 与上游
  /// PR #2157 后 defaults.conf:309 默认 10000（对齐 Redis maxclients）；
  /// 全监听器共享进程级上限，accept 成功即刻计量在途数，超限臂计数
  /// rejected_connections 并向明文对端写出
  /// `-ERR max number of clients reached` 后关闭（TLS 对端仅计数——明文
  /// 错误帧对只发了 ClientHello 的对端是协议违例）。运行时经
  /// CONFIG SET maxclients 可调：调低不断既有连接、只拒新连接（同 Redis）；
  /// 副本与 gossip 链路一并计入。自动纳入 TOML 导入/导出面
  #[arg(
    long = "network-connection-limit",
    default_value_t = DEFAULT_NETWORK_CONNECTION_LIMIT,
    allow_hyphen_values = true
  )]
  #[toml(default = DEFAULT_NETWORK_CONNECTION_LIMIT)]
  pub network_connection_limit: i32,

  /// 网络缓冲内存预算（对标 C# Options.cs:433-435 键
  /// network-buffer-memory-budget、[MemorySizeValidation]，defaults.conf:342
  /// 默认 "1g"，GarnetServerOptions.cs DefaultNetworkBufferMemoryBudget =
  /// 1L << 30）：活跃客户端连接网络缓冲的进程级预算，全监听器共享。连接
  /// 少时宽松、每连接全额基准规格；预算 ÷ 活跃缓冲数低于基准规格时新缓冲
  /// 基准向下适配至 16K 接收地板，使总量贴住预算。按需增长永不钳制——
  /// 大请求不受影响。0 = 禁用自适应（单连接规格不设界）。例：1g、512m
  #[arg(long = "network-buffer-memory-budget")]
  #[toml(default)]
  pub network_buffer_memory_budget: Option<String>,

  /// 是否启用 AOF 持久化日志（对标 C# Options.cs:209 EnableAOF）
  #[arg(long, default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub aof: bool,

  /// 是否禁用发布订阅功能（对标 C# ServerOptions.cs:107 DisablePubSub，
  /// C# 默认 false 即默认启用 pubsub）
  #[arg(long = "disable-pubsub", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub disable_pubsub: bool,

  /// 启动时从最新检查点与 AOF 日志恢复（若存在；对标 C# Options.cs:139 Recover）
  #[arg(short = 'r', long, default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub recover: bool,

  /// AOF 周期提交毫秒数（对标 C# Options.cs:250 CommitFrequencyMs，默认 0；-1 为手动提交）
  #[arg(long = "aof-commit-ms", allow_hyphen_values = true)]
  pub aof_commit_ms: Option<i32>,

  /// AOF 提交等待档（对标 C# Options.cs:253 WaitForCommit，选项
  /// `--aof-commit-wait`，默认 false）：置位后会话解析期按命令依赖性维护
  /// `wait_for_aof_blocking`，应答出网前阻塞等待 AOF 提交落盘
  ///（C# RespServerSession.Send 读点；代价为逐命令延迟大幅上升）
  #[arg(long = "aof-commit-wait", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub aof_commit_wait: bool,

  /// 无盘（diskless）复制同步开关（对标 C# Options.cs:458 ReplicaDisklessSync，
  /// defaults.conf:349 与 GarnetServerOptions.cs:410 默认 false）：副本侧发起
  /// 同步时按本开关在 diskless（副本经 CLUSTER ATTACH_SYNC 主动接入主端、主端
  /// 流式快照直推、零本地检查点文件）与 diskbased（检查点传输）两支选路，
  /// 消费面统一经 ClusterProvider 的 replica_diskless_sync 访问器读取
  #[arg(long = "repl-diskless-sync", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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

  /// 副本 attach 超时秒数（对标 C# Options.cs:464-466 `--repl-attach-timeout`
  /// IntRangeValidation(0, int.MaxValue) 与 GarnetServerOptions.cs:425 默认 60 秒；
  /// <= 0 = 无限超时，经 seconds_from_time_span 归 0 表达；正值越 i32::MAX 秒
  /// 契约带上界启动拒启，见 [`NodeArgs::validate`]）。
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
  #[arg(long = "sg-get", default_value_t = DEFAULT_ENABLE_SCATTER_GATHER_GET, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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
  #[arg(long = "fast-aof-truncate", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub fast_aof_truncate: bool,

  /// 按需检查点开关（对标 C# Options.cs:453-454 OnDemandCheckpoint，
  /// defaults.conf:346 与 GarnetServerOptions.cs:405 默认 true）：与
  /// fast-aof-truncate 配套——主端在副本 attach 前发现检查点覆盖起点已落后截断线时
  /// 补拍一次检查点，避免直推 AOF 丢数据。两处读者为按需重拍判据与
  /// ClusterProvider::allow_data_loss 派生式（C# ReplicaSyncSession.cs:190、:280）
  #[arg(long = "on-demand-checkpoint", default_value_t = DEFAULT_ON_DEMAND_CHECKPOINT, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default = DEFAULT_ON_DEMAND_CHECKPOINT)]
  pub on_demand_checkpoint: bool,

  /// 日志追加文件路径（设置后日志同步落文件；对标 serverSettings.FileLogger）
  #[arg(long)]
  pub file_logger: Option<String>,

  /// 控制台日志最低级别（对标 serverSettings.LogLevel；值域收 C# CLI 成员名与 conf 别名、数字 0-6，大小写不敏感）
  #[arg(long, value_parser = parse_log_level)]
  pub log_level: Option<String>,

  /// 启动静默开关：不输出启动横幅与监听就绪文本（对标 C# Options.cs:363-364
  /// QuietMode，短选项 `-q`、别名 quiet_mode；C# bool? 缺省经
  /// GetValueOrDefault 折 false。消费面 wnode ServerBootstrap::run_async，
  /// 门禁对位 GarnetServer.cs:174 横幅与 :535 `* Ready to accept connections`）
  #[arg(short = 'q', long, alias = "quiet_mode", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub quiet: bool,

  /// 关闭控制台日志 sink（对标 C# Options.cs:374-375 DisableConsoleLogger，
  /// 别名 DisableConsoleLogger；消费面 wnode LoggingBuilder::from_node，
  /// 门禁对位 GarnetServer.cs:115-122 仅在未禁用时 AddSimpleConsole）
  #[arg(long = "disable-console-logger", alias = "DisableConsoleLogger", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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
  #[arg(long = "protected-mode", default_value_t = DEFAULT_PROTECTED_MODE, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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

  /// 指标监视器采样周期秒数（0 = 禁用采样任务，维持 C# Options.cs:358-360
  /// MetricsSamplingFrequency「Value of 0 disables metrics monitor task」缺省口径；
  /// 高边 IntRangeValidation(0, int.MaxValue) 契约带闸见 [`NodeArgs::validate`]）
  #[arg(
    long = "metrics-sampling-freq",
    default_value_t = DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
  )]
  #[toml(default)]
  pub metrics_sampling_frequency_secs: u64,

  /// 是否启用延迟监视（跟踪各事件类别延迟分布；对标 C# Options.cs:344
  /// LatencyMonitor）
  #[arg(long = "latency-monitor", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub latency_monitor: bool,

  /// 是否启用逐命令使用统计（calls / failed / rejected，经 INFO COMMANDSTATS
  /// 输出；对标 C# Options.cs:348 CommandStatsMonitor）
  #[arg(long = "commandstats-monitor", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub commandstats_monitor: bool,

  /// 是否启用 Lua 脚本（对标 C# Options.cs:284 EnableLua，GarnetServerOptions.cs:91
  /// 默认 false）
  #[arg(long, default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
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
  #[arg(long = "enable-vector-set-preview", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub enable_vector_set_preview: bool,

  /// AOF 体积限额（尺寸字符串如 "64mb"，向下取 2 的幂；周期检查超限即自动
  /// checkpoint 并截断 AOF 防磁盘无界增长。空 = 关闭（默认，对标 C#
  /// Options.cs:256 AofSizeLimit = ""）；须与 AOF 同启）
  #[arg(long = "aof-size-limit")]
  pub aof_size_limit: Option<String>,

  /// AOF 常驻内存窗口上限（尺寸字符串如 "128m"，就近下取 2 的幂，超出即溢盘；
  /// 对标 C# Options.cs:211-213 AofMemorySize 旗标 `--aof-memory`
  ///（`[MemorySizeValidation]`）。缺省唯一真源在 `RuntimeServerOptions::default()`
  ///（"128m"），NodeArgs 不携带第二套缺省常量；组合互校验（须至少为 aof-page-size
  /// 的两倍）唯一真源在 wnode `AofSettings::from_options`，启动期执行。经
  /// NodeArgs 的 toml 派生自动纳入 TOML 导入/导出面
  #[arg(long = "aof-memory")]
  pub aof_memory_size: Option<String>,

  /// AOF 日志页容量（尺寸字符串如 "32m"，就近下取 2 的幂；对标 C#
  /// Options.cs:215-217 AofPageSize 旗标 `--aof-page-size`
  ///（`[MemorySizeValidation]`）。缺省唯一真源在 RuntimeServerOptions（"32m"）；
  /// 页容量下限由 wconf 页尺寸校验核（`size::validated_page_size_bits`）承担，
  /// 组合互校验（须至少为主存日志页的两倍、且不得大于 aof-segment-size）唯一
  /// 真源在 wnode `AofSettings::from_options`。经 toml 派生自动纳入
  /// TOML 导入/导出面
  #[arg(long = "aof-page-size")]
  pub aof_page_size: Option<String>,

  /// AOF 物理段（文件）容量（尺寸字符串如 "1g"，就近下取 2 的幂；段文件创建与
  /// 回收粒度。对标 C# Options.cs:219-221 AofSegmentSize 旗标
  /// `--aof-segment-size`（`[MemorySizeValidation]`）。缺省唯一真源在
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

  /// 索引自动扩容检测周期秒数（对标 C# Options.cs:616-618
  /// IndexResizeFrequencySecs IntRangeValidation(1, int.MaxValue,
  /// isRequired:false)，默认 60；高边 i32::MAX 秒契约带闸见
  /// [`NodeArgs::validate`]，低边 0 档经消费侧 `.max(1)` 折 1 秒维持现放行）
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
      network_buffer_memory_budget: None,
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
}
