//! NodeArgs 字段默认值常量族（对标 C# host/defaults.conf 单份默认值基线）
//!
//! 全部经 mod.rs 组装再导出，外部路径 `wconf::node_options::DEFAULT_*` 不变。

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
/// 可改基目录，见 `NodeArgs::checkpoint_base_dir`）、WAL 落
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
/// 默认保护模式（对标 defaults.conf ProtectedMode = "yes"）
pub(crate) const DEFAULT_PROTECTED_MODE: bool = true;
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
pub(crate) const DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS: u64 = 0;
/// 默认最大并发网络连接数（对标上游 PR #2157 后 defaults.conf:309
/// NetworkConnectionLimit = 10000 与 GarnetServerOptions.cs:362
/// DefaultNetworkConnectionLimit = 10000——对齐 Redis maxclients 默认；
/// -1 = 不限，运行时经 CONFIG SET maxclients 可调）
pub const DEFAULT_NETWORK_CONNECTION_LIMIT: i32 = 10000;
/// 默认网络缓冲内存预算字节（对标 GarnetServerOptions.cs
/// DefaultNetworkBufferMemoryBudget = 1L << 30 与 defaults.conf:342 "1g"：
/// 连接少时宽松、每连接全额基准规格；预算 ÷ 活跃缓冲数低于基准规格时新
/// 缓冲基准向下适配（接收地板 16K）。0 = 禁用自适应）
pub const DEFAULT_NETWORK_BUFFER_MEMORY_BUDGET: i64 = 1 << 30;
/// 日志文件刷盘间隔毫秒数（0 = 逐行立即刷盘；对标 C#
/// GarnetServer.cs:128 `builder.AddFile(serverSettings.FileLogger)` 省略
/// flushInterval 形参，取 FileLoggerProvider.cs:26 `int flushInterval = default`
/// 的零值默认）
pub const DEFAULT_LOG_FLUSH_INTERVAL: i32 = 0;

/// AOF 体积限额检查周期秒数（对标 GarnetServerOptions.cs:206
/// AofSizeLimitEnforceFrequencySecs = 5）
pub const DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS: u64 = 5;

/// 索引周期自动扩容检测周期秒数（对标 GarnetServerOptions.cs:167
/// IndexResizeFrequencySecs = 60）
pub(crate) const DEFAULT_INDEX_RESIZE_FREQUENCY_SECS: u64 = 60;

/// 索引自动扩容触发阈值：溢出桶数超过 index_size × 阈值% 即扩容
///（对标 GarnetServerOptions.cs:172 IndexResizeThreshold = 50）
pub(crate) const DEFAULT_INDEX_RESIZE_THRESHOLD: i64 = 50;

/// 复制同步超时缺省秒数（对标 GarnetServerOptions.cs:420 ReplicaSyncTimeout = 5；
/// <=0 = 无限超时哨兵）
pub const DEFAULT_REPLICA_SYNC_TIMEOUT_SECS: i32 = 5;
/// 复制同步超时的无限哨兵秒数（u64 槽唯一字面值源）：非正值输入经
/// `NodeArgs::runtime_server_options` 折进本值入槽，消费侧（wedb
/// ClusterProvider::replica_sync_timeout）据此判无限折 None，不折
/// `Duration::from_secs(u64::MAX)`——compio 定时器 `Instant::now() + d`
/// 于该值即溢出 panic（无限即不挂计时器、永等，对标 `Timeout.InfiniteTimeSpan`）
pub const INFINITE_SYNC_TIMEOUT_SECS: u64 = u64::MAX;
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
